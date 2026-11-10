"""
Unified security layer: access policies, PII masking and audit logging behind one object.

This layer is defence in depth for an AI client that may already query the model. It checks
query text and result column names against name-based rules and masks values that look like
personal data. It cannot see the model's data or schema, so it is not a security boundary:
row-level and object-level security in the model are. See access_policy.py for the details
of what a policy can and cannot guarantee.

Failing closed: when a policy file was configured and cannot be used (missing, unparseable,
or with a single invalid rule) the layer is LOCKED. pre_query_check refuses every query with
a reason naming the file and the problem, process_results nulls every value, and get_status
reports it. With no file configured at all the layer is permissive and get_status says that
no policies are loaded.
"""
import logging
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Tuple

from .access_policy import AccessPolicyEngine, PolicyAction, PolicyCheckResult
from .audit_logger import AuditLogger, configure_audit_logger, get_audit_logger
from .config import (
    AuditSettings,
    PIISettings,
    PolicyConfigError,
    parse_audit_settings,
    parse_pii_settings,
    read_policy_file,
)
from .pii_detector import PIIDetector
from .redaction import redact_secrets
from .transforms import resolve_hash_key

logger = logging.getLogger(__name__)

AUDIT_DIR_ENV = "POWERBI_MCP_AUDIT_DIR"
_INSTALL_ROOT = Path(__file__).parent.parent.parent


class SecurityLayer:
    """
    Unified security layer for Power BI MCP Server

    Usage:
        security = SecurityLayer(config_path="config/policies.yaml")

        check = security.pre_query_check(query, measure_definitions=measures)
        if not check.allowed:
            return error(check.reason)

        safe_rows, report = security.process_results(
            rows, query=query, measure_definitions=measures, tool="execute_dax")

    ``process_results`` repeats the policy decision for the query it is given, so a caller that
    skips ``pre_query_check`` still gets nulled rows for a query that is not allowed. Always
    pass the query: without it only result column names can be checked.
    """

    def __init__(
        self,
        config_path: Optional[str] = None,
        enable_pii_detection: bool = True,
        enable_audit: bool = True,
        enable_policies: bool = True,
        audit_logger: Optional[AuditLogger] = None,
    ):
        """
        Args:
            config_path: Path to policies.yaml. None means "no policy file": permissive.
                A path that does not exist, or a file with any error, locks the layer.
            enable_pii_detection: Enable PII detection
            enable_audit: Enable audit logging
            enable_policies: Enable policy enforcement. When False a broken policy file is
                reported in the status but does not lock the layer.
            audit_logger: Use this logger instead of the one the configuration describes
        """
        self.enable_pii_detection = enable_pii_detection
        self.enable_audit = enable_audit
        self.enable_policies = enable_policies
        self.config_path: Optional[str] = str(config_path) if config_path else None
        self.config_error: Optional[str] = None
        self.audit_all_queries = True

        self.policy_engine = AccessPolicyEngine() if enable_policies else None
        # A hash key too short to use is a configuration error like any other: the engine
        # locks on it, and the status reports it even when policies are switched off.
        self.config_error = resolve_hash_key()[2]

        pii_settings = PIISettings()
        audit_settings = AuditSettings()
        if self.config_path:
            loaded = self._load_config(self.config_path)
            if loaded is not None:
                pii_settings, audit_settings = loaded

        self.pii_detector = PIIDetector(
            default_strategy=pii_settings.default_strategy,
            enabled_types=pii_settings.enabled_types,
            type_strategies=pii_settings.type_strategies,
            max_scan_chars=pii_settings.max_scan_chars,
        ) if self.enable_pii_detection else None

        self.audit_logger: Optional[AuditLogger] = None
        if enable_audit:
            self.audit_logger = audit_logger or self._make_audit_logger(audit_settings)

        logger.info(
            "Security layer initialized (PII: %s, Audit: %s, Policies: %s%s)",
            self.enable_pii_detection, enable_audit, enable_policies,
            ", LOCKED: " + self.config_error if self.locked else "")

    # ------------------------------------------------------------------ configuration

    @property
    def locked(self) -> bool:
        """True when queries are refused because the configuration cannot be trusted."""
        return bool(self.config_error) and self.enable_policies

    def _load_config(self, config_path: str) -> Optional[Tuple[PIISettings, AuditSettings]]:
        """Read and validate the whole file. On any problem the layer is locked and None is
        returned, so the defaults (every PII type, default audit log) stay in force."""
        try:
            config = read_policy_file(config_path)
            # A scratch engine validates the policy sections even when enforcement is off, so
            # the status can still say the file is broken.
            engine = self.policy_engine if self.policy_engine is not None else AccessPolicyEngine()
            engine.load_from_dict(config, source=config_path)
            global_section = config.get("global") or {}
            pii_settings = parse_pii_settings(config.get("pii"), global_section.get("pii_default_action"))
            audit_settings = parse_audit_settings(config.get("audit"))
        except PolicyConfigError as e:
            self.config_error = f"policy file {config_path}: {e}"
            if self.policy_engine is not None:
                self.policy_engine.lock(self.config_error)
            else:
                logger.error("Security configuration is invalid (policies are disabled, so queries "
                             "are not refused): %s", self.config_error)
            return None
        if not engine.global_policy.enable_pii_detection:
            self.enable_pii_detection = False
        self.audit_all_queries = engine.global_policy.audit_all_queries
        logger.info("Loaded security config from: %s", config_path)
        return pii_settings, audit_settings

    @staticmethod
    def _make_audit_logger(settings: AuditSettings) -> AuditLogger:
        # POWERBI_MCP_AUDIT_DIR overrides where the log goes without editing the policy file:
        # for a read-only install folder, a container volume, or a test run.
        override = os.environ.get(AUDIT_DIR_ENV, "").strip()
        if not settings.present and not override:
            return get_audit_logger()
        log_dir = None
        if override:
            log_dir = override
        elif settings.log_dir:
            path = Path(settings.log_dir)
            # A relative folder is relative to the server's install folder, not to whatever
            # directory the MCP client happened to start the process in.
            log_dir = str(path if path.is_absolute() else _INSTALL_ROOT / path)
        return configure_audit_logger(
            log_dir=log_dir,
            log_file=settings.log_file,
            max_file_size_mb=settings.max_file_size_mb,
            backup_count=settings.backup_count,
            store_query_text=settings.store_query_text,
            required=settings.required,
        )

    # ------------------------------------------------------------------ query path

    def pre_query_check(
        self,
        query: str,
        tables: Optional[List[str]] = None,
        columns: Optional[List[str]] = None,
        measure_definitions: Optional[Mapping[str, str]] = None,
    ) -> PolicyCheckResult:
        """
        Check if a query is allowed before execution.

        Args:
            query: The DAX query
            tables, columns: Only used when ``query`` is empty (see AccessPolicyEngine.check_query)
            measure_definitions: Measure name -> DAX of the connected model, so measures the
                query references are checked through their definitions

        Returns:
            PolicyCheckResult with the decision, the reason and the row cap
        """
        if not self.enable_policies or not self.policy_engine:
            return PolicyCheckResult(allowed=True, action=PolicyAction.ALLOW)

        return self.policy_engine.check_query(query, tables, columns, measure_definitions)

    def process_results(
        self,
        results: List[Dict[str, Any]],
        query: str = "",
        source: str = "desktop",
        model_name: Optional[str] = None,
        port: Optional[int] = None,
        table_name: Optional[str] = None,
        duration_ms: Optional[float] = None,
        success: bool = True,
        error_message: Optional[str] = None,
        measure_definitions: Optional[Mapping[str, str]] = None,
        tool: Optional[str] = None,
        enforce_row_limit: bool = True,
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """
        Filter query results through the access policy and PII masking, and audit the query.

        Args:
            results: Raw rows (a scalar result is passed as one row)
            query: The executed query. Needed for everything beyond name matching.
            source: "desktop" or "cloud"
            model_name: Name of the model
            port: Port (for desktop)
            table_name: Table to assume for result keys that carry none
            duration_ms: Query duration
            success: Whether the query succeeded
            error_message: Error if it failed
            measure_definitions: Measure name -> DAX of the connected model
            tool: Name of the MCP tool that ran the query (recorded in the audit log)
            enforce_row_limit: Cut the rows to the policy's row cap. Leave on for data
                queries; turn off only for the server's own metadata queries.

        Returns:
            (processed_results, security_report). ``security_report['query_denied']`` is True
            when the policy refuses the query; every value is then None.
        """
        start_time = time.time()
        rows = results if isinstance(results, list) else list(results or [])
        returned_by_engine = len(rows)
        security_report: Dict[str, Any] = {
            'pii_detected': False,
            'pii_count': 0,
            'pii_types': [],
            'policy_applied': False,
            'query_denied': False,
            'denied_reason': None,
            'locked': self.locked,
            'columns_masked': [],
            'columns_blocked': [],
            'columns_transformed': [],
            'derived_action': PolicyAction.ALLOW.value,
            'rows_truncated': False,
            'max_rows': None,
        }
        check: Optional[PolicyCheckResult] = None
        transformed: List[str] = []

        if self.enable_policies and self.policy_engine:
            engine = self.policy_engine
            decision = engine._decide(query, measure_definitions=measure_definitions) if (
                query or engine.config_error) else None
            check = decision.result if decision else None
            if check is not None and check.allowed and check.max_rows:
                security_report['max_rows'] = check.max_rows
                if enforce_row_limit and len(rows) > check.max_rows:
                    rows = rows[:check.max_rows]
                    security_report['rows_truncated'] = True
            rows, policy_report = engine._apply(rows, decision, table_name)
            security_report['policy_applied'] = policy_report.get('applied', False)
            security_report['columns_blocked'] = policy_report.get('blocked_columns', [])
            security_report['columns_masked'] = list(policy_report.get('masked_columns', []))
            transformed = policy_report.get('transformed_columns', [])
            security_report['columns_transformed'] = list(transformed)
            security_report['derived_action'] = policy_report.get(
                'derived_action', PolicyAction.ALLOW.value)
            if check is not None and not check.allowed:
                security_report['query_denied'] = True
                security_report['denied_reason'] = check.reason

        pii_columns: List[str] = []
        if self.pii_detector and rows and not security_report['query_denied']:
            rows, pii_summary = self.pii_detector.process_results(rows, skip_columns=transformed)
            security_report['pii_detected'] = pii_summary['total_detections'] > 0
            security_report['pii_count'] = pii_summary['total_detections']
            security_report['pii_types'] = pii_summary['types_detected']
            pii_columns = pii_summary['columns_affected']
            security_report['columns_masked'].extend(c for c in pii_columns
                                                     if c not in security_report['columns_masked'])

        if self.enable_audit and self.audit_logger:
            self._audit_query(query, source, model_name, port, returned_by_engine, duration_ms, success,
                              error_message, security_report, check, pii_columns, tool)

        security_report['processing_time_ms'] = (time.time() - start_time) * 1000
        return rows, security_report

    def safe_error_text(
        self,
        message: Any,
        query: str = "",
        measure_definitions: Optional[Mapping[str, str]] = None,
        extra_secrets: Optional[List[str]] = None,
    ) -> str:
        """An engine error message that is safe to return to the model.

        An error can carry data: ERROR(CONCATENATEX(T, T[col], ",")) puts column values in
        the message, and conversion errors quote the value that failed. So:
          * if the query is refused by policy, or reads any protected column, the engine's
            text is withheld and a fixed sentence is returned;
          * otherwise secrets are redacted and PII found in the text is masked.
        """
        text = redact_secrets(message, extra_secrets)
        if self.enable_policies and self.policy_engine and (query or self.policy_engine.config_error):
            check = self.policy_engine.check_query(query, measure_definitions=measure_definitions)
            if not check.allowed:
                return f"Query blocked by security policy: {check.reason}"
            if check.derived_action != PolicyAction.ALLOW:
                return ("The query failed. The engine's error text is withheld because the query reads "
                        "protected column(s) and an error message can quote their values.")
        if self.pii_detector:
            text = str(self.pii_detector.process_value(text, None)[0])
        return text

    def _audit_query(self, query, source, model_name, port, row_count, duration_ms, success,
                     error_message, report, check, pii_columns, tool) -> None:
        audit = self.audit_logger
        notable = (not success or report['pii_detected'] or report['query_denied']
                   or bool(report['columns_transformed']))
        if report['query_denied']:
            audit.log_policy_violation(policy_name="query_policy", violation_type=report['denied_reason'],
                                       query=query, tool=tool)
        if self.audit_all_queries or notable:
            policy = None
            if report['policy_applied'] or report['query_denied']:
                policy = {
                    'denied': report['query_denied'],
                    'reason': report['denied_reason'],
                    'blocked_columns': report['columns_blocked'],
                    'transformed_columns': report['columns_transformed'],
                    'derived_action': report['derived_action'],
                    'rows_truncated': report['rows_truncated'],
                }
            audit.log_query(
                query=query,
                source=source,
                model_name=model_name,
                port=port,
                result_count=row_count,
                duration_ms=duration_ms,
                success=success,
                error_message=error_message,
                tables_accessed=check.tables[:100] if check else None,
                columns_accessed=check.columns[:200] if check else None,
                pii_detected=report['pii_detected'],
                pii_types=report['pii_types'],
                pii_count=report['pii_count'],
                policy_applied=policy,
                tool=tool,
            )
        if report['pii_detected']:
            audit.log_pii_detection(
                pii_types=report['pii_types'],
                count=report['pii_count'],
                columns_affected=pii_columns,
                action_taken='masked',
                tool=tool,
            )

    # ------------------------------------------------------------------ audit helpers

    def log_connection(
        self,
        source: str,
        model_name: Optional[str] = None,
        port: Optional[int] = None,
        workspace: Optional[str] = None,
        success: bool = True,
        error_message: Optional[str] = None,
        tool: Optional[str] = None,
    ):
        """Log a connection event"""
        if self.enable_audit and self.audit_logger:
            self.audit_logger.log_connection(
                source=source,
                model_name=model_name,
                port=port,
                workspace=workspace,
                success=success,
                error_message=error_message,
                tool=tool,
            )

    def log_policy_violation(
        self,
        policy_name: str,
        violation_type: str,
        table: Optional[str] = None,
        column: Optional[str] = None,
        query: Optional[str] = None,
        tool: Optional[str] = None,
    ):
        """Log a policy violation"""
        if self.enable_audit and self.audit_logger:
            self.audit_logger.log_policy_violation(
                policy_name=policy_name,
                violation_type=violation_type,
                table=table,
                column=column,
                action_taken='blocked',
                query=query,
                tool=tool,
            )

    def verify_audit_integrity(self) -> Dict[str, Any]:
        """Verify the tamper-evident hash chain of the audit log."""
        if not self.enable_audit or not self.audit_logger:
            return {"valid": True, "checked": 0, "message": "Audit logging is disabled."}
        return self.audit_logger.verify_chain()

    # ------------------------------------------------------------------ status

    def get_status(self) -> Dict[str, Any]:
        """Current state of the layer, including whether it is locked and why, whether any
        policies are loaded, and whether the audit log has failed to record anything."""
        engine = self.policy_engine
        if self.config_error:
            policy_message = ("LOCKED: " if self.locked else "Configuration error (policies disabled): ") \
                + self.config_error
        elif not self.enable_policies:
            policy_message = "Access policies are disabled."
        elif engine is not None and engine.policies_loaded:
            policy_message = f"Policies loaded from {self.config_path}."
        else:
            policy_message = "No policy file is configured: no column or table policies are loaded."
        detector = self.pii_detector
        return {
            'enabled': {
                'pii_detection': self.enable_pii_detection,
                'audit_logging': self.enable_audit,
                'access_policies': self.enable_policies
            },
            'locked': self.locked,
            'config_error': self.config_error,
            'config_path': self.config_path,
            'pii_detector': {
                'strategy': detector.default_strategy.value if detector else None,
                'enabled_types': [t.value for t in detector.enabled_types] if detector else [],
                'type_strategies': {t.value: s.value for t, s in detector.type_strategies.items()}
                if detector else {},
                'max_scan_chars': detector.max_scan_chars if detector else None,
            },
            'audit': self.audit_logger.get_session_summary() if self.audit_logger else None,
            'policies': {
                'loaded': bool(engine and engine.policies_loaded),
                'source': engine.config_source if engine else None,
                'table_count': len(engine.table_policies) if engine else 0,
                'global_enabled': engine.global_policy.enabled if engine else False,
                'hash_key_persistent': engine.hash_key_persistent if engine else False,
                'message': policy_message,
            }
        }

    def get_policy_summary(self) -> Dict[str, Any]:
        """Get summary of active policies"""
        if not self.policy_engine:
            return {'enabled': False, 'locked': False, 'loaded': False, 'config_error': self.config_error}

        engine = self.policy_engine
        return {
            'enabled': engine.global_policy.enabled,
            'locked': self.locked,
            'loaded': engine.policies_loaded,
            'config_error': self.config_error,
            'max_rows': engine.global_policy.max_rows_per_query,
            'pii_detection': self.enable_pii_detection,
            'pii_action': engine.global_policy.pii_default_action.value,
            'tables_with_policies': list(engine.table_policies.keys()),
            'blocked_patterns_count': len(engine.global_policy.blocked_patterns)
        }


# Global security layer instance
_security_layer: Optional[SecurityLayer] = None


def get_security_layer() -> SecurityLayer:
    """Get or create the global security layer"""
    global _security_layer
    if _security_layer is None:
        # The bundled default file is the only one allowed to be absent.
        config_path = _INSTALL_ROOT / "config" / "policies.yaml"
        _security_layer = SecurityLayer(
            config_path=str(config_path) if config_path.exists() else None
        )
    return _security_layer


def configure_security_layer(**kwargs) -> SecurityLayer:
    """Configure and return the global security layer"""
    global _security_layer
    _security_layer = SecurityLayer(**kwargs)
    return _security_layer
