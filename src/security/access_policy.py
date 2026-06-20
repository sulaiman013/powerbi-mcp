"""
Data Access Policy Engine
Enforces data access rules on queries and results
"""
import hashlib
import logging
import re
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
import yaml

logger = logging.getLogger(__name__)


# DAX result columns arrive as keys like "Sales[Amount]", "'Sales Data'[Amt]" or "[Total Sales]".
_COL_KEY_RE = re.compile(r"^\s*'?([^'\[\]]+?)'?\s*\[([^\]]+)\]\s*$")
_MEASURE_KEY_RE = re.compile(r"^\s*\[([^\]]+)\]\s*$")
# Table/column references inside a DAX query body.
_REF_RE = re.compile(r"(?:'([^']+)'|([A-Za-z_][\w ]*?))\s*\[([^\]]+)\]")
_BARE_MEASURE_RE = re.compile(r"(?<![\w'\]])\[([^\]]+)\]")


def parse_column_key(key: str) -> Tuple[Optional[str], str]:
    """Parse a DAX result column key into (table, column).

    "Sales[Amount]"      -> ("Sales", "Amount")
    "'Sales Data'[Amt]"  -> ("Sales Data", "Amt")
    "[Total Sales]"      -> (None, "Total Sales")   # measure / unqualified
    "Amount"             -> (None, "Amount")
    """
    if not isinstance(key, str):
        return None, str(key)
    m = _COL_KEY_RE.match(key)
    if m:
        return m.group(1).strip(), m.group(2).strip()
    m2 = _MEASURE_KEY_RE.match(key)
    if m2:
        return None, m2.group(1).strip()
    return None, key.strip()


def mask_value(value: Any) -> Any:
    """Partial mask used when a policy explicitly requests MASK (independent of PII detection)."""
    if value is None:
        return None
    s = str(value)
    if len(s) <= 4:
        return "*" * len(s)
    return "*" * (len(s) - 4) + s[-4:]


class PolicyAction(Enum):
    """Actions that can be taken on data"""
    ALLOW = "allow"         # Return data as-is
    MASK = "mask"           # Mask the data (use PII detector)
    BLOCK = "block"         # Block access entirely (return null/error)
    AGGREGATE_ONLY = "aggregate_only"  # Only allow in aggregations
    HASH = "hash"           # Return hashed value
    REDACT = "redact"       # Replace with [REDACTED]


class PolicyLevel(Enum):
    """Level at which policy applies"""
    TABLE = "table"
    COLUMN = "column"
    GLOBAL = "global"


@dataclass
class ColumnPolicy:
    """Policy for a specific column"""
    name: str
    action: PolicyAction = PolicyAction.ALLOW
    mask_strategy: Optional[str] = None  # partial, full, hash
    reason: str = ""
    sensitivity: str = "normal"  # low, normal, high, critical

    def to_dict(self) -> Dict[str, Any]:
        return {
            'name': self.name,
            'action': self.action.value,
            'mask_strategy': self.mask_strategy,
            'reason': self.reason,
            'sensitivity': self.sensitivity
        }


@dataclass
class TablePolicy:
    """Policy for a specific table"""
    name: str
    default_action: PolicyAction = PolicyAction.ALLOW
    columns: Dict[str, ColumnPolicy] = field(default_factory=dict)
    max_rows: Optional[int] = None
    require_filter: bool = False
    sensitivity: str = "normal"
    description: str = ""

    def get_column_policy(self, column_name: str) -> ColumnPolicy:
        """Get policy for a column, or default if not specified"""
        col_lower = column_name.lower().strip('[]')

        # Check exact match
        if col_lower in self.columns:
            return self.columns[col_lower]

        # Check pattern matches
        for col_pattern, policy in self.columns.items():
            if '*' in col_pattern:
                pattern = col_pattern.replace('*', '.*')
                if re.match(pattern, col_lower, re.IGNORECASE):
                    return policy

        # Return default policy
        return ColumnPolicy(
            name=column_name,
            action=self.default_action
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            'name': self.name,
            'default_action': self.default_action.value,
            'columns': {k: v.to_dict() for k, v in self.columns.items()},
            'max_rows': self.max_rows,
            'require_filter': self.require_filter,
            'sensitivity': self.sensitivity,
            'description': self.description
        }


@dataclass
class GlobalPolicy:
    """Global policy settings"""
    enabled: bool = True
    default_action: PolicyAction = PolicyAction.ALLOW
    max_rows_per_query: int = 10000
    enable_pii_detection: bool = True
    pii_default_action: PolicyAction = PolicyAction.MASK
    blocked_patterns: List[str] = field(default_factory=list)  # Regex patterns to block
    audit_all_queries: bool = True

    def to_dict(self) -> Dict[str, Any]:
        return {
            'enabled': self.enabled,
            'default_action': self.default_action.value,
            'max_rows_per_query': self.max_rows_per_query,
            'enable_pii_detection': self.enable_pii_detection,
            'pii_default_action': self.pii_default_action.value,
            'blocked_patterns': self.blocked_patterns,
            'audit_all_queries': self.audit_all_queries
        }


@dataclass
class PolicyCheckResult:
    """Result of a policy check"""
    allowed: bool
    action: PolicyAction
    reason: str = ""
    violations: List[Dict[str, Any]] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    columns_to_mask: List[str] = field(default_factory=list)
    columns_to_block: List[str] = field(default_factory=list)
    max_rows: Optional[int] = None


class AccessPolicyEngine:
    """
    Engine for enforcing data access policies

    Usage:
        engine = AccessPolicyEngine()
        engine.load_from_file("config/policies.yaml")

        # Check if query is allowed
        result = engine.check_query(query, tables=['Sales', 'Customers'])

        # Apply policies to results
        processed = engine.apply_to_results(results, table='Customers')
    """

    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize the policy engine

        Args:
            config_path: Path to YAML config file (optional)
        """
        self.global_policy = GlobalPolicy()
        self.table_policies: Dict[str, TablePolicy] = {}
        self._compiled_blocked_patterns: List[re.Pattern] = []

        if config_path:
            self.load_from_file(config_path)

    def load_from_file(self, config_path: str) -> bool:
        """
        Load policies from a YAML configuration file

        Args:
            config_path: Path to the YAML file

        Returns:
            True if loaded successfully
        """
        path = Path(config_path)
        if not path.exists():
            logger.warning(f"Policy config not found: {config_path}")
            return False

        try:
            with open(path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)

            self._parse_config(config)
            logger.info(f"Loaded access policies from: {config_path}")
            return True

        except Exception as e:
            logger.error(f"Failed to load policy config: {e}")
            return False

    def load_from_dict(self, config: Dict[str, Any]):
        """Load policies from a dictionary"""
        self._parse_config(config)

    def _parse_config(self, config: Dict[str, Any]):
        """Parse configuration dictionary into policy objects"""

        # Parse global settings
        if 'global' in config:
            g = config['global']
            # Handle None values from YAML (e.g., when list items are commented out)
            blocked_patterns = g.get('blocked_patterns') or []
            self.global_policy = GlobalPolicy(
                enabled=g.get('enabled', True),
                default_action=PolicyAction(g.get('default_action', 'allow')),
                max_rows_per_query=g.get('max_rows_per_query', 10000),
                enable_pii_detection=g.get('enable_pii_detection', True),
                pii_default_action=PolicyAction(g.get('pii_default_action', 'mask')),
                blocked_patterns=blocked_patterns,
                audit_all_queries=g.get('audit_all_queries', True)
            )

            # Compile blocked patterns
            self._compiled_blocked_patterns = [
                re.compile(p, re.IGNORECASE)
                for p in self.global_policy.blocked_patterns
            ]

        # Parse table policies
        if 'tables' in config:
            for table_config in config['tables']:
                table_name = table_config.get('name', '').lower()
                if not table_name:
                    continue

                columns = {}
                for col_config in table_config.get('columns', []):
                    col_name = col_config.get('name', '').lower()
                    if col_name:
                        columns[col_name] = ColumnPolicy(
                            name=col_name,
                            action=PolicyAction(col_config.get('action', 'allow')),
                            mask_strategy=col_config.get('mask_strategy'),
                            reason=col_config.get('reason', ''),
                            sensitivity=col_config.get('sensitivity', 'normal')
                        )

                self.table_policies[table_name] = TablePolicy(
                    name=table_name,
                    default_action=PolicyAction(table_config.get('default_action', 'allow')),
                    columns=columns,
                    max_rows=table_config.get('max_rows'),
                    require_filter=table_config.get('require_filter', False),
                    sensitivity=table_config.get('sensitivity', 'normal'),
                    description=table_config.get('description', '')
                )

    def add_table_policy(self, policy: TablePolicy):
        """Add or update a table policy"""
        self.table_policies[policy.name.lower()] = policy

    def add_column_policy(self, table_name: str, column_policy: ColumnPolicy):
        """Add a column policy to a table"""
        table_lower = table_name.lower()
        if table_lower not in self.table_policies:
            self.table_policies[table_lower] = TablePolicy(name=table_name)
        self.table_policies[table_lower].columns[column_policy.name.lower()] = column_policy

    def get_table_policy(self, table_name: str) -> Optional[TablePolicy]:
        """Get policy for a table"""
        return self.table_policies.get(table_name.lower().strip('[]\''))

    @staticmethod
    def extract_references(query: str) -> Tuple[List[str], List[str]]:
        """Best-effort extraction of referenced tables and columns/measures from a DAX query.

        Used so that pre-query policy checks can see which tables/columns a query touches
        even though DAX is not fully parsed. Over-approximates rather than misses.
        """
        tables: Set[str] = set()
        columns: Set[str] = set()
        if not query:
            return [], []
        for m in _REF_RE.finditer(query):
            tbl = m.group(1) or m.group(2)
            if tbl and tbl.strip():
                tables.add(tbl.strip())
            columns.add(m.group(3).strip())
        for m in _BARE_MEASURE_RE.finditer(query):
            columns.add(m.group(1).strip())
        return sorted(tables), sorted(columns)

    # Restrictiveness ordering so the strongest policy wins when several match a column.
    _ACTION_RANK = {
        PolicyAction.ALLOW: 0,
        PolicyAction.AGGREGATE_ONLY: 1,
        PolicyAction.MASK: 1,
        PolicyAction.REDACT: 2,
        PolicyAction.HASH: 2,
        PolicyAction.BLOCK: 3,
    }

    def resolve_column_policy(self, table: Optional[str], column: str) -> ColumnPolicy:
        """Resolve the effective column policy, consulting the specific table AND the '*' wildcard
        table, and falling back to scanning all tables when the owning table is unknown.

        Returns the most restrictive matching policy (wildcard-aware via TablePolicy.get_column_policy).
        """
        candidates: List[TablePolicy] = []
        if table:
            tp = self.get_table_policy(table)
            if tp:
                candidates.append(tp)
        else:
            # Owning table unknown (e.g. a measure): consider every concrete table policy.
            candidates.extend(tp for k, tp in self.table_policies.items() if k != '*')
        wildcard = self.table_policies.get('*')
        if wildcard:
            candidates.append(wildcard)

        best = ColumnPolicy(name=column, action=self.global_policy.default_action)
        for tp in candidates:
            cp = tp.get_column_policy(column)
            if self._ACTION_RANK.get(cp.action, 0) > self._ACTION_RANK.get(best.action, 0):
                best = cp
        return best

    def check_query(
        self,
        query: str,
        tables: Optional[List[str]] = None,
        columns: Optional[List[str]] = None
    ) -> PolicyCheckResult:
        """
        Check if a query is allowed by policies

        Args:
            query: The DAX query text
            tables: List of tables accessed (if known)
            columns: List of columns accessed (if known)

        Returns:
            PolicyCheckResult with decision and details
        """
        if not self.global_policy.enabled:
            return PolicyCheckResult(allowed=True, action=PolicyAction.ALLOW)

        violations = []
        warnings = []
        columns_to_mask = []
        columns_to_block = []
        max_rows = self.global_policy.max_rows_per_query

        # Check blocked patterns
        for pattern in self._compiled_blocked_patterns:
            if pattern.search(query):
                violations.append({
                    'type': 'blocked_pattern',
                    'pattern': pattern.pattern,
                    'message': f"Query matches blocked pattern: {pattern.pattern}"
                })

        # Check table policies
        if tables:
            for table in tables:
                table_policy = self.get_table_policy(table)
                if table_policy:
                    # Check table-level restrictions
                    if table_policy.default_action == PolicyAction.BLOCK:
                        violations.append({
                            'type': 'table_blocked',
                            'table': table,
                            'message': f"Access to table '{table}' is blocked"
                        })

                    # Check max rows
                    if table_policy.max_rows:
                        max_rows = min(max_rows, table_policy.max_rows)

                    # Check require_filter
                    if table_policy.require_filter:
                        # Simple check - look for FILTER or WHERE-like clauses
                        if 'FILTER' not in query.upper() and 'WHERE' not in query.upper():
                            warnings.append(f"Table '{table}' requires a filter clause")

        # Check column policies (wildcard-aware; consults concrete tables + the '*' policy)
        if columns:
            for column in columns:
                col_policy = self.resolve_column_policy(None, column)

                if col_policy.action == PolicyAction.BLOCK:
                    if column not in columns_to_block:
                        columns_to_block.append(column)
                    msg = col_policy.reason or f"Column '{column}' access blocked by policy"
                    violations.append({
                        'type': 'column_blocked',
                        'column': column,
                        'reason': msg,
                        'message': msg
                    })

                elif col_policy.action in (PolicyAction.MASK, PolicyAction.HASH, PolicyAction.REDACT):
                    if column not in columns_to_mask:
                        columns_to_mask.append(column)
                        warnings.append(f"Column '{column}' will be masked/redacted")

        # Determine final result
        allowed = len(violations) == 0
        action = PolicyAction.BLOCK if not allowed else PolicyAction.ALLOW

        return PolicyCheckResult(
            allowed=allowed,
            action=action,
            reason=violations[0]['message'] if violations else "",
            violations=violations,
            warnings=warnings,
            columns_to_mask=columns_to_mask,
            columns_to_block=columns_to_block,
            max_rows=max_rows
        )

    def apply_to_results(
        self,
        results: List[Dict[str, Any]],
        table_name: Optional[str] = None
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """
        Apply policies to query results

        Args:
            results: Query results to process
            table_name: Name of the primary table

        Returns:
            Tuple of (processed_results, policy_report)
        """
        if not self.global_policy.enabled or not results:
            return results, {'applied': False}

        processed = []
        blocked_columns = set()
        masked_columns = set()

        # Resolve the effective policy once per distinct column key (rows share the same schema).
        policy_cache: Dict[str, ColumnPolicy] = {}

        for row in results:
            processed_row = {}

            for col_name, value in row.items():
                col_policy = policy_cache.get(col_name)
                if col_policy is None:
                    # DAX result keys look like "Table[Column]" or "[Measure]"; resolve the owning
                    # table from the key, falling back to the caller-provided primary table.
                    parsed_table, parsed_col = parse_column_key(col_name)
                    lookup_table = parsed_table or table_name
                    col_policy = self.resolve_column_policy(lookup_table, parsed_col)
                    policy_cache[col_name] = col_policy

                # Apply action
                if col_policy.action == PolicyAction.BLOCK:
                    processed_row[col_name] = None
                    blocked_columns.add(col_name)

                elif col_policy.action == PolicyAction.REDACT:
                    processed_row[col_name] = "[REDACTED]"
                    masked_columns.add(col_name)

                elif col_policy.action == PolicyAction.HASH:
                    if value is not None:
                        hash_val = hashlib.sha256(str(value).encode()).hexdigest()[:12]
                        processed_row[col_name] = f"[HASH:{hash_val}]"
                    else:
                        processed_row[col_name] = None
                    masked_columns.add(col_name)

                elif col_policy.action == PolicyAction.MASK:
                    processed_row[col_name] = mask_value(value)
                    masked_columns.add(col_name)

                else:
                    processed_row[col_name] = value

            processed.append(processed_row)

        report = {
            'applied': True,
            'rows_processed': len(results),
            'blocked_columns': list(blocked_columns),
            'masked_columns': list(masked_columns)
        }

        return processed, report

    def get_column_action(self, table_name: str, column_name: str) -> PolicyAction:
        """Get the action for a specific column"""
        table_policy = self.get_table_policy(table_name)
        if table_policy:
            return table_policy.get_column_policy(column_name).action
        return self.global_policy.default_action

    def get_sensitive_columns(self, table_name: str) -> List[str]:
        """Get list of sensitive columns for a table"""
        sensitive = []
        table_policy = self.get_table_policy(table_name)
        if table_policy:
            for col_name, col_policy in table_policy.columns.items():
                if col_policy.action != PolicyAction.ALLOW:
                    sensitive.append(col_name)
        return sensitive

    def export_config(self) -> Dict[str, Any]:
        """Export current configuration as a dictionary"""
        return {
            'global': self.global_policy.to_dict(),
            'tables': [p.to_dict() for p in self.table_policies.values()]
        }

    def export_to_file(self, path: str):
        """Export configuration to a YAML file"""
        config = self.export_config()
        with open(path, 'w', encoding='utf-8') as f:
            yaml.dump(config, f, default_flow_style=False, sort_keys=False)
        logger.info(f"Exported policy config to: {path}")


# Convenience function
def create_default_policy_engine() -> AccessPolicyEngine:
    """Create a policy engine with sensible defaults"""
    engine = AccessPolicyEngine()

    # Add default policies for common sensitive columns
    default_sensitive_columns = [
        ('*', 'ssn', PolicyAction.BLOCK, 'Social Security Number'),
        ('*', 'social_security*', PolicyAction.BLOCK, 'Social Security Number'),
        ('*', 'credit_card*', PolicyAction.MASK, 'Credit Card Number'),
        ('*', 'password*', PolicyAction.BLOCK, 'Password field'),
        ('*', 'secret*', PolicyAction.BLOCK, 'Secret field'),
        ('*', 'api_key*', PolicyAction.BLOCK, 'API Key'),
        ('*', '*token*', PolicyAction.BLOCK, 'Token field'),
    ]

    # Add wildcard table for global column rules
    wildcard_table = TablePolicy(name='*')
    for table, col, action, reason in default_sensitive_columns:
        wildcard_table.columns[col] = ColumnPolicy(
            name=col,
            action=action,
            reason=reason,
            sensitivity='critical'
        )
    engine.table_policies['*'] = wildcard_table

    return engine
