"""
Power BI MCP Server

One stdio MCP server over five integration paths: Power BI Desktop (ADOMD), Power BI Service
(XMLA + REST), live model writes (TOM), offline PBIP files (TMDL + PBIR), and the Desktop
Bridge. This module owns the tool registry, the request boundary (read-only gate, statement
guard, policy and PII enforcement, secret redaction) and the handlers; the work itself lives
in the connectors and the pure modules beside it.
"""
import asyncio
import json
import logging
import os
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from mcp.server import Server, NotificationOptions
from mcp.server.stdio import stdio_server
from mcp.types import (
    Tool, TextContent, ToolAnnotations,
    Resource, ResourceTemplate,
    Prompt, PromptArgument, PromptMessage, GetPromptResult,
    Completion,
)
from mcp.server.models import InitializationOptions
from urllib.parse import unquote

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stderr)]
)
logger = logging.getLogger("powerbi-mcp")

__version__ = "3.8.0"


def env_flag(name: str, default: bool = False) -> bool:
    """Read a boolean environment variable: 1/true/yes/on and 0/false/no/off in any case.
    Unset or blank gives ``default``; so does an unrecognised value, with a warning. (The
    server's own safety switches are read with a stricter fallback, see PowerBIMCPServer.)"""
    raw = os.getenv(name)
    value = _security_env_flag(name, default, invalid=default)
    if raw is not None and raw.strip() and raw.strip().lower() not in (
            "1", "true", "yes", "on", "0", "false", "no", "off"):
        logger.warning(f"{name}={raw!r} is not a recognised boolean; using {default}")
    return value


def redact_secrets(text: Any, extra_secrets: Optional[List[str]] = None) -> str:
    """Redact connection-string secrets, tokens and known secret values before text is logged
    or returned to the client. Implemented in security.redaction; kept here under its
    long-standing name because handlers and tests call it as server.redact_secrets."""
    return _security_redact_secrets(text, extra_secrets)


def define_measures_block(measures: List[Dict[str, str]]) -> str:
    """A DEFINE block declaring query-scoped measures: [{table, name, expression}, ...].

    Each expression sits on its own lines, so a trailing ``--`` comment in one measure cannot
    swallow the next declaration. Returns "" for an empty list.
    """
    if not measures:
        return ""
    lines = ["DEFINE"]
    for m in measures:
        host = dax_tokens.quote_table(str(m["table"]), force=True)
        lines.append(f"\tMEASURE {host}{dax_tokens.bracket(str(m['name']))} =\n{(m.get('expression') or '').strip()}")
    return "\n".join(lines) + "\n"


def build_validation_probe(dax: str, as_measure: bool = False, table: Optional[str] = None,
                           extra_measures: Optional[List[Dict[str, str]]] = None) -> str:
    """Wrap DAX into an executable probe that makes the engine parse and evaluate it.

    A full query (EVALUATE / DEFINE, possibly after comments) runs as it is. A measure
    expression is validated as a measure: given a host ``table`` it becomes a query-scoped
    DEFINE MEASURE, which is how the engine will compile the real one, so a stray top-level
    comma is rejected instead of being read as extra ROW() arguments. Without a table it is
    evaluated inside ROW(). ``extra_measures`` ([{table, name, expression}]) are declared in
    the same query, so the expression may reference measures that do not exist in the model
    yet. The expression always sits on its own lines, so a trailing ``--`` comment cannot
    swallow the rest of the probe.
    """
    stripped = (dax or "").strip()
    if not as_measure and dax_tokens.statement_kind(stripped) == "dax":
        return stripped
    if table:
        probe = {"table": table, "name": "__mcp_validation__", "expression": stripped}
        return (define_measures_block(list(extra_measures or []) + [probe])
                + 'EVALUATE ROW("validation", [__mcp_validation__])')
    return define_measures_block(list(extra_measures or [])) + f'EVALUATE ROW("validation",\n{stripped}\n)'


# Real constraints on INFO.CALCDEPENDENCY (not "old engine"): it needs write permission on
# the model and cannot run over a live Power BI Desktop connection.
INFO_CALCDEP_NOTE = (
    "INFO.CALCDEPENDENCY needs write permission on the model and does not run over a live "
    "Power BI Desktop connection (it also requires a reasonably recent engine)."
)


# Import connectors
from powerbi_rest_connector import PowerBIRestConnector
from powerbi_xmla_connector import PowerBIXmlaConnector
from powerbi_desktop_connector import PowerBIDesktopConnector
from powerbi_tom_connector import PowerBITOMConnector
from powerbi_pbip_connector import PowerBIPBIPConnector

# Pure-Python model analysis (BPA + AI-readiness), refresh diagnostics, governance
import model_analysis
import refresh_diagnostics
import governance
import dax_lint
import svg_measures
import naming_audit
import pbix_tools
import publish_stage
import report_spec
import report_audit
import html_measures
import js_app_measure
import model_embed
import payload_measures as payload_gen
import bpa_authoring
import dax_generator
import dax_tokens
import safe_io
import star_schema
import tmdl_authoring
import desktop_bridge

# Import security layer
from security import SecurityLayer
from security import env_flag as _security_env_flag
from security import redact_secrets as _security_redact_secrets
from security import redact_structure as security_redact_structure


class PowerBIMCPServer:
    """Power BI MCP Server supporting Cloud and Desktop connectivity"""

    def __init__(self):
        self.server = Server("powerbi-mcp")

        # Cloud credentials (optional for Desktop-only usage)
        self.tenant_id = os.getenv("TENANT_ID", "")
        self.client_id = os.getenv("CLIENT_ID", "")
        self.client_secret = os.getenv("CLIENT_SECRET", "")

        # Connector instances
        self.rest_connector: Optional[PowerBIRestConnector] = None
        self.xmla_connector_cache: Dict[str, PowerBIXmlaConnector] = {}
        self.desktop_connector: Optional[PowerBIDesktopConnector] = None
        self.tom_connector: Optional[PowerBITOMConnector] = None
        self.pbip_connector: Optional[PowerBIPBIPConnector] = None

        # A TOM transaction is owned by the connector (see _tom_transaction_active).
        # Measures created inside the open transaction ({table, name, expression}). They are
        # not on the server until commit, so validation probes declare them with DEFINE MEASURE:
        # a later edit in the same transaction can reference them and still be checked.
        self._tom_pending_measures: List[Dict[str, str]] = []

        # Model metadata gathered through INFO.VIEW is reused briefly: completion, resources
        # and back-to-back analysis tools would otherwise each re-run four metadata queries.
        self._metadata_cache: Dict[Any, Any] = {}

        # Security layer. POWERBI_MCP_POLICY_FILE names an explicit policy file; a path that is
        # set but missing is passed through on purpose so the layer refuses queries instead of
        # running with no policies at all. Only the bundled default is allowed to be absent.
        explicit_policy = os.getenv("POWERBI_MCP_POLICY_FILE", "").strip()
        default_policy = Path(__file__).parent.parent / "config" / "policies.yaml"
        if explicit_policy:
            policy_path: Optional[str] = explicit_policy
        else:
            policy_path = str(default_policy) if default_policy.exists() else None
        self.security = SecurityLayer(
            config_path=policy_path,
            enable_pii_detection=_security_env_flag("ENABLE_PII_DETECTION", True, invalid=True),
            enable_audit=_security_env_flag("ENABLE_AUDIT", True, invalid=True),
            enable_policies=_security_env_flag("ENABLE_POLICIES", True, invalid=True),
        )

        # Single source of truth for call routing and MCP safety hints. New tools
        # register here (dispatch + annotations) in addition to handle_list_tools.
        self._tool_dispatch = self._build_tool_dispatch()
        self._tool_annotations = self._build_tool_annotations()
        self._prompts = self._build_prompts()

        # Read-only / lockdown mode: when POWERBI_MCP_READONLY is on, every write tool is
        # refused. Write tools = destructive ops plus the non-destructive creates/commit.
        # A value that cannot be read as a boolean turns lockdown ON: a typo must not
        # silently leave the write tools enabled.
        self._read_only = _security_env_flag("POWERBI_MCP_READONLY", False, invalid=True)
        # Tools refused in read-only/lockdown mode = anything that mutates a model/report
        # (destructive-hinted) plus the non-destructive creates/commit, plus the tools that
        # WRITE FILES to disk (a data dictionary, a snapshot, an extracted PBIX). Session-only
        # tools (desktop_connect, desktop_set_rls_role) are intentionally NOT refused: they let
        # an agent connect and read under lockdown without persisting any change.
        self._write_tools = (
            {n for n, a in self._tool_annotations.items() if a.destructiveHint}
            | {"create_measure", "create_relationship", "tom_commit_transaction",
               "batch_create_measures"}
            | {"export_data_dictionary", "model_snapshot", "pbix_extract", "bridge_screenshot"}
        )
        # Every tool that is not annotated read-only is refused in lockdown unless it is one of
        # these session-scoped tools, which change no model, report or file. A new write tool is
        # therefore gated by default instead of depending on someone remembering to list it.
        session_only = {"desktop_connect", "desktop_set_rls_role", "tom_begin_transaction",
                        "tom_rollback_transaction"}
        # These three stay available in lockdown and refuse only their own write option.
        session_only |= {"generate_measure_suite", "build_js_app_measure", "cross_workspace_lineage"}
        self._write_tools |= {n for n, a in self._tool_annotations.items()
                              if not a.readOnlyHint and n not in session_only}
        # generate_measure_suite is generation-only by default; its write targets
        # (target="pbip"/"live") are refused inside the handler when read-only.

        self._setup_handlers()

    def _build_prompts(self):
        """Reusable, user-invokable BI workflows exposed as MCP prompts. Each renders a
        guidance message that orchestrates the right tools in the right order."""
        return {
            "optimize_measure": {
                "title": "Optimize a DAX measure",
                "description": "Diagnose and optimize a DAX measure safely",
                "arguments": [PromptArgument(name="measure_name", description="Measure to optimize", required=True)],
                "render": lambda a: (
                    f"Optimize the DAX measure [{a.get('measure_name','<name>')}] in the connected model.\n"
                    "1) scan_measure_dependencies to understand what it uses and what depends on it.\n"
                    "2) analyze_query_performance on a query that exercises it to get a baseline.\n"
                    "3) Propose an improved expression; validate_dax it before changing anything.\n"
                    "4) Apply with create_measure/batch_update_measures (inside a tom transaction) and re-check."
                ),
            },
            "explain_measure": {
                "title": "Explain a measure",
                "description": "Explain what a measure computes in business terms",
                "arguments": [PromptArgument(name="measure_name", description="Measure to explain", required=True)],
                "render": lambda a: (
                    f"Explain the measure [{a.get('measure_name','<name>')}] in plain business language.\n"
                    "Use desktop_list_measures for its DAX and scan_measure_dependencies for context. "
                    "Describe inputs, the calculation, filter context, and a usage example."
                ),
            },
            "audit_model": {
                "title": "Audit the model",
                "description": "Full quality + AI-readiness audit of the connected model",
                "arguments": [],
                "render": lambda a: (
                    "Audit the connected Power BI model end to end:\n"
                    "1) run_bpa (note errors/warnings by category).\n"
                    "2) audit_ai_readiness (descriptions/format coverage).\n"
                    "3) analyze_model_storage (largest tables).\n"
                    "Then summarize the top issues and a prioritized remediation plan."
                ),
            },
            "document_model": {
                "title": "Document the model",
                "description": "Generate human-readable documentation for the connected model",
                "arguments": [],
                "render": lambda a: (
                    "Generate documentation for the connected model.\n"
                    "Use get_model_info / desktop_get_model_info and the powerbi://desktop/schema resource. "
                    "Produce: overview, table-by-table (purpose, key columns), key measures (with descriptions), "
                    "relationships, and any best-practice issues from run_bpa."
                ),
            },
            "pre_deploy_review": {
                "title": "Pre-deploy quality gate",
                "description": "Run a full quality gate before shipping a model/report",
                "arguments": [],
                "render": lambda a: (
                    "Run a pre-deployment quality gate on the connected model and report the verdict:\n"
                    "1) run_bpa (fail the gate on any error-severity findings).\n"
                    "2) audit_ai_readiness (warn if score < 70).\n"
                    "3) export_data_dictionary (so docs ship with the release).\n"
                    "4) If a .pbip project is loaded: pbip_validate + pbip_scan_broken_refs.\n"
                    "Summarize PASS/FAIL with the blocking issues and a remediation checklist."
                ),
            },
            "plan_safe_rename": {
                "title": "Plan a safe rename",
                "description": "Plan a rename that won't break visuals or downstream measures",
                "arguments": [
                    PromptArgument(name="old_name", description="Current name", required=True),
                    PromptArgument(name="new_name", description="New name", required=True),
                ],
                "render": lambda a: (
                    f"Plan a SAFE rename from [{a.get('old_name','<old>')}] to [{a.get('new_name','<new>')}].\n"
                    "1) scan_measure_dependencies (downstream) and pbip_scan_broken_refs to see impact.\n"
                    "2) Use the PBIP tools (pbip_rename_tables/columns/measures) which update model AND report visuals - "
                    "do NOT use the deprecated TOM batch_rename_* tools (they break visuals).\n"
                    "3) After renaming, run pbip_validate and pbip_scan_broken_refs to confirm nothing is broken."
                ),
            },
        }

    def _build_tool_dispatch(self):
        """Map tool name -> coroutine handler. Every entry accepts the args dict
        (handlers that take no arguments simply ignore it). Replaces the former
        34-branch if/elif chain so list_tools and call_tool cannot drift apart."""
        return {
            # Desktop
            "desktop_discover_instances": lambda a: self._handle_desktop_discover(),
            "desktop_connect": lambda a: self._handle_desktop_connect(a),
            "desktop_list_tables": lambda a: self._handle_desktop_list_tables(),
            "desktop_list_columns": lambda a: self._handle_desktop_list_columns(a),
            "desktop_list_measures": lambda a: self._handle_desktop_list_measures(),
            "desktop_execute_dax": lambda a: self._handle_desktop_execute_dax(a),
            "desktop_get_model_info": lambda a: self._handle_desktop_get_model_info(),
            # Cloud
            "list_workspaces": lambda a: self._handle_list_workspaces(),
            "list_datasets": lambda a: self._handle_list_datasets(a),
            "list_tables": lambda a: self._handle_list_tables(a),
            "list_columns": lambda a: self._handle_list_columns(a),
            "execute_dax": lambda a: self._handle_execute_dax(a),
            "get_model_info": lambda a: self._handle_get_model_info(a),
            # Security
            "security_status": lambda a: self._handle_security_status(),
            "security_audit_log": lambda a: self._handle_security_audit_log(a),
            # RLS
            "desktop_list_rls_roles": lambda a: self._handle_desktop_list_rls_roles(),
            "desktop_set_rls_role": lambda a: self._handle_desktop_set_rls_role(a),
            "desktop_rls_status": lambda a: self._handle_desktop_rls_status(),
            # TOM write operations
            "batch_rename_tables": lambda a: self._handle_batch_rename_tables(a),
            "batch_rename_columns": lambda a: self._handle_batch_rename_columns(a),
            "batch_rename_measures": lambda a: self._handle_batch_rename_measures(a),
            "batch_update_measures": lambda a: self._handle_batch_update_measures(a),
            "create_measure": lambda a: self._handle_create_measure(a),
            "delete_measure": lambda a: self._handle_delete_measure(a),
            "scan_table_dependencies": lambda a: self._handle_scan_table_dependencies(a),
            # PBIP file-based editing
            "pbip_load_project": lambda a: self._handle_pbip_load_project(a),
            "pbip_get_project_info": lambda a: self._handle_pbip_get_project_info(),
            "pbip_rename_tables": lambda a: self._handle_pbip_rename_tables(a),
            "pbip_rename_columns": lambda a: self._handle_pbip_rename_columns(a),
            "pbip_rename_measures": lambda a: self._handle_pbip_rename_measures(a),
            # PBIP repair
            "pbip_fix_broken_visuals": lambda a: self._handle_pbip_fix_broken_visuals(a),
            "pbip_fix_dax_quoting": lambda a: self._handle_pbip_fix_dax_quoting(a),
            "pbip_scan_broken_refs": lambda a: self._handle_pbip_scan_broken_refs(),
            "pbip_validate": lambda a: self._handle_pbip_validate(),
            # PBIR report authoring (preview)
            "pbir_add_page": lambda a: self._handle_pbir_add_page(a),
            "pbir_add_visual": lambda a: self._handle_pbir_add_visual(a),
            "pbir_bind_fields": lambda a: self._handle_pbir_bind_fields(a),
            "pbir_validate_report": lambda a: self._handle_pbir_validate_report(),
            # DAX safety loop + transactions (Bundle A)
            "validate_dax": lambda a: self._handle_validate_dax(a),
            "scan_measure_dependencies": lambda a: self._handle_scan_measure_dependencies(a),
            "tom_begin_transaction": lambda a: self._handle_tom_begin_transaction(),
            "tom_commit_transaction": lambda a: self._handle_tom_commit_transaction(),
            "tom_rollback_transaction": lambda a: self._handle_tom_rollback_transaction(),
            # Model quality & performance (Bundle B)
            "run_bpa": lambda a: self._handle_run_bpa(a),
            "audit_ai_readiness": lambda a: self._handle_audit_ai_readiness(a),
            "analyze_model_storage": lambda a: self._handle_analyze_model_storage(a),
            "analyze_query_performance": lambda a: self._handle_analyze_query_performance(a),
            "export_data_dictionary": lambda a: self._handle_export_data_dictionary(a),
            "model_snapshot": lambda a: self._handle_model_snapshot(a),
            "model_diff": lambda a: self._handle_model_diff(a),
            "pre_deploy_gate": lambda a: self._handle_pre_deploy_gate(a),
            # Diagnostics & ops (Wave 2)
            "refresh_doctor": lambda a: self._handle_refresh_doctor(a),
            "find_unused_objects": lambda a: self._handle_find_unused_objects(a),
            "impact_analysis": lambda a: self._handle_impact_analysis(a),
            "rls_test_harness": lambda a: self._handle_rls_test_harness(a),
            "run_dax_tests": lambda a: self._handle_run_dax_tests(a),
            "verify_audit_integrity": lambda a: self._handle_verify_audit_integrity(),
            # Governance-ops fleet (Wave 3, admin-gated)
            "cross_workspace_lineage": lambda a: self._handle_cross_workspace_lineage(a),
            "fleet_refresh_monitor": lambda a: self._handle_fleet_refresh_monitor(a),
            "usage_and_orphan_analytics": lambda a: self._handle_usage_and_orphan_analytics(a),
            # Relationship management (Bundle D)
            "create_relationship": lambda a: self._handle_create_relationship(a),
            "delete_relationship": lambda a: self._handle_delete_relationship(a),
            # DAX quality (Wave 4: reach + quality)
            "dax_lint": lambda a: self._handle_dax_lint(a),
            "dax_suggest_rewrite": lambda a: self._handle_dax_suggest_rewrite(a),
            # Authoring helpers (Wave 4)
            "generate_svg_measure": lambda a: self._handle_generate_svg_measure(a),
            "audit_naming": lambda a: self._handle_audit_naming(a),
            # PBIX onboarding (Wave 4)
            "pbix_inspect": lambda a: self._handle_pbix_inspect(a),
            "pbix_extract": lambda a: self._handle_pbix_extract(a),
            # Custom BPA governance (Wave 4)
            "bpa_validate_rules": lambda a: self._handle_bpa_validate_rules(a),
            "bpa_audit_rule_sources": lambda a: self._handle_bpa_audit_rule_sources(a),
            # Data modelling / warehousing / bulk DAX (Wave 5)
            "generate_measure_suite": lambda a: self._handle_generate_measure_suite(a),
            "batch_create_measures": lambda a: self._handle_batch_create_measures(a),
            "audit_star_schema": lambda a: self._handle_audit_star_schema(a),
            "scan_referential_integrity": lambda a: self._handle_scan_referential_integrity(a),
            "pbip_add_measures": lambda a: self._handle_pbip_add_measures(a),
            "pbip_create_date_table": lambda a: self._handle_pbip_create_date_table(a),
            "pbip_add_calculation_group": lambda a: self._handle_pbip_add_calculation_group(a),
            "pbip_add_hierarchy": lambda a: self._handle_pbip_add_hierarchy(a),
            # Desktop Bridge (Wave 6): JSON-RPC to the running Power BI Desktop process
            "bridge_status": lambda a: self._handle_bridge_status(a),
            "bridge_manifest": lambda a: self._handle_bridge_manifest(a),
            "bridge_screenshot": lambda a: self._handle_bridge_screenshot(a),
            "bridge_reload": lambda a: self._handle_bridge_reload(a),
            # Report as code (Wave 7): the gallery build pipeline as tools
            "pbip_stage_publish": lambda a: self._handle_pbip_stage_publish(a),
            "pbir_apply_spec": lambda a: self._handle_pbir_apply_spec(a),
            "generate_payload_measure": lambda a: self._handle_generate_payload_measure(a),
            "generate_html_measure": lambda a: self._handle_generate_html_measure(a),
            "build_js_app_measure": lambda a: self._handle_build_js_app_measure(a),
            "pbip_embed_data": lambda a: self._handle_pbip_embed_data(a),
            "audit_report_design": lambda a: self._handle_audit_report_design(a),
            "pbip_undo": lambda a: self._handle_pbip_undo(a),
        }

    def _build_tool_annotations(self):
        """Map tool name -> ToolAnnotations. Hints let MCP clients auto-approve safe
        reads and require confirmation for destructive writes (the spec's primary safe-agent
        lever, since destructive-op guards are not otherwise standardized)."""
        def ann(read_only, destructive=False, idempotent=False, open_world=False):
            return ToolAnnotations(
                readOnlyHint=read_only,
                destructiveHint=destructive,
                idempotentHint=idempotent,
                openWorldHint=open_world,
            )

        local_read = ann(True, open_world=False)
        cloud_read = ann(True, open_world=True)
        local_state = ann(False, destructive=False, idempotent=True, open_world=False)
        local_destructive = ann(False, destructive=True, open_world=False)
        # Writes new files on disk without altering a model or an existing report.
        local_file_writer = ann(False, destructive=False, idempotent=True, open_world=False)
        return {
            # Desktop reads (local)
            "desktop_discover_instances": local_read,
            "desktop_connect": local_state,
            "desktop_list_tables": local_read,
            "desktop_list_columns": local_read,
            "desktop_list_measures": local_read,
            "desktop_execute_dax": local_read,  # DAX EVALUATE is a query, not a mutation
            "desktop_get_model_info": local_read,
            # Cloud reads (open world / network)
            "list_workspaces": cloud_read,
            "list_datasets": cloud_read,
            "list_tables": cloud_read,
            "list_columns": cloud_read,
            "execute_dax": cloud_read,
            "get_model_info": cloud_read,
            # Security (local reads)
            "security_status": local_read,
            "security_audit_log": local_read,
            # RLS
            "desktop_list_rls_roles": local_read,
            "desktop_set_rls_role": local_state,  # changes session RLS context, not data
            "desktop_rls_status": local_read,
            # TOM writes (mutate the live model)
            "scan_table_dependencies": local_read,
            "batch_rename_tables": local_destructive,
            "batch_rename_columns": local_destructive,
            "batch_rename_measures": local_destructive,
            "batch_update_measures": local_destructive,
            "create_measure": ann(False, destructive=False, idempotent=False, open_world=False),
            "delete_measure": local_destructive,
            # PBIP (file edits)
            "pbip_load_project": ann(True, open_world=False),  # reads project files
            "pbip_get_project_info": local_read,
            "pbip_rename_tables": local_destructive,
            "pbip_rename_columns": local_destructive,
            "pbip_rename_measures": local_destructive,
            "pbip_fix_broken_visuals": local_destructive,
            "pbip_fix_dax_quoting": local_destructive,
            "pbip_scan_broken_refs": local_read,
            "pbip_validate": local_read,
            # PBIR report authoring (preview) - write files
            "pbir_add_page": local_destructive,
            "pbir_add_visual": local_destructive,
            "pbir_bind_fields": local_destructive,
            "pbir_validate_report": local_read,
            # DAX safety loop + transactions (Bundle A)
            "validate_dax": ann(True, open_world=False),
            "scan_measure_dependencies": local_read,
            "tom_begin_transaction": ann(False, destructive=False, idempotent=False),
            "tom_commit_transaction": ann(False, destructive=False, idempotent=False),
            "tom_rollback_transaction": ann(False, destructive=False, idempotent=True),
            # Model quality & performance (Bundle B) - all read-only analysis
            "run_bpa": local_read,
            "audit_ai_readiness": local_read,
            "analyze_model_storage": local_read,
            "analyze_query_performance": local_read,
            "export_data_dictionary": ann(False, destructive=False, idempotent=True, open_world=False),
            "model_snapshot": ann(False, destructive=False, idempotent=True, open_world=False),
            "model_diff": local_read,
            "pre_deploy_gate": local_read,
            # Diagnostics & ops (Wave 2)
            "refresh_doctor": cloud_read,
            "find_unused_objects": local_read,
            "impact_analysis": local_read,
            "rls_test_harness": local_read,
            "run_dax_tests": local_read,
            "verify_audit_integrity": local_read,
            # Governance-ops fleet (Wave 3) - read-only, cloud/admin
            # reads the tenant; can also write a cache file on request (refused in read-only mode)
            "cross_workspace_lineage": ann(False, destructive=False, idempotent=True, open_world=True),
            "fleet_refresh_monitor": cloud_read,
            "usage_and_orphan_analytics": cloud_read,
            # Relationship management (Bundle D) - mutate the live model
            "create_relationship": ann(False, destructive=False, idempotent=False, open_world=False),
            "delete_relationship": local_destructive,
            # DAX quality (Wave 4) - read-only static analysis
            "dax_lint": local_read,
            "dax_suggest_rewrite": local_read,
            # Authoring helpers (Wave 4) - generate DAX / analyze names, no live mutation
            "generate_svg_measure": ann(True, open_world=False),
            "audit_naming": local_read,
            # PBIX onboarding (Wave 4) - read a file / write extracted files
            "pbix_inspect": ann(True, open_world=False),
            "pbix_extract": ann(False, destructive=False, idempotent=True, open_world=False),
            # Custom BPA governance (Wave 4) - read-only validation/discovery
            "bpa_validate_rules": local_read,
            "bpa_audit_rule_sources": local_read,
            # Data modelling / warehousing / bulk DAX (Wave 5)
            "generate_measure_suite": ann(False, destructive=False, idempotent=False, open_world=False),  # can write (target)
            "batch_create_measures": ann(False, destructive=False, idempotent=False, open_world=False),
            "audit_star_schema": local_read,
            "scan_referential_integrity": local_read,
            "pbip_add_measures": local_destructive,
            "pbip_create_date_table": local_destructive,
            "pbip_add_calculation_group": local_destructive,
            "pbip_add_hierarchy": local_destructive,
            # Desktop Bridge (Wave 6)
            "bridge_status": local_read,
            "bridge_manifest": local_read,
            "bridge_screenshot": ann(False, destructive=False, idempotent=True, open_world=False),  # writes PNGs
            "bridge_reload": local_destructive,  # can discard unsaved Desktop changes (guarded)
            # Report as code (Wave 7)
            "pbip_stage_publish": local_file_writer,  # writes a staged copy; the source is untouched
            "pbir_apply_spec": local_destructive,  # writes report files; prune removes managed ones
            "generate_payload_measure": ann(True, open_world=False),  # generates DAX + a JavaScript parser; writes nothing
            "generate_html_measure": ann(True, open_world=False),  # generates DAX; writes nothing
            "build_js_app_measure": ann(False, destructive=False, idempotent=True, open_world=False),  # can write measures (target='pbip'); refused in the handler when read-only
            "pbip_embed_data": local_destructive,  # rewrites partition sources (dry_run by default)
            "audit_report_design": local_read,  # reads report files only
            "pbip_undo": local_destructive,  # restores files changed by the last rename or repair
        }

    def _setup_handlers(self):
        """Set up MCP tool handlers"""

        def build_tools() -> List[Tool]:
            """Every tool this server exposes, with its schemas and safety annotations."""
            tools = [
                # === DESKTOP TOOLS ===
                Tool(
                    name="desktop_discover_instances",
                    description="Discover all running Power BI Desktop instances on this machine",
                    inputSchema={
                        "type": "object",
                        "properties": {},
                        "required": []
                    }
                ),
                Tool(
                    name="desktop_connect",
                    description="Connect to a Power BI Desktop instance by port number. Optionally specify an RLS role to test.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "port": {
                                "type": "integer",
                                "description": "Port number of the Power BI Desktop instance (optional - auto-selects if not provided)"
                            },
                            "rls_role": {
                                "type": "string",
                                "description": "Optional RLS role name to test. Queries will be filtered by this role's DAX filters."
                            }
                        },
                        "required": []
                    }
                ),
                Tool(
                    name="desktop_list_tables",
                    description="List all tables in the connected Power BI Desktop model",
                    inputSchema={
                        "type": "object",
                        "properties": {},
                        "required": []
                    }
                ),
                Tool(
                    name="desktop_list_columns",
                    description="List columns for a table in the connected Power BI Desktop model",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "table_name": {
                                "type": "string",
                                "description": "Name of the table"
                            }
                        },
                        "required": ["table_name"]
                    }
                ),
                Tool(
                    name="desktop_list_measures",
                    description="List all measures in the connected Power BI Desktop model",
                    inputSchema={
                        "type": "object",
                        "properties": {},
                        "required": []
                    }
                ),
                Tool(
                    name="desktop_execute_dax",
                    description="Execute a DAX query against the connected Power BI Desktop model",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "dax_query": {
                                "type": "string",
                                "description": "DAX query to execute"
                            },
                            "max_rows": {
                                "type": "integer",
                                "description": "Maximum rows to return (default: 100)",
                                "default": 100
                            }
                        },
                        "required": ["dax_query"]
                    }
                ),
                Tool(
                    name="desktop_get_model_info",
                    description="Get comprehensive model info (tables, columns, measures, relationships) from Power BI Desktop",
                    inputSchema={
                        "type": "object",
                        "properties": {},
                        "required": []
                    }
                ),
                # === CLOUD TOOLS (from V1) ===
                Tool(
                    name="list_workspaces",
                    description="List all Power BI Service workspaces accessible to the Service Principal",
                    inputSchema={
                        "type": "object",
                        "properties": {},
                        "required": []
                    }
                ),
                Tool(
                    name="list_datasets",
                    description="List all datasets in a Power BI Service workspace",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "workspace_id": {
                                "type": "string",
                                "description": "ID of the workspace"
                            }
                        },
                        "required": ["workspace_id"]
                    }
                ),
                Tool(
                    name="list_tables",
                    description="List all tables in a Power BI Service dataset via XMLA",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "workspace_name": {
                                "type": "string",
                                "description": "Name of the workspace"
                            },
                            "dataset_name": {
                                "type": "string",
                                "description": "Name of the dataset"
                            }
                        },
                        "required": ["workspace_name", "dataset_name"]
                    }
                ),
                Tool(
                    name="list_columns",
                    description="List columns for a table in a Power BI Service dataset",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "workspace_name": {
                                "type": "string",
                                "description": "Name of the workspace"
                            },
                            "dataset_name": {
                                "type": "string",
                                "description": "Name of the dataset"
                            },
                            "table_name": {
                                "type": "string",
                                "description": "Name of the table"
                            }
                        },
                        "required": ["workspace_name", "dataset_name", "table_name"]
                    }
                ),
                Tool(
                    name="execute_dax",
                    description="Execute a DAX query against a Power BI Service dataset",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "workspace_name": {
                                "type": "string",
                                "description": "Name of the workspace"
                            },
                            "dataset_name": {
                                "type": "string",
                                "description": "Name of the dataset"
                            },
                            "dax_query": {
                                "type": "string",
                                "description": "DAX query to execute"
                            }
                        },
                        "required": ["workspace_name", "dataset_name", "dax_query"]
                    }
                ),
                Tool(
                    name="get_model_info",
                    description="Get comprehensive model info from a Power BI Service dataset using INFO.VIEW functions",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "workspace_name": {
                                "type": "string",
                                "description": "Name of the workspace"
                            },
                            "dataset_name": {
                                "type": "string",
                                "description": "Name of the dataset"
                            }
                        },
                        "required": ["workspace_name", "dataset_name"]
                    }
                ),
                # === SECURITY TOOLS ===
                Tool(
                    name="security_status",
                    description="Get the current security settings and status (PII detection, audit logging, access policies)",
                    inputSchema={
                        "type": "object",
                        "properties": {},
                        "required": []
                    }
                ),
                Tool(
                    name="security_audit_log",
                    description="View recent entries from the security audit log",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "count": {
                                "type": "integer",
                                "description": "Number of recent entries to show (default: 10)",
                                "default": 10
                            }
                        },
                        "required": []
                    }
                ),
                # === RLS (Row-Level Security) TOOLS ===
                Tool(
                    name="desktop_list_rls_roles",
                    description="List all RLS (Row-Level Security) roles defined in the Power BI Desktop model",
                    inputSchema={
                        "type": "object",
                        "properties": {},
                        "required": []
                    }
                ),
                Tool(
                    name="desktop_set_rls_role",
                    description="Set or clear the active RLS role for testing. When set, all queries will be filtered by that role's DAX filters.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "role_name": {
                                "type": "string",
                                "description": "Name of the RLS role to activate. Omit or set to empty string to clear."
                            }
                        },
                        "required": []
                    }
                ),
                Tool(
                    name="desktop_rls_status",
                    description="Get the current RLS status including active role and available roles",
                    inputSchema={
                        "type": "object",
                        "properties": {},
                        "required": []
                    }
                ),
                # === BATCH/WRITE OPERATIONS (TOM) - DEPRECATED FOR RENAMING ===
                Tool(
                    name="batch_rename_tables",
                    description="⚠️ DEPRECATED: Use 'pbip_rename_tables' instead. This TOM-based tool only updates in-memory model and DOES NOT update report visuals. Use PBIP tools for safe renaming.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "renames": {
                                "type": "array",
                                "description": "Array of rename operations",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "old_name": {"type": "string", "description": "Current table name"},
                                        "new_name": {"type": "string", "description": "New table name"}
                                    },
                                    "required": ["old_name", "new_name"]
                                }
                            },
                            "auto_save": {
                                "type": "boolean",
                                "description": "Whether to automatically save changes (default: true)",
                                "default": True
                            }
                        },
                        "required": ["renames"]
                    }
                ),
                Tool(
                    name="batch_rename_columns",
                    description="⚠️ DEPRECATED: Use 'pbip_rename_columns' instead. This TOM-based tool only updates in-memory model and DOES NOT update report visuals. Use PBIP tools for safe renaming.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "renames": {
                                "type": "array",
                                "description": "Array of rename operations",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "table_name": {"type": "string", "description": "Table containing the column"},
                                        "old_name": {"type": "string", "description": "Current column name"},
                                        "new_name": {"type": "string", "description": "New column name"}
                                    },
                                    "required": ["table_name", "old_name", "new_name"]
                                }
                            },
                            "auto_save": {
                                "type": "boolean",
                                "description": "Whether to automatically save changes (default: true)",
                                "default": True
                            }
                        },
                        "required": ["renames"]
                    }
                ),
                Tool(
                    name="batch_rename_measures",
                    description="⚠️ DEPRECATED: Use 'pbip_rename_measures' instead. This TOM-based tool only updates in-memory model and DOES NOT update report visuals. Use PBIP tools for safe renaming.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "renames": {
                                "type": "array",
                                "description": "Array of rename operations",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "old_name": {"type": "string", "description": "Current measure name"},
                                        "new_name": {"type": "string", "description": "New measure name"},
                                        "table_name": {"type": "string", "description": "Table containing the measure (optional)"}
                                    },
                                    "required": ["old_name", "new_name"]
                                }
                            },
                            "auto_save": {
                                "type": "boolean",
                                "description": "Whether to automatically save changes (default: true)",
                                "default": True
                            }
                        },
                        "required": ["renames"]
                    }
                ),
                Tool(
                    name="batch_update_measures",
                    description="Bulk update multiple measure expressions in the Power BI Desktop model.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "updates": {
                                "type": "array",
                                "description": "Array of measure updates",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "measure_name": {"type": "string", "description": "Name of the measure"},
                                        "expression": {"type": "string", "description": "New DAX expression"},
                                        "table_name": {"type": "string", "description": "Table containing the measure (optional)"}
                                    },
                                    "required": ["measure_name", "expression"]
                                }
                            },
                            "auto_save": {
                                "type": "boolean",
                                "description": "Whether to automatically save changes (default: true, or false inside an open transaction)"
                            },
                            "skip_validation": {
                                "type": "boolean",
                                "description": "Skip validating each new expression against the model before committing (default: false)",
                                "default": False
                            }
                        },
                        "required": ["updates"]
                    }
                ),
                Tool(
                    name="create_measure",
                    description="Create a new DAX measure in the Power BI Desktop model.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "table_name": {
                                "type": "string",
                                "description": "Table to add the measure to"
                            },
                            "measure_name": {
                                "type": "string",
                                "description": "Name for the new measure"
                            },
                            "expression": {
                                "type": "string",
                                "description": "DAX expression for the measure"
                            },
                            "format_string": {
                                "type": "string",
                                "description": "Optional format string (e.g., '#,##0' or '0.00%')"
                            },
                            "description": {
                                "type": "string",
                                "description": "Optional description for the measure"
                            },
                            "skip_validation": {
                                "type": "boolean",
                                "description": "Skip validating the expression against the model before creating (default: false)",
                                "default": False
                            }
                        },
                        "required": ["table_name", "measure_name", "expression"]
                    }
                ),
                Tool(
                    name="delete_measure",
                    description="Delete a measure from the Power BI Desktop model.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "measure_name": {
                                "type": "string",
                                "description": "Name of the measure to delete"
                            },
                            "table_name": {
                                "type": "string",
                                "description": "Table containing the measure (optional)"
                            }
                        },
                        "required": ["measure_name"]
                    }
                ),
                Tool(
                    name="scan_table_dependencies",
                    description="Scan a table to find all references before renaming. Shows measures, calculated columns, and relationships that depend on this table. IMPORTANT: Use this before batch_rename_tables to understand the impact.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "table_name": {
                                "type": "string",
                                "description": "Name of the table to scan for dependencies"
                            }
                        },
                        "required": ["table_name"]
                    }
                ),
                # === PBIP TOOLS (File-based editing for safe renames) ===
                Tool(
                    name="pbip_load_project",
                    description="Load a PBIP (Power BI Project) for file-based editing. PBIP format allows safe bulk renames without breaking report visuals. Use 'File > Save as > Power BI Project' in Power BI Desktop to create a PBIP.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "pbip_path": {
                                "type": "string",
                                "description": "Path to the .pbip file or project folder"
                            }
                        },
                        "required": ["pbip_path"]
                    }
                ),
                Tool(
                    name="pbip_get_project_info",
                    description="Get information about the loaded PBIP project including paths to TMDL files and report.json",
                    inputSchema={
                        "type": "object",
                        "properties": {},
                        "required": []
                    }
                ),
                Tool(
                    name="pbip_rename_tables",
                    description="✅ RECOMMENDED: Safely rename tables in a PBIP project. Updates EVERYTHING: TMDL files, DAX references (with proper quoting), report visuals, and Q&A schema. Close Power BI Desktop first, then reopen after.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "renames": {
                                "type": "array",
                                "description": "Array of rename operations",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "old_name": {"type": "string", "description": "Current table name"},
                                        "new_name": {"type": "string", "description": "New table name"}
                                    },
                                    "required": ["old_name", "new_name"]
                                }
                            }
                        },
                        "required": ["renames"]
                    }
                ),
                Tool(
                    name="pbip_rename_columns",
                    description="✅ RECOMMENDED: Safely rename columns in a PBIP project. Updates TMDL files, DAX references, and report visuals. Close Power BI Desktop first, then reopen after.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "renames": {
                                "type": "array",
                                "description": "Array of rename operations",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "table_name": {"type": "string", "description": "Table containing the column"},
                                        "old_name": {"type": "string", "description": "Current column name"},
                                        "new_name": {"type": "string", "description": "New column name"}
                                    },
                                    "required": ["table_name", "old_name", "new_name"]
                                }
                            }
                        },
                        "required": ["renames"]
                    }
                ),
                Tool(
                    name="pbip_rename_measures",
                    description="✅ RECOMMENDED: Safely rename measures in a PBIP project. Updates TMDL files, DAX references, and report visuals. Close Power BI Desktop first, then reopen after.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "renames": {
                                "type": "array",
                                "description": "Array of rename operations",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "old_name": {"type": "string", "description": "Current measure name"},
                                        "new_name": {"type": "string", "description": "New measure name"},
                                        "table_name": {"type": "string", "description": "Optional: the measure's home table, when the name alone is ambiguous"}
                                    },
                                    "required": ["old_name", "new_name"]
                                }
                            }
                        },
                        "required": ["renames"]
                    }
                ),
                # === PBIP REPAIR TOOLS (Fix broken visuals) ===
                Tool(
                    name="pbip_fix_broken_visuals",
                    description="Fix broken visual references after a table rename. Use this when TOM/API renamed a table but visuals still reference the old name. Supports both PBIR-Legacy and PBIR-Enhanced formats.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "old_table_name": {
                                "type": "string",
                                "description": "The old table name that visuals are still referencing (broken)"
                            },
                            "new_table_name": {
                                "type": "string",
                                "description": "The correct new table name in the semantic model"
                            }
                        },
                        "required": ["old_table_name", "new_table_name"]
                    }
                ),
                Tool(
                    name="pbip_fix_dax_quoting",
                    description="Fix DAX expressions by quoting table names that need it: Leads Sales Data[Amount] -> 'Leads Sales Data'[Amount]. DAX only (Power Query is not touched); every file is fixed or none. Use dry_run=true to see what would change.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "dry_run": {"type": "boolean", "default": False, "description": "Report what would change without writing"}
                        },
                        "required": []
                    }
                ),
                Tool(
                    name="pbip_scan_broken_refs",
                    description="Scan the PBIP project for broken references. Compares table names in semantic model vs report visuals to find mismatches.",
                    inputSchema={
                        "type": "object",
                        "properties": {},
                        "required": []
                    }
                ),
                Tool(
                    name="pbip_validate",
                    description="Validate TMDL syntax in the loaded PBIP project. Checks for unquoted names with spaces, invalid references, etc.",
                    inputSchema={
                        "type": "object",
                        "properties": {},
                        "required": []
                    }
                ),
                # === PBIR REPORT AUTHORING (preview) ===
                Tool(
                    name="pbir_add_page",
                    description="[PREVIEW] Add a new report page to the loaded PBIR-Enhanced PBIP project. Writes a schema-valid page.json and registers it in pages.json. Close Power BI Desktop before editing; reopen after.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "display_name": {"type": "string", "description": "Visible page name"},
                            "width": {"type": "integer", "default": 1280},
                            "height": {"type": "integer", "default": 720},
                            "set_active": {"type": "boolean", "default": False}
                        },
                        "required": ["display_name"]
                    }
                ),
                Tool(
                    name="pbir_add_visual",
                    description="[PREVIEW] Add a visual to a page in the loaded PBIR-Enhanced project. Supported visual_type: card, kpi, tableEx, slicer, barChart, columnChart, lineChart, areaChart, pieChart, donutChart, gauge, pivotTable. 'fields' maps a query role to 'Table.Field' or a list (roles per type: e.g. barChart -> Category/Y/Series, card -> Values, tableEx -> Values, slicer -> Values). Field existence is validated against the model first.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "page": {"type": "string", "description": "Page display name or folder name"},
                            "visual_type": {"type": "string"},
                            "position": {"type": "object", "description": "{x, y, width, height} (and optional z, tabOrder)"},
                            "fields": {"type": "object", "description": "Role -> 'Table.Field' or ['Table.Field', ...] (e.g. {\"Category\":\"Date.Month\",\"Y\":\"Sales.Total Sales\"})"},
                            "skip_validation": {"type": "boolean", "default": False}
                        },
                        "required": ["page", "visual_type"]
                    }
                ),
                Tool(
                    name="pbir_bind_fields",
                    description="[PREVIEW] Add or replace field bindings on an existing visual without recreating it. mode 'add' appends projections (deduped); 'replace' replaces the given roles.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "page": {"type": "string"},
                            "visual_name": {"type": "string", "description": "The visual's folder/name (from pbir_add_visual)"},
                            "fields": {"type": "object", "description": "Role -> 'Table.Field' or list"},
                            "mode": {"type": "string", "enum": ["add", "replace"], "default": "add"},
                            "skip_validation": {"type": "boolean", "default": False}
                        },
                        "required": ["page", "visual_name", "fields"]
                    }
                ),
                Tool(
                    name="pbir_validate_report",
                    description="[PREVIEW] Validate that every field referenced by the report's visuals exists in the model (the #1 cause of blank visuals / repair prompts after edits).",
                    inputSchema={"type": "object", "properties": {}, "required": []}
                ),
                # === DAX SAFETY LOOP & TRANSACTIONS (Bundle A) ===
                Tool(
                    name="validate_dax",
                    description="Validate a DAX query or scalar measure expression against the connected model WITHOUT committing anything. Executes a minimal probe and returns syntax/semantic errors so an agent can self-correct before writing a measure.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "dax": {"type": "string", "description": "A full DAX query (EVALUATE/DEFINE...) or a scalar measure expression to validate"},
                            "as_measure": {"type": "boolean", "description": "Treat 'dax' as a scalar measure expression rather than a query. Default false.", "default": False},
                            "table_name": {"type": "string", "description": "With as_measure: the table that will host the measure. When given, the expression is validated as a real query-scoped measure (DEFINE MEASURE), which is stricter than the default."},
                            "source": {"type": "string", "enum": ["desktop", "cloud"], "description": "Validate against Desktop (local) or cloud. Default 'desktop'.", "default": "desktop"},
                            "workspace_name": {"type": "string", "description": "Cloud only: workspace name"},
                            "dataset_name": {"type": "string", "description": "Cloud only: dataset name"}
                        },
                        "required": ["dax"]
                    },
                    outputSchema={
                        "type": "object",
                        "properties": {
                            "valid": {"type": "boolean"},
                            "error": {"type": ["string", "null"]},
                            "probe": {"type": "string"}
                        }
                    }
                ),
                Tool(
                    name="scan_measure_dependencies",
                    description="Analyze the dependency graph of a measure or column using INFO.CALCDEPENDENCY: upstream (what it depends on) and downstream (what depends on it). Use before renaming or deleting to see what would break.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "measure_name": {"type": "string", "description": "Name of the measure or column to analyze"},
                            "table_name": {"type": "string", "description": "Optional owning table to disambiguate a name used on multiple tables"},
                            "direction": {"type": "string", "enum": ["upstream", "downstream", "both"], "description": "upstream = dependencies; downstream = dependents. Default 'both'.", "default": "both"},
                            "source": {"type": "string", "enum": ["desktop", "cloud"], "description": "Desktop (local) or cloud. Default 'desktop'.", "default": "desktop"},
                            "workspace_name": {"type": "string", "description": "Cloud only: workspace name"},
                            "dataset_name": {"type": "string", "description": "Cloud only: dataset name"}
                        },
                        "required": ["measure_name"]
                    }
                ),
                Tool(
                    name="tom_begin_transaction",
                    description="Begin a TOM write transaction. While open, model write tools (create_measure, delete_measure, batch_update_measures) defer saving until tom_commit_transaction, so a batch of edits is atomic and can be rolled back.",
                    inputSchema={"type": "object", "properties": {}, "required": []}
                ),
                Tool(
                    name="tom_commit_transaction",
                    description="Commit (SaveChanges) all pending TOM model edits made since tom_begin_transaction and close the transaction.",
                    inputSchema={"type": "object", "properties": {}, "required": []}
                ),
                Tool(
                    name="tom_rollback_transaction",
                    description="Roll back (UndoLocalChanges) all pending TOM model edits made since tom_begin_transaction and close the transaction.",
                    inputSchema={"type": "object", "properties": {}, "required": []}
                ),
                # === MODEL QUALITY & PERFORMANCE (Bundle B) ===
                Tool(
                    name="run_bpa",
                    description="Run a Best Practice Analyzer over the connected semantic model (performance, DAX, naming, formatting, maintenance, error-prevention rules). Returns findings with severity, the offending object, and a fix hint.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "source": {"type": "string", "enum": ["desktop", "cloud"], "default": "desktop"},
                            "categories": {"type": "array", "items": {"type": "string"}, "description": "Optional category filter, e.g. ['Performance','DAX','Naming','Formatting','Maintenance','Error Prevention']"},
                            "min_severity": {"type": "string", "enum": ["info", "warning", "error"], "default": "info", "description": "Only return findings at or above this severity"},
                            "workspace_name": {"type": "string"},
                            "dataset_name": {"type": "string"}
                        },
                        "required": []
                    },
                    outputSchema={
                        "type": "object",
                        "properties": {
                            "summary": {"type": "object"},
                            "findings": {"type": "array"}
                        }
                    }
                ),
                Tool(
                    name="audit_ai_readiness",
                    description="Score how AI-ready (Copilot/agent-ready) the model is: coverage of descriptions and format strings on measures, columns, and tables. Returns a 0-100 score, metrics, and concrete recommendations.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "source": {"type": "string", "enum": ["desktop", "cloud"], "default": "desktop"},
                            "workspace_name": {"type": "string"},
                            "dataset_name": {"type": "string"}
                        },
                        "required": []
                    },
                    outputSchema={
                        "type": "object",
                        "properties": {
                            "score": {"type": "number"},
                            "grade": {"type": "string"},
                            "metrics": {"type": "object"},
                            "recommendations": {"type": "array"}
                        }
                    }
                ),
                Tool(
                    name="analyze_model_storage",
                    description="VertiPaq-style storage analysis: per-table row counts (exact via DAX COUNTROWS), column counts, and best-effort sizes, ranked to find the biggest/most expensive tables for optimization.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "source": {"type": "string", "enum": ["desktop", "cloud"], "default": "desktop"},
                            "workspace_name": {"type": "string"},
                            "dataset_name": {"type": "string"}
                        },
                        "required": []
                    }
                ),
                Tool(
                    name="analyze_query_performance",
                    description="Execute a DAX query and report duration, row count, and heuristic optimization hints. For storage-engine vs formula-engine server timings use DAX Studio.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "dax": {"type": "string", "description": "The DAX query to time"},
                            "source": {"type": "string", "enum": ["desktop", "cloud"], "default": "desktop"},
                            "workspace_name": {"type": "string"},
                            "dataset_name": {"type": "string"}
                        },
                        "required": ["dax"]
                    }
                ),
                # === RELATIONSHIP MANAGEMENT (Bundle D) ===
                Tool(
                    name="create_relationship",
                    description="Create a relationship between two columns in the connected Power BI Desktop model (TOM). Honors an open tom transaction.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "from_table": {"type": "string", "description": "Many-side table (the fact table, typically)"},
                            "from_column": {"type": "string", "description": "Many-side column"},
                            "to_table": {"type": "string", "description": "One-side table (the dimension, typically)"},
                            "to_column": {"type": "string", "description": "One-side column (usually the key)"},
                            "cardinality": {"type": "string", "enum": ["many_to_one", "one_to_many", "one_to_one", "many_to_many"], "default": "many_to_one"},
                            "cross_filter": {"type": "string", "enum": ["single", "both"], "default": "single"},
                            "is_active": {"type": "boolean", "default": True}
                        },
                        "required": ["from_table", "from_column", "to_table", "to_column"]
                    }
                ),
                Tool(
                    name="delete_relationship",
                    description="Delete a relationship in the connected Power BI Desktop model (TOM), identified by name or by from/to table (and optionally column). Honors an open tom transaction.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "from_table": {"type": "string"},
                            "from_column": {"type": "string"},
                            "to_table": {"type": "string"},
                            "to_column": {"type": "string"},
                            "name": {"type": "string", "description": "Relationship name (alternative to from/to)"}
                        },
                        "required": []
                    }
                ),
                # === DAX QUALITY (Wave 4) ===
                Tool(
                    name="dax_lint",
                    description="Static-analyze DAX for performance anti-patterns and correctness traps (FILTER over a whole table in CALCULATE, nested CALCULATE, '/' instead of DIVIDE, IFERROR, EARLIER, SUMMARIZE used for aggregation, '+ 0' blank suppression, unrecognized/likely-hallucinated functions). Lint a raw expression, one measure, or every measure in the connected model. Each finding carries a severity, line, and a concrete rewrite hint.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "expression": {"type": "string", "description": "A raw DAX expression to lint (takes precedence over the model)"},
                            "name": {"type": "string", "description": "Optional label for the expression in findings"},
                            "measure_name": {"type": "string", "description": "Lint just this measure from the connected model"},
                            "source": {"type": "string", "enum": ["desktop", "cloud"], "default": "desktop"},
                            "min_severity": {"type": "string", "enum": ["info", "warning", "error"], "default": "info"},
                            "workspace_name": {"type": "string"},
                            "dataset_name": {"type": "string"}
                        },
                        "required": []
                    },
                    outputSchema={
                        "type": "object",
                        "properties": {
                            "summary": {"type": "object"},
                            "findings": {"type": "array"}
                        }
                    }
                ),
                Tool(
                    name="dax_suggest_rewrite",
                    description="Rewrite help for DAX anti-patterns, for a raw expression or a named measure. Bare '/' division is rewritten mechanically to DIVIDE(a, b) and the whole rewritten expression is returned (kind 'rewrite' / 'expression'). FILTER-over-a-whole-table and SUMMARIZE-with-aggregation need a judgement call, so they come back as before/after templates (kind 'template'), not as edits of your text. Run validate_dax on the result before saving it.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "expression": {"type": "string", "description": "A raw DAX expression (takes precedence over the model)"},
                            "name": {"type": "string"},
                            "measure_name": {"type": "string", "description": "Suggest rewrites for this measure from the connected model"},
                            "source": {"type": "string", "enum": ["desktop", "cloud"], "default": "desktop"},
                            "workspace_name": {"type": "string"},
                            "dataset_name": {"type": "string"}
                        },
                        "required": []
                    },
                    outputSchema={
                        "type": "object",
                        "properties": {
                            "rewrites": {"type": "array"}
                        }
                    }
                ),
                # === AUTHORING HELPERS (Wave 4) ===
                Tool(
                    name="generate_svg_measure",
                    description="Generate a ready-to-use DAX measure that returns an inline SVG micro-visual (progress bar, bullet chart, status pill, or sparkline) as a data:image/svg+xml URI. Set the measure's data category to 'Image URL' so Power BI renders it in a table/matrix column or card. Pure DAX, no custom visual.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "kind": {"type": "string", "enum": ["progress", "bullet", "status_pill", "sparkline"]},
                            "name": {"type": "string", "description": "Optional name for the generated measure"},
                            "value_measure": {"type": "string", "description": "The measure to visualize (progress/bullet/status_pill/sparkline)"},
                            "target_measure": {"type": "string", "description": "Target measure (bullet)"},
                            "max_value": {"type": "number", "description": "Scale maximum (progress/bullet)"},
                            "min_value": {"type": "number", "description": "Scale minimum (progress)"},
                            "axis_column": {"type": "string", "description": "Axis column to plot across, e.g. 'Date'[Month] (sparkline)"},
                            "sort_column": {"type": "string", "description": "Column to order the sparkline x-axis by (defaults to axis_column)"},
                            "thresholds": {"type": "array", "description": "status_pill bands: [{max, color, label}], last band uses max=null", "items": {"type": "object"}},
                            "width": {"type": "integer"},
                            "height": {"type": "integer"},
                            "fill": {"type": "string"},
                            "track": {"type": "string"},
                            "target": {"type": "string"},
                            "stroke": {"type": "string"}
                        },
                        "required": ["kind"]
                    },
                    outputSchema={
                        "type": "object",
                        "properties": {
                            "name": {"type": "string"},
                            "kind": {"type": "string"},
                            "dax": {"type": "string"},
                            "notes": {"type": "string"}
                        }
                    }
                ),
                Tool(
                    name="audit_naming",
                    description="Audit naming conventions across the connected model's tables, columns, and measures and return a rename PLAN (old -> new with reasons): snake_case/camelCase to spaced Title Case, strip warehouse DIM_/FACT_ prefixes, trim whitespace, optionally expand abbreviations. Suggestions that would collide with an existing or another suggested name are flagged (safe=false). Apply the safe ones with the pbip_rename_* tools, which update the model and the report together.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "source": {"type": "string", "enum": ["desktop", "cloud"], "default": "desktop"},
                            "scope": {"type": "array", "items": {"type": "string", "enum": ["tables", "columns", "measures"]}, "description": "Which object types to audit (default all)"},
                            "target_case": {"type": "string", "enum": ["title", "none"], "default": "title"},
                            "strip_warehouse_prefixes": {"type": "boolean", "default": True},
                            "expand_abbreviations": {"type": "boolean", "default": False},
                            "acronyms": {"type": "array", "items": {"type": "string"}, "description": "Words to keep in capitals (e.g. ['AOP', 'EBITDA']); added to the built-in list"},
                            "replace_acronyms": {"type": "boolean", "default": False, "description": "Use only the given acronyms instead of adding them to the built-in list"},
                            "workspace_name": {"type": "string"},
                            "dataset_name": {"type": "string"}
                        },
                        "required": []
                    },
                    outputSchema={
                        "type": "object",
                        "properties": {
                            "summary": {"type": "object"},
                            "plan": {"type": "array"}
                        }
                    }
                ),
                # === PBIX ONBOARDING (Wave 4) ===
                Tool(
                    name="pbix_inspect",
                    description="Inspect a .pbix file (an OPC ZIP package) without extracting: classify it as thick (imported model) vs thin (live connection), detect the report format (legacy Report/Layout vs PBIR), count pages, and list every internal entry with size. The first step to working with a real .pbix.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "path": {"type": "string", "description": "Path to the .pbix file"}
                        },
                        "required": ["path"]
                    },
                    outputSchema={
                        "type": "object",
                        "properties": {
                            "type": {"type": "string"},
                            "report_format": {"type": "string"},
                            "page_count": {"type": ["integer", "null"]},
                            "entries": {"type": "array"}
                        }
                    }
                ),
                Tool(
                    name="pbix_extract",
                    description="Extract a .pbix package to a folder (Zip-Slip protected). Also decodes the legacy UTF-16-LE Report/Layout into a readable UTF-8 Report/Layout.json so an agent can inspect or edit the report structure. Returns the list of extracted files.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "path": {"type": "string", "description": "Path to the .pbix file"},
                            "dest": {"type": "string", "description": "Destination folder to extract into"},
                            "decode_layout": {"type": "boolean", "default": True, "description": "Also write a decoded Report/Layout.json"},
                            "max_bytes": {"type": "integer", "description": "Refuse a package whose extracted size exceeds this (default 8 GiB)"}
                        },
                        "required": ["path", "dest"]
                    },
                    outputSchema={
                        "type": "object",
                        "properties": {
                            "dest": {"type": "string"},
                            "file_count": {"type": "integer"},
                            "layout_decoded": {"type": "boolean"},
                            "files": {"type": "array"}
                        }
                    }
                ),
                # === CUSTOM BPA GOVERNANCE (Wave 4) ===
                Tool(
                    name="bpa_validate_rules",
                    description="Validate a custom Best Practice Analyzer rules JSON (the public BPA rule shape): required fields (ID/Name/Category/Severity/Scope/Expression), valid Severity (1/2/3) and Scope values, duplicate IDs, destructive Delete() fixes on low-severity rules, and stray runtime-only fields. With fix=true also returns a cleaned copy. Pure validation, no external tool.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "rules": {"description": "The rules as a JSON string, array, or {Rules:[...]} object"},
                            "rules_path": {"type": "string", "description": "Path to a BPARules.json file (alternative to 'rules')"},
                            "fix": {"type": "boolean", "default": False, "description": "Also return a cleaned copy (runtime fields stripped, null FixExpression dropped)"}
                        },
                        "required": []
                    },
                    outputSchema={
                        "type": "object",
                        "properties": {
                            "valid": {"type": "boolean"},
                            "rule_count": {"type": "integer"},
                            "errors": {"type": "array"},
                            "warnings": {"type": "array"}
                        }
                    }
                ),
                Tool(
                    name="bpa_audit_rule_sources",
                    description="Audit where BPA rules live for the loaded PBIP project: rules embedded in the model (BestPracticeAnalyzer annotation), external rule-file URLs, and ignored rule IDs, merged with any local user/machine BPARules.json found. Reveals shadow governance and ignored rules.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "model_text": {"type": "string", "description": "Optional raw model TMDL/BIM text to scan instead of the loaded project"}
                        },
                        "required": []
                    },
                    outputSchema={
                        "type": "object",
                        "properties": {
                            "embedded_rule_count": {"type": "integer"},
                            "external_rule_files": {"type": "array"},
                            "ignored_rule_ids": {"type": "array"}
                        }
                    }
                ),
                # === DATA MODELLING / WAREHOUSING / BULK DAX (Wave 5) ===
                Tool(
                    name="generate_measure_suite",
                    description="Bulk-generate a governed suite of DAX measures from one base measure or column: time intelligence (YTD/QTD/MTD/PY/YoY/YoY %/MoM %/rolling windows), share-of-total ratios per dimension, ranks, or column statistics. Every measure gets a name, self-contained DAX, format string, display folder, and description. target='none' returns the suite; 'pbip' writes it into the loaded PBIP project's TMDL; 'live' creates it in the connected Desktop model (validated batch).",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "kind": {"type": "string", "enum": ["time_intelligence", "ratios", "ranking", "column_stats"]},
                            "base_measure": {"type": "string", "description": "Base measure name (time_intelligence/ratios/ranking)"},
                            "date_column": {"type": "string", "description": "Date column as Table.Column, e.g. Date.Date (time_intelligence)"},
                            "variants": {"type": "array", "items": {"type": "string"}, "description": "Time-intelligence subset, e.g. ['ytd','py','yoy_pct'] (default: a sensible core set)"},
                            "dimension_columns": {"type": "array", "items": {"type": "string"}, "description": "Dimension columns as Table.Column (ratios/ranking)"},
                            "column": {"type": "string", "description": "Numeric column as Table.Column (column_stats)"},
                            "stats": {"type": "array", "items": {"type": "string"}, "description": "Stats subset: sum/avg/min/max/median/distinct"},
                            "display_folder": {"type": "string"},
                            "base_format": {"type": "string", "description": "Base measure's format string, inherited by non-percent variants (default: read from the connected model)"},
                            "fiscal_year_end": {"type": "string", "description": "time_intelligence: fiscal year end as MM-DD (e.g. '06-30') for YTD/QTD"},
                            "sort_by_columns": {"type": "object", "description": "ratios/ranking: {'Table.Column': 'Table.SortColumn'} so the share and rank also clear the sort-by column (default: read from the connected model)"},
                            "include_all_selected": {"type": "boolean", "default": True, "description": "ratios/ranking: also emit the ALLSELECTED variant that respects slicers"},
                            "column_format": {"type": "string", "description": "column_stats: format string for the generated measures"},
                            "target": {"type": "string", "enum": ["none", "pbip", "live"], "default": "none"},
                            "table": {"type": "string", "description": "Table to host the measures (required for target pbip/live)"},
                            "skip_validation": {"type": "boolean", "default": False, "description": "target='live' only: skip the per-expression validation probe"}
                        },
                        "required": ["kind"]
                    },
                    outputSchema={
                        "type": "object",
                        "properties": {
                            "measures": {"type": "array"},
                            "written": {"type": "boolean"},
                            "target": {"type": "string"}
                        }
                    }
                ),
                Tool(
                    name="batch_create_measures",
                    description="Create MANY measures on a table of the connected Power BI Desktop model in one all-or-nothing batch (TOM). The whole batch is pre-validated (each expression probed against the live model, duplicate names rejected) before anything is created. Honors an open tom transaction.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "table_name": {"type": "string"},
                            "measures": {"type": "array", "items": {"type": "object", "properties": {
                                "name": {"type": "string"},
                                "expression": {"type": "string"},
                                "format_string": {"type": "string"},
                                "description": {"type": "string"},
                                "display_folder": {"type": "string"}
                            }, "required": ["name", "expression"]}},
                            "skip_validation": {"type": "boolean", "default": False}
                        },
                        "required": ["table_name", "measures"]
                    }
                ),
                Tool(
                    name="audit_star_schema",
                    description="Audit the model as a dimensional (star-schema) design: classifies every table (fact / dimension / date dimension / bridge / disconnected) from relationship topology, then flags snowflake chains, bidirectional filters, many-to-many, fact-to-fact joins, a missing or unmarked date table, measure-less facts, and text attributes stranded on facts. Returns a 0-100 score, a grade, and a recommendation per finding.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "source": {"type": "string", "enum": ["desktop", "cloud"], "default": "desktop"},
                            "workspace_name": {"type": "string"},
                            "dataset_name": {"type": "string"}
                        },
                        "required": []
                    },
                    outputSchema={
                        "type": "object",
                        "properties": {
                            "classification": {"type": "object"},
                            "findings": {"type": "array"},
                            "summary": {"type": "object"}
                        }
                    }
                ),
                Tool(
                    name="scan_referential_integrity",
                    description="Scan every active relationship for referential-integrity violations: fact keys with no matching dimension row (the cause of the hidden blank row and silently wrong totals). Runs an EXCEPT count per relationship against the connected model and reports orphan-key counts with samples.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "source": {"type": "string", "enum": ["desktop", "cloud"], "default": "desktop"},
                            "max_samples": {"type": "integer", "default": 5, "description": "Sample orphan keys to list per violating relationship"},
                            "workspace_name": {"type": "string"},
                            "dataset_name": {"type": "string"}
                        },
                        "required": []
                    },
                    outputSchema={
                        "type": "object",
                        "properties": {
                            "checked": {"type": "integer"},
                            "violations": {"type": "array"}
                        }
                    }
                ),
                Tool(
                    name="pbip_add_measures",
                    description="Bulk-add measures OFFLINE into a loaded PBIP project's TMDL (no Power BI needed): each measure is written to the target table's .tmdl with formatString, displayFolder, and description. The batch is linted (dax_lint) and checked for name collisions first, and the write is atomic. mode='add' refuses a name that already exists; mode='upsert' replaces a same-named measure of that table in place (keeping its lineageTag) and gives new measures a deterministic tag, so re-running a build leaves the file byte-identical.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "table": {"type": "string", "description": "Table whose .tmdl hosts the measures"},
                            "measures": {"type": "array", "items": {"type": "object", "properties": {
                                "name": {"type": "string"},
                                "expression": {"type": "string"},
                                "format_string": {"type": "string"},
                                "description": {"type": "string"},
                                "display_folder": {"type": "string"}
                            }, "required": ["name", "expression"]}},
                            "mode": {"type": "string", "enum": ["add", "upsert"], "default": "add", "description": "add = refuse existing names; upsert = replace a same-named measure of this table in place (idempotent rebuilds)"},
                            "skip_lint": {"type": "boolean", "default": False}
                        },
                        "required": ["table", "measures"]
                    },
                    outputSchema={
                        "type": "object",
                        "properties": {
                            "created": {"type": "array"},
                            "updated": {"type": "array"},
                            "path": {"type": "string"}
                        }
                    }
                ),
                Tool(
                    name="pbip_create_date_table",
                    description="Create a complete DAX date-dimension table OFFLINE in the loaded PBIP project: a calculated table over CALENDAR() with Year/Quarter/Month/Month Number/Week/Day columns, marked as the model's date table (date key), sort-by-column wiring, and hidden helper columns. The foundation for time intelligence.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "name": {"type": "string", "default": "Date", "description": "Date table name"},
                            "start_date": {"type": "string", "description": "ISO date, e.g. 2015-01-01 (default: auto from MIN/MAX of fact dates is NOT possible offline; defaults to 2015-01-01)"},
                            "end_date": {"type": "string", "description": "ISO date (default 2030-12-31)"},
                            "fiscal_year_start_month": {"type": "integer", "description": "1-12; when set, adds Fiscal Year / Fiscal Quarter columns"}
                        },
                        "required": []
                    }
                ),
                Tool(
                    name="pbip_add_calculation_group",
                    description="Create a calculation group OFFLINE in the loaded PBIP project (TMDL): the professional alternative to bulk-copying time-intelligence measures. Provide calculation items (name + DAX over SELECTEDMEASURE()); a ready-made time-intelligence set (YTD/QTD/MTD/PY/YoY/YoY %) can be generated by passing preset='time_intelligence' with a date_column.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "name": {"type": "string", "description": "Calculation group table name, e.g. 'Time Intelligence'"},
                            "column_name": {"type": "string", "default": "Calculation", "description": "The calc group's selector column name"},
                            "precedence": {"type": "integer", "default": 1},
                            "items": {"type": "array", "items": {"type": "object", "properties": {
                                "name": {"type": "string"},
                                "expression": {"type": "string"},
                                "format_string_expression": {"type": "string", "description": "Optional dynamic format string DAX, e.g. \"0.0%\""}
                            }, "required": ["name", "expression"]}, "description": "Calculation items (omit when using a preset)"},
                            "preset": {"type": "string", "enum": ["time_intelligence"], "description": "Generate a standard item set instead of passing items"},
                            "date_column": {"type": "string", "description": "Date column as Table.Column (required for the time_intelligence preset)"}
                        },
                        "required": ["name"]
                    }
                ),
                Tool(
                    name="pbip_add_hierarchy",
                    description="Add a drill-down hierarchy to a table OFFLINE in the loaded PBIP project (TMDL), e.g. Year > Quarter > Month > Date or Category > Subcategory > Product. Levels are existing columns of the table (validated first).",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "table": {"type": "string"},
                            "name": {"type": "string", "description": "Hierarchy name, e.g. 'Date Hierarchy'"},
                            "levels": {"type": "array", "items": {"type": "string"}, "description": "Column names in drill order (top first)"}
                        },
                        "required": ["table", "name", "levels"]
                    }
                ),
                # === POWER BI DESKTOP BRIDGE (Wave 6, preview) ===
                Tool(
                    name="bridge_status",
                    description="Discover running Power BI Desktop Bridge instances (the JSON-RPC endpoint inside each open Desktop window; preview feature, on by default). For each instance returns the process id, the open file path, whether it has unsaved changes, and - for PBIP/PBIR files - the report pages (id, display name, active). Start here before bridge_reload or bridge_screenshot.",
                    inputSchema={"type": "object", "properties": {}, "required": []},
                    outputSchema={
                        "type": "object",
                        "properties": {
                            "instances": {"type": "array"}
                        }
                    }
                ),
                Tool(
                    name="bridge_manifest",
                    description="Return the Desktop Bridge method manifest for a running Power BI Desktop process: which bridge methods this Desktop build supports, with their descriptions. Use to discover capabilities before calling them (the bridge surface is preview and grows over time).",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "pid": {"type": "integer", "description": "Desktop process id (from bridge_status); optional when exactly one instance is running"}
                        },
                        "required": []
                    },
                    outputSchema={
                        "type": "object",
                        "properties": {
                            "methods": {"type": "array"}
                        }
                    }
                ),
                Tool(
                    name="bridge_screenshot",
                    description="Capture PNG screenshots of report pages from the RUNNING Power BI Desktop via the Desktop Bridge - the agent can literally see the rendered report. Pass a page id or display name, or 'all' for every page. Images are saved to output_dir and the file paths returned. The heart of the edit-and-verify loop: author with pbir_*/pbip_* tools, bridge_reload, then screenshot to verify.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "page": {"type": "string", "description": "Page id, page display name, or 'all' (default 'all')"},
                            "pid": {"type": "integer", "description": "Desktop process id; optional when exactly one instance is running"},
                            "scale": {"type": "number", "description": "Scale factor 1.0-3.0 (default 1.0)"},
                            "output_dir": {"type": "string", "description": "Folder to save PNGs (default: a 'bridge-screenshots' folder in the temp dir)"}
                        },
                        "required": []
                    },
                    outputSchema={
                        "type": "object",
                        "properties": {
                            "screenshots": {"type": "array"}
                        }
                    }
                ),
                Tool(
                    name="bridge_reload",
                    description="Hot-reload the file open in Power BI Desktop from disk via the Desktop Bridge - NO close/reopen needed. This is how offline edits (pbir_add_page, pbir_add_visual, pbip_add_measures, pbip_create_date_table, renames...) become visible in the running Desktop. Refuses to reload over unsaved Desktop changes unless force=true. reload_model_definition=false reloads only the report layer.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "pid": {"type": "integer", "description": "Desktop process id; optional when exactly one instance is running"},
                            "reload_model_definition": {"type": "boolean", "default": True, "description": "Also re-apply the semantic model definition (default true)"},
                            "force": {"type": "boolean", "default": False, "description": "Reload even if Desktop has unsaved changes (they will be lost)"}
                        },
                        "required": []
                    }
                ),
                # === DOCUMENTATION (Wave 1) ===
                Tool(
                    name="export_data_dictionary",
                    description="Generate a portable data dictionary (tables, columns, measures with DAX, relationships) for the connected model, with a documentation-coverage score. Returns Markdown or HTML; optionally writes to a file. Re-runnable in CI so docs never go stale.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "source": {"type": "string", "enum": ["desktop", "cloud"], "default": "desktop"},
                            "format": {"type": "string", "enum": ["markdown", "html"], "default": "markdown"},
                            "output_path": {"type": "string", "description": "Optional file path to write the dictionary to; if omitted, the content is returned"},
                            "workspace_name": {"type": "string"},
                            "dataset_name": {"type": "string"}
                        },
                        "required": []
                    }
                ),
                # === SNAPSHOT / DIFF / QUALITY GATE (Wave 1) ===
                Tool(
                    name="model_snapshot",
                    description="Capture the connected model's metadata (tables/columns/measures/relationships) to a JSON snapshot file for later comparison with model_diff. Run before a change to establish a baseline.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "output_path": {"type": "string", "description": "File path to write the JSON snapshot; if omitted, the JSON is returned"},
                            "source": {"type": "string", "enum": ["desktop", "cloud"], "default": "desktop"},
                            "workspace_name": {"type": "string"},
                            "dataset_name": {"type": "string"}
                        },
                        "required": []
                    }
                ),
                Tool(
                    name="model_diff",
                    description="Produce a human-readable semantic diff (added/removed/changed tables, columns, measures, relationships) between a baseline snapshot and either another snapshot or the live model. Ideal for PR review and pre-deploy 'what changed'.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "baseline_path": {"type": "string", "description": "Path to the baseline JSON snapshot (from model_snapshot)"},
                            "compare_path": {"type": "string", "description": "Optional path to a second snapshot to compare against; if omitted, compares against the live model"},
                            "source": {"type": "string", "enum": ["desktop", "cloud"], "default": "desktop"},
                            "workspace_name": {"type": "string"},
                            "dataset_name": {"type": "string"}
                        },
                        "required": ["baseline_path"]
                    }
                ),
                Tool(
                    name="pre_deploy_gate",
                    description="CI/pre-deploy quality gate: runs the Best Practice Analyzer and AI-readiness audit and returns a machine PASS/FAIL verdict with blocking issues. Fails on any BPA error (and optionally warnings) or AI score below min_ai_score.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "min_ai_score": {"type": "number", "description": "Minimum AI-readiness score to pass (default 60)", "default": 60},
                            "block_on_warnings": {"type": "boolean", "description": "Also fail on BPA warnings (default false)", "default": False},
                            "source": {"type": "string", "enum": ["desktop", "cloud"], "default": "desktop"},
                            "workspace_name": {"type": "string"},
                            "dataset_name": {"type": "string"}
                        },
                        "required": []
                    },
                    outputSchema={
                        "type": "object",
                        "properties": {
                            "passed": {"type": "boolean"},
                            "bpa_errors": {"type": "integer"},
                            "bpa_warnings": {"type": "integer"},
                            "ai_score": {"type": "number"},
                            "blocking": {"type": "array"}
                        }
                    }
                ),
                # === DIAGNOSTICS & OPS (Wave 2) ===
                Tool(
                    name="refresh_doctor",
                    description="Diagnose a dataset's refresh failures: pulls refresh history via REST, classifies the most recent failure (expired credentials, capacity throttle, model eviction 0xC11C0020, gateway down, timeout, source/query error) with a concrete fix, and warns when approaching the 4-consecutive-failure auto-disable. Reads work on Pro.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "workspace_name": {"type": "string"},
                            "dataset_name": {"type": "string"},
                            "history_count": {"type": "integer", "description": "How many recent refreshes to inspect (default 10)", "default": 10}
                        },
                        "required": ["workspace_name", "dataset_name"]
                    },
                    outputSchema={
                        "type": "object",
                        "properties": {
                            "completed": {"type": "integer"},
                            "failed": {"type": "integer"},
                            "consecutive_failures": {"type": "integer"},
                            "most_recent_status": {"type": ["string", "null"]},
                            "diagnosis": {"type": ["object", "null"]}
                        }
                    }
                ),
                Tool(
                    name="find_unused_objects",
                    description="Find columns and measures not referenced by any other model object (INFO.CALCDEPENDENCY), relationship, or - when a PBIP project is loaded - any report visual. Safe-cleanup candidate list (a free replacement for paid unused-object tools).",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "source": {"type": "string", "enum": ["desktop", "cloud"], "default": "desktop"},
                            "workspace_name": {"type": "string"},
                            "dataset_name": {"type": "string"}
                        },
                        "required": []
                    }
                ),
                Tool(
                    name="impact_analysis",
                    description="Blast radius before a change: lists model objects that depend on a measure/column (INFO.CALCDEPENDENCY) and, when a PBIP project is loaded, the report files/visuals that reference it. Run before renaming or deleting.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "object_name": {"type": "string", "description": "Measure or column name to analyze"},
                            "table_name": {"type": "string", "description": "Optional owning table to disambiguate"},
                            "source": {"type": "string", "enum": ["desktop", "cloud"], "default": "desktop"},
                            "workspace_name": {"type": "string"},
                            "dataset_name": {"type": "string"}
                        },
                        "required": ["object_name"]
                    }
                ),
                Tool(
                    name="rls_test_harness",
                    description="Evaluate a measure or table row count under EVERY RLS role and return a pass/fail matrix vs the unrestricted baseline, flagging roles that see everything (no filtering) or nothing. Tests row-level security systematically on the connected Desktop model. Always restores the cleared role afterward.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "table_name": {"type": "string", "description": "Table to COUNTROWS under each role"},
                            "measure_name": {"type": "string", "description": "Measure to evaluate under each role"},
                            "dax": {"type": "string", "description": "Custom EVALUATE query to run under each role (overrides table/measure)"},
                            "roles": {"type": "array", "items": {"type": "string"}, "description": "Specific roles to test (default: all roles)"}
                        },
                        "required": []
                    }
                ),
                Tool(
                    name="run_dax_tests",
                    description="Run a suite of DAX regression tests against the model: each test is {name, dax, expected, tolerance?}; returns PASS/FAIL per test and an overall verdict. Use to catch measure regressions before deploy. Provide tests inline or via tests_path (JSON file).",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "tests": {"type": "array", "description": "Array of {name, dax, expected, tolerance?}",
                                      "items": {"type": "object", "properties": {
                                          "name": {"type": "string"}, "dax": {"type": "string"},
                                          "expected": {}, "tolerance": {"type": "number"}}, "required": ["dax"]}},
                            "tests_path": {"type": "string", "description": "Path to a JSON file with the tests array (alternative to 'tests')"},
                            "source": {"type": "string", "enum": ["desktop", "cloud"], "default": "desktop"},
                            "workspace_name": {"type": "string"},
                            "dataset_name": {"type": "string"}
                        },
                        "required": []
                    },
                    outputSchema={
                        "type": "object",
                        "properties": {
                            "passed": {"type": "integer"}, "total": {"type": "integer"},
                            "all_passed": {"type": "boolean"}, "results": {"type": "array"}
                        }
                    }
                ),
                Tool(
                    name="verify_audit_integrity",
                    description="Verify the audit-log hash chain (compliance/forensics). Detects edited, inserted, deleted, or hash-stripped entries; returns INTACT or TAMPERED with the first broken line. The chain is HMAC-keyed (cryptographically strong) when POWERBI_MCP_AUDIT_KEY is set, otherwise a plain SHA-256 chain that catches accidental edits and naive tampering.",
                    inputSchema={"type": "object", "properties": {}, "required": []},
                    outputSchema={
                        "type": "object",
                        "properties": {
                            "valid": {"type": "boolean"}, "checked": {"type": "integer"}, "message": {"type": "string"}
                        }
                    }
                ),
                # === GOVERNANCE-OPS FLEET (Wave 3, admin-gated) ===
                Tool(
                    name="cross_workspace_lineage",
                    description="Tenant-wide inventory + lineage via the Admin Scanner API: workspace/dataset/report counts, datasets missing RLS or a sensitivity label, and (with dataset_name) the downstream reports that depend on a dataset across all workspaces. Requires Fabric admin (or an SP allowed to use read-only admin APIs). Cache the scan to cache_path to avoid rescanning.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "dataset_name": {"type": "string", "description": "Optional: focus lineage on this dataset (downstream reports)"},
                            "workspace_ids": {"type": "array", "items": {"type": "string"}, "description": "Optional explicit workspace GUIDs (default: every workspace the admin API lists, up to max_workspaces)"},
                            "max_workspaces": {"type": "integer", "default": 500, "description": "Upper bound on workspaces scanned in one call (scanned 100 per scan). The output says so when the tenant has more."},
                            "max_wait_seconds": {"type": "number", "default": 240, "description": "How long to wait for the scans before returning their ids for a later resume"},
                            "scan_ids": {"type": "array", "items": {"type": "string"}, "description": "Resume: scan ids returned by an earlier call that timed out (no new scan is started)"},
                            "cache_path": {"type": "string", "description": "Optional path to read/write the scan result for reuse (not written in read-only mode)"},
                            "use_cache": {"type": "boolean", "default": True}
                        },
                        "required": []
                    }
                ),
                Tool(
                    name="fleet_refresh_monitor",
                    description="Refresh health across many datasets: for each refreshable dataset in the given workspaces, check the most recent refresh and classify failures (root cause). Centralized 'what failed across the fleet' view. Needs access to the listed workspaces.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "workspace_ids": {"type": "array", "items": {"type": "string"}, "description": "Workspace GUIDs to monitor (required, to bound the scan)"}
                        },
                        "required": ["workspace_ids"]
                    }
                ),
                Tool(
                    name="usage_and_orphan_analytics",
                    description="Tenant usage analytics from the Admin Activity Events API for one UTC day: total events, distinct users, top activities, top viewed reports, top users. Requires Fabric admin (28-day retention). Defaults to yesterday (today is incomplete).",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "date": {"type": "string", "description": "UTC day to analyze, YYYY-MM-DD (default: yesterday). Must be within 28 days."},
                            "filter": {"type": "string", "description": "Optional OData filter, e.g. \"Activity eq 'viewreport'\""}
                        },
                        "required": []
                    }
                ),
                # === REPORT AS CODE (Wave 7): publish staging ===
                Tool(
                    name="pbip_stage_publish",
                    description="Stage a PBIP report (and optionally its model) for publishing IN PLACE: copies the report into a stage folder with definition.pbir rebound from the local model path to a published semantic model (byConnection), drops local caches, and returns the one Fabric CLI command that imports it. Importing over an existing report keeps its id, so shared and public links keep working. Offline: never contacts the service and never changes the source project. Uses the loaded PBIP project unless report_dir is given.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "stage_dir": {"type": "string", "description": "Folder to stage into (must be outside the project)"},
                            "workspace": {"type": "string", "description": "Workspace name that holds the published model"},
                            "model_name": {"type": "string", "description": "Published semantic model name"},
                            "model_id": {"type": "string", "description": "Published semantic model GUID"},
                            "display_name": {"type": "string", "description": "Name of the report item to import over (default: the local report name)"},
                            "report_dir": {"type": "string", "description": "A *.Report folder (default: the loaded project's report)"},
                            "include_model": {"type": "boolean", "default": False, "description": "Also stage the semantic model definition and report whether the service could refresh it"},
                            "model_dir": {"type": "string", "description": "A *.SemanticModel folder (default: the loaded project's model)"},
                            "overwrite": {"type": "boolean", "default": False, "description": "Replace a previously staged folder of the same name"}
                        },
                        "required": ["stage_dir", "workspace", "model_name", "model_id"]
                    },
                    outputSchema={
                        "type": "object",
                        "properties": {
                            "report": {"type": "object"},
                            "model": {"type": ["object", "null"]},
                            "error": {"type": ["string", "null"]}
                        }
                    }
                ),
                Tool(
                    name="pbir_apply_spec",
                    description=(
                        "Report as code: build or update the loaded PBIP report from ONE declarative JSON spec "
                        "(theme, pages, visuals, text, shapes, images, navigation buttons, slicers with sync, "
                        "HTML Content and Deneb visuals, private custom visuals, hidden pop-ups with open/close "
                        "bookmarks, and elements shared across pages). Idempotent: every page and element has a "
                        "stable name, so re-applying updates in place and an unchanged spec rewrites nothing. "
                        "All-or-nothing: the whole spec is validated (names, layout, navigation targets, and "
                        "every field against the model) before any file is written. Pages and visuals the spec "
                        "did not create are never touched. Use dry_run=true first to see the plan. "
                        "Spec keys: theme, custom_visuals, page_defaults, pages[{name, display_name, width, "
                        "height, background, elements[], popups[]}], shared[{name, pages, elements[], popups[]}], "
                        "bookmarks[], page_order, active_page. Element: {name, type, x, y, width, height, ...} "
                        "with type textbox | shape | image | button | slicer | html | deneb | visual | group; a "
                        "visual takes visual_type and fields {role: 'Table.Field'}; a button takes text and "
                        "action {type: page | bookmark | back | url | clear_slicers, target}. Fields are "
                        "'Table.Field' or {measure: 'Table.Name'} / {column: 'Table.Name'}. Full format and an "
                        "example: docs/REPORT_SPEC.md."),
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "spec": {"type": "object", "description": "The report spec (see docs/REPORT_SPEC.md)"},
                            "spec_path": {"type": "string", "description": "Alternative: path to a JSON file holding the spec"},
                            "dry_run": {"type": "boolean", "default": False, "description": "Validate and return the files that would be written or removed, without touching disk"},
                            "prune": {"type": "boolean", "default": False, "description": "Also remove pages, elements and bookmarks that an earlier apply created and this spec no longer contains (never anything else)"},
                            "adopt": {"type": "boolean", "default": False, "description": "Take over existing pages or elements that already use a name from the spec instead of refusing"},
                            "backup": {"type": "boolean", "default": True, "description": "Back up the project before writing"},
                            "skip_field_validation": {"type": "boolean", "default": False, "description": "Do not check field references against the model"}
                        },
                        "required": []
                    },
                    outputSchema={
                        "type": "object",
                        "properties": {
                            "success": {"type": "boolean"},
                            "written": {"type": "array", "items": {"type": "string"}},
                            "removed": {"type": "array", "items": {"type": "string"}},
                            "unchanged": {"type": "integer"},
                            "warnings": {"type": "array", "items": {"type": "string"}},
                            "errors": {"type": "array", "items": {"type": "string"}}
                        }
                    }
                ),
                Tool(
                    name="generate_payload_measure",
                    description=(
                        "DAX as transport: generate a text measure that packs rows at a chosen grain into one "
                        "string for a JavaScript consumer (an HTML Content app or a custom visual), plus the "
                        "matching JavaScript parser. The browser then filters and aggregates in memory, so a "
                        "click inside the visual costs no query, while report slicers still narrow what the "
                        "measure sends. Numbers are emitted locale-safe (integers through FORMAT(x, \"0\"), "
                        "decimals as scaled integers, dates as yyyymmdd) and text is escaped for the chosen "
                        "format. Pass 'fields' for a fact payload, or 'dimensions' for key-to-label lookup tables "
                        "in one measure. Returns the DAX, the parser and a character-budget estimate (the "
                        "2,100,000-character ceiling is an empirical figure, not a documented limit). Generation "
                        "only: save it with pbip_add_measures or create_measure."),
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "name": {"type": "string", "description": "Measure name"},
                            "table": {"type": "string", "description": "Fact payload: the table whose rows (or groups) are packed"},
                            "fields": {"type": "array", "description": "Fact payload: each 'Column:type' or {column | measure | expression, type, name?, scale?, blank?, aggregate?}; type is int | decimal | date | bool | text", "items": {}},
                            "dimensions": {"type": "array", "description": "Dimension payload instead of fields: each {table, name?, key, key_type?, labels: [columns], where?, all_rows?}", "items": {"type": "object"}},
                            "format": {"type": "string", "enum": ["delimited", "json"], "default": "delimited"},
                            "grain": {"type": "string", "enum": ["rows", "groups"], "description": "rows = one record per table row; groups = one per distinct combination of the column fields (default when a measure field is present)"},
                            "where": {"type": "string", "description": "Row-level DAX condition on the table, e.g. Sales[Qty] > 0"},
                            "order_by": {"type": "array", "items": {}, "description": "Field names (or [name, 'ASC'|'DESC']) for a stable record order"},
                            "record_sep": {"type": "string", "description": "Delimited format: record separator (default ';')"},
                            "field_sep": {"type": "string", "description": "Delimited format: field separator (default ',' for facts, '^' for dimensions)"},
                            "expected_rows": {"description": "Row count (facts) or {dimension: rows} to size the payload against the ceiling"},
                            "display_folder": {"type": "string"}
                        },
                        "required": ["name"]
                    },
                    outputSchema={
                        "type": "object",
                        "properties": {
                            "name": {"type": "string"},
                            "expression": {"type": "string"},
                            "js_parser": {"type": "string"},
                            "budget": {"type": "object"},
                            "notes": {},
                            "error": {"type": ["string", "null"]}
                        }
                    }
                ),
                Tool(
                    name="generate_html_measure",
                    description=(
                        "Generate a DAX measure that returns styled HTML for the HTML Content visual (no "
                        "JavaScript): a KPI card with an optional comparison, a KPI strip, a headline sentence "
                        "with live numbers, a ranked bar list, or a simple table. Inline CSS only; every value is "
                        "HTML-escaped inside the DAX; blank-safe. kind = kpi_card (value_measure, label, "
                        "format_string, comparison_measure, comparison_label, higher_is_better) | kpi_strip "
                        "(items: [{value_measure, label, format_string, ...}]) | headline (template with {0} {1} "
                        "placeholders, values: [{measure, format_string} | text]) | bar_list (category "
                        "'Table.Column', value_measure, top_n, title) | table (category, columns: [{measure, "
                        "header, format_string}], top_n). Style keys such as font_family, text_colour, accent, "
                        "background apply to every kind. Generation only: save it with pbip_add_measures or "
                        "create_measure, then bind it with an 'html' element in pbir_apply_spec."),
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "kind": {"type": "string", "enum": ["kpi_card", "kpi_strip", "headline", "bar_list", "table"]},
                            "name": {"type": "string", "description": "Measure name (a default is derived when omitted)"},
                            "params": {"type": "object", "description": "The parameters for that kind (see the description)"}
                        },
                        "required": ["kind"]
                    },
                    outputSchema={
                        "type": "object",
                        "properties": {
                            "name": {"type": "string"},
                            "kind": {"type": "string"},
                            "dax": {"type": "string"},
                            "expression": {"type": "string"},
                            "notes": {},
                            "error": {"type": ["string", "null"]}
                        }
                    }
                ),
                Tool(
                    name="build_js_app_measure",
                    description=(
                        "A JavaScript app inside a DAX measure: assemble CSS and ordered JavaScript modules into "
                        "three measures for the HTML Content visual: '<Prefix> Styles' and '<Prefix> Runtime' "
                        "(constants) and '<Prefix>' (the document: style, a stage element and one script that "
                        "sets the payload globals, then runs the runtime; an empty-state message when the "
                        "payload is blank). Modules share one function scope and define main(app); a persistent "
                        "window singleton and keyed listeners make re-evaluation in the reused iframe safe. The "
                        "build strips comments and whitespace with a minifier that understands strings, template "
                        "literals and regex literals, refuses sources that cannot be shipped (</script, control "
                        "characters), checks syntax with Node when it is installed, and reports each measure's "
                        "size against the ceiling. Pair with generate_payload_measure for the data. "
                        "target='none' returns sizes and warnings; target='pbip' writes the measures into the "
                        "loaded project (idempotent upsert)."),
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "prefix": {"type": "string", "description": "Measure name stem; the document measure is the prefix itself"},
                            "css": {"type": "string", "description": "Stylesheet source"},
                            "css_path": {"type": "string", "description": "Alternative: path to a .css file"},
                            "js_modules": {"type": "array", "description": "Ordered modules: [{name, source}]", "items": {"type": "object"}},
                            "js_dir": {"type": "string", "description": "Alternative: a folder whose .js files are taken in file-name order"},
                            "payload_measures": {"type": "array", "description": "[{measure, global, embed_as?: string | json | text, required?}]: each measure is assigned to that JavaScript global before the runtime runs", "items": {"type": "object"}},
                            "root_id": {"type": "string", "default": "app-root"},
                            "empty_message": {"type": "string"},
                            "minify": {"type": "boolean", "default": True},
                            "display_folder": {"type": "string"},
                            "target": {"type": "string", "enum": ["none", "pbip"], "default": "none"},
                            "table": {"type": "string", "description": "target='pbip': table that hosts the measures"},
                            "return_expressions": {"type": "boolean", "default": False, "description": "Include the full measure expressions in the result (they can exceed 100 KB)"}
                        },
                        "required": ["prefix"]
                    },
                    outputSchema={
                        "type": "object",
                        "properties": {
                            "measures": {"type": "array"},
                            "sizes": {"type": "object"},
                            "warnings": {"type": "array"},
                            "node_check": {"type": "string"},
                            "written": {"type": "boolean"},
                            "error": {"type": ["string", "null"]}
                        }
                    }
                ),
                Tool(
                    name="pbip_embed_data",
                    description=(
                        "Make the loaded PBIP model self-contained: replace the query of each named table with "
                        "its CSV data embedded in the partition (deflate + base64 inside the M expression), so "
                        "the model can be published and refreshed in the service with no local files, no "
                        "credentials and no gateway. Suited to small static or demo data. Column types come from "
                        "the model; the CSV header must use the table's source column names. A folder parameter "
                        "that nothing references any more is removed. DRY RUN BY DEFAULT: it reports sizes and "
                        "what would change; pass dry_run=false to write (all tables together or none)."),
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "tables": {"type": "object", "description": "{table name: path of a UTF-8 CSV file}", "additionalProperties": {"type": "string"}},
                            "dry_run": {"type": "boolean", "default": True},
                            "max_bytes": {"type": "integer", "description": "Largest CSV accepted per table (default 8 MiB)"},
                            "delimiter": {"type": "string", "default": ","},
                            "backup": {"type": "boolean", "default": True, "description": "Back up the project before writing"}
                        },
                        "required": ["tables"]
                    },
                    outputSchema={
                        "type": "object",
                        "properties": {
                            "success": {"type": "boolean"},
                            "dry_run": {"type": "boolean"},
                            "tables": {"type": "array"},
                            "self_contained": {"type": "boolean"},
                            "changed_files": {"type": "array"},
                            "error": {"type": ["string", "null"]}
                        }
                    }
                ),
                Tool(
                    name="audit_report_design",
                    description=(
                        "Best Practice Analyzer for the REPORT layer: audit the loaded PBIP report (or report_dir) "
                        "without changing it. Structure: unparseable JSON, byte order marks, folder and name "
                        "mismatches, pages.json consistency, custom visuals used but not registered, visuals "
                        "outside the page, misplaced properties, mixed schema versions, fields missing from the "
                        "model. Design: overlapping data visuals, crowded pages, missing alt text and titles, "
                        "tiny elements, inconsistent page sizes, hidden pages nothing navigates to, the same "
                        "field sliced on several pages without a sync group, buttons and bookmarks pointing at "
                        "missing targets, hard-coded colours that bypass the theme. Governance notes: private "
                        "(uncertified) custom visuals and script-capable HTML visuals a tenant admin may block."),
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "report_dir": {"type": "string", "description": "A *.Report folder or its definition folder (default: the loaded project's report)"},
                            "min_severity": {"type": "string", "enum": ["info", "warning", "error"], "default": "info"},
                            "max_data_visuals_per_page": {"type": "integer", "default": 12},
                            "max_visuals_per_page": {"type": "integer", "default": 40}
                        },
                        "required": []
                    },
                    outputSchema={
                        "type": "object",
                        "properties": {
                            "summary": {"type": "object"},
                            "findings": {"type": "array"},
                            "error": {"type": ["string", "null"]}
                        }
                    }
                ),
                Tool(
                    name="pbip_undo",
                    description="Undo the last committed pbip_rename_* / pbip_fix_* change on the loaded PBIP project by restoring every file it changed to its previous bytes. One level of undo, for this session only.",
                    inputSchema={
                        "type": "object",
                        "properties": {},
                        "required": []
                    }
                )
            ]
            # Attach MCP safety/behavior hints from the annotations registry.
            for t in tools:
                annotations = self._tool_annotations.get(t.name)
                if annotations is not None:
                    t.annotations = annotations
            return tools

        self._tools = build_tools()
        # Tools that declare an outputSchema must always return structuredContent, including
        # on refusal and on an unexpected error, or the SDK rejects the result.
        self._structured_tools = {t.name for t in self._tools if t.outputSchema}

        @self.server.list_tools()
        async def handle_list_tools() -> List[Tool]:
            return self._tools

        @self.server.call_tool()
        async def handle_call_tool(name: str, arguments: Optional[Dict[str, Any]]):
            """Request boundary for every tool call: read-only gate, dispatch, then secret
            redaction of whatever the handler returned (text and structured content alike), so a
            handler that passes an exception message through cannot leak a credential."""
            try:
                args = arguments or {}
                logger.info(f"Tool called: {name}")
                # Arguments can carry DAX (with PII literals) or secrets; only log at DEBUG, redacted.
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f"Tool args ({name}): {self._redact(json.dumps(args, default=str))}")

                handler = self._tool_dispatch.get(name)
                if handler is None:
                    return [TextContent(type="text", text=f"Unknown tool: {name}")]

                if self._read_only and name in self._write_tools:
                    return [TextContent(type="text", text=(
                        f"Refused: '{name}' is a write operation and the server is running in "
                        "READ-ONLY mode. Unset POWERBI_MCP_READONLY to allow model/report changes."
                    ))], {"error": "read_only_mode", "refused": True}

                annotations = self._tool_annotations.get(name)
                try:
                    result = await handler(args)
                finally:
                    if annotations is None or not annotations.readOnlyHint:
                        self._metadata_cache.clear()  # the model or connection may have changed

                # Handlers return a plain string, or (text, structured_dict) for tools that
                # declare an outputSchema; the SDK puts the dict in structuredContent.
                if isinstance(result, tuple) and len(result) == 2:
                    text, structured = result
                    return ([TextContent(type="text", text=self._redact(text))],
                            self._redact_structure(structured))
                text = self._redact(result)
                if name in self._structured_tools:
                    return [TextContent(type="text", text=text)], {"error": text}
                return [TextContent(type="text", text=text)]

            except Exception as e:
                safe_err = self._redact(str(e))
                logger.error(f"Error executing {name}: {safe_err}")
                if logger.isEnabledFor(logging.DEBUG):
                    import traceback
                    logger.debug(self._redact(traceback.format_exc()))
                message = f"Error executing {name}: {safe_err}"
                if name in self._structured_tools:
                    return [TextContent(type="text", text=message)], {"error": safe_err}
                return [TextContent(type="text", text=message)]

        # ---------- MCP Resources: model context without spending a tool call ----------
        @self.server.list_resources()
        async def handle_list_resources():
            return [
                Resource(uri="powerbi://desktop/schema", name="desktop_schema",
                         title="Connected model schema",
                         description="Tables, columns, measures and relationships of the connected Power BI Desktop model",
                         mimeType="application/json"),
                Resource(uri="powerbi://desktop/measures", name="desktop_measures",
                         title="Model measures",
                         description="All measures (with DAX expressions) in the connected Desktop model",
                         mimeType="application/json"),
                Resource(uri="powerbi://desktop/bpa", name="desktop_bpa",
                         title="Best Practice Analyzer findings",
                         description="BPA scan of the connected Desktop model",
                         mimeType="application/json"),
                Resource(uri="powerbi://desktop/ai-readiness", name="desktop_ai_readiness",
                         title="AI-readiness report",
                         description="AI-readiness score and metrics for the connected Desktop model",
                         mimeType="application/json"),
                Resource(uri="powerbi://reference/bpa-rules", name="bpa_rules",
                         title="Best Practice Analyzer rules",
                         description="The built-in BPA rule catalog (id, category, severity, name)",
                         mimeType="application/json"),
                Resource(uri="powerbi://reference/refresh-errors", name="refresh_errors",
                         title="Refresh error remediation map",
                         description="Known refresh failure causes and their fixes (used by refresh_doctor)",
                         mimeType="application/json"),
            ]

        @self.server.list_resource_templates()
        async def handle_list_resource_templates():
            return [
                ResourceTemplate(uriTemplate="powerbi://cloud/{workspace}/{dataset}/schema",
                                 name="cloud_schema", title="Cloud model schema",
                                 description="Schema of a published semantic model via the XMLA endpoint",
                                 mimeType="application/json"),
            ]

        @self.server.read_resource()
        async def handle_read_resource(uri):
            return self._redact(await self._read_resource(str(uri)))

        # ---------- MCP Prompts: reusable guided BI workflows ----------
        @self.server.list_prompts()
        async def handle_list_prompts():
            return [
                Prompt(name=n, title=p.get("title", n), description=p["description"],
                       arguments=p.get("arguments", []))
                for n, p in self._prompts.items()
            ]

        @self.server.get_prompt()
        async def handle_get_prompt(name, arguments):
            p = self._prompts.get(name)
            if not p:
                raise ValueError(f"Unknown prompt: {name}")
            text = p["render"](arguments or {})
            return GetPromptResult(
                description=p["description"],
                messages=[PromptMessage(role="user", content=TextContent(type="text", text=text))],
            )

        # ---------- MCP Completion: ground arguments in real model object names ----------
        @self.server.completion()
        async def handle_completion(ref, argument, context):
            return await self._complete_argument(argument)

    # ==================== BOUNDARY HELPERS ====================

    def _redact(self, text: Any) -> str:
        """Secret-redacted text for anything that leaves the process (responses and logs)."""
        return redact_secrets(text, [self.client_secret])

    def _redact_structure(self, obj: Any) -> Any:
        """Redact secrets inside structured content: every string at any depth, and any value
        stored under a credential-named key."""
        return security_redact_structure(obj, [self.client_secret])

    async def _measure_definitions(self, query: str, source: str, workspace=None, dataset=None) -> Dict[str, str]:
        """Measure name -> DAX for the connected model, so the policy check can see through a
        measure that wraps a protected column. Only read when the query uses an unqualified
        [Name] at all (a query without one cannot call a measure); empty when the model cannot
        be read, in which case measures are judged by name only."""
        if not any(r.kind == "bare" for r in dax_tokens.references(query)):
            return {}
        try:
            model, err = await self._gather_model_metadata(source, workspace, dataset)
        except Exception as e:
            logger.warning(f"Could not read measure definitions for the policy check: {self._redact(str(e))}")
            return {}
        if err or not model:
            return {}
        return {m["name"]: m.get("expression") or ""
                for t in model.get("tables", []) for m in t.get("measures", []) if m.get("name")}

    async def _run_guarded_query(self, query: str, *, tool: str, source: str = "desktop",
                                 workspace: Optional[str] = None, dataset: Optional[str] = None,
                                 max_rows: int = 1000, run=None):
        """Run a DAX query on behalf of a tool and return (rows, report, error).

        Every tool that executes caller-supplied DAX goes through here, not only the two
        execute tools: the text must be a DAX query (never TMSL or XMLA), it must pass the
        access policy (which sees through aliases and through measures), and the rows that come
        back are filtered by the same column policies and PII masking and written to the audit
        log. ``error`` is a message for the caller when the query was refused or failed; in that
        case ``rows`` is empty.

        ``run`` overrides how the query is executed (a callable taking the query text); by
        default the Desktop or XMLA connector for ``source`` is used.
        """
        try:
            dax_tokens.ensure_query(query)
        except dax_tokens.NonQueryStatementError as e:
            self.security.log_policy_violation(policy_name="statement_guard", violation_type=str(e),
                                               query=query, tool=tool)
            return [], {}, f"Refused: {e}"

        measures = await self._measure_definitions(query, source, workspace, dataset)
        check = self.security.pre_query_check(query, measure_definitions=measures)
        if not check.allowed:
            self.security.log_policy_violation(policy_name="query_policy", violation_type=check.reason,
                                               query=query, tool=tool)
            return [], {}, f"Query blocked by security policy: {check.reason}"
        if check.max_rows and check.max_rows < max_rows:
            max_rows = check.max_rows

        loop = asyncio.get_event_loop()
        model_name = dataset
        port = None
        if run is None:
            if (source or "desktop").lower() == "cloud":
                connector = await loop.run_in_executor(None, self._get_xmla_connector, workspace, dataset)
                if not connector:
                    return [], {}, f"Error: could not connect to dataset '{dataset}'"
                run = lambda q: connector.execute_dax(q, max_rows)  # noqa: E731
            else:
                desktop = self._get_desktop_connector()
                if not desktop.current_port:
                    return [], {}, "Not connected to Power BI Desktop. Use 'desktop_connect' first."
                model_name, port = getattr(desktop, "current_model_name", None), desktop.current_port
                run = lambda q: desktop.execute_dax(q, max_rows)  # noqa: E731

        start = time.time()
        try:
            rows = await loop.run_in_executor(None, run, query)
        except Exception as e:
            # An engine error can quote data (ERROR(CONCATENATEX(...))): the layer withholds the
            # text when the query reads a protected column, and redacts secrets otherwise.
            safe = self.security.safe_error_text(str(e), query=query, measure_definitions=measures,
                                                 extra_secrets=[self.client_secret])
            self.security.process_results(results=[], query=query, source=source, model_name=model_name,
                                          success=False, error_message=safe, measure_definitions=measures,
                                          tool=tool)
            return [], {}, f"Error executing DAX: {safe}"
        duration_ms = (time.time() - start) * 1000
        truncated = bool(getattr(rows, "truncated", False))
        rows = list(rows) if isinstance(rows, list) else []
        if len(rows) > max_rows:
            truncated = True
            rows = rows[:max_rows]
        safe_rows, report = self.security.process_results(
            results=rows, query=query, source=source, model_name=model_name, port=port,
            duration_ms=duration_ms, success=True, measure_definitions=measures, tool=tool)
        report = dict(report or {})
        if report.get("query_denied"):
            return [], report, f"Query blocked by security policy: {report.get('denied_reason') or 'denied'}"
        report.update({"duration_ms": duration_ms, "tool": tool,
                       "truncated": truncated or bool(report.get("rows_truncated"))})
        return safe_rows, report, None

    @staticmethod
    def _first_value(rows: List[Dict[str, Any]]):
        """First cell of the first row, or None: how the scalar-style tools read a result."""
        if not rows:
            return None
        return next(iter(rows[0].values()), None)

    @staticmethod
    def _row_limit(requested: Any, default: int, ceiling: int = 100000) -> int:
        """A sane row cap from a caller-supplied value: default when absent or invalid,
        never below 1 and never above the hard ceiling that protects memory."""
        try:
            value = int(requested) if requested is not None else default
        except (TypeError, ValueError):
            value = default
        return max(1, min(value, ceiling))

    @staticmethod
    def _render_query_result(rows: List[Dict[str, Any]], report: Dict[str, Any]) -> str:
        lines = [f"Query returned {len(rows)} row(s)"]
        if report.get("pii_detected"):
            lines.append(f"PII detected and masked: {report.get('pii_count', 0)} instance(s) of "
                         f"{', '.join(report.get('pii_types', []))}")
        if report.get("columns_blocked"):
            lines.append(f"Blocked columns: {', '.join(report['columns_blocked'])}")
        if report.get("truncated"):
            lines.append(f"(Note: result truncated to the first {len(rows)} rows)")
        return "\n".join(lines) + "\n\n" + json.dumps(rows, indent=2, default=str)

    # ==================== DESKTOP HANDLERS ====================

    def _get_desktop_connector(self) -> PowerBIDesktopConnector:
        """Get or create Desktop connector"""
        if not self.desktop_connector:
            self.desktop_connector = PowerBIDesktopConnector()
        return self.desktop_connector

    async def _handle_desktop_discover(self) -> str:
        """Discover running Power BI Desktop instances"""
        try:
            connector = self._get_desktop_connector()

            if not connector.is_available():
                return (
                    "Error: Desktop connectivity unavailable. Ensure 'psutil' is installed and "
                    "the ADOMD.NET client library can be found. Newer Power BI Desktop builds no "
                    "longer ship it, so install SSMS or the Microsoft.AnalysisServices.AdomdClient "
                    "NuGet package, or set ADOMD_DLL_PATH to the folder containing "
                    "Microsoft.AnalysisServices.AdomdClient.dll."
                )

            instances = await asyncio.get_event_loop().run_in_executor(
                None, connector.discover_instances
            )

            if not instances:
                return "No Power BI Desktop instances found. Please open a .pbix file in Power BI Desktop."

            result = f"Found {len(instances)} Power BI Desktop instance(s):\n\n"
            for i, inst in enumerate(instances, 1):
                result += f"{i}. Port: {inst['port']}\n"
                result += f"   Model: {inst['model_name']}\n"
                result += f"   PID: {inst['pid']}\n\n"

            result += "\nUse 'desktop_connect' with a port number to connect to an instance."
            return result

        except Exception as e:
            logger.error(f"Desktop discover error: {e}")
            return f"Error discovering instances: {str(e)}"

    async def _handle_desktop_connect(self, args: Dict[str, Any]) -> str:
        """Connect to a Power BI Desktop instance"""
        try:
            connector = self._get_desktop_connector()
            port = args.get("port")
            rls_role = args.get("rls_role")

            # Use lambda to pass both arguments
            connect_fn = lambda: connector.connect(port=port, rls_role=rls_role)
            success = await asyncio.get_event_loop().run_in_executor(None, connect_fn)

            if success:
                model_name = connector.current_model_name or "Unknown"
                result = f"Connected to Power BI Desktop!\n\nModel: {model_name}\nPort: {connector.current_port}"

                if rls_role:
                    result += f"\nRLS Role: {rls_role} (active)"
                else:
                    result += "\nRLS: None (full data access)"

                return result
            else:
                return "Failed to connect. Ensure Power BI Desktop is running with a .pbix file open."

        except Exception as e:
            logger.error(f"Desktop connect error: {e}")
            return f"Error connecting: {str(e)}"

    async def _handle_desktop_list_tables(self) -> str:
        """List tables from connected Desktop model"""
        try:
            connector = self._get_desktop_connector()

            if not connector.current_port:
                return "Not connected to Power BI Desktop. Use 'desktop_connect' first."

            tables = await asyncio.get_event_loop().run_in_executor(
                None, connector.list_tables
            )

            if not tables:
                return "No tables found in the model."

            result = f"Tables in {connector.current_model_name or 'model'} ({len(tables)}):\n\n"
            for table in tables:
                result += f"  - {table['name']}\n"

            return result

        except Exception as e:
            logger.error(f"Desktop list tables error: {e}")
            return f"Error listing tables: {str(e)}"

    async def _handle_desktop_list_columns(self, args: Dict[str, Any]) -> str:
        """List columns for a table in Desktop model"""
        try:
            connector = self._get_desktop_connector()
            table_name = args.get("table_name")

            if not connector.current_port:
                return "Not connected to Power BI Desktop. Use 'desktop_connect' first."

            if not table_name:
                return "Error: table_name is required"

            columns = await asyncio.get_event_loop().run_in_executor(
                None, connector.list_columns, table_name
            )

            if not columns:
                return f"No columns found for table '{table_name}'."

            result = f"Columns in '{table_name}' ({len(columns)}):\n\n"
            for col in columns:
                result += f"  - {col['name']} ({col.get('type', 'Unknown')})\n"

            return result

        except Exception as e:
            logger.error(f"Desktop list columns error: {e}")
            return f"Error listing columns: {str(e)}"

    async def _handle_desktop_list_measures(self) -> str:
        """List measures from connected Desktop model"""
        try:
            connector = self._get_desktop_connector()

            if not connector.current_port:
                return "Not connected to Power BI Desktop. Use 'desktop_connect' first."

            measures = await asyncio.get_event_loop().run_in_executor(
                None, connector.list_measures
            )

            if not measures:
                return "No measures found in the model."

            result = f"Measures ({len(measures)}):\n\n"
            for m in measures:
                result += f"  - {m['name']}\n"
                if m.get('expression'):
                    expr = m['expression'][:60] + "..." if len(m['expression']) > 60 else m['expression']
                    result += f"    = {expr}\n"

            return result

        except Exception as e:
            logger.error(f"Desktop list measures error: {e}")
            return f"Error listing measures: {str(e)}"

    async def _handle_desktop_execute_dax(self, args: Dict[str, Any]) -> str:
        """Execute a DAX query on the Desktop model (guarded: query-only, policy, PII, audit)."""
        connector = self._get_desktop_connector()
        dax_query = args.get("dax_query")
        if not connector.current_port:
            return "Not connected to Power BI Desktop. Use 'desktop_connect' first."
        if not dax_query:
            return "Error: dax_query is required"
        rows, report, error = await self._run_guarded_query(
            dax_query, tool="desktop_execute_dax", source="desktop",
            max_rows=self._row_limit(args.get("max_rows"), 100))
        if error:
            return error
        return self._render_query_result(rows, report)

    async def _handle_desktop_get_model_info(self) -> str:
        """Get comprehensive model info from Desktop"""
        try:
            connector = self._get_desktop_connector()

            if not connector.current_port:
                return "Not connected to Power BI Desktop. Use 'desktop_connect' first."

            result = f"=== Model Info: {connector.current_model_name or 'Unknown'} ===\n\n"

            # Tables
            tables = await asyncio.get_event_loop().run_in_executor(
                None, connector.list_tables
            )
            result += f"--- TABLES ({len(tables)}) ---\n"
            for t in tables:
                result += f"  - {t['name']}\n"
            result += "\n"

            # Measures
            measures = await asyncio.get_event_loop().run_in_executor(
                None, connector.list_measures
            )
            result += f"--- MEASURES ({len(measures)}) ---\n"
            for m in measures:
                result += f"  - {m['name']}\n"
            result += "\n"

            # Relationships
            rels = await asyncio.get_event_loop().run_in_executor(
                None, connector.list_relationships
            )
            result += f"--- RELATIONSHIPS ({len(rels)}) ---\n"
            for r in rels:
                result += f"  - {r}\n"

            return result

        except Exception as e:
            logger.error(f"Desktop get model info error: {e}")
            return f"Error getting model info: {str(e)}"

    # ==================== CLOUD HANDLERS ====================

    def _get_rest_connector(self) -> Optional[PowerBIRestConnector]:
        """Get or create REST connector"""
        if not self.tenant_id or not self.client_id or not self.client_secret:
            logger.warning("Cloud credentials not configured")
            return None

        if not self.rest_connector:
            self.rest_connector = PowerBIRestConnector(
                self.tenant_id, self.client_id, self.client_secret
            )
        return self.rest_connector

    def _get_xmla_connector(self, workspace_name: str, dataset_name: str) -> Optional[PowerBIXmlaConnector]:
        """Get or create XMLA connector for a specific workspace/dataset"""
        if not self.tenant_id or not self.client_id or not self.client_secret:
            logger.warning("Cloud credentials not configured")
            return None

        cache_key = f"{workspace_name}:{dataset_name}"

        if cache_key not in self.xmla_connector_cache:
            connector = PowerBIXmlaConnector(
                self.tenant_id, self.client_id, self.client_secret
            )
            if connector.connect(workspace_name, dataset_name):
                self.xmla_connector_cache[cache_key] = connector
            else:
                return None

        return self.xmla_connector_cache.get(cache_key)

    async def _handle_list_workspaces(self) -> str:
        """List Power BI Service workspaces"""
        try:
            connector = self._get_rest_connector()
            if not connector:
                return "Error: Cloud credentials not configured. Set TENANT_ID, CLIENT_ID, CLIENT_SECRET in .env"

            workspaces = await asyncio.get_event_loop().run_in_executor(
                None, connector.list_workspaces
            )

            if not workspaces:
                return "No workspaces found or authentication failed."

            result = f"Power BI Workspaces ({len(workspaces)}):\n\n"
            for ws in workspaces:
                result += f"  - {ws['name']}\n"
                result += f"    ID: {ws['id']}\n\n"

            return result

        except Exception as e:
            logger.error(f"List workspaces error: {e}")
            return f"Error listing workspaces: {str(e)}"

    async def _handle_list_datasets(self, args: Dict[str, Any]) -> str:
        """List datasets in a workspace"""
        try:
            connector = self._get_rest_connector()
            workspace_id = args.get("workspace_id")

            if not connector:
                return "Error: Cloud credentials not configured."

            if not workspace_id:
                return "Error: workspace_id is required"

            datasets = await asyncio.get_event_loop().run_in_executor(
                None, connector.list_datasets, workspace_id
            )

            if not datasets:
                return "No datasets found in this workspace."

            result = f"Datasets ({len(datasets)}):\n\n"
            for ds in datasets:
                result += f"  - {ds['name']}\n"
                result += f"    ID: {ds['id']}\n"
                result += f"    Configured by: {ds.get('configuredBy', 'Unknown')}\n\n"

            return result

        except Exception as e:
            logger.error(f"List datasets error: {e}")
            return f"Error listing datasets: {str(e)}"

    async def _handle_list_tables(self, args: Dict[str, Any]) -> str:
        """List tables in a Cloud dataset"""
        try:
            workspace_name = args.get("workspace_name")
            dataset_name = args.get("dataset_name")

            if not workspace_name or not dataset_name:
                return "Error: workspace_name and dataset_name are required"

            connector = await asyncio.get_event_loop().run_in_executor(
                None, self._get_xmla_connector, workspace_name, dataset_name
            )

            if not connector:
                return f"Error: Could not connect to dataset '{dataset_name}'"

            tables = await asyncio.get_event_loop().run_in_executor(
                None, connector.discover_tables
            )

            result = f"Tables in '{dataset_name}' ({len(tables)}):\n\n"
            for table in tables:
                result += f"  - {table['name']}\n"

            return result

        except Exception as e:
            logger.error(f"List tables error: {e}")
            return f"Error listing tables: {str(e)}"

    async def _handle_list_columns(self, args: Dict[str, Any]) -> str:
        """List columns for a table in Cloud dataset"""
        try:
            workspace_name = args.get("workspace_name")
            dataset_name = args.get("dataset_name")
            table_name = args.get("table_name")

            if not all([workspace_name, dataset_name, table_name]):
                return "Error: workspace_name, dataset_name, and table_name are required"

            connector = await asyncio.get_event_loop().run_in_executor(
                None, self._get_xmla_connector, workspace_name, dataset_name
            )

            if not connector:
                return f"Error: Could not connect to dataset '{dataset_name}'"

            schema = await asyncio.get_event_loop().run_in_executor(
                None, connector.get_table_schema, table_name
            )

            columns = schema.get("columns", [])
            result = f"Columns in '{table_name}' ({len(columns)}):\n\n"
            for col in columns:
                result += f"  - {col['name']} ({col.get('type', 'Unknown')})\n"

            return result

        except Exception as e:
            logger.error(f"List columns error: {e}")
            return f"Error listing columns: {str(e)}"

    async def _handle_execute_dax(self, args: Dict[str, Any]) -> str:
        """Execute a DAX query on a cloud dataset (guarded: query-only, policy, PII, audit)."""
        workspace_name = args.get("workspace_name")
        dataset_name = args.get("dataset_name")
        dax_query = args.get("dax_query")
        if not all([workspace_name, dataset_name, dax_query]):
            return "Error: workspace_name, dataset_name, and dax_query are required"
        rows, report, error = await self._run_guarded_query(
            dax_query, tool="execute_dax", source="cloud", workspace=workspace_name,
            dataset=dataset_name, max_rows=self._row_limit(args.get("max_rows"), 10000))
        if error:
            return error
        return self._render_query_result(rows, report)

    async def _handle_get_model_info(self, args: Dict[str, Any]) -> str:
        """Get model info from Cloud dataset using INFO.VIEW functions"""
        try:
            workspace_name = args.get("workspace_name")
            dataset_name = args.get("dataset_name")

            if not workspace_name or not dataset_name:
                return "Error: workspace_name and dataset_name are required"

            connector = await asyncio.get_event_loop().run_in_executor(
                None, self._get_xmla_connector, workspace_name, dataset_name
            )

            if not connector:
                return f"Error: Could not connect to dataset '{dataset_name}'"

            result = f"=== Semantic Model Info: {dataset_name} ===\n\n"

            # INFO.VIEW.TABLES
            try:
                tables = await asyncio.get_event_loop().run_in_executor(
                    None, connector.execute_dax, "EVALUATE INFO.VIEW.TABLES()"
                )
                result += f"--- TABLES ({len(tables)}) ---\n"
                for t in tables:
                    name = t.get("[Name]", t.get("Name", "Unknown"))
                    if not t.get("[IsHidden]", t.get("IsHidden", False)):
                        result += f"  - {name}\n"
                result += "\n"
            except Exception as e:
                result += f"--- TABLES ---\nError: {e}\n\n"

            # INFO.VIEW.MEASURES
            try:
                measures = await asyncio.get_event_loop().run_in_executor(
                    None, connector.execute_dax, "EVALUATE INFO.VIEW.MEASURES()"
                )
                result += f"--- MEASURES ({len(measures)}) ---\n"
                for m in measures:
                    name = m.get("[Name]", m.get("Name", "Unknown"))
                    result += f"  - {name}\n"
                result += "\n"
            except Exception as e:
                result += f"--- MEASURES ---\nError: {e}\n\n"

            # INFO.VIEW.RELATIONSHIPS
            try:
                rels = await asyncio.get_event_loop().run_in_executor(
                    None, connector.execute_dax, "EVALUATE INFO.VIEW.RELATIONSHIPS()"
                )
                result += f"--- RELATIONSHIPS ({len(rels)}) ---\n"
                for r in rels:
                    from_t = r.get("[FromTableName]", r.get("FromTableName", ""))
                    from_c = r.get("[FromColumnName]", r.get("FromColumnName", ""))
                    to_t = r.get("[ToTableName]", r.get("ToTableName", ""))
                    to_c = r.get("[ToColumnName]", r.get("ToColumnName", ""))
                    result += f"  - {from_t}[{from_c}] -> {to_t}[{to_c}]\n"
                result += "\n"
            except Exception as e:
                result += f"--- RELATIONSHIPS ---\nError: {e}\n\n"

            return result

        except Exception as e:
            logger.error(f"Get model info error: {e}")
            return f"Error getting model info: {str(e)}"

    # ==================== SECURITY HANDLERS ====================

    async def _handle_security_status(self) -> str:
        """Get security layer status"""
        try:
            status = self.security.get_status()
            policy_summary = self.security.get_policy_summary()
            enabled = status.get("enabled", {})
            policies = status.get("policies", {}) or {}
            audit = status.get("audit", {}) or {}
            on = lambda flag: "Enabled" if flag else "Disabled"  # noqa: E731

            result = "=== Power BI MCP Security Status ===\n\n"
            if status.get("locked"):
                result += ("LOCKED: the access policies could not be loaded, so every query is refused.\n"
                           f"  Reason: {status.get('config_error')}\n"
                           "  Fix the policy file (or unset POWERBI_MCP_POLICY_FILE) and restart the server.\n\n")
            result += "--- Features ---\n"
            result += f"  PII Detection:    {on(enabled.get('pii_detection'))}\n"
            result += f"  Audit Logging:    {on(enabled.get('audit_logging'))}\n"
            result += f"  Access Policies:  {on(enabled.get('access_policies'))}\n"
            result += f"  Read-only mode:   {on(self._read_only)}\n\n"

            if enabled.get("pii_detection"):
                pii = status.get("pii_detector", {})
                result += "--- PII Detection ---\n"
                result += f"  Strategy: {pii.get('strategy', 'N/A')}\n"
                result += f"  Types: {', '.join(pii.get('enabled_types', []))}\n\n"

            if enabled.get("access_policies"):
                result += "--- Access Policies ---\n"
                result += f"  {policies.get('message') or 'No status message.'}\n"
                result += f"  Max rows per query: {policy_summary.get('max_rows', 'N/A')}\n"
                result += f"  Tables with policies: {len(policy_summary.get('tables_with_policies', []))}\n"
                if not policies.get("hash_key_persistent"):
                    result += ("  Hash key: per session (set POWERBI_MCP_HASH_KEY for hashes that stay the "
                               "same across sessions)\n")
                result += ("  Note: policies work on query text and result names. They are defence in depth; "
                           "row-level and object-level security in the model is the real boundary.\n\n")

            if enabled.get("audit_logging"):
                result += "--- Audit Log ---\n"
                result += f"  Session ID: {audit.get('session_id', 'N/A')}\n"
                result += f"  Queries logged: {audit.get('query_count', 0)}\n"
                result += f"  Log file: {audit.get('log_file', 'N/A')}\n"
                result += (f"  Chain: {'HMAC-SHA256 (keyed)' if audit.get('keyed') else 'SHA-256, no key'}"
                           f"; query text stored: {audit.get('store_query_text', 'N/A')}\n")
                if audit.get("healthy") is False or audit.get("write_failures"):
                    result += (f"  WARNING: {audit.get('write_failures', 0)} audit write(s) failed. "
                               f"Last error: {audit.get('last_write_error')}\n")
                if audit.get("rotation_failures"):
                    result += f"  WARNING: log rotation failed: {audit.get('last_rotation_error')}\n"

            return result

        except Exception as e:
            logger.error(f"Security status error: {self._redact(str(e))}")
            return f"Error getting security status: {self._redact(str(e))}"

    async def _handle_security_audit_log(self, args: Dict[str, Any]) -> str:
        """View recent audit log entries"""
        try:
            count = args.get("count", 10)

            if not self.security.enable_audit or not self.security.audit_logger:
                return "Audit logging is not enabled."

            events = self.security.audit_logger.get_recent_events(count)

            if not events:
                return "No audit log entries found."

            result = f"=== Recent Audit Log ({len(events)} entries) ===\n\n"

            for event in events[-count:]:
                timestamp = event.get('timestamp', 'N/A')
                event_type = event.get('event_type', 'unknown')
                severity = event.get('severity', 'info')

                result += f"[{timestamp}] [{severity.upper()}] {event_type}\n"

                # Show details based on event type
                if event_type in ('query_success', 'query_failure'):
                    query_info = event.get('query', {})
                    result_info = event.get('result', {})
                    pii_info = event.get('pii', {})

                    result += f"  Query: {query_info.get('fingerprint', 'N/A')}\n"
                    result += f"  Rows: {result_info.get('row_count', 0)}, Duration: {(result_info.get('duration_ms') or 0):.0f}ms\n"

                    if pii_info.get('detected'):
                        result += f"  ⚠️ PII: {pii_info.get('count', 0)} instances\n"

                elif event_type == 'policy_violation':
                    details = event.get('details', {})
                    result += f"  Policy: {details.get('policy', 'N/A')}\n"
                    result += f"  Violation: {details.get('violation', 'N/A')}\n"

                result += "\n"

            return result

        except Exception as e:
            logger.error(f"Audit log error: {e}")
            return f"Error reading audit log: {str(e)}"

    # ==================== RLS HANDLERS ====================

    async def _handle_desktop_list_rls_roles(self) -> str:
        """List RLS roles in the Desktop model"""
        try:
            connector = self._get_desktop_connector()

            if not connector.current_port:
                return "Not connected to Power BI Desktop. Use 'desktop_connect' first."

            roles = await asyncio.get_event_loop().run_in_executor(
                None, connector.list_rls_roles
            )

            if not roles:
                return "No RLS roles found in this model.\n\nNote: RLS roles are defined in Power BI Desktop under 'Manage Roles' in the Modeling tab."

            result = f"=== RLS Roles ({len(roles)}) ===\n\n"
            for role in roles:
                result += f"  - {role['name']}"
                if role.get('description'):
                    result += f": {role['description']}"
                result += "\n"

            result += "\nUse 'desktop_set_rls_role' with a role name to test queries with that role's filters."
            return result

        except Exception as e:
            logger.error(f"List RLS roles error: {e}")
            return f"Error listing RLS roles: {str(e)}"

    async def _handle_desktop_set_rls_role(self, args: Dict[str, Any]) -> str:
        """Set or clear the active RLS role"""
        try:
            connector = self._get_desktop_connector()
            role_name = args.get("role_name", "").strip() or None

            if not connector.current_port:
                return "Not connected to Power BI Desktop. Use 'desktop_connect' first."

            set_role_fn = lambda: connector.set_rls_role(role_name)
            success = await asyncio.get_event_loop().run_in_executor(None, set_role_fn)

            if success:
                if role_name:
                    return f"RLS role '{role_name}' is now active.\n\nAll subsequent queries will be filtered by this role's DAX filters."
                else:
                    return "RLS role cleared.\n\nQueries now have full data access (no RLS filtering)."
            else:
                return f"Failed to set RLS role '{role_name}'.\n\nEnsure the role name is correct and exists in the model."

        except Exception as e:
            logger.error(f"Set RLS role error: {e}")
            return f"Error setting RLS role: {str(e)}"

    async def _handle_desktop_rls_status(self) -> str:
        """Get RLS status"""
        try:
            connector = self._get_desktop_connector()

            if not connector.current_port:
                return "Not connected to Power BI Desktop. Use 'desktop_connect' first."

            status = await asyncio.get_event_loop().run_in_executor(
                None, connector.get_rls_status
            )

            result = "=== RLS Status ===\n\n"
            result += f"Active: {'Yes' if status['rls_active'] else 'No'}\n"

            if status['current_role']:
                result += f"Current Role: {status['current_role']}\n"
            else:
                result += "Current Role: None (full data access)\n"

            result += f"\n--- Available Roles ({len(status['available_roles'])}) ---\n"
            if status['available_roles']:
                for role in status['available_roles']:
                    marker = " (active)" if role['name'] == status['current_role'] else ""
                    result += f"  - {role['name']}{marker}\n"
            else:
                result += "  No RLS roles defined in this model.\n"

            return result

        except Exception as e:
            logger.error(f"RLS status error: {e}")
            return f"Error getting RLS status: {str(e)}"

    # ==================== BATCH/WRITE OPERATION HANDLERS (TOM) ====================

    def _get_tom_connector(self) -> PowerBITOMConnector:
        """Get or create TOM connector instance"""
        if not self.tom_connector:
            self.tom_connector = PowerBITOMConnector()
        return self.tom_connector

    async def _ensure_tom_connected(self) -> Optional[str]:
        """Ensure TOM connector is connected, returns error message if not"""
        if not PowerBITOMConnector.is_available():
            details = getattr(PowerBITOMConnector, "availability_details", None)
            return ("TOM (Tabular Object Model) is not available. Write operations require "
                    "Microsoft.AnalysisServices.Tabular.dll." + (f"\n{details()}" if callable(details) else ""))

        desktop = self._get_desktop_connector()
        if not desktop.current_port:
            return "Not connected to Power BI Desktop. Use 'desktop_connect' first."

        tom = self._get_tom_connector()
        if not tom.model or tom.current_port != desktop.current_port:
            # Connect TOM to the same port as the desktop connector. The connector refuses to
            # switch instances while a transaction is open or edits are staged.
            connect_fn = lambda: tom.connect(desktop.current_port)  # noqa: E731
            success = await asyncio.get_event_loop().run_in_executor(None, connect_fn)
            if not success:
                reason = getattr(tom, "last_error", None)
                return ("Failed to connect TOM to Power BI Desktop"
                        + (f": {self._redact(reason)}" if reason else ". Write operations may not be supported."))

        return None

    async def _handle_batch_rename_tables(self, args: Dict[str, Any]) -> str:
        """Handle batch table rename"""
        try:
            error = await self._ensure_tom_connected()
            if error:
                return error

            renames = args.get("renames", [])
            auto_save = bool(args.get("auto_save", True)) and not self._tom_transaction_active

            if not renames:
                return "Error: 'renames' array is required"

            tom = self._get_tom_connector()

            # Execute batch rename
            batch_fn = lambda: tom.batch_rename_tables(renames, auto_save=auto_save)
            result = await asyncio.get_event_loop().run_in_executor(None, batch_fn)

            # Build response with deprecation warning
            response = "⚠️ DEPRECATED TOOL - Use 'pbip_rename_tables' instead!\n"
            response += "This TOM-based rename does NOT update report visuals.\n"
            response += "=" * 50 + "\n\n"
            response += f"{result.message}\n\n"

            if result.details:
                response += "--- Rename Results ---\n"
                for item in result.details.get("results", []):
                    status = "✅" if item.get("success") else "❌"
                    response += f"  {status} '{item.get('old_name')}' -> '{item.get('new_name')}'"
                    if item.get("error"):
                        response += f" ({item['error']})"
                    response += "\n"
                    # Show updated references per rename
                    if item.get("updated_measures"):
                        response += f"      Updated measures: {', '.join(item['updated_measures'][:5])}"
                        if len(item['updated_measures']) > 5:
                            response += f" (+{len(item['updated_measures'])-5} more)"
                        response += "\n"

                # Summary of all updated references
                if result.details.get("total_updated_measures", 0) > 0 or result.details.get("total_updated_calculated_columns", 0) > 0:
                    response += f"\n--- Model References Updated ---\n"
                    response += f"  Measures: {result.details.get('total_updated_measures', 0)}\n"
                    response += f"  Calculated columns: {result.details.get('total_updated_calculated_columns', 0)}\n"

                # Warning about visuals
                if result.details.get("warning"):
                    response += f"\n{result.details['warning']}\n"

                # PBIP/PBIR recommendation
                response += "\n💡 TIP: For bulk edits without breaking visuals, consider using PBIP (Power BI Project) format.\n"
                response += "   In Power BI Desktop: File > Save as > Power BI Project (.pbip)\n"
                response += "   PBIP stores model and report as text files, enabling safe find-and-replace across all references.\n"

            return response

        except Exception as e:
            logger.error(f"Batch rename tables error: {e}")
            return f"Error: {str(e)}"

    async def _handle_scan_table_dependencies(self, args: Dict[str, Any]) -> str:
        """Handle scan table dependencies"""
        try:
            error = await self._ensure_tom_connected()
            if error:
                return error

            table_name = args.get("table_name")
            if not table_name:
                return "Error: 'table_name' is required"

            tom = self._get_tom_connector()

            # Scan dependencies
            scan_fn = lambda: tom.scan_table_dependencies(table_name)
            result = await asyncio.get_event_loop().run_in_executor(None, scan_fn)

            if not result.success:
                return f"Error: {result.message}"

            details = result.details or {}
            response = f"=== Dependencies for Table '{table_name}' ===\n\n"
            response += f"Total references found: {details.get('total_references', 0)}\n\n"

            # Measures
            measures = details.get("measures", [])
            if measures:
                response += f"--- Measures ({len(measures)}) ---\n"
                for m in measures[:10]:  # Limit to first 10
                    response += f"  • {m['table']}[{m['name']}]\n"
                    if m.get('expression'):
                        expr_preview = m['expression'][:100] + "..." if len(m['expression']) > 100 else m['expression']
                        response += f"    = {expr_preview}\n"
                if len(measures) > 10:
                    response += f"  ... and {len(measures) - 10} more\n"
                response += "\n"

            # Calculated columns
            calc_cols = details.get("calculated_columns", [])
            if calc_cols:
                response += f"--- Calculated Columns ({len(calc_cols)}) ---\n"
                for c in calc_cols[:10]:
                    response += f"  • {c['table']}[{c['name']}]\n"
                if len(calc_cols) > 10:
                    response += f"  ... and {len(calc_cols) - 10} more\n"
                response += "\n"

            # Relationships
            rels = details.get("relationships", [])
            if rels:
                response += f"--- Relationships ({len(rels)}) ---\n"
                for r in rels:
                    response += f"  • {r['from_table']} -> {r['to_table']}\n"
                response += "\n"

            # Warning
            if details.get("warning"):
                response += f"\n{details['warning']}\n"

            if details.get('total_references', 0) == 0:
                response += "✅ No model-level dependencies found. However, report visuals may still reference this table.\n"

            response += "\n💡 For safe table renames, consider using PBIP (Power BI Project) format which allows text-based editing.\n"

            return response

        except Exception as e:
            logger.error(f"Scan table dependencies error: {e}")
            return f"Error: {str(e)}"

    async def _handle_batch_rename_columns(self, args: Dict[str, Any]) -> str:
        """Handle batch column rename"""
        try:
            error = await self._ensure_tom_connected()
            if error:
                return error

            renames = args.get("renames", [])
            auto_save = bool(args.get("auto_save", True)) and not self._tom_transaction_active

            if not renames:
                return "Error: 'renames' array is required"

            tom = self._get_tom_connector()

            # Execute batch rename
            batch_fn = lambda: tom.batch_rename_columns(renames, auto_save=auto_save)
            result = await asyncio.get_event_loop().run_in_executor(None, batch_fn)

            # Build response with deprecation warning
            response = "⚠️ DEPRECATED TOOL - Use 'pbip_rename_columns' instead!\n"
            response += "This TOM-based rename does NOT update report visuals.\n"
            response += "=" * 50 + "\n\n"
            response += f"{result.message}\n\n"

            if result.details:
                response += "--- Rename Results ---\n"
                for item in result.details.get("results", []):
                    status = "✅" if item.get("success") else "❌"
                    response += f"  {status} '{item.get('table_name')}'[{item.get('old_name')}] -> [{item.get('new_name')}]"
                    if item.get("error"):
                        response += f" ({item['error']})"
                    response += "\n"
                    # Show updated references
                    if item.get("updated_measures"):
                        response += f"      Updated measures: {', '.join(item['updated_measures'][:3])}"
                        if len(item['updated_measures']) > 3:
                            response += f" (+{len(item['updated_measures'])-3} more)"
                        response += "\n"

                # Summary
                if result.details.get("total_updated_measures", 0) > 0:
                    response += f"\n--- Model References Updated ---\n"
                    response += f"  Measures: {result.details.get('total_updated_measures', 0)}\n"
                    response += f"  Calculated columns: {result.details.get('total_updated_calculated_columns', 0)}\n"

            return response

        except Exception as e:
            logger.error(f"Batch rename columns error: {e}")
            return f"Error: {str(e)}"

    async def _handle_batch_rename_measures(self, args: Dict[str, Any]) -> str:
        """Handle batch measure rename"""
        try:
            error = await self._ensure_tom_connected()
            if error:
                return error

            renames = args.get("renames", [])
            auto_save = bool(args.get("auto_save", True)) and not self._tom_transaction_active

            if not renames:
                return "Error: 'renames' array is required"

            tom = self._get_tom_connector()

            # Execute batch rename
            batch_fn = lambda: tom.batch_rename_measures(renames, auto_save=auto_save)
            result = await asyncio.get_event_loop().run_in_executor(None, batch_fn)

            # Build response with deprecation warning
            response = "⚠️ DEPRECATED TOOL - Use 'pbip_rename_measures' instead!\n"
            response += "This TOM-based rename does NOT update report visuals.\n"
            response += "=" * 50 + "\n\n"
            response += f"{result.message}\n\n"

            if result.details:
                response += "--- Rename Results ---\n"
                for item in result.details.get("results", []):
                    status = "✅" if item.get("success") else "❌"
                    response += f"  {status} '{item.get('old_name')}' -> '{item.get('new_name')}'"
                    if item.get("error"):
                        response += f" ({item['error']})"
                    response += "\n"
                    # Show updated references
                    if item.get("updated_measures"):
                        response += f"      Updated other measures: {', '.join(item['updated_measures'][:3])}"
                        if len(item['updated_measures']) > 3:
                            response += f" (+{len(item['updated_measures'])-3} more)"
                        response += "\n"

                # Summary
                if result.details.get("total_updated_measures", 0) > 0:
                    response += f"\n--- Cross-References Updated ---\n"
                    response += f"  Other measures updated: {result.details.get('total_updated_measures', 0)}\n"

            return response

        except Exception as e:
            logger.error(f"Batch rename measures error: {e}")
            return f"Error: {str(e)}"

    async def _handle_batch_update_measures(self, args: Dict[str, Any]) -> str:
        """Handle batch measure expression update"""
        try:
            error = await self._ensure_tom_connected()
            if error:
                return error

            updates = args.get("updates", [])

            if not updates:
                return "Error: 'updates' array is required"

            # Validate-before-commit: probe every new expression first.
            if not bool(args.get("skip_validation", False)):
                invalid = []
                for u in updates:
                    expr = u.get("expression")
                    if expr:
                        status, verr = await self._validate_via_desktop(expr, as_measure=True)
                        if status is False:
                            invalid.append((u.get("measure_name", "?"), redact_secrets(verr, [self.client_secret])))
                if invalid:
                    detail = "\n".join(f"  - {n}: {e}" for n, e in invalid)
                    return (
                        f"[INVALID] No measures updated - {len(invalid)} expression(s) failed validation:\n"
                        f"{detail}\n\n(pass skip_validation=true to override)"
                    )

            # Inside an explicit transaction, defer the save until commit.
            auto_save = bool(args.get("auto_save", True)) and not self._tom_transaction_active

            tom = self._get_tom_connector()

            # Execute batch update
            batch_fn = lambda: tom.batch_update_measures(updates, auto_save=auto_save)
            result = await asyncio.get_event_loop().run_in_executor(None, batch_fn)

            # Build response
            response = f"=== Batch Update Measures ===\n\n{result.message}\n\n"

            if result.details:
                response += "--- Details ---\n"
                for item in result.details.get("results", []):
                    status = "[OK]" if item.get("success") else "[FAIL]"
                    response += f"  {status} '{item.get('measure_name')}'"
                    if item.get("error"):
                        response += f" ({item['error']})"
                    response += "\n"

            return response

        except Exception as e:
            logger.error(f"Batch update measures error: {e}")
            return f"Error: {str(e)}"

    async def _handle_create_measure(self, args: Dict[str, Any]) -> str:
        """Create one measure on the live model, validated first."""
        try:
            error = await self._ensure_tom_connected()
            if error:
                return error

            table_name = args.get("table_name")
            measure_name = args.get("measure_name")
            expression = args.get("expression")
            if not all([table_name, measure_name, expression]):
                return "Error: table_name, measure_name, and expression are required"

            # Validate-before-commit: compile the expression as a measure on its target table.
            if not bool(args.get("skip_validation", False)):
                status, verr = await self._validate_via_desktop(expression, as_measure=True, table=table_name)
                if status is False:
                    return (
                        f"[INVALID] Measure '{measure_name}' was NOT created - the expression failed validation.\n\n"
                        f"Error: {self._redact(verr)}\n\n"
                        "Fix the DAX and retry (or pass skip_validation=true to override)."
                    )

            tom = self._get_tom_connector()
            optional = {"format_string": args.get("format_string"), "description": args.get("description")}
            if args.get("display_folder"):
                optional["display_folder"] = args["display_folder"]
            create_fn = lambda: tom.create_measure(table_name, measure_name, expression, **optional)  # noqa: E731
            result = await asyncio.get_event_loop().run_in_executor(None, create_fn)
            if not result.success:
                return f"Failed to create measure: {result.message}"

            ok, note = await self._tom_save(tom)
            if not ok:
                return f"Measure '{measure_name}' was NOT created: the change could not be saved ({note})."
            if self._tom_transaction_active:
                self._tom_pending_measures.append(
                    {"table": table_name, "name": measure_name, "expression": expression})
                return (f"Measure '{measure_name}' created in table '{table_name}' (PENDING - "
                        f"run tom_commit_transaction to save).\n\nExpression: {expression}")
            return f"Measure '{measure_name}' created successfully in table '{table_name}'.\n\nExpression: {expression}"

        except Exception as e:
            logger.error(f"Create measure error: {self._redact(str(e))}")
            return f"Error: {self._redact(str(e))}"

    _SCAN_POLL_SECONDS = 15.0
    _SCAN_CHUNK = 100          # the Scanner API accepts at most 100 workspaces per getInfo call

    async def _handle_delete_measure(self, args: Dict[str, Any]) -> str:
        """Handle delete measure"""
        try:
            error = await self._ensure_tom_connected()
            if error:
                return error

            measure_name = args.get("measure_name")
            table_name = args.get("table_name")
            if not measure_name:
                return "Error: measure_name is required"

            tom = self._get_tom_connector()
            delete_fn = lambda: tom.delete_measure(measure_name, table_name)  # noqa: E731
            result = await asyncio.get_event_loop().run_in_executor(None, delete_fn)
            if not result.success:
                return f"Failed to delete measure: {result.message}"

            ok, note = await self._tom_save(tom)
            if not ok:
                return f"Measure '{measure_name}' was NOT deleted: the change could not be saved ({note})."
            self._tom_pending_measures[:] = [m for m in self._tom_pending_measures
                                             if not dax_tokens.names_equal(m["name"], measure_name)]
            return f"Measure '{measure_name}' deleted{note or ' successfully'}."

        except Exception as e:
            logger.error(f"Delete measure error: {self._redact(str(e))}")
            return f"Error: {self._redact(str(e))}"

    # ==================== RELATIONSHIPS (Bundle D) ====================

    async def _handle_create_relationship(self, args: Dict[str, Any]) -> str:
        """Create a relationship between two columns (TOM)."""
        try:
            error = await self._ensure_tom_connected()
            if error:
                return error
            ft, fc = args.get("from_table"), args.get("from_column")
            tt, tc = args.get("to_table"), args.get("to_column")
            if not all([ft, fc, tt, tc]):
                return "Error: from_table, from_column, to_table, and to_column are required"
            tom = self._get_tom_connector()
            fn = lambda: tom.create_relationship(  # noqa: E731
                ft, fc, tt, tc,
                cardinality=args.get("cardinality", "many_to_one"),
                cross_filter=args.get("cross_filter", "single"),
                is_active=args.get("is_active", True),
            )
            result = await asyncio.get_event_loop().run_in_executor(None, fn)
            if not result.success:
                return f"Failed to create relationship: {result.message}"
            ok, note = await self._tom_save(tom)
            if not ok:
                return f"Relationship was NOT created: the change could not be saved ({note})."
            return f"{result.message}{note}"
        except Exception as e:
            return f"Error creating relationship: {self._redact(str(e))}"

    async def _handle_delete_relationship(self, args: Dict[str, Any]) -> str:
        """Delete a relationship by name or by from/to table[/column] (TOM)."""
        try:
            error = await self._ensure_tom_connected()
            if error:
                return error
            tom = self._get_tom_connector()
            fn = lambda: tom.delete_relationship(  # noqa: E731
                from_table=args.get("from_table"), from_column=args.get("from_column"),
                to_table=args.get("to_table"), to_column=args.get("to_column"),
                name=args.get("name"),
            )
            result = await asyncio.get_event_loop().run_in_executor(None, fn)
            if not result.success:
                return f"Failed to delete relationship: {result.message}"
            ok, note = await self._tom_save(tom)
            if not ok:
                return f"Relationship was NOT deleted: the change could not be saved ({note})."
            return f"{result.message}{note}"
        except Exception as e:
            return f"Error deleting relationship: {self._redact(str(e))}"

    # ==================== DAX SAFETY LOOP & TRANSACTIONS (Bundle A) ====================

    async def _validate_via_desktop(self, dax: str, as_measure: bool = True, table: Optional[str] = None):
        """Validate a DAX expression against the connected Desktop model.

        Returns (status, error): status is True (valid), False (invalid or refused by policy),
        or None (could not validate, e.g. Desktop not connected, so the caller skips).
        The probe runs through the guarded query path, so validating an expression cannot be
        used to read a protected column out through an engine error message. Measures created
        earlier in an open transaction are defined in the probe, so an expression may refer to
        them before they are saved.
        """
        probe = build_validation_probe(dax, as_measure, table,
                                       extra_measures=self._tom_pending_measures if as_measure else None)
        _rows, _report, error = await self._run_guarded_query(probe, tool="validate", max_rows=1)
        if error is None:
            return True, None
        if error.startswith("Not connected"):
            return None, "Desktop not connected"
        return False, error[len("Error executing DAX: "):] if error.startswith("Error executing DAX: ") else error

    async def _handle_validate_dax(self, args: Dict[str, Any]):
        """Validate DAX (query or scalar measure expression) without committing.

        Returns (text, {valid, error, probe}) so agents get a typed result.
        """
        dax = args.get("dax")
        probe = build_validation_probe(dax or "", bool(args.get("as_measure", False)), args.get("table_name"))
        if not dax:
            return ("Error: dax is required", {"valid": False, "error": "dax is required", "probe": probe})
        source = (args.get("source") or "desktop").lower()
        workspace, dataset = args.get("workspace_name"), args.get("dataset_name")
        if source == "cloud" and not (workspace and dataset):
            msg = "Error: workspace_name and dataset_name are required for cloud validation"
            return (msg, {"valid": False, "error": msg, "probe": probe})
        _rows, _report, error = await self._run_guarded_query(
            probe, tool="validate_dax", source=source, workspace=workspace, dataset=dataset, max_rows=1)
        if error is None:
            return (
                f"[VALID] DAX validated successfully against the model.\n\nProbe executed:\n{probe}",
                {"valid": True, "error": None, "probe": probe},
            )
        if error.startswith(("Refused:", "Query blocked")):
            return (f"[BLOCKED] {error}", {"valid": False, "error": error, "probe": probe, "blocked": True})
        if error.startswith(("Not connected", "Error: could not connect")):
            return (error, {"valid": False, "error": error, "probe": probe})
        msg = error[len("Error executing DAX: "):] if error.startswith("Error executing DAX: ") else error
        return (
            f"[INVALID] DAX failed validation.\n\nError:\n{msg}\n\nProbe:\n{probe}",
            {"valid": False, "error": msg, "probe": probe},
        )

    @staticmethod
    def _row_get(row: Dict[str, Any], *names):
        """Read a field from an INFO.* result row tolerating bracketed/cased key variants."""
        for n in names:
            for k in (n, f"[{n}]", n.lower(), f"[{n.lower()}]", n.upper(), f"[{n.upper()}]"):
                if k in row:
                    return row[k]
        return None

    async def _handle_scan_measure_dependencies(self, args: Dict[str, Any]) -> str:
        """Dependency/impact analysis via INFO.CALCDEPENDENCY."""
        try:
            name = args.get("measure_name") or args.get("object_name")
            if not name:
                return "Error: measure_name is required"
            direction = (args.get("direction") or "both").lower()
            source = (args.get("source") or "desktop").lower()
            esc = str(name).replace('"', '""')
            table = args.get("table_name")
            etbl = str(table).replace('"', '""') if table else None
            loop = asyncio.get_event_loop()

            # Resolve an executor callable for the chosen source
            if source == "cloud":
                workspace = args.get("workspace_name")
                dataset = args.get("dataset_name")
                if not (workspace and dataset):
                    return "Error: workspace_name and dataset_name are required for cloud"
                connector = await loop.run_in_executor(
                    None, self._get_xmla_connector, workspace, dataset
                )
                if not connector:
                    return f"Error: could not connect to dataset '{dataset}'"
                run = lambda q: connector.execute_dax(q)
            else:
                desktop = self._get_desktop_connector()
                if not desktop.current_port:
                    return "Not connected to Power BI Desktop. Use 'desktop_connect' first."
                run = lambda q: desktop.execute_dax(q, 5000)

            response = f"=== Dependency analysis for '{name}' ===\n\n"

            if direction in ("upstream", "both"):
                ufilt = f'[OBJECT] = "{esc}"' + (f' && [TABLE] = "{etbl}"' if etbl else "")
                q = f'EVALUATE FILTER(INFO.CALCDEPENDENCY(), {ufilt})'
                rows = await loop.run_in_executor(None, run, q)
                response += f"--- Upstream (what '{name}' depends on): {len(rows)} ---\n"
                for r in rows[:50]:
                    rtype = self._row_get(r, "REFERENCED_OBJECT_TYPE") or "?"
                    rtable = self._row_get(r, "REFERENCED_TABLE") or ""
                    robj = self._row_get(r, "REFERENCED_OBJECT") or ""
                    response += f"  -> [{rtype}] {rtable}{('[' + robj + ']') if robj else ''}\n"
                response += "\n"

            if direction in ("downstream", "both"):
                dfilt = f'[REFERENCED_OBJECT] = "{esc}"' + (f' && [REFERENCED_TABLE] = "{etbl}"' if etbl else "")
                q = f'EVALUATE FILTER(INFO.CALCDEPENDENCY(), {dfilt})'
                rows = await loop.run_in_executor(None, run, q)
                response += f"--- Downstream (what depends on '{name}'): {len(rows)} ---\n"
                if not rows:
                    response += "  (none found - safe to change from a model-dependency standpoint;\n"
                    response += "   report visuals are not covered here - use pbip_scan_broken_refs for that)\n"
                for r in rows[:50]:
                    otype = self._row_get(r, "OBJECT_TYPE") or "?"
                    otable = self._row_get(r, "TABLE") or ""
                    oobj = self._row_get(r, "OBJECT") or ""
                    response += f"  <- [{otype}] {otable}{('[' + oobj + ']') if oobj else ''}\n"
                response += "\n"

            return response

        except Exception as e:
            msg = redact_secrets(str(e), [self.client_secret])
            return (
                f"Error scanning dependencies: {msg}\n\n"
                f"Note: {INFO_CALCDEP_NOTE} If unsupported, use scan_table_dependencies."
            )

    async def _handle_tom_begin_transaction(self) -> str:
        """Begin a TOM write transaction: live edits are held until commit."""
        error = await self._ensure_tom_connected()
        if error:
            return error
        if self._tom_transaction_active:
            return "A TOM transaction is already open. Commit or roll it back first."
        tom = self._get_tom_connector()
        result = await asyncio.get_event_loop().run_in_executor(None, tom.begin_transaction)
        if not getattr(result, "success", False):
            return f"Could not start a transaction: {self._redact(getattr(result, 'message', 'unknown error'))}"
        self._tom_pending_measures.clear()
        return (
            "TOM transaction started. Subsequent live model edits (measures, relationships, renames) "
            "are held until tom_commit_transaction, or discarded by tom_rollback_transaction."
        )

    async def _handle_tom_commit_transaction(self) -> str:
        """Commit pending TOM edits. A failed commit leaves the transaction open."""
        if not self._tom_transaction_active:
            return "No TOM transaction is open."
        tom = self._get_tom_connector()
        result = await asyncio.get_event_loop().run_in_executor(None, tom.commit_transaction)
        message = self._redact(getattr(result, "message", "") or "")
        if getattr(result, "success", False):
            self._tom_pending_measures.clear()
            return f"Transaction committed. {message}".strip()
        return (f"Commit failed: {message or 'unknown error'}\n\n"
                "Nothing was saved and the transaction is still open. Fix the problem and run "
                "tom_commit_transaction again, or run tom_rollback_transaction to discard the pending edits.")

    async def _handle_tom_rollback_transaction(self) -> str:
        """Roll back pending TOM edits."""
        if not self._tom_transaction_active:
            return "No TOM transaction is open."
        tom = self._get_tom_connector()
        result = await asyncio.get_event_loop().run_in_executor(None, tom.rollback_transaction)
        message = self._redact(getattr(result, "message", "") or "")
        if getattr(result, "success", False):
            self._tom_pending_measures.clear()
            return f"Transaction rolled back. {message}".strip()
        return f"Rollback failed: {message or 'unknown error'}. The transaction is still open."

    # ==================== MODEL ANALYSIS (Bundle B) ====================

    async def _get_query_runner(self, source: str, workspace=None, dataset=None):
        """Return (run, error): a synchronous DAX executor for the chosen source.

        run(query_str) -> list[dict]. Used by analysis tools that issue INFO/DMV/DAX queries.
        """
        loop = asyncio.get_event_loop()
        if (source or "desktop").lower() == "cloud":
            if not (workspace and dataset):
                return None, "workspace_name and dataset_name are required for cloud"
            connector = await loop.run_in_executor(None, self._get_xmla_connector, workspace, dataset)
            if not connector:
                return None, f"could not connect to dataset '{dataset}'"
            return (lambda q: connector.execute_dax(q)), None
        desktop = self._get_desktop_connector()
        if not desktop.current_port:
            return None, "Not connected to Power BI Desktop. Use 'desktop_connect' first."
        return (lambda q: desktop.execute_dax(q, 100000)), None

    _METADATA_TTL_SECONDS = 20.0

    async def _gather_model_metadata(self, source: str, workspace=None, dataset=None):
        """Normalized model dict (tables, columns, measures, relationships). Returns (model, error).

        Results are reused for a few seconds per connection: completion fires on every
        keystroke and analysis tools are usually called back to back, and each gather costs
        four metadata queries. The cache is dropped whenever a non-read-only tool runs, and
        callers get their own copy so one tool cannot alter what the next one sees.
        """
        import copy
        desktop = self.desktop_connector
        key = ((source or "desktop").lower(), workspace, dataset,
               getattr(desktop, "current_port", None), getattr(desktop, "current_rls_role", None))
        hit = self._metadata_cache.get(key)
        if hit and time.time() - hit[0] < self._METADATA_TTL_SECONDS:
            return copy.deepcopy(hit[1]), None
        model, err = await self._gather_model_metadata_uncached(source, workspace, dataset)
        if not err and model is not None:
            self._metadata_cache[key] = (time.time(), model)
            return copy.deepcopy(model), None
        return model, err

    async def _gather_model_metadata_uncached(self, source: str, workspace=None, dataset=None):
        """Read the model through INFO.VIEW.* DAX. Returns (model_dict, error)."""
        run, err = await self._get_query_runner(source, workspace, dataset)
        if err:
            return None, err
        loop = asyncio.get_event_loop()
        g = self._row_get

        async def q(query):
            return await loop.run_in_executor(None, run, query)

        try:
            tables_rows = await q("EVALUATE INFO.VIEW.TABLES()")
            cols_rows = await q("EVALUATE INFO.VIEW.COLUMNS()")
            meas_rows = await q("EVALUATE INFO.VIEW.MEASURES()")
        except Exception as e:
            return None, (f"could not read model metadata via INFO.VIEW: {e}. "
                          "INFO.VIEW.* requires a recent Analysis Services engine.")
        try:
            rel_rows = await q("EVALUATE INFO.VIEW.RELATIONSHIPS()")
        except Exception:
            rel_rows = []

        tmap: Dict[str, Any] = {}
        for r in tables_rows:
            nm = g(r, "Name")
            if nm is None:
                continue
            tmap[nm] = {"name": nm, "is_hidden": g(r, "IsHidden"),
                        "description": g(r, "Description") or "", "data_category": g(r, "DataCategory"),
                        "columns": [], "measures": []}
        for r in cols_rows:
            tn = g(r, "Table")
            tmap.setdefault(tn, {"name": tn, "is_hidden": False, "description": "", "columns": [], "measures": []})
            ctype = str(g(r, "ColumnType") or "").lower()
            tmap[tn]["columns"].append({
                "name": g(r, "Name"), "table": tn, "data_type": g(r, "DataType"),
                "is_hidden": g(r, "IsHidden"), "is_key": g(r, "IsKey"),
                "summarize_by": g(r, "SummarizeBy"), "sort_by": g(r, "SortByColumn"),
                "description": g(r, "Description") or "", "display_folder": g(r, "DisplayFolder"),
                "data_category": g(r, "DataCategory"), "is_calculated": ctype == "calculated",
                "expression": g(r, "Expression"), "format_string": g(r, "FormatString"),
            })
        for r in meas_rows:
            tn = g(r, "Table")
            tmap.setdefault(tn, {"name": tn, "is_hidden": False, "description": "", "columns": [], "measures": []})
            tmap[tn]["measures"].append({
                "name": g(r, "Name"), "table": tn, "expression": g(r, "Expression"),
                "format_string": g(r, "FormatString"), "description": g(r, "Description") or "",
                "display_folder": g(r, "DisplayFolder"), "is_hidden": g(r, "IsHidden"),
                "data_type": g(r, "DataType"),
            })
        rels = []
        for r in rel_rows:
            rels.append({
                "from_table": g(r, "FromTable"), "from_column": g(r, "FromColumn"),
                "to_table": g(r, "ToTable"), "to_column": g(r, "ToColumn"),
                "is_active": g(r, "IsActive"),
                "cross_filter": g(r, "CrossFilteringBehavior", "CrossFilterDirection"),
                "from_cardinality": g(r, "FromCardinality"), "to_cardinality": g(r, "ToCardinality"),
            })
        return {"tables": list(tmap.values()), "relationships": rels}, None

    async def _handle_run_bpa(self, args: Dict[str, Any]):
        """Run the Best Practice Analyzer over the connected model. Returns (text, result)."""
        try:
            model, err = await self._gather_model_metadata(
                args.get("source") or "desktop", args.get("workspace_name"), args.get("dataset_name")
            )
            if err:
                return (f"Error: {err}", {"error": err, "summary": {"total": 0}, "findings": []})
            result = model_analysis.run_bpa(
                model, categories=args.get("categories"),
                min_severity=(args.get("min_severity") or "info"),
            )
            s = result["summary"]
            out = "=== Best Practice Analyzer ===\n\n"
            out += f"Findings: {s['total']}  (errors: {s['by_severity'].get('error', 0)}, "
            out += f"warnings: {s['by_severity'].get('warning', 0)}, info: {s['by_severity'].get('info', 0)})\n"
            if s["by_category"]:
                out += "By category: " + ", ".join(f"{k}={v}" for k, v in s["by_category"].items()) + "\n"
            out += "\n"
            current_rule = None
            for f in result["findings"][:200]:
                if f["rule_id"] != current_rule:
                    current_rule = f["rule_id"]
                    out += f"--- [{f['severity'].upper()}] {f['name']} ({f['category']}) ---\n"
                out += f"  - {f['object']}: {f['detail']}\n"
            if s["total"] > 200:
                out += f"\n... and {s['total'] - 200} more findings (filter by category or min_severity).\n"
            if s["total"] == 0:
                out += "No issues found for the selected rules.\n"
            return (out, result)
        except Exception as e:
            msg = redact_secrets(str(e), [self.client_secret])
            return (f"Error running BPA: {msg}", {"error": msg, "summary": {"total": 0}, "findings": []})

    async def _handle_audit_ai_readiness(self, args: Dict[str, Any]):
        """Score how AI-ready (Copilot/agent-ready) the connected model is. Returns (text, result)."""
        try:
            model, err = await self._gather_model_metadata(
                args.get("source") or "desktop", args.get("workspace_name"), args.get("dataset_name")
            )
            if err:
                return (f"Error: {err}", {"error": err, "score": 0})
            r = model_analysis.audit_ai_readiness(model)
            m = r["metrics"]
            out = "=== AI-Readiness Audit ===\n\n"
            out += f"Score: {r['score']}/100  (Grade {r['grade']})\n\n"
            out += "--- Metrics ---\n"
            out += f"  Measures with descriptions: {m['measures_with_description_pct']}% of {m['measures_total']}\n"
            out += f"  Measures with format string: {m['measures_with_format_pct']}%\n"
            out += f"  Visible columns with descriptions: {m['columns_with_description_pct']}% of {m['visible_columns_total']}\n"
            out += f"  Tables with descriptions: {m['tables_with_description_pct']}% of {m['tables_total']}\n\n"
            out += "--- Recommendations ---\n"
            for rec in r["recommendations"]:
                out += f"  - {rec}\n"
            return (out, r)
        except Exception as e:
            msg = redact_secrets(str(e), [self.client_secret])
            return (f"Error auditing AI-readiness: {msg}", {"error": msg, "score": 0})

    def _measures_from_model(self, model: Dict[str, Any], measure_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """Flatten {name, expression} for every measure in a gathered model, optionally filtered."""
        out: List[Dict[str, Any]] = []
        for t in model.get("tables", []):
            for m in t.get("measures", []):
                if measure_name and m.get("name") != measure_name:
                    continue
                out.append({"name": m.get("name"), "expression": m.get("expression") or ""})
        return out

    async def _handle_dax_lint(self, args: Dict[str, Any]):
        """Static DAX anti-pattern linter (raw expression, one measure, or whole model).
        Returns (text, result)."""
        try:
            min_rank = dax_lint.SEVERITY_RANK.get((args.get("min_severity") or "info").lower(), 1)
            expr = args.get("expression")
            if expr:
                measures = [{"name": args.get("name") or "(expression)", "expression": expr}]
            else:
                model, err = await self._gather_model_metadata(
                    args.get("source") or "desktop", args.get("workspace_name"), args.get("dataset_name"))
                if err:
                    return (f"Error: {err}", {"error": err, "summary": {"total": 0}, "findings": []})
                measures = self._measures_from_model(model, args.get("measure_name"))
                if args.get("measure_name") and not measures:
                    return (f"Measure '{args.get('measure_name')}' not found in the model.",
                            {"error": "measure_not_found", "summary": {"total": 0}, "findings": []})
            result = dax_lint.lint_measures(measures, min_severity=(args.get("min_severity") or "info").lower())
            result["findings"] = [f for f in result["findings"]
                                  if dax_lint.SEVERITY_RANK.get(f["severity"], 0) >= min_rank]
            s = result["summary"]
            out = "=== DAX Lint ===\n\n"
            out += f"Scanned {s['measures_scanned']} expression(s); {len(result['findings'])} finding(s)"
            if s.get("by_severity"):
                out += "  (" + ", ".join(f"{k}: {v}" for k, v in s["by_severity"].items()) + ")"
            out += "\n\n"
            cur = None
            for f in result["findings"][:200]:
                if f.get("object") != cur:
                    cur = f.get("object")
                    out += f"--- {cur} ---\n"
                out += f"  [{f['severity'].upper()}] {f['rule_id']} (line {f['line']}): {f['message']}\n"
                out += f"      fix: {f['suggestion']}\n"
            if not result["findings"]:
                out += "No issues found.\n"
            return (out, result)
        except Exception as e:
            msg = redact_secrets(str(e), [self.client_secret])
            return (f"Error linting DAX: {msg}", {"error": msg, "summary": {"total": 0}, "findings": []})

    async def _handle_dax_suggest_rewrite(self, args: Dict[str, Any]):
        """Concrete before/after rewrite hints for auto-fixable DAX anti-patterns.
        Returns (text, result)."""
        try:
            expr = args.get("expression")
            rewrites: List[Dict[str, Any]] = []
            if expr:
                rewrites = dax_lint.suggest_rewrites(args.get("name") or "(expression)", expr)
            else:
                model, err = await self._gather_model_metadata(
                    args.get("source") or "desktop", args.get("workspace_name"), args.get("dataset_name"))
                if err:
                    return (f"Error: {err}", {"error": err, "rewrites": []})
                for m in self._measures_from_model(model, args.get("measure_name")):
                    for h in dax_lint.suggest_rewrites(m["name"], m["expression"]):
                        h["object"] = m["name"]
                        rewrites.append(h)
            out = "=== DAX Rewrite Suggestions ===\n\n"
            if not rewrites:
                out += "No auto-fixable anti-patterns detected.\n"
            for h in rewrites[:100]:
                label = (h.get("object") + ": ") if h.get("object") else ""
                kind = {"rewrite": "rewrite", "expression": "rewritten expression",
                        "template": "template only"}.get(h.get("kind"), h.get("kind") or "hint")
                out += f"--- {label}{h['rule_id']} (line {h['line']}) [{kind}] ---\n"
                out += f"  before: {h['before']}\n  after:  {h['after']}\n  note:   {h['note']}\n\n"
            return (out, {"rewrites": rewrites})
        except Exception as e:
            msg = redact_secrets(str(e), [self.client_secret])
            return (f"Error suggesting rewrites: {msg}", {"error": msg, "rewrites": []})

    _SVG_PARAMS = {
        "progress": ["value_measure", "max_value", "min_value", "fill", "track", "width", "height"],
        "bullet": ["value_measure", "target_measure", "max_value", "fill", "target", "track", "width", "height"],
        "status_pill": ["value_measure", "thresholds", "width", "height"],
        "sparkline": ["axis_column", "value_measure", "sort_column", "stroke", "width", "height"],
    }

    async def _handle_generate_svg_measure(self, args: Dict[str, Any]):
        """Generate an SVG micro-visual DAX measure. Returns (text, result)."""
        try:
            kind = (args.get("kind") or "").lower()
            kind = "status_pill" if kind in ("status", "pill") else kind
            allowed = self._SVG_PARAMS.get(kind)
            if not allowed:
                return (f"Error: unknown kind '{args.get('kind')}'. Use progress|bullet|status_pill|sparkline.",
                        {"error": "unknown_kind"})
            kwargs = {k: args[k] for k in allowed if args.get(k) is not None}
            result = svg_measures.generate(kind, name=args.get("name"), **kwargs)
            out = f"=== SVG measure: {result['name']} ({result['kind']}) ===\n\n{result['dax']}\n\n{result['notes']}\n"
            return (out, result)
        except TypeError as e:
            return (f"Error: missing/invalid parameter for this kind: {e}", {"error": str(e)})
        except Exception as e:
            msg = redact_secrets(str(e), [self.client_secret])
            return (f"Error generating SVG measure: {msg}", {"error": msg})

    async def _handle_audit_naming(self, args: Dict[str, Any]):
        """Audit naming conventions and return a rename plan. Returns (text, result)."""
        try:
            model, err = await self._gather_model_metadata(
                args.get("source") or "desktop", args.get("workspace_name"), args.get("dataset_name"))
            if err:
                return (f"Error: {err}", {"error": err, "summary": {"total_suggestions": 0}, "plan": []})
            options = {k: args[k] for k in ("scope", "target_case", "strip_warehouse_prefixes",
                                            "expand_abbreviations", "acronyms", "replace_acronyms")
                       if k in args and args[k] is not None}
            result = naming_audit.audit(model, options)
            s = result["summary"]
            out = "=== Naming Audit ===\n\n"
            out += f"Suggestions: {s['total_suggestions']}"
            if s.get("by_type"):
                out += "  (" + ", ".join(f"{k}: {v}" for k, v in s["by_type"].items()) + ")"
            out += f"\nDominant style: {s.get('dominant_style')}  (single style: {s.get('single_style')})\n\n"
            unsafe = [p for p in result["plan"] if p.get("safe") is False]
            for p in result["plan"][:200]:
                loc = f"{p['table']}[{p['old']}]" if p.get("table") else p["old"]
                mark = "  [DO NOT APPLY AS IS]" if p.get("safe") is False else ""
                out += f"  {p['object_type']}: {loc}  ->  '{p['new']}'  ({', '.join(p['reasons'])}){mark}\n"
            if not result["plan"]:
                out += "No naming issues found.\n"
            else:
                if unsafe:
                    out += (f"\n{len(unsafe)} suggestion(s) collide with another name and are marked; "
                            "choose different names for those before applying.\n")
                out += ("\nApply the safe entries with pbip_rename_tables / pbip_rename_columns / "
                        "pbip_rename_measures, which update the model and the report together.\n")
            return (out, result)
        except Exception as e:
            msg = redact_secrets(str(e), [self.client_secret])
            return (f"Error auditing naming: {msg}", {"error": msg, "summary": {"total_suggestions": 0}, "plan": []})

    async def _handle_pbix_inspect(self, args: Dict[str, Any]):
        """Inspect a .pbix package. Returns (text, result)."""
        try:
            path = args.get("path")
            if not path:
                return ("Error: 'path' is required.", {"error": "path_required"})
            result = pbix_tools.inspect(path)
            out = f"=== PBIX: {result['path']} ===\n\n"
            out += f"Type: {result['type']}\nReport format: {result['report_format']}\n"
            out += f"Pages: {result['page_count']}\nEntries: {result['entry_count']}\n\n"
            out += "Largest entries:\n"
            for e in result["entries"][:15]:
                out += f"  {e['size']:>12,}  {e['name']}\n"
            return (out, result)
        except Exception as e:
            msg = redact_secrets(str(e), [self.client_secret])
            return (f"Error inspecting PBIX: {msg}", {"error": msg})

    async def _handle_pbix_extract(self, args: Dict[str, Any]):
        """Extract a .pbix package (Zip-Slip protected). Returns (text, result)."""
        try:
            path, dest = args.get("path"), args.get("dest")
            if not path or not dest:
                return ("Error: 'path' and 'dest' are required.", {"error": "path_and_dest_required"})
            options = {"decode_layout": args.get("decode_layout", True)}
            if args.get("max_bytes"):
                options["max_bytes"] = int(args["max_bytes"])
            result = pbix_tools.extract(path, dest, **options)
            out = f"=== Extracted {result['file_count']} file(s) to {result['dest']} ===\n"
            if result.get("layout_decoded"):
                out += "Decoded legacy layout to Report/Layout.json\n"
            out += "\n" + "\n".join(f"  {f}" for f in result["files"][:50])
            if result["file_count"] > 50:
                out += f"\n  ... and {result['file_count'] - 50} more"
            return (out, result)
        except Exception as e:
            msg = self._redact(str(e))
            return (f"Error extracting PBIX: {msg}", {"error": msg})

    async def _handle_bpa_validate_rules(self, args: Dict[str, Any]):
        """Validate a custom BPA rules JSON. Returns (text, result)."""
        try:
            rules = args.get("rules")
            path = args.get("rules_path")
            if rules is None and path:
                with open(path, "r", encoding="utf-8-sig") as f:
                    rules = f.read()
            if rules is None:
                return ("Error: provide 'rules' (JSON) or 'rules_path'.", {"error": "rules_required"})
            result = bpa_authoring.validate_rules(rules, fix=bool(args.get("fix")))
            out = "=== BPA Rules Validation ===\n\n"
            out += f"Rules: {result['rule_count']}  |  Valid: {result['valid']}  |  "
            out += f"Errors: {len(result['errors'])}  Warnings: {len(result['warnings'])}\n\n"
            for e in result["errors"][:100]:
                out += f"  [ERROR] {e.get('rule_id') or '(rule #' + str(e.get('index')) + ')'}: {e['message']}\n"
            for w in result["warnings"][:100]:
                out += f"  [WARN]  {w.get('rule_id') or '(rule #' + str(w.get('index')) + ')'}: {w['message']}\n"
            if result["valid"] and not result["warnings"]:
                out += "All rules conform.\n"
            return (out, result)
        except Exception as e:
            msg = redact_secrets(str(e), [self.client_secret])
            return (f"Error validating BPA rules: {msg}", {"error": msg, "valid": False})

    @staticmethod
    def _bpa_local_rule_paths() -> Dict[str, str]:
        paths: Dict[str, str] = {}
        la = os.environ.get("LOCALAPPDATA")
        pd = os.environ.get("PROGRAMDATA")
        if la:
            paths["user (TE2)"] = os.path.join(la, "TabularEditor", "BPARules.json")
            paths["user (TE3)"] = os.path.join(la, "TabularEditor3", "BPARules.json")
        if pd:
            paths["machine"] = os.path.join(pd, "TabularEditor", "BPARules.json")
        return paths

    async def _handle_bpa_audit_rule_sources(self, args: Dict[str, Any]):
        """Audit where BPA rules live for the loaded project. Returns (text, result)."""
        try:
            model_text = args.get("model_text")
            if not model_text:
                connector = self._get_pbip_connector()
                if not connector.current_project:
                    return ("No PBIP project loaded and no 'model_text' provided. Load a project first.",
                            {"error": "no_project"})
                parts = []
                for f in connector.current_project.tmdl_files:
                    try:
                        parts.append(connector._read_text(f))
                    except Exception:
                        continue
                model_text = "\n".join(parts)
            local: Dict[str, str] = {}
            for label, p in self._bpa_local_rule_paths().items():
                try:
                    with open(p, "r", encoding="utf-8-sig") as fh:
                        local[label] = fh.read()
                except Exception:
                    continue
            result = bpa_authoring.audit_rule_sources(model_text, local_rule_files=local or None)
            out = "=== BPA Rule Sources ===\n\n"
            out += f"Embedded in model: {result['embedded_rule_count']} rule(s)\n"
            out += f"External rule files: {len(result['external_rule_files'])}\n"
            for u in result["external_rule_files"][:20]:
                out += f"  - {u}\n"
            out += f"Ignored rule IDs: {len(result['ignored_rule_ids'])}"
            if result["ignored_rule_ids"]:
                out += " (" + ", ".join(result["ignored_rule_ids"][:20]) + ")"
            out += "\n"
            for lf in result.get("local_rule_files", []):
                out += f"  local {lf.get('source')}: {lf.get('rule_count', lf.get('error'))}\n"
            return (out, result)
        except Exception as e:
            msg = redact_secrets(str(e), [self.client_secret])
            return (f"Error auditing BPA rule sources: {msg}", {"error": msg})

    # ==================== Wave 5: data modelling / warehousing / bulk DAX ====================

    @staticmethod
    def _render_measure_list(measures: List[Dict[str, Any]]) -> str:
        out = ""
        for m in measures:
            out += f"--- {m['name']} ---\n"
            if m.get("format_string"):
                out += f"  format: {m['format_string']}\n"
            if m.get("display_folder"):
                out += f"  folder: {m['display_folder']}\n"
            out += f"  {m['expression']}\n\n".replace("\n", "\n  ").rstrip() + "\n\n"
        return out

    def _suite_kwargs(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Map tool args onto dax_generator.generate_suite kwargs by kind."""
        kind = (args.get("kind") or "").lower()
        if kind in ("time_intelligence", "time", "ti"):
            kw = {"base_measure": args.get("base_measure"), "date_column": args.get("date_column"),
                  "variants": args.get("variants"), "display_folder": args.get("display_folder"),
                  "base_format": args.get("base_format"), "fiscal_year_end": args.get("fiscal_year_end")}
            if not kw["base_measure"] or not kw["date_column"]:
                raise ValueError("time_intelligence needs base_measure and date_column")
        elif kind in ("ratios", "ratio", "share"):
            kw = {"base_measure": args.get("base_measure"),
                  "dimension_columns": args.get("dimension_columns"),
                  "display_folder": args.get("display_folder"),
                  "include_all_selected": args.get("include_all_selected"),
                  "sort_by_columns": args.get("sort_by_columns")}
            if not kw["base_measure"] or not kw["dimension_columns"]:
                raise ValueError("ratios needs base_measure and dimension_columns")
        elif kind in ("ranking", "rank"):
            kw = {"base_measure": args.get("base_measure"),
                  "dimension_columns": args.get("dimension_columns"),
                  "display_folder": args.get("display_folder"),
                  "include_all_selected": args.get("include_all_selected"),
                  "sort_by_columns": args.get("sort_by_columns")}
            if not kw["base_measure"] or not kw["dimension_columns"]:
                raise ValueError("ranking needs base_measure and dimension_columns")
        elif kind in ("column_stats", "stats"):
            kw = {"column": args.get("column"), "stats": args.get("stats"),
                  "display_folder": args.get("display_folder"), "column_format": args.get("column_format")}
            if not kw["column"]:
                raise ValueError("column_stats needs column")
        else:
            raise ValueError(f"Unknown kind '{args.get('kind')}'")
        return {k: v for k, v in kw.items() if v is not None}

    async def _batch_create_live(self, table: str, measures: List[Dict[str, Any]],
                                 skip_validation: bool = False):
        """Shared live bulk-create path. Returns (created, message).

        The whole batch is validated against the live model first, sibling references included
        (see _validate_measure_batch), and only then handed to the connector's all-or-nothing
        batch create. Inside an open transaction the measures stay pending until commit."""
        error = await self._ensure_tom_connected()
        if error:
            return False, error
        if not skip_validation:
            problem = await self._validate_measure_batch(table, measures)
            if problem:
                return False, problem

        tom = self._get_tom_connector()
        in_transaction = self._tom_transaction_active
        create_fn = lambda: tom.batch_create_measures(table, measures, auto_save=not in_transaction)  # noqa: E731
        result = await asyncio.get_event_loop().run_in_executor(None, create_fn)
        if not result.success:
            return False, f"Failed: {result.message}"
        if in_transaction:
            self._tom_pending_measures.extend(
                {"table": table, "name": m["name"], "expression": m.get("expression") or ""} for m in measures)
            return True, f"{result.message} (PENDING - run tom_commit_transaction to save)"
        return True, result.message

    async def _handle_generate_measure_suite(self, args: Dict[str, Any]):
        """Bulk-generate a measure suite; optionally write it (pbip TMDL or live TOM).
        Returns (text, result)."""
        try:
            target = (args.get("target") or "none").lower()
            if target not in ("none", "pbip", "live"):
                return (f"Error: unknown target '{target}'.", {"error": "bad_target", "measures": []})
            if target != "none" and self._read_only:
                return ("Refused: writing a measure suite is a write operation and the server is in "
                        "READ-ONLY mode. Use target='none' to generate without writing.",
                        {"error": "read_only", "measures": []})
            suite_kwargs = await self._suite_model_defaults(self._suite_kwargs(args))
            measures = dax_generator.generate_suite(args.get("kind"), **suite_kwargs)
            result = {"measures": measures, "written": False, "target": target}
            out = f"=== Measure suite: {args.get('kind')} ({len(measures)} measures) ===\n\n"
            out += self._render_measure_list(measures)

            if target == "none":
                out += "Not written (target='none'). Write with target='pbip' (offline TMDL) or 'live' (TOM)."
                return (out, result)
            table = args.get("table")
            if not table:
                return ("Error: 'table' is required for target pbip/live.", {"error": "table_required", "measures": measures})
            if target == "pbip":
                connector = self._get_pbip_connector()
                write_fn = lambda: connector.add_measures(table, measures)
                res = await asyncio.get_event_loop().run_in_executor(None, write_fn)
                if not res.get("success"):
                    return (f"Generated but NOT written: {res.get('message')}", {"error": res.get("message"), "measures": measures})
                result["written"] = True
                out += f"Written into TMDL: {res.get('path')} ({len(res.get('created', []))} measures).\n"
                out += self._RELOAD_HINT
                return (out, result)
            # live
            created, msg = await self._batch_create_live(table, measures, bool(args.get("skip_validation", False)))
            result["written"] = created
            if not created:
                result["error"] = msg
            return (out + msg, result)
        except ValueError as e:
            return (f"Error: {e}", {"error": str(e), "measures": []})
        except Exception as e:
            msg = redact_secrets(str(e), [self.client_secret])
            return (f"Error generating measure suite: {msg}", {"error": msg, "measures": []})

    async def _handle_batch_create_measures(self, args: Dict[str, Any]) -> str:
        """All-or-nothing live bulk measure creation with validate-before-commit."""
        try:
            table = args.get("table_name")
            measures = args.get("measures") or []
            if not table or not measures:
                return "Error: table_name and a non-empty measures array are required"
            bad = [m for m in measures if not (isinstance(m, dict) and m.get("name") and m.get("expression"))]
            if bad:
                return "Error: every measure needs both 'name' and 'expression'"
            _created, message = await self._batch_create_live(table, measures, bool(args.get("skip_validation", False)))
            return message
        except Exception as e:
            return f"Error creating measures: {self._redact(str(e))}"

    async def _handle_audit_star_schema(self, args: Dict[str, Any]):
        """Dimensional-model audit. Returns (text, result)."""
        try:
            model, err = await self._gather_model_metadata(
                args.get("source") or "desktop", args.get("workspace_name"), args.get("dataset_name"))
            if err:
                return (f"Error: {err}", {"error": err, "findings": [], "summary": {}})
            result = star_schema.audit_star_schema(model)
            s = result["summary"]
            out = "=== Star Schema Audit ===\n\n"
            out += f"Score: {s['score']}/100 (Grade {s['grade']})   Relationships: {s['relationships']}\n"
            out += "Tables: " + ", ".join(f"{k}={v}" for k, v in s["tables_by_class"].items()) + "\n\n"
            out += "--- Classification ---\n"
            for name, c in result["classification"].items():
                mark = " [marked date table]" if c.get("marked_date_table") else ""
                out += f"  {name}: {c['class']}{mark}  (many-side of {c['many_side_of']}, one-side of {c['one_side_of']})\n"
            out += "\n--- Findings ---\n"
            for f in result["findings"]:
                loc = f" [{f['table']}]" if f.get("table") else ""
                out += f"  [{f['severity'].upper()}] {f['code']}{loc}: {f['message']}\n"
                out += f"      fix: {f['recommendation']}\n"
            if not result["findings"]:
                out += "  Clean star schema. No findings.\n"
            return (out, result)
        except Exception as e:
            msg = redact_secrets(str(e), [self.client_secret])
            return (f"Error auditing star schema: {msg}", {"error": msg, "findings": [], "summary": {}})

    @staticmethod
    def _dax_col(table: str, column: str) -> str:
        return ("'" + str(table).replace("'", "''") + "'["
                + str(column).replace("]", "]]") + "]")

    async def _handle_scan_referential_integrity(self, args: Dict[str, Any]):
        """Orphan-key scan across active relationships. Returns (text, result)."""
        try:
            source = args.get("source") or "desktop"
            workspace, dataset = args.get("workspace_name"), args.get("dataset_name")
            model, err = await self._gather_model_metadata(source, workspace, dataset)
            if err:
                return (f"Error: {err}", {"error": err, "checked": 0, "violations": []})
            run, err = await self._get_query_runner(source, workspace, dataset)
            if err:
                return (f"Error: {err}", {"error": err, "checked": 0, "violations": []})
            loop = asyncio.get_event_loop()
            try:
                max_samples = max(1, min(100, int(args.get("max_samples", 5) or 5)))
            except (TypeError, ValueError):
                max_samples = 5
            checked, violations = 0, []
            for r in model.get("relationships", []):
                if not (r.get("from_table") and r.get("from_column") and r.get("to_table") and r.get("to_column")):
                    continue
                active = r.get("is_active", True)
                if isinstance(active, str):
                    active = active.strip().lower() in ("true", "1", "yes")
                if not active:
                    continue
                label = f"{r['from_table']}[{r['from_column']}] -> {r['to_table']}[{r['to_column']}]"
                fcol = dax_tokens.column_ref(r["from_table"], r["from_column"])
                tcol = dax_tokens.column_ref(r["to_table"], r["to_column"])
                q = f"EVALUATE ROW(\"Orphans\", COUNTROWS(EXCEPT(DISTINCT({fcol}), DISTINCT({tcol}))))"
                try:
                    rows = await loop.run_in_executor(None, run, q)
                    count = int(float(list(rows[0].values())[0] or 0)) if rows else 0
                except Exception as qe:
                    violations.append({"relationship": label, "error": self._redact(str(qe))})
                    continue
                checked += 1
                if count > 0:
                    # The sample keys are real data values, so they go through the guarded path
                    # and get the same column policies and PII masking as any query result.
                    sq = f"EVALUATE TOPN({max_samples}, EXCEPT(DISTINCT({fcol}), DISTINCT({tcol})))"
                    srows, _report, serr = await self._run_guarded_query(
                        sq, tool="scan_referential_integrity", source=source, workspace=workspace,
                        dataset=dataset, max_rows=max_samples)
                    entry = {"relationship": label, "orphan_keys": count,
                             "samples": [] if serr else [list(x.values())[0] for x in srows]}
                    if serr:
                        entry["samples_note"] = serr[:160]
                    violations.append(entry)
            result = {"checked": checked, "violations": violations}
            out = "=== Referential Integrity Scan ===\n\n"
            out += f"Relationships checked: {checked}   Violations: {len([v for v in violations if v.get('orphan_keys')])}\n\n"
            for v in violations:
                if v.get("error"):
                    out += f"  [SKIPPED] {v['relationship']}: {v['error']}\n"
                else:
                    out += f"  [ORPHANS] {v['relationship']}: {v['orphan_keys']} key(s) missing on the one side"
                    if v.get("samples"):
                        out += f"  e.g. {', '.join(str(s) for s in v['samples'])}"
                    elif v.get("samples_note"):
                        out += f"  (samples withheld: {v['samples_note']})"
                    out += "\n"
            if not violations:
                out += "  No orphan keys. Every fact key has a matching dimension row.\n"
            else:
                out += ("\nOrphan keys land in the hidden blank row and silently distort totals; fix the "
                        "source join or add the missing dimension rows.\n")
            return (out, result)
        except Exception as e:
            msg = self._redact(str(e))
            return (f"Error scanning referential integrity: {msg}", {"error": msg, "checked": 0, "violations": []})

    async def _handle_pbip_add_measures(self, args: Dict[str, Any]):
        """Bulk-add (or upsert) measures offline into the loaded PBIP project's TMDL. Returns (text, result)."""
        try:
            table = args.get("table")
            measures = args.get("measures") or []
            if not table or not measures:
                return ("Error: table and a non-empty measures array are required",
                        {"error": "args", "created": [], "updated": []})
            mode = (args.get("mode") or "add").lower()
            if mode not in ("add", "upsert"):
                return (f"Error: unknown mode '{mode}'. Use add or upsert.",
                        {"error": "bad_mode", "created": [], "updated": []})
            lint_note = ""
            if not args.get("skip_lint"):
                lint = dax_lint.lint_measures([{"name": m.get("name"), "expression": m.get("expression", "")}
                                               for m in measures])
                warn = [f for f in lint["findings"] if f["severity"] in ("warning", "error")]
                if warn:
                    lint_note = f"\nLint: {len(warn)} finding(s) - " + "; ".join(
                        f"{f['object']}: {f['rule_id']}" for f in warn[:8]) + " (written anyway; review suggested)\n"
            connector = self._get_pbip_connector()
            write_fn = lambda: connector.add_measures(table, measures, mode=mode)  # noqa: E731
            res = await asyncio.get_event_loop().run_in_executor(None, write_fn)
            if not res.get("success"):
                return (f"Error: {res.get('message')}", {"error": res.get("message"), "created": [], "updated": []})
            created, updated = res.get("created", []), res.get("updated", [])
            out = f"'{table}' in {res.get('path')}: added {len(created)} measure(s)"
            if mode == "upsert":
                out += f", replaced {len(updated)} in place"
            out += f".{lint_note}\nValidate with pbip_validate. " + self._RELOAD_HINT
            return (out, {"created": created, "updated": updated, "path": res.get("path")})
        except Exception as e:
            msg = self._redact(str(e))
            return (f"Error adding measures: {msg}", {"error": msg, "created": [], "updated": []})

    async def _handle_pbip_create_date_table(self, args: Dict[str, Any]) -> str:
        """Create an offline DAX date-dimension table in the loaded PBIP project."""
        try:
            connector = self._get_pbip_connector()
            fn = lambda: connector.create_date_table(
                name=args.get("name") or "Date",
                start_date=args.get("start_date") or "2015-01-01",
                end_date=args.get("end_date") or "2030-12-31",
                fiscal_year_start_month=args.get("fiscal_year_start_month"),
            )
            res = await asyncio.get_event_loop().run_in_executor(None, fn)
            if not res.get("success"):
                return f"Error: {res.get('message')}"
            return (f"Date table '{res.get('table')}' created at {res.get('path')} "
                    f"(marked as date table; {res.get('columns')} columns).\n"
                    "Relate your fact date columns to it. " + self._RELOAD_HINT)
        except Exception as e:
            return f"Error creating date table: {redact_secrets(str(e), [self.client_secret])}"

    async def _handle_pbip_add_calculation_group(self, args: Dict[str, Any]) -> str:
        """Create a calculation group offline in the loaded PBIP project."""
        try:
            name = args.get("name")
            if not name:
                return "Error: name is required"
            items = args.get("items")
            if (args.get("preset") or "").lower() == "time_intelligence":
                if not args.get("date_column"):
                    return "Error: date_column is required for the time_intelligence preset"
                items = tmdl_authoring.time_intelligence_calc_items(args["date_column"])
            if not items:
                return "Error: provide items or preset='time_intelligence' with date_column"
            connector = self._get_pbip_connector()
            fn = lambda: connector.add_calculation_group(
                name, items, column_name=args.get("column_name") or "Calculation",
                precedence=int(args.get("precedence", 1) or 1))
            res = await asyncio.get_event_loop().run_in_executor(None, fn)
            if not res.get("success"):
                return f"Error: {res.get('message')}"
            return (f"Calculation group '{name}' created at {res.get('path')} with "
                    f"{res.get('items')} item(s).\n" + self._RELOAD_HINT + " Then drop the "
                    f"'{args.get('column_name') or 'Calculation'}' column on a slicer or matrix to use it.")
        except Exception as e:
            return f"Error creating calculation group: {redact_secrets(str(e), [self.client_secret])}"

    async def _handle_pbip_add_hierarchy(self, args: Dict[str, Any]) -> str:
        """Add a hierarchy offline to a table in the loaded PBIP project."""
        try:
            table, name, levels = args.get("table"), args.get("name"), args.get("levels") or []
            if not table or not name or not levels:
                return "Error: table, name, and a non-empty levels array are required"
            connector = self._get_pbip_connector()
            fn = lambda: connector.add_hierarchy(table, name, levels)
            res = await asyncio.get_event_loop().run_in_executor(None, fn)
            if not res.get("success"):
                return f"Error: {res.get('message')}"
            return (f"Hierarchy '{name}' added to '{table}' ({' > '.join(levels)}).\n"
                    + self._RELOAD_HINT)
        except Exception as e:
            return f"Error adding hierarchy: {redact_secrets(str(e), [self.client_secret])}"

    # ==================== Wave 6: Power BI Desktop Bridge ====================

    @staticmethod
    def _get_bridge(pid: Optional[int] = None):
        """Resolve a Desktop Bridge client. Returns (client, pid, error)."""
        bridges = desktop_bridge.discover_bridges()
        if not bridges:
            return None, None, ("No Power BI Desktop Bridge found. Open a report in Power BI "
                                "Desktop and ensure the preview option 'Enable external tool "
                                "access to Power BI Desktop through secure local APIs' is on "
                                "(File > Options > Preview features; on by default).")
        if pid is not None:
            match = [b for b in bridges if b["pid"] == int(pid)]
            if not match:
                return None, None, (f"No Desktop Bridge for pid {pid}. Running instances: "
                                    + ", ".join(str(b["pid"]) for b in bridges))
            chosen = match[0]
        elif len(bridges) == 1:
            chosen = bridges[0]
        else:
            return None, None, ("Multiple Power BI Desktop windows are open (pids: "
                                + ", ".join(str(b["pid"]) for b in bridges)
                                + "). Pass 'pid' to choose one (see bridge_status).")
        return desktop_bridge.DesktopBridgeClient(chosen["pipe"]), chosen["pid"], None

    async def _handle_bridge_status(self, args: Dict[str, Any]):
        """Discover Desktop Bridge instances + open-file state + PBIR pages. Returns (text, result)."""
        try:
            loop = asyncio.get_event_loop()
            bridges = desktop_bridge.discover_bridges()
            instances = []
            out = "=== Power BI Desktop Bridge ===\n\n"
            if not bridges:
                out += ("No bridge instances found. Open a report in Power BI Desktop and check the "
                        "preview option 'Enable external tool access to Power BI Desktop through "
                        "secure local APIs'.\n")
                return (out, {"instances": []})
            for b in bridges:
                client = desktop_bridge.DesktopBridgeClient(b["pipe"])
                inst: Dict[str, Any] = {"pid": b["pid"]}
                try:
                    state = await loop.run_in_executor(None, client.get_state)
                    inst["file"] = state.get("currentFilePath")
                    inst["has_unsaved_changes"] = state.get("hasUnsavedChanges")
                    inst["pages"] = desktop_bridge.pages_for_file(inst["file"] or "")
                    if b["pid"]:
                        inst["msmdsrv_port"] = await loop.run_in_executor(
                            None, desktop_bridge.msmdsrv_port_for_desktop_pid, b["pid"])
                except Exception as e:
                    inst["error"] = redact_secrets(str(e), [self.client_secret])
                instances.append(inst)
                out += f"pid {inst['pid']}: {inst.get('file') or '(no file / state unavailable)'}\n"
                if inst.get("error"):
                    out += f"  error: {inst['error']}\n"
                else:
                    out += f"  unsaved changes: {inst.get('has_unsaved_changes')}\n"
                    if inst.get("msmdsrv_port"):
                        out += (f"  analysis services port: {inst['msmdsrv_port']} "
                                "(use with desktop_connect for DAX/TOM)\n")
                    pages = inst.get("pages") or []
                    if pages:
                        out += "  pages:\n"
                        for p in pages:
                            mark = " (active)" if p.get("active") else ""
                            out += f"    - {p['display_name']}{mark}  [id: {p['id']}]\n"
                    else:
                        out += "  pages: (not a PBIP/PBIR file, or pages folder not found)\n"
            return (out, {"instances": instances})
        except Exception as e:
            msg = redact_secrets(str(e), [self.client_secret])
            return (f"Error querying Desktop Bridge: {msg}", {"error": msg, "instances": []})

    async def _handle_bridge_manifest(self, args: Dict[str, Any]):
        """Desktop Bridge method discovery. Returns (text, result)."""
        try:
            client, pid, err = self._get_bridge(args.get("pid"))
            if err:
                return (f"Error: {err}", {"error": err, "methods": []})
            manifest = await asyncio.get_event_loop().run_in_executor(None, client.manifest)
            methods = manifest.get("methods", [])
            out = f"=== Desktop Bridge manifest (pid {pid}) ===\n\n"
            for m in methods:
                out += f"- {m.get('name')}: {m.get('description', '')}\n"
            return (out, {"methods": methods})
        except Exception as e:
            msg = redact_secrets(str(e), [self.client_secret])
            return (f"Error reading bridge manifest: {msg}", {"error": msg, "methods": []})

    async def _handle_bridge_screenshot(self, args: Dict[str, Any]):
        """Capture report-page PNGs from the running Desktop. Returns (text, result)."""
        try:
            import base64
            import tempfile
            loop = asyncio.get_event_loop()
            client, pid, err = self._get_bridge(args.get("pid"))
            if err:
                return (f"Error: {err}", {"error": err, "screenshots": []})
            state = await loop.run_in_executor(None, client.get_state)
            pages = desktop_bridge.pages_for_file(state.get("currentFilePath") or "")
            ref = (args.get("page") or "all").strip()
            if ref.lower() == "all":
                targets = [p["id"] for p in pages]
                if not targets:
                    return ("Error: no PBIR pages found for the open file; pass an explicit page id.",
                            {"error": "no_pages", "screenshots": []})
            else:
                page_id = desktop_bridge.resolve_page_id(pages, ref) or ref  # allow raw ids for .pbix
                targets = [page_id]
            out_dir = Path(args.get("output_dir") or (Path(tempfile.gettempdir()) / "bridge-screenshots"))
            out_dir.mkdir(parents=True, exist_ok=True)
            scale = args.get("scale")
            by_id = {p["id"]: p["display_name"] for p in pages}
            shots = []
            for page_id in targets:
                snap = await loop.run_in_executor(None, client.capture_snapshot, page_id, scale)
                data = base64.b64decode(snap.get("payload") or "")
                display = snap.get("pageDisplayName") or by_id.get(page_id, page_id)
                safe = re.sub(r'[<>:"/\\|?*]', "_", display)
                safe_id = re.sub(r"[^A-Za-z0-9_-]", "_", str(page_id))[:24]
                path = out_dir / f"{safe} [{safe_id}].png"
                path.write_bytes(data)
                shots.append({"page_id": page_id, "page": display, "path": str(path), "bytes": len(data)})
            out = f"=== Captured {len(shots)} page screenshot(s) from Desktop (pid {pid}) ===\n\n"
            for s in shots:
                out += f"  {s['page']}: {s['path']} ({s['bytes']:,} bytes)\n"
            out += "\nRead the PNG file(s) to visually verify the report."
            return (out, {"screenshots": shots})
        except desktop_bridge.BridgeError as e:
            hint = ""
            if e.code == -32000:
                hint = ("\nNote: report.snapshot.capture is a Desktop-side preview method and can "
                        "return an internal error (Desktop -32000) independent of this server - "
                        "observed even on a valid PBIP and for an unknown page id, so it is a "
                        "Power BI Desktop limitation on the current build, not a client fault. "
                        "bridge_status, bridge_manifest, and bridge_reload work. Confirm the page "
                        "renders in Desktop and retry on a later Desktop build.")
            return (f"Bridge refused the screenshot: {e}{hint}", {"error": str(e), "screenshots": []})
        except Exception as e:
            msg = redact_secrets(str(e), [self.client_secret])
            return (f"Error capturing screenshot: {msg}", {"error": msg, "screenshots": []})

    async def _handle_bridge_reload(self, args: Dict[str, Any]) -> str:
        """Hot-reload the open file from disk (guarded against unsaved-change loss)."""
        try:
            loop = asyncio.get_event_loop()
            client, pid, err = self._get_bridge(args.get("pid"))
            if err:
                return f"Error: {err}"
            state = await loop.run_in_executor(None, client.get_state)
            if state.get("hasUnsavedChanges") and not args.get("force"):
                return ("Refused: Power BI Desktop has UNSAVED changes for "
                        f"'{state.get('currentFilePath')}'. Reloading would discard them. "
                        "Save in Desktop first, or pass force=true to discard.")
            reload_model = args.get("reload_model_definition", True)
            result = await loop.run_in_executor(None, client.reload_file, bool(reload_model))
            scope = "report + model definition" if reload_model else "report only"
            ok = result.get("success", True)
            return (f"{'Reloaded' if ok else 'Reload reported failure for'} "
                    f"'{state.get('currentFilePath')}' from disk ({scope}) in Desktop pid {pid}. "
                    "Verify visually with bridge_screenshot.")
        except desktop_bridge.BridgeError as e:
            return f"Bridge refused the reload: {e}"
        except Exception as e:
            return f"Error reloading: {redact_secrets(str(e), [self.client_secret])}"

    async def _handle_analyze_model_storage(self, args: Dict[str, Any]) -> str:
        """VertiPaq-style storage analysis: per-table row counts (reliable via DAX) plus
        best-effort sizes, to find the biggest/most expensive tables."""
        try:
            source = args.get("source") or "desktop"
            run, err = await self._get_query_runner(source, args.get("workspace_name"), args.get("dataset_name"))
            if err:
                return f"Error: {err}"
            loop = asyncio.get_event_loop()

            # Table list (+ column counts) from metadata
            model, merr = await self._gather_model_metadata(source, args.get("workspace_name"), args.get("dataset_name"))
            if merr:
                return f"Error: {merr}"
            tables = [t for t in model["tables"] if not model_analysis._truthy(t.get("is_hidden"))]

            # Reliable row counts via DAX COUNTROWS per table
            rows_by_table = {}
            for t in tables:
                name = t["name"]
                q = f"EVALUATE ROW(\"r\", COUNTROWS({dax_tokens.quote_table(str(name), force=True)}))"
                try:
                    res = await loop.run_in_executor(None, run, q)
                    val = None
                    if res:
                        val = next(iter(res[0].values()), None)
                    rows_by_table[name] = int(val) if val is not None else None
                except Exception:
                    rows_by_table[name] = None

            # Best-effort VertiPaq sizes (desktop only)
            sizes = {}
            if source.lower() != "cloud":
                try:
                    desktop = self._get_desktop_connector()
                    stats = await loop.run_in_executor(None, desktop.get_vertipaq_stats)
                    for t in stats.get("tables", []):
                        sizes[t.get("name")] = t.get("size", 0)
                except Exception:
                    pass

            ranked = sorted(
                tables, key=lambda t: (rows_by_table.get(t["name"]) or 0), reverse=True
            )
            out = "=== Model Storage Analysis ===\n\n"
            total_rows = sum(v for v in rows_by_table.values() if v)
            out += f"Tables: {len(tables)}   Total rows (visible tables): {total_rows:,}\n\n"
            out += f"{'Table':<35} {'Rows':>14} {'Cols':>6} {'Size(KB)':>10}\n"
            out += "-" * 68 + "\n"
            for t in ranked[:50]:
                nm = t["name"]
                rc = rows_by_table.get(nm)
                rc_s = f"{rc:,}" if rc is not None else "n/a"
                sz = sizes.get(nm)
                sz_s = f"{round(sz/1024):,}" if sz else "-"
                out += f"{nm[:34]:<35} {rc_s:>14} {len(t.get('columns', [])):>6} {sz_s:>10}\n"
            out += "\nNotes: row counts via DAX COUNTROWS (exact). Sizes are best-effort from\n"
            out += "$SYSTEM DMVs (desktop only); for full VertiPaq detail use DAX Studio / Tabular Editor.\n"
            return out
        except Exception as e:
            return f"Error analyzing storage: {redact_secrets(str(e), [self.client_secret])}"

    async def _handle_analyze_query_performance(self, args: Dict[str, Any]) -> str:
        """Time a DAX query and return duration, row count, and heuristic optimization hints."""
        try:
            dax = args.get("dax")
            if not dax:
                return "Error: dax is required"
            source = args.get("source") or "desktop"
            rows, report, error = await self._run_guarded_query(
                dax, tool="analyze_query_performance", source=source,
                workspace=args.get("workspace_name"), dataset=args.get("dataset_name"), max_rows=20000)
            if error:
                return error if error.startswith(("Refused:", "Query blocked", "Not connected")) else \
                    f"Error analyzing query performance: {error}"
            duration_ms = report.get("duration_ms", 0.0)
            row_count = len(rows)

            hints = []
            # Count calls on tokens, so a FILTER( inside a string or comment is not a hint.
            calls = [t.text.upper() for t in dax_tokens.tokenize(dax) if t.kind == dax_tokens.IDENT]
            if duration_ms > 2000:
                hints.append(f"Slow ({duration_ms:.0f} ms). Check relationship cardinality and avoid row-by-row iterators over large fact tables.")
            if report.get("truncated"):
                hints.append(f"Large result (more than {row_count:,} rows; counting stopped there). Add TOPN / SUMMARIZECOLUMNS filters.")
            elif row_count > 10000:
                hints.append(f"Large result ({row_count:,} rows). Add TOPN / SUMMARIZECOLUMNS filters.")
            if calls.count("FILTER") >= 3:
                hints.append("Multiple FILTER() calls; prefer CALCULATE with boolean filters or KEEPFILTERS where possible.")
            if "ADDCOLUMNS" in calls and "SUMMARIZE" in calls:
                hints.append("SUMMARIZE+ADDCOLUMNS pattern; SUMMARIZECOLUMNS is usually faster and safer.")
            lint = [f for f in dax_lint.lint_expression("query", dax) if f.get("severity") in ("warning", "error")]
            for f in lint[:5]:
                hints.append(f"{f['rule_id']}: {f['message']}")
            if not hints:
                hints.append("No obvious red flags. For storage-engine vs formula-engine timings, use DAX Studio Server Timings.")

            out = "=== Query Performance ===\n\n"
            out += f"Duration: {duration_ms:.0f} ms\n"
            out += f"Rows returned: {row_count:,}{'+' if report.get('truncated') else ''}\n\n"
            out += "--- Hints ---\n"
            for h in hints:
                out += f"  - {h}\n"
            return out
        except Exception as e:
            return f"Error analyzing query performance: {self._redact(str(e))}"

    async def _handle_export_data_dictionary(self, args: Dict[str, Any]) -> str:
        """Generate a portable data dictionary (Markdown/HTML) with a documentation-coverage score."""
        try:
            source = args.get("source") or "desktop"
            model, err = await self._gather_model_metadata(
                source, args.get("workspace_name"), args.get("dataset_name")
            )
            if err:
                return f"Error: {err}"
            fmt = (args.get("format") or "markdown").lower()
            doc = model_analysis.render_data_dictionary(model, fmt=fmt)

            # Live INFO.VIEW can blank measure expressions for non-admin/live connections.
            note = ""
            if source.lower() != "cloud":
                note = ("\n\n(Note: over a live Desktop connection, measure expressions read via "
                        "INFO.VIEW can be blank for non-admin users. For guaranteed expressions, "
                        "generate from a loaded PBIP/TMDL project or connect with model-admin rights.)")

            output_path = args.get("output_path")
            if output_path:
                try:
                    with open(output_path, "w", encoding="utf-8") as f:
                        f.write(doc)
                    return f"Data dictionary ({fmt}) written to {output_path} ({len(doc)} chars).{note}"
                except Exception as e:
                    return f"Error writing to {output_path}: {redact_secrets(str(e), [self.client_secret])}"
            return doc + note
        except Exception as e:
            return f"Error exporting data dictionary: {redact_secrets(str(e), [self.client_secret])}"

    async def _handle_model_snapshot(self, args: Dict[str, Any]) -> str:
        """Capture the connected model's metadata to a JSON snapshot (for later model_diff)."""
        try:
            model, err = await self._gather_model_metadata(
                args.get("source") or "desktop", args.get("workspace_name"), args.get("dataset_name")
            )
            if err:
                return f"Error: {err}"
            payload = json.dumps(model, default=str, indent=2)
            output_path = args.get("output_path")
            if output_path:
                with open(output_path, "w", encoding="utf-8") as f:
                    f.write(payload)
                tcount = len(model.get("tables", []))
                return f"Snapshot of {tcount} table(s) written to {output_path} ({len(payload)} chars)."
            return payload
        except Exception as e:
            return f"Error creating snapshot: {redact_secrets(str(e), [self.client_secret])}"

    async def _handle_model_diff(self, args: Dict[str, Any]) -> str:
        """Semantic diff between a baseline snapshot and another snapshot or the live model."""
        try:
            baseline_path = args.get("baseline_path")
            if not baseline_path:
                return "Error: baseline_path is required (a JSON snapshot from model_snapshot)"
            try:
                with open(baseline_path, "r", encoding="utf-8") as f:
                    before = json.load(f)
            except Exception as e:
                return f"Error reading baseline snapshot: {e}"

            compare_path = args.get("compare_path")
            if compare_path:
                try:
                    with open(compare_path, "r", encoding="utf-8") as f:
                        after = json.load(f)
                except Exception as e:
                    return f"Error reading compare snapshot: {e}"
            else:
                after, err = await self._gather_model_metadata(
                    args.get("source") or "desktop", args.get("workspace_name"), args.get("dataset_name")
                )
                if err:
                    return f"Error reading live model to compare: {err}"

            return model_analysis.diff_models(before, after)["markdown"]
        except Exception as e:
            return f"Error diffing models: {redact_secrets(str(e), [self.client_secret])}"

    async def _handle_pre_deploy_gate(self, args: Dict[str, Any]):
        """CI quality gate: run BPA + AI-readiness and return a machine PASS/FAIL verdict."""
        try:
            model, err = await self._gather_model_metadata(
                args.get("source") or "desktop", args.get("workspace_name"), args.get("dataset_name")
            )
            if err:
                return (f"Error: {err}", {"passed": False, "error": err})
            bpa = model_analysis.run_bpa(model)
            ai = model_analysis.audit_ai_readiness(model)
            errors = [f for f in bpa["findings"] if f["severity"] == "error"]
            warnings = [f for f in bpa["findings"] if f["severity"] == "warning"]
            min_ai = args.get("min_ai_score", 60)
            block_on_warnings = bool(args.get("block_on_warnings", False))
            passed = (len(errors) == 0 and ai["score"] >= min_ai
                      and (not block_on_warnings or len(warnings) == 0))

            structured = {
                "passed": passed,
                "bpa_errors": len(errors),
                "bpa_warnings": len(warnings),
                "ai_score": ai["score"],
                "blocking": [f"{f['rule_id']}: {f['object']}" for f in errors],
            }
            verdict = "PASS" if passed else "FAIL"
            text = f"[{verdict}] Pre-deploy quality gate\n\n"
            text += f"  BPA errors: {len(errors)}  warnings: {len(warnings)}\n"
            text += f"  AI-readiness: {ai['score']}/100 (min {min_ai})\n"
            if errors:
                text += "\nBlocking errors:\n"
                for f in errors[:50]:
                    text += f"  - {f['rule_id']}: {f['object']} ({f['detail']})\n"
            return (text, structured)
        except Exception as e:
            msg = redact_secrets(str(e), [self.client_secret])
            return (f"Error running pre-deploy gate: {msg}", {"passed": False, "error": msg})

    # ==================== DIAGNOSTICS & OPS (Wave 2) ====================

    async def _handle_refresh_doctor(self, args: Dict[str, Any]):
        """Diagnose dataset refresh failures (root cause + remediation) from REST history."""
        try:
            workspace = args.get("workspace_name")
            dataset = args.get("dataset_name")
            if not (workspace and dataset):
                return ("Error: workspace_name and dataset_name are required",
                        {"error": "missing workspace_name/dataset_name"})
            rest = self._get_rest_connector()
            if not rest:
                return ("Error: cloud credentials not configured (TENANT_ID / CLIENT_ID / CLIENT_SECRET).",
                        {"error": "no cloud credentials"})
            loop = asyncio.get_event_loop()
            wid, did, err = await loop.run_in_executor(None, rest.resolve_dataset, workspace, dataset)
            if err:
                return (f"Error: {err}", {"error": err})
            top = int(args.get("history_count", 10))
            history = await loop.run_in_executor(None, rest.get_refresh_history, wid, did, top)
            if not history:
                return ("No refresh history found (dataset may never have refreshed, or history expired ~30 days).",
                        {"history": 0})

            completed = sum(1 for h in history if str(h.get("status")) == "Completed")
            failed = [h for h in history if str(h.get("status")) == "Failed"]
            # Consecutive leading failures (history is most-recent-first). The status enum is
            # Unknown | Completed | Failed | Disabled: count Failed, stop at any non-Failed
            # terminal (Completed/Disabled), and skip Unknown (in-progress) without resetting.
            consecutive = 0
            for h in history:
                s = str(h.get("status"))
                if s == "Failed":
                    consecutive += 1
                elif s in ("Completed", "Disabled"):
                    break
                # Unknown / in-progress: skip

            out = f"=== Refresh Doctor: {dataset} ===\n\n"
            out += f"Last {len(history)} refreshes: {completed} completed, {len(failed)} failed\n"
            recent = history[0]
            out += f"Most recent: {recent.get('status')} ({recent.get('refreshType','?')}) ended {recent.get('endTime','?')}\n\n"

            diagnosis = None
            if failed:
                err_text = failed[0].get("serviceExceptionJson") or ""
                diag = refresh_diagnostics.classify_refresh_error(err_text)
                diagnosis = diag
                out += f"Most recent failure:\n  Cause: {diag['cause']}\n  Fix: {diag['remediation']}\n"
                if err_text:
                    out += f"  Raw: {redact_secrets(err_text, [self.client_secret])[:300]}\n"
                out += "\n"
            thr = refresh_diagnostics.CONSECUTIVE_FAILURE_DISABLE_THRESHOLD
            if consecutive >= thr - 1:
                out += (f"WARNING: {consecutive} consecutive failure(s). Power BI auto-disables a "
                        f"refresh schedule after {thr} consecutive failures.\n")

            structured = {
                "completed": completed, "failed": len(failed),
                "consecutive_failures": consecutive,
                "most_recent_status": recent.get("status"),
                "diagnosis": diagnosis,
            }
            return (out, structured)
        except Exception as e:
            msg = redact_secrets(str(e), [self.client_secret])
            return (f"Error diagnosing refresh: {msg}", {"error": msg})

    async def _handle_find_unused_objects(self, args: Dict[str, Any]) -> str:
        """Find columns/measures not referenced by any other model object nor any report visual."""
        try:
            source = args.get("source") or "desktop"
            model, err = await self._gather_model_metadata(source, args.get("workspace_name"), args.get("dataset_name"))
            if err:
                return f"Error: {err}"
            run, rerr = await self._get_query_runner(source, args.get("workspace_name"), args.get("dataset_name"))
            if rerr:
                return f"Error: {rerr}"
            loop = asyncio.get_event_loop()
            try:
                dep_rows = await loop.run_in_executor(None, run, "EVALUATE INFO.CALCDEPENDENCY()")
            except Exception as e:
                return (f"Error reading INFO.CALCDEPENDENCY: {redact_secrets(str(e), [self.client_secret])}. "
                        + INFO_CALCDEP_NOTE)

            used = set()
            for r in dep_rows:
                rt = self._row_get(r, "REFERENCED_TABLE")
                ro = self._row_get(r, "REFERENCED_OBJECT")
                if rt and ro:
                    used.add((str(rt), str(ro)))
            # columns used in relationships are not 'unused'
            for rel in model.get("relationships", []):
                if rel.get("from_table") and rel.get("from_column"):
                    used.add((str(rel["from_table"]), str(rel["from_column"])))
                if rel.get("to_table") and rel.get("to_column"):
                    used.add((str(rel["to_table"]), str(rel["to_column"])))
            # A column that only orders another column (Month Number behind Month) is not in
            # the calculation dependency graph, yet deleting it breaks the sort.
            for t in model.get("tables", []):
                for c in t.get("columns", []):
                    if c.get("sort_by"):
                        used.add((str(t.get("name")), str(c["sort_by"])))

            report_scanned = False
            pbip = self.pbip_connector
            if pbip and pbip.current_project:
                used |= pbip.collect_report_references()
                report_scanned = True

            unused_cols, unused_measures = [], []
            for t in model.get("tables", []):
                tn = t.get("name")
                for c in t.get("columns", []):
                    if (tn, c.get("name")) not in used:
                        unused_cols.append(f"{tn}[{c.get('name')}]")
                for m in t.get("measures", []):
                    if (tn, m.get("name")) not in used:
                        unused_measures.append(f"{tn}[{m.get('name')}]")

            out = "=== Unused Object Scan ===\n\n"
            if not report_scanned:
                out += ("WARNING: no PBIP project loaded, so REPORT usage was NOT checked - objects used only "
                        "in reports may be wrongly listed. Load the .pbip with pbip_load_project for an accurate scan.\n\n")
            out += f"Unused measures ({len(unused_measures)}):\n"
            out += ("\n".join(f"  - {m}" for m in unused_measures[:100]) or "  (none)") + "\n\n"
            out += f"Unused columns ({len(unused_cols)}):\n"
            out += ("\n".join(f"  - {c}" for c in unused_cols[:100]) or "  (none)") + "\n"
            out += ("\nNote: 'used' means referenced by a measure, calculated column, RLS rule, calculation "
                    "group or field parameter (INFO.CALCDEPENDENCY), a relationship, a sort-by setting, or "
                    "a visual in the loaded report. Columns used only as hierarchy levels, or only by "
                    "other reports and external tools, are not visible here; review before deleting.")
            return out
        except Exception as e:
            return f"Error finding unused objects: {redact_secrets(str(e), [self.client_secret])}"

    async def _handle_impact_analysis(self, args: Dict[str, Any]) -> str:
        """Blast radius for an object: model dependents (INFO.CALCDEPENDENCY) + report visuals using it."""
        try:
            name = args.get("object_name") or args.get("measure_name") or args.get("column_name")
            if not name:
                return "Error: object_name is required (a measure or column name)"
            table = args.get("table_name")
            source = args.get("source") or "desktop"
            run, rerr = await self._get_query_runner(source, args.get("workspace_name"), args.get("dataset_name"))
            if rerr:
                return f"Error: {rerr}"
            loop = asyncio.get_event_loop()
            esc = str(name).replace('"', '""')
            filt = f'[REFERENCED_OBJECT] = "{esc}"'
            if table:
                et = str(table).replace('"', '""')
                filt = f'[REFERENCED_TABLE] = "{et}" && {filt}'
            try:
                q = f'EVALUATE FILTER(INFO.CALCDEPENDENCY(), {filt})'
                rows = await loop.run_in_executor(None, run, q)
            except Exception as e:
                return (f"Error reading INFO.CALCDEPENDENCY: {redact_secrets(str(e), [self.client_secret])}. "
                        + INFO_CALCDEP_NOTE)

            out = f"=== Impact Analysis: {name}{(' in ' + table) if table else ''} ===\n\n"
            out += f"--- Model objects that depend on it ({len(rows)}) ---\n"
            if not rows:
                out += "  (none at the model level)\n"
            for r in rows[:100]:
                otype = self._row_get(r, "OBJECT_TYPE") or "?"
                otable = self._row_get(r, "TABLE") or ""
                oobj = self._row_get(r, "OBJECT") or ""
                out += f"  <- [{otype}] {otable}{('[' + oobj + ']') if oobj else ''}\n"

            # Report-layer usage from a loaded PBIP project
            pbip = self.pbip_connector
            if pbip and pbip.current_project:
                by_file = pbip.collect_report_references_by_file()
                hit_files = [fp for fp, refs in by_file.items()
                             if any(fld == name and (not table or tbl == table) for (tbl, fld) in refs)]
                out += f"\n--- Report files that reference it ({len(hit_files)}) ---\n"
                if not hit_files:
                    out += "  (none in the loaded report)\n"
                for fp in hit_files[:100]:
                    out += f"  * {fp}\n"
            else:
                out += "\n(No PBIP project loaded - report-layer usage not checked. Use pbip_load_project.)\n"

            if not rows:
                out += "\nSafe to change from a model-dependency standpoint (verify report usage above)."
            return out
        except Exception as e:
            return f"Error in impact analysis: {redact_secrets(str(e), [self.client_secret])}"

    async def _handle_rls_test_harness(self, args: Dict[str, Any]) -> str:
        """Evaluate a measure/table under every RLS role and return a pass/fail matrix."""
        connector = self._get_desktop_connector()
        if not connector.current_port:
            return "Not connected to Power BI Desktop. Use 'desktop_connect' first."
        loop = asyncio.get_event_loop()

        dax = args.get("dax")
        table = args.get("table_name")
        measure = args.get("measure_name")
        if not dax:
            if table:
                dax = f'EVALUATE ROW("rows", COUNTROWS({dax_tokens.quote_table(str(table), force=True)}))'
            elif measure:
                dax = f'EVALUATE ROW("value", {dax_tokens.bracket(str(measure))})'
            else:
                return "Error: provide one of dax, table_name, or measure_name"

        all_roles = await loop.run_in_executor(None, connector.list_rls_roles)
        role_names = args.get("roles") or [r.get("name") for r in all_roles if r.get("name")]
        if not role_names:
            return "No RLS roles found in the model (nothing to test)."

        # The harness switches the session's role; put back whatever the caller had active,
        # so a session that was testing as one role does not silently end up unrestricted.
        previous_role = getattr(connector, "current_rls_role", None)
        baseline = None
        results = []
        try:
            await loop.run_in_executor(None, connector.set_rls_role, None)
            rows, _report, error = await self._run_guarded_query(dax, tool="rls_test_harness", max_rows=10)
            if error:
                return error
            baseline = self._first_value(rows)
            for role in role_names:
                ok = await loop.run_in_executor(None, connector.set_rls_role, role)
                if not ok:
                    results.append((role, None, "ERROR activating role"))
                    continue
                rows, _report, error = await self._run_guarded_query(dax, tool="rls_test_harness", max_rows=10)
                if error:
                    results.append((role, None, f"ERROR: {error[:80]}"))
                    continue
                val = self._first_value(rows)
                if val is None or self._is_zero(val):
                    note = "sees NOTHING (verify the role filter)"
                elif baseline is not None and str(val) == str(baseline):
                    note = "sees EVERYTHING (no row reduction - verify intended)"
                else:
                    note = "filtered"
                results.append((role, val, note))
        finally:
            await loop.run_in_executor(None, connector.set_rls_role, previous_role)

        out = "=== RLS Test Matrix ===\n"
        out += f"Query: {dax}\nUnrestricted baseline: {baseline}\n\n"
        out += f"{'Role':<32} {'Result':>14}  Note\n"
        out += "-" * 70 + "\n"
        for role, val, note in results:
            out += f"{str(role)[:31]:<32} {str(val):>14}  {note}\n"
        if previous_role:
            out += f"\nSession role restored to '{previous_role}'."
        out += ("\nNote: tests row-level (RLS) filters via role simulation. Object-level security "
                "(OLS) visibility is not covered here; check object access separately.")
        return out

    async def _handle_run_dax_tests(self, args: Dict[str, Any]):
        """Run a suite of DAX test cases and report pass/fail vs expected results (regression testing)."""
        try:
            tests = args.get("tests")
            tests_path = args.get("tests_path")
            if not tests and tests_path:
                try:
                    with open(tests_path, "r", encoding="utf-8-sig") as f:
                        tests = json.load(f)
                except Exception as e:
                    return (f"Error reading tests_path: {e}", {"passed": 0, "total": 0, "error": str(e)})
            if not tests or not isinstance(tests, list):
                return ("Error: provide 'tests' (array of {name, dax, expected, tolerance?}) or 'tests_path'.",
                        {"passed": 0, "total": 0})
            source = args.get("source") or "desktop"
            workspace, dataset = args.get("workspace_name"), args.get("dataset_name")
            _run, rerr = await self._get_query_runner(source, workspace, dataset)
            if rerr:
                return (f"Error: {rerr}", {"passed": 0, "total": len(tests), "error": rerr})

            results = []
            passed = 0
            for t in tests:
                if not isinstance(t, dict):
                    results.append({"name": str(t)[:40], "status": "ERROR", "detail": "test is not an object"})
                    continue
                dax = t.get("dax")
                name = t.get("name") or (str(dax)[:40] if dax else "test")
                if not dax:
                    results.append({"name": name, "status": "ERROR", "detail": "no dax"})
                    continue
                rows, _report, error = await self._run_guarded_query(
                    dax, tool="run_dax_tests", source=source, workspace=workspace, dataset=dataset, max_rows=10)
                if error:
                    results.append({"name": name, "status": "ERROR", "detail": error[:200]})
                    continue
                actual = self._first_value(rows)
                if "expected" not in t:
                    results.append({"name": name, "status": "INFO", "detail": f"actual={actual} (no expected given)"})
                    continue
                try:
                    ok, detail = model_analysis.dax_test_verdict(
                        actual, t.get("expected"), t.get("tolerance", 0), rel_tolerance=t.get("rel_tolerance"))
                except Exception as e:
                    results.append({"name": name, "status": "ERROR", "detail": f"could not compare: {e}"[:200]})
                    continue
                results.append({"name": name, "status": "PASS" if ok else "FAIL", "detail": detail})
                if ok:
                    passed += 1

            graded = [r for r in results if r["status"] in ("PASS", "FAIL")]
            errored = [r for r in results if r["status"] == "ERROR"]
            total = len(graded)
            # A test that could not run is not a pass: the suite only passes when every graded
            # test passed and none errored.
            all_passed = total > 0 and passed == total and not errored
            out = f"[{'PASS' if all_passed else 'FAIL'}] DAX tests: {passed}/{total} passed"
            if errored:
                out += f", {len(errored)} could not run"
            out += "\n\n"
            for r in results:
                out += f"  [{r['status']}] {r['name']}: {r['detail']}\n"
            return (out, {"passed": passed, "total": total, "errors": len(errored),
                          "all_passed": all_passed, "results": results})
        except Exception as e:
            msg = self._redact(str(e))
            return (f"Error running DAX tests: {msg}", {"passed": 0, "total": 0, "error": msg})

    async def _handle_cross_workspace_lineage(self, args: Dict[str, Any]) -> str:
        """Tenant-wide inventory + lineage via the Admin Scanner API (admin-gated)."""
        try:
            rest = self._get_rest_connector()
            if not rest:
                return "Error: cloud credentials not configured (TENANT_ID / CLIENT_ID / CLIENT_SECRET)."

            # Reuse a cached scanResult if provided (the Scanner API is rate-limited).
            cache_path = args.get("cache_path")
            notes: List[str] = []
            scan = None
            if cache_path and args.get("use_cache", True) and not args.get("scan_ids"):
                try:
                    with open(cache_path, "r", encoding="utf-8-sig") as f:
                        scan = json.load(f)
                    notes.append(f"Read from cache: {cache_path} (pass use_cache=false to rescan).")
                except FileNotFoundError:
                    scan = None
                except Exception as e:
                    notes.append(f"Ignored an unreadable cache file ({e}); rescanning.")
                    scan = None

            if scan is None:
                scan, error, scan_notes = await self._run_workspace_scan(rest, args)
                notes.extend(scan_notes)
                if error:
                    return error + ("\n\n" + "\n".join(notes) if notes else "")
                if cache_path:
                    if self._read_only:
                        notes.append("READ-ONLY mode: the scan result was not written to cache_path.")
                    else:
                        try:
                            safe_io.write_text(cache_path, json.dumps(scan))
                        except Exception as e:
                            notes.append(f"Could not write the cache file: {e}")

            s = governance.summarize_scan(scan, dataset_name=args.get("dataset_name"))
            out = "=== Cross-Workspace Lineage / Inventory ===\n\n"
            out += f"Workspaces: {s['workspaces']}  Datasets: {s['datasets']}  Reports: {s['reports']}\n\n"
            if s.get("focus_dataset"):
                out += f"Dataset '{s['focus_dataset']}' found in: {', '.join(s['focus_found_in']) or '(not found)'}\n"
                out += f"Downstream reports ({len(s['downstream_reports'])}):\n"
                out += ("\n".join(f"  - {r}" for r in s['downstream_reports']) or "  (none)") + "\n\n"
            out += f"Datasets WITHOUT RLS roles ({len(s['datasets_without_rls'])}):\n"
            out += ("\n".join(f"  - {d}" for d in s['datasets_without_rls'][:50]) or "  (none)") + "\n\n"
            unknown_rls = s.get("datasets_rls_unknown") or []
            if unknown_rls:
                out += f"Datasets whose RLS state is UNKNOWN ({len(unknown_rls)}):\n"
                out += "\n".join(f"  - {d}" for d in unknown_rls[:50]) + "\n\n"
            out += f"Datasets WITHOUT a sensitivity label ({len(s['datasets_without_sensitivity_label'])}):\n"
            out += ("\n".join(f"  - {d}" for d in s['datasets_without_sensitivity_label'][:50]) or "  (none)") + "\n"
            if notes:
                out += "\n" + "\n".join(f"Note: {n}" for n in notes) + "\n"
            return out
        except Exception as e:
            return f"Error in cross-workspace lineage: {self._redact(str(e))}"

    async def _handle_fleet_refresh_monitor(self, args: Dict[str, Any]) -> str:
        """Refresh health across many datasets/workspaces, classifying failures (admin or workspace access)."""
        try:
            rest = self._get_rest_connector()
            if not rest:
                return "Error: cloud credentials not configured (TENANT_ID / CLIENT_ID / CLIENT_SECRET)."
            loop = asyncio.get_event_loop()
            workspace_ids = args.get("workspace_ids")
            if not workspace_ids:
                return ("Error: workspace_ids is required (a list of workspace GUIDs) to bound the scan. "
                        "Use list_workspaces or cross_workspace_lineage to discover them.")

            failures = []
            unchecked = []     # could not be read: reported, never counted as healthy
            healthy = 0
            never_refreshed = 0
            checked = 0
            for wid in workspace_ids:
                try:
                    datasets = await loop.run_in_executor(None, rest.list_datasets, wid)
                except Exception as e:
                    unchecked.append((f"workspace {wid}", self._redact(str(e))[:120]))
                    continue
                for ds in datasets:
                    if not ds.get("isRefreshable"):
                        continue
                    checked += 1
                    try:
                        hist = await loop.run_in_executor(None, rest.get_refresh_history, wid, ds["id"], 1)
                    except Exception as e:
                        unchecked.append((ds.get("name") or ds.get("id"), self._redact(str(e))[:120]))
                        continue
                    if not hist:
                        never_refreshed += 1
                        continue
                    last = hist[0]
                    if str(last.get("status")) == "Failed":
                        diag = refresh_diagnostics.classify_refresh_error(last.get("serviceExceptionJson") or "")
                        failures.append((ds.get("name"), last.get("endTime"), diag["cause"]))
                    else:
                        healthy += 1

            out = "=== Fleet Refresh Monitor ===\n\n"
            out += f"Refreshable datasets checked: {checked}\n"
            out += f"Most-recent-refresh FAILURES: {len(failures)}\n"
            out += f"Healthy: {healthy}   No refresh history: {never_refreshed}   Could not check: {len(unchecked)}\n\n"
            for name, when, cause in failures[:100]:
                out += f"  [FAILED] {name} ({when}): {cause}\n"
            for name, why in unchecked[:50]:
                out += f"  [UNKNOWN] {name}: {why}\n"
            if not failures and not unchecked:
                out += "  All checked datasets' most recent refresh succeeded.\n"
            elif not failures:
                out += ("  No failures among the datasets that could be read, but some could not be "
                        "checked (see UNKNOWN above).\n")
            return out
        except Exception as e:
            return f"Error in fleet refresh monitor: {self._redact(str(e))}"

    async def _handle_usage_and_orphan_analytics(self, args: Dict[str, Any]) -> str:
        """Tenant usage analytics from the Admin Activity Events API for a single UTC day."""
        try:
            rest = self._get_rest_connector()
            if not rest:
                return "Error: cloud credentials not configured (TENANT_ID / CLIENT_ID / CLIENT_SECRET)."
            date = args.get("date")
            if not date:
                from datetime import datetime, timezone, timedelta
                # default to yesterday UTC (today is incomplete; events lag up to ~60 min)
                date = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
            loop = asyncio.get_event_loop()
            try:
                events = await loop.run_in_executor(
                    None, rest.admin_get_activity_events_for_day, date, args.get("filter")
                )
            except Exception as e:
                return (f"Error reading activity events (needs admin / read-only admin APIs; 28-day "
                        f"retention): {redact_secrets(str(e), [self.client_secret])}")
            agg = governance.aggregate_activity(events)
            out = f"=== Usage Analytics ({date} UTC) ===\n\n"
            out += f"Total events: {agg['total_events']}   Distinct users: {agg['distinct_users']}\n\n"
            out += "Top activities:\n"
            out += ("\n".join(f"  {n}  {a}" for a, n in agg["by_activity"][:15]) or "  (none)") + "\n\n"
            out += "Top viewed reports:\n"
            out += ("\n".join(f"  {n}  {r}" for r, n in agg["top_reports_by_views"][:15]) or "  (none)") + "\n\n"
            out += "Top users:\n"
            out += ("\n".join(f"  {n}  {u}" for u, n in agg["top_users"][:15]) or "  (none)") + "\n"
            out += ("\nNote: 28-day retention; pull a fully-past day for completeness. For orphan "
                    "(zero-view) detection, correlate cross_workspace_lineage inventory with a longer "
                    "activity window persisted over time.")
            return out
        except Exception as e:
            return f"Error in usage analytics: {redact_secrets(str(e), [self.client_secret])}"

    async def _handle_verify_audit_integrity(self):
        """Verify the tamper-evident hash chain of the audit log."""
        res = self.security.verify_audit_integrity()
        status = "INTACT" if res.get("valid") else "TAMPERED"
        text = f"[{status}] Audit log integrity: {res.get('message', '')}\n  Entries checked: {res.get('checked', 0)}"
        if res.get("files"):
            text += f" across {res['files']} file(s)"
        if not res.get("valid") and res.get("broken_line"):
            where = f" of {res['broken_file']}" if res.get("broken_file") else ""
            text += f"\n  First problem at line {res['broken_line']}{where}"
        if not res.get("keyed"):
            text += ("\n  Note: no POWERBI_MCP_AUDIT_KEY is set, so this chain only detects accidental edits. "
                     "Someone who can rewrite the log can also recompute it.")
        elif res.get("unkeyed_entries"):
            text += f"\n  Warning: {res['unkeyed_entries']} entr(ies) were written without the key."
        return (text, res)

    # ==================== MCP RESOURCES & COMPLETION (Bundle C) ====================

    async def _read_resource(self, uri: str) -> str:
        """Resolve a powerbi:// resource URI to a JSON document (read-only model context)."""
        try:
            rest = uri.split("://", 1)[1] if "://" in uri else uri
            parts = [unquote(p) for p in rest.split("/") if p != ""]
            if not parts:
                return json.dumps({"error": f"unrecognized resource uri: {uri}"})

            if parts[0] == "reference":
                what = parts[1] if len(parts) > 1 else ""
                if what == "bpa-rules":
                    rules = [{"id": r["id"], "category": r["category"], "severity": r["severity"], "name": r["name"]}
                             for r in model_analysis.DEFAULT_BPA_RULES]
                    return json.dumps(rules, indent=2)
                if what == "refresh-errors":
                    return json.dumps({
                        "consecutive_failure_disable_threshold": refresh_diagnostics.CONSECUTIVE_FAILURE_DISABLE_THRESHOLD,
                        "rules": refresh_diagnostics.REFRESH_ERROR_RULES,
                    }, indent=2)
                return json.dumps({"error": f"unknown reference resource '{what}'"})

            if parts[0] == "desktop":
                kind = parts[1] if len(parts) > 1 else "schema"
                model, err = await self._gather_model_metadata("desktop")
                if err:
                    return json.dumps({"error": err})
                if kind == "schema":
                    return json.dumps(model, default=str, indent=2)
                if kind == "measures":
                    measures = [m for t in model["tables"] for m in t.get("measures", [])]
                    return json.dumps(measures, default=str, indent=2)
                if kind == "bpa":
                    return json.dumps(model_analysis.run_bpa(model), default=str, indent=2)
                if kind in ("ai-readiness", "ai_readiness"):
                    return json.dumps(model_analysis.audit_ai_readiness(model), default=str, indent=2)
                return json.dumps({"error": f"unknown desktop resource '{kind}'"})

            if parts[0] == "cloud" and len(parts) >= 3:
                workspace, dataset = parts[1], parts[2]
                model, err = await self._gather_model_metadata("cloud", workspace, dataset)
                if err:
                    return json.dumps({"error": err})
                return json.dumps(model, default=str, indent=2)

            return json.dumps({"error": f"unrecognized resource uri: {uri}"})
        except Exception as e:
            return json.dumps({"error": redact_secrets(str(e), [self.client_secret])})

    async def _complete_argument(self, argument) -> Completion:
        """Completion for prompt/resource-template arguments: real table/measure names
        from the connected Desktop model when available."""
        try:
            name = getattr(argument, "name", "") or ""
            value = (getattr(argument, "value", "") or "").lower()
            candidates = []
            desktop = self.desktop_connector
            if desktop is not None and getattr(desktop, "current_port", None):
                model, err = await self._gather_model_metadata("desktop")
                if not err and model:
                    measures = [m["name"] for t in model["tables"] for m in t.get("measures", []) if m.get("name")]
                    tables = [t["name"] for t in model["tables"] if t.get("name")]
                    if name in ("measure_name", "old_name", "new_name"):
                        candidates = measures + tables
                    elif name in ("table", "dataset", "table_name"):
                        candidates = tables
                    else:
                        candidates = tables + measures
            filtered = [c for c in candidates if value in c.lower()][:100]
            return Completion(values=filtered, total=len(filtered), hasMore=False)
        except Exception:
            return Completion(values=[], total=0, hasMore=False)

    # ==================== PBIP HANDLERS (File-based editing) ====================

    def _get_pbip_connector(self) -> PowerBIPBIPConnector:
        """Get or create PBIP connector. Offline edits are made by an agent, so a backup of the
        project files is taken before each rename or repair unless POWERBI_MCP_PBIP_BACKUPS is
        turned off."""
        if not self.pbip_connector:
            self.pbip_connector = PowerBIPBIPConnector(
                auto_backup=_security_env_flag("POWERBI_MCP_PBIP_BACKUPS", True, invalid=True))
        return self.pbip_connector

    _RELOAD_HINT = ("If the report is open in Power BI Desktop, hot-reload it with bridge_reload "
                    "(check bridge_status first; never reload over unsaved changes).")

    async def _handle_pbip_load_project(self, args: Dict[str, Any]) -> str:
        """Load a PBIP project for editing"""
        try:
            pbip_path = args.get("pbip_path")
            if not pbip_path:
                return "Error: 'pbip_path' is required"

            connector = self._get_pbip_connector()
            load_fn = lambda: connector.load_project(pbip_path)  # noqa: E731
            success = await asyncio.get_event_loop().run_in_executor(None, load_fn)
            if not success:
                reason = getattr(connector, "last_error", None)
                return (f"Failed to load PBIP project from: {pbip_path}\n\n"
                        + (f"Reason: {reason}\n\n" if reason else "")
                        + "Point at a .pbip file, or at a folder that contains exactly one.")

            info = connector.get_project_info()
            result = "=== PBIP Project Loaded Successfully ===\n\n"
            result += f"Project: {info.get('pbip_file', 'N/A')}\n"
            result += f"Root Path: {info.get('root_path', 'N/A')}\n\n"
            if info.get('semantic_model_folder'):
                result += f"Semantic Model: {info.get('semantic_model_folder')}\n"
                result += f"TMDL Files: {info.get('tmdl_file_count', 0)}\n\n"
            elif info.get('remote_model'):
                result += "Semantic Model: remote (the report is bound byConnection; model tools are unavailable)\n\n"
            if info.get('report_folder'):
                result += f"Report Folder: {info.get('report_folder')}\n"
                result += f"Report format: {info.get('report_format', 'N/A')}\n\n"

            result += "You can now use:\n"
            result += "  - pbip_rename_tables / pbip_rename_columns / pbip_rename_measures (model AND report)\n"
            result += "  - pbip_add_measures, pbip_create_date_table, pbip_add_calculation_group, pbip_add_hierarchy\n"
            result += "  - pbir_apply_spec (build report pages from a spec), audit_report_design\n"
            result += "  - pbip_validate, pbir_validate_report, pbip_scan_broken_refs\n"
            return result

        except Exception as e:
            logger.error(f"PBIP load error: {self._redact(str(e))}")
            return f"Error loading PBIP project: {self._redact(str(e))}"

    async def _handle_pbip_get_project_info(self) -> str:
        """Get info about loaded PBIP project"""
        try:
            connector = self._get_pbip_connector()

            if not connector.current_project:
                return "No PBIP project loaded. Use 'pbip_load_project' first."

            info = connector.get_project_info()

            result = "=== PBIP Project Info ===\n\n"
            result += f"Project File: {info.get('pbip_file', 'N/A')}\n"
            result += f"Root Path: {info.get('root_path', 'N/A')}\n\n"

            result += "--- Semantic Model ---\n"
            if info.get('semantic_model_folder'):
                result += f"  Folder: {info.get('semantic_model_folder')}\n"
                result += f"  TMDL Files: {info.get('tmdl_file_count', 0)}\n"
            else:
                result += "  Not found\n"
            result += "\n"

            result += "--- Report ---\n"
            if info.get('report_folder'):
                result += f"  Folder: {info.get('report_folder')}\n"
                result += f"  report.json: {'Present' if info.get('report_json_path') else 'Missing'}\n"
            else:
                result += "  Not found\n"

            return result

        except Exception as e:
            logger.error(f"PBIP info error: {e}")
            return f"Error: {str(e)}"

    async def _handle_pbip_rename_tables(self, args: Dict[str, Any]) -> str:
        """Rename tables in PBIP files (model + report), all or nothing."""
        return await self._run_pbip_rename("PBIP Batch Rename Tables", "batch_rename_tables", args)

    async def _handle_pbip_rename_columns(self, args: Dict[str, Any]) -> str:
        """Rename columns in PBIP files (model + report), all or nothing."""
        return await self._run_pbip_rename("PBIP Batch Rename Columns", "batch_rename_columns", args)

    async def _handle_pbip_rename_measures(self, args: Dict[str, Any]) -> str:
        """Rename measures in PBIP files (model + report), all or nothing."""
        return await self._run_pbip_rename("PBIP Batch Rename Measures", "batch_rename_measures", args)

    async def _handle_pbip_fix_broken_visuals(self, args: Dict[str, Any]) -> str:
        """Fix broken visual references after a table rename"""
        try:
            connector = self._get_pbip_connector()
            if not connector.current_project:
                return "No PBIP project loaded. Use 'pbip_load_project' first."

            old_table_name = args.get("old_table_name")
            new_table_name = args.get("new_table_name")
            if not old_table_name or not new_table_name:
                return "Error: 'old_table_name' and 'new_table_name' are required"

            fix_fn = lambda: connector.fix_broken_visual_references(old_table_name, new_table_name)  # noqa: E731
            result = await asyncio.get_event_loop().run_in_executor(None, fix_fn)

            response = "=== Fix Broken Visual References ===\n\n"
            response += f"Old table name: {old_table_name}\n"
            response += f"New table name: {new_table_name}\n"
            response += f"Report format: {result.get('format', 'Unknown')}\n\n"
            if result.get("success"):
                response += f"Fixed {result.get('references_fixed', 0)} reference(s).\n\n"
                if result.get("files_modified"):
                    response += "--- Files Modified ---\n"
                    for f in result["files_modified"][:15]:
                        response += f"  - {f}\n"
                    if len(result["files_modified"]) > 15:
                        response += f"  ... and {len(result['files_modified']) - 15} more\n"
                if result.get("backup_created"):
                    response += f"\nBackup: {result['backup_created']}\n"
                response += "\n" + self._RELOAD_HINT + "\n"
            else:
                response += f"Nothing was changed. {result.get('message') or 'No references matched.'}\n"
                response += "Use pbip_scan_broken_refs to see which table names the report uses.\n"
            for w in (result.get("warnings") or [])[:20]:
                response += f"  ! {w}\n"
            return response

        except Exception as e:
            logger.error(f"PBIP fix broken visuals error: {self._redact(str(e))}")
            return f"Error: {self._redact(str(e))}"

    async def _handle_pbip_fix_dax_quoting(self, args: Optional[Dict[str, Any]] = None) -> str:
        """Fix DAX expressions by properly quoting table names with spaces"""
        try:
            connector = self._get_pbip_connector()
            if not connector.current_project:
                return "No PBIP project loaded. Use 'pbip_load_project' first."

            dry_run = bool((args or {}).get("dry_run", False))
            fix_fn = lambda: connector.fix_all_dax_quoting(dry_run=dry_run)  # noqa: E731
            result = await asyncio.get_event_loop().run_in_executor(None, fix_fn)

            response = f"=== Fix DAX Table Name Quoting {'(dry run) ' if dry_run else ''}===\n\n"
            if result.get("count", 0) > 0:
                verb = "Would fix" if dry_run else "Fixed"
                response += f"{verb} {result['count']} unquoted table reference(s).\n\n"
                if result.get("tables_fixed"):
                    response += "--- Tables That Needed Quoting ---\n"
                    for table in result["tables_fixed"]:
                        response += f"  - {table} -> '{table}'\n"
                    response += "\n"
                if result.get("files_modified"):
                    response += "--- Files ---\n"
                    for f in result["files_modified"][:10]:
                        response += f"  - {f}\n"
                    if len(result["files_modified"]) > 10:
                        response += f"  ... and {len(result['files_modified']) - 10} more\n"
                    response += "\n"
                response += ("Nothing was written (dry run).\n" if dry_run else self._RELOAD_HINT + "\n")
            else:
                response += "No fixes needed - all table names are properly quoted.\n"

            if result.get("errors"):
                response += "\n--- Errors ---\n"
                for err in result["errors"]:
                    response += f"  {err.get('file')}: {err.get('error')}\n"
            return response

        except Exception as e:
            logger.error(f"PBIP fix DAX quoting error: {self._redact(str(e))}")
            return f"Error: {self._redact(str(e))}"

    async def _handle_pbip_scan_broken_refs(self) -> str:
        """Scan for broken references in the PBIP project"""
        try:
            connector = self._get_pbip_connector()

            if not connector.current_project:
                return "No PBIP project loaded. Use 'pbip_load_project' first."

            # Execute scan
            scan_fn = lambda: connector.scan_broken_references()
            result = await asyncio.get_event_loop().run_in_executor(None, scan_fn)

            # Build response
            response = "=== Scan for Broken References ===\n\n"

            report_format = "PBIR-Enhanced" if connector.current_project.is_pbir_enhanced else "PBIR-Legacy"
            response += f"Report format: {report_format}\n\n"

            # Model tables
            model_tables = result.get("model_tables", [])
            response += f"--- Tables in Semantic Model ({len(model_tables)}) ---\n"
            for t in sorted(model_tables)[:20]:
                response += f"  • {t}\n"
            if len(model_tables) > 20:
                response += f"  ... and {len(model_tables) - 20} more\n"
            response += "\n"

            # Report tables
            report_tables = result.get("report_tables", [])
            response += f"--- Tables Referenced in Visuals ({len(report_tables)}) ---\n"
            for t in sorted(report_tables)[:20]:
                in_model = "✓" if t in model_tables else "✗ MISSING"
                response += f"  • {t} [{in_model}]\n"
            if len(report_tables) > 20:
                response += f"  ... and {len(report_tables) - 20} more\n"
            response += "\n"

            # Broken references
            broken = result.get("broken_references", [])
            orphaned = result.get("orphaned_table_names", [])

            if broken:
                response += f"--- ❌ BROKEN REFERENCES ({len(broken)}) ---\n"
                response += "These visuals reference tables that don't exist in the model:\n\n"

                # Group by entity
                by_entity = {}
                for b in broken:
                    entity = b["entity"]
                    if entity not in by_entity:
                        by_entity[entity] = []
                    by_entity[entity].append(b)

                for entity, refs in by_entity.items():
                    response += f"  '{entity}' (missing) - {len(refs)} visual(s)\n"

                response += "\n💡 FIX: Use 'pbip_fix_broken_visuals' with:\n"
                for entity in orphaned:
                    response += f"   old_table_name='{entity}', new_table_name='<correct_name>'\n"
            else:
                response += "✅ No broken references found! All visuals reference valid tables.\n"

            return response

        except Exception as e:
            logger.error(f"PBIP scan broken refs error: {e}")
            return f"Error: {str(e)}"

    async def _handle_pbip_validate(self) -> str:
        """Validate TMDL syntax in the loaded project"""
        try:
            connector = self._get_pbip_connector()
            if not connector.current_project:
                return "No PBIP project loaded. Use 'pbip_load_project' first."

            validate_fn = lambda: connector.validate_tmdl_syntax()  # noqa: E731
            errors = await asyncio.get_event_loop().run_in_executor(None, validate_fn)

            response = "=== PBIP Validation Results ===\n\n"
            report_format = "PBIR-Enhanced" if connector.current_project.is_pbir_enhanced else "PBIR-Legacy"
            response += f"Report format: {report_format}\n"
            response += f"TMDL files: {len(connector.current_project.tmdl_files)}\n"
            response += f"Visual files: {len(connector.current_project.visual_json_files)}\n\n"

            if not errors:
                return response + "No validation errors found. TMDL syntax is valid.\n"

            response += f"Found {len(errors)} validation error(s):\n\n"
            by_type: Dict[str, list] = {}
            for err in errors:
                by_type.setdefault(err.error_type, []).append(err)
            for error_type, type_errors in by_type.items():
                response += f"--- {error_type} ({len(type_errors)}) ---\n"
                for err in type_errors[:5]:
                    response += f"  {os.path.basename(str(err.file_path))}:{err.line_number}: {err.message}\n"
                    if err.context:
                        ctx = err.context[:60] + "..." if len(err.context) > 60 else err.context
                        response += f"    Context: {ctx}\n"
                if len(type_errors) > 5:
                    response += f"  ... and {len(type_errors) - 5} more\n"
                response += "\n"
            if "UNQUOTED_TABLE_IN_DAX" in by_type:
                response += "Fix the quoting issues with pbip_fix_dax_quoting (dry_run=true shows the changes first).\n"
            return response

        except Exception as e:
            logger.error(f"PBIP validate error: {self._redact(str(e))}")
            return f"Error: {self._redact(str(e))}"

    # ==================== PBIR REPORT AUTHORING (preview) ====================

    async def _handle_pbir_add_page(self, args: Dict[str, Any]) -> str:
        """Add a report page to the loaded PBIR-Enhanced project."""
        try:
            connector = self._get_pbip_connector()
            if not connector.current_project:
                return "No PBIP project loaded. Use 'pbip_load_project' first."
            display_name = args.get("display_name")
            if not display_name:
                return "Error: display_name is required"
            fn = lambda: connector.add_page(
                display_name, width=int(args.get("width", 1280)),
                height=int(args.get("height", 720)), set_active=bool(args.get("set_active", False)))
            result = await asyncio.get_event_loop().run_in_executor(None, fn)
            if not result.get("success"):
                return f"Failed to add page: {result.get('message')}"
            return (f"Added page '{display_name}' (name {result['page_name']}).\n"
                    f"Path: {result['path']}\n" + self._RELOAD_HINT)
        except Exception as e:
            return f"Error adding page: {redact_secrets(str(e), [self.client_secret])}"

    async def _handle_pbir_add_visual(self, args: Dict[str, Any]) -> str:
        """Add a visual to a page in the loaded PBIR-Enhanced project."""
        try:
            connector = self._get_pbip_connector()
            if not connector.current_project:
                return "No PBIP project loaded. Use 'pbip_load_project' first."
            page = args.get("page")
            visual_type = args.get("visual_type")
            if not page or not visual_type:
                return "Error: page and visual_type are required"
            fn = lambda: connector.add_visual(
                page, visual_type, position=args.get("position"),
                fields_by_role=args.get("fields"), skip_validation=bool(args.get("skip_validation", False)))
            result = await asyncio.get_event_loop().run_in_executor(None, fn)
            if not result.get("success"):
                msg = result.get("message", "failed")
                if result.get("missing_fields"):
                    msg += "\nMissing fields: " + ", ".join(result["missing_fields"])
                return f"Failed to add visual: {msg}"
            return (f"Added {visual_type} '{result['visual_name']}' to page '{result['page']}'.\n"
                    f"Path: {result['path']}\n" + self._RELOAD_HINT)
        except Exception as e:
            return f"Error adding visual: {redact_secrets(str(e), [self.client_secret])}"

    async def _handle_pbir_bind_fields(self, args: Dict[str, Any]) -> str:
        """Add or replace field bindings on an existing visual."""
        try:
            connector = self._get_pbip_connector()
            if not connector.current_project:
                return "No PBIP project loaded. Use 'pbip_load_project' first."
            page = args.get("page")
            visual_name = args.get("visual_name")
            fields = args.get("fields")
            if not (page and visual_name and fields):
                return "Error: page, visual_name, and fields are required"
            fn = lambda: connector.bind_fields(
                page, visual_name, fields, mode=(args.get("mode") or "add"),
                skip_validation=bool(args.get("skip_validation", False)))
            result = await asyncio.get_event_loop().run_in_executor(None, fn)
            if not result.get("success"):
                msg = result.get("message", "failed")
                if result.get("missing_fields"):
                    msg += "\nMissing fields: " + ", ".join(result["missing_fields"])
                return f"Failed to bind fields: {msg}"
            return f"Bound fields on visual '{visual_name}' (page '{result['page']}', mode {result['mode']})."
        except Exception as e:
            return f"Error binding fields: {redact_secrets(str(e), [self.client_secret])}"

    async def _handle_pbir_validate_report(self) -> str:
        """Validate that every report field binding exists in the model (catches blank visuals)."""
        try:
            connector = self._get_pbip_connector()
            if not connector.current_project:
                return "No PBIP project loaded. Use 'pbip_load_project' first."
            fn = lambda: connector.validate_report_bindings()
            errors = await asyncio.get_event_loop().run_in_executor(None, fn)
            out = "=== Report Binding Validation ===\n\n"
            if not errors:
                out += "All report field bindings resolve to existing model tables/columns/measures.\n"
                return out
            out += f"Found {len(errors)} binding issue(s) (these render blank or trigger repair):\n\n"
            for e in errors[:100]:
                out += f"  [{e.error_type}] {e.message}\n"
            return out
        except Exception as e:
            return f"Error validating report bindings: {redact_secrets(str(e), [self.client_secret])}"

    @staticmethod
    def _is_zero(value: Any) -> bool:
        """True for a numeric zero however the connector delivered it (0, 0.0, "0", "0.00")."""
        try:
            return float(value) == 0.0
        except (TypeError, ValueError):
            return False

    async def _validate_measure_batch(self, table: str, measures: List[Dict[str, Any]]) -> Optional[str]:
        """Validate a batch of new measures BEFORE any of them is created. Returns an error
        message, or None when the batch is valid (or cannot be validated: Desktop not connected).

        All measures of the batch, plus any still pending in an open transaction, are declared
        with DEFINE MEASURE in one query and evaluated together, so a measure may reference a
        sibling from the same batch and still be checked up front. When the combined probe
        fails, each measure is re-checked with only its own dependencies defined, smallest
        dependency set first, so the message names the measure that is actually broken rather
        than one that merely depends on it.
        """
        batch = [{"table": table, "name": m["name"], "expression": m.get("expression") or ""} for m in measures]
        defined = list(self._tom_pending_measures) + batch
        columns = ", ".join(f'"v{i}", {dax_tokens.bracket(m["name"])}' for i, m in enumerate(batch))
        probe = define_measures_block(defined) + f"EVALUATE ROW({columns})"
        _rows, _report, error = await self._run_guarded_query(probe, tool="validate", max_rows=1)
        if error is None or error.startswith("Not connected"):
            return None
        if error.startswith(("Refused:", "Query blocked")):
            return f"[BLOCKED] Batch NOT created. {error}"

        by_name = {m["name"].casefold(): m for m in defined}

        def closure(m: Dict[str, str]) -> List[Dict[str, str]]:
            seen: Dict[str, Dict[str, str]] = {}
            stack = [m]
            while stack:
                cur = stack.pop()
                key = cur["name"].casefold()
                if key in seen:
                    continue
                seen[key] = cur
                for r in dax_tokens.references(cur["expression"]):
                    dep = by_name.get((r.name or "").casefold()) if r.kind == "bare" else None
                    if dep is not None:
                        stack.append(dep)
            return list(seen.values())

        closures = sorted(((closure(m), m) for m in batch), key=lambda pair: len(pair[0]))
        for needed, m in closures:
            single = define_measures_block(needed) + f'EVALUATE ROW("validation", {dax_tokens.bracket(m["name"])})'
            _rows, _report, err1 = await self._run_guarded_query(single, tool="validate", max_rows=1)
            if err1:
                detail = err1[len("Error executing DAX: "):] if err1.startswith("Error executing DAX: ") else err1
                return (f"[INVALID] Batch NOT created - measure '{m['name']}' failed validation.\n\n"
                        f"Error: {detail}\n\nFix the DAX and retry (or pass skip_validation=true).")
        detail = error[len("Error executing DAX: "):] if error.startswith("Error executing DAX: ") else error
        return ("[INVALID] Batch NOT created - the measures failed validation together.\n\n"
                f"Error: {detail}\n\nFix the DAX and retry (or pass skip_validation=true).")

    async def _run_workspace_scan(self, rest, args: Dict[str, Any]):
        """Run (or resume) Admin Scanner scans and merge their results.

        Returns (scan, error, notes). A tenant with more than 100 workspaces needs several
        scans; they are started together and polled within one time budget. If the budget runs
        out, the error message carries every scan id so the caller can resume with ``scan_ids``
        (the service keeps a finished scan's result for 24 hours).
        """
        loop = asyncio.get_event_loop()
        notes: List[str] = []
        scan_ids = [s for s in (args.get("scan_ids") or []) if s]
        if not scan_ids:
            ids = args.get("workspace_ids")
            if not ids:
                try:
                    listed = await loop.run_in_executor(None, rest.admin_list_workspaces)
                except Exception as e:
                    return None, ("Error listing workspaces (needs admin / read-only admin APIs enabled): "
                                  f"{self._redact(str(e))}"), notes
                ids = [w.get("id") for w in listed if w.get("id")]
            ids = list(dict.fromkeys(i for i in (ids or []) if i))
            if not ids:
                return None, "No workspaces found to scan.", notes
            try:
                limit = max(1, int(args.get("max_workspaces", 500) or 500))
            except (TypeError, ValueError):
                limit = 500
            if len(ids) > limit:
                notes.append(f"Scanned the first {limit} of {len(ids)} workspaces. Raise max_workspaces or "
                             "pass workspace_ids to cover the rest; the inventory below is partial.")
                ids = ids[:limit]
            start_many = getattr(rest, "admin_start_scans", None)
            if callable(start_many):
                # dataset_schema=True: without it the Scanner omits roles and every dataset
                # would read as "RLS unknown".
                started_ids = await loop.run_in_executor(
                    None, lambda: start_many(ids, lineage=True, dataset_schema=True))
                scan_ids.extend(i for i in (started_ids or []) if i)
                if not scan_ids:
                    return None, "Error: no scan was started.", notes
            else:
                for start in range(0, len(ids), self._SCAN_CHUNK):
                    started = await loop.run_in_executor(
                        None, rest.admin_post_workspace_info, ids[start:start + self._SCAN_CHUNK], True)
                    scan_id = (started or {}).get("id")
                    if not scan_id:
                        return None, f"Error: scan did not start ({started}).", notes
                    scan_ids.append(scan_id)

        try:
            raw_budget = args.get("max_wait_seconds")
            budget = 240.0 if raw_budget is None else max(0.0, float(raw_budget))
        except (TypeError, ValueError):
            budget = 240.0
        deadline = time.time() + budget
        merged: Dict[str, Any] = {"workspaces": []}
        unfinished = []
        for scan_id in scan_ids:
            status = ""
            last: Dict[str, Any] = {}
            while True:
                last = await loop.run_in_executor(None, rest.admin_get_scan_status, scan_id) or {}
                status = str(last.get("status", "")).lower()
                if status in ("succeeded", "failed") or time.time() >= deadline:
                    break
                await asyncio.sleep(min(self._SCAN_POLL_SECONDS, max(0.0, deadline - time.time())))
            if status == "succeeded":
                result = await loop.run_in_executor(None, rest.admin_get_scan_result, scan_id) or {}
                merged["workspaces"].extend(result.get("workspaces") or [])
            elif status == "failed":
                return None, f"Scan {scan_id} failed: {last.get('error') or last}", notes
            else:
                unfinished.append(scan_id)
        if unfinished:
            return None, (f"{len(unfinished)} of {len(scan_ids)} scan(s) were still running after {budget:.0f}s. "
                          "Call again with scan_ids set to the list below to pick up the results without "
                          "starting new scans (results are kept for 24 hours):\n"
                          f"  scan_ids: {json.dumps(scan_ids)}"), notes
        return merged, None, notes

    async def _handle_pbip_stage_publish(self, args: Dict[str, Any]):
        """Stage a report (and optionally its model) for an in-place publish. Returns (text, result)."""
        try:
            project = self.pbip_connector.current_project if self.pbip_connector else None
            report_dir = args.get("report_dir") or (
                str(project.report_folder) if project and project.report_folder else None)
            if not report_dir:
                msg = "No report to stage: load a PBIP project (pbip_load_project) or pass report_dir."
                return (msg, {"report": {}, "model": None, "error": msg})
            stage_dir = args.get("stage_dir")
            if not stage_dir:
                return ("Error: stage_dir is required", {"report": {}, "model": None, "error": "stage_dir is required"})
            overwrite = bool(args.get("overwrite", False))
            loop = asyncio.get_event_loop()

            model_result = None
            if args.get("include_model"):
                model_dir = args.get("model_dir") or (
                    str(project.semantic_model_folder) if project and project.semantic_model_folder else None)
                if not model_dir:
                    msg = "include_model was set but there is no model folder: load a project or pass model_dir."
                    return (msg, {"report": {}, "model": None, "error": msg})
                model_result = await loop.run_in_executor(None, lambda: publish_stage.stage_model(
                    model_dir, stage_dir, display_name=args.get("model_name"), overwrite=overwrite))

            report_result = await loop.run_in_executor(None, lambda: publish_stage.stage_report(
                report_dir, stage_dir, workspace=args.get("workspace"), model_name=args.get("model_name"),
                model_id=args.get("model_id"), display_name=args.get("display_name"), overwrite=overwrite))

            out = "=== Staged for publish ===\n\n"
            if model_result:
                out += "Model:\n  " + publish_stage.describe(model_result).replace("\n", "\n  ") + "\n\n"
            out += "Report:\n  " + publish_stage.describe(report_result).replace("\n", "\n  ") + "\n\n"
            out += ("Nothing was sent to the service. Check `fab auth status` shows the intended tenant, import "
                    "the model first when it changed, then the report. Importing over an existing report "
                    "keeps its id and its links.")
            return (out, {"report": report_result, "model": model_result, "error": None})
        except publish_stage.StageError as e:
            return (f"Error: {e}", {"report": {}, "model": None, "error": str(e)})
        except Exception as e:
            msg = self._redact(str(e))
            return (f"Error staging for publish: {msg}", {"report": {}, "model": None, "error": msg})

    def _pbip_model_fields(self) -> Optional[Dict[str, Any]]:
        """The loaded project's model fields with kinds and data types, for validating report
        field references. None when no project (or no model) is loaded."""
        connector = self.pbip_connector
        if not connector or not connector.current_project or not connector.current_project.semantic_model_folder:
            return None
        index = getattr(connector, "_model_field_index", None)
        try:
            return index() if callable(index) else connector.model_field_catalog()
        except Exception as e:
            logger.warning(f"Could not read the model's fields: {self._redact(str(e))}")
            return None

    async def _handle_pbir_apply_spec(self, args: Dict[str, Any]):
        """Build or update report pages from a declarative spec (report as code). Returns (text, result)."""
        empty = {"success": False, "written": [], "removed": [], "unchanged": 0, "warnings": [], "errors": []}
        try:
            connector = self._get_pbip_connector()
            project = connector.current_project
            if not project or not project.report_folder:
                msg = "No PBIP project with a report is loaded. Use 'pbip_load_project' first."
                return (msg, dict(empty, errors=[msg]))
            spec = args.get("spec")
            spec_path = args.get("spec_path")
            if spec is None and spec_path:
                try:
                    spec = safe_io.read_json(spec_path)
                except Exception as e:
                    msg = f"Could not read spec_path: {e}"
                    return (msg, dict(empty, errors=[msg]))
            if isinstance(spec, str):
                try:
                    spec = json.loads(spec)
                except ValueError as e:
                    msg = f"'spec' is not valid JSON: {e}"
                    return (msg, dict(empty, errors=[msg]))
            if not isinstance(spec, dict):
                msg = "Provide 'spec' (an object) or 'spec_path' (a JSON file)."
                return (msg, dict(empty, errors=[msg]))

            dry_run = bool(args.get("dry_run", False))
            prune = bool(args.get("prune", False))
            loop = asyncio.get_event_loop()
            model_fields = self._pbip_model_fields() if not args.get("skip_field_validation") else None

            backup = None
            if not dry_run and args.get("backup", True):
                backup = await loop.run_in_executor(None, connector.create_backup)

            apply_fn = lambda: report_spec.apply_report_spec(  # noqa: E731
                str(project.report_folder), spec, model_fields=model_fields, prune=prune,
                dry_run=dry_run, adopt=bool(args.get("adopt", False)))
            result = await loop.run_in_executor(None, apply_fn)

            if result.get("success") and not dry_run:
                # The connector caches the list of visual files; pick up what was just written.
                await loop.run_in_executor(None, connector.load_project, str(project.pbip_file))

            verb = "Would write" if dry_run else "Wrote"
            out = f"=== Report spec {'(dry run) ' if dry_run else ''}===\n\n"
            if not result.get("success"):
                out += f"NOT applied: {len(result.get('errors', []))} problem(s). Nothing was written.\n\n"
                for e in result.get("errors", [])[:60]:
                    out += f"  [ERROR] {e}\n"
            else:
                out += (f"{verb} {len(result.get('written', []))} file(s), "
                        f"{'would remove' if dry_run else 'removed'} {len(result.get('removed', []))}, "
                        f"{result.get('unchanged', 0)} already up to date.\n")
                for p in result.get("written", [])[:40]:
                    out += f"  + {p}\n"
                if len(result.get("written", [])) > 40:
                    out += f"  ... and {len(result['written']) - 40} more\n"
                for p in result.get("removed", [])[:40]:
                    out += f"  - {p}\n"
            for w in result.get("warnings", [])[:40]:
                out += f"  [WARN] {w}\n"
            if backup:
                out += f"\nBackup of the project before this change: {backup}\n"
            if result.get("success") and not dry_run and result.get("written"):
                out += ("\nReload the open report with bridge_reload (never over unsaved changes), then check it "
                        "with bridge_screenshot or audit_report_design.")
            result = dict(result)
            result["backup"] = str(backup) if backup else None
            return (out, result)
        except Exception as e:
            msg = self._redact(str(e))
            return (f"Error applying report spec: {msg}", dict(empty, errors=[msg]))

    @property
    def _tom_transaction_active(self) -> bool:
        """True while a TOM transaction is open. The connector owns that state, so a failed
        commit or a reconnect cannot leave the server and the model disagreeing about it."""
        tom = self.tom_connector
        return bool(tom is not None and getattr(tom, "in_transaction", False))

    async def _tom_save(self, tom):
        """Persist one live edit. Returns (ok, note): inside a transaction nothing is saved and
        the note says the edit is pending; otherwise the note is empty or carries the error."""
        if self._tom_transaction_active:
            return True, " (PENDING - run tom_commit_transaction to save)"
        result = await asyncio.get_event_loop().run_in_executor(None, tom.save_changes)
        if getattr(result, "success", False):
            return True, ""
        return False, self._redact(getattr(result, "message", "save failed"))

    async def _suite_model_defaults(self, kwargs: Dict[str, Any]) -> Dict[str, Any]:
        """Fill generator options the connected model can answer better than a default: the
        base measure's own format string, and each dimension's sort-by column (without which
        a share-of-total or rank over a sorted column silently returns 100% / 1 on every row).
        Leaves explicit arguments alone and does nothing when no Desktop model is connected."""
        desktop = self.desktop_connector
        if desktop is None or not getattr(desktop, "current_port", None):
            return kwargs
        needs_format = "base_measure" in kwargs and "date_column" in kwargs and not kwargs.get("base_format")
        needs_sort = "dimension_columns" in kwargs and not kwargs.get("sort_by_columns")
        if not (needs_format or needs_sort):
            return kwargs
        try:
            model, err = await self._gather_model_metadata("desktop")
            if err or not model:
                return kwargs
            if needs_format:
                fmt = dax_generator.measure_format_from_model(model, kwargs["base_measure"])
                if fmt:
                    kwargs["base_format"] = fmt
            if needs_sort:
                mapping = dax_generator.sort_by_columns_from_model(model)
                if mapping:
                    kwargs["sort_by_columns"] = mapping
        except Exception as e:
            logger.warning(f"Could not read generator defaults from the model: {self._redact(str(e))}")
        return kwargs

    async def _handle_generate_payload_measure(self, args: Dict[str, Any]):
        """Generate a row-packing text measure and its JavaScript parser. Returns (text, result)."""
        try:
            name = args.get("name")
            if not name:
                return ("Error: name is required", {"error": "name is required"})
            common = {k: args[k] for k in ("format", "record_sep", "field_sep", "display_folder", "expected_rows")
                      if args.get(k) is not None}
            if args.get("dimensions"):
                result = payload_gen.generate_dimension_payload(name, args["dimensions"], **common)
            else:
                if not args.get("table") or not args.get("fields"):
                    msg = "Provide table and fields (fact payload), or dimensions (lookup tables)."
                    return (f"Error: {msg}", {"error": msg})
                extra = {k: args[k] for k in ("order_by", "where", "grain") if args.get(k) is not None}
                result = payload_gen.generate_payload_measure(name, args["table"], args["fields"], **common, **extra)
            budget = result.get("budget") or {}
            out = f"=== Payload measure: {result['name']} ({result.get('format')}) ===\n\n{result['dax']}\n\n"
            out += f"--- JavaScript parser ({result.get('parser_name')}) ---\n{result['js_parser']}\n\n"
            if budget:
                out += (f"Budget: about {budget.get('estimated_chars_per_row')} characters per row; "
                        f"roughly {budget.get('max_rows_at_ceiling'):,} rows fit under the "
                        f"{budget.get('ceiling'):,}-character ceiling.\n"
                        if isinstance(budget.get("max_rows_at_ceiling"), int) else f"Budget: {budget}\n")
            notes = result.get("notes")
            for n in (notes if isinstance(notes, list) else [notes] if notes else []):
                out += f"Note: {n}\n"
            result = dict(result, error=None)
            return (out, result)
        except ValueError as e:
            return (f"Error: {e}", {"error": str(e)})
        except Exception as e:
            msg = self._redact(str(e))
            return (f"Error generating payload measure: {msg}", {"error": msg})

    async def _handle_generate_html_measure(self, args: Dict[str, Any]):
        """Generate an HTML Content measure (no JavaScript). Returns (text, result)."""
        try:
            kind = args.get("kind")
            if not kind:
                return ("Error: kind is required", {"error": "kind is required"})
            params = dict(args.get("params") or {})
            # Accept the parameters at the top level too, so a caller need not nest them.
            for key, value in args.items():
                if key not in ("kind", "name", "params") and value is not None:
                    params.setdefault(key, value)
            result = html_measures.generate(kind, name=args.get("name"), **params)
            out = f"=== HTML measure: {result['name']} ({result['kind']}) ===\n\n{result['dax']}\n\n{result['notes']}\n"
            return (out, dict(result, expression=result["dax"], error=None))
        except (ValueError, TypeError) as e:
            return (f"Error: {e}", {"error": str(e)})
        except Exception as e:
            msg = self._redact(str(e))
            return (f"Error generating HTML measure: {msg}", {"error": msg})

    async def _handle_build_js_app_measure(self, args: Dict[str, Any]):
        """Assemble CSS + JavaScript modules into measures for the HTML Content visual. Returns (text, result)."""
        empty = {"measures": [], "sizes": {}, "warnings": [], "node_check": "skipped", "written": False}
        try:
            prefix = args.get("prefix")
            if not prefix:
                return ("Error: prefix is required", dict(empty, error="prefix is required"))
            target = (args.get("target") or "none").lower()
            if target not in ("none", "pbip"):
                return (f"Error: unknown target '{target}'.", dict(empty, error="bad_target"))
            if target == "pbip" and self._read_only:
                msg = ("Refused: writing measures is a write operation and the server is in READ-ONLY mode. "
                       "Use target='none' to build without writing.")
                return (msg, dict(empty, error="read_only"))

            css = args.get("css")
            if css is None and args.get("css_path"):
                css, _fmt = safe_io.read_text(args["css_path"])
            modules = args.get("js_dir")
            if not modules:
                modules = [(m.get("name") or f"module{i}", m.get("source") or "")
                           for i, m in enumerate(args.get("js_modules") or [])]
            if not modules:
                msg = "Provide js_modules ([{name, source}]) or js_dir (a folder of .js files)."
                return (f"Error: {msg}", dict(empty, error=msg))

            loop = asyncio.get_event_loop()
            build_fn = lambda: js_app_measure.build_js_app_measures(  # noqa: E731
                prefix, css=css or "", js_modules=modules, payload_measures=args.get("payload_measures"),
                root_id=args.get("root_id") or "app-root",
                empty_message=args.get("empty_message") or "No data for the current filters.",
                minify=bool(args.get("minify", True)), display_folder=args.get("display_folder"))
            built = await loop.run_in_executor(None, build_fn)

            written = False
            write_note = "Not written (target='none')."
            if target == "pbip":
                table = args.get("table")
                if not table:
                    return ("Error: 'table' is required for target='pbip'.", dict(empty, error="table_required"))
                connector = self._get_pbip_connector()
                if not connector.current_project:
                    return ("No PBIP project loaded. Use 'pbip_load_project' first.", dict(empty, error="no_project"))
                res = await loop.run_in_executor(
                    None, lambda: connector.add_measures(table, built["measures"], mode="upsert"))
                if not res.get("success"):
                    msg = res.get("message") or "write failed"
                    return (f"Built but NOT written: {msg}", dict(empty, sizes=built.get("sizes", {}), error=msg))
                written = True
                write_note = (f"Written into {res.get('path')}: created {len(res.get('created', []))}, "
                              f"updated {len(res.get('updated', []))}. Reload the report with bridge_reload.")

            sizes = built.get("sizes") or {}
            out = f"=== JavaScript app measures: {prefix} ===\n\n"
            for m in built["measures"]:
                out += f"  {m['name']}: {len(m['expression']):,} characters\n"
            if isinstance(sizes.get("total"), int) and sizes.get("ceiling"):
                out += f"  total: {sizes['total']:,} characters ({100.0 * sizes['total'] / sizes['ceiling']:.1f}% of the ceiling)\n"
            out += f"\nNode syntax check: {built.get('node_check')}\n"
            for w in built.get("warnings", []):
                out += f"  [WARN] {w}\n"
            out += f"\n{write_note}\n"
            measures = built["measures"] if args.get("return_expressions") else [
                {"name": m["name"], "chars": len(m["expression"]), "display_folder": m.get("display_folder")}
                for m in built["measures"]]
            return (out, {"measures": measures, "sizes": sizes, "warnings": built.get("warnings", []),
                          "node_check": built.get("node_check"), "written": written, "error": None})
        except ValueError as e:
            return (f"Build failed: {e}", dict(empty, error=str(e)))
        except Exception as e:
            msg = self._redact(str(e))
            return (f"Error building the app measures: {msg}", dict(empty, error=msg))

    async def _handle_pbip_embed_data(self, args: Dict[str, Any]):
        """Embed CSV data into the loaded model's partitions (dry run by default). Returns (text, result)."""
        empty = {"success": False, "dry_run": True, "tables": [], "self_contained": False, "changed_files": []}
        try:
            connector = self._get_pbip_connector()
            project = connector.current_project
            if not project or not project.semantic_model_folder:
                msg = "No PBIP project with a semantic model is loaded. Use 'pbip_load_project' first."
                return (msg, dict(empty, error=msg))
            tables = args.get("tables")
            if not isinstance(tables, dict) or not tables:
                msg = "tables is required: {table name: path of a CSV file}."
                return (f"Error: {msg}", dict(empty, error=msg))
            dry_run = bool(args.get("dry_run", True))
            options = {"dry_run": dry_run}
            if args.get("max_bytes"):
                options["max_bytes"] = int(args["max_bytes"])
            if args.get("delimiter"):
                options["delimiter"] = args["delimiter"]
            loop = asyncio.get_event_loop()
            backup = None
            if not dry_run and args.get("backup", True):
                backup = await loop.run_in_executor(None, connector.create_backup)
            result = await loop.run_in_executor(
                None, lambda: model_embed.embed_tables(str(project.semantic_model_folder), tables, **options))
            if result.get("success") and not dry_run:
                await loop.run_in_executor(None, connector.load_project, str(project.pbip_file))

            out = f"=== Embed data {'(dry run) ' if dry_run else ''}===\n\n"
            for t in result.get("tables", []):
                out += (f"  {t.get('table')}: {t.get('rows'):,} rows, {t.get('columns')} columns, "
                        f"CSV {t.get('csv_bytes'):,} bytes -> {t.get('embedded_chars'):,} characters embedded\n")
                for w in t.get("warnings", []):
                    out += f"      [WARN] {w}\n"
            for name in result.get("removed_expressions", []):
                out += f"  parameter removed: {name}\n"
            for kept in result.get("kept_expressions", []):
                out += f"  parameter kept: {kept.get('name')} ({kept.get('reason')})\n"
            for ext in result.get("external_sources", []):
                out += f"  still reads from outside: {ext.get('object')} ({', '.join(ext.get('functions', []))})\n"
            out += f"\nSelf-contained after this change: {result.get('self_contained')}\n"
            if dry_run:
                out += "Nothing was written. Run again with dry_run=false to apply.\n"
            else:
                out += (f"Changed {len(result.get('changed_files', []))} file(s). Publishing a model definition "
                        "uploads no data: refresh once in the service after importing it.\n")
                if backup:
                    out += f"Backup of the project before this change: {backup}\n"
            notes = result.get("notes")
            for n in (notes if isinstance(notes, list) else [notes] if notes else []):
                out += f"Note: {n}\n"
            return (out, dict(result, error=None))
        except ValueError as e:
            return (f"Error: {e}", dict(empty, error=str(e)))
        except Exception as e:
            msg = self._redact(str(e))
            return (f"Error embedding data: {msg}", dict(empty, error=msg))

    async def _handle_audit_report_design(self, args: Dict[str, Any]):
        """Audit the report layer (structure, design, governance). Returns (text, result)."""
        try:
            report_dir = args.get("report_dir")
            model_fields = None
            if not report_dir:
                connector = self.pbip_connector
                project = connector.current_project if connector else None
                if not project or not project.report_folder:
                    msg = "No report to audit: load a PBIP project (pbip_load_project) or pass report_dir."
                    return (msg, {"summary": {}, "findings": [], "error": msg})
                report_dir = str(project.report_folder)
                index = self._pbip_model_fields()
                if index:
                    model_fields = {table: (list(fields) if not isinstance(fields, dict) else list(fields.keys()))
                                    for table, fields in index.items()}
            options = {k: int(args[k]) for k in ("max_data_visuals_per_page", "max_visuals_per_page")
                       if args.get(k) is not None}
            result = await asyncio.get_event_loop().run_in_executor(
                None, lambda: report_audit.audit_report(report_dir, model_fields=model_fields, **options))
            rank = {"info": 1, "warning": 2, "error": 3}
            floor = rank.get((args.get("min_severity") or "info").lower(), 1)
            findings = [f for f in result.get("findings", []) if rank.get(f.get("severity"), 1) >= floor]
            s = result.get("summary", {})
            counts: Dict[str, int] = {}
            for f in findings:
                counts[f.get("severity", "info")] = counts.get(f.get("severity", "info"), 0) + 1
            out = "=== Report Audit ===\n\n"
            out += f"Pages: {s.get('pages')}   Visuals: {s.get('visuals')}   Bookmarks: {s.get('bookmarks')}\n"
            out += (f"Findings: {len(findings)}  (errors: {counts.get('error', 0)}, warnings: "
                    f"{counts.get('warning', 0)}, info: {counts.get('info', 0)})\n")
            if s.get("field_check"):
                out += f"Field check: {s.get('field_check')}\n"
            out += "\n"
            for f in findings[:200]:
                where = " / ".join(str(x) for x in (f.get("page"), f.get("visual")) if x)
                out += f"  [{str(f.get('severity', 'info')).upper()}] {f.get('code')}{(' ' + where) if where else ''}: {f.get('message')}\n"
                if f.get("recommendation"):
                    out += f"      fix: {f['recommendation']}\n"
            if len(findings) > 200:
                out += f"\n... and {len(findings) - 200} more (raise min_severity to narrow).\n"
            if not findings:
                out += "  No findings at this severity.\n"
            return (out, {"summary": s, "findings": findings, "error": None})
        except ValueError as e:
            return (f"Error: {e}", {"summary": {}, "findings": [], "error": str(e)})
        except Exception as e:
            msg = self._redact(str(e))
            return (f"Error auditing the report: {msg}", {"summary": {}, "findings": [], "error": msg})

    def _render_rename_result(self, title: str, result) -> str:
        """One honest rendering for every rename tool: what was written, or that nothing was."""
        response = f"=== {title} ===\n\n"
        if result.success:
            response += f"{result.message}\n\n"
            if result.files_modified:
                response += "--- Files Modified ---\n"
                for f in result.files_modified[:10]:
                    response += f"  - {f}\n"
                if len(result.files_modified) > 10:
                    response += f"  ... and {len(result.files_modified) - 10} more\n"
                response += "\n"
            response += f"Total references updated: {result.references_updated}\n"
            if result.backup_created:
                response += f"Backup of the project before this change: {result.backup_created}\n"
            warnings = list(getattr(result, "warnings", None) or [])
            if warnings:
                response += f"\n--- Check these by hand ({len(warnings)}) ---\n"
                for w in warnings[:20]:
                    response += f"  ! {w}\n"
                if len(warnings) > 20:
                    response += f"  ... and {len(warnings) - 20} more\n"
            response += ("\nThe model and the report were updated together. Confirm with pbir_validate_report. "
                         "pbip_undo restores the files as they were before this change.\n" + self._RELOAD_HINT + "\n")
            return response

        response += f"NOT RENAMED. Nothing was written.\n\n{result.message}\n"
        if result.validation_errors:
            response += f"\n--- Why ({len(result.validation_errors)}) ---\n"
            for err in result.validation_errors[:10]:
                response += f"  [{err.error_type}] {err.file_path}:{err.line_number}\n    {err.message}\n"
            if len(result.validation_errors) > 10:
                response += f"  ... and {len(result.validation_errors) - 10} more\n"
        return response

    async def _run_pbip_rename(self, title: str, method: str, args: Dict[str, Any]) -> str:
        try:
            connector = self._get_pbip_connector()
            if not connector.current_project:
                return "No PBIP project loaded. Use 'pbip_load_project' first."
            renames = args.get("renames", [])
            if not renames:
                return "Error: 'renames' array is required"
            batch_fn = lambda: getattr(connector, method)(renames)  # noqa: E731
            result = await asyncio.get_event_loop().run_in_executor(None, batch_fn)
            return self._render_rename_result(title, result)
        except Exception as e:
            logger.error(f"{title} error: {self._redact(str(e))}")
            return f"Error: {self._redact(str(e))}"

    async def _handle_pbip_undo(self, args: Dict[str, Any]) -> str:
        """Undo the last committed rename, repair or quoting fix on the loaded project."""
        try:
            connector = self._get_pbip_connector()
            if not connector.current_project:
                return "No PBIP project loaded. Use 'pbip_load_project' first."
            ok = await asyncio.get_event_loop().run_in_executor(None, connector.rollback_changes)
            if ok:
                return ("Undone: every file changed by the last rename or repair was restored to its "
                        "previous bytes.\n" + self._RELOAD_HINT)
            reason = getattr(connector, "last_error", None)
            return "Nothing was undone" + (f": {reason}" if reason else ": there is no change to undo in this session.")
        except Exception as e:
            return f"Error undoing the last change: {self._redact(str(e))}"

    async def run(self):
        """Run the MCP server"""
        async with stdio_server() as (read_stream, write_stream):
            logger.info("Power BI MCP Server V2 starting...")
            logger.info("Supports: Power BI Desktop (local) + Power BI Service (cloud)")
            await self.server.run(
                read_stream,
                write_stream,
                InitializationOptions(
                    server_name="powerbi-mcp-v2",
                    server_version="2.0.0",
                    capabilities=self.server.get_capabilities(
                        notification_options=NotificationOptions(),
                        experimental_capabilities={}
                    )
                )
            )


def main():
    """Main entry point"""
    server = PowerBIMCPServer()
    asyncio.run(server.run())


if __name__ == "__main__":
    main()
