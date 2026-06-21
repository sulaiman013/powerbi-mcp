"""
Power BI MCP Server V2
Supports both Power BI Service (Cloud) and Power BI Desktop (Local)
Features: PII Detection, Audit Logging, Access Policies
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
logger = logging.getLogger("powerbi-mcp-v2")


def redact_secrets(text: Any, extra_secrets: Optional[List[str]] = None) -> str:
    """Redact connection-string secrets and known secret values before logging or returning to the client.

    Power BI cloud connectors embed the service-principal client secret directly in the
    ADOMD/MSOLAP connection string (Password=...). Exception messages and verbose argument
    logs can therefore leak credentials. This masks the common vectors.
    """
    if text is None:
        return ""
    s = str(text)
    # Connection-string / URL style secrets
    s = re.sub(r"(?i)(password\s*=\s*)[^;]+", r"\1***", s)
    s = re.sub(r"(?i)(client_secret\s*=\s*)[^;&\s]+", r"\1***", s)
    s = re.sub(r"(?i)(\bsecret\s*=\s*)[^;&\s]+", r"\1***", s)
    # Known literal secret values (e.g. the configured client secret)
    for sec in (extra_secrets or []):
        if sec and len(sec) >= 6:
            s = s.replace(sec, "***")
    return s


def build_validation_probe(dax: str, as_measure: bool = False) -> str:
    """Wrap a DAX expression/query into an executable probe used to validate it.

    A full query (starts with EVALUATE/DEFINE) is run as-is; a scalar measure
    expression is wrapped in EVALUATE ROW(...) so the engine parses and evaluates it.
    Executing the probe with a tiny row cap surfaces syntax/semantic errors without
    materializing real data.
    """
    stripped = (dax or "").strip()
    upper = stripped.upper()
    if not as_measure and (upper.startswith("EVALUATE") or upper.startswith("DEFINE")):
        return stripped
    return f'EVALUATE ROW("validation", {stripped})'


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
import bpa_authoring

# Import security layer
from security import SecurityLayer, get_security_layer
from security.access_policy import AccessPolicyEngine


class PowerBIMCPServer:
    """Power BI MCP Server supporting Cloud and Desktop connectivity"""

    def __init__(self):
        self.server = Server("powerbi-mcp-v2")

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

        # When a TOM transaction is open, write tools defer SaveChanges until commit.
        self._tom_transaction_active = False

        # Initialize security layer
        config_path = Path(__file__).parent.parent / "config" / "policies.yaml"
        self.security = SecurityLayer(
            config_path=str(config_path) if config_path.exists() else None,
            enable_pii_detection=os.getenv("ENABLE_PII_DETECTION", "true").lower() == "true",
            enable_audit=os.getenv("ENABLE_AUDIT", "true").lower() == "true",
            enable_policies=os.getenv("ENABLE_POLICIES", "true").lower() == "true"
        )

        # Single source of truth for call routing and MCP safety hints. New tools
        # register here (dispatch + annotations) in addition to handle_list_tools.
        self._tool_dispatch = self._build_tool_dispatch()
        self._tool_annotations = self._build_tool_annotations()
        self._prompts = self._build_prompts()

        # Read-only / lockdown mode: when POWERBI_MCP_READONLY=true, every write tool is
        # refused. Write tools = destructive ops plus the non-destructive creates/commit.
        self._read_only = os.getenv("POWERBI_MCP_READONLY", "false").lower() == "true"
        self._write_tools = (
            {n for n, a in self._tool_annotations.items() if a.destructiveHint}
            | {"create_measure", "create_relationship", "tom_commit_transaction"}
        )

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
            "pbip_fix_dax_quoting": lambda a: self._handle_pbip_fix_dax_quoting(),
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
            "cross_workspace_lineage": cloud_read,
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
        }

    def _setup_handlers(self):
        """Set up MCP tool handlers"""

        @self.server.list_tools()
        async def handle_list_tools() -> List[Tool]:
            """Return list of available tools"""
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
                                        "new_name": {"type": "string", "description": "New measure name"}
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
                    description="Fix all DAX expressions by properly quoting table names with spaces. Fixes: Leads Sales Data[Amount] -> 'Leads Sales Data'[Amount]",
                    inputSchema={
                        "type": "object",
                        "properties": {},
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
                            "as_measure": {"type": "boolean", "description": "Treat 'dax' as a scalar measure expression (wraps it in EVALUATE ROW for validation). Default false.", "default": False},
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
                    description="For the auto-fixable DAX anti-patterns (bare '/' -> DIVIDE, FILTER(whole table) -> boolean filter, SUMMARIZE-aggregation -> SUMMARIZECOLUMNS), return concrete before/after rewrite hints for a raw expression or a named measure. Pair with validate_dax to confirm the rewrite is still valid.",
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
                    description="Audit naming conventions across the connected model's tables, columns, and measures and return a rename PLAN (old -> new with reasons): snake_case/camelCase to spaced Title Case, strip warehouse DIM_/FACT_ prefixes, trim whitespace, optionally expand abbreviations. Apply the plan with the rename tools (batch_rename_* live, or pbip_rename_* for model + report).",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "source": {"type": "string", "enum": ["desktop", "cloud"], "default": "desktop"},
                            "scope": {"type": "array", "items": {"type": "string", "enum": ["tables", "columns", "measures"]}, "description": "Which object types to audit (default all)"},
                            "target_case": {"type": "string", "enum": ["title", "none"], "default": "title"},
                            "strip_warehouse_prefixes": {"type": "boolean", "default": True},
                            "expand_abbreviations": {"type": "boolean", "default": False},
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
                            "decode_layout": {"type": "boolean", "default": True, "description": "Also write a decoded Report/Layout.json"}
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
                    description="Verify the tamper-evident hash chain of the audit log. Detects any edited, inserted, or deleted audit entries (compliance/forensics). Returns INTACT or TAMPERED with the first broken line.",
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
                            "workspace_ids": {"type": "array", "items": {"type": "string"}, "description": "Optional explicit workspace GUIDs (default: up to 100 tenant workspaces)"},
                            "cache_path": {"type": "string", "description": "Optional path to read/write the scan result for reuse"},
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
                )
            ]
            # Attach MCP safety/behavior hints from the annotations registry.
            for t in tools:
                annotations = self._tool_annotations.get(t.name)
                if annotations is not None:
                    t.annotations = annotations
            return tools

        @self.server.call_tool()
        async def handle_call_tool(name: str, arguments: Optional[Dict[str, Any]]) -> List[TextContent]:
            """Handle tool calls"""
            try:
                args = arguments or {}
                logger.info(f"Tool called: {name}")
                # Arguments can carry DAX (with PII literals) or secrets; only log at DEBUG, redacted.
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        f"Tool args ({name}): "
                        f"{redact_secrets(json.dumps(args, default=str), [self.client_secret])}"
                    )

                handler = self._tool_dispatch.get(name)
                if handler is None:
                    return [TextContent(type="text", text=f"Unknown tool: {name}")]

                if self._read_only and name in self._write_tools:
                    return [TextContent(type="text", text=(
                        f"Refused: '{name}' is a write operation and the server is running in "
                        "READ-ONLY mode. Unset POWERBI_MCP_READONLY to allow model/report changes."
                    ))]

                result = await handler(args)
                # Handlers return either a plain string (text only) or a
                # (text, structured_dict) tuple for tools that declare an outputSchema.
                # The MCP SDK puts the dict in structuredContent for typed, chainable results.
                if isinstance(result, tuple) and len(result) == 2:
                    text, structured = result
                    return [TextContent(type="text", text=text)], structured
                return [TextContent(type="text", text=result)]

            except Exception as e:
                safe_err = redact_secrets(str(e), [self.client_secret])
                logger.error(f"Error executing {name}: {safe_err}", exc_info=True)
                return [TextContent(type="text", text=f"Error executing {name}: {safe_err}")]

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
            return await self._read_resource(str(uri))

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
        """Execute DAX query on Desktop model with security processing"""
        try:
            connector = self._get_desktop_connector()
            dax_query = args.get("dax_query")
            max_rows = args.get("max_rows", 100)

            if not connector.current_port:
                return "Not connected to Power BI Desktop. Use 'desktop_connect' first."

            if not dax_query:
                return "Error: dax_query is required"

            # Pre-query security check (resolve referenced tables/columns so column policies fire)
            ref_tables, ref_columns = AccessPolicyEngine.extract_references(dax_query)
            policy_check = self.security.pre_query_check(
                dax_query, tables=ref_tables, columns=ref_columns
            )
            if not policy_check.allowed:
                self.security.log_policy_violation(
                    policy_name="query_policy",
                    violation_type=policy_check.reason,
                    query=dax_query
                )
                return f"Query blocked by security policy: {policy_check.reason}"

            # Apply max_rows from policy if lower
            if policy_check.max_rows and policy_check.max_rows < max_rows:
                max_rows = policy_check.max_rows

            # Execute query with timing
            start_time = time.time()
            rows = await asyncio.get_event_loop().run_in_executor(
                None, connector.execute_dax, dax_query, max_rows
            )
            duration_ms = (time.time() - start_time) * 1000

            # Process results through security layer (PII detection, masking, audit)
            safe_rows, security_report = self.security.process_results(
                results=rows,
                query=dax_query,
                source="desktop",
                model_name=connector.current_model_name,
                port=connector.current_port,
                duration_ms=duration_ms,
                success=True
            )

            # Build response
            result = f"Query returned {len(safe_rows)} row(s)"

            # Add security notices
            if security_report.get('pii_detected'):
                result += f"\n⚠️ PII detected and masked: {security_report['pii_count']} instance(s) of {', '.join(security_report['pii_types'])}"

            if security_report.get('columns_blocked'):
                result += f"\n🚫 Blocked columns: {', '.join(security_report['columns_blocked'])}"

            result += "\n\n"
            result += json.dumps(safe_rows, indent=2, default=str)

            return result

        except Exception as e:
            logger.error(f"Desktop execute DAX error: {e}")
            # Log failed query to audit
            self.security.process_results(
                results=[],
                query=args.get("dax_query", ""),
                source="desktop",
                success=False,
                error_message=str(e)
            )
            return f"Error executing DAX: {str(e)}"

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
        """Execute DAX on Cloud dataset with security processing"""
        try:
            workspace_name = args.get("workspace_name")
            dataset_name = args.get("dataset_name")
            dax_query = args.get("dax_query")

            if not all([workspace_name, dataset_name, dax_query]):
                return "Error: workspace_name, dataset_name, and dax_query are required"

            # Pre-query security check (resolve referenced tables/columns so column policies fire)
            ref_tables, ref_columns = AccessPolicyEngine.extract_references(dax_query)
            policy_check = self.security.pre_query_check(
                dax_query, tables=ref_tables, columns=ref_columns
            )
            if not policy_check.allowed:
                self.security.log_policy_violation(
                    policy_name="query_policy",
                    violation_type=policy_check.reason,
                    query=dax_query
                )
                return f"Query blocked by security policy: {policy_check.reason}"

            # Determine a row cap (cloud execute_dax is otherwise unbounded). Honor an explicit
            # max_rows, clamp to policy, and enforce an absolute ceiling to protect memory.
            cap = policy_check.max_rows or 10000
            requested = args.get("max_rows")
            max_rows = min(requested, cap) if requested else cap
            max_rows = min(max_rows, 100000)

            connector = await asyncio.get_event_loop().run_in_executor(
                None, self._get_xmla_connector, workspace_name, dataset_name
            )

            if not connector:
                return f"Error: Could not connect to dataset '{dataset_name}'"

            # Execute query with timing
            start_time = time.time()
            rows = await asyncio.get_event_loop().run_in_executor(
                None, connector.execute_dax, dax_query
            )
            duration_ms = (time.time() - start_time) * 1000

            # Enforce the row cap (XMLA connector does not cap internally)
            truncated = False
            if isinstance(rows, list) and len(rows) > max_rows:
                rows = rows[:max_rows]
                truncated = True

            # Process results through security layer
            safe_rows, security_report = self.security.process_results(
                results=rows,
                query=dax_query,
                source="cloud",
                model_name=dataset_name,
                duration_ms=duration_ms,
                success=True
            )

            # Build response
            result = f"Query returned {len(safe_rows)} row(s)"

            # Add security notices
            if security_report.get('pii_detected'):
                result += f"\n⚠️ PII detected and masked: {security_report['pii_count']} instance(s) of {', '.join(security_report['pii_types'])}"

            if security_report.get('columns_blocked'):
                result += f"\n🚫 Blocked columns: {', '.join(security_report['columns_blocked'])}"

            if truncated:
                result += f"\n(Note: result truncated to the first {max_rows} rows)"

            result += "\n\n"
            result += json.dumps(safe_rows, indent=2, default=str)

            return result

        except Exception as e:
            logger.error(f"Execute DAX error: {e}")
            # Log failed query to audit
            self.security.process_results(
                results=[],
                query=args.get("dax_query", ""),
                source="cloud",
                success=False,
                error_message=str(e)
            )
            return f"Error executing DAX: {str(e)}"

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

            result = "=== Power BI MCP Security Status ===\n\n"

            # Enabled features
            result += "--- Features ---\n"
            enabled = status.get('enabled', {})
            result += f"  PII Detection:    {'✅ Enabled' if enabled.get('pii_detection') else '❌ Disabled'}\n"
            result += f"  Audit Logging:    {'✅ Enabled' if enabled.get('audit_logging') else '❌ Disabled'}\n"
            result += f"  Access Policies:  {'✅ Enabled' if enabled.get('access_policies') else '❌ Disabled'}\n\n"

            # PII Detection settings
            if enabled.get('pii_detection'):
                pii = status.get('pii_detector', {})
                result += "--- PII Detection ---\n"
                result += f"  Strategy: {pii.get('strategy', 'N/A')}\n"
                result += f"  Types: {', '.join(pii.get('enabled_types', []))}\n\n"

            # Policy settings
            if enabled.get('access_policies'):
                result += "--- Access Policies ---\n"
                result += f"  Enabled: {policy_summary.get('enabled', False)}\n"
                result += f"  Max rows per query: {policy_summary.get('max_rows', 'N/A')}\n"
                result += f"  Tables with policies: {len(policy_summary.get('tables_with_policies', []))}\n\n"

            # Audit log info
            if enabled.get('audit_logging'):
                audit = status.get('audit', {})
                result += "--- Audit Log ---\n"
                result += f"  Session ID: {audit.get('session_id', 'N/A')}\n"
                result += f"  Queries logged: {audit.get('query_count', 0)}\n"
                result += f"  Log file: {audit.get('log_file', 'N/A')}\n"

            return result

        except Exception as e:
            logger.error(f"Security status error: {e}")
            return f"Error getting security status: {str(e)}"

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
                    result += f"  Rows: {result_info.get('row_count', 0)}, Duration: {result_info.get('duration_ms', 0):.0f}ms\n"

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
            return "TOM (Tabular Object Model) is not available. Write operations require Microsoft.AnalysisServices.Tabular.dll."

        desktop = self._get_desktop_connector()
        if not desktop.current_port:
            return "Not connected to Power BI Desktop. Use 'desktop_connect' first."

        tom = self._get_tom_connector()
        if not tom.model or tom.current_port != desktop.current_port:
            # Connect TOM to the same port as desktop connector
            connect_fn = lambda: tom.connect(desktop.current_port)
            success = await asyncio.get_event_loop().run_in_executor(None, connect_fn)
            if not success:
                return "Failed to connect TOM to Power BI Desktop. Write operations may not be supported."

        return None

    async def _handle_batch_rename_tables(self, args: Dict[str, Any]) -> str:
        """Handle batch table rename"""
        try:
            error = await self._ensure_tom_connected()
            if error:
                return error

            renames = args.get("renames", [])
            auto_save = args.get("auto_save", True)

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
            auto_save = args.get("auto_save", True)

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
            auto_save = args.get("auto_save", True)

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
            auto_save = args.get("auto_save", not self._tom_transaction_active)

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
        """Handle create measure"""
        try:
            error = await self._ensure_tom_connected()
            if error:
                return error

            table_name = args.get("table_name")
            measure_name = args.get("measure_name")
            expression = args.get("expression")
            format_string = args.get("format_string")
            description = args.get("description")

            if not all([table_name, measure_name, expression]):
                return "Error: table_name, measure_name, and expression are required"

            # Validate-before-commit: probe the expression against the live model first.
            if not bool(args.get("skip_validation", False)):
                status, verr = await self._validate_via_desktop(expression, as_measure=True)
                if status is False:
                    return (
                        f"[INVALID] Measure '{measure_name}' was NOT created - the expression failed validation.\n\n"
                        f"Error: {redact_secrets(verr, [self.client_secret])}\n\n"
                        "Fix the DAX and retry (or pass skip_validation=true to override)."
                    )

            tom = self._get_tom_connector()

            # Create measure
            create_fn = lambda: tom.create_measure(
                table_name, measure_name, expression,
                format_string=format_string,
                description=description
            )
            result = await asyncio.get_event_loop().run_in_executor(None, create_fn)

            if result.success:
                if self._tom_transaction_active:
                    return (
                        f"Measure '{measure_name}' created in table '{table_name}' (PENDING - "
                        "run tom_commit_transaction to save).\n\nExpression: {expr}".format(expr=expression)
                    )
                # Auto-save
                save_fn = lambda: tom.save_changes()
                save_result = await asyncio.get_event_loop().run_in_executor(None, save_fn)

                if save_result.success:
                    return f"Measure '{measure_name}' created successfully in table '{table_name}'.\n\nExpression: {expression}"
                else:
                    return f"Measure created but failed to save: {save_result.message}"
            else:
                return f"Failed to create measure: {result.message}"

        except Exception as e:
            logger.error(f"Create measure error: {e}")
            return f"Error: {str(e)}"

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

            # Delete measure
            delete_fn = lambda: tom.delete_measure(measure_name, table_name)
            result = await asyncio.get_event_loop().run_in_executor(None, delete_fn)

            if result.success:
                if self._tom_transaction_active:
                    return f"Measure '{measure_name}' deleted (PENDING - run tom_commit_transaction to save)."
                # Auto-save
                save_fn = lambda: tom.save_changes()
                save_result = await asyncio.get_event_loop().run_in_executor(None, save_fn)

                if save_result.success:
                    return f"Measure '{measure_name}' deleted successfully."
                else:
                    return f"Measure deleted but failed to save: {save_result.message}"
            else:
                return f"Failed to delete measure: {result.message}"

        except Exception as e:
            logger.error(f"Delete measure error: {e}")
            return f"Error: {str(e)}"

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
            loop = asyncio.get_event_loop()
            fn = lambda: tom.create_relationship(
                ft, fc, tt, tc,
                cardinality=args.get("cardinality", "many_to_one"),
                cross_filter=args.get("cross_filter", "single"),
                is_active=args.get("is_active", True),
            )
            result = await loop.run_in_executor(None, fn)
            if not result.success:
                return f"Failed to create relationship: {result.message}"
            if self._tom_transaction_active:
                return f"{result.message} (PENDING - run tom_commit_transaction to save)."
            save = await loop.run_in_executor(None, tom.save_changes)
            return result.message + ("" if getattr(save, "success", False) else f" (save failed: {save.message})")
        except Exception as e:
            return f"Error creating relationship: {redact_secrets(str(e), [self.client_secret])}"

    async def _handle_delete_relationship(self, args: Dict[str, Any]) -> str:
        """Delete a relationship by name or by from/to table[/column] (TOM)."""
        try:
            error = await self._ensure_tom_connected()
            if error:
                return error
            tom = self._get_tom_connector()
            loop = asyncio.get_event_loop()
            fn = lambda: tom.delete_relationship(
                from_table=args.get("from_table"), from_column=args.get("from_column"),
                to_table=args.get("to_table"), to_column=args.get("to_column"),
                name=args.get("name"),
            )
            result = await loop.run_in_executor(None, fn)
            if not result.success:
                return f"Failed to delete relationship: {result.message}"
            if self._tom_transaction_active:
                return f"{result.message} (PENDING - run tom_commit_transaction to save)."
            save = await loop.run_in_executor(None, tom.save_changes)
            return result.message + ("" if getattr(save, "success", False) else f" (save failed: {save.message})")
        except Exception as e:
            return f"Error deleting relationship: {redact_secrets(str(e), [self.client_secret])}"

    # ==================== DAX SAFETY LOOP & TRANSACTIONS (Bundle A) ====================

    async def _validate_via_desktop(self, dax: str, as_measure: bool = True):
        """Validate a DAX expression against the connected Desktop model.

        Returns (status, error) where status is True (valid), False (invalid),
        or None (could not validate, e.g. Desktop not connected -> caller skips).
        """
        desktop = self._get_desktop_connector()
        if not desktop.current_port:
            return None, "Desktop not connected"
        probe = build_validation_probe(dax, as_measure)
        try:
            await asyncio.get_event_loop().run_in_executor(
                None, desktop.execute_dax, probe, 1
            )
            return True, None
        except Exception as e:
            return False, str(e)

    async def _handle_validate_dax(self, args: Dict[str, Any]):
        """Validate DAX (query or scalar measure expression) without committing.

        Returns (text, {valid, error, probe}) so agents get a typed result.
        """
        dax = args.get("dax")
        probe = build_validation_probe(dax or "", bool(args.get("as_measure", False)))
        if not dax:
            return ("Error: dax is required", {"valid": False, "error": "dax is required", "probe": probe})
        source = (args.get("source") or "desktop").lower()
        loop = asyncio.get_event_loop()
        try:
            if source == "cloud":
                workspace = args.get("workspace_name")
                dataset = args.get("dataset_name")
                if not (workspace and dataset):
                    msg = "Error: workspace_name and dataset_name are required for cloud validation"
                    return (msg, {"valid": False, "error": msg, "probe": probe})
                connector = await loop.run_in_executor(None, self._get_xmla_connector, workspace, dataset)
                if not connector:
                    msg = f"Error: could not connect to dataset '{dataset}'"
                    return (msg, {"valid": False, "error": msg, "probe": probe})
                await loop.run_in_executor(None, connector.execute_dax, probe)
            else:
                desktop = self._get_desktop_connector()
                if not desktop.current_port:
                    msg = "Not connected to Power BI Desktop. Use 'desktop_connect' first."
                    return (msg, {"valid": False, "error": msg, "probe": probe})
                await loop.run_in_executor(None, desktop.execute_dax, probe, 1)
            return (
                f"[VALID] DAX validated successfully against the model.\n\nProbe executed:\n{probe}",
                {"valid": True, "error": None, "probe": probe},
            )
        except Exception as e:
            msg = redact_secrets(str(e), [self.client_secret])
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
        """Begin a TOM write transaction (defers SaveChanges until commit)."""
        error = await self._ensure_tom_connected()
        if error:
            return error
        if self._tom_transaction_active:
            return "A TOM transaction is already open. Commit or roll it back first."
        self._tom_transaction_active = True
        return (
            "TOM transaction started. Subsequent create_measure / delete_measure / "
            "batch_update_measures edits are deferred until tom_commit_transaction "
            "(or discarded by tom_rollback_transaction)."
        )

    async def _handle_tom_commit_transaction(self) -> str:
        """Commit pending TOM edits."""
        if not self._tom_transaction_active:
            return "No TOM transaction is open."
        tom = self._get_tom_connector()
        result = await asyncio.get_event_loop().run_in_executor(None, tom.save_changes)
        self._tom_transaction_active = False
        if getattr(result, "success", False):
            return f"Transaction committed. {getattr(result, 'message', '')}".strip()
        return f"Commit failed: {getattr(result, 'message', 'unknown error')}"

    async def _handle_tom_rollback_transaction(self) -> str:
        """Roll back pending TOM edits."""
        if not self._tom_transaction_active:
            return "No TOM transaction is open."
        tom = self._get_tom_connector()
        result = await asyncio.get_event_loop().run_in_executor(None, tom.discard_changes)
        self._tom_transaction_active = False
        if getattr(result, "success", False):
            return f"Transaction rolled back. {getattr(result, 'message', '')}".strip()
        return f"Rollback failed: {getattr(result, 'message', 'unknown error')}"

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

    async def _gather_model_metadata(self, source: str, workspace=None, dataset=None):
        """Build a normalized model dict (for BPA / AI-readiness) via INFO.VIEW.* DAX.

        Returns (model_dict, error).
        """
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
                        "description": g(r, "Description") or "", "columns": [], "measures": []}
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
                "expression": g(r, "Expression"),
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
            result = dax_lint.lint_measures(measures)
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
                out += f"--- {label}{h['rule_id']} (line {h['line']}) ---\n"
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
            options = {k: args[k] for k in ("scope", "target_case", "strip_warehouse_prefixes", "expand_abbreviations")
                       if k in args and args[k] is not None}
            result = naming_audit.audit(model, options)
            s = result["summary"]
            out = "=== Naming Audit ===\n\n"
            out += f"Suggestions: {s['total_suggestions']}"
            if s.get("by_type"):
                out += "  (" + ", ".join(f"{k}: {v}" for k, v in s["by_type"].items()) + ")"
            out += f"\nDominant style: {s.get('dominant_style')}  (consistent: {s.get('consistent')})\n\n"
            for p in result["plan"][:200]:
                loc = f"{p['table']}[{p['old']}]" if p.get("table") else p["old"]
                out += f"  {p['object_type']}: {loc}  ->  '{p['new']}'  ({', '.join(p['reasons'])})\n"
            if not result["plan"]:
                out += "No naming issues found.\n"
            else:
                out += "\nApply with the rename tools (pbip_rename_* updates model + report; batch_rename_* is live).\n"
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
            result = pbix_tools.extract(path, dest, decode_layout=args.get("decode_layout", True))
            out = f"=== Extracted {result['file_count']} file(s) to {result['dest']} ===\n"
            if result["layout_decoded"]:
                out += "Decoded legacy layout to Report/Layout.json\n"
            out += "\n" + "\n".join(f"  {f}" for f in result["files"][:50])
            if result["file_count"] > 50:
                out += f"\n  ... and {result['file_count'] - 50} more"
            return (out, result)
        except Exception as e:
            msg = redact_secrets(str(e), [self.client_secret])
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
                q = f"EVALUATE ROW(\"r\", COUNTROWS('{name}'))"
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
            run, err = await self._get_query_runner(source, args.get("workspace_name"), args.get("dataset_name"))
            if err:
                return f"Error: {err}"
            loop = asyncio.get_event_loop()
            start = time.time()
            rows = await loop.run_in_executor(None, run, dax)
            duration_ms = (time.time() - start) * 1000
            row_count = len(rows) if isinstance(rows, list) else 0

            hints = []
            up = dax.upper()
            if duration_ms > 2000:
                hints.append(f"Slow ({duration_ms:.0f} ms). Check relationship cardinality and avoid row-by-row iterators over large fact tables.")
            if row_count > 10000:
                hints.append(f"Large result ({row_count:,} rows). Add TOPN / SUMMARIZECOLUMNS filters.")
            if up.count("FILTER(") >= 3:
                hints.append("Multiple FILTER() calls; prefer CALCULATE with boolean filters or KEEPFILTERS where possible.")
            if "ADDCOLUMNS(" in up and "SUMMARIZE(" in up:
                hints.append("SUMMARIZE+ADDCOLUMNS pattern; SUMMARIZECOLUMNS is usually faster and safer.")
            if not hints:
                hints.append("No obvious red flags. For storage-engine vs formula-engine timings, use DAX Studio Server Timings.")

            out = "=== Query Performance ===\n\n"
            out += f"Duration: {duration_ms:.0f} ms\n"
            out += f"Rows returned: {row_count:,}\n\n"
            out += "--- Hints ---\n"
            for h in hints:
                out += f"  - {h}\n"
            return out
        except Exception as e:
            return f"Error analyzing query performance: {redact_secrets(str(e), [self.client_secret])}"

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
            out += ("\nNote: dependency graph includes measures, calc columns, RLS, calc groups and "
                    "field parameters (via INFO.CALCDEPENDENCY) plus relationships; review before deleting.")
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
                dax = f"EVALUATE ROW(\"rows\", COUNTROWS('{table}'))"
            elif measure:
                dax = f"EVALUATE ROW(\"value\", [{measure}])"
            else:
                return "Error: provide one of dax, table_name, or measure_name"

        all_roles = await loop.run_in_executor(None, connector.list_rls_roles)
        role_names = args.get("roles") or [r.get("name") for r in all_roles if r.get("name")]
        if not role_names:
            return "No RLS roles found in the model (nothing to test)."

        def metric(rows):
            if not rows:
                return None
            return next(iter(rows[0].values()), None)

        try:
            await loop.run_in_executor(None, connector.set_rls_role, None)
            baseline = metric(await loop.run_in_executor(None, connector.execute_dax, dax, 10))
            results = []
            for role in role_names:
                ok = await loop.run_in_executor(None, connector.set_rls_role, role)
                if not ok:
                    results.append((role, None, "ERROR activating role"))
                    continue
                val = metric(await loop.run_in_executor(None, connector.execute_dax, dax, 10))
                if val is None or val == 0:
                    note = "sees NOTHING (verify the role filter)"
                elif baseline is not None and val == baseline:
                    note = "sees EVERYTHING (no row reduction - verify intended)"
                else:
                    note = "filtered"
                results.append((role, val, note))
        finally:
            await loop.run_in_executor(None, connector.set_rls_role, None)  # always restore

        out = "=== RLS Test Matrix ===\n"
        out += f"Query: {dax}\nUnrestricted baseline: {baseline}\n\n"
        out += f"{'Role':<32} {'Result':>14}  Note\n"
        out += "-" * 70 + "\n"
        for role, val, note in results:
            out += f"{str(role)[:31]:<32} {str(val):>14}  {note}\n"
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
                    with open(tests_path, "r", encoding="utf-8") as f:
                        tests = json.load(f)
                except Exception as e:
                    return (f"Error reading tests_path: {e}", {"passed": 0, "total": 0, "error": str(e)})
            if not tests or not isinstance(tests, list):
                return ("Error: provide 'tests' (array of {name, dax, expected, tolerance?}) or 'tests_path'.",
                        {"passed": 0, "total": 0})
            run, rerr = await self._get_query_runner(args.get("source") or "desktop",
                                                     args.get("workspace_name"), args.get("dataset_name"))
            if rerr:
                return (f"Error: {rerr}", {"passed": 0, "total": len(tests), "error": rerr})
            loop = asyncio.get_event_loop()

            results = []
            passed = 0
            for t in tests:
                name = t.get("name", t.get("dax", "test")[:40])
                dax = t.get("dax")
                if not dax:
                    results.append({"name": name, "status": "ERROR", "detail": "no dax"})
                    continue
                try:
                    rows = await loop.run_in_executor(None, run, dax)
                    actual = next(iter(rows[0].values()), None) if rows else None
                except Exception as e:
                    results.append({"name": name, "status": "ERROR", "detail": redact_secrets(str(e), [self.client_secret])[:200]})
                    continue
                if "expected" not in t:
                    results.append({"name": name, "status": "INFO", "detail": f"actual={actual} (no expected given)"})
                    continue
                ok, detail = model_analysis.dax_test_verdict(actual, t.get("expected"), t.get("tolerance", 0))
                results.append({"name": name, "status": "PASS" if ok else "FAIL", "detail": detail})
                if ok:
                    passed += 1

            graded = [r for r in results if r["status"] in ("PASS", "FAIL")]
            total = len(graded)
            all_passed = total > 0 and passed == total
            out = f"[{'PASS' if all_passed else 'FAIL'}] DAX tests: {passed}/{total} passed\n\n"
            for r in results:
                out += f"  [{r['status']}] {r['name']}: {r['detail']}\n"
            return (out, {"passed": passed, "total": total, "all_passed": all_passed, "results": results})
        except Exception as e:
            msg = redact_secrets(str(e), [self.client_secret])
            return (f"Error running DAX tests: {msg}", {"passed": 0, "total": 0, "error": msg})

    async def _handle_cross_workspace_lineage(self, args: Dict[str, Any]) -> str:
        """Tenant-wide inventory + lineage via the Admin Scanner API (admin-gated)."""
        try:
            rest = self._get_rest_connector()
            if not rest:
                return "Error: cloud credentials not configured (TENANT_ID / CLIENT_ID / CLIENT_SECRET)."
            loop = asyncio.get_event_loop()

            # Reuse a cached scanResult if provided (Scanner API is rate-limited).
            cache_path = args.get("cache_path")
            scan = None
            if cache_path and args.get("use_cache", True):
                try:
                    with open(cache_path, "r", encoding="utf-8") as f:
                        scan = json.load(f)
                except Exception:
                    scan = None

            if scan is None:
                ids = args.get("workspace_ids")
                if not ids:
                    try:
                        ws = await loop.run_in_executor(None, rest.admin_list_workspaces, 100)
                        ids = [w.get("id") for w in ws if w.get("id")]
                    except Exception as e:
                        return (f"Error listing workspaces (needs admin / read-only admin APIs enabled): "
                                f"{redact_secrets(str(e), [self.client_secret])}")
                ids = [i for i in (ids or [])][:100]  # Scanner caps at 100 workspaces/call
                if not ids:
                    return "No workspaces found to scan."
                started = await loop.run_in_executor(None, rest.admin_post_workspace_info, ids, True)
                scan_id = started.get("id")
                if not scan_id:
                    return f"Error: scan did not start ({started})."
                status = ""
                last_st = {}
                for _ in range(20):  # ~5 min budget (Microsoft suggests 30-60s polling for big scans)
                    last_st = await loop.run_in_executor(None, rest.admin_get_scan_status, scan_id)
                    status = str(last_st.get("status", "")).lower()
                    if status == "succeeded":
                        break
                    if status == "failed":
                        return f"Scan failed: {last_st.get('error') or last_st}"
                    await asyncio.sleep(15)
                if status != "succeeded":
                    return (f"Scan still running after the wait (status={status}). Re-run with the same "
                            "cache_path to resume later, or scan fewer workspace_ids.")
                scan = await loop.run_in_executor(None, rest.admin_get_scan_result, scan_id)
                if cache_path:
                    try:
                        with open(cache_path, "w", encoding="utf-8") as f:
                            json.dump(scan, f)
                    except Exception:
                        pass

            s = governance.summarize_scan(scan, dataset_name=args.get("dataset_name"))
            out = "=== Cross-Workspace Lineage / Inventory ===\n\n"
            out += f"Workspaces: {s['workspaces']}  Datasets: {s['datasets']}  Reports: {s['reports']}\n\n"
            if s.get("focus_dataset"):
                out += f"Dataset '{s['focus_dataset']}' found in: {', '.join(s['focus_found_in']) or '(not found)'}\n"
                out += f"Downstream reports ({len(s['downstream_reports'])}):\n"
                out += ("\n".join(f"  - {r}" for r in s['downstream_reports']) or "  (none)") + "\n\n"
            out += f"Datasets WITHOUT RLS roles ({len(s['datasets_without_rls'])}):\n"
            out += ("\n".join(f"  - {d}" for d in s['datasets_without_rls'][:50]) or "  (none)") + "\n\n"
            out += f"Datasets WITHOUT a sensitivity label ({len(s['datasets_without_sensitivity_label'])}):\n"
            out += ("\n".join(f"  - {d}" for d in s['datasets_without_sensitivity_label'][:50]) or "  (none)") + "\n"
            return out
        except Exception as e:
            return f"Error in cross-workspace lineage: {redact_secrets(str(e), [self.client_secret])}"

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
            checked = 0
            for wid in workspace_ids:
                try:
                    datasets = await loop.run_in_executor(None, rest.list_datasets, wid)
                except Exception:
                    continue
                for ds in datasets:
                    if not ds.get("isRefreshable"):
                        continue
                    checked += 1
                    try:
                        hist = await loop.run_in_executor(None, rest.get_refresh_history, wid, ds["id"], 1)
                    except Exception:
                        continue
                    if not hist:
                        continue
                    last = hist[0]
                    if str(last.get("status")) == "Failed":
                        diag = refresh_diagnostics.classify_refresh_error(last.get("serviceExceptionJson") or "")
                        failures.append((ds.get("name"), last.get("endTime"), diag["cause"]))

            out = "=== Fleet Refresh Monitor ===\n\n"
            out += f"Refreshable datasets checked: {checked}\n"
            out += f"Most-recent-refresh FAILURES: {len(failures)}\n\n"
            for name, when, cause in failures[:100]:
                out += f"  [FAILED] {name} ({when}): {cause}\n"
            if not failures:
                out += "  All checked datasets' most recent refresh succeeded.\n"
            return out
        except Exception as e:
            return f"Error in fleet refresh monitor: {redact_secrets(str(e), [self.client_secret])}"

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
        if not res.get("valid") and res.get("broken_line"):
            text += f"\n  First problem at line {res['broken_line']}"
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
        """Get or create PBIP connector"""
        if not self.pbip_connector:
            self.pbip_connector = PowerBIPBIPConnector()
        return self.pbip_connector

    async def _handle_pbip_load_project(self, args: Dict[str, Any]) -> str:
        """Load a PBIP project for editing"""
        try:
            pbip_path = args.get("pbip_path")

            if not pbip_path:
                return "Error: 'pbip_path' is required"

            connector = self._get_pbip_connector()

            # Load the project
            load_fn = lambda: connector.load_project(pbip_path)
            success = await asyncio.get_event_loop().run_in_executor(None, load_fn)

            if success:
                info = connector.get_project_info()
                result = "=== PBIP Project Loaded Successfully ===\n\n"
                result += f"Project: {info.get('pbip_file', 'N/A')}\n"
                result += f"Root Path: {info.get('root_path', 'N/A')}\n\n"

                if info.get('semantic_model_folder'):
                    result += f"Semantic Model: {info.get('semantic_model_folder')}\n"
                    result += f"TMDL Files: {info.get('tmdl_file_count', 0)}\n\n"

                if info.get('report_folder'):
                    result += f"Report Folder: {info.get('report_folder')}\n"
                    result += f"Report JSON: {'Yes' if info.get('report_json_path') else 'No'}\n\n"

                result += "You can now use:\n"
                result += "  - pbip_rename_tables: Rename tables (updates model AND report visuals)\n"
                result += "  - pbip_rename_columns: Rename columns (updates model AND report visuals)\n"
                result += "  - pbip_rename_measures: Rename measures (updates model AND report visuals)\n"

                return result
            else:
                return f"Failed to load PBIP project from: {pbip_path}\n\nEnsure the path points to a valid .pbip file or folder containing one."

        except Exception as e:
            logger.error(f"PBIP load error: {e}")
            return f"Error loading PBIP project: {str(e)}"

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
        """Rename tables in PBIP files (model + report)"""
        try:
            connector = self._get_pbip_connector()

            if not connector.current_project:
                return "No PBIP project loaded. Use 'pbip_load_project' first."

            renames = args.get("renames", [])

            if not renames:
                return "Error: 'renames' array is required"

            # Execute batch rename
            batch_fn = lambda: connector.batch_rename_tables(renames)
            result = await asyncio.get_event_loop().run_in_executor(None, batch_fn)

            # Build response
            response = "=== PBIP Batch Rename Tables ===\n\n"

            # Show backup info if created
            if result.backup_created:
                response += f"BACKUP CREATED: {result.backup_created}\n\n"

            response += f"{result.message}\n\n"

            if result.files_modified:
                response += "--- Files Modified ---\n"
                for f in result.files_modified[:10]:
                    response += f"  - {f}\n"
                if len(result.files_modified) > 10:
                    response += f"  ... and {len(result.files_modified) - 10} more\n"
                response += "\n"

            response += f"Total references updated: {result.references_updated}\n\n"

            # Show validation errors if any
            if result.validation_errors:
                response += "--- VALIDATION ERRORS ---\n"
                response += "WARNING: The following issues were detected:\n\n"
                for err in result.validation_errors[:10]:
                    response += f"  [{err.error_type}] {err.file_path}:{err.line_number}\n"
                    response += f"    {err.message}\n"
                    if err.context:
                        response += f"    Context: {err.context[:80]}...\n" if len(err.context) > 80 else f"    Context: {err.context}\n"
                    response += "\n"
                if len(result.validation_errors) > 10:
                    response += f"  ... and {len(result.validation_errors) - 10} more errors\n"
                response += "\nConsider using connector.rollback_changes() to undo these changes.\n\n"

            if result.success:
                response += "SUCCESS: All table names properly quoted. Report visuals should NOT break!\n"
                response += "\nNext steps:\n"
                response += "  1. Open the .pbip file in Power BI Desktop\n"
                response += "  2. Verify the changes look correct\n"
                response += "  3. Save as .pbix if you want to share the file\n"
            else:
                response += "FAILED: Validation errors detected. Review and fix before opening in Power BI Desktop.\n"
                if result.backup_created:
                    response += f"\nTo restore: Copy files from backup folder: {result.backup_created}\n"

            return response

        except Exception as e:
            logger.error(f"PBIP rename tables error: {e}")
            return f"Error: {str(e)}"

    async def _handle_pbip_rename_columns(self, args: Dict[str, Any]) -> str:
        """Rename columns in PBIP files (model + report)"""
        try:
            connector = self._get_pbip_connector()

            if not connector.current_project:
                return "No PBIP project loaded. Use 'pbip_load_project' first."

            renames = args.get("renames", [])

            if not renames:
                return "Error: 'renames' array is required"

            # Execute batch rename
            batch_fn = lambda: connector.batch_rename_columns(renames)
            result = await asyncio.get_event_loop().run_in_executor(None, batch_fn)

            # Build response
            response = "=== PBIP Batch Rename Columns ===\n\n"

            # Show backup info if created
            if result.backup_created:
                response += f"BACKUP CREATED: {result.backup_created}\n\n"

            response += f"{result.message}\n\n"

            if result.files_modified:
                response += "--- Files Modified ---\n"
                for f in result.files_modified[:10]:
                    response += f"  - {f}\n"
                if len(result.files_modified) > 10:
                    response += f"  ... and {len(result.files_modified) - 10} more\n"
                response += "\n"

            response += f"Total references updated: {result.references_updated}\n\n"

            if result.success:
                response += "SUCCESS: Column names properly updated. Report visuals should NOT break!\n"
                response += "\nNext steps:\n"
                response += "  1. Reopen the .pbip file in Power BI Desktop to see changes\n"
                response += "  2. Verify the changes look correct\n"
                response += "  3. Save as .pbix if you want to share the file\n"

            return response

        except Exception as e:
            logger.error(f"PBIP rename columns error: {e}")
            return f"Error: {str(e)}"

    async def _handle_pbip_rename_measures(self, args: Dict[str, Any]) -> str:
        """Rename measures in PBIP files (model + report)"""
        try:
            connector = self._get_pbip_connector()

            if not connector.current_project:
                return "No PBIP project loaded. Use 'pbip_load_project' first."

            renames = args.get("renames", [])

            if not renames:
                return "Error: 'renames' array is required"

            # Execute batch rename
            batch_fn = lambda: connector.batch_rename_measures(renames)
            result = await asyncio.get_event_loop().run_in_executor(None, batch_fn)

            # Build response
            response = "=== PBIP Batch Rename Measures ===\n\n"

            # Show backup info if created
            if result.backup_created:
                response += f"BACKUP CREATED: {result.backup_created}\n\n"

            response += f"{result.message}\n\n"

            if result.files_modified:
                response += "--- Files Modified ---\n"
                for f in result.files_modified[:10]:
                    response += f"  - {f}\n"
                if len(result.files_modified) > 10:
                    response += f"  ... and {len(result.files_modified) - 10} more\n"
                response += "\n"

            response += f"Total references updated: {result.references_updated}\n\n"

            if result.success:
                response += "SUCCESS: Measure names properly updated. Report visuals should NOT break!\n"
                response += "\nNext steps:\n"
                response += "  1. Reopen the .pbip file in Power BI Desktop to see changes\n"
                response += "  2. Verify the changes look correct\n"
                response += "  3. Save as .pbix if you want to share the file\n"

            return response

        except Exception as e:
            logger.error(f"PBIP rename measures error: {e}")
            return f"Error: {str(e)}"

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

            # Execute fix
            fix_fn = lambda: connector.fix_broken_visual_references(old_table_name, new_table_name)
            result = await asyncio.get_event_loop().run_in_executor(None, fix_fn)

            # Build response
            response = "=== Fix Broken Visual References ===\n\n"

            response += f"Old table name: {old_table_name}\n"
            response += f"New table name: {new_table_name}\n"
            response += f"Report format: {result.get('format', 'Unknown')}\n\n"

            if result.get("success"):
                response += f"✅ SUCCESS: Fixed {result.get('references_fixed', 0)} references\n\n"

                if result.get("files_modified"):
                    response += "--- Files Modified ---\n"
                    for f in result["files_modified"][:15]:
                        response += f"  - {f}\n"
                    if len(result["files_modified"]) > 15:
                        response += f"  ... and {len(result['files_modified']) - 15} more\n"
                    response += "\nNext step: Reopen the report in Power BI Desktop to see changes.\n"
            else:
                response += f"❌ No references found for '{old_table_name}'\n"
                response += "\nPossible reasons:\n"
                response += "  - The old table name doesn't exist in visuals\n"
                response += "  - Visuals may already be updated\n"
                response += "  - Try using 'pbip_scan_broken_refs' to diagnose\n"

            return response

        except Exception as e:
            logger.error(f"PBIP fix broken visuals error: {e}")
            return f"Error: {str(e)}"

    async def _handle_pbip_fix_dax_quoting(self) -> str:
        """Fix DAX expressions by properly quoting table names with spaces"""
        try:
            connector = self._get_pbip_connector()

            if not connector.current_project:
                return "No PBIP project loaded. Use 'pbip_load_project' first."

            # Execute fix
            fix_fn = lambda: connector.fix_all_dax_quoting()
            result = await asyncio.get_event_loop().run_in_executor(None, fix_fn)

            # Build response
            response = "=== Fix DAX Table Name Quoting ===\n\n"

            if result.get("count", 0) > 0:
                response += f"✅ SUCCESS: Fixed {result['count']} unquoted table references\n\n"

                if result.get("tables_fixed"):
                    response += "--- Tables That Needed Quoting ---\n"
                    for table in result["tables_fixed"]:
                        response += f"  • {table} -> '{table}'\n"
                    response += "\n"

                if result.get("files_modified"):
                    response += "--- Files Modified ---\n"
                    for f in result["files_modified"][:10]:
                        response += f"  - {f}\n"
                    if len(result["files_modified"]) > 10:
                        response += f"  ... and {len(result['files_modified']) - 10} more\n"
                    response += "\nNext step: Reopen the report in Power BI Desktop to see changes.\n"
            else:
                response += "✅ No fixes needed - all table names are properly quoted.\n"

            if result.get("errors"):
                response += "\n--- Errors ---\n"
                for err in result["errors"]:
                    response += f"  ❌ {err['file']}: {err['error']}\n"

            return response

        except Exception as e:
            logger.error(f"PBIP fix DAX quoting error: {e}")
            return f"Error: {str(e)}"

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

            # Execute validation
            validate_fn = lambda: connector.validate_tmdl_syntax()
            errors = await asyncio.get_event_loop().run_in_executor(None, validate_fn)

            # Build response
            response = "=== PBIP Validation Results ===\n\n"

            report_format = "PBIR-Enhanced" if connector.current_project.is_pbir_enhanced else "PBIR-Legacy"
            response += f"Report format: {report_format}\n"
            response += f"TMDL files: {len(connector.current_project.tmdl_files)}\n"
            response += f"Visual files: {len(connector.current_project.visual_json_files)}\n\n"

            if errors:
                response += f"❌ Found {len(errors)} validation error(s):\n\n"

                # Group errors by type
                by_type = {}
                for err in errors:
                    if err.error_type not in by_type:
                        by_type[err.error_type] = []
                    by_type[err.error_type].append(err)

                for error_type, type_errors in by_type.items():
                    response += f"--- {error_type} ({len(type_errors)}) ---\n"
                    for err in type_errors[:5]:
                        response += f"  Line {err.line_number}: {err.message}\n"
                        if err.context:
                            ctx = err.context[:60] + "..." if len(err.context) > 60 else err.context
                            response += f"    Context: {ctx}\n"
                    if len(type_errors) > 5:
                        response += f"  ... and {len(type_errors) - 5} more\n"
                    response += "\n"

                response += "💡 FIX: Use 'pbip_fix_dax_quoting' to automatically fix quoting issues.\n"
            else:
                response += "✅ No validation errors found! TMDL syntax is valid.\n"

            return response

        except Exception as e:
            logger.error(f"PBIP validate error: {e}")
            return f"Error: {str(e)}"

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
                    f"Path: {result['path']}\nReopen Power BI Desktop to see it.")
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
                    f"Path: {result['path']}\nReopen Power BI Desktop to see it.")
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
