"""
Power BI MCP Server V2
Supports both Power BI Service (Cloud) and Power BI Desktop (Local)
Features: PII Detection, Audit Logging, Access Policies
"""
import asyncio
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from mcp.server import Server, NotificationOptions
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent
from mcp.server.models import InitializationOptions

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stderr)]
)
logger = logging.getLogger("powerbi-mcp-v2")

# Import connectors
from powerbi_rest_connector import PowerBIRestConnector
from powerbi_xmla_connector import PowerBIXmlaConnector
from powerbi_desktop_connector import PowerBIDesktopConnector
from powerbi_tom_connector import PowerBITOMConnector
from powerbi_pbip_connector import PowerBIPBIPConnector

# Import security layer
from security import SecurityLayer, get_security_layer


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

        # Initialize security layer
        config_path = Path(__file__).parent.parent / "config" / "policies.yaml"
        self.security = SecurityLayer(
            config_path=str(config_path) if config_path.exists() else None,
            enable_pii_detection=os.getenv("ENABLE_PII_DETECTION", "true").lower() == "true",
            enable_audit=os.getenv("ENABLE_AUDIT", "true").lower() == "true",
            enable_policies=os.getenv("ENABLE_POLICIES", "true").lower() == "true"
        )

        self._setup_handlers()

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
                # === BATCH/WRITE OPERATIONS (TOM) ===
                Tool(
                    name="batch_rename_tables",
                    description="Bulk rename multiple tables in the Power BI Desktop model. Requires confirmation before saving.",
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
                    description="Bulk rename multiple columns in the Power BI Desktop model. Requires confirmation before saving.",
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
                    description="Bulk rename multiple measures in the Power BI Desktop model. Requires confirmation before saving.",
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
                                "description": "Whether to automatically save changes (default: true)",
                                "default": True
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
                    description="Bulk rename tables in a PBIP project. Updates ALL references including TMDL model files AND report.json visuals. IMPORTANT: This ensures report visuals don't break!",
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
                    description="Bulk rename columns in a PBIP project. Updates ALL references including TMDL model files AND report.json visuals.",
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
                    description="Bulk rename measures in a PBIP project. Updates ALL references including TMDL model files AND report.json visuals.",
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
                )
            ]
            return tools

        @self.server.call_tool()
        async def handle_call_tool(name: str, arguments: Optional[Dict[str, Any]]) -> List[TextContent]:
            """Handle tool calls"""
            try:
                logger.info(f"Tool called: {name} with args: {arguments}")
                args = arguments or {}

                # Desktop tools
                if name == "desktop_discover_instances":
                    result = await self._handle_desktop_discover()
                elif name == "desktop_connect":
                    result = await self._handle_desktop_connect(args)
                elif name == "desktop_list_tables":
                    result = await self._handle_desktop_list_tables()
                elif name == "desktop_list_columns":
                    result = await self._handle_desktop_list_columns(args)
                elif name == "desktop_list_measures":
                    result = await self._handle_desktop_list_measures()
                elif name == "desktop_execute_dax":
                    result = await self._handle_desktop_execute_dax(args)
                elif name == "desktop_get_model_info":
                    result = await self._handle_desktop_get_model_info()
                # Cloud tools
                elif name == "list_workspaces":
                    result = await self._handle_list_workspaces()
                elif name == "list_datasets":
                    result = await self._handle_list_datasets(args)
                elif name == "list_tables":
                    result = await self._handle_list_tables(args)
                elif name == "list_columns":
                    result = await self._handle_list_columns(args)
                elif name == "execute_dax":
                    result = await self._handle_execute_dax(args)
                elif name == "get_model_info":
                    result = await self._handle_get_model_info(args)
                # Security tools
                elif name == "security_status":
                    result = await self._handle_security_status()
                elif name == "security_audit_log":
                    result = await self._handle_security_audit_log(args)
                # RLS tools
                elif name == "desktop_list_rls_roles":
                    result = await self._handle_desktop_list_rls_roles()
                elif name == "desktop_set_rls_role":
                    result = await self._handle_desktop_set_rls_role(args)
                elif name == "desktop_rls_status":
                    result = await self._handle_desktop_rls_status()
                # Batch/Write operations (TOM)
                elif name == "batch_rename_tables":
                    result = await self._handle_batch_rename_tables(args)
                elif name == "batch_rename_columns":
                    result = await self._handle_batch_rename_columns(args)
                elif name == "batch_rename_measures":
                    result = await self._handle_batch_rename_measures(args)
                elif name == "batch_update_measures":
                    result = await self._handle_batch_update_measures(args)
                elif name == "create_measure":
                    result = await self._handle_create_measure(args)
                elif name == "delete_measure":
                    result = await self._handle_delete_measure(args)
                elif name == "scan_table_dependencies":
                    result = await self._handle_scan_table_dependencies(args)
                # PBIP tools (file-based editing)
                elif name == "pbip_load_project":
                    result = await self._handle_pbip_load_project(args)
                elif name == "pbip_get_project_info":
                    result = await self._handle_pbip_get_project_info()
                elif name == "pbip_rename_tables":
                    result = await self._handle_pbip_rename_tables(args)
                elif name == "pbip_rename_columns":
                    result = await self._handle_pbip_rename_columns(args)
                elif name == "pbip_rename_measures":
                    result = await self._handle_pbip_rename_measures(args)
                # PBIP Repair tools
                elif name == "pbip_fix_broken_visuals":
                    result = await self._handle_pbip_fix_broken_visuals(args)
                elif name == "pbip_fix_dax_quoting":
                    result = await self._handle_pbip_fix_dax_quoting()
                elif name == "pbip_scan_broken_refs":
                    result = await self._handle_pbip_scan_broken_refs()
                elif name == "pbip_validate":
                    result = await self._handle_pbip_validate()
                else:
                    result = f"Unknown tool: {name}"

                return [TextContent(type="text", text=result)]

            except Exception as e:
                error_msg = f"Error executing {name}: {str(e)}"
                logger.error(error_msg, exc_info=True)
                return [TextContent(type="text", text=error_msg)]

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
                return "Error: Desktop connectivity unavailable. Ensure psutil and ADOMD.NET are installed."

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

            # Pre-query security check
            policy_check = self.security.pre_query_check(dax_query)
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

            # Pre-query security check
            policy_check = self.security.pre_query_check(dax_query)
            if not policy_check.allowed:
                self.security.log_policy_violation(
                    policy_name="query_policy",
                    violation_type=policy_check.reason,
                    query=dax_query
                )
                return f"Query blocked by security policy: {policy_check.reason}"

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

            # Build response
            response = f"=== Batch Rename Tables ===\n\n{result.message}\n\n"

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

            # Build response
            response = f"=== Batch Rename Columns ===\n\n{result.message}\n\n"

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

            # Build response
            response = f"=== Batch Rename Measures ===\n\n{result.message}\n\n"

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
            auto_save = args.get("auto_save", True)

            if not updates:
                return "Error: 'updates' array is required"

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

            tom = self._get_tom_connector()

            # Create measure
            create_fn = lambda: tom.create_measure(
                table_name, measure_name, expression,
                format_string=format_string,
                description=description
            )
            result = await asyncio.get_event_loop().run_in_executor(None, create_fn)

            if result.success:
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
