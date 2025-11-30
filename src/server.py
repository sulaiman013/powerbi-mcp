#!/usr/bin/env python3
"""
Power BI MCP Server with Dynamic Table Discovery
Uses XMLA endpoint for true dynamic table/column discovery
"""
import asyncio
import json
import logging
import os
import sys
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from mcp.server import Server, NotificationOptions
from mcp.server.models import InitializationOptions
from mcp.server.stdio import stdio_server
from mcp.types import TextContent, Tool

# Load environment variables
load_dotenv()

# Setup logging to stderr (required for MCP)
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stderr
)
logger = logging.getLogger(__name__)

# Import connectors
from powerbi_rest_connector import PowerBIRestConnector
from powerbi_xmla_connector import PowerBIXmlaConnector


class PowerBIMCPServer:
    """MCP Server for Power BI with dynamic table discovery"""

    def __init__(self):
        self.server = Server("powerbi-mcp")
        self.rest_connector: Optional[PowerBIRestConnector] = None
        self.xmla_connector: Optional[PowerBIXmlaConnector] = None

        # Current connection state
        self.current_workspace_id: Optional[str] = None
        self.current_workspace_name: Optional[str] = None
        self.current_dataset_id: Optional[str] = None
        self.current_dataset_name: Optional[str] = None
        self.connected = False

        # Setup handlers
        self._setup_handlers()

    def _get_credentials(self):
        """Get credentials from environment"""
        tenant_id = os.getenv("TENANT_ID")
        client_id = os.getenv("CLIENT_ID")
        client_secret = os.getenv("CLIENT_SECRET")
        return tenant_id, client_id, client_secret

    def _ensure_rest_connector(self) -> bool:
        """Ensure REST connector is authenticated"""
        if self.rest_connector:
            return True

        tenant_id, client_id, client_secret = self._get_credentials()
        if not all([tenant_id, client_id, client_secret]):
            return False

        self.rest_connector = PowerBIRestConnector(tenant_id, client_id, client_secret)
        return self.rest_connector.authenticate()

    def _setup_handlers(self):
        """Setup MCP tool handlers"""

        @self.server.list_tools()
        async def handle_list_tools() -> List[Tool]:
            """List available tools"""
            return [
                Tool(
                    name="list_workspaces",
                    description="List all Power BI workspaces accessible by the Service Principal",
                    inputSchema={"type": "object", "properties": {}}
                ),
                Tool(
                    name="list_datasets",
                    description="List all datasets (semantic models) in a workspace",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "workspace_id": {
                                "type": "string",
                                "description": "Workspace ID to list datasets from"
                            }
                        },
                        "required": ["workspace_id"]
                    }
                ),
                Tool(
                    name="list_tables",
                    description="List all tables in a dataset/semantic model (dynamically discovered via XMLA)",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "workspace_name": {
                                "type": "string",
                                "description": "Name of the workspace (e.g., 'Salesforce Reports')"
                            },
                            "dataset_name": {
                                "type": "string",
                                "description": "Name of the dataset/semantic model (e.g., 'Salesforce BI')"
                            }
                        },
                        "required": ["workspace_name", "dataset_name"]
                    }
                ),
                Tool(
                    name="list_columns",
                    description="List all columns in a specific table",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "workspace_name": {
                                "type": "string",
                                "description": "Name of the workspace"
                            },
                            "dataset_name": {
                                "type": "string",
                                "description": "Name of the dataset/semantic model"
                            },
                            "table_name": {
                                "type": "string",
                                "description": "Name of the table to get columns from"
                            }
                        },
                        "required": ["workspace_name", "dataset_name", "table_name"]
                    }
                ),
                Tool(
                    name="execute_dax",
                    description="Execute a DAX query on the connected dataset",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "workspace_name": {
                                "type": "string",
                                "description": "Name of the workspace"
                            },
                            "dataset_name": {
                                "type": "string",
                                "description": "Name of the dataset/semantic model"
                            },
                            "query": {
                                "type": "string",
                                "description": "DAX query to execute (must start with EVALUATE)"
                            }
                        },
                        "required": ["workspace_name", "dataset_name", "query"]
                    }
                ),
                Tool(
                    name="get_sample_data",
                    description="Get sample rows from a table",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "workspace_name": {
                                "type": "string",
                                "description": "Name of the workspace"
                            },
                            "dataset_name": {
                                "type": "string",
                                "description": "Name of the dataset/semantic model"
                            },
                            "table_name": {
                                "type": "string",
                                "description": "Name of the table"
                            },
                            "num_rows": {
                                "type": "integer",
                                "description": "Number of rows to retrieve (default: 5)",
                                "default": 5
                            }
                        },
                        "required": ["workspace_name", "dataset_name", "table_name"]
                    }
                ),
                Tool(
                    name="get_model_info",
                    description="Get detailed semantic model information including tables, columns, measures, and relationships using INFO.VIEW DAX functions",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "workspace_name": {
                                "type": "string",
                                "description": "Name of the workspace"
                            },
                            "dataset_name": {
                                "type": "string",
                                "description": "Name of the dataset/semantic model"
                            }
                        },
                        "required": ["workspace_name", "dataset_name"]
                    }
                )
            ]

        @self.server.call_tool()
        async def handle_call_tool(name: str, arguments: Optional[Dict[str, Any]]) -> List[TextContent]:
            """Handle tool calls"""
            try:
                logger.info(f"Tool called: {name} with args: {arguments}")
                args = arguments or {}

                if name == "list_workspaces":
                    result = await self._handle_list_workspaces()
                elif name == "list_datasets":
                    result = await self._handle_list_datasets(args)
                elif name == "list_tables":
                    result = await self._handle_list_tables(args)
                elif name == "list_columns":
                    result = await self._handle_list_columns(args)
                elif name == "execute_dax":
                    result = await self._handle_execute_dax(args)
                elif name == "get_sample_data":
                    result = await self._handle_get_sample_data(args)
                elif name == "get_model_info":
                    result = await self._handle_get_model_info(args)
                else:
                    result = f"Unknown tool: {name}"

                return [TextContent(type="text", text=result)]

            except Exception as e:
                error_msg = f"Error executing {name}: {str(e)}"
                logger.error(error_msg, exc_info=True)
                return [TextContent(type="text", text=error_msg)]

    async def _handle_list_workspaces(self) -> str:
        """List all accessible workspaces"""
        try:
            # Ensure REST connector is ready
            if not await asyncio.get_event_loop().run_in_executor(None, self._ensure_rest_connector):
                return "Error: Missing credentials. Set TENANT_ID, CLIENT_ID, CLIENT_SECRET in .env file"

            # List workspaces
            workspaces = await asyncio.get_event_loop().run_in_executor(
                None, self.rest_connector.list_workspaces
            )

            if not workspaces:
                return "No workspaces found. Ensure your Service Principal has access to Power BI workspaces."

            result = f"Found {len(workspaces)} workspace(s):\n\n"
            for i, ws in enumerate(workspaces, 1):
                result += f"{i}. {ws['name']}\n"
                result += f"   ID: {ws['id']}\n"
                result += f"   Type: {ws['type']}\n\n"

            return result

        except Exception as e:
            logger.error(f"List workspaces error: {str(e)}")
            return f"Error listing workspaces: {str(e)}"

    async def _handle_list_datasets(self, args: Dict[str, Any]) -> str:
        """List datasets in a workspace"""
        try:
            workspace_id = args.get("workspace_id")
            if not workspace_id:
                return "Error: workspace_id is required"

            # Ensure REST connector is ready
            if not await asyncio.get_event_loop().run_in_executor(None, self._ensure_rest_connector):
                return "Error: Missing credentials. Set TENANT_ID, CLIENT_ID, CLIENT_SECRET in .env file"

            # List datasets
            datasets = await asyncio.get_event_loop().run_in_executor(
                None, self.rest_connector.list_datasets, workspace_id
            )

            if not datasets:
                return f"No datasets found in workspace {workspace_id}"

            result = f"Found {len(datasets)} dataset(s):\n\n"
            for i, ds in enumerate(datasets, 1):
                result += f"{i}. {ds['name']}\n"
                result += f"   ID: {ds['id']}\n"
                result += f"   Configured by: {ds['configuredBy']}\n\n"

            return result

        except Exception as e:
            logger.error(f"List datasets error: {str(e)}")
            return f"Error listing datasets: {str(e)}"

    def _get_xmla_connector(self, workspace_name: str, dataset_name: str) -> Optional[PowerBIXmlaConnector]:
        """Get or create XMLA connector for the given workspace/dataset"""
        tenant_id, client_id, client_secret = self._get_credentials()
        if not all([tenant_id, client_id, client_secret]):
            return None

        # Check if we need a new connector
        if (self.xmla_connector is None or
            self.current_workspace_name != workspace_name or
            self.current_dataset_name != dataset_name):

            # Create new connector
            self.xmla_connector = PowerBIXmlaConnector(tenant_id, client_id, client_secret)

            # Connect
            if not self.xmla_connector.connect(workspace_name, dataset_name):
                self.xmla_connector = None
                return None

            self.current_workspace_name = workspace_name
            self.current_dataset_name = dataset_name
            self.connected = True

        return self.xmla_connector

    async def _handle_list_tables(self, args: Dict[str, Any]) -> str:
        """List tables in a dataset using XMLA"""
        try:
            workspace_name = args.get("workspace_name")
            dataset_name = args.get("dataset_name")

            if not workspace_name or not dataset_name:
                return "Error: workspace_name and dataset_name are required"

            # Get XMLA connector
            connector = await asyncio.get_event_loop().run_in_executor(
                None, self._get_xmla_connector, workspace_name, dataset_name
            )

            if not connector:
                return f"Error: Could not connect to dataset '{dataset_name}' in workspace '{workspace_name}'. Check workspace/dataset names and XMLA endpoint access."

            # Discover tables
            tables = await asyncio.get_event_loop().run_in_executor(
                None, connector.discover_tables
            )

            if not tables:
                return f"No tables found in dataset '{dataset_name}'"

            result = f"Found {len(tables)} table(s) in '{dataset_name}':\n\n"
            for i, table in enumerate(tables, 1):
                result += f"{i}. {table['name']}\n"
                if table.get('description') and table['description'] != "No description available":
                    result += f"   Description: {table['description']}\n"
                result += "\n"

            return result

        except Exception as e:
            logger.error(f"List tables error: {str(e)}")
            return f"Error listing tables: {str(e)}"

    async def _handle_list_columns(self, args: Dict[str, Any]) -> str:
        """List columns in a table using XMLA"""
        try:
            workspace_name = args.get("workspace_name")
            dataset_name = args.get("dataset_name")
            table_name = args.get("table_name")

            if not all([workspace_name, dataset_name, table_name]):
                return "Error: workspace_name, dataset_name, and table_name are required"

            # Get XMLA connector
            connector = await asyncio.get_event_loop().run_in_executor(
                None, self._get_xmla_connector, workspace_name, dataset_name
            )

            if not connector:
                return f"Error: Could not connect to dataset '{dataset_name}' in workspace '{workspace_name}'"

            # Get table schema
            schema = await asyncio.get_event_loop().run_in_executor(
                None, connector.get_table_schema, table_name
            )

            columns = schema.get("columns", [])
            if not columns:
                return f"No columns found in table '{table_name}'"

            result = f"Found {len(columns)} column(s) in table '{table_name}':\n\n"
            for i, col in enumerate(columns, 1):
                result += f"{i}. {col['name']}\n"
                result += f"   Type: {col.get('type', 'Unknown')}\n"
                if col.get('description'):
                    result += f"   Description: {col['description']}\n"
                result += "\n"

            return result

        except Exception as e:
            logger.error(f"List columns error: {str(e)}")
            return f"Error listing columns: {str(e)}"

    async def _handle_execute_dax(self, args: Dict[str, Any]) -> str:
        """Execute a DAX query"""
        try:
            workspace_name = args.get("workspace_name")
            dataset_name = args.get("dataset_name")
            query = args.get("query")

            if not all([workspace_name, dataset_name, query]):
                return "Error: workspace_name, dataset_name, and query are required"

            # Get XMLA connector
            connector = await asyncio.get_event_loop().run_in_executor(
                None, self._get_xmla_connector, workspace_name, dataset_name
            )

            if not connector:
                return f"Error: Could not connect to dataset '{dataset_name}' in workspace '{workspace_name}'"

            # Execute query
            rows = await asyncio.get_event_loop().run_in_executor(
                None, connector.execute_dax, query
            )

            if not rows:
                return "Query executed successfully but returned no results."

            result = f"Query returned {len(rows)} row(s):\n\n"
            result += json.dumps(rows[:20], indent=2)  # Limit to 20 rows for display

            if len(rows) > 20:
                result += f"\n\n... and {len(rows) - 20} more rows"

            return result

        except Exception as e:
            logger.error(f"Execute DAX error: {str(e)}")
            return f"Error executing DAX query: {str(e)}"

    async def _handle_get_sample_data(self, args: Dict[str, Any]) -> str:
        """Get sample data from a table"""
        try:
            workspace_name = args.get("workspace_name")
            dataset_name = args.get("dataset_name")
            table_name = args.get("table_name")
            num_rows = args.get("num_rows", 5)

            if not all([workspace_name, dataset_name, table_name]):
                return "Error: workspace_name, dataset_name, and table_name are required"

            # Get XMLA connector
            connector = await asyncio.get_event_loop().run_in_executor(
                None, self._get_xmla_connector, workspace_name, dataset_name
            )

            if not connector:
                return f"Error: Could not connect to dataset '{dataset_name}' in workspace '{workspace_name}'"

            # Get sample data
            rows = await asyncio.get_event_loop().run_in_executor(
                None, connector.get_sample_data, table_name, num_rows
            )

            if not rows:
                return f"No data found in table '{table_name}'"

            result = f"Sample data from '{table_name}' ({len(rows)} rows):\n\n"
            result += json.dumps(rows, indent=2)

            return result

        except Exception as e:
            logger.error(f"Get sample data error: {str(e)}")
            return f"Error getting sample data: {str(e)}"

    async def _handle_get_model_info(self, args: Dict[str, Any]) -> str:
        """Get detailed model info using INFO.VIEW DAX functions"""
        try:
            workspace_name = args.get("workspace_name")
            dataset_name = args.get("dataset_name")

            if not workspace_name or not dataset_name:
                return "Error: workspace_name and dataset_name are required"

            # Get XMLA connector
            connector = await asyncio.get_event_loop().run_in_executor(
                None, self._get_xmla_connector, workspace_name, dataset_name
            )

            if not connector:
                return f"Error: Could not connect to dataset '{dataset_name}' in workspace '{workspace_name}'"

            result = f"=== Semantic Model Info: {dataset_name} ===\n\n"

            # Query 1: INFO.VIEW.TABLES()
            try:
                tables_query = "EVALUATE INFO.VIEW.TABLES()"
                tables = await asyncio.get_event_loop().run_in_executor(
                    None, connector.execute_dax, tables_query
                )
                result += f"--- TABLES ({len(tables)}) ---\n"
                for t in tables:
                    name = t.get("[Name]", t.get("Name", "Unknown"))
                    is_hidden = t.get("[IsHidden]", t.get("IsHidden", False))
                    if not is_hidden:
                        result += f"  - {name}\n"
                result += "\n"
            except Exception as e:
                result += f"--- TABLES ---\nError: {str(e)}\n\n"

            # Query 2: INFO.VIEW.COLUMNS()
            try:
                columns_query = "EVALUATE INFO.VIEW.COLUMNS()"
                columns = await asyncio.get_event_loop().run_in_executor(
                    None, connector.execute_dax, columns_query
                )
                result += f"--- COLUMNS ({len(columns)}) ---\n"
                # Group by table
                tables_cols = {}
                for c in columns:
                    table = c.get("[TableName]", c.get("TableName", "Unknown"))
                    col_name = c.get("[Name]", c.get("Name", "Unknown"))
                    data_type = c.get("[DataType]", c.get("DataType", ""))
                    is_hidden = c.get("[IsHidden]", c.get("IsHidden", False))
                    if not is_hidden:
                        if table not in tables_cols:
                            tables_cols[table] = []
                        tables_cols[table].append(f"{col_name} ({data_type})")

                for table, cols in sorted(tables_cols.items()):
                    result += f"  {table}:\n"
                    for col in cols[:10]:
                        result += f"    - {col}\n"
                    if len(cols) > 10:
                        result += f"    ... and {len(cols) - 10} more\n"
                result += "\n"
            except Exception as e:
                result += f"--- COLUMNS ---\nError: {str(e)}\n\n"

            # Query 3: INFO.VIEW.MEASURES()
            try:
                measures_query = "EVALUATE INFO.VIEW.MEASURES()"
                measures = await asyncio.get_event_loop().run_in_executor(
                    None, connector.execute_dax, measures_query
                )
                result += f"--- MEASURES ({len(measures)}) ---\n"
                for m in measures:
                    name = m.get("[Name]", m.get("Name", "Unknown"))
                    table = m.get("[TableName]", m.get("TableName", ""))
                    expr = m.get("[Expression]", m.get("Expression", ""))
                    # Truncate long expressions
                    if len(expr) > 50:
                        expr = expr[:50] + "..."
                    result += f"  - [{table}]{name}\n"
                    if expr:
                        result += f"    Expression: {expr}\n"
                result += "\n"
            except Exception as e:
                result += f"--- MEASURES ---\nError: {str(e)}\n\n"

            # Query 4: INFO.VIEW.RELATIONSHIPS()
            try:
                rels_query = "EVALUATE INFO.VIEW.RELATIONSHIPS()"
                rels = await asyncio.get_event_loop().run_in_executor(
                    None, connector.execute_dax, rels_query
                )
                result += f"--- RELATIONSHIPS ({len(rels)}) ---\n"
                for r in rels:
                    from_table = r.get("[FromTableName]", r.get("FromTableName", ""))
                    from_col = r.get("[FromColumnName]", r.get("FromColumnName", ""))
                    to_table = r.get("[ToTableName]", r.get("ToTableName", ""))
                    to_col = r.get("[ToColumnName]", r.get("ToColumnName", ""))
                    cardinality = r.get("[Cardinality]", r.get("Cardinality", ""))
                    is_active = r.get("[IsActive]", r.get("IsActive", True))

                    status = "" if is_active else " [INACTIVE]"
                    result += f"  - {from_table}[{from_col}] -> {to_table}[{to_col}] ({cardinality}){status}\n"
                result += "\n"
            except Exception as e:
                result += f"--- RELATIONSHIPS ---\nError: {str(e)}\n\n"

            return result

        except Exception as e:
            logger.error(f"Get model info error: {str(e)}")
            return f"Error getting model info: {str(e)}"

    async def run(self):
        """Run the MCP server"""
        async with stdio_server() as (read_stream, write_stream):
            logger.info("Power BI MCP Server starting...")
            await self.server.run(
                read_stream,
                write_stream,
                InitializationOptions(
                    server_name="powerbi-mcp",
                    server_version="1.0.0",
                    capabilities=self.server.get_capabilities(
                        notification_options=NotificationOptions(),
                        experimental_capabilities={}
                    )
                )
            )


async def main():
    """Main entry point"""
    server = PowerBIMCPServer()
    await server.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Server stopped by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
