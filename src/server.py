import argparse
import asyncio
import json
import logging
import os
import re
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional

import anyio  # Needed for server run loop cleanup
import openai
import uvicorn
from dotenv import load_dotenv
from mcp.server import NotificationOptions, Server
from mcp.server.models import InitializationOptions
from mcp.server.sse import SseServerTransport
from starlette.applications import Starlette
from starlette.responses import Response
from starlette.routing import Mount, Route

# Configure logging to stderr for MCP debugging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", stream=sys.stderr
)
logger = logging.getLogger(__name__)

# Updated imports
try:
    from mcp.server.types import ToolResult
    from mcp.types import Prompt, Resource, TextContent, Tool
except ImportError:
    from mcp.types import TextContent, Tool

    # Define missing types as stubs if not available
    Resource = None
    Prompt = None


# Custom JSON encoder for Power BI data types
class PowerBIJSONEncoder(json.JSONEncoder):
    """Custom JSON encoder to handle Power BI data types"""

    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        elif isinstance(obj, Decimal):
            return float(obj)
        elif hasattr(obj, "__dict__"):
            return str(obj)
        return super().default(obj)


def safe_json_dumps(data, indent=2):
    """Safely serialize data containing datetime and other non-JSON types"""
    return json.dumps(data, indent=indent, cls=PowerBIJSONEncoder)


def clean_dax_query(dax_query: str) -> str:
    """Remove HTML/XML tags and other artifacts from DAX queries"""
    # Remove HTML/XML tags like <oii>, </oii>, etc.
    cleaned = re.sub(r"<[^>]+>", "", dax_query)
    # Collapse extra whitespace
    cleaned = " ".join(cleaned.split())
    return cleaned


# Load environment variables
load_dotenv()

# Prepare ADOMD.NET search paths before importing pyadomd
env_adomd = os.environ.get("ADOMD_LIB_DIR")

# Try to use NuGet packages first (if available)
user_nuget_path = os.path.expanduser(r"~\.nuget\packages")
nuget_adomd_path = os.path.join(
    user_nuget_path, "microsoft.analysisservices.adomdclient.netcore.retail.amd64", "19.84.1", "lib", "netcoreapp3.0"
)
nuget_config_path = os.path.join(user_nuget_path, "system.configuration.configurationmanager", "9.0.7", "lib", "net8.0")
nuget_identity_path = os.path.join(user_nuget_path, "microsoft.identity.client", "4.74.0", "lib", "net8.0")
nuget_identity_abs_path = os.path.join(
    user_nuget_path, "microsoft.identitymodel.abstractions", "6.35.0", "lib", "net6.0"
)

adomd_paths = [
    env_adomd,
    nuget_adomd_path if os.path.exists(nuget_adomd_path) else None,
    nuget_config_path if os.path.exists(nuget_config_path) else None,
    nuget_identity_path if os.path.exists(nuget_identity_path) else None,
    nuget_identity_abs_path if os.path.exists(nuget_identity_abs_path) else None,
    r"C:\\Program Files\\Microsoft.NET\\ADOMD.NET\\160",
    r"C:\\Program Files\\Microsoft.NET\\ADOMD.NET\\150",
    r"C:\\Program Files (x86)\\Microsoft.NET\\ADOMD.NET\\160",
    r"C:\\Program Files (x86)\\Microsoft.NET\\ADOMD.NET\\150",
    r"C:\\Program Files (x86)\\MicrosoftOffice\\root\\vfs\\ProgramFilesX86\\Microsoft.NET\\ADOMD.NET\\130",
]
logger.info(
    "Adding ADOMD.NET paths to sys.path: %s",
    ", ".join([p for p in adomd_paths if p]),
)
for p in adomd_paths:
    if p and os.path.exists(p):
        sys.path.append(p)

import platform
import sys

# Ensure pythonnet uses coreclr runtime (works on Linux) - MUST be done before importing clr
import pythonnet

# Choose appropriate runtime based on platform
# Use coreclr by default on all platforms as it works with our NuGet packages
pythonnet_runtime = os.environ.get("PYTHONNET_RUNTIME", "coreclr")

logger.info("Configuring pythonnet runtime: %s for %s", pythonnet_runtime, platform.system())
try:
    pythonnet.set_runtime(pythonnet_runtime)
except Exception as e:  # pragma: no cover - best effort
    logger.warning("Failed to set pythonnet runtime: %s", e)
    # Try alternative runtime
    try:
        if platform.system() == "Linux":
            pythonnet.set_runtime("mono")
            logger.info("Fallback to mono runtime")
        else:
            pythonnet.set_runtime("coreclr")
            logger.info("Fallback to coreclr runtime")
    except Exception as e2:  # pragma: no cover - best effort
        logger.warning("Failed to set fallback runtime: %s", e2)

# Attempt to import clr and pyadomd. These may be missing when ADOMD.NET is not
# installed. We load the actual ADOMD.NET assembly later if possible.
try:
    import clr  # type: ignore
    from pyadomd import Pyadomd  # type: ignore

    logger.debug("pythonnet and pyadomd imported successfully")
except Exception as e:  # pragma: no cover - runtime environment dependent
    clr = None
    Pyadomd = None
    logger.warning("pyadomd not available: %s", e)


# Placeholder for AdomdSchemaGuid if the assembly fails to load
class _DummySchemaGuid:
    Tables = 0


AdomdSchemaGuid = _DummySchemaGuid

# Try to load ADOMD.NET assemblies if clr is available and not skipped
adomd_loaded = False
skip_adomd_load = os.environ.get("SKIP_ADOMD_LOAD", "0").lower() in ("1", "true", "yes")

if clr and not skip_adomd_load:
    logger.info("Searching for ADOMD.NET in: %s", ", ".join([p for p in adomd_paths if p]))
    for path in adomd_paths:
        if not path:
            continue
        if os.path.exists(path):
            dll = os.path.join(path, "Microsoft.AnalysisServices.AdomdClient.dll")
            try:
                sys.path.append(path)
                clr.AddReference(dll)
                adomd_loaded = True
                logger.info("Loaded ADOMD.NET from %s", dll)
                break
            except Exception as e:  # pragma: no cover - best effort
                logger.warning("Failed to load ADOMD.NET from %s: %s", dll, e)
                continue

    if adomd_loaded:
        try:
            from Microsoft.AnalysisServices.AdomdClient import AdomdSchemaGuid as _ASG

            globals()["AdomdSchemaGuid"] = _ASG
            logger.debug("ADOMD.NET types imported")
        except Exception as e:  # pragma: no cover - best effort
            logger.warning("Failed to import AdomdSchemaGuid: %s", e)

if not adomd_loaded:
    if skip_adomd_load:
        logger.info("ADOMD.NET loading skipped due to SKIP_ADOMD_LOAD environment variable")
    else:
        logger.warning("ADOMD.NET library not found. Pyadomd functionality will be disabled.")


class PowerBIConnector:
    def __init__(self):
        self.connection_string = None
        self.connected = False
        self.tables = []
        self.metadata = {}
        # Thread pool for async operations
        self.executor = ThreadPoolExecutor(max_workers=4)

    def _check_pyadomd(self):
        if Pyadomd is None:
            raise Exception("Pyadomd library not available. Ensure .NET runtime and ADOMD.NET are installed")

    def connect(
        self, xmla_endpoint: str, tenant_id: str, client_id: str, client_secret: str, initial_catalog: str
    ) -> bool:
        """Establish connection to Power BI dataset"""
        self._check_pyadomd()
        self.connection_string = (
            f"Provider=MSOLAP;"
            f"Data Source={xmla_endpoint};"
            f"Initial Catalog={initial_catalog};"
            f"User ID=app:{client_id}@{tenant_id};"
            f"Password={client_secret};"
        )

        try:
            # Test connection
            with Pyadomd(self.connection_string):
                self.connected = True
                logger.info(f"Connected to Power BI dataset: {initial_catalog}")
                # Don't discover tables during connection to speed up
                return True
        except Exception as e:
            self.connected = False
            logger.error(f"Connection failed: {str(e)}")
            raise Exception(f"Connection failed: {str(e)}")

    def discover_tables(self) -> List[Dict[str, Any]]:
        """Discover all user-facing tables in the dataset with their descriptions and relationships"""
        if not self.connected:
            raise Exception("Not connected to Power BI")

        self._check_pyadomd()

        # Return cached tables if already discovered
        if self.tables:
            return self.tables

        tables_list = []
        try:
            with Pyadomd(self.connection_string) as pyadomd_conn:
                adomd_connection = pyadomd_conn.conn
                tables_dataset = adomd_connection.GetSchemaDataSet(AdomdSchemaGuid.Tables, None)

                tables_list_obj = getattr(tables_dataset, "Tables", None)
                if tables_list_obj and len(tables_list_obj) > 0:
                    schema_table = tables_list_obj[0]
                    for row in schema_table.Rows:
                        table_name = row["TABLE_NAME"]
                        if (
                            not table_name.startswith("$")
                            and not table_name.startswith("DateTableTemplate_")
                            and not row["TABLE_SCHEMA"] == "$SYSTEM"
                        ):
                            # Get table description from TMSCHEMA_TABLES
                            table_description = self._get_table_description_direct(table_name)

                            # Get relationships for this table
                            table_relationships = self._get_table_relationships(table_name)

                            tables_list.append(
                                {
                                    "name": table_name,
                                    "description": table_description or "No description available",
                                    "relationships": table_relationships,
                                }
                            )

            self.tables = tables_list
            logger.info(f"Discovered {len(tables_list)} tables with relationships")
            return tables_list
        except Exception as e:
            logger.error(f"Failed to discover tables: {str(e)}")
            raise Exception(f"Failed to discover tables: {str(e)}")

    def get_table_schema(self, table_name: str) -> Dict[str, Any]:
        """Get schema information for a specific table including description and column descriptions"""
        if not self.connected:
            raise Exception("Not connected to Power BI")

        self._check_pyadomd()

        try:
            with Pyadomd(self.connection_string) as conn:
                cursor = conn.cursor()

                # Get table description
                table_description = self._get_table_description_direct(table_name)

                # Try to get column information
                try:
                    dax_query = f"EVALUATE TOPN(1, '{table_name}')"
                    cursor.execute(dax_query)
                    columns = [desc[0] for desc in cursor.description]
                    cursor.close()

                    # Get column descriptions from TMSCHEMA_COLUMNS
                    column_descriptions = self._get_column_descriptions(table_name)

                    # Create enhanced columns list with descriptions
                    enhanced_columns = []
                    for col_name in columns:
                        # Find description for this column
                        col_description = None
                        col_data_type = None

                        # Try exact match first
                        for col_info in column_descriptions:
                            if col_info["name"] == col_name:
                                col_description = col_info["description"]
                                col_data_type = col_info["data_type"]
                                break

                        # If no exact match, try partial match (remove table prefix if present)
                        if not col_description:
                            # Extract column name from format like "Employee Skills[Id]" -> "Id"
                            if "[" in col_name and "]" in col_name:
                                clean_col_name = col_name.split("[")[1].replace("]", "")
                            else:
                                clean_col_name = col_name

                            for col_info in column_descriptions:
                                if col_info["name"] == clean_col_name:
                                    col_description = col_info["description"]
                                    col_data_type = col_info["data_type"]
                                    logger.debug(f"Matched column '{col_name}' with '{clean_col_name}'")
                                    break

                        # Create enhanced column info
                        enhanced_columns.append(
                            {
                                "name": col_name,
                                "description": col_description or "No description available",
                                "data_type": col_data_type,
                            }
                        )

                    return {
                        "table_name": table_name,
                        "type": "data_table",
                        "description": table_description or "No description available",
                        "columns": enhanced_columns,
                    }
                except:
                    # This might be a measure table
                    cursor.close()
                    measure_info = self.get_measures_for_table(table_name)
                    measure_info["description"] = table_description or "No description available"
                    return measure_info

        except Exception as e:
            logger.error(f"Failed to get schema for table '{table_name}': {str(e)}")
            raise Exception(f"Failed to get schema for table '{table_name}': {str(e)}")

    def _get_table_description_direct(self, table_name: str) -> Optional[str]:
        """Get table description using direct Pyadomd connection"""
        try:
            with Pyadomd(self.connection_string) as conn:
                cursor = conn.cursor()
                # Try different approaches to get table description

                # First try: Direct query
                desc_query = f"SELECT [Description] FROM $SYSTEM.TMSCHEMA_TABLES WHERE [Name] = '{table_name}'"
                logger.debug(f"Trying query: {desc_query}")
                cursor.execute(desc_query)
                results = cursor.fetchall()  # Use fetchall instead of fetchone
                cursor.close()

                if results and len(results) > 0:
                    result = results[0]
                    if result and len(result) > 0 and result[0]:
                        logger.debug(f"Found description for {table_name}: {result[0]}")
                        return str(result[0])
                    else:
                        logger.debug(f"Description is empty for table {table_name}")
                else:
                    logger.debug(f"No results found for table {table_name}")

                # Second try: Show all columns to understand the schema
                cursor = conn.cursor()
                debug_query = "SELECT TOP 5 [Name], [Description] FROM $SYSTEM.TMSCHEMA_TABLES"
                logger.debug(f"Debug query: {debug_query}")
                cursor.execute(debug_query)
                debug_results = cursor.fetchall()
                cursor.close()

                logger.debug(f"Debug results from TMSCHEMA_TABLES: {debug_results}")

                return None
        except Exception as e:
            logger.debug(f"Failed to get description for table '{table_name}': {str(e)}")
            return None

    def _get_table_relationships(self, table_name: str) -> List[Dict[str, Any]]:
        """Get relationships for a specific table"""
        try:
            with Pyadomd(self.connection_string) as conn:
                # First get the table ID
                cursor = conn.cursor()
                table_id_query = f"SELECT [ID] FROM $SYSTEM.TMSCHEMA_TABLES WHERE [Name] = '{table_name}'"
                logger.debug(f"Table ID query: {table_id_query}")
                cursor.execute(table_id_query)
                table_id_result = cursor.fetchone()
                logger.debug(f"fetchone() returned: {table_id_result} (type: {type(table_id_result)})")

                if not table_id_result:
                    cursor.close()
                    logger.debug(f"Table ID not found for {table_name}")
                    return []

                # fetchone() returns a generator that yields tuples, we need the first value from the first tuple
                table_id_tuple = list(table_id_result)[0]  # Get first tuple from generator: (13,)
                table_id = table_id_tuple[0]  # Get the integer from the tuple: 13
                cursor.close()
                logger.debug(f"Found table ID {table_id} (type: {type(table_id)}) for table '{table_name}'")
                relationships = []

                # First, let's see what relationships exist in the model at all
                cursor = conn.cursor()
                all_rels_query = "SELECT [FromTableID], [ToTableID], [IsActive] FROM $SYSTEM.TMSCHEMA_RELATIONSHIPS"
                logger.debug(f"All relationships query: {all_rels_query}")
                cursor.execute(all_rels_query)
                all_rels = cursor.fetchall()
                cursor.close()
                logger.debug(f"Found {len(all_rels)} total relationships in model: {all_rels}")

                # Get relationships where this table is the "From" table (Many side)
                cursor = conn.cursor()
                from_rel_query = f"""
                SELECT
                    [ToTableID], [ToColumnID], [FromColumnID],
                    [IsActive], [Type], [CrossFilteringBehavior],
                    [FromCardinality], [ToCardinality]
                FROM $SYSTEM.TMSCHEMA_RELATIONSHIPS
                WHERE [FromTableID] = {table_id}
                """
                logger.debug(f"From relationships query: {from_rel_query}")
                cursor.execute(from_rel_query)
                from_relationships = cursor.fetchall()
                cursor.close()
                logger.debug(
                    f"Found {len(from_relationships)} 'from' relationships for table {table_name}: {from_relationships}"
                )

                # Process "From" relationships (where current table is Many side)
                for rel in from_relationships:
                    to_table_id, to_column_id, from_column_id = rel[0], rel[1], rel[2]
                    is_active, _, cross_filter = rel[3], rel[4], rel[5]
                    from_cardinality, to_cardinality = rel[6], rel[7]

                    # Get related table name
                    cursor = conn.cursor()
                    to_table_query = f"SELECT [Name] FROM $SYSTEM.TMSCHEMA_TABLES WHERE [ID] = {to_table_id}"
                    cursor.execute(to_table_query)
                    to_table_result = cursor.fetchone()

                    if to_table_result:
                        to_table_name = list(to_table_result)[0][0]  # Extract string from tuple
                        cursor.close()

                        # Get column names
                        cursor = conn.cursor()
                        from_col_query = (
                            f"SELECT [ExplicitName] FROM $SYSTEM.TMSCHEMA_COLUMNS WHERE [ID] = {from_column_id}"
                        )
                        cursor.execute(from_col_query)
                        from_col_result = cursor.fetchone()
                        from_col_name = (
                            list(from_col_result)[0][0] if from_col_result else None
                        )  # Extract string from tuple
                        cursor.close()

                        cursor = conn.cursor()
                        to_col_query = (
                            f"SELECT [ExplicitName] FROM $SYSTEM.TMSCHEMA_COLUMNS WHERE [ID] = {to_column_id}"
                        )
                        cursor.execute(to_col_query)
                        to_col_result = cursor.fetchone()
                        to_col_name = list(to_col_result)[0][0] if to_col_result else None  # Extract string from tuple
                        cursor.close()

                        if from_col_name and to_col_name:
                            relationships.append(
                                {
                                    "relatedTable": to_table_name,
                                    "fromColumn": from_col_name,
                                    "toColumn": to_col_name,
                                    "cardinality": self._format_cardinality(from_cardinality, to_cardinality),
                                    "isActive": bool(is_active),
                                    "crossFilterDirection": self._format_cross_filter(cross_filter),
                                    "relationshipType": "Many-to-One",
                                }
                            )
                    else:
                        cursor.close()

                # Get relationships where this table is the "To" table (One side)
                cursor = conn.cursor()
                to_rel_query = f"""
                SELECT
                    [FromTableID], [FromColumnID], [ToColumnID],
                    [IsActive], [Type], [CrossFilteringBehavior],
                    [FromCardinality], [ToCardinality]
                FROM $SYSTEM.TMSCHEMA_RELATIONSHIPS
                WHERE [ToTableID] = {table_id}
                """
                logger.debug(f"To relationships query: {to_rel_query}")
                cursor.execute(to_rel_query)
                to_relationships = cursor.fetchall()
                cursor.close()
                logger.debug(
                    f"Found {len(to_relationships)} 'to' relationships for table {table_name}: {to_relationships}"
                )

                # Process "To" relationships (where current table is One side)
                for rel in to_relationships:
                    from_table_id, from_column_id, to_column_id = rel[0], rel[1], rel[2]
                    is_active, _, cross_filter = rel[3], rel[4], rel[5]
                    from_cardinality, to_cardinality = rel[6], rel[7]

                    # Get related table name
                    cursor = conn.cursor()
                    from_table_query = f"SELECT [Name] FROM $SYSTEM.TMSCHEMA_TABLES WHERE [ID] = {from_table_id}"
                    cursor.execute(from_table_query)
                    from_table_result = cursor.fetchone()

                    if from_table_result:
                        from_table_name = list(from_table_result)[0][0]  # Extract string from tuple
                        cursor.close()

                        # Get column names
                        cursor = conn.cursor()
                        from_col_query = (
                            f"SELECT [ExplicitName] FROM $SYSTEM.TMSCHEMA_COLUMNS WHERE [ID] = {from_column_id}"
                        )
                        cursor.execute(from_col_query)
                        from_col_result = cursor.fetchone()
                        from_col_name = (
                            list(from_col_result)[0][0] if from_col_result else None
                        )  # Extract string from tuple
                        cursor.close()

                        cursor = conn.cursor()
                        to_col_query = (
                            f"SELECT [ExplicitName] FROM $SYSTEM.TMSCHEMA_COLUMNS WHERE [ID] = {to_column_id}"
                        )
                        cursor.execute(to_col_query)
                        to_col_result = cursor.fetchone()
                        to_col_name = list(to_col_result)[0][0] if to_col_result else None  # Extract string from tuple
                        cursor.close()

                        if from_col_name and to_col_name:
                            relationships.append(
                                {
                                    "relatedTable": from_table_name,
                                    "fromColumn": to_col_name,  # Current table column
                                    "toColumn": from_col_name,  # Related table column
                                    "cardinality": self._format_cardinality(to_cardinality, from_cardinality),
                                    "isActive": bool(is_active),
                                    "crossFilterDirection": self._format_cross_filter(cross_filter),
                                    "relationshipType": "One-to-Many",
                                }
                            )
                    else:
                        cursor.close()

                logger.debug(f"Found {len(relationships)} relationships for table {table_name}")
                return relationships

        except Exception as e:
            logger.warning(f"Failed to get relationships for table '{table_name}': {str(e)}")
            import traceback

            logger.debug(f"Traceback: {traceback.format_exc()}")
            return []

    def _get_column_descriptions(self, table_name: str) -> List[Dict[str, Any]]:
        """Get column descriptions for a specific table"""
        try:
            with Pyadomd(self.connection_string) as conn:
                # First get the table ID
                cursor = conn.cursor()
                table_id_query = f"SELECT [ID] FROM $SYSTEM.TMSCHEMA_TABLES WHERE [Name] = '{table_name}'"
                logger.debug(f"Table ID query for column descriptions: {table_id_query}")
                cursor.execute(table_id_query)
                table_id_result = cursor.fetchone()

                if not table_id_result:
                    cursor.close()
                    logger.debug(f"Table ID not found for {table_name}")
                    return []

                # Extract table ID from generator and tuple
                table_id_tuple = list(table_id_result)[0]  # Get first tuple from generator
                table_id = table_id_tuple[0]  # Get the integer from the tuple
                cursor.close()
                logger.debug(f"Found table ID: {table_id} for column descriptions")

                # Get column descriptions from TMSCHEMA_COLUMNS
                cursor = conn.cursor()
                columns_query = f"""
                SELECT
                    [ExplicitName] as ColumnName,
                    [Description] as ColumnDescription,
                    [ExplicitDataType] as DataType
                FROM $SYSTEM.TMSCHEMA_COLUMNS
                WHERE [TableID] = {table_id}
                ORDER BY [ExplicitName]
                """
                logger.debug(f"Columns query: {columns_query}")
                cursor.execute(columns_query)
                columns_results = cursor.fetchall()
                cursor.close()

                logger.debug(f"Found {len(columns_results)} columns with metadata")

                # Process results
                column_descriptions = []
                for col_result in columns_results:
                    col_name = col_result[0] if col_result[0] else "Unknown"
                    col_description = col_result[1] if len(col_result) > 1 and col_result[1] else None
                    col_data_type = col_result[2] if len(col_result) > 2 else None

                    column_descriptions.append(
                        {"name": col_name, "description": col_description, "data_type": col_data_type}
                    )

                    # Debug output
                    desc_text = col_description if col_description else "No description"
                    logger.debug(f"Column {col_name} ({col_data_type}): {desc_text}")

                return column_descriptions

        except Exception as e:
            logger.warning(f"Failed to get column descriptions for table '{table_name}': {str(e)}")
            import traceback

            logger.debug(f"Traceback: {traceback.format_exc()}")
            return []

    def _format_cardinality(self, from_cardinality: int, to_cardinality: int) -> str:
        """Format cardinality based on numeric codes"""
        cardinality_map = {1: "One", 2: "Many"}
        from_card = cardinality_map.get(from_cardinality, "Unknown")
        to_card = cardinality_map.get(to_cardinality, "Unknown")
        return f"{from_card}-to-{to_card}"

    def _format_cross_filter(self, cross_filter_behavior: int) -> str:
        """Format cross filter direction based on numeric codes"""
        cross_filter_map = {1: "Single", 2: "Both", 3: "Automatic", 4: "None"}
        return cross_filter_map.get(cross_filter_behavior, "Unknown")

    def get_measures_for_table(self, table_name: str) -> Dict[str, Any]:
        """Get measures for a measure table"""
        if not self.connected:
            raise Exception("Not connected to Power BI")

        self._check_pyadomd()
        try:
            with Pyadomd(self.connection_string) as conn:
                # Get table ID
                id_cursor = conn.cursor()
                id_query = f"SELECT [ID] FROM $SYSTEM.TMSCHEMA_TABLES WHERE [Name] = '{table_name}'"
                id_cursor.execute(id_query)
                table_id_result = id_cursor.fetchone()
                id_cursor.close()

                if not table_id_result:
                    return {"table_name": table_name, "type": "unknown", "measures": []}

                table_id = list(table_id_result)[0]

                # Get measures
                measure_cursor = conn.cursor()
                measure_query = f"SELECT [Name], [Expression] FROM $SYSTEM.TMSCHEMA_MEASURES WHERE [TableID] = {table_id} ORDER BY [Name]"
                measure_cursor.execute(measure_query)
                measures = measure_cursor.fetchall()
                measure_cursor.close()

                return {
                    "table_name": table_name,
                    "type": "measure_table",
                    "measures": [{"name": m[0], "dax": m[1]} for m in measures],
                }

        except Exception as e:
            logger.error(f"Failed to get measures for table '{table_name}': {str(e)}")
            return {"table_name": table_name, "type": "error", "error": str(e)}

    def execute_dax_query(self, dax_query: str) -> List[Dict[str, Any]]:
        """Execute a DAX query and return results"""
        if not self.connected:
            raise Exception("Not connected to Power BI")

        self._check_pyadomd()

        # Clean the DAX query
        cleaned_query = clean_dax_query(dax_query)
        logger.info(f"Executing DAX query: {cleaned_query}")

        try:
            with Pyadomd(self.connection_string) as conn:
                cursor = conn.cursor()
                cursor.execute(cleaned_query)

                headers = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                cursor.close()

                # Convert to list of dictionaries
                results = []
                for row in rows:
                    results.append(dict(zip(headers, row)))

                logger.info(f"Query returned {len(results)} rows")
                return results

        except Exception as e:
            logger.error(f"DAX query failed: {str(e)}")
            raise Exception(f"DAX query failed: {str(e)}")

    def get_sample_data(self, table_name: str, num_rows: int = 10) -> List[Dict[str, Any]]:
        """Get sample data from a table"""
        dax_query = f"EVALUATE TOPN({num_rows}, '{table_name}')"
        return self.execute_dax_query(dax_query)


class DataAnalyzer:
    def __init__(self, api_key: str):
        self.client = openai.OpenAI(api_key=api_key)
        self.context = {"tables": [], "schemas": {}, "sample_data": {}}

    def set_data_context(
        self, tables: List[str], schemas: Dict[str, Any], sample_data: Dict[str, List[Dict[str, Any]]]
    ):
        """Set the data context for the analyzer"""
        self.context = {"tables": tables, "schemas": schemas, "sample_data": sample_data}

    def generate_dax_query(self, user_question: str) -> str:
        """Generate a DAX query based on user question"""
        prompt = f"""
        You are a Power BI DAX expert. Generate a DAX query to answer the following question.

        Available tables and their schemas:
        {safe_json_dumps(self.context['schemas'], indent=2)}

        Sample data for reference:
        {safe_json_dumps(self.context['sample_data'], indent=2)}

        User question: {user_question}

        IMPORTANT RULES:
        1. Generate only the DAX query without any explanation
        2. Do NOT use any HTML or XML tags in the query
        3. Do NOT use angle brackets < or > except for DAX operators
        4. Use only valid DAX syntax
        5. Reference only columns that exist in the schema
        6. The query should be executable as-is

        Example format:
        EVALUATE SUMMARIZE(Sales, Product[Category], "Total", SUM(Sales[Amount]))
        """

        response = self.client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": "You are a DAX query expert. Generate only valid, clean DAX queries without any markup or formatting.",
                },
                {"role": "user", "content": prompt},
            ],
            temperature=0.1,
        )

        query = response.choices[0].message.content.strip()
        # Clean any remaining artifacts
        return clean_dax_query(query)

    def interpret_results(self, user_question: str, query_results: List[Dict[str, Any]], dax_query: str) -> str:
        """Interpret query results and provide a natural language answer"""
        prompt = f"""
        You are a data analyst helping users understand their Power BI data.

        User question: {user_question}

        DAX query executed: {dax_query}

        Query results:
        {safe_json_dumps(query_results, indent=2)}

        Provide a clear, concise answer to the user's question based on the results.
        Include relevant numbers and insights. Format the response in a user-friendly way.
        Do not use any HTML or XML markup in your response.
        """

        response = self.client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a helpful data analyst providing insights from Power BI data."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.3,
        )

        return response.choices[0].message.content

    def suggest_questions(self) -> List[str]:
        """Suggest relevant questions based on available data"""
        prompt = f"""
        Based on the following Power BI dataset structure, suggest 5 interesting questions a user might ask:

        Tables and schemas:
        {safe_json_dumps(self.context['schemas'], indent=2)}

        Generate 5 diverse questions that would showcase different aspects of the data.
        Return only the questions as a JSON array.
        """

        response = self.client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a data analyst suggesting interesting questions about data."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.7,
        )

        try:
            questions = json.loads(response.choices[0].message.content)
            return questions
        except:
            return [
                "What are the total sales?",
                "Show me the top 10 products",
                "What is the trend over time?",
                "Which region has the highest revenue?",
                "What are the key metrics?",
            ]


class PowerBIMCPServer:
    def __init__(self, host: Optional[str] = None, port: Optional[int] = None):
        self.host = host or os.getenv("HOST", "0.0.0.0")
        self.port = int(port or os.getenv("PORT", "8000"))
        self.server = Server("powerbi-mcp-server")
        self.connector = PowerBIConnector()
        self.analyzer = None
        self.is_connected = False
        self.connection_lock = threading.Lock()

        # Setup server handlers
        self._setup_handlers()

    def _openai_enabled(self) -> bool:
        """Return True if OpenAI features are enabled"""
        return bool(os.getenv("OPENAI_API_KEY"))

    def _setup_handlers(self):
        @self.server.list_tools()
        async def handle_list_tools() -> List[Tool]:
            tools = [
                Tool(
                    name="connect_powerbi",
                    description="Connect to a Power BI dataset using XMLA endpoint",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "xmla_endpoint": {"type": "string", "description": "Power BI XMLA endpoint URL"},
                            "tenant_id": {"type": "string", "description": "Azure AD Tenant ID (optional)"},
                            "client_id": {"type": "string", "description": "Service Principal Client ID (optional)"},
                            "client_secret": {
                                "type": "string",
                                "description": "Service Principal Client Secret (optional)",
                            },
                            "initial_catalog": {"type": "string", "description": "Dataset name"},
                        },
                        "required": ["xmla_endpoint", "initial_catalog"],
                    },
                ),
                Tool(
                    name="list_tables",
                    description="List all available tables in the connected Power BI dataset",
                    inputSchema={"type": "object", "properties": {}},
                ),
                Tool(
                    name="get_table_info",
                    description="Get detailed information about a specific table",
                    inputSchema={
                        "type": "object",
                        "properties": {"table_name": {"type": "string", "description": "Name of the table"}},
                        "required": ["table_name"],
                    },
                ),
                Tool(
                    name="execute_dax",
                    description="Execute a custom DAX query",
                    inputSchema={
                        "type": "object",
                        "properties": {"dax_query": {"type": "string", "description": "DAX query to execute"}},
                        "required": ["dax_query"],
                    },
                ),
            ]

            if self._openai_enabled():
                tools.append(
                    Tool(
                        name="query_data",
                        description="Ask a question about the data in natural language",
                        inputSchema={
                            "type": "object",
                            "properties": {
                                "question": {"type": "string", "description": "Your question about the data"}
                            },
                            "required": ["question"],
                        },
                    )
                )
                tools.append(
                    Tool(
                        name="suggest_questions",
                        description="Get suggestions for interesting questions to ask about the data",
                        inputSchema={"type": "object", "properties": {}},
                    )
                )

            return tools

        @self.server.list_resources()
        async def handle_list_resources():
            """Return empty list of resources - stub implementation"""
            return []

        @self.server.list_prompts()
        async def handle_list_prompts():
            """Return empty list of prompts - stub implementation"""
            return []

        @self.server.call_tool()
        async def handle_call_tool(name: str, arguments: Optional[Dict[str, Any]]) -> List[TextContent]:
            """Handle tool calls and return results as TextContent"""
            try:
                logger.info(f"Handling tool call: {name}")

                if name == "connect_powerbi":
                    result = await self._handle_connect(arguments)
                elif name == "list_tables":
                    result = await self._handle_list_tables()
                elif name == "get_table_info":
                    result = await self._handle_get_table_info(arguments)
                elif name == "query_data":
                    if not self._openai_enabled():
                        result = "OpenAI API key not configured."
                    else:
                        result = await self._handle_query_data(arguments)
                elif name == "execute_dax":
                    result = await self._handle_execute_dax(arguments)
                elif name == "suggest_questions":
                    if not self._openai_enabled():
                        result = "OpenAI API key not configured."
                    else:
                        result = await self._handle_suggest_questions()
                else:
                    logger.warning(f"Unknown tool: {name}")
                    return [TextContent(type="text", text=f"Unknown tool: {name}")]

                # Convert string result to TextContent
                return [TextContent(type="text", text=result)]

            except Exception as e:
                logger.error(f"Error executing {name}: {str(e)}", exc_info=True)
                return [TextContent(type="text", text=f"Error executing {name}: {str(e)}")]

    async def _handle_connect(self, arguments: Dict[str, Any]) -> str:
        """Handle connection to Power BI"""
        try:
            with self.connection_lock:
                # Connect to Power BI
                tenant_id = arguments.get("tenant_id") or os.getenv("DEFAULT_TENANT_ID")
                client_id = arguments.get("client_id") or os.getenv("DEFAULT_CLIENT_ID")
                client_secret = arguments.get("client_secret") or os.getenv("DEFAULT_CLIENT_SECRET")

                if not all([tenant_id, client_id, client_secret]):
                    return (
                        "Missing credentials. Provide tenant_id, client_id, and client_secret "
                        "either in the action arguments or via DEFAULT_* values in the .env file."
                    )

                await asyncio.get_event_loop().run_in_executor(
                    None,
                    self.connector.connect,
                    arguments["xmla_endpoint"],
                    tenant_id,
                    client_id,
                    client_secret,
                    arguments["initial_catalog"],
                )

                # Initialize the analyzer with OpenAI API key
                api_key = os.getenv("OPENAI_API_KEY")
                if not api_key:
                    logger.warning("OpenAI API key not provided - natural language features disabled")
                    self.analyzer = None
                else:
                    self.analyzer = DataAnalyzer(api_key)

                self.is_connected = True

                # Discover tables in background only if analyzer is available
                if self.analyzer:
                    asyncio.create_task(self._async_prepare_context())

                return f"Successfully connected to Power BI dataset '{arguments['initial_catalog']}'. Discovering tables..."

        except Exception as e:
            self.is_connected = False
            logger.error(f"Connection failed: {str(e)}")
            return f"Connection failed: {str(e)}"

    async def _async_prepare_context(self):
        """Prepare data context asynchronously"""
        try:
            # Discover tables
            tables = await asyncio.get_event_loop().run_in_executor(None, self.connector.discover_tables)

            schemas = {}
            sample_data = {}

            # Get schemas for first 5 tables only to speed up
            for table_info in tables[:5]:
                table_name = table_info["name"]
                try:
                    schema = await asyncio.get_event_loop().run_in_executor(
                        None, self.connector.get_table_schema, table_name
                    )
                    schemas[table_name] = schema

                    if schema["type"] == "data_table":
                        samples = await asyncio.get_event_loop().run_in_executor(
                            None, self.connector.get_sample_data, table_name, 3
                        )
                        sample_data[table_name] = samples
                except Exception as e:
                    logger.warning(f"Failed to get schema for table {table_name}: {e}")

            if self.analyzer:
                # Extract table names for the analyzer
                table_names = [table_info["name"] for table_info in tables]
                self.analyzer.set_data_context(table_names, schemas, sample_data)
                logger.info(f"Context prepared with {len(tables)} tables")

        except Exception as e:
            logger.error(f"Failed to prepare context: {e}")

    async def _handle_list_tables(self) -> str:
        """List all available tables with descriptions and relationships"""
        if not self.is_connected:
            return "Not connected to Power BI. Please connect first using 'connect_powerbi'."

        try:
            tables = await asyncio.get_event_loop().run_in_executor(None, self.connector.discover_tables)

            if not tables:
                return "No tables found in the dataset."

            result = "Available tables with relationships:\n\n"
            for table in tables:
                result += f"📊 **{table['name']}**\n"
                result += f"   Description: {table['description']}\n"

                if table.get("relationships"):
                    result += f"   Relationships ({len(table['relationships'])}):\n"
                    for rel in table["relationships"]:
                        result += f"     • {rel['relationshipType']} with {rel['relatedTable']}\n"
                        result += (
                            f"       {table['name']}.{rel['fromColumn']} → {rel['relatedTable']}.{rel['toColumn']}\n"
                        )
                        result += f"       Cardinality: {rel['cardinality']}, Active: {rel['isActive']}\n"
                        result += f"       Cross Filter: {rel['crossFilterDirection']}\n"
                else:
                    result += "   Relationships: None\n"
                result += "\n"

            return result
        except Exception as e:
            logger.error(f"Failed to list tables: {e}")
            return f"Error listing tables: {str(e)}"

    async def _handle_get_table_info(self, arguments: Dict[str, Any]) -> str:
        """Get information about a specific table"""
        if not self.is_connected:
            return "Not connected to Power BI. Please connect first."

        table_name = arguments.get("table_name")
        if not table_name:
            return "Please provide a table name."

        try:
            schema = await asyncio.get_event_loop().run_in_executor(None, self.connector.get_table_schema, table_name)

            if schema["type"] == "data_table":
                sample_data = await asyncio.get_event_loop().run_in_executor(
                    None, self.connector.get_sample_data, table_name, 5
                )
                # Extract column names from enhanced column objects
                column_names = [col["name"] for col in schema["columns"]]
                result = (
                    f"Table: {table_name}\n"
                    f"Type: Data Table\n"
                    f"Description: {schema.get('description', 'No description available')}\n"
                    f"Columns: {', '.join(column_names)}\n\nColumn Details:\n"
                )
                # Add detailed column information
                for col in schema["columns"]:
                    result += f"  - {col['name']}: {col.get('description', 'No description')} ({col.get('data_type', 'Unknown type')})\n"
                result += "\nSample data:\n"
                result += safe_json_dumps(sample_data, indent=2)
            elif schema["type"] == "measure_table":
                result = (
                    f"Table: {table_name}\n"
                    f"Type: Measure Table\n"
                    f"Description: {schema.get('description', 'No description available')}\n"
                    f"Measures:\n"
                )
                for measure in schema["measures"]:
                    result += f"\n- {measure['name']}:\n  DAX: {measure['dax']}\n"
            else:
                result = (
                    f"Table: {table_name}\n"
                    f"Type: {schema['type']}\n"
                    f"Description: {schema.get('description', 'No description available')}"
                )

            return result

        except Exception as e:
            logger.error(f"Error getting table info: {e}")
            return f"Error getting table info: {str(e)}"

    async def _handle_query_data(self, arguments: Dict[str, Any]) -> str:
        """Handle natural language queries about data"""
        if not self.is_connected:
            return "Not connected to Power BI. Please connect first."

        if not self.analyzer:
            return "Data analyzer not initialized."

        question = arguments.get("question")
        if not question:
            return "Please provide a question."

        try:
            # Generate DAX query
            dax_query = await asyncio.get_event_loop().run_in_executor(None, self.analyzer.generate_dax_query, question)

            # Execute the query
            results = await asyncio.get_event_loop().run_in_executor(None, self.connector.execute_dax_query, dax_query)

            # Interpret results
            interpretation = await asyncio.get_event_loop().run_in_executor(
                None, self.analyzer.interpret_results, question, results, dax_query
            )

            response = f"Question: {question}\n\nDAX Query:\n{dax_query}\n\nAnswer:\n{interpretation}"

            return response

        except Exception as e:
            logger.error(f"Error processing query: {e}")
            return f"Error processing query: {str(e)}"

    async def _handle_execute_dax(self, arguments: Dict[str, Any]) -> str:
        """Execute a custom DAX query"""
        if not self.is_connected:
            return "Not connected to Power BI. Please connect first."

        dax_query = arguments.get("dax_query")
        if not dax_query:
            return "Please provide a DAX query."

        try:
            results = await asyncio.get_event_loop().run_in_executor(None, self.connector.execute_dax_query, dax_query)
            return safe_json_dumps(results, indent=2)
        except Exception as e:
            logger.error(f"DAX execution error: {e}")
            return f"DAX execution error: {str(e)}"

    async def _handle_suggest_questions(self) -> str:
        """Suggest interesting questions about the data"""
        if not self.is_connected:
            return "Not connected to Power BI. Please connect first."

        if not self.analyzer:
            return "Data analyzer not initialized. Please wait for tables to be discovered."

        try:
            questions = await asyncio.get_event_loop().run_in_executor(None, self.analyzer.suggest_questions)
            result = "Here are some questions you might want to ask:\n\n"
            for i, q in enumerate(questions, 1):
                result += f"{i}. {q}\n"

            return result

        except Exception as e:
            logger.error(f"Error generating suggestions: {e}")
            return f"Error generating suggestions: {str(e)}"

    async def run(self):
        """Run the MCP server over SSE"""
        persist = os.getenv("MCP_PERSIST", "1") != "0"
        host = self.host
        port = self.port

        sse = SseServerTransport("/messages/")

        class SseApp:
            async def __call__(self, scope, receive, send):
                async with sse.connect_sse(scope, receive, send) as (
                    read_stream,
                    write_stream,
                ):
                    await self_server.run(
                        read_stream,
                        write_stream,
                        InitializationOptions(
                            server_name="powerbi-mcp-server",
                            server_version="1.0.0",
                            capabilities=self_server.get_capabilities(
                                notification_options=NotificationOptions(),
                                experimental_capabilities={},
                            ),
                        ),
                    )
                # Return empty response once stream closes
                await Response()(scope, receive, send)

        self_server = self.server
        sse_app = SseApp()

        routes = [
            Route("/sse", sse_app, methods=["GET"]),
            Mount("/messages/", app=sse.handle_post_message),
        ]

        app = Starlette(routes=routes)
        config = uvicorn.Config(app, host=host, port=port, log_level="info")
        server = uvicorn.Server(config)

        try:
            logger.info("Starting Power BI MCP Server on %s:%s...", host, port)
            await server.serve()
            logger.info("Server run completed")
        except Exception as e:
            logger.error(f"Server error: {e}", exc_info=True)
        finally:
            if persist:
                logger.info("Entering idle loop to keep server running")
                try:
                    while True:
                        await asyncio.sleep(3600)
                except asyncio.CancelledError:
                    pass
            logger.info("Server shutting down")


# Main entry point
async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default=os.getenv("HOST", "0.0.0.0"))
    parser.add_argument("--port", type=int, default=int(os.getenv("PORT", "8000")))
    args = parser.parse_args()

    server = PowerBIMCPServer(host=args.host, port=args.port)
    await server.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Server stopped by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
