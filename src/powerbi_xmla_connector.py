"""
Power BI XMLA Connector
Provides dynamic table discovery and DAX queries through the Power BI XMLA endpoint.
Requires: Windows + ADOMD.NET client libraries

The connection string carries the service principal's secret, so two rules hold throughout:

* every caller-supplied part (workspace, dataset, effective user) is quoted by
  ``connection_strings.build``; none of them can add or override a connection property;
* every provider error is cleaned and redacted before it is logged or raised, on every path
  (connect, discovery and queries alike).

Like the Desktop connector, no connection is kept open between calls.
"""
import logging
from typing import Any, Dict, List, Optional

import adomd_loader
import dax_tokens
from adomd_loader import NOT_FOUND_HELP
from adomd_query import QueryError, QueryRows, clean_error, run_query
from connection_strings import ConnectionStringError, build as _build_pairs

logger = logging.getLogger(__name__)

# Load ADOMD.NET (shared, runtime-aware discovery with fall-through; see adomd_loader)
_adomd = adomd_loader.load_adomd()
_adomd_available = _adomd.available
AdomdConnection = _adomd.value.connection if _adomd.available else None
AdomdCommand = _adomd.value.command if _adomd.available else None
AdomdSchemaGuid = _adomd.value.schema_guid if _adomd.available else None

XMLA_ENDPOINT = "powerbi://api.powerbi.com/v1.0/myorg/"

# Cloud models can be far larger than anything a caller can use; this bounds memory when a
# caller does not pass its own limit. The result says when the limit cut it short.
DEFAULT_MAX_ROWS = 100_000


class PowerBIXmlaConnector:
    """Power BI connector using XMLA endpoint (ADOMD.NET)"""

    def __init__(self, tenant_id: str, client_id: str, client_secret: str):
        """Initialize connector with Azure AD credentials"""
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.connection_string = None
        self.connection = None  # kept for compatibility; no connection is held open
        self.workspace_name = None
        self.dataset_name = None
        self.effective_user: Optional[str] = None  # For RLS impersonation
        self.last_error: Optional[str] = None
        self.last_query_truncated: bool = False

    # ==================== CONNECTION ====================

    def _secrets(self) -> List[str]:
        return [self.client_secret] if self.client_secret else []

    def _safe(self, error: Any) -> str:
        """Provider error text that is safe to log or raise."""
        return clean_error(error, self._secrets())

    def _build_connection_string(self, workspace_name: str, dataset_name: str,
                                 effective_user: Optional[str] = None) -> str:
        """Service-principal connection string with every value quoted.

        Raises connection_strings.ConnectionStringError for an empty name or one that contains
        control characters.
        """
        if workspace_name is None or not str(workspace_name).strip():
            raise ConnectionStringError("workspace_name must not be empty")
        pairs = [
            ("Data Source", XMLA_ENDPOINT + str(workspace_name).strip()),
            ("Initial Catalog", dataset_name),
            ("User ID", f"app:{self.client_id}@{self.tenant_id}"),
            ("Password", self.client_secret),
        ]
        if effective_user is not None and str(effective_user).strip():
            # EffectiveUserName makes the engine apply that user's RLS roles.
            pairs.append(("EffectiveUserName", str(effective_user).strip()))
        return "Provider=MSOLAP;" + _build_pairs(pairs) + ";"

    def _open(self, connection_string: Optional[str] = None) -> Any:
        """Open a connection; the caller closes it in a ``finally`` block. A connection that
        fails to open is closed here, since the caller never receives it."""
        connection = AdomdConnection(connection_string or self.connection_string)
        try:
            connection.Open()
        except Exception:
            self._close(connection)
            raise
        return connection

    @staticmethod
    def _close(connection: Any) -> None:
        if connection is None:
            return
        try:
            connection.Close()
        except Exception as e:
            # Cleanup only: must not replace the result or the error of the call itself.
            logger.debug(f"closing connection failed: {clean_error(e)}")

    def connect(self, workspace_name: str, dataset_name: str, effective_user: Optional[str] = None) -> bool:
        """
        Connect to Power BI dataset via XMLA endpoint

        Args:
            workspace_name: Name of the Power BI workspace
            dataset_name: Name of the dataset (semantic model)
            effective_user: Optional user email to impersonate for RLS.
                           When specified, queries are filtered by that user's RLS roles.

        Returns:
            True if connection successful. On failure the previous connection settings (if
            any) stay in force and ``last_error`` holds a redacted reason.
        """
        self.last_error = None
        if not _adomd_available or AdomdConnection is None:
            self.last_error = _adomd.describe() if _adomd.searched else NOT_FOUND_HELP
            logger.error("ADOMD.NET libraries not available - cannot connect via XMLA")
            return False

        try:
            connection_string = self._build_connection_string(workspace_name, dataset_name, effective_user)
        except ConnectionStringError as e:
            self.last_error = str(e)
            logger.error(f"XMLA connection refused: {self.last_error}")
            return False

        connection = None
        try:
            logger.info(f"Connecting via XMLA: workspace '{workspace_name}', dataset '{dataset_name}'")
            connection = self._open(connection_string)
            state = connection.State
            # ConnectionState.Open can be 1 (int) or "Open" (string) depending on the library version
            if not (state == 1 or str(state) in ("Open", "1")):
                raise QueryError(f"Connection state is not Open (State={state})")
        except Exception as e:
            self.last_error = self._safe(e)
            logger.error(f"XMLA connection failed: {self.last_error}")
            lowered = self.last_error.lower()
            if "login" in lowered or "auth" in lowered:
                logger.error("Authentication failed - check Service Principal credentials")
            elif "catalog" in lowered or "database" in lowered:
                logger.error("Dataset (catalog) not found - check dataset name")
            elif "workspace" in lowered or "server" in lowered:
                logger.error("Workspace not found - check workspace name")
            return False
        finally:
            self._close(connection)

        user = str(effective_user).strip() if effective_user is not None else ""
        self.workspace_name = workspace_name
        self.dataset_name = dataset_name
        self.effective_user = user or None
        self.connection_string = connection_string
        if self.effective_user:
            logger.info(f"RLS enabled: Impersonating user '{self.effective_user}'")
        logger.info("Successfully connected to Power BI via XMLA")
        return True

    def _require_connection(self) -> None:
        if not self.connection_string:
            raise QueryError("Not connected - call connect() first")

    # ==================== SCHEMA DISCOVERY ====================

    @staticmethod
    def _cell(row: Any, column_names: List[str], name: str, default: Any = None) -> Any:
        """A schema-rowset cell, or ``default`` when this rowset has no such column or the cell
        is NULL. Rowset columns differ between engine versions, so absence is normal."""
        if name not in column_names:
            return default
        value = row[name]
        if value is None or type(value).__name__ == "DBNull":
            return default
        return value

    def discover_tables(self) -> List[Dict[str, Any]]:
        """
        Discover all tables in the dataset using XMLA schema discovery

        Returns:
            List of tables with their metadata

        Raises:
            QueryError if not connected or the schema cannot be read.
        """
        self._require_connection()
        logger.info("Discovering tables via XMLA...")
        tables = []
        connection = None
        try:
            connection = self._open()
            schema_table = connection.GetSchemaDataSet(AdomdSchemaGuid.Tables, None).Tables[0]
            column_names = [str(col.ColumnName) for col in schema_table.Columns]
            # Filter out system and hidden tables
            system_prefixes = ("$", "DateTableTemplate_", "LocalDateTable_",
                               "DBSCHEMA_", "MDSCHEMA_", "TMSCHEMA_",
                               "DMSCHEMA_", "DISCOVER_")
            for row in schema_table.Rows:
                table_name = str(row["TABLE_NAME"])
                is_hidden = bool(self._cell(row, column_names, "TABLE_HIDDEN", False))
                if table_name.startswith(system_prefixes) or is_hidden:
                    continue
                description = self._cell(row, column_names, "DESCRIPTION", "")
                tables.append({
                    "name": table_name,
                    "description": str(description) if description else "No description available",
                    "type": str(self._cell(row, column_names, "TABLE_TYPE", "TABLE")),
                })
        except Exception as e:
            message = self._safe(e)
            logger.error(f"Table discovery failed: {message}")
            raise QueryError(f"Table discovery failed: {message}") from None
        finally:
            self._close(connection)

        logger.info(f"Discovered {len(tables)} visible tables")
        return tables

    def get_table_schema(self, table_name: str) -> Dict[str, Any]:
        """
        Get columns for a specific table using XMLA schema discovery

        Args:
            table_name: Name of the table

        Returns:
            Dictionary with table metadata and columns

        Raises:
            QueryError if not connected or the schema cannot be read.
        """
        self._require_connection()
        logger.info(f"Getting schema for table: {table_name}")
        columns = []
        connection = None
        try:
            connection = self._open()
            # Restrictions are matched as values by the provider (catalog, schema, table, column).
            restrictions = [None, None, table_name, None]
            schema_table = connection.GetSchemaDataSet(AdomdSchemaGuid.Columns, restrictions).Tables[0]
            column_names = [str(col.ColumnName) for col in schema_table.Columns]
            for row in schema_table.Rows:
                if bool(self._cell(row, column_names, "COLUMN_HIDDEN", False)):
                    continue  # only visible columns
                description = self._cell(row, column_names, "DESCRIPTION", "")
                columns.append({
                    "name": str(row["COLUMN_NAME"]),
                    "type": self._map_data_type(str(self._cell(row, column_names, "DATA_TYPE", "Unknown"))),
                    "description": str(description) if description else "",
                })
        except Exception as e:
            message = self._safe(e)
            logger.error(f"Failed to get schema for table '{table_name}': {message}")
            raise QueryError(f"Failed to get schema for table '{table_name}': {message}") from None
        finally:
            self._close(connection)

        return {"table_name": table_name, "columns": columns}

    def _map_data_type(self, adomd_type: str) -> str:
        """Map ADOMD data types to readable names"""
        type_mapping = {
            "2": "Integer",
            "3": "Double",
            "5": "Float",
            "6": "Currency",
            "7": "DateTime",
            "8": "String",
            "11": "Boolean",
            "17": "Decimal",
            "130": "String",
            "131": "Decimal"
        }
        return type_mapping.get(str(adomd_type), f"Type_{adomd_type}")

    # ==================== QUERIES ====================

    def execute_dax(self, dax_query: str, max_rows: Optional[int] = DEFAULT_MAX_ROWS,
                    allow_dmv: bool = False, timeout: Optional[float] = None) -> QueryRows:
        """
        Execute a DAX query via XMLA

        Args:
            dax_query: DAX query string. Must be a query (EVALUATE / DEFINE ... EVALUATE);
                       TMSL, XMLA and MDX are refused.
            max_rows: Stop reading after this many rows (None reads everything). Rows beyond
                      the limit are never materialised.
            allow_dmv: Also accept ``SELECT ... FROM $SYSTEM.<rowset>``.
            timeout: Command timeout in seconds (default POWERBI_MCP_QUERY_TIMEOUT or 120).

        Returns:
            ``QueryRows``: a list of row dicts with ``.truncated`` set when max_rows cut it short.

        Raises:
            dax_tokens.NonQueryStatementError for non-query text; QueryError otherwise.
        """
        dax_tokens.ensure_query(dax_query, allow_dmv=allow_dmv)
        self._require_connection()
        # The query text can carry literals worth protecting, so only its size is logged.
        logger.info(f"Executing DAX query via XMLA ({len(dax_query)} chars)")
        rows = run_query(AdomdConnection, AdomdCommand, self.connection_string, dax_query,
                         max_rows=max_rows, timeout=timeout, allow_dmv=allow_dmv,
                         secrets=self._secrets())
        self.last_query_truncated = rows.truncated
        logger.info(f"Query returned {len(rows)} rows" + (" (truncated)" if rows.truncated else ""))
        return rows

    def get_sample_data(self, table_name: str, num_rows: int = 5) -> QueryRows:
        """
        Get sample data from a table

        Args:
            table_name: Name of the table
            num_rows: Number of rows to retrieve

        Returns:
            List of row dictionaries

        Raises:
            QueryError if the query fails.
        """
        # Always quoted, with inner quotes doubled, so a name like O'Brien stays one table name.
        quoted_name = dax_tokens.quote_table(str(table_name), force=True)
        return self.execute_dax(f"EVALUATE TOPN({int(num_rows)}, {quoted_name})", max_rows=int(num_rows))

    # ==================== RLS ====================

    def set_effective_user(self, user_email: Optional[str]) -> bool:
        """
        Set or clear the effective user for RLS impersonation

        Args:
            user_email: User email to impersonate, or None to clear

        Returns:
            True if successful. On failure the previous effective user stays in force.
        """
        if not self.workspace_name or not self.dataset_name:
            self.last_error = "Not connected - cannot set effective user"
            logger.error(self.last_error)
            return False

        return self.connect(
            self.workspace_name,
            self.dataset_name,
            effective_user=user_email
        )

    def get_rls_status(self) -> Dict[str, Any]:
        """
        Get current RLS status

        Returns:
            Dictionary with RLS status information
        """
        return {
            'rls_active': self.effective_user is not None,
            'effective_user': self.effective_user,
            'workspace': self.workspace_name,
            'dataset': self.dataset_name
        }

    def close(self):
        """Forget the connection settings (there is no open handle to release)."""
        self.connection = None
        self.connection_string = None
        self.effective_user = None
        logger.info("Connection closed")
