"""
Power BI Desktop Connector
Connects to locally running Power BI Desktop instances via msmdsrv.exe
No authentication required - connects to localhost

Contract worth knowing before changing anything here:

* No connection is kept open. Every call opens its own ADOMD connection and closes it in a
  ``finally`` block, so the connector holds only state (port, role), never a live handle.
* Queries go through ``adomd_query.run_query``: read-only text only, a command timeout, a row
  limit that reports truncation, culture-invariant values.
* Metadata helpers read every row and raise on failure. An empty list always means "the model
  has none", never "the query failed".
* A failed ``connect`` or ``set_rls_role`` leaves the previous connection and role in force.
  In particular a failed role switch never drops to unrestricted access.
"""
import logging
import threading
from typing import Any, Dict, List, Optional

import adomd_loader
from adomd_loader import find_adomd_dll, NOT_FOUND_HELP
from adomd_query import QueryError, QueryRows, clean_error, field, run_query
from connection_strings import build as _build_pairs, local_data_source, role_list

logger = logging.getLogger(__name__)

# Try to import psutil for process discovery
try:
    import psutil
    _psutil_available = True
except ImportError:
    logger.warning("psutil not installed - run: pip install psutil")
    _psutil_available = False

# Backwards-compatible alias (older code/tests referenced this name)
_find_adomd_dll = find_adomd_dll

# Load ADOMD.NET (runtime-aware discovery with fall-through; see adomd_loader)
_adomd = adomd_loader.load_adomd()
_adomd_available = _adomd.available
_adomd_path = _adomd.directory
AdomdConnection = _adomd.value.connection if _adomd.available else None
AdomdCommand = _adomd.value.command if _adomd.available else None

# Sentinel: "use the connector's current role" as opposed to None ("no role").
_CURRENT = object()

_PROBE_QUERY = 'EVALUATE ROW("ok", 1)'
_LOOPBACK = ("127.0.0.1", "::1")


class PowerBIDesktopConnector:
    """Connector for Power BI Desktop instances running locally"""

    def __init__(self):
        """Initialize the Desktop connector"""
        self.current_port: Optional[int] = None
        self.current_model_name: Optional[str] = None
        self.connection_string: Optional[str] = None
        self.current_rls_role: Optional[str] = None  # Active RLS role for testing
        self.last_error: Optional[str] = None        # Why the last connect / role switch failed
        self.last_query_truncated: bool = False      # Convenience copy of QueryRows.truncated
        # Handlers run in executor threads; the lock keeps (port, role, connection string)
        # changing together.
        self._lock = threading.RLock()

    @staticmethod
    def is_available() -> bool:
        """Check if Desktop connectivity is available"""
        return _adomd_available and _psutil_available

    @staticmethod
    def availability_details() -> str:
        """Which ADOMD.NET DLL was loaded, or exactly why none could be."""
        text = _adomd.describe() if _adomd.searched else NOT_FOUND_HELP
        if not _psutil_available:
            text += "\npsutil is not installed (pip install psutil); instance discovery is unavailable."
        return text

    # ==================== DISCOVERY ====================

    def discover_instances(self) -> List[Dict[str, Any]]:
        """
        Discover all running Power BI Desktop instances

        Returns:
            List of instances with port, pid, and model info. Instances listening on the
            loopback interface come first: that is how Power BI Desktop binds its engine, while
            a full Analysis Services service listens on every interface.

        Raises:
            RuntimeError if psutil is missing or the process scan itself fails.
        """
        if not _psutil_available:
            raise RuntimeError("psutil is not installed - cannot discover instances (pip install psutil)")

        instances = []
        # Find all msmdsrv.exe processes (Analysis Services engine)
        for proc in psutil.process_iter(['pid', 'name']):
            try:
                if not proc.info['name'] or proc.info['name'].lower() != 'msmdsrv.exe':
                    continue
                pid = proc.info['pid']
                # psutil 6 renamed Process.connections() to net_connections().
                connections = getattr(proc, "net_connections", None) or proc.connections
                listening = [c for c in connections() if c.status == 'LISTEN']
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                # The process ended, or belongs to another account: not ours to connect to.
                continue

            loopback = [c for c in listening if c.laddr.ip in _LOOPBACK]
            wildcard = [c for c in listening if c.laddr.ip in ("0.0.0.0", "::")]
            chosen = (loopback or wildcard or [None])[0]
            if chosen is None:
                continue
            port = chosen.laddr.port
            model_name = self._get_model_name(port)
            instances.append({
                'pid': pid,
                'port': port,
                'model_name': model_name or f"Model on port {port}",
                'connection_string': f"Data Source=localhost:{port}",
                'loopback_only': bool(loopback),
            })
            logger.info(f"Found Analysis Services instance: port={port}, pid={pid}, model={model_name}")

        instances.sort(key=lambda inst: 0 if inst['loopback_only'] else 1)
        return instances

    def _get_model_name(self, port: int) -> Optional[str]:
        """Get the model/database name from an instance (a label only; None if it cannot be read)."""
        if not _adomd_available:
            return None
        try:
            rows = run_query(AdomdConnection, AdomdCommand, f"Data Source={local_data_source(port)}",
                             "SELECT [CATALOG_NAME] FROM $SYSTEM.DBSCHEMA_CATALOGS",
                             max_rows=1, allow_dmv=True, label="Catalog lookup")
        except Exception as e:
            # Used only to label an instance in a listing; the port is what identifies it.
            logger.debug(f"Could not get model name for port {port}: {clean_error(e)}")
            return None
        if not rows:
            return None
        value = field(rows[0], "CATALOG_NAME")
        return None if value is None else str(value)

    # ==================== CONNECTION ====================

    @staticmethod
    def _connection_string_for(port: Any, rls_role: Any = None) -> str:
        """Connection string for a local instance; the role list is quoted, never spliced in."""
        text = f"Data Source={local_data_source(port)}"
        roles = role_list(rls_role)
        if roles:
            # The Roles property applies those roles' RLS filters to every query.
            text += ";" + _build_pairs([("Roles", roles)])
        return text

    def _probe(self, connection_string: str) -> None:
        """Open the connection and run a trivial query, so an unknown role or a dead instance
        fails here rather than on the user's first real query."""
        run_query(AdomdConnection, AdomdCommand, connection_string, _PROBE_QUERY,
                  max_rows=1, label="Connection test")

    def connect(self, port: Optional[int] = None, rls_role: Optional[str] = None) -> bool:
        """
        Connect to a Power BI Desktop instance

        Args:
            port: Specific port to connect to. If None, connects to first available instance.
            rls_role: Optional RLS role name to test (or a list of role names). When specified,
                      queries will be filtered according to that role's DAX filters.

        Returns:
            True if connection successful. On failure the previous connection (if any) stays
            in force and ``last_error`` says why.
        """
        with self._lock:
            self.last_error = None
            if not _adomd_available:
                self.last_error = self.availability_details()
                logger.error("ADOMD.NET not available - cannot connect")
                return False
            try:
                if port is None:
                    instances = self.discover_instances()
                    if not instances:
                        raise RuntimeError("No Power BI Desktop instances found. Please open a .pbix file.")
                    port = instances[0]['port']
                    logger.info(f"Auto-selected instance on port {port}")
                connection_string = self._connection_string_for(port, rls_role)
                self._probe(connection_string)
            except Exception as e:
                self.last_error = clean_error(e)
                logger.error(f"Failed to connect to Power BI Desktop: {self.last_error}")
                return False

            self.current_port = int(port)
            self.connection_string = connection_string
            self.current_rls_role = role_list(rls_role)
            self.current_model_name = self._get_model_name(self.current_port)
            rls_info = f" with RLS role '{self.current_rls_role}'" if self.current_rls_role else ""
            logger.info(f"Connected to Power BI Desktop on port {self.current_port}{rls_info}")
            return True

    def _connection_for(self, rls_role: Any = _CURRENT, admin: bool = False) -> str:
        """The connection string a call should use.

        ``admin`` ignores any active role: schema rowsets such as TMSCHEMA_ROLES and the storage
        DMVs are refused to a connection that is impersonating a role.
        """
        with self._lock:
            if not self.connection_string or not self.current_port:
                raise QueryError("Not connected - call connect() first")
            if admin:
                return self._connection_string_for(self.current_port)
            if rls_role is _CURRENT:
                return self.connection_string
            return self._connection_string_for(self.current_port, rls_role)

    # ==================== QUERIES ====================

    def execute_dax(self, dax_query: str, max_rows: Optional[int] = 1000, allow_dmv: bool = False,
                    timeout: Optional[float] = None, rls_role: Any = _CURRENT) -> QueryRows:
        """
        Execute a DAX query against the connected model

        Args:
            dax_query: DAX query string. Must be a query (EVALUATE / DEFINE ... EVALUATE);
                       TMSL, XMLA and MDX are refused.
            max_rows: Maximum rows to return; None reads everything.
            allow_dmv: Also accept ``SELECT ... FROM $SYSTEM.<rowset>`` (internal metadata callers).
            timeout: Command timeout in seconds (default POWERBI_MCP_QUERY_TIMEOUT or 120).
            rls_role: Run this one query under the given role (None = unrestricted) without
                      changing the connector's active role.

        Returns:
            ``QueryRows``: a list of row dicts with ``.truncated`` set when max_rows cut it short.
        """
        rows = run_query(AdomdConnection, AdomdCommand, self._connection_for(rls_role), dax_query,
                         max_rows=max_rows, timeout=timeout, allow_dmv=allow_dmv)
        self.last_query_truncated = rows.truncated
        logger.info(f"DAX query returned {len(rows)} rows" + (" (truncated)" if rows.truncated else ""))
        return rows

    def _metadata(self, query: str, admin: bool = False, allow_dmv: bool = False,
                  label: str = "Metadata query") -> QueryRows:
        """Run an internal metadata query: every row, and an exception on failure."""
        return run_query(AdomdConnection, AdomdCommand, self._connection_for(admin=admin), query,
                         max_rows=None, allow_dmv=allow_dmv, label=label)

    def list_tables(self) -> List[Dict[str, Any]]:
        """
        List all visible tables in the model

        Returns:
            List of tables with name and properties

        Raises:
            QueryError if the model cannot be read.
        """
        # Use INFO.VIEW.TABLES() which works in Power BI Desktop
        query = """
            EVALUATE
            SELECTCOLUMNS(
                INFO.VIEW.TABLES(),
                "Name", [Name],
                "IsHidden", [IsHidden]
            )
        """
        tables = []
        for row in self._metadata(query, label="Listing tables"):
            name = field(row, "Name", "")
            is_hidden = field(row, "IsHidden", False)
            # Filter system and hidden tables
            if name and not is_hidden and not name.startswith(('$', 'DateTableTemplate_', 'LocalDateTable_')):
                tables.append({
                    'name': name,
                    'type': 'TABLE'
                })
        return tables

    def list_columns(self, table_name: str) -> List[Dict[str, Any]]:
        """
        List columns for a specific table

        Args:
            table_name: Name of the table

        Returns:
            List of columns with name and type

        Raises:
            QueryError if the model cannot be read.
        """
        # Use INFO.VIEW.COLUMNS() and filter by table. Escape double quotes so a table name
        # cannot break out of the DAX string literal (DAX escapes " by doubling it).
        safe_table = str(table_name).replace('"', '""')
        query = f"""
            EVALUATE
            SELECTCOLUMNS(
                FILTER(
                    INFO.VIEW.COLUMNS(),
                    [Table] = "{safe_table}"
                ),
                "Name", [Name],
                "DataType", [DataType],
                "IsHidden", [IsHidden],
                "Description", [Description]
            )
        """
        columns = []
        for row in self._metadata(query, label="Listing columns"):
            if not field(row, "IsHidden", False):
                columns.append({
                    'name': field(row, "Name", ""),
                    'type': field(row, "DataType", ""),
                    'description': field(row, "Description", "") or ''
                })
        return columns

    def list_measures(self) -> List[Dict[str, Any]]:
        """
        List all measures in the model

        Returns:
            List of measures with name, table, and expression

        Raises:
            QueryError if the model cannot be read.
        """
        # Use INFO.VIEW.MEASURES() - correct column names: [Table], not [TableName]
        query = """
            EVALUATE
            SELECTCOLUMNS(
                INFO.VIEW.MEASURES(),
                "Name", [Name],
                "Table", [Table],
                "Expression", [Expression],
                "IsHidden", [IsHidden]
            )
        """
        measures = []
        for row in self._metadata(query, label="Listing measures"):
            if not field(row, "IsHidden", False):
                measures.append({
                    'name': field(row, "Name", ""),
                    'table': field(row, "Table", ""),
                    'expression': field(row, "Expression", "")
                })
        return measures

    def list_relationships(self) -> List[Dict[str, Any]]:
        """
        List all relationships in the model

        Returns:
            List of relationships

        Raises:
            QueryError if the model cannot be read.
        """
        # Use INFO.VIEW.RELATIONSHIPS() - correct column names
        query = """
            EVALUATE
            SELECTCOLUMNS(
                INFO.VIEW.RELATIONSHIPS(),
                "FromTable", [FromTable],
                "FromColumn", [FromColumn],
                "ToTable", [ToTable],
                "ToColumn", [ToColumn],
                "IsActive", [IsActive],
                "FromCardinality", [FromCardinality],
                "ToCardinality", [ToCardinality]
            )
        """
        relationships = []
        for row in self._metadata(query, label="Listing relationships"):
            from_card = field(row, "FromCardinality", "")
            to_card = field(row, "ToCardinality", "")
            cardinality = f"{from_card}:{to_card}" if from_card and to_card else ""

            relationships.append({
                'from_table': field(row, "FromTable", ""),
                'from_column': field(row, "FromColumn", ""),
                'to_table': field(row, "ToTable", ""),
                'to_column': field(row, "ToColumn", ""),
                'is_active': field(row, "IsActive", True),
                'cardinality': cardinality
            })
        return relationships

    # ==================== STORAGE (VertiPaq) ====================

    _STORAGE_QUERIES = {
        # rowset -> the columns this code needs from it
        "DISCOVER_STORAGE_TABLES": ("DIMENSION_NAME", "TABLE_ID", "ROWS_COUNT"),
        "DISCOVER_STORAGE_TABLE_COLUMNS": ("DIMENSION_NAME", "COLUMN_TYPE", "DICTIONARY_SIZE"),
        "DISCOVER_STORAGE_TABLE_COLUMN_SEGMENTS": ("DIMENSION_NAME", "USED_SIZE"),
    }

    def get_vertipaq_stats(self) -> Dict[str, Any]:
        """
        Get VertiPaq storage statistics per table.

        Size is what VertiPaq Analyzer reports as the table size: the used size of every column
        segment that belongs to the table (data, attribute hierarchies, relationship and user
        hierarchy structures) plus the dictionaries of its columns. Rows come from the storage
        table that holds the table's own data.

        Returns:
            {'available': True, 'tables': [{'name', 'rows', 'size', 'data_size',
            'dictionary_size'}], 'total_size', 'error': None}, tables largest first; or
            {'available': False, 'tables': [], 'total_size': 0, 'error': <reason>} when the
            storage DMVs cannot be read. Sizes are never guessed.
        """
        stats: Dict[str, Any] = {'available': False, 'tables': [], 'total_size': 0, 'error': None}
        try:
            rowsets = {}
            for rowset, needed in self._STORAGE_QUERIES.items():
                # SELECT * so that a build with extra or reordered columns still answers; the
                # columns that matter are checked by name below.
                rows = self._metadata(f"SELECT * FROM $SYSTEM.{rowset}", admin=True, allow_dmv=True,
                                      label=f"Storage DMV {rowset}")
                if rows:
                    present = {str(c).strip("[]").casefold() for c in (rows.columns or rows[0].keys())}
                    missing = [c for c in needed if c.casefold() not in present]
                    if missing:
                        raise QueryError(f"{rowset} does not expose {', '.join(missing)} on this engine")
                rowsets[rowset] = rows
        except Exception as e:
            stats['error'] = clean_error(e)
            logger.warning(f"VertiPaq statistics unavailable: {stats['error']}")
            return stats

        tables: Dict[str, Dict[str, Any]] = {}

        def entry(name: Any) -> Dict[str, Any]:
            key = str(name)
            return tables.setdefault(key, {'name': key, 'rows': 0, 'size': 0,
                                           'data_size': 0, 'dictionary_size': 0})

        def number(value: Any) -> int:
            try:
                return int(value or 0)
            except (TypeError, ValueError):
                return int(float(value))

        for row in rowsets["DISCOVER_STORAGE_TABLES"]:
            name, table_id = field(row, "DIMENSION_NAME"), str(field(row, "TABLE_ID", "") or "")
            if not name:
                continue
            # H$ (attribute hierarchy), R$ (relationship) and U$ (user hierarchy) are internal
            # structures of a table; only the remaining entry carries its row count.
            if not table_id.startswith(("H$", "R$", "U$")):
                entry(name)['rows'] = number(field(row, "ROWS_COUNT"))
        for row in rowsets["DISCOVER_STORAGE_TABLE_COLUMN_SEGMENTS"]:
            name = field(row, "DIMENSION_NAME")
            if name:
                entry(name)['data_size'] += number(field(row, "USED_SIZE"))
        for row in rowsets["DISCOVER_STORAGE_TABLE_COLUMNS"]:
            name = field(row, "DIMENSION_NAME")
            if name and str(field(row, "COLUMN_TYPE", "") or "").upper() == "BASIC_DATA":
                entry(name)['dictionary_size'] += number(field(row, "DICTIONARY_SIZE"))

        for t in tables.values():
            t['size'] = t['data_size'] + t['dictionary_size']
        stats['tables'] = sorted(tables.values(), key=lambda t: t['size'], reverse=True)
        stats['total_size'] = sum(t['size'] for t in stats['tables'])
        stats['available'] = True
        return stats

    # ==================== RLS ====================

    def list_rls_roles(self) -> List[Dict[str, Any]]:
        """
        List all RLS (Row-Level Security) roles defined in the model

        Returns:
            List of roles with name and description

        Raises:
            QueryError if the roles cannot be read. The listing always runs without a role:
            the schema rowset is not served to a connection impersonating one.
        """
        rows = self._metadata("SELECT [Name], [Description] FROM $SYSTEM.TMSCHEMA_ROLES",
                              admin=True, allow_dmv=True, label="Listing RLS roles")
        roles = []
        for row in rows:
            name = field(row, "Name", "")
            if name:
                roles.append({'name': name, 'description': field(row, "Description", "") or ''})
        logger.info(f"Found {len(roles)} RLS role(s)")
        return roles

    def set_rls_role(self, role_name: Optional[str]) -> bool:
        """
        Set or clear the active RLS role for subsequent queries

        Args:
            role_name: Name of the RLS role to apply (or a list of names), or None to clear

        Returns:
            True if successful. On failure the PREVIOUS role stays active: a mistyped role name
            must not turn a restricted session into an unrestricted one. ``last_error`` says why.
        """
        with self._lock:
            self.last_error = None
            if not self.current_port:
                self.last_error = "Not connected - cannot set RLS role"
                logger.error(self.last_error)
                return False
            try:
                connection_string = self._connection_string_for(self.current_port, role_name)
                self._probe(connection_string)
            except Exception as e:
                self.last_error = clean_error(e)
                kept = f"'{self.current_rls_role}'" if self.current_rls_role else "none"
                logger.error(f"Failed to set RLS role '{role_name}': {self.last_error} "
                             f"(active role unchanged: {kept})")
                return False

            self.connection_string = connection_string
            self.current_rls_role = role_list(role_name)
            if self.current_rls_role:
                logger.info(f"RLS role set to '{self.current_rls_role}'")
            else:
                logger.info("RLS role cleared - full data access")
            return True

    def get_rls_status(self) -> Dict[str, Any]:
        """
        Get current RLS status

        Returns:
            Dictionary with RLS status information. ``roles_error`` is set (and
            ``available_roles`` empty) when the role list could not be read.
        """
        status: Dict[str, Any] = {
            'rls_active': self.current_rls_role is not None,
            'current_role': self.current_rls_role,
            'available_roles': [],
            'roles_error': None,
        }
        if self.current_port:
            try:
                status['available_roles'] = self.list_rls_roles()
            except Exception as e:
                status['roles_error'] = clean_error(e)
        return status

    # ==================== MODEL INFO ====================

    def get_model_info(self) -> Dict[str, Any]:
        """
        Get comprehensive model information

        Returns:
            Dictionary with model metadata including tables, measures, relationships. An
            ``error`` key is present when any part could not be read.
        """
        if not self.connection_string:
            return {'error': 'Not connected'}

        info = {
            'model_name': self.current_model_name,
            'port': self.current_port,
            'rls_active': self.current_rls_role is not None,
            'current_rls_role': self.current_rls_role,
            'tables': [],
            'measures': [],
            'relationships': [],
            'table_count': 0,
            'measure_count': 0,
            'relationship_count': 0
        }

        try:
            tables = self.list_tables()
            info['tables'] = [t.get('name') for t in tables]
            info['table_count'] = len(tables)

            measures = self.list_measures()
            info['measures'] = [{'name': m.get('name'), 'table': m.get('table')} for m in measures]
            info['measure_count'] = len(measures)

            relationships = self.list_relationships()
            info['relationships'] = relationships
            info['relationship_count'] = len(relationships)
        except Exception as e:
            info['error'] = clean_error(e)
            logger.error(f"Failed to get model info: {info['error']}")

        return info

    def close(self):
        """Forget the connection (there is no open handle to release)."""
        with self._lock:
            self.current_port = None
            self.current_model_name = None
            self.current_rls_role = None
            self.connection_string = None
        logger.info("Desktop connection closed")
