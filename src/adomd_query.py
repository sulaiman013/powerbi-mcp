"""
One place where a query is sent to Analysis Services through ADOMD.NET, shared by the Desktop
and the XMLA connector, so both get the same guarantees:

* only read-only text is forwarded (``dax_tokens.ensure_query``): the query tools are annotated
  read-only and stay enabled in lockdown mode, so TMSL / XMLA / MDX must never reach the engine
  through them;
* the reader and the connection are closed whatever happens;
* the command has a timeout;
* reading stops at ``max_rows`` and the caller can tell that it did;
* values are converted without consulting the machine's regional settings.

Nothing here imports .NET: the ADOMD classes are passed in, which is also what lets the tests
drive this module with plain Python fakes.
"""
import datetime as _dt
import decimal as _decimal
import logging
import os
from typing import Any, Callable, Dict, Iterable, List, Optional

import dax_tokens
from connection_strings import redact

logger = logging.getLogger(__name__)

DEFAULT_QUERY_TIMEOUT = 120
QUERY_TIMEOUT_ENV = "POWERBI_MCP_QUERY_TIMEOUT"


class QueryError(Exception):
    """A query (or the connection it needed) failed. The message is safe to show: no .NET
    stack trace, no connection-string secret."""


class QueryRows(list):
    """Query result: a plain list of row dicts that also says whether it is complete.

    A list subclass rather than a wrapper so existing callers (len, indexing, json.dumps) keep
    working, and rather than a flag on the connector so two concurrent queries cannot read each
    other's answer.
    """
    truncated: bool = False
    columns: List[str]
    row_limit: Optional[int] = None

    def __init__(self, *args: Any) -> None:
        super().__init__(*args)
        self.columns = []


def query_timeout(timeout: Optional[float] = None) -> int:
    """Command timeout in seconds: the argument, else POWERBI_MCP_QUERY_TIMEOUT, else 120.

    0 means "no limit" to ADOMD.NET and is accepted only when asked for explicitly.
    """
    source = "timeout"
    value: Any = timeout
    if value is None:
        raw = os.environ.get(QUERY_TIMEOUT_ENV, "").strip()
        if not raw:
            return DEFAULT_QUERY_TIMEOUT
        source, value = QUERY_TIMEOUT_ENV, raw
    try:
        seconds = int(float(value))
    except (TypeError, ValueError):
        raise ValueError(f"{source} must be a number of seconds, got {value!r}") from None
    if seconds < 0:
        raise ValueError(f"{source} must not be negative, got {value!r}")
    return seconds


def clean_error(error: Any, secrets: Iterable[Optional[str]] = ()) -> str:
    """The message of a .NET/ADOMD/TOM error without its stack trace, with secrets masked.

    pythonnet renders a .NET exception as the message followed by "\\r\\n   at Namespace.Type..."
    frames; only the message is useful to a caller and the frames leak local paths.
    """
    text = "" if error is None else str(error)
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    cut = text.find("\n   at ")
    if cut != -1:
        text = text[:cut]
    if text.startswith("   at "):
        text = ""
    lines = [ln.rstrip() for ln in text.split("\n")]
    # Inner-exception markers add nothing once the frames are gone.
    lines = [ln for ln in lines if ln.strip() and not ln.strip().startswith("--- End of")]
    text = "\n".join(lines).strip()
    if not text and error is not None and not isinstance(error, str):
        text = type(error).__name__
    return redact(text, secrets)


# ---------------------------------------------------------------- value conversion

def _invariant_culture() -> Any:
    # Only reached for a System.Decimal value, so .NET is already loaded.
    from System.Globalization import CultureInfo
    return CultureInfo.InvariantCulture


def _type_name(value: Any) -> str:
    cls = type(value)
    return f"{getattr(cls, '__module__', '')}.{getattr(cls, '__name__', '')}"


def _convert_decimal(value: Any) -> Any:
    # ToString() alone follows the thread culture ("1234,50" on a German machine).
    number = _decimal.Decimal(str(value.ToString(_invariant_culture())))
    exponent = number.as_tuple().exponent
    if isinstance(exponent, int) and exponent >= 0:
        return int(number)
    return float(number)


def _convert_datetime(value: Any) -> str:
    stamp = _dt.datetime(int(value.Year), int(value.Month), int(value.Day), int(value.Hour),
                         int(value.Minute), int(value.Second), int(value.Millisecond) * 1000)
    return stamp.isoformat()


_CONVERTERS: Dict[str, Callable[[Any], Any]] = {
    "System.DBNull": lambda value: None,
    "System.Decimal": _convert_decimal,
    "System.DateTime": _convert_datetime,
}


def to_python(value: Any) -> Any:
    """Convert one cell to a JSON-friendly Python value, independent of regional settings.

    pythonnet already hands back int/float/bool/str for the matching .NET primitives. What is
    left arrives as .NET objects: Decimal (fixed-decimal and currency columns) becomes a number,
    DateTime an ISO 8601 string, DBNull None. Anything else is rendered with str().
    """
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    converter = _CONVERTERS.get(_type_name(value))
    if converter is not None:
        return converter(value)
    return str(value)


# ---------------------------------------------------------------- execution

def _close_quietly(obj: Any, what: str) -> None:
    """Close a reader/connection during cleanup. A failure here must not replace the error (or
    the result) of the query itself, so it is logged and dropped."""
    if obj is None:
        return
    try:
        obj.Close()
    except Exception as exc:
        logger.debug(f"closing {what} failed: {clean_error(exc)}")


def run_query(connection_cls: Any, command_cls: Any, connection_string: str, query: str, *,
              max_rows: Optional[int] = None, timeout: Optional[float] = None,
              allow_dmv: bool = False, secrets: Iterable[Optional[str]] = (),
              label: str = "DAX query") -> QueryRows:
    """Run a read-only query and return its rows as ``QueryRows``.

    ``max_rows=None`` reads everything. With a limit, reading stops as soon as one more row is
    known to exist and ``rows.truncated`` is set; the running command is then cancelled so the
    engine does not keep producing (and the client keep draining) rows nobody will see.
    Raises ``dax_tokens.NonQueryStatementError`` for text that is not a query, ``QueryError``
    for anything the engine or the provider reports.
    """
    dax_tokens.ensure_query(query, allow_dmv=allow_dmv)
    if max_rows is not None:
        max_rows = int(max_rows)
        if max_rows < 0:
            raise ValueError("max_rows must not be negative")
    seconds = query_timeout(timeout)
    secrets = tuple(secrets)

    connection = None
    reader = None
    try:
        connection = connection_cls(connection_string)
        connection.Open()
        command = command_cls(query, connection)
        command.CommandTimeout = seconds
        reader = command.ExecuteReader()

        rows = QueryRows()
        rows.row_limit = max_rows
        rows.columns = [str(reader.GetName(i)) for i in range(int(reader.FieldCount))]
        while reader.Read():
            if max_rows is not None and len(rows) >= max_rows:
                rows.truncated = True
                break
            rows.append({name: to_python(reader[i]) for i, name in enumerate(rows.columns)})
        if rows.truncated:
            try:
                command.Cancel()
            except Exception as exc:
                # Cancelling is an optimisation; without it Close() drains the rest of the stream.
                logger.debug(f"cancel after row limit failed: {clean_error(exc, secrets)}")
        return rows
    except Exception as exc:
        message = clean_error(exc, secrets)
        logger.debug(f"{label} failed: {message}")  # callers decide how loudly to report it
        # 'from None': the original exception text can quote the connection string, and a
        # chained traceback would print it wherever this one is logged.
        raise QueryError(f"{label} failed: {message}") from None
    finally:
        _close_quietly(reader, "reader")
        _close_quietly(connection, "connection")


def field(row: Dict[str, Any], name: str, default: Any = None) -> Any:
    """Read ``name`` from a result row whatever shape the provider gave the column name:
    ``Name``, ``[Name]`` or ``Table[Name]``, in any letter case."""
    for key in (name, f"[{name}]"):
        if key in row:
            return row[key]
    wanted = name.casefold()
    for key, value in row.items():
        text = str(key)
        if "[" in text and text.endswith("]"):
            text = text[text.rindex("[") + 1:-1]
        if text.casefold() == wanted:
            return value
    return default
