"""
Tests for the ADOMD side of the live connectivity layer, driven entirely by Python fakes (no
.NET, no Power BI, no network):

  connection_strings          quoting/escaping, port and role validation, redaction
  adomd_query                 read-only gate, finally-close, timeout, row limit, value conversion
  powerbi_desktop_connector   connection strings, metadata helpers, RLS role switching, storage
  powerbi_xmla_connector      connection strings, redacted errors, bounded reads

Run: python tests/test_connectors_adomd.py
"""
import io
import json
import logging
import os
import sys
import types

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))
# Check names include non-ASCII test values; a redirected Windows console (cp1252) cannot encode
# some of them, and a print failure must not look like a test failure.
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(errors="backslashreplace")
import adomd_loader  # noqa: E402

# The connectors load ADOMD.NET when imported. Tests must not start a .NET runtime, so the
# loader reports "nothing found" and the fakes below are installed in its place.
_NOT_LOADED = adomd_loader.LoadResult(name="ADOMD.NET", runtime="netfx")
adomd_loader.load_adomd = lambda force=False: _NOT_LOADED

import adomd_query as aq  # noqa: E402
import connection_strings as cs  # noqa: E402
import dax_tokens  # noqa: E402
import powerbi_desktop_connector as desk  # noqa: E402
import powerbi_xmla_connector as xmla  # noqa: E402

# The connectors log every failure these tests provoke on purpose; keep that off stderr.
logging.getLogger().addHandler(logging.NullHandler())

_failures = []


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f": {detail}" if detail and not cond else ""))
    if not cond:
        _failures.append(name)


def eq(name, actual, expected):
    check(name, actual == expected, f"got {actual!r}, expected {expected!r}")


def raises(name, exc_type, fn, contains=None):
    try:
        fn()
    except exc_type as e:
        ok = contains is None or contains.lower() in str(e).lower()
        check(name, ok, f"raised {type(e).__name__} but message was {str(e)!r}")
        return e
    except Exception as e:  # wrong type: report it, do not crash the suite
        check(name, False, f"raised {type(e).__name__}: {e}")
        return e
    check(name, False, "did not raise")
    return None


# ---------------------------------------------------------------- a connection-string parser

def parse_connection_string(text):
    """Independent reading of a ``Key=Value;Key2='quoted value';...`` string as (key, value) pairs.

    Written from the OLE DB / ADOMD.NET grammar, not from the code under test: a value may be
    bare (ends at the next ';') or wrapped in single or double quotes, where the doubled quote
    character stands for itself.
    """
    pairs, i, n = [], 0, len(text)
    while i < n:
        while i < n and text[i] in " ;":
            i += 1
        if i >= n:
            break
        eq_at = text.index("=", i)
        key = text[i:eq_at].strip()
        i = eq_at + 1
        while i < n and text[i] == " ":
            i += 1
        if i < n and text[i] in "\"'":
            quote, i, buf = text[i], i + 1, []
            while True:
                if i >= n:
                    raise ValueError("unterminated quoted value")
                if text[i] == quote:
                    if i + 1 < n and text[i + 1] == quote:
                        buf.append(quote)
                        i += 2
                        continue
                    i += 1
                    break
                buf.append(text[i])
                i += 1
            while i < n and text[i] == " ":
                i += 1
            if i < n and text[i] != ";":
                raise ValueError(f"unexpected text after a quoted value at {i}")
            value = "".join(buf)
        else:
            end = text.find(";", i)
            end = n if end == -1 else end
            value = text[i:end].strip()
            i = end
        pairs.append((key, value))
    return pairs


def effective(text):
    """What a provider ends up using: for a repeated key the LAST value wins (as ADOMD.NET does)."""
    return {k.lower(): v for k, v in parse_connection_string(text)}


TRICKY = ["Sales; Finance", "Sales;Data Source=evil.example;", 'a"b', "O'Brien", "x=y", 'ends with "',
          '""', "semi;'quote\"mix", "Zo" + chr(0xEB) + " " + chr(0x2713), "a{b}c", "trail" + chr(92)]


def test_connection_string_helper():
    print("\n== connection_strings: quoting, validation ==")
    eq("parser sanity: last duplicate key wins",
       effective("Data Source=localhost:1;Roles=East;Data Source=evil.example:2383")["data source"],
       "evil.example:2383")
    for value in TRICKY:
        text = cs.build([("Initial Catalog", value), ("Password", value)])
        got = effective(text)
        check(f"round-trips {value!r}", got == {"initial catalog": value, "password": value}, repr(got))
    eq("plain value is still quoted", cs.quote_value("Sales"), '"Sales"')
    eq("embedded quote is doubled", cs.quote_value('a"b'), '"a""b"')
    for code in (0, 9, 10, 13, 27, 127):
        raises(f"control character U+{code:04X} rejected", cs.ConnectionStringError,
               lambda c=code: cs.quote_value("ab" + chr(c) + "cd", "dataset"), "control character")
    raises("empty value rejected", cs.ConnectionStringError, lambda: cs.quote_value("  ", "dataset"))
    raises("None rejected", cs.ConnectionStringError, lambda: cs.quote_value(None, "dataset"))
    raises("bad key rejected", cs.ConnectionStringError, lambda: cs.build([("Data Source;X", "v")]))

    eq("port as int", cs.local_data_source(51234), "localhost:51234")
    eq("port as numeric text", cs.local_data_source("51234"), "localhost:51234")
    for bad in ("51234;Roles=Admin", "abc", 0, 70000, -1, None, True, 1.5):
        raises(f"port {bad!r} rejected", cs.ConnectionStringError, lambda b=bad: cs.local_data_source(b))

    eq("no role", cs.role_list(None), None)
    eq("blank role", cs.role_list("   "), None)
    eq("one role", cs.role_list(" East "), "East")
    eq("several roles", cs.role_list(["East", "West"]), "East,West")
    raises("comma inside one role name refused", cs.ConnectionStringError,
           lambda: cs.role_list("East,West"), "comma")
    raises("control character in a role refused", cs.ConnectionStringError,
           lambda: cs.role_list("East" + chr(10) + "x"))


def test_redact():
    print("\n== connection_strings: redaction ==")
    secret = "S3cr3t~Value.123"
    text = f'Provider=MSOLAP;Data Source=x;Password="{secret};tail";User ID=app:a@b'
    out = cs.redact(text)
    check("quoted password masked whole (including the part after ';')",
          secret not in out and "tail" not in out and 'Password=***' in out, out)
    out = cs.redact("Password=plain;User ID=u")
    eq("bare password masked", out, "Password=***;User ID=u")
    out = cs.redact(f"the service said {secret} was wrong", [secret])
    check("known secret value masked anywhere", secret not in out, out)
    out = cs.redact("Authorization: Bearer eyJhbGciOiJSUzI1NiJ9.payload.signature")
    check("bearer token masked", "eyJ" not in out, out)
    eq("short 'secret' does not blank ordinary text", cs.redact("a b c", ["a"]), "a b c")
    eq("None becomes empty", cs.redact(None), "")


# ---------------------------------------------------------------- fake ADOMD.NET

class State:
    """What the fake provider saw, and how it should behave."""

    def __init__(self):
        self.reset()

    def reset(self):
        self.created = []        # connection strings of every connection object
        self.opened = 0
        self.closed = 0
        self.readers = []
        self.commands = []
        self.open_error = None   # callable(connection_string) -> Exception | None
        self.responder = lambda query, conn: (["c"], [])
        self.schema = None       # callable(guid, restrictions) -> (column_names, [row dict])


S = State()


class FakeReader:
    def __init__(self, columns, rows, fail_at=None):
        self.columns, self.rows, self.fail_at = columns, rows, fail_at
        self.position, self.reads, self.closed = -1, 0, False
        self.FieldCount = len(columns)

    def GetName(self, i):
        return self.columns[i]

    def Read(self):
        self.reads += 1
        self.position += 1
        if self.fail_at is not None and self.position == self.fail_at:
            raise Exception("transport error while reading\r\n   at Microsoft.AnalysisServices.Xmla.Read()")
        return self.position < len(self.rows)

    def __getitem__(self, i):
        return self.rows[self.position][i]

    def Close(self):
        self.closed = True


class FakeCommand:
    def __init__(self, query, connection):
        self.query, self.connection = query, connection
        self.CommandTimeout = None
        self.cancelled = False
        S.commands.append(self)

    def ExecuteReader(self):
        result = S.responder(self.query, self.connection.connection_string)
        if isinstance(result, Exception):
            raise result
        columns, rows = result[0], result[1]
        reader = FakeReader(columns, rows, result[2] if len(result) > 2 else None)
        S.readers.append(reader)
        return reader

    def Cancel(self):
        self.cancelled = True


class _Rows(list):
    @property
    def Count(self):
        return len(self)


class FakeConnection:
    def __init__(self, connection_string=None):
        self.connection_string = connection_string
        self.State = 0
        S.created.append(connection_string)

    def Open(self):
        error = S.open_error(self.connection_string) if S.open_error else None
        if error is not None:
            raise error
        S.opened += 1
        self.State = 1

    def Close(self):
        S.closed += 1
        self.State = 0

    def GetSchemaDataSet(self, guid, restrictions):
        names, rows = S.schema(guid, restrictions)
        table = types.SimpleNamespace(
            Columns=[types.SimpleNamespace(ColumnName=n) for n in names], Rows=_Rows(rows))
        return types.SimpleNamespace(Tables=[table])


for module in (desk, xmla):
    module._adomd_available = True
    module.AdomdConnection = FakeConnection
    module.AdomdCommand = FakeCommand
xmla.AdomdSchemaGuid = types.SimpleNamespace(Tables="TABLES", Columns="COLUMNS")
desk._psutil_available = True


def run(query, **kw):
    return aq.run_query(FakeConnection, FakeCommand, "Data Source=localhost:1", query, **kw)


class LogCapture:
    """Collect everything the connectors log while a block runs."""

    def __enter__(self):
        self.buffer = io.StringIO()
        self.handler = logging.StreamHandler(self.buffer)
        self.handler.setLevel(logging.DEBUG)
        root = logging.getLogger()
        self.previous = root.level
        root.setLevel(logging.DEBUG)
        root.addHandler(self.handler)
        return self

    def __exit__(self, *exc):
        root = logging.getLogger()
        root.removeHandler(self.handler)
        root.setLevel(self.previous)
        return False

    @property
    def text(self):
        return self.buffer.getvalue()


# ---------------------------------------------------------------- adomd_query

def test_read_only_gate():
    print("\n== run_query: only queries reach the engine ==")
    refused = [
        '{"createOrReplace": {"object": {"database": "x"}, "database": {}}}',
        '<Batch xmlns="http://schemas.microsoft.com/analysisservices/2003/engine"><Delete/></Batch>',
        "SELECT [Measures].[x] ON 0 FROM [Model]",
        "SELECT * FROM $SYSTEM.TMSCHEMA_ROLES",
        "",
        "-- just a comment",
    ]
    for text in refused:
        S.reset()
        raises(f"refused: {text[:26]!r}", dax_tokens.NonQueryStatementError, lambda t=text: run(t))
        eq("  nothing was opened for it", (len(S.created), len(S.commands)), (0, 0))
    S.reset()
    run("EVALUATE ROW(\"a\", 1)")
    run("DEFINE MEASURE T[m] = 1 EVALUATE ROW(\"a\", [m])")
    run("SELECT * FROM $SYSTEM.TMSCHEMA_ROLES", allow_dmv=True)
    eq("DAX queries and an allowed DMV were executed", len(S.commands), 3)
    S.reset()
    raises("allow_dmv does not let MDX through", dax_tokens.NonQueryStatementError,
           lambda: run("SELECT [Measures].[x] ON 0 FROM [Model]", allow_dmv=True))

    conn = desk.PowerBIDesktopConnector()
    conn.current_port, conn.connection_string = 51234, "Data Source=localhost:51234"
    S.reset()
    raises("Desktop.execute_dax refuses TMSL", dax_tokens.NonQueryStatementError,
           lambda: conn.execute_dax('{"delete": {"object": {"database": "x"}}}'))
    raises("Desktop.execute_dax refuses a DMV unless allowed", dax_tokens.NonQueryStatementError,
           lambda: conn.execute_dax("SELECT * FROM $SYSTEM.DISCOVER_SESSIONS"))
    eq("  no connection was made", len(S.created), 0)
    x = xmla.PowerBIXmlaConnector("tid", "cid", "S3cr3t~Value.123")
    x.connection_string = "cs"
    raises("XMLA.execute_dax refuses TMSL", dax_tokens.NonQueryStatementError,
           lambda: x.execute_dax('{"refresh": {"type": "full"}}'))
    eq("  no connection was made", len(S.created), 0)


def test_always_closes():
    print("\n== run_query: reader and connection are closed on every path ==")
    S.reset()
    S.responder = lambda q, c: (["a"], [(1,), (2,)])
    run("EVALUATE T")
    eq("success: opened/closed", (S.opened, S.closed, S.readers[0].closed), (1, 1, True))

    S.reset()
    S.responder = lambda q, c: Exception(
        "Query (1, 10) syntax error\r\n   at Microsoft.AnalysisServices.Foo()")
    for _ in range(5):
        raises("failing query raises QueryError", aq.QueryError, lambda: run("EVALUATE bad("), "syntax error")
    eq("5 failed queries: 5 opened, 5 closed", (S.opened, S.closed), (5, 5))

    S.reset()
    S.responder = lambda q, c: (["a"], [(1,), (2,), (3,)], 2)
    raises("failure while reading raises QueryError", aq.QueryError, lambda: run("EVALUATE T"), "transport")
    eq("mid-read failure: reader and connection closed", (S.closed, S.readers[0].closed), (1, True))

    S.reset()
    S.open_error = lambda c: Exception("cannot open")
    raises("open failure raises QueryError", aq.QueryError, lambda: run("EVALUATE T"), "cannot open")
    eq("open failure: the connection object is still closed", S.closed, 1)


def test_error_text():
    print("\n== run_query / clean_error: message only, no stack, no secret ==")
    secret = "S3cr3t~Value.123"
    S.reset()
    S.responder = lambda q, c: Exception(
        f"Cannot connect using 'Provider=MSOLAP;Password=\"{secret}\";Data Source=x'\r\n"
        "   at Microsoft.AnalysisServices.AdomdClient.XmlaClient.Connect()\r\n   at Program.Main()")
    e = raises("error raised", aq.QueryError,
               lambda: aq.run_query(FakeConnection, FakeCommand, "cs", "EVALUATE T", secrets=[secret]))
    text = str(e)
    check("stack frames removed", "   at " not in text and "XmlaClient" not in text, text)
    check("secret removed", secret not in text, text)
    check("original exception is not chained (its text would be printed with a traceback)",
          e.__cause__ is None and e.__suppress_context__ is True)
    eq("clean_error on plain text", aq.clean_error("boom\n   at X.Y()"), "boom")
    eq("clean_error keeps a multi-line message", aq.clean_error("line one\r\nline two\r\n   at X()"),
       "line one\nline two")
    eq("clean_error of an empty .NET message names the type", aq.clean_error(KeyError()), "KeyError")


def test_timeout():
    print("\n== run_query: command timeout ==")
    saved = os.environ.pop(aq.QUERY_TIMEOUT_ENV, None)
    try:
        S.reset()
        run("EVALUATE T")
        eq("default is 120 s", S.commands[-1].CommandTimeout, 120)
        run("EVALUATE T", timeout=30)
        eq("explicit timeout", S.commands[-1].CommandTimeout, 30)
        os.environ[aq.QUERY_TIMEOUT_ENV] = "7"
        run("EVALUATE T")
        eq("environment variable", S.commands[-1].CommandTimeout, 7)
        run("EVALUATE T", timeout=45)
        eq("argument beats the environment", S.commands[-1].CommandTimeout, 45)
        os.environ[aq.QUERY_TIMEOUT_ENV] = "soon"
        raises("unparsable environment value is an error, not a silent default", ValueError,
               lambda: run("EVALUATE T"), aq.QUERY_TIMEOUT_ENV)
        os.environ.pop(aq.QUERY_TIMEOUT_ENV)
        raises("negative timeout rejected", ValueError, lambda: run("EVALUATE T", timeout=-1))
        conn = desk.PowerBIDesktopConnector()
        conn.current_port, conn.connection_string = 1, "Data Source=localhost:1"
        conn.execute_dax("EVALUATE T", 10, timeout=9)
        eq("Desktop.execute_dax passes the timeout", S.commands[-1].CommandTimeout, 9)
        x = xmla.PowerBIXmlaConnector("t", "c", "S3cr3t~Value.123")
        x.connection_string = "cs"
        x.execute_dax("EVALUATE T", timeout=11)
        eq("XMLA.execute_dax passes the timeout", S.commands[-1].CommandTimeout, 11)
    finally:
        os.environ.pop(aq.QUERY_TIMEOUT_ENV, None)
        if saved is not None:
            os.environ[aq.QUERY_TIMEOUT_ENV] = saved


def test_row_limit():
    print("\n== run_query: row limit stops reading and reports truncation ==")
    S.reset()
    S.responder = lambda q, c: (["a"], [(i,) for i in range(250000)])
    rows = run("EVALUATE Big", max_rows=3)
    eq("3 rows returned", [r["a"] for r in rows], [0, 1, 2])
    check("flagged truncated", rows.truncated is True and rows.row_limit == 3)
    eq("reader was read 4 times, not 250000", S.readers[0].reads, 4)
    check("the running command was cancelled", S.commands[0].cancelled is True)

    S.reset()
    S.responder = lambda q, c: (["a"], [(1,), (2,), (3,)])
    rows = run("EVALUATE T", max_rows=3)
    check("exactly max_rows rows is not truncation", len(rows) == 3 and rows.truncated is False)
    check("no cancel when complete", S.commands[0].cancelled is False)
    rows = run("EVALUATE T", max_rows=None)
    check("max_rows=None reads everything", len(rows) == 3 and rows.truncated is False)
    rows = run("EVALUATE T", max_rows=0)
    check("max_rows=0 returns nothing and says so", len(rows) == 0 and rows.truncated is True)
    raises("negative max_rows rejected", ValueError, lambda: run("EVALUATE T", max_rows=-5))

    check("result is a real list", isinstance(rows, list) and json.dumps(run("EVALUATE T")) is not None)
    eq("columns are exposed", run("EVALUATE T").columns, ["a"])

    S.reset()
    S.responder = lambda q, c: (["a"], [(i,) for i in range(250000)])
    x = xmla.PowerBIXmlaConnector("t", "c", "S3cr3t~Value.123")
    x.connection_string = "cs"
    rows = x.execute_dax("EVALUATE BigFact", max_rows=1000)
    eq("XMLA: rows materialised", len(rows), 1000)
    eq("XMLA: reads stop right after the limit", S.readers[0].reads, 1001)
    check("XMLA: truncation visible on the result and the connector",
          rows.truncated and x.last_query_truncated is True)
    S.reset()
    S.responder = lambda q, c: (["a"], [(i,) for i in range(250000)])
    rows = x.execute_dax("EVALUATE BigFact")
    eq("XMLA: default limit protects memory", (len(rows), rows.truncated), (xmla.DEFAULT_MAX_ROWS, True))

    conn = desk.PowerBIDesktopConnector()
    conn.current_port, conn.connection_string = 1, "Data Source=localhost:1"
    rows = conn.execute_dax("EVALUATE BigFact", 10)
    check("Desktop: truncation visible", len(rows) == 10 and rows.truncated and conn.last_query_truncated)


def _dotnet(name, **members):
    """A stand-in for a pythonnet object of type System.<name>."""
    return type(name, (), {"__module__": "System", **members})


def test_value_conversion():
    print("\n== to_python: culture-invariant values ==")
    invariant = object()
    original = aq._invariant_culture
    aq._invariant_culture = lambda: invariant

    def decimal(text):
        # Like System.Decimal on a German machine: a comma unless the invariant culture is asked for.
        def to_string(self, provider=None):
            return text if provider is invariant else text.replace(".", ",")
        return _dotnet("Decimal", ToString=to_string)()

    try:
        value = aq.to_python(decimal("1234.50"))
        check("Decimal with a fraction -> float", isinstance(value, float) and value == 1234.5, repr(value))
        zero = aq.to_python(decimal("0"))
        check("Decimal zero -> a number equal to 0", zero == 0 and isinstance(zero, int), repr(zero))
        eq("Decimal negative", aq.to_python(decimal("-0.0001")), -0.0001)
        big = aq.to_python(decimal("79228162514264337593543950335"))
        eq("large integral Decimal keeps every digit", big, 79228162514264337593543950335)
        eq("Decimal 100.0000 -> 100.0", aq.to_python(decimal("100.0000")), 100.0)

        stamp = _dotnet("DateTime", Year=2026, Month=3, Day=1, Hour=4, Minute=5, Second=6, Millisecond=789)()
        eq("DateTime -> ISO 8601", aq.to_python(stamp), "2026-03-01T04:05:06.789000")
        midnight = _dotnet("DateTime", Year=2026, Month=12, Day=31, Hour=0, Minute=0, Second=0,
                           Millisecond=0)()
        eq("date-only DateTime", aq.to_python(midnight), "2026-12-31T00:00:00")
        eq("DBNull -> None", aq.to_python(_dotnet("DBNull")()), None)
        guid = _dotnet("Guid", __str__=lambda self: "0f8fad5b-d9cb-469f-a165-70867728950e")()
        eq("other .NET objects are rendered with str()", aq.to_python(guid),
           "0f8fad5b-d9cb-469f-a165-70867728950e")
        for plain in (None, True, 5, 2.5, "text"):
            check(f"{plain!r} passes through unchanged", aq.to_python(plain) is plain)

        S.reset()
        S.responder = lambda q, c: (["[Amount]", "[When]", "[Missing]"],
                                    [(decimal("0"), stamp, _dotnet("DBNull")())])
        row = run("EVALUATE T")[0]
        eq("conversion is applied to query results",
           row, {"[Amount]": 0, "[When]": "2026-03-01T04:05:06.789000", "[Missing]": None})
    finally:
        aq._invariant_culture = original


# ---------------------------------------------------------------- Desktop connector

def connected_desktop(role=None):
    conn = desk.PowerBIDesktopConnector()
    S.reset()
    check("fixture: connect", conn.connect(port=51234, rls_role=role) is True, str(conn.last_error))
    return conn


def test_desktop_connection_strings():
    print("\n== Desktop: connection string cannot be extended through the role ==")
    injected = "East;EffectiveUserName=ceo@corp.com;Data Source=evil.example:2383"
    conn = connected_desktop(role=injected)
    got = effective(conn.connection_string)
    eq("data source stays local", got["data source"], "localhost:51234")
    eq("the whole text is one role value", got["roles"], injected)
    check("no property was injected", "effectiveusername" not in got and len(got) == 2, repr(got))
    eq("current_rls_role exposed", conn.current_rls_role, injected)
    eq("without a role there is only the data source",
       effective(desk.PowerBIDesktopConnector._connection_string_for(1)), {"data source": "localhost:1"})

    S.reset()
    other = desk.PowerBIDesktopConnector()
    check("port carrying connection-string text is refused", other.connect(port="51234;Roles=Admin") is False)
    check("  with a reason", "port" in (other.last_error or "").lower(), str(other.last_error))
    eq("  and nothing was opened", len(S.created), 0)
    check("role with a comma is refused", other.connect(port=51234, rls_role="East,West") is False
          and "comma" in other.last_error)


def test_desktop_connect_keeps_state():
    print("\n== Desktop: a failed connect keeps the previous connection ==")
    conn = connected_desktop(role="East")
    before = (conn.current_port, conn.connection_string, conn.current_rls_role)
    S.open_error = lambda c: Exception("No connection could be made\r\n   at System.Net.Sockets.Connect()")
    check("connect to a dead port fails", conn.connect(port=1) is False)
    eq("port, connection string and role unchanged", (conn.current_port, conn.connection_string,
                                                     conn.current_rls_role), before)
    check("last_error has the message without the stack",
          "No connection could be made" in conn.last_error and "   at " not in conn.last_error,
          conn.last_error)
    S.open_error = None
    check("the connect probe ran a query", any("ROW" in c.query for c in S.commands))


def test_desktop_rls():
    print("\n== Desktop: RLS role switching never falls back to unrestricted ==")
    conn = connected_desktop()
    check("set East", conn.set_rls_role("East") is True and conn.current_rls_role == "East")
    eq("Roles in the connection string", effective(conn.connection_string).get("roles"), "East")

    S.open_error = lambda c: Exception("role not found") if "Typo" in c else None
    check("switching to an unknown role fails", conn.set_rls_role("Typo") is False)
    eq("previous role is still active", conn.current_rls_role, "East")
    eq("previous connection string is still used", effective(conn.connection_string).get("roles"), "East")
    check("last_error explains", "role not found" in (conn.last_error or ""), str(conn.last_error))

    S.open_error = None
    S.responder = lambda q, c: Exception("role rejected by the engine") if "Typo" in c else (["ok"], [(1,)])
    check("a role rejected only when a query runs is also caught", conn.set_rls_role("Typo") is False
          and conn.current_rls_role == "East")

    S.responder = lambda q, c: (["ok"], [(1,)])
    S.open_error = lambda c: Exception("engine went away")
    check("clearing the role fails while the engine is unreachable", conn.set_rls_role(None) is False)
    eq("...and the restricted role stays in force", conn.current_rls_role, "East")
    S.open_error = None
    check("clearing works when reachable", conn.set_rls_role("") is True and conn.current_rls_role is None)
    check("no Roles left in the connection string", "roles" not in effective(conn.connection_string))

    check("several roles at once", conn.set_rls_role(["East", "West"]) is True)
    eq("  as one comma-separated value", effective(conn.connection_string)["roles"], "East,West")
    check("comma inside a single name refused, role unchanged",
          conn.set_rls_role("A,B") is False and conn.current_rls_role == "East,West")

    conn.set_rls_role("East")
    S.reset()
    S.responder = lambda q, c: (["n"], [(1,)])
    conn.execute_dax("EVALUATE T", 10, rls_role="West")
    eq("per-call role is used for that query", effective(S.created[-1]).get("roles"), "West")
    conn.execute_dax("EVALUATE T", 10, rls_role=None)
    check("per-call None means unrestricted for that query", "roles" not in effective(S.created[-1]))
    conn.execute_dax("EVALUATE T", 10)
    eq("the connector's own role is untouched",
       (conn.current_rls_role, effective(S.created[-1]).get("roles")), ("East", "East"))

    disconnected = desk.PowerBIDesktopConnector()
    check("set_rls_role before connect fails cleanly", disconnected.set_rls_role("East") is False
          and "Not connected" in disconnected.last_error)


def test_desktop_metadata():
    print("\n== Desktop: metadata helpers return everything and raise on failure ==")
    conn = connected_desktop()
    S.responder = lambda q, c: (["[Name]", "[Table]", "[Expression]", "[IsHidden]"],
                                [(f"M{i}", "T", "1", False) for i in range(1500)])
    eq("1500 measures -> 1500 returned (no 1000-row cap)", len(conn.list_measures()), 1500)
    S.responder = lambda q, c: (["[Name]", "[IsHidden]"],
                                [("Sales", False), ("Hidden", True), ("LocalDateTable_1", False)])
    eq("list_tables filters hidden and system tables", conn.list_tables(),
       [{"name": "Sales", "type": "TABLE"}])
    S.responder = lambda q, c: (["[Name]", "[DataType]", "[IsHidden]", "[Description]"],
                                [("Amount", "Double", False, None), ("Key", "Int64", True, "k")])
    eq("list_columns", conn.list_columns('Sa"les'), [{"name": "Amount", "type": "Double", "description": ""}])
    check("table name is escaped inside the DAX string", '[Table] = "Sa""les"' in S.commands[-1].query)
    S.responder = lambda q, c: (
        ["[FromTable]", "[FromColumn]", "[ToTable]", "[ToColumn]", "[IsActive]", "[FromCardinality]",
         "[ToCardinality]"], [("Sales", "DateKey", "Date", "DateKey", True, "Many", "One")])
    eq("list_relationships", conn.list_relationships()[0]["cardinality"], "Many:One")

    S.responder = lambda q, c: Exception("connection lost\r\n   at X.Y()")
    for name, call in (("list_tables", conn.list_tables), ("list_measures", conn.list_measures),
                       ("list_relationships", conn.list_relationships),
                       ("list_columns", lambda: conn.list_columns("Sales")),
                       ("list_rls_roles", conn.list_rls_roles)):
        raises(f"{name} raises instead of returning []", aq.QueryError, call, "connection lost")
    info = conn.get_model_info()
    check("get_model_info reports the error explicitly", "connection lost" in info.get("error", ""),
          repr(info))
    status = conn.get_rls_status()
    check("get_rls_status reports that roles could not be read",
          status["available_roles"] == [] and "connection lost" in (status["roles_error"] or ""),
          repr(status))

    fresh = desk.PowerBIDesktopConnector()
    raises("not connected raises", aq.QueryError, fresh.list_tables, "Not connected")
    raises("execute_dax when not connected raises", aq.QueryError, lambda: fresh.execute_dax("EVALUATE T"))

    conn = connected_desktop(role="East")
    S.reset()
    S.responder = lambda q, c: (["Name", "Description"], [("East", None), ("West", "w")])
    roles = conn.list_rls_roles()
    eq("roles listed", roles, [{"name": "East", "description": ""}, {"name": "West", "description": "w"}])
    check("the role listing runs WITHOUT the active role (the rowset needs admin rights)",
          "roles" not in effective(S.created[-1]), S.created[-1])
    check("  through the roles DMV", "TMSCHEMA_ROLES" in S.commands[-1].query)


def test_desktop_storage():
    print("\n== Desktop: VertiPaq statistics are real or explicitly unavailable ==")
    conn = connected_desktop(role="East")

    def responder(query, connection_string):
        if "DISCOVER_STORAGE_TABLE_COLUMN_SEGMENTS" in query:
            return (["DIMENSION_NAME", "TABLE_ID", "COLUMN_ID", "USED_SIZE"],
                    [("Sales", "Sales (10)", "Amount (11)", 4096), ("Sales", "Sales (10)", "Qty (12)", 2048),
                     ("Sales", "H$Sales (10)$Amount (11)", "POS_TO_ID", 100),
                     ("Date", "Date (20)", "Date (21)", "512")])
        if "DISCOVER_STORAGE_TABLE_COLUMNS" in query:
            return (["DIMENSION_NAME", "COLUMN_ID", "COLUMN_TYPE", "DICTIONARY_SIZE"],
                    [("Sales", "Amount (11)", "BASIC_DATA", 1000), ("Sales", "RowNumber", "ROW_NUMBER", 120),
                     ("Date", "Date (21)", "BASIC_DATA", 50)])
        if "DISCOVER_STORAGE_TABLES" in query:
            return (["DIMENSION_NAME", "TABLE_ID", "ROWS_COUNT"],
                    [("Sales", "Sales (10)", 1000), ("Sales", "H$Sales (10)$Amount (11)", 37),
                     ("Sales", "R$Sales (10)$x", 5), ("Date", "Date (20)", 365)])
        return (["ok"], [(1,)])

    S.reset()
    S.responder = responder
    stats = conn.get_vertipaq_stats()
    check("available", stats["available"] is True and stats["error"] is None, repr(stats))
    by_name = {t["name"]: t for t in stats["tables"]}
    eq("Sales rows come from the table's own storage entry", by_name["Sales"]["rows"], 1000)
    eq("Sales size = segments (data + hierarchy) + dictionaries of data columns",
       (by_name["Sales"]["data_size"], by_name["Sales"]["dictionary_size"], by_name["Sales"]["size"]),
       (6244, 1000, 7244))
    eq("Date", (by_name["Date"]["rows"], by_name["Date"]["size"]), (365, 562))
    eq("total", stats["total_size"], 7806)
    eq("largest first", [t["name"] for t in stats["tables"]], ["Sales", "Date"])
    check("the storage DMVs are queried without the active role",
          all("roles" not in effective(c) for c in S.created), repr(S.created))

    S.responder = lambda q, c: Exception("The DISCOVER_STORAGE_TABLES rowset requires admin\r\n   at X()")
    stats = conn.get_vertipaq_stats()
    check("failure is explicit, not a table of zeros",
          stats["available"] is False and "requires admin" in stats["error"] and stats["tables"] == []
          and stats["total_size"] == 0, repr(stats))

    S.responder = lambda q, c: (["TableName", "RowCount"], [("Sales", 1)])
    stats = conn.get_vertipaq_stats()
    check("an unexpected rowset shape is reported as unavailable",
          stats["available"] is False and "does not expose" in stats["error"], repr(stats))


def test_desktop_discovery():
    print("\n== Desktop: instance discovery ==")

    def process(pid, name, listeners, error=None):
        def connections():
            if error:
                raise error
            return [types.SimpleNamespace(status=status, laddr=types.SimpleNamespace(ip=ip, port=port))
                    for status, ip, port in listeners]
        return types.SimpleNamespace(info={"pid": pid, "name": name}, net_connections=connections)

    class AccessDenied(Exception):
        pass

    fake_psutil = types.SimpleNamespace(
        NoSuchProcess=KeyError, AccessDenied=AccessDenied, ZombieProcess=LookupError,
        process_iter=lambda attrs: [
            process(1, "msmdsrv.exe", [("LISTEN", "0.0.0.0", 2383)]),                 # a full SSAS service
            process(2, "MSMDSRV.EXE", [("ESTABLISHED", "127.0.0.1", 9), ("LISTEN", "::1", 51111),
                                       ("LISTEN", "127.0.0.1", 51111)]),             # Power BI Desktop
            process(3, "msmdsrv.exe", [], error=AccessDenied()),                      # another user's
            process(4, "python.exe", [("LISTEN", "127.0.0.1", 8000)]),
        ])
    original = desk.psutil if hasattr(desk, "psutil") else None
    desk.psutil = fake_psutil
    try:
        S.reset()
        S.responder = lambda q, c: (["CATALOG_NAME"], [("6f9619ff-8b86-d011-b42d-00c04fc964ff",)])
        conn = desk.PowerBIDesktopConnector()
        found = conn.discover_instances()
        eq("two instances, the loopback-bound one (Desktop) first", [(i["pid"], i["port"]) for i in found],
           [(2, 51111), (1, 2383)])
        eq("model name read through a DMV", found[0]["model_name"], "6f9619ff-8b86-d011-b42d-00c04fc964ff")
        eq("every lookup connection was closed", (S.opened, S.closed), (2, 2))
        S.reset()
        check("connect() without a port picks the Desktop instance, not the SSAS service",
              conn.connect() is True and conn.current_port == 51111, str(conn.last_error))

        def broken(attrs):
            raise RuntimeError("process table unavailable")
        desk.psutil = types.SimpleNamespace(**{**vars(fake_psutil), "process_iter": broken})
        raises("a failed process scan is an error, not 'no instances'", RuntimeError, conn.discover_instances)
    finally:
        if original is not None:
            desk.psutil = original


# ---------------------------------------------------------------- XMLA connector

SECRET = "S3cr3t~Value.123"


def test_xmla_connection_string():
    print("\n== XMLA: names cannot add or override connection properties ==")
    x = xmla.PowerBIXmlaConnector("tenant-id", "client-id", SECRET)
    text = x._build_connection_string("Finance", "Sales;Data Source=evil.example;", "a@b.com;Roles=Admin")
    got = effective(text)
    eq("data source is the Power BI endpoint", got["data source"],
       "powerbi://api.powerbi.com/v1.0/myorg/Finance")
    eq("dataset is one literal catalog name", got["initial catalog"], "Sales;Data Source=evil.example;")
    eq("effective user is one literal value", got["effectiveusername"], "a@b.com;Roles=Admin")
    eq("credentials intact", (got["user id"], got["password"]), ("app:client-id@tenant-id", SECRET))
    expected_keys = ["data source", "effectiveusername", "initial catalog", "password", "provider", "user id"]
    check("nothing else was introduced", sorted(got) == expected_keys, repr(sorted(got)))
    eq("a dataset called 'Sales; Finance' keeps its full name",
       effective(x._build_connection_string("WS", "Sales; Finance"))["initial catalog"], "Sales; Finance")
    eq("a workspace name with a semicolon stays inside the data source",
       effective(x._build_connection_string("A;Password=x", "DS"))["password"], SECRET)
    tricky = xmla.PowerBIXmlaConnector("t", "c", 'p;w"d=1')
    eq("a secret containing ; and \" survives intact",
       effective(tricky._build_connection_string("WS", "DS"))["password"], 'p;w"d=1')
    check("no EffectiveUserName unless asked for",
          "effectiveusername" not in effective(x._build_connection_string("WS", "DS")))

    S.reset()
    check("a control character in the dataset name is refused",
          x.connect("WS", "Sales" + chr(10) + "x") is False)
    check("  with a reason and no connection attempt",
          "control character" in (x.last_error or "") and len(S.created) == 0, str(x.last_error))
    check("an empty workspace is refused", x.connect("  ", "DS") is False and len(S.created) == 0)


def test_xmla_errors_are_redacted():
    print("\n== XMLA: provider errors never carry the secret, on any path ==")
    x = xmla.PowerBIXmlaConnector("tenant-id", "client-id", SECRET)
    S.reset()
    S.open_error = lambda c: Exception(f"Cannot connect using connection string '{c}'\r\n   at Adomd.Open()")
    with LogCapture() as log:
        ok = x.connect("Finance", "Sales")
    check("connect fails", ok is False)
    check("the secret is in neither the log nor last_error",
          SECRET not in log.text and SECRET not in (x.last_error or ""), log.text[-300:])
    check("the stack trace is in neither", "Adomd.Open" not in log.text and "   at " not in x.last_error)
    check("the failure is still explained", "Cannot connect" in x.last_error, x.last_error)
    eq("the failed connection object was closed", S.closed, 1)
    check("a failed connect leaves the connector unconnected", x.connection_string is None)

    S.reset()
    check("connect succeeds", x.connect("Finance", "Sales") is True and S.closed == 1)
    S.open_error = lambda c: Exception(f"Login failed for '{c}'")
    closed_before = S.closed
    with LogCapture() as log:
        e1 = raises("discover_tables raises (not [])", aq.QueryError, x.discover_tables, "Login failed")
        e2 = raises("get_table_schema raises (not an empty schema)", aq.QueryError,
                    lambda: x.get_table_schema("Sales"), "Login failed")
        e3 = raises("execute_dax raises", aq.QueryError, lambda: x.execute_dax("EVALUATE T"), "Login failed")
    check("no secret in any exception or log line",
          all(SECRET not in str(e) for e in (e1, e2, e3)) and SECRET not in log.text)
    eq("each of the three failed calls closed its connection object", S.closed - closed_before, 3)

    S.reset()
    with LogCapture() as log:
        x.execute_dax('EVALUATE FILTER(Customer, Customer[Email] = "jane.doe@example.com")')
    check("query text (and the literals in it) is not logged", "jane.doe" not in log.text, log.text[-200:])

    fresh = xmla.PowerBIXmlaConnector("t", "c", SECRET)
    raises("execute_dax when not connected raises (not [])", aq.QueryError,
           lambda: fresh.execute_dax("EVALUATE T"), "Not connected")
    raises("discover_tables when not connected raises", aq.QueryError, fresh.discover_tables)


def test_xmla_discovery_and_rls():
    print("\n== XMLA: schema discovery, sample data, effective user ==")
    x = xmla.PowerBIXmlaConnector("tenant-id", "client-id", SECRET)
    S.reset()
    x.connect("Finance", "Sales")

    def schema(guid, restrictions):
        if guid == "TABLES":
            return (["TABLE_NAME", "TABLE_TYPE", "DESCRIPTION"],
                    [{"TABLE_NAME": "Sales", "TABLE_TYPE": "TABLE", "DESCRIPTION": None},
                     {"TABLE_NAME": "$Sales", "TABLE_TYPE": "SYSTEM TABLE", "DESCRIPTION": None},
                     {"TABLE_NAME": "Date", "TABLE_TYPE": "TABLE", "DESCRIPTION": "Calendar"}])
        return (["COLUMN_NAME", "DATA_TYPE", "COLUMN_HIDDEN"],
                [{"COLUMN_NAME": "Amount", "DATA_TYPE": 5, "COLUMN_HIDDEN": False},
                 {"COLUMN_NAME": "Key", "DATA_TYPE": 3, "COLUMN_HIDDEN": True}])

    S.schema = schema
    eq("discover_tables", x.discover_tables(),
       [{"name": "Sales", "description": "No description available", "type": "TABLE"},
        {"name": "Date", "description": "Calendar", "type": "TABLE"}])
    eq("get_table_schema", x.get_table_schema("Sales"),
       {"table_name": "Sales", "columns": [{"name": "Amount", "type": "Float", "description": ""}]})
    eq("connections closed after discovery", S.opened, S.closed)

    S.responder = lambda q, c: (["a"], [(1,)])
    x.get_sample_data("O'Brien", 7)
    eq("sample query quotes the table name", S.commands[-1].query, "EVALUATE TOPN(7, 'O''Brien')")

    check("set effective user", x.set_effective_user("alice@contoso.com") is True
          and x.effective_user == "alice@contoso.com")
    eq("EffectiveUserName in the connection string",
       effective(x.connection_string)["effectiveusername"], "alice@contoso.com")
    S.open_error = lambda c: Exception("user unknown") if "bob" in c else None
    check("a failed switch keeps the previous user", x.set_effective_user("bob@contoso.com") is False
          and x.effective_user == "alice@contoso.com"
          and effective(x.connection_string)["effectiveusername"] == "alice@contoso.com")
    S.open_error = None
    check("clearing the effective user", x.set_effective_user(None) is True and x.effective_user is None
          and "effectiveusername" not in effective(x.connection_string))
    eq("rls status", x.get_rls_status(), {"rls_active": False, "effective_user": None,
                                          "workspace": "Finance", "dataset": "Sales"})


if __name__ == "__main__":
    for fn in (test_connection_string_helper, test_redact, test_read_only_gate, test_always_closes,
               test_error_text, test_timeout, test_row_limit, test_value_conversion,
               test_desktop_connection_strings, test_desktop_connect_keeps_state, test_desktop_rls,
               test_desktop_metadata, test_desktop_storage, test_desktop_discovery,
               test_xmla_connection_string, test_xmla_errors_are_redacted, test_xmla_discovery_and_rls):
        fn()
    print()
    if _failures:
        print(f"FAILED: {len(_failures)} check(s): {_failures}")
        sys.exit(1)
    print("ALL ADOMD CONNECTOR TESTS PASSED")
