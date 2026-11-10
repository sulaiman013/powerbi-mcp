"""
Tests for the server's request boundary and the handlers that run caller-supplied DAX, using
fake connectors (no Power BI, no .NET, no network):

  * environment flags, the statement guard, and the read-only gate
  * every DAX-running tool goes through one guarded path (policy, masking, audit)
  * structured tools always return structured content; secrets are redacted in both forms
  * measure validation: real measure probes, sibling and pending references
  * tenant scan: chunking beyond 100 workspaces, resume, read-only cache
  * fleet monitor, RLS harness, unused-object scan, metadata cache

Run: python tests/test_server_boundary.py
"""
import asyncio
import json
import os
import sys
import tempfile

_AUDIT_DIR = tempfile.mkdtemp(prefix="pbi-mcp-test-audit-")
os.environ["ENABLE_AUDIT"] = "false"          # keep the repo's logs/ folder clean
os.environ.setdefault("POWERBI_MCP_AUDIT_DIR", __import__("tempfile").mkdtemp(prefix="pbi-mcp-audit-"))  # keep logs/ clean
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))
import mcp.types as T  # noqa: E402
import server  # noqa: E402

_failures = []


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f": {detail}" if detail and not cond else ""))
    if not cond:
        _failures.append(name)


def run(coro):
    return asyncio.run(coro)


def call_tool(srv, name, arguments=None):
    """Call a tool through the real MCP request handler, as a client would."""
    handler = srv.server.request_handlers[T.CallToolRequest]
    req = T.CallToolRequest(method="tools/call", params=T.CallToolRequestParams(name=name, arguments=arguments or {}))
    return run(handler(req)).root


class FakeResult:
    def __init__(self, success=True, message="ok", details=None):
        self.success, self.message, self.details = success, message, details or {}


class FakeDesktop:
    """Records every query. Raises for a query containing a 'bad' substring; otherwise returns
    ``rows`` (or a per-substring override from ``answers``)."""
    current_port = 12345
    current_model_name = "Test"

    def __init__(self, rows=None, bad=(), answers=None):
        self.rows = rows if rows is not None else [{"[v]": 1}]
        self.bad, self.answers = bad, answers or {}
        self.queries = []
        self.current_rls_role = None

    def execute_dax(self, q, max_rows=None):
        self.queries.append(q)
        for b in self.bad:
            if b in q:
                raise Exception(f"DAX error near '{b}'")
        for key, value in self.answers.items():
            if key in q:
                return value
        return list(self.rows)

    def list_rls_roles(self):
        return [{"name": "East"}, {"name": "West"}]

    def set_rls_role(self, role):
        self.current_rls_role = role
        return True


class FakeTOM:
    def __init__(self):
        self.model, self.current_port = object(), 12345
        self.created, self.batches, self.saved = [], [], 0

    def create_measure(self, t, n, e, format_string=None, description=None, display_folder=None):
        self.created.append((t, n, e))
        return FakeResult(True, "created")

    def batch_create_measures(self, table, measures, auto_save=True):
        self.batches.append((table, [m["name"] for m in measures], auto_save))
        if auto_save:
            self.saved += 1
        return FakeResult(True, f"Created {len(measures)} measure(s)")

    def save_changes(self):
        self.saved += 1
        return FakeResult(True, "saved")

    def discard_changes(self):
        return FakeResult(True, "discarded")

    # Transactions are owned by the connector: these mirror PowerBITOMConnector's contract.
    in_transaction = False

    def begin_transaction(self):
        self.in_transaction = True
        return FakeResult(True, "transaction started")

    def commit_transaction(self):
        self.in_transaction = False
        self.saved += 1
        return FakeResult(True, "saved")

    def rollback_transaction(self):
        self.in_transaction = False
        self.discarded = getattr(self, "discarded", 0) + 1
        return FakeResult(True, "discarded")


def make_server(desktop=None, tom=None, env=None):
    for key, value in (env or {}).items():
        os.environ[key] = value
    try:
        server.PowerBITOMConnector.is_available = staticmethod(lambda: True)
        srv = server.PowerBIMCPServer()
    finally:
        for key in (env or {}):
            os.environ.pop(key, None)
    srv.desktop_connector = desktop if desktop is not None else FakeDesktop()
    if tom is not None:
        srv.tom_connector = tom
    return srv


def test_env_flag():
    print("\n== environment flags ==")
    cases = {"1": True, "true": True, "TRUE": True, " Yes ": True, "on": True,
             "0": False, "false": False, "No": False, "off": False}
    for raw, expected in cases.items():
        os.environ["PBI_MCP_TEST_FLAG"] = raw
        check(f"{raw!r} -> {expected}", server.env_flag("PBI_MCP_TEST_FLAG", not expected) is expected)
    os.environ["PBI_MCP_TEST_FLAG"] = "maybe"
    check("unrecognised value falls back to the default", server.env_flag("PBI_MCP_TEST_FLAG", True) is True)
    os.environ["PBI_MCP_TEST_FLAG"] = "  "
    check("blank falls back to the default", server.env_flag("PBI_MCP_TEST_FLAG", True) is True)
    os.environ.pop("PBI_MCP_TEST_FLAG")
    check("unset falls back to the default", server.env_flag("PBI_MCP_TEST_FLAG", True) is True)
    for raw in ("1", "true", "YES"):
        srv = make_server(env={"POWERBI_MCP_READONLY": raw})
        check(f"POWERBI_MCP_READONLY={raw} enables lockdown", srv._read_only is True)
    check("lockdown is off by default", make_server()._read_only is False)
    check("an unreadable lockdown value fails safe (on)",
          make_server(env={"POWERBI_MCP_READONLY": "treu"})._read_only is True)
    check("POWERBI_MCP_READONLY=0 keeps it off", make_server(env={"POWERBI_MCP_READONLY": "0"})._read_only is False)


def test_statement_guard():
    print("\n== statement guard on the query tools ==")
    desk = FakeDesktop()
    srv = make_server(desk)
    for command in ('{"delete": {"object": {"database": "x"}}}', "<Batch/>", "SELECT {} ON 0 FROM [Model]",
                    "CREATE MEMBER x AS 1", '/* EVALUATE */ {"refresh": {}}'):
        out = run(srv._handle_desktop_execute_dax({"dax_query": command}))
        check(f"refused: {command[:22]!r}", out.startswith("Refused:"), out[:80])
    check("nothing reached the engine", desk.queries == [], str(desk.queries))
    ok = run(srv._handle_desktop_execute_dax({"dax_query": "EVALUATE {1}"}))
    check("a DAX query runs", ok.startswith("Query returned 1 row(s)"), ok[:60])
    out = run(srv._handle_analyze_query_performance({"dax": '{"refresh": {}}'}))
    check("analyze_query_performance refuses a command", out.startswith("Refused:"), out[:80])
    text, structured = run(srv._handle_run_dax_tests({"tests": [{"name": "t", "dax": '{"x": 1}', "expected": 1}]}))
    check("run_dax_tests reports a refused test as an error, not a pass",
          structured["results"][0]["status"] == "ERROR" and structured["all_passed"] is False, str(structured))


def test_guarded_path_enforces_policy():
    print("\n== every DAX-running tool is policy-checked ==")
    blocked = "EVALUATE ROW(\"x\", CONCATENATEX(Customers, Customers[ssn], \",\"))"
    desk = FakeDesktop(rows=[{"[x]": "123-45-6789"}])
    srv = make_server(desk)
    out = run(srv._handle_desktop_execute_dax({"dax_query": blocked}))
    check("execute refuses a blocked column", "blocked by security policy" in out, out[:90])
    text, structured = run(srv._handle_run_dax_tests({"tests": [{"name": "leak", "dax": blocked}]}))
    check("run_dax_tests refuses it too", "123-45-6789" not in text and structured["results"][0]["status"] == "ERROR", text)
    text, structured = run(srv._handle_validate_dax({"dax": blocked}))
    check("validate_dax reports BLOCKED", text.startswith("[BLOCKED]") and structured.get("blocked") is True, text[:80])
    out = run(srv._handle_rls_test_harness({"dax": blocked}))
    check("rls_test_harness refuses it", "blocked by security policy" in out and "123-45-6789" not in out, out[:90])
    out = run(srv._handle_analyze_query_performance({"dax": blocked}))
    check("analyze_query_performance refuses it", "blocked by security policy" in out, out[:90])
    check("the engine never saw the blocked query", desk.queries == [], str(desk.queries))
    status, err = run(srv._validate_via_desktop("ERROR(CONCATENATEX(Customers, Customers[ssn], \",\"))", True, "T"))
    check("measure validation cannot be used as an error oracle", status is False and "policy" in (err or ""), str(err))


class LeakyModelDesktop(FakeDesktop):
    """A model whose measure [Leak] wraps a blocked column."""

    def execute_dax(self, q, max_rows=None):
        u = q.upper()
        if "INFO.VIEW.TABLES" in u:
            return [{"[Name]": "Customers", "[IsHidden]": False}]
        if "INFO.VIEW.COLUMNS" in u:
            return [{"[Name]": "ssn", "[Table]": "Customers", "[DataType]": "String"}]
        if "INFO.VIEW.MEASURES" in u:
            return [{"[Name]": "Leak", "[Table]": "Customers",
                     "[Expression]": 'CONCATENATEX(Customers, Customers[ssn], ",")'},
                    {"[Name]": "Rows", "[Table]": "Customers", "[Expression]": "COUNTROWS(Customers)"}]
        if "INFO.VIEW.RELATIONSHIPS" in u:
            return []
        return super().execute_dax(q, max_rows)


def test_policy_sees_through_measures_and_aliases():
    print("\n== policy sees through measures and aliases ==")
    desk = LeakyModelDesktop(rows=[{"[x]": "123-45-6789"}])
    srv = make_server(desk)
    out = run(srv._handle_desktop_execute_dax({"dax_query": 'EVALUATE ROW("x", [Leak])'}))
    check("a measure that wraps a blocked column is refused", "blocked by security policy" in out, out[:120])
    check("the wrapped value is not returned", "123-45-6789" not in out)
    ran = [q for q in desk.queries if "INFO.VIEW" not in q.upper()]
    check("the engine never ran the query", ran == [], str(ran))

    harmless = LeakyModelDesktop(rows=[{"[x]": 42}])
    out = run(make_server(harmless)._handle_desktop_execute_dax({"dax_query": 'EVALUATE ROW("x", [Rows])'}))
    check("a harmless measure still runs", out.startswith("Query returned 1 row(s)") and "42" in out, out[:80])

    alias = FakeDesktop(rows=[{"[x]": 4111111111111111}])
    out = run(make_server(alias)._handle_desktop_execute_dax(
        {"dax_query": 'EVALUATE SELECTCOLUMNS(Customers, "x", Customers[card_number])'}))
    check("an aliased masked column does not come back raw", "4111111111111111" not in out, out[:200])

    plain = FakeDesktop(rows=[{"Product[Description]": "Blue cotton shirt", "Hotel[City]": "Lisbon", "[d]": "2024-03-15"}])
    out = run(make_server(plain)._handle_desktop_execute_dax({"dax_query": "EVALUATE Product"}))
    check("ordinary text, city names and dates are left alone",
          "Blue cotton shirt" in out and "Lisbon" in out and "2024-03-15" in out, out[:260])

    err = FakeDesktop(bad=("Customers",))
    err.execute_dax = lambda q, max_rows=None: (_ for _ in ()).throw(Exception("value '123-45-6789' is bad"))
    out = run(make_server(err)._handle_desktop_execute_dax(
        {"dax_query": 'EVALUATE SELECTCOLUMNS(Customers, "x", Customers[card_number])'}))
    check("an engine error is not an oracle for protected data", "123-45-6789" not in out, out)


def test_locked_policy_file_refuses_queries():
    print("\n== a broken policy file locks the layer ==")
    missing = os.path.join(tempfile.mkdtemp(), "nope.yaml")
    desk = FakeDesktop()
    srv = make_server(desk, env={"POWERBI_MCP_POLICY_FILE": missing})
    out = run(srv._handle_desktop_execute_dax({"dax_query": "EVALUATE {1}"}))
    check("queries are refused", "blocked by security policy" in out and desk.queries == [], out[:160])
    status = run(srv._handle_security_status())
    check("security_status explains why", status.count("LOCKED") >= 1 and "nope.yaml" in status, status[:260])
    text, structured = run(srv._handle_validate_dax({"dax": "EVALUATE {1}"}))
    check("validation is refused too", structured.get("valid") is False and structured.get("blocked") is True, text[:80])


def test_boundary_structured_and_redaction():
    print("\n== boundary: structured results and redaction ==")
    srv = make_server()
    srv.client_secret = "sup3r-s3cret-value"

    async def boom(_args):
        raise RuntimeError("Provider failed: Password=sup3r-s3cret-value;Data Source=x")
    srv._tool_dispatch["validate_dax"] = boom
    res = call_tool(srv, "validate_dax", {"dax": "EVALUATE {1}"})
    check("an exception in a structured tool still returns structured content",
          isinstance(res.structuredContent, dict) and "error" in res.structuredContent, str(res.structuredContent))
    check("secret redacted in the text", "sup3r-s3cret-value" not in res.content[0].text, res.content[0].text)
    check("secret redacted in the structured content", "sup3r-s3cret-value" not in json.dumps(res.structuredContent))

    async def leaky(_args):
        return ("fine", {"note": "conn Password=sup3r-s3cret-value;", "nested": [{"k": "sup3r-s3cret-value"}], "n": 3})
    srv._tool_dispatch["validate_dax"] = leaky
    res = call_tool(srv, "validate_dax", {"dax": "EVALUATE {1}"})
    check("structured strings are redacted at any depth",
          "sup3r-s3cret-value" not in json.dumps(res.structuredContent) and res.structuredContent["n"] == 3,
          str(res.structuredContent))

    async def plain(_args):
        return "just text"
    srv._tool_dispatch["validate_dax"] = plain
    res = call_tool(srv, "validate_dax", {"dax": "EVALUATE {1}"})
    check("a plain-text result from a structured tool is wrapped", isinstance(res.structuredContent, dict))

    ro = make_server(env={"POWERBI_MCP_READONLY": "1"})
    res = call_tool(ro, "bridge_screenshot", {})
    check("read-only refusal is structured", res.structuredContent == {"error": "read_only_mode", "refused": True},
          str(res.structuredContent))
    names = {t.name for t in ro._tools}
    check("every tool with an outputSchema is known to the boundary", ro._structured_tools <= names)
    writers = sorted(n for n, a in ro._tool_annotations.items() if not a.readOnlyHint and n not in ro._write_tools)
    check("only session-scoped tools escape the read-only gate",
          set(writers) <= {"desktop_connect", "desktop_set_rls_role", "generate_measure_suite",
                           "build_js_app_measure", "cross_workspace_lineage", "tom_begin_transaction",
                           "tom_rollback_transaction"},
          str(writers))
    for name in ("generate_measure_suite", "build_js_app_measure"):
        res = call_tool(ro, name, {"kind": "column_stats", "column": "Sales[Amount]", "target": "pbip", "table": "T",
                                   "prefix": "App", "js_modules": [{"name": "m", "source": "function main(app) {}"}]})
        check(f"{name} refuses its write target in lockdown", "READ-ONLY" in res.content[0].text, res.content[0].text[:90])


def test_validation_probes():
    print("\n== validation probes ==")
    p = server.build_validation_probe("SUM(Sales[Amt]) -- note", as_measure=True, table="O'Brien")
    check("measure probe defines a query-scoped measure on the host table",
          p.startswith("DEFINE\n\tMEASURE 'O''Brien'[__mcp_validation__] =\nSUM(Sales[Amt]) -- note\n"), p)
    check("a trailing comment cannot swallow the EVALUATE", p.rstrip().endswith('EVALUATE ROW("validation", [__mcp_validation__])'))
    p2 = server.build_validation_probe("[A] + 1 -- c", as_measure=True)
    check("without a table the expression sits on its own lines inside ROW", p2 == 'EVALUATE ROW("validation",\n[A] + 1 -- c\n)', p2)
    check("a query is passed through", server.build_validation_probe("  EVALUATE Sales ") == "EVALUATE Sales")
    check("a query after a comment is still a query",
          server.build_validation_probe("-- hi\nEVALUATE Sales") == "-- hi\nEVALUATE Sales")
    p3 = server.build_validation_probe("[Base] * 2", True, "T", extra_measures=[{"table": "T", "name": "Base", "expression": "1"}])
    check("pending measures are declared before the probe measure",
          p3.index("MEASURE 'T'[Base] =") < p3.index("MEASURE 'T'[__mcp_validation__] ="), p3)


def test_batch_validation():
    print("\n== batch create: validate before anything is created ==")
    measures = [{"name": "Base", "expression": "SUM(Sales[Amt])"},
                {"name": "Double", "expression": "[Base] * 2"},
                {"name": "Share", "expression": "DIVIDE([Double], [Base])"}]
    desk, tom = FakeDesktop(), FakeTOM()
    srv = make_server(desk, tom)
    out = run(srv._handle_batch_create_measures({"table_name": "Sales", "measures": measures}))
    check("valid batch with sibling references is created", out.startswith("Created 3"), out)
    probes = [q for q in desk.queries if "INFO.VIEW" not in q.upper()]
    check("one combined probe declared all three measures",
          len(probes) == 1 and all(f"[{m['name']}] =" in probes[0] for m in measures), str(probes))
    check("saved once", tom.saved == 1)

    desk, tom = FakeDesktop(bad=("BROKEN(",)), FakeTOM()
    srv = make_server(desk, tom)
    bad = [{"name": "Base", "expression": "BROKEN("},
           {"name": "Double", "expression": "[Base] * 2"},
           {"name": "Other", "expression": "1"}]
    out = run(srv._handle_batch_create_measures({"table_name": "Sales", "measures": bad}))
    check("invalid batch is rejected", out.startswith("[INVALID] Batch NOT created"), out[:70])
    check("the broken measure is named, not its dependant", "measure 'Base' failed" in out, out[:120])
    check("nothing was created", tom.batches == [] and tom.saved == 0)

    desk, tom = FakeDesktop(), FakeTOM()
    srv = make_server(desk, tom)
    run(srv._handle_tom_begin_transaction())
    run(srv._handle_create_measure({"table_name": "Sales", "measure_name": "Pending", "expression": "1"}))
    check("create in a transaction is tracked as pending", [m["name"] for m in srv._tom_pending_measures] == ["Pending"])
    desk.queries.clear()
    out = run(srv._handle_batch_create_measures(
        {"table_name": "Sales", "measures": [{"name": "Uses", "expression": "[Pending] + 1"}]}))
    probes = [q for q in desk.queries if "INFO.VIEW" not in q.upper()]
    check("a later batch can reference the pending measure", "PENDING" in out and "MEASURE 'Sales'[Pending] =" in probes[0],
          out + " | " + str(probes[:1]))
    check("nothing is saved inside the transaction", tom.saved == 0 and tom.batches[-1][2] is False)

    text, structured = run(make_server(FakeDesktop(bad=("BROKEN(",)), FakeTOM())._handle_generate_measure_suite(
        {"kind": "column_stats", "column": "Sales[Amount]", "target": "none"}))
    check("generate_measure_suite still previews without writing", structured["written"] is False)


class ScanRest:
    """Fake admin REST: every scan needs ``polls_needed`` status checks before it succeeds."""

    def __init__(self, workspace_count=250, polls_needed=1):
        self.workspace_count, self.polls_needed = workspace_count, polls_needed
        self.started, self.status_calls = [], {}

    def admin_list_workspaces(self, top=None):
        return [{"id": f"w{i}"} for i in range(self.workspace_count)][:top]

    def admin_post_workspace_info(self, ids, lineage=True):
        self.started.append(list(ids))
        return {"id": f"scan{len(self.started)}"}

    def admin_get_scan_status(self, scan_id):
        n = self.status_calls.get(scan_id, 0) + 1
        self.status_calls[scan_id] = n
        return {"status": "Succeeded" if n >= self.polls_needed else "Running"}

    def admin_get_scan_result(self, scan_id):
        idx = int(scan_id.replace("scan", "")) - 1
        ids = self.started[idx] if idx < len(self.started) else ["w0"]
        return {"workspaces": [{"id": i, "name": i, "datasets": [{"id": "d", "name": "M", "roles": []}],
                                "reports": []} for i in ids]}


def lineage_server(rest, read_only=False):
    srv = make_server(env={"POWERBI_MCP_READONLY": "true"} if read_only else None)
    srv.tenant_id = srv.client_id = srv.client_secret = "configured"
    srv.rest_connector = rest
    srv._SCAN_POLL_SECONDS = 0.0
    return srv


def test_lineage_scan():
    print("\n== tenant scan: chunking, resume, read-only cache ==")
    rest = ScanRest(workspace_count=250)
    out = run(lineage_server(rest)._handle_cross_workspace_lineage({}))
    check("250 workspaces are scanned in three chunks", [len(c) for c in rest.started] == [100, 100, 50], str(rest.started and [len(c) for c in rest.started]))
    check("all 250 are in the inventory", "Workspaces: 250" in out, out[:120])

    rest = ScanRest(workspace_count=250)
    out = run(lineage_server(rest)._handle_cross_workspace_lineage({"max_workspaces": 120}))
    check("a cap is reported, never silent", "first 120 of 250" in out and "Workspaces: 120" in out, out)

    rest = ScanRest(workspace_count=150, polls_needed=3)
    srv = lineage_server(rest)
    out = run(srv._handle_cross_workspace_lineage({"max_wait_seconds": 0}))
    check("a timeout returns the scan ids to resume with", "scan_ids" in out and '["scan1", "scan2"]' in out, out)
    started_before = len(rest.started)
    out2 = run(srv._handle_cross_workspace_lineage({"scan_ids": ["scan1", "scan2"], "max_wait_seconds": 5}))
    check("resume starts no new scan", len(rest.started) == started_before)
    check("resume returns the merged inventory", "Workspaces: 150" in out2, out2[:120])

    with tempfile.TemporaryDirectory() as d:
        cache = os.path.join(d, "scan.json")
        run(lineage_server(ScanRest(5), read_only=True)._handle_cross_workspace_lineage({"cache_path": cache}))
        check("read-only mode does not write the cache file", not os.path.exists(cache))
        out = run(lineage_server(ScanRest(5))._handle_cross_workspace_lineage({"cache_path": cache}))
        check("normal mode writes the cache file", os.path.isfile(cache) and "Workspaces: 5" in out)
        rest = ScanRest(99)
        out = run(lineage_server(rest)._handle_cross_workspace_lineage({"cache_path": cache}))
        check("a cached scan is reused without calling the API", rest.started == [] and "Workspaces: 5" in out, out[:120])


def test_fleet_monitor_reports_unknowns():
    print("\n== fleet monitor does not count unreadable datasets as healthy ==")

    class Rest:
        def list_datasets(self, wid):
            if wid == "locked":
                raise RuntimeError("403 Forbidden")
            return [{"id": "ok", "name": "Good", "isRefreshable": True},
                    {"id": "err", "name": "Unreadable", "isRefreshable": True},
                    {"id": "new", "name": "Never", "isRefreshable": True}]

        def get_refresh_history(self, wid, did, top=1):
            if did == "err":
                raise RuntimeError("500 boom")
            return [] if did == "new" else [{"status": "Completed"}]

    srv = make_server()
    srv.tenant_id = srv.client_id = srv.client_secret = "configured"
    srv.rest_connector = Rest()
    out = run(srv._handle_fleet_refresh_monitor({"workspace_ids": ["w1", "locked"]}))
    check("healthy, never-refreshed and unreadable are counted separately",
          "Healthy: 1" in out and "No refresh history: 1" in out and "Could not check: 2" in out, out)
    check("unreadable items are listed", "[UNKNOWN] Unreadable" in out and "[UNKNOWN] workspace locked" in out, out)
    check("it does not claim everything succeeded", "All checked datasets' most recent refresh succeeded" not in out)


def test_rls_harness_restores_role():
    print("\n== RLS harness restores the caller's role ==")
    desk = FakeDesktop(rows=[{"[rows]": 10}])
    desk.current_rls_role = "East"
    srv = make_server(desk)
    out = run(srv._handle_rls_test_harness({"table_name": "O'Brien Sales"}))
    check("previous role is restored, not cleared", desk.current_rls_role == "East", str(desk.current_rls_role))
    check("the table name is quoted safely", "COUNTROWS('O''Brien Sales')" in out, out[:160])
    check("the output says the role was restored", "restored to 'East'" in out)
    out = run(srv._handle_rls_test_harness({"measure_name": "Net]Sales"}))
    check("the measure name is bracket-escaped", "[Net]]Sales]" in out, out[:160])
    zero = FakeDesktop(rows=[{"[rows]": "0.00"}])
    out = run(make_server(zero)._handle_rls_test_harness({"table_name": "T"}))
    check("a zero delivered as text still reads as nothing", "sees NOTHING" in out, out)


class ModelDesktop(FakeDesktop):
    def __init__(self):
        super().__init__()
        self.metadata_calls = 0

    def execute_dax(self, q, max_rows=None):
        u = q.upper()
        if "INFO.VIEW.TABLES" in u:
            self.metadata_calls += 1
            return [{"[Name]": "Date", "[IsHidden]": False}]
        if "INFO.VIEW.COLUMNS" in u:
            return [{"[Name]": "Month", "[Table]": "Date", "[DataType]": "String", "[SortByColumn]": "Month No"},
                    {"[Name]": "Month No", "[Table]": "Date", "[DataType]": "Int64"},
                    {"[Name]": "Spare", "[Table]": "Date", "[DataType]": "Int64"}]
        if "INFO.VIEW.MEASURES" in u or "INFO.VIEW.RELATIONSHIPS" in u:
            return []
        if "CALCDEPENDENCY" in u:
            return [{"[OBJECT]": "X", "[TABLE]": "Date", "[REFERENCED_OBJECT]": "Month", "[REFERENCED_TABLE]": "Date"}]
        return super().execute_dax(q, max_rows)


def test_unused_objects_and_cache():
    print("\n== unused-object scan and the metadata cache ==")
    desk = ModelDesktop()
    srv = make_server(desk)
    out = run(srv._handle_find_unused_objects({}))
    unused = out.split("Unused columns")[1]
    check("a sort-by column is not reported as unused", "Date[Month No]" not in unused, unused)
    check("a genuinely unused column still is", "Date[Spare]" in unused, unused)

    calls = desk.metadata_calls
    run(srv._gather_model_metadata("desktop"))
    run(srv._gather_model_metadata("desktop"))
    check("metadata is reused within the cache window", desk.metadata_calls == calls, f"{calls} -> {desk.metadata_calls}")
    model, _ = run(srv._gather_model_metadata("desktop"))
    model["tables"].clear()
    again, _ = run(srv._gather_model_metadata("desktop"))
    check("callers get their own copy", len(again["tables"]) == 1)
    call_tool(srv, "desktop_connect", {"port": 1})   # a non-read-only tool invalidates the cache
    run(srv._gather_model_metadata("desktop"))
    check("a write or connection change drops the cache", desk.metadata_calls == calls + 1, str(desk.metadata_calls))


def test_transactions():
    print("\n== transactions are owned by the connector ==")

    class FlakyTOM(FakeTOM):
        commit_ok = False

        def commit_transaction(self):
            if not self.commit_ok:
                return FakeResult(False, "the model has an error")
            return super().commit_transaction()

        def delete_measure(self, name, table=None):
            return FakeResult(True, "deleted")

    tom = FlakyTOM()
    srv = make_server(FakeDesktop(), tom)
    check("no transaction before begin", srv._tom_transaction_active is False)
    run(srv._handle_tom_begin_transaction())
    check("begin opens it on the connector", tom.in_transaction and srv._tom_transaction_active)
    again = run(srv._handle_tom_begin_transaction())
    check("a second begin is refused", "already open" in again, again)
    out = run(srv._handle_delete_measure({"measure_name": "M"}))
    check("an edit inside the transaction is pending and unsaved", "PENDING" in out and tom.saved == 0, out)
    failed = run(srv._handle_tom_commit_transaction())
    check("a failed commit says nothing was saved", failed.startswith("Commit failed") and "still open" in failed, failed)
    check("a failed commit leaves the transaction open", srv._tom_transaction_active is True)
    rolled = run(srv._handle_tom_rollback_transaction())
    check("rollback is still possible after a failed commit", "rolled back" in rolled.lower() and not tom.in_transaction, rolled)
    check("commit with no transaction is refused", run(srv._handle_tom_commit_transaction()) == "No TOM transaction is open.")

    tom2 = FlakyTOM()
    tom2.save_changes = lambda: FakeResult(False, "disk full")
    srv2 = make_server(FakeDesktop(), tom2)
    out = run(srv2._handle_delete_measure({"measure_name": "M"}))
    check("a failed save is reported as not done", "NOT deleted" in out and "disk full" in out, out)
    out = run(srv2._handle_create_measure({"table_name": "T", "measure_name": "M", "expression": "1"}))
    check("create reports a failed save honestly", "NOT created" in out, out)


def test_identity():
    print("\n== identity ==")
    srv = make_server()
    check("server name has no stale version suffix", srv.server.name == "powerbi-mcp", srv.server.name)
    check("version is a semantic version string", server.__version__.count(".") == 2)


def test_sdk_version_guard():
    print("\n== the mcp SDK version is checked at start-up ==")
    check("the SDK line this server is written for is accepted, and so is an unreadable version",
          all(server.unsupported_sdk(v) is None for v in ("1.10.0", "1.27.0", "1.30.0", "1.10", None, "", "weird")))
    for bad in ("2.0.0", "2.3.0", "1.9.4", "0.9.1", "10.0.0"):
        msg = server.unsupported_sdk(bad) or ""
        check(f"mcp {bad} is refused, with the fix", bad in msg and "pip install" in msg
              and server.SUPPORTED_SDK in msg, msg)
    check("the SDK this suite runs on is a supported one",
          server.unsupported_sdk(server.installed_sdk_version()) is None, str(server.installed_sdk_version()))

    real = server.installed_sdk_version
    server.installed_sdk_version = lambda: "2.3.0"
    try:
        try:
            server.PowerBIMCPServer()
            check("the server says so instead of failing somewhere inside", False, "it started")
        except RuntimeError as e:
            check("the server says so instead of failing somewhere inside",
                  "2.3.0" in str(e) and server.SUPPORTED_SDK in str(e), str(e))
    finally:
        server.installed_sdk_version = real

    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    for name in ("requirements.txt", "requirements-core.txt"):
        with open(os.path.join(root, name), encoding="utf-8") as f:
            lines = [ln.split("#")[0].strip().replace(" ", "") for ln in f]
        declared = [ln for ln in lines if ln.startswith("mcp")]
        check(f"{name} asks for the same range", declared == ["mcp>=1.10.0,<2"], str(declared))


if __name__ == "__main__":
    for fn in (test_env_flag, test_statement_guard, test_guarded_path_enforces_policy,
               test_policy_sees_through_measures_and_aliases, test_locked_policy_file_refuses_queries,
               test_boundary_structured_and_redaction, test_validation_probes, test_batch_validation,
               test_lineage_scan, test_fleet_monitor_reports_unknowns, test_rls_harness_restores_role,
               test_unused_objects_and_cache, test_transactions, test_identity, test_sdk_version_guard):
        fn()
    print()
    if _failures:
        print(f"FAILED: {len(_failures)} check(s): {_failures}")
        sys.exit(1)
    print("ALL SERVER BOUNDARY TESTS PASSED")
