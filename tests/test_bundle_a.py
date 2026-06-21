"""
Bundle A (DAX safety loop + transactions) control-flow tests using mocked connectors.

Live Power BI / ADOMD is not required: the desktop/TOM connectors are replaced with
fakes so we can assert the server's NEW logic:
  - validate_dax returns [VALID]/[INVALID]
  - create_measure / batch_update_measures validate BEFORE committing
  - an open transaction DEFERS SaveChanges until commit; rollback discards

Run: python test_bundle_a.py
"""
import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))
import server  # noqa: E402

_failures = []


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f": {detail}" if detail and not cond else ""))
    if not cond:
        _failures.append(name)


class FakeResult:
    def __init__(self, success=True, message="ok", details=None):
        self.success, self.message, self.details = success, message, details or {}


class FakeDesktop:
    """execute_dax raises if the query contains any 'bad' substring (simulating a DAX error)."""
    def __init__(self, bad=()):
        self.current_port = 12345
        self.current_model_name = "Test"
        self.bad = bad
        self.queries = []

    def execute_dax(self, q, max_rows=None):
        self.queries.append(q)
        for b in self.bad:
            if b in q:
                raise Exception(f"DAX syntax error near '{b}'")
        return []


class FakeTOM:
    def __init__(self):
        self.model = object()
        self.current_port = 12345
        self.created, self.saved, self.discarded = [], 0, 0

    def create_measure(self, t, n, e, format_string=None, description=None):
        self.created.append((t, n, e))
        return FakeResult(True, "created")

    def delete_measure(self, n, t=None):
        return FakeResult(True, "deleted")

    def save_changes(self):
        self.saved += 1
        return FakeResult(True, "saved")

    def discard_changes(self):
        self.discarded += 1
        return FakeResult(True, "discarded")

    def batch_update_measures(self, updates, auto_save=True):
        if auto_save:
            self.saved += 1
        return FakeResult(True, "updated", {"results": [{"measure_name": u.get("measure_name"), "success": True} for u in updates]})


def make_server(bad=()):
    server.PowerBITOMConnector.is_available = staticmethod(lambda: True)  # bypass DLL check
    srv = server.PowerBIMCPServer()
    srv.desktop_connector = FakeDesktop(bad=bad)
    srv.tom_connector = FakeTOM()
    return srv


def run(coro):
    return asyncio.run(coro)


def test_validate_dax():
    print("\n== validate_dax ==")
    srv = make_server(bad=("BADTOKEN",))
    ok_text, ok_struct = run(srv._handle_validate_dax({"dax": "EVALUATE Sales"}))
    check("valid query -> [VALID]", ok_text.startswith("[VALID]"), ok_text[:60])
    check("valid -> structured valid=True", ok_struct.get("valid") is True, str(ok_struct))
    bad_text, bad_struct = run(srv._handle_validate_dax({"dax": "EVALUATE BADTOKEN"}))
    check("bad query -> [INVALID]", bad_text.startswith("[INVALID]"), bad_text[:60])
    check("bad -> structured valid=False", bad_struct.get("valid") is False, str(bad_struct))


def test_create_measure_validation():
    print("\n== create_measure validate-before-commit ==")
    srv = make_server(bad=("BADREF",))
    res = run(srv._handle_create_measure({"table_name": "Sales", "measure_name": "M", "expression": "SUM(BADREF)"}))
    check("invalid expr blocked", res.startswith("[INVALID]"), res[:60])
    check("tom.create_measure NOT called", srv.tom_connector.created == [], str(srv.tom_connector.created))
    check("nothing saved", srv.tom_connector.saved == 0)

    srv2 = make_server()
    res2 = run(srv2._handle_create_measure({"table_name": "Sales", "measure_name": "M", "expression": "SUM(Sales[Amt])"}))
    check("valid measure created", "created successfully" in res2, res2[:60])
    check("saved once (no txn)", srv2.tom_connector.saved == 1, str(srv2.tom_connector.saved))


def test_transaction_defers_save():
    print("\n== transactions defer save / rollback ==")
    srv = make_server()
    begin = run(srv._handle_tom_begin_transaction())
    check("transaction started", "transaction started" in begin.lower(), begin[:60])

    res = run(srv._handle_create_measure({"table_name": "Sales", "measure_name": "M", "expression": "SUM(Sales[Amt])"}))
    check("create returns PENDING", "PENDING" in res, res[:60])
    check("created in memory", len(srv.tom_connector.created) == 1)
    check("NOT saved yet (deferred)", srv.tom_connector.saved == 0, str(srv.tom_connector.saved))

    commit = run(srv._handle_tom_commit_transaction())
    check("commit saved once", srv.tom_connector.saved == 1, str(srv.tom_connector.saved))
    check("commit message", "committed" in commit.lower(), commit[:60])
    check("txn flag cleared", srv._tom_transaction_active is False)

    # rollback path
    srv2 = make_server()
    run(srv2._handle_tom_begin_transaction())
    run(srv2._handle_delete_measure({"measure_name": "M"}))
    check("delete deferred (not saved)", srv2.tom_connector.saved == 0)
    run(srv2._handle_tom_rollback_transaction())
    check("rollback discarded once", srv2.tom_connector.discarded == 1, str(srv2.tom_connector.discarded))


def test_batch_update_validation():
    print("\n== batch_update_measures validation ==")
    srv = make_server(bad=("OOPS",))
    res = run(srv._handle_batch_update_measures({"updates": [
        {"measure_name": "Good", "expression": "SUM(Sales[Amt])"},
        {"measure_name": "Bad", "expression": "OOPS("},
    ]}))
    check("batch blocked on invalid", res.startswith("[INVALID]"), res[:60])
    check("no save on invalid batch", srv.tom_connector.saved == 0)


if __name__ == "__main__":
    print("=" * 70)
    print("  BUNDLE A (DAX SAFETY LOOP + TRANSACTIONS) TESTS")
    print("=" * 70)
    test_validate_dax()
    test_create_measure_validation()
    test_transaction_defers_save()
    test_batch_update_validation()
    print("\n" + "=" * 70)
    if _failures:
        print(f"  {len(_failures)} CHECK(S) FAILED: {', '.join(_failures)}")
        sys.exit(1)
    print("  ALL BUNDLE A CHECKS PASSED")
    print("=" * 70)
