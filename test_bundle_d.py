"""
Bundle D (relationship management) control-flow tests with mocked TOM connector.

Run: python test_bundle_d.py
"""
import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))
import server  # noqa: E402

_failures = []


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f": {detail}" if detail and not cond else ""))
    if not cond:
        _failures.append(name)


class FakeResult:
    def __init__(self, success=True, message="ok"):
        self.success, self.message = success, message


class FakeDesktop:
    current_port = 12345
    current_model_name = "Test"


class FakeTOM:
    def __init__(self):
        self.model = object()
        self.current_port = 12345
        self.created, self.deleted, self.saved = [], 0, 0

    def create_relationship(self, ft, fc, tt, tc, cardinality="many_to_one", cross_filter="single", is_active=True):
        self.created.append((ft, fc, tt, tc, cardinality, cross_filter))
        return FakeResult(True, f"Created relationship {ft}[{fc}] -> {tt}[{tc}]")

    def delete_relationship(self, from_table=None, from_column=None, to_table=None, to_column=None, name=None):
        self.deleted += 1
        return FakeResult(True, "Deleted relationship")

    def save_changes(self):
        self.saved += 1
        return FakeResult(True, "saved")


def make_server():
    server.PowerBITOMConnector.is_available = staticmethod(lambda: True)
    srv = server.PowerBIMCPServer()
    srv.desktop_connector = FakeDesktop()
    srv.tom_connector = FakeTOM()
    return srv


def run(coro):
    return asyncio.run(coro)


def test_create_relationship():
    print("\n== create_relationship ==")
    srv = make_server()
    err = run(srv._handle_create_relationship({"from_table": "Sales"}))
    check("missing args -> error", err.startswith("Error:"), err[:50])

    res = run(srv._handle_create_relationship({"from_table": "Sales", "from_column": "DateKey", "to_table": "DateDim", "to_column": "DateKey"}))
    check("created", "Created relationship" in res, res[:60])
    check("saved (no txn)", srv.tom_connector.saved == 1)
    check("recorded", srv.tom_connector.created and srv.tom_connector.created[0][0] == "Sales")


def test_relationship_transaction():
    print("\n== relationship honors transaction ==")
    srv = make_server()
    run(srv._handle_tom_begin_transaction())
    res = run(srv._handle_create_relationship({"from_table": "S", "from_column": "K", "to_table": "D", "to_column": "K"}))
    check("create deferred (PENDING)", "PENDING" in res, res[:60])
    check("not saved during txn", srv.tom_connector.saved == 0)
    run(srv._handle_tom_commit_transaction())
    check("saved on commit", srv.tom_connector.saved == 1)


def test_delete_relationship():
    print("\n== delete_relationship ==")
    srv = make_server()
    res = run(srv._handle_delete_relationship({"from_table": "Sales", "to_table": "DateDim"}))
    check("deleted", "Deleted relationship" in res, res[:60])
    check("delete recorded", srv.tom_connector.deleted == 1)
    check("saved (no txn)", srv.tom_connector.saved == 1)


if __name__ == "__main__":
    print("=" * 70)
    print("  BUNDLE D (RELATIONSHIP MANAGEMENT) TESTS")
    print("=" * 70)
    test_create_relationship()
    test_relationship_transaction()
    test_delete_relationship()
    print("\n" + "=" * 70)
    if _failures:
        print(f"  {len(_failures)} CHECK(S) FAILED: {', '.join(_failures)}")
        sys.exit(1)
    print("  ALL BUNDLE D CHECKS PASSED")
    print("=" * 70)
