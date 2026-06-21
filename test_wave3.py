"""
Wave 3 (governance-ops fleet) offline tests: Scanner-result summary, cross_workspace_lineage
(via cached scan, no API), fleet_refresh_monitor (mock REST). Live admin paths need a tenant.
Run: python test_wave3.py
"""
import asyncio
import json
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))
import server  # noqa: E402
from governance import summarize_scan  # noqa: E402

_failures = []


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f": {detail}" if detail and not cond else ""))
    if not cond:
        _failures.append(name)


def run(coro):
    return asyncio.run(coro)


SCAN = {"workspaces": [
    {"id": "w1", "name": "Sales WS",
     "datasets": [
         {"id": "d1", "name": "Sales Model", "roles": [{"name": "East"}], "sensitivityLabel": {"labelId": "L1"}},
         {"id": "d2", "name": "Adhoc", "roles": []},
     ],
     "reports": [{"id": "r1", "name": "Sales Report", "datasetId": "d1"},
                 {"id": "r2", "name": "Exec", "datasetId": "d1"}]},
    {"id": "w2", "name": "Fin WS",
     "datasets": [{"id": "d3", "name": "Finance", "roles": []}], "reports": []},
]}


def test_summarize():
    print("\n== summarize_scan ==")
    s = summarize_scan(SCAN, dataset_name="Sales Model")
    check("counts", s["workspaces"] == 2 and s["datasets"] == 3 and s["reports"] == 2, str(s))
    check("downstream reports found", set(s["downstream_reports"]) == {"Sales WS/Sales Report", "Sales WS/Exec"}, str(s["downstream_reports"]))
    check("no-RLS datasets flagged", "Sales WS/Adhoc" in s["datasets_without_rls"] and "Fin WS/Finance" in s["datasets_without_rls"])
    check("RLS dataset not flagged", "Sales WS/Sales Model" not in s["datasets_without_rls"])
    check("unlabeled flagged", "Sales WS/Adhoc" in s["datasets_without_sensitivity_label"] and "Sales WS/Sales Model" not in s["datasets_without_sensitivity_label"])


def test_lineage_handler_cache():
    print("\n== cross_workspace_lineage (cached scan, no API) ==")
    srv = server.PowerBIMCPServer()
    srv.tenant_id = srv.client_id = srv.client_secret = "x"  # so _get_rest_connector returns a connector
    with tempfile.TemporaryDirectory() as d:
        p = os.path.join(d, "scan.json")
        with open(p, "w", encoding="utf-8") as f:
            json.dump(SCAN, f)
        out = run(srv._handle_cross_workspace_lineage({"cache_path": p, "dataset_name": "Sales Model"}))
        check("inventory counts shown", "Datasets: 3" in out and "Reports: 2" in out, out[:120])
        check("downstream reports listed", "Sales Report" in out and "Exec" in out)
        check("no-RLS section", "Adhoc" in out and "Finance" in out)


class FakeRest:
    def list_datasets(self, wid):
        return [{"id": "d1", "name": "A", "isRefreshable": True},
                {"id": "d2", "name": "B", "isRefreshable": False}]

    def get_refresh_history(self, wid, did, top=1):
        return [{"status": "Failed", "endTime": "2026-06-21T00:00:00Z",
                 "serviceExceptionJson": '{"errorCode":"gateway is offline"}'}]


def test_fleet_monitor():
    print("\n== fleet_refresh_monitor (mock REST) ==")
    srv = server.PowerBIMCPServer()
    srv.tenant_id = srv.client_id = srv.client_secret = "x"
    srv.rest_connector = FakeRest()
    out = run(srv._handle_fleet_refresh_monitor({"workspace_ids": ["w1"]}))
    check("one refreshable checked", "checked: 1" in out, out)
    check("one failure found", "FAILURES: 1" in out, out)
    check("classifies gateway cause", "gateway" in out.lower(), out)
    check("non-refreshable skipped (B absent)", "] B (" not in out)


if __name__ == "__main__":
    print("=" * 70)
    print("  WAVE 3 (GOVERNANCE-OPS FLEET) TESTS")
    print("=" * 70)
    test_summarize()
    test_lineage_handler_cache()
    test_fleet_monitor()
    print("\n" + "=" * 70)
    if _failures:
        print(f"  {len(_failures)} CHECK(S) FAILED: {', '.join(_failures)}")
        sys.exit(1)
    print("  ALL WAVE 3 CHECKS PASSED")
    print("=" * 70)
