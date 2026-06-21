"""
Wave 2 tests (offline-verifiable parts): PBIR reference scanner, refresh_doctor
classification, find_unused_objects, impact_analysis. Live model/cloud paths are
mocked. Run: python test_wave2.py
"""
import asyncio
import json
import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))
import server  # noqa: E402
from powerbi_pbip_connector import PowerBIPBIPConnector, PBIPProject  # noqa: E402

_failures = []


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f": {detail}" if detail and not cond else ""))
    if not cond:
        _failures.append(name)


def run(coro):
    return asyncio.run(coro)


# A realistic-ish PBIR visual.json referencing Sales[Amount] (column) and Sales[Total] (measure)
VISUAL_JSON = {
    "visual": {
        "query": {
            "queryState": {
                "Values": {
                    "projections": [
                        {"field": {"Column": {"Expression": {"SourceRef": {"Entity": "Sales"}}, "Property": "Amount"}}},
                        {"field": {"Measure": {"Expression": {"SourceRef": {"Entity": "Sales"}}, "Property": "Total"}}},
                    ]
                }
            }
        }
    }
}


def make_pbip_with_report(tmp):
    """Build a PBIPConnector with a loaded enhanced project containing one visual.json."""
    root = Path(tmp)
    vpath = root / "visual.json"
    vpath.write_text(json.dumps(VISUAL_JSON), encoding="utf-8")
    proj = PBIPProject(
        root_path=root, pbip_file=root / "p.pbip", semantic_model_folder=None,
        report_folder=root, report_json_path=None, tmdl_files=[],
        is_pbir_enhanced=True, visual_json_files=[vpath],
    )
    c = PowerBIPBIPConnector(auto_backup=False)
    c.current_project = proj
    return c


def test_pbir_scanner():
    print("\n== PBIR reference scanner ==")
    with tempfile.TemporaryDirectory() as d:
        c = make_pbip_with_report(d)
        refs = c.collect_report_references()
        check("finds Sales[Amount]", ("Sales", "Amount") in refs, str(refs))
        check("finds Sales[Total]", ("Sales", "Total") in refs, str(refs))
        byfile = c.collect_report_references_by_file()
        check("by-file has one file", len(byfile) == 1 and any(("Sales", "Total") in s for s in byfile.values()))


class FakeRest:
    def resolve_dataset(self, ws, ds):
        return "wid", "did", None

    def get_refresh_history(self, wid, did, top=10):
        return [
            {"status": "Failed", "refreshType": "Scheduled", "endTime": "2026-06-21T01:00:00Z",
             "serviceExceptionJson": '{"errorCode":"0xC11C0020","errorDescription":"model evicted, out of memory"}'},
            {"status": "Failed", "refreshType": "Scheduled", "endTime": "2026-06-20T01:00:00Z",
             "serviceExceptionJson": '{"errorCode":"0xC11C0020"}'},
            {"status": "Failed", "refreshType": "Scheduled", "endTime": "2026-06-19T01:00:00Z",
             "serviceExceptionJson": '{"errorCode":"0xC11C0020"}'},
            {"status": "Completed", "refreshType": "Scheduled", "endTime": "2026-06-18T01:00:00Z"},
        ]


def test_refresh_doctor():
    print("\n== refresh_doctor (mock REST) ==")
    srv = server.PowerBIMCPServer()
    srv.tenant_id = srv.client_id = srv.client_secret = "x"  # make _get_rest_connector return ours
    srv.rest_connector = FakeRest()
    text, structured = run(srv._handle_refresh_doctor({"workspace_name": "WS", "dataset_name": "DS"}))
    check("classifies eviction", structured.get("diagnosis", {}).get("id") == "model_eviction", str(structured))
    check("counts 3 consecutive failures", structured.get("consecutive_failures") == 3, str(structured))
    check("warns about auto-disable", "auto-disable" in text, text)


class FakeDesktop:
    current_port = 12345

    def execute_dax(self, q, max_rows=None):
        u = q.upper()
        if "INFO.VIEW.TABLES" in u:
            return [{"[Name]": "Sales", "[IsHidden]": False}]
        if "INFO.VIEW.COLUMNS" in u:
            return [{"[Name]": "Amount", "[Table]": "Sales", "[DataType]": "Double"},
                    {"[Name]": "Qty", "[Table]": "Sales", "[DataType]": "Int64"}]
        if "INFO.VIEW.MEASURES" in u:
            return [{"[Name]": "Total", "[Table]": "Sales", "[Expression]": "SUM(Sales[Amount])"},
                    {"[Name]": "Orphan", "[Table]": "Sales", "[Expression]": "1"}]
        if "INFO.VIEW.RELATIONSHIPS" in u:
            return []
        if "CALCDEPENDENCY" in u:
            # Total depends on Amount; nothing depends on Qty or Orphan
            base = [{"[OBJECT]": "Total", "[TABLE]": "Sales", "[OBJECT_TYPE]": "MEASURE",
                     "[REFERENCED_OBJECT]": "Amount", "[REFERENCED_TABLE]": "Sales", "[REFERENCED_OBJECT_TYPE]": "COLUMN"}]
            if "REFERENCED_OBJECT] = \"Amount\"" in q:
                return base
            if "REFERENCED_OBJECT]" in q:  # filtered for some other object
                return []
            return base
        return []


def test_find_unused():
    print("\n== find_unused_objects (mock model + report) ==")
    with tempfile.TemporaryDirectory() as d:
        srv = server.PowerBIMCPServer()
        srv.desktop_connector = FakeDesktop()
        srv.pbip_connector = make_pbip_with_report(d)  # report uses Sales[Amount], Sales[Total]
        out = run(srv._handle_find_unused_objects({}))
        check("Qty flagged unused (not used anywhere)", "Sales[Qty]" in out, out)
        check("Orphan measure flagged unused", "Sales[Orphan]" in out, out)
        check("Amount NOT unused (used by Total + report)", "Sales[Amount]" not in out.split("Unused columns")[1] if "Unused columns" in out else True)
        check("Total NOT unused (used in report)", "Sales[Total]" not in out)


def test_impact():
    print("\n== impact_analysis (mock) ==")
    with tempfile.TemporaryDirectory() as d:
        srv = server.PowerBIMCPServer()
        srv.desktop_connector = FakeDesktop()
        srv.pbip_connector = make_pbip_with_report(d)
        out = run(srv._handle_impact_analysis({"object_name": "Amount"}))
        check("shows model dependent Total", "Total" in out, out)
        check("shows report files referencing Amount", "visual.json" in out, out)


class RlsDesktop:
    """Returns role-dependent counts: East sees 30, Admin sees all (100), Empty sees 0."""
    current_port = 12345

    def __init__(self):
        self.active = None

    def list_rls_roles(self):
        return [{"name": "Sales_East"}, {"name": "Admin"}, {"name": "Empty"}]

    def set_rls_role(self, role):
        self.active = role
        return True

    def execute_dax(self, dax, max_rows=1000):
        counts = {None: 100, "Sales_East": 30, "Admin": 100, "Empty": 0}
        return [{"rows": counts.get(self.active, 0)}]


def test_rls_harness():
    print("\n== rls_test_harness (mock) ==")
    srv = server.PowerBIMCPServer()
    dt = RlsDesktop()
    srv.desktop_connector = dt
    out = run(srv._handle_rls_test_harness({"table_name": "Sales"}))
    check("baseline shown", "baseline: 100" in out, out)
    check("East is filtered", "Sales_East" in out and "filtered" in out)
    check("Admin flagged sees everything", "EVERYTHING" in out)
    check("Empty flagged sees nothing", "NOTHING" in out)
    check("role restored to None after run", dt.active is None, f"active={dt.active}")


if __name__ == "__main__":
    print("=" * 70)
    print("  WAVE 2 (DIAGNOSTICS & OPS) TESTS")
    print("=" * 70)
    test_pbir_scanner()
    test_refresh_doctor()
    test_find_unused()
    test_impact()
    test_rls_harness()
    print("\n" + "=" * 70)
    if _failures:
        print(f"  {len(_failures)} CHECK(S) FAILED: {', '.join(_failures)}")
        sys.exit(1)
    print("  ALL WAVE 2 CHECKS PASSED")
    print("=" * 70)
