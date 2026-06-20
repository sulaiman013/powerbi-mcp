"""
Bundle B (quality & performance) end-to-end wiring tests with a mocked connector.

A fake desktop connector returns INFO.VIEW-shaped rows (bracketed keys, to exercise
the tolerant key parser) so we can assert _gather_model_metadata normalization plus
run_bpa / audit_ai_readiness / analyze_model_storage / analyze_query_performance.

Run: python test_bundle_b.py
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


class FakeDesktop:
    current_port = 12345
    current_model_name = "Test"

    def execute_dax(self, q, max_rows=None):
        u = q.upper()
        if "INFO.VIEW.TABLES" in u:
            return [{"[Name]": "Sales", "[IsHidden]": False, "[Description]": "fact"},
                    {"[Name]": "DateDim", "[IsHidden]": False, "[Description]": ""}]
        if "INFO.VIEW.COLUMNS" in u:
            return [
                {"[Name]": "Amount", "[Table]": "Sales", "[DataType]": "Double", "[IsHidden]": False, "[Description]": "", "[ColumnType]": "Data"},
                {"[Name]": "DateKey", "[Table]": "Sales", "[DataType]": "Int64", "[IsHidden]": True, "[Description]": "k", "[ColumnType]": "Data"},
                {"[Name]": "DateKey", "[Table]": "DateDim", "[DataType]": "String", "[IsHidden]": False, "[Description]": "key", "[ColumnType]": "Data"},
            ]
        if "INFO.VIEW.MEASURES" in u:
            return [{"[Name]": "Total", "[Table]": "Sales", "[Expression]": "SUM(Sales[Amount])", "[FormatString]": "", "[Description]": "", "[IsHidden]": False}]
        if "INFO.VIEW.RELATIONSHIPS" in u:
            return [{"[FromTable]": "Sales", "[FromColumn]": "DateKey", "[ToTable]": "DateDim", "[ToColumn]": "DateKey", "[IsActive]": True, "[CrossFilteringBehavior]": "OneDirection"}]
        if "COUNTROWS" in u:
            return [{"[r]": 1000}]
        return [{"[v]": 1}, {"[v]": 2}]


def make_server():
    srv = server.PowerBIMCPServer()
    srv.desktop_connector = FakeDesktop()
    return srv


def run(coro):
    return asyncio.run(coro)


def test_run_bpa():
    print("\n== run_bpa (wired) ==")
    out, structured = run(make_server()._handle_run_bpa({}))
    check("header present", "Best Practice Analyzer" in out, out[:60])
    check("float column found", "Sales[Amount]" in out and "float" in out.lower())
    check("no-format measure found", "Sales[Total]" in out)
    check("rel type mismatch found", "different data types" in out.lower())
    check("structured findings present", isinstance(structured.get("findings"), list) and structured["findings"], "no structured findings")


def test_ai_readiness():
    print("\n== audit_ai_readiness (wired) ==")
    out, structured = run(make_server()._handle_audit_ai_readiness({}))
    check("score present", "Score:" in out and "Grade" in out, out[:60])
    check("metrics present", "Measures with descriptions" in out)
    check("structured score present", isinstance(structured.get("score"), (int, float)), str(structured)[:60])


def test_storage():
    print("\n== analyze_model_storage (wired) ==")
    out = run(make_server()._handle_analyze_model_storage({}))
    check("table listed", "Sales" in out)
    check("row count from COUNTROWS", "1,000" in out, out)


def test_query_perf():
    print("\n== analyze_query_performance (wired) ==")
    out = run(make_server()._handle_analyze_query_performance({"dax": "EVALUATE Sales"}))
    check("duration reported", "Duration:" in out)
    check("rows reported", "Rows returned: 2" in out, out)


if __name__ == "__main__":
    print("=" * 70)
    print("  BUNDLE B (QUALITY & PERFORMANCE) WIRING TESTS")
    print("=" * 70)
    test_run_bpa()
    test_ai_readiness()
    test_storage()
    test_query_perf()
    print("\n" + "=" * 70)
    if _failures:
        print(f"  {len(_failures)} CHECK(S) FAILED: {', '.join(_failures)}")
        sys.exit(1)
    print("  ALL BUNDLE B CHECKS PASSED")
    print("=" * 70)
