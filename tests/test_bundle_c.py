"""
Bundle C (modern MCP surface) tests: resources, prompts, completion, structured output.
Uses a mocked connector for the model-backed resources/completion.

Run: python test_bundle_c.py
"""
import asyncio
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))
import server  # noqa: E402
from mcp.types import CompletionArgument  # noqa: E402

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
            return [{"[Name]": "Sales", "[IsHidden]": False, "[Description]": "fact"}]
        if "INFO.VIEW.COLUMNS" in u:
            return [{"[Name]": "Amount", "[Table]": "Sales", "[DataType]": "Double", "[IsHidden]": False, "[Description]": "", "[ColumnType]": "Data"}]
        if "INFO.VIEW.MEASURES" in u:
            return [{"[Name]": "Total Sales", "[Table]": "Sales", "[Expression]": "SUM(Sales[Amount])", "[FormatString]": "", "[Description]": "", "[IsHidden]": False}]
        if "INFO.VIEW.RELATIONSHIPS" in u:
            return []
        return []


def make_server():
    srv = server.PowerBIMCPServer()
    srv.desktop_connector = FakeDesktop()
    return srv


def run(coro):
    return asyncio.run(coro)


def test_resources():
    print("\n== read_resource ==")
    srv = make_server()
    schema = json.loads(run(srv._read_resource("powerbi://desktop/schema")))
    check("schema has Sales table", any(t["name"] == "Sales" for t in schema.get("tables", [])), str(schema)[:80])
    measures = json.loads(run(srv._read_resource("powerbi://desktop/measures")))
    check("measures listed", any(m["name"] == "Total Sales" for m in measures), str(measures)[:80])
    bpa = json.loads(run(srv._read_resource("powerbi://desktop/bpa")))
    check("bpa has findings key", "findings" in bpa)
    ai = json.loads(run(srv._read_resource("powerbi://desktop/ai-readiness")))
    check("ai-readiness has score", "score" in ai, str(ai)[:60])
    bad = json.loads(run(srv._read_resource("powerbi://desktop/nonsense")))
    check("unknown resource -> error json", "error" in bad)


def test_prompts():
    print("\n== prompts ==")
    srv = make_server()
    check("prompts registered", len(srv._prompts) >= 5 and "plan_safe_rename" in srv._prompts, str(list(srv._prompts)))
    rendered = srv._prompts["plan_safe_rename"]["render"]({"old_name": "OldT", "new_name": "NewT"})
    check("rename prompt includes names", "OldT" in rendered and "NewT" in rendered)
    check("rename prompt steers to PBIP", "pbip_rename" in rendered.lower() and "deprecated" in rendered.lower())
    audit = srv._prompts["audit_model"]["render"]({})
    check("audit prompt orchestrates tools", "run_bpa" in audit and "audit_ai_readiness" in audit)


def test_completion():
    print("\n== completion ==")
    srv = make_server()
    comp = run(srv._complete_argument(CompletionArgument(name="measure_name", value="tot")))
    check("completes measure name", "Total Sales" in comp.values, str(comp.values))
    comp2 = run(srv._complete_argument(CompletionArgument(name="table_name", value="sal")))
    check("completes table name", "Sales" in comp2.values, str(comp2.values))
    # disconnected -> empty, no crash
    srv2 = server.PowerBIMCPServer()
    srv2.desktop_connector = None
    comp3 = run(srv2._complete_argument(CompletionArgument(name="measure_name", value="x")))
    check("disconnected -> empty completion", comp3.values == [])


def test_structured_output():
    print("\n== structured output ==")
    srv = make_server()
    text, structured = run(srv._handle_run_bpa({}))
    check("run_bpa returns (text, dict)", isinstance(text, str) and isinstance(structured, dict))
    check("structured has summary+findings", "summary" in structured and "findings" in structured)


if __name__ == "__main__":
    print("=" * 70)
    print("  BUNDLE C (RESOURCES / PROMPTS / COMPLETION / STRUCTURED OUTPUT) TESTS")
    print("=" * 70)
    test_resources()
    test_prompts()
    test_completion()
    test_structured_output()
    print("\n" + "=" * 70)
    if _failures:
        print(f"  {len(_failures)} CHECK(S) FAILED: {', '.join(_failures)}")
        sys.exit(1)
    print("  ALL BUNDLE C CHECKS PASSED")
    print("=" * 70)
