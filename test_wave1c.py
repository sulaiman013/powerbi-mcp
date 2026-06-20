"""
Wave 1c tests: refresh-error classifier, governance reference resources, read-only mode.

Run: python test_wave1c.py   (pure Python, no Power BI)
"""
import asyncio
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

_failures = []


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f": {detail}" if detail and not cond else ""))
    if not cond:
        _failures.append(name)


def test_classifier():
    print("\n== classify_refresh_error ==")
    from refresh_diagnostics import classify_refresh_error
    check("credentials", classify_refresh_error("AccessUnauthorized: credentials failed")["id"] == "credentials_expired")
    check("eviction 0xC11C0020", classify_refresh_error("error 0xC11C0020 model evicted")["id"] == "model_eviction")
    check("capacity throttle", classify_refresh_error("exceeded the capacity limit for semantic model refreshes")["id"] == "capacity_throttle")
    check("gateway", classify_refresh_error("The gateway is offline")["id"] == "gateway_unreachable")
    check("timeout", classify_refresh_error("operation timed out")["id"] == "timeout")
    check("unknown fallback", classify_refresh_error("something weird")["id"] == "unknown")
    check("empty -> none", classify_refresh_error("")["id"] == "none")


def test_resources():
    print("\n== governance reference resources ==")
    import server
    srv = server.PowerBIMCPServer()
    rules = json.loads(asyncio.run(srv._read_resource("powerbi://reference/bpa-rules")))
    check("bpa-rules is a list", isinstance(rules, list) and len(rules) > 5, str(type(rules)))
    check("rule has id+severity", all("id" in r and "severity" in r for r in rules))
    errs = json.loads(asyncio.run(srv._read_resource("powerbi://reference/refresh-errors")))
    check("refresh-errors has rules", "rules" in errs and len(errs["rules"]) > 3)
    check("disable threshold present", errs.get("consecutive_failure_disable_threshold") == 4)


def test_read_only_mode():
    print("\n== read-only / lockdown mode ==")
    import importlib
    os.environ["POWERBI_MCP_READONLY"] = "true"
    import server
    importlib.reload(server)  # re-read env in __init__
    srv = server.PowerBIMCPServer()
    check("read_only flag set", srv._read_only is True)
    check("delete_measure is a write tool", "delete_measure" in srv._write_tools)
    check("create_measure is a write tool", "create_measure" in srv._write_tools)
    check("security_status is NOT a write tool", "security_status" not in srv._write_tools)

    from mcp.types import CallToolRequest
    handler = srv.server.request_handlers[CallToolRequest]

    def call(name, args=None):
        req = CallToolRequest(method="tools/call", params={"name": name, "arguments": args or {}})
        res = asyncio.run(handler(req))
        return res.root.content[0].text

    refused = call("delete_measure", {"measure_name": "X"})
    check("write tool refused in read-only", "READ-ONLY" in refused, refused[:80])
    allowed = call("security_status")
    check("read tool allowed in read-only", "READ-ONLY" not in allowed, allowed[:80])

    os.environ.pop("POWERBI_MCP_READONLY", None)
    importlib.reload(server)


if __name__ == "__main__":
    print("=" * 70)
    print("  WAVE 1C (REFRESH CLASSIFIER / RESOURCES / READ-ONLY MODE) TESTS")
    print("=" * 70)
    test_classifier()
    test_resources()
    test_read_only_mode()
    print("\n" + "=" * 70)
    if _failures:
        print(f"  {len(_failures)} CHECK(S) FAILED: {', '.join(_failures)}")
        sys.exit(1)
    print("  ALL WAVE 1C CHECKS PASSED")
    print("=" * 70)
