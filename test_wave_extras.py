"""
SemanticOps-parity extras: tamper-evident audit hash chain + DAX regression runner.
Run: python test_wave_extras.py   (pure Python, no Power BI)
"""
import asyncio
import json
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))
import server  # noqa: E402
from model_analysis import dax_test_verdict  # noqa: E402
from security.audit_logger import AuditLogger, AuditEventType  # noqa: E402

_failures = []


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f": {detail}" if detail and not cond else ""))
    if not cond:
        _failures.append(name)


def run(coro):
    return asyncio.run(coro)


def test_dax_verdict():
    print("\n== dax_test_verdict ==")
    check("numeric exact pass", dax_test_verdict(100, 100)[0] is True)
    check("numeric exact fail", dax_test_verdict(101, 100)[0] is False)
    check("numeric tolerance", dax_test_verdict(100.4, 100, tolerance=0.5)[0] is True)
    check("bool", dax_test_verdict(True, True)[0] is True)
    check("string mismatch", dax_test_verdict("a", "b")[0] is False)


def test_audit_chain():
    print("\n== tamper-evident audit chain ==")
    with tempfile.TemporaryDirectory() as d:
        a = AuditLogger(log_dir=d)
        for i in range(3):
            a.log_event(AuditEventType.QUERY_SUCCESS, message=f"event {i}")
        res = a.verify_chain()
        check("fresh chain valid", res["valid"] is True and res["checked"] == 3, str(res))

        # Tamper with line 2's content but keep its stored hashes
        lines = open(a.log_file, encoding="utf-8").read().splitlines()
        ev = json.loads(lines[1])
        ev["message"] = "TAMPERED"
        lines[1] = json.dumps(ev, default=str)
        open(a.log_file, "w", encoding="utf-8").write("\n".join(lines) + "\n")

        res2 = a.verify_chain()
        check("tamper detected", res2["valid"] is False, str(res2))
        check("points at broken line 2", res2.get("broken_line") == 2, str(res2))


def test_audit_chain_delete_detected():
    print("\n== audit chain detects deletion ==")
    with tempfile.TemporaryDirectory() as d:
        a = AuditLogger(log_dir=d)
        for i in range(4):
            a.log_event(AuditEventType.QUERY_SUCCESS, message=f"e{i}")
        lines = open(a.log_file, encoding="utf-8").read().splitlines()
        del lines[1]  # delete an entry -> linkage breaks
        open(a.log_file, "w", encoding="utf-8").write("\n".join(lines) + "\n")
        res = a.verify_chain()
        check("deletion detected", res["valid"] is False, str(res))


class FakeDesktop:
    current_port = 12345

    def execute_dax(self, dax, max_rows=None):
        # return the integer embedded in the query (so tests can assert expected)
        import re
        m = re.search(r"(\d+)", dax)
        return [{"v": int(m.group(1))}] if m else [{"v": 0}]


def test_run_dax_tests():
    print("\n== run_dax_tests (mock) ==")
    srv = server.PowerBIMCPServer()
    srv.desktop_connector = FakeDesktop()
    tests = [
        {"name": "sales total", "dax": "EVALUATE ROW(\"v\", 42)", "expected": 42},
        {"name": "wrong", "dax": "EVALUATE ROW(\"v\", 7)", "expected": 99},
    ]
    text, structured = run(srv._handle_run_dax_tests({"tests": tests}))
    check("1 of 2 passed", structured["passed"] == 1 and structured["total"] == 2, str(structured))
    check("overall FAIL", structured["all_passed"] is False)
    check("text shows verdict", text.startswith("[FAIL]"), text[:40])


if __name__ == "__main__":
    print("=" * 70)
    print("  SEMANTICOPS-PARITY EXTRAS TESTS")
    print("=" * 70)
    test_dax_verdict()
    test_audit_chain()
    test_audit_chain_delete_detected()
    test_run_dax_tests()
    print("\n" + "=" * 70)
    if _failures:
        print(f"  {len(_failures)} CHECK(S) FAILED: {', '.join(_failures)}")
        sys.exit(1)
    print("  ALL EXTRAS CHECKS PASSED")
    print("=" * 70)
