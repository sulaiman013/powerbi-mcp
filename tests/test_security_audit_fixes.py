"""
Regression tests for the security/UAT audit fixes (2026-06): secret redaction, audit-chain
tamper detection + HMAC, error scrubbing, ReDoS-safe reference regex, PII summary leak, and the
status_pill empty-band guard. No Power BI required. Run: python tests/test_security_audit_fixes.py
"""
import json
import os
import sys
import tempfile
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

_failures = []


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f": {detail}" if detail and not cond else ""))
    if not cond:
        _failures.append(name)


def test_redact_secrets():
    print("\n== SEC-SECRET-1: connection-string secret is redacted ==")
    import server
    leaked = "DAX query failed: Provider error; Data Source=...;Password=SuperSecret123;User ID=app:x"
    out = server.redact_secrets(leaked, ["SuperSecret123"])
    check("password value removed", "SuperSecret123" not in out, out)
    check("password key masked", "Password=***" in out, out)
    # empty/short secret must not corrupt output
    check("empty secret is a no-op", server.redact_secrets("hello world", [""]) == "hello world")


def test_audit_chain_tamper_and_scrub():
    print("\n== SEC-AUDIT-1/3/4: tamper detection + error scrubbing ==")
    from security.audit_logger import AuditLogger
    with tempfile.TemporaryDirectory() as tmp:
        al = AuditLogger(log_dir=tmp)
        al.log_query(query="EVALUATE Sales", result_count=1, duration_ms=5, success=True)
        al.log_query(query="EVALUATE Bad", result_count=0, duration_ms=5, success=False,
                     error_message="connect failed; Password=topsecret9;")
        check("clean chain verifies", al.verify_chain()["valid"] is True)
        # stored error must be scrubbed
        lines = [json.loads(l) for l in open(al.log_file, encoding="utf-8") if l.strip()]
        err = lines[-1]["result"]["error"]
        check("stored error scrubbed", err and "topsecret9" not in err and "Password=***" in err, str(err))
        # tamper: strip entry_hash from the first entry -> must be detected
        lines[0].pop("entry_hash", None)
        with open(al.log_file, "w", encoding="utf-8") as f:
            for ln in lines:
                f.write(json.dumps(ln) + "\n")
        v = al.verify_chain()
        check("stripped entry_hash detected as tampering", v["valid"] is False, str(v))


def test_hmac_keyed_chain():
    print("\n== SEC-AUDIT-2: HMAC keying when POWERBI_MCP_AUDIT_KEY is set ==")
    from security.audit_logger import AuditLogger
    os.environ["POWERBI_MCP_AUDIT_KEY"] = "unit-test-key"
    try:
        with tempfile.TemporaryDirectory() as tmp:
            al = AuditLogger(log_dir=tmp)
            al.log_query(query="EVALUATE X", result_count=1, duration_ms=1, success=True)
            line = [json.loads(l) for l in open(al.log_file, encoding="utf-8") if l.strip()][0]
            # recomputing with a plain SHA256 must NOT match the HMAC hash
            import hashlib
            ev = {k: v for k, v in line.items() if k != "entry_hash"}
            plain = hashlib.sha256(json.dumps(ev, sort_keys=True, default=str).encode()).hexdigest()
            check("hash is HMAC, not plain sha256", line["entry_hash"] != plain)
            check("keyed chain verifies", al.verify_chain()["valid"] is True)
    finally:
        del os.environ["POWERBI_MCP_AUDIT_KEY"]


def test_ref_regex_redos_safe():
    print("\n== SEC-REDOS-1: reference regex is linear + still correct ==")
    from security.access_policy import AccessPolicyEngine
    extract = AccessPolicyEngine.extract_references
    pathological = "A" * 20000 + " "  # long run, no closing bracket -> used to backtrack
    t0 = time.time()
    extract("EVALUATE FILTER(" + pathological + ", TRUE())")
    elapsed = time.time() - t0
    check("no catastrophic backtracking", elapsed < 1.0, f"{elapsed:.2f}s")
    # quoted-table reference still extracted
    tables, columns = extract("EVALUATE 'Sales Data'[Amount] + Sales[Qty]")
    check("quoted + bare tables extracted", "Sales Data" in tables and "Sales" in tables, str(tables))
    check("columns extracted", "Amount" in columns and "Qty" in columns, str(columns))


def test_pii_summary_no_raw():
    print("\n== SEC-PII-1: PII detection summary never carries the raw value ==")
    from security.pii_detector import PIIDetector
    det = PIIDetector()
    _, summary = det.process_results([{"Customer[Email]": "john.doe@example.com"}])
    raw_present = any("original" in d or "john.doe@example.com" in json.dumps(d) for d in summary.get("detections", []))
    check("no raw PII in detections", not raw_present, str(summary.get("detections")))


def test_status_pill_no_numeric_band():
    print("\n== UAT-SVG-1: status_pill with no numeric band emits valid DAX ==")
    import svg_measures
    dax = svg_measures.generate("status_pill", value_measure="M",
                                thresholds=[{"max": None, "color": "#3FA34D", "label": "OK"}])["dax"]
    check("no malformed bare SWITCH", "SWITCH( TRUE()" not in dax, dax)
    check("balanced string literals", dax.count('"') % 2 == 0)


if __name__ == "__main__":
    print("=" * 70)
    print("  SECURITY / UAT FIX REGRESSION TESTS")
    print("=" * 70)
    test_redact_secrets()
    test_audit_chain_tamper_and_scrub()
    test_hmac_keyed_chain()
    test_ref_regex_redos_safe()
    test_pii_summary_no_raw()
    test_status_pill_no_numeric_band()
    print("\n" + "=" * 70)
    if _failures:
        print(f"  {len(_failures)} CHECK(S) FAILED: {', '.join(_failures)}")
        sys.exit(1)
    print("  ALL SECURITY/UAT FIX CHECKS PASSED")
    print("=" * 70)
