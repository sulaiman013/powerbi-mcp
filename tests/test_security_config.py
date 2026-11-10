"""
Configuration handling of the security layer: it fails closed and visibly. A policy file that
is missing, unparseable or contains a single invalid rule locks the layer (queries refused
with a reason naming the file and the error, status says so); no file at all stays permissive
and says that no policies are loaded. Also: env_flag, the pii and audit sections being
honoured, and the shipped policies.yaml matching the spellings real models use.
No Power BI required. Run: python tests/test_security_config.py
"""
import logging
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))
from security import (  # noqa: E402
    AccessPolicyEngine,
    PolicyConfigError,
    SecurityLayer,
    env_flag,
)
from security.pii_detector import MaskingStrategy, PIIType  # noqa: E402

logging.disable(logging.CRITICAL)  # every "locked" case logs an error by design

# The checks below assert where the audit log goes when only the policy file says so. A
# developer who has these set in their shell must not get a false failure, so start clean; the
# tests that exercise the override set the variable themselves.
for _var in ("POWERBI_MCP_AUDIT_DIR", "POWERBI_MCP_AUDIT_KEY", "POWERBI_MCP_HASH_KEY", "POWERBI_MCP_POLICY_FILE"):
    os.environ.pop(_var, None)

CONFIG = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config", "policies.yaml")
VALID = """
global:
  enabled: true
  max_rows_per_query: 500
tables:
  - name: "*"
    columns:
      - name: ssn
        action: block
"""

_failures = []


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f": {detail}" if detail and not cond else ""))
    if not cond:
        _failures.append(name)


def eq(name, actual, expected):
    check(name, actual == expected, f"got {actual!r}, expected {expected!r}")


def write(folder, text, name="policies.yaml"):
    path = os.path.join(folder, name)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)
    return path


def test_valid_file_loads():
    print("\n== a valid file loads ==")
    with tempfile.TemporaryDirectory() as tmp:
        sec = SecurityLayer(config_path=write(tmp, VALID), enable_audit=False)
        check("not locked", sec.locked is False, str(sec.config_error))
        check("policy enforced", sec.pre_query_check("EVALUATE VALUES(C[ssn])").allowed is False)
        check("harmless query allowed", sec.pre_query_check("EVALUATE Sales").allowed is True)
        status = sec.get_status()
        check("status: loaded, not locked", status["policies"]["loaded"] and not status["locked"], str(status))
        eq("summary carries the row cap", sec.get_policy_summary()["max_rows"], 500)


def test_broken_files_lock():
    print("\n== a broken or missing file locks the layer ==")
    broken = {
        "YAML syntax error": VALID + "\n   oops: [unclosed\n",
        "unknown action": VALID.replace("action: block", "action: blocked"),
        "misspelt key in a column rule": VALID.replace("action: block", "acton: block"),
        "misspelt top-level key": VALID.replace("tables:", "table:"),
        "misspelt key in global": VALID.replace("enabled: true", "enabeld: true"),
        "misspelt key in a table": VALID.replace("    columns:", "    colums:"),
        "empty file": "# everything commented out\n",
        "a list instead of a mapping": "- a\n- b\n",
        "rule without an action": VALID.replace("        action: block\n", ""),
        "rule with an empty name": VALID.replace("name: ssn", 'name: ""'),
        "rule name with no letters": VALID.replace("name: ssn", 'name: "---"'),
        "wildcard in a word rule": VALID.replace("name: ssn", 'name: "ssn*"\n        match: word'),
        "unknown match mode": VALID.replace("name: ssn", "name: ssn\n        match: regex"),
        "invalid blocked pattern": VALID.replace("enabled: true", 'enabled: true\n  blocked_patterns: ["(unclosed"]'),
        "non-boolean enabled": VALID.replace("enabled: true", "enabled: maybe"),
        "negative row cap": VALID.replace("max_rows_per_query: 500", "max_rows_per_query: -1"),
        "duplicate table": VALID + '  - name: "*"\n',
        "unknown PII type": VALID + "pii:\n  enabled_types: [ssn, shoe_size]\n",
        "unknown PII strategy": VALID + "pii:\n  strategies:\n    email: scramble\n",
        "unknown audit key": VALID + "audit:\n  log_directory: ./x\n",
        "unknown store_query_text": VALID + "audit:\n  store_query_text: everything\n",
        "unsupported pii_default_action": VALID.replace("enabled: true", "enabled: true\n  pii_default_action: numeric_mask"),
    }
    with tempfile.TemporaryDirectory() as tmp:
        for label, text in broken.items():
            path = write(tmp, text)
            sec = SecurityLayer(config_path=path, enable_audit=False)
            res = sec.pre_query_check("EVALUATE Sales")
            ok = sec.locked and res.allowed is False and res.locked and path in res.reason
            check(f"{label}: locked, query refused, reason names the file", ok, f"{sec.config_error!r} / {res.reason!r}")

        path = write(tmp, VALID.replace("action: block", "action: blocked"))
        sec = SecurityLayer(config_path=path, enable_audit=False)
        check("the reason names the bad value and where it is",
              "blocked" in sec.config_error and "ssn" in sec.config_error and "action" in sec.config_error, sec.config_error)
        status = sec.get_status()
        check("status reports the lock and the error", status["locked"] is True and path in status["config_error"]
              and status["policies"]["message"].startswith("LOCKED"), str(status))
        check("policy summary reports it too", sec.get_policy_summary()["locked"] is True)
        rows, report = sec.process_results([{"Sales[Amount]": 5, "[x]": "y"}], query="EVALUATE Sales")
        eq("process_results returns no data while locked", rows, [{"Sales[Amount]": None, "[x]": None}])
        check("... and says why", report["query_denied"] and report["locked"] and path in report["denied_reason"], str(report))
        rows, report = sec.process_results([{"Sales[Amount]": 5}])
        eq("... even without a query", rows, [{"Sales[Amount]": None}])

        missing = os.path.join(tmp, "does-not-exist.yaml")
        sec = SecurityLayer(config_path=missing, enable_audit=False)
        res = sec.pre_query_check("EVALUATE Sales")
        check("a configured but missing file locks", sec.locked and not res.allowed and missing in res.reason
              and "does not exist" in res.reason, res.reason)

        engine = AccessPolicyEngine(config_path=missing)
        check("the engine on its own locks as well", engine.locked and not engine.check_query("EVALUATE Sales").allowed)
        rows, report = engine.apply_to_results([{"T[c]": 1}])
        eq("... and nulls results", (rows, report["denied"]), ([{"T[c]": None}], True))
        check("load_from_file reports failure", AccessPolicyEngine().load_from_file(missing) is False)

        engine = AccessPolicyEngine()
        try:
            engine.load_from_dict({"tables": [{"name": "T", "columns": [{"name": "c", "action": "nope"}]}]})
            check("load_from_dict raises on an invalid rule", False, "no exception")
        except PolicyConfigError as e:
            check("load_from_dict raises on an invalid rule", "nope" in str(e), str(e))
        check("... and still leaves the engine locked", engine.locked)

        good_then_bad = AccessPolicyEngine(config_path=write(tmp, VALID, "good.yaml"))
        check("a good engine is not locked", not good_then_bad.locked)
        good_then_bad.load_from_file(write(tmp, "nonsense: true\n", "bad.yaml"))
        check("reloading a bad file locks a previously good engine", good_then_bad.locked)


def test_permissive_without_a_file():
    print("\n== no file configured: permissive, and the status says so ==")
    sec = SecurityLayer(config_path=None, enable_audit=False)
    check("not locked", sec.locked is False)
    check("queries allowed", sec.pre_query_check("EVALUATE VALUES(C[ssn])").allowed is True)
    status = sec.get_status()
    check("status says no policies are loaded", status["policies"]["loaded"] is False
          and "no column or table policies are loaded" in status["policies"]["message"], str(status["policies"]))
    eq("summary agrees", sec.get_policy_summary()["loaded"], False)

    with tempfile.TemporaryDirectory() as tmp:
        path = write(tmp, "nonsense: true\n")
        sec = SecurityLayer(config_path=path, enable_audit=False, enable_policies=False)
        check("policies switched off: a broken file does not lock", sec.locked is False
              and sec.pre_query_check("EVALUATE Sales").allowed is True)
        status = sec.get_status()
        check("... but the status still reports the error", status["config_error"] and path in status["config_error"]
              and "Configuration error" in status["policies"]["message"], str(status))
        check("... and PII masking stays on with its defaults", sec.pii_detector is not None
              and PIIType.SSN in sec.pii_detector.enabled_types)

        disabled = write(tmp, "global:\n  enabled: false\n", "off.yaml")
        sec = SecurityLayer(config_path=disabled, enable_audit=False)
        check("'enabled: false' is the explicit way to run without policies",
              not sec.locked and sec.pre_query_check("EVALUATE VALUES(C[ssn])").allowed)

    saved = os.environ.pop("POWERBI_MCP_HASH_KEY", None)
    os.environ["POWERBI_MCP_HASH_KEY"] = "tooshort"
    try:
        sec = SecurityLayer(config_path=None, enable_audit=False)
        check("an unusable hash key locks the layer even without a policy file",
              sec.locked and "POWERBI_MCP_HASH_KEY" in sec.pre_query_check("EVALUATE Sales").reason)
        sec = SecurityLayer(config_path=None, enable_audit=False, enable_policies=False)
        check("... and is reported in the status when policies are off",
              not sec.locked and "POWERBI_MCP_HASH_KEY" in str(sec.get_status()["config_error"]))
    finally:
        os.environ.pop("POWERBI_MCP_HASH_KEY", None)
        if saved is not None:
            os.environ["POWERBI_MCP_HASH_KEY"] = saved


def test_env_flag():
    print("\n== env_flag ==")
    name = "POWERBI_MCP_TEST_FLAG"
    saved = os.environ.pop(name, None)
    try:
        eq("unset: default False", env_flag(name), False)
        eq("unset: default True", env_flag(name, True), True)
        for raw in ("1", "true", "TRUE", "True", "yes", "YES", "on", "On", " true ", "\ttrue\n"):
            os.environ[name] = raw
            eq(f"{raw!r} is true", env_flag(name, False), True)
        for raw in ("0", "false", "FALSE", "no", "No", "off", "OFF", " false "):
            os.environ[name] = raw
            eq(f"{raw!r} is false", env_flag(name, True), False)
        for raw in ("", "   "):
            os.environ[name] = raw
            eq(f"blank {raw!r} gives the default", (env_flag(name, True), env_flag(name, False)), (True, False))
        for raw in ("ture", "enabled", "2", "y"):
            os.environ[name] = raw
            try:
                env_flag(name, True)
                check(f"{raw!r} is refused, not guessed", False, "no exception")
            except ValueError as e:
                check(f"{raw!r} is refused, not guessed", name in str(e), str(e))
            eq(f"{raw!r} with an explicit fallback", (env_flag(name, False, invalid=True),
                                                     env_flag(name, True, invalid=False)), (True, False))
    finally:
        if saved is None:
            os.environ.pop(name, None)
        else:
            os.environ[name] = saved


def test_pii_and_audit_sections_are_honoured():
    print("\n== pii and audit sections ==")
    with tempfile.TemporaryDirectory() as tmp:
        log_dir = os.path.join(tmp, "audit here")
        text = VALID + f"""
pii:
  enabled_types: [email, phone]
  default_strategy: full
  strategies:
    email: redact
  max_scan_chars: 2000
audit:
  log_dir: {log_dir!r}
  log_file: trail.log
  max_file_size_mb: 0.5
  backup_count: 2
  store_query_text: fingerprint
  required: true
"""
        sec = SecurityLayer(config_path=write(tmp, text), enable_audit=True)
        check("loads", not sec.locked, str(sec.config_error))
        det = sec.pii_detector
        eq("enabled types", det.enabled_types, [PIIType.EMAIL, PIIType.PHONE])
        eq("default strategy", det.default_strategy, MaskingStrategy.FULL)
        eq("per-type strategy", det.type_strategies, {PIIType.EMAIL: MaskingStrategy.REDACT})
        eq("scan limit", det.max_scan_chars, 2000)
        rows, report = sec.process_results(
            [{"C[Email]": "jane@example.com", "C[Phone]": "555-123-4567", "C[SSN]": "123-45-6789",
              "C[Note]": "ip 10.1.2.3 mail bob@example.org"}], query="EVALUATE C", tool="unit_test")
        eq("strategies applied; disabled types left alone", rows[0], {
            "C[Email]": "[REDACTED-EMAIL]", "C[Phone]": "**********", "C[SSN]": None,
            "C[Note]": "ip 10.1.2.3 mail [REDACTED-EMAIL]"})
        status = sec.get_status()
        eq("status lists the types", status["pii_detector"]["enabled_types"], ["email", "phone"])

        audit = sec.audit_logger
        eq("audit folder", str(audit.log_dir), log_dir)
        eq("audit file", audit.log_file.name, "trail.log")
        eq("rotation size", audit.max_file_size, int(0.5 * 1024 * 1024))
        eq("backup count", audit.backup_count, 2)
        eq("query text mode", audit.store_query_text, "fingerprint")
        eq("required", audit.required, True)
        check("the entry went to the configured file", os.path.isfile(os.path.join(log_dir, "trail.log")))
        eq("status shows the audit log is healthy", status["audit"]["healthy"], True)

        elsewhere = os.path.join(tmp, "from-env")
        saved = os.environ.pop("POWERBI_MCP_AUDIT_DIR", None)
        os.environ["POWERBI_MCP_AUDIT_DIR"] = elsewhere
        try:
            moved = SecurityLayer(config_path=write(tmp, text), enable_audit=True)
            eq("POWERBI_MCP_AUDIT_DIR overrides the folder", str(moved.audit_logger.log_dir), elsewhere)
            eq("... and keeps the other audit settings", moved.audit_logger.log_file.name, "trail.log")
            bare = SecurityLayer(config_path=None, enable_audit=True)
            eq("... also when there is no policy file", str(bare.audit_logger.log_dir), elsewhere)
        finally:
            os.environ.pop("POWERBI_MCP_AUDIT_DIR", None)
            if saved is not None:
                os.environ["POWERBI_MCP_AUDIT_DIR"] = saved

        off = write(tmp, VALID.replace("enabled: true", "enabled: true\n  enable_pii_detection: false"), "nopii.yaml")
        sec = SecurityLayer(config_path=off, enable_audit=False)
        check("global.enable_pii_detection: false switches the detector off",
              sec.pii_detector is None and sec.get_status()["enabled"]["pii_detection"] is False)
        legacy = write(tmp, VALID.replace("enabled: true", "enabled: true\n  pii_default_action: redact"), "legacy.yaml")
        eq("legacy global.pii_default_action maps to a strategy",
           SecurityLayer(config_path=legacy, enable_audit=False).pii_detector.default_strategy, MaskingStrategy.REDACT)


def test_shipped_config():
    print("\n== shipped config/policies.yaml ==")
    sec = SecurityLayer(config_path=CONFIG, enable_audit=False)
    check("loads without locking", not sec.locked, str(sec.config_error))
    engine = sec.policy_engine

    def action(column):
        return engine.resolve_column_policy("AnyTable", column).action.value

    for column in ("ssn", "SSN", "SSN Number", "ssn_number", "CustomerSSN", "Customer SSN Hash",
                   "Social Security Number", "social_security_number", "SocialSecurityNo",
                   "password", "Password", "PasswordHash", "password_hash", "User Password", "passwordsalt", "Pwd",
                   "API Key", "api_key", "ApiKey", "APIKey", "apikey", "Client Secret", "client_secret", "secret",
                   "token", "Token", "AccessToken", "access_token", "Refresh Token", "API Token", "AuthToken"):
        eq(f"blocked: {column}", action(column), "block")
    for column in ("Card Number", "card_number", "CardNumber", "Credit Card Number", "credit_card_number",
                   "CreditCard", "cardnumber", "Customer Card Number"):
        eq(f"masked: {column}", action(column), "mask")
    for column in ("Description", "City", "Business Unit", "Product Name", "Name", "Category Name", "ClassName",
                   "Token Count", "Tokens Used", "Card Type", "Discard Reason", "Secretary", "Keyboard", "Amount",
                   "Order Number", "Passport Country", "Assn Code", "Mission"):
        eq(f"untouched: {column}", action(column), "allow")

    eq("PII types from the file", [t.value for t in sec.pii_detector.enabled_types],
       ["ssn", "credit_card", "email", "phone", "ip_address"])
    rows, report = sec.process_results([{
        "Product[Description]": "Blue cotton t-shirt", "Product[Product Name]": "Widget Pro",
        "Sales[Business Unit]": "Retail North", "Shipping[Region]": "EMEA", "Hotel[City]": "Lisbon",
        "Geo[Zip]": "78701", "Sales[OrderDate]": "2024-03-15", "Sales[OrderNo]": "4500012345",
        "[Count]": "42 items", "[Day]": "Monday", "Customer[Name]": "Jane Doe"}], query="EVALUATE Sales")
    eq("ordinary columns come back untouched", report["pii_count"], 0)
    eq("... every one of them", rows[0]["Product[Description]"] + rows[0]["Sales[OrderDate]"] + rows[0]["Sales[OrderNo]"],
       "Blue cotton t-shirt2024-03-154500012345")
    eq("row cap", sec.get_policy_summary()["max_rows"], 1000)


if __name__ == "__main__":
    for fn in (test_valid_file_loads, test_broken_files_lock, test_permissive_without_a_file, test_env_flag,
               test_pii_and_audit_sections_are_honoured, test_shipped_config):
        fn()
    print()
    if _failures:
        print(f"FAILED: {len(_failures)} check(s): {_failures}")
        sys.exit(1)
    print("ALL SECURITY CONFIG TESTS PASSED")
