"""
Regression tests for the access-policy enforcement fix.

Before the fix, column-level policies silently no-opped at runtime because:
  - process_results / apply_to_results received no table context, and
  - the fallback matched columns by exact dict key ("table[col]") instead of
    parsing DAX result keys of the form Table[Column], and
  - wildcard ('*') column policies were never consulted.

These tests assert that BLOCK / MASK / HASH / REDACT actually fire against
realistic DAX result column keys, that the '*' wildcard table is honored, and
that pre-query checks block queries referencing blocked columns.

Run: python test_security_enforcement.py   (pure Python, no Power BI required)
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from security.access_policy import (  # noqa: E402
    AccessPolicyEngine,
    ColumnPolicy,
    PolicyAction,
    TablePolicy,
    parse_column_key,
)
from security.security_layer import SecurityLayer  # noqa: E402

CONFIG = os.path.join(os.path.dirname(__file__), "config", "policies.yaml")

_failures = []


def check(name, cond, detail=""):
    status = "PASS" if cond else "FAIL"
    print(f"  [{status}] {name}" + (f": {detail}" if detail and not cond else ""))
    if not cond:
        _failures.append(name)


def test_parse_column_key():
    print("\n== parse_column_key ==")
    check("qualified", parse_column_key("Sales[Amount]") == ("Sales", "Amount"))
    check("quoted table", parse_column_key("'Sales Data'[Amt]") == ("Sales Data", "Amt"))
    check("measure", parse_column_key("[Total Sales]") == (None, "Total Sales"))
    check("bare", parse_column_key("Amount") == (None, "Amount"))


def test_extract_references():
    print("\n== extract_references ==")
    tables, columns = AccessPolicyEngine.extract_references(
        "EVALUATE FILTER('Customers', 'Customers'[ssn] <> \"\") ORDER BY [Total Sales]"
    )
    check("tables found", "Customers" in tables, str(tables))
    check("columns found", "ssn" in columns, str(columns))
    check("measure found", "Total Sales" in columns, str(columns))


def test_apply_to_results_shipped_config():
    print("\n== apply_to_results (shipped policies.yaml, wildcard '*' table) ==")
    engine = AccessPolicyEngine(config_path=CONFIG)
    check("wildcard table loaded", "*" in engine.table_policies, str(list(engine.table_policies)))

    rows = [
        {
            "Customers[ssn]": "123-45-6789",
            "Customers[card_number]": "4111111111111234",
            "Customers[password]": "hunter2",
            "Customers[CustomerName]": "Jane Doe",
        }
    ]
    processed, report = engine.apply_to_results(rows)
    out = processed[0]

    check("ssn BLOCKED (None)", out["Customers[ssn]"] is None, repr(out["Customers[ssn]"]))
    check("password BLOCKED (None)", out["Customers[password]"] is None, repr(out["Customers[password]"]))
    check(
        "card_number MASKED (last4 kept)",
        out["Customers[card_number]"] != "4111111111111234" and str(out["Customers[card_number]"]).endswith("1234"),
        repr(out["Customers[card_number]"]),
    )
    check("name ALLOWED (unchanged)", out["Customers[CustomerName]"] == "Jane Doe", repr(out["Customers[CustomerName]"]))
    check("report flags blocked", "Customers[ssn]" in report.get("blocked_columns", []), str(report))


def test_hash_and_redact_actions():
    print("\n== HASH / REDACT actions fire ==")
    engine = AccessPolicyEngine()
    wildcard = TablePolicy(name="*")
    wildcard.columns["email"] = ColumnPolicy(name="email", action=PolicyAction.HASH)
    wildcard.columns["notes"] = ColumnPolicy(name="notes", action=PolicyAction.REDACT)
    engine.table_policies["*"] = wildcard

    rows = [{"Users[Email]": "a@b.com", "Users[Notes]": "secret note", "Users[Id]": 7}]
    out = engine.apply_to_results(rows)[0][0]
    check("email HASHED", str(out["Users[Email]"]).startswith("[HASH:"), repr(out["Users[Email]"]))
    check("notes REDACTED", out["Users[Notes]"] == "[REDACTED]", repr(out["Users[Notes]"]))
    check("id untouched", out["Users[Id]"] == 7, repr(out["Users[Id]"]))


def test_pre_query_check_blocks():
    print("\n== check_query blocks queries referencing blocked columns ==")
    engine = AccessPolicyEngine(config_path=CONFIG)
    tables, columns = AccessPolicyEngine.extract_references(
        "EVALUATE FILTER(Customers, Customers[ssn] <> BLANK())"
    )
    res = engine.check_query("EVALUATE FILTER(Customers, Customers[ssn] <> BLANK())", tables, columns)
    check("blocked query not allowed", res.allowed is False, res.reason)

    t2, c2 = AccessPolicyEngine.extract_references("EVALUATE Sales")
    res2 = engine.check_query("EVALUATE Sales", t2, c2)
    check("safe query allowed", res2.allowed is True, res2.reason)


def test_security_layer_end_to_end():
    print("\n== SecurityLayer.process_results end-to-end (audit off) ==")
    sec = SecurityLayer(config_path=CONFIG, enable_audit=False)
    rows = [{"Customers[ssn]": "111-22-3333", "Customers[City]": "Austin"}]
    safe, report = sec.process_results(results=rows, query="EVALUATE Customers", source="desktop")
    check("ssn blocked through layer", safe[0]["Customers[ssn]"] is None, repr(safe[0]["Customers[ssn]"]))
    check("city preserved", safe[0]["Customers[City]"] == "Austin", repr(safe[0]["Customers[City]"]))


if __name__ == "__main__":
    print("=" * 70)
    print("  ACCESS-POLICY ENFORCEMENT REGRESSION TESTS")
    print("=" * 70)
    test_parse_column_key()
    test_extract_references()
    test_apply_to_results_shipped_config()
    test_hash_and_redact_actions()
    test_pre_query_check_blocks()
    test_security_layer_end_to_end()

    print("\n" + "=" * 70)
    if _failures:
        print(f"  {len(_failures)} CHECK(S) FAILED: {', '.join(_failures)}")
        sys.exit(1)
    print("  ALL SECURITY ENFORCEMENT CHECKS PASSED")
    print("=" * 70)
