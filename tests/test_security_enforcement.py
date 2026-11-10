"""
Access-policy enforcement on query results (src/security/access_policy.py and the layer on
top of it): every action fires on realistic DAX result keys, the '*' table is honoured, an
aliased or computed column is treated like the protected column it came from, HASH is keyed,
NUMERIC_MASK is not a near-identity, and the layer's process_results path is correct for the
ways the server calls it.

Run: python tests/test_security_enforcement.py   (pure Python, no Power BI required)
"""
import hashlib
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

from security.access_policy import (  # noqa: E402
    AccessPolicyEngine,
    ColumnPolicy,
    PolicyAction,
    TablePolicy,
    parse_column_key,
)
from security.security_layer import SecurityLayer  # noqa: E402

CONFIG = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config", "policies.yaml")

_failures = []


def check(name, cond, detail=""):
    status = "PASS" if cond else "FAIL"
    print(f"  [{status}] {name}" + (f": {detail}" if detail and not cond else ""))
    if not cond:
        _failures.append(name)


def eq(name, actual, expected):
    check(name, actual == expected, f"got {actual!r}, expected {expected!r}")


def engine_with(**rules):
    """An engine whose '*' table has one column rule per keyword: name=PolicyAction."""
    engine = AccessPolicyEngine()
    wildcard = TablePolicy(name="*")
    for name, action in rules.items():
        wildcard.columns[name] = ColumnPolicy(name=name, action=action)
    engine.table_policies["*"] = wildcard
    return engine


def test_parse_column_key():
    print("\n== parse_column_key ==")
    eq("qualified", parse_column_key("Sales[Amount]"), ("Sales", "Amount"))
    eq("quoted table", parse_column_key("'Sales Data'[Amt]"), ("Sales Data", "Amt"))
    eq("quoted table with an apostrophe", parse_column_key("'O''Brien'[Amt]"), ("O'Brien", "Amt"))
    eq("measure", parse_column_key("[Total Sales]"), (None, "Total Sales"))
    eq("bare", parse_column_key("Amount"), (None, "Amount"))
    eq("a computed column named like a model column is still computed",
       parse_column_key("[Customers[City]]"), (None, "Customers[City]"))
    eq("non-string key", parse_column_key(7), (None, "7"))


def test_extract_references():
    print("\n== extract_references ==")
    tables, columns = AccessPolicyEngine.extract_references(
        "EVALUATE FILTER('Customers', 'Customers'[ssn] <> \"\") ORDER BY [Total Sales]"
    )
    check("tables found", "Customers" in tables, str(tables))
    check("columns found", "ssn" in columns, str(columns))
    check("measure found", "Total Sales" in columns, str(columns))
    tables, columns = AccessPolicyEngine.extract_references(
        'EVALUATE ROW("x", "Hidden[ssn]") -- Other[password]\n/* Third[token] */')
    eq("names inside strings and comments are not references", (tables, columns), ([], []))
    tables, _ = AccessPolicyEngine.extract_references("EVALUATE Customers")
    eq("a table used on its own is found", tables, ["Customers"])
    eq("empty query", AccessPolicyEngine.extract_references(""), ([], []))


def test_apply_to_results_shipped_config():
    print("\n== apply_to_results (shipped policies.yaml, wildcard '*' table) ==")
    engine = AccessPolicyEngine(config_path=CONFIG)
    check("shipped config loads without locking", not engine.locked, str(engine.config_error))
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
    eq("card_number MASKED (last4 kept)", out["Customers[card_number]"], "************1234")
    check("name ALLOWED (unchanged)", out["Customers[CustomerName]"] == "Jane Doe", repr(out["Customers[CustomerName]"]))
    check("report flags blocked", "Customers[ssn]" in report.get("blocked_columns", []), str(report))
    eq("report lists masked", report.get("masked_columns"), ["Customers[card_number]"])
    eq("input rows are not mutated", rows[0]["Customers[ssn]"], "123-45-6789")


def test_hash_and_redact_actions():
    print("\n== HASH / REDACT actions fire ==")
    engine = engine_with(email=PolicyAction.HASH, notes=PolicyAction.REDACT)

    rows = [{"Users[Email]": "a@b.com", "Users[Notes]": "secret note", "Users[Id]": 7},
            {"Users[Email]": "a@b.com", "Users[Notes]": None, "Users[Id]": 8},
            {"Users[Email]": None, "Users[Notes]": "x", "Users[Id]": 9}]
    out = engine.apply_to_results(rows)[0]
    check("email HASHED", str(out[0]["Users[Email]"]).startswith("[HASH:"), repr(out[0]["Users[Email]"]))
    check("equal values hash equal", out[0]["Users[Email]"] == out[1]["Users[Email]"])
    check("a null stays null under HASH", out[2]["Users[Email]"] is None)
    check("notes REDACTED", out[0]["Users[Notes]"] == "[REDACTED]", repr(out[0]["Users[Notes]"]))
    check("id untouched", out[0]["Users[Id]"] == 7, repr(out[0]["Users[Id]"]))


def test_hash_is_keyed():
    print("\n== HASH is HMAC-keyed (POWERBI_MCP_HASH_KEY, else a per-session key) ==")
    saved = os.environ.pop("POWERBI_MCP_HASH_KEY", None)
    try:
        def hashed(value):
            return engine_with(dob=PolicyAction.HASH).apply_to_results([{"U[dob]": value}])[0][0]["U[dob]"]

        session = hashed("1984-07-23")
        plain = hashlib.sha256(b"1984-07-23").hexdigest()
        check("not a plain SHA-256 of the value", plain[:12] not in session and plain[:16] not in session, session)
        eq("stable within one process", hashed("1984-07-23"), session)
        check("status says the key is per-session",
              engine_with(dob=PolicyAction.HASH).hash_key_persistent is False)

        os.environ["POWERBI_MCP_HASH_KEY"] = "first-key-0123456789"
        first = hashed("1984-07-23")
        eq("stable under a configured key", hashed("1984-07-23"), first)
        check("configured key differs from the session key", first != session)
        check("engine reports a persistent key", engine_with(dob=PolicyAction.HASH).hash_key_persistent is True)
        os.environ["POWERBI_MCP_HASH_KEY"] = "second-key-0123456789"
        check("a different key gives a different hash", hashed("1984-07-23") != first)

        os.environ["POWERBI_MCP_HASH_KEY"] = "short"
        weak = AccessPolicyEngine()
        check("a key too short to resist guessing locks the engine", weak.locked, str(weak.config_error))
        check("... and the reason names the variable", "POWERBI_MCP_HASH_KEY" in str(weak.config_error))
        check("... and queries are refused", weak.check_query("EVALUATE Sales").allowed is False)
    finally:
        if saved is None:
            os.environ.pop("POWERBI_MCP_HASH_KEY", None)
        else:
            os.environ["POWERBI_MCP_HASH_KEY"] = saved


def test_numeric_mask():
    print("\n== NUMERIC_MASK (session-randomized, stats-preserving) ==")
    engine = engine_with(revenue=PolicyAction.NUMERIC_MASK)
    engine.numeric_coefficient = 2.0  # deterministic for the test

    rows = [{"Sales[Revenue]": 100, "Sales[City]": "Austin"},
            {"Sales[Revenue]": 50, "Sales[City]": "Dallas"}]
    out = engine.apply_to_results(rows)[0]
    check("revenue scaled by coefficient", out[0]["Sales[Revenue]"] == 200, repr(out[0]["Sales[Revenue]"]))
    check("ratios preserved (100:50 -> 200:100)", out[1]["Sales[Revenue]"] == 100, repr(out[1]["Sales[Revenue]"]))
    check("non-numeric untouched", out[0]["Sales[City]"] == "Austin")

    out = engine.apply_to_results([{"Sales[Revenue]": v} for v in (1.5, None, "1234.50", "250", "about 9000", True)])[0]
    eq("float scaled", out[0]["Sales[Revenue]"], 3.0)
    eq("null stays null", out[1]["Sales[Revenue]"], None)
    eq("a fixed-decimal value delivered as text is scaled", out[2]["Sales[Revenue]"], "2469.0")
    eq("an integer delivered as text is scaled", out[3]["Sales[Revenue]"], "500")
    eq("text that is not a number is redacted, not passed through", out[4]["Sales[Revenue]"], "[REDACTED]")
    eq("a boolean is redacted", out[5]["Sales[Revenue]"], "[REDACTED]")

    coefficients = [AccessPolicyEngine().numeric_coefficient for _ in range(400)]
    check("the factor is never close to 1.0",
          all(0.25 <= c <= 0.75 or 1.5 <= c <= 4.0 for c in coefficients),
          str([c for c in coefficients if not (0.25 <= c <= 0.75 or 1.5 <= c <= 4.0)][:3]))
    check("both ranges are used", any(c < 1 for c in coefficients) and any(c > 1 for c in coefficients))
    check("the factor is not drawn from a short list", len(set(coefficients)) == len(coefficients))


def test_pre_query_check_blocks():
    print("\n== check_query blocks queries referencing blocked columns ==")
    engine = AccessPolicyEngine(config_path=CONFIG)
    tables, columns = AccessPolicyEngine.extract_references(
        "EVALUATE FILTER(Customers, Customers[ssn] <> BLANK())"
    )
    res = engine.check_query("EVALUATE FILTER(Customers, Customers[ssn] <> BLANK())", tables, columns)
    check("blocked query not allowed", res.allowed is False, res.reason)
    eq("the reason is the rule's reason", res.reason, "Social Security Numbers are never exposed")

    t2, c2 = AccessPolicyEngine.extract_references("EVALUATE Sales")
    res2 = engine.check_query("EVALUATE Sales", t2, c2)
    check("safe query allowed", res2.allowed is True, res2.reason)
    eq("row cap from the shipped config", res2.max_rows, 1000)

    check("unqualified [ssn] is blocked too",
          engine.check_query("EVALUATE SELECTCOLUMNS(Customers, \"x\", [ssn])").allowed is False)
    check("a blocked name inside a string or comment does not block",
          engine.check_query('EVALUATE ROW("x", "Customers[ssn]") -- Customers[password]').allowed is True)
    check("no query text: the supplied column list is checked",
          engine.check_query("", tables=["Customers"], columns=["ssn"]).allowed is False)
    check("no query text, harmless columns", engine.check_query("", tables=["Sales"], columns=["Amount"]).allowed)


def test_alias_gets_the_referenced_action():
    print("\n== a computed column is treated like the protected column it was computed from ==")
    engine = AccessPolicyEngine(config_path=CONFIG)
    query = 'EVALUATE SELECTCOLUMNS(Customers, "x", Customers[card_number], "c", Customers[City])'
    res = engine.check_query(query)
    check("masked column may be referenced", res.allowed, res.reason)
    eq("derived action is the referenced column's action", res.derived_action, PolicyAction.MASK)
    out, report = engine.apply_to_results(
        [{"[x]": 4111111111111111, "[c]": "Austin", "Customers[City]": "Austin"}], query=query)
    eq("aliased card number (an integer) is masked", out[0]["[x]"], "************1111")
    eq("a plain model column in the same result keeps its own policy", out[0]["Customers[City]"], "Austin")
    check("every other computed column is masked as well", out[0]["[c]"] != "Austin", repr(out[0]["[c]"]))
    eq("report names the derived action", report["derived_action"], "mask")

    out, _ = engine.apply_to_results([{"[x]": 4111111111111111}])
    eq("without the query only names can be checked (documented)", out[0]["[x]"], 4111111111111111)

    out, _ = engine.apply_to_results([{"[Total]": 10}], query='EVALUATE ROW("Total", SUM(Sales[Amount]))')
    eq("a query that touches nothing protected is left alone", out[0]["[Total]"], 10)

    engine = engine_with(email=PolicyAction.HASH, notes=PolicyAction.REDACT, revenue=PolicyAction.NUMERIC_MASK)
    engine.numeric_coefficient = 2.0
    q = 'EVALUATE ADDCOLUMNS(Sales, "x", Sales[Revenue] * 1)'
    out, _ = engine.apply_to_results([{"Sales[Revenue]": 1000.0, "[x]": 1000.0}], query=q)
    eq("numeric mask: the alias is scaled by the same factor (no free recovery)", out[0], {
        "Sales[Revenue]": 2000.0, "[x]": 2000.0})
    out, _ = engine.apply_to_results([{"[e]": "a@b.com"}], query='EVALUATE ROW("e", MAX(Users[Email]))')
    check("hash: the alias is hashed", str(out[0]["[e]"]).startswith("[HASH:"), repr(out[0]["[e]"]))
    out, _ = engine.apply_to_results([{"[n]": "private", "[e]": "a@b.com"}],
                                     query='EVALUATE ROW("n", MAX(T[Notes]), "e", MAX(T[Email]))')
    eq("the most restrictive referenced action wins for computed columns", out[0], {
        "[n]": "[REDACTED]", "[e]": "[REDACTED]"})
    out, _ = engine.apply_to_results(
        [{"[Customers[City]]": "private"}], query='EVALUATE ROW("Customers[City]", MAX(T[Notes]))')
    eq("an alias dressed up as a model column is still computed", out[0]["[Customers[City]]"], "[REDACTED]")

    probe = 'EVALUATE ROW("hit", COUNTROWS(FILTER(Users, Users[Email] = "a@b.com")))'
    out, _ = engine.apply_to_results([{"[hit]": 1}], query=probe)
    check("a protected column used only in a filter still protects what is computed",
          str(out[0]["[hit]"]).startswith("[HASH:"), repr(out[0]["[hit]"]))


def test_most_restrictive_rule_wins():
    print("\n== rule resolution ==")
    engine = AccessPolicyEngine()
    engine.load_from_dict({"tables": [
        {"name": "*", "columns": [{"name": "email", "action": "mask"}, {"name": "*mail*", "action": "hash"}]},
        {"name": "Staff", "default_action": "redact", "columns": [
            {"name": "email", "action": "block"}, {"name": "Name", "action": "allow"}]},
    ]})
    eq("several matching rules: most restrictive", engine.resolve_column_policy("Users", "Email").action,
       PolicyAction.HASH)
    eq("table rule and wildcard rule: most restrictive", engine.resolve_column_policy("Staff", "Email").action,
       PolicyAction.BLOCK)
    eq("an explicit rule beats the table default", engine.resolve_column_policy("Staff", "Name").action,
       PolicyAction.ALLOW)
    eq("no rule: the table default", engine.resolve_column_policy("Staff", "Grade").action, PolicyAction.REDACT)
    eq("another table is not affected by that default", engine.resolve_column_policy("Users", "Grade").action,
       PolicyAction.ALLOW)
    eq("unknown table: explicit rules of every table, by name",
       engine.resolve_column_policy(None, "email").action, PolicyAction.BLOCK)
    eq("unknown table: table defaults do not apply", engine.resolve_column_policy(None, "Grade").action,
       PolicyAction.ALLOW)
    eq("get_column_action agrees", engine.get_column_action("staff", "EMAIL"), PolicyAction.BLOCK)
    eq("get_sensitive_columns lists the non-allow rules", sorted(engine.get_sensitive_columns("Staff")),
       ["*mail*", "email"])


def test_security_layer_end_to_end():
    print("\n== SecurityLayer.process_results end-to-end (audit off) ==")
    sec = SecurityLayer(config_path=CONFIG, enable_audit=False)
    rows = [{"Customers[ssn]": "111-22-3333", "Customers[City]": "Austin"}]
    safe, report = sec.process_results(results=rows, query="EVALUATE Customers", source="desktop")
    check("ssn blocked through layer", safe[0]["Customers[ssn]"] is None, repr(safe[0]["Customers[ssn]"]))
    check("city preserved", safe[0]["Customers[City]"] == "Austin", repr(safe[0]["Customers[City]"]))
    eq("report lists the blocked column", report["columns_blocked"], ["Customers[ssn]"])
    check("report says the query itself was allowed", report["query_denied"] is False)

    # A tool that skips pre_query_check still must not get data for a refused query.
    leak = 'EVALUATE ROW("x", CONCATENATEX(Customers, Customers[ssn], ","))'
    safe, report = sec.process_results(results=[{"[x]": "111-22-3333,222-33-4444"}], query=leak, tool="run_dax_tests")
    eq("a refused query returns nulls", safe, [{"[x]": None}])
    check("report says denied, with the reason", report["query_denied"] and "Social Security" in report["denied_reason"],
          str(report))

    # A scalar result wrapped as one row, the way the scalar-style tools call it.
    safe, report = sec.process_results(results=[{"[Value]": 42}], query='EVALUATE ROW("Value", COUNTROWS(Sales))')
    eq("scalar one-row result passes through", safe, [{"[Value]": 42}])
    check("nothing flagged", not report["pii_detected"] and not report["columns_blocked"] and not report["query_denied"])

    safe, report = sec.process_results(results=[], query="EVALUATE Sales", success=False, error_message="boom")
    eq("failed query: empty in, empty out", safe, [])

    measures = {"All Cards": 'CONCATENATEX(Customers, Customers[Card Number], ",")'}
    safe, report = sec.process_results(results=[{"[x]": "4111111111111111,5500005555555559"}],
                                       query='EVALUATE ROW("x", [All Cards])', measure_definitions=measures)
    check("a measure wrapping a masked column is masked through its definition",
          "4111" not in str(safe[0]["[x]"]) and "5500" not in str(safe[0]["[x]"]), repr(safe[0]["[x]"]))
    eq("... and the report says so", report["derived_action"], "mask")


def test_error_text_cannot_carry_protected_data():
    print("\n== engine error messages ==")
    sec = SecurityLayer(config_path=CONFIG, enable_audit=False)
    leak = "DAX query failed: 4111111111111111,5500005555555559"
    out = sec.safe_error_text(leak, query='EVALUATE ROW("x", ERROR(CONCATENATEX(C, C[Card Number], ",")))')
    check("a query that reads a protected column gets no engine text back",
          "4111" not in out and "5500" not in out and "withheld" in out, out)
    out = sec.safe_error_text("anything", query="EVALUATE VALUES(C[SSN])")
    check("a refused query gets the policy reason", out.startswith("Query blocked by security policy"), out)
    out = sec.safe_error_text("Cannot convert value 'jane@example.com' of type Text to type Number; Password=abc123;",
                              query='EVALUATE ROW("x", VALUE(MAX(C[Contact])))', extra_secrets=["topsecretvalue"])
    eq("otherwise the text is kept, with PII masked and secrets redacted", out,
       "Cannot convert value 'j***@e****.com' of type Text to type Number; Password=***;")
    eq("a plain syntax error passes through unchanged",
       sec.safe_error_text("Syntax error near ')'", query="EVALUATE Sales )"), "Syntax error near ')'")
    measures = {"Cards": 'CONCATENATEX(C, C[Card Number], ",")'}
    out = sec.safe_error_text(leak, query='EVALUATE ROW("x", ERROR([Cards]))', measure_definitions=measures)
    check("measures are followed here too", "4111" not in out, out)
    eq("the literal secret is removed", sec.safe_error_text("failed for topsecretvalue", extra_secrets=["topsecretvalue"]),
       "failed for ***")


def test_layer_row_limit_and_pii_interplay():
    print("\n== layer: row cap, and no re-masking of policy output ==")
    sec = SecurityLayer(config_path=None, enable_audit=False)
    sec.policy_engine.load_from_dict({
        "global": {"max_rows_per_query": 5},
        "tables": [{"name": "*", "columns": [{"name": "email", "action": "hash"},
                                              {"name": "customer name", "action": "redact"}]}]})
    rows = [{"Users[Id]": i} for i in range(12)]
    safe, report = sec.process_results(results=rows, query="EVALUATE Users")
    eq("rows are cut to the policy cap", len(safe), 5)
    check("report says truncated", report["rows_truncated"] is True and report["max_rows"] == 5, str(report))
    safe, report = sec.process_results(results=rows, query="EVALUATE Users", enforce_row_limit=False)
    eq("the cap can be waived for internal metadata queries", len(safe), 12)

    rows = [{"Users[Email]": "a@b.com", "Users[Customer Name]": "Jane Doe"},
            {"Users[Email]": "c@d.org", "Users[Customer Name]": "John Roe"}]
    safe, report = sec.process_results(results=rows, query="EVALUATE Users")
    check("the hash survives the PII pass", all(str(r["Users[Email]"]).startswith("[HASH:") for r in safe), str(safe))
    check("different values keep different hashes", safe[0]["Users[Email]"] != safe[1]["Users[Email]"])
    eq("[REDACTED] is not masked again", safe[0]["Users[Customer Name]"], "[REDACTED]")
    check("policy-transformed columns are not reported as PII", report["pii_count"] == 0, str(report))
    eq("... but are reported as transformed", sorted(report["columns_transformed"]),
       ["Users[Customer Name]", "Users[Email]"])


def test_export_round_trip():
    print("\n== export_config reloads to the same policies ==")
    engine = AccessPolicyEngine(config_path=CONFIG)
    exported = engine.export_config()
    again = AccessPolicyEngine()
    again.load_from_dict(exported)
    for column in ("SSN Number", "Card Number", "PasswordHash", "City"):
        eq(f"{column!r} resolves the same after a round trip",
           again.resolve_column_policy("T", column).action, engine.resolve_column_policy("T", column).action)
    eq("row cap survives", again.global_policy.max_rows_per_query, 1000)


if __name__ == "__main__":
    print("=" * 70)
    print("  ACCESS-POLICY ENFORCEMENT TESTS")
    print("=" * 70)
    for fn in (test_parse_column_key, test_extract_references, test_apply_to_results_shipped_config,
               test_hash_and_redact_actions, test_hash_is_keyed, test_numeric_mask, test_pre_query_check_blocks,
               test_alias_gets_the_referenced_action, test_most_restrictive_rule_wins,
               test_security_layer_end_to_end, test_error_text_cannot_carry_protected_data,
               test_layer_row_limit_and_pii_interplay, test_export_round_trip):
        fn()

    print("\n" + "=" * 70)
    if _failures:
        print(f"  {len(_failures)} CHECK(S) FAILED: {', '.join(_failures)}")
        sys.exit(1)
    print("  ALL SECURITY ENFORCEMENT CHECKS PASSED")
    print("=" * 70)
