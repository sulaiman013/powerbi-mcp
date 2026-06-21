"""
Tests for custom-BPA rule validation + rule-source auditing. No Power BI required.
Run: python tests/test_bpa_authoring.py
"""
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))
import bpa_authoring  # noqa: E402

_failures = []


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f": {detail}" if detail and not cond else ""))
    if not cond:
        _failures.append(name)


GOOD = [{
    "ID": "AVOID_FLOATING_POINT", "Name": "Avoid floating point data types", "Category": "Performance",
    "Severity": 2, "Scope": "Column", "Expression": "DataType = \"Double\"",
}]


def test_valid_ruleset():
    print("\n== a well-formed rule set validates ==")
    res = bpa_authoring.validate_rules(GOOD)
    check("valid", res["valid"] and not res["errors"], str(res["errors"]))
    check("rule counted", res["rule_count"] == 1)
    check("accepts JSON string input", bpa_authoring.validate_rules(json.dumps(GOOD))["valid"])
    check("accepts {Rules:[...]} wrapper", bpa_authoring.validate_rules({"Rules": GOOD})["valid"])


def test_catches_problems():
    print("\n== validation catches missing fields, bad scope/severity, dups ==")
    bad = [
        {"ID": "R1", "Name": "n", "Category": "c", "Severity": 9, "Scope": "Measure", "Expression": "x"},
        {"ID": "R2", "Name": "n", "Category": "c", "Severity": 2, "Scope": "Measur", "Expression": "x"},
        {"ID": "R3", "Name": "n", "Category": "c", "Severity": 2, "Scope": "Measure"},  # missing Expression
        {"ID": "R1", "Name": "n", "Category": "c", "Severity": 2, "Scope": "Measure", "Expression": "x"},  # dup ID
    ]
    res = bpa_authoring.validate_rules(bad)
    msgs = " | ".join(e["message"] for e in res["errors"])
    check("invalid overall", not res["valid"])
    check("bad severity flagged", "Severity" in msgs)
    check("bad scope flagged", "Scope value" in msgs)
    check("missing expression flagged", "Expression" in msgs)
    check("duplicate id flagged", "Duplicate rule ID" in msgs)


def test_warnings_and_fix():
    print("\n== warnings (destructive fix, runtime fields) + --fix cleanup ==")
    risky = [{
        "ID": "DEL RULE", "Name": "n", "Category": "c", "Severity": 1, "Scope": "Column",
        "Expression": "x", "FixExpression": "it.Delete()", "ObjectCount": 5, "FixExpression2": None,
    }]
    res = bpa_authoring.validate_rules(risky, fix=True)
    wmsgs = " | ".join(w["message"] for w in res["warnings"])
    check("destructive-fix warning", "destructive auto-fix" in wmsgs or "deletes an object" in wmsgs)
    check("runtime-field warning", "runtime-only field" in wmsgs)
    check("id-with-spaces warning", "spaces" in wmsgs)
    check("fixed copy strips ObjectCount", "ObjectCount" not in res["fixed"][0])


def test_audit_sources():
    print("\n== audit_rule_sources parses embedded / external / ignored rules ==")
    embedded = json.dumps([{"ID": "EMB1"}, {"ID": "EMB2"}])
    model_tmdl = (
        "model Model\n"
        f"\tannotation BestPracticeAnalyzer = {embedded}\n"
        "\tannotation BestPracticeAnalyzer_ExternalRuleFiles = [\"https://example.com/rules.json\"]\n"
        "\tannotation BestPracticeAnalyzer_IgnoreRules = {\"RuleIDs\":[\"IGN1\",\"IGN2\"]}\n"
    )
    res = bpa_authoring.audit_rule_sources(model_tmdl, local_rule_files={"user": json.dumps(GOOD)})
    check("embedded rules found", res["embedded_rule_count"] == 2 and "EMB1" in res["embedded_rule_ids"], str(res))
    check("external rule file found", "https://example.com/rules.json" in res["external_rule_files"], str(res["external_rule_files"]))
    check("ignored ids found", res["ignored_rule_ids"] == ["IGN1", "IGN2"], str(res["ignored_rule_ids"]))
    check("local file summarized", res["local_rule_files"][0]["rule_count"] == 1)


if __name__ == "__main__":
    print("=" * 70)
    print("  BPA AUTHORING TESTS")
    print("=" * 70)
    test_valid_ruleset()
    test_catches_problems()
    test_warnings_and_fix()
    test_audit_sources()
    print("\n" + "=" * 70)
    if _failures:
        print(f"  {len(_failures)} CHECK(S) FAILED: {', '.join(_failures)}")
        sys.exit(1)
    print("  ALL BPA AUTHORING CHECKS PASSED")
    print("=" * 70)
