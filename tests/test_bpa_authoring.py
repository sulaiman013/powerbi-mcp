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
    "Severity": 2, "Scope": "DataColumn, CalculatedColumn", "Expression": "DataType = \"Double\"",
}]
RULE = {"ID": "R1", "Name": "n", "Category": "c", "Severity": 2, "Scope": "Measure", "Expression": "x"}


def messages(result, key="errors"):
    return " | ".join(e["message"] for e in result[key])


def test_valid_ruleset():
    print("\n== a well-formed rule set validates ==")
    res = bpa_authoring.validate_rules(GOOD)
    check("valid", res["valid"] and not res["errors"] and not res["warnings"], str(res))
    check("rule counted", res["rule_count"] == 1)
    check("accepts JSON string input", bpa_authoring.validate_rules(json.dumps(GOOD))["valid"])
    check("accepts {Rules:[...]} wrapper", bpa_authoring.validate_rules({"Rules": GOOD})["valid"])
    check("accepts {rules:[...]} wrapper", bpa_authoring.validate_rules({"rules": GOOD})["rule_count"] == 1)
    check("result keys", set(res) == {"valid", "rule_count", "errors", "warnings"})


def test_catches_problems():
    print("\n== validation catches missing fields, bad scope/severity, dups ==")
    bad = [
        {"ID": "R1", "Name": "n", "Category": "c", "Severity": 9, "Scope": "Measure", "Expression": "x"},
        {"ID": "R2", "Name": "n", "Category": "c", "Severity": 2, "Scope": "Measur", "Expression": "x"},
        {"ID": "R3", "Name": "n", "Category": "c", "Severity": 2, "Scope": "Measure"},  # missing Expression
        {"ID": "R1", "Name": "n", "Category": "c", "Severity": 2, "Scope": "Measure", "Expression": "x"},  # dup ID
    ]
    res = bpa_authoring.validate_rules(bad)
    msgs = messages(res)
    check("invalid overall", not res["valid"])
    check("bad severity flagged", "Severity" in msgs)
    check("bad scope flagged", "Scope value" in msgs)
    check("missing expression flagged", "Expression" in msgs)
    check("duplicate id flagged", "Duplicate rule ID" in msgs)
    check("errors carry index and rule id", res["errors"][0]["index"] == 0 and res["errors"][0]["rule_id"] == "R1")


def test_warnings_and_fix():
    print("\n== warnings (destructive fix, runtime fields) + --fix cleanup ==")
    risky = [{
        "ID": "DEL RULE", "Name": "n", "Category": "c", "Severity": 1, "Scope": "DataColumn",
        "Expression": "x", "FixExpression": "it.Delete()", "ObjectCount": 5, "FixExpression2": None,
    }]
    res = bpa_authoring.validate_rules(risky, fix=True)
    wmsgs = messages(res, "warnings")
    check("destructive-fix warning", "destructive auto-fix" in wmsgs or "deletes an object" in wmsgs)
    check("runtime-field warning", "runtime-only field" in wmsgs)
    check("id-with-spaces warning", "spaces" in wmsgs)
    check("fixed copy strips ObjectCount", "ObjectCount" not in res["fixed"][0])
    check("a bare Delete() fix is destructive too",
          "deletes an object" in messages(bpa_authoring.validate_rules([dict(RULE, FixExpression="Delete()")]), "warnings"))
    check("Delete on an error-severity rule is not warned about",
          bpa_authoring.validate_rules([dict(RULE, Severity=3, FixExpression="Delete()")])["warnings"] == [])
    res = bpa_authoring.validate_rules([dict(RULE, FixExpression=None), "junk"], fix=True)
    check("null FixExpression dropped, non-object rules left out of the cleaned copy",
          res["fixed"] == [RULE] and not res["valid"], str(res.get("fixed")))


def test_bare_rule_object():
    print("\n== defect 27: one rule object is one rule, not 'valid, 0 rules' ==")
    res = bpa_authoring.validate_rules(RULE)
    check("a bare rule object is validated", res["rule_count"] == 1 and res["valid"], str(res))
    res = bpa_authoring.validate_rules({"ID": "X", "Name": "n"})
    check("an incomplete bare rule object fails", res["rule_count"] == 1 and not res["valid"]
          and "Missing required field 'Expression'" in messages(res), messages(res))
    check("a bare rule object as JSON text", bpa_authoring.validate_rules(json.dumps(RULE))["rule_count"] == 1)

    def rejected(value, needle):
        try:
            bpa_authoring.validate_rules(value)
        except ValueError as e:
            return needle in str(e)
        return False
    check("an unrelated object is rejected", rejected({"foo": 1}, "neither a rule"))
    check("a wrapper that does not hold a list is rejected", rejected({"Rules": "oops"}, "JSON array"))
    check("invalid JSON text is rejected with the reason", rejected("{not json", "not valid JSON"))
    check("a number is rejected", rejected(5, "JSON array"))
    check("an empty list is valid and empty", bpa_authoring.validate_rules([]) ==
          {"valid": True, "rule_count": 0, "errors": [], "warnings": []})


def test_field_types():
    print("\n== defect 28: wrong value types are errors, never crashes ==")

    def errs(**overrides):
        return messages(bpa_authoring.validate_rules([dict(RULE, **overrides)]))

    check("Severity list", "Severity [2] must be a whole number" in errs(Severity=[2]), errs(Severity=[2]))
    check("Severity dict", "must be a whole number" in errs(Severity={"a": 1}))
    check("Severity text", "must be a whole number" in errs(Severity="2"))
    check("Severity true", "must be a whole number" in errs(Severity=True))
    check("Severity 2.0", "must be a whole number" in errs(Severity=2.0))
    check("Severity 0 and 4", "not one of [1, 2, 3]" in errs(Severity=0) and "not one of [1, 2, 3]" in errs(Severity=4))
    check("Severity 1, 2, 3 pass", all(errs(Severity=s) == "" for s in (1, 2, 3)))
    check("ID list", "'ID' must be text (got list)" in errs(ID=["a"]), errs(ID=["a"]))
    check("ID number", "'ID' must be text" in errs(ID=7))
    check("Scope list", "'Scope' must be text (got list)" in errs(Scope=["Measure"]))
    check("Expression number", "'Expression' must be text" in errs(Expression=5))
    check("Expression of only spaces is missing", "Missing required field 'Expression'" in errs(Expression="   "))
    check("FixExpression number", "'FixExpression' must be text" in errs(FixExpression=5))
    check("CompatibilityLevel text", "CompatibilityLevel 'abc'" in errs(CompatibilityLevel="abc"))
    check("CompatibilityLevel number passes", errs(CompatibilityLevel=1200) == "")
    two = bpa_authoring.validate_rules([{k: v for k, v in RULE.items() if k != "ID"}] * 2)
    check("rules without an ID are each reported, not called duplicates",
          messages(two).count("Missing required field 'ID'") == 2 and "Duplicate" not in messages(two))
    case = bpa_authoring.validate_rules([dict(RULE, ID="abc"), dict(RULE, ID="ABC")])
    check("IDs differing only in case: valid, with a warning", case["valid"] and "letter case" in messages(case, "warnings"))


def test_scope_names():
    print("\n== defect 29: scope vocabulary ==")

    def run(scope):
        res = bpa_authoring.validate_rules([dict(RULE, Scope=scope)])
        return res["valid"], messages(res), messages(res, "warnings")

    for scope in ("Model", "Table", "Measure", "Hierarchy", "Level", "Relationship", "Perspective", "Culture",
                  "Partition", "ProviderDataSource", "DataColumn", "CalculatedColumn", "CalculatedTable",
                  "CalculatedTableColumn", "KPI", "StructuredDataSource", "Variation", "NamedExpression", "ModelRole",
                  "CalculationGroup", "CalculationItem", "TablePermission"):
        check(f"scope {scope}", run(scope) == (True, "", ""), str(run(scope)))
    check("several scopes, spaces around the commas", run("Measure , CalculatedColumn,DataColumn") == (True, "", ""))
    for alias, current in (("Column", "DataColumn, CalculatedColumn, CalculatedTableColumn"),
                           ("DataSource", "ProviderDataSource"), ("Role", "ModelRole")):
        valid, errs, warns = run(alias)
        check(f"older spelling {alias} is accepted with a pointer to {current}", valid and current in warns, warns)
    valid, errs, _ = run("measure")
    check("wrong letter case is an error with a suggestion", not valid and "did you mean Measure?" in errs, errs)
    valid, errs, _ = run("Measur, Table")
    check("unknown scope is an error, known ones in the same list are not named", not valid and "Measur." in errs and "Table" not in errs, errs)


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
    check("local rule IDs are returned", res["local_rule_files"][0]["rule_ids"] == ["AVOID_FLOATING_POINT"]
          and res["local_rule_ids"] == ["AVOID_FLOATING_POINT"])
    bad_local = bpa_authoring.audit_rule_sources("", {"broken": "{nope", "wrong": json.dumps([{"ID": 1}])})
    check("an unreadable local file is reported, not raised",
          "not valid JSON" in bad_local["local_rule_files"][0]["error"] and bad_local["local_rule_files"][1]["valid"] is False)
    empty = bpa_authoring.audit_rule_sources(None)
    check("no model text", empty["embedded_rule_count"] == 0 and empty["ignored_rule_ids"] == [] and empty["external_rule_files"] == [])


def test_audit_multiline_annotation():
    print("\n== audit_rule_sources parses a MULTI-LINE embedded rules annotation ==")
    embedded = json.dumps([{"ID": "EMB1"}, {"ID": "EMB2"}, {"ID": "EMB3"}], indent=2)  # multi-line
    model_tmdl = "model Model\n\tannotation BestPracticeAnalyzer = " + embedded + "\n\tannotation Foo = bar\n"
    res = bpa_authoring.audit_rule_sources(model_tmdl)
    check("multi-line embedded rules parsed", res["embedded_rule_count"] == 3, str(res["embedded_rule_ids"]))


def test_all_annotations_are_read():
    print("\n== defect 26: ignore lists on every object are read ==")
    text = ("model Model\n"
            "\tannotation BestPracticeAnalyzer_IgnoreRules = {\"RuleIDs\":[\"MODEL_RULE\"]}\n"
            "\n"
            "table Sales\n"
            "\tannotation BestPracticeAnalyzer_IgnoreRules = {\"RuleIDs\":[\"TABLE_RULE\",\"MODEL_RULE\"]}\n"
            "\tmeasure X = 1\n"
            "\t\tannotation BestPracticeAnalyzer_IgnoreRules = {\"RuleIDs\":[\"MEASURE_RULE_A\",\"MEASURE_RULE_B\"]}\n"
            "\tcolumn C\n"
            "\t\tannotation BestPracticeAnalyzer_IgnoreRulesX = {\"RuleIDs\":[\"NOT_THIS\"]}\n")
    res = bpa_authoring.audit_rule_sources(text)
    check("every annotation contributes, without duplicates",
          res["ignored_rule_ids"] == ["MODEL_RULE", "TABLE_RULE", "MEASURE_RULE_A", "MEASURE_RULE_B"], str(res["ignored_rule_ids"]))
    check("the number of ignore annotations is reported", res["ignore_annotation_count"] == 3)
    check("a longer annotation name is a different annotation", "NOT_THIS" not in res["ignored_rule_ids"])
    two = ("model Model\n\tannotation BestPracticeAnalyzer = [{\"ID\":\"A\"}]\n"
           "ref model\n\tannotation BestPracticeAnalyzer = [{\"ID\":\"B\"}]\n")
    check("every embedded rules annotation contributes", bpa_authoring.audit_rule_sources(two)["embedded_rule_ids"] == ["A", "B"])


def test_external_rule_paths():
    print("\n== defect 28: external rule files include local paths ==")
    text = ("model Model\n\tannotation BestPracticeAnalyzer_ExternalRuleFiles = "
            + json.dumps(["..\\Rules\\BPARules.json", "C:\\bpa\\team.json", "https://example.com/r.json",
                          "https://example.com/r.json", "rules/shared.json"]) + "\n")
    res = bpa_authoring.audit_rule_sources(text)
    check("all entries are listed once, in order", res["external_rule_files"] ==
          ["..\\Rules\\BPARules.json", "C:\\bpa\\team.json", "https://example.com/r.json", "rules/shared.json"],
          str(res["external_rule_files"]))
    check("URLs and paths are told apart", res["external_rule_urls"] == ["https://example.com/r.json"]
          and len(res["external_rule_paths"]) == 3)
    loose = "model Model\n\tannotation BestPracticeAnalyzer_ExternalRuleFiles = https://a.example/x.json, https://b.example/y.json\n"
    check("a non-JSON value still yields its URLs", bpa_authoring.audit_rule_sources(loose)["external_rule_urls"] ==
          ["https://a.example/x.json", "https://b.example/y.json"])


def test_value_shapes():
    print("\n== defect 28: every way the annotation value can be written ==")
    rules = [{"ID": "EMB1"}, {"ID": "EMB2"}]

    def ids(text):
        return bpa_authoring.audit_rule_sources(text)["embedded_rule_ids"]

    fenced_inline = "model Model\n\tannotation BestPracticeAnalyzer = ```\n\t\t" + json.dumps(rules) + "\n\t\t```\n"
    check("``` fence opened on the annotation line", ids(fenced_inline) == ["EMB1", "EMB2"], str(ids(fenced_inline)))
    fenced_below = ("model Model\n\tannotation BestPracticeAnalyzer =\n\t\t\t```\n\t\t\t"
                    + json.dumps(rules, indent=2).replace("\n", "\n\t\t\t") + "\n\t\t\t```\n\tannotation Other = 1\n")
    check("``` fence on the lines below", ids(fenced_below) == ["EMB1", "EMB2"], str(ids(fenced_below)))
    below = "model Model\n\tannotation BestPracticeAnalyzer =\n\t\t\t[{\"ID\":\"EMB1\"},\n\t\t\t {\"ID\":\"EMB2\"}]\n\tannotation Other = [{\"ID\":\"NO\"}]\n"
    check("value indented on the lines below, stopping at the next property", ids(below) == ["EMB1", "EMB2"], str(ids(below)))
    quoted = "model Model\n\tannotation BestPracticeAnalyzer = " + json.dumps(json.dumps(rules)) + "\n"
    check("a quoted string holding the JSON", ids(quoted) == ["EMB1", "EMB2"], str(ids(quoted)))
    doubled = 'model Model\n\tannotation BestPracticeAnalyzer = "[{""ID"":""EMB1""}]"\n'
    check("a TMDL string with doubled quotes", ids(doubled) == ["EMB1"], str(ids(doubled)))
    wrapper = "model Model\n\tannotation BestPracticeAnalyzer = " + json.dumps({"Rules": rules}) + "\n"
    check("a {Rules: [...]} wrapper", ids(wrapper) == ["EMB1", "EMB2"])
    crlf = fenced_below.replace("\n", "\r\n")
    check("CRLF line endings", ids(crlf) == ["EMB1", "EMB2"], str(ids(crlf)))
    garbage = "model Model\n\tannotation BestPracticeAnalyzer = not json at all\n"
    check("an unparseable value yields nothing and does not raise", ids(garbage) == [])

    bim = json.dumps({"model": {
        "annotations": [{"value": json.dumps(rules), "name": "BestPracticeAnalyzer"},                 # value before name
                        {"name": "BestPracticeAnalyzer_IgnoreRules", "value": json.dumps({"RuleIDs": ["M1"]})}],
        "tables": [{"name": "T", "annotations": [
            {"name": "BestPracticeAnalyzer_IgnoreRules", "value": json.dumps({"RuleIDs": ["T1"]})}],
            "measures": [{"name": "m", "annotations": [
                {"name": "BestPracticeAnalyzer_IgnoreRules", "value": ["{\"RuleIDs\":", "[\"X1\"]}"]}]}]}]}}, indent=2)
    res = bpa_authoring.audit_rule_sources(bim)
    check("BIM: any key order", res["embedded_rule_ids"] == ["EMB1", "EMB2"], str(res["embedded_rule_ids"]))
    check("BIM: annotations at every level, including line-array values", res["ignored_rule_ids"] == ["M1", "T1", "X1"],
          str(res["ignored_rule_ids"]))
    fragment = '"annotations": [ {"value": "[{\\"ID\\":\\"F1\\"}]", "name": "BestPracticeAnalyzer"} ]'
    check("BIM fragment (not a whole document), value before name", ids(fragment) == ["F1"], str(ids(fragment)))


if __name__ == "__main__":
    print("=" * 70)
    print("  BPA AUTHORING TESTS")
    print("=" * 70)
    test_valid_ruleset()
    test_catches_problems()
    test_warnings_and_fix()
    test_bare_rule_object()
    test_field_types()
    test_scope_names()
    test_audit_sources()
    test_audit_multiline_annotation()
    test_all_annotations_are_read()
    test_external_rule_paths()
    test_value_shapes()
    print("\n" + "=" * 70)
    if _failures:
        print(f"  {len(_failures)} CHECK(S) FAILED: {', '.join(_failures)}")
        sys.exit(1)
    print("  ALL BPA AUTHORING CHECKS PASSED")
    print("=" * 70)
