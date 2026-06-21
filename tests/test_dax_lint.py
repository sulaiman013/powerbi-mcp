"""
Tests for the pure-Python DAX linter: each rule fires on a positive case and stays silent on
a clean equivalent, plus tokenizer comment/string handling. No Power BI required.
Run: python tests/test_dax_lint.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))
import dax_lint  # noqa: E402

_failures = []


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f": {detail}" if detail and not cond else ""))
    if not cond:
        _failures.append(name)


def rules(dax):
    return {f["rule_id"] for f in dax_lint.lint_expression("M", dax)}


def test_rules_fire():
    print("\n== each rule fires on its anti-pattern ==")
    check("DL001 FILTER whole table", "DL001" in rules('CALCULATE(SUM(Sales[Amt]), FILTER(Sales, Sales[Region]="X"))'))
    check("DL002 nested CALCULATE", "DL002" in rules("CALCULATE(CALCULATE([Sales]), Sales[Y]=1)"))
    check("DL003 bare division", "DL003" in rules("[Profit] / [Sales]"))
    check("DL004 IFERROR", "DL004" in rules("IFERROR([a]/[b], 0)"))
    check("DL005 plus zero", "DL005" in rules("[Total Sales] + 0"))
    check("DL006 EARLIER", "DL006" in rules("SUMX(T, T[x] - EARLIER(T[x]))"))
    check("DL007 SUMMARIZE aggregation", "DL007" in rules('SUMMARIZE(Sales, Sales[Region], "Tot", SUM(Sales[Amt]))'))
    check("DL008 unknown function", "DL008" in rules("TOTALSALEZ(Sales[Amt])"))


def test_clean_is_silent():
    print("\n== clean equivalents do not fire ==")
    check("DIVIDE has no DL003", "DL003" not in rules("DIVIDE([Profit], [Sales])"))
    check("boolean filter has no DL001", "DL001" not in rules('CALCULATE(SUM(Sales[Amt]), Sales[Region]="X")'))
    check("FILTER(VALUES(..)) has no DL001", "DL001" not in rules('CALCULATE([X], FILTER(VALUES(Sales[Region]), [X] > 0))'))
    check("real function not DL008", "DL008" not in rules("SUMX(Sales, Sales[Qty] * Sales[Price])"))
    check("SUMMARIZECOLUMNS not DL007", "DL007" not in rules('SUMMARIZECOLUMNS(Sales[Region], "Tot", SUM(Sales[Amt]))'))
    check("dotted function recognized", "DL008" not in rules("PERCENTILE.INC(Sales[Amt], 0.9)"))
    check("VAR name not unknown-func", "DL008" not in rules("VAR Threshold = 10 RETURN IF([X] > Threshold, 1, 0)"))


def test_tokenizer_robustness():
    print("\n== comments and strings do not create false positives ==")
    check("slash in line comment ignored", "DL003" not in rules("SUM(Sales[Amt]) // ratio a/b here"))
    check("slash in block comment ignored", "DL003" not in rules("SUM(Sales[Amt]) /* a/b */ "))
    check("slash in string ignored", "DL003" not in rules('CONCATENATE("50/50", [x])'))
    check("comment does not hide real division", "DL003" in rules("// note\n[a] / [b]"))


def test_rewrites_and_summary():
    print("\n== rewrite hints + measure summary ==")
    hints = {h["rule_id"] for h in dax_lint.suggest_rewrites("M", 'CALCULATE([X], FILTER(Sales, Sales[R]="A")) + [Y]/[Z]')}
    check("rewrite for DL001", "DL001" in hints)
    check("rewrite for DL003", "DL003" in hints)
    res = dax_lint.lint_measures([
        {"name": "Bad", "expression": "[a]/[b]"},
        {"name": "Good", "expression": "DIVIDE([a],[b])"},
    ])
    check("summary counts findings", res["summary"]["total"] >= 1 and res["summary"]["measures_scanned"] == 2)
    check("findings carry object name", any(f["object"] == "Bad" for f in res["findings"]))


if __name__ == "__main__":
    print("=" * 70)
    print("  DAX LINTER TESTS")
    print("=" * 70)
    test_rules_fire()
    test_clean_is_silent()
    test_tokenizer_robustness()
    test_rewrites_and_summary()
    print("\n" + "=" * 70)
    if _failures:
        print(f"  {len(_failures)} CHECK(S) FAILED: {', '.join(_failures)}")
        sys.exit(1)
    print("  ALL DAX LINTER CHECKS PASSED")
    print("=" * 70)
