"""
Tests for the pure model-analysis engine (BPA + AI-readiness). No Power BI needed.

Run: python test_model_analysis.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))
from model_analysis import run_bpa, audit_ai_readiness  # noqa: E402

_failures = []


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f": {detail}" if detail and not cond else ""))
    if not cond:
        _failures.append(name)


MODEL = {
    "tables": [
        {
            "name": "Sales", "is_hidden": False, "description": "Sales fact",
            "columns": [
                {"name": "Amount", "data_type": "double", "is_hidden": False, "description": ""},
                {"name": "Qty", "data_type": "int64", "is_hidden": False, "description": "Units sold"},
                {"name": "DateKey", "data_type": "int64", "is_hidden": True, "is_key": True},
                {"name": "Bucket", "data_type": "string", "is_hidden": False, "is_calculated": True,
                 "expression": "IF(Sales[Amount]>100,\"Big\",\"Small\")", "description": "size bucket"},
            ],
            "measures": [
                {"name": "Total Sales", "expression": "SUM(Sales[Amount])", "format_string": "#,##0",
                 "description": "Sum of amount", "is_hidden": False},
                {"name": "Margin %", "expression": "[Profit] / [Total Sales]", "format_string": "",
                 "description": "", "is_hidden": False},
                {"name": "Safe Ratio ", "expression": "DIVIDE([Profit],[Total Sales])", "format_string": "0.0%",
                 "description": "ratio", "is_hidden": False},
            ],
        },
        {
            "name": "DateDim", "is_hidden": False, "description": "",
            "columns": [
                {"name": "DateKey", "data_type": "string", "is_hidden": False, "description": "date key"},
                {"name": "Year", "data_type": "int64", "is_hidden": False, "description": "year"},
            ],
            "measures": [],
        },
    ],
    "relationships": [
        {"from_table": "Sales", "from_column": "DateKey", "to_table": "DateDim", "to_column": "DateKey",
         "is_active": True, "cross_filter": "both"},
    ],
}


def has_rule(findings, rule_id, obj_contains=None):
    return any(f["rule_id"] == rule_id and (obj_contains is None or obj_contains in (f["object"] or "")) for f in findings)


def test_bpa():
    print("\n== run_bpa ==")
    res = run_bpa(MODEL)
    f = res["findings"]
    check("float column flagged", has_rule(f, "PERF_FLOAT_COLUMN", "Sales[Amount]"))
    check("calculated column flagged", has_rule(f, "PERF_CALC_COLUMN", "Sales[Bucket]"))
    check("bidirectional flagged", has_rule(f, "PERF_BIDIRECTIONAL"))
    check("DIVIDE suggested for Margin %", has_rule(f, "DAX_USE_DIVIDE", "Margin %"))
    check("DIVIDE NOT flagged for Safe Ratio", not has_rule(f, "DAX_USE_DIVIDE", "Safe Ratio"))
    check("trailing space flagged", has_rule(f, "NAMING_TRAILING_SPACE", "Safe Ratio "))
    check("no-format measure flagged", has_rule(f, "FORMAT_MEASURE_NO_FORMAT", "Margin %"))
    check("no-description measure flagged", has_rule(f, "MAINT_MEASURE_NO_DESC", "Margin %"))
    check("no-description column flagged", has_rule(f, "MAINT_COLUMN_NO_DESC", "Sales[Amount]"))
    check("rel type mismatch flagged", has_rule(f, "ERR_REL_TYPE_MISMATCH"))
    check("summary counts present", res["summary"]["total"] == len(f) and res["summary"]["by_severity"]["error"] >= 1)

    # category + severity filters
    only_dax = run_bpa(MODEL, categories=["DAX"])
    check("category filter works", all(x["category"] == "DAX" for x in only_dax["findings"]) and only_dax["findings"])
    warns = run_bpa(MODEL, min_severity="warning")
    check("severity filter works", all(x["severity"] in ("warning", "error") for x in warns["findings"]))


def test_ai_readiness():
    print("\n== audit_ai_readiness ==")
    r = audit_ai_readiness(MODEL)
    check("score is 0-100", 0 <= r["score"] <= 100, str(r["score"]))
    check("grade present", r["grade"] in list("ABCDF"))
    check("metrics present", "measures_with_description_pct" in r["metrics"])
    check("recommends measure descriptions", any("description" in rec.lower() for rec in r["recommendations"]))

    # A fully documented model should score higher
    good = {
        "tables": [{"name": "T", "description": "d", "columns": [
            {"name": "C", "data_type": "int64", "is_hidden": False, "description": "c"}],
            "measures": [{"name": "M", "expression": "1", "format_string": "0", "description": "m", "is_hidden": False}]}],
        "relationships": [],
    }
    check("documented model scores higher", audit_ai_readiness(good)["score"] > r["score"])


if __name__ == "__main__":
    print("=" * 70)
    print("  MODEL ANALYSIS (BPA + AI-READINESS) TESTS")
    print("=" * 70)
    test_bpa()
    test_ai_readiness()
    print("\n" + "=" * 70)
    if _failures:
        print(f"  {len(_failures)} CHECK(S) FAILED: {', '.join(_failures)}")
        sys.exit(1)
    print("  ALL MODEL ANALYSIS CHECKS PASSED")
    print("=" * 70)
