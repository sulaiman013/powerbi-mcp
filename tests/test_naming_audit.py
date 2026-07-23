"""
Tests for the naming-convention audit -> rename plan. No Power BI required.
Run: python tests/test_naming_audit.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))
import naming_audit  # noqa: E402

_failures = []


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f": {detail}" if detail and not cond else ""))
    if not cond:
        _failures.append(name)


MODEL = {
    "tables": [
        {"name": "DIM_Customer",
         "columns": [{"name": "customer_id"}, {"name": "salesAmount"}, {"name": "  Region "},
                     {"name": "ID"}, {"name": "PRODUCT"}, {"name": "Qty Sold"}],
         "measures": [{"name": "Total_Sales"}]},
        {"name": "Sales",
         "columns": [{"name": "Order Date"}],
         "measures": [{"name": "Revenue"}]},
    ]
}


def _by_old(plan, old):
    for p in plan:
        if p["old"] == old:
            return p
    return None


def test_detection():
    print("\n== detects and normalizes the common conventions ==")
    res = naming_audit.audit(MODEL)
    plan = res["plan"]
    p = _by_old(plan, "DIM_Customer")
    check("warehouse prefix stripped", p and p["new"] == "Customer" and "warehouse DIM_/FACT_ prefix" in p["reasons"], str(p))
    p = _by_old(plan, "salesAmount")
    check("camelCase split + titled", p and p["new"] == "Sales Amount" and "camelCase" in p["reasons"], str(p))
    p = _by_old(plan, "customer_id")
    check("snake_case converted", p and p["new"] == "Customer Id" and "snake_case" in p["reasons"], str(p))
    p = _by_old(plan, "  Region ")
    check("whitespace trimmed", p and p["new"] == "Region", str(p))
    p = _by_old(plan, "PRODUCT")
    check("uppercase titled", p and p["new"] == "Product", str(p))


def test_acronyms_and_clean_preserved():
    print("\n== acronyms and already-clean names are left alone ==")
    res = naming_audit.audit(MODEL)
    plan = res["plan"]
    check("acronym ID preserved (no suggestion)", _by_old(plan, "ID") is None)
    check("clean 'Order Date' preserved", _by_old(plan, "Order Date") is None)
    check("clean 'Revenue' preserved", _by_old(plan, "Revenue") is None)


def test_abbreviations_opt_in():
    print("\n== abbreviation expansion is opt-in ==")
    off = naming_audit.audit(MODEL)
    on = naming_audit.audit(MODEL, {"expand_abbreviations": True})
    check("Qty Sold untouched by default", _by_old(off["plan"], "Qty Sold") is None)
    p = _by_old(on["plan"], "Qty Sold")
    check("Qty -> Quantity when enabled", p and p["new"] == "Quantity Sold" and "abbreviation" in p["reasons"], str(p))


def test_summary_and_scope():
    print("\n== summary + scope filter ==")
    res = naming_audit.audit(MODEL)
    s = res["summary"]
    check("summary counts suggestions", s["total_suggestions"] == len(res["plan"]) and s["total_suggestions"] >= 5)
    check("by_reason populated", s["by_reason"].get("snake_case", 0) >= 1)
    check("observed styles recorded", "snake_case" in s["observed_styles"])
    only_tables = naming_audit.audit(MODEL, {"scope": ["tables"]})
    check("scope=tables yields only tables", all(p["object_type"] == "table" for p in only_tables["plan"]))


def test_hidden_and_internal_columns_skipped():
    print("\n== live-UAT regression: hidden + internal RowNumber columns skipped ==")
    model = {"tables": [{
        "name": "dim_date",
        "columns": [
            {"name": "RowNumber-2662979B-1795-4F74-8F37-6A1BA8059B61"},
            {"name": "hidden_helper", "is_hidden": True},
            {"name": "monthName"},
        ],
        "measures": [{"name": "secretCalc", "is_hidden": "true"}],
    }]}
    res = naming_audit.audit(model)
    olds = {p["old"] for p in res["plan"]}
    check("RowNumber-GUID never in plan", not any(o.startswith("RowNumber-") for o in olds), str(olds))
    check("hidden column never in plan", "hidden_helper" not in olds, str(olds))
    check("hidden measure never in plan", "secretCalc" not in olds, str(olds))
    check("visible column still audited", any(p["old"] == "monthName" for p in res["plan"]))


if __name__ == "__main__":
    print("=" * 70)
    print("  NAMING AUDIT TESTS")
    print("=" * 70)
    test_detection()
    test_acronyms_and_clean_preserved()
    test_abbreviations_opt_in()
    test_summary_and_scope()
    test_hidden_and_internal_columns_skipped()
    print("\n" + "=" * 70)
    if _failures:
        print(f"  {len(_failures)} CHECK(S) FAILED: {', '.join(_failures)}")
        sys.exit(1)
    print("  ALL NAMING AUDIT CHECKS PASSED")
    print("=" * 70)
