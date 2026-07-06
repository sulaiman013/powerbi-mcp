"""
Tests for the bulk DAX measure-suite generator: every kind produces named, documented,
self-contained measures whose DAX is clean under our own linter. No Power BI required.
Run: python tests/test_dax_generator.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))
import dax_generator as dg  # noqa: E402
import dax_lint  # noqa: E402

_failures = []


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f": {detail}" if detail and not cond else ""))
    if not cond:
        _failures.append(name)


def test_ref_helpers():
    print("\n== reference helpers ==")
    check("Table.Column form", dg.column_ref("Date.Date") == "'Date'[Date]")
    check("quoted-table form", dg.column_ref("'Order Date'.Date") == "'Order Date'[Date]")
    check("literal DAX passthrough", dg.column_ref("Sales[Amount]") == "Sales[Amount]")
    check("measure ref bracketing", dg.measure_ref("Total Sales") == "[Total Sales]")
    check("split back", dg.split_column_ref("Date.Date") == ("Date", "Date"))


def test_time_intelligence():
    print("\n== time intelligence suite ==")
    suite = dg.generate_time_intelligence("Total Sales", "Date.Date")
    names = {m["name"] for m in suite}
    check("default variant count", len(suite) == len(dg.DEFAULT_TI_VARIANTS), str(len(suite)))
    check("YTD named", "Total Sales YTD" in names, str(names))
    check("YoY % named", "Total Sales YoY %" in names)
    ytd = next(m for m in suite if m["name"] == "Total Sales YTD")
    check("YTD expression", ytd["expression"] == "CALCULATE([Total Sales], DATESYTD('Date'[Date]))", ytd["expression"])
    yoy_pct = next(m for m in suite if m["name"] == "Total Sales YoY %")
    check("YoY % gets pct format", "%" in (yoy_pct["format_string"] or ""))
    check("YoY % is self-contained (VAR, no dependency on generated names)",
          "VAR __py" in yoy_pct["expression"] and "[Total Sales PY]" not in yoy_pct["expression"])
    check("every measure documented", all(m["description"] and m["display_folder"] for m in suite))
    # base format inheritance
    suite2 = dg.generate_time_intelligence("Total Sales", "Date.Date", variants=["ytd"], base_format="#,0")
    check("inherit base format", suite2[0]["format_string"] == "#,0")
    try:
        dg.generate_time_intelligence("X", "Date.Date", variants=["bogus"])
        check("unknown variant raises", False)
    except ValueError:
        check("unknown variant raises", True)


def test_ratios_ranking_stats():
    print("\n== ratios, ranking, column stats ==")
    ratios = dg.generate_ratios("Total Sales", ["Product.Category"])
    check("ratio pair generated", len(ratios) == 2)
    check("pct format on ratios", all(m["format_string"] == "0.0%" for m in ratios))
    check("ALL + ALLSELECTED variants", any("ALLSELECTED" in m["expression"] for m in ratios)
          and any("ALL('Product'[Category])" in m["expression"] for m in ratios))
    ranks = dg.generate_ranking("Total Sales", ["Product.Category", "Customer.Region"])
    check("rank per dimension", len(ranks) == 2)
    check("rank guards totals with HASONEVALUE", all("HASONEVALUE" in m["expression"] for m in ranks))
    stats = dg.generate_column_stats("Sales.Amount", stats=["sum", "avg", "distinct"])
    check("stats subset", {m["name"] for m in stats} == {"Total Amount", "Average Amount", "Distinct Amount"})


def test_all_generated_dax_is_lint_clean():
    print("\n== every generated expression is clean under dax_lint ==")
    all_measures = (
        dg.generate_time_intelligence("Total Sales", "Date.Date",
                                      variants=list(dg.TIME_INTELLIGENCE_VARIANTS))
        + dg.generate_ratios("Total Sales", ["Product.Category"])
        + dg.generate_ranking("Total Sales", ["Product.Category"])
        + dg.generate_column_stats("Sales.Amount")
    )
    res = dax_lint.lint_measures([{"name": m["name"], "expression": m["expression"]} for m in all_measures])
    bad = [f for f in res["findings"] if f["severity"] in ("warning", "error")]
    check(f"no warnings/errors across {len(all_measures)} measures", not bad,
          str([(f['object'], f['rule_id']) for f in bad]))


def test_dispatcher():
    print("\n== dispatcher ==")
    s = dg.generate_suite("ti", base_measure="X", date_column="D.D", variants=["ytd"])
    check("alias kind works", len(s) == 1 and s[0]["name"] == "X YTD")
    try:
        dg.generate_suite("nope")
        check("unknown kind raises", False)
    except ValueError:
        check("unknown kind raises", True)


if __name__ == "__main__":
    print("=" * 70)
    print("  DAX GENERATOR TESTS")
    print("=" * 70)
    test_ref_helpers()
    test_time_intelligence()
    test_ratios_ranking_stats()
    test_all_generated_dax_is_lint_clean()
    test_dispatcher()
    print("\n" + "=" * 70)
    if _failures:
        print(f"  {len(_failures)} CHECK(S) FAILED: {', '.join(_failures)}")
        sys.exit(1)
    print("  ALL DAX GENERATOR CHECKS PASSED")
    print("=" * 70)
