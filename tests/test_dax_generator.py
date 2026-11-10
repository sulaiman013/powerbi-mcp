"""
Tests for the bulk DAX measure-suite generator: every kind produces named, formatted,
documented, self-contained measures; inputs are validated; names are escaped; and the output
is clean under the project's own linter and Best Practice Analyzer. No Power BI required.
Run: python tests/test_dax_generator.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))
import dax_generator as dg  # noqa: E402
import dax_lint  # noqa: E402
import dax_tokens  # noqa: E402
import model_analysis  # noqa: E402

_failures = []


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f": {detail}" if detail and not cond else ""))
    if not cond:
        _failures.append(name)


def raises(fn, *needles):
    """True when fn() raises ValueError whose message contains every needle."""
    try:
        fn()
    except ValueError as e:
        return all(n in str(e) for n in needles)
    except Exception:
        return False
    return False


def by_name(suite):
    return {m["name"]: m for m in suite}


def everything():
    return (dg.generate_time_intelligence("Total Sales", "Date.Date", variants=list(dg.TIME_INTELLIGENCE_VARIANTS))
            + dg.generate_time_intelligence("Total Sales", "Date.Date", variants=["ytd", "qtd"], fiscal_year_end="01-31")
            + dg.generate_ratios("Total Sales", ["Product.Category", "Date.Month Name"],
                                 sort_by_columns={"Date.Month Name": "Date.Month No"})
            + dg.generate_ranking("Total Sales", ["Product.Category", "Date.Month Name"],
                                  sort_by_columns={"Date.Month Name": "Date.Month No"})
            + dg.generate_column_stats("Sales.Amount"))


def test_ref_helpers():
    print("\n== reference helpers ==")
    check("Table.Column form", dg.column_ref("Date.Date") == "'Date'[Date]")
    check("quoted-table form", dg.column_ref("'Order Date'.Date") == "'Order Date'[Date]")
    check("Table[Column] form is normalized to a quoted table", dg.column_ref("Sales[Amount]") == "'Sales'[Amount]")
    check("unquoted table with spaces", dg.column_ref("Sales Data[Amount]") == "'Sales Data'[Amount]")
    check("already-quoted DAX form", dg.column_ref("'Sales Data'[Net Amount]") == "'Sales Data'[Net Amount]")
    check("spaces around the dot", dg.column_ref("Date . Date") == "'Date'[Date]")
    check("measure ref bracketing", dg.measure_ref("Total Sales") == "[Total Sales]")
    check("bracketed measure ref passes through", dg.measure_ref(" [Total Sales] ") == "[Total Sales]")
    check("split back", dg.split_column_ref("Date.Date") == ("Date", "Date"))
    check("split a bracket form", dg.split_column_ref("'Order Date'[Month Name]") == ("Order Date", "Month Name"))


def test_escaping():
    print("\n== defect 13: names are escaped through dax_tokens ==")
    check("apostrophe in a dotted table name", dg.column_ref("O'Brien.Col") == "'O''Brien'[Col]", dg.column_ref("O'Brien.Col"))
    check("apostrophe in a bracket-form table name", dg.column_ref("O'Brien[Col]") == "'O''Brien'[Col]")
    check("escaped apostrophe in a quoted table", dg.column_ref("'O''Brien'.Col") == "'O''Brien'[Col]", dg.column_ref("'O''Brien'.Col"))
    check("] in a dotted column name", dg.column_ref("Sales.Amt]x") == "'Sales'[Amt]]x]")
    check("]] in a bracketed column name", dg.split_column_ref("Sales[Amt]]x]") == ("Sales", "Amt]x"))
    check("table name that needs quoting (leading digit)", dg.column_ref("2024Sales[Amt]") == "'2024Sales'[Amt]")
    check("] in a measure name", dg.measure_ref("Sales [Net]") == "[Sales [Net]]]")
    check("escaped measure reference round-trips", dg.measure_ref("[A]]B]") == "[A]]B]" and dg.bare_name("[A]]B]") == "A]B")
    check("Table[Measure] is reduced to the measure", dg.measure_ref("Sales[Total]") == "[Total]"
          and dg.measure_ref("'Sales Data'[Total]") == "[Total]")
    suite = dg.generate_time_intelligence("[A]]B]", "O'Brien.Date", variants=["ytd"])
    check("generated name uses the unescaped measure name", suite[0]["name"] == "A]B YTD", suite[0]["name"])
    check("generated DAX uses the escaped forms",
          suite[0]["expression"] == "CALCULATE([A]]B], DATESYTD('O''Brien'[Date]))", suite[0]["expression"])
    tokens = [t for t in dax_tokens.tokenize(suite[0]["expression"]) if t.kind in (dax_tokens.BRACKET, dax_tokens.QNAME)]
    check("the DAX lexes back to the original names", [t.name for t in tokens] == ["A]B", "O'Brien", "Date"])
    stats = dg.generate_column_stats("'Sales [Net]'[Amt]]x]", stats=["sum"])
    check("stats: brackets in table and column", stats[0]["expression"] == "SUM('Sales [Net]'[Amt]]x])"
          and stats[0]["name"] == "Total Amt]x", str(stats[0]))


def test_validation():
    print("\n== defect 13: invalid input is rejected with the reason ==")
    check("column without a table", raises(lambda: dg.column_ref("Amount"), "has no table"))
    check("column stats on a bare column name", raises(lambda: dg.generate_column_stats("Amount"), "'Amount'", "has no table"))
    check("empty column part", raises(lambda: dg.column_ref("Sales."), "has no column"))
    check("empty table part", raises(lambda: dg.column_ref(".Amount"), "has no table"))
    check("empty brackets", raises(lambda: dg.column_ref("Sales[]"), "has no column"))
    check("unterminated bracket", raises(lambda: dg.column_ref("Sales[Amount"), "not Table[Column]"))
    check("two bracket groups", raises(lambda: dg.column_ref("Sales[Net][Amount]"), "not Table[Column]"))
    check("unterminated quoted table", raises(lambda: dg.column_ref("'Sales.Amount"), "unterminated"))
    check("quoted table without a column", raises(lambda: dg.column_ref("'Sales'"), "no column"))
    check("non-text column", raises(lambda: dg.column_ref(None), "required") and raises(lambda: dg.column_ref(5), "required"))
    check("empty measure name", raises(lambda: dg.generate_time_intelligence("", "Date.Date"), "measure name"))
    check("blank bracketed measure", raises(lambda: dg.measure_ref("[ ]"), "empty"))
    check("two references are not one measure", raises(lambda: dg.measure_ref("[Net] Sales [USD]"), "not a single"))
    check("ratio dimension without a table", raises(lambda: dg.generate_ratios("Total", ["Category"]), "has no table"))
    check("empty dimension list", raises(lambda: dg.generate_ranking("Total", []), "non-empty list"))
    check("empty variants list", raises(lambda: dg.generate_time_intelligence("T", "Date.Date", variants=[]), "non-empty"))
    check("variants given as text", raises(lambda: dg.generate_time_intelligence("T", "Date.Date", variants="ytd"), "list"))
    check("empty stats list", raises(lambda: dg.generate_column_stats("S.A", stats=[]), "non-empty"))
    check("unknown stat", raises(lambda: dg.generate_column_stats("S.A", stats=["mode"]), "Unknown stat"))
    check("empty display folder", raises(lambda: dg.generate_ratios("T", ["P.C"], display_folder=" "), "display_folder"))
    one = dg.generate_ratios("Total", "Product.Category")
    check("a single column passed as text is one column, not its characters", len(one) == 2, str(len(one)))
    dup = dg.generate_time_intelligence("Total", "Date.Date", variants=["ytd", "YTD", "ytd"])
    check("duplicate variants yield one measure", [m["name"] for m in dup] == ["Total YTD"])
    dup = dg.generate_ranking("Total", ["Product.Category", "'Product'[category]"], include_all_selected=False)
    check("the same column twice yields one measure", len(dup) == 1)


def test_time_intelligence():
    print("\n== time intelligence suite ==")
    suite = dg.generate_time_intelligence("Total Sales", "Date.Date")
    names = {m["name"] for m in suite}
    check("default variant count", len(suite) == len(dg.DEFAULT_TI_VARIANTS), str(len(suite)))
    check("YTD named", "Total Sales YTD" in names, str(names))
    check("YoY % named", "Total Sales YoY %" in names)
    ytd = next(m for m in suite if m["name"] == "Total Sales YTD")
    check("YTD expression", ytd["expression"] == "CALCULATE([Total Sales], DATESYTD('Date'[Date]))", ytd["expression"])
    check("YTD description (written as /// in TMDL)", ytd["description"].startswith("Total Sales accumulated"))
    yoy_pct = next(m for m in suite if m["name"] == "Total Sales YoY %")
    check("YoY % gets pct format", "%" in (yoy_pct["format_string"] or ""))
    check("YoY % is self-contained (VAR, no dependency on generated names)",
          "VAR __py" in yoy_pct["expression"] and "[Total Sales PY]" not in yoy_pct["expression"])
    check("every measure documented", all(m["description"] and m["display_folder"] for m in suite))
    # base format inheritance
    suite2 = dg.generate_time_intelligence("Total Sales", "Date.Date", variants=["ytd"], base_format="#,0")
    check("inherit base format", suite2[0]["format_string"] == "#,0")
    check("unknown variant raises", raises(lambda: dg.generate_time_intelligence("X", "Date.Date", variants=["bogus"]),
                                           "Unknown time-intelligence variant"))

    full = by_name(dg.generate_time_intelligence("Total Sales", "Date.Date", variants=list(dg.TIME_INTELLIGENCE_VARIANTS)))
    b, d = "[Total Sales]", "'Date'[Date]"
    exact = {
        "Total Sales QTD": f"CALCULATE({b}, DATESQTD({d}))",
        "Total Sales MTD": f"CALCULATE({b}, DATESMTD({d}))",
        "Total Sales PY": f"CALCULATE({b}, SAMEPERIODLASTYEAR({d}))",
        "Total Sales PM": f"CALCULATE({b}, DATEADD({d}, -1, MONTH))",
        "Total Sales Rolling 12M": f"CALCULATE({b}, DATESINPERIOD({d}, MAX({d}), -12, MONTH))",
        "Total Sales Rolling 3M": f"CALCULATE({b}, DATESINPERIOD({d}, MAX({d}), -3, MONTH))",
    }
    for name, want in exact.items():
        check(f"{name} pattern", full[name]["expression"] == want, full[name]["expression"])
    check("eleven variants", len(full) == 11, str(sorted(full)))


def test_blank_safe_comparisons():
    print("\n== defect 10: YoY / MoM are blank unless both periods have data ==")
    full = by_name(dg.generate_time_intelligence("Total Sales", "Date.Date", variants=["yoy", "yoy_pct", "mom", "mom_pct"]))
    want_yoy_pct = ("VAR __cur = [Total Sales]\n"
                    "VAR __py = CALCULATE([Total Sales], SAMEPERIODLASTYEAR('Date'[Date]))\n"
                    "RETURN\n"
                    "    IF(NOT ISBLANK(__cur) && NOT ISBLANK(__py), DIVIDE(__cur - __py, __py))")
    check("YoY % exact pattern", full["Total Sales YoY %"]["expression"] == want_yoy_pct, full["Total Sales YoY %"]["expression"])
    for name, var in [("Total Sales YoY", "__py"), ("Total Sales YoY %", "__py"),
                      ("Total Sales MoM", "__pm"), ("Total Sales MoM %", "__pm")]:
        expr = full[name]["expression"]
        check(f"{name}: guards the current period and the prior period",
              f"IF(NOT ISBLANK(__cur) && NOT ISBLANK({var})," in expr, expr)
        check(f"{name}: the base measure is evaluated once into a VAR", expr.count("VAR __cur = [Total Sales]") == 1)
    check("YoY returns the difference", full["Total Sales YoY"]["expression"].endswith("__cur - __py)"))
    check("MoM uses DATEADD -1 MONTH", "DATEADD('Date'[Date], -1, MONTH)" in full["Total Sales MoM %"]["expression"])
    check("percent variants use DIVIDE, never '/'", all("/" not in m["expression"] for m in full.values()))
    check("descriptions say when the result is blank", all("blank unless both" in m["description"] for m in full.values()))


def test_formats():
    print("\n== defect 12: every generated measure has a format string ==")
    suite = everything()
    missing = [m["name"] for m in suite if not m.get("format_string")]
    check(f"no measure of {len(suite)} lacks a format string", not missing, str(missing))
    check("every measure has a description and a folder", all(m["description"] and m["display_folder"] for m in suite))
    check("exactly the five documented keys", all(set(m) == {"name", "expression", "format_string", "display_folder", "description"}
                                                  for m in suite))
    ti = by_name(dg.generate_time_intelligence("Total Sales", "Date.Date"))
    check("amount variants default to a number format", ti["Total Sales YTD"]["format_string"] == dg.DEFAULT_NUMBER_FORMAT)
    check("percent variants keep the signed percent format", ti["Total Sales YoY %"]["format_string"] == "+0.0%;-0.0%;0.0%")
    ti = by_name(dg.generate_time_intelligence("Total Sales", "Date.Date", base_format="$#,0"))
    check("base_format reaches every amount variant",
          all(m["format_string"] == "$#,0" for n, m in ti.items() if not n.endswith("%")), str({n: m["format_string"] for n, m in ti.items()}))
    stats = by_name(dg.generate_column_stats("Sales.Amount", column_format="#,0.000"))
    check("column_format reaches the value statistics", stats["Average Amount"]["format_string"] == "#,0.000"
          and stats["Distinct Amount"]["format_string"] == "#,0")
    check("names in a suite are unique", len({m["name"] for m in suite}) == len(suite))


def test_ratios_ranking_stats():
    print("\n== ratios, ranking, column stats ==")
    ratios = dg.generate_ratios("Total Sales", ["Product.Category"])
    check("ratio pair generated", len(ratios) == 2)
    check("pct format on ratios", all(m["format_string"] == "0.0%" for m in ratios))
    check("ALL + ALLSELECTED variants", any("ALLSELECTED" in m["expression"] for m in ratios)
          and any("ALL('Product'[Category])" in m["expression"] for m in ratios))
    check("share of total pattern", ratios[0]["expression"] ==
          "DIVIDE([Total Sales], CALCULATE([Total Sales], ALL('Product'[Category])))", ratios[0]["expression"])
    check("ALLSELECTED can be switched off", len(dg.generate_ratios("T", ["P.C"], include_all_selected=False)) == 1)
    ranks = dg.generate_ranking("Total Sales", ["Product.Category", "Customer.Region"])
    check("two ranks per dimension (all, selected)", len(ranks) == 4, str([m["name"] for m in ranks]))
    check("rank guards totals with HASONEVALUE", all("HASONEVALUE" in m["expression"] for m in ranks))
    stats = dg.generate_column_stats("Sales.Amount", stats=["sum", "avg", "distinct"])
    check("stats subset", {m["name"] for m in stats} == {"Total Amount", "Average Amount", "Distinct Amount"})
    check("stats patterns", [m["expression"] for m in stats] ==
          ["SUM('Sales'[Amount])", "AVERAGE('Sales'[Amount])", "DISTINCTCOUNT('Sales'[Amount])"])


def test_rank_variants():
    print("\n== defect 11: rank among all vs among selected; blank without data ==")
    ranks = by_name(dg.generate_ranking("Total Sales", ["Product.Category"]))
    check("names", sorted(ranks) == ["Total Sales Rank by Category", "Total Sales Rank by Selected Category"], str(sorted(ranks)))
    check("rank among all", ranks["Total Sales Rank by Category"]["expression"] ==
          "IF(\n    HASONEVALUE('Product'[Category]) && NOT ISBLANK([Total Sales]),\n"
          "    RANKX(ALL('Product'[Category]), [Total Sales], , DESC, DENSE)\n)",
          ranks["Total Sales Rank by Category"]["expression"])
    check("rank among selected", "RANKX(ALLSELECTED('Product'[Category]), [Total Sales], , DESC, DENSE)"
          in ranks["Total Sales Rank by Selected Category"]["expression"])
    check("a member without data is not ranked", all("NOT ISBLANK([Total Sales])" in m["expression"] for m in ranks.values()))
    check("integer format", all(m["format_string"] == "#,0" for m in ranks.values()))
    check("selected variant can be switched off",
          len(dg.generate_ranking("Total Sales", ["Product.Category"], include_all_selected=False)) == 1)


def test_sort_by_columns():
    print("\n== defect 11: the sort-by column is cleared together with the column ==")
    sort = {"Date.Month Name": "'Date'[Month No]"}
    ratios = by_name(dg.generate_ratios("Total Sales", ["Date.Month Name", "Product.Category"], sort_by_columns=sort))
    check("ALL clears both columns", ratios["Total Sales % of Total Month Name"]["expression"] ==
          "DIVIDE([Total Sales], CALCULATE([Total Sales], ALL('Date'[Month Name], 'Date'[Month No])))",
          ratios["Total Sales % of Total Month Name"]["expression"])
    check("ALLSELECTED clears both columns", "ALLSELECTED('Date'[Month Name], 'Date'[Month No])"
          in ratios["Total Sales % of Selected Month Name"]["expression"])
    check("a column without a sort-by column is unchanged", "ALL('Product'[Category])"
          in ratios["Total Sales % of Total Category"]["expression"])
    ranks = by_name(dg.generate_ranking("Total Sales", ["Date.Month Name"], sort_by_columns=sort))
    check("rank iterates both columns", "RANKX(ALL('Date'[Month Name], 'Date'[Month No]), [Total Sales], , DESC, DENSE)"
          in ranks["Total Sales Rank by Month Name"]["expression"], ranks["Total Sales Rank by Month Name"]["expression"])
    check("rank still tests the dimension column itself", "HASONEVALUE('Date'[Month Name])"
          in ranks["Total Sales Rank by Month Name"]["expression"])
    check("mapping keys match case-insensitively and in any spelling",
          "ALL('Date'[Month Name], 'Date'[Month No])" in dg.generate_ratios(
              "T", ["'Date'[Month Name]"], sort_by_columns={"date.month name": "Date.Month No"})[0]["expression"])
    check("a mapping for other columns is ignored",
          dg.generate_ratios("T", ["Product.Category"], sort_by_columns=sort)[0]["expression"].count("ALL('Product'[Category])") == 1)
    check("a column sorted by itself adds nothing",
          "ALL('Date'[Month])" in dg.generate_ratios("T", ["Date.Month"], sort_by_columns={"Date.Month": "Date.Month"})[0]["expression"])
    check("sort column in another table is rejected",
          raises(lambda: dg.generate_ratios("T", ["Date.Month"], sort_by_columns={"Date.Month": "Other.No"}), "same table"))
    check("a non-mapping is rejected", raises(lambda: dg.generate_ranking("T", ["Date.Month"], sort_by_columns=["x"]), "must map"))


def test_model_lookups():
    print("\n== helpers that read sort-by columns and base formats from a gathered model ==")
    model = {"tables": [
        {"name": "Date", "columns": [{"name": "Month Name", "sort_by": "Month No"}, {"name": "Month No", "sort_by": None},
                                     {"name": "Day]Name", "sort_by": "Day No"}, {"name": "Self", "sort_by": "self"},
                                     None, {"name": None, "sort_by": "x"}],
         "measures": []},
        {"name": "O'Brien", "columns": [{"name": "Label", "sort_by": "Order"}],
         "measures": [{"name": "Total Sales", "format_string": "$#,0"}, {"name": "No Format", "format_string": ""}, None]},
        None, {"name": None, "columns": [{"name": "a", "sort_by": "b"}]}]}
    mapping = dg.sort_by_columns_from_model(model)
    check("every sorted column is mapped, escaped", mapping == {
        "'Date'[Month Name]": "'Date'[Month No]", "'Date'[Day]]Name]": "'Date'[Day No]",
        "'O''Brien'[Label]": "'O''Brien'[Order]"}, str(mapping))
    ratios = dg.generate_ratios("Total Sales", ["Date.Month Name"], sort_by_columns=mapping)
    check("the mapping feeds straight into the generators",
          "ALL('Date'[Month Name], 'Date'[Month No])" in ratios[0]["expression"])
    check("base format lookup (any spelling, any case)", dg.measure_format_from_model(model, "[total sales]") == "$#,0")
    check("no format / unknown measure / bad input give None", dg.measure_format_from_model(model, "No Format") is None
          and dg.measure_format_from_model(model, "Nope") is None and dg.measure_format_from_model(model, "") is None
          and dg.measure_format_from_model({}, "x") is None and dg.sort_by_columns_from_model(None) == {})
    suite = dg.generate_time_intelligence("Total Sales", "Date.Date", variants=["ytd"],
                                          base_format=dg.measure_format_from_model(model, "Total Sales"))
    check("the looked-up format is inherited", suite[0]["format_string"] == "$#,0")


def test_fiscal_year():
    print("\n== defect 13: optional fiscal year end ==")
    june = by_name(dg.generate_time_intelligence("Total Sales", "Date.Date", variants=["ytd", "qtd", "mtd", "py"],
                                                 fiscal_year_end="06-30"))
    check("fiscal YTD passes the year end to DATESYTD", june["Total Sales FYTD"]["expression"] ==
          'CALCULATE([Total Sales], DATESYTD(\'Date\'[Date], "06-30"))', str(sorted(june)))
    check("fiscal quarters equal calendar quarters for a June year end", june["Total Sales FQTD"]["expression"] ==
          "CALCULATE([Total Sales], DATESQTD('Date'[Date]))")
    check("fiscal measures say so", "fiscal year" in june["Total Sales FYTD"]["description"]
          and "06-30" in june["Total Sales FYTD"]["description"])
    check("MTD and PY are unaffected", {"Total Sales MTD", "Total Sales PY"} <= set(june))
    jan = by_name(dg.generate_time_intelligence("Total Sales", "Date.Date", variants=["qtd"], fiscal_year_end=1))
    check("off-calendar fiscal quarter computes its own start (year ends January -> quarters start Feb/May/Aug/Nov)",
          jan["Total Sales FQTD"]["expression"] ==
          "VAR __last = MAX('Date'[Date])\n"
          "VAR __into = MOD(MONTH(__last) - 2, 3)\n"
          "VAR __start = DATE(YEAR(__last), MONTH(__last) - __into, 1)\n"
          "RETURN\n"
          "    IF(NOT ISBLANK(__last), CALCULATE([Total Sales], DATESBETWEEN('Date'[Date], __start, __last)))",
          jan["Total Sales FQTD"]["expression"])
    for spelling in ("06-30", "6/30", "6-30", (6, 30), [6, 30], 6):
        check(f"year end spelled {spelling!r}", dg.parse_fiscal_year_end(spelling) == (6, 30))
    check("31 December is the calendar year", dg.parse_fiscal_year_end("12-31") is None
          and dg.generate_time_intelligence("T", "D.D", variants=["ytd"], fiscal_year_end=12)[0]["name"] == "T YTD")
    check("February month number means the 28th", dg.parse_fiscal_year_end(2) == (2, 28))
    for bad in ("13-01", "06-31", "June", "0630", 0, 13, True, 6.5, (6,)):
        check(f"invalid year end {bad!r} is rejected", raises(lambda b=bad: dg.parse_fiscal_year_end(b), "fiscal_year_end"))
    check("fiscal QTD needs a month-end year end",
          raises(lambda: dg.generate_time_intelligence("T", "D.D", variants=["qtd"], fiscal_year_end="06-15"), "last day of a month"))
    check("fiscal YTD accepts a mid-month year end",
          '"06-15"' in dg.generate_time_intelligence("T", "D.D", variants=["ytd"], fiscal_year_end="06-15")[0]["expression"])


def test_all_generated_dax_is_lint_clean():
    print("\n== every generated expression is clean under dax_lint and the BPA ==")
    all_measures = everything()
    res = dax_lint.lint_measures([{"name": m["name"], "expression": m["expression"]} for m in all_measures])
    check(f"no finding of any severity across {len(all_measures)} measures", res["summary"]["total"] == 0,
          str([(f['object'], f['rule_id']) for f in res["findings"]]))
    nasty = (dg.generate_time_intelligence("Net -- Adj [x]", "Dates//Old.Date", variants=list(dg.TIME_INTELLIGENCE_VARIANTS))
             + dg.generate_ratios('6" Pipe / Unit', ["O'Brien.Cat/Sub"]))
    res = dax_lint.lint_measures([{"name": m["name"], "expression": m["expression"]} for m in nasty])
    check("still clean when names hold --, //, quotes, slashes and brackets", res["summary"]["total"] == 0,
          str([(f['object'], f['rule_id']) for f in res["findings"]]))

    def c(name, data_type="int64"):
        return {"name": name, "data_type": data_type, "is_hidden": False, "description": "d", "sort_by": None}
    model = {"tables": [
        {"name": "Sales", "description": "d", "is_hidden": False,
         "columns": [c("Amount", "decimal"), c("DateKey"), c("ProductKey")],
         "measures": [{"name": "Total Sales", "expression": "SUMX('Sales', 'Sales'[Amount] * 1.1)", "format_string": "#,0",
                       "description": "d", "display_folder": "Base", "is_hidden": False}] + all_measures},
        {"name": "Date", "description": "d", "is_hidden": False,
         "columns": [dict(c("Date", "dateTime"), is_key=True), dict(c("Month Name", "string"), sort_by="Month No"), c("Month No")],
         "measures": []},
        {"name": "Product", "description": "d", "is_hidden": False,
         "columns": [c("ProductKey"), c("Category", "string")], "measures": []}],
        "relationships": []}
    bpa = model_analysis.run_bpa(model, categories=["DAX", "Formatting", "Error Prevention", "Naming"])
    check("the project's own BPA has nothing to say about the suite (no missing formats, no broken references)",
          bpa["findings"] == [], str([(f["rule_id"], f["object"]) for f in bpa["findings"]]))
    rewritten = [m["name"] for m in all_measures if dax_lint.rewrite_expression(m["expression"])["changed"]]
    check("nothing for the rewriter to do", rewritten == [])


def test_dispatcher():
    print("\n== dispatcher ==")
    s = dg.generate_suite("ti", base_measure="X", date_column="D.D", variants=["ytd"])
    check("alias kind works", len(s) == 1 and s[0]["name"] == "X YTD")
    check("unknown kind raises", raises(lambda: dg.generate_suite("nope"), "Unknown suite kind"))
    check("None kind raises", raises(lambda: dg.generate_suite(None), "Unknown suite kind"))
    check("missing argument is a ValueError", raises(lambda: dg.generate_suite("ti", base_measure="X"), "date_column"))
    check("unexpected argument is a ValueError", raises(lambda: dg.generate_suite("stats", column="S.A", bogus=1), "bogus"))
    check("new optional parameters reach the generators",
          "FYTD" in dg.generate_suite("time_intelligence", base_measure="X", date_column="D.D", variants=["ytd"],
                                      fiscal_year_end="03-31")[0]["name"]
          and "ALL('D'[M], 'D'[N])" in dg.generate_suite("ratios", base_measure="X", dimension_columns=["D.M"],
                                                         sort_by_columns={"D.M": "D.N"})[0]["expression"])


if __name__ == "__main__":
    print("=" * 70)
    print("  DAX GENERATOR TESTS")
    print("=" * 70)
    test_ref_helpers()
    test_escaping()
    test_validation()
    test_time_intelligence()
    test_blank_safe_comparisons()
    test_formats()
    test_ratios_ranking_stats()
    test_rank_variants()
    test_sort_by_columns()
    test_model_lookups()
    test_fiscal_year()
    test_all_generated_dax_is_lint_clean()
    test_dispatcher()
    print("\n" + "=" * 70)
    if _failures:
        print(f"  {len(_failures)} CHECK(S) FAILED: {', '.join(_failures)}")
        sys.exit(1)
    print("  ALL DAX GENERATOR CHECKS PASSED")
    print("=" * 70)
