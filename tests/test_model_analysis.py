"""
Tests for the pure model-analysis engine: BPA rules, AI-readiness, the data dictionary, the
semantic model diff and the DAX test verdict. No Power BI needed.

Run: python tests/test_model_analysis.py
"""
import copy
import os
import sys
from datetime import date, datetime
from decimal import Decimal

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))
import dax_lint  # noqa: E402
import model_analysis  # noqa: E402
from model_analysis import (run_bpa, audit_ai_readiness, render_data_dictionary, diff_models,  # noqa: E402
                            dax_test_verdict)

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


def hits(model, rule_id):
    return [f["object"] for f in run_bpa(model)["findings"] if f["rule_id"] == rule_id]


def col(name, data_type="int64", **kw):
    c = {"name": name, "data_type": data_type, "is_hidden": False, "description": "d"}
    c.update(kw)
    return c


def meas(name, expression, **kw):
    m = {"name": name, "expression": expression, "format_string": "0", "description": "d", "is_hidden": False}
    m.update(kw)
    return m


def table(name, columns=(), measures=(), **kw):
    t = {"name": name, "is_hidden": False, "description": "d", "columns": list(columns), "measures": list(measures)}
    t.update(kw)
    return t


def rel(ft, fc, tt, tc, **kw):
    r = {"from_table": ft, "from_column": fc, "to_table": tt, "to_column": tc, "is_active": True,
         "cross_filter": "OneDirection", "from_cardinality": "Many", "to_cardinality": "One"}
    r.update(kw)
    return r


def one_measure(expression, **kw):
    return {"tables": [table("T", [col("x"), col("y")], [meas("M", expression, **kw)])], "relationships": []}


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
    check("no rule crashed", res["summary"]["rule_errors"] == [], str(res["summary"]["rule_errors"]))
    check("every finding has the documented keys",
          all(set(x) == {"rule_id", "name", "category", "severity", "object", "detail"} for x in f))

    # category + severity filters
    only_dax = run_bpa(MODEL, categories=["DAX"])
    check("category filter works", all(x["category"] == "DAX" for x in only_dax["findings"]) and only_dax["findings"])
    warns = run_bpa(MODEL, min_severity="warning")
    check("severity filter works", all(x["severity"] in ("warning", "error") for x in warns["findings"]))


def test_rules_run_reflects_filters():
    print("\n== minor: rules_run counts the rules that ran ==")
    all_rules = model_analysis.DEFAULT_BPA_RULES
    n_err = sum(1 for r in all_rules if r["severity"] == "error")
    n_dax = sum(1 for r in all_rules if r["category"] == "DAX")
    check("no filter runs every rule", run_bpa(MODEL)["summary"]["rules_run"] == len(all_rules))
    check("min_severity=error runs only error rules", run_bpa(MODEL, min_severity="error")["summary"]["rules_run"] == n_err,
          str(run_bpa(MODEL, min_severity="error")["summary"]["rules_run"]))
    check("category filter counts its rules", run_bpa(MODEL, categories=["dax"])["summary"]["rules_run"] == n_dax)
    check("both filters combine", run_bpa(MODEL, categories=["Naming"], min_severity="error")["summary"]["rules_run"] == 1)
    check("an explicit empty rule list runs nothing", run_bpa(MODEL, rules=[])["summary"] ==
          {"total": 0, "by_severity": {"error": 0, "warning": 0, "info": 0}, "by_category": {}, "rules_run": 0,
           "rule_errors": []})
    check("rule ids are unique", len({r["id"] for r in all_rules}) == len(all_rules))

    def boom(_model):
        raise RuntimeError("bad rule")
    broken = [{"id": "X", "category": "Test", "severity": "error", "name": "x", "check": boom}]
    res = run_bpa(MODEL, rules=broken)
    check("a crashing rule is reported, not raised", res["summary"]["rule_errors"] == [{"rule_id": "X", "error": "bad rule"}])
    check("a crashing rule never becomes an error finding", res["summary"]["by_severity"]["error"] == 0)
    check("a crashing rule respects min_severity", run_bpa(MODEL, rules=broken, min_severity="warning")["findings"] == [])


def test_trailing_space_on_tables():
    print("\n== defect 2: NAMING_TRAILING_SPACE covers tables ==")
    m = {"tables": [table("Sales ", [col(" Amt"), col("K")]), table(" Dim", [col("K"), col("N")])],
         "relationships": [rel("Sales ", "K", " Dim", "K")]}
    found = hits(m, "NAMING_TRAILING_SPACE")
    check("both tables and the column are flagged", len(found) == 3, str(found))
    check("the whitespace is visible in the finding", "'Sales '" in found and "' Dim'" in found, str(found))
    check("clean names are not flagged", hits({"tables": [table("Sales", [col("Amt")])]}, "NAMING_TRAILING_SPACE") == [])


def test_dax_rules_agree_with_linter():
    print("\n== defect 3: the BPA DAX rules are the linter's rules ==")
    cases = ['SUM(T[x]) /* total */', 'FORMAT([d], "MM/DD/YYYY")', 'SUM(T[x]) -- a/b', 'SUM(T[x]) // a/b',
             'DIVIDE(T[x], T[y]) + T[x] / T[y]', 'T[x] / T[y] + DIVIDE (T[x], T[y])', 'T[x] / 100', 'T[x] / T[y]']
    for expr in cases:
        bpa = bool(hits(one_measure(expr), "DAX_USE_DIVIDE"))
        lint = any(f["rule_id"] == "DL003" for f in dax_lint.lint_expression("M", expr))
        check(f"DAX_USE_DIVIDE == DL003 for {expr}", bpa == lint, f"bpa={bpa} lint={lint}")
    check("a block comment is not a division", hits(one_measure('SUM(T[x]) /* total */'), "DAX_USE_DIVIDE") == [])
    check("a division next to a DIVIDE call is still reported",
          hits(one_measure('DIVIDE(T[x], T[y]) + T[x] / T[y]'), "DAX_USE_DIVIDE") == ["T[M]"])
    check("IFERROR with a space before the parenthesis", hits(one_measure("IFERROR (T[x], 0)"), "DAX_AVOID_IFERROR") == ["T[M]"])
    check("IFERROR inside a string is not a call", hits(one_measure('"iferror(" & T[x]'), "DAX_AVOID_IFERROR") == [])
    check("IFERROR inside a comment is not a call", hits(one_measure("SUM(T[x]) -- was IFERROR(x)"), "DAX_AVOID_IFERROR") == [])
    check("FILTER over a whole table", hits(one_measure("CALCULATE(SUM(T[x]), FILTER(T, T[y] = 1))"), "DAX_FILTER_WHOLE_TABLE") == ["T[M]"])
    check("SUMMARIZE extension column", hits(one_measure('COUNTROWS(SUMMARIZE(T, T[x], "s", SUM(T[y])))'), "DAX_SUMMARIZE_EXTENSION") == ["T[M]"])
    check("adding zero", hits(one_measure("SUM(T[x]) + 0"), "DAX_AVOID_ADD_ZERO") == ["T[M]"])
    check("the detail names the lint rule and the line",
          "DL003, line 2" in [f["detail"] for f in run_bpa(one_measure("VAR a = 1\nRETURN T[x] / T[y]"))["findings"]
                              if f["rule_id"] == "DAX_USE_DIVIDE"][0])
    calc = {"tables": [table("T", [col("x"), col("r", expression="T[x] / T[x]", is_calculated=True)])]}
    check("calculated columns are linted too", hits(calc, "DAX_USE_DIVIDE") == ["T[r]"])


def test_dax_model_rules():
    print("\n== DAX rules that need the model ==")
    m = {"tables": [table("T", [col("x"), col("y")], [
        meas("A", "SUM(T[x])"), meas("B", "sum( T[x] )  -- same"), meas("C", "[A]"), meas("D", "SUM(T[y])"),
        meas("E", "SUMX(T, [x] * T[y])"), meas("F", "T[A] + 1"),
        meas("G", 'SUMX(ADDCOLUMNS(T, "@v", T[x]), [@v]) + SUMX({1, 2}, [Value])')])]}
    check("duplicate definitions (ignoring case, spacing, comments)", hits(m, "DAX_DUPLICATE_DEFINITION") == ["T[A]"],
          str(hits(m, "DAX_DUPLICATE_DEFINITION")))
    check("direct measure reference", hits(m, "DAX_DIRECT_MEASURE_REFERENCE") == ["T[C]"])
    check("unqualified column reference", hits(m, "DAX_UNQUALIFIED_COLUMN") == ["T[E]"], str(hits(m, "DAX_UNQUALIFIED_COLUMN")))
    check("table-qualified measure reference", hits(m, "DAX_QUALIFIED_MEASURE") == ["T[F]"])
    check("extension and constructor columns are not model columns", hits(m, "ERR_BROKEN_REFERENCE") == [],
          str(run_bpa(m, categories=["Error Prevention"])["findings"]))
    info = one_measure("COUNTROWS(FILTER(INFO.VIEW.MEASURES(), [IsHidden] && [Table] = \"T\")) + SUM(T[x])")
    check("columns of INFO.* tables are not judged against the model", hits(info, "ERR_BROKEN_REFERENCE") == []
          and hits(info, "DAX_UNQUALIFIED_COLUMN") == [])
    info_bad = one_measure("COUNTROWS(INFO.VIEW.MEASURES()) + SUM(T[nope])")
    check("...but a qualified reference next to them still is", hits(info_bad, "ERR_BROKEN_REFERENCE") == ["T[M]"])


def test_error_rules():
    print("\n== error-severity rules: a broken model fails the gate ==")
    bad = {"tables": [
        table("Sales", [col("Amount", "double"), col("OrderDate", "dateTime"), col("ShipDate", "dateTime"),
                        col("Bad\tName")],
              [meas("Good", "SUM(Sales[Amount])"),
               meas("Gone Column", "SUM(Sales[Amuont])"),
               meas("Gone Table", "COUNTROWS('Old Sales') + SUM(Orders[Qty])"),
               meas("Gone Measure", "[Good] + [Deleted Measure]"),
               meas("Ship", "CALCULATE([Good], USERELATIONSHIP(Sales[ShipDate], 'Date'[Date]))"),
               meas("No Rel", "CALCULATE([Good], USERELATIONSHIP(Sales[Amount], 'Date'[Date]))"),
               meas("Empty", ""),
               meas("Levels", "CALCULATE([Good], 'Date'[Date].[Year] = 2024)")]),
        table("Date", [col("Date", "dateTime", is_key=True), col("Year")])],
        "relationships": [rel("Sales", "OrderDate", "Date", "Date"),
                          rel("Sales", "ShipDate", "Date", "Date", is_active=False)]}
    res = run_bpa(bad, min_severity="error")
    by_rule = {}
    for f in res["findings"]:
        by_rule.setdefault(f["rule_id"], []).append(f["object"])
    check("broken column / table / measure references", by_rule.get("ERR_BROKEN_REFERENCE") ==
          ["Sales[Gone Column]", "Sales[Gone Table]", "Sales[Gone Measure]"], str(by_rule.get("ERR_BROKEN_REFERENCE")))
    details = " ".join(f["detail"] for f in res["findings"] if f["rule_id"] == "ERR_BROKEN_REFERENCE")
    check("the detail names what is missing", all(s in details for s in
          ("Sales[Amuont]", "table 'Old Sales'", "table 'Orders'", "[Deleted Measure]")), details)
    check("hierarchy level syntax is not a broken reference", "Sales[Levels]" not in by_rule.get("ERR_BROKEN_REFERENCE", []))
    check("USERELATIONSHIP without a relationship", by_rule.get("ERR_USERELATIONSHIP_MISSING") == ["Sales[No Rel]"],
          str(by_rule.get("ERR_USERELATIONSHIP_MISSING")))
    check("measure without an expression", by_rule.get("ERR_MEASURE_NO_EXPRESSION") == ["Sales[Empty]"])
    check("control character in a name", len(by_rule.get("ERR_INVALID_NAME_CHARS", [])) == 1
          and "\\t" in by_rule["ERR_INVALID_NAME_CHARS"][0], str(by_rule.get("ERR_INVALID_NAME_CHARS")))
    check("several distinct error rules fire", len(by_rule) >= 4 and res["summary"]["by_severity"]["error"] >= 6,
          str(res["summary"]))

    good = copy.deepcopy(bad)
    good["tables"][0]["columns"] = [c for c in good["tables"][0]["columns"] if c["name"] != "Bad\tName"]
    good["tables"][0]["measures"] = [m for m in good["tables"][0]["measures"] if m["name"] in ("Good", "Ship", "Levels")]
    check("the repaired model has no errors", run_bpa(good, min_severity="error")["findings"] == [],
          str(run_bpa(good, min_severity="error")["findings"]))

    print("\n== error rules stay silent when the metadata to judge is missing ==")
    blank = {"tables": [table("T", [col("x")], [meas("A", ""), meas("B", None)])]}
    check("all expressions blank (no admin rights) is not an error", hits(blank, "ERR_MEASURE_NO_EXPRESSION") == [])
    no_rels = {"tables": [table("T", [col("x")], [meas("A", "CALCULATE(1, USERELATIONSHIP(T[x], U[x]))")]),
                          table("U", [col("x")])]}
    check("no relationship metadata: USERELATIONSHIP is not judged", hits(no_rels, "ERR_USERELATIONSHIP_MISSING") == [])
    no_cols = {"tables": [table("T", [], [meas("A", "SUM(T[x]) + [x]")])]}
    check("no column metadata: references are not judged", hits(no_cols, "ERR_BROKEN_REFERENCE") == [])
    check("inactive relationship that is activated is fine", hits(good, "ERR_INACTIVE_REL_UNUSED") == [])
    unused = copy.deepcopy(good)
    unused["tables"][0]["measures"] = [m for m in unused["tables"][0]["measures"] if m["name"] != "Ship"]
    check("inactive relationship nobody activates", len(hits(unused, "ERR_INACTIVE_REL_UNUSED")) == 1)


def test_relationship_rules():
    print("\n== relationship and key rules ==")
    m = {"tables": [
        table("Sales", [col("CustomerKey", summarize_by="Sum"), col("Price", "double"), col("Code", "string"),
                        col("HiddenKey", is_hidden=True, summarize_by="Sum"), col("Amount", "decimal", summarize_by="Sum")]),
        table("Customer", [col("CustomerKey", is_key=True, summarize_by="None"), col("Name", "string")]),
        table("Price", [col("Price", "double"), col("Band", "string")]),
        table("Codes", [col("Code", "string"), col("Text", "string")]),
        table("Other", [col("HiddenKey"), col("V")]),
        table("Budget", [col("CustomerKey"), col("Budget", "decimal")])],
        "relationships": [
            rel("Sales", "CustomerKey", "Customer", "CustomerKey"),
            rel("Sales", "Price", "Price", "Price"),
            rel("Sales", "Code", "Codes", "Code"),
            rel("Sales", "HiddenKey", "Other", "HiddenKey"),
            rel("Budget", "CustomerKey", "Sales", "CustomerKey", from_cardinality="Many", to_cardinality="Many",
                cross_filter="BothDirections")]}
    check("many-to-many flagged once", hits(m, "PERF_MANY_TO_MANY") == ["Budget[CustomerKey] -> Sales[CustomerKey]"])
    m2m = [f["detail"] for f in run_bpa(m)["findings"] if f["rule_id"] == "PERF_MANY_TO_MANY"][0]
    check("many-to-many detail mentions the bidirectional filter", "bidirectional" in m2m)
    check("float key on a relationship", hits(m, "PERF_REL_FLOAT_KEY") == ["Sales[Price] -> Price[Price]"])
    check("text key on a relationship", hits(m, "PERF_REL_TEXT_KEY") == ["Sales[Code] -> Codes[Code]"])
    check("visible foreign keys (hidden ones are fine, many-to-many is not a foreign key)",
          hits(m, "FORMAT_VISIBLE_FOREIGN_KEY") == ["Sales[CustomerKey]", "Sales[Price]", "Sales[Code]"],
          str(hits(m, "FORMAT_VISIBLE_FOREIGN_KEY")))
    check("summarized key column (not hidden keys, not plain numeric columns)",
          hits(m, "FORMAT_KEY_SUMMARIZED") == ["Sales[CustomerKey]"], str(hits(m, "FORMAT_KEY_SUMMARIZED")))
    for label, dtype, expect in [("Double", "Double", True), ("Decimal Number", "Decimal Number", True),
                                 ("Decimal (fixed)", "Decimal", False), ("Int64", "Int64", False)]:
        mm = {"tables": [table("T", [col("C", dtype)])]}
        check(f"float rule on type {label}", bool(hits(mm, "PERF_FLOAT_COLUMN")) is expect)


def test_structure_rules():
    print("\n== structure rules ==")
    auto = {"tables": [table("Sales", [col("D", "dateTime"), col("x")]),
                       table("LocalDateTable_0b1c", [col("Date", "dateTime")], is_hidden=True),
                       table("DateTableTemplate_9f", [col("Date", "dateTime")], is_hidden=True)],
            "relationships": [rel("Sales", "D", "LocalDateTable_0b1c", "Date")]}
    found = [f for f in run_bpa(auto)["findings"] if f["rule_id"] == "PERF_AUTO_DATE_TIME"]
    check("auto date/time reported once for the model", len(found) == 1 and "2 auto date/time" in found[0]["detail"], str(found))

    calc = {"tables": [table("Customer", [
        col("Key"), col("Total", expression="CALCULATE(SUM(Sales[Amount]))", is_calculated=True),
        col("Orders", expression="COUNTROWS(RELATEDTABLE(Sales))", is_calculated=True),
        col("Capped", expression="MIN(Customer[Key], 10)", is_calculated=True),
        col("Label", "string", expression='Customer[Key] & "-x"', is_calculated=True)]),
        table("Sales", [col("Key"), col("Amount", "decimal")])]}
    check("aggregating calculated columns (scalar MIN(a, b) is not one)",
          hits(calc, "PERF_CALC_COLUMN_AGGREGATE") == ["Customer[Total]", "Customer[Orders]"],
          str(hits(calc, "PERF_CALC_COLUMN_AGGREGATE")))

    many = {"tables": [table("M", [col("c")], [meas(f"m{i}", str(i), display_folder="F" if i < 3 else "") for i in range(12)]),
                       table("Few", [col("c")], [meas("only", "1", display_folder="")])]}
    found = [f for f in run_bpa(many)["findings"] if f["rule_id"] == "MAINT_MEASURE_NO_FOLDER"]
    check("unfoldered measures in a large table, one finding per table",
          len(found) == 1 and found[0]["object"] == "M" and "9 of 12" in found[0]["detail"], str(found))

    months = {"tables": [table("Date", [col("Month Name", "string", sort_by=None), col("Month", "string", sort_by="Month No"),
                                        col("Months Open", "string", sort_by=None), col("Month No", sort_by=None),
                                        col("MonthLabel", "string")])]}
    check("unsorted text month column (sorted, numeric, 'months' and unknown metadata are skipped)",
          hits(months, "FORMAT_MONTH_NOT_SORTED") == ["Date[Month Name]"], str(hits(months, "FORMAT_MONTH_NOT_SORTED")))

    def date_model(name, is_key):
        return {"tables": [table("Sales", [col("D", "dateTime"), col("x")]),
                           table(name, [col("Date", "dateTime", is_key=is_key), col("Year")])],
                "relationships": [rel("Sales", "D", name, "Date")]}
    check("unmarked date table", hits(date_model("DateDim", False), "MAINT_DATE_TABLE_NOT_MARKED") == ["DateDim"])
    check("marked date table", hits(date_model("Calendar", True), "MAINT_DATE_TABLE_NOT_MARKED") == [])
    check("'Update Log' is not a date table name", hits(date_model("Update Log", False), "MAINT_DATE_TABLE_NOT_MARKED") == [])
    check("no key metadata at all: not judged", hits(date_model("DateDim", None), "MAINT_DATE_TABLE_NOT_MARKED") == [])


def test_table_no_relationship():
    print("\n== defect 5: MAINT_TABLE_NO_REL ignores measure and parameter tables ==")
    row_number = "RowNumber-2662979B-1795-4F74-8F37-6A1BA8059B61"
    m = {"tables": [
        table("Sales", [col("K"), col("Amount")]), table("Dim", [col("K"), col("Name", "string")]),
        table("_Measures", [col("Col1", is_hidden=True)], [meas("A", "1")]),
        table("Param", [col(row_number, is_hidden=True), col("Value")]),
        table("Orphan", [col("a"), col("b")]),
        table("HiddenHelper", [col("a"), col("b")], is_hidden=True)],
        "relationships": [rel("Sales", "K", "Dim", "K")]}
    check("only the real orphan table is flagged", hits(m, "MAINT_TABLE_NO_REL") == ["Orphan"], str(hits(m, "MAINT_TABLE_NO_REL")))
    check("the internal RowNumber column is not a column anywhere",
          not any(row_number in (f["object"] or "") for f in run_bpa(m)["findings"]))
    check("a one-table model has nothing to relate", hits({"tables": [table("Flat", [col("a"), col("b")])]}, "MAINT_TABLE_NO_REL") == [])
    two = {"tables": [table("A", [col("a"), col("b")]), table("B", [col("a"), col("b")])]}
    check("two unrelated tables are both flagged", hits(two, "MAINT_TABLE_NO_REL") == ["A", "B"])


def test_malformed_input():
    print("\n== malformed metadata is tolerated, not raised ==")
    junk = {"tables": [None, "x", {"name": None, "columns": None, "measures": None},
                       {"name": "T", "description": [], "columns": [None, 5, {"name": None, "data_type": 6, "expression": 5},
                                                                     {"name": "c", "expression": ["not", "text"]}],
                        "measures": [None, {"name": "m", "expression": 5, "format_string": 0}, {"name": 7, "expression": "[c] / 2"}]}],
            "relationships": [None, {"from_table": None}, {"from_table": "T", "from_column": 1, "to_table": 2, "to_column": None}]}
    for label, fn in [("run_bpa", lambda: run_bpa(junk)), ("audit_ai_readiness", lambda: audit_ai_readiness(junk)),
                      ("render markdown", lambda: render_data_dictionary(junk)),
                      ("render html", lambda: render_data_dictionary(junk, "html")),
                      ("diff_models", lambda: diff_models(junk, MODEL)), ("diff_models reversed", lambda: diff_models(MODEL, junk)),
                      ("None lists", lambda: run_bpa({"tables": None, "relationships": None}))]:
        try:
            result, ok = fn(), True
        except Exception as e:  # noqa: BLE001 - the test reports the crash
            result, ok = repr(e), False
        check(f"{label} does not raise", ok, str(result)[:120])
    check("no rule crashed on it", run_bpa(junk)["summary"]["rule_errors"] == [], str(run_bpa(junk)["summary"]["rule_errors"]))
    check("a non-text expression is 'no expression'", "(unavailable)" in render_data_dictionary(junk))
    check("an empty-list description is not a description", audit_ai_readiness(junk)["metrics"]["tables_with_description_pct"] == 0)


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
    check("fully documented model is 100 / A", audit_ai_readiness(good)["score"] == 100.0 and audit_ai_readiness(good)["grade"] == "A")

    print("\n== minor: an empty model is not 100 / A ==")
    empty = audit_ai_readiness({})
    check("empty model scores 0 / F", empty["score"] == 0.0 and empty["grade"] == "F", str(empty))
    check("empty model says why", "no visible tables" in empty["recommendations"][0])
    check("empty model percentages are 0, not 100", all(v == 0 for k, v in empty["metrics"].items() if k.endswith("_pct")))
    bare = audit_ai_readiness({"tables": [{"name": "T"}]})
    check("an undocumented, empty table earns nothing", bare["score"] == 0.0, str(bare))
    no_measures = audit_ai_readiness({"tables": [table("T", [col("c")])]})
    check("no measures: scored on what exists, with a recommendation",
          no_measures["score"] == 100.0 and any("explicit measures" in x for x in no_measures["recommendations"]),
          str(no_measures))
    hidden = audit_ai_readiness({"tables": [table("T", [col("c")], [meas("m", "1")]),
                                            table("H", [col("c", description="")], description="", is_hidden=True)]})
    check("hidden tables and their columns are not assessed", hidden["score"] == 100.0 and hidden["metrics"]["tables_total"] == 1,
          str(hidden))


def test_data_dictionary():
    print("\n== render_data_dictionary ==")
    md = render_data_dictionary(MODEL, fmt="markdown")
    check("has title", md.startswith("# Data Dictionary"))
    check("includes a table heading", "## Sales" in md)
    check("includes measures section", "### Measures" in md and "Total Sales" in md)
    check("includes coverage score", "AI-readiness score" in md)
    check("includes relationships", "## Relationships" in md and "DateKey" in md)
    check("single-line expression stays in the table", "| `SUM(Sales[Amount])` |" in md)
    html = render_data_dictionary(MODEL, fmt="html")
    check("html wrapper", html.startswith("<!doctype html>"))
    check("html format name is case-insensitive", render_data_dictionary(MODEL, fmt="HTML").startswith("<!doctype html>"))

    print("\n== minor: multi-line expressions, escaping ==")
    multi = "VAR a = 1\nRETURN\n    IF(a < 2 && a > 0, a || `x`)"
    x = {"tables": [{"name": "T<script>alert(1)</script>", "description": "<img src=x onerror=alert(1)>\n# Injected",
                     "columns": [{"name": "c|1", "data_type": "string", "description": "<b>bold</b> & co\nline2"},
                                 {"name": "RowNumber-2662979B-1795-4F74-8F37-6A1BA8059B61", "data_type": "int64"}],
                     "measures": [{"name": "m", "format_string": "0", "description": "a | b", "expression": multi},
                                  {"name": "one", "format_string": "0", "description": "", "expression": "1 || `t`"}]}],
         "relationships": []}
    md = render_data_dictionary(x)
    check("markdown: multi-line expression is a fenced block with real line breaks",
          "````dax\n" + multi + "\n````" in md or "```dax\n" + multi + "\n```" in md, md)
    check("markdown: the fence is longer than any backtick run inside", "```dax\nVAR a = 1" in md and "<br>RETURN" not in md)
    check("markdown: table cell points at the definition", "_see definition below_" in md)
    check("markdown: no raw HTML from model text", "<script>" not in md and "<img" not in md and "<b>" not in md, md)
    check("markdown: description cannot start a heading", "\n# Injected" not in md)
    check("markdown: pipes are escaped in cells", "c\\|1" in md and "a \\| b" in md)
    check("markdown: inline code survives backticks and pipes", "`` 1 \\|\\| `t` ``" in md, md)
    check("markdown: internal RowNumber column is not listed", "RowNumber-" not in md and "Columns: **1**" in md)
    h = render_data_dictionary(x, fmt="html")
    check("html: no raw markup from model text", "<script>" not in h and "<img" not in h and "<b>bold" not in h)
    check("html: values are escaped, not dropped", "T&lt;script&gt;alert(1)&lt;/script&gt;" in h and "&amp; co" in h)
    check("html: multi-line expression keeps its line breaks in <pre>",
          "<pre>VAR a = 1\nRETURN\n    IF(a &lt; 2 &amp;&amp; a &gt; 0, a || `x`)</pre>" in h, h)
    check("html: no literal <br> text or markdown table syntax", "&lt;br&gt;" not in h and "|---|" not in h)
    check("html: real tables", h.count("<table>") == 2 and "<th>Measure</th>" in h)
    check("unknown format falls back to markdown", render_data_dictionary(x, fmt="pdf").startswith("# Data Dictionary"))
    check("empty model renders", render_data_dictionary({}).startswith("# Data Dictionary"))


BASE = {"tables": [
    table("Sales", [col("Amt", "double", description="", expression=None, sort_by=None, display_folder="", format_string=""),
                    col("Calc", expression="Sales[Amt] * 2", is_calculated=True, sort_by=None, display_folder="",
                        format_string=""),
                    col("K", sort_by=None, display_folder="", format_string="")],
          [meas("Total", "SUM(Sales[Amt])", display_folder="A"), meas("Double", "[Total] * 2", display_folder="A")]),
    table("Dim", [col("K", sort_by=None, display_folder="", format_string=""),
                  col("Name", "string", sort_by=None, display_folder="", format_string="")])],
    "relationships": [rel("Sales", "K", "Dim", "K")]}


def test_diff_models():
    print("\n== diff_models ==")
    same = diff_models(MODEL, MODEL)
    check("identical -> no changes", same["has_changes"] is False and same["total_changes"] == 0)
    check("identical markdown says so", "No semantic changes" in same["markdown"])

    after = copy.deepcopy(MODEL)
    # add a measure, remove a column, change a measure expression
    after["tables"][0]["measures"].append({"name": "New Measure", "expression": "1", "is_hidden": False})
    after["tables"][0]["columns"] = [c for c in after["tables"][0]["columns"] if c["name"] != "Qty"]
    after["tables"][0]["measures"][0]["expression"] = "SUM(Sales[Amount]) * 2"
    d = diff_models(MODEL, after)
    check("detects changes", d["has_changes"] is True)
    check("measure added", "Sales[New Measure]" in d["markdown"])
    check("column removed", "Sales[Qty]" in d["markdown"])
    check("measure changed", d["summary"]["measures_changed"] >= 1, str(d["summary"]))


def test_diff_every_property():
    print("\n== defect 1: every gathered property is compared ==")
    a = copy.deepcopy(BASE)
    a["relationships"][0].update(cross_filter="BothDirections", is_active=False, to_cardinality="Many")
    d = diff_models(BASE, a)
    check("relationship flags are a change", d["summary"]["relationships_changed"] == 1 and d["total_changes"] == 1, str(d["summary"]))
    line = [x for x in d["markdown"].splitlines() if x.startswith("- ~")][0]
    check("the line says what changed", all(s in line for s in ("active yes -> no", "cross filter", "`BothDirections`",
                                                                "to cardinality `One` -> `Many`")), line)

    def changed(mutate, kind):
        after = copy.deepcopy(BASE)
        mutate(after)
        res = diff_models(BASE, after)
        return [c for c in res["changes"] if c["kind"] == kind and c["change"] == "changed"], res

    for label, key, value in [("data type", "data_type", "int64"), ("hidden", "is_hidden", True),
                              ("sort-by column", "sort_by", "K"), ("format", "format_string", "0.00"),
                              ("display folder", "display_folder", "Keys"), ("description", "description", "new")]:
        ch, res = changed(lambda m, k=key, v=value: m["tables"][0]["columns"][0].__setitem__(k, v), "column")
        check(f"column {label}", len(ch) == 1 and ch[0]["object"] == "Sales[Amt]" and key in ch[0]["properties"]
              and res["total_changes"] == 1, str(res["summary"]))
    ch, _ = changed(lambda m: m["tables"][0]["columns"][1].__setitem__("expression", "Sales[Amt] * 3"), "column")
    check("calculated column expression", len(ch) == 1 and ch[0]["details"] == ["expression changed"], str(ch))
    for label, key, value in [("expression", "expression", "SUM(Sales[Amt]) + 1"), ("format", "format_string", "#,0"),
                              ("display folder", "display_folder", "B"), ("description", "description", "x"),
                              ("hidden", "is_hidden", True)]:
        ch, res = changed(lambda m, k=key, v=value: m["tables"][0]["measures"][0].__setitem__(k, v), "measure")
        check(f"measure {label}", len(ch) == 1 and ch[0]["object"] == "Sales[Total]" and key in ch[0]["properties"]
              and res["total_changes"] == 1, str(res["summary"]))
    for label, key, value in [("hidden", "is_hidden", True), ("description", "description", "changed")]:
        ch, res = changed(lambda m, k=key, v=value: m["tables"][1].__setitem__(k, v), "table")
        check(f"table {label}", len(ch) == 1 and ch[0]["object"] == "Dim" and res["summary"]["tables_changed"] == 1)

    print("\n== equal values in different spellings are not changes ==")
    b = copy.deepcopy(BASE)
    b["tables"][0]["columns"][0].update(data_type="Double", is_hidden="False", description=None)
    b["tables"][0]["measures"][0].update(expression="SUM(Sales[Amt])\r\n", is_hidden="false")
    b["relationships"][0].update(is_active="True", cross_filter="onedirection")
    check("type case, bool spelling, None vs '', trailing newline", diff_models(BASE, b)["total_changes"] == 0,
          diff_models(BASE, b)["markdown"])
    c = copy.deepcopy(BASE)
    c["tables"][0]["measures"][0]["expression"] = "SUM( Sales[Amt] )  // total"
    ch = [x for x in diff_models(BASE, c)["changes"] if x["change"] == "changed"]
    check("whitespace/comment-only edits are labelled as such", len(ch) == 1 and "reformatted" in ch[0]["details"][0], str(ch))
    older = copy.deepcopy(BASE)
    for column in older["tables"][0]["columns"] + older["tables"][1]["columns"]:
        column.pop("sort_by")
    check("a snapshot that predates a field does not report it as changed", diff_models(older, BASE)["total_changes"] == 0)
    mixed = copy.deepcopy(BASE)
    mixed["tables"].reverse()
    mixed["tables"][1]["columns"].reverse()
    mixed["tables"][1]["measures"].reverse()
    check("object order is not a change", diff_models(BASE, mixed)["total_changes"] == 0)


def test_diff_renames():
    print("\n== defect 1: renames are detected, and their fallout is not noise ==")
    a = copy.deepcopy(BASE)
    a["tables"][1]["name"] = "Customer"
    a["relationships"][0]["to_table"] = "Customer"
    d = diff_models(BASE, a)
    check("table rename is one change", d["summary"]["tables_renamed"] == 1 and d["total_changes"] == 1, str(d["summary"]))
    check("table rename line", "- ~ Dim -> Customer" in d["markdown"], d["markdown"])

    a = copy.deepcopy(BASE)
    a["tables"][0]["measures"][0]["name"] = "Total Sales"
    a["tables"][0]["measures"][1]["expression"] = "[Total Sales] * 2"     # the dependent follows the rename
    d = diff_models(BASE, a)
    check("measure rename is one change", d["summary"]["measures_renamed"] == 1 and d["total_changes"] == 1, d["markdown"])
    check("measure rename line", "- ~ Sales[Total] -> Sales[Total Sales]" in d["markdown"])

    a = copy.deepcopy(BASE)
    a["tables"][0]["columns"][0]["name"] = "Amount"
    a["tables"][0]["columns"][1]["expression"] = "Sales[Amount] * 2"
    a["tables"][0]["measures"][0]["expression"] = "SUM(Sales[Amount])"
    d = diff_models(BASE, a)
    check("column rename is one change (expressions that follow it are not)",
          d["summary"]["columns_renamed"] == 1 and d["total_changes"] == 1, d["markdown"])

    a = copy.deepcopy(BASE)
    a["tables"][0]["measures"][0].update(name="Total Sales", format_string="#,0")
    a["tables"][0]["measures"][1]["expression"] = "[Total Sales] * 3"
    d = diff_models(BASE, a)
    check("rename plus real edits reports all of them",
          d["summary"]["measures_renamed"] == 1 and d["summary"]["measures_changed"] == 2, str(d["summary"]))

    a = copy.deepcopy(BASE)
    moved = a["tables"][0]["measures"].pop(0)
    a["tables"][1]["measures"].append(moved)
    d = diff_models(BASE, a)
    check("a measure moved to another table is a change, not remove + add",
          d["summary"]["measures_changed"] == 1 and d["summary"]["measures_added"] == 0
          and "moved from table `Sales` to `Dim`" in d["markdown"], d["markdown"])

    a = copy.deepcopy(BASE)
    a["tables"][0]["measures"][0] = meas("Total Sales", "SUM(Sales[K])", display_folder="A")
    d = diff_models(BASE, a)
    check("different definition is remove + add, not a rename",
          d["summary"]["measures_renamed"] == 0 and d["summary"]["measures_added"] == 1 and d["summary"]["measures_removed"] == 1)

    two = {"tables": [table("T", [col("A"), col("B"), col("C", "string")])]}
    amb = {"tables": [table("T", [col("X"), col("Y"), col("C", "string")])]}
    d = diff_models(two, amb)
    check("an ambiguous match is not called a rename",
          d["summary"]["columns_renamed"] == 0 and d["summary"]["columns_added"] == 2 and d["summary"]["columns_removed"] == 2)
    blank = {"tables": [table("T", [col("c")], [meas("Old", "")])]}
    blank2 = {"tables": [table("T", [col("c")], [meas("New", "")])]}
    check("measures without expressions are never matched as renames",
          diff_models(blank, blank2)["summary"]["measures_renamed"] == 0)


def test_diff_ordering():
    print("\n== defect 1: output order is stable ==")
    a = copy.deepcopy(BASE)
    a["tables"][0]["measures"] += [meas("zeta", "1"), meas("Alpha", "2"), meas("beta", "3")]
    a["tables"].append(table("Zed", [col("z")]))
    a["tables"].append(table("alpha", [col("a")]))
    d1 = diff_models(BASE, a)
    shuffled = copy.deepcopy(a)
    shuffled["tables"].reverse()
    shuffled["tables"][-1]["measures"].reverse()
    d2 = diff_models(BASE, shuffled)
    check("same diff whatever the input order", d1["markdown"] == d2["markdown"])
    added = [c["object"] for c in d1["changes"] if c["kind"] == "measure" and c["change"] == "added"]
    check("names sort case-insensitively", added == ["Sales[Alpha]", "Sales[beta]", "Sales[zeta]"], str(added))
    check("summary totals add up", d1["total_changes"] == sum(d1["summary"].values()) == len(d1["changes"]))
    check("robust to missing keys and junk", diff_models({}, {"tables": [None, {"name": None}, {"name": "T"}]})["summary"]["tables_added"] == 1)


def test_dax_test_verdict():
    print("\n== defect 4: dax_test_verdict ==")

    def v(actual, expected, tolerance=0, **kw):
        return dax_test_verdict(actual, expected, tolerance, **kw)[0]

    check("int exact", v(100, 100) and not v(101, 100))
    check("int vs float", v(100, 100.0) and v(100.0, 100))
    check("Decimal vs int", v(Decimal("100.00"), 100) and not v(Decimal("100.01"), 100))
    check("Decimal within tolerance", v(Decimal("100.4"), 100, 0.5) and not v(Decimal("100.6"), 100, 0.5))
    check("numeric string on either side of a number", v(100.0, "100") and v("100", 100) and not v("abc", 100))
    check("two strings stay text", not v("100.0", "100") and v("007", "007"))
    check("float noise is absorbed", v(0.1 + 0.2, 0.3) and v(1234.5600000000002, 1234.56))
    check("a real float difference is not", not v(0.3001, 0.3) and not v(1e-6, 0))
    check("large integers are compared exactly", not v(10**17 + 1, 10**17))
    check("absolute tolerance", v(100.4, 100, 0.5) and not v(100.6, 100, 0.5))
    check("relative tolerance", v(1010, 1000, rel_tolerance=0.01) and not v(1011, 1000, rel_tolerance=0.01))
    check("the wider of the two tolerances wins", v(1010, 1000, 1, rel_tolerance=0.01) and v(5, 0, 5, rel_tolerance=0.01))
    check("tolerance as a numeric string", v(99, 100, "1"))
    check("tolerance None means none", v(100, 100, None) and not v(99, 100, None))
    ok, detail = dax_test_verdict(99, 100, "abc")
    check("bad tolerance fails with a clear message", ok is False and "invalid tolerance 'abc'" in detail, detail)
    ok, detail = dax_test_verdict(99, 100, -1)
    check("negative tolerance is rejected", ok is False and "invalid tolerance" in detail, detail)
    check("bad rel_tolerance is rejected", "invalid rel_tolerance" in dax_test_verdict(1, 1, 0, rel_tolerance=float("nan"))[1])
    check("a boolean tolerance is rejected", dax_test_verdict(1, 1, True)[0] is False)
    check("booleans compare as booleans", v(True, True) and v(False, False) and not v(True, False))
    check("boolean text is accepted", v("true", True) and v("False", False) and not v("False", True))
    check("a number or other text is not a boolean", not v(5, True) and not v(1, True) and not v("abc", True) and not v(0, False))
    check("BLANK only equals BLANK", v(None, None) and not v(None, 0) and not v(0, None) and not v(None, False)
          and not v("", None))
    check("NaN equals NaN, infinities by sign", v(float("nan"), float("nan")) and v(float("inf"), float("inf"))
          and not v(float("inf"), float("-inf")) and not v(float("nan"), 1))
    check("dates against ISO text", v(datetime(2024, 1, 31), "2024-01-31") and v(date(2024, 1, 31), "2024-01-31T00:00:00")
          and not v(datetime(2024, 1, 31, 12), "2024-01-31"))
    check("text", v("a", "a") and not v("a", "b") and not v("A", "a"))
    check("detail shows both values", dax_test_verdict(1.5, 2, 0.1)[1] == "1.5 vs 2 (±0.1)", dax_test_verdict(1.5, 2, 0.1)[1])
    check("positional call used by the server still works", dax_test_verdict(42, 42, 0) == (True, "42 vs 42"))


if __name__ == "__main__":
    print("=" * 70)
    print("  MODEL ANALYSIS (BPA + AI-READINESS + DIFF + VERDICT) TESTS")
    print("=" * 70)
    test_bpa()
    test_rules_run_reflects_filters()
    test_trailing_space_on_tables()
    test_dax_rules_agree_with_linter()
    test_dax_model_rules()
    test_error_rules()
    test_relationship_rules()
    test_structure_rules()
    test_table_no_relationship()
    test_malformed_input()
    test_ai_readiness()
    test_data_dictionary()
    test_diff_models()
    test_diff_every_property()
    test_diff_renames()
    test_diff_ordering()
    test_dax_test_verdict()
    print("\n" + "=" * 70)
    if _failures:
        print(f"  {len(_failures)} CHECK(S) FAILED: {', '.join(_failures)}")
        sys.exit(1)
    print("  ALL MODEL ANALYSIS CHECKS PASSED")
    print("=" * 70)
