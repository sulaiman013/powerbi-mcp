"""
Tests for TMDL authoring: measure/hierarchy/date-table/calculation-group emission verified
against the doc-confirmed shapes, plus end-to-end connector round-trips on a temp PBIP project
(emitted TMDL must be parsed back by the connector's own parser). No Power BI required.
Run: python tests/test_tmdl_authoring.py
"""
import json
import os
import re
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))
import tmdl_authoring as ta  # noqa: E402
import dax_generator as dg  # noqa: E402
from powerbi_pbip_connector import PowerBIPBIPConnector  # noqa: E402

_failures = []


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f": {detail}" if detail and not cond else ""))
    if not cond:
        _failures.append(name)


SALES_TMDL = """table Sales
\tcolumn Region
\t\tdataType: string
\tcolumn Amount
\t\tdataType: double
\tcolumn OrderDate
\t\tdataType: dateTime
\tmeasure 'Total Sales' = SUM(Sales[Amount])
"""

MODEL_TMDL = "model Model\n\tculture: en-US\n\tdefaultPowerBIDataSourceVersion: powerBI_V3\n"
DATABASE_TMDL = "database\n\tcompatibilityLevel: 1550\n"


def _build_project(tmp):
    root = Path(tmp)
    (root / "proj.pbip").write_text("{}", encoding="utf-8")
    smdef = root / "proj.SemanticModel" / "definition"
    (smdef / "tables").mkdir(parents=True)
    (smdef / "tables" / "Sales.tmdl").write_text(SALES_TMDL, encoding="utf-8")
    (smdef / "model.tmdl").write_text(MODEL_TMDL, encoding="utf-8")
    (smdef / "database.tmdl").write_text(DATABASE_TMDL, encoding="utf-8")
    pages = root / "proj.Report" / "definition" / "pages"
    pages.mkdir(parents=True)
    (root / "proj.Report" / "definition" / "report.json").write_text(json.dumps({"$schema": "x"}), encoding="utf-8")
    proj = PowerBIPBIPConnector._parse_pbip_project(root / "proj.pbip")
    c = PowerBIPBIPConnector(auto_backup=False)
    c.current_project = proj
    return c


def test_quote_name():
    print("\n== quote_name rules ==")
    check("simple stays bare", ta.quote_name("Sales") == "Sales")
    check("space quoted", ta.quote_name("Total Sales") == "'Total Sales'")
    check("dot quoted", ta.quote_name("v1.2") == "'v1.2'")
    check("leading digit quoted", ta.quote_name("1st Metric") == "'1st Metric'")
    check("internal quote doubled", ta.quote_name("Customer's") == "'Customer''s'")


def test_measure_shapes():
    print("\n== measure block shapes (doc-verified) ==")
    single = ta.render_measure({"name": "Total Sales", "expression": "SUM(Sales[Amount])",
                                "format_string": "#,0", "display_folder": "Base",
                                "description": "Sum of sales."})
    lines = single.splitlines()
    check("description as /// at measure indent", lines[0] == "\t/// Sum of sales.")
    check("no blank between /// and declaration", lines[1].startswith("\tmeasure 'Total Sales' = "))
    check("single-line declaration", "\tmeasure 'Total Sales' = SUM(Sales[Amount])" in lines)
    check("formatString at 2 tabs", "\t\tformatString: #,0" in lines)
    check("displayFolder at 2 tabs", "\t\tdisplayFolder: Base" in lines)
    check("NO description: property", "description:" not in single)

    multi = ta.render_measure({"name": "YoY", "expression": "VAR __py = [X]\nRETURN\n    [Y] - __py"})
    mlines = multi.splitlines()
    check("multi-line: bare = on declaration", mlines[0] == "\tmeasure YoY =")
    check("body at 3 tabs (decl + 2)", mlines[1] == "\t\t\tVAR __py = [X]", repr(mlines[1]))
    check("nested body keeps spaces after 3 tabs", "\t\t\t    [Y] - __py" in mlines, repr(mlines))

    hidden = ta.render_measure({"name": "H", "expression": "1", "is_hidden": True})
    check("isHidden bare flag", "\t\tisHidden" in hidden.splitlines() and "isHidden:" not in hidden)


def test_add_measures_roundtrip():
    print("\n== add_measures: bulk write + parser round-trip + collision guard ==")
    with tempfile.TemporaryDirectory() as tmp:
        c = _build_project(tmp)
        suite = dg.generate_time_intelligence("Total Sales", "Date.OrderDate", variants=["ytd", "yoy_pct"])
        res = c.add_measures("Sales", suite)
        check("bulk write ok", res.get("success") is True, str(res))
        index = c._model_field_index()
        check("parser sees new measures", {"Total Sales YTD", "Total Sales YoY %"} <= set(index.get("Sales", {})),
              str(set(index.get("Sales", {}))))
        check("kind is measure", index["Sales"]["Total Sales YTD"]["kind"] == "measure")
        content = (Path(res["path"])).read_text(encoding="utf-8")
        check("description written as ///", "/// Total Sales accumulated" in content)
        # collisions: existing model measure + duplicate within batch -> nothing written
        before = content
        bad = c.add_measures("Sales", [{"name": "Total Sales", "expression": "1"},
                                       {"name": "N1", "expression": "1"},
                                       {"name": "N1", "expression": "2"}])
        check("collision batch rejected", bad.get("success") is False and "already exists" in bad["message"]
              and "duplicated" in bad["message"], str(bad))
        check("nothing written on rejection", (Path(res["path"])).read_text(encoding="utf-8") == before)
        check("unknown table rejected", c.add_measures("Nope", [{"name": "X", "expression": "1"}])["success"] is False)


def test_date_table():
    print("\n== create_date_table: file shape + parser round-trip ==")
    with tempfile.TemporaryDirectory() as tmp:
        c = _build_project(tmp)
        res = c.create_date_table("Date", "2020-01-01", "2026-12-31")
        check("created", res.get("success") is True, str(res))
        content = Path(res["path"]).read_text(encoding="utf-8")
        check("dataCategory: Time on table", "\tdataCategory: Time" in content)
        check("isKey on Date column", re.search(r"\tcolumn Date\n(\t\t.+\n)*?\t\tisKey", content) is not None)
        check("bracketed sourceColumn", "\t\tsourceColumn: [Date]" in content)
        check("isNameInferred flags", content.count("\t\tisNameInferred") >= 8)
        check("partition = calculated", "\tpartition Date = calculated" in content)
        check("mode: import", "\t\tmode: import" in content)
        check("source body at 4 tabs", "\n\t\t\t\tADDCOLUMNS(" in content)
        check("month sorted by number", re.search(r"column Month\n(\t\t.+\n)*?\t\tsortByColumn: 'Month Number'", content) is not None)
        check("UnderlyingDateTimeDataType annotation", "annotation UnderlyingDateTimeDataType = Date" in content)
        index = c._model_field_index()
        check("parser sees date table columns", {"Date", "Year", "Quarter", "Month"} <= set(index.get("Date", {})),
              str(set(index.get("Date", {}))))
        check("duplicate rejected", c.create_date_table("Date")["success"] is False)

        res_f = c.create_date_table("FiscalDate", "2020-01-01", "2021-12-31", fiscal_year_start_month=7)
        fcontent = Path(res_f["path"]).read_text(encoding="utf-8")
        check("fiscal columns emitted", "column 'Fiscal Year'" in fcontent and "column 'Fiscal Quarter'" in fcontent)
        check("no self-reference in ADDCOLUMNS", "[Fiscal Quarter Number])" not in fcontent.split("partition")[1]
              or "QUOTIENT" in fcontent)
        bad = c.create_date_table("Bad", fiscal_year_start_month=13)
        check("bad fiscal month rejected", bad.get("success") is False and "1-12" in bad.get("message", ""), str(bad))


def test_calculation_group():
    print("\n== add_calculation_group: shape + model.tmdl flag ==")
    with tempfile.TemporaryDirectory() as tmp:
        c = _build_project(tmp)
        items = ta.time_intelligence_calc_items("Date.OrderDate")
        res = c.add_calculation_group("Time Intelligence", items, column_name="Time Calculation")
        check("created", res.get("success") is True, str(res))
        content = Path(res["path"]).read_text(encoding="utf-8")
        check("calculationGroup at 1 tab", "\tcalculationGroup\n" in content)
        check("precedence at 2 tabs", "\t\tprecedence: 1" in content)
        check("items multi-line at 4 tabs", "\t\tcalculationItem YTD =\n\t\t\t\tCALCULATE(SELECTEDMEASURE(), DATESYTD('Date'[OrderDate]))" in content)
        check("quoted item name", "\t\tcalculationItem 'YoY %' =" in content)
        check("formatStringDefinition at 3 tabs w/ quotes", '\t\t\tformatStringDefinition = "0.0%"' in content)
        check("selector column sourceColumn Name", re.search(r"column 'Time Calculation'\n(\t\t.+\n)*?\t\tsourceColumn: Name", content) is not None)
        check("sortByColumn Ordinal", "\t\tsortByColumn: Ordinal" in content)
        check("Ordinal hidden int64", re.search(r"column Ordinal\n(\t\t.+\n)*?\t\tisHidden", content) is not None)
        check("no partition in calc group table", "partition" not in content)
        model_content = (Path(tmp) / "proj.SemanticModel" / "definition" / "model.tmdl").read_text(encoding="utf-8")
        check("discourageImplicitMeasures set in model.tmdl", "\tdiscourageImplicitMeasures" in model_content)
        check("flag reported", res.get("discourage_implicit_measures_set") is True)
        check("no compat warning at 1550", res.get("compat_warning") is None, str(res.get("compat_warning")))
        check("duplicate rejected", c.add_calculation_group("Time Intelligence", items)["success"] is False)


def test_hierarchy():
    print("\n== add_hierarchy: shape + validation ==")
    with tempfile.TemporaryDirectory() as tmp:
        c = _build_project(tmp)
        res = c.add_hierarchy("Sales", "Geo Hierarchy", ["Region"])
        check("created", res.get("success") is True, str(res))
        content = Path(res["path"]).read_text(encoding="utf-8")
        check("hierarchy at 1 tab quoted", "\thierarchy 'Geo Hierarchy'" in content)
        check("level at 2 tabs", "\t\tlevel Region" in content)
        check("column: at 3 tabs", "\t\t\tcolumn: Region" in content)
        check("missing column rejected", c.add_hierarchy("Sales", "H2", ["Nope"])["success"] is False)
        check("duplicate hierarchy rejected", c.add_hierarchy("Sales", "Geo Hierarchy", ["Region"])["success"] is False)
        check("measure not usable as level", c.add_hierarchy("Sales", "H3", ["Total Sales"])["success"] is False)


def test_filename_safety_and_quoted_date_column():
    print("\n== live-UAT regressions: filename safety + quoted-table calc items ==")
    with tempfile.TemporaryDirectory() as tmp:
        c = _build_project(tmp)
        for bad_name, why in [("../Evil", "traversal"), ("Time: Intel", "colon"),
                              ("CON", "reserved"), ("a*b", "wildcard")]:
            res = c.create_date_table(bad_name)
            check(f"date table rejects {why}", res.get("success") is False, str(res))
            res2 = c.add_calculation_group(bad_name, [{"name": "X", "expression": "SELECTEDMEASURE()"}])
            check(f"calc group rejects {why}", res2.get("success") is False, str(res2))
        # nothing escaped the project tree
        escaped = list(Path(tmp).parent.glob("Evil.tmdl"))
        check("no file escaped the tables folder", not escaped)
        # a pre-existing FILE with that name (different table inside) must not be overwritten
        tables_dir = Path(tmp) / "proj.SemanticModel" / "definition" / "tables"
        (tables_dir / "Clash.tmdl").write_text("table SomethingElse\n", encoding="utf-8")
        res3 = c.create_date_table("Clash")
        check("existing file not overwritten", res3.get("success") is False and "already exists" in res3["message"], str(res3))

    items = ta.time_intelligence_calc_items("'Order Date'.Date")
    ytd = next(i for i in items if i["name"] == "YTD")
    check("quoted table parsed once (no double quotes)", "'Order Date'[Date]" in ytd["expression"]
          and "''Order Date''" not in ytd["expression"], ytd["expression"])


def test_rename_integration_with_wave5_constructs():
    print("\n== workflow-UAT regressions: rename cascade vs hierarchies/sortBy/sourceColumn + scoped rollback ==")
    with tempfile.TemporaryDirectory() as tmp:
        c = _build_project(tmp)
        c.create_date_table("Date", "2020-01-01", "2021-12-31")
        c.add_hierarchy("Date", "Calendar Hierarchy", ["Year", "Month"])
        path = Path(c._find_table_file("Date"))

        res = c.rename_column_in_files("Date", "Month", "Month Label")
        check("column rename succeeds", res.success is True, res.message)
        content = path.read_text(encoding="utf-8")
        check("column declaration renamed", "column 'Month Label'" in content)
        check("hierarchy level ref updated", "\t\t\tcolumn: 'Month Label'" in content, content[content.find("hierarchy"):][:400])
        check("sourceColumn kept (DAX alias unchanged)", "sourceColumn: [Month]" in content)
        res2 = c.rename_column_in_files("Date", "Month Number", "MonthNum")
        content2 = path.read_text(encoding="utf-8")
        check("sortByColumn updated", "sortByColumn: MonthNum" in content2, str(res2.message))
        check("sortByColumn old gone", "sortByColumn: 'Month Number'" not in content2)

        # measure rename must not corrupt a sourceColumn mapping sharing the name
        c.add_measures("Sales", [{"name": "Year", "expression": "SUM(Sales[Amount])"}])
        c.rename_measure_in_files("Year", "Year Total")
        content3 = path.read_text(encoding="utf-8")
        check("sourceColumn: [Year] survives measure rename", "sourceColumn: [Year]" in content3)

        # scoped rollback: a failing rename must NOT roll files back past unrelated changes
        c.add_measures("Sales", [{"name": "Keep Me", "expression": "1"}])
        sales_path = Path(c._find_table_file("Sales"))
        orig_fail = c._rename_column_in_visual_files
        c.current_project.is_pbir_enhanced = True
        c._rename_column_in_visual_files = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom"))
        res3 = c.rename_column_in_files("Sales", "Region", "Zone")
        c._rename_column_in_visual_files = orig_fail
        check("failed rename reports rollback", res3.success is False)
        after = sales_path.read_text(encoding="utf-8")
        check("rollback did NOT eat unrelated added measure", "'Keep Me'" in after or "measure 'Keep Me'" in after, after[-300:])
        check("rollback restored the column name", "column Region" in after)


def test_structural_tabs_only():
    print("\n== structural indentation is tabs-only ==")
    content = ta.build_date_table("Date", "2020-01-01", "2021-01-01")
    bad = [ln for ln in content.splitlines()
           if ln.strip() and not ln.startswith("///") and not ln.startswith("table")
           and re.match(r"^\t* +\S", ln) and not ln.lstrip("\t").startswith(" ")]
    # structural lines (keyword lines) must not use spaces before the keyword
    struct_bad = [ln for ln in content.splitlines()
                  if re.match(r"^ +(column|partition|measure|hierarchy|table)", ln)]
    check("no space-indented structural lines", not struct_bad, str(struct_bad))


if __name__ == "__main__":
    print("=" * 70)
    print("  TMDL AUTHORING TESTS")
    print("=" * 70)
    test_quote_name()
    test_measure_shapes()
    test_add_measures_roundtrip()
    test_date_table()
    test_calculation_group()
    test_hierarchy()
    test_filename_safety_and_quoted_date_column()
    test_rename_integration_with_wave5_constructs()
    test_structural_tabs_only()
    print("\n" + "=" * 70)
    if _failures:
        print(f"  {len(_failures)} CHECK(S) FAILED: {', '.join(_failures)}")
        sys.exit(1)
    print("  ALL TMDL AUTHORING CHECKS PASSED")
    print("=" * 70)
