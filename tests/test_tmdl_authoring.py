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

        # a refused rename, and undoing a rename, must not disturb changes made in between
        c.add_measures("Sales", [{"name": "Keep Me", "expression": "1"}])
        sales_path = Path(c._find_table_file("Sales"))
        res3 = c.rename_column_in_files("Sales", "Region", "Amount")      # collides with a column
        check("colliding rename is refused", res3.success is False and "already has" in res3.message, res3.message)
        after = sales_path.read_text(encoding="utf-8")
        check("refusal did NOT eat the unrelated added measure", "measure 'Keep Me' = 1" in after, after[-300:])
        check("refusal left the column name", "column Region" in after)
        res4 = c.rename_column_in_files("Sales", "Region", "Zone")
        check("valid rename succeeds", res4.success, res4.message)
        check("undo restores the column", c.rollback_changes() and "column Region" in sales_path.read_text(encoding="utf-8"))
        check("undo keeps the measure added before the rename",
              "measure 'Keep Me' = 1" in sales_path.read_text(encoding="utf-8"))


UPSERT_TABLE = (
    "table Sales\n"
    "\tlineageTag: 11111111-1111-1111-1111-111111111111\n\n"
    "\tcolumn Amount\n\t\tdataType: double\n\t\tsourceColumn: Amount\n\n"
    "\t/// Old description that is\n"
    "\t/// two lines long.\n"
    "\tmeasure 'Total Sales' =\n"
    "\t\t\tVAR _x = SUM(Sales[Amount])\n"
    "\t\t\tRETURN\n"
    "\t\t\t    _x\n"
    "\t\tlineageTag: aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee\n"
    "\t\tformatString: #,0\n\n"
    "\t\tannotation PBI_FormatHint = {\"isGeneralNumber\":true}\n\n"
    "\tmeasure Other = 1\n"
    "\t\tlineageTag: 99999999-0000-0000-0000-000000000000\n\n"
    "\tpartition Sales = m\n\t\tmode: import\n\t\tsource =\n\t\t\t\tlet\n\t\t\t\t    Source = 1\n\t\t\t\tin\n\t\t\t\t    Source\n"
)
UPSERT_OTHER = "table Dim\n\tcolumn Key\n\t\tdataType: int64\n\t\tsourceColumn: Key\n\n\tmeasure Elsewhere = 1\n"


def _upsert_project(tmp, crlf=False):
    root = Path(tmp)
    (root / "proj.pbip").write_text("{}", encoding="utf-8")
    tables = root / "proj.SemanticModel" / "definition" / "tables"
    tables.mkdir(parents=True)
    nl = "\r\n" if crlf else "\n"
    (tables / "Sales.tmdl").write_bytes(UPSERT_TABLE.replace("\n", nl).encode("utf-8"))
    (tables / "Dim.tmdl").write_bytes(UPSERT_OTHER.replace("\n", nl).encode("utf-8"))
    c = PowerBIPBIPConnector(auto_backup=False)
    c.current_project = PowerBIPBIPConnector._parse_pbip_project(root / "proj.pbip")
    return c, tables / "Sales.tmdl"


def test_upsert_measures():
    print("\n== add_measures(mode='upsert'): replace in place, deterministic, idempotent ==")
    batch = [
        {"name": "Total Sales", "expression": "SUM(Sales[Amount]) * 1", "format_string": "0.00",
         "description": "New description."},
        {"name": "Margin", "expression": "VAR _a = [Total Sales]\nRETURN\n    _a * 0.2", "display_folder": "Ratios"},
        {"name": "Tiny", "expression": "1"},
    ]
    for crlf in (False, True):
        tag = "CRLF" if crlf else "LF"
        with tempfile.TemporaryDirectory() as tmp:
            c, path = _upsert_project(tmp, crlf)
            res = c.add_measures("Sales", batch, mode="upsert")
            check(f"{tag}: upsert succeeds", res.get("success") is True, str(res))
            check(f"{tag}: reports what was replaced and what was added",
                  res.get("updated") == ["Total Sales"] and res.get("created") == ["Margin", "Tiny"], str(res))
            raw = path.read_bytes()
            first = raw
            check(f"{tag}: file keeps its line endings",
                  (raw.count(b"\r\n") == raw.count(b"\n")) if crlf else (b"\r" not in raw))
            text = raw.decode("utf-8").replace("\r\n", "\n")
            check(f"{tag}: old description, multi-line expression, format and annotation are gone",
                  "Old description" not in text and "two lines long" not in text and "VAR _x" not in text
                  and "PBI_FormatHint" not in text and "formatString: #,0\n" not in text, text)
            check(f"{tag}: new definition sits where the old one was (before 'Other')",
                  "\t/// New description.\n\tmeasure 'Total Sales' = SUM(Sales[Amount]) * 1\n\t\tformatString: 0.00\n"
                  "\t\tlineageTag: aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee\n\n\tmeasure Other = 1\n" in text, text)
            check(f"{tag}: the replaced measure keeps its lineageTag",
                  text.count("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee") == 1)
            check(f"{tag}: untouched neighbours are intact",
                  "\tmeasure Other = 1\n\t\tlineageTag: 99999999-0000-0000-0000-000000000000\n" in text
                  and "\tcolumn Amount\n\t\tdataType: double\n" in text and "\t\t\t\t    Source = 1\n" in text)
            margin = text[text.index("\tmeasure Margin ="):]
            new_tag = re.search(r"lineageTag: ([0-9a-f-]{36})", margin).group(1)
            check(f"{tag}: a new measure gets a lineageTag and its multi-line body",
                  "\tmeasure Margin =\n\t\t\tVAR _a = [Total Sales]\n\t\t\tRETURN\n\t\t\t    _a * 0.2\n"
                  "\t\tdisplayFolder: Ratios\n\t\tlineageTag: " + new_tag in text, margin[:200])

            res2 = c.add_measures("Sales", batch, mode="upsert")
            check(f"{tag}: second identical upsert succeeds, all as updates",
                  res2.get("success") is True and res2.get("created") == []
                  and res2.get("updated") == ["Total Sales", "Margin", "Tiny"], str(res2))
            check(f"{tag}: and leaves the file byte-identical", path.read_bytes() == first)
            index = c._model_field_index()
            check(f"{tag}: the file still parses with every object", {"Amount", "Total Sales", "Other", "Margin", "Tiny"}
                  <= set(index.get("Sales", {})), str(set(index.get("Sales", {}))))
            check(f"{tag}: validation finds nothing", c.validate_tmdl_syntax() == [],
                  str([e.message for e in c.validate_tmdl_syntax()]))

        with tempfile.TemporaryDirectory() as tmp:           # same build on a fresh copy
            c, path = _upsert_project(tmp, crlf)
            c.add_measures("Sales", batch, mode="upsert")
            check(f"{tag}: a rebuild from scratch produces the same bytes (deterministic tags)", path.read_bytes() == first)

    with tempfile.TemporaryDirectory() as tmp:
        c, path = _upsert_project(tmp)
        before = path.read_bytes()
        bad = c.add_measures("Sales", [{"name": "Tiny", "expression": "1"}, {"name": "Elsewhere", "expression": "2"}],
                             mode="upsert")
        check("a measure of that name in another table is a collision", bad.get("success") is False
              and "'Elsewhere': already exists in 'Dim'" in bad["message"], str(bad))
        bad2 = c.add_measures("Sales", [{"name": "Amount", "expression": "1"}], mode="upsert")
        check("a column of that name is a collision", bad2.get("success") is False and "'Amount'" in bad2["message"])
        check("a rejected batch writes nothing", path.read_bytes() == before)
        bad3 = c.add_measures("Sales", [{"name": "Other", "expression": "2"}])
        check("mode='add' still refuses an existing measure", bad3.get("success") is False
              and "already exists" in bad3["message"] and path.read_bytes() == before, str(bad3))
        check("unknown mode is refused", c.add_measures("Sales", [{"name": "Z", "expression": "1"}], mode="merge")
              .get("success") is False)
        res = c.add_measures("Sales", [{"name": "total sales", "expression": "0"}], mode="upsert")
        text = path.read_text(encoding="utf-8")
        check("names match case-insensitively; the definition's spelling wins",
              res.get("updated") == ["total sales"] and "\tmeasure 'total sales' = 0\n" in text
              and "measure 'Total Sales'" not in text, text[:400])


def test_line_break_guards():
    print("\n== single-line TMDL fields cannot smuggle in new lines ==")
    with tempfile.TemporaryDirectory() as tmp:
        c = _build_project(tmp)
        path = Path(c._find_table_file("Sales"))
        before = path.read_bytes()
        bad = c.add_hierarchy("Sales", "Geo\n\tmeasure Injected = 1", ["Region"])
        check("hierarchy name with a line break refused", bad.get("success") is False and path.read_bytes() == before, str(bad))
        bad = c.add_calculation_group("CG", [{"name": "A", "expression": "SELECTEDMEASURE()"}],
                                      column_name="Col\n\tcolumn Evil")
        check("calculation group column name with a line break refused", bad.get("success") is False, str(bad))
        check("and no table file was created", not (path.parent / "CG.tmdl").exists())
        res = c.add_measures("Sales", [{"name": "CR", "expression": "1\r\tmeasure Injected2 = 2"}])
        text = path.read_text(encoding="utf-8")
        check("a bare carriage return in an expression becomes an expression line, not a new object",
              res.get("success") is True and "\tmeasure CR =\n\t\t\t1\n\t\t\t\tmeasure Injected2 = 2\n" in text
              and "Injected2" not in c._model_field_index().get("Sales", {}), text[-200:])


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
    test_upsert_measures()
    test_line_break_guards()
    test_structural_tabs_only()
    print("\n" + "=" * 70)
    if _failures:
        print(f"  {len(_failures)} CHECK(S) FAILED: {', '.join(_failures)}")
        sys.exit(1)
    print("  ALL TMDL AUTHORING CHECKS PASSED")
    print("=" * 70)
