"""
PBIP rename, model side (TMDL): a rename changes the object, every reference to it and nothing
else. Each test here is a regression for a way the old regex-based rename corrupted a model:
same-named columns in other tables, measures sharing a column's name, M code quoted like DAX,
new names with digits or backslashes, quoted names with apostrophes, BOM and CRLF files.
No Power BI required. Run: python tests/test_pbip_rename_model.py
"""
import json
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import pbip_fixtures as fx  # noqa: E402

_failures = []


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f": {detail}" if detail and not cond else ""))
    if not cond:
        _failures.append(name)


def has(name, text, needle):
    check(name, needle in text, f"missing {needle!r}")


def lacks(name, text, needle):
    check(name, needle not in text, f"unexpected {needle!r}")


def warned(result, *words):
    return any(all(w in warning for w in words) for warning in result.warnings)


EXTRA_MEASURES = """
\tmeasure 'Max ID' = MAXX(Sales, [ID]) + MAX(Customers[ID]) + MAX(Sales[ID]) + max(sales[id])
\t\tlineageTag: aaaaaaaa-0000-0000-0000-000000000001
"""


def model_with(**overrides):
    files = dict(fx.MODEL_FILES)
    files.update(overrides)
    return files


def test_column_rename_is_scoped_to_its_table():
    print("\n== column rename: only the owning table's column ==")
    with tempfile.TemporaryDirectory() as tmp:
        measures = fx.MEASURES.replace("\tpartition _Measures", EXTRA_MEASURES.lstrip("\n") + "\n\tpartition _Measures")
        p = fx.build_project(tmp, model=model_with(**{"tables/_Measures.tmdl": measures}))
        customers_before = (p.definition / "tables/Customers.tmdl").read_bytes()
        r = p.connector.rename_column_in_files("Sales", "ID", "SalesID")
        check("rename succeeds", r.success, r.message)
        sales = p.tmdl("tables/Sales.tmdl")
        has("declaration renamed", sales, "\tcolumn SalesID\n")
        has("sourceColumn keeps the source name", sales, "\t\tsourceColumn: ID\n")
        has("unqualified ref in the table's own calculated column follows", sales,
            "column 'ID Label' = \"#\" & [SalesID]")
        check("same-named column of another table: file byte-identical",
              (p.definition / "tables/Customers.tmdl").read_bytes() == customers_before)
        rel = p.tmdl("relationships.tmdl")
        has("other table's relationship endpoint untouched", rel, "toColumn: Customers.ID")
        m = p.tmdl("tables/_Measures.tmdl")
        has("qualified refs renamed, any letter case; other table and ambiguous [ID] kept", m,
            "MAXX(Sales, [ID]) + MAX(Customers[ID]) + MAX(Sales[SalesID]) + max(sales[SalesID])")
        check("ambiguous unqualified reference is reported", warned(r, "unqualified [ID]", "Max ID"),
              str(r.warnings))
        check("no validation errors", p.connector.validate_tmdl_syntax() == [],
              str([e.message for e in p.connector.validate_tmdl_syntax()]))


def test_column_rename_with_a_same_named_measure():
    print("\n== column rename: a measure of the same name is not touched ==")
    with tempfile.TemporaryDirectory() as tmp:
        p = fx.build_project(tmp)
        r = p.connector.rename_column_in_files("Sales", "Revenue", "Net Revenue")
        check("rename succeeds", r.success, r.message)
        sales = p.tmdl("tables/Sales.tmdl")
        has("declaration renamed and quoted", sales, "\tcolumn 'Net Revenue'\n")
        has("sourceColumn unchanged", sales, "\t\tsourceColumn: Revenue\n")
        has("M code unchanged", sales, "Kept = Table.SelectRows(Data, each [Revenue] > 0)")
        has("unqualified [Revenue] left for a person (measure of that name exists)", sales,
            "column Margin = [Revenue] * 0.2")
        check("that decision is reported", warned(r, "unqualified [Revenue]", "Margin"), str(r.warnings))
        m = p.tmdl("tables/_Measures.tmdl")
        has("measure declaration keeps its name; its column ref follows", m,
            "measure Revenue = SUM(Sales[Net Revenue])")
        has("measure reference untouched", m, "measure 'Rev x2' = [Revenue] * 2")
        has("multi-line expression: qualified ref follows, comment does not", m,
            "DIVIDE(SUMX(_rows, Sales[Net Revenue]), _n) -- uses Sales[Revenue]")
        has("string literal untouched", m, 'measure Label = "Sales[Revenue] is " & FORMAT([Revenue], "#,0")')
        check("text that still spells the old reference is reported",
              warned(r, "string literal(s) or comment(s)", "[Revenue]"), str(r.warnings))
        role = p.tmdl("roles/Reader.tmdl")
        has("RLS filter: unqualified left, table untouched", role,
            'tablePermission Sales = Sales[Region] = "East" && [Revenue] > 0')
        persp = p.tmdl("perspectives/Core.tmdl")
        has("perspective column follows", persp, "\t\tperspectiveColumn 'Net Revenue'\n")
        has("perspective measure untouched", persp, "\t\tperspectiveMeasure Revenue\n")
        culture = p.tmdl("cultures/en-US.tmdl")
        has("linguistic binding of the column follows", culture,
            '"ConceptualEntity": "Sales", "ConceptualProperty": "Net Revenue"')
        has("linguistic binding of the measure untouched", culture,
            '"ConceptualEntity": "_Measures", "ConceptualProperty": "Revenue"')
        has("translation of the column follows", culture, "\t\t\t\tcolumn 'Net Revenue'\n")
        has("translation of the measure untouched", culture, "\t\t\t\tmeasure Revenue\n")
        has("generated linguistic terms are left alone", culture, '"sale.revenue": {')


def test_measure_rename_leaves_columns_alone():
    print("\n== measure rename: same-named columns, M code and sourceColumn survive ==")
    with tempfile.TemporaryDirectory() as tmp:
        p = fx.build_project(tmp)
        r = p.connector.rename_measure_in_files("Revenue", "Total Revenue")
        check("rename succeeds", r.success, r.message)
        m = p.tmdl("tables/_Measures.tmdl")
        has("declaration renamed, column reference kept", m, "measure 'Total Revenue' = SUM(Sales[Revenue])")
        has("description block kept above it", m, "\t/// Total revenue across all sales.\n\tmeasure 'Total Revenue'")
        has("bare references follow", m, "measure 'Rev x2' = [Total Revenue] * 2")
        has("reference inside CALCULATE follows", m, 'CALCULATE([Total Revenue], Sales[Region] = "East"')
        has("string kept, expression part follows", m,
            'measure Label = "Sales[Revenue] is " & FORMAT([Total Revenue], "#,0")')
        has("qualified column in a multi-line measure kept", m, "SUMX(_rows, Sales[Revenue])")
        sales = p.tmdl("tables/Sales.tmdl")
        has("column declaration kept", sales, "\tcolumn Revenue\n")
        has("M code kept", sales, "each [Revenue] > 0")
        has("calculated column of the table that owns a column of that name kept", sales,
            "column Margin = [Revenue] * 0.2")
        check("and reported", warned(r, "unqualified [Revenue]", "Margin"), str(r.warnings))
        has("RLS filter on that table kept", p.tmdl("roles/Reader.tmdl"), "&& [Revenue] > 0")
        persp = p.tmdl("perspectives/Core.tmdl")
        has("perspective measure follows", persp, "\t\tperspectiveMeasure 'Total Revenue'\n")
        has("perspective column kept", persp, "\t\tperspectiveColumn Revenue\n")
        culture = p.tmdl("cultures/en-US.tmdl")
        has("linguistic binding of the measure follows", culture,
            '"ConceptualEntity": "_Measures", "ConceptualProperty": "Total Revenue"')
        has("linguistic binding of the column kept", culture,
            '"ConceptualEntity": "Sales", "ConceptualProperty": "Revenue"')
        has("translation of the measure follows", culture, "\t\t\t\tmeasure 'Total Revenue'\n")
        check("model still validates", p.connector.validate_tmdl_syntax() == [])


def test_table_rename_everywhere():
    print("\n== table rename: declarations, DAX, M, relationships, roles, cultures, diagram ==")
    with tempfile.TemporaryDirectory() as tmp:
        p = fx.build_project(tmp)
        r = p.connector.rename_table_in_files("Sales", "Sales Data")
        check("rename succeeds", r.success, r.message)
        model = p.tmdl("model.tmdl")
        has("ref table", model, "ref table 'Sales Data'\n")
        has("PBI_QueryOrder", model, 'annotation PBI_QueryOrder = ["Sales Data","Customers","Dim Date","_Measures"]')
        rel = p.tmdl("relationships.tmdl")
        has("relationship endpoint", rel, "fromColumn: 'Sales Data'.CustomerID")
        has("relationship endpoint with a quoted column", rel, "fromColumn: 'Sales Data'.'Order Date'")
        has("auto-style relationship name", rel, "relationship 'Sales Data to Dim Date'")
        sales = p.tmdl("tables/Sales.tmdl")
        has("table declaration", sales, "table 'Sales Data'\n")
        has("partition name", sales, "\tpartition 'Sales Data' = m\n")
        has("source object name in M is never touched", sales, 'Source{[Schema="dbo",Item="Sales"]}[Data]')
        customers = p.tmdl("tables/Customers.tmdl")
        has("M reference uses M quoting, the join's new column name is kept", customers,
            'Table.NestedJoin(Data, {"ID"}, #"Sales Data", {"CustomerID"}, "Sales", JoinKind.LeftOuter)')
        has("M reference inside a list", customers, 'Table.Combine({#"Sales Data", Joined})')
        lacks("no DAX quoting in M", customers, "'Sales Data'")
        date = p.tmdl("tables/Dim Date.tmdl")
        has("calculated table DAX", date, "CALENDAR(MIN('Sales Data'[Order Date]), MAX('Sales Data'[Order Date]))")
        m = p.tmdl("tables/_Measures.tmdl")
        has("qualified reference", m, "measure Revenue = SUM('Sales Data'[Revenue])")
        has("table assigned to a variable uses DAX quoting", m, "VAR _rows = 'Sales Data'\n")
        lacks("no M quoting in DAX", m, '#"Sales Data"')
        has("comment untouched", m, "-- uses Sales[Revenue]")
        has("string untouched", m, '"Sales[Revenue] is "')
        check("stale text is reported", warned(r, "string literal(s) or comment(s)"), str(r.warnings))
        role = p.tmdl("roles/Reader.tmdl")
        has("RLS table permission", role, "tablePermission 'Sales Data' = 'Sales Data'[Region] = \"East\"")
        has("perspective", p.tmdl("perspectives/Core.tmdl"), "\tperspectiveTable 'Sales Data'\n")
        culture = p.tmdl("cultures/en-US.tmdl")
        check("both linguistic bindings", culture.count('"ConceptualEntity": "Sales Data"') == 2, culture)
        has("translation", culture, "\t\t\ttable 'Sales Data'\n")
        diagram = fx.load_json(p.model / "diagramLayout.json")
        check("diagram layout", [n["nodeIndex"] for n in diagram["diagrams"][0]["nodes"]]
              == ["Sales Data", "Customers", "Dim Date"])
        has("lineage tags untouched", sales, "lineageTag: 11111111-1111-1111-1111-111111111111")
        check("model still validates", p.connector.validate_tmdl_syntax() == [],
              str([e.message for e in p.connector.validate_tmdl_syntax()]))


def test_names_that_broke_regex_templates():
    print("\n== new names with digits, backslashes, quotes and brackets ==")
    for new in ("2024 Sales", "9Lives", "A\\1B", "Net $& \\g<0>", "O'Brien", "Sales.EU"):
        with tempfile.TemporaryDirectory() as tmp:
            p = fx.build_project(tmp)
            r = p.connector.rename_table_in_files("Sales", new)
            check(f"{new!r}: rename succeeds", r.success, r.message)
            model = p.tmdl("model.tmdl")
            line = next(ln for ln in model.splitlines() if ln.startswith("annotation PBI_QueryOrder"))
            check(f"{new!r}: PBI_QueryOrder holds the exact name",
                  json.loads(line.split("=", 1)[1])[0] == new, line)
            quoted = "'" + new.replace("'", "''") + "'"
            has(f"{new!r}: DAX reference correctly quoted", p.tmdl("tables/_Measures.tmdl"),
                f"measure Revenue = SUM({quoted}[Revenue])")
            has(f"{new!r}: relationship keyword intact", p.tmdl("relationships.tmdl"),
                f"relationship {quoted[:-1]} to Dim Date'")
            check(f"{new!r}: model still validates", p.connector.validate_tmdl_syntax() == [],
                  str([e.message for e in p.connector.validate_tmdl_syntax()]))
    with tempfile.TemporaryDirectory() as tmp:
        p = fx.build_project(tmp)
        r = p.connector.rename_column_in_files("Sales", "Revenue", "Net]Rev")
        check("column name with ]: succeeds", r.success, r.message)
        has("] doubled inside DAX brackets", p.tmdl("tables/_Measures.tmdl"), "SUM(Sales[Net]]Rev])")
        has("TMDL declaration needs no escape", p.tmdl("tables/Sales.tmdl"), "\tcolumn Net]Rev\n")


def test_old_name_with_apostrophe():
    print("\n== old name containing an apostrophe (escaped as '' in TMDL and DAX) ==")
    files = {
        "model.tmdl": "model Model\n\nref table 'O''Brien'\nref table M\n",
        "tables/OBrien.tmdl": "table 'O''Brien'\n\tcolumn X\n\t\tdataType: int64\n\t\tsourceColumn: X\n",
        "tables/M.tmdl": "table M\n\tmeasure Total = SUM('O''Brien'[X]) + COUNTROWS('O''Brien')\n",
    }
    with tempfile.TemporaryDirectory() as tmp:
        p = fx.build_project(tmp, model=files)
        r = p.connector.rename_table_in_files("O'Brien", "Clients")
        check("rename succeeds", r.success, r.message)
        has("declaration", p.tmdl("tables/OBrien.tmdl"), "table Clients\n")
        has("ref table", p.tmdl("model.tmdl"), "ref table Clients\n")
        has("DAX references (a quoted reference stays quoted)", p.tmdl("tables/M.tmdl"),
            "SUM('Clients'[X]) + COUNTROWS('Clients')")


def test_structural_properties_are_scoped():
    print("\n== sortByColumn, hierarchy levels, relationships: scoped to the owning table ==")
    with tempfile.TemporaryDirectory() as tmp:
        p = fx.build_project(tmp)
        r = p.connector.rename_column_in_files("Customers", "Region", "Area")
        check("rename succeeds", r.success, r.message)
        customers = p.tmdl("tables/Customers.tmdl")
        has("declaration", customers, "\tcolumn Area\n")
        has("hierarchy level keeps its own name, its column follows", customers,
            "\t\tlevel Region\n\t\t\tcolumn: Area\n")
        has("same-named column of another table untouched", p.tmdl("tables/Sales.tmdl"), "\tcolumn Region\n")
        has("DAX: only this table's column", p.tmdl("tables/_Measures.tmdl"),
            'CALCULATE([Revenue], Sales[Region] = "East", Customers[Area] = "East")')
        role = p.tmdl("roles/Reader.tmdl")
        has("RLS on this table follows", role, 'tablePermission Customers = Customers[Area] = "East"')
        has("RLS on the other table untouched", role, 'tablePermission Sales = Sales[Region] = "East"')
        has("perspective of the other table untouched", p.tmdl("perspectives/Core.tmdl"), "perspectiveColumn Region")

        r = p.connector.rename_column_in_files("Customers", "ID", "CustID")
        check("second rename succeeds", r.success, r.message)
        customers = p.tmdl("tables/Customers.tmdl")
        has("sortByColumn follows", customers, "\t\tsortByColumn: CustID\n")
        has("M column list untouched", customers, 'Table.NestedJoin(Data, {"ID"}, Sales')
        has("relationship endpoint follows", p.tmdl("relationships.tmdl"), "toColumn: Customers.CustID")
        has("other table's ID untouched", p.tmdl("tables/Sales.tmdl"), "\tcolumn ID\n")


def test_inferred_name_flag_is_removed():
    print("\n== calculated-table column: isNameInferred removed, source mapping kept ==")
    with tempfile.TemporaryDirectory() as tmp:
        p = fx.build_project(tmp)
        r = p.connector.rename_column_in_files("Dim Date", "Month Number", "MonthNo")
        check("rename succeeds", r.success, r.message)
        date = p.tmdl("tables/Dim Date.tmdl")
        block = date[date.index("\tcolumn MonthNo"):date.index("\thierarchy Calendar")]
        lacks("isNameInferred gone from the renamed column", block, "isNameInferred")
        has("source mapping kept", block, "\t\tsourceColumn: [Month Number]\n")
        has("sortByColumn of the sibling column follows, unquoted", date, "\t\tsortByColumn: MonthNo\n")
        check("other columns keep the flag", date.count("isNameInferred") == 2, str(date.count("isNameInferred")))
        has("the DAX that names the source column is untouched", date, '"Month Number", MONTH([Date])')
        check("removal is reported", warned(r, "isNameInferred"), str(r.warnings))


def test_expression_kinds():
    print("\n== fenced expressions, KPIs, detail rows, format strings, calculation items ==")
    tick = "`" * 3
    files = {
        "model.tmdl": "model Model\n\tdiscourageImplicitMeasures\n\nref table Fact\nref table 'Time Calc'\n",
        "tables/Fact.tmdl": (
            "table Fact\n"
            "\tcolumn Qty\n\t\tdataType: int64\n\t\tsourceColumn: Qty\n\n"
            "\tmeasure Units = " + tick + "\n\t\t\tSUM ( Fact[Qty] )\n\t\t\t" + tick + "\n"
            "\t\tformatStringDefinition = IF ( SUM ( Fact[Qty] ) > 1000, \"#,0,K\", \"#,0\" )\n"
            "\t\tdetailRowsDefinition = SELECTCOLUMNS ( Fact, \"Quantity\", Fact[Qty] )\n\n"
            "\t\tkpi\n"
            "\t\t\ttargetExpression = MAX ( Fact[Qty] ) * 2\n"
            "\t\t\tstatusExpression = IF ( [Units] > 10, 1, -1 )\n\n"
            "\tmeasure Double = [Units] * 2\n"),
        "tables/Time Calc.tmdl": (
            "table 'Time Calc'\n"
            "\tcalculationGroup\n"
            "\t\tprecedence: 1\n\n"
            "\t\tcalculationItem Scaled =\n"
            "\t\t\t\tSELECTEDMEASURE() * MAX ( Fact[Qty] )\n\n"
            "\t\t\tformatStringDefinition = IF ( ISSELECTEDMEASURE ( [Units] ), \"0\", SELECTEDMEASUREFORMATSTRING() )\n\n"
            "\tcolumn Calc\n\t\tdataType: string\n\t\tsourceColumn: Name\n"),
    }
    with tempfile.TemporaryDirectory() as tmp:
        p = fx.build_project(tmp, model=files)
        r = p.connector.rename_column_in_files("Fact", "Qty", "Quantity Sold")
        check("column rename succeeds", r.success, r.message)
        fact = p.tmdl("tables/Fact.tmdl")
        has("fenced measure expression", fact, "\t\t\tSUM ( Fact[Quantity Sold] )\n\t\t\t" + tick)
        has("format string expression", fact, "formatStringDefinition = IF ( SUM ( Fact[Quantity Sold] ) > 1000")
        has("detail rows; the output column name it defines is kept", fact,
            'SELECTCOLUMNS ( Fact, "Quantity", Fact[Quantity Sold] )')
        has("KPI target", fact, "targetExpression = MAX ( Fact[Quantity Sold] ) * 2")
        calc = p.tmdl("tables/Time Calc.tmdl")
        has("calculation item", calc, "SELECTEDMEASURE() * MAX ( Fact[Quantity Sold] )")

        r = p.connector.rename_measure_in_files("Units", "Units Sold")
        check("measure rename succeeds", r.success, r.message)
        fact = p.tmdl("tables/Fact.tmdl")
        has("declaration with a fenced expression", fact, "\tmeasure 'Units Sold' = " + tick + "\n")
        has("KPI status", fact, "statusExpression = IF ( [Units Sold] > 10, 1, -1 )")
        has("other measure", fact, "measure Double = [Units Sold] * 2")
        has("calculation item format string", p.tmdl("tables/Time Calc.tmdl"), "ISSELECTEDMEASURE ( [Units Sold] )")


def test_m_shadowing_is_left_alone():
    print("\n== M: a step that shares the table's name is not the table ==")
    other = (
        "table Other\n\tcolumn A\n\t\tdataType: int64\n\t\tsourceColumn: A\n\n"
        "\tpartition Other = m\n\t\tmode: import\n\t\tsource =\n"
        "\t\t\t\tlet\n"
        "\t\t\t\t    Sales = Csv.Document(File.Contents(\"sales.csv\")),\n"
        "\t\t\t\t    Typed = Table.TransformColumnTypes(Sales, {{\"A\", Int64.Type}})\n"
        "\t\t\t\tin\n"
        "\t\t\t\t    Typed\n")
    files = model_with(**{"tables/Other.tmdl": other})
    files["model.tmdl"] = fx.MODEL + "ref table Other\n"
    with tempfile.TemporaryDirectory() as tmp:
        p = fx.build_project(tmp, model=files)
        before = (p.definition / "tables/Other.tmdl").read_bytes()
        r = p.connector.rename_table_in_files("Sales", "Sales Data")
        check("rename succeeds", r.success, r.message)
        check("query with its own 'Sales' step is byte-identical",
              (p.definition / "tables/Other.tmdl").read_bytes() == before)
        check("and that is reported", warned(r, "defines its own 'Sales'", "Other"), str(r.warnings))


def test_encoding_and_line_endings_survive():
    print("\n== BOM, CRLF and LF files keep their format through a rename ==")
    for crlf, bom in ((True, False), (False, False), (True, True), (False, True)):
        tag = f"{'CRLF' if crlf else 'LF'}{'+BOM' if bom else ''}"
        with tempfile.TemporaryDirectory() as tmp:
            p = fx.build_project(tmp, crlf=crlf, bom=bom)
            before = fx.snapshot(p.root)
            r = p.connector.rename_table_in_files("Sales", "Sales Data")
            check(f"{tag}: rename succeeds", r.success, r.message)
            after = fx.snapshot(p.root)
            changed = [k for k in before if before[k] != after[k]]
            check(f"{tag}: several files changed", len(changed) >= 6, str(changed))
            raw = after[f"{p.name}.SemanticModel/definition/tables/Sales.tmdl"]
            check(f"{tag}: BOM kept exactly once", raw.startswith(fx.BOM) == bom and raw.count(fx.BOM) == int(bom))
            check(f"{tag}: declaration on a BOM'd first line was renamed",
                  raw.decode("utf-8-sig").startswith("table 'Sales Data'"))
            for k in changed:
                data = after[k]
                pure = (data.count(b"\r\n") == data.count(b"\n")) if crlf else (b"\r" not in data)
                check(f"{tag}: {k.rsplit('/', 1)[-1]} keeps its line endings", pure)
            untouched = [k for k in before if k not in changed]
            check(f"{tag}: untouched files byte-identical", all(before[k] == after[k] for k in untouched))


def test_round_trip_is_byte_identical():
    print("\n== renaming there and back restores every byte ==")
    with tempfile.TemporaryDirectory() as tmp:
        p = fx.build_project(tmp)
        before = fx.snapshot(p.root)
        steps = [
            lambda: p.connector.rename_table_in_files("Customers", "Clients"),
            lambda: p.connector.rename_column_in_files("Sales", "CustomerID", "ClientKey"),
            lambda: p.connector.rename_measure_in_files("Rev x2", "Rev_x2_b"),
            lambda: p.connector.rename_measure_in_files("Rev_x2_b", "Rev x2"),
            lambda: p.connector.rename_column_in_files("Sales", "ClientKey", "CustomerID"),
            lambda: p.connector.rename_table_in_files("Clients", "Customers"),
        ]
        results = [step() for step in steps]
        check("all six renames succeed", all(r.success for r in results), str([r.message for r in results]))
        after = fx.snapshot(p.root)
        check("project is byte-identical again", before == after,
              str([k for k in before if before[k] != after.get(k)]))


if __name__ == "__main__":
    print("=" * 70)
    print("  PBIP RENAME: MODEL (TMDL) TESTS")
    print("=" * 70)
    for fn in (test_column_rename_is_scoped_to_its_table, test_column_rename_with_a_same_named_measure,
               test_measure_rename_leaves_columns_alone, test_table_rename_everywhere,
               test_names_that_broke_regex_templates, test_old_name_with_apostrophe,
               test_structural_properties_are_scoped, test_inferred_name_flag_is_removed,
               test_expression_kinds, test_m_shadowing_is_left_alone,
               test_encoding_and_line_endings_survive, test_round_trip_is_byte_identical):
        fn()
    print("\n" + "=" * 70)
    if _failures:
        print(f"  {len(_failures)} CHECK(S) FAILED: {', '.join(_failures)}")
        sys.exit(1)
    print("  ALL PBIP MODEL RENAME CHECKS PASSED")
    print("=" * 70)
