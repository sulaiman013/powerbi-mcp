"""
Quoting in TMDL and DAX: the name helpers, detection and repair of multi-word table names used
without quotes in DAX, and renames whose new name needs quotes (table declaration, DAX,
relationship endpoints written as Table.Column, the way TMDL stores them).
No Power BI required. Run: python tests/test_dax_quoting_fixes.py
"""
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import pbip_fixtures as fx  # noqa: E402
from powerbi_pbip_connector import (  # noqa: E402
    fix_dax_table_references,
    needs_tmdl_quoting,
    quote_mcode_name,
    quote_tmdl_name,
    unquote_tmdl_name,
)

_failures = []


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f": {detail}" if detail and not cond else ""))
    if not cond:
        _failures.append(name)


def eq(name, actual, expected):
    check(name, actual == expected, f"got {actual!r}, expected {expected!r}")


def test_tmdl_quoting():
    print("\n== name quoting helpers ==")
    for name, expected, why in [
        ("Sales", False, "simple name"),
        ("Customer Appointments", True, "space"),
        ("_measures", False, "underscore prefix"),
        ("123Table", True, "starts with a digit"),
        ("table", True, "reserved word"),
        ("Normal_Name", False, "underscore inside"),
        ("Name's", True, "apostrophe"),
        ("Leads Sales Data", True, "several words"),
    ]:
        eq(f"needs_tmdl_quoting({name!r}): {why}", needs_tmdl_quoting(name), expected)
    for name, expected in [
        ("Sales", "Sales"),
        ("Customer Appointments", "'Customer Appointments'"),
        ("My Table's Data", "'My Table''s Data'"),
    ]:
        eq(f"quote_tmdl_name({name!r})", quote_tmdl_name(name), expected)
        eq(f"unquote round-trips {name!r}", unquote_tmdl_name(quote_tmdl_name(name)), name)
    eq("M: regular identifier stays bare", quote_mcode_name("Salesforce_Data"), "Salesforce_Data")
    eq("M: spaces use #\"...\"", quote_mcode_name("Leads Sales Data"), '#"Leads Sales Data"')
    eq("M: an inner double quote is doubled", quote_mcode_name('6" Pipe'), '#"6"" Pipe"')
    eq("M: a keyword is quoted", quote_mcode_name("type"), '#"type"')


def test_fix_dax_table_references():
    print("\n== quoting multi-word table names inside one DAX expression ==")
    one = ["Leads Sales Data"]
    for label, dax, tables, expected in [
        ("simple reference", "SUM(Leads Sales Data[Amount])", one, "SUM('Leads Sales Data'[Amount])"),
        ("nested function", "CALCULATE(SUM(Leads Sales Data[Amount]))", one,
         "CALCULATE(SUM('Leads Sales Data'[Amount]))"),
        ("already quoted is left alone", "SUM('Leads Sales Data'[Amount])", one, "SUM('Leads Sales Data'[Amount])"),
        ("inside VAR / RETURN", "var x = SUM(Leads Sales Data[Amount]) RETURN x", one,
         "var x = SUM('Leads Sales Data'[Amount]) RETURN x"),
        ("several references", 'CALCULATE(SUM(Leads Sales Data[Amount]), Leads Sales Data[Status] = "Won")', one,
         'CALCULATE(SUM(\'Leads Sales Data\'[Amount]), \'Leads Sales Data\'[Status] = "Won")'),
        ("comparison operands", "IF(Leads Sales Data[Amount] > 1000, Leads Sales Data[Amount] * 2, 0)", one,
         "IF('Leads Sales Data'[Amount] > 1000, 'Leads Sales Data'[Amount] * 2, 0)"),
        ("mixed quoted and unquoted", "SUM(Leads Sales Data[Amount]) + SUM('Other Table'[Amount])",
         ["Leads Sales Data", "Other Table"], "SUM('Leads Sales Data'[Amount]) + SUM('Other Table'[Amount])"),
        ("whole table as a function argument", "COUNTROWS(Leads Sales Data) + COUNTROWS(FILTER(Leads Sales Data, TRUE()))",
         one, "COUNTROWS('Leads Sales Data') + COUNTROWS(FILTER('Leads Sales Data', TRUE()))"),
        ("a longer table name wins over its suffix", "SUM(Leads Sales Data[Amount]) + SUM(Sales Data[Amount])",
         ["Sales Data", "Leads Sales Data"], "SUM('Leads Sales Data'[Amount]) + SUM('Sales Data'[Amount])"),
        ("text in a string literal is not DAX", '"Leads Sales Data[Amount]" & SUM(Leads Sales Data[Amount])', one,
         '"Leads Sales Data[Amount]" & SUM(\'Leads Sales Data\'[Amount])'),
        ("text in a comment is not DAX", "SUM(Leads Sales Data[Amount]) -- Leads Sales Data[Amount]", one,
         "SUM('Leads Sales Data'[Amount]) -- Leads Sales Data[Amount]"),
        ("a measure name in brackets is not a table", "[Leads Sales Data] + 1", one, "[Leads Sales Data] + 1"),
        ("single-word tables are never touched", "SUM(Sales[Amount])", ["Sales"], "SUM(Sales[Amount])"),
    ]:
        eq(label, fix_dax_table_references(dax, tables), expected)


MODEL = "model Model\n\nref table Salesforce_Data\nref table dim_projects\nref table 'Leads Sales Data'\n"
SALESFORCE = """table Salesforce_Data
\tcolumn 'Created Date'
\t\tdataType: dateTime
\t\tsourceColumn: Created Date

\tcolumn Amount
\t\tdataType: decimal
\t\tsourceColumn: Amount

\tcolumn ProjectId
\t\tdataType: string
\t\tsourceColumn: ProjectId

\tmeasure TotalAmount = SUM(Salesforce_Data[Amount])

\tmeasure 'Total with Spaces' =
\t\t\tVAR convertedProject = SELECTEDVALUE(dim_projects[Project Name])
\t\t\tRETURN
\t\t\t    IF(convertedProject <> BLANK(), CALCULATE(SUM(Leads Sales Data[Amount])), BLANK())
"""
PROJECTS = """table dim_projects
\tcolumn ProjectId
\t\tdataType: string
\t\tsourceColumn: ProjectId

\tcolumn 'Project Name'
\t\tdataType: string
\t\tsourceColumn: Project Name
"""
LEADS = """table 'Leads Sales Data'
\tcolumn Amount
\t\tdataType: decimal
\t\tsourceColumn: Amount
"""
RELATIONSHIPS = """relationship 'Salesforce_Data to dim_projects'
\tfromColumn: Salesforce_Data.ProjectId
\ttoColumn: dim_projects.ProjectId

relationship 5c0f8a52-0f5e-4c5f-9d0e-6f0d6c1a2b3c
\tfromColumn: Salesforce_Data.'Created Date'
\ttoColumn: dim_projects.'Project Name'
"""
FILES = {"model.tmdl": MODEL, "relationships.tmdl": RELATIONSHIPS, "tables/Salesforce_Data.tmdl": SALESFORCE,
         "tables/dim_projects.tmdl": PROJECTS, "tables/Leads Sales Data.tmdl": LEADS}


def kinds(errors):
    return sorted(e.error_type for e in errors)


def test_detect_and_fix_in_a_project():
    print("\n== detect and repair unquoted table names across a project ==")
    with tempfile.TemporaryDirectory() as tmp:
        p = fx.build_project(tmp, name="TestProject", model=FILES)
        c = p.connector
        errors = c.validate_tmdl_syntax()
        eq("the unquoted table in a multi-line measure body is found, once", kinds(errors), ["UNQUOTED_TABLE_IN_DAX"])
        check("it points at the measure", errors and errors[0].file_path.endswith("Salesforce_Data.tmdl")
              and "Total with Spaces" in errors[0].context, str(errors))
        before = fx.snapshot(p.root)
        plan = c.fix_all_dax_quoting(dry_run=True)
        check("dry run reports the plan", plan["count"] == 1 and len(plan["files_modified"]) == 1
              and plan["tables_fixed"] == ["Leads Sales Data"] and plan["dry_run"] is True, str(plan))
        check("dry run writes nothing", fx.snapshot(p.root) == before)
        result = c.fix_all_dax_quoting()
        check("fix reports what it changed", result["count"] == 1 and result["errors"] == []
              and [os.path.basename(f) for f in result["files_modified"]] == ["Salesforce_Data.tmdl"], str(result))
        text = p.tmdl("tables/Salesforce_Data.tmdl")
        check("reference is quoted", "CALCULATE(SUM('Leads Sales Data'[Amount]))" in text, text)
        check("a bracketed column with a space is untouched", "SELECTEDVALUE(dim_projects[Project Name])" in text)
        eq("no validation errors remain", kinds(c.validate_tmdl_syntax()), [])
        again = c.fix_all_dax_quoting()
        check("a second run finds nothing to do", again["count"] == 0 and again["files_modified"] == [], str(again))
        check("the fix can be undone", c.rollback_changes() and fx.snapshot(p.root) == before)


def test_validator_reports_structural_problems():
    print("\n== validator: names and endpoints that need quotes, unresolved references ==")
    bad = dict(FILES)
    bad["tables/Bad.tmdl"] = "table Bad Name\n\tcolumn A\n\t\tdataType: int64\n"
    bad["relationships.tmdl"] = RELATIONSHIPS + (
        "\nrelationship r3\n\tfromColumn: Salesforce_Data.Created Date\n\ttoColumn: dim_projects.Missing\n")
    with tempfile.TemporaryDirectory() as tmp:
        p = fx.build_project(tmp, name="TestProject", model=bad)
        errors = p.connector.validate_tmdl_syntax()
        by_type = {}
        for e in errors:
            by_type.setdefault(e.error_type, []).append(e)
        check("unquoted table name with a space", len(by_type.get("UNQUOTED_NAME", [])) == 1
              and by_type["UNQUOTED_NAME"][0].line_number == 1, str(by_type.get("UNQUOTED_NAME")))
        check("relationship endpoint with an unquoted space", len(by_type.get("UNQUOTED_REFERENCE", [])) == 1
              and "Created Date" in by_type["UNQUOTED_REFERENCE"][0].message, str(by_type.get("UNQUOTED_REFERENCE")))
        check("relationship endpoint that does not exist",
              any("'dim_projects'[missing]" in e.message for e in by_type.get("UNRESOLVED_REFERENCE", [])),
              str([e.message for e in by_type.get("UNRESOLVED_REFERENCE", [])]))
        check("the unquoted DAX table is still reported", len(by_type.get("UNQUOTED_TABLE_IN_DAX", [])) == 1)


def test_rename_with_quoting():
    print("\n== rename to a name that needs quotes ==")
    with tempfile.TemporaryDirectory() as tmp:
        p = fx.build_project(tmp, name="TestProject", model=FILES)
        c = p.connector
        check("repair the unquoted reference first", c.fix_all_dax_quoting()["count"] == 1)
        r = c.rename_table_in_files("Salesforce_Data", "Sales Force Data")
        check("rename succeeds", r.success, r.message)
        text = p.tmdl("tables/Salesforce_Data.tmdl")
        check("table declaration quoted", text.startswith("table 'Sales Force Data'\n"), text[:40])
        check("DAX reference quoted", "measure TotalAmount = SUM('Sales Force Data'[Amount])" in text, text)
        rel = p.tmdl("relationships.tmdl")
        check("relationship name follows", "relationship 'Sales Force Data to dim_projects'\n" in rel, rel)
        check("endpoint table quoted, column kept", "\tfromColumn: 'Sales Force Data'.ProjectId\n" in rel, rel)
        check("endpoint with a quoted column", "\tfromColumn: 'Sales Force Data'.'Created Date'\n" in rel, rel)
        check("the other endpoint is untouched", "\ttoColumn: dim_projects.ProjectId\n" in rel, rel)
        check("model.tmdl ref follows", "ref table 'Sales Force Data'\n" in p.tmdl("model.tmdl"))
        eq("the model validates", kinds(c.validate_tmdl_syntax()), [])

        r = c.rename_table_in_files("Sales Force Data", "SalesForce")
        check("rename back to a name without spaces succeeds", r.success, r.message)
        check("quotes are dropped where TMDL does not need them",
              p.tmdl("tables/Salesforce_Data.tmdl").startswith("table SalesForce\n")
              and "\tfromColumn: SalesForce.ProjectId\n" in p.tmdl("relationships.tmdl"))


def test_column_rename_in_relationships():
    print("\n== column rename reaches relationship endpoints of that table only ==")
    with tempfile.TemporaryDirectory() as tmp:
        p = fx.build_project(tmp, name="TestProject", model=FILES)
        c = p.connector
        c.fix_all_dax_quoting()
        r = c.rename_column_in_files("Salesforce_Data", "ProjectId", "Project Key")
        check("rename succeeds", r.success, r.message)
        rel = p.tmdl("relationships.tmdl")
        check("fromColumn follows, quoted", "\tfromColumn: Salesforce_Data.'Project Key'\n" in rel, rel)
        check("same-named column of the other table untouched", "\ttoColumn: dim_projects.ProjectId\n" in rel, rel)
        check("the other table's declaration untouched", "\tcolumn ProjectId\n" in p.tmdl("tables/dim_projects.tmdl"))
        r = c.rename_column_in_files("dim_projects", "Project Name", "ProjectName")
        check("second rename succeeds", r.success, r.message)
        check("a quoted endpoint column loses its quotes when it no longer needs them",
              "\ttoColumn: dim_projects.ProjectName\n" in p.tmdl("relationships.tmdl"), p.tmdl("relationships.tmdl"))
        check("DAX reference follows", "SELECTEDVALUE(dim_projects[ProjectName])" in p.tmdl("tables/Salesforce_Data.tmdl"))
        eq("the model validates", kinds(c.validate_tmdl_syntax()), [])


if __name__ == "__main__":
    print("=" * 70)
    print("  TMDL / DAX QUOTING TESTS")
    print("=" * 70)
    for fn in (test_tmdl_quoting, test_fix_dax_table_references, test_detect_and_fix_in_a_project,
               test_validator_reports_structural_problems, test_rename_with_quoting,
               test_column_rename_in_relationships):
        fn()
    print("\n" + "=" * 70)
    if _failures:
        print(f"  {len(_failures)} CHECK(S) FAILED: {', '.join(_failures)}")
        sys.exit(1)
    print("  ALL QUOTING CHECKS PASSED")
    print("=" * 70)
