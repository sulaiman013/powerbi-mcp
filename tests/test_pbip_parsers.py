"""
Tests for the parsers the offline PBIP path is built on: position-preserving JSON editing
(pbip_json), the TMDL object scanner and model catalog (tmdl_scan), the Power Query identifier
rewriter (pbip_mquery), and the read-only diagnostic tool that uses them.
No Power BI required. Run: python tests/test_pbip_parsers.py
"""
import json
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import pbip_fixtures as fx  # noqa: E402
import pbip_json as pj  # noqa: E402
import pbip_mquery as mq  # noqa: E402
import tmdl_scan as ts  # noqa: E402

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pbip_diagnostic_tool as diag  # noqa: E402

_failures = []
BACKSLASH = chr(92)


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f": {detail}" if detail and not cond else ""))
    if not cond:
        _failures.append(name)


def eq(name, actual, expected):
    check(name, actual == expected, f"got {actual!r}, expected {expected!r}")


def test_json_parse_and_edit():
    print("\n== pbip_json: same values as the standard parser, edits touch only their token ==")
    samples = [
        '{"a": 1, "b": [true, false, null, -2.50, 1E3, 0.000010], "c": {"d": "x"}}',
        '  [ ]  ', '{}', '"just a string"', '{"k": "tab' + BACKSLASH + 't quote' + BACKSLASH + '" slash' + BACKSLASH + '/"}',
        json.dumps({"nested": {"deep": [{"x": [1, [2, [3]]]}]}, "uni": "caf" + chr(0xE9)}, indent=4, ensure_ascii=False),
        json.dumps(fx.visual("v", "card", {"Values": [fx.projection(fx.measure("T", "M"), "T.M")]}), indent=2),
    ]
    for s in samples:
        eq(f"parses like json.loads: {s[:30]!r}", pj.parse(s), json.loads(s))
        eq(f"no edits means no change: {s[:30]!r}", pj.Edits(s).apply(), s)
    for bad in ('{"a":1,}', "[1 2]", '{"a"}', "", '{"a":1} x', "{'a':1}", "[01]", '{"a": tru}', '["unterminated]'):
        try:
            pj.parse(bad)
            check(f"rejects {bad!r}", False)
        except pj.JsonSyntaxError:
            check(f"rejects {bad!r}", True)

    text = '{\n\t"num": 1.50,   "name": "old",\n\t"map": {"old": {"x": 1E3}},\n\t"list": ["a", "old"]\n}\n'
    doc = pj.parse(text)
    edits = pj.Edits(text)
    check("set_string reports a change", edits.set_string(doc["name"], "new \"quoted\"") is True)
    check("set_string with the same value is not an edit", edits.set_string(doc["list"][0], "a") is False)
    check("rename_key", edits.rename_key(doc["map"], "old", "new key") is True)
    check("rename_key of a missing key", edits.rename_key(doc["map"], "nope", "x") is False)
    edits.set_string(doc["list"][1], "caf" + chr(0xE9))
    out = edits.apply()
    eq("only the edited tokens differ; numbers, tabs and spacing are kept", out,
       '{\n\t"num": 1.50,   "name": "new ' + BACKSLASH + '"quoted' + BACKSLASH + '"",\n\t"map": {"new key": {"x": 1E3}},'
       '\n\t"list": ["a", "caf' + chr(0xE9) + '"]\n}\n')
    eq("the result parses to the intended values", json.loads(out)["map"], {"new key": {"x": 1000.0}})

    escaped = '{"a": "x' + BACKSLASH + 'u00e9", "b": "plain"}'
    doc = pj.parse(escaped)
    edits = pj.Edits(escaped)
    edits.set_string(doc["a"], "y" + chr(0xE9))
    edits.set_string(doc["b"], "z" + chr(0xE9))
    eq("a token written with unicode escapes keeps that style; others stay readable", edits.apply(),
       '{"a": "y' + BACKSLASH + 'u00e9", "b": "z' + chr(0xE9) + '"}')
    eq("literal_unwrap", (pj.literal_unwrap("'it''s'"), pj.literal_unwrap("12D"), pj.literal_unwrap("true")),
       ("it's", None, None))
    eq("literal_wrap", pj.literal_wrap("it's"), "'it''s'")


TMDL = """/// A doc comment that mentions table Fake and measure Fake = 1
table 'Net Sales'
\tlineageTag: abc

\tmeasure Total = SUM('Net Sales'[Amount])
\t\tformatString: #,0

\tmeasure 'Big One' =
\t\t\tVAR x = 1

\t\t\tRETURN x
\t\tdisplayFolder: Stuff

\t\tannotation Note = {"a": 1}

\tmeasure Fenced = ```
\t\t\tmeasure NotAnObject = 1
\t\t\t```
\t\tlineageTag: def

\tcolumn Amount
\t\tdataType: double
\t\tsortByColumn: 'Sort Key'
\t\tsourceColumn: Amount

\tcolumn Calc = [Amount] * 2
\t\tdataType: double

\thierarchy 'Geo H'
\t\tlevel Top
\t\t\tcolumn: Amount

\tpartition 'Net Sales' = m
\t\tmode: import
\t\tsource =
\t\t\t\tlet
\t\t\t\t    measure = 1
\t\t\t\tin
\t\t\t\t    measure

\tannotation PBI_ResultType = Table
"""


def test_tmdl_scan():
    print("\n== tmdl_scan: objects, properties and expression extents ==")
    doc = ts.scan(TMDL)
    table = doc.roots[0]
    eq("one root: the table, unquoted", (len(doc.roots), table.keyword, table.name), (1, "table", "Net Sales"))
    eq("name span covers the quoted token", TMDL[table.name_span[0]:table.name_span[1]], "'Net Sales'")
    kids = [(c.keyword, c.name) for c in table.children]
    eq("children are the table's own objects (lines inside expressions are not objects)", kids,
       [("lineageTag", None), ("measure", "Total"), ("measure", "Big One"), ("measure", "Fenced"),
        ("column", "Amount"), ("column", "Calc"), ("hierarchy", "Geo H"), ("partition", "Net Sales"),
        ("annotation", "PBI_ResultType")])
    total, big, fenced = table.children_of("measure")
    eq("single-line expression", doc.expression(total), "SUM('Net Sales'[Amount])")
    eq("multi-line expression keeps its blank line and stops before the properties",
       doc.expression(big), "\t\t\tVAR x = 1\n\n\t\t\tRETURN x")
    eq("properties after a multi-line expression belong to the measure",
       [(c.keyword, c.value) for c in big.children if c.value is not None], [("displayFolder", "Stuff")])
    eq("fenced expression is taken verbatim", doc.expression(fenced).strip(), "measure NotAnObject = 1")
    eq("the property after the fence is still the measure's", fenced.child("lineageTag").value, "def")
    amount = table.children_of("column")[0]
    eq("property value and span", (amount.child("sortByColumn").value,
                                   TMDL[slice(*amount.child("sortByColumn").value_span)]), ("'Sort Key'", "'Sort Key'"))
    level_col = table.child("hierarchy").child("level").child("column")
    eq("a hierarchy level's column is a property, and knows its table", (level_col.value, level_col.table),
       ("Amount", "Net Sales"))
    part = table.child("partition")
    eq("languages", [doc.language(n) for n in (total, table.children_of("column")[1], part.child("source"),
                                               part, big.child("annotation"))],
       ["dax", "dax", "m", None, None])
    check("M body is one expression, not objects", "measure = 1" in doc.expression(part.child("source"))
          and not any(n.name == "=" for n in doc.walk()))
    check("nothing is malformed", not [n for n in doc.walk() if n.malformed])
    eq("line numbers are 1-based", (table.line, total.line), (2, 5))

    crlf = ts.scan(TMDL.replace("\n", "\r\n"))
    eq("CRLF text scans to the same tree", [(n.keyword, n.name, n.value) for n in crlf.walk()],
       [(n.keyword, n.name, n.value) for n in doc.walk()])
    spaces = ts.scan("table T\n    column A\n        dataType: int64\n    measure M =\n            1 +\n            2\n        formatString: 0\n")
    m = spaces.roots[0].child("measure")
    eq("a file indented with spaces nests the same way", (spaces.roots[0].child("column").child("dataType").value,
                                                         spaces.expression(m).split(), m.child("formatString").value),
       ("int64", ["1", "+", "2"], "0"))
    bad = ts.scan("table My Table\n\tcolumn A\n???\n\tmeasure X = ```\n\t\t\tnever closed\n")
    eq("problems are recorded, not raised", sorted(n.malformed.split(" (")[0].split(":")[0] for n in bad.walk() if n.malformed),
       ["expression opened with ``` is never closed", "line does not start with a TMDL keyword",
        "unexpected text after the object name"])

    eq("split_object_path", [p[0] for p in ts.split_object_path("'Dim Date'.'Month Name'")], ["Dim Date", "Month Name"])
    eq("split_object_path with an escaped quote", [p[0] for p in ts.split_object_path("'O''Brien'.Col")], ["O'Brien", "Col"])
    eq("split_object_path rejects an unquoted space", ts.split_object_path("My Table.Col"), [])
    eq("quote_name", [ts.quote_name(n) for n in ("Sales", "Net Sales", "a.b", "x=y", "O'Brien", "Sales-2024", "k:v")],
       ["Sales", "'Net Sales'", "'a.b'", "'x=y'", "'O''Brien'", "Sales-2024", "'k:v'"])

    cat = ts.ModelCatalog.from_texts({"t.tmdl": TMDL, "o.tmdl": "table Other\n\tcolumn Amount\n\tmeasure Total2 = 1\n"})
    check("catalog lookups are case-insensitive", cat.has_column("net sales", "AMOUNT") and cat.has_measure("Net Sales", "big one")
          and cat.has_member("Net Sales", "Geo H") and cat.has_member("Net Sales", "Top"))
    check("catalog does not invent objects from expression text", not cat.has_field("Net Sales", "NotAnObject")
          and not cat.has_table("Fake"))
    eq("which tables own a name", (sorted(cat.column_tables("amount")), cat.measure_tables("TOTAL")),
       (["Net Sales", "Other"], ["Net Sales"]))
    dup = ts.ModelCatalog.from_texts({"a": "table T\n\tcolumn X\n\tmeasure X = 1\n", "b": "table t\n"})
    check("duplicates are reported", len(dup.duplicates) == 2, str(dup.duplicates))


def test_m_identifier_rewrite():
    print("\n== pbip_mquery: only references to the query follow a rename ==")
    cases = [
        ("plain reference", "let Source = Sales in Source", "Sales", "Orders", ("let Source = Orders in Source", 1, False)),
        ("new name needs M quoting", 'Table.SelectRows(Sales, each [Amount] > 1)', "Sales", "Sales Data",
         ('Table.SelectRows(#"Sales Data", each [Amount] > 1)', 1, False)),
        ("quoted reference and list", 'Table.Combine({#"Net Sales", Other, #"Net Sales"})', "Net Sales", "Sales",
         ("Table.Combine({Sales, Other, Sales})", 2, False)),
        ("strings name source objects and are never touched", 'Source{[Schema="dbo",Item="Sales"]}[Data] & "Sales"',
         "Sales", "X", ('Source{[Schema="dbo",Item="Sales"]}[Data] & "Sales"', 0, False)),
        ("dataflow entity reference untouched", '{[entity="Sales",version=""]}[Data]', "Sales", "X",
         ('{[entity="Sales",version=""]}[Data]', 0, False)),
        ("field access and field names are not the query", "let r = Src[Sales], t = [Sales = 1, b = Sales] in r", "Sales", "X",
         ("let r = Src[Sales], t = [Sales = 1, b = X] in r", 1, False)),
        ("a record type's field is not the query", "#table(type table [Sales = text], {}) & Sales", "Sales", "X",
         ("#table(type table [Sales = text], {}) & X", 1, False)),
        ("a let step of that name shadows the query", 'let Sales = Csv.Document("x"), T = Sales in T', "Sales", "X",
         ('let Sales = Csv.Document("x"), T = Sales in T', 0, True)),
        ("a function parameter of that name shadows the query", "let f = (Sales) => Sales + 1 in f(1)", "Sales", "X",
         ("let f = (Sales) => Sales + 1 in f(1)", 0, True)),
        ("comments untouched", "Sales // Sales\n/* Sales */", "Sales", "X", ("X // Sales\n/* Sales */", 1, False)),
        ("names are case-sensitive in M", "sales & SALES & Sales", "Sales", "X", ("sales & SALES & X", 1, False)),
        ("longer identifiers are different identifiers", "Sales2 & Sales.EU & MySales", "Sales", "X",
         ("Sales2 & Sales.EU & MySales", 0, False)),
        ("a double quote in the new name is doubled", "Sales", "Sales", '6" Pipe', ('#"6"" Pipe"', 1, False)),
    ]
    for label, src, old, new, expected in cases:
        eq(label, mq.rename_query_references(src, old, new), expected)
    weird = 'let a = #"unterminated, b = "also unterminated'
    eq("odd input round-trips through the tokenizer", "".join(t.text for t in mq.tokenize(weird)), weird)


def test_diagnostic_tool_is_read_only():
    print("\n== pbip_diagnostic_tool: analysis and fix preview never write ==")
    files = {
        "model.tmdl": "model Model\n\nref table 'Leads Sales Data'\n",
        "tables/Leads.tmdl": ("table 'Leads Sales Data'\n\tcolumn Amount\n\t\tdataType: double\n\t\tsourceColumn: Amount\n\n"
                              "\tmeasure M = SUM(Leads Sales Data[Amount])\n"),
    }
    with tempfile.TemporaryDirectory() as tmp:
        p = fx.build_project(tmp, model=files, visuals={"overview/v": fx.visual("v", "card", {"Values": [
            fx.projection(fx.measure("Leads Sales Data", "Gone"), "Leads Sales Data.Gone")]})})
        before = fx.snapshot(p.root)
        analysis = diag.analyze_pbip_structure(str(p.pbip))
        eq("analysis counts the unquoted DAX table and the broken binding",
           (analysis["error_summary"]["unquoted_dax"], analysis["error_summary"]["report_binding_errors"]), (1, 1))
        preview = diag.preview_dax_quoting_fix(str(p.pbip))
        eq("preview reports what would change",
           (preview["references_to_fix"], [os.path.basename(f) for f in preview["files_to_modify"]],
            preview["tables_affected"]), (1, ["Leads.tmdl"], ["Leads Sales Data"]))
        check("neither call modified or added a file", fx.snapshot(p.root) == before
              and not (p.root / ".pbip-backups").exists())
        check("the command-line run is read-only too", _run_main(str(p.pbip)) == 0 and fx.snapshot(p.root) == before)
        check("a bad path is reported, not raised", "error" in diag.analyze_pbip_structure(os.path.join(tmp, "nope.pbip")))
        found = diag.find_pbip_files(tmp)
        eq("find_pbip_files finds the project", [f.name for f in found], ["Retail.pbip"])
        fx.write(p.root / ".pbip-backups" / "old" / "Retail.pbip", "{}")
        fx.write(p.root / "a" / "b" / "c" / "d" / "Deep.pbip", "{}")
        eq("backups and folders deeper than max_depth are skipped",
           [f.name for f in diag.find_pbip_files(tmp, max_depth=3)], ["Retail.pbip"])


def _run_main(path):
    import contextlib
    import io
    argv = sys.argv
    sys.argv = ["pbip_diagnostic_tool.py", path]
    try:
        with contextlib.redirect_stdout(io.StringIO()):
            return diag.main()
    finally:
        sys.argv = argv


if __name__ == "__main__":
    print("=" * 70)
    print("  PBIP PARSER TESTS")
    print("=" * 70)
    for fn in (test_json_parse_and_edit, test_tmdl_scan, test_m_identifier_rewrite, test_diagnostic_tool_is_read_only):
        fn()
    print("\n" + "=" * 70)
    if _failures:
        print(f"  {len(_failures)} CHECK(S) FAILED: {', '.join(_failures)}")
        sys.exit(1)
    print("  ALL PBIP PARSER CHECKS PASSED")
    print("=" * 70)
