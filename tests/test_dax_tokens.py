"""
Tests for the shared DAX tokenizer and reference rewriter (src/dax_tokens.py): lossless
lexing, name escaping, reference extraction, and table/column/measure renames that leave
strings, comments and unrelated names untouched. No Power BI required.
Run: python tests/test_dax_tokens.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))
import dax_tokens as dt  # noqa: E402

_failures = []


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f": {detail}" if detail and not cond else ""))
    if not cond:
        _failures.append(name)


def eq(name, actual, expected):
    check(name, actual == expected, f"got {actual!r}, expected {expected!r}")


def kinds(expr):
    return [(t.kind, t.text) for t in dt.tokenize(expr) if t.kind != dt.WS]


def test_lexing():
    print("\n== lexing ==")
    samples = [
        'CALCULATE ( SUM ( Sales[Amount] ), \'Dim Date\'[Year] = 2024 ) -- total\n',
        '"a ""quoted"" string" & [Net]]Sales] /* block\ncomment */ + 1.5e3',
        "'O''Brien'[Col] // trailing",
        "",
        "[unterminated",
        '"unterminated string',
        "'unterminated name",
        "/* unterminated comment",
        "VAR x = 1\r\nRETURN x / .5",
        "Ventes[Montant] + 'Zoë''s'[Größe]",
    ]
    for s in samples:
        eq(f"round-trips {s[:28]!r}", "".join(t.text for t in dt.tokenize(s)), s)
    eq("string keeps doubled quotes", kinds('"a ""b"" c"'), [(dt.STRING, '"a ""b"" c"')])
    eq("bracket keeps doubled ]", kinds("[Net]]Sales]"), [(dt.BRACKET, "[Net]]Sales]")])
    eq("quoted name keeps doubled '", kinds("'O''Brien'"), [(dt.QNAME, "'O''Brien'")])
    eq("line comment stops at newline", kinds("1 -- a/b\n2"),
       [(dt.NUMBER, "1"), (dt.COMMENT, "-- a/b"), (dt.NUMBER, "2")])
    eq("// inside a quoted table is not a comment", kinds("'Sales//Archive'[Amt] / 2"),
       [(dt.QNAME, "'Sales//Archive'"), (dt.BRACKET, "[Amt]"), (dt.OP, "/"), (dt.NUMBER, "2")])
    eq("-- inside a bracket is not a comment", kinds("[Sales -- Net] / [Units]"),
       [(dt.BRACKET, "[Sales -- Net]"), (dt.OP, "/"), (dt.BRACKET, "[Units]")])
    eq('a " inside a bracket is not a string', kinds('[6" Pipe] + 1 -- ratio a/b'),
       [(dt.BRACKET, '[6" Pipe]'), (dt.OP, "+"), (dt.NUMBER, "1"), (dt.COMMENT, "-- ratio a/b")])
    eq("dotted function name is one identifier", kinds("INFO.VIEW.TABLES()"),
       [(dt.IDENT, "INFO.VIEW.TABLES"), (dt.OP, "("), (dt.OP, ")")])
    eq("two-char operators", [t for k, t in kinds("a<=b&&c<>d")], ["a", "<=", "b", "&&", "c", "<>", "d"])
    eq("token name unescapes", [t.name for t in dt.tokenize("'O''Brien'[A]]B]")], ["O'Brien", "A]B"])
    eq("strip_comments keeps offsets and lines",
       dt.strip_comments("a /* x\ny */ b -- c\nd"), "a     \n     b     \nd")


def test_names():
    print("\n== name helpers ==")
    eq("simple name unquoted", dt.quote_table("Sales"), "Sales")
    eq("space needs quotes", dt.quote_table("Net Sales"), "'Net Sales'")
    eq("leading digit needs quotes", dt.quote_table("2024 Sales"), "'2024 Sales'")
    eq("apostrophe doubled", dt.quote_table("O'Brien"), "'O''Brien'")
    eq("keyword needs quotes", dt.quote_table("Table"), "'Table'")
    eq("force quotes", dt.quote_table("Sales", force=True), "'Sales'")
    eq("bracket doubles ]", dt.bracket("Net]Sales"), "[Net]]Sales]")
    eq("column_ref", dt.column_ref("O'Brien", "A]B"), "'O''Brien'[A]]B]")
    check("names_equal is case-insensitive", dt.names_equal("SALES", "sales") and not dt.names_equal("a", None))


def refs(expr):
    return [(r.kind, r.table, r.name) for r in dt.references(expr)]


def test_references():
    print("\n== references ==")
    eq("qualified, bare and table",
       refs("CALCULATE([Total], FILTER(Sales, Sales[Qty] > 0), 'Dim Date'[Year] = 2024)"),
       [("bare", None, "Total"), ("table", "Sales", None), ("column", "Sales", "Qty"),
        ("column", "Dim Date", "Year")])
    eq("whitespace between table and column still qualifies", refs("'Sales' [Amount] + Sales [Qty]"),
       [("column", "Sales", "Amount"), ("column", "Sales", "Qty")])
    eq("keyword before a bracket is not a qualifier", refs("VAR x = [A] RETURN [B]"),
       [("bare", None, "A"), ("bare", None, "B")])
    eq("variables are not tables", refs("VAR t = VALUES(Sales[Region]) RETURN COUNTROWS(t)"),
       [("column", "Sales", "Region")])
    eq("names inside strings and comments are ignored",
       refs('"Sales[Amount]" & [X] // Sales[Hidden]\n/* Other[Y] */'), [("bare", None, "X")])
    eq("function calls are not tables", refs("SUM(Sales[Amount])"), [("column", "Sales", "Amount")])
    eq("a level after a dot is not a reference", refs("CALCULATE([Sales], 'Date'[Date].[Year] = 2024) + [Year]"),
       [("bare", None, "Sales"), ("column", "Date", "Date"), ("bare", None, "Year")])
    eq("renaming a measure leaves a same-named variation level alone",
       dt.rename_measure("CALCULATE([Sales], 'Date'[Date].[Year] = 2024) + [Year]", "Year", "Yr"),
       ("CALCULATE([Sales], 'Date'[Date].[Year] = 2024) + [Yr]", 1))
    eq("renaming a column leaves it alone too",
       dt.rename_column("Date[Date] . [Year] + [Year]", "Date", "Year", "Yr", unqualified=True),
       ("Date[Date] . [Year] + [Yr]", 1, 0))
    tables, columns = dt.referenced_names("EVALUATE TOPN(5, Customers, Customers[ssn])")
    check("bare EVALUATE table is reported", "Customers" in tables)
    check("column pair is reported", ("Customers", "ssn") in columns)


def test_rename_table():
    print("\n== rename table ==")
    eq("qualified refs, quoting only when needed",
       dt.rename_table("SUM(Sales[Amount]) + COUNTROWS(Sales)", "Sales", "Net Sales"),
       ("SUM('Net Sales'[Amount]) + COUNTROWS('Net Sales')", 2))
    eq("quoted stays quoted", dt.rename_table("'Sales'[Amount]", "sales", "Revenue"), ("'Revenue'[Amount]", 1))
    eq("unquoted stays unquoted", dt.rename_table("Sales[Amount]", "Sales", "Revenue"), ("Revenue[Amount]", 1))
    eq("digit-leading new name", dt.rename_table("Sales[Amount]", "Sales", "2024 Sales"),
       ("'2024 Sales'[Amount]", 1))
    eq("new name with apostrophe and backslash", dt.rename_table("Sales[A]", "Sales", "O'Brien \\1"),
       ("'O''Brien \\1'[A]", 1))
    eq("prefix and suffix names untouched",
       dt.rename_table("Sales2[A] + SalesTarget[B] + 'Old Sales'[C]", "Sales", "X"),
       ("Sales2[A] + SalesTarget[B] + 'Old Sales'[C]", 0))
    eq("strings, comments, measures and functions untouched",
       dt.rename_table('"Sales" & [Sales] & Sales(1) -- Sales[x]', "Sales", "X"),
       ('"Sales" & [Sales] & Sales(1) -- Sales[x]', 0))
    eq("a variable shadowing the table is left alone",
       dt.rename_table("VAR Sales = 1 RETURN Sales + 1", "Sales", "X"), ("VAR Sales = 1 RETURN Sales + 1", 0))
    eq("option word is not the table",
       dt.rename_table("CALCULATE([T], DATEADD('Date'[Date], -1, MONTH)) + COUNTROWS(Month)", "Month", "Fiscal Month"),
       ("CALCULATE([T], DATEADD('Date'[Date], -1, MONTH)) + COUNTROWS('Fiscal Month')", 1))
    eq("formatting preserved", dt.rename_table("SUM (\r\n\tSales [Amount]  -- note\r\n)", "Sales", "S 2"),
       ("SUM (\r\n\t'S 2' [Amount]  -- note\r\n)", 1))


def test_rename_column():
    print("\n== rename column ==")
    eq("only the owning table's column",
       dt.rename_column("Sales[ID] + Customers[ID] + sales[id]", "Sales", "ID", "SalesID"),
       ("Sales[SalesID] + Customers[ID] + sales[SalesID]", 2, 0))
    eq("unqualified left alone by default and counted",
       dt.rename_column("SUMX(Sales, [Qty] * Sales[Qty])", "Sales", "Qty", "Units"),
       ("SUMX(Sales, [Qty] * Sales[Units])", 1, 1))
    eq("unqualified renamed on request",
       dt.rename_column("SUMX(Sales, [Qty] * Sales[Qty])", "Sales", "Qty", "Units", unqualified=True),
       ("SUMX(Sales, [Units] * Sales[Units])", 2, 0))
    eq("] in the new name is escaped", dt.rename_column("T[a]", "T", "a", "b]c"), ("T[b]]c]", 1, 0))
    eq("escaped old name matches", dt.rename_column("T[x]]y]", "T", "x]y", "z"), ("T[z]", 1, 0))
    eq("string literal untouched", dt.rename_column('"Sales[Qty]" & Sales[Qty]', "Sales", "Qty", "U"),
       ('"Sales[Qty]" & Sales[U]', 1, 0))


def test_rename_measure():
    print("\n== rename measure ==")
    has_col = lambda table, name: (table.lower(), name.lower()) == ("sales", "revenue")  # noqa: E731
    eq("bare ref renamed, same-named column kept",
       dt.rename_measure("DIVIDE([Revenue], SUM(Sales[Revenue]))", "Revenue", "Total Revenue", is_column=has_col),
       ("DIVIDE([Total Revenue], SUM(Sales[Revenue]))", 1))
    eq("qualified measure ref renamed when the table has no such column",
       dt.rename_measure("_Measures[Revenue] + Sales[Revenue]", "Revenue", "R2", is_column=has_col),
       ("_Measures[R2] + Sales[Revenue]", 1))
    eq("without hints qualified refs are left alone",
       dt.rename_measure("Sales[Revenue] + [Revenue]", "Revenue", "R2"), ("Sales[Revenue] + [R2]", 1))
    eq("measure_table hint", dt.rename_measure("M[x] + T[x]", "x", "y", measure_table="M"), ("M[y] + T[x]", 1))
    eq("comment and string untouched", dt.rename_measure('"[Revenue]" -- [Revenue]', "Revenue", "R2"),
       ('"[Revenue]" -- [Revenue]', 0))


def test_statement_kind():
    print("\n== statement classification ==")
    eq("EVALUATE", dt.statement_kind("  EVALUATE Sales"), "dax")
    eq("DEFINE after comments", dt.statement_kind("-- note\n/* x */ define measure T[m] = 1 evaluate {1}"), "dax")
    eq("DMV", dt.statement_kind("SELECT * FROM $SYSTEM.DISCOVER_STORAGE_TABLE_COLUMNS"), "dmv")
    eq("DMV lower case", dt.statement_kind("select [Name] from $system.TMSCHEMA_ROLES"), "dmv")
    eq("TMSL", dt.statement_kind('{"delete": {"object": {"database": "x"}}}'), "command")
    eq("XMLA", dt.statement_kind("<Batch xmlns='x'/>"), "command")
    eq("MDX select", dt.statement_kind("SELECT {} ON 0 FROM [Model]"), "command")
    eq("other statement", dt.statement_kind("CREATE MEMBER x AS 1"), "command")
    eq("comment hiding a command", dt.statement_kind('/* EVALUATE */ {"refresh": {}}'), "command")
    eq("empty", dt.statement_kind("  -- nothing\n"), "empty")
    for ok in ("EVALUATE {1}", "DEFINE VAR x = 1 EVALUATE {x}"):
        try:
            dt.ensure_query(ok)
            check(f"ensure_query accepts {ok[:14]}", True)
        except dt.NonQueryStatementError:
            check(f"ensure_query accepts {ok[:14]}", False)
    for bad in ('{"createOrReplace": {}}', "SELECT * FROM $SYSTEM.TMSCHEMA_ROLES", ""):
        try:
            dt.ensure_query(bad)
            check(f"ensure_query refuses {bad[:18]!r}", False)
        except dt.NonQueryStatementError:
            check(f"ensure_query refuses {bad[:18]!r}", True)
    try:
        dt.ensure_query("SELECT * FROM $SYSTEM.TMSCHEMA_ROLES", allow_dmv=True)
        check("ensure_query allows a DMV when asked", True)
    except dt.NonQueryStatementError:
        check("ensure_query allows a DMV when asked", False)


if __name__ == "__main__":
    for fn in (test_lexing, test_names, test_references, test_rename_table, test_rename_column,
               test_rename_measure, test_statement_kind):
        fn()
    print()
    if _failures:
        print(f"FAILED: {len(_failures)} check(s): {_failures}")
        sys.exit(1)
    print("ALL DAX TOKEN TESTS PASSED")
