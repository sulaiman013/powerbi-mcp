"""
Query-level policy rules (src/security/access_policy.py + query_analysis.py): enforcement that
does not hinge on result column names. Whole-table serialisers and relabelling functions,
measure expansion, table rules (max_rows, require_filter, default block), aggregate_only,
wildcard semantics, and query-scoped DEFINE TABLE / COLUMN. No Power BI required.
Run: python tests/test_security_query_rules.py
"""
import logging
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))
from security.access_policy import AccessPolicyEngine, PolicyAction  # noqa: E402
from security.query_analysis import analyze_text, build_query_facts  # noqa: E402

logging.disable(logging.CRITICAL)  # expected "locked" errors would only clutter the output

_failures = []


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f": {detail}" if detail and not cond else ""))
    if not cond:
        _failures.append(name)


def eq(name, actual, expected):
    check(name, actual == expected, f"got {actual!r}, expected {expected!r}")


def engine(tables, **global_settings):
    e = AccessPolicyEngine()
    e.load_from_dict({"global": global_settings, "tables": tables} if global_settings else {"tables": tables})
    return e


BASELINE = [{"name": "*", "columns": [
    {"name": "ssn", "match": "word", "action": "block"},
    {"name": "card number", "match": "word", "action": "mask"},
]}]


def allowed(e, query, **kw):
    return e.check_query(query, **kw).allowed


def test_serialisers():
    print("\n== TOCSV / TOJSON over a table with protected columns ==")
    e = engine(BASELINE)
    for query in (
        'EVALUATE ROW("x", TOCSV(Customers))',
        'EVALUATE ROW("x", TOJSON(Customers))',
        'EVALUATE ROW("x", tocsv ( \'Customers\' , 100 ))',
        'EVALUATE ROW("x", TOCSV(FILTER(Customers, TRUE())))',
        'EVALUATE ROW("x", TOCSV(TOPN(5, Customers)))',
        'EVALUATE ROW("x", TOJSON(CALCULATETABLE(Customers, Customers[City] = "Austin")))',
        'EVALUATE VAR t = Customers RETURN ROW("x", TOCSV(t))',
        'EVALUATE VAR a = Customers VAR b = FILTER(a, TRUE()) RETURN ROW("x", TOJSON(b))',
        'DEFINE VAR t = Customers EVALUATE ROW("x", TOCSV(t))',
        'EVALUATE ROW("x", TOCSV((Customers)))',
    ):
        res = e.check_query(query)
        check(f"refused: {query[:60]}", res.allowed is False and "TO" in res.reason.upper(), res.reason)
    for query in (
        'EVALUATE ROW("x", TOCSV(VALUES(Customers[City])))',
        'EVALUATE ROW("x", TOCSV(SELECTCOLUMNS(Customers, "c", Customers[City])))',
        'EVALUATE ROW("x", TOCSV({1, 2, 3}), "n", COUNTROWS(Customers))',
        'EVALUATE ROW("x", "TOCSV(Customers)") -- TOCSV(Customers)',
    ):
        check(f"allowed: {query[:60]}", allowed(e, query), e.check_query(query).reason)
    res = e.check_query('EVALUATE ROW("x", TOCSV(SELECTCOLUMNS(Customers, "c", Customers[Card Number])))')
    check("serialising a masked column is allowed but the text is masked",
          res.allowed and res.derived_action == PolicyAction.MASK, f"{res.allowed} {res.derived_action}")

    specific = engine([{"name": "Employees", "columns": [{"name": "Salary", "action": "block"}]}])
    check("only tables that have protected columns are affected",
          allowed(specific, 'EVALUATE ROW("x", TOCSV(Sales))')
          and not allowed(specific, 'EVALUATE ROW("x", TOCSV(Employees))'))
    check("a table cannot be hidden behind a same-named variable declared in another block",
          not allowed(e, 'EVALUATE ROW("a", VAR Customers = 0 RETURN 1, "x", TOCSV(Customers))'))
    check("... while a variable used inside its own block is still a variable",
          allowed(e, 'EVALUATE VAR t = SELECTCOLUMNS(Customers, "c", Customers[City]) RETURN ROW("x", TOCSV(t))'))
    check("a variable is followed to the table it holds",
          allowed(specific, 'EVALUATE VAR t = Sales RETURN ROW("x", TOCSV(t))')
          and not allowed(specific, 'EVALUATE VAR t = Employees RETURN ROW("x", TOCSV(t))'))
    hidden = {"Dump": "TOCSV(Customers)"}
    check("a serialiser hidden inside a measure is found",
          not allowed(e, 'EVALUATE ROW("x", [Dump])', measure_definitions=hidden))


def test_relabelling_functions():
    print("\n== UNION / TREATAS / COLUMNSTATISTICS ==")
    e = engine(BASELINE)
    for query in (
        'EVALUATE UNION(ROW("a", "", "b", ""), Customers)',
        'EVALUATE UNION(Dim3, Customers)',
        'EVALUATE UNION(ROW("a", ""), FILTER(Customers, TRUE()))',
        'EVALUATE VAR t = Customers RETURN UNION(ROW("a", ""), t)',
        'EVALUATE TREATAS(Customers, Other[a], Other[b])',
        'EVALUATE TREATAS(TOPN(10, Customers), Other[a], Other[b])',
        'EVALUATE COLUMNSTATISTICS()',
    ):
        check(f"refused: {query[:60]}", not allowed(e, query), "was allowed")
    for query in (
        'EVALUATE UNION(Customers, Customers)',
        'EVALUATE UNION(SELECTCOLUMNS(A, "c", A[x]), SELECTCOLUMNS(B, "c", B[x]))',
        'EVALUATE VAR a = SELECTCOLUMNS(A, "c", A[x]) VAR b = SELECTCOLUMNS(B, "c", B[x]) RETURN UNION(a, b)',
        'EVALUATE UNION(ROW("n", COUNTROWS(Customers)), ROW("n", 1))',
        'EVALUATE UNION(VALUES(A[x]), VALUES(B[x]))',
        'EVALUATE TREATAS({"a", "b"}, Dim[Code])',
        'EVALUATE CALCULATETABLE(Sales, TREATAS(VALUES(A[x]), Sales[x]))',
        'EVALUATE ROW("n", COUNTROWS(Customers))',
        'EVALUATE FILTER(Customers, Customers[City] = "Austin")',
    ):
        check(f"allowed: {query[:60]}", allowed(e, query), e.check_query(query).reason)

    # A masked column relabelled as another model column: the result key cannot be trusted.
    query = 'EVALUATE TREATAS(VALUES(Customers[Card Number]), Other[Code])'
    res = e.check_query(query)
    check("TREATAS over a masked column is allowed", res.allowed, res.reason)
    out, _ = e.apply_to_results([{"Other[Code]": "4111111111111111"}], query=query)
    eq("... but the relabelled column is masked too", out[0]["Other[Code]"], "************1111")
    query = 'EVALUATE UNION(TOPN(0, VALUES(Other[Code])), VALUES(Customers[Card Number]))'
    out, _ = e.apply_to_results([{"Other[Code]": "4111111111111111"}], query=query)
    eq("same for UNION", out[0]["Other[Code]"], "************1111")
    out, _ = e.apply_to_results([{"Other[Code]": "ABC"}], query='EVALUATE VALUES(Other[Code])')
    eq("without a relabelling function a plain column keeps its own policy", out[0]["Other[Code]"], "ABC")

    none = engine([])
    check("with no protected data anywhere these functions are not restricted",
          allowed(none, 'EVALUATE UNION(ROW("a", ""), Customers)') and allowed(none, "EVALUATE COLUMNSTATISTICS()")
          and allowed(none, 'EVALUATE ROW("x", TOCSV(Customers))'))


def test_measure_expansion():
    print("\n== measures are checked through their definitions ==")
    e = engine(BASELINE)
    measures = {
        "SSN List": 'CONCATENATEX(Customers, Customers[SSN Number], ",")',
        "Indirect": "[Wrapper] & \"\"",
        "Wrapper": "IF(TRUE(), [SSN List])",
        "Card Max": "MAX(Customers[Card Number])",
        "Card Via": "[Card Max]",
        "Total Sales": "SUM(Sales[Amount])",
        "Loop A": "[Loop B] + 1",
        "Loop B": "[Loop A] + 1",
        "Blank": "",
        "Null": None,
    }
    check("without definitions a measure is judged by name only (documented limit)",
          allowed(e, 'EVALUATE ROW("x", [Indirect])'))
    check("... and its name is enough when it names protected data",
          not allowed(e, 'EVALUATE ROW("x", [SSN List])'))
    res = e.check_query('EVALUATE ROW("x", [SSN List])', measure_definitions=measures)
    check("a measure over a blocked column is refused", res.allowed is False, res.reason)
    eq("the result lists what was expanded", res.measures_expanded, ["SSN List"])
    check("two levels of indirection", not allowed(e, 'EVALUATE ROW("x", [Indirect])', measure_definitions=measures))
    check("measure names are case-insensitive", not allowed(e, 'EVALUATE ROW("x", [ssn LIST])', measure_definitions=measures))
    check("a table-qualified measure reference is expanded too",
          not allowed(e, "EVALUATE ROW(\"x\", 'Customers'[Indirect])", measure_definitions=measures))
    res = e.check_query('EVALUATE ROW("x", [Card Via])', measure_definitions=measures)
    check("a measure over a masked column is allowed and marks computed columns",
          res.allowed and res.derived_action == PolicyAction.MASK, f"{res.allowed} {res.derived_action}")
    out, _ = e.apply_to_results([{"[x]": "4111111111111111"}], query='EVALUATE ROW("x", [Card Via])',
                                measure_definitions=measures)
    eq("... and the value comes back masked", out[0]["[x]"], "************1111")
    res = e.check_query('EVALUATE ROW("x", [Total Sales])', measure_definitions=measures)
    check("a harmless measure is untouched", res.allowed and res.derived_action == PolicyAction.ALLOW)
    start = time.time()
    res = e.check_query('EVALUATE ROW("x", [Loop A])', measure_definitions=measures)
    check("measures that reference each other terminate", res.allowed and time.time() - start < 1.0)
    check("blank and null definitions are skipped",
          allowed(e, 'EVALUATE ROW("x", [Blank] + [Null])', measure_definitions=measures))
    facts = build_query_facts('EVALUATE ROW("x", [Indirect])', measures)
    eq("expansion order does not matter for the result", sorted(facts.measures_expanded),
       ["Indirect", "SSN List", "Wrapper"])


def test_table_rules():
    print("\n== table rules: max_rows, require_filter, default block ==")
    e = engine([
        {"name": "Customers", "max_rows": 10, "require_filter": True},
        {"name": "HR", "default_action": "block", "columns": [{"name": "Department", "action": "allow"}]},
        {"name": "Payroll", "default_action": "redact"},
    ], max_rows_per_query=1000)
    eq("max_rows applies to a table used on its own",
       e.check_query("EVALUATE FILTER(Customers, TRUE())").max_rows, 10)
    eq("max_rows applies through a column reference",
       e.check_query("EVALUATE FILTER(VALUES(Customers[City]), TRUE())").max_rows, 10)
    eq("max_rows applies through a measure",
       e.check_query('EVALUATE FILTER(ROW("x", [Cust Count]), TRUE())',
                     measure_definitions={"Cust Count": "COUNTROWS(Customers)"}).max_rows, 10)
    eq("other tables keep the global cap", e.check_query("EVALUATE Sales").max_rows, 1000)

    res = e.check_query("EVALUATE Customers")
    check("require_filter refuses an unfiltered query", res.allowed is False and "filter" in res.reason.lower(), res.reason)
    check("... and a query that only mentions FILTER in a string or comment",
          not allowed(e, 'EVALUATE ADDCOLUMNS(Customers, "x", "FILTER(") -- FILTER(Customers'))
    for query in ("EVALUATE FILTER(Customers, Customers[City] = \"Austin\")",
                  "EVALUATE CALCULATETABLE(Customers, Customers[City] = \"Austin\")",
                  "EVALUATE TOPN(5, Customers)",
                  'EVALUATE ROW("n", CALCULATE(COUNTROWS(Customers), Customers[City] = "Austin"))'):
        check(f"filtered query allowed: {query[:50]}", allowed(e, query), e.check_query(query).reason)
    check("require_filter does not touch other tables", allowed(e, "EVALUATE Sales"))

    check("a blocked table cannot be used on its own", not allowed(e, "EVALUATE HR"))
    check("... not even to count it", not allowed(e, 'EVALUATE ROW("n", COUNTROWS(HR))'))
    check("... nor through a measure", not allowed(e, 'EVALUATE ROW("n", [Heads])',
                                                    measure_definitions={"Heads": "COUNTROWS(HR)"}))
    check("its explicitly allowed column can be read", allowed(e, "EVALUATE VALUES(HR[Department])"))
    check("its other columns cannot", not allowed(e, "EVALUATE VALUES(HR[Salary])"))
    check("a blocked table does not block unrelated measures (regression)",
          allowed(e, 'EVALUATE ROW("x", [Total Sales])'))
    out, _ = e.apply_to_results([{"[Total Sales]": 5, "Sales[City]": "Austin"}],
                                query='EVALUATE SUMMARIZECOLUMNS(Sales[City], "Total Sales", [Total Sales])')
    eq("... nor their results", out[0], {"[Total Sales]": 5, "Sales[City]": "Austin"})
    out, _ = e.apply_to_results([{"HR[Salary]": 9, "HR[Department]": "Ops"}])
    eq("result keys of a blocked table: default applies, exceptions hold", out[0],
       {"HR[Salary]": None, "HR[Department]": "Ops"})

    res = e.check_query('EVALUATE ROW("x", MAXX(Payroll, [Net Pay]))')
    eq("an unqualified column is judged against the tables the query uses", res.derived_action, PolicyAction.REDACT)
    res = e.check_query('EVALUATE ROW("n", COUNTROWS(Payroll))')
    eq("counting a table with a default action does not transform the count", res.derived_action, PolicyAction.ALLOW)
    res = e.check_query('EVALUATE ROW("a", VAR Payroll = 0 RETURN 1, "x", MAXX(Payroll, Payroll[Net Pay]))')
    eq("a same-named variable elsewhere does not hide the table from its default action",
       res.derived_action, PolicyAction.REDACT)
    check("... nor a blocked table", not allowed(e, 'EVALUATE ROW("a", VAR HR = 0 RETURN 1, "n", COUNTROWS(HR))'))
    eq("option words are not mistaken for tables",
       sorted(build_query_facts("EVALUATE ROW(\"x\", CALCULATE([T], DATEADD('Date'[Date], -1, MONTH)))").tables.values()),
       ["Date"])


def test_aggregate_only():
    print("\n== aggregate_only ==")
    e = engine([{"name": "HR", "columns": [{"name": "Salary", "action": "aggregate_only"}]}])
    for query in ('EVALUATE ROW("t", SUM(HR[Salary]))',
                  'EVALUATE ROW("a", AVERAGE ( HR[Salary] ), "n", COUNT(HR[Salary]))',
                  'EVALUATE SUMMARIZECOLUMNS(HR[Department], "t", SUM(HR[Salary]))',
                  'EVALUATE ROW("d", DISTINCTCOUNT(HR[Salary]))'):
        check(f"allowed: {query[:55]}", allowed(e, query), e.check_query(query).reason)
    for query in ("EVALUATE VALUES(HR[Salary])",
                  'EVALUATE SELECTCOLUMNS(HR, "s", HR[Salary])',
                  'EVALUATE ROW("m", MAX(HR[Salary]))',
                  'EVALUATE ROW("t", SUMX(HR, HR[Salary]))',
                  'EVALUATE ROW("t", SUM(HR[Salary]), "leak", MINX(HR, HR[Salary]))',
                  'EVALUATE ROW("c", CONCATENATEX(HR, HR[Salary], ","))',
                  'EVALUATE FILTER(HR, HR[Salary] > 100000)',
                  'EVALUATE ROW("t", SUM(HR[Salary] * 1))'):
        res = e.check_query(query)
        check(f"refused: {query[:55]}", res.allowed is False and "aggregation" in res.reason, res.reason)
    measures = {"Total Salary": "SUM(HR[Salary])", "Salaries": 'CONCATENATEX(HR, HR[Salary], ",")',
                "Salary": "SUM(HR[Salary])"}
    check("a measure that aggregates the column is fine",
          allowed(e, 'EVALUATE ROW("t", [Total Salary])', measure_definitions=measures))
    check("a measure that lists the column is refused",
          not allowed(e, 'EVALUATE ROW("t", [Salaries])', measure_definitions=measures))
    check("a measure that merely shares the column's name is judged by its definition",
          allowed(e, 'EVALUATE ROW("t", [Salary])', measure_definitions=measures))
    out, _ = e.apply_to_results([{"HR[Salary]": 91000, "[Salary]": 500000, "[t]": 500000}],
                                query='EVALUATE ROW("t", SUM(HR[Salary]))')
    eq("the raw column is nulled if it turns up; aggregates and same-named aliases pass", out[0],
       {"HR[Salary]": None, "[Salary]": 500000, "[t]": 500000})


def test_wildcards():
    print("\n== wildcards: fnmatch semantics, escaped and anchored ==")
    e = engine([{"name": "*", "columns": [
        {"name": "*_ssn", "action": "block"},
        {"name": "tax?id", "action": "block"},
        {"name": "Cost (USD)*", "action": "mask"},
        {"name": "c++ score", "action": "redact"},
        {"name": "token", "action": "block"},
        {"name": "api key", "match": "word", "action": "block"},
        {"name": "password", "match": "word", "action": "block"},
    ]}])

    def action(column):
        return e.resolve_column_policy("T", column).action.value

    eq("*_ssn matches a suffix", action("Customer SSN"), "block")
    eq("... and is anchored at the end", action("customer_ssn_hint_flag"), "allow")
    eq("a pattern without * is the whole name", (action("Token"), action("Token Count"), action("AccessTokenX")),
       ("block", "allow", "allow"))
    eq("? is exactly one character", (action("tax_id"), action("TaxId"), action("taxid"), action("tax__id")),
       ("block", "block", "allow", "block"))
    eq("regex metacharacters in a rule name are literal",
       (action("Cost (USD) Total"), action("Cost (USD)"), action("cost_usd_net"), action("Costs"), action("Cost"),
        action("CostXUSD)")),
       ("mask", "mask", "mask", "allow", "allow", "allow"))
    eq("... and never raise", action("C++ Score"), "redact")
    eq("case and spacing variants are one name",
       [action(n) for n in ("API Key", "api_key", "ApiKey", "APIKey", "Customer API Key 2")], ["block"] * 5)
    eq("word rules match whole words only", (action("Capikeys"), action("Keyboard API")), ("allow", "allow"))
    eq("word rules catch run-together spellings of long words",
       (action("passwordhash"), action("userpassword"), action("PasswordHash")), ("block", "block", "block"))
    check("a query over a rule with metacharacters does not raise",
          e.check_query('EVALUATE ROW("x", [Total Sales])').allowed)
    wide = "".join(chr(ord(ch) - ord("a") + 0xFF41) for ch in "token")  # full-width letters
    eq("full-width and accented spellings: compatibility forms fold, accents do not",
       (action(wide), action("T" + chr(0xF6) + "ken")), ("block", "allow"))

    tables = engine([{"name": "dim_*", "default_action": "redact"}, {"name": "Fact.Sales", "default_action": "mask"}])
    eq("table names accept fnmatch patterns", tables.resolve_column_policy("DIM_Customer", "Name").action,
       PolicyAction.REDACT)
    eq("... anchored", tables.resolve_column_policy("xdim_Customer", "Name").action, PolicyAction.ALLOW)
    eq("... with other characters literal",
       (tables.resolve_column_policy("Fact.Sales", "x").action, tables.resolve_column_policy("FactXSales", "x").action),
       (PolicyAction.MASK, PolicyAction.ALLOW))


def test_query_scoped_definitions():
    print("\n== DEFINE TABLE / DEFINE COLUMN ==")
    e = engine([{"name": "Employees", "columns": [{"name": "Salary", "action": "redact"}]}])
    query = "DEFINE TABLE X = Employees EVALUATE X"
    res = e.check_query(query)
    check("query-defined copy of a table is allowed", res.allowed, res.reason)
    out, _ = e.apply_to_results([{"X[Salary]": 91000, "X[Name]": "Jane"}], query=query)
    eq("its columns are resolved against the tables the query used", out[0], {"X[Salary]": "[REDACTED]", "X[Name]": "Jane"})
    query = "DEFINE TABLE X = Employees EVALUATE SELECTCOLUMNS(X, \"s\", X[Salary])"
    eq("a column qualified by a query-defined table is resolved the same way",
       e.check_query(query).derived_action, PolicyAction.REDACT)
    query = "DEFINE COLUMN Employees[Pay] = Employees[Salary] EVALUATE VALUES(Employees[Pay])"
    out, _ = e.apply_to_results([{"Employees[Pay]": 91000}], query=query)
    eq("a query-defined column is a computed column", out[0]["Employees[Pay]"], "[REDACTED]")
    facts = analyze_text("DEFINE TABLE 'My X' = Employees COLUMN T[c] = 1 MEASURE T[m] = 2 EVALUATE 'My X'")
    eq("definitions are recognised", (sorted(facts.defined_tables), sorted(facts.defined_columns)),
       (["my x"], [("t", "c")]))


def test_analysis_is_robust_and_linear():
    print("\n== analysis never raises and stays linear ==")
    e = engine(BASELINE)
    for text in ("", "EVALUATE", "EVALUATE ((((", "EVALUATE ))))", "EVALUATE TOCSV(", "EVALUATE UNION(",
                 "EVALUATE [unterminated", "EVALUATE 'unterminated", 'EVALUATE "unterminated', "VAR", "VAR x =",
                 "EVALUATE VAR x = VAR y = Customers RETURN y RETURN TOCSV(x)", "EVALUATE {{{", "/* open"):
        try:
            e.check_query(text)
            check(f"no exception on {text[:28]!r}", True)
        except Exception as ex:  # noqa: BLE001 - the point of the test is that nothing escapes
            check(f"no exception on {text[:28]!r}", False, repr(ex))
    check("a nested VAR block that cannot be followed is refused, not waved through",
          not allowed(e, 'EVALUATE VAR x = VAR y = Customers RETURN y RETURN ROW("a", TOCSV(x))'))
    hostile = [
        ("EVALUATE FILTER(" + "A" * 200000 + " , TRUE())", "one very long identifier"),
        ("EVALUATE " + "(" * 50000, "deeply nested parentheses"),
        ("EVALUATE ROW(" + ", ".join(f'"c{i}", T[c{i}]' for i in range(20000)) + ")", "20,000 column references"),
        ("EVALUATE " + " ".join(f"VAR v{i} = v{i - 1}" for i in range(1, 3000)) + " RETURN TOCSV(v2999)",
         "a long chain of variables"),
        ("EVALUATE UNION(" + ", ".join(["Customers"] * 20000) + ")", "a very wide UNION"),
    ]
    for text, label in hostile:
        start = time.time()
        e.check_query(text)
        elapsed = time.time() - start
        check(f"{label} ({len(text):,} chars) in {elapsed:.2f}s", elapsed < 5.0, f"{elapsed:.2f}s")


if __name__ == "__main__":
    for fn in (test_serialisers, test_relabelling_functions, test_measure_expansion, test_table_rules,
               test_aggregate_only, test_wildcards, test_query_scoped_definitions,
               test_analysis_is_robust_and_linear):
        fn()
    print()
    if _failures:
        print(f"FAILED: {len(_failures)} check(s): {_failures}")
        sys.exit(1)
    print("ALL QUERY RULE TESTS PASSED")
