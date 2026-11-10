"""
Tests for the pure-Python DAX linter: each rule fires on a positive case and stays silent on
a clean equivalent, lexical edge cases (names holding comment markers, quotes, ]] escapes),
line numbers, the function catalog, and the mechanical DIVIDE rewriter. No Power BI required.
Run: python tests/test_dax_lint.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))
import dax_lint  # noqa: E402
import dax_functions  # noqa: E402

_failures = []


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f": {detail}" if detail and not cond else ""))
    if not cond:
        _failures.append(name)


def found(dax, **kw):
    return [(f["rule_id"], f["line"]) for f in dax_lint.lint_expression("M", dax, **kw)]


def rules(dax, **kw):
    return {r for r, _ in found(dax, **kw)}


def count(dax, rule):
    return sum(1 for r, _ in found(dax) if r == rule)


def test_rules_fire():
    print("\n== each rule fires on its anti-pattern ==")
    check("DL001 FILTER whole table", "DL001" in rules('CALCULATE(SUM(Sales[Amt]), FILTER(Sales, Sales[Region]="X"))'))
    check("DL002 nested CALCULATE", "DL002" in rules("CALCULATE(CALCULATE([Sales]), Sales[Y]=1)"))
    check("DL003 bare division", "DL003" in rules("[Profit] / [Sales]"))
    check("DL004 IFERROR", "DL004" in rules("IFERROR([a]/[b], 0)"))
    check("DL005 plus zero", "DL005" in rules("[Total Sales] + 0"))
    check("DL006 EARLIER", "DL006" in rules("SUMX(T, T[x] - EARLIER(T[x]))"))
    check("DL007 SUMMARIZE aggregation", "DL007" in rules('SUMMARIZE(Sales, Sales[Region], "Tot", SUM(Sales[Amt]))'))
    check("DL008 unknown function", "DL008" in rules("TOTALSALEZ(Sales[Amt])"))


def test_clean_is_silent():
    print("\n== clean equivalents do not fire ==")
    check("DIVIDE has no DL003", "DL003" not in rules("DIVIDE([Profit], [Sales])"))
    check("boolean filter has no DL001", "DL001" not in rules('CALCULATE(SUM(Sales[Amt]), Sales[Region]="X")'))
    check("FILTER(VALUES(..)) has no DL001", "DL001" not in rules('CALCULATE([X], FILTER(VALUES(Sales[Region]), [X] > 0))'))
    check("real function not DL008", "DL008" not in rules("SUMX(Sales, Sales[Qty] * Sales[Price])"))
    check("SUMMARIZECOLUMNS not DL007", "DL007" not in rules('SUMMARIZECOLUMNS(Sales[Region], "Tot", SUM(Sales[Amt]))'))
    check("dotted function recognized", "DL008" not in rules("PERCENTILE.INC(Sales[Amt], 0.9)"))
    check("VAR name not unknown-func", "DL008" not in rules("VAR Threshold = 10 RETURN IF([X] > Threshold, 1, 0)"))
    check("lower-case functions recognized", rules("sum(Sales[x]) + calculate([y])") == set())
    check("space before the parenthesis", "DL008" not in rules("SUM (Sales[x])"))


def test_tokenizer_robustness():
    print("\n== comments and strings do not create false positives ==")
    check("slash in line comment ignored", "DL003" not in rules("SUM(Sales[Amt]) // ratio a/b here"))
    check("slash in -- comment ignored", "DL003" not in rules("SUM(Sales[Amt]) -- a/b"))
    check("slash in block comment ignored", "DL003" not in rules("SUM(Sales[Amt]) /* a/b */ "))
    check("slash in string ignored", "DL003" not in rules('CONCATENATE("50/50", [x])'))
    check("slash in a string with \"\" escapes ignored", "DL003" not in rules('IF([x]="a""/""b", 1)'))
    check("url in a string ignored", rules('CONCATENATE("http://x/y", [z])') == set())
    check("comment does not hide real division", "DL003" in rules("// note\n[a] / [b]"))
    check("unterminated block comment swallows the rest", rules("[a] /* oops [b] / [c]") == set())


def test_names_holding_comment_markers():
    print("\n== defect 6: comment markers and quotes inside names are part of the name ==")
    check("-- inside [measure]: the division is still seen", count("[Sales -- Net] / [Units]", "DL003") == 1,
          str(found("[Sales -- Net] / [Units]")))
    check("// inside 'table': the division is still seen",
          count("SUM('Sales//Archive'[Amt]) / [Units]", "DL003") == 1)
    check("/* inside [measure] is not a comment", count("[A /* B] + [C */ D] / [E]", "DL003") == 1)
    check("quote inside [measure]: later comment still a comment",
          count('[6" Pipe] + 1 -- ratio a/b', "DL003") == 0, str(found('[6" Pipe] + 1 -- ratio a/b')))
    check("quote inside 'table': later comment still a comment",
          count("SUM('6\" Pipe'[Amt]) // a/b", "DL003") == 0)
    check("slash inside a [name] is not a division", rules("[Price/Unit] + 1") == set())
    check("slash inside a 'name' is not a division", rules("SUM('Sales/Returns'[Amt])") == set())
    check("defect 9: ]] escape keeps the name whole", rules("[Rate]]/x] + 1") == set(), str(found("[Rate]]/x] + 1")))
    toks = dax_lint.tokenize("[Rate]]/x] + 1")
    check("tokenize() keeps its dict shape", toks[0] == {"type": "column", "value": "[Rate]]/x]", "pos": 0, "line": 1},
          str(toks[:1]))
    check("tokenize() classifies punctuation", [t["type"] for t in dax_lint.tokenize("SUM('T'[x], 1)")]
          == ["ident", "punc", "qtable", "column", "punc", "number", "punc"])


def test_line_numbers():
    print("\n== defect 9: line numbers refer to the original text ==")
    check("after a 3-line block comment", found("VAR x = 1\n/* a\nb\nc */\nRETURN [a] / [b]") == [("DL003", 5)],
          str(found("VAR x = 1\n/* a\nb\nc */\nRETURN [a] / [b]")))
    check("after a block comment on line 1", found("/* a\n b\n c */ [a] / [b]") == [("DL003", 3)])
    check("after line comments", found("// one\n-- two\n[a] / [b]") == [("DL003", 3)])
    check("CRLF line endings", found("VAR x = 1\r\nRETURN\r\n    [a] / [b]") == [("DL003", 3)])


def test_dl001_scope():
    print("\n== defect 8: DL001 is about whole-table scans, not about any identifier ==")
    check("a VAR table is not a model table",
          "DL001" not in rules("VAR t = VALUES(Sales[Region]) RETURN CALCULATE([X], FILTER(t, [X] > 0))"))
    check("FILTER(ALL(Sales), ..) is a whole-table scan",
          "DL001" in rules('CALCULATE([X], FILTER(ALL(Sales), Sales[Region]="X"))'))
    check("FILTER(ALL('Sales Data'), ..) too",
          "DL001" in rules("CALCULATE([X], FILTER(ALL('Sales Data'), 'Sales Data'[R]=1))"))
    check("FILTER(ALL(Sales[Region]), ..) is a column scan",
          "DL001" not in rules('CALCULATE([X], FILTER(ALL(Sales[Region]), Sales[Region]="X"))'))
    check("KEEPFILTERS(FILTER(Sales, ..)) still scans the table",
          "DL001" in rules('CALCULATE([X], KEEPFILTERS(FILTER(Sales, Sales[Region]="X")))'))
    check("quoted table", "DL001" in rules("CALCULATE([X], FILTER('Sales Data', 'Sales Data'[R]=1))"))
    cyr = "".join(chr(c) for c in (0x41F, 0x440, 0x43E, 0x434, 0x430, 0x436, 0x438))   # a Cyrillic table name
    check("non-ASCII bare table name", "DL001" in rules(f"CALCULATE([X], FILTER({cyr}, {cyr}[S] > 0))"))
    check("CALCULATETABLE's own table expression is not a filter argument",
          "DL001" not in rules("CALCULATETABLE(FILTER(Sales, Sales[x] > 1), Sales[y] = 2)"))
    check("CALCULATETABLE filter argument", "DL001" in rules("CALCULATETABLE(Sales, FILTER(Sales, Sales[x]>1))"))
    check("FILTER feeding an iterator is not a CALCULATE filter",
          "DL001" not in rules("SUMX(FILTER(Sales, Sales[x]>1), Sales[y])"))
    check("a UDF table parameter is not a model table",
          "DL001" not in rules("DEFINE FUNCTION F = (t : anyref, m : expr) => CALCULATE(m, FILTER(t, m > 0)) "
                               "EVALUATE { F(Sales, [X]) }"))


def test_dl002_scope():
    print("\n== defect 9: DL002 only for a CALCULATE directly in another's expression ==")
    check("CALCULATETABLE as a filter argument is fine",
          "DL002" not in rules("CALCULATE([x], CALCULATETABLE(VALUES(T[c]), T[d]=1))"))
    check("CALCULATE inside an iterator is a deliberate context transition",
          "DL002" not in rules("CALCULATE(SUMX(Sales, CALCULATE([x])), T[d]=1)"))
    check("sibling CALCULATEs are not nested", "DL002" not in rules("CALCULATE([a], T[d]=1) + CALCULATE([b], T[d]=2)"))
    check("inside IF in the expression is still nested",
          "DL002" in rules("CALCULATE(IF([c] > 0, CALCULATE([a], T[e]=1)), T[d]=1)"))
    check("triple nest reports each outer CALCULATE", count("CALCULATE(CALCULATE(CALCULATE([a])))", "DL002") == 2)


def test_dl003_dl005_precision():
    print("\n== DL003 / DL005 precision ==")
    check("division by a non-zero literal is safe", "DL003" not in rules("[x] / 100"))
    check("division by a negative literal is safe", "DL003" not in rules("[x] / -2.5"))
    check("division by literal zero is reported", "DL003" in rules("[x] / 0"))
    check("literal divided by a measure is reported", "DL003" in rules("100 / [x]"))
    check("division inside DIVIDE's argument is reported", "DL003" in rules("DIVIDE([a] / [c], [b])"))
    check("a slash with no divisor is a syntax error, not a division", rules("[a] /") == set() and rules("/ [a]") == set())
    check("a division without both operands is left unchanged by the rewriter",
          all(dax_lint.rewrite_expression(s)["expression"] == s for s in ("[a] /", "/ [a]", "[a] / )", "[a] / SUM(", "")))
    for dax, expect in [("[x] + 0", True), ("[x]+0", True), ("[x] + 0.0", True), ("0 + [x]", True),
                        ("IF([c], 0 + [x])", True), ("[x] + 0 + [y]", True),
                        ("[x] + 0.5", False), ("[x] + 0.05", False), ("[x] + 10", False),
                        ("[x] + 0 * [y]", False), ("[a] * 0 + [b]", False), ("[x] - 0", False),
                        ('FORMAT([x], "+0.0%")', False)]:
        check(f"DL005 {'fires' if expect else 'silent'} on {dax}", ("DL005" in rules(dax)) is expect)


def test_dl007_scope():
    print("\n== defect 9: DL007 is any SUMMARIZE extension column ==")
    check("measure reference as extension column", "DL007" in rules('SUMMARIZE(Sales, Sales[Region], "Tot", [Total Sales])'))
    check("DIVIDE(SUM, SUM) as extension column",
          "DL007" in rules('SUMMARIZE(Sales, Sales[Region], "R", DIVIDE(SUM(Sales[a]), SUM(Sales[b])))'))
    check("statistical function as extension column",
          "DL007" in rules('SUMMARIZE(Sales, Sales[Region], "x", STDEV.P(Sales[a]))'))
    check("group-only SUMMARIZE is fine", "DL007" not in rules("SUMMARIZE(Sales, Sales[Region], Sales[City])"))
    check("ADDCOLUMNS(SUMMARIZE(..)) is the fix, not a finding",
          "DL007" not in rules('ADDCOLUMNS(SUMMARIZE(Sales, Sales[Region]), "Tot", CALCULATE(SUM(Sales[Amt])))'))
    check("one finding per SUMMARIZE", count('SUMMARIZE(Sales, Sales[R], "a", [A], "b", [B])', "DL007") == 1)


def test_function_catalog():
    print("\n== defect 7: the function catalog knows the whole language ==")
    names = """LINEST LINESTX INFO.VIEW.TABLES INFO.VIEW.COLUMNS INFO.VIEW.MEASURES INFO.VIEW.RELATIONSHIPS
        INFO.CALCDEPENDENCY INFO.TABLES INFO.MEASURES INFO.STORAGETABLECOLUMNS INFO.ROLES RUNNINGSUM
        MOVINGAVERAGE COLLAPSE COLLAPSEALL EXPAND EXPANDALL PREVIOUS NEXT FIRST LAST RANGE ISATLEVEL LOOKUP
        LOOKUPWITHTOTALS QUARTER YEARFRAC NETWORKDAYS MAXA MINA AVERAGEA BITAND BITOR BITXOR BITLSHIFT
        BITRSHIFT ACOS ACOSH ACOT ACOTH ASIN ASINH ATAN ATANH COS COSH COT COTH SIN SINH TAN TANH DEGREES
        RADIANS SQRTPI ISO.CEILING XIRR XNPV PMT PV FV NPER RATE IPMT PPMT NOMINAL EFFECT SLN DB DDB
        NORM.DIST NORM.INV NORM.S.DIST NORM.S.INV BETA.DIST BETA.INV CHISQ.DIST CHISQ.INV CHISQ.DIST.RT
        CHISQ.INV.RT EXPON.DIST POISSON.DIST T.DIST T.DIST.2T T.DIST.RT T.INV T.INV.2T CONFIDENCE.NORM
        CONFIDENCE.T PERMUT COMBIN COMBINA CUSTOMDATA APPROXIMATEDISTINCTCOUNT ADDMISSINGITEMS IGNORE
        ISAFTER COLUMNSTATISTICS EXTERNALMEASURE DATESWTD TOTALWTD STARTOFWEEK ENDOFWEEK PREVIOUSWEEK
        NEXTWEEK ROLLUPISSUBTOTAL FILTERS WINDOW OFFSET INDEX RANK ROWNUMBER MATCHBY ORDERBY PARTITIONBY
        EVALUATEANDLOG TOCSV TOJSON SUM SUMX CALCULATE DIVIDE SWITCH TREATAS SELECTEDVALUE DATEADD
        PERCENTILEX.INC RANK.EQ STDEV.S IF.EAGER NAMEOF TRUE FALSE BLANK AND OR NOT""".split()
    flagged = [n for n in names if "DL008" in rules(f"{n}(1)")]
    check(f"none of {len(names)} real functions is reported", not flagged, " ".join(flagged))
    check("the server's own metadata query is clean", rules("EVALUATE INFO.VIEW.TABLES()") == set())
    check("a typo in INFO.VIEW.* is still reported", "DL008" in rules("EVALUATE INFO.VIEW.TABELS()"))
    check("a misspelled dotted function is reported", "DL008" in rules("PERCENTILE.INK(Sales[Amt], 0.9)"))
    check("PROPER is not DAX (it is Power Query / Excel)", "DL008" in rules("PROPER([x])"))
    check("catalog has several hundred names", len(dax_functions.FUNCTIONS) > 400, str(len(dax_functions.FUNCTIONS)))
    check("iterators are derived from the catalog", {"SUMX", "RANKX", "FILTER", "ADDCOLUMNS"} <= dax_functions.ITERATORS
          and "MAX" not in dax_functions.ITERATORS and "INDEX" not in dax_functions.ITERATORS)


def test_user_defined_functions():
    print("\n== defect 7: user-defined functions are known ==")
    udf = "DEFINE FUNCTION AddTax = (a : NUMERIC) => a * 1.1 EVALUATE { AddTax(10) }"
    check("DEFINE FUNCTION name is known in the same expression", "DL008" not in rules(udf), str(found(udf)))
    check("a namespaced UDF definition is known",
          "DL008" not in rules("DEFINE FUNCTION Fin.Margin = (r, c) => DIVIDE(r - c, r) EVALUATE { Fin.Margin(2, 1) }"))
    check("a model UDF is unknown without a hint", "DL008" in rules("Fin.Margin([Revenue], [Cost])"))
    check("known_functions accepts model UDFs", "DL008" not in rules("Fin.Margin([Revenue], [Cost])",
                                                                     known_functions=["fin.margin"]))
    res = dax_lint.lint_measures([{"name": "A", "expression": "Fin.Margin(1, 2)"}], known_functions=["Fin.Margin"])
    check("lint_measures passes known_functions through", res["summary"]["total"] == 0, str(res["summary"]))


def test_rewrite_divide():
    print("\n== suggest_rewrites: real DIVIDE rewrites from token spans ==")

    def rw(dax):
        return dax_lint.rewrite_expression(dax)["expression"]

    cases = [
        ("[a] / [b]", "DIVIDE([a], [b])"),
        ("[a] / [b] / [c]", "DIVIDE(DIVIDE([a], [b]), [c])"),
        ("[a] * [b] / [c]", "DIVIDE([a] * [b], [c])"),
        ("[a] / [b] * [c]", "DIVIDE([a], [b]) * [c]"),
        ("[x] - [a] / [b]", "[x] - DIVIDE([a], [b])"),
        ("[x] + [a] / [b] ^ 2 - 1", "[x] + DIVIDE([a], [b] ^ 2) - 1"),
        ("-[a] / [b]", "DIVIDE(-[a], [b])"),
        ("[a] * -[b] / [c]", "DIVIDE([a] * -[b], [c])"),
        ("([a] + [b]) / ([c] - [d])", "DIVIDE([a] + [b], [c] - [d])"),
        ("([a] / [b]) / ([c] / [d])", "DIVIDE(DIVIDE([a], [b]), DIVIDE([c], [d]))"),
        ("SUM(Sales[Amt]) / COUNTROWS(Sales)", "DIVIDE(SUM(Sales[Amt]), COUNTROWS(Sales))"),
        ("Sales[Amt] / 'Sales Data'[Qty]", "DIVIDE(Sales[Amt], 'Sales Data'[Qty])"),
        ("VAR r = [a] / [b] RETURN r", "VAR r = DIVIDE([a], [b]) RETURN r"),
        ("IF([c] > 0, [a] / [b], 0)", "IF([c] > 0, DIVIDE([a], [b]), 0)"),
        ("[a] / 100 / [b]", "DIVIDE([a] / 100, [b])"),
        ("[a] / 100", "[a] / 100"),
    ]
    for dax, want in cases:
        check(f"{dax}", rw(dax) == want, rw(dax))
    # '&' binds looser than '/', so only [a] is the numerator.
    check("string concatenation is not part of the numerator",
          rw('"50/50" & [a] / [b] -- a/b') == '"50/50" & DIVIDE([a], [b]) -- a/b', rw('"50/50" & [a] / [b] -- a/b'))
    multi = "VAR x =\n    [a] /* keep */ * [b]\n        / [c]   -- tail\nRETURN x"
    check("comments and layout outside the span survive",
          rw(multi) == "VAR x =\n    DIVIDE([a] /* keep */ * [b], [c])   -- tail\nRETURN x", repr(rw(multi)))
    check("a comment between operand and operator blocks the rewrite",
          rw("[a] /* why */ / [b]") == "[a] /* why */ / [b]")
    check("names with slashes and brackets are untouched", rw("[Price/Unit] / [Rate]]/x]") == "DIVIDE([Price/Unit], [Rate]]/x])")
    check("rewritten output is itself lint-clean", "DL003" not in rules(rw("[a] / [b] / ([c] / [d])")))

    hints = dax_lint.suggest_rewrites("M", "[a] / [b]\n+ [c] / [d]")
    kinds = [h["kind"] for h in hints]
    check("one rewrite per division plus the full expression", kinds == ["rewrite", "rewrite", "expression"], str(kinds))
    check("rewrite lines are the original lines", [h["line"] for h in hints[:2]] == [1, 2])
    check("before is real source text", hints[0]["before"] == "[a] / [b]" and hints[0]["after"] == "DIVIDE([a], [b])")
    check("the expression hint carries the whole rewritten text",
          hints[-1]["after"] == "DIVIDE([a], [b])\n+ DIVIDE([c], [d])" and hints[-1]["before"] == "[a] / [b]\n+ [c] / [d]")
    check("every hint keeps the documented keys",
          all({"rule_id", "kind", "line", "before", "after", "note"} <= set(h) for h in hints))


def test_rewrite_templates():
    print("\n== suggest_rewrites: everything non-mechanical is labelled a template ==")
    hints = dax_lint.suggest_rewrites("M", 'CALCULATE([X], FILTER(Sales, Sales[R]="A")) + [Y]/[Z]')
    ids = {h["rule_id"] for h in hints}
    check("rewrite for DL001", "DL001" in ids)
    check("rewrite for DL003", "DL003" in ids)
    t1 = next(h for h in hints if h["rule_id"] == "DL001")
    check("DL001 is a template, and says so", t1["kind"] == "template" and t1["note"].startswith("Template"))
    check("DL001 template preserves the existing filter", "KEEPFILTERS" in t1["after"])
    check("DL001 template covers measure predicates", "FILTER(VALUES(" in t1["note"])
    t7 = dax_lint.suggest_rewrites("M", 'SUMMARIZE(Sales, Sales[R], "T", SUM(Sales[A]))')
    check("DL007 is a template using ADDCOLUMNS", len(t7) == 1 and t7[0]["kind"] == "template"
          and t7[0]["after"].startswith("ADDCOLUMNS(SUMMARIZE("), str(t7))
    check("no hints for clean DAX", dax_lint.suggest_rewrites("M", "DIVIDE([a], [b])") == [])
    check("no hints for a constant divisor", dax_lint.suggest_rewrites("M", "[a] / 100") == [])


def test_summary():
    print("\n== measure summary ==")
    res = dax_lint.lint_measures([
        {"name": "Bad", "expression": "IFERROR([a]/[b], 0)"},
        {"name": "Good", "expression": "DIVIDE([a],[b])"},
    ])
    check("summary counts findings", res["summary"]["total"] == 2 and res["summary"]["measures_scanned"] == 2)
    check("findings carry object name", all(f["object"] == "Bad" for f in res["findings"]))
    warn = dax_lint.lint_measures([{"name": "Bad", "expression": "IFERROR([a]/[b], 0)"}], min_severity="warning")
    check("min_severity filters findings and summary together",
          warn["summary"]["total"] == 1 == len(warn["findings"]) and warn["summary"]["by_severity"] == {"warning": 1},
          str(warn["summary"]))
    check("empty and None inputs", dax_lint.lint_measures(None)["summary"]["total"] == 0
          and dax_lint.lint_expression("x", "   ") == [])
    check("severity ranks exported", dax_lint.SEVERITY_RANK == {"error": 3, "warning": 2, "info": 1})


if __name__ == "__main__":
    print("=" * 70)
    print("  DAX LINTER TESTS")
    print("=" * 70)
    test_rules_fire()
    test_clean_is_silent()
    test_tokenizer_robustness()
    test_names_holding_comment_markers()
    test_line_numbers()
    test_dl001_scope()
    test_dl002_scope()
    test_dl003_dl005_precision()
    test_dl007_scope()
    test_function_catalog()
    test_user_defined_functions()
    test_rewrite_divide()
    test_rewrite_templates()
    test_summary()
    print("\n" + "=" * 70)
    if _failures:
        print(f"  {len(_failures)} CHECK(S) FAILED: {', '.join(_failures)}")
        sys.exit(1)
    print("  ALL DAX LINTER CHECKS PASSED")
    print("=" * 70)
