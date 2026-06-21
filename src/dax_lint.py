"""
DAX performance + correctness linter ("BPA for DAX").

A pure-Python, dependency-free static analyzer for DAX expressions. It tokenizes an
expression and runs an original rule set over the token stream to flag well-known
performance anti-patterns and likely correctness traps, with a concrete rewrite hint for
each. The anti-patterns themselves are public, non-copyrightable DAX facts; the tokenizer,
rule detectors, and rewrite hints here are original.

Designed to pair with validate_dax (prove a rewrite is still valid) and, where a live
connection exists, with query benchmarking (prove a rewrite is actually faster).

Entry points:
    lint_expression(name, dax)  -> list[Finding-as-dict]
    lint_measures(measures)     -> {"summary": {...}, "findings": [...]}  (measures: [{name, expression}])
    suggest_rewrites(name, dax) -> list[{rule_id, before, after, note}]
"""
import re
from typing import Any, Dict, List, Optional, Tuple

SEVERITY_RANK = {"error": 3, "warning": 2, "info": 1}

# A curated set of DAX functions/keywords used to flag likely-hallucinated or misspelled
# function calls. Not exhaustive of every niche function, so UNKNOWN_FUNCTION is "info" and
# names defined as VARs in the same expression are always excluded.
DAX_KEYWORDS = {
    "VAR", "RETURN", "EVALUATE", "DEFINE", "MEASURE", "COLUMN", "TABLE", "ORDER", "BY",
    "START", "AT", "ASC", "DESC", "IN", "NOT", "TRUE", "FALSE", "BLANK",
}
DAX_FUNCTIONS = {
    # Aggregations
    "SUM", "SUMX", "AVERAGE", "AVERAGEX", "MIN", "MINX", "MAX", "MAXX", "COUNT", "COUNTX",
    "COUNTA", "COUNTAX", "COUNTROWS", "COUNTBLANK", "DISTINCTCOUNT", "DISTINCTCOUNTNOBLANK",
    "PRODUCT", "PRODUCTX", "GEOMEAN", "GEOMEANX", "MEDIAN", "MEDIANX", "PERCENTILE.INC",
    "PERCENTILE.EXC", "PERCENTILEX.INC", "PERCENTILEX.EXC", "RANK.EQ", "RANKX", "RANK",
    "STDEV.S", "STDEV.P", "STDEVX.S", "STDEVX.P", "VAR.S", "VAR.P", "VARX.S", "VARX.P",
    # Filter / context
    "CALCULATE", "CALCULATETABLE", "FILTER", "ALL", "ALLEXCEPT", "ALLSELECTED", "ALLNOBLANKROW",
    "ALLCROSSFILTERED", "REMOVEFILTERS", "KEEPFILTERS", "VALUES", "DISTINCT", "EARLIER",
    "EARLIEST", "RELATED", "RELATEDTABLE", "USERELATIONSHIP", "CROSSFILTER", "SELECTEDVALUE",
    "HASONEVALUE", "HASONEFILTER", "ISFILTERED", "ISCROSSFILTERED", "ISINSCOPE", "FIRSTNONBLANK",
    "LASTNONBLANK", "FIRSTNONBLANKVALUE", "LASTNONBLANKVALUE", "CALCULATEERROR",
    # Logical / info
    "IF", "IFERROR", "IF.EAGER", "SWITCH", "AND", "OR", "NOT", "COALESCE", "ISBLANK", "ISERROR",
    "ISEMPTY", "ISNUMBER", "ISTEXT", "ISLOGICAL", "ISNONTEXT", "ISEVEN", "ISODD", "ISONORAFTER",
    "ISSELECTEDMEASURE", "CONTAINS", "CONTAINSROW", "CONTAINSSTRING", "CONTAINSSTRINGEXACT",
    "DIVIDE", "QUOTIENT", "MOD", "ABS", "SIGN", "ROUND", "ROUNDUP", "ROUNDDOWN", "INT", "TRUNC",
    "CEILING", "FLOOR", "MROUND", "POWER", "SQRT", "EXP", "LN", "LOG", "LOG10", "FACT", "GCD",
    "LCM", "PI", "EVEN", "ODD", "RAND", "RANDBETWEEN", "CURRENCY", "FIXED",
    # Text
    "CONCATENATE", "CONCATENATEX", "FORMAT", "LEFT", "RIGHT", "MID", "LEN", "LOWER", "UPPER",
    "TRIM", "SUBSTITUTE", "REPLACE", "REPT", "SEARCH", "FIND", "VALUE", "UNICHAR", "UNICODE",
    "EXACT", "COMBINEVALUES", "PROPER",
    # Date/time + time intelligence
    "DATE", "TIME", "DATEVALUE", "TIMEVALUE", "YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND",
    "WEEKDAY", "WEEKNUM", "NOW", "TODAY", "UTCNOW", "UTCTODAY", "EDATE", "EOMONTH", "DATEDIFF",
    "DATEADD", "DATESBETWEEN", "DATESINPERIOD", "DATESYTD", "DATESQTD", "DATESMTD", "TOTALYTD",
    "TOTALQTD", "TOTALMTD", "SAMEPERIODLASTYEAR", "PARALLELPERIOD", "PREVIOUSDAY", "PREVIOUSMONTH",
    "PREVIOUSQUARTER", "PREVIOUSYEAR", "NEXTDAY", "NEXTMONTH", "NEXTQUARTER", "NEXTYEAR",
    "STARTOFMONTH", "STARTOFQUARTER", "STARTOFYEAR", "ENDOFMONTH", "ENDOFQUARTER", "ENDOFYEAR",
    "FIRSTDATE", "LASTDATE", "OPENINGBALANCEMONTH", "OPENINGBALANCEQUARTER", "OPENINGBALANCEYEAR",
    "CLOSINGBALANCEMONTH", "CLOSINGBALANCEQUARTER", "CLOSINGBALANCEYEAR", "CALENDAR", "CALENDARAUTO",
    # Table-returning / iterators / shaping
    "ADDCOLUMNS", "SELECTCOLUMNS", "SUMMARIZE", "SUMMARIZECOLUMNS", "GROUPBY", "ROLLUP",
    "ROLLUPADDISSUBTOTAL", "ROLLUPGROUP", "ISSUBTOTAL", "CROSSJOIN", "GENERATE", "GENERATEALL",
    "GENERATESERIES", "ROW", "DATATABLE", "UNION", "INTERSECT", "EXCEPT", "NATURALINNERJOIN",
    "NATURALLEFTOUTERJOIN", "TOPN", "TOPNSKIP", "SAMPLE", "TREATAS", "LOOKUPVALUE", "SUBSTITUTEWITHINDEX",
    "CURRENTGROUP", "EXPANDED", "WINDOW", "OFFSET", "INDEX", "RANK", "ROWNUMBER", "ORDERBY",
    "PARTITIONBY", "MATCHBY", "NONVISUAL", "DETAILROWS", "SELECTEDMEASURE", "SELECTEDMEASURENAME",
    "SELECTEDMEASUREFORMATSTRING", "USERNAME", "USERPRINCIPALNAME", "USEROBJECTID", "USERCULTURE",
    "PATH", "PATHCONTAINS", "PATHITEM", "PATHITEMREVERSE", "PATHLENGTH", "BLANK", "ERROR",
    "CONVERT", "DATATABLE", "EVALUATEANDLOG", "NAMEOF", "TOCSV", "TOJSON",
}

# Aggregator names whose presence inside a SUMMARIZE argument signals the
# SUMMARIZE-as-measure-host anti-pattern.
_AGGREGATORS = {"SUM", "AVERAGE", "MIN", "MAX", "COUNT", "COUNTA", "COUNTROWS", "DISTINCTCOUNT",
                "SUMX", "AVERAGEX", "MINX", "MAXX", "COUNTX", "PRODUCT", "MEDIAN"}


def _strip_comments(dax: str) -> str:
    """Remove /* */ block comments and // or -- line comments, preserving string literals."""
    out = []
    i, n = 0, len(dax)
    while i < n:
        ch = dax[i]
        if ch == '"':  # string literal: copy verbatim until closing quote
            j = i + 1
            while j < n and dax[j] != '"':
                j += 1
            out.append(dax[i:min(j + 1, n)])
            i = j + 1
            continue
        if ch == "/" and i + 1 < n and dax[i + 1] == "*":
            k = dax.find("*/", i + 2)
            i = n if k == -1 else k + 2
            out.append(" ")
            continue
        if (ch == "/" and i + 1 < n and dax[i + 1] == "/") or (ch == "-" and i + 1 < n and dax[i + 1] == "-"):
            k = dax.find("\n", i)
            i = n if k == -1 else k
            continue
        out.append(ch)
        i += 1
    return "".join(out)


_TOKEN_RE = re.compile(
    r"""
      (?P<string>"(?:[^"]|"")*")              # "double quoted" string
    | (?P<column>\[[^\]]*\])                   # [Column or Measure]
    | (?P<qtable>'(?:[^']|'')*')               # 'Quoted Table'
    | (?P<number>\d+\.?\d*(?:[eE][+-]?\d+)?)   # number
    | (?P<ident>[A-Za-z_][A-Za-z0-9_.]*)       # identifier (function/table/keyword)
    | (?P<op><=|>=|<>|\|\||&&|[-+*/^=<>&])      # operators
    | (?P<punc>[(),])                           # parens / comma
    | (?P<ws>\s+)
    | (?P<other>.)
    """,
    re.VERBOSE,
)


def tokenize(dax: str) -> List[Dict[str, Any]]:
    """Tokenize DAX (comments stripped) into [{type, value, pos, line}]."""
    text = _strip_comments(dax)
    tokens: List[Dict[str, Any]] = []
    for m in _TOKEN_RE.finditer(text):
        kind = m.lastgroup
        val = m.group()
        if kind == "ws":
            continue
        line = text.count("\n", 0, m.start()) + 1
        tokens.append({"type": kind, "value": val, "pos": m.start(), "line": line})
    return tokens


def _var_names(tokens: List[Dict[str, Any]]) -> set:
    """Names introduced by VAR so they are not mistaken for unknown function calls."""
    names = set()
    for i, t in enumerate(tokens):
        if t["type"] == "ident" and t["value"].upper() == "VAR" and i + 1 < len(tokens):
            nxt = tokens[i + 1]
            if nxt["type"] == "ident":
                names.add(nxt["value"].upper())
    return names


def _is_call(tokens: List[Dict[str, Any]], i: int) -> bool:
    """Is tokens[i] an identifier immediately followed by '(' (a function call)?"""
    return (tokens[i]["type"] == "ident" and i + 1 < len(tokens)
            and tokens[i + 1]["type"] == "punc" and tokens[i + 1]["value"] == "(")


def _arg_span(tokens: List[Dict[str, Any]], open_paren_idx: int) -> Tuple[int, int]:
    """Given the index of a '(' token, return (start, end) token indices of its contents
    (exclusive of the parens), end being the matching ')' index."""
    depth = 0
    for j in range(open_paren_idx, len(tokens)):
        if tokens[j]["type"] == "punc" and tokens[j]["value"] == "(":
            depth += 1
        elif tokens[j]["type"] == "punc" and tokens[j]["value"] == ")":
            depth -= 1
            if depth == 0:
                return open_paren_idx + 1, j
    return open_paren_idx + 1, len(tokens)


def _finding(rule_id, severity, message, suggestion, line, obj=None) -> Dict[str, Any]:
    return {"rule_id": rule_id, "severity": severity, "message": message,
            "suggestion": suggestion, "line": line, "object": obj}


def lint_expression(name: Optional[str], dax: str) -> List[Dict[str, Any]]:
    """Lint a single DAX expression, returning a list of finding dicts."""
    if not dax or not dax.strip():
        return []
    tokens = tokenize(dax)
    var_names = _var_names(tokens)
    findings: List[Dict[str, Any]] = []
    n = len(tokens)

    for i, t in enumerate(tokens):
        if t["type"] != "ident":
            # DL003: bare division operator (divide-by-zero risk vs DIVIDE)
            if t["type"] == "op" and t["value"] == "/":
                findings.append(_finding(
                    "DL003", "warning",
                    "Division with '/' does not guard against divide-by-zero (returns Infinity/error).",
                    "Use DIVIDE(numerator, denominator) which returns BLANK on a zero denominator.",
                    t["line"], name))
            continue

        up = t["value"].upper()

        if _is_call(tokens, i):
            a0, a1 = _arg_span(tokens, i + 1)

            # DL001: FILTER over a whole (bare) table inside CALCULATE/CALCULATETABLE.
            if up in ("CALCULATE", "CALCULATETABLE"):
                # scan this call's direct arguments for a FILTER(<bareTable>, ...) at depth 1
                depth = 0
                k = a0
                while k < a1:
                    tk = tokens[k]
                    if tk["type"] == "punc" and tk["value"] == "(":
                        depth += 1
                    elif tk["type"] == "punc" and tk["value"] == ")":
                        depth -= 1
                    elif (depth == 0 and tk["type"] == "ident" and tk["value"].upper() == "FILTER"
                          and _is_call(tokens, k)):
                        f0, f1 = _arg_span(tokens, k + 1)
                        # first FILTER arg = single table identifier (not itself a function call)?
                        if f0 < f1 and tokens[f0]["type"] in ("ident", "qtable") and not _is_call(tokens, f0):
                            # ensure the first arg is JUST a table (next token is the comma)
                            if f0 + 1 < f1 and tokens[f0 + 1]["type"] == "punc" and tokens[f0 + 1]["value"] == ",":
                                findings.append(_finding(
                                    "DL001", "warning",
                                    "FILTER over an entire table inside CALCULATE materializes the whole table.",
                                    "Use a boolean predicate directly in CALCULATE (e.g. CALCULATE(..., Table[Col] = x)) "
                                    "or FILTER a reduced column set (FILTER(VALUES(Table[Col]), ...)).",
                                    tokens[k]["line"], name))
                    k += 1

            # DL002: CALCULATE nested directly inside another CALCULATE's arguments.
            if up in ("CALCULATE", "CALCULATETABLE"):
                depth = 0
                k = a0
                while k < a1:
                    tk = tokens[k]
                    if tk["type"] == "punc" and tk["value"] == "(":
                        depth += 1
                    elif tk["type"] == "punc" and tk["value"] == ")":
                        depth -= 1
                    elif (tk["type"] == "ident" and tk["value"].upper() in ("CALCULATE", "CALCULATETABLE")
                          and _is_call(tokens, k)):
                        findings.append(_finding(
                            "DL002", "info",
                            "Nested CALCULATE adds an extra context transition and is often unintended.",
                            "Lift inner logic into VARs computed before the outer CALCULATE, or confirm the "
                            "double context transition is intentional.",
                            tokens[k]["line"], name))
                        break
                    k += 1

            # DL004: IFERROR (optimizer fence; usually a code smell)
            if up == "IFERROR":
                findings.append(_finding(
                    "DL004", "info",
                    "IFERROR prevents query-plan optimization and hides the real error.",
                    "Prefer DIVIDE for division, or fix the root cause and use COALESCE/ISBLANK as needed.",
                    t["line"], name))

            # DL006: EARLIER (legacy; VAR is clearer and avoids nested-row-context bugs)
            if up == "EARLIER":
                findings.append(_finding(
                    "DL006", "info",
                    "EARLIER references an outer row context and is error-prone in nested iterators.",
                    "Capture the value in a VAR before the inner iterator and reference the VAR instead.",
                    t["line"], name))

            # DL007: SUMMARIZE hosting an aggregation expression (classic perf trap)
            if up == "SUMMARIZE":
                depth = 0
                k = a0
                while k < a1:
                    tk = tokens[k]
                    if tk["type"] == "punc" and tk["value"] == "(":
                        depth += 1
                    elif tk["type"] == "punc" and tk["value"] == ")":
                        depth -= 1
                    elif (depth == 0 and tk["type"] == "ident" and tk["value"].upper() in _AGGREGATORS
                          and _is_call(tokens, k)):
                        findings.append(_finding(
                            "DL007", "warning",
                            "SUMMARIZE used to compute an aggregation can produce wrong results and is slow.",
                            "Use SUMMARIZECOLUMNS, or ADDCOLUMNS(SUMMARIZE(group cols), ...) with CALCULATE around "
                            "the aggregation.",
                            tokens[k]["line"], name))
                        break
                    k += 1

            # DL008: unknown function (likely typo or hallucinated function)
            if up not in DAX_FUNCTIONS and up not in DAX_KEYWORDS and up not in var_names:
                findings.append(_finding(
                    "DL008", "info",
                    f"'{t['value']}' is called like a function but is not a recognized DAX function.",
                    "Check for a typo or an unsupported/hallucinated function name; verify against the DAX reference.",
                    t["line"], name))

        else:
            # DL005: '+ 0' blank-suppression (ident path won't catch; handled below via op scan)
            pass

    # DL005: "+ 0" / "+0" appended to force blanks to zero
    for i in range(n - 1):
        if (tokens[i]["type"] == "op" and tokens[i]["value"] == "+"
                and tokens[i + 1]["type"] == "number" and tokens[i + 1]["value"].rstrip("0").rstrip(".") in ("", "0")):
            # number is 0 / 0.0
            if float(tokens[i + 1]["value"]) == 0.0:
                findings.append(_finding(
                    "DL005", "info",
                    "Adding 0 forces BLANK results to 0, which removes valid blank suppression and can slow scans.",
                    "Confirm zeros are intended; otherwise drop '+ 0' and let measures return BLANK.",
                    tokens[i]["line"], name))

    findings.sort(key=lambda f: (-SEVERITY_RANK.get(f["severity"], 0), f["line"], f["rule_id"]))
    return findings


def lint_measures(measures: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Lint a list of {name, expression} measures. Returns {summary, findings}."""
    all_findings: List[Dict[str, Any]] = []
    for m in measures or []:
        all_findings.extend(lint_expression(m.get("name"), m.get("expression") or m.get("dax") or ""))
    by_sev: Dict[str, int] = {}
    by_rule: Dict[str, int] = {}
    for f in all_findings:
        by_sev[f["severity"]] = by_sev.get(f["severity"], 0) + 1
        by_rule[f["rule_id"]] = by_rule.get(f["rule_id"], 0) + 1
    return {
        "summary": {"total": len(all_findings), "by_severity": by_sev, "by_rule": by_rule,
                    "measures_scanned": len(measures or [])},
        "findings": all_findings,
    }


def suggest_rewrites(name: Optional[str], dax: str) -> List[Dict[str, Any]]:
    """Concrete, mechanical rewrite hints for the auto-fixable rules. Conservative: returns
    hints (before/after templates), not a guaranteed-equivalent transformed expression."""
    hints: List[Dict[str, Any]] = []
    for f in lint_expression(name, dax):
        if f["rule_id"] == "DL003":
            hints.append({"rule_id": "DL003", "line": f["line"],
                          "before": "<numerator> / <denominator>",
                          "after": "DIVIDE(<numerator>, <denominator>)",
                          "note": "DIVIDE returns BLANK (not an error) when the denominator is 0."})
        elif f["rule_id"] == "DL001":
            hints.append({"rule_id": "DL001", "line": f["line"],
                          "before": "CALCULATE(<expr>, FILTER(Table, Table[Col] = x))",
                          "after": "CALCULATE(<expr>, Table[Col] = x)",
                          "note": "A boolean filter argument is applied without materializing the whole table."})
        elif f["rule_id"] == "DL007":
            hints.append({"rule_id": "DL007", "line": f["line"],
                          "before": "SUMMARIZE(Table, Table[Group], \"Total\", SUM(Table[Amount]))",
                          "after": "SUMMARIZECOLUMNS(Table[Group], \"Total\", SUM(Table[Amount]))",
                          "note": "SUMMARIZECOLUMNS computes the aggregation in the correct filter context."})
    return hints
