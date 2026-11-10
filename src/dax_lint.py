"""
DAX performance + correctness linter ("BPA for DAX").

A pure-Python static analyzer for DAX expressions. It lexes with the shared tokenizer
(dax_tokens: strings, comments, quoted names and [bracketed ]] names] are single tokens, so a
slash or a comment marker inside a name or a string is never mistaken for code) and runs a rule
set over the significant tokens. The anti-patterns are public DAX facts; the detectors and the
rewriter here are original.

Rules:
    DL001 warning  FILTER over a whole table as a CALCULATE filter argument
    DL002 info     CALCULATE nested directly in another CALCULATE's expression
    DL003 warning  '/' division by something that can be zero (use DIVIDE)
    DL004 info     IFERROR
    DL005 info     '+ 0' / '0 +' blank-to-zero coercion
    DL006 info     EARLIER / EARLIEST
    DL007 warning  SUMMARIZE with extension columns
    DL008 info     call to a name that is not a DAX function

Entry points:
    lint_expression(name, dax)   -> list of finding dicts
    lint_measures(measures)      -> {"summary": {...}, "findings": [...]}
    suggest_rewrites(name, dax)  -> list of {rule_id, kind, line, before, after, note}
    rewrite_expression(dax)      -> {"expression", "changed", "rewrites"}
"""
import bisect
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import dax_tokens as dt
from dax_functions import FUNCTIONS, ITERATORS, is_known_function

SEVERITY_RANK = {"error": 3, "warning": 2, "info": 1}

# Kept for callers that imported these names from the linter.
DAX_KEYWORDS = set(dt.KEYWORDS) | {"MPARAMETER"}
DAX_FUNCTIONS = set(FUNCTIONS)

# FILTER(<one of these>(Table), ...) still scans every row of the table.
_WHOLE_TABLE_WRAPPERS = {"ALL", "ALLNOBLANKROW", "ALLSELECTED", "VALUES", "DISTINCT"}
_CALCULATES = ("CALCULATE", "CALCULATETABLE")
_MAX_REWRITES = 500


class _Lexed:
    """An expression's significant tokens plus the lookups the rules need."""

    def __init__(self, dax: str):
        self.text = dax or ""
        self.tokens = dt.tokenize(self.text)
        self.sig: List[dt.Token] = [t for t in self.tokens if t.kind not in (dt.WS, dt.COMMENT)]
        self._line_starts = [0] + [i + 1 for i, ch in enumerate(self.text) if ch == "\n"]
        self._match: Dict[int, int] = {}
        stack: List[int] = []
        for p, t in enumerate(self.sig):
            if t.kind != dt.OP:
                continue
            if t.text in "({":
                stack.append(p)
            elif t.text in ")}" and stack:
                opener = stack.pop()
                self._match[opener] = p
                self._match[p] = opener

    def __len__(self) -> int:
        return len(self.sig)

    def line(self, tok: dt.Token) -> int:
        return bisect.bisect_right(self._line_starts, tok.start)

    def is_op(self, p: int, text: str) -> bool:
        return 0 <= p < len(self.sig) and self.sig[p].kind == dt.OP and self.sig[p].text == text

    def upper(self, p: int) -> str:
        return self.sig[p].text.upper()

    def is_call(self, p: int) -> bool:
        """sig[p] is an identifier directly followed by '(' and is not a keyword."""
        return (0 <= p < len(self.sig) and self.sig[p].kind == dt.IDENT and self.is_op(p + 1, "(")
                and self.upper(p) not in dt.KEYWORDS)

    def close(self, open_p: int) -> int:
        """Position of the bracket matching the opener at ``open_p`` (len(sig) if unbalanced)."""
        return self._match.get(open_p, len(self.sig))

    def opener(self, close_p: int) -> Optional[int]:
        return self._match.get(close_p)

    def args(self, open_p: int) -> List[Tuple[int, int]]:
        """Half-open (start, end) position ranges of the top-level arguments of the call whose
        '(' is at ``open_p``. An empty call has no arguments."""
        end = self.close(open_p)
        out: List[Tuple[int, int]] = []
        start = open_p + 1
        p = start
        while p < end:
            t = self.sig[p]
            if t.kind == dt.OP and t.text in "({":
                p = self.close(p)
                if p >= end:
                    break
            elif t.kind == dt.OP and t.text == ",":
                out.append((start, p))
                start = p + 1
            p += 1
        if start < end or out:
            out.append((start, end))
        return out


def tokenize(dax: str) -> List[Dict[str, Any]]:
    """Significant tokens as [{type, value, pos, line}] (comments and whitespace dropped).

    ``pos`` and ``line`` refer to the original text. Types: string, column, qtable, number,
    ident, op, punc.
    """
    lx = _Lexed(dax)
    kinds = {dt.STRING: "string", dt.BRACKET: "column", dt.QNAME: "qtable",
             dt.NUMBER: "number", dt.IDENT: "ident"}
    out = []
    for t in lx.sig:
        kind = kinds.get(t.kind) or ("punc" if t.text in ("(", ")", ",") else "op")
        out.append({"type": kind, "value": t.text, "pos": t.start, "line": lx.line(t)})
    return out


# ---------------------------------------------------------------- shared token predicates

def _is_operand_end(t: dt.Token) -> bool:
    """Can this token be the last token of an operand (so a following +, -, / is binary)?"""
    if t.kind in (dt.BRACKET, dt.NUMBER, dt.STRING, dt.QNAME):
        return True
    if t.kind == dt.IDENT:
        return t.text.upper() not in dt.KEYWORDS or t.text.upper() in ("TRUE", "FALSE")
    return t.kind == dt.OP and t.text in (")", "}")


def _is_zero(t: dt.Token) -> bool:
    if t.kind != dt.NUMBER:
        return False
    try:
        return float(t.text) == 0.0
    except ValueError:
        return False


def _local_names(lx: _Lexed) -> Tuple[Set[str], Set[str]]:
    """(names that are local values, names defined as functions) in this expression, case-folded.

    Locals are VAR names and the parameters of DEFINE FUNCTION bodies: neither is a model table,
    so FILTER(<local>, ...) is not a whole-table scan.
    """
    locals_ = set(dt.variable_names(lx.tokens))
    functions: Set[str] = set()
    for p, t in enumerate(lx.sig):
        if not (t.kind == dt.IDENT and t.text.upper() == "FUNCTION"):
            continue
        if p + 2 < len(lx) and lx.sig[p + 1].kind == dt.IDENT and lx.is_op(p + 2, "="):
            functions.add(lx.sig[p + 1].text.casefold())
            if lx.is_op(p + 3, "("):
                for a0, a1 in lx.args(p + 3):
                    if a0 < a1 and lx.sig[a0].kind == dt.IDENT:
                        locals_.add(lx.sig[a0].text.casefold())
    return locals_, functions


def _is_bare_table(lx: _Lexed, a0: int, a1: int, locals_: Set[str]) -> bool:
    """The argument sig[a0:a1] is exactly one table name (not a variable, not a column)."""
    if a1 - a0 != 1:
        return False
    t = lx.sig[a0]
    if t.kind == dt.QNAME:
        return True
    return (t.kind == dt.IDENT and t.text.upper() not in dt.KEYWORDS
            and t.text.casefold() not in locals_)


def _is_whole_table(lx: _Lexed, a0: int, a1: int, locals_: Set[str]) -> bool:
    """Table, or ALL(Table) / VALUES(Table) / ...: every row of the table is iterated."""
    if _is_bare_table(lx, a0, a1, locals_):
        return True
    if lx.is_call(a0) and lx.upper(a0) in _WHOLE_TABLE_WRAPPERS and lx.close(a0 + 1) == a1 - 1:
        inner = lx.args(a0 + 1)
        return len(inner) == 1 and _is_bare_table(lx, inner[0][0], inner[0][1], locals_)
    return False


def _constant_divisor(lx: _Lexed, slash_p: int) -> bool:
    """Is the divisor a non-zero numeric literal? Such a division cannot divide by zero, and
    '/' is the faster and clearer choice for it."""
    p = slash_p + 1
    if p < len(lx) and lx.sig[p].kind == dt.OP and lx.sig[p].text in "+-":
        p += 1
    return p < len(lx) and lx.sig[p].kind == dt.NUMBER and not _is_zero(lx.sig[p])


def _finding(rule_id, severity, message, suggestion, line, obj=None) -> Dict[str, Any]:
    return {"rule_id": rule_id, "severity": severity, "message": message,
            "suggestion": suggestion, "line": line, "object": obj}


# ---------------------------------------------------------------- the rules

def lint_expression(name: Optional[str], dax: str,
                    known_functions: Optional[Iterable[str]] = None) -> List[Dict[str, Any]]:
    """Lint a single DAX expression, returning a list of finding dicts.

    ``known_functions``: extra callable names to accept, e.g. the model's user-defined functions.
    Functions declared with DEFINE FUNCTION inside the expression are always accepted.
    """
    if not dax or not dax.strip():
        return []
    lx = _Lexed(dax)
    locals_, udfs = _local_names(lx)
    extra = {str(f).casefold() for f in (known_functions or [])}
    findings: List[Dict[str, Any]] = []
    n = len(lx)

    for p, t in enumerate(lx.sig):
        if t.kind == dt.OP and t.text == "/":
            # DL003: only a binary '/' (an operand on both sides) whose divisor could be zero.
            if 0 < p < n - 1 and _is_operand_end(lx.sig[p - 1]) and not _constant_divisor(lx, p):
                findings.append(_finding(
                    "DL003", "warning",
                    "Division with '/' does not guard against divide-by-zero (returns Infinity/error).",
                    "Use DIVIDE(numerator, denominator) which returns BLANK on a zero denominator.",
                    lx.line(t), name))
            continue

        if t.kind == dt.NUMBER and _is_zero(t):
            _check_plus_zero(lx, p, name, findings)
            continue

        if not lx.is_call(p):
            continue
        up = lx.upper(p)
        arglist = lx.args(p + 1)

        if up in _CALCULATES:
            # DL001: the first argument is the expression being calculated; the rest are filters.
            for a0, a1 in arglist[1:]:
                if lx.is_call(a0) and lx.upper(a0) == "KEEPFILTERS" and lx.close(a0 + 1) == a1 - 1:
                    inner = lx.args(a0 + 1)
                    if len(inner) == 1:
                        a0, a1 = inner[0]
                if not (lx.is_call(a0) and lx.upper(a0) == "FILTER" and lx.close(a0 + 1) == a1 - 1):
                    continue
                fargs = lx.args(a0 + 1)
                if len(fargs) >= 2 and _is_whole_table(lx, fargs[0][0], fargs[0][1], locals_):
                    findings.append(_finding(
                        "DL001", "warning",
                        "FILTER over an entire table inside CALCULATE materializes the whole table.",
                        "Filter the column instead: CALCULATE(..., KEEPFILTERS(Table[Col] = x)), or for a "
                        "measure predicate FILTER(VALUES(Table[Col]), [Measure] > x).",
                        lx.line(lx.sig[a0]), name))

            # DL002: a CALCULATE reached from the outer one's expression without a row context
            # in between. Inside an iterator the inner CALCULATE is a deliberate context transition.
            if arglist:
                e0, e1 = arglist[0]
                open_calls: List[Tuple[int, bool]] = []
                for k in range(e0, e1):
                    while open_calls and open_calls[-1][0] < k:
                        open_calls.pop()
                    if not lx.is_call(k):
                        continue
                    fn = lx.upper(k)
                    if fn in _CALCULATES and not any(row_ctx for _, row_ctx in open_calls):
                        findings.append(_finding(
                            "DL002", "info",
                            "Nested CALCULATE adds an extra context transition and is often unintended.",
                            "Merge the filters into one CALCULATE, or lift the inner one into a VAR "
                            "computed first; keep it only if the double transition is intended.",
                            lx.line(lx.sig[k]), name))
                        break
                    open_calls.append((lx.close(k + 1), fn in ITERATORS))

        if up == "IFERROR":
            findings.append(_finding(
                "DL004", "info",
                "IFERROR prevents query-plan optimization and hides the real error.",
                "Prefer DIVIDE for division, or fix the root cause and use COALESCE/ISBLANK as needed.",
                lx.line(t), name))

        if up in ("EARLIER", "EARLIEST"):
            findings.append(_finding(
                "DL006", "info",
                f"{up} references an outer row context and is error-prone in nested iterators.",
                "Capture the value in a VAR before the inner iterator and reference the VAR instead.",
                lx.line(t), name))

        if up == "SUMMARIZE":
            # DL007: a "name", <expression> pair after the table is an extension column, whatever
            # the expression is (an aggregation, DIVIDE(...) or a measure reference).
            for i in range(1, len(arglist) - 1):
                a0, a1 = arglist[i]
                if a1 - a0 == 1 and lx.sig[a0].kind == dt.STRING:
                    findings.append(_finding(
                        "DL007", "warning",
                        "SUMMARIZE with an extension column can produce wrong results and is slow.",
                        "Group with SUMMARIZE and add the column outside it: "
                        "ADDCOLUMNS(SUMMARIZE(Table, Table[Group]), \"Name\", CALCULATE(<expr>)), "
                        "or use SUMMARIZECOLUMNS in a query.",
                        lx.line(lx.sig[a0]), name))
                    break

        # DL008: a call to something that is not a function.
        fold = t.text.casefold()
        if not (is_known_function(up) or up in DAX_KEYWORDS or fold in locals_ or fold in udfs
                or fold in extra):
            findings.append(_finding(
                "DL008", "info",
                f"'{t.text}' is called like a function but is not a recognized DAX function.",
                "Check for a typo or a hallucinated function name. If it is a user-defined function "
                "stored in the model, it is fine.",
                lx.line(t), name))

    findings.sort(key=lambda f: (-SEVERITY_RANK.get(f["severity"], 0), f["line"], f["rule_id"]))
    return findings


def _check_plus_zero(lx: _Lexed, p: int, name: Optional[str], findings: List[Dict[str, Any]]) -> None:
    """DL005 for the zero literal at sig[p]: '<operand> + 0' or '0 + <operand>'.

    A zero that is an operand of a tighter-binding operator ([x] + 0 * [y]) is not an added zero.
    """
    n = len(lx)
    nxt_tight = p + 1 < n and lx.sig[p + 1].kind == dt.OP and lx.sig[p + 1].text in ("*", "/", "^")
    trailing = (p >= 2 and lx.is_op(p - 1, "+") and _is_operand_end(lx.sig[p - 2]) and not nxt_tight)
    leading = False
    if not trailing and lx.is_op(p + 1, "+") and p + 2 < n:
        prev = lx.sig[p - 1] if p > 0 else None
        starts_term = (prev is None
                       or (prev.kind == dt.OP and prev.text in ("(", ",", "=", "{"))
                       or (prev.kind == dt.IDENT and prev.text.upper() == "RETURN"))
        leading = starts_term
    if trailing or leading:
        findings.append(_finding(
            "DL005", "info",
            "Adding 0 forces BLANK results to 0, which removes valid blank suppression and can slow scans.",
            "Confirm zeros are intended; otherwise drop '+ 0' and let measures return BLANK.",
            lx.line(lx.sig[p - 1] if trailing else lx.sig[p]), name))


def lint_measures(measures: List[Dict[str, Any]], min_severity: Optional[str] = None,
                  known_functions: Optional[Iterable[str]] = None) -> Dict[str, Any]:
    """Lint a list of {name, expression} measures. Returns {summary, findings}.

    With ``min_severity`` the findings below it are dropped and the summary counts what is left.
    """
    min_rank = SEVERITY_RANK.get((min_severity or "info").lower(), 1)
    known = list(known_functions or [])
    all_findings: List[Dict[str, Any]] = []
    for m in measures or []:
        found = lint_expression(m.get("name"), m.get("expression") or m.get("dax") or "", known)
        all_findings.extend(f for f in found if SEVERITY_RANK.get(f["severity"], 0) >= min_rank)
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


# ---------------------------------------------------------------- rewriting

def _primary_end(lx: _Lexed, p: int) -> Optional[int]:
    """Position just past the factor starting at sig[p] (sign, primary, exponents), or None."""
    n = len(lx)
    while p < n and lx.sig[p].kind == dt.OP and lx.sig[p].text in "+-":
        p += 1
    if p >= n:
        return None
    t = lx.sig[p]
    if t.kind == dt.OP and t.text in "({":
        close = lx.close(p)
        if close >= n:
            return None
        end = close + 1
    elif t.kind == dt.IDENT:
        up = t.text.upper()
        if up in dt.KEYWORDS and up not in ("TRUE", "FALSE"):
            return None
        if lx.is_op(p + 1, "("):
            close = lx.close(p + 1)
            if close >= n:
                return None
            end = close + 1
        elif p + 1 < n and lx.sig[p + 1].kind == dt.BRACKET:
            end = p + 2
        else:
            end = p + 1
    elif t.kind == dt.QNAME:
        end = p + 2 if p + 1 < n and lx.sig[p + 1].kind == dt.BRACKET else p + 1
    elif t.kind in (dt.BRACKET, dt.NUMBER, dt.STRING):
        end = p + 1
    else:
        return None
    if lx.is_op(end, "^"):
        return _primary_end(lx, end + 1)
    return end


def _primary_start(lx: _Lexed, q: int, locals_: Set[str]) -> Optional[int]:
    """Position of the first token of the primary whose last token is sig[q], or None."""
    if q < 0:
        return None
    t = lx.sig[q]
    if t.kind == dt.OP and t.text in ")}":
        opener = lx.opener(q)
        if opener is None:
            return None
        if t.text == ")" and opener > 0 and lx.is_call(opener - 1):
            return opener - 1
        return opener
    if t.kind == dt.BRACKET:
        prev = lx.sig[q - 1] if q > 0 else None
        if prev is not None and (prev.kind == dt.QNAME or (
                prev.kind == dt.IDENT and prev.text.upper() not in dt.KEYWORDS
                and prev.text.casefold() not in locals_)):
            return q - 1
        return q
    if t.kind in (dt.NUMBER, dt.STRING, dt.QNAME):
        return q
    if t.kind == dt.IDENT and _is_operand_end(t):
        return q
    return None


def _term_start(lx: _Lexed, q: int, locals_: Set[str]) -> Optional[int]:
    """Start of the multiplicative term that ends at sig[q]: everything '/' divides."""
    s = _primary_start(lx, q, locals_)
    while s is not None and s > 0:
        prev = lx.sig[s - 1]
        if prev.kind != dt.OP:
            break
        if prev.text in ("*", "/", "^"):
            s = _primary_start(lx, s - 2, locals_)
        elif prev.text in ("+", "-") and (s - 2 < 0 or not _is_operand_end(lx.sig[s - 2])):
            s -= 1                      # a sign, not a subtraction: it belongs to the term
        else:
            break
    return s


def _strip_outer_parens(text: str) -> str:
    """'(a + b)' -> 'a + b' when the parentheses wrap the whole operand."""
    lx = _Lexed(text)
    if len(lx) < 2 or not lx.is_op(0, "(") or lx.close(0) != len(lx) - 1:
        return text.strip()
    if len(lx.args(0)) != 1:        # "()" or a row constructor "(a, b)": the parentheses matter
        return text.strip()
    return text[lx.sig[0].end:lx.sig[-1].start].strip()


def _division_site(lx: _Lexed, slash_p: int, locals_: Set[str]) -> Optional[Tuple[int, int]]:
    """(first, last) significant positions of 'numerator / divisor' around sig[slash_p], or None
    when the operands cannot be delimited with certainty."""
    if slash_p == 0 or not _is_operand_end(lx.sig[slash_p - 1]):
        return None
    start = _term_start(lx, slash_p - 1, locals_)
    end = _primary_end(lx, slash_p + 1)
    if start is None or end is None:
        return None
    # A comment between an operand and the operator would be lost by the rewrite.
    gap_from, gap_to = lx.sig[slash_p - 1].end, lx.sig[slash_p + 1].start
    if any(t.kind == dt.COMMENT and gap_from <= t.start < gap_to for t in lx.tokens):
        return None
    return start, end - 1


def _rewrite_divisions(dax: str) -> Tuple[str, List[Dict[str, Any]], List[int]]:
    """Rewrite every unsafe 'a / b' to DIVIDE(a, b), leftmost first.

    Returns (new_text, rewrites, lines_left_alone). Each pass rewrites one operator and re-lexes,
    so a chain a / b / c nests correctly as DIVIDE(DIVIDE(a, b), c).
    """
    text = dax or ""
    first = _Lexed(text)
    lines = [first.line(t) for t in first.sig if t.kind == dt.OP and t.text == "/"]
    rewrites: List[Dict[str, Any]] = []
    unparsed: List[int] = []
    skip = 0            # leading '/' operators already examined and left as they are
    for _ in range(_MAX_REWRITES):
        lx = _Lexed(text)
        slashes = [p for p, t in enumerate(lx.sig) if t.kind == dt.OP and t.text == "/"]
        if skip >= len(slashes):
            break
        p = slashes[skip]
        locals_, _ = _local_names(lx)
        site = None
        constant = p > 0 and _constant_divisor(lx, p)
        if not constant:
            site = _division_site(lx, p, locals_)
        if site is None:
            if not constant and p > 0 and _is_operand_end(lx.sig[p - 1]):
                unparsed.append(lines[skip])
            skip += 1
            continue
        s, e = site
        left = text[lx.sig[s].start:lx.sig[p - 1].end]
        right = text[lx.sig[p + 1].start:lx.sig[e].end]
        before = text[lx.sig[s].start:lx.sig[e].end]
        after = f"DIVIDE({_strip_outer_parens(left)}, {_strip_outer_parens(right)})"
        rewrites.append({"line": lines.pop(skip), "before": before, "after": after})
        text = text[:lx.sig[s].start] + after + text[lx.sig[e].end:]
    return text, rewrites, unparsed


def rewrite_expression(dax: str) -> Dict[str, Any]:
    """Apply the mechanical rewrites to an expression.

    Only 'a / b' -> DIVIDE(a, b) qualifies: the operands are delimited from the token stream, so
    the result differs from the input only where the divisor is zero or blank (BLANK instead of
    an error or infinity). Everything outside the rewritten spans is returned byte for byte.
    Returns {"expression", "changed", "rewrites": [{rule_id, line, before, after}]}.
    """
    new_text, rewrites, _ = _rewrite_divisions(dax or "")
    return {"expression": new_text, "changed": bool(rewrites),
            "rewrites": [dict(r, rule_id="DL003") for r in rewrites]}


_TEMPLATES = {
    "DL001": ("CALCULATE(<expr>, FILTER(Table, Table[Col] = x))",
              "CALCULATE(<expr>, KEEPFILTERS(Table[Col] = x))",
              "Template, not a rewrite of your expression. A column predicate does not materialize the "
              "table; KEEPFILTERS keeps the existing filter on the column, as FILTER(Table, ...) did. "
              "For a measure predicate use FILTER(VALUES(Table[Col]), [Measure] > x)."),
    "DL003": ("<numerator> / <denominator>",
              "DIVIDE(<numerator>, <denominator>)",
              "Template, not a rewrite of your expression: the operands could not be delimited "
              "mechanically here. DIVIDE returns BLANK when the denominator is 0."),
    "DL007": ("SUMMARIZE(Table, Table[Group], \"Total\", SUM(Table[Amount]))",
              "ADDCOLUMNS(SUMMARIZE(Table, Table[Group]), \"Total\", CALCULATE(SUM(Table[Amount])))",
              "Template, not a rewrite of your expression. SUMMARIZE only groups; the CALCULATE gives "
              "each group its own filter context. In a query, SUMMARIZECOLUMNS is the shorter form."),
}


def suggest_rewrites(name: Optional[str], dax: str,
                     known_functions: Optional[Iterable[str]] = None) -> List[Dict[str, Any]]:
    """Rewrite suggestions for an expression: {rule_id, kind, line, before, after, note}.

    kind "rewrite"    before/after are real text: ``before`` is a span of the expression (or of
                      the expression after the rewrites listed above it) and ``after`` replaces it.
    kind "expression" the whole expression with every "rewrite" applied.
    kind "template"   a generic pattern to adapt by hand; nothing was derived from your text.
    """
    new_text, rewrites, unparsed = _rewrite_divisions(dax or "")
    hints: List[Dict[str, Any]] = [
        {"rule_id": "DL003", "kind": "rewrite", "line": r["line"], "before": r["before"], "after": r["after"],
         "note": "Mechanical rewrite. DIVIDE returns BLANK (not an error) when the denominator is 0 or "
                 "BLANK; otherwise the result is unchanged."}
        for r in rewrites]
    templates: List[Dict[str, Any]] = []
    for line in unparsed:
        before, after, note = _TEMPLATES["DL003"]
        templates.append({"rule_id": "DL003", "kind": "template", "line": line,
                          "before": before, "after": after, "note": note})
    for f in lint_expression(name, dax, known_functions):
        if f["rule_id"] in ("DL001", "DL007"):
            before, after, note = _TEMPLATES[f["rule_id"]]
            templates.append({"rule_id": f["rule_id"], "kind": "template", "line": f["line"],
                              "before": before, "after": after, "note": note})
    hints.extend(sorted(templates, key=lambda h: (h["line"], h["rule_id"])))
    if rewrites:
        hints.append({"rule_id": "DL003", "kind": "expression", "line": 1,
                      "before": dax, "after": new_text,
                      "note": f"The full expression with the {len(rewrites)} mechanical rewrite(s) applied. "
                              "Validate it against the model before saving."})
    return hints
