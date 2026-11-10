"""
What a DAX query touches, as far as the access-policy engine can tell from its text.

The shared tokenizer (dax_tokens) says which tables and columns a query names. This module
adds the structure the policy needs on top of that:

  * measures: a measure reference is expanded into the measure's own DAX, transitively, when
    the caller supplies the model's measure definitions, so SUM(Customers[ssn]) hidden behind
    [Some Measure] is still seen;
  * aggregation: whether every use of a column is the direct argument of an aggregate such
    as SUM(T[c]) (for aggregate_only columns);
  * row flow: whether whole rows of a table can reach a function that hands them back under
    other names or as text, where a name-based column check cannot follow them: TOCSV and
    TOJSON (serialised), TREATAS (relabelled), UNION (later arguments take the first one's
    column names);
  * query-defined tables and columns (DEFINE TABLE / DEFINE COLUMN), whose result keys look
    like model columns but are not.

Nothing here knows the model's schema, and DAX is not fully parsed. The analysis is built to
err on the side of reporting a flow that is not there; it can still miss one, for example
through a table function it does not know or a calculated table that copies protected data
under new names. Treat it as defence in depth over RLS and OLS in the model.
"""
from bisect import bisect_left
from dataclasses import dataclass, field
from functools import lru_cache
from typing import Dict, FrozenSet, List, Mapping, Optional, Set, Tuple

import dax_tokens as dt

# Aggregates that return a statistic of the column and never one of its values. MIN, MAX,
# MEDIAN and the percentiles are left out on purpose: they return actual data points.
AGGREGATE_FUNCTIONS = frozenset({
    "SUM", "AVERAGE", "AVERAGEA", "COUNT", "COUNTA", "COUNTBLANK", "DISTINCTCOUNT",
    "DISTINCTCOUNTNOBLANK", "APPROXIMATEDISTINCTCOUNT", "STDEV.S", "STDEV.P", "VAR.S", "VAR.P",
})
# What counts as "the query filters" for a table rule with require_filter.
FILTER_FUNCTIONS = frozenset({"FILTER", "CALCULATETABLE", "CALCULATE", "TOPN"})
SERIALIZERS = frozenset({"TOCSV", "TOJSON"})

# Table functions whose result rows carry the columns of a table argument unchanged, and
# which arguments those are (None = any). A whole-table reference "flows" through these;
# through anything else (a projection such as SELECTCOLUMNS, an aggregate, an unknown
# function) it does not.
_ANY = None
_ROW_PASSTHROUGH: Dict[str, Optional[FrozenSet[int]]] = {
    "FILTER": frozenset({0}), "CALCULATETABLE": frozenset({0}), "KEEPFILTERS": frozenset({0}),
    "NONVISUAL": frozenset({0}), "TOPN": frozenset({1}), "TOPNSKIP": frozenset({2}),
    "SAMPLE": frozenset({1}), "DISTINCT": frozenset({0}), "VALUES": frozenset({0}),
    "ALL": _ANY, "ALLSELECTED": _ANY, "ALLNOBLANKROW": _ANY, "ALLEXCEPT": frozenset({0}),
    "ADDCOLUMNS": frozenset({0}), "UNION": _ANY, "EXCEPT": frozenset({0}), "INTERSECT": frozenset({0}),
    "CROSSJOIN": _ANY, "GENERATE": _ANY, "GENERATEALL": _ANY, "NATURALINNERJOIN": _ANY,
    "NATURALLEFTOUTERJOIN": _ANY, "RELATEDTABLE": frozenset({0}), "DETAILROWS": _ANY,
    "EVALUATEANDLOG": frozenset({0}), "TREATAS": frozenset({0}), "SUBSTITUTEWITHINDEX": frozenset({0}),
    "ADDMISSINGITEMS": _ANY, "WINDOW": _ANY, "OFFSET": _ANY, "INDEX": _ANY,
    "IF": frozenset({1, 2}), "IF.EAGER": frozenset({1, 2}), "SWITCH": _ANY, "IFERROR": _ANY,
    "COALESCE": _ANY,
}
_VAR_TERMINATORS = frozenset({"VAR", "RETURN", "EVALUATE", "DEFINE", "MEASURE", "COLUMN", "TABLE",
                              "FUNCTION", "ORDER", "START"})
# Analyses of texts up to this size are cached; a measure body or a typical query is far below it.
_CACHE_TEXT_LIMIT = 20_000
# Limits that only adversarial input reaches. Past them the analysis stops following rows and
# reports "untraced", which the policy treats as "may be protected".
_MAX_NESTING = 512
_WORK_BUDGET = 2_000_000


@dataclass(frozen=True)
class SinkFinding:
    """Whole rows may reach ``function``. ``tables`` are the tables they may come from;
    ``untraced`` is True when a variable or expression could not be followed to a table."""
    function: str
    tables: FrozenSet[str]
    untraced: bool


@dataclass(frozen=True)
class TextFacts:
    """Facts about one DAX text (a query, or one measure body)."""
    # (table or None, name) -> True when every occurrence is a direct aggregate argument
    columns: Tuple[Tuple[Optional[str], str, bool], ...]
    bare_tables: Tuple[str, ...]          # tables used on their own: EVALUATE T, COUNTROWS(T)
    functions: FrozenSet[str]             # upper-cased names of the functions called
    sinks: Tuple[SinkFinding, ...]
    defined_tables: FrozenSet[str]        # case-folded names from DEFINE TABLE
    defined_columns: FrozenSet[Tuple[str, str]]   # case-folded (table, column) from DEFINE COLUMN


@dataclass
class QueryFacts:
    """Facts about a query together with every measure it reaches."""
    # key: (case-folded table or None, case-folded name) -> (table, name, always_aggregated)
    columns: Dict[Tuple[Optional[str], str], Tuple[Optional[str], str, bool]] = field(default_factory=dict)
    bare_tables: Dict[str, str] = field(default_factory=dict)      # case-folded -> as written
    tables: Dict[str, str] = field(default_factory=dict)           # bare tables and qualifiers
    functions: Set[str] = field(default_factory=set)               # across query and measures
    query_functions: FrozenSet[str] = frozenset()                  # in the query text itself
    sinks: List[SinkFinding] = field(default_factory=list)
    defined_tables: Set[str] = field(default_factory=set)
    defined_columns: Set[Tuple[str, str]] = field(default_factory=set)
    measures_expanded: List[str] = field(default_factory=list)


def _analyze(text: str) -> TextFacts:
    tokens = dt.tokenize(text)
    sig = dt.significant(tokens)
    n = len(sig)
    position = {index: p for p, index in enumerate(sig)}
    variables = dt.variable_names(tokens)

    # ---- call tree: which call encloses each significant token, and where its commas are
    call_name: List[str] = []
    call_open: List[int] = []
    call_close: List[int] = []
    call_parent: List[int] = []
    call_commas: List[List[int]] = []
    enclosing = [-1] * n
    stack: List[int] = []
    for p in range(n):
        tok = tokens[sig[p]]
        enclosing[p] = stack[-1] if stack else -1
        if tok.kind != dt.OP:
            continue
        if tok.text in ("(", "{"):
            name = ""
            if tok.text == "{":
                name = "{"  # a table constructor holds scalar expressions only
            elif p > 0:
                prev = tokens[sig[p - 1]]
                if prev.kind == dt.IDENT and prev.text.upper() not in dt.KEYWORDS:
                    name = prev.text.upper()
            call_name.append(name)
            call_open.append(p)
            call_close.append(n)
            call_parent.append(stack[-1] if stack else -1)
            call_commas.append([])
            stack.append(len(call_name) - 1)
        elif tok.text in (")", "}"):
            if stack:
                call_close[stack.pop()] = p
        elif tok.text == "," and stack:
            call_commas[stack[-1]].append(p)

    def flows_to_top(p: int, span_start: int) -> bool:
        """True when a table expression at position ``p`` can be (part of) the value of the
        span starting at ``span_start``: every call between them passes rows through."""
        child = p
        call = enclosing[p]
        for _ in range(_MAX_NESTING):
            if call == -1 or call_open[call] < span_start:
                return True
            name = call_name[call]
            if name:
                if name not in _ROW_PASSTHROUGH:
                    return False
                allowed = _ROW_PASSTHROUGH[name]
                if allowed is not None and bisect_left(call_commas[call], child) not in allowed:
                    return False
            child = call_open[call]
            call = call_parent[call]
        return True  # nested deeper than any real query: assume the rows get through

    # ---- references
    columns: Dict[Tuple[Optional[str], str], Tuple[Optional[str], str, bool]] = {}
    bare_tables: Dict[str, str] = {}
    table_positions: List[Tuple[int, str]] = []
    for ref in dt.iter_references(tokens):
        p = position[ref.index]
        if ref.kind == "table":
            tok = tokens[ref.index]
            if (tok.kind == dt.IDENT and tok.text.upper() in dt.ENUM_WORDS and p > 0
                    and tokens[sig[p - 1]].kind == dt.OP and tokens[sig[p - 1]].text == ","):
                continue  # DATEADD(..., MONTH): an option word, not a table
            bare_tables.setdefault(ref.table.casefold(), ref.table)
            table_positions.append((p, ref.table))
            continue
        first = position[ref.table_index] if ref.kind == "column" else p
        call = enclosing[p]
        aggregated = (call != -1 and call_name[call] in AGGREGATE_FUNCTIONS
                      and call_open[call] + 1 == first and p + 1 == call_close[call])
        key = (ref.table.casefold() if ref.table is not None else None, ref.name.casefold())
        seen = columns.get(key)
        columns[key] = (ref.table, ref.name, aggregated if seen is None else (seen[2] and aggregated))

    # ---- variables: where each is defined and where it is used
    var_defs: List[Tuple[int, int, str]] = []   # (start, end, name) in order of appearance
    var_untraced: Set[str] = set()
    var_scopes: Dict[str, List[Tuple[int, int]]] = {}
    var_uses: List[Tuple[int, str]] = []
    declaration_sites = set()
    budget = _WORK_BUDGET  # bounds the scans below on adversarially nested input
    for p in range(n - 2):
        tok = tokens[sig[p]]
        if tok.kind != dt.IDENT or tok.text.upper() != "VAR":
            continue
        name_tok, eq_tok = tokens[sig[p + 1]], tokens[sig[p + 2]]
        if name_tok.kind != dt.IDENT or eq_tok.kind != dt.OP or eq_tok.text != "=":
            continue
        declaration_sites.add(p + 1)
        name = name_tok.text.casefold()
        level = enclosing[p]
        start = p + 3
        end = start
        while end < n and budget > 0:
            t = tokens[sig[end]]
            if level != -1 and end == call_close[level]:
                break
            if enclosing[end] == level and t.kind == dt.IDENT and t.text.upper() in _VAR_TERMINATORS:
                break
            end += 1
            budget -= 1
        if end == start or budget <= 0:
            # VAR a = VAR b = ... (a nested block this scan does not follow), or out of budget
            var_untraced.add(name)
        var_defs.append((start, end, name))
        # The variable is visible from the end of its definition to the end of its VAR ...
        # RETURN block: the end of the argument it sits in, or of the text at top level.
        scope_end = n
        if level != -1:
            commas = call_commas[level]
            later = bisect_left(commas, end)
            scope_end = commas[later] if later < len(commas) else call_close[level]
        var_scopes.setdefault(name, []).append((end, scope_end))
    if variables:
        for p in range(n):
            tok = tokens[sig[p]]
            if tok.kind != dt.IDENT or p in declaration_sites or tok.text.casefold() not in variables:
                continue
            if p + 1 < n and tokens[sig[p + 1]].kind == dt.OP and tokens[sig[p + 1]].text == "(":
                continue  # a function call
            folded = tok.text.casefold()
            scopes = var_scopes.get(folded, ())
            budget -= len(scopes)
            if budget > 0 and not any(lo <= p < hi for lo, hi in scopes):
                # The tokenizer's list of variables has no scopes: this name is a variable
                # somewhere else in the text, not here. Outside its block it can only be a
                # table, and hiding a table behind a same-named variable must not work.
                bare_tables.setdefault(folded, tok.text)
                table_positions.append((p, tok.text))
            else:
                var_uses.append((p, folded))
        table_positions.sort()
    table_at = [p for p, _ in table_positions]
    var_use_at = [p for p, _ in var_uses]
    var_memo: Dict[str, Tuple[Set[str], bool]] = {}

    def span_sources(start: int, end: int) -> Tuple[Set[str], bool]:
        """(tables whose whole rows can be the value of the expression in [start, end),
        untraced). ``untraced`` means some part of it could not be followed to a table."""
        nonlocal budget
        tables: Set[str] = set()
        untraced = False
        lo, hi = bisect_left(table_at, start), bisect_left(table_at, end)
        budget -= hi - lo
        for p, table in table_positions[lo:hi]:
            if flows_to_top(p, start):
                tables.add(table)
        lo, hi = bisect_left(var_use_at, start), bisect_left(var_use_at, end)
        budget -= hi - lo
        for p, name in var_uses[lo:hi]:
            if flows_to_top(p, start):
                known = var_memo.get(name)
                if known is None:
                    untraced = True  # not defined before this point
                else:
                    tables |= known[0]
                    untraced = untraced or known[1]
        return tables, untraced or budget <= 0

    # A variable can only use variables declared before it, so one pass in textual order
    # resolves them all without recursion. A name declared twice keeps both definitions.
    for start, end, name in var_defs:
        tables, untraced = span_sources(start, end)
        prior = var_memo.get(name)
        if prior is not None:
            tables |= prior[0]
            untraced = untraced or prior[1]
        var_memo[name] = (tables, untraced or name in var_untraced)

    def argument_spans(call: int) -> List[Tuple[int, int]]:
        bounds = [call_open[call]] + call_commas[call] + [call_close[call]]
        return [(bounds[i] + 1, bounds[i + 1]) for i in range(len(bounds) - 1)]

    # ---- sinks: functions that return rows under other names, or as text
    sinks: List[SinkFinding] = []
    for call, name in enumerate(call_name):
        if name in SERIALIZERS or name == "TREATAS":
            start, end = argument_spans(call)[0]
            tables, untraced = span_sources(start, end)
            if tables or untraced:
                sinks.append(SinkFinding(name, frozenset(tables), untraced))
        elif name == "UNION":
            spans = argument_spans(call)
            first, first_untraced = span_sources(*spans[0])
            first_folded = {t.casefold() for t in first}
            for start, end in spans[1:]:
                tables, untraced = span_sources(start, end)
                # Rows of the same table under its own column names are checked by name as
                # usual; only a different source is carried under the first argument's names.
                extra = {t for t in tables if t.casefold() not in first_folded}
                if extra or untraced or (tables and first_untraced):
                    sinks.append(SinkFinding(name, frozenset(extra or tables), untraced or first_untraced))

    # ---- query-scoped definitions
    defined_tables: Set[str] = set()
    defined_columns: Set[Tuple[str, str]] = set()
    for p in range(n - 2):
        tok = tokens[sig[p]]
        if tok.kind != dt.IDENT:
            continue
        word = tok.text.upper()
        nxt = tokens[sig[p + 1]]
        if nxt.kind not in (dt.IDENT, dt.QNAME):
            continue
        after = tokens[sig[p + 2]]
        if word == "TABLE" and after.kind == dt.OP and after.text == "=":
            defined_tables.add(nxt.name.casefold())
        elif word == "COLUMN" and after.kind == dt.BRACKET:
            defined_columns.add((nxt.name.casefold(), after.name.casefold()))

    return TextFacts(
        columns=tuple(columns.values()),
        bare_tables=tuple(bare_tables.values()),
        functions=frozenset(name for name in call_name if name and name != "{"),
        sinks=tuple(sinks),
        defined_tables=frozenset(defined_tables),
        defined_columns=frozenset(defined_columns),
    )


_analyze_cached = lru_cache(maxsize=1024)(_analyze)


def analyze_text(text: str) -> TextFacts:
    """Facts about one DAX text. Never raises: malformed DAX is analysed as far as it lexes."""
    text = text or ""
    return _analyze_cached(text) if len(text) <= _CACHE_TEXT_LIMIT else _analyze(text)


def build_query_facts(query: str, measure_definitions: Optional[Mapping[str, str]] = None) -> QueryFacts:
    """Facts about ``query`` and, transitively, every measure it references.

    ``measure_definitions`` maps a measure name to its DAX. Entries that are not text, or are
    blank (a live connection can return blank expressions to a non-admin), are skipped: such a
    measure is then judged by its name alone.
    """
    definitions: Dict[str, Tuple[str, str]] = {}
    for name, expression in (measure_definitions or {}).items():
        if isinstance(name, str) and isinstance(expression, str) and expression.strip():
            definitions[name.casefold()] = (name, expression)

    facts = QueryFacts()
    root = analyze_text(query)
    facts.query_functions = root.functions
    pending = [root]
    expanded: Set[str] = set()
    while pending:
        text_facts = pending.pop()
        facts.functions |= text_facts.functions
        facts.sinks.extend(text_facts.sinks)
        facts.defined_tables |= text_facts.defined_tables
        facts.defined_columns |= text_facts.defined_columns
        for table in text_facts.bare_tables:
            facts.bare_tables.setdefault(table.casefold(), table)
            facts.tables.setdefault(table.casefold(), table)
        for table, name, aggregated in text_facts.columns:
            key = (table.casefold() if table is not None else None, name.casefold())
            seen = facts.columns.get(key)
            facts.columns[key] = (table, name, aggregated if seen is None else (seen[2] and aggregated))
            if table is not None:
                facts.tables.setdefault(table.casefold(), table)
            folded = name.casefold()
            if folded in definitions and folded not in expanded:
                expanded.add(folded)
                facts.measures_expanded.append(definitions[folded][0])
                pending.append(analyze_text(definitions[folded][1]))
    return facts
