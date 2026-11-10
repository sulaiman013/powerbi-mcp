"""
Model analysis: a Best Practice Analyzer (BPA), an AI-readiness auditor, a semantic model diff,
a data-dictionary renderer and the DAX test verdict, all over a normalized model metadata dict.

These are pure functions (no Power BI / .NET dependency) so they can be unit tested without a
live model. The server gathers metadata via INFO.VIEW.* DAX and passes a normalized dict in here.

Normalized model dict shape:
{
  "tables": [
    {
      "name": str, "is_hidden": bool, "description": str,
      "columns": [ {"name","table","data_type","is_hidden","is_key",
                    "summarize_by","sort_by","description","display_folder",
                    "data_category","is_calculated","expression"} ],
      "measures": [ {"name","table","expression","format_string","description",
                     "display_folder","is_hidden","data_type"} ],
    }
  ],
  "relationships": [ {"from_table","from_column","to_table","to_column",
                      "is_active","cross_filter","from_cardinality","to_cardinality"} ],
}
Every field is optional; rules degrade gracefully when a field is missing. A rule that needs a
field the gatherer could not supply (None everywhere) stays silent rather than guessing.

The DAX rules are the linter's rules (dax_lint), so run_bpa and dax_lint never disagree about
the same expression.
"""
import html
import math
import re
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Set, Tuple

import dax_lint
import dax_tokens as dt
from dax_functions import AGGREGATORS

SEVERITY_ORDER = {"error": 3, "warning": 2, "info": 1}

_FLOAT_TYPES = {"double", "float", "real", "decimalnumber"}
_TEXT_TYPES = {"string", "text"}
_NUMERIC_TYPES = _FLOAT_TYPES | {"int64", "integer", "wholenumber", "decimal", "currency",
                                 "fixeddecimalnumber", "number"}
_CONTROL_CHARS = re.compile("[" + "".join(chr(c) for c in range(32)) + chr(127) + "]")


# ---------------------------------------------------------------- small helpers

def _truthy(v) -> bool:
    if isinstance(v, str):
        return v.strip().lower() in ("true", "1", "yes")
    return bool(v)


def _empty(v) -> bool:
    if v is None or (isinstance(v, str) and v.strip() == ""):
        return True
    return isinstance(v, (list, tuple, dict)) and not v      # an empty container is not a value


def _fold(v) -> str:
    return str(v).casefold() if v is not None else ""


def _type_key(v) -> str:
    """'Decimal Number' / 'decimal_number' / 'DecimalNumber' -> 'decimalnumber'."""
    return re.sub(r"[\s_]+", "", str(v or "")).casefold()


def _is_internal_column(name) -> bool:
    """The engine's RowNumber-<GUID> column: present in metadata, never a user object."""
    return isinstance(name, str) and name.startswith("RowNumber-")


def _is_auto_date_table(name) -> bool:
    return isinstance(name, str) and name.startswith(("LocalDateTable_", "DateTableTemplate_"))


_WORD_RE = re.compile(r"[A-Z]+(?=[A-Z][a-z])|[A-Z]?[a-z]+|[A-Z]+|[0-9]+")
_DATE_WORDS = {"date", "dates", "calendar"}
_DATE_PREFIXES = ("dim", "tbl", "d")
_DATE_SUFFIXES = ("dimension", "table", "dim", "tbl")


def _name_words(name) -> List[str]:
    """Lower-cased words of an identifier in any style: 'DimDate', 'dim_date', 'DIM_DATE' and
    'Dim Date' all give ['dim', 'date']; 'HTTPStatusCode' gives ['http', 'status', 'code']."""
    return [w.casefold() for w in _WORD_RE.findall(str(name or ""))]


def _has_date_word(name, extra: Tuple[str, ...] = ()) -> bool:
    """Does the name say 'date table'? Whole words only ('Update Log' and 'Sentiment' do not),
    plus the glued lower-case forms 'dimdate' / 'datedim'."""
    words = _DATE_WORDS | set(extra)
    for w in _name_words(name):
        if w in words:
            return True
        if any(w.startswith(p) and w[len(p):] in words for p in _DATE_PREFIXES):
            return True
        if any(w.endswith(s) and w[:-len(s)] in words for s in _DATE_SUFFIXES):
            return True
    return False


def _tables(model: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [t for t in (model.get("tables") or []) if isinstance(t, dict)]


def _relationships(model: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [r for r in (model.get("relationships") or []) if isinstance(r, dict)]


def _children(table: Dict[str, Any], key: str) -> List[Dict[str, Any]]:
    """The table's columns or measures, dropping anything that is not an object."""
    return [x for x in (table.get(key) or []) if isinstance(x, dict)]


def _dax(obj: Dict[str, Any]) -> str:
    """The object's DAX expression, or '' when it has none (missing, blank, or not text)."""
    expr = obj.get("expression")
    return expr if isinstance(expr, str) and expr.strip() else ""


def _all_measures(model: Dict[str, Any]) -> List[Dict[str, Any]]:
    out = []
    for t in _tables(model):
        for m in _children(t, "measures"):
            m = dict(m)
            m.setdefault("table", t.get("name"))
            out.append(m)
    return out


def _all_columns(model: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Every user column (the internal RowNumber column is left out)."""
    out = []
    for t in _tables(model):
        for c in _children(t, "columns"):
            if _is_internal_column(c.get("name")):
                continue
            c = dict(c)
            c.setdefault("table", t.get("name"))
            out.append(c)
    return out


def _label(table, name) -> str:
    return f"{table}[{name}]"


def _rel_label(r: Dict[str, Any]) -> str:
    return (f"{r.get('from_table')}[{r.get('from_column')}] -> "
            f"{r.get('to_table')}[{r.get('to_column')}]")


def _is_calculated(c: Dict[str, Any]) -> bool:
    return _truthy(c.get("is_calculated")) or bool(_dax(c))


def _string_value(tok: dt.Token) -> str:
    body = tok.text[1:-1] if len(tok.text) >= 2 and tok.text.endswith('"') else tok.text[1:]
    return body.replace('""', '"')


# ---------------------------------------------------------------- per-run index

class _Index:
    """Lookups shared by the rules of one run, so each expression is lexed and linted once."""

    def __init__(self, model: Dict[str, Any]):
        self.tables = _tables(model)
        self.columns = _all_columns(model)
        self.measures = _all_measures(model)
        self.relationships = _relationships(model)
        self.table_names: Set[str] = {_fold(t.get("name")) for t in self.tables if t.get("name")}
        self.columns_by_table: Dict[str, Set[str]] = {}
        self.column_names: Set[str] = set()
        for c in self.columns:
            if c.get("name") is None:
                continue
            self.columns_by_table.setdefault(_fold(c.get("table")), set()).add(_fold(c["name"]))
            self.column_names.add(_fold(c["name"]))
        self.measure_names: Set[str] = {_fold(m["name"]) for m in self.measures if m.get("name")}
        # (label, kind, expression) for everything that carries DAX
        self.expressions: List[Tuple[str, str, str]] = []
        for m in self.measures:
            if _dax(m):
                self.expressions.append((_label(m.get("table"), m.get("name")), "measure", _dax(m)))
        for c in self.columns:
            if _dax(c):
                self.expressions.append((_label(c.get("table"), c.get("name")), "column", _dax(c)))
        self.rel_pairs: Set[frozenset] = set()
        for r in self.relationships:
            self.rel_pairs.add(frozenset((
                (_fold(r.get("from_table")), _fold(r.get("from_column"))),
                (_fold(r.get("to_table")), _fold(r.get("to_column"))))))
        self._lint: Dict[str, List[Dict[str, Any]]] = {}
        self._lexed: Dict[str, Any] = {}
        self._refs: Dict[str, Dict[str, List[str]]] = {}

    def lint(self, expr: str) -> List[Dict[str, Any]]:
        if expr not in self._lint:
            self._lint[expr] = dax_lint.lint_expression(None, expr)
        return self._lint[expr]

    def lexed(self, expr: str):
        if expr not in self._lexed:
            self._lexed[expr] = dax_lint._Lexed(expr)
        return self._lexed[expr]

    def calls(self, expr: str) -> List[Tuple[str, int]]:
        """(FUNCTION NAME, position) of every call in the expression."""
        lx = self.lexed(expr)
        return [(lx.upper(p), p) for p in range(len(lx)) if lx.is_call(p)]

    def userelationship_pairs(self, expr: str) -> List[Tuple[frozenset, str]]:
        """Column pairs named by USERELATIONSHIP(a, b) calls, with a printable form."""
        lx = self.lexed(expr)
        out = []
        for fn, p in self.calls(expr):
            if fn != "USERELATIONSHIP":
                continue
            ends = []
            for a0, a1 in lx.args(p + 1):
                if (a1 - a0 == 2 and lx.sig[a0].kind in (dt.IDENT, dt.QNAME)
                        and lx.sig[a0 + 1].kind == dt.BRACKET):
                    ends.append((lx.sig[a0].name, lx.sig[a0 + 1].name))
            if len(ends) == 2:
                pair = frozenset((_fold(t), _fold(c)) for t, c in ends)
                out.append((pair, " and ".join(_label(t, c) for t, c in ends)))
        return out

    def references(self, expr: str) -> Dict[str, List[str]]:
        """Classify the name references of an expression against the model:
        broken (the object does not exist), unqualified column references, and
        table-qualified measure references."""
        if expr in self._refs:
            return self._refs[expr]
        lx = self.lexed(expr)
        tokens = lx.tokens
        prev_sig: Dict[int, Optional[dt.Token]] = {}
        last = None
        for i, t in enumerate(tokens):
            prev_sig[i] = last
            if t.kind not in (dt.WS, dt.COMMENT):
                last = t
        # Names an expression can introduce itself: "name" arguments of ADDCOLUMNS, SELECTCOLUMNS,
        # ROW, SUMMARIZE..., and the implicit columns of constructors and generators.
        own = {_fold(_string_value(t)) for t in tokens if t.kind == dt.STRING}
        fns = {fn for fn, _ in self.calls(expr)}
        has_constructor = any(t.kind == dt.OP and t.text == "{" for t in tokens)
        if has_constructor or "GENERATESERIES" in fns:
            own.add("value")
        if fns & {"CALENDAR", "CALENDARAUTO"}:
            own.add("date")
        # INFO.* and COLUMNSTATISTICS return tables whose columns are not model columns, so an
        # unqualified [Name] next to them cannot be judged.
        opaque = any(fn.startswith("INFO.") or fn == "COLUMNSTATISTICS" for fn in fns)
        broken: List[str] = []
        unqualified: List[str] = []
        qualified_measures: List[str] = []

        def add(bucket, text):
            if text not in bucket:
                bucket.append(text)

        for r in dt.iter_references(tokens):
            name = _fold(r.name)
            if r.kind == "column":
                tkey = _fold(r.table)
                if tkey not in self.table_names:
                    add(broken, f"table '{r.table}'")
                elif name in self.columns_by_table.get(tkey, ()):
                    continue
                elif name in self.measure_names:
                    add(qualified_measures, _label(r.table, r.name))
                elif self.columns_by_table.get(tkey):       # columns of this table are known
                    add(broken, _label(r.table, r.name))
            elif r.kind == "table":
                if tokens[r.index].kind == dt.QNAME and _fold(r.table) not in self.table_names:
                    add(broken, f"table '{r.table}'")
            else:
                before = prev_sig.get(r.index)
                if before is not None and before.kind == dt.OP and before.text == ".":
                    continue                                 # 'T'[Date].[Year]: a hierarchy level
                if name in self.measure_names:
                    continue
                if name in own or (has_constructor and re.fullmatch(r"value\d+", name)):
                    continue
                if opaque:
                    continue
                if name in self.column_names:
                    add(unqualified, f"[{r.name}]")
                else:
                    add(broken, f"[{r.name}]")
        self._refs[expr] = {"broken": broken, "unqualified": unqualified,
                            "qualified_measures": qualified_measures}
        return self._refs[expr]


_ACTIVE: Dict[int, _Index] = {}


def _index(model: Dict[str, Any]) -> _Index:
    """The index for ``model``: shared while run_bpa is running it, built on demand otherwise
    (a rule function called directly still works)."""
    return _ACTIVE.get(id(model)) or _Index(model)


# ---------------------------------------------------------------- rules: performance

def _rule_float_columns(model):
    hits = []
    for c in _index(model).columns:
        if _type_key(c.get("data_type")) in _FLOAT_TYPES:
            hits.append({"object": _label(c.get("table"), c.get("name")),
                         "detail": "Floating-point column; prefer Fixed Decimal (Currency) for exact "
                                   "aggregation and smaller size."})
    return hits


def _rule_calculated_columns(model):
    hits = []
    for c in _index(model).columns:
        if _is_calculated(c):
            hits.append({"object": _label(c.get("table"), c.get("name")),
                         "detail": "Calculated column; consider computing in Power Query / source for "
                                   "better compression and refresh."})
    return hits


def _rule_calc_column_aggregate(model):
    idx = _index(model)
    hits = []
    for c in idx.columns:
        expr = _dax(c)
        if not expr:
            continue
        lx = idx.lexed(expr)
        used = []
        for fn, p in idx.calls(expr):
            if fn in ("CALCULATE", "CALCULATETABLE", "RELATEDTABLE"):
                used.append(fn)
            elif fn in AGGREGATORS and not (fn in ("MIN", "MAX") and len(lx.args(p + 1)) == 2):
                used.append(fn)                              # MIN(a, b) / MAX(a, b) are scalar
        if used:
            names = ", ".join(sorted(set(used)))
            hits.append({"object": _label(c.get("table"), c.get("name")),
                         "detail": f"Calculated column aggregates data ({names}); the result is fixed at "
                                   "refresh and ignores slicers. If it should respond to filters, make "
                                   "it a measure."})
    return hits


def _rule_bidirectional(model):
    hits = []
    for r in _index(model).relationships:
        if _fold(r.get("cross_filter")) in ("both", "bothdirections", "2"):
            hits.append({"object": _rel_label(r),
                         "detail": "Bidirectional cross-filter; can cause ambiguity and slow queries. "
                                   "Use single direction + CROSSFILTER where needed."})
    return hits


def _is_many_to_many(r) -> bool:
    return _fold(r.get("from_cardinality")) == "many" and _fold(r.get("to_cardinality")) == "many"


def _rule_many_to_many(model):
    hits = []
    for r in _index(model).relationships:
        if _is_many_to_many(r):
            both = _fold(r.get("cross_filter")) in ("both", "bothdirections", "2")
            hits.append({"object": _rel_label(r),
                         "detail": "Many-to-many relationship"
                                   + (" with bidirectional filtering" if both else "")
                                   + "; totals can double count and the join is slow. Model the shared grain "
                                     "with a bridge table, or at least keep the filter single-direction."})
    return hits


def _rule_auto_date_time(model):
    names = [t.get("name") for t in _index(model).tables if _is_auto_date_table(t.get("name"))]
    if not names:
        return []
    return [{"object": "(model)",
             "detail": f"{len(names)} auto date/time table(s) (LocalDateTable_* / DateTableTemplate_*). "
                       "Turn off Auto date/time and relate facts to one marked date table; the hidden "
                       "tables bloat the model, one per date column."}]


def _rel_endpoint_types(idx: _Index):
    types = {(_fold(c.get("table")), _fold(c.get("name"))): _type_key(c.get("data_type"))
             for c in idx.columns}
    for r in idx.relationships:
        ends = [(r.get("from_table"), r.get("from_column")), (r.get("to_table"), r.get("to_column"))]
        yield r, [(t, c, types.get((_fold(t), _fold(c)), "")) for t, c in ends]


def _rule_rel_float_key(model):
    hits = []
    for r, ends in _rel_endpoint_types(_index(model)):
        bad = [_label(t, c) for t, c, ty in ends if ty in _FLOAT_TYPES]
        if bad:
            hits.append({"object": _rel_label(r),
                         "detail": f"Relationship on a floating-point column ({', '.join(bad)}); "
                                   "equal-looking values can differ in the last bits and silently fail to "
                                   "match. Use a whole-number key."})
    return hits


def _rule_rel_text_key(model):
    hits = []
    for r, ends in _rel_endpoint_types(_index(model)):
        bad = [_label(t, c) for t, c, ty in ends if ty in _TEXT_TYPES]
        if bad:
            hits.append({"object": _rel_label(r),
                         "detail": f"Relationship on a text column ({', '.join(bad)}); text keys compress "
                                   "and join worse than whole numbers. Prefer an integer surrogate key."})
    return hits


# ---------------------------------------------------------------- rules: DAX (shared with dax_lint)

def _lint_rule(lint_id: str, text: str):
    def check(model):
        idx = _index(model)
        hits = []
        for label, _kind, expr in idx.expressions:
            lines = [f["line"] for f in idx.lint(expr) if f["rule_id"] == lint_id]
            if lines:
                where = ", ".join(str(n) for n in lines[:5]) + (" ..." if len(lines) > 5 else "")
                plural = "s" if len(lines) > 1 else ""
                hits.append({"object": label,
                             "detail": f"{text} (dax_lint {lint_id}, line{plural} {where})"})
        return hits
    return check


_rule_divide = _lint_rule(
    "DL003", "Uses '/' division; use DIVIDE(numerator, denominator) to handle divide-by-zero safely.")
_rule_iferror = _lint_rule(
    "DL004", "Uses IFERROR; prefer DIVIDE / explicit error handling for performance and clarity.")
_rule_filter_whole_table = _lint_rule(
    "DL001", "FILTER over a whole table as a CALCULATE filter; filter the column instead.")
_rule_summarize_extension = _lint_rule(
    "DL007", "SUMMARIZE with an extension column; use ADDCOLUMNS(SUMMARIZE(...), ...) or SUMMARIZECOLUMNS.")
_rule_add_zero = _lint_rule(
    "DL005", "Adds 0 to force BLANK to zero; this defeats blank suppression in visuals.")


def _normalized_dax(idx: _Index, expr: str) -> str:
    lx = idx.lexed(expr)
    return " ".join(t.text.upper() if t.kind == dt.IDENT else t.text for t in lx.sig)


def _rule_duplicate_definition(model):
    idx = _index(model)
    groups: Dict[str, List[str]] = {}
    for label, kind, expr in idx.expressions:
        if kind == "measure":
            groups.setdefault(_normalized_dax(idx, expr), []).append(label)
    hits = []
    for labels in groups.values():
        if len(labels) > 1:
            hits.append({"object": labels[0],
                         "detail": "Same definition as " + ", ".join(labels[1:])
                                   + "; keep one measure and reference it from the others."})
    return hits


def _rule_direct_measure_reference(model):
    idx = _index(model)
    hits = []
    for label, kind, expr in idx.expressions:
        lx = idx.lexed(expr)
        if kind == "measure" and len(lx) == 1 and lx.sig[0].kind == dt.BRACKET \
                and _fold(lx.sig[0].name) in idx.measure_names:
            hits.append({"object": label,
                         "detail": f"Is only a reference to {lx.sig[0].text}; a duplicate measure under "
                                   "another name. Use the original, or give this one its own logic."})
    return hits


def _reference_rule(bucket: str, text: str):
    def check(model):
        idx = _index(model)
        if not idx.columns:
            return []                                        # no column metadata to check against
        hits = []
        for label, _kind, expr in idx.expressions:
            names = idx.references(expr)[bucket]
            if names:
                shown = ", ".join(names[:6]) + (" ..." if len(names) > 6 else "")
                hits.append({"object": label, "detail": text.format(names=shown)})
        return hits
    return check


_rule_unqualified_column = _reference_rule(
    "unqualified", "Column referenced without its table ({names}); write Table[Column] so it cannot be "
                   "confused with a measure.")
_rule_qualified_measure = _reference_rule(
    "qualified_measures", "Measure referenced with a table prefix ({names}); write [Measure] so it "
                          "survives being moved to another table.")


# ---------------------------------------------------------------- rules: error prevention

_rule_broken_reference = _reference_rule(
    "broken", "References an object that is not in the model ({names}); the expression cannot be "
              "evaluated. Fix the name or restore the object.")


def _rule_userelationship_missing(model):
    idx = _index(model)
    if not idx.relationships:
        return []                                            # relationship metadata unavailable
    hits = []
    for label, _kind, expr in idx.expressions:
        missing = [text for pair, text in idx.userelationship_pairs(expr) if pair not in idx.rel_pairs]
        if missing:
            hits.append({"object": label,
                         "detail": f"USERELATIONSHIP names columns with no relationship between them "
                                   f"({'; '.join(missing)}); the measure raises an error when evaluated."})
    return hits


def _rule_inactive_rel_unused(model):
    idx = _index(model)
    if not idx.expressions:
        return []                                            # expressions unavailable: cannot tell
    activated: Set[frozenset] = set()
    for _label_, _kind, expr in idx.expressions:
        activated.update(pair for pair, _ in idx.userelationship_pairs(expr))
    hits = []
    for r in idx.relationships:
        if r.get("is_active") is None or _truthy(r.get("is_active")):
            continue
        pair = frozenset(((_fold(r.get("from_table")), _fold(r.get("from_column"))),
                          (_fold(r.get("to_table")), _fold(r.get("to_column")))))
        if pair not in activated:
            hits.append({"object": _rel_label(r),
                         "detail": "Inactive relationship that no expression activates with USERELATIONSHIP; "
                                   "it never filters anything. Use it in a measure or remove it."})
    return hits


def _rule_measure_no_expression(model):
    idx = _index(model)
    # A live connection without model-admin rights returns every expression blank. Only when some
    # measures do have an expression is an empty one a defect rather than missing metadata.
    if not any(_dax(m) for m in idx.measures):
        return []
    return [{"object": _label(m.get("table"), m.get("name")),
             "detail": "Measure has no expression; it cannot be evaluated."}
            for m in idx.measures if not _dax(m)]


def _named_objects(idx: _Index):
    for t in idx.tables:
        yield t.get("name"), t.get("name")
    for m in idx.measures:
        yield _label(m.get("table"), m.get("name")), m.get("name")
    for c in idx.columns:
        yield _label(c.get("table"), c.get("name")), c.get("name")


def _rule_invalid_name_chars(model):
    hits = []
    for label, name in _named_objects(_index(model)):
        if isinstance(name, str) and _CONTROL_CHARS.search(name):
            hits.append({"object": repr(label),
                         "detail": "Name contains a control character (tab, line break or similar); it "
                                   "breaks scripts, deployment and DAX references. Remove it."})
    return hits


def _rule_rel_type_mismatch(model):
    hits = []
    for r, ends in _rel_endpoint_types(_index(model)):
        ft, tt = ends[0][2], ends[1][2]
        if ft and tt and ft != tt:
            hits.append({"object": _rel_label(r),
                         "detail": f"Relationship columns have different data types ({ft} vs {tt}); can "
                                   "cause errors or slow joins."})
    return hits


# ---------------------------------------------------------------- rules: naming / formatting

def _rule_trailing_space(model):
    hits = []
    for label, name in _named_objects(_index(model)):
        if isinstance(name, str) and name != name.strip():
            hits.append({"object": repr(label), "detail": "Name has leading/trailing whitespace; trim it."})
    return hits


def _rule_measure_no_format(model):
    hits = []
    for m in _index(model).measures:
        if _truthy(m.get("is_hidden")):
            continue
        if _empty(m.get("format_string")):
            hits.append({"object": _label(m.get("table"), m.get("name")),
                         "detail": "Visible measure has no format string; set one (e.g. '#,##0' or '0.0%')."})
    return hits


def _many_to_one(r) -> bool:
    """A regular fact-to-dimension relationship (cardinality missing counts as many-to-one)."""
    return _fold(r.get("from_cardinality")) in ("", "many") and _fold(r.get("to_cardinality")) in ("", "one")


def _rule_visible_foreign_key(model):
    idx = _index(model)
    cols = {(_fold(c.get("table")), _fold(c.get("name"))): c for c in idx.columns}
    hidden_tables = {_fold(t.get("name")) for t in idx.tables if _truthy(t.get("is_hidden"))}
    hits, seen = [], set()
    for r in idx.relationships:
        key = (_fold(r.get("from_table")), _fold(r.get("from_column")))
        c = cols.get(key)
        if c is None or key in seen or not _many_to_one(r) or key[0] in hidden_tables:
            continue
        seen.add(key)
        if c.get("is_hidden") is not None and not _truthy(c.get("is_hidden")):
            hits.append({"object": _label(c.get("table"), c.get("name")),
                         "detail": f"Foreign key is visible; hide it so report authors use the "
                                   f"{r.get('to_table')} dimension's column instead."})
    return hits


def _rule_key_summarized(model):
    idx = _index(model)
    endpoints = set()
    for r in idx.relationships:
        endpoints.add((_fold(r.get("from_table")), _fold(r.get("from_column"))))
        endpoints.add((_fold(r.get("to_table")), _fold(r.get("to_column"))))
    hits = []
    for c in idx.columns:
        if _empty(c.get("summarize_by")) or _fold(c.get("summarize_by")) == "none":
            continue
        is_key = _truthy(c.get("is_key")) or (_fold(c.get("table")), _fold(c.get("name"))) in endpoints
        if is_key and _type_key(c.get("data_type")) in _NUMERIC_TYPES and not _truthy(c.get("is_hidden")):
            hits.append({"object": _label(c.get("table"), c.get("name")),
                         "detail": f"Key column summarizes by {c.get('summarize_by')}; dropping it in a "
                                   "visual adds up key values. Set Summarize by to None."})
    return hits


def _rule_month_not_sorted(model):
    hits = []
    for c in _index(model).columns:
        if "sort_by" not in c:
            continue                                         # sort metadata was not gathered
        name = _fold(c.get("name"))
        if ("month" in name and "months" not in name and _type_key(c.get("data_type")) in _TEXT_TYPES
                and _empty(c.get("sort_by")) and not _truthy(c.get("is_hidden"))):
            hits.append({"object": _label(c.get("table"), c.get("name")),
                         "detail": "Text month column has no sort-by column; it sorts alphabetically "
                                   "(April, August, ...). Sort it by a month-number column."})
    return hits


# ---------------------------------------------------------------- rules: maintenance

def _rule_measure_no_description(model):
    hits = []
    for m in _index(model).measures:
        if _truthy(m.get("is_hidden")):
            continue
        if _empty(m.get("description")):
            hits.append({"object": _label(m.get("table"), m.get("name")),
                         "detail": "Visible measure has no description; descriptions improve maintainability "
                                   "and Copilot/agent accuracy."})
    return hits


def _rule_column_no_description(model):
    hits = []
    for c in _index(model).columns:
        if _truthy(c.get("is_hidden")):
            continue
        if _empty(c.get("description")):
            hits.append({"object": _label(c.get("table"), c.get("name")),
                         "detail": "Visible column has no description (lowers AI-readiness)."})
    return hits


def _user_columns(t: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [c for c in _children(t, "columns") if not _is_internal_column(c.get("name"))]


def _rule_table_no_relationship(model):
    idx = _index(model)
    used = set()
    for r in idx.relationships:
        used.add(_fold(r.get("from_table")))
        used.add(_fold(r.get("to_table")))
    # Tables that are meant to stand alone: measure tables and single-column parameter tables
    # (at most one real column), hidden helpers, and the engine's own date tables.
    candidates = [t for t in idx.tables
                  if len(_user_columns(t)) > 1 and not _truthy(t.get("is_hidden"))
                  and not _is_auto_date_table(t.get("name"))]
    if len(candidates) < 2 and not idx.relationships:
        return []                                            # a one-table model has nothing to relate
    return [{"object": t.get("name"),
             "detail": "Table participates in no relationship; verify it is intentionally disconnected."}
            for t in candidates if _fold(t.get("name")) not in used]


def _rule_measure_no_folder(model, threshold: int = 10):
    hits = []
    for t in _index(model).tables:
        visible = [m for m in _children(t, "measures") if not _truthy(m.get("is_hidden"))]
        loose = [m for m in visible if _empty(m.get("display_folder"))]
        if len(visible) > threshold and loose:
            hits.append({"object": t.get("name"),
                         "detail": f"{len(loose)} of {len(visible)} visible measures have no display folder; "
                                   "group them so the field list stays navigable."})
    return hits


def _rule_date_table_not_marked(model):
    idx = _index(model)
    # 'Marked as date table' shows as a key flag on the date column. Without any key metadata in
    # the model the answer is unknown, so the rule stays silent.
    if not any(c.get("is_key") is not None for c in idx.columns):
        return []
    one_side = {_fold(r.get("to_table")) for r in idx.relationships}
    hits = []
    for t in idx.tables:
        name = _fold(t.get("name"))
        if _is_auto_date_table(t.get("name")) or not _has_date_word(t.get("name")):
            continue
        date_cols = [c for c in _user_columns(t) if "date" in _type_key(c.get("data_type"))]
        if not date_cols or name not in one_side:
            continue
        if _fold(t.get("data_category")) == "time" or any(_truthy(c.get("is_key")) for c in date_cols):
            continue
        hits.append({"object": t.get("name"),
                     "detail": "Looks like the date dimension but is not marked as a date table; time "
                               "intelligence functions are only reliable on a marked date table."})
    return hits


# Rule registry: id, category, severity, name, check
DEFAULT_BPA_RULES = [
    {"id": "PERF_FLOAT_COLUMN", "category": "Performance", "severity": "warning",
     "name": "Avoid floating-point data types", "check": _rule_float_columns},
    {"id": "PERF_CALC_COLUMN", "category": "Performance", "severity": "info",
     "name": "Reduce use of calculated columns", "check": _rule_calculated_columns},
    {"id": "PERF_CALC_COLUMN_AGGREGATE", "category": "Performance", "severity": "warning",
     "name": "Calculated columns that aggregate should be measures", "check": _rule_calc_column_aggregate},
    {"id": "PERF_BIDIRECTIONAL", "category": "Performance", "severity": "warning",
     "name": "Avoid bidirectional relationships", "check": _rule_bidirectional},
    {"id": "PERF_MANY_TO_MANY", "category": "Performance", "severity": "warning",
     "name": "Avoid many-to-many relationships", "check": _rule_many_to_many},
    {"id": "PERF_AUTO_DATE_TIME", "category": "Performance", "severity": "warning",
     "name": "Remove auto date/time tables", "check": _rule_auto_date_time},
    {"id": "PERF_REL_FLOAT_KEY", "category": "Performance", "severity": "warning",
     "name": "Relationship keys should not be floating-point", "check": _rule_rel_float_key},
    {"id": "PERF_REL_TEXT_KEY", "category": "Performance", "severity": "info",
     "name": "Prefer whole-number relationship keys over text", "check": _rule_rel_text_key},
    {"id": "DAX_USE_DIVIDE", "category": "DAX", "severity": "warning",
     "name": "Use DIVIDE for division", "check": _rule_divide},
    {"id": "DAX_AVOID_IFERROR", "category": "DAX", "severity": "warning",
     "name": "Avoid IFERROR", "check": _rule_iferror},
    {"id": "DAX_FILTER_WHOLE_TABLE", "category": "DAX", "severity": "warning",
     "name": "Do not FILTER a whole table inside CALCULATE", "check": _rule_filter_whole_table},
    {"id": "DAX_SUMMARIZE_EXTENSION", "category": "DAX", "severity": "warning",
     "name": "Do not add columns inside SUMMARIZE", "check": _rule_summarize_extension},
    {"id": "DAX_AVOID_ADD_ZERO", "category": "DAX", "severity": "info",
     "name": "Avoid adding 0 to a measure", "check": _rule_add_zero},
    {"id": "DAX_DUPLICATE_DEFINITION", "category": "DAX", "severity": "warning",
     "name": "No two measures should have the same definition", "check": _rule_duplicate_definition},
    {"id": "DAX_DIRECT_MEASURE_REFERENCE", "category": "DAX", "severity": "warning",
     "name": "Measures should not be direct references of other measures",
     "check": _rule_direct_measure_reference},
    {"id": "DAX_UNQUALIFIED_COLUMN", "category": "DAX", "severity": "warning",
     "name": "Column references should be fully qualified", "check": _rule_unqualified_column},
    {"id": "DAX_QUALIFIED_MEASURE", "category": "DAX", "severity": "warning",
     "name": "Measure references should be unqualified", "check": _rule_qualified_measure},
    {"id": "ERR_BROKEN_REFERENCE", "category": "Error Prevention", "severity": "error",
     "name": "DAX must not reference objects that do not exist", "check": _rule_broken_reference},
    {"id": "ERR_USERELATIONSHIP_MISSING", "category": "Error Prevention", "severity": "error",
     "name": "USERELATIONSHIP must name an existing relationship", "check": _rule_userelationship_missing},
    {"id": "ERR_MEASURE_NO_EXPRESSION", "category": "Error Prevention", "severity": "error",
     "name": "Measures must have an expression", "check": _rule_measure_no_expression},
    {"id": "ERR_INVALID_NAME_CHARS", "category": "Error Prevention", "severity": "error",
     "name": "No control characters in names", "check": _rule_invalid_name_chars},
    {"id": "ERR_REL_TYPE_MISMATCH", "category": "Error Prevention", "severity": "warning",
     "name": "Relationship columns should share a data type", "check": _rule_rel_type_mismatch},
    {"id": "ERR_INACTIVE_REL_UNUSED", "category": "Error Prevention", "severity": "warning",
     "name": "Inactive relationships should be activated somewhere", "check": _rule_inactive_rel_unused},
    {"id": "NAMING_TRAILING_SPACE", "category": "Naming", "severity": "error",
     "name": "No leading/trailing spaces in names", "check": _rule_trailing_space},
    {"id": "FORMAT_MEASURE_NO_FORMAT", "category": "Formatting", "severity": "warning",
     "name": "Measures should have a format string", "check": _rule_measure_no_format},
    {"id": "FORMAT_VISIBLE_FOREIGN_KEY", "category": "Formatting", "severity": "warning",
     "name": "Hide foreign keys", "check": _rule_visible_foreign_key},
    {"id": "FORMAT_KEY_SUMMARIZED", "category": "Formatting", "severity": "warning",
     "name": "Do not summarize key columns", "check": _rule_key_summarized},
    {"id": "FORMAT_MONTH_NOT_SORTED", "category": "Formatting", "severity": "warning",
     "name": "Month name columns must be sorted by a number", "check": _rule_month_not_sorted},
    {"id": "MAINT_MEASURE_NO_DESC", "category": "Maintenance", "severity": "info",
     "name": "Measures should have descriptions", "check": _rule_measure_no_description},
    {"id": "MAINT_COLUMN_NO_DESC", "category": "Maintenance", "severity": "info",
     "name": "Visible columns should have descriptions", "check": _rule_column_no_description},
    {"id": "MAINT_TABLE_NO_REL", "category": "Maintenance", "severity": "warning",
     "name": "Tables should participate in a relationship", "check": _rule_table_no_relationship},
    {"id": "MAINT_MEASURE_NO_FOLDER", "category": "Maintenance", "severity": "info",
     "name": "Organize measures into display folders", "check": _rule_measure_no_folder},
    {"id": "MAINT_DATE_TABLE_NOT_MARKED", "category": "Maintenance", "severity": "warning",
     "name": "Date tables should be marked as a date table", "check": _rule_date_table_not_marked},
]


def run_bpa(model: Dict[str, Any], rules=None, categories=None, min_severity="info") -> Dict[str, Any]:
    """Run the Best Practice Analyzer over a normalized model dict.

    Returns {"summary": {...}, "findings": [ {rule_id, name, category, severity, object, detail} ]}.
    summary.rules_run counts the rules that actually ran after the category and severity filters.
    """
    rules = DEFAULT_BPA_RULES if rules is None else rules
    min_rank = SEVERITY_ORDER.get(str(min_severity or "info").lower(), 1)
    cat_filter = set(str(c).lower() for c in categories) if categories else None

    findings = []
    rule_errors = []
    rules_run = 0
    _ACTIVE[id(model)] = _Index(model)
    try:
        for rule in rules:
            if cat_filter and rule["category"].lower() not in cat_filter:
                continue
            if SEVERITY_ORDER.get(rule["severity"], 1) < min_rank:
                continue
            rules_run += 1
            try:
                for hit in rule["check"](model):
                    findings.append({
                        "rule_id": rule["id"],
                        "name": rule["name"],
                        "category": rule["category"],
                        "severity": rule["severity"],
                        "object": hit.get("object"),
                        "detail": hit.get("detail"),
                    })
            except Exception as e:  # a malformed rule must not break the whole scan
                rule_errors.append({"rule_id": rule["id"], "error": str(e)})
                if min_rank <= SEVERITY_ORDER["info"]:
                    findings.append({"rule_id": rule["id"], "name": rule["name"],
                                     "category": rule["category"], "severity": "info",
                                     "object": "(rule error)", "detail": str(e)})
    finally:
        _ACTIVE.pop(id(model), None)

    findings.sort(key=lambda f: (-SEVERITY_ORDER.get(f["severity"], 1), f["category"], f["rule_id"]))
    by_sev = {"error": 0, "warning": 0, "info": 0}
    by_cat: Dict[str, int] = {}
    for f in findings:
        by_sev[f["severity"]] = by_sev.get(f["severity"], 0) + 1
        by_cat[f["category"]] = by_cat.get(f["category"], 0) + 1
    return {
        "summary": {"total": len(findings), "by_severity": by_sev, "by_category": by_cat,
                    "rules_run": rules_run, "rule_errors": rule_errors},
        "findings": findings,
    }


# ---------------------------------------------------------------- AI-readiness

def audit_ai_readiness(model: Dict[str, Any]) -> Dict[str, Any]:
    """Score how 'AI-ready' a model is (descriptions, formats, hidden technical columns).

    Better-described, well-formatted models produce better Copilot / data-agent / LLM output.
    Returns a 0-100 score, component metrics, and concrete recommendations.

    Only visible user objects are assessed. A component with nothing to assess (a model without
    measures, say) is left out and the remaining weights are rescaled, so it earns no free
    points; a model with no visible tables scores 0.
    """
    tables = [t for t in _tables(model)
              if not _truthy(t.get("is_hidden")) and not _is_auto_date_table(t.get("name"))]
    visible_measures = [m for m in _all_measures({"tables": tables}) if not _truthy(m.get("is_hidden"))]
    visible_columns = [c for c in _all_columns({"tables": tables}) if not _truthy(c.get("is_hidden"))]

    def pct(num, den):
        return round(100.0 * num / den, 1) if den else 0.0

    m_desc = sum(1 for m in visible_measures if not _empty(m.get("description")))
    m_fmt = sum(1 for m in visible_measures if not _empty(m.get("format_string")))
    c_desc = sum(1 for c in visible_columns if not _empty(c.get("description")))
    t_desc = sum(1 for t in tables if not _empty(t.get("description")))

    metrics = {
        "measures_total": len(visible_measures),
        "measures_with_description_pct": pct(m_desc, len(visible_measures)),
        "measures_with_format_pct": pct(m_fmt, len(visible_measures)),
        "visible_columns_total": len(visible_columns),
        "columns_with_description_pct": pct(c_desc, len(visible_columns)),
        "tables_total": len(tables),
        "tables_with_description_pct": pct(t_desc, len(tables)),
    }

    # Weighted score: measure descriptions/formats matter most for NL-to-DAX grounding.
    components = [
        (0.30, metrics["measures_with_description_pct"], len(visible_measures)),
        (0.20, metrics["measures_with_format_pct"], len(visible_measures)),
        (0.30, metrics["columns_with_description_pct"], len(visible_columns)),
        (0.20, metrics["tables_with_description_pct"], len(tables)),
    ]
    weight = sum(w for w, _, n in components if n)
    score = round(sum(w * p for w, p, n in components if n) / weight, 1) if weight else 0.0

    recs = []
    if not tables:
        recs.append("The model has no visible tables; there is nothing for an agent to query.")
    else:
        if not visible_measures:
            recs.append("Add explicit measures; agents answer far more reliably from named, described "
                        "measures than from implicit column aggregation.")
        if metrics["measures_with_description_pct"] < 90 and visible_measures:
            recs.append(f"Add descriptions to measures ({m_desc}/{len(visible_measures)} done) - the "
                        "strongest lever for NL-to-DAX accuracy.")
        if metrics["measures_with_format_pct"] < 90 and visible_measures:
            recs.append(f"Set format strings on measures ({m_fmt}/{len(visible_measures)} done).")
        if metrics["columns_with_description_pct"] < 70 and visible_columns:
            recs.append(f"Describe visible columns ({c_desc}/{len(visible_columns)} done), especially keys "
                        "and ambiguous names.")
        if metrics["tables_with_description_pct"] < 80:
            recs.append(f"Describe tables ({t_desc}/{len(tables)} done) so agents pick the right "
                        "fact/dimension.")
        if not recs:
            recs.append("Model is well documented. Consider adding synonyms / verified answers for Copilot.")

    grade = ("A" if score >= 90 else "B" if score >= 75 else "C" if score >= 60
             else "D" if score >= 40 else "F")
    return {"score": score, "grade": grade, "metrics": metrics, "recommendations": recs}


# ---------------------------------------------------------------- DAX test verdict

_DEFAULT_REL_TOL = 1e-9     # absorbs binary floating-point noise (0.1 + 0.2 vs 0.3)
_DEFAULT_ABS_TOL = 1e-12


def _as_number(v):
    """int / float / Decimal for a number or a numeric string; None for anything else."""
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float, Decimal)):
        return v
    if isinstance(v, str) and v.strip():
        try:
            return Decimal(v.strip())
        except InvalidOperation:
            return None
    return None


def _as_bool(v):
    if isinstance(v, bool):
        return v
    if isinstance(v, str) and v.strip().lower() in ("true", "false"):
        return v.strip().lower() == "true"
    return None


def _as_datetime(v):
    if isinstance(v, datetime):
        return v.replace(tzinfo=None)
    if isinstance(v, date):
        return datetime(v.year, v.month, v.day)
    if isinstance(v, str):
        try:
            return datetime.fromisoformat(v.strip().replace("Z", "+00:00")).replace(tzinfo=None)
        except ValueError:
            return None
    return None


def _tolerance(value, label: str) -> float:
    if value is None or value == "":
        return 0.0
    num = None if isinstance(value, bool) else _as_number(value)
    if num is None:
        raise ValueError(f"invalid {label} {value!r}: must be a number")
    num = float(num)
    if math.isnan(num) or math.isinf(num) or num < 0:
        raise ValueError(f"invalid {label} {value!r}: must be a finite number >= 0")
    return num


def _numbers_match(a, e, abs_tol: float, rel_tol: float) -> bool:
    fa, fe = float(a), float(e)
    if math.isnan(fa) or math.isnan(fe):
        return math.isnan(fa) and math.isnan(fe)
    if math.isinf(fa) or math.isinf(fe):
        return fa == fe
    if not isinstance(a, float) and not isinstance(e, float):
        # Integers and decimals are exact: compare them exactly, without a float round trip.
        allowed = max(Decimal(repr(abs_tol)), Decimal(repr(rel_tol)) * abs(Decimal(e)))
        return abs(Decimal(a) - Decimal(e)) <= allowed
    if abs_tol == 0 and rel_tol == 0:
        return abs(fa - fe) <= max(_DEFAULT_ABS_TOL, _DEFAULT_REL_TOL * max(abs(fa), abs(fe)))
    return abs(fa - fe) <= max(abs_tol, rel_tol * abs(fe))


def dax_test_verdict(actual, expected, tolerance=0, rel_tolerance=None):
    """Compare a DAX test's actual scalar result to its expected value.

    Returns (passed: bool, detail: str). Never raises: a bad tolerance fails the test with a
    detail that says so.

    - BLANK (None) only equals BLANK.
    - Booleans compare as booleans (True/False, or the strings "true"/"false"); a number or any
      other text is not a boolean.
    - Numbers (int, float, Decimal, numeric strings when the other side is a number) compare by
      value: within ``tolerance`` (absolute) or ``rel_tolerance`` (fraction of the expected
      value), whichever allows more. With no tolerance, integers and decimals must be equal and
      floating-point values must agree to about nine significant digits.
    - Dates compare as dates when either side is a date; an ISO string is accepted.
    - Anything else compares as text.
    """
    try:
        abs_tol = _tolerance(tolerance, "tolerance")
        rel_tol = _tolerance(rel_tolerance, "rel_tolerance")
    except ValueError as e:
        return False, str(e)

    if actual is None or expected is None:
        return actual is None and expected is None, f"{_show(actual)} vs expected {_show(expected)}"

    if isinstance(expected, bool) or isinstance(actual, bool):
        ba, be = _as_bool(actual), _as_bool(expected)
        if ba is None or be is None:
            return False, f"{actual!r} vs expected {expected!r} (a boolean only equals a boolean)"
        return ba == be, f"{actual!r} vs expected {expected!r}"

    if isinstance(actual, (date, datetime)) or isinstance(expected, (date, datetime)):
        da, de = _as_datetime(actual), _as_datetime(expected)
        if da is not None and de is not None:
            return da == de, f"{da.isoformat()} vs expected {de.isoformat()}"

    na, ne = _as_number(actual), _as_number(expected)
    genuine = any(isinstance(v, (int, float, Decimal)) and not isinstance(v, bool)
                  for v in (actual, expected))
    if genuine and na is not None and ne is not None:
        note = ""
        if abs_tol or rel_tol:
            parts = ([f"±{abs_tol:g}"] if abs_tol else []) + ([f"±{rel_tol:g} relative"] if rel_tol else [])
            note = f" ({', '.join(parts)})"
        try:
            return _numbers_match(na, ne, abs_tol, rel_tol), f"{actual} vs {expected}{note}"
        except (ValueError, ArithmeticError):
            pass                                             # e.g. a signaling NaN: compare as text

    return str(actual) == str(expected), f"{actual!r} vs expected {expected!r}"


def _show(v) -> str:
    return "BLANK" if v is None else repr(v)


# ---------------------------------------------------------------- model diff

# (key, label, kind). Kinds decide how two values are compared, so that None, "" and a missing
# key are one value, booleans arrive as bool or "True"/"1", and enum-like text ignores case.
_TABLE_PROPS = [("is_hidden", "hidden", "bool"), ("description", "description", "text")]
_COLUMN_PROPS = [("data_type", "data type", "enum"), ("is_hidden", "hidden", "bool"),
                 ("expression", "expression", "dax"), ("sort_by", "sort-by column", "name"),
                 ("format_string", "format", "text"), ("display_folder", "display folder", "text"),
                 ("description", "description", "text"), ("summarize_by", "summarize by", "enum"),
                 ("data_category", "data category", "enum"), ("is_key", "key", "bool")]
_MEASURE_PROPS = [("expression", "expression", "dax"), ("format_string", "format", "text"),
                  ("display_folder", "display folder", "text"), ("description", "description", "text"),
                  ("is_hidden", "hidden", "bool"), ("data_type", "data type", "enum")]
_REL_PROPS = [("is_active", "active", "active"), ("cross_filter", "cross filter", "enum"),
              ("from_cardinality", "from cardinality", "enum"), ("to_cardinality", "to cardinality", "enum")]


def _norm(value, kind: str):
    if kind == "bool":
        return _truthy(value)
    if kind == "active":
        return True if value is None else _truthy(value)
    if kind in ("enum", "name"):
        return re.sub(r"\s+", " ", str(value or "")).strip().casefold()
    text = str(value or "").replace("\r\n", "\n").replace("\r", "\n")
    return text.strip()


def _display(value, kind: str) -> str:
    if kind in ("bool", "active"):
        if kind == "active" and value is None:
            return "yes"
        return "yes" if _truthy(value) else "no"
    text = re.sub(r"\s+", " ", str(value if value is not None else "")).strip()
    if not text:
        return "(none)"
    return "`" + (text if len(text) <= 60 else text[:57] + "...") + "`"


def _same_tokens(a: str, b: str) -> bool:
    """Do two expressions differ only in whitespace and comments?"""
    ta = [t.text for t in dt.tokenize(a) if t.kind not in (dt.WS, dt.COMMENT)]
    tb = [t.text for t in dt.tokenize(b) if t.kind not in (dt.WS, dt.COMMENT)]
    return ta == tb


def _prop_changes(before: Dict[str, Any], after: Dict[str, Any], props,
                  before_overrides: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """Property-level differences. A property is compared only when both sides carry the key:
    a snapshot written before a field was gathered must not read as 'everything changed'.
    ``before_overrides`` substitutes a before-value already adjusted for renames."""
    out = []
    for key, label, kind in props:
        if key not in before or key not in after:
            continue
        b_val = (before_overrides or {}).get(key, before.get(key))
        a_val = after.get(key)
        nb, na = _norm(b_val, kind), _norm(a_val, kind)
        if nb == na:
            continue
        if kind == "dax":
            only_format = _same_tokens(nb, na)
            text = ("expression reformatted (whitespace/comments only)" if only_format
                    else "expression changed")
            out.append({"property": key, "label": label, "before": before.get(key), "after": a_val,
                        "text": text})
        else:
            out.append({"property": key, "label": label, "before": before.get(key), "after": a_val,
                        "text": f"{label} {_display(before.get(key), kind)} -> {_display(a_val, kind)}"})
    return out


def _signature(obj: Dict[str, Any], props, overrides: Optional[Dict[str, Any]] = None) -> tuple:
    return tuple(_norm((overrides or {}).get(k, obj.get(k)), kind) for k, _, kind in props)


def _unique_matches(removed: Dict[str, Any], added: Dict[str, Any]) -> Dict[str, str]:
    """Pair removed and added names whose signatures are equal, but only when the signature
    identifies exactly one object on each side: an ambiguous match is not a rename."""
    by_sig_removed: Dict[Any, List[str]] = {}
    by_sig_added: Dict[Any, List[str]] = {}
    for name, sig in removed.items():
        by_sig_removed.setdefault(sig, []).append(name)
    for name, sig in added.items():
        by_sig_added.setdefault(sig, []).append(name)
    return {olds[0]: by_sig_added[sig][0] for sig, olds in by_sig_removed.items()
            if sig is not None and len(olds) == 1 and len(by_sig_added.get(sig, [])) == 1}


def _named(items) -> Dict[str, Dict[str, Any]]:
    return {x["name"]: x for x in (items or [])
            if isinstance(x, dict) and isinstance(x.get("name"), str) and x["name"] != ""
            and not _is_internal_column(x["name"])}


def _sorted(names) -> List[str]:
    return sorted(names, key=lambda s: (s.casefold(), s))


def diff_models(before: Dict[str, Any], after: Dict[str, Any]) -> Dict[str, Any]:
    """Compute a human-readable semantic diff between two normalized model dicts.

    Returns {"has_changes", "total_changes", "summary" (dict of counts), "changes", "markdown"}.
    Pure function - used for PR/CI 'what changed' summaries and pre-deploy review.

    Compared: tables (hidden, description), columns (type, hidden, expression, sort-by, format,
    display folder, description, summarize-by, data category, key), measures (home table,
    expression, format, display folder, description, hidden, data type) and relationships
    (active, cross filter, cardinalities).

    Renames: a removed and an added object with the same definition are reported as one rename
    when the match is unique (tables: same columns and measures; columns: same properties;
    measures: same expression). References to renamed objects are followed before expressions
    are compared, so a rename does not also report every expression that mentions it.
    Output order is deterministic: by object name, case-insensitively.
    """
    tb, ta = _named(_tables(before)), _named(_tables(after))

    # --- tables: added / removed / renamed
    def table_signature(t):
        cols = tuple(sorted((_fold(n), _norm(c.get("data_type"), "enum"))
                            for n, c in _named(t.get("columns")).items()))
        meas = tuple(sorted(_fold(n) for n in _named(t.get("measures"))))
        return (cols, meas) if (cols or meas) else None

    removed_t, added_t = set(tb) - set(ta), set(ta) - set(tb)
    table_renames = _unique_matches({n: table_signature(tb[n]) for n in removed_t},
                                    {n: table_signature(ta[n]) for n in added_t})
    removed_t -= set(table_renames)
    added_t -= set(table_renames.values())
    table_pairs = [(n, n) for n in set(tb) & set(ta)] + list(table_renames.items())
    table_pairs.sort(key=lambda p: (p[1].casefold(), p[1]))

    # --- columns: added / removed / renamed (per table)
    column_renames: Dict[Tuple[str, str], str] = {}          # (new table, old column) -> new column
    col_added, col_removed = [], []
    for old_t, new_t in table_pairs:
        cb, ca = _named(tb[old_t].get("columns")), _named(ta[new_t].get("columns"))
        gone, new = set(cb) - set(ca), set(ca) - set(cb)
        pairs = _unique_matches({n: _signature(cb[n], _COLUMN_PROPS) for n in gone},
                                {n: _signature(ca[n], _COLUMN_PROPS) for n in new})
        for old_c, new_c in pairs.items():
            column_renames[(new_t, old_c)] = new_c
        col_removed += [_label(new_t, c) for c in gone - set(pairs)]
        col_added += [_label(new_t, c) for c in new - set(pairs.values())]

    def follow(expr, measure_renames):
        """A before-expression with the detected renames applied, for comparison only."""
        text = str(expr or "")
        if not text:
            return text
        for old, new in table_renames.items():
            text = dt.rename_table(text, old, new)[0]
        for (table, old), new in column_renames.items():
            text = dt.rename_column(text, table, old, new)[0]
        for old, new in measure_renames.items():
            text = dt.rename_measure(text, old, new)[0]
        return text

    # --- measures are model-wide objects: match them by name across tables
    def all_measures(tables):
        out = {}
        for t_name, t in tables.items():
            for n, m in _named(t.get("measures")).items():
                out[n] = (t_name, m)
        return out

    mb, ma = all_measures(tb), all_measures(ta)
    removed_m, added_m = set(mb) - set(ma), set(ma) - set(mb)
    measure_renames: Dict[str, str] = {}
    for _ in range(5):                                       # a rename can unlock another one
        def msig(expr):
            norm = _norm(expr, "dax")
            return norm or None                              # no expression: nothing to match on
        found = _unique_matches(
            {n: msig(follow(mb[n][1].get("expression"), measure_renames)) for n in removed_m},
            {n: msig(ma[n][1].get("expression")) for n in added_m})
        if not found:
            break
        measure_renames.update(found)
        removed_m -= set(found)
        added_m -= set(found.values())

    changes: List[Dict[str, Any]] = []

    def record(kind, change, obj, details=None, **extra):
        changes.append(dict({"kind": kind, "change": change, "object": obj,
                             "details": [d["text"] for d in (details or [])],
                             "properties": {d["property"]: {"before": d["before"], "after": d["after"]}
                                            for d in (details or [])}}, **extra))

    for n in _sorted(added_t):
        record("table", "added", n)
    for n in _sorted(removed_t):
        record("table", "removed", n)
    for old, new in sorted(table_renames.items(), key=lambda p: (p[1].casefold(), p[1])):
        record("table", "renamed", new, before_name=old)

    for old_t, new_t in table_pairs:
        details = _prop_changes(tb[old_t], ta[new_t], _TABLE_PROPS)
        if details:
            record("table", "changed", new_t, details)

    for label in _sorted(col_added):
        record("column", "added", label)
    for label in _sorted(col_removed):
        record("column", "removed", label)
    for (table, old), new in sorted(column_renames.items(),
                                    key=lambda p: (_label(p[0][0], p[1]).casefold(), p[0][1])):
        record("column", "renamed", _label(table, new), before_name=_label(table, old))

    for old_t, new_t in table_pairs:
        cb, ca = _named(tb[old_t].get("columns")), _named(ta[new_t].get("columns"))
        col_pairs = [(c, c) for c in set(cb) & set(ca)]
        col_pairs += [(old, new) for (t, old), new in column_renames.items() if t == new_t]
        for old_c, new_c in sorted(col_pairs, key=lambda p: (p[1].casefold(), p[1])):
            overrides = {"expression": follow(cb[old_c].get("expression"), measure_renames)}
            sort_by = cb[old_c].get("sort_by")
            if isinstance(sort_by, str) and (new_t, sort_by) in column_renames:
                overrides["sort_by"] = column_renames[(new_t, sort_by)]
            details = _prop_changes(cb[old_c], ca[new_c], _COLUMN_PROPS, overrides)
            if details:
                record("column", "changed", _label(new_t, new_c), details)

    for n in _sorted(added_m):
        record("measure", "added", _label(ma[n][0], n))
    for n in _sorted(removed_m):
        record("measure", "removed", _label(mb[n][0], n))
    for old, new in sorted(measure_renames.items(), key=lambda p: (p[1].casefold(), p[1])):
        record("measure", "renamed", _label(ma[new][0], new), before_name=_label(mb[old][0], old))

    measure_pairs = [(n, n) for n in set(mb) & set(ma)] + list(measure_renames.items())
    for old, new in sorted(measure_pairs, key=lambda p: (p[1].casefold(), p[1])):
        (bt, bm), (at, am) = mb[old], ma[new]
        overrides = {"expression": follow(bm.get("expression"), measure_renames)}
        details = _prop_changes(bm, am, _MEASURE_PROPS, overrides)
        if table_renames.get(bt, bt) != at:
            details.insert(0, {"property": "table", "label": "home table", "before": bt, "after": at,
                               "text": f"moved from table `{bt}` to `{at}`"})
        if details:
            record("measure", "changed", _label(at, new), details)

    # --- relationships, keyed by their endpoints as they are named after the renames
    def rel_key(r, renamed: bool):
        ends = []
        for t_key, c_key in (("from_table", "from_column"), ("to_table", "to_column")):
            t, c = r.get(t_key), r.get(c_key)
            if renamed:
                t = table_renames.get(t, t)
                c = column_renames.get((t, c), c)
            ends.append((_fold(t), _fold(c)))
        return tuple(ends)

    rb = {rel_key(r, True): r for r in _relationships(before)}
    ra = {rel_key(r, False): r for r in _relationships(after)}
    for key in sorted(set(ra) - set(rb)):
        record("relationship", "added", _rel_label(ra[key]))
    for key in sorted(set(rb) - set(ra)):
        record("relationship", "removed", _rel_label(rb[key]))
    for key in sorted(set(rb) & set(ra)):
        details = _prop_changes(rb[key], ra[key], _REL_PROPS)
        if details:
            record("relationship", "changed", _rel_label(ra[key]), details)

    def n(kind, change):
        return sum(1 for c in changes if c["kind"] == kind and c["change"] == change)

    summary = {
        "tables_added": n("table", "added"), "tables_removed": n("table", "removed"),
        "tables_renamed": n("table", "renamed"), "tables_changed": n("table", "changed"),
        "columns_added": n("column", "added"), "columns_removed": n("column", "removed"),
        "columns_renamed": n("column", "renamed"), "columns_changed": n("column", "changed"),
        "measures_added": n("measure", "added"), "measures_removed": n("measure", "removed"),
        "measures_renamed": n("measure", "renamed"), "measures_changed": n("measure", "changed"),
        "relationships_added": n("relationship", "added"),
        "relationships_removed": n("relationship", "removed"),
        "relationships_changed": n("relationship", "changed"),
    }
    total = sum(summary.values())

    lines = ["# Model Diff", ""]
    if total == 0:
        lines.append("No semantic changes detected.")
    else:
        lines.append(f"**{total} change(s)**")
        lines.append("")
        titles = {"table": "Tables", "column": "Columns", "measure": "Measures",
                  "relationship": "Relationships"}
        for kind in ("table", "measure", "column", "relationship"):
            for change, prefix in (("added", "+"), ("removed", "-"), ("renamed", "~"), ("changed", "~")):
                items = [c for c in changes if c["kind"] == kind and c["change"] == change]
                if not items:
                    continue
                heading = f"{titles[kind]} {change}"
                if change == "renamed":
                    heading += " (same definition, new name)"
                lines.append(f"## {heading} ({len(items)})")
                for c in items:
                    if change == "renamed":
                        lines.append(f"- {prefix} {c['before_name']} -> {c['object']}")
                    elif change == "changed":
                        lines.append(f"- {prefix} {c['object']}: {'; '.join(c['details'])}")
                    else:
                        lines.append(f"- {prefix} {c['object']}")
                lines.append("")

    return {"has_changes": total > 0, "total_changes": total, "summary": summary,
            "changes": changes, "markdown": "\n".join(lines).rstrip("\n")}


# ---------------------------------------------------------------- data dictionary

def _md_text(s) -> str:
    """Plain text for a Markdown heading or paragraph: one line, no raw HTML."""
    if s is None:
        return ""
    text = re.sub(r"\s*[\r\n]+\s*", " ", str(s)).strip()
    return text.replace("<", "&lt;").replace(">", "&gt;")


def _md_escape(s) -> str:
    """Escape a value for a Markdown table cell (line breaks become <br>, no raw HTML)."""
    if s is None:
        return ""
    text = str(s).replace("<", "&lt;").replace(">", "&gt;").replace("|", "\\|")
    return re.sub(r"\r\n|\r|\n", "<br>", text)


def _md_code_span(expr: str) -> str:
    """An inline code span that survives backticks and pipes in the expression."""
    ticks = "`" * (max((len(run) for run in re.findall(r"`+", expr)), default=0) + 1)
    pad = " " if expr.startswith("`") or expr.endswith("`") or len(ticks) > 1 else ""
    return f"{ticks}{pad}{expr.replace('|', chr(92) + '|')}{pad}{ticks}"


def _md_fence(expr: str) -> str:
    ticks = "`" * max(3, max((len(run) for run in re.findall(r"`+", expr)), default=0) + 1)
    body = expr.replace("\r\n", "\n").replace("\r", "\n").strip("\n")
    return f"{ticks}dax\n{body}\n{ticks}"


def _is_inline_expression(expr: str) -> bool:
    return "\n" not in expr.strip() and "\r" not in expr.strip() and len(expr.strip()) <= 100


def _dictionary_tables(model: Dict[str, Any]) -> List[Dict[str, Any]]:
    return sorted(_tables(model),
                  key=lambda x: (str(x.get("name") or "").casefold(), str(x.get("name") or "")))


def _active_text(r) -> str:
    return "" if (str(r.get("is_active")).lower() in ("false", "0")) else "yes"


def _cardinality_text(r) -> str:
    fc, tc = r.get("from_cardinality"), r.get("to_cardinality")
    return f"{fc} to {tc}" if fc and tc else ""


def render_data_dictionary(model: Dict[str, Any], fmt: str = "markdown") -> str:
    """Render a portable data dictionary (tables, columns, measures, relationships) from a
    normalized model dict, with a documentation-coverage summary. Pure function (no I/O).

    fmt "markdown" (default) or "html". Single-line expressions sit in the measure table; long
    or multi-line ones are printed in full below it, with their line breaks intact.
    """
    ai = audit_ai_readiness(model)
    tables = _dictionary_tables(model)
    rels = _relationships(model)
    counts = {
        "tables": len(tables),
        "columns": sum(len(_user_columns(t)) for t in tables),
        "measures": sum(len(_children(t, "measures")) for t in tables),
        "relationships": len(rels),
    }
    if str(fmt or "").lower() == "html":
        return _render_dictionary_html(tables, rels, counts, ai)

    lines = []
    lines.append("# Data Dictionary")
    lines.append("")
    lines.append(f"- Tables: **{counts['tables']}**  |  Columns: **{counts['columns']}**  |  "
                 f"Measures: **{counts['measures']}**  |  Relationships: **{counts['relationships']}**")
    lines.append(f"- Documentation / AI-readiness score: **{ai['score']}/100 (grade {ai['grade']})**")
    lines.append("")

    for t in tables:
        hidden = " _(hidden)_" if _truthy(t.get("is_hidden")) else ""
        lines.append(f"## {_md_text(t.get('name'))}{hidden}")
        if not _empty(t.get("description")):
            lines.append(f"_{_md_text(t['description'])}_")
        lines.append("")
        cols = _user_columns(t)
        if cols:
            lines.append("### Columns")
            lines.append("| Column | Type | Hidden | Description |")
            lines.append("|---|---|---|---|")
            for c in cols:
                kind = _md_escape(c.get("data_type")) + (" (calculated)" if _is_calculated(c) else "")
                hidden_cell = "yes" if _truthy(c.get("is_hidden")) else ""
                lines.append(f"| {_md_escape(c.get('name'))} | {kind} | {hidden_cell} | "
                             f"{_md_escape(c.get('description'))} |")
            lines.append("")
        measures = _children(t, "measures")
        if measures:
            lines.append("### Measures")
            lines.append("| Measure | Format | Description | Expression |")
            lines.append("|---|---|---|---|")
            long_form = []
            for m in measures:
                expr = _dax(m)
                if not expr:
                    expr_cell = "_(unavailable)_"
                elif _is_inline_expression(expr):
                    expr_cell = _md_code_span(expr.strip())
                else:
                    expr_cell = "_see definition below_"
                    long_form.append(m)
                lines.append(f"| {_md_escape(m.get('name'))} | {_md_escape(m.get('format_string'))} | "
                             f"{_md_escape(m.get('description'))} | {expr_cell} |")
            lines.append("")
            for m in long_form:
                lines.append(f"**{_md_text(m.get('name'))}**")
                lines.append("")
                lines.append(_md_fence(_dax(m)))
                lines.append("")

    if rels:
        lines.append("## Relationships")
        lines.append("| From | To | Active | Cross-filter | Cardinality |")
        lines.append("|---|---|---|---|---|")
        for r in rels:
            lines.append(f"| {_md_escape(_label(r.get('from_table'), r.get('from_column')))} | "
                         f"{_md_escape(_label(r.get('to_table'), r.get('to_column')))} | {_active_text(r)} | "
                         f"{_md_escape(r.get('cross_filter'))} | {_md_escape(_cardinality_text(r))} |")
        lines.append("")

    return "\n".join(lines)


_HTML_STYLE = (
    "body{font-family:Segoe UI,Arial,sans-serif;font-size:14px;margin:24px;color:#222}"
    "table{border-collapse:collapse;margin:8px 0 20px}"
    "th,td{border:1px solid #ccc;padding:4px 8px;text-align:left;vertical-align:top}"
    "th{background:#f3f3f3}"
    "pre{margin:0;font-family:Consolas,monospace;font-size:13px;white-space:pre-wrap}"
    ".muted{color:#777}")


def _render_dictionary_html(tables, rels, counts, ai) -> str:
    """A self-contained HTML document. Every model value is escaped; expressions go in <pre>, so
    their line breaks and indentation are shown as written."""
    def e(v) -> str:
        return html.escape("" if v is None else str(v), quote=True)

    def row(cells, tag="td"):
        return "<tr>" + "".join(f"<{tag}>{c}</{tag}>" for c in cells) + "</tr>"

    out = ["<!doctype html>", "<html lang='en'><head><meta charset='utf-8'>",
           "<title>Data Dictionary</title>", f"<style>{_HTML_STYLE}</style></head><body>",
           "<h1>Data Dictionary</h1>",
           f"<p>Tables: <b>{counts['tables']}</b> | Columns: <b>{counts['columns']}</b> | "
           f"Measures: <b>{counts['measures']}</b> | Relationships: <b>{counts['relationships']}</b><br>"
           f"Documentation / AI-readiness score: <b>{e(ai['score'])}/100 (grade {e(ai['grade'])})</b></p>"]
    for t in tables:
        hidden = " <span class='muted'>(hidden)</span>" if _truthy(t.get("is_hidden")) else ""
        out.append(f"<h2>{e(t.get('name'))}{hidden}</h2>")
        if not _empty(t.get("description")):
            out.append(f"<p><i>{e(t['description'])}</i></p>")
        cols = _user_columns(t)
        if cols:
            out.append("<h3>Columns</h3><table>")
            out.append(row(["Column", "Type", "Hidden", "Description"], "th"))
            for c in cols:
                kind = e(c.get("data_type")) + (" (calculated)" if _is_calculated(c) else "")
                out.append(row([e(c.get("name")), kind, "yes" if _truthy(c.get("is_hidden")) else "",
                                e(c.get("description")).replace("\n", "<br>")]))
            out.append("</table>")
        measures = _children(t, "measures")
        if measures:
            out.append("<h3>Measures</h3><table>")
            out.append(row(["Measure", "Format", "Description", "Expression"], "th"))
            for m in measures:
                expr = _dax(m).replace("\r\n", "\n").strip("\n")
                cell = f"<pre>{e(expr)}</pre>" if expr else "<span class='muted'>(unavailable)</span>"
                out.append(row([e(m.get("name")), e(m.get("format_string")),
                                e(m.get("description")).replace("\n", "<br>"), cell]))
            out.append("</table>")
    if rels:
        out.append("<h2>Relationships</h2><table>")
        out.append(row(["From", "To", "Active", "Cross-filter", "Cardinality"], "th"))
        for r in rels:
            out.append(row([e(_label(r.get("from_table"), r.get("from_column"))),
                            e(_label(r.get("to_table"), r.get("to_column"))), _active_text(r),
                            e(r.get("cross_filter")), e(_cardinality_text(r))]))
        out.append("</table>")
    out.append("</body></html>")
    return "\n".join(out)
