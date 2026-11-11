"""
Filters for a PBIR report: the entries of filterConfig.filters in report.json (the whole report),
page.json (one page) and visual.json (one visual), built from a compact description.

    {"field": ..., "in": ["Lyon", "Turin"]}                  is one of (basic filtering)
    {"field": ..., "not_in": ["Lyon"]}                       is none of
    {"field": ..., "op": ">=", "value": 100,
     "and": {"op": "<", "value": 500}}                       one condition, or two joined by and / or
    {"field": ..., "between": [100, 500]}                    both ends included
    {"field": ..., "blank": false}                           is not blank
    {"field": ..., "top": {"n": 5, "by": ..., "direction": "top"}}     top or bottom N
    {"field": ..., "relative_date": {"last": 3, "unit": "months", "include_today": true}}
    {"field": ...}                                           an empty card for the reader to fill

Options on any of them: display_name, hidden, locked, single_select.

Nothing here touches disk or knows about a report spec. report_spec.py resolves the fields
against the model, names the filters and decides which filters of a file are its own.

Where the shapes come from. The container and its options are the filterConfiguration schema
(1.3.0), and every condition is an expression of the semanticQuery schema (1.4.0), which also
gives the comparison and time unit numbers. Each form was then loaded into Power BI Desktop
and seen to filter: in / not_in, comparisons on a column and on a measure, contains, between
(numbers and dates), blank, top and bottom N, and relative dates ("last" kept and dropped a
known date as it should; "next" and "this" were only seen to run without an error). The
decimal literal (1.5M) is from the schema and was not exercised: that model has no decimal column.
"""
import copy
import datetime
import math
import re
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

import pbir_builders as pb

Json = Dict[str, Any]

FILTER_KEYS = ("field", "name", "display_name", "hidden", "locked", "single_select", "in", "not_in", "op",
               "value", "and", "or", "between", "blank", "top", "relative_date")
_FORMS = ("in", "not_in", "op", "between", "blank", "top", "relative_date")

# QueryComparisonKind and TimeUnit of the semanticQuery schema.
COMPARISONS = {"=": 0, ">": 1, ">=": 2, "<": 3, "<=": 4}
TEXT_TESTS = {"contains": "Contains", "starts_with": "StartsWith"}
OPERATORS = (*COMPARISONS, "!=", *TEXT_TESTS, *(f"not_{t}" for t in TEXT_TESTS))
TIME_UNITS = {"days": 0, "weeks": 1, "months": 2, "years": 3}
_DAY = TIME_UNITS["days"]
_DESCENDING, _ASCENDING = 2, 1          # SortDirection

_FAMILIES = {"string": "text", "text": "text", "int64": "number", "double": "number", "decimal": "number",
             "int": "number", "integer": "number", "number": "number", "datetime": "date", "date": "date",
             "boolean": "true/false", "bool": "true/false"}
_ISO_DATE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})(?:[T ](\d{2}):(\d{2})(?::(\d{2}))?)?$")
_INT64 = (-2 ** 63, 2 ** 63 - 1)
# The literal forms of the semanticQuery schema (QueryLiteralExpression), for a literal a caller
# writes out in full.
_RAW_LITERALS = (
    ("text", re.compile(r"^'(?:[^']|'')*'$")),
    ("number", re.compile(r"^-?\d+L$")),
    ("number", re.compile(r"^-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?[DM]$")),
    ("true/false", re.compile(r"^(?:true|false)$")),
    ("null", re.compile(r"^null$")),
    ("date", re.compile(r"^datetime'(\d{4}-\d{2}-\d{2})T(\d{2}:\d{2}:\d{2})(?:\.\d{1,7})?'$")),
)


# ---------------------------------------------------------------------------------------------
# expressions
# ---------------------------------------------------------------------------------------------
def field_expr(field: Mapping[str, Any], source: Optional[str] = None) -> Json:
    """The expression for a resolved field {"table", "field", "kind", "aggregation"?}.

    A filter names its field twice: by table in the container ("Entity"), and by the alias its
    own From clause gives that table inside the condition (``source``).
    """
    ref = {"SourceRef": {"Source": source}} if source else {"SourceRef": {"Entity": field["table"]}}
    if field.get("kind") == "Measure":
        return {"Measure": {"Expression": ref, "Property": field["field"]}}
    column = {"Column": {"Expression": ref, "Property": field["field"]}}
    if field.get("aggregation") is not None:
        return {"Aggregation": {"Expression": column, "Function": int(field["aggregation"])}}
    return column


def _moment(value: str) -> Optional[datetime.datetime]:
    """The date and time an ISO text names, or None when it names none: 30 February and 25
    o'clock have the right shape and are still not dates, and Desktop draws an error box for
    a filter that compares with one."""
    m = _ISO_DATE.match(value.strip())
    if not m:
        return None
    try:
        return datetime.datetime(*(int(g or 0) for g in m.groups()))
    except ValueError:
        return None


def _raw_literal(text: str) -> Optional[str]:
    """The kind of value a literal written out by hand is ("text", "number", "true/false",
    "date", "null"), or None when it is not one of the forms the schema has. Desktop drops a
    visual whose filter holds anything else, without a word."""
    for kind, pattern in _RAW_LITERALS:
        m = pattern.match(text)
        if not m:
            continue
        if kind == "date" and _moment(f"{m.group(1)}T{m.group(2)}") is None:
            return None
        if text.endswith("L") and not _INT64[0] <= int(text[:-1]) <= _INT64[1]:
            return None
        return kind
    return None


def literal(value: Any, data_type: Optional[str] = None) -> Json:
    """A value as a typed literal: 'text', 12L, 1.5D, 1.5M, true, null, datetime'...'.

    ``data_type`` is the column's type in the model. With it a value of the wrong kind is
    refused and a date written as text becomes a date; without it the value's own type decides.
    {"literal": "..."} takes a literal as written, provided it is one of those forms.
    """
    family = _FAMILIES.get(str(data_type).lower()) if data_type else None

    def mismatch(what: str, shown: Any = value) -> ValueError:
        return ValueError(f"{shown!r} is {what}, but the field holds {family} values ({data_type})")

    if isinstance(value, Mapping):
        raw = value.get("literal") if set(value) == {"literal"} else None
        if not isinstance(raw, str):
            raise ValueError(f'{dict(value)!r} is not a value; use text, a number, true/false, null, or '
                             '{"literal": "..."}')
        kind = _raw_literal(raw)
        if kind is None:
            raise ValueError(f"{raw!r} is not a literal Power BI reads; the forms are 'text', 12L, 1.5D, "
                             "1.5M, true, false, null and datetime'2026-01-31T00:00:00'")
        if family and kind not in (family, "null"):
            raise mismatch({"number": "a number", "date": "a date"}.get(kind, kind), raw)
        return {"Literal": {"Value": raw}}
    if value is None:
        return {"Literal": {"Value": "null"}}
    if isinstance(value, bool):
        if family not in (None, "true/false"):
            raise mismatch("true/false")
        return {"Literal": {"Value": "true" if value else "false"}}
    if isinstance(value, (int, float)):
        if family not in (None, "number"):
            raise mismatch("a number")
        if not math.isfinite(value):
            raise ValueError(f"{value!r} is not a number a filter can compare with")
        kind = str(data_type).lower() if family else ""
        whole = float(value).is_integer()
        if kind == "decimal":
            suffix = "M"
        elif kind == "double" or not whole or (not kind and isinstance(value, float)):
            suffix = "D"
        else:
            suffix = "L"
        if suffix == "L" and not _INT64[0] <= int(value) <= _INT64[1]:
            raise ValueError(f"{value!r} is outside the range of a whole number (64 bits)")
        text = str(int(value)) if whole else repr(float(value))
        return {"Literal": {"Value": text + suffix}}
    if isinstance(value, str):
        if family == "date":
            moment = _moment(value)
            if moment is None:
                raise ValueError(f"{value!r} is not a date; write it as YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS")
            return {"Literal": {"Value": f"datetime'{moment.isoformat(timespec='seconds')}'"}}
        if family not in (None, "text"):
            raise mismatch("text")
        return {"Literal": {"Value": "'" + value.replace("'", "''") + "'"}}
    raise ValueError(f"{value!r} is not a value a filter can compare with")


def _rank(value: Any, data_type: Optional[str]) -> Any:
    """Something two ends of a range can be compared by, or None when their order cannot be told."""
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str) and _FAMILIES.get(str(data_type).lower()) == "date":
        return _moment(value)
    return None


def _alias(table: str, taken: Iterable[str] = ()) -> str:
    """The name a From clause gives a table: its first letter, as Desktop writes it."""
    base = table[:1].lower() if table[:1].isalpha() else "t"
    taken = set(taken)
    name, n = base, 0
    while name in taken:
        n += 1
        name = f"{base}{n}"
    return name


def _test(target: Json, cond: Any, data_type: Optional[str]) -> Json:
    """One condition of an advanced filter: {"op", "value"}, {"between": [low, high]} or {"blank": bool}."""
    if not isinstance(cond, Mapping):
        raise ValueError('a condition is an object such as {"op": ">=", "value": 10}')
    unknown = sorted(set(cond) - {"op", "value", "between", "blank"})
    if unknown:
        raise ValueError(f"unknown key(s) {unknown} in a condition; known keys: between, blank, op, value")
    given = [k for k in ("op", "between", "blank") if k in cond]
    if len(given) != 1:
        raise ValueError('a condition needs exactly one of "op" (with "value"), "between" or "blank"')
    if "blank" in cond:
        if not isinstance(cond["blank"], bool):
            raise ValueError('"blank" must be true (is blank) or false (is not blank)')
        test = {"Comparison": {"ComparisonKind": COMPARISONS["="], "Left": target, "Right": literal(None)}}
        return test if cond["blank"] else {"Not": {"Expression": test}}
    if "between" in cond:
        ends = cond["between"]
        if not isinstance(ends, (list, tuple)) or len(ends) != 2:
            raise ValueError('"between" needs two values: [lowest, highest]')
        if ends[0] is None or ends[1] is None:
            raise ValueError('"between" needs a value at both ends; for a range that is open at one end '
                             'use "op"')
        low, high = (literal(v, data_type) for v in ends)
        first, last = (_rank(v, data_type) for v in ends)
        if first is not None and last is not None and first > last:
            raise ValueError(f'"between" needs the lowest value first: {ends[0]!r} is above {ends[1]!r}, '
                             "so nothing could pass")
        return {"And": {"Left": {"Comparison": {"ComparisonKind": COMPARISONS[">="], "Left": target,
                                                "Right": low}},
                        "Right": {"Comparison": {"ComparisonKind": COMPARISONS["<="], "Left": target,
                                                 "Right": high}}}}
    op = cond["op"]
    if op not in OPERATORS:
        raise ValueError(f"unknown operator {op!r}; use one of {list(OPERATORS)}")
    if "value" not in cond:
        raise ValueError(f'"op": {op!r} needs "value"')
    negate = op == "!=" or op.startswith("not_")
    base = "=" if op == "!=" else op[4:] if op.startswith("not_") else op
    if base in TEXT_TESTS:
        if not isinstance(cond["value"], str):
            raise ValueError(f"{op!r} compares text, so its value must be text")
        test = {TEXT_TESTS[base]: {"Left": target, "Right": literal(cond["value"], data_type)}}
    else:
        test = {"Comparison": {"ComparisonKind": COMPARISONS[base], "Left": target,
                               "Right": literal(cond["value"], data_type)}}
    return {"Not": {"Expression": test}} if negate else test


def _top_n(field: Mapping[str, Any], alias: str, target: Json, top: Any,
           order_by: Optional[Mapping[str, Any]]) -> tuple:
    """(From, condition) of a top N filter: the field's values that a ranked subquery returns."""
    if not isinstance(top, Mapping):
        raise ValueError('"top" is an object: {"n": 5, "by": "Table.Measure", "direction": "top"}')
    unknown = sorted(set(top) - {"n", "by", "direction"})
    if unknown:
        raise ValueError(f'unknown key(s) {unknown} in "top"; known keys: by, direction, n')
    n = top.get("n")
    if isinstance(n, bool) or not isinstance(n, int) or n < 1:
        raise ValueError('"top" needs "n", a whole number of 1 or more')
    direction = top.get("direction", "top")
    if direction not in ("top", "bottom"):
        raise ValueError(f'"direction" is "top" or "bottom", not {direction!r}')
    if order_by is None:
        raise ValueError('"top" needs "by", the measure to rank by')
    table = {"Name": alias, "Entity": field["table"], "Type": 0}
    inner = [dict(table)]
    by_alias = alias
    if order_by["table"] != field["table"]:
        by_alias = _alias(order_by["table"], [alias])
        inner.append({"Name": by_alias, "Entity": order_by["table"], "Type": 0})
    query = {"Version": 2, "From": inner, "Select": [{**target, "Name": "field"}],
             "OrderBy": [{"Direction": _DESCENDING if direction == "top" else _ASCENDING,
                          "Expression": field_expr(order_by, by_alias)}],
             "Top": n}
    sources = [{"Name": "subquery", "Expression": {"Subquery": {"Query": query}}, "Type": 2}, table]
    return sources, {"In": {"Expressions": [target], "Table": {"SourceRef": {"Source": "subquery"}}}}


def _relative_date(target: Json, rel: Any) -> Json:
    """Between two day boundaries counted from now, the way the filter card's own choices read:
    "in the last 3 months" runs from tomorrow minus three months up to today."""
    if not isinstance(rel, Mapping):
        raise ValueError('"relative_date" is an object: {"last": 3, "unit": "months"}')
    unknown = sorted(set(rel) - {"last", "next", "this", "unit", "include_today"})
    if unknown:
        raise ValueError(f'unknown key(s) {unknown} in "relative_date"; known keys: include_today, last, '
                         "next, this, unit")
    spans = [k for k in ("last", "next", "this") if k in rel]
    if len(spans) != 1:
        raise ValueError('"relative_date" needs exactly one of "last", "next" or "this"')
    unit_name = str(rel.get("unit", ""))
    unit = TIME_UNITS.get(unit_name if unit_name.endswith("s") else unit_name + "s")
    if unit is None:
        raise ValueError(f'"unit" must be one of {list(TIME_UNITS)}, not {rel.get("unit")!r}')
    include_today = rel.get("include_today", True)
    if not isinstance(include_today, bool):
        raise ValueError('"include_today" must be true or false')
    now: Json = {"Now": {}}

    def add(expr: Json, amount: int, time_unit: int) -> Json:
        return {"DateAdd": {"Expression": expr, "Amount": amount, "TimeUnit": time_unit}}

    def span(expr: Json, time_unit: int) -> Json:
        return {"DateSpan": {"Expression": expr, "TimeUnit": time_unit}}

    kind = spans[0]
    if kind == "this":
        if rel["this"] is not True:
            raise ValueError('"this" must be true: {"this": true, "unit": "months"}')
        low = high = span(now, unit)
    else:
        n = rel[kind]
        if isinstance(n, bool) or not isinstance(n, int) or n < 1:
            raise ValueError(f'"{kind}" must be a whole number of 1 or more')
        if kind == "last" and include_today:
            low, high = span(add(add(now, 1, _DAY), -n, unit), _DAY), span(now, _DAY)
        elif kind == "last":
            low, high = span(add(now, -n, unit), _DAY), span(add(now, -1, _DAY), _DAY)
        elif include_today:
            low, high = span(now, _DAY), span(add(add(now, -1, _DAY), n, unit), _DAY)
        else:
            low, high = span(add(now, 1, _DAY), _DAY), span(add(now, n, unit), _DAY)
    return {"Between": {"Expression": target, "LowerBound": low, "UpperBound": high}}


# ---------------------------------------------------------------------------------------------
# the filter container
# ---------------------------------------------------------------------------------------------
def filter_container(name: str, field: Mapping[str, Any], definition: Mapping[str, Any], *,
                     data_type: Optional[str] = None, order_by: Optional[Mapping[str, Any]] = None,
                     how_created: str = "User") -> Json:
    """One entry of filterConfig.filters.

    field       the resolved field: {"table", "field", "kind": "Column" | "Measure", "aggregation"?}
    definition  the compact filter (see the module docstring); its "field", "name" and "by" are
                resolved by the caller and not read here
    data_type   the column's type in the model, to type the values and refuse ones that cannot fit
    order_by    the resolved field a top N filter ranks by
    how_created "Auto" when the visual itself shows the field (Desktop then has one card for
                it, not two), otherwise "User"
    Raises ValueError with a message that says what is wrong.
    """
    unknown = sorted(set(definition) - set(FILTER_KEYS))
    if unknown:
        raise ValueError(f"unknown filter key(s) {unknown}; known keys: {sorted(FILTER_KEYS)}")
    forms = [k for k in _FORMS if k in definition]
    if len(forms) > 1:
        raise ValueError(f"a filter is one of {', '.join(forms)}, not several; join two conditions "
                         'with "and" / "or"')
    form = forms[0] if forms else None
    joins = [k for k in ("and", "or") if k in definition]
    if "value" in definition and form != "op":
        raise ValueError('"value" goes with "op"')
    if joins and form not in ("op", "between", "blank"):
        raise ValueError(f'"{joins[0]}" joins a second condition to "op", "between" or "blank"')
    if len(joins) > 1:
        raise ValueError('a filter takes "and" or "or", not both')
    for flag in ("hidden", "locked", "single_select"):
        if flag in definition and not isinstance(definition[flag], bool):
            raise ValueError(f'"{flag}" must be true or false')
    if not isinstance(definition.get("display_name", ""), str):
        raise ValueError('"display_name" must be text')
    calculated = field.get("kind") == "Measure" or field.get("aggregation") is not None
    if calculated and form not in (None, "op", "between", "blank"):
        raise ValueError(f'a measure or an aggregated column can only be compared ("op", "between", '
                         f'"blank"), not filtered with "{form}"')
    if calculated:
        data_type = None                # the column's type says nothing about a sum or a count of it

    alias = _alias(field["table"])
    target = field_expr(field, alias)
    sources: List[Json] = [{"Name": alias, "Entity": field["table"], "Type": 0}]
    general: Json = {}
    condition: Optional[Json] = None
    kind = "Advanced" if calculated else "Categorical"
    if form in ("in", "not_in"):
        values = definition[form]
        if not isinstance(values, (list, tuple)) or not values:
            raise ValueError(f'"{form}" needs a list of at least one value')
        condition = {"In": {"Expressions": [target], "Values": [[literal(v, data_type)] for v in values]}}
        if form == "not_in":
            condition = {"Not": {"Expression": condition}}
            general["isInvertedSelectionMode"] = pb.lit_bool(True)
    elif form in ("op", "between", "blank"):
        kind = "Advanced"
        condition = _test(target, {k: definition[k] for k in ("op", "value", "between", "blank")
                                   if k in definition}, data_type)
        if joins:
            second = _test(target, definition[joins[0]], data_type)
            condition = {"And" if joins[0] == "and" else "Or": {"Left": condition, "Right": second}}
    elif form == "top":
        kind = "TopN"
        sources, condition = _top_n(field, alias, target, definition["top"], order_by)
    elif form == "relative_date":
        if data_type and _FAMILIES.get(str(data_type).lower()) != "date":
            raise ValueError(f"a relative date filter needs a date column, and this one is {data_type}")
        kind = "RelativeDate"
        condition = _relative_date(target, definition["relative_date"])
    if definition.get("single_select"):
        if form not in (None, "in") or calculated:
            raise ValueError('"single_select" goes with "in" (or with an empty filter card)')
        if form == "in" and len(definition["in"]) > 1:
            raise ValueError('"single_select" allows one value in "in"')
        general["requireSingleSelect"] = pb.lit_bool(True)

    out: Json = {"name": name}
    if definition.get("display_name"):
        out["displayName"] = str(definition["display_name"])
    out["field"] = field_expr(field)
    out["type"] = kind
    if condition is not None:
        out["filter"] = {"Version": 2, "From": sources, "Where": [{"Condition": condition}]}
    out["howCreated"] = how_created
    if definition.get("hidden"):
        out["isHiddenInViewMode"] = True
    if definition.get("locked"):
        out["isLockedInViewMode"] = True
    if general:
        out["objects"] = {"general": [pb.entry(general)]}
    return out


def binding_filter(name: str, field: Mapping[str, Any]) -> Json:
    """The empty filter a drillthrough or tooltip page keeps for one of its fields. The page
    binding points at it by name, and the value arrives when a reader drills through.

    "Drillthrough" is what keeps it out of "Filters on this page": written as a "User" filter,
    Desktop lists it there as an ordinary page filter.
    """
    return {"name": name, "field": field_expr(field), "type": "Categorical", "howCreated": "Drillthrough"}


# ---------------------------------------------------------------------------------------------
# a file's filters: ours and everyone else's
# ---------------------------------------------------------------------------------------------
def filter_names(config: Any) -> List[str]:
    """The names of the filters in a filterConfig, in order."""
    filters = config.get("filters") if isinstance(config, Mapping) else None
    return [str(f.get("name", "")) for f in filters or [] if isinstance(f, Mapping)]


def merge_filters(config: Any, ours: Sequence[Mapping[str, Any]],
                  previous: Iterable[str] = ()) -> Optional[Json]:
    """The filterConfig a file should hold: ``ours`` in place of what an earlier apply wrote.

    ``previous`` names the filters that earlier apply created. Every other filter in ``config``
    was added in Desktop and stays where it is. One of ours keeps the position it had; a new
    one goes to the end. Returns None when nothing is left, so the key can go rather than
    stay behind empty.
    """
    out: Json = dict(config) if isinstance(config, Mapping) else {}
    mine = {str(f["name"]).lower(): f for f in ours}
    gone = {str(n).lower() for n in previous} - set(mine)
    kept: List[Any] = []
    placed = set()
    for item in out.get("filters") or []:
        key = str(item.get("name", "")).lower() if isinstance(item, Mapping) else ""
        if key in mine:
            if key not in placed:
                kept.append(copy.deepcopy(dict(mine[key])))
                placed.add(key)
        elif key not in gone:
            kept.append(item)
    kept += [copy.deepcopy(dict(f)) for f in ours if str(f["name"]).lower() not in placed]
    if kept:
        out["filters"] = kept
    else:
        out.pop("filters", None)
    return out or None
