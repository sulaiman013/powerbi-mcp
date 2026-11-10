"""
Bulk DAX measure-suite generation.

Pure functions that expand a base measure (or column) into a governed suite of measures:
time intelligence (YTD/QTD/MTD/PY/YoY/YoY %/PM/MoM/MoM %/rolling windows), share-of-total
ratios, ranks, and column statistics. Every generated measure carries a name, self-contained DAX
(no dependency on other generated measures), a format string, a display folder, and a
description, so bulk creation produces a documented, AI-readable model rather than a pile of
bare formulas.

Consumed by the generate_measure_suite tool, which can return the suite as DAX, write it
offline into a PBIP project's TMDL (tmdl_authoring), or create it live via TOM.

Inputs are validated before anything is generated (ValueError with the reason): a column
reference must name its table, a measure reference must be one measure. Names are escaped with
the shared helpers in dax_tokens, so a ']' in a column name or an apostrophe in a table name
produces valid DAX.

Entry point:
    generate_suite(kind, **params) -> List[{name, expression, format_string, display_folder, description}]
Kinds: "time_intelligence", "ratios", "ranking", "column_stats".
"""
import calendar
import inspect
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple, Union

import dax_tokens as dt

# Used where a measure inherits the base measure's format and the caller did not supply it.
# A measure without a format string shows raw precision in visuals and fails the model's own BPA.
DEFAULT_NUMBER_FORMAT = "#,0.00"
COUNT_FORMAT = "#,0"
PERCENT_FORMAT = "0.0%"
SIGNED_PERCENT_FORMAT = "+0.0%;-0.0%;0.0%"

_COLUMN_HINT = "Write it as Table.Column, 'Table Name'.Column or 'Table Name'[Column]."


# ---------------------------------------------------------------- reference helpers

def _text(value, what: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{what} is required and must be text (got {value!r}).")
    return value.strip()


def _single_bracket(text: str) -> Optional[str]:
    """The name inside ``text`` when it is exactly one complete [bracketed] token, else None."""
    tokens = dt.tokenize(text)
    if len(tokens) == 1 and tokens[0].kind == dt.BRACKET and dt.bracket(tokens[0].name) == tokens[0].text:
        return tokens[0].name
    return None


def bare_name(name: str) -> str:
    """The measure's name from any accepted spelling:
    'Total Sales', '[Total Sales]', '[Net]]Sales]' (escaped), or Table[Measure] / 'Table'[Measure]."""
    s = _text(name, "A measure name")
    inner = _single_bracket(s)
    if inner is not None:
        if not inner.strip():
            raise ValueError("The measure name is empty.")
        return inner
    tokens = dt.tokenize(s)
    if (len(tokens) == 2 and tokens[0].kind in (dt.IDENT, dt.QNAME) and tokens[1].kind == dt.BRACKET
            and dt.bracket(tokens[1].name) == tokens[1].text):
        return tokens[1].name                       # Table[Measure]: measures are referenced unqualified
    if s.startswith("["):
        raise ValueError(f"{name!r} is not a single [Measure] reference. Pass the plain measure name, or "
                         "one bracketed reference with any ']' in the name doubled.")
    return s


def measure_ref(name: str) -> str:
    """'Total Sales' or '[Total Sales]' -> '[Total Sales]', with ']' escaped as ']]'."""
    return dt.bracket(bare_name(name))


def split_column_ref(ref: str) -> Tuple[str, str]:
    """(table, column), unescaped, from any accepted column-reference spelling.

    Accepted: Table.Column (the FIRST dot separates table from column), 'Quoted Table'.Column,
    Table[Column], 'Quoted Table'[Column] (']' doubled inside brackets, apostrophes doubled inside
    quotes). A reference without a table is rejected: 'Amount' alone cannot be resolved.
    """
    s = _text(ref, "A column reference")
    if s.startswith("'"):
        first = dt.tokenize(s)[0]
        if first.kind != dt.QNAME or dt.quote_table(first.name, force=True) != first.text:
            raise ValueError(f"Column reference {ref!r} has an unterminated quoted table name. "
                             f"{_COLUMN_HINT}")
        table, rest = first.name, s[len(first.text):].strip()
        if rest.startswith("."):
            column = rest[1:].strip()
        else:
            column = _single_bracket(rest)
            if column is None:
                raise ValueError(f"Column reference {ref!r} has no column after the table. {_COLUMN_HINT}")
    elif "[" in s:
        cut = s.index("[")
        table, column = s[:cut].strip(), _single_bracket(s[cut:].strip())
        if column is None or "]" in table:
            raise ValueError(f"Column reference {ref!r} is not Table[Column]. {_COLUMN_HINT}")
    else:
        table, dot, column = s.partition(".")
        if not dot:
            raise ValueError(f"Column reference {ref!r} has no table. {_COLUMN_HINT}")
        table, column = table.strip(), column.strip()
    if not table:
        raise ValueError(f"Column reference {ref!r} has no table. {_COLUMN_HINT}")
    if not column:
        raise ValueError(f"Column reference {ref!r} has no column. {_COLUMN_HINT}")
    return table, column


def column_ref(ref: str) -> str:
    """A valid DAX column reference, 'Table'[Column], from any accepted spelling (see
    split_column_ref). The table is always quoted, which is valid for every table name."""
    table, column = split_column_ref(ref)
    return dt.column_ref(table, column)


def _column_key(table: str, column: str) -> Tuple[str, str]:
    return table.casefold(), column.casefold()


def _columns(value, what: str) -> List[Tuple[str, str]]:
    """Validated, de-duplicated (table, column) list from a list of references (or one reference)."""
    if isinstance(value, str):
        value = [value]
    if not isinstance(value, (list, tuple)) or not value:
        raise ValueError(f"{what} must be a non-empty list of column references. {_COLUMN_HINT}")
    out, seen = [], set()
    for item in value:
        table, column = split_column_ref(item)
        if _column_key(table, column) not in seen:
            seen.add(_column_key(table, column))
            out.append((table, column))
    return out


def _sort_columns(sort_by_columns) -> Dict[Tuple[str, str], Tuple[str, str]]:
    """{column -> its sort-by column} keyed case-insensitively. Both must be in the same table:
    ALL() and ALLSELECTED() only take several columns of one table."""
    if sort_by_columns is None:
        return {}
    if not isinstance(sort_by_columns, Mapping):
        raise ValueError("sort_by_columns must map a column reference to its sort-by column reference, "
                         "e.g. {'Date.Month Name': 'Date.Month No'}.")
    out = {}
    for key, value in sort_by_columns.items():
        table, column = split_column_ref(key)
        sort_table, sort_column = split_column_ref(value)
        if table.casefold() != sort_table.casefold():
            raise ValueError(f"sort_by_columns: {value!r} is not in the same table as {key!r}; a column can "
                             "only be sorted by a column of its own table.")
        if column.casefold() != sort_column.casefold():
            out[_column_key(table, column)] = (sort_table, sort_column)
    return out


def _clear_list(table: str, column: str, sort_map) -> str:
    """The column list for ALL()/ALLSELECTED(): the column plus its sort-by column, if any.

    Power BI adds the sort-by column to the query whenever the column is on a visual, so a filter
    on it survives ALL(<column>) and the 'total' would be the current row again.
    """
    refs = [dt.column_ref(table, column)]
    if _column_key(table, column) in sort_map:
        refs.append(dt.column_ref(*sort_map[_column_key(table, column)]))
    return ", ".join(refs)


def _m(name, expression, format_string, folder, description) -> Dict[str, Any]:
    return {"name": name, "expression": expression, "format_string": format_string,
            "display_folder": folder, "description": description}


def _folder(display_folder, default: str) -> str:
    if display_folder is None:
        return default
    return _text(display_folder, "display_folder")


def _format(value, default: str, what: str) -> str:
    if value is None:
        return default
    return _text(value, what)


# ---------------------------------------------------------------- time intelligence

# variant -> name suffix, format ('inherit' = the base measure's), description.
# {base} = base measure name. The DAX itself is built in _ti_expression.
TIME_INTELLIGENCE_VARIANTS: Dict[str, Dict[str, str]] = {
    "ytd": {"suffix": "YTD", "format": "inherit",
            "desc": "{base} accumulated from the start of the year to the current date context."},
    "qtd": {"suffix": "QTD", "format": "inherit",
            "desc": "{base} accumulated from the start of the quarter to the current date context."},
    "mtd": {"suffix": "MTD", "format": "inherit",
            "desc": "{base} accumulated from the start of the month to the current date context."},
    "py": {"suffix": "PY", "format": "inherit",
           "desc": "{base} for the same period one year earlier."},
    "yoy": {"suffix": "YoY", "format": "inherit",
            "desc": "Absolute change of {base} versus the same period last year (blank unless both "
                    "periods have data)."},
    "yoy_pct": {"suffix": "YoY %", "format": SIGNED_PERCENT_FORMAT,
                "desc": "Percent change of {base} versus the same period last year (blank unless both "
                        "periods have data)."},
    "pm": {"suffix": "PM", "format": "inherit",
           "desc": "{base} for the previous month (requires a contiguous date table)."},
    "mom": {"suffix": "MoM", "format": "inherit",
            "desc": "Absolute change of {base} versus the previous month (blank unless both months "
                    "have data)."},
    "mom_pct": {"suffix": "MoM %", "format": SIGNED_PERCENT_FORMAT,
                "desc": "Percent change of {base} versus the previous month (blank unless both months "
                        "have data)."},
    "rolling_12m": {"suffix": "Rolling 12M", "format": "inherit",
                    "desc": "{base} over the trailing 12 months ending at the current date context."},
    "rolling_3m": {"suffix": "Rolling 3M", "format": "inherit",
                   "desc": "{base} over the trailing 3 months ending at the current date context."},
}

DEFAULT_TI_VARIANTS = ["ytd", "qtd", "mtd", "py", "yoy", "yoy_pct", "mom_pct", "rolling_12m"]


def parse_fiscal_year_end(value) -> Optional[Tuple[int, int]]:
    """(month, day) of the fiscal year's last day, or None for a calendar year.

    Accepts 'MM-DD' / 'M/D' text, a (month, day) pair, or a month number (its last day is used).
    """
    if value is None or value == "":
        return None
    try:
        if isinstance(value, bool):
            raise ValueError
        if isinstance(value, int):
            month, day = value, None
        elif isinstance(value, (list, tuple)) and len(value) == 2:
            month, day = int(value[0]), int(value[1])
        elif isinstance(value, str):
            parts = value.strip().replace("/", "-").split("-")
            if len(parts) != 2:
                raise ValueError
            month, day = int(parts[0]), int(parts[1])
        else:
            raise ValueError
        if not 1 <= month <= 12:
            raise ValueError
        last = calendar.monthrange(2000, month)[1]          # a leap year, so 29 February is accepted
        day = (28 if month == 2 else last) if day is None else day
        if not 1 <= day <= last:
            raise ValueError
    except (TypeError, ValueError):
        raise ValueError(f"fiscal_year_end {value!r} is not a month-day. Use 'MM-DD' (e.g. '06-30' for a "
                         "fiscal year ending 30 June) or the month number.") from None
    return None if (month, day) == (12, 31) else (month, day)


def _is_month_end(month: int, day: int) -> bool:
    return day >= (28 if month == 2 else calendar.monthrange(2001, month)[1])


def _both_periods(b: str, shifted: str, var: str, percent: bool) -> str:
    """Current vs shifted period, blank unless BOTH have data. Without the guard on the current
    period a future month shows -100% (or minus last year's value) as soon as the prior period
    has data."""
    result = f"DIVIDE(__cur - {var}, {var})" if percent else f"__cur - {var}"
    return (f"VAR __cur = {b}\n"
            f"VAR {var} = CALCULATE({b}, {shifted})\n"
            "RETURN\n"
            f"    IF(NOT ISBLANK(__cur) && NOT ISBLANK({var}), {result})")


def _ti_expression(variant: str, b: str, d: str, fiscal: Optional[Tuple[int, int]]) -> str:
    if variant == "ytd":
        if fiscal:
            return f'CALCULATE({b}, DATESYTD({d}, "{fiscal[0]:02d}-{fiscal[1]:02d}"))'
        return f"CALCULATE({b}, DATESYTD({d}))"
    if variant == "qtd":
        # DATESQTD has no year-end argument. Fiscal quarters equal calendar quarters whenever the
        # fiscal year ends on a calendar quarter end; otherwise the quarter start is computed.
        if not fiscal or fiscal[0] % 3 == 0:
            return f"CALCULATE({b}, DATESQTD({d}))"
        start_month = fiscal[0] % 12 + 1
        return (f"VAR __last = MAX({d})\n"
                f"VAR __into = MOD(MONTH(__last) - {start_month}, 3)\n"
                "VAR __start = DATE(YEAR(__last), MONTH(__last) - __into, 1)\n"
                "RETURN\n"
                f"    IF(NOT ISBLANK(__last), CALCULATE({b}, DATESBETWEEN({d}, __start, __last)))")
    if variant == "mtd":
        return f"CALCULATE({b}, DATESMTD({d}))"
    if variant == "py":
        return f"CALCULATE({b}, SAMEPERIODLASTYEAR({d}))"
    if variant in ("yoy", "yoy_pct"):
        return _both_periods(b, f"SAMEPERIODLASTYEAR({d})", "__py", variant == "yoy_pct")
    if variant == "pm":
        return f"CALCULATE({b}, DATEADD({d}, -1, MONTH))"
    if variant in ("mom", "mom_pct"):
        return _both_periods(b, f"DATEADD({d}, -1, MONTH)", "__pm", variant == "mom_pct")
    months = {"rolling_12m": 12, "rolling_3m": 3}[variant]
    return f"CALCULATE({b}, DATESINPERIOD({d}, MAX({d}), -{months}, MONTH))"


def generate_time_intelligence(base_measure: str, date_column: str,
                               variants: Optional[List[str]] = None,
                               display_folder: Optional[str] = None,
                               base_format: Optional[str] = None,
                               fiscal_year_end: Union[str, int, Sequence[int], None] = None
                               ) -> List[Dict[str, Any]]:
    """Expand a base measure into time-intelligence measures over a date column.

    base_format: the base measure's format string; variants that are amounts inherit it. When it
        is not given they get DEFAULT_NUMBER_FORMAT, so pass it whenever the base is not a plain
        decimal amount (a count, a percentage, a currency).
    fiscal_year_end: last day of the fiscal year ('06-30', or a month number). YTD and QTD then
        follow the fiscal year and are named FYTD / FQTD. Fiscal QTD needs a month-end year end.
    """
    base = bare_name(base_measure)
    b = dt.bracket(base)
    d = column_ref(date_column)
    fiscal = parse_fiscal_year_end(fiscal_year_end)
    folder = _folder(display_folder, f"Time Intelligence\\{base}")
    inherited = _format(base_format, DEFAULT_NUMBER_FORMAT, "base_format")

    if variants is None:
        chosen = list(DEFAULT_TI_VARIANTS)
    elif isinstance(variants, str) or not isinstance(variants, (list, tuple)) or not variants:
        raise ValueError("variants must be a non-empty list. Valid: "
                         + ", ".join(sorted(TIME_INTELLIGENCE_VARIANTS)))
    else:
        chosen = list(dict.fromkeys(str(v).strip().lower() for v in variants))
    unknown = [v for v in chosen if v not in TIME_INTELLIGENCE_VARIANTS]
    if unknown:
        raise ValueError(f"Unknown time-intelligence variant(s): {', '.join(unknown)}. "
                         f"Valid: {', '.join(sorted(TIME_INTELLIGENCE_VARIANTS))}")
    if fiscal and "qtd" in chosen and not _is_month_end(*fiscal):
        raise ValueError(f"A fiscal quarter-to-date needs a fiscal year that ends on the last day of a "
                         f"month; {fiscal[0]:02d}-{fiscal[1]:02d} does not. Drop 'qtd' or change "
                         "fiscal_year_end.")

    out: List[Dict[str, Any]] = []
    for v in chosen:
        t = TIME_INTELLIGENCE_VARIANTS[v]
        suffix, desc = t["suffix"], t["desc"].format(base=base)
        if fiscal and v in ("ytd", "qtd"):
            suffix = "F" + suffix
            desc = desc.replace("start of the year", "start of the fiscal year") \
                       .replace("start of the quarter", "start of the fiscal quarter") \
                + f" Fiscal year ends {fiscal[0]:02d}-{fiscal[1]:02d}."
        fmt = inherited if t["format"] == "inherit" else t["format"]
        out.append(_m(f"{base} {suffix}", _ti_expression(v, b, d, fiscal), fmt, folder, desc))
    return out


# ---------------------------------------------------------------- ratios / ranking / stats

def generate_ratios(base_measure: str, dimension_columns: List[str],
                    display_folder: Optional[str] = None,
                    include_all_selected: bool = True,
                    sort_by_columns: Optional[Mapping[str, str]] = None) -> List[Dict[str, Any]]:
    """Share-of-total measures: base as a % of the total over each dimension column
    (ALL = grand total ignoring that filter; ALLSELECTED = % of the visible total).

    sort_by_columns: {column: its sort-by column} for dimension columns that have one. The
    sort-by column is then cleared together with the column; without it a sorted column (Month
    Name sorted by Month No) reports 100% on every row.
    """
    base = bare_name(base_measure)
    b = dt.bracket(base)
    dims = _columns(dimension_columns, "dimension_columns")
    sort_map = _sort_columns(sort_by_columns)
    folder = _folder(display_folder, f"Ratios\\{base}")
    out: List[Dict[str, Any]] = []
    for table, col in dims:
        cleared = _clear_list(table, col, sort_map)
        out.append(_m(
            f"{base} % of Total {col}",
            f"DIVIDE({b}, CALCULATE({b}, ALL({cleared})))",
            PERCENT_FORMAT, folder,
            f"{base} as a share of the grand total across all {col} values.",
        ))
        if include_all_selected:
            out.append(_m(
                f"{base} % of Selected {col}",
                f"DIVIDE({b}, CALCULATE({b}, ALLSELECTED({cleared})))",
                PERCENT_FORMAT, folder,
                f"{base} as a share of the total across the currently selected {col} values.",
            ))
    return out


def generate_ranking(base_measure: str, dimension_columns: List[str],
                     display_folder: Optional[str] = None,
                     include_all_selected: bool = True,
                     sort_by_columns: Optional[Mapping[str, str]] = None) -> List[Dict[str, Any]]:
    """Rank of the current dimension member by the base measure (dense, descending).

    Two measures per dimension: the rank among ALL members (slicers on the dimension are
    ignored) and, unless include_all_selected is False, the rank among the SELECTED members
    (1..n over what the user can see). Both are blank at totals and for a member without data.
    sort_by_columns: as for generate_ratios; without it a sorted column ranks every row 1.
    """
    base = bare_name(base_measure)
    b = dt.bracket(base)
    dims = _columns(dimension_columns, "dimension_columns")
    sort_map = _sort_columns(sort_by_columns)
    folder = _folder(display_folder, f"Ranking\\{base}")

    def rank(d: str, scope: str) -> str:
        return (f"IF(\n    HASONEVALUE({d}) && NOT ISBLANK({b}),\n"
                f"    RANKX({scope}, {b}, , DESC, DENSE)\n)")

    out: List[Dict[str, Any]] = []
    for table, col in dims:
        d = dt.column_ref(table, col)
        cleared = _clear_list(table, col, sort_map)
        out.append(_m(
            f"{base} Rank by {col}",
            rank(d, f"ALL({cleared})"),
            COUNT_FORMAT, folder,
            f"Dense descending rank of the current {col} by {base} among all {col} values "
            "(blank at totals and where there is no data).",
        ))
        if include_all_selected:
            out.append(_m(
                f"{base} Rank by Selected {col}",
                rank(d, f"ALLSELECTED({cleared})"),
                COUNT_FORMAT, folder,
                f"Dense descending rank of the current {col} by {base} among the currently selected "
                f"{col} values (blank at totals and where there is no data).",
            ))
    return out


_STATS = {
    "sum": ("Total {col}", "SUM", "Sum of {col}."),
    "avg": ("Average {col}", "AVERAGE", "Arithmetic mean of {col}."),
    "min": ("Min {col}", "MIN", "Minimum value of {col}."),
    "max": ("Max {col}", "MAX", "Maximum value of {col}."),
    "median": ("Median {col}", "MEDIAN", "Median value of {col}."),
    "distinct": ("Distinct {col}", "DISTINCTCOUNT", "Count of distinct {col} values."),
}


def generate_column_stats(column: str, display_folder: Optional[str] = None,
                          stats: Optional[List[str]] = None,
                          column_format: Optional[str] = None) -> List[Dict[str, Any]]:
    """Basic statistical measures over a numeric column: sum/avg/min/max/median/distinct.

    column_format: the column's format string, used for every statistic except the distinct
    count (DEFAULT_NUMBER_FORMAT when not given).
    """
    table, col = split_column_ref(column)
    d = dt.column_ref(table, col)
    folder = _folder(display_folder, f"Statistics\\{col}")
    value_format = _format(column_format, DEFAULT_NUMBER_FORMAT, "column_format")
    if stats is None:
        chosen = list(_STATS)
    elif isinstance(stats, str) or not isinstance(stats, (list, tuple)) or not stats:
        raise ValueError(f"stats must be a non-empty list. Valid: {', '.join(sorted(_STATS))}")
    else:
        chosen = list(dict.fromkeys(str(s).strip().lower() for s in stats))
    unknown = [s for s in chosen if s not in _STATS]
    if unknown:
        raise ValueError(f"Unknown stat(s): {', '.join(unknown)}. Valid: {', '.join(sorted(_STATS))}")
    out: List[Dict[str, Any]] = []
    for s in chosen:
        name, function, desc = _STATS[s]
        out.append(_m(name.format(col=col), f"{function}({d})",
                      COUNT_FORMAT if s == "distinct" else value_format, folder, desc.format(col=col)))
    return out


# ---------------------------------------------------------------- model lookups

def sort_by_columns_from_model(model: Dict[str, Any]) -> Dict[str, str]:
    """The sort_by_columns mapping for a gathered model dict (see model_analysis for the shape):
    every column that has a sort-by column, as {'Table'[Column]: 'Table'[Sort Column]}.
    Pass the result to generate_ratios / generate_ranking so sorted columns are handled."""
    out: Dict[str, str] = {}
    for t in (model or {}).get("tables") or []:
        if not isinstance(t, dict) or not isinstance(t.get("name"), str) or not t["name"].strip():
            continue
        for c in t.get("columns") or []:
            if not isinstance(c, dict):
                continue
            name, sort = c.get("name"), c.get("sort_by")
            if (isinstance(name, str) and name.strip() and isinstance(sort, str) and sort.strip()
                    and name.casefold() != sort.casefold()):
                out[dt.column_ref(t["name"], name)] = dt.column_ref(t["name"], sort)
    return out


def measure_format_from_model(model: Dict[str, Any], measure: str) -> Optional[str]:
    """The format string of ``measure`` in a gathered model dict, or None when the measure is not
    there or has none. Pass it as base_format so generated amounts look like their base measure."""
    try:
        wanted = bare_name(measure).casefold()
    except ValueError:
        return None
    for t in (model or {}).get("tables") or []:
        for m in (t.get("measures") or []) if isinstance(t, dict) else []:
            if isinstance(m, dict) and isinstance(m.get("name"), str) and m["name"].casefold() == wanted:
                fmt = m.get("format_string")
                return fmt.strip() if isinstance(fmt, str) and fmt.strip() else None
    return None


# ---------------------------------------------------------------- dispatcher

def generate_suite(kind: str, **params) -> List[Dict[str, Any]]:
    """Dispatch to a generator by kind. Every problem with the arguments is a ValueError."""
    kind = (kind or "").lower() if isinstance(kind, str) else ""
    generators = {
        "time_intelligence": generate_time_intelligence, "time": generate_time_intelligence,
        "ti": generate_time_intelligence,
        "ratios": generate_ratios, "ratio": generate_ratios, "share": generate_ratios,
        "ranking": generate_ranking, "rank": generate_ranking,
        "column_stats": generate_column_stats, "stats": generate_column_stats,
    }
    if kind not in generators:
        raise ValueError(f"Unknown suite kind '{kind}'. "
                         "Use time_intelligence | ratios | ranking | column_stats.")
    generator = generators[kind]
    try:
        inspect.signature(generator).bind(**params)
    except TypeError as e:
        # a missing or unexpected keyword argument is bad input, not a crash
        raise ValueError(f"{kind}: {e}") from None
    return generator(**params)
