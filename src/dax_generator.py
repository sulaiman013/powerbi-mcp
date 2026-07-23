"""
Bulk DAX measure-suite generation.

Pure functions that expand a base measure (or column) into a governed suite of measures:
time intelligence (YTD/QTD/MTD/PY/YoY/YoY %/MoM/rolling windows), share-of-total ratios,
ranks, and column statistics. Every generated measure carries a name, self-contained DAX
(no dependency on other generated measures), a format string, a display folder, and a
description, so bulk creation produces a documented, AI-readable model rather than a pile
of bare formulas.

Consumed by the generate_measure_suite tool, which can return the suite as DAX, write it
offline into a PBIP project's TMDL (tmdl_authoring), or create it live via TOM.

Entry point:
    generate_suite(kind, **params) -> List[{name, expression, format_string, display_folder, description}]
Kinds: "time_intelligence", "ratios", "ranking", "column_stats".
"""
import re
from typing import Any, Dict, List, Optional

# ---------------------------------------------------------------- reference helpers

def _bracket(prop: str) -> str:
    """Wrap a column/measure name in brackets, escaping ']' as ']]' (the DAX rule)."""
    return "[" + str(prop).replace("]", "]]") + "]"


def measure_ref(name: str) -> str:
    """'Total Sales' or '[Total Sales]' -> '[Total Sales]'."""
    name = name.strip()
    if name.startswith("[") and name.endswith("]"):
        return name
    return _bracket(name)


def bare_name(name: str) -> str:
    """'[Total Sales]' -> 'Total Sales'."""
    name = name.strip()
    if name.startswith("[") and name.endswith("]"):
        return name[1:-1]
    return name


def column_ref(ref: str) -> str:
    """Accept 'Date.Date', 'Date'[Date], Sales[Amount], or "Table Name[Col]" and return a
    valid DAX column reference.

    A ref containing '[' is treated as bracketed DAX, but its table part is single-quoted
    when needed (a bare table name with spaces is not valid DAX). Otherwise the first dot
    splits table from column and the table is always single-quoted (always safe in DAX).
    """
    s = ref.strip()
    if "[" in s:
        table, bracket, rest = s.partition("[")
        table = table.strip()
        if table and not table.startswith("'") and re.search(r"[^\w]", table):
            table = "'" + table.replace("'", "''") + "'"
        return f"{table}{bracket}{rest}"
    if s.startswith("'"):
        end = s.find("'", 1)
        if end != -1:
            table = s[1:end]
            col = s[end + 1:].lstrip(".")
            return f"'{table}'" + _bracket(col)
    table, _, col = s.partition(".")
    return f"'{table.strip()}'" + _bracket(col.strip())


def split_column_ref(ref: str):
    """Return (table, column) from any accepted column-ref spelling."""
    s = column_ref(ref)
    tbl, _, rest = s.partition("[")
    return tbl.strip().strip("'"), rest.rstrip("]")


def _m(name, expression, format_string, folder, description) -> Dict[str, Any]:
    return {"name": name, "expression": expression, "format_string": format_string,
            "display_folder": folder, "description": description}


# ---------------------------------------------------------------- time intelligence

# variant -> (name suffix, expression template, format ('inherit' = keep base's), description)
# Templates use {b} = base measure ref, {d} = date column ref, {base} = base measure bare name.
TIME_INTELLIGENCE_VARIANTS: Dict[str, Dict[str, str]] = {
    "ytd": {
        "suffix": "YTD",
        "expr": "CALCULATE({b}, DATESYTD({d}))",
        "format": "inherit",
        "desc": "{base} accumulated from the start of the year to the current date context.",
    },
    "qtd": {
        "suffix": "QTD",
        "expr": "CALCULATE({b}, DATESQTD({d}))",
        "format": "inherit",
        "desc": "{base} accumulated from the start of the quarter to the current date context.",
    },
    "mtd": {
        "suffix": "MTD",
        "expr": "CALCULATE({b}, DATESMTD({d}))",
        "format": "inherit",
        "desc": "{base} accumulated from the start of the month to the current date context.",
    },
    "py": {
        "suffix": "PY",
        "expr": "CALCULATE({b}, SAMEPERIODLASTYEAR({d}))",
        "format": "inherit",
        "desc": "{base} for the same period one year earlier.",
    },
    "yoy": {
        "suffix": "YoY",
        "expr": ("VAR __py = CALCULATE({b}, SAMEPERIODLASTYEAR({d}))\n"
                 "RETURN\n"
                 "    IF(NOT ISBLANK(__py), {b} - __py)"),
        "format": "inherit",
        "desc": "Absolute change of {base} versus the same period last year (blank when no prior-year data).",
    },
    "yoy_pct": {
        "suffix": "YoY %",
        "expr": ("VAR __py = CALCULATE({b}, SAMEPERIODLASTYEAR({d}))\n"
                 "RETURN\n"
                 "    DIVIDE({b} - __py, __py)"),
        "format": "+0.0%;-0.0%;0.0%",
        "desc": "Percent change of {base} versus the same period last year.",
    },
    "pm": {
        "suffix": "PM",
        "expr": "CALCULATE({b}, DATEADD({d}, -1, MONTH))",
        "format": "inherit",
        "desc": "{base} for the previous month (requires a contiguous date table).",
    },
    "mom": {
        "suffix": "MoM",
        "expr": ("VAR __pm = CALCULATE({b}, DATEADD({d}, -1, MONTH))\n"
                 "RETURN\n"
                 "    IF(NOT ISBLANK(__pm), {b} - __pm)"),
        "format": "inherit",
        "desc": "Absolute change of {base} versus the previous month.",
    },
    "mom_pct": {
        "suffix": "MoM %",
        "expr": ("VAR __pm = CALCULATE({b}, DATEADD({d}, -1, MONTH))\n"
                 "RETURN\n"
                 "    DIVIDE({b} - __pm, __pm)"),
        "format": "+0.0%;-0.0%;0.0%",
        "desc": "Percent change of {base} versus the previous month.",
    },
    "rolling_12m": {
        "suffix": "Rolling 12M",
        "expr": "CALCULATE({b}, DATESINPERIOD({d}, MAX({d}), -12, MONTH))",
        "format": "inherit",
        "desc": "{base} over the trailing 12 months ending at the current date context.",
    },
    "rolling_3m": {
        "suffix": "Rolling 3M",
        "expr": "CALCULATE({b}, DATESINPERIOD({d}, MAX({d}), -3, MONTH))",
        "format": "inherit",
        "desc": "{base} over the trailing 3 months ending at the current date context.",
    },
}

DEFAULT_TI_VARIANTS = ["ytd", "qtd", "mtd", "py", "yoy", "yoy_pct", "mom_pct", "rolling_12m"]


def generate_time_intelligence(base_measure: str, date_column: str,
                               variants: Optional[List[str]] = None,
                               display_folder: Optional[str] = None,
                               base_format: Optional[str] = None) -> List[Dict[str, Any]]:
    """Expand a base measure into time-intelligence measures over a date column.

    base_format is applied where a variant inherits the base's format (pass the base
    measure's formatString when known; None leaves format_string None = model default).
    """
    b = measure_ref(base_measure)
    base = bare_name(base_measure)
    d = column_ref(date_column)
    folder = display_folder or f"Time Intelligence\\{base}"
    out: List[Dict[str, Any]] = []
    unknown = [v for v in (variants or DEFAULT_TI_VARIANTS) if v not in TIME_INTELLIGENCE_VARIANTS]
    if unknown:
        raise ValueError(f"Unknown time-intelligence variant(s): {', '.join(unknown)}. "
                         f"Valid: {', '.join(sorted(TIME_INTELLIGENCE_VARIANTS))}")
    for v in (variants or DEFAULT_TI_VARIANTS):
        t = TIME_INTELLIGENCE_VARIANTS[v]
        fmt = base_format if t["format"] == "inherit" else t["format"]
        out.append(_m(
            f"{base} {t['suffix']}",
            t["expr"].format(b=b, d=d, base=base),
            fmt, folder,
            t["desc"].format(base=base),
        ))
    return out


# ---------------------------------------------------------------- ratios / ranking / stats

def generate_ratios(base_measure: str, dimension_columns: List[str],
                    display_folder: Optional[str] = None,
                    include_all_selected: bool = True) -> List[Dict[str, Any]]:
    """Share-of-total measures: base as a % of the total over each dimension column
    (ALL = grand total ignoring that filter; ALLSELECTED = % of the visible total)."""
    b = measure_ref(base_measure)
    base = bare_name(base_measure)
    folder = display_folder or f"Ratios\\{base}"
    out: List[Dict[str, Any]] = []
    for dim in dimension_columns:
        d = column_ref(dim)
        _, col = split_column_ref(dim)
        out.append(_m(
            f"{base} % of Total {col}",
            f"DIVIDE({b}, CALCULATE({b}, ALL({d})))",
            "0.0%", folder,
            f"{base} as a share of the grand total across all {col} values.",
        ))
        if include_all_selected:
            out.append(_m(
                f"{base} % of Selected {col}",
                f"DIVIDE({b}, CALCULATE({b}, ALLSELECTED({d})))",
                "0.0%", folder,
                f"{base} as a share of the total across the currently selected {col} values.",
            ))
    return out


def generate_ranking(base_measure: str, dimension_columns: List[str],
                     display_folder: Optional[str] = None) -> List[Dict[str, Any]]:
    """Rank of the current dimension member by the base measure (dense, descending)."""
    b = measure_ref(base_measure)
    base = bare_name(base_measure)
    folder = display_folder or f"Ranking\\{base}"
    out: List[Dict[str, Any]] = []
    for dim in dimension_columns:
        d = column_ref(dim)
        _, col = split_column_ref(dim)
        out.append(_m(
            f"{base} Rank by {col}",
            (f"IF(\n    HASONEVALUE({d}),\n"
             f"    RANKX(ALL({d}), {b}, , DESC, DENSE)\n)"),
            "0", folder,
            f"Dense descending rank of the current {col} by {base} (blank at totals).",
        ))
    return out


def generate_column_stats(column: str, display_folder: Optional[str] = None,
                          stats: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    """Basic statistical measures over a numeric column: sum/avg/min/max/median/distinct."""
    d = column_ref(column)
    _, col = split_column_ref(column)
    folder = display_folder or f"Statistics\\{col}"
    templates = {
        "sum": (f"Total {col}", f"SUM({d})", "Sum of {col}."),
        "avg": (f"Average {col}", f"AVERAGE({d})", "Arithmetic mean of {col}."),
        "min": (f"Min {col}", f"MIN({d})", "Minimum value of {col}."),
        "max": (f"Max {col}", f"MAX({d})", "Maximum value of {col}."),
        "median": (f"Median {col}", f"MEDIAN({d})", "Median value of {col}."),
        "distinct": (f"Distinct {col}", f"DISTINCTCOUNT({d})", "Count of distinct {col} values."),
    }
    chosen = stats or ["sum", "avg", "min", "max", "median", "distinct"]
    unknown = [s for s in chosen if s not in templates]
    if unknown:
        raise ValueError(f"Unknown stat(s): {', '.join(unknown)}. Valid: {', '.join(sorted(templates))}")
    out: List[Dict[str, Any]] = []
    for s in chosen:
        name, expr, desc = templates[s]
        out.append(_m(name, expr, "0" if s == "distinct" else None, folder, desc.format(col=col)))
    return out


# ---------------------------------------------------------------- dispatcher

def generate_suite(kind: str, **params) -> List[Dict[str, Any]]:
    """Dispatch to a generator by kind."""
    kind = (kind or "").lower()
    if kind in ("time_intelligence", "time", "ti"):
        return generate_time_intelligence(**params)
    if kind in ("ratios", "ratio", "share"):
        return generate_ratios(**params)
    if kind in ("ranking", "rank"):
        return generate_ranking(**params)
    if kind in ("column_stats", "stats"):
        return generate_column_stats(**params)
    raise ValueError(f"Unknown suite kind '{kind}'. "
                     "Use time_intelligence | ratios | ranking | column_stats.")
