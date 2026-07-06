"""
TMDL authoring primitives: emit measures, date-dimension tables, calculation groups, and
hierarchies as TMDL text that matches what Power BI Desktop itself serializes.

Shapes are doc-verified against the TMDL language reference and real Desktop PBIP exports:
- Structural indentation is TABS (1 per level). A table child (measure/column/hierarchy/
  partition) sits at 1 tab; its properties at 2 tabs.
- A measure DESCRIPTION is written as /// doc-comment line(s) immediately above the
  declaration at the same indent (there is NO description: property).
- Multi-line expressions: nothing follows '='; the body starts on the next line TWO tabs
  deeper than the declaration; property lines then continue ONE tab deeper than the
  declaration (the indent drop terminates the expression).
- Property casing is camelCase (formatString, displayFolder, sortByColumn, dataCategory);
  bare flags (isHidden, isKey, isNameInferred) stand alone with no value.
- Calculated-table columns are declared with isNameInferred + sourceColumn: [Name]
  (BRACKETED - unique to calculated tables), and a date table is marked with BOTH
  dataCategory: Time on the table AND isKey on the date column.
- lineageTag is optional; Desktop generates missing tags on save, so new objects omit them.

Used by the PBIP connector's add_measures / create_date_table / add_calculation_group /
add_hierarchy methods.
"""
import datetime
import re
from typing import Any, Dict, List, Optional

TAB = "\t"
_NEEDS_QUOTE = re.compile(r"[\s.='':]|^\d")


def quote_name(name: str) -> str:
    """Single-quote a TMDL object name when needed (whitespace, dot, =, :, quote, or a
    leading digit); internal single quotes are escaped by doubling."""
    if _NEEDS_QUOTE.search(name or ""):
        return "'" + str(name).replace("'", "''") + "'"
    return name


def _doc_comment(description: Optional[str], indent: str) -> List[str]:
    """Render a description as /// doc-comment lines (wrapped ~100 chars) at the given indent."""
    if not description:
        return []
    words = str(description).split()
    lines, cur = [], ""
    for w in words:
        if cur and len(cur) + 1 + len(w) > 100:
            lines.append(cur)
            cur = w
        else:
            cur = f"{cur} {w}" if cur else w
    if cur:
        lines.append(cur)
    return [f"{indent}/// {ln}" for ln in lines]


def render_measure(measure: Dict[str, Any], depth: int = 1) -> str:
    """Render one measure block at the given tab depth (1 = direct table child).

    measure: {name, expression, format_string?, display_folder?, description?, is_hidden?}
    """
    ind = TAB * depth
    prop = TAB * (depth + 1)
    body = TAB * (depth + 2)
    name = quote_name(measure["name"])
    expr = str(measure["expression"]).strip()
    lines: List[str] = []
    lines.extend(_doc_comment(measure.get("description"), ind))
    if "\n" in expr:
        lines.append(f"{ind}measure {name} =")
        for raw in expr.splitlines():
            lines.append(f"{body}{raw.rstrip()}" if raw.strip() else "")
    else:
        lines.append(f"{ind}measure {name} = {expr}")
    if measure.get("format_string"):
        lines.append(f"{prop}formatString: {measure['format_string']}")
    if measure.get("display_folder"):
        lines.append(f"{prop}displayFolder: {measure['display_folder']}")
    if measure.get("is_hidden"):
        lines.append(f"{prop}isHidden")
    return "\n".join(lines)


def render_measures(measures: List[Dict[str, Any]], depth: int = 1) -> str:
    """Render several measure blocks separated by blank lines (the Desktop convention)."""
    return "\n\n".join(render_measure(m, depth) for m in measures)


def render_hierarchy(name: str, levels: List[str], depth: int = 1) -> str:
    """Render a hierarchy block: levels are column names in drill order (top first).
    Level ordering in TMDL is purely positional (no ordinal property)."""
    ind = TAB * depth
    lvl = TAB * (depth + 1)
    prop = TAB * (depth + 2)
    lines = [f"{ind}hierarchy {quote_name(name)}"]
    for col in levels:
        lines.append("")
        lines.append(f"{lvl}level {quote_name(col)}")
        lines.append(f"{prop}column: {quote_name(col)}")
    return "\n".join(lines)


# ---------------------------------------------------------------- date dimension

def _date_literal(iso: str) -> str:
    d = datetime.date.fromisoformat(str(iso).strip())
    return f"DATE({d.year}, {d.month}, {d.day})"


def _inferred_column(name: str, data_type: Optional[str] = None, *, is_key: bool = False,
                     format_string: Optional[str] = None, sort_by: Optional[str] = None,
                     hidden: bool = False, date_annotation: bool = False) -> str:
    """A calculated-table column block: isNameInferred + bracketed sourceColumn."""
    ind, prop = TAB, TAB * 2
    lines = [f"{ind}column {quote_name(name)}"]
    if data_type:
        lines.append(f"{prop}dataType: {data_type}")
    if is_key:
        lines.append(f"{prop}isKey")
    if format_string:
        lines.append(f"{prop}formatString: {format_string}")
    lines.append(f"{prop}summarizeBy: none")
    if sort_by:
        lines.append(f"{prop}sortByColumn: {quote_name(sort_by)}")
    if hidden:
        lines.append(f"{prop}isHidden")
    lines.append(f"{prop}isNameInferred")
    lines.append(f"{prop}sourceColumn: [{name}]")
    lines.append("")
    lines.append(f"{prop}annotation SummarizationSetBy = User")
    if date_annotation:
        lines.append("")
        lines.append(f"{prop}annotation UnderlyingDateTimeDataType = Date")
    return "\n".join(lines)


def build_date_table(name: str = "Date", start_date: str = "2015-01-01",
                     end_date: str = "2030-12-31",
                     fiscal_year_start_month: Optional[int] = None) -> str:
    """A complete date-dimension table .tmdl: an ADDCOLUMNS(CALENDAR(...)) calculated table,
    marked as a date table (dataCategory: Time + isKey), with sorted month/quarter labels
    and optional fiscal-year columns."""
    fm = int(fiscal_year_start_month) if fiscal_year_start_month else None
    if fm is not None and not (1 <= fm <= 12):
        raise ValueError("fiscal_year_start_month must be 1-12")

    dax_cols = [
        '"Year", YEAR([Date])',
        '"Quarter Number", QUARTER([Date])',
        '"Quarter", "Q" & QUARTER([Date])',
        '"Month Number", MONTH([Date])',
        '"Month", FORMAT([Date], "MMM")',
        '"Week Number", WEEKNUM([Date])',
        '"Day", DAY([Date])',
    ]
    columns = [
        _inferred_column("Date", "dateTime", is_key=True, format_string="Short Date",
                         date_annotation=True),
        _inferred_column("Year", "int64", format_string="0"),
        _inferred_column("Quarter Number", "int64", hidden=True),
        _inferred_column("Quarter", "string", sort_by="Quarter Number"),
        _inferred_column("Month Number", "int64", hidden=True),
        _inferred_column("Month", "string", sort_by="Month Number"),
        _inferred_column("Week Number", "int64", format_string="0"),
        _inferred_column("Day", "int64", format_string="0"),
    ]
    if fm and fm != 1:
        dax_cols.append(f'"Fiscal Year", YEAR([Date]) + IF(MONTH([Date]) >= {fm}, 1, 0)')
        dax_cols.append(f'"Fiscal Quarter Number", QUOTIENT(MOD(MONTH([Date]) - {fm} + 12, 12), 3) + 1')
        dax_cols.append('"Fiscal Quarter", "FQ" & [Fiscal Quarter Number]')
        columns.append(_inferred_column("Fiscal Year", "int64", format_string="0"))
        columns.append(_inferred_column("Fiscal Quarter Number", "int64", hidden=True))
        columns.append(_inferred_column("Fiscal Quarter", "string", sort_by="Fiscal Quarter Number"))

    # Fiscal Quarter references [Fiscal Quarter Number] within the same ADDCOLUMNS, which DAX
    # does not allow; inline the expression instead.
    dax_cols = [c.replace('[Fiscal Quarter Number]',
                          f'(QUOTIENT(MOD(MONTH([Date]) - {fm} + 12, 12), 3) + 1)') if fm else c
                for c in dax_cols]

    body = TAB * 4  # partition source body: two levels deeper than 'source =' (Desktop convention)
    dax_lines = [f"{body}ADDCOLUMNS(",
                 f"{body}    CALENDAR({_date_literal(start_date)}, {_date_literal(end_date)}),"]
    for i, c in enumerate(dax_cols):
        comma = "," if i < len(dax_cols) - 1 else ""
        dax_lines.append(f"{body}    {c}{comma}")
    dax_lines.append(f"{body})")

    parts = [
        f"/// Date dimension generated by powerbi-mcp ({start_date} to {end_date}).",
        f"table {quote_name(name)}",
        f"{TAB}dataCategory: Time",
        "",
        "\n\n".join(columns),
        "",
        f"{TAB}partition {quote_name(name)} = calculated",
        f"{TAB * 2}mode: import",
        f"{TAB * 2}source =",
        "\n".join(dax_lines),
        "",
    ]
    return "\n".join(parts)


# ---------------------------------------------------------------- calculation groups

def time_intelligence_calc_items(date_column: str) -> List[Dict[str, Any]]:
    """A standard time-intelligence calculation-item set over SELECTEDMEASURE()."""
    d = date_column.strip()
    if "[" not in d:
        table, _, col = d.partition(".")
        d = f"'{table.strip()}'[{col.strip()}]"
    return [
        {"name": "Current", "expression": "SELECTEDMEASURE()"},
        {"name": "YTD", "expression": f"CALCULATE(SELECTEDMEASURE(), DATESYTD({d}))"},
        {"name": "QTD", "expression": f"CALCULATE(SELECTEDMEASURE(), DATESQTD({d}))"},
        {"name": "MTD", "expression": f"CALCULATE(SELECTEDMEASURE(), DATESMTD({d}))"},
        {"name": "PY", "expression": f"CALCULATE(SELECTEDMEASURE(), SAMEPERIODLASTYEAR({d}))"},
        {"name": "YoY",
         "expression": (f"VAR __py = CALCULATE(SELECTEDMEASURE(), SAMEPERIODLASTYEAR({d}))\n"
                        "RETURN\n"
                        "    IF(NOT ISBLANK(__py), SELECTEDMEASURE() - __py)")},
        {"name": "YoY %",
         "expression": (f"VAR __py = CALCULATE(SELECTEDMEASURE(), SAMEPERIODLASTYEAR({d}))\n"
                        "RETURN\n"
                        "    DIVIDE(SELECTEDMEASURE() - __py, __py)"),
         "format_string_expression": '"0.0%"'},
    ]


def build_calculation_group(name: str, items: List[Dict[str, Any]],
                            column_name: str = "Calculation", precedence: int = 1) -> str:
    """A complete calculation-group table .tmdl.

    Each item: {name, expression, format_string_expression?}. Item order in the file defines
    the ordinal. The selector column is a string column over sourceColumn Name.
    """
    ind, prop, body = TAB, TAB * 2, TAB * 3
    lines = [f"table {quote_name(name)}", f"{ind}calculationGroup"]
    if precedence is not None:
        lines.append(f"{prop}precedence: {int(precedence)}")
    for it in items:
        lines.append("")
        iname = quote_name(it["name"])
        expr = str(it["expression"]).strip()
        # Desktop always serializes calculation-item expressions multi-line (body two tabs
        # deeper than the calculationItem line); match it for clean round-trips.
        lines.append(f"{prop}calculationItem {iname} =")
        for raw in expr.splitlines():
            lines.append(f"{TAB * 4}{raw.rstrip()}" if raw.strip() else "")
        if it.get("format_string_expression"):
            lines.append("")
            lines.append(f"{body}formatStringDefinition = {it['format_string_expression']}")
    lines.append("")
    lines.append(f"{ind}column {quote_name(column_name)}")
    lines.append(f"{prop}dataType: string")
    lines.append(f"{prop}summarizeBy: none")
    lines.append(f"{prop}sourceColumn: Name")
    lines.append(f"{prop}sortByColumn: Ordinal")
    lines.append("")
    lines.append(f"{prop}annotation SummarizationSetBy = Automatic")
    lines.append("")
    lines.append(f"{ind}column Ordinal")
    lines.append(f"{prop}dataType: int64")
    lines.append(f"{prop}formatString: 0")
    lines.append(f"{prop}isHidden")
    lines.append(f"{prop}summarizeBy: sum")
    lines.append(f"{prop}sourceColumn: Ordinal")
    lines.append("")
    lines.append(f"{prop}annotation SummarizationSetBy = Automatic")
    lines.append("")
    return "\n".join(lines)


# ---------------------------------------------------------------- table-file helpers

def find_table_end(content: str) -> int:
    """Index in content where new table children can be appended (end of the single-table
    file, trimmed of trailing whitespace)."""
    return len(content.rstrip())


def append_block(content: str, block: str) -> str:
    """Append a rendered block to a table .tmdl, separated by one blank line, preserving a
    trailing newline."""
    head = content.rstrip()
    return head + "\n\n" + block.rstrip() + "\n"
