"""
HTML measure generators: small report components as DAX, with no JavaScript.

Pure functions that emit a DAX measure returning an HTML fragment for a visual that renders
HTML (for example an HTML Content custom visual). The measure is evaluated in the visual's
filter context, so the component follows slicers like any other measure.

Rules every generator keeps:
  * Inline CSS only, in single-quoted attributes, so the markup sits inside a double-quoted
    DAX string literal without escaping.
  * Text that comes from the model at query time (category labels, text measures) is
    HTML-escaped inside the DAX: "&" first, or the entities written by the later steps
    would be escaped again. Text known at build time (titles, labels) is escaped here.
  * Colours and font names are parameters and are validated, because they are written into
    a style attribute: a value that could close the attribute is refused.
  * Blank-safe: a blank measure shows ``blank_text`` and an empty list shows ``empty_text``.
  * Every reference is built with dax_tokens, so any table, column or measure name works.

Entry point:
    generate(kind, name=None, **params) -> {"name", "kind", "dax", "notes"}
Kinds: "kpi_card", "kpi_strip", "headline", "bar_list", "table".
"""
import html
import re
import string
from typing import Any, Dict, List, Optional, Sequence, Tuple

import dax_tokens

KINDS = ("kpi_card", "kpi_strip", "headline", "bar_list", "table")

DEFAULT_FONT = "Segoe UI, Arial, sans-serif"
_COLOUR = re.compile(
    r"#(?:[0-9a-fA-F]{3,4}|[0-9a-fA-F]{6}|[0-9a-fA-F]{8})"
    r"|(?:rgb|rgba|hsl|hsla)\([0-9.,%/ a-z]+\)"
    r"|[a-zA-Z]{3,24}")
_FONT = re.compile(r"[A-Za-z0-9 ,_-]+")
# "&" must stay first: every later step writes an "&".
_HTML_ESCAPES = (("&", "&amp;"), ("<", "&lt;"), (">", "&gt;"), ('"', "&quot;"), ("'", "&#39;"))


# ---------------------------------------------------------------- small helpers

def _q(text: str) -> str:
    """A DAX string literal."""
    return '"' + str(text).replace('"', '""') + '"'


def _text(value: Any) -> str:
    """Build-time text, HTML-escaped (quotes included, so it is also safe in an attribute)."""
    return html.escape("" if value is None else str(value), quote=True)


def _measure(name: str) -> str:
    """'Total Sales' or '[Total Sales]' -> a bracketed measure reference."""
    bare = dax_tokens.unbracket(str(name or "").strip())
    if not bare:
        raise ValueError("A measure name is required.")
    return dax_tokens.bracket(bare)


def _column(ref: Any) -> str:
    """'Table[Column]', "'My Table'[Column]" or (table, column) -> a quoted column reference."""
    if isinstance(ref, (tuple, list)) and len(ref) == 2:
        return dax_tokens.column_ref(str(ref[0]), str(ref[1]))
    text = str(ref or "").strip()
    sig = [t for t in dax_tokens.tokenize(text) if t.kind != dax_tokens.WS]
    # The column is the bracketed name that ends the text; everything before it is the table,
    # quoted or not (callers often write Dim Site[Name] without the quotes DAX requires).
    if sig and sig[-1].kind == dax_tokens.BRACKET and sig[-1].end == len(text) and text.endswith("]"):
        table = dax_tokens.unquote_table(text[:sig[-1].start].strip())
        if table and "[" not in table:
            return dax_tokens.column_ref(table, sig[-1].name)
    raise ValueError(f"Cannot read the column reference {ref!r}. Use 'Table[Column]' or (table, column).")


def _colour(value: str, what: str) -> str:
    text = str(value or "").strip()
    if not _COLOUR.fullmatch(text):
        raise ValueError(f"{what}: {value!r} is not a colour this generator accepts. Use #RGB, #RRGGBB, "
                         "#RRGGBBAA, rgb()/rgba()/hsl()/hsla() or a colour name.")
    return text


def _font(value: str) -> str:
    text = str(value or "").strip()
    if not _FONT.fullmatch(text):
        raise ValueError(f"font_family: {value!r} may only hold letters, digits, spaces, commas, "
                         "hyphens and underscores (write font names without quotes).")
    return text


def _px(value: Any, what: str, low: int = 1, high: int = 4000) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError):
        raise ValueError(f"{what} must be a whole number of pixels.") from None
    if not low <= number <= high:
        raise ValueError(f"{what} must be between {low} and {high}.")
    return number


def _format(fmt: Optional[str]) -> str:
    text = "#,0" if fmt is None else str(fmt)
    if not text or "\n" in text or "\r" in text:
        raise ValueError("format_string must be a one-line DAX format string such as \"#,0\" or \"0.0%\".")
    return text


def escape_html(expr: str) -> str:
    """DAX that HTML-escapes the text ``expr`` evaluates to."""
    for find, replacement in _HTML_ESCAPES:
        expr = f"SUBSTITUTE ( {expr}, {_q(find)}, {_q(replacement)} )"
    return expr


def _formatted(expr: str, fmt: str, blank_text: str) -> str:
    """Blank-safe FORMAT. The result is escaped only when the format string itself can
    introduce markup; digits, separators and currency symbols cannot."""
    text = f"FORMAT ( {expr}, {_q(fmt)} )"
    if any(ch in fmt for ch in "&<>\"'"):
        text = escape_html(text)
    return f"IF ( ISBLANK ( {expr} ), {_q(_text(blank_text))}, {text} )"


def _join(parts: Sequence[str], indent: int = 4) -> str:
    """Concatenate DAX string parts, one per line."""
    return (" &\n" + " " * indent).join(parts)


class _Look:
    """Validated colours and font shared by the components."""

    def __init__(self, params: Dict[str, Any]):
        take = params.pop
        self.font = _font(take("font_family", DEFAULT_FONT))
        self.text = _colour(take("text_colour", "#111827"), "text_colour")
        self.muted = _colour(take("muted_colour", "#6B7280"), "muted_colour")
        self.accent = _colour(take("accent", "#2563EB"), "accent")
        self.background = _colour(take("background", "#FFFFFF"), "background")
        self.border = _colour(take("border_colour", "#E5E7EB"), "border_colour")
        self.good = _colour(take("good_colour", "#15803D"), "good_colour")
        self.bad = _colour(take("bad_colour", "#B91C1C"), "bad_colour")
        self.blank_text = str(take("blank_text", "n/a"))
        self.empty_text = str(take("empty_text", "No data for the current filters."))

    def empty(self) -> str:
        return (f"<div style='font-family:{self.font};font-size:13px;color:{self.muted};padding:8px 0'>"
                f"{_text(self.empty_text)}</div>")


def _no_extras(params: Dict[str, Any], kind: str) -> None:
    if params:
        raise ValueError(f"Unknown parameter(s) for {kind}: {', '.join(sorted(params))}")


# ---------------------------------------------------------------- KPI card and strip

def _card(index: int, item: Dict[str, Any], look: _Look, value_px: int,
          flex: bool) -> Tuple[List[str], List[str]]:
    """(VAR lines, HTML parts) for one KPI tile."""
    if not isinstance(item, dict) or not item.get("measure"):
        raise ValueError(f"KPI #{index + 1} needs a measure.")
    v, value = f"_v{index}", f"_value{index}"
    fmt = _format(item.get("format_string"))
    lines = [f"VAR {v} = {_measure(item['measure'])}",
             f"VAR {value} = {_formatted(v, fmt, look.blank_text)}"]
    grow = "flex:1 1 0;min-width:0;" if flex else ""
    parts = [
        _q(f"<div style='{grow}box-sizing:border-box;font-family:{look.font};background:{look.background};"
           f"border:1px solid {look.border};border-left:4px solid {look.accent};border-radius:6px;"
           f"padding:12px 16px'>"
           f"<div style='font-size:12px;color:{look.muted};text-transform:uppercase;letter-spacing:.04em'>"
           f"{_text(item.get('label') or dax_tokens.unbracket(str(item['measure']).strip()))}</div>"
           f"<div style='font-size:{value_px}px;font-weight:600;line-height:1.25;color:{look.text}'>")
        + f" & {value} & " + _q("</div>"),
    ]
    if item.get("comparison_measure"):
        delta, shown = f"_delta{index}", f"_change{index}"
        better = ">=" if item.get("higher_is_better", True) else "<="
        delta_fmt = _format(item.get("delta_format_string") or "0.0%")
        caption = _text(item.get("comparison_label") or "vs comparison")
        lines += [
            f"VAR _c{index} = {_measure(item['comparison_measure'])}",
            f"VAR {delta} = DIVIDE ( {v} - _c{index}, ABS ( _c{index} ) )",
            f"VAR {shown} =",
            "    IF (",
            f"        ISBLANK ( {delta} ),",
            '        "",',
            "        " + _q("<div style='font-size:12px;margin-top:2px;color:")
            + f" & IF ( {delta} {better} 0, {_q(look.good)}, {_q(look.bad)} ) & " + _q("'>"),
            f"            & IF ( {delta} >= 0, UNICHAR ( 9650 ), UNICHAR ( 9660 ) ) & \" \"",
            f"            & IF ( {delta} > 0, \"+\", \"\" ) & FORMAT ( {delta}, {_q(delta_fmt)} )",
            "            & " + _q(f" <span style='color:{look.muted}'>{caption}</span></div>"),
            "    )",
        ]
        parts.append(shown)
    parts.append(_q("</div>"))
    return lines, parts


def kpi_card(value_measure: str, label: Optional[str] = None, format_string: Optional[str] = None,
             comparison_measure: Optional[str] = None, comparison_label: Optional[str] = None,
             higher_is_better: bool = True, delta_format_string: Optional[str] = None,
             value_size: int = 28, **style: Any) -> str:
    """One KPI tile: a label, the formatted value and, with ``comparison_measure``, the change
    against it as a signed percentage in a good or bad colour.

    Style parameters (all optional): font_family, text_colour, muted_colour, accent, background,
    border_colour, good_colour, bad_colour, blank_text.
    """
    look = _Look(style)
    _no_extras(style, "kpi_card")
    item = {"measure": value_measure, "label": label, "format_string": format_string,
            "comparison_measure": comparison_measure, "comparison_label": comparison_label,
            "higher_is_better": higher_is_better, "delta_format_string": delta_format_string}
    lines, parts = _card(0, item, look, _px(value_size, "value_size", 8, 200), False)
    return "\n".join(lines) + "\nRETURN\n    " + _join(parts)


def kpi_strip(items: Sequence[Dict[str, Any]], gap: int = 12, value_size: int = 24, **style: Any) -> str:
    """A row of KPI tiles of equal width. Each item is {"measure", "label"?, "format_string"?,
    "comparison_measure"?, "comparison_label"?, "higher_is_better"?, "delta_format_string"?}."""
    look = _Look(style)
    _no_extras(style, "kpi_strip")
    if not items:
        raise ValueError("kpi_strip needs at least one item.")
    size = _px(value_size, "value_size", 8, 200)
    lines: List[str] = []
    parts = [_q(f"<div style='display:flex;gap:{_px(gap, 'gap', 0, 200)}px;align-items:stretch'>")]
    for i, item in enumerate(items):
        card_lines, card_parts = _card(i, item, look, size, True)
        lines += card_lines
        parts += card_parts
    parts.append(_q("</div>"))
    return "\n".join(lines) + "\nRETURN\n    " + _join(parts)


# ---------------------------------------------------------------- headline

def headline(template: str, values: Dict[str, Dict[str, Any]], font_size: int = 18, **style: Any) -> str:
    """A sentence with live, emphasised figures.

    template: text with {name} placeholders, for example
        "Sales reached {sales}, {growth} above last year."   (write {{ and }} for literal braces)
    values: {name: {"measure", "format_string"?}} for a number, or {name: {"measure", "text": True}}
        for a measure that returns text (it is HTML-escaped, not formatted).
    """
    look = _Look(style)
    _no_extras(style, "headline")
    size = _px(font_size, "font_size", 8, 200)
    lines: List[str] = []
    parts = [_q(f"<div style='font-family:{look.font};font-size:{size}px;line-height:1.4;"
                f"color:{look.text}'>")]
    emphasis = _q(f"<b style='font-weight:600;color:{look.accent}'>")
    used: Dict[str, str] = {}
    try:
        pieces = list(string.Formatter().parse(template or ""))
    except ValueError as e:
        raise ValueError(f"template: {e}. Write {{{{ and }}}} for literal braces.") from None
    pending = ""                          # literal text is gathered so "{{" does not split a literal
    for literal, field, spec, conversion in pieces:
        pending += literal or ""
        if field is None:
            continue
        if pending:
            parts.append(_q(_text(pending)))
            pending = ""
        if not field or spec or conversion or field not in (values or {}):
            raise ValueError(f"template placeholder {{{field}}} needs a matching entry in values "
                             "(plain names only, no format specification).")
        if field not in used:
            item = values[field]
            var = f"_h{len(used)}"
            ref = _measure(item.get("measure") if isinstance(item, dict) else None)
            if item.get("text"):
                shown = f"IF ( ISBLANK ( {var}v ), {_q(_text(look.blank_text))}, {escape_html(var + 'v')} )"
            else:
                shown = _formatted(var + "v", _format(item.get("format_string")), look.blank_text)
            lines += [f"VAR {var}v = {ref}", f"VAR {var} = {shown}"]
            used[field] = var
        parts.append(f"{emphasis} & {used[field]} & " + _q("</b>"))
    if pending:
        parts.append(_q(_text(pending)))
    if not used:
        raise ValueError("template holds no {placeholder}: there is nothing live in the sentence.")
    unused = sorted(set(values or {}) - set(used))
    if unused:
        raise ValueError(f"values has entries the template never uses: {', '.join(unused)}")
    parts.append(_q("</div>"))
    return "\n".join(lines) + "\nRETURN\n    " + _join(parts)


# ---------------------------------------------------------------- ranked bar list

def _label(column: str) -> str:
    return f'IF ( ISBLANK ( {column} ), "(blank)", {escape_html(column)} )'


def bar_list(category: Any, value_measure: str, top_n: int = 10, format_string: Optional[str] = None,
             title: Optional[str] = None, bar_colour: str = "#2563EB", track_colour: str = "#E5E7EB",
             label_width: int = 140, value_width: int = 72, row_height: int = 22, **style: Any) -> str:
    """The top ``top_n`` members of ``category`` by ``value_measure``, each with a bar whose
    width is its share of the largest value. Rows with a blank value are left out. Ties at
    the cut-off are all shown (TOPN keeps them)."""
    look = _Look(style)
    _no_extras(style, "bar_list")
    col, value = _column(category), _measure(value_measure)
    fmt, n = _format(format_string), _px(top_n, "top_n", 1, 500)
    bar, track = _colour(bar_colour, "bar_colour"), _colour(track_colour, "track_colour")
    lw, vw = _px(label_width, "label_width"), _px(value_width, "value_width")
    rh = _px(row_height, "row_height", 8, 200)
    row = _join([
        _q(f"<div style='display:flex;align-items:center;gap:8px;height:{rh}px;font-size:13px;"
           f"color:{look.text}'>"
           f"<div style='flex:0 0 {lw}px;overflow:hidden;white-space:nowrap;text-overflow:ellipsis'>")
        + f" & {_label(col)} & " + _q("</div>"),
        _q(f"<div style='flex:1 1 auto;height:8px;border-radius:4px;background:{track}'>"
           f"<div style='height:8px;border-radius:4px;background:{bar};width:")
        + " & _w & " + _q("%'></div></div>"),
        _q(f"<div style='flex:0 0 {vw}px;text-align:right;font-variant-numeric:tabular-nums'>")
        + f" & {_formatted('[@v]', fmt, look.blank_text)} & " + _q("</div></div>"),
    ], 16)
    heading = (_q(f"<div style='font-size:13px;font-weight:600;color:{look.text};margin-bottom:6px'>"
                  f"{_text(title)}</div>") + " & ") if title else ""
    wrapper = _q(f"<div style='font-family:{look.font}'>")
    return (
        f'VAR _base = FILTER ( ADDCOLUMNS ( VALUES ( {col} ), "@v", {value} ), NOT ISBLANK ( [@v] ) )\n'
        f"VAR _rows = TOPN ( {n}, _base, [@v], DESC, {col}, ASC )\n"
        "VAR _max = MAXX ( _rows, [@v] )\n"
        "VAR _body =\n"
        "    CONCATENATEX (\n"
        "        _rows,\n"
        # The width is a whole number written with FORMAT 0, so no locale can put a comma in the CSS.
        '        VAR _w = FORMAT ( ROUND ( MIN ( MAX ( DIVIDE ( [@v], _max ), 0 ), 1 ) * 100, 0 ), "0" )\n'
        "        RETURN\n"
        f"            {row},\n"
        '        "",\n'
        f"        [@v], DESC, {col}, ASC\n"
        "    )\n"
        "RETURN\n"
        "    IF (\n"
        "        ISBLANK ( _max ),\n"
        f"        {_q(look.empty())},\n"
        f"        {wrapper} & {heading}_body & {_q('</div>')}\n"
        "    )"
    )


# ---------------------------------------------------------------- simple table

def table(category: Any, columns: Sequence[Dict[str, Any]], category_header: Optional[str] = None,
          top_n: int = 20, header_background: str = "#F3F4F6", stripe_colour: Optional[str] = None,
          font_size: int = 13, **style: Any) -> str:
    """A plain HTML table: one row per member of ``category``, one column per measure.

    columns: [{"measure", "header"?, "format_string"?, "align"? ('left' | 'right' | 'center')}].
    Rows are the top ``top_n`` by the first measure, largest first; rows where it is blank are
    left out. ``stripe_colour`` shades every second row.
    """
    look = _Look(style)
    _no_extras(style, "table")
    if not columns:
        raise ValueError("table needs at least one measure column.")
    col, n, size = _column(category), _px(top_n, "top_n", 1, 5000), _px(font_size, "font_size", 8, 100)
    head_bg = _colour(header_background, "header_background")
    stripe = _colour(stripe_colour, "stripe_colour") if stripe_colour else None
    cell = f"padding:4px 8px;border-bottom:1px solid {look.border}"
    heads = [f"<th style='text-align:left;{cell};background:{head_bg}'>"
             f"{_text(category_header or _bare(col))}</th>"]
    adds, cells = [], []
    for i, spec in enumerate(columns):
        if not isinstance(spec, dict) or not spec.get("measure"):
            raise ValueError(f"Column #{i + 1} needs a measure.")
        align = str(spec.get("align") or "right").lower()
        if align not in ("left", "right", "center"):
            raise ValueError(f"Column #{i + 1}: align must be left, right or center.")
        ref = _measure(spec["measure"])
        adds.append(f'"@m{i}", {ref}')
        heads.append(f"<th style='text-align:{align};{cell};background:{head_bg}'>"
                     f"{_text(spec.get('header') or dax_tokens.unbracket(ref))}</th>")
        cells.append(_q(f"<td style='text-align:{align};{cell};font-variant-numeric:tabular-nums'>")
                     + f" & {_formatted(f'[@m{i}]', _format(spec.get('format_string')), look.blank_text)} & "
                     + _q("</td>"))
    row_open = _q("<tr>")
    order = f"[@m0], DESC, {col}, ASC"
    rank = ""
    if stripe:
        # RANKX over the kept rows gives each row its position, so every second one can be shaded.
        rank = f"        VAR _n = RANKX ( _rows, [@m0], , DESC, DENSE )\n        RETURN\n    "
        row_open = (_q("<tr style='background:")
                    + f' & IF ( MOD ( _n, 2 ) = 0, {_q(stripe)}, "transparent" ) & ' + _q("'>"))
    row = _join([row_open,
                 _q(f"<td style='text-align:left;{cell}'>") + f" & {_label(col)} & " + _q("</td>")]
                + cells + [_q("</tr>")], 16 if stripe else 12)
    return (
        f"VAR _base = ADDCOLUMNS ( VALUES ( {col} ), {', '.join(adds)} )\n"
        f"VAR _rows = TOPN ( {n}, FILTER ( _base, NOT ISBLANK ( [@m0] ) ), {order} )\n"
        "VAR _body =\n"
        "    CONCATENATEX (\n"
        "        _rows,\n"
        f"{rank}        {row},\n"
        '        "",\n'
        f"        {order}\n"
        "    )\n"
        "RETURN\n"
        "    IF (\n"
        "        ISBLANK ( COUNTROWS ( _rows ) ),\n"
        f"        {_q(look.empty())},\n"
        "        " + _q(f"<table style='border-collapse:collapse;width:100%;font-family:{look.font};"
                        f"font-size:{size}px;color:{look.text}'>"
                        f"<thead><tr>{''.join(heads)}</tr></thead><tbody>")
        + " & _body & " + _q("</tbody></table>") + "\n"
        "    )"
    )


def _bare(column_ref: str) -> str:
    """The column name inside a reference built by _column."""
    sig = [t for t in dax_tokens.tokenize(column_ref) if t.kind == dax_tokens.BRACKET]
    return sig[-1].name if sig else column_ref


# ---------------------------------------------------------------- dispatcher

_NOTES = ("Create this as a measure and bind it to a visual that renders HTML (for example an HTML Content "
          "custom visual). It holds no script. Text read from the model is HTML-escaped inside the measure; "
          "numbers are written with FORMAT, which follows the model culture for separators. The DAX has "
          "not been evaluated here: check it in DAX query view before publishing.")


def generate(kind: str, name: Optional[str] = None, **params: Any) -> Dict[str, Any]:
    """Dispatch to a generator and return {name, kind, dax, notes}."""
    kind = (kind or "").lower().replace("-", "_")
    builders = {"kpi_card": kpi_card, "kpi_strip": kpi_strip, "headline": headline,
                "bar_list": bar_list, "table": table}
    kind = {"kpi": "kpi_card", "card": "kpi_card", "strip": "kpi_strip", "bars": "bar_list"}.get(kind, kind)
    if kind not in builders:
        raise ValueError(f"Unknown HTML measure kind '{kind}'. Use one of: {', '.join(KINDS)}")
    try:
        dax = builders[kind](**params)
    except TypeError as e:
        raise ValueError(f"{kind}: {e}") from None
    return {"name": name or f"{kind} (HTML)", "kind": kind, "dax": dax, "notes": _NOTES}
