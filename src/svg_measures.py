"""
SVG micro-visual DAX measure generators.

Pure functions that emit ready-to-use DAX measure expressions returning a
`data:image/svg+xml;utf8,...` string. Set the resulting measure's data category to "Image URL"
(ImageUrl) so Power BI renders it inline in tables, matrices, and cards: instant sparklines,
bullet charts, progress bars, and status pills with no custom visual.

Every measure returns BLANK() when its value measure is blank, so a table or matrix keeps
suppressing rows without data instead of drawing an empty bar on each of them.

How the text is encoded, outermost first:
  DAX string   SVG attributes are single-quoted, so the markup sits in a double-quoted DAX
               literal as is; a double quote in a label is doubled.
  data URI     '%' is written %25 and '#' is written %23. A raw '#' starts the URI fragment and
               cuts the image off at the first hex colour in a strict URI parser.
  XML          label text has & < > escaped.
Coordinates are whole numbers (the sparkline uses a 10x viewBox for sub-pixel precision), so no
decimal separator is ever converted to text and the result does not depend on the model locale.

All numeric and colour parameters are validated; a bad one raises ValueError before any DAX is
produced. Entry point:
    generate(kind, **params) -> {"name", "kind", "dax", "notes"}

Kinds: "progress", "bullet", "status_pill", "sparkline".
"""
import math
import re
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Tuple

import dax_tokens as dt
from dax_functions import is_known_function
from dax_generator import split_column_ref

URI_PREFIX = "data:image/svg+xml;utf8,"
KINDS = ("progress", "bullet", "status_pill", "sparkline")
_SVG_OPEN = "<svg xmlns='http://www.w3.org/2000/svg' width='{w}' height='{h}'{extra}>"
_COLOR_RE = re.compile(
    r"#(?:[0-9a-fA-F]{3}|[0-9a-fA-F]{4}|[0-9a-fA-F]{6}|[0-9a-fA-F]{8})"   # hex
    r"|[a-zA-Z]{3,30}"                                                     # a named colour
    r"|(?:rgb|rgba|hsl|hsla)\([0-9.,%\s/]{1,60}\)")                        # functional notation
_SCALE = 10     # sparkline viewBox units per pixel


# ---------------------------------------------------------------- encoding helpers

def _q(s: str) -> str:
    """Render a Python string as a DAX double-quoted string literal."""
    return '"' + str(s).replace('"', '""') + '"'


def _uri(text: str) -> str:
    """Escape the two characters that are not literal inside a data URI."""
    return str(text).replace("%", "%25").replace("#", "%23")


def _xml_text(text: str) -> str:
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _lit(svg_fragment: str) -> str:
    """A DAX string literal holding a piece of SVG markup, URI-escaped."""
    return _q(_uri(svg_fragment))


def _join(parts: List[str]) -> str:
    """Join DAX string-concatenation parts with ' &' on their own lines."""
    return " &\n    ".join(parts)


# ---------------------------------------------------------------- validation helpers

def _number(value, what: str, *, integer: bool = False, minimum: Optional[float] = None,
            exclusive: bool = False):
    """A finite number from an int, float, Decimal or numeric string; ValueError otherwise."""
    num = None
    if isinstance(value, bool):
        pass
    elif isinstance(value, (int, float, Decimal)):
        num = value
    elif isinstance(value, str) and value.strip():
        try:
            num = Decimal(value.strip())
        except InvalidOperation:
            num = None
    if num is None or math.isnan(float(num)) or math.isinf(float(num)):
        raise ValueError(f"{what} must be a number (got {value!r}).")
    if integer:
        if float(num) != int(num):
            raise ValueError(f"{what} must be a whole number (got {value!r}).")
        num = int(num)
    elif not isinstance(num, int):
        num = float(num)
    if minimum is not None and (num < minimum or (exclusive and num == minimum)):
        bound = "greater than" if exclusive else "at least"
        raise ValueError(f"{what} must be {bound} {minimum} (got {value!r}).")
    return num


def _num(value) -> str:
    """A DAX numeric literal: plain digits, never exponent notation."""
    if isinstance(value, int):
        return str(value)
    text = format(Decimal(repr(float(value))), "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def _size(width, height) -> Tuple[int, int]:
    return (_number(width, "width", integer=True, minimum=1),
            _number(height, "height", integer=True, minimum=1))


def _color(value, what: str) -> str:
    """A CSS colour that is safe inside a single-quoted SVG attribute."""
    if not isinstance(value, str) or not _COLOR_RE.fullmatch(value.strip()):
        raise ValueError(f"{what} must be a colour such as '#118DFF', 'red' or 'rgb(17,141,255)' "
                         f"(got {value!r}).")
    return value.strip()


def _measure_ref(name: str) -> str:
    """A measure reference from 'Total Sales', '[Total Sales]' or Table[Measure].

    ']' in a plain name is escaped as ']]'. An expression is rejected: the generated DAX reads
    the value once into a VAR and tests it for BLANK, which only makes sense for one measure.
    """
    if not isinstance(name, str) or not name.strip():
        raise ValueError(f"A measure name is required (got {name!r}).")
    s = name.strip()
    tokens = dt.tokenize(s)
    if len(tokens) == 1 and tokens[0].kind == dt.BRACKET and dt.bracket(tokens[0].name) == s:
        return s
    if (len(tokens) == 2 and tokens[0].kind in (dt.IDENT, dt.QNAME) and tokens[1].kind == dt.BRACKET
            and dt.bracket(tokens[1].name) == tokens[1].text):
        return s                                    # Table[Measure] / 'Table'[Measure]
    # '[a] + [b]' starts like a reference but is not one; 'SUM(T[c])' is a function call.
    is_call = any(t.kind == dt.IDENT and is_known_function(t.text) and i + 1 < len(tokens)
                  and tokens[i + 1].kind == dt.OP and tokens[i + 1].text == "("
                  for i, t in enumerate(tokens))
    if s.startswith("[") or is_call:
        raise ValueError(f"{name!r} is not a measure name. Pass the measure's name, or one [Measure] "
                         "reference with any ']' in the name doubled; put an expression in a measure first.")
    return dt.bracket(s)


def _column(ref: str, what: str) -> Tuple[str, str]:
    try:
        return split_column_ref(ref)
    except ValueError as e:
        raise ValueError(f"{what}: {e}") from None


def _wrap(variables: List[str], svg_parts: List[str], condition: str) -> str:
    """VARs, the SVG text in its own VAR, and a RETURN that is BLANK without data."""
    lines = list(variables)
    lines.append("VAR _svg =\n    " + _join(svg_parts))
    lines.append(f"RETURN\n    IF( {condition}, _svg )")
    return "\n".join(lines)


# ---------------------------------------------------------------- generators

def progress_bar(value_measure: str, max_value: float = 1.0, min_value: float = 0.0,
                 width: int = 100, height: int = 16, fill: str = "#118DFF",
                 track: str = "#E6E6E6") -> str:
    """A horizontal progress bar from a min..max measure (default 0..1)."""
    v = _measure_ref(value_measure)
    width, height = _size(width, height)
    hi, lo = _number(max_value, "max_value"), _number(min_value, "min_value")
    if hi <= lo:
        raise ValueError(f"max_value ({max_value!r}) must be greater than min_value ({min_value!r}).")
    fill, track = _color(fill, "fill"), _color(track, "track")
    numerator = "_v" if lo == 0 else f"_v - {_num(lo)}"
    return _wrap(
        [f"VAR _v = {v}",
         f"VAR _pct = MIN( MAX( DIVIDE( {numerator}, {_num(hi - lo)} ), 0 ), 1 )",
         f"VAR _w = ROUND( _pct * {width}, 0 )"],
        [_q(URI_PREFIX),
         _lit(_SVG_OPEN.format(w=width, h=height, extra="")),
         _lit(f"<rect width='{width}' height='{height}' rx='3' fill='{track}'/>"),
         _lit(f"<rect height='{height}' rx='3' fill='{fill}' width='") + " & _w & " + _lit("'/>"),
         _lit("</svg>")],
        "NOT ISBLANK( _v )")


def bullet(value_measure: str, target_measure: str, max_value: float,
           width: int = 120, height: int = 16, fill: str = "#118DFF",
           target: str = "#D64550", track: str = "#E6E6E6") -> str:
    """A bullet chart: a value bar with a vertical target marker, scaled to max_value.
    The marker is left out when the target measure is blank."""
    v = _measure_ref(value_measure)
    t = _measure_ref(target_measure)
    width, height = _size(width, height)
    if width < 3:
        raise ValueError("width must be at least 3 for a bullet chart.")
    hi = _num(_number(max_value, "max_value", minimum=0, exclusive=True))
    fill, target, track = _color(fill, "fill"), _color(target, "target"), _color(track, "track")
    marker = (_lit(f"<rect width='2' height='{height}' fill='{target}' x='") + " & _tx & " + _lit("'/>"))
    return _wrap(
        [f"VAR _v = {v}",
         f"VAR _t = {t}",
         f"VAR _vw = ROUND( MIN( MAX( DIVIDE( _v, {hi} ), 0 ), 1 ) * {width}, 0 )",
         # the marker is 2 wide: scale to width - 2 so it stays on the canvas at 100%
         f"VAR _tx = ROUND( MIN( MAX( DIVIDE( _t, {hi} ), 0 ), 1 ) * {width - 2}, 0 )",
         f"VAR _marker = IF( NOT ISBLANK( _t ), {marker} )"],
        [_q(URI_PREFIX),
         _lit(_SVG_OPEN.format(w=width, h=height, extra="")),
         _lit(f"<rect width='{width}' height='{height}' fill='{track}'/>"),
         _lit(f"<rect height='{height}' fill='{fill}' width='") + " & _vw & " + _lit("'/>"),
         "_marker",
         _lit("</svg>")],
        "NOT ISBLANK( _v )")


_DEFAULT_THRESHOLDS = [
    {"max": 0.5, "color": "#D64550", "label": "Low"},
    {"max": 0.8, "color": "#E8A317", "label": "OK"},
    {"max": None, "color": "#3FA34D", "label": "Good"},
]


def _bands(thresholds) -> List[Tuple[Optional[str], str, str]]:
    """Validated (max literal or None, colour, label) bands, lowest first."""
    if thresholds is None:
        thresholds = _DEFAULT_THRESHOLDS
    if not isinstance(thresholds, (list, tuple)) or not thresholds:
        raise ValueError("thresholds must be a non-empty list of {max, color, label} bands.")
    bands = []
    previous = None
    for i, th in enumerate(thresholds):
        if not isinstance(th, dict):
            raise ValueError(f"thresholds[{i}] must be an object with max, color and label.")
        color = _color(th.get("color"), f"thresholds[{i}].color")
        label = th.get("label", "")
        if label is None:
            label = ""
        if not isinstance(label, (str, int, float)) or isinstance(label, bool):
            raise ValueError(f"thresholds[{i}].label must be text (got {label!r}).")
        if th.get("max") is None:
            if i != len(thresholds) - 1:
                raise ValueError("Only the last threshold may have max = null (it is the top band).")
            bands.append((None, color, str(label)))
            continue
        top = _number(th["max"], f"thresholds[{i}].max")
        if previous is not None and top <= previous:
            raise ValueError(f"thresholds must be in ascending order of max; thresholds[{i}].max "
                             f"({th['max']!r}) is not above the band before it.")
        previous = top
        bands.append((_num(top), color, str(label)))
    return bands


def status_pill(value_measure: str, thresholds: Optional[List[Dict[str, Any]]] = None,
                width: int = 64, height: int = 18) -> str:
    """A colored status pill chosen by ascending thresholds. Each threshold is
    {"max": <number, or None for the top band>, "color": "#hex", "label": "text"}.
    A value at or below a band's max falls in that band. Without a max = None band, the last
    band also takes every value above its max."""
    v = _measure_ref(value_measure)
    width, height = _size(width, height)
    bands = _bands(thresholds)
    *lower, top = bands

    def switch(values: List[str]) -> str:
        if not lower:
            # A single band: a constant (a SWITCH with no condition pairs would be invalid DAX).
            return values[-1]
        pairs = ",\n".join(f"        _v <= {band[0]}, {value}" for band, value in zip(lower, values))
        return "SWITCH( TRUE(),\n" + pairs + f",\n        {values[-1]} )"

    colors = [_lit(color) for _, color, _ in bands]
    labels = [_lit(_xml_text(label)) for _, _, label in bands]
    return _wrap(
        [f"VAR _v = {v}",
         f"VAR _color = {switch(colors)}",
         f"VAR _label = {switch(labels)}"],
        [_q(URI_PREFIX),
         _lit(_SVG_OPEN.format(w=width, h=height, extra="")),
         _lit(f"<rect width='{width}' height='{height}' rx='{min(9, height // 2)}' fill='")
         + " & _color & " + _lit("'/>"),
         _lit(f"<text x='{width // 2}' y='{max(1, height - 5)}' font-size='11' fill='#FFFFFF' "
              "text-anchor='middle' font-family='Segoe UI,Arial'>") + " & _label & " + _lit("</text>"),
         _lit("</svg>")],
        "NOT ISBLANK( _v )")


def sparkline(axis_column: str, value_measure: str, sort_column: Optional[str] = None,
              width: int = 120, height: int = 28, stroke: str = "#118DFF", pad: int = 2) -> str:
    """A line sparkline of value_measure across axis_column (e.g. 'Date'[Month] or Date.Month).

    The points are ordered by sort_column when given (a text month name sorted by its month
    number), else by the axis column itself. The sort column must be in the axis column's table:
    the axis is then built as the distinct (axis, sort) pairs, so the sort value is available in
    the row context that ranks the points. Points without a value are skipped.
    """
    v = _measure_ref(value_measure)
    width, height = _size(width, height)
    pad = _number(pad, "pad", integer=True, minimum=0)
    if 2 * pad >= min(width, height):
        raise ValueError(f"pad ({pad}) leaves no room in a {width}x{height} sparkline.")
    stroke = _color(stroke, "stroke")
    table, column = _column(axis_column, "axis_column")
    axis = dt.column_ref(table, column)
    order = axis
    axis_table = f"VALUES( {axis} )"
    if sort_column is not None and str(sort_column).strip():
        sort_table, sort_name = _column(sort_column, "sort_column")
        if sort_table.casefold() != table.casefold():
            raise ValueError(f"sort_column {sort_column!r} must be in the same table as axis_column "
                             f"{axis_column!r}.")
        if sort_name.casefold() != column.casefold():
            order = dt.column_ref(table, sort_name)
            axis_table = f"SUMMARIZE( {dt.quote_table(table, force=True)}, {axis}, {order} )"
    inner_w = (width - 2 * pad) * _SCALE
    inner_h = (height - 2 * pad) * _SCALE
    left, bottom = pad * _SCALE, (height - pad) * _SCALE
    # DIVIDE's third argument centres the point when the range is zero (one point, or a flat line).
    return _wrap(
        [f"VAR _axis = {axis_table}",
         "VAR _data =\n"
         "    FILTER(\n"
         f"        ADDCOLUMNS( _axis, \"@x\", RANKX( _axis, {order}, , ASC, DENSE ), \"@y\", {v} ),\n"
         "        NOT ISBLANK( [@y] )\n"
         "    )",
         "VAR _minx = MINX( _data, [@x] )",
         "VAR _maxx = MAXX( _data, [@x] )",
         "VAR _miny = MINX( _data, [@y] )",
         "VAR _maxy = MAXX( _data, [@y] )",
         "VAR _points =\n"
         "    CONCATENATEX(\n"
         "        _data,\n"
         f"        VAR _px = ROUND( DIVIDE( [@x] - _minx, _maxx - _minx, 0.5 ) * {inner_w} + {left}, 0 )\n"
         f"        VAR _py = ROUND( {bottom} - DIVIDE( [@y] - _miny, _maxy - _miny, 0.5 ) * {inner_h}, 0 )\n"
         "        RETURN _px & \",\" & _py,\n"
         "        \" \",\n"
         "        [@x], ASC\n"
         "    )"],
        [_q(URI_PREFIX),
         _lit(_SVG_OPEN.format(w=width, h=height,
                               extra=f" viewBox='0 0 {width * _SCALE} {height * _SCALE}'")),
         _lit(f"<polyline fill='none' stroke='{stroke}' stroke-width='{int(1.5 * _SCALE)}' "
              "stroke-linejoin='round' stroke-linecap='round' points='")
         + " & _points & " + _lit("'/>"),
         _lit("</svg>")],
        "NOT ISEMPTY( _data )")


def generate(kind: str, name: Optional[str] = None, **params) -> Dict[str, Any]:
    """Dispatch to a generator and return {name, kind, dax, notes}."""
    kind = (kind or "").lower() if isinstance(kind, str) else ""
    if kind == "progress":
        dax = progress_bar(**params)
    elif kind == "bullet":
        dax = bullet(**params)
    elif kind in ("status_pill", "status", "pill"):
        kind = "status_pill"
        dax = status_pill(**params)
    elif kind == "sparkline":
        dax = sparkline(**params)
    else:
        raise ValueError(f"Unknown SVG measure kind '{kind}'. Use one of: {', '.join(KINDS)}")
    return {
        "name": name or f"{kind} (SVG)",
        "kind": kind,
        "dax": dax,
        "notes": "Create this as a measure, then set its data category to 'Image URL' so Power BI "
                 "renders the SVG inline (in a table/matrix column or a card). The measure returns "
                 "BLANK when there is no value, so rows without data stay hidden.",
    }
