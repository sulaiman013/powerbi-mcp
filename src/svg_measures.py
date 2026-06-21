"""
SVG micro-visual DAX measure generators.

Pure functions that emit ready-to-use DAX measure expressions returning a
`data:image/svg+xml;utf8,...` string. Set the resulting measure's data category to "Image URL"
(ImageUrl) so Power BI renders it inline in tables, matrices, and cards: instant sparklines,
bullet charts, progress bars, and status pills with no custom visual.

All output is original DAX over public SVG and DAX facts. SVG attributes are single-quoted so
they sit inside a double-quoted DAX string literal without any escaping. Entry point:
    generate(kind, **params) -> {"name", "kind", "dax", "notes"}

Kinds: "progress", "bullet", "status_pill", "sparkline".
"""
from typing import Any, Dict, List, Optional

URI_PREFIX = "data:image/svg+xml;utf8,"
KINDS = ("progress", "bullet", "status_pill", "sparkline")


def _q(s: str) -> str:
    """Render a Python string as a DAX double-quoted string literal."""
    return '"' + str(s).replace('"', '""') + '"'


def _measure_ref(name: str) -> str:
    """Accept 'Total Sales' or '[Total Sales]' and return a bracketed measure reference."""
    name = name.strip()
    return name if name.startswith("[") else f"[{name}]"


def _join(parts: List[str]) -> str:
    """Join DAX string-concatenation parts with ' &' on their own lines."""
    return " &\n    ".join(parts)


def progress_bar(value_measure: str, max_value: float = 1.0, min_value: float = 0.0,
                 width: int = 100, height: int = 16, fill: str = "#118DFF",
                 track: str = "#E6E6E6") -> str:
    """A horizontal progress bar from a min..max measure (default 0..1)."""
    v = _measure_ref(value_measure)
    body = _join([
        _q(URI_PREFIX),
        _q(f"<svg xmlns='http://www.w3.org/2000/svg' width='{width}' height='{height}'>"),
        _q(f"<rect width='{width}' height='{height}' rx='3' fill='{track}'/>"),
        _q(f"<rect height='{height}' rx='3' fill='{fill}' width='") + " & _w & " + _q("'/>"),
        _q("</svg>"),
    ])
    return (
        f"VAR _v = {v}\n"
        f"VAR _pct = MIN( MAX( DIVIDE( _v - {min_value}, {max_value} - {min_value} ), 0 ), 1 )\n"
        f"VAR _w = ROUND( _pct * {width}, 0 )\n"
        f"RETURN\n    {body}"
    )


def bullet(value_measure: str, target_measure: str, max_value: float,
           width: int = 120, height: int = 16, fill: str = "#118DFF",
           target: str = "#D64550", track: str = "#E6E6E6") -> str:
    """A bullet chart: a value bar with a vertical target marker, scaled to max_value."""
    v = _measure_ref(value_measure)
    t = _measure_ref(target_measure)
    body = _join([
        _q(URI_PREFIX),
        _q(f"<svg xmlns='http://www.w3.org/2000/svg' width='{width}' height='{height}'>"),
        _q(f"<rect width='{width}' height='{height}' fill='{track}'/>"),
        _q(f"<rect height='{height}' fill='{fill}' width='") + " & _vw & " + _q("'/>"),
        _q(f"<rect width='2' height='{height}' fill='{target}' x='") + " & _tx & " + _q("'/>"),
        _q("</svg>"),
    ])
    return (
        f"VAR _v = {v}\n"
        f"VAR _t = {t}\n"
        f"VAR _vw = ROUND( MIN( MAX( DIVIDE( _v, {max_value} ), 0 ), 1 ) * {width}, 0 )\n"
        f"VAR _tx = ROUND( MIN( MAX( DIVIDE( _t, {max_value} ), 0 ), 1 ) * {width}, 0 )\n"
        f"RETURN\n    {body}"
    )


def status_pill(value_measure: str, thresholds: Optional[List[Dict[str, Any]]] = None,
                width: int = 64, height: int = 18) -> str:
    """A colored status pill chosen by ascending thresholds. Each threshold is
    {"max": <number or None for the top band>, "color": "#hex", "label": "text"}."""
    v = _measure_ref(value_measure)
    thresholds = thresholds or [
        {"max": 0.5, "color": "#D64550", "label": "Low"},
        {"max": 0.8, "color": "#E8A317", "label": "OK"},
        {"max": None, "color": "#3FA34D", "label": "Good"},
    ]
    color_branches, label_branches = [], []
    for th in thresholds:
        if th.get("max") is None:
            continue
        color_branches.append(f"        _v <= {th['max']}, {_q(th['color'])}")
        label_branches.append(f"        _v <= {th['max']}, {_q(th.get('label', ''))}")
    top = thresholds[-1]
    color_switch = "SWITCH( TRUE(),\n" + ",\n".join(color_branches) + f",\n        {_q(top.get('color', '#3FA34D'))} )"
    label_switch = "SWITCH( TRUE(),\n" + ",\n".join(label_branches) + f",\n        {_q(top.get('label', ''))} )"
    body = _join([
        _q(URI_PREFIX),
        _q(f"<svg xmlns='http://www.w3.org/2000/svg' width='{width}' height='{height}'>"),
        _q(f"<rect width='{width}' height='{height}' rx='9' fill='") + " & _color & " + _q("'/>"),
        _q(f"<text x='{width // 2}' y='{height - 5}' font-size='11' fill='#FFFFFF' "
           f"text-anchor='middle' font-family='Segoe UI,Arial'>") + " & _label & " + _q("</text>"),
        _q("</svg>"),
    ])
    return (
        f"VAR _v = {v}\n"
        f"VAR _color = {color_switch}\n"
        f"VAR _label = {label_switch}\n"
        f"RETURN\n    {body}"
    )


def sparkline(axis_column: str, value_measure: str, sort_column: Optional[str] = None,
              width: int = 120, height: int = 28, stroke: str = "#118DFF", pad: int = 2) -> str:
    """A line sparkline of value_measure across axis_column (e.g. 'Date'[Month]).

    Builds an SVG polyline: an x ordinal comes from RANKX over the axis (by sort_column, which
    defaults to axis_column), and y is the value scaled into the chart's value range.
    """
    v = _measure_ref(value_measure)
    sort_expr = sort_column or axis_column
    body = _join([
        _q(URI_PREFIX),
        _q(f"<svg xmlns='http://www.w3.org/2000/svg' width='{width}' height='{height}'>"),
        _q(f"<polyline fill='none' stroke='{stroke}' stroke-width='1.5' points='")
        + " & _points & " + _q("'/>"),
        _q("</svg>"),
    ])
    inner_w = width - 2 * pad
    inner_h = height - 2 * pad
    return (
        f"VAR _axis = VALUES( {axis_column} )\n"
        f"VAR _n = COUNTROWS( _axis )\n"
        f"VAR _data = ADDCOLUMNS( _axis, \"@x\", RANKX( _axis, {sort_expr}, , ASC, DENSE ), \"@y\", {v} )\n"
        f"VAR _min = MINX( _data, [@y] )\n"
        f"VAR _max = MAXX( _data, [@y] )\n"
        f"VAR _points =\n"
        f"    CONCATENATEX(\n"
        f"        _data,\n"
        f"        VAR _px = ROUND( DIVIDE( [@x] - 1, _n - 1 ) * {inner_w} + {pad}, 1 )\n"
        f"        VAR _py = ROUND( {height - pad} - DIVIDE( [@y] - _min, _max - _min ) * {inner_h}, 1 )\n"
        f"        RETURN _px & \",\" & _py,\n"
        f"        \" \",\n"
        f"        [@x], ASC\n"
        f"    )\n"
        f"RETURN\n    {body}"
    )


def generate(kind: str, name: Optional[str] = None, **params) -> Dict[str, Any]:
    """Dispatch to a generator and return {name, kind, dax, notes}."""
    kind = (kind or "").lower()
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
                 "renders the SVG inline (in a table/matrix column or a card).",
    }
