"""
Deneb (Vega-Lite) specifications with Power BI's interactivity wired in.

Deneb is the AppSource visual that renders a Vega-Lite spec inside Power BI. Drawing the chart is
the easy half. The half people get wrong is making it behave like a native visual: cross-filter
the page when a mark is clicked, show what other visuals highlight, raise Power BI's own tooltips
(report-page tooltips included) and offer drillthrough from the right-click menu.

    generate_deneb_spec(kind, fields, **options) -> {
        "spec", "config", "fields", "dataset", "interactivity", "element", "notes", ...}

returns the spec with those parts in place, the config Deneb stores beside it, the fields to bind,
the Deneb settings the spec relies on, and a ``deneb`` element ready for the report spec
(report_spec.py). Nothing is written and nothing is evaluated: render it in Desktop and look.

How the interactivity is wired
  cross-filter     Deneb adds __selected__ ("on" | "off" | "neutral") to every row and resolves a
                   click through the row index (__row__) on the clicked mark's datum. Bars, points,
                   cells, ticks and labels are drawn one per row and keep that index; unselected
                   ones are dimmed by an opacity condition on __selected__. A line or an area is
                   one mark for many rows, so it is drawn from copies of the rows that carry
                   neither the index nor a dataset field name: a click on it selects nothing.
  cross-highlight  Deneb adds <measure>__highlight. Each value mark is drawn twice: the full
                   value dimmed behind, the highlighted value in front. The front mark reads the
                   highlight through a calculated field that falls back to the full value when
                   Deneb does not supply one, so the chart is whole with the setting off. A
                   scatter point does not move and a heatmap keeps its colour scale: there the
                   front mark is left out for the rows the highlight does not reach.
  tooltips         One tooltip expression shared by the layers (a line or an area opts out). It
                   hands Deneb the raw value of every bound field under the field's own name,
                   which is what makes Deneb format the numbers with the model's format string.
                   Under a cross-highlight it adds the highlighted value of each measure.
  context menu     Nothing in the spec: Deneb resolves the right-clicked mark to its row. It is a
                   setting, listed in "interactivity".

Where each Deneb name used here comes from (https://deneb.guide/docs/<page>, Deneb 2.0)
  dataset "dataset"; the field-name rule (\\ " . [ ] become _)          dataset
  __selected__ and its three values; the 1 to 250 selection limit       interactivity-selection
  <measure>__highlight                                                   interactivity-highlight
  <measure>__format                                                      formatting
  pbiColor(index | name, shade); pbiColorNominal / Linear / Divergent    schemes
  pbiFormat(value, format, options)                                      formatting
  vega.enableTooltips, enableContextMenu, enableContextMenuSelector,
    enableHighlight, enableSelection, selectionMode,
    selectionMaxDataPoints; stateManagement.supportFieldConfiguration    pbir-guide
The settings were also checked against Deneb's capabilities.json; how a click, a right-click and a
tooltip are resolved against its source (interactivity/data-point.ts, tooltip.ts); and the
patterns against the "Interactive bar chart" template Deneb ships.
"""
import copy
import json
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Mapping, Optional, Set, Tuple, Union

import pbir_authoring
from pbip_report import deneb_field_name

Json = Dict[str, Any]
FieldRef = Union[str, Mapping[str, Any]]

KINDS = ("bar", "column", "stacked_bar", "stacked_column", "line", "area", "scatter", "heatmap",
         "lollipop", "bullet")
# The keyword options of generate_deneb_spec. A caller that passes arguments through hands over
# everything it was given except "kind" and "fields": a name that is not here is refused.
OPTIONS = ("title", "sort", "color", "value_format", "top_n", "labels", "label_units", "category_type",
           "facet_columns", "panel_width", "panel_height", "width", "cross_filter", "cross_highlight",
           "tooltips", "context_menu", "max_selected", "config")
ARGUMENTS = ("kind", "fields") + OPTIONS
SORTS = ("value_desc", "value_asc", "category_asc", "category_desc", "none")
THEME_COLOR_NAMES = ("min", "middle", "max", "negative", "neutral", "positive", "bad", "good")
HEATMAP_SCHEMES = {"linear": "pbiColorLinear", "divergent": "pbiColorDivergent"}

DATASET = "dataset"
ROW_FIELD = "__row__"
SELECTED_FIELD = "__selected__"
HIGHLIGHT_SUFFIX = "__highlight"
FORMAT_SUFFIX = "__format"
MAX_SELECTED_LIMIT = 250
DIMMED = 0.3

# The key of the report spec's ``deneb`` element that sets each Deneb setting; the returned
# element is built from this table. The settings without a key need none: the element always
# writes selectionMode "simple", Deneb's default covers enableContextMenuSelector, and a visual
# the report spec creates gets every supporting field of its measures (Deneb reads a visual
# with no version stamp as a pre-2.0 project).
ELEMENT_KEYS: Dict[str, str] = {
    "enableTooltips": "tooltips",
    "enableContextMenu": "context_menu",
    "enableSelection": "selection",
    "selectionMaxDataPoints": "max_selected",
    "enableHighlight": "cross_highlight",
}

_COLUMN, _MEASURE, _EITHER = "column", "measure", "either"
_CATEGORY_VALUE = (("facet", _COLUMN, False), ("category", _COLUMN, True), ("value", _MEASURE, True))
_STACKED = (("facet", _COLUMN, False), ("category", _COLUMN, True), ("series", _COLUMN, True),
            ("value", _MEASURE, True))
_TREND = (("facet", _COLUMN, False), ("category", _COLUMN, True), ("series", _COLUMN, False),
          ("value", _MEASURE, True))
# kind -> (role, what it accepts, required), in the order the fields are bound. Power BI sorts
# the rows by the columns in that order, each by the model's own sort order, so the first
# column's order is always the model's. The facet goes first for that reason: its panels then
# arrive in order.
_ROLES: Dict[str, Tuple[Tuple[str, str, bool], ...]] = {
    "bar": _CATEGORY_VALUE, "column": _CATEGORY_VALUE, "lollipop": _CATEGORY_VALUE,
    "stacked_bar": _STACKED, "stacked_column": _STACKED,
    "line": _TREND, "area": _TREND,
    "bullet": (("category", _COLUMN, True), ("value", _MEASURE, True), ("target", _MEASURE, True)),
    "scatter": (("facet", _COLUMN, False), ("category", _COLUMN, False), ("series", _COLUMN, False),
                ("x", _EITHER, True), ("y", _EITHER, True), ("size", _MEASURE, False)),
    "heatmap": (("x", _COLUMN, True), ("y", _COLUMN, True), ("value", _MEASURE, True)),
}
_VERTICAL = {"column", "stacked_column"}
_STACKED_KINDS = {"stacked_bar", "stacked_column"}
_TREND_KINDS = {"line", "area"}
_RANKED_KINDS = {"bar", "column", "lollipop", "bullet", "stacked_bar", "stacked_column"}
_LABEL_KINDS = {"bar", "column", "lollipop", "bullet", "heatmap"}
_CATEGORY_TYPES = ("ordinal", "temporal", "quantitative")
_FACET_DEFAULTS = {"facet_columns": 3, "panel_width": 180, "panel_height": 120}
_PANEL_GAP = 16
# Room, in pixels. Beside the plot: the category labels (cut at _LABEL_LIMIT in small multiples,
# at _LABEL_SHARE of the visual's width otherwise) or the value axis with its title. Around a
# row of panels: the padding of the view and the visual. A category under a slot narrower than
# _NARROW_SLOT is written upright, cut at _UPRIGHT_SHARE of the visual's height. A legend entry
# is a symbol and a label cut at _LEGEND_LABEL. A data label is taken to need _LABEL_CHAR a
# character and a line _LABEL_LINE high.
_LABEL_LIMIT = 90
_LABEL_SHARE = 0.4
_UPRIGHT_SHARE = 0.3
_LABEL_GAP = 8
_NARROW_SLOT = 48
_CATEGORY_AXIS = _LABEL_LIMIT + _LABEL_GAP
_VALUE_AXIS = 72
_VIEW_PADDING = 40
_LEGEND_LABEL = 100
_LEGEND_ENTRY = _LEGEND_LABEL + 24
_LABEL_CHAR = 6
_LABEL_LINE = 12
_TIP = "a highlighted value in the tooltip"
# The parameters that hold the size Deneb gives the visual.
VISUAL_WIDTH, VISUAL_HEIGHT = "visualWidth", "visualHeight"
_FIELD_KEYS = {"table", "entity", "field", "property", "column", "measure", "kind", "aggregation"}
# Members every JavaScript object inherits. With a field of one of these names Vega-Lite fails
# to compile the spec or Vega refuses the expression that reads it (each was tried).
_OBJECT_MEMBERS = {"constructor", "hasOwnProperty", "isPrototypeOf", "propertyIsEnumerable", "toLocaleString",
                   "toString", "valueOf", "__proto__", "__defineGetter__", "__defineSetter__",
                   "__lookupGetter__", "__lookupSetter__"}

# The standard config of the deneb-visuals reference, with these changes. No fixed colour: the
# grid keeps Vega's own grey, so nothing here stops following the theme. The view has neither
# fill nor stroke, so the plot's background (a panel's, in small multiples) is not something a
# click can land on. Category labels read horizontally, as on a native chart. On any axis a
# label that would overlap the one before it is left out, and two numbers keep a gap between
# them. A number near the end of a horizontal axis is drawn inside the plot's width, because
# the layout is fitted once and a label hanging over the edge would be cut off.
_CONFIG: Json = {
    "autosize": {"type": "fit", "contains": "padding"},
    "view": {"stroke": None, "fill": None},
    "font": "Segoe UI",
    "axis": {"ticks": False, "grid": False, "domain": False, "labelFontSize": 11, "titleFontSize": 12,
             "labelOverlap": "greedy"},
    "axisX": {"labelAngle": 0},
    "axisQuantitative": {"tickCount": 5, "grid": True, "gridDash": [2, 4], "labelSeparation": 6},
    "axisXQuantitative": {"labelFlush": 24},
    "legend": {"orient": "top", "labelFontSize": 11, "labelLimit": _LEGEND_LABEL},
}


@dataclass(frozen=True)
class _Field:
    role: str
    table: str
    name: str          # the name in the model
    kind: str          # "column" | "measure"
    data: str          # the name Deneb gives it in the dataset

    @property
    def key(self) -> Tuple[str, str, str]:
        return self.table, self.name, self.kind


# ---------------------------------------------------------------------------------------------
# names
# ---------------------------------------------------------------------------------------------
def _js(text: str) -> str:
    """Text as a string literal of a Vega expression."""
    return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"


def datum(name: str) -> str:
    """A dataset field inside a Vega expression, in the bracket form that suits every name:
    datum['Order Lines']. A backslash or a single quote in the name is escaped."""
    return f"datum[{_js(name)}]"


def vl_field(name: str) -> str:
    """A dataset field as a Vega-Lite ``field`` value. Deneb has already replaced what Vega-Lite
    reads as a path (. [ ] and the backslash). What is left is the single quote: where a path
    segment may begin, it opens a quoted one ("Units' sold" is refused as an unclosed quote), so
    every one is escaped."""
    return name.replace("'", "\\'")


def _ref_text(table: str, name: str) -> str:
    """'Table.Field', with the table quoted when its own name would be split."""
    if "." in table or table.startswith("'"):
        table = "'" + table.replace("'", "''") + "'"
    return f"{table}.{name}"


def _num(value: float) -> str:
    return str(int(value)) if float(value).is_integer() else repr(float(value))


# ---------------------------------------------------------------------------------------------
# input
# ---------------------------------------------------------------------------------------------
def _flag(name: str, value: Any) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{name} must be true or false, not {value!r}")
    return value


def _whole(name: str, value: Any, low: int, high: int) -> int:
    """A whole number in range. 3.0 counts: a JSON number may arrive as a float."""
    number = isinstance(value, (int, float)) and not isinstance(value, bool)
    if not number or value != value or value in (float("inf"), float("-inf")) or int(value) != value \
            or not low <= value <= high:
        raise ValueError(f"{name} must be a whole number from {low} to {high}, not {value!r}")
    return int(value)


def _model_lookup(model_fields: Any) -> Optional[Dict[str, Tuple[str, Dict[str, Tuple[str, Optional[str]]]]]]:
    """model_fields, in either shape the report spec takes, keyed without regard to case:
    {table: (Table, {field: (Field, "column" | "measure" | None)})}."""
    if model_fields is None:
        return None
    if not isinstance(model_fields, Mapping):
        raise ValueError("model_fields must be {table: [field names]} or {table: {field: {\"kind\": "
                         "\"column\" | \"measure\"}}}")
    out: Dict[str, Tuple[str, Dict[str, Tuple[str, Optional[str]]]]] = {}
    for table, fields in model_fields.items():
        entries: Dict[str, Tuple[str, Optional[str]]] = {}
        if isinstance(fields, Mapping):
            for name, meta in fields.items():
                kind = str(meta.get("kind", "")).lower() if isinstance(meta, Mapping) else ""
                entries[str(name).lower()] = (str(name), kind if kind in (_COLUMN, _MEASURE) else None)
        elif isinstance(fields, (list, tuple, set, frozenset)):
            for name in fields:
                entries[str(name).lower()] = (str(name), None)
        else:
            raise ValueError(f"model_fields[{table!r}] must be a list of field names or an object of fields")
        out[str(table).lower()] = (str(table), entries)
    return out


def _parse_field(kind: str, role: str, ref: Any, accepts: str, model: Optional[Mapping[str, Any]]) -> _Field:
    where = f"fields.{role}"
    declared: Optional[str] = None
    if isinstance(ref, Mapping):
        unknown = sorted(str(k) for k in ref if k not in _FIELD_KEYS)
        if unknown:
            raise ValueError(f"{where}: unknown key(s) {unknown} in a field; write "
                             "{\"column\": \"Table.Name\"} or {\"measure\": \"Table.Name\"} (\"table\" may "
                             "be given separately)")
        if ref.get("aggregation") is not None:
            raise ValueError(
                f"{where}: an aggregated column cannot be used. Power BI would hand it to Deneb under a "
                "name of its own making (\"Sum of Amount\", translated with the report language), so the "
                "spec could not refer to it. Create a measure and pass {\"measure\": \"Table.Name\"}")
        named = [key for key in ("column", "measure", "field", "property") if ref.get(key) is not None]
        if len(named) != 1:
            raise ValueError(f"{where}: name the field once, as \"column\" or as \"measure\" "
                             f"(found {named if named else 'neither'})")
        if named[0] in (_COLUMN, _MEASURE):
            declared = named[0]
        if ref.get("kind") is not None:
            stated = str(ref["kind"]).lower()
            if stated not in (_COLUMN, _MEASURE) or (declared and declared != stated):
                raise ValueError(f"{where}: kind {ref['kind']!r} must be \"column\" or \"measure\", and "
                                 "agree with the key the name is given under")
            declared = stated
        table = ref.get("table") if ref.get("table") is not None else ref.get("entity")
        name = ref[named[0]]
        if not isinstance(name, str) or not isinstance(table, (str, type(None))):
            raise ValueError(f"{where}: the table and the field name must be text, not {ref!r}")
        if table is None:
            table, name = pbir_authoring.split_table_field(name)
    elif isinstance(ref, str):
        table, name = pbir_authoring.split_table_field(ref)
    else:
        raise ValueError(f"{where}: a field is 'Table.Field' or an object such as "
                         f"{{\"measure\": \"Table.Name\"}}, not {type(ref).__name__}")
    table, name = table.strip(), name.strip()
    if not table or not name:
        raise ValueError(f"{where}: {ref!r} is not a field; write it as 'Table.Field'")
    if any(ord(ch) < 32 for ch in table + name):
        raise ValueError(f"{where}: {ref!r} holds a control character, which no model name can")
    if model is not None:
        found = model.get(table.lower())
        hit = found[1].get(name.lower()) if found else None
        if not hit:
            reason = "no such table" if not found else "no such field in that table"
            raise ValueError(f"{where}: {table}[{name}] is not in the model ({reason})")
        table, (name, model_kind) = found[0], hit
        if model_kind:
            if declared and declared != model_kind:
                raise ValueError(f"{where}: {table}[{name}] is a {model_kind} in the model, not a {declared}")
            declared = model_kind
    if accepts == _EITHER:
        if declared is None:
            raise ValueError(f"{where}: cannot tell whether {table}[{name}] is a column or a measure; write "
                             f"{{\"measure\": \"{_ref_text(table, name)}\"}} or {{\"column\": ...}}")
        accepts = declared
    elif declared and declared != accepts:
        hint = (" A column here would be grouped, not summed: create a measure for it."
                if accepts == _MEASURE else " A measure cannot label or split the marks.")
        raise ValueError(f"{where}: {table}[{name}] is a {declared}, but \"{role}\" of a {kind} chart takes "
                         f"a {accepts}.{hint}")
    data = deneb_field_name(name)
    if data in _OBJECT_MEMBERS:
        raise ValueError(f"{where}: a field called \"{data}\" cannot be drawn. Every JavaScript object has a "
                         "member of that name, and Vega-Lite and Vega stop on it. Rename the field in the "
                         "model, or in the visual's Values well")
    return _Field(role, table, name, accepts, data)


def _parse_fields(kind: str, fields: Any, model: Optional[Mapping[str, Any]]) -> Dict[str, _Field]:
    roles = _ROLES[kind]
    if not isinstance(fields, Mapping):
        raise ValueError("fields must be an object of role: field, for example {\"category\": "
                         "\"Product.Category\", \"value\": {\"measure\": \"Sales.Total Sales\"}}")
    known = [role for role, _, _ in roles]
    for role in fields:
        if role not in known:
            raise ValueError(f"fields.{role}: a {kind} chart has no \"{role}\" role; its roles are {known}")
    out: Dict[str, _Field] = {}
    for role, accepts, required in roles:
        if fields.get(role) is None:
            if required:
                raise ValueError(f"fields.{role} is required for a {kind} chart (roles: {known})")
            continue
        out[role] = _parse_field(kind, role, fields[role], accepts, model)
    by_data: Dict[str, _Field] = {}
    for f in out.values():
        other = by_data.setdefault(f.data, f)
        if other.key != f.key:
            raise ValueError(
                f"fields.{other.role} ({other.table}[{other.name}]) and fields.{f.role} "
                f"({f.table}[{f.name}]) would both be called \"{f.data}\" in Deneb's dataset, which names a "
                "field by its display name alone; rename one of them in the model")
    return out


def _theme_color(color: Any) -> Tuple[str, float]:
    """(pbiColor's first argument as expression text, shade) of a ``color`` option."""
    shade: Any = 0
    if isinstance(color, Mapping):
        if "theme" not in color or set(color) - {"theme", "shade"}:
            raise ValueError("color as an object is {\"theme\": index or name, \"shade\": -1 to 1}")
        color, shade = color["theme"], color.get("shade", 0)
    if isinstance(shade, bool) or not isinstance(shade, (int, float)) or not -1 <= shade <= 1:
        raise ValueError("color shade is a number from -1 (darker) to 1 (lighter)")
    if color is None:
        return "0", shade
    if isinstance(color, int) and not isinstance(color, bool) and 0 <= color <= 99:
        return str(color), shade
    if isinstance(color, str) and color in THEME_COLOR_NAMES:
        return f"'{color}'", shade
    raise ValueError(
        f"color {color!r} is not a theme colour. Give the zero-based index of a theme data colour (0 to 99), "
        f"or one of {list(THEME_COLOR_NAMES)}. A fixed colour such as \"#1F6FEB\" is refused on purpose: the "
        "visual would stop following the report theme. Put the colour in the theme instead")


def _merge(base: Json, over: Mapping[str, Any]) -> Json:
    out = copy.deepcopy(base)
    for key, value in over.items():
        if isinstance(value, Mapping) and isinstance(out.get(key), dict):
            out[key] = _merge(out[key], value)
        else:
            out[key] = copy.deepcopy(value)
    return out


# ---------------------------------------------------------------------------------------------
# the spec
# ---------------------------------------------------------------------------------------------
Pick = Callable[[_Field], str]


class _Chart:
    """One chart being built: its fields and options, and the pieces every kind shares."""

    def __init__(self, kind: str, fields: Dict[str, _Field], *, sort: str, color: Any,
                 formats: Dict[str, str], top_n: Optional[int], labels: bool, label_units: bool,
                 category_type: str, cross_filter: bool, cross_highlight: bool, tooltips: bool):
        self.kind = kind
        self.f = fields
        self.sort = sort
        self.sort_field = ""                 # the field a value sort orders the categories by
        self.formats = formats
        self.top_n = top_n
        self.labels = labels
        self.label_units = label_units
        self.category_type = category_type
        self.cross_filter = cross_filter
        self.tooltips = tooltips
        self.facet = fields.get("facet")
        self.series = fields.get("series")
        self.own_axis = self.facet is not None and top_n is not None   # each panel ranks its own categories
        self.transforms: List[Json] = []
        self.params: Dict[str, Json] = {}
        self.outside = "width"               # the visual's width where there is no container to ask
        self.reads_highlight: Set[str] = set()    # measures whose __highlight the spec reads
        self.reads_format: Set[str] = set()       # fields whose __format the spec reads
        self.formatted: List[str] = []            # the parts written with it, for the note
        self._names = {f.data for f in fields.values()}
        self._shown: Dict[str, str] = {}
        self._path: Dict[str, str] = {}
        self.color_arg, self.color_shade = ("0", 0.0) if kind == "heatmap" else _theme_color(color)
        # A column has no highlight value, so a chart that plots only columns has nothing to pair.
        plotted = [fields[role] for role in ("value", "x", "y") if role in fields]
        self.highlight = cross_highlight and any(f.kind == _MEASURE for f in plotted)

    # -- names the spec makes for itself
    def derive(self, name: str) -> str:
        """A name for a calculated field that no bound field already has."""
        while name in self._names:
            name += "_"
        self._names.add(name)
        return name

    def highlight_of(self, f: _Field) -> str:
        self.reads_highlight.add(f.data)
        return datum(f.data + HIGHLIGHT_SUFFIX)

    def shown(self, f: _Field) -> str:
        """The field a front mark draws: the highlighted value of a measure when cross-highlight
        is wired, falling back to the full value while Deneb supplies no highlight field (the
        setting is off, or the field's highlight value was switched off)."""
        if not self.highlight or f.kind != _MEASURE:
            return f.data
        if f.data not in self._shown:
            name = self.derive(f"shown_{f.role}")
            highlight = self.highlight_of(f)
            self.transforms.append({"calculate": f"isDefined({highlight}) ? {highlight} : {datum(f.data)}",
                                    "as": name})
            self._shown[f.data] = name
        return self._shown[f.data]

    # -- numbers as text, through Deneb's Power BI formatter
    def _reads_format(self, f: _Field, part: str) -> None:
        self.reads_format.add(f.data)
        if part not in self.formatted:
            self.formatted.append(part)

    def row_format(self, f: _Field, part: str) -> str:
        """The model's format string of a field, as the row carries it ('' when it does not).
        ``part`` names what is written with it."""
        self._reads_format(f, part)
        return f"({datum(f.data + FORMAT_SUFFIX)} || '')"

    def model_format(self, f: _Field, part: str) -> str:
        """The same for an axis or a legend, which has no row: a parameter read off the first."""
        name = f"{f.role}Format"
        if name not in self.params:
            first = f"data('{DATASET}')[0][{_js(f.data + FORMAT_SUFFIX)}]"
            self.params[name] = {"name": name,
                                 "expr": f"length(data('{DATASET}')) && isString({first}) ? {first} : ''"}
        self._reads_format(f, part)
        return name

    def _container(self, name: str, side: int, outside: str) -> str:
        if name not in self.params:
            size = f"containerSize()[{side}]"
            self.params[name] = {"name": name, "expr": f"isFinite({size}) ? {size} : {outside}"}
        return name

    def visual_width(self) -> str:
        """The parameter holding the width Deneb gives the visual: that of the element it
        renders into. Outside Deneb, with no such element, the view's own width stands in."""
        return self._container(VISUAL_WIDTH, 0, self.outside)

    def visual_height(self) -> str:
        """The same for the height, of a view that is not small multiples."""
        return self._container(VISUAL_HEIGHT, 1, "height")

    @staticmethod
    def unit(scale: str) -> str:
        """pbiFormat options that give every number on a scale the same display unit, the one of
        the scale's largest value. Below a thousand there is no unit, and nothing is passed: the
        option would only make a format-less number print its floating-point error."""
        largest = f"max(abs(domain('{scale}')[0]), abs(domain('{scale}')[1]))"
        return f"{largest} >= 1000 ? {{value: {largest}}} : {{}}"

    def scale_labels(self, f: _Field, scale: str) -> str:
        """Axis or legend labels: the given format as it is, or the model's with one unit."""
        if f.role in self.formats:
            return f"pbiFormat(datum.value, {_js(self.formats[f.role])})"
        part = "the legend" if scale == "color" else "the axis"
        return f"pbiFormat(datum.value, {self.model_format(f, part)}, {self.unit(scale)})"

    def label_text(self, f: _Field, scale: str) -> str:
        """The expression for a data label: the given format, or the model's, in full unless
        units were asked for. A blank value gets no label."""
        value = datum(f.data)
        if f.role in self.formats:
            text = f"pbiFormat({value}, {_js(self.formats[f.role])})"
        elif self.label_units:
            text = f"pbiFormat({value}, {self.row_format(f, 'the labels')}, {self.unit(scale)})"
        else:
            text = f"pbiFormat({value}, {self.row_format(f, 'the labels')})"
        return f"isValid({value}) ? {text} : ''"

    # -- encodings
    def value_axis(self, f: _Field, scale: str, title: bool = True) -> Json:
        axis = {"title": f.name if title else None, "labelExpr": self.scale_labels(f, scale)}
        channel: Json = {"type": "quantitative", "axis": axis}
        if self.labels and self.kind != "heatmap":
            # The longest bar ends at the plot's edge, so its label overhangs the plot by the
            # label's own length whatever the plot's width, and the one fit makes room for it.
            # On a rounded-up axis the overhang would grow as the fit narrows the plot.
            channel["scale"] = {"nice": False}
        return channel

    def category_channel(self, f: _Field, scale: str = "nominal", sort: Any = Ellipsis) -> Json:
        channel: Json = {"field": vl_field(f.data), "type": scale, "axis": {"title": None}}
        if scale in ("nominal", "ordinal"):
            channel["sort"] = self.category_sort() if sort is Ellipsis else sort
        elif scale == "quantitative":
            channel["axis"]["labelExpr"] = "pbiFormat(datum.value, '')"      # 2021, not 2,021.0
        return channel

    def category_sort(self) -> Any:
        if self.sort == "none":
            return None                      # the order Power BI delivered the rows in
        order = "ascending" if self.sort.endswith("asc") else "descending"
        if self.sort.startswith("category"):
            return order
        # A sort on a named field survives the layers sharing one axis; "-x" does not, because
        # each layer would sort by its own field.
        return {"field": vl_field(self.sort_field), "op": "max", "order": order}

    def arrival_sort(self, name: str, within: _Field) -> Json:
        """The delivered order of a column that is not the first one bound. Its values arrive in
        order inside each group of the column before it, but a group may lack some of them, so
        first appearance is not the order. Each value is placed by the furthest position it
        takes in any group."""
        position = self.derive(name)
        self.transforms.append({"window": [{"op": "row_number", "as": position}],
                                "groupby": [vl_field(within.data)]})
        return {"field": position, "op": "max", "order": "ascending"}

    def series_color(self) -> Json:
        # The legend would otherwise take the opacity of the first layer, the dimmed twin.
        return {"field": vl_field(self.series.data), "type": "nominal", "title": self.series.name,
                "scale": {"scheme": "pbiColorNominal"}, "legend": {"symbolOpacity": 1}}

    def mark_color(self, shade: float = 0) -> Json:
        shade = max(-1.0, min(1.0, self.color_shade + shade))
        arguments = f"{self.color_arg}, {_num(shade)}" if shade else self.color_arg
        return {"expr": f"pbiColor({arguments})"}

    def tooltip(self) -> Json:
        """Every bound field once, raw, under its dataset name: Deneb formats a number itself,
        with the model's format string, when the entry is a number and is named after a field.
        Under a cross-highlight a measure gets a second entry for its highlighted value."""
        entries: List[str] = []
        seen: List[_Field] = []
        for f in self.f.values():
            if any(f.data == other.data for other in seen):
                continue
            seen.append(f)
            value = datum(f.data)
            if f.role == "category" and self.category_type == "temporal":
                shown = f"timeFormat({value}, '%b %d, %Y')"
            else:
                shown = value
            entries.append(f"{_js(f.data)}: isValid({value}) ? {shown} : '(Blank)'")
        expr = "{" + ", ".join(entries) + "}"
        measures = [f for f in seen if f.kind == _MEASURE] if self.highlight else []
        extra = []
        for f in measures:
            title = "Highlighted" if len(measures) == 1 else f"Highlighted {f.data}"
            while any(title == other.data for other in seen):
                title += " "
            value, highlight = datum(f.data), self.highlight_of(f)
            extra.append(f"isValid({highlight}) && {highlight} !== {value} ? "
                         f"{{{_js(title)}: pbiFormat({highlight}, {self.row_format(f, _TIP)})}} : {{}}")
        if extra:
            expr = f"merge({expr}, {', '.join(extra)})"
        return {"value": {"expr": expr}}

    # -- layers
    def layer(self, mark: Json, encoding: Json, *, back: bool = False, paired: bool = True,
              opacity: float = 1, hide: Optional[str] = None) -> Json:
        """One layer of marks drawn one per row, with the interactivity that belongs on it.

        back    the dimmed full-value mark of a highlight pair.
        paired  False for a mark that has no dimmed twin behind it (a label).
        hide    a test for rows this front mark must not show (a row outside the highlight).
        """
        mark, encoding = dict(mark), dict(encoding)
        if back:
            mark["opacity"] = DIMMED
            return {"mark": mark, "encoding": encoding}
        conditions = []
        if self.cross_filter:
            # In front of its dimmed twin an unselected mark disappears and the twin shows
            # through; a mark on its own is dimmed.
            conditions.append({"test": {"field": SELECTED_FIELD, "equal": "off"},
                               "value": 0 if self.highlight and paired else DIMMED})
        if hide:
            conditions.append({"test": hide, "value": 0})
        if conditions:
            encoding["opacity"] = {"condition": conditions[0] if len(conditions) == 1 else conditions,
                                   "value": opacity}
        elif opacity != 1:
            mark["opacity"] = opacity
        return {"mark": mark, "encoding": encoding}

    def path(self, mark: Json, value: str) -> Json:
        """A line or an area. Vega picks a path as one item whose datum is its first row, so drawn
        from the rows themselves a click anywhere on it would select that row. It is drawn from
        copies instead, grouped under names of the spec's own: with no row index and no dataset
        field name on them, Deneb resolves a click, a right-click or a hover to nothing."""
        names = self.path_names()
        fields = [{"op": "max", "field": vl_field(value), "as": names["y"]}]
        if self.sort_field:                  # the field the x axis is ordered by must come along
            fields.append({"op": "max", "field": self.sort_field, "as": self.sort_field})
        encoding: Json = {"x": {"field": names["x"]}, "y": {"field": names["y"]}}
        if self.series:
            encoding["color"] = {"field": names["series"]}
        if self.tooltips:
            encoding["tooltip"] = None       # opts this layer out of the tooltip the layers share
        groupby = [names["x"]] + ([names["series"]] if self.series else [])
        return {"mark": mark, "transform": [{"aggregate": fields, "groupby": groupby}], "encoding": encoding}

    def path_names(self) -> Dict[str, str]:
        """The names a path is drawn under: copies of the category and the series, made on the
        rows, and the value aggregated from them."""
        if not self._path:
            self._path = {"x": self.derive("path_x"), "y": self.derive("path_y")}
            self.transforms.append({"calculate": datum(self.f["category"].data), "as": self._path["x"]})
            if self.series:
                self._path["series"] = self.derive("path_series")
                self.transforms.append({"calculate": datum(self.series.data), "as": self._path["series"]})
        return self._path

    def pair(self, make: Callable[[Pick, bool], List[Json]]) -> List[Json]:
        """The marks ``make`` builds, drawn the way Power BI draws a cross-highlight: once with the
        full values, dimmed, and once in front with the highlighted values."""
        layers = make(lambda f: f.data, True) if self.highlight else []
        return layers + make(self.shown, False)

    def label(self, f: _Field, channel: str, *, gap: int = 4, past: Optional[_Field] = None) -> Json:
        """Data labels past the value end of a bar, or above a column. The label of a negative
        value stands at zero instead, on the free side: clear of its bar, and clear of the
        category labels that lie beyond the bar's far end. ``past`` is a second measure drawn
        on the same row (a bullet's target) that the label must clear as well."""
        at = self.derive("label_at")
        ends = [datum(f.data)] + ([datum(past.data)] if past else []) + ["0"]
        self.transforms.append({"calculate": f"max({', '.join(ends)})", "as": at})
        text = self.label_text(f, channel)
        if channel == "y":
            place = {"align": "center", "baseline": "bottom", "dy": -gap}
            text = self.fitted(text, "child_x" if self.own_axis else "x")
        else:
            place = {"align": "left", "baseline": "middle", "dx": gap}
        encoding = {channel: {"field": at}, "text": {"value": {"expr": text}}}
        return self.layer({"type": "text", **place}, encoding, paired=False)

    @staticmethod
    def fitted(text: str, scale: str, low: str = "") -> str:
        """A label's expression, giving no text when the label is wider than its column or its
        cell (or the cell is too ``low`` for a line): it would run into its neighbours. An
        expression cannot measure text, so the width is taken as _LABEL_CHAR pixels a
        character, a little more than a digit takes."""
        test = f"length({text}) * {_LABEL_CHAR} > bandwidth('{scale}')" + (f" || {low}" if low else "")
        return f"{test} ? '' : {text}"

    # -- transforms shared by several kinds
    @property
    def panel(self) -> List[str]:
        return [vl_field(self.facet.data)] if self.facet else []

    def keep_top(self, by: str, op: str) -> None:
        if self.top_n is None:
            return
        rank = self.derive("value_rank")
        window: Json = {"window": [{"op": op, "as": rank}],
                        "sort": [{"field": vl_field(by), "order": "descending"}]}
        if self.panel:
            window["groupby"] = self.panel
        self.transforms += [window, {"filter": f"{datum(rank)} <= {self.top_n}"}]

    def category_order(self) -> Any:
        """The sort of the category axis. In small multiples the category is the second column
        bound, so its delivered order is worked out rather than taken from first appearance."""
        if self.sort == "none" and self.facet:
            order = self.arrival_sort("category_position", self.facet)
            self.sort_field = order["field"]
            return order
        return self.category_sort()


def _bars(c: _Chart) -> Tuple[Json, List[Json]]:
    category, value = c.f["category"], c.f["value"]
    v, k = ("y", "x") if c.kind in _VERTICAL else ("x", "y")
    c.sort_field = value.data
    c.keep_top(value.data, "rank")
    # stack: null, or Vega-Lite stacks the bars of a category and writes its own fields on the rows
    encoding = {k: c.category_channel(category, sort=c.category_order()),
                v: {**c.value_axis(value, v), "stack": None}}

    def make(pick: Pick, back: bool) -> List[Json]:
        return [c.layer({"type": "bar", "color": c.mark_color()}, {v: {"field": vl_field(pick(value))}},
                        back=back)]

    layers = c.pair(make)
    if c.labels:
        layers.append(c.label(value, v))
    return encoding, layers


def _stacked(c: _Chart) -> Tuple[Json, List[Json]]:
    category, series, value = c.f["category"], c.f["series"], c.f["value"]
    v, k = ("y", "x") if c.kind in _VERTICAL else ("x", "y")
    group = [vl_field(category.data)] + c.panel
    if c.top_n is not None or c.sort.startswith("value"):
        total = c.derive("category_total")
        c.transforms.append({"joinaggregate": [{"op": "sum", "field": vl_field(value.data), "as": total}],
                             "groupby": group})
        c.sort_field = total
        c.keep_top(total, "dense_rank")
    order = c.category_order()
    # The stack is laid out here, not by the encoding, so that the highlighted part of a segment
    # can start where its own segment starts.
    start, end = c.derive("stack_start"), c.derive("stack_end")
    c.transforms.append({"stack": vl_field(value.data), "groupby": group, "offset": "zero",
                         "sort": [{"field": vl_field(series.data), "order": "ascending"}],
                         "as": [start, end]})
    shown_end = end
    if c.highlight:
        shown = c.shown(value)
        shown_end = c.derive("shown_end")
        c.transforms.append({"calculate": f"{datum(start)} + {datum(shown)}", "as": shown_end})
    encoding = {k: c.category_channel(category, sort=order), v: c.value_axis(value, v),
                "color": c.series_color()}

    def make(_pick: Pick, back: bool) -> List[Json]:
        far = end if back else shown_end
        return [c.layer({"type": "bar"}, {v: {"field": start}, v + "2": {"field": far}}, back=back)]

    return encoding, c.pair(make)


def _trend(c: _Chart) -> Tuple[Json, List[Json]]:
    category, value = c.f["category"], c.f["value"]
    order = c.category_order() if c.category_type == "ordinal" else None
    if isinstance(order, str):
        # The points stand on the category and the paths on its copy. Vega-Lite merges the two
        # into one axis in the order asked only when both are sorted by one and the same field;
        # the copy is on the rows as well.
        order = {"field": c.path_names()["x"], "op": "min", "order": order}
    encoding = {"x": c.category_channel(category, c.category_type, sort=order),
                "y": c.value_axis(value, "y")}
    colour: Json = {}
    if c.series:
        encoding["color"] = c.series_color()
    else:
        colour = {"color": c.mark_color()}
    if c.kind == "area":
        encoding["y"]["stack"] = None        # series overlap, so a highlight sits inside its own area

    def make(pick: Pick, back: bool) -> List[Json]:
        field = pick(value)
        layers = []
        if c.kind == "area":
            layers.append(c.path({"type": "area", "opacity": 0.1 if back else 0.3, **colour}, field))
        layers.append(c.path({"type": "line", **colour, **({"opacity": DIMMED} if back else {})}, field))
        layers.append(c.layer({"type": "point", "filled": True, "size": 50, **colour},
                              {"y": {"field": vl_field(field)}}, back=back))
        return layers

    return encoding, c.pair(make)


def _scatter(c: _Chart) -> Tuple[Json, List[Json]]:
    x, y = c.f["x"], c.f["y"]
    encoding: Json = {role: {**c.value_axis(f, role), "scale": {"zero": False}}
                      for role, f in (("x", x), ("y", y))}
    mark: Json = {"type": "point", "filled": True}
    if c.series:
        encoding["color"] = c.series_color()
    else:
        mark["color"] = c.mark_color()
    if "size" in c.f:
        # A blank size draws the smallest point; left blank, Vega-Lite would drop the row. No size
        # legend, as on Power BI's own scatter chart: the tooltip gives the number.
        size = c.derive("size_value")
        value = datum(c.f["size"].data)
        c.transforms.append({"calculate": f"isValid({value}) ? {value} : 0", "as": size})
        encoding["size"] = {"field": size, "type": "quantitative", "legend": None}
    position = {"x": {"field": vl_field(x.data)}, "y": {"field": vl_field(y.data)}}
    if not c.highlight:
        return encoding, [c.layer(mark, position, opacity=0.8)]
    # A point keeps its place under a cross-highlight, so the axes and the sizes stay as they
    # are: the rows outside the highlight are the ones left dimmed.
    flag = c.highlight_of(y if y.kind == _MEASURE else x)
    return encoding, [c.layer(mark, position, back=True),
                      c.layer(mark, position, opacity=0.8, hide=f"isDefined({flag}) && !isValid({flag})")]


def _heatmap(c: _Chart, scheme: str) -> Tuple[Json, List[Json]]:
    x, y, value = c.f["x"], c.f["y"], c.f["value"]
    y_sort = c.arrival_sort("y_position", x) if c.sort == "none" else c.category_sort()
    encoding = {"x": c.category_channel(x, "ordinal"), "y": c.category_channel(y, "ordinal", sort=y_sort)}
    # The gradient is as long as the plot is wide, up to 200: Vega-Lite's own never goes under
    # 100, which is wider than a narrow heatmap's cells.
    legend = {"title": value.name, "gradientOpacity": 1, "labelExpr": c.scale_labels(value, "color"),
              "gradientLength": {"expr": "clamp(width, 40, 200)"}}
    colour = {"field": vl_field(value.data), "type": "quantitative",
              "scale": {"scheme": scheme, "clamp": True}, "legend": legend}
    shown = datum(c.shown(value))
    pale = []                                # when a cell reads as light whatever its value
    if c.highlight:
        # The colour scale belongs to the full values alone: the cell in front looks its colour
        # up on it, so a highlight never shifts the colours of the cells behind.
        pale.append(f"!isValid({shown})")
        layers = [c.layer({"type": "rect"}, {"color": colour}, back=True),
                  c.layer({"type": "rect"}, {"color": {"value": {"expr": f"scale('color', {shown})"}}},
                          hide=pale[0])]
    else:
        layers = [c.layer({"type": "rect"}, {"color": colour})]
    if c.labels:
        # Black on a light cell, white on a dark one. A cell that is dimmed, or shows only its
        # dimmed twin, is light.
        if c.cross_filter:
            pale.append(f"{datum(SELECTED_FIELD)} === 'off'")
        pale.append(f"luminance(scale('color', {shown})) > 0.18")
        ink = {"expr": f"{' || '.join(pale)} ? 'black' : 'white'"}
        text = c.fitted(c.label_text(value, "color"), "x", low=f"bandwidth('y') < {_LABEL_LINE}")
        layers.append(c.layer({"type": "text", "color": ink}, {"text": {"value": {"expr": text}}},
                              paired=False))
    return encoding, layers


def _lollipop(c: _Chart) -> Tuple[Json, List[Json]]:
    category, value = c.f["category"], c.f["value"]
    c.sort_field = value.data
    c.keep_top(value.data, "rank")
    encoding = {"y": c.category_channel(category, sort=c.category_order()), "x": c.value_axis(value, "x")}

    def make(pick: Pick, back: bool) -> List[Json]:
        x = {"field": vl_field(pick(value))}
        stem = {"type": "rule", "strokeWidth": 2, "color": c.mark_color()}
        head = {"type": "point", "filled": True, "size": 90, "color": c.mark_color()}
        return [c.layer(stem, {"x": x, "x2": {"datum": 0}}, back=back), c.layer(head, {"x": x}, back=back)]

    layers = c.pair(make)
    if c.labels:
        layers.append(c.label(value, "x", gap=8))
    return encoding, layers


def _bullet(c: _Chart) -> Tuple[Json, List[Json]]:
    category, value, target = c.f["category"], c.f["value"], c.f["target"]
    c.sort_field = value.data
    c.keep_top(value.data, "rank")
    # Actual and target share the axis, so it carries no single measure's name.
    encoding = {"y": c.category_channel(category),
                "x": {**c.value_axis(value, "x", title=False), "stack": None}}

    def make(pick: Pick, back: bool) -> List[Json]:
        return [c.layer({"type": "bar", "color": c.mark_color(), "height": {"band": 0.5}},
                        {"x": {"field": vl_field(pick(value))}}, back=back),
                c.layer({"type": "tick", "thickness": 2, "color": c.mark_color(-0.5)},
                        {"x": {"field": vl_field(pick(target))}}, back=back)]

    layers = c.pair(make)
    if c.labels:
        layers.append(c.label(value, "x", past=target))      # or the target's tick would cross it
    return encoding, layers


# ---------------------------------------------------------------------------------------------
# what the visual must be told
# ---------------------------------------------------------------------------------------------
def _literal(value: Any) -> str:
    """A value as PBIR stores it in expr.Literal.Value (the property types of Deneb's PBIR guide)."""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return _num(value) + "D"
    if not isinstance(value, str):
        value = json.dumps(value, separators=(",", ":"), ensure_ascii=False)
    return "'" + value.replace("'", "''") + "'"


def _setting(feature: str, obj: str, prop: str, value: Any, default: Any, label: str) -> Json:
    return {"feature": feature, "object": obj, "property": prop, "value": value, "literal": _literal(value),
            "default": default, "element_key": ELEMENT_KEYS.get(prop), "label": label,
            "path": f"visual.objects.{obj}[0].properties.{prop}"}


def _settings(c: _Chart, *, context_menu: bool, max_selected: int) -> List[Json]:
    """Every Deneb setting the spec relies on, with the value it must have. ``default`` is what
    Deneb assumes when the property is absent, ``label`` what its settings pane calls it."""
    out = [_setting("tooltips", "vega", "enableTooltips", c.tooltips, True, "Tooltip handler"),
           _setting("context_menu", "vega", "enableContextMenu", context_menu, True,
                    "Show context menu on right-click")]
    if context_menu:
        out.append(_setting("context_menu", "vega", "enableContextMenuSelector", True, True,
                            "Attempt to resolve data point-specific actions"))
    out.append(_setting("cross_filter", "vega", "enableSelection", c.cross_filter, False,
                        "Expose cross-filtering values for dataset rows"))
    if c.cross_filter:
        out += [_setting("cross_filter", "vega", "selectionMode", "simple", "simple",
                         "Cross-filtering management"),
                _setting("cross_filter", "vega", "selectionMaxDataPoints", max_selected, 50,
                         "Data Point Limit")]
    out.append(_setting("cross_highlight", "vega", "enableHighlight", c.highlight, False,
                        "Expose cross-highlight values for measures"))
    # The supporting fields the spec reads, as Deneb's per-field flag map: an entry that is
    # present states all five base flags.
    support = {name: {"highlight": name in c.reads_highlight, "highlightStatus": False,
                      "highlightComparator": False, "format": name in c.reads_format, "formatted": False}
               for name in sorted(c.reads_highlight | c.reads_format)}
    if support:
        out.append(_setting("supporting_fields", "stateManagement", "supportFieldConfiguration", support, "",
                            "Supporting Fields: dataset"))
    return out


def _listed(parts: List[str]) -> str:
    return parts[0] if len(parts) == 1 else ", ".join(parts[:-1]) + " and " + parts[-1]


def _notes(c: _Chart, *, context_menu: bool, max_selected: int, cross_highlight: bool, checked: bool,
           fitted: bool, shared_axis: bool) -> List[str]:
    notes: List[str] = []
    if not checked:
        notes.append("Field names were not checked against the model. Deneb's dataset names are "
                     "case-sensitive: write every table and field exactly as the model spells it (the report "
                     "spec binds the model's spelling whatever the case given, and a spec that names it "
                     "otherwise draws nothing), or pass model_fields.")
    renamed = sorted({f"{f.name!r} is {f.data!r}" for f in c.f.values() if f.data != f.name})
    if renamed:
        notes.append("Inside the spec " + ", ".join(renamed) + ": Deneb replaces \\ \" . [ and ] in a field "
                     "name with _. A field renamed in the Values well goes by its new name instead.")
    if c.cross_filter:
        marks = "point" if c.kind in _TREND_KINDS | {"scatter"} else "mark"
        rest = (" A click on the line or the area between the points selects nothing and clears the "
                "selection, as a click on empty space does." if c.kind in _TREND_KINDS else
                " A click anywhere else (an axis, the legend, empty space) clears the selection.")
        notes.append(f"Cross-filter: a click on a {marks} selects its row; Ctrl or Shift adds to the "
                     f"selection.{rest} Deneb passes Power BI at most {max_selected} selected rows "
                     "(selectionMaxDataPoints, 1 to 250). Keep one row per mark: a field added to the "
                     "visual that the spec does not draw splits a mark into several rows.")
    if c.highlight:
        how = {"scatter": "The points stay where they are and the ones outside the highlight are dimmed.",
               "heatmap": "The cell in front takes the colour of its highlighted value on the scale of the "
                          "full values."}.get(c.kind, "The axis grows if a highlighted value is larger than "
                                                      "the full one (an average can be).")
        notes.append("Cross-highlight needs measures: Power BI sends highlight values for measures only, "
                     f"never for a column. {how} The other visual's interaction with this one must be "
                     "Highlight (Format > Edit interactions); with Filter the rows arrive filtered and each "
                     "pair draws as one mark.")
    elif cross_highlight:
        notes.append("Cross-highlight was left out: the chart plots no measure, and Power BI sends highlight "
                     "values for measures only.")
    if c.tooltips:
        notes.append("Tooltips: with a report-page tooltip that uses one of these columns, Power BI shows "
                     "that page for the hovered row; otherwise its default tooltip lists the bound fields. "
                     "Their numbers always take the model's format string, also when value_format is given: "
                     "Deneb formats tooltip numbers itself."
                     + (" Under a cross-highlight a measure's highlighted value is listed beside it."
                        if c.highlight else ""))
    if context_menu:
        marks = "point" if c.kind in _TREND_KINDS | {"scatter"} else "mark"
        notes.append(f"Drillthrough: right-click a {marks}. Deneb hands Power BI that one row, and the menu "
                     "offers drillthrough where a drillthrough page takes one of its columns.")
    if c.facet:
        notes.append("Small multiples: " + (
            "the panels were sized to fit the given width." if fitted else
            "Deneb does not fit a faceted view to the visual. Pass width (the visual's width in pixels) to "
            "have the panels sized to it, or set panel_width; a row of panels wider than the visual is cut "
            "off at its edge.") + " The panel height is panel_height whatever the visual's height: set it "
            "so that the rows of panels fit, or the last ones are cut off. Panels come in the order Power BI "
            "delivers the facet column, which is the model's sort order because the facet is bound first. "
            "Where the last row is not full, Vega-Lite still draws a shared x axis under the empty places."
            + (" The panels share one category axis, so a value sort orders the categories by their largest "
               "value in any panel, not panel by panel." if shared_axis else
               " With top_n each panel has its own category axis, with its own top categories in its own "
               "order."))
    if c.top_n is not None:
        notes.append("top_n hides rows Deneb has already received. For a long list add a visual-level Top N "
                     "filter as well, so fewer rows are queried.")
    if c.formatted:
        notes.append("The model's format string reaches " + _listed(c.formatted) + " through "
                     "<measure>__format. A visual the report spec creates has that field; in a project "
                     "created in Deneb 2.0's editor tick \"Format string\" for the measure, or those numbers "
                     "are written without a format.")
    if c.kind != "heatmap" and c.color_arg not in ("0",) and not c.color_arg.startswith("'"):
        notes.append(f"Theme colour {c.color_arg} must exist: Deneb draws black for an index the theme's "
                     "data colours do not reach.")
    if all(f.kind == _COLUMN for f in c.f.values()):
        notes.append("This chart binds no measure. Deneb's documentation says a custom visual needs at least "
                     "one measure in its dataset to save its properties with the report.")
    return notes


# ---------------------------------------------------------------------------------------------
# entry point
# ---------------------------------------------------------------------------------------------
def generate_deneb_spec(kind: str, fields: Mapping[str, FieldRef], *,
                        model_fields: Optional[Mapping[str, Any]] = None,
                        title: Optional[str] = None,
                        sort: Optional[str] = None,
                        color: Union[int, str, Mapping[str, Any], None] = None,
                        value_format: Union[str, Mapping[str, str], None] = None,
                        top_n: Optional[int] = None,
                        labels: bool = False,
                        label_units: bool = False,
                        category_type: Optional[str] = None,
                        facet_columns: Optional[int] = None,
                        panel_width: Optional[int] = None,
                        panel_height: Optional[int] = None,
                        width: Optional[int] = None,
                        cross_filter: bool = True,
                        cross_highlight: bool = True,
                        tooltips: bool = True,
                        context_menu: bool = True,
                        max_selected: int = 50,
                        config: Optional[Mapping[str, Any]] = None,
                        **unknown: Any) -> Json:
    """A Deneb Vega-Lite spec for one chart, with Power BI's interactivity wired in.

    kind    one of KINDS. "bar" is horizontal, "column" vertical; "bullet" is actual against
            target; a "facet" field turns bar, column, the stacked kinds, line, area, lollipop
            and scatter into small multiples.
    fields  {role: field}. A field is "Table.Field" or {"column" | "measure": "Table.Name"}.
              bar, column, lollipop        category, value, facet?
              stacked_bar, stacked_column  category, series, value, facet?
              line, area                   category, value, series?, facet?
              bullet                       category, value, target
              scatter                      x, y (each a measure or a column: say which),
                                           category?, series?, size?, facet?
              heatmap                      x, y, value
            category, series, facet and a heatmap's x and y are columns; value, target and size
            are measures.
    model_fields
            the model's fields, in either shape the report spec takes: {table: [names]} or
            {table: {field: {"kind": "column" | "measure"}}}. With it every field is checked
            and written the way the model spells it, which matters because Deneb's dataset
            names are case-sensitive. Without it the names are used as given.

    title          the visual's title. It goes on the element, so Power BI draws it in the
                   theme's title style; the spec itself carries none.
    sort           "value_desc" | "value_asc" | "category_asc" | "category_desc" | "none" (the
                   order Power BI delivers, which follows the model's sort-by column). Default:
                   by value, largest first, for bars; "none" for line, area and heatmap. Small
                   multiples share one category axis, so there a value sort goes by a category's
                   largest value in any panel; with top_n each panel sorts its own.
    color          a theme colour for single-colour marks: its zero-based index, a name from
                   THEME_COLOR_NAMES, or {"theme": ..., "shade": -1..1}. A series is coloured by
                   the theme palette. For a heatmap: "linear" (default) or "divergent".
    value_format   a Power BI format string for the value axis and the labels ("#,0.0", "0.0%"),
                   used exactly as given; or {role: format} when several measures are drawn
                   (scatter). Without it they take the model's own format string, and the axis
                   one display unit (K, M, bn) for all its ticks. Tooltips are not affected:
                   Power BI always shows their numbers in the model's format.
    top_n          keep the N largest categories (per panel when faceted).
    labels         data labels (bar, column, lollipop, bullet, heatmap), in full. A label wider
                   than its column or its heatmap cell is left out rather than run into the next.
    label_units    write the labels in the display unit of the axis instead (1.5K, 2.4K).
    category_type  "ordinal" (default); for line and area also "temporal" for a date column and
                   "quantitative" for a number.
    facet_columns, panel_width, panel_height, width
                   small multiples: panels per row (3) and the size of one panel (180 x 120).
                   Give width, the visual's width in pixels, to have the panel width worked out
                   so that a row of panels fits. The height is not fitted: Vega-Lite takes a
                   panel's size as a fixed number, and the number of rows depends on the data.
    cross_filter, cross_highlight, tooltips, context_menu
                   each switches one piece of interactivity off. max_selected is Deneb's limit on
                   selected rows, 1 to 250.
    config         Vega-Lite config merged over the standard one.

    Returns {"kind", "provider", "spec", "config", "fields", "dataset", "interactivity",
    "element", "notes"}. "fields" is the list to bind, in order; "dataset" names each field as
    the spec knows it; "interactivity" lists every Deneb setting the spec relies on, with the
    value it must have, where Deneb stores it and the element key that sets it (None when the
    element has none); "element" is a ``deneb`` element for the report spec, without a name or
    a position. Raises ValueError for input it cannot use, an option it does not have included.
    """
    if unknown:
        raise ValueError(f"unknown option(s) {sorted(unknown)}; the options are {list(OPTIONS)}")
    if not isinstance(kind, str) or kind not in KINDS:
        raise ValueError(f"unknown kind {kind!r}; use one of {list(KINDS)}")
    parsed = _parse_fields(kind, fields, _model_lookup(model_fields))
    facet = "facet" in parsed
    ranked = kind in _RANKED_KINDS
    flags = {name: _flag(name, value) for name, value in (
        ("labels", labels), ("label_units", label_units), ("cross_filter", cross_filter),
        ("cross_highlight", cross_highlight), ("tooltips", tooltips), ("context_menu", context_menu))}

    if sort is None:
        sort = "value_desc" if ranked else "none"
    if not isinstance(sort, str) or sort not in SORTS:
        raise ValueError(f"unknown sort {sort!r}; use one of {list(SORTS)}")
    if kind == "scatter" and sort != "none":
        raise ValueError("sort does not apply to a scatter chart: both of its axes are numbers")
    if sort.startswith("value") and not ranked:
        raise ValueError(f"sort {sort!r} does not apply to a {kind} chart; use \"category_asc\", "
                         "\"category_desc\" or \"none\"")
    if top_n is not None:
        if not ranked:
            raise ValueError(f"top_n does not apply to a {kind} chart; it ranks the categories of "
                             f"{sorted(_RANKED_KINDS)}")
        top_n = _whole("top_n", top_n, 1, 10000)
    if flags["labels"] and kind not in _LABEL_KINDS:
        raise ValueError(f"labels are not drawn on a {kind} chart; they apply to {sorted(_LABEL_KINDS)}")
    if flags["label_units"] and not flags["labels"]:
        raise ValueError("label_units chooses how the data labels are written; it needs labels to be on")
    if category_type is None:
        category_type = "ordinal"
    if not isinstance(category_type, str) or category_type not in _CATEGORY_TYPES:
        raise ValueError(f"unknown category_type {category_type!r}; use one of {list(_CATEGORY_TYPES)}")
    if category_type != "ordinal" and kind not in _TREND_KINDS:
        raise ValueError(f"category_type {category_type!r} applies to line and area charts; a {kind} chart "
                         "has an ordinal category axis")
    if category_type != "ordinal" and sort != "none":
        raise ValueError(f"sort does not apply to a {category_type} axis, which is ordered by its own values")
    max_selected = _whole("max_selected", max_selected, 1, MAX_SELECTED_LIMIT)
    if title is not None and not isinstance(title, str):
        raise ValueError("title must be text")
    if config is not None:
        if not isinstance(config, Mapping):
            raise ValueError("config must be an object of Vega-Lite config")
        try:
            json.dumps(config, allow_nan=False)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"config must be plain JSON: {exc}") from None

    sizes = {name: _whole(name, value, 1, 4000) for name, value in (
        ("facet_columns", facet_columns), ("panel_width", panel_width), ("panel_height", panel_height),
        ("width", width)) if value is not None}
    if sizes and not facet:
        raise ValueError(f"{sorted(sizes)[0]} sizes small multiples, and this chart has no \"facet\" field")
    panel = {**_FACET_DEFAULTS, **sizes}
    if panel["facet_columns"] > 50:
        raise ValueError("facet_columns must be a whole number from 1 to 50")

    formats: Dict[str, str] = {}
    if isinstance(value_format, str):
        if kind == "scatter":
            raise ValueError("a scatter chart draws several numbers; give value_format per role, for example "
                             "{\"x\": \"#,0\", \"y\": \"0.0%\"}")
        formats = {role: value_format for role in ("value", "target") if role in parsed}
    elif isinstance(value_format, Mapping):
        numeric = [role for role, accepts, _ in _ROLES[kind] if accepts != _COLUMN and role in parsed]
        for role, text in value_format.items():
            if role not in numeric:
                raise ValueError(f"value_format.{role}: this chart's numeric roles are {numeric}")
            formats[role] = text
    elif value_format is not None:
        raise ValueError("value_format must be a Power BI format string such as \"#,0.0\", or {role: format}")
    if any(not isinstance(text, str) or not text.strip() for text in formats.values()):
        raise ValueError("value_format must be a Power BI format string such as \"#,0.0\"; leave it out to "
                         "use the model's")
    if flags["label_units"] and "value" in formats:
        raise ValueError("label_units writes the labels in the model's format with a display unit; with "
                         "value_format the format string decides, so put the unit in it")

    scheme = HEATMAP_SCHEMES["linear"]
    if kind == "heatmap" and color is not None:
        if not isinstance(color, str) or color not in HEATMAP_SCHEMES:
            raise ValueError(f"a heatmap's color is one of {sorted(HEATMAP_SCHEMES)}: the theme's gradient "
                             "from its minimum to its maximum colour, or the one through its middle colour")
        scheme = HEATMAP_SCHEMES[color]
    elif color is not None and "series" in parsed:
        raise ValueError("color applies to marks of a single colour; a series is coloured by the theme "
                         "palette (pbiColorNominal). Leave color out, or drop fields.series")

    chart = _Chart(kind, parsed, sort=sort, color=color, formats=formats, top_n=top_n, labels=flags["labels"],
                   label_units=flags["label_units"], category_type=category_type,
                   cross_filter=flags["cross_filter"], cross_highlight=flags["cross_highlight"],
                   tooltips=flags["tooltips"])
    panel_name = ""
    if facet:
        # The panels are split on a copy of the facet column under a name of the spec's own, so
        # that nothing a panel draws for many rows at once is named after a dataset field.
        panel_name = chart.derive("panel")
        chart.transforms.append({"calculate": datum(parsed["facet"].data), "as": panel_name})
    if kind in ("bar", "column"):
        encoding, layers = _bars(chart)
    elif kind in _STACKED_KINDS:
        encoding, layers = _stacked(chart)
    elif kind in _TREND_KINDS:
        encoding, layers = _trend(chart)
    elif kind == "scatter":
        encoding, layers = _scatter(chart)
    elif kind == "heatmap":
        encoding, layers = _heatmap(chart, scheme)
    elif kind == "lollipop":
        encoding, layers = _lollipop(chart)
    else:
        encoding, layers = _bullet(chart)

    if chart.tooltips:
        encoding["tooltip"] = chart.tooltip()
    spec: Json = {"data": {"name": DATASET}}
    view: Json = {"encoding": encoding, "layer": layers}
    merged_config = copy.deepcopy(_CONFIG)
    own_axis = chart.own_axis
    category_channel = "x" if kind in _VERTICAL | _TREND_KINDS | {"heatmap"} else "y"
    labelled = kind in ("bar", "stacked_bar", "lollipop", "bullet", "heatmap")   # categories label the y axis
    # ``beside`` is the room the y axis takes, as expression text. The plot's own width is no
    # measure for what follows: Vega fits a layout once, so a label that turned upright, or a
    # legend that wrapped, because the fitted plot came out narrow would find no room made for
    # it. The visual's width does not change with the fit.
    if facet:
        columns = panel["facet_columns"]
        beside = str(_CATEGORY_AXIS if labelled else _VALUE_AXIS)
        if labelled:
            encoding["y"]["axis"]["labelLimit"] = _LABEL_LIMIT
        axes = columns if own_axis and labelled else 1       # a y axis for every panel, or one a row
        around = _VIEW_PADDING + (columns - 1) * _PANEL_GAP + int(beside) * axes
        if "width" in sizes and "panel_width" not in sizes:
            panel["panel_width"] = max(40, (sizes["width"] - around) // columns)
        chart.outside = str(sizes.get("width", columns * panel["panel_width"] + around))
    elif labelled:
        limit = f"clamp({chart.visual_width()} * {_LABEL_SHARE}, 40, 180)"
        encoding["y"]["axis"]["labelLimit"] = {"expr": limit}
        beside = f"({limit} + {_LABEL_GAP})"
    else:
        beside = str(_VALUE_AXIS)
    if category_channel == "x" and encoding["x"]["type"] in ("nominal", "ordinal"):
        # A label under a column or a point has the width of its slot. While that is enough to
        # say something the label lies flat and is cut to the slot, so neighbours never run into
        # each other and none sticks out of the plot; in a narrower slot it stands upright.
        count = f"max(1, length(domain('{'child_x' if own_axis else 'x'}')))"
        if facet:
            slot = flat = f"{panel['panel_width']} / {count}"
            upright = str(_LABEL_LIMIT)
        else:
            slot, flat = f"({chart.visual_width()} - {beside}) / {count}", f"width / {count}"
            upright = f"clamp({chart.visual_height()} * {_UPRIGHT_SHARE}, 30, {_LABEL_LIMIT})"
        narrow = f"{slot} < {_NARROW_SLOT}"
        encoding["x"]["axis"].update({
            "labelAngle": {"expr": f"{narrow} ? 270 : 0"},
            "labelAlign": {"expr": f"{narrow} ? 'right' : 'center'"},
            "labelBaseline": {"expr": f"{narrow} ? 'middle' : 'top'"},
            "labelLimit": {"expr": f"{narrow} ? {upright} : {flat}"}})
    if chart.series:
        # As many legend entries to a row as fit above the plot; the rest wrap.
        encoding["color"]["legend"]["columns"] = {
            "expr": f"max(1, floor(({chart.visual_width()} - {beside}) / {_LEGEND_ENTRY}))"}
    if chart.params:
        spec["params"] = list(chart.params.values())
    if chart.transforms:
        spec["transform"] = chart.transforms
    if facet:
        # Panels in the order their first rows arrive (__row__ is Deneb's index of the row).
        spec["facet"] = {"field": panel_name, "type": "nominal", "header": {"title": None},
                         "sort": {"field": ROW_FIELD, "op": "min", "order": "ascending"}}
        spec["columns"] = panel["facet_columns"]
        spec["spacing"] = _PANEL_GAP
        if own_axis:
            spec["resolve"] = {"scale": {category_channel: "independent"}}
        spec["spec"] = {"width": panel["panel_width"], "height": panel["panel_height"], **view}
        del merged_config["autosize"]        # "fit" is for single and layered views only
    else:
        spec.update(view)
    if config:
        merged_config = _merge(merged_config, config)

    bound: List[_Field] = []
    for f in parsed.values():
        if all(f.key != b.key for b in bound):
            bound.append(f)
    field_refs = [{f.kind: _ref_text(f.table, f.name)} for f in bound]
    settings = _settings(chart, context_menu=flags["context_menu"], max_selected=max_selected)
    element: Json = {"type": "deneb", "fields": copy.deepcopy(field_refs), "spec": copy.deepcopy(spec),
                     "config": copy.deepcopy(merged_config), "provider": "vegaLite"}
    element.update({s["element_key"]: s["value"] for s in settings if s["element_key"]})
    if title and title.strip():
        element["title"] = title
    return {
        "kind": kind,
        "provider": "vegaLite",
        "spec": spec,
        "config": merged_config,
        "fields": field_refs,
        "dataset": [{"role": f.role, "field": {f.kind: _ref_text(f.table, f.name)}, "name": f.data,
                     "kind": f.kind} for f in parsed.values()],
        "interactivity": settings,
        "element": element,
        "notes": _notes(chart, context_menu=flags["context_menu"], max_selected=max_selected,
                        cross_highlight=flags["cross_highlight"], checked=model_fields is not None,
                        fitted="width" in sizes and "panel_width" not in sizes,
                        shared_axis=facet and not own_axis and sort.startswith("value")),
    }


def to_text(result: Mapping[str, Any]) -> str:
    """A result as the text a tool returns: what to bind, what must be switched on, the notes,
    and the element to paste into a report spec (it holds the spec and the config)."""
    lines = [f"=== Deneb spec: {result['kind']} ({result['provider']}) ===", "",
             "Fields to bind, in order, and their names inside the spec:"]
    for entry in result["dataset"]:
        (kind, ref), = entry["field"].items()
        lines.append(f"  {entry['role']:<9}{kind:<8}{ref}  ->  {entry['name']}")
    lines += ["", "Deneb settings the spec relies on:"]
    for s in result["interactivity"]:
        if s["element_key"]:
            how = f"element key \"{s['element_key']}\""
        else:
            how = "Deneb's default" if s["value"] == s["default"] else "no element key"
        lines.append(f"  {s['object']}.{s['property']} = {s['literal']}  ({how})")
    lines += ["", "Notes:"] + [f"  - {note}" for note in result["notes"]]
    lines += ["", "Element for pbir_apply_spec (add name, x, y, width, height):",
              json.dumps(result["element"], ensure_ascii=False)]
    return "\n".join(lines) + "\n"
