"""
PBIR emitters for report-as-code authoring: pure functions that return the JSON documents a
PBIR-Enhanced report is made of (visual.json, page.json, bookmarks, the custom theme, and the
report.json registrations). Nothing here touches disk; report_spec.py decides what to write.

Where pbir_authoring emits a bare visual (type, position, fields), this module adds what a
designed report needs: formatting objects, textboxes, shapes, images, buttons with actions,
slicers, visual groups, show/hide bookmarks, a theme built from a small palette, and the
AppSource / private custom visual plumbing.

Every shape was taken from report files that open in Power BI Desktop. A few are inferred from
the format rather than seen in such a file; they are marked "(inferred)" at the function.

Conventions
  * A field is "Table.Field", "'Table Name'.Field", or a dict
    {"table", "field" | "column" | "measure", "kind"?, "aggregation"?}.
  * A colour is a "#RRGGBB" string.
  * ``container`` is a ready visualContainerObjects dict (see container_objects). Visuals that
    are decoration rather than data (textbox, shape, image, button, slicer) default to
    PLAIN_CHROME: no title, background, border, shadow or header, and no padding.
  * ``schema_url`` should be the $schema of a sibling file in the same project when there is
    one; DEFAULT_SCHEMAS is only the fallback for a brand-new report.
"""
import copy
import json
import re
from typing import Any, Dict, List, Mapping, Optional, Sequence, Union

import pbir_authoring

Json = Dict[str, Any]
FieldRef = Union[str, Mapping[str, Any]]

_SCHEMA_ROOT = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/"
DEFAULT_SCHEMAS: Dict[str, str] = {
    **pbir_authoring.DEFAULT_SCHEMAS,
    "report": _SCHEMA_ROOT + "report/3.3.0/schema.json",
    "version": _SCHEMA_ROOT + "versionMetadata/1.0.0/schema.json",
    "bookmark": _SCHEMA_ROOT + "bookmark/1.4.0/schema.json",
    "bookmarksMetadata": _SCHEMA_ROOT + "bookmarksMetadata/1.0.0/schema.json",
}
REPORT_DEFINITION_VERSION = "2.0.0"

# AppSource visuals are referenced by id in report.json "publicCustomVisuals"; nothing is stored
# in the project. Two HTML Content ids occur in reports that open in Desktop. Which edition each
# one is (the one that runs scripts, or the certified one that does not) was not established, so
# neither is named for it: pass the id your report already uses.
HTML_CONTENT_ID = "htmlContent443BE3AD55E043BF878BED274D3A6855"
HTML_CONTENT_ID_ALT = "htmlContent443BE3AD55E043BF878BED274D3A6865"
DENEB_ID = "deneb7E15AEF80B9E4D4F8E12924291ECE89A"
DENEB_VEGA_LITE_VERSION = "6.4.1"

# The version triple Desktop stores beside a custom theme. Used only when the report has no
# base theme to copy it from.
THEME_VERSION_AT_IMPORT = {"visual": "1.8.95", "report": "2.0.95", "page": "1.3.95"}

PLAIN_CHROME: Dict[str, Any] = {"title": False, "background": False, "border": False, "shadow": False,
                                "header": False, "padding": 0}
BUTTON_STATES = ("default", "hover", "press", "selected", "disabled")
_SIDES = ("top", "bottom", "left", "right")
_HEX = re.compile(r"^#(?:[0-9A-Fa-f]{6}|[0-9A-Fa-f]{8})$")


# ---------------------------------------------------------------------------------------------
# literals and expressions
# ---------------------------------------------------------------------------------------------
def _num(n: Union[int, float]) -> str:
    if isinstance(n, bool):
        raise TypeError("a number is required, not a bool")
    if isinstance(n, int):
        return str(n)
    f = float(n)
    return str(int(f)) if f.is_integer() else repr(f)


def lit_raw(value: str) -> Json:
    """A literal exactly as written, for the cases the typed helpers do not cover."""
    return {"expr": {"Literal": {"Value": value}}}


def lit_str(text: Any) -> Json:
    """A text literal: single-quoted, with embedded single quotes doubled."""
    return lit_raw("'" + str(text).replace("'", "''") + "'")


def lit_num(n: Union[int, float]) -> Json:
    """A number as PBIR stores sizes, percentages and weights: 12 -> "12D"."""
    return lit_raw(_num(n) + "D")


def lit_int(n: int) -> Json:
    """A whole number for the few properties stored as integers: 2 -> "2L"."""
    return lit_raw(str(int(n)) + "L")


def lit_bool(flag: Any) -> Json:
    return lit_raw("true" if flag else "false")


def lit(value: Any) -> Any:
    """bool / number / text -> the matching literal. A dict is taken to be an expression already."""
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, bool):
        return lit_bool(value)
    if isinstance(value, (int, float)):
        return lit_num(value)
    return lit_str(value)


def color(value: Union[str, Mapping[str, Any]]) -> Json:
    """A solid colour fill. A dict is taken to be a colour object already."""
    if isinstance(value, Mapping):
        return dict(value)
    return {"solid": {"color": lit_str(value)}}


# Builders that take a ``color`` argument (a button's text colour) reach the helper by this name.
_paint = color


def theme_color(color_id: int, percent: float = 0) -> Json:
    """A colour that follows the theme's data colours instead of a fixed hex (inferred)."""
    return {"solid": {"color": {"expr": {"ThemeDataColor": {"ColorId": int(color_id), "Percent": percent}}}}}


def is_hex_color(value: Any) -> bool:
    return isinstance(value, str) and bool(_HEX.match(value))


def split_field(ref: FieldRef) -> tuple:
    """(table, field) of a field reference in either accepted form. A dict may name the table
    separately ({"table": "Sales", "measure": "Total"}) or inline ({"measure": "Sales.Total"})."""
    if isinstance(ref, Mapping):
        table = ref.get("table") or ref.get("entity") or ""
        field = ref.get("field") or ref.get("property") or ref.get("column") or ref.get("measure") or ""
        if not table:
            return pbir_authoring.split_table_field(str(field))
        return str(table), str(field)
    return pbir_authoring.split_table_field(str(ref))


def measure_expr(ref: FieldRef) -> Json:
    """A measure used as a property value (an image whose source is a measure, for example)."""
    table, name = split_field(ref)
    return {"expr": {"Measure": {"Expression": {"SourceRef": {"Entity": table}}, "Property": name}}}


def resource_expr(item_name: str, package: str = "RegisteredResources") -> Json:
    """A file registered in report.json resourcePackages, used as a property value."""
    return {"expr": {"ResourcePackageItem": {"PackageName": package, "PackageType": 1,
                                             "ItemName": item_name}}}


def projection(ref: FieldRef, kind: str = "Column", active: bool = False) -> Json:
    """One query projection. ``kind`` is the default for a plain "Table.Field" string; a dict may
    carry its own kind, an aggregation, a queryRef and a nativeQueryRef."""
    table, field = split_field(ref)
    if isinstance(ref, Mapping):
        if "kind" in ref:
            kind = ref["kind"]
        elif "measure" in ref:
            kind = "Measure"
        elif "column" in ref:
            kind = "Column"
        return pbir_authoring.emit_projection(table, field, kind, ref.get("queryRef"), active,
                                              ref.get("nativeQueryRef"), ref.get("aggregation"))
    return pbir_authoring.emit_projection(table, field, kind, None, active)


# ---------------------------------------------------------------------------------------------
# objects
# ---------------------------------------------------------------------------------------------
def entry(properties: Mapping[str, Any], selector: Optional[str] = None) -> Json:
    """One entry of a formatting object. ``selector`` is a state id such as "default" or "hover"."""
    out: Json = {"properties": dict(properties)}
    if selector:
        out["selector"] = {"id": selector}
    return out


def props(**properties: Any) -> List[Json]:
    """A formatting object with a single entry, the common case."""
    return [entry(properties)]


def simple_objects(objects: Optional[Mapping[str, Any]]) -> Json:
    """Formatting objects from a compact form.

    {"legend": {"show": False, "fontSize": 10, "labelColor": "#333333"}} becomes the PBIR
    {"legend": [{"properties": {...literals...}}]}. Plain values are converted (a "#RRGGBB"
    string becomes a solid colour); a dict value is passed through as an expression; an object
    given as a list is passed through untouched, for per-state or per-series entries.
    """
    out: Json = {}
    for name, value in (objects or {}).items():
        if isinstance(value, list):
            out[name] = copy.deepcopy(value)
            continue
        converted = {}
        for key, val in dict(value).items():
            converted[key] = color(val) if is_hex_color(val) else lit(val)
        out[name] = [entry(converted)]
    return out


def _show(flag: Any) -> List[Json]:
    return props(show=lit_bool(flag))


def _text_object(value: Any) -> Optional[List[Json]]:
    """title / subTitle: False, True, the text, or {text, show, color, size, font, align, bold, wrap}."""
    if value is None:
        return None
    if isinstance(value, bool):
        return _show(value)
    if isinstance(value, str):
        return props(show=lit_bool(True), text=lit_str(value))
    p: Json = {"show": lit_bool(value.get("show", True))}
    if value.get("text") is not None:
        p["text"] = lit_str(value["text"])
    if value.get("color"):
        p["fontColor"] = color(value["color"])
    if value.get("size") is not None:
        p["fontSize"] = lit_num(value["size"])
    if value.get("font"):
        p["fontFamily"] = lit_str(value["font"])
    if value.get("align"):
        p["alignment"] = lit_str(value["align"])
    if value.get("bold") is not None:
        p["bold"] = lit_bool(value["bold"])
    if value.get("wrap") is not None:
        p["titleWrap"] = lit_bool(value["wrap"])
    return [entry(p)]


def _fill_object(value: Any, with_radius: bool) -> Optional[List[Json]]:
    """background / border: False, True, a colour, or {show, color, transparency | radius}."""
    if value is None:
        return None
    if isinstance(value, bool):
        return _show(value)
    if isinstance(value, str):
        value = {"color": value}
    p: Json = {"show": lit_bool(value.get("show", True))}
    if value.get("color"):
        p["color"] = color(value["color"])
    if with_radius:
        if value.get("radius") is not None:
            p["radius"] = lit_num(value["radius"])
    elif value.get("color") or value.get("transparency") is not None:
        p["transparency"] = lit_num(value.get("transparency", 0))
    return [entry(p)]


def container_objects(*, title: Any = None, subtitle: Any = None, background: Any = None, border: Any = None,
                      shadow: Optional[bool] = None, padding: Any = None, header: Optional[bool] = None,
                      alt_text: Optional[str] = None, lock_aspect: Optional[bool] = None,
                      keep_layer_order: Optional[bool] = True, tooltip: Optional[bool] = None,
                      link: Optional[List[Json]] = None, extra: Optional[Mapping[str, Any]] = None) -> Json:
    """The visualContainerObjects of a visual: the frame every visual has, whatever its type.

    None leaves a setting to the theme. ``padding`` is one number or {top, bottom, left, right}.
    ``link`` is an action from action_link. ``keep_layer_order`` is on by default: without it
    Desktop lifts a clicked visual above its neighbours, and a full-page visual then covers the
    buttons drawn over it. ``extra`` is merged last, object by object.
    """
    out: Json = {}
    for key, obj in (("title", _text_object(title)), ("subTitle", _text_object(subtitle)),
                     ("background", _fill_object(background, False)), ("border", _fill_object(border, True))):
        if obj is not None:
            out[key] = obj
    if shadow is not None:
        out["dropShadow"] = _show(shadow)
    if header is not None:
        out["visualHeader"] = _show(header)
    if padding is not None:
        sides = padding if isinstance(padding, Mapping) else {s: padding for s in _SIDES}
        out["padding"] = [entry({s: lit_num(sides[s]) for s in ("top", "bottom", "left",
                                                                "right") if s in sides})]
    if link:
        out["visualLink"] = copy.deepcopy(link)
    if tooltip is not None:
        out["visualTooltip"] = _show(tooltip)
    if lock_aspect is not None:
        out["lockAspect"] = _show(lock_aspect)
    general: Json = {}
    if alt_text:
        general["altText"] = lit_str(alt_text)
    if keep_layer_order:
        general["keepLayerOrder"] = lit_bool(True)
    if general:
        out["general"] = [entry(general)]
    for key, obj in (extra or {}).items():
        out[key] = copy.deepcopy(obj)
    return out


def action_link(kind: str, target: Optional[str] = None, *, tooltip: Optional[str] = None,
                default_tooltip: Optional[bool] = None) -> List[Json]:
    """The visualLink object that makes a button, shape or image clickable.

    kind: "page" (target = page name), "bookmark" (target = bookmark name), "clear_slicers",
    "back" (inferred) or "url" (target = web address, inferred).
    """
    kinds = {"page": ("PageNavigation", "navigationSection"), "bookmark": ("Bookmark", "bookmark"),
             "clear_slicers": ("ClearAllSlicers", None), "back": ("Back", None), "url": ("WebUrl", "webUrl")}
    if kind not in kinds:
        raise ValueError(f"unknown action kind {kind!r}; use one of {sorted(kinds)}")
    link_type, target_prop = kinds[kind]
    p: Json = {"show": lit_bool(True), "type": lit_str(link_type)}
    if target_prop:
        if not target:
            raise ValueError(f"action {kind!r} needs a target")
        p[target_prop] = lit_str(target)
    if tooltip:
        p["tooltip"] = lit_str(tooltip)
    if default_tooltip is not None:
        p["showDefaultTooltip"] = lit_bool(default_tooltip)
    return [entry(p)]


# ---------------------------------------------------------------------------------------------
# containers
# ---------------------------------------------------------------------------------------------
def _coord(v: Union[int, float]) -> Union[int, float]:
    f = float(v)
    return int(f) if f.is_integer() else round(f, 2)


def position(x: float, y: float, width: float, height: float, z: int = 0,
             tab_order: Optional[int] = None) -> Json:
    """Pixels on the page. The tab order follows z unless given, so keyboard order tracks layering."""
    return {"x": _coord(x), "y": _coord(y), "z": int(z), "width": _coord(width), "height": _coord(height),
            "tabOrder": int(z if tab_order is None else tab_order)}


def visual_container(name: str, pos: Mapping[str, Any], visual: Json, *, schema_url: Optional[str] = None,
                     parent_group: Optional[str] = None, hidden: bool = False) -> Json:
    """Wrap a visual body into a visual.json document."""
    doc: Json = {"$schema": schema_url or DEFAULT_SCHEMAS["visual"], "name": name, "position": dict(pos),
                 "visual": visual}
    if hidden:
        doc["isHidden"] = True
    if parent_group:
        doc["parentGroupName"] = parent_group
    return doc


def _body(visual_type: str, objects: Optional[Json], container: Optional[Json],
          default_chrome: Mapping[str, Any],
          query_state: Optional[Json] = None, link: Optional[List[Json]] = None,
          extra: Optional[Mapping[str, Any]] = None) -> Json:
    vco = copy.deepcopy(container) if container is not None else container_objects(**default_chrome)
    if link:
        vco["visualLink"] = copy.deepcopy(link)
        # keep "general" last, where Desktop writes it
        if "general" in vco:
            vco["general"] = vco.pop("general")
    body: Json = {"visualType": visual_type}
    if query_state:
        body["query"] = {"queryState": query_state}
    if objects:
        body["objects"] = objects
    body["visualContainerObjects"] = vco
    for key, value in (extra or {}).items():
        body[key] = value
    body["drillFilterOtherVisuals"] = True
    return body


# Roles that hold a drillable hierarchy. In report files only the first column of such a role
# carries "active": true; value roles, Series, table columns and custom visual roles never do.
ACTIVE_ROLES = {"category", "rows", "columns"}


def query_state(fields_by_role: Mapping[str, Any], *, mark_active: bool = True) -> Json:
    """query.queryState from {role: field | [fields]}.

    Each projection comes from pbir_authoring.emit_projection. A plain string is read as a
    measure in a value role and as a column elsewhere; a dict says which it is. The "active"
    flag is set the way Desktop sets it (see ACTIVE_ROLES), which is narrower than
    pbir_authoring.build_query_state, where the first field of every role is active.
    ``mark_active=False`` is for tables and custom visuals, whose fields are never active.
    """
    state: Json = {}
    for role, refs in fields_by_role.items():
        default = "Measure" if role.lower() in pbir_authoring.AGGREGATING_ROLES else "Column"
        projections: List[Json] = []
        for ref in (refs if isinstance(refs, list) else [refs]):
            if not ref:
                continue
            item = projection(ref, default)
            if mark_active and not projections and role.lower() in ACTIVE_ROLES and "Column" in item["field"]:
                item["active"] = True
            projections.append(item)
        if projections:
            state[role] = {"projections": projections}
    return state


def data_visual(name: str, visual_type: str, pos: Mapping[str, Any],
                fields_by_role: Optional[Mapping[str, Any]] = None, *, objects: Optional[Json] = None,
                container: Optional[Json] = None, mark_active: Optional[bool] = None,
                schema_url: Optional[str] = None, parent_group: Optional[str] = None,
                hidden: bool = False) -> Json:
    """Any native or custom visual: type, position, fields by role, formatting.

    Fields go through query_state. ``mark_active`` defaults to off for a table (tableEx) and on
    otherwise; pass False for a custom visual. With no ``container`` the frame is left to the theme.
    """
    if mark_active is None:
        mark_active = visual_type != "tableEx"
    state = query_state(dict(fields_by_role or {}), mark_active=mark_active)
    body = _body(visual_type, objects, container, {}, state)
    return visual_container(name, pos, body, schema_url=schema_url, parent_group=parent_group, hidden=hidden)


def text_run(text: Any, *, font: Optional[str] = None, size: Optional[float] = None,
             color: Optional[str] = None,
             bold: bool = False, italic: bool = False, underline: bool = False) -> Json:
    """One run of a textbox paragraph. ``size`` is in points. italic and underline are inferred."""
    style: Json = {}
    if font:
        style["fontFamily"] = font
    if size is not None:
        style["fontSize"] = _num(size) + "pt"
    if color:
        style["color"] = color
    if bold:
        style["fontWeight"] = "bold"
    if italic:
        style["fontStyle"] = "italic"
    if underline:
        style["textDecoration"] = "underline"
    run: Json = {"value": str(text)}
    if style:
        run["textStyle"] = style
    return run


def paragraph(runs: Sequence[Json], align: Optional[str] = None) -> Json:
    """A textbox paragraph. ``align`` is "left", "center" or "right"."""
    para: Json = {"textRuns": list(runs)}
    if align:
        para["horizontalTextAlignment"] = align
    return para


def textbox(name: str, pos: Mapping[str, Any], paragraphs: Sequence[Json], *,
            container: Optional[Json] = None,
            schema_url: Optional[str] = None, parent_group: Optional[str] = None,
            hidden: bool = False) -> Json:
    body = _body("textbox", {"general": props(paragraphs=list(paragraphs))}, container, PLAIN_CHROME)
    return visual_container(name, pos, body, schema_url=schema_url, parent_group=parent_group, hidden=hidden)


def shape(name: str, pos: Mapping[str, Any], *, fill: str = "#FFFFFF", transparency: float = 0,
          radius: float = 0,
          outline: Optional[str] = None, outline_weight: float = 1,
          tile_shape: str = "rectangleRoundedByPixel",
          link: Optional[List[Json]] = None, container: Optional[Json] = None,
          schema_url: Optional[str] = None,
          parent_group: Optional[str] = None, hidden: bool = False) -> Json:
    """A filled rectangle (rounded by ``radius`` pixels): a panel, a rule, or the veil behind a pop-up."""
    line = [entry({"show": lit_bool(bool(outline))})]
    if outline:
        line.append(entry({"lineColor": color(outline), "weight": lit_num(outline_weight)}, "default"))
    objects = {
        "fill": [entry({"fillColor": color(fill), "transparency": lit_num(transparency)}, "default")],
        "outline": line,
        "shape": props(tileShape=lit_str(tile_shape), roundEdge=lit_num(radius)),
    }
    body = _body("shape", objects, container, PLAIN_CHROME, link=link)
    return visual_container(name, pos, body, schema_url=schema_url, parent_group=parent_group, hidden=hidden)


def image(name: str, pos: Mapping[str, Any], *, resource: Optional[str] = None,
          measure: Optional[FieldRef] = None,
          scaling: Optional[str] = None, transparency: float = 0, link: Optional[List[Json]] = None,
          container: Optional[Json] = None, schema_url: Optional[str] = None,
          parent_group: Optional[str] = None,
          hidden: bool = False) -> Json:
    """An image from a registered resource file, or from a measure.

    ``measure`` is for a measure that returns an image URL, typically an SVG data URI built in
    DAX (a KPI card with a sparkline); give that measure the data category ImageUrl in the model.
    ``scaling`` is "Fill" ("Fit" and "Normal" are inferred) and applies to resource images.
    """
    if bool(resource) == bool(measure):
        raise ValueError("an image needs exactly one of resource or measure")
    if measure:
        objects: Json = {"image": props(sourceType=lit_str("imageData"), sourceField=measure_expr(measure),
                                        transparency=lit_num(transparency))}
    else:
        objects = {"general": props(imageUrl=resource_expr(resource))}
        if scaling:
            objects["imageScaling"] = props(imageScalingType=lit_str(scaling))
    body = _body("image", objects, container, PLAIN_CHROME, link=link)
    return visual_container(name, pos, body, schema_url=schema_url, parent_group=parent_group, hidden=hidden)


def action_button(name: str, pos: Mapping[str, Any], *, text: Optional[str] = None,
                  link: Optional[List[Json]] = None,
                  font: Optional[str] = None, size: Optional[float] = None, color: Optional[str] = None,
                  bold: Optional[bool] = None, fill: Optional[str] = None, fill_transparency: float = 0,
                  outline: Optional[str] = None, outline_weight: float = 1, radius: Optional[float] = None,
                  states: Optional[Mapping[str, Mapping[str, Any]]] = None, container: Optional[Json] = None,
                  schema_url: Optional[str] = None, parent_group: Optional[str] = None,
                  hidden: bool = False) -> Json:
    """A button. With no text and no fill it is a transparent hit area over something another
    visual drew, which is how a card painted by a custom visual becomes a navigation link.

    ``states`` restyles the other states: {"hover": {"fill", "fill_transparency", "color",
    "text"}, "press": {...}, "selected": {...}, "disabled": {...}}. ``radius`` rounds the
    corners in every state.
    """
    states = states or {}
    unknown = set(states) - set(BUTTON_STATES[1:])
    if unknown:
        raise ValueError(f"unknown button state(s) {sorted(unknown)}; use {list(BUTTON_STATES[1:])}")

    fills = [entry({"show": lit_bool(True)})]
    if fill:
        fills.append(entry({"fillColor": _paint(fill), "transparency": lit_num(fill_transparency)},
                           "default"))
    else:
        fills.append(entry({"fillColor": _paint("#FFFFFF"), "transparency": lit_num(100)}, "default"))
    texts = [entry({"show": lit_bool(text is not None and text != "")})]
    if text:
        base: Json = {"text": lit_str(text)}
        if color:
            base["fontColor"] = _paint(color)
        if font:
            base["fontFamily"] = lit_str(font)
        if size is not None:
            base["fontSize"] = lit_num(size)
        if bold is not None:
            base["bold"] = lit_bool(bold)
        texts.append(entry(base, "default"))
    for state in BUTTON_STATES[1:]:
        st = states.get(state)
        if not st:
            continue
        if st.get("fill"):
            fills.append(entry({"fillColor": _paint(st["fill"]),
                                "transparency": lit_num(st.get("fill_transparency", 0))}, state))
        t: Json = {}
        if st.get("text"):
            t["text"] = lit_str(st["text"])
        if st.get("color"):
            t["fontColor"] = _paint(st["color"])
        if t and text:
            texts.append(entry(t, state))
    line = [entry({"show": lit_bool(bool(outline))})]
    if outline:
        line.append(entry({"lineColor": _paint(outline), "weight": lit_num(outline_weight)}, "default"))
    objects: Json = {"icon": _show(False), "fill": fills, "outline": line}
    if radius is not None:
        objects["shape"] = [entry({"tileShape": lit_str("rectangleRounded")})] + [
            entry({"roundEdge": lit_num(radius)}, s) for s in BUTTON_STATES[:4]]
    objects["text"] = texts
    body = _body("actionButton", objects, container, PLAIN_CHROME, link=link)
    return visual_container(name, pos, body, schema_url=schema_url, parent_group=parent_group, hidden=hidden)


SLICER_MODES = {"list": "Basic", "tiles": "Basic", "dropdown": "Dropdown", "between": "Between"}


def slicer(name: str, pos: Mapping[str, Any], field: FieldRef, *, mode: str = "dropdown",
           single_select: Optional[bool] = None, strict_single_select: Optional[bool] = None,
           select_all: Optional[bool] = None, header: Any = None, items: Optional[Mapping[str, Any]] = None,
           sync_group: Optional[str] = None, container: Optional[Json] = None,
           schema_url: Optional[str] = None,
           parent_group: Optional[str] = None, hidden: bool = False) -> Json:
    """A slicer on one column.

    mode: "list" (vertical list), "tiles" (the list laid out horizontally), "dropdown", or
    "between" (a range on a number or date; inferred). ``header`` is False, the text, or
    {text, color, size, font}. ``items`` styles the values: {color, background, size, font,
    outline, outline_color}. Slicers that share a ``sync_group`` on different pages keep one
    selection between them.
    """
    if mode not in SLICER_MODES:
        raise ValueError(f"unknown slicer mode {mode!r}; use one of {sorted(SLICER_MODES)}")
    objects: Json = {"data": props(mode=lit_str(SLICER_MODES[mode]))}
    if header is not None:
        if isinstance(header, bool):
            objects["header"] = _show(header)
        else:
            h = {"text": header} if isinstance(header, str) else dict(header)
            p: Json = {"show": lit_bool(h.get("show", True))}
            if h.get("text") is not None:
                p["text"] = lit_str(h["text"])
            if h.get("color"):
                p["fontColor"] = color(h["color"])
            if h.get("font"):
                p["fontFamily"] = lit_str(h["font"])
            if h.get("size") is not None:
                p["textSize"] = lit_num(h["size"])
            objects["header"] = [entry(p)]
    if items:
        p = {}
        if items.get("color"):
            p["fontColor"] = color(items["color"])
        if items.get("background"):
            p["background"] = color(items["background"])
        if items.get("size") is not None:
            p["textSize"] = lit_num(items["size"])
        if items.get("font"):
            p["fontFamily"] = lit_str(items["font"])
        if items.get("outline"):
            p["outline"] = lit_str(items["outline"])
        if items.get("outline_color"):
            p["outlineColor"] = color(items["outline_color"])
        if p:
            objects["items"] = [entry(p)]
    selection: Json = {}
    if select_all is not None:
        selection["selectAllCheckboxEnabled"] = lit_bool(select_all)
    if single_select is not None:
        selection["singleSelect"] = lit_bool(single_select)
    if strict_single_select is not None:
        selection["strictSingleSelect"] = lit_bool(strict_single_select)
    if selection:
        objects["selection"] = [entry(selection)]
    if mode == "tiles":
        objects["general"] = props(orientation=lit_raw("1D"))
    state = {"Values": {"projections": [projection(field, "Column", active=True)]}}
    extra = {"syncGroup": {"groupName": sync_group, "fieldChanges": True,
                           "filterChanges": True}} if sync_group else None
    body = _body("slicer", objects, container, PLAIN_CHROME, state, extra=extra)
    return visual_container(name, pos, body, schema_url=schema_url, parent_group=parent_group, hidden=hidden)


def html_visual(name: str, pos: Mapping[str, Any], measure: FieldRef, *, visual_type: str = HTML_CONTENT_ID,
                objects: Optional[Json] = None, container: Optional[Json] = None,
                schema_url: Optional[str] = None,
                parent_group: Optional[str] = None, hidden: bool = False) -> Json:
    """The HTML Content visual showing the HTML a measure returns. Register ``visual_type`` in
    report.json with register_public_visual."""
    state = {"content": {"projections": [projection(measure, "Measure")]}}
    body = _body(visual_type, objects, container, PLAIN_CHROME, state)
    return visual_container(name, pos, body, schema_url=schema_url, parent_group=parent_group, hidden=hidden)


def json_literal(value: Union[str, Mapping[str, Any], Sequence[Any]]) -> Json:
    """A JSON document carried inside a text literal, compact. A string is taken to be JSON already."""
    text = value if isinstance(value, str) else json.dumps(value, separators=(",", ":"), ensure_ascii=False)
    return lit_str(text)


def deneb_visual(name: str, pos: Mapping[str, Any], fields: Sequence[FieldRef],
                 spec: Union[str, Mapping[str, Any]], *, config: Union[str, Mapping[str, Any], None] = None,
                 provider: str = "vegaLite", version: Optional[str] = None, tooltips: bool = True,
                 selection: bool = False, max_selected: int = 50, container: Optional[Json] = None,
                 schema_url: Optional[str] = None, parent_group: Optional[str] = None,
                 hidden: bool = False) -> Json:
    """A Deneb visual. ``fields`` become the columns of the dataset named "dataset" in the spec;
    a plain string is read as a column, so write a measure as {"measure": "Table.Name"}.

    ``version`` is the Vega-Lite (or Vega) version Deneb records; the default is the one seen
    with provider "vegaLite". Give it explicitly for provider "vega". ``selection`` turns on
    cross-filtering; the spec must then use Deneb's __selected__ field itself.
    """
    if provider not in ("vegaLite", "vega"):
        raise ValueError("provider must be 'vegaLite' or 'vega'")
    if version is None:
        if provider != "vegaLite":
            raise ValueError("give the Vega version explicitly for provider 'vega'")
        version = DENEB_VEGA_LITE_VERSION
    vega: Json = {"provider": lit_str(provider), "version": lit_str(version), "jsonSpec": json_literal(spec)}
    if config is not None:
        vega["jsonConfig"] = json_literal(config)
    vega.update({"enableTooltips": lit_bool(tooltips), "enableSelection": lit_bool(selection),
                 "selectionMode": lit_str("simple"), "selectionMaxDataPoints": lit_num(max_selected)})
    state = {"dataset": {"projections": [projection(f, "Column") for f in fields]}}
    body = _body(DENEB_ID, {"vega": [entry(vega)]}, container, {}, state)
    return visual_container(name, pos, body, schema_url=schema_url, parent_group=parent_group, hidden=hidden)


def visual_group(name: str, pos: Mapping[str, Any], display_name: str, *, hidden: bool = False,
                 parent_group: Optional[str] = None, schema_url: Optional[str] = None) -> Json:
    """A group. Its members name it in parentGroupName; hiding the group hides them all, which is
    what a show/hide bookmark toggles. The group's own background is switched off: a theme that
    gives every visual a background would otherwise paint an opaque panel under the members.
    """
    doc: Json = {"$schema": schema_url or DEFAULT_SCHEMAS["visual"], "name": name, "position": dict(pos),
                 "visualGroup": {"displayName": display_name, "groupMode": "ScaleMode",
                                 "objects": {"background": _show(False)}}}
    if hidden:
        doc["isHidden"] = True
    if parent_group:
        doc["parentGroupName"] = parent_group
    return doc


# ---------------------------------------------------------------------------------------------
# pages and bookmarks
# ---------------------------------------------------------------------------------------------
def page(name: str, display_name: str, width: int = 1280, height: int = 720, *,
         display_option: str = "FitToPage",
         background: Any = None, wallpaper: Any = None, hidden: bool = False, tooltip: bool = False,
         vertical_alignment: Optional[str] = None, schema_url: Optional[str] = None) -> Json:
    """A page.json document.

    ``background`` is a colour, {color, transparency}, or {image, scaling, transparency} naming
    a registered resource image. ``wallpaper`` is the colour around the page. ``hidden`` keeps
    the page out of the reader's page list. ``tooltip`` makes it a tooltip page (and hides it).
    display_option: "FitToPage", "FitToWidth" or "ActualSize" (the last is inferred).
    """
    doc = pbir_authoring.build_page(name, display_name, width, height, schema_url)
    doc["displayOption"] = display_option
    objects: Json = {}
    if vertical_alignment:
        objects["displayArea"] = props(verticalAlignment=lit_str(vertical_alignment))
    if background is not None:
        bg = {"color": background} if isinstance(background, str) else dict(background)
        p: Json = {}
        if bg.get("image"):
            p["image"] = {"image": {"name": lit_str(bg["image"]), "url": resource_expr(bg["image"]),
                                    "scaling": lit_str(bg.get("scaling", "Fill"))}}
        elif bg.get("color"):
            p["color"] = color(bg["color"])
        p["transparency"] = lit_num(bg.get("transparency", 0))
        objects["background"] = [entry(p)]
    if wallpaper is not None:
        wp = {"color": wallpaper} if isinstance(wallpaper, str) else dict(wallpaper)
        objects["outspace"] = props(color=color(wp["color"]), transparency=lit_num(wp.get("transparency", 0)))
    if objects:
        doc["objects"] = objects
    if hidden or tooltip:
        doc["visibility"] = "HiddenInViewMode"
    if tooltip:
        doc["type"] = "Tooltip"
    return doc


def _exploration(page_name: str, groups: Optional[Mapping[str, bool]],
                 visuals: Optional[Mapping[str, Mapping[str, Any]]]) -> Json:
    containers: Json = {}
    for visual_name, info in (visuals or {}).items():
        single: Json = {"visualType": info["visual_type"]}
        if info.get("hidden"):
            single["display"] = {"mode": "hidden"}
        containers[visual_name] = {"singleVisual": single}
    section: Json = {"visualContainers": containers}
    if groups:
        section["visualContainerGroups"] = {g: {"isHidden": bool(h)} for g, h in groups.items()}
    return {"version": "1.0", "activeSection": page_name, "sections": {page_name: section}}


def visibility_bookmark(name: str, display_name: str, page_name: str, *,
                        groups: Optional[Mapping[str, bool]] = None,
                        visuals: Optional[Mapping[str, Mapping[str, Any]]] = None,
                        schema_url: Optional[str] = None) -> Json:
    """A bookmark that only shows or hides the things it names, on one page.

    ``groups`` maps a group name to True (hidden) or False (shown). ``visuals`` maps a visual
    name to {"visual_type", "hidden"} (single visuals are inferred; groups are the tested path).
    It leaves filters, slicers and the current page alone, so a pop-up can open and close
    without undoing what the reader selected.
    """
    targets = list(groups or {}) + list(visuals or {})
    return {
        "$schema": schema_url or DEFAULT_SCHEMAS["bookmark"],
        "displayName": display_name,
        "name": name,
        "options": {"applyOnlyToTargetVisuals": True, "targetVisualNames": targets,
                    "suppressActiveSection": True, "suppressData": True},
        "explorationState": _exploration(page_name, groups, visuals),
    }


def bookmark(name: str, display_name: str, page_name: str, *, groups: Optional[Mapping[str, bool]] = None,
             visuals: Optional[Mapping[str, Mapping[str, Any]]] = None,
             schema_url: Optional[str] = None) -> Json:
    """An ordinary bookmark: it opens ``page_name`` and applies the visibility it lists.

    It carries no filter or slicer state (that can only be captured in Desktop), so it is
    written with the Data option off and never changes what the reader selected.
    """
    return {
        "$schema": schema_url or DEFAULT_SCHEMAS["bookmark"],
        "displayName": display_name,
        "name": name,
        "options": {"targetVisualNames": [], "suppressData": True},
        "explorationState": _exploration(page_name, groups, visuals),
    }


def bookmarks_metadata(names: Sequence[str], schema_url: Optional[str] = None) -> Json:
    """bookmarks.json: the order of the bookmarks pane."""
    return {"$schema": schema_url or DEFAULT_SCHEMAS["bookmarksMetadata"],
            "items": [{"name": n} for n in names]}


def version_metadata(schema_url: Optional[str] = None) -> Json:
    """definition/version.json."""
    return {"$schema": schema_url or DEFAULT_SCHEMAS["version"], "version": REPORT_DEFINITION_VERSION}


# ---------------------------------------------------------------------------------------------
# theme
# ---------------------------------------------------------------------------------------------
def _merge(base: Json, extra: Mapping[str, Any]) -> Json:
    for key, value in extra.items():
        if isinstance(value, Mapping) and isinstance(base.get(key), dict):
            _merge(base[key], value)
        else:
            base[key] = copy.deepcopy(value)
    return base


def _solid(hex_color: str) -> Json:
    return {"solid": {"color": hex_color}}


THEME_PLAIN_TYPES = ("textbox", "shape", "actionButton", "image", "slicer")


def theme(name: str, *, colors: Optional[Mapping[str, Any]] = None, fonts: Optional[Mapping[str, Any]] = None,
          card: Optional[Mapping[str, Any]] = None, plain_types: Sequence[str] = THEME_PLAIN_TYPES,
          extra: Optional[Mapping[str, Any]] = None) -> Json:
    """A custom theme from a compact description.

    colors: data (list of series colours), background (visual cards), page, wallpaper,
            foreground, muted, subtle, border, accent, good, neutral, bad, maximum, center, minimum
    fonts:  title, label, callout, header: each {family, size, color}
    card:   the frame every data visual gets: {radius, border, shadow, padding, header, title}
            (border / shadow / header / title are booleans; padding is a number or per side)
    plain_types: visual types that get no frame at all (text, shapes, buttons, images, slicers)
    extra:  raw theme JSON merged over the result, for anything this does not cover
    """
    c = dict(colors or {})
    f = dict(fonts or {})
    k = dict(card or {})
    out: Json = {"name": name}
    if c.get("data"):
        out["dataColors"] = list(c["data"])
    for theme_key, key in (("foreground", "foreground"), ("foregroundNeutralSecondary", "muted"),
                           ("foregroundNeutralTertiary", "subtle"), ("background", "background"),
                           ("backgroundLight", "page"), ("backgroundNeutral", "border"),
                           ("tableAccent", "accent"),
                           ("hyperlink", "accent"), ("good", "good"), ("neutral", "neutral"), ("bad", "bad"),
                           ("maximum", "maximum"), ("center", "center"), ("minimum", "minimum")):
        if c.get(key):
            out[theme_key] = c[key]
    classes: Json = {}
    for cls in ("callout", "title", "header", "label"):
        if f.get(cls):
            spec = f[cls]
            one: Json = {}
            if spec.get("family"):
                one["fontFace"] = spec["family"]
            if spec.get("size") is not None:
                one["fontSize"] = spec["size"]
            if spec.get("color"):
                one["color"] = spec["color"]
            classes[cls] = one
    if classes:
        out["textClasses"] = classes

    every: Json = {}
    if c.get("background"):
        every["background"] = [{"show": True, "color": _solid(c["background"]), "transparency": 0}]
    border_on = k.get("border", bool(c.get("border")))
    border: Json = {"show": bool(border_on)}
    if border_on and c.get("border"):
        border["color"] = _solid(c["border"])
    if k.get("radius") is not None:
        border["radius"] = k["radius"]
    every["border"] = [border]
    every["dropShadow"] = [{"show": bool(k.get("shadow", False))}]
    # Layer order is kept everywhere so a click never lifts a visual above its neighbours.
    every["general"] = [{"keepLayerOrder": True}]
    if k.get("padding") is not None:
        pad = k["padding"]
        every["padding"] = [dict(pad) if isinstance(pad, Mapping) else {s: pad for s in _SIDES}]
    title: Json = {"show": bool(k.get("title", True))}
    if f.get("title"):
        t = f["title"]
        if t.get("family"):
            title["fontFamily"] = t["family"]
        if t.get("size") is not None:
            title["fontSize"] = t["size"]
        if t.get("color"):
            title["fontColor"] = _solid(t["color"])
    every["title"] = [title]
    every["visualHeader"] = [{"show": bool(k.get("header", False))}]
    styles: Json = {"*": {"*": every}}
    page_style: Json = {}
    if c.get("page"):
        page_style["background"] = [{"color": _solid(c["page"]), "transparency": 0}]
    if c.get("wallpaper"):
        page_style["outspace"] = [{"color": _solid(c["wallpaper"]), "transparency": 0}]
    if page_style:
        styles["page"] = {"*": page_style}
    bare = {"background": [{"show": False}], "border": [{"show": False}], "dropShadow": [{"show": False}],
            "title": [{"show": False}], "visualHeader": [{"show": False}],
            "padding": [{"top": 0, "bottom": 0, "left": 0, "right": 0}]}
    for visual_type in plain_types:
        styles[visual_type] = {"*": copy.deepcopy(bare)}
    out["visualStyles"] = styles
    return _merge(out, extra or {})


# ---------------------------------------------------------------------------------------------
# report.json
# ---------------------------------------------------------------------------------------------
def new_report(schema_url: Optional[str] = None) -> Json:
    """The smallest report.json: used when a report folder has no definition yet (inferred: no
    report was seen without a base theme, so Desktop falling back to its default theme here is
    an expectation, not an observation)."""
    return {"$schema": schema_url or DEFAULT_SCHEMAS["report"], "themeCollection": {}}


def _package(report: Json, name: str, package_type: str, create: bool = True) -> Optional[Json]:
    packages = report.setdefault("resourcePackages", []) if create else report.get("resourcePackages", [])
    for pkg in packages:
        if pkg.get("name") == name:
            return pkg
    if not create:
        return None
    pkg = {"name": name, "type": package_type, "items": []}
    packages.append(pkg)
    return pkg


def register_resource(report: Mapping[str, Any], file_name: str, item_type: str) -> Json:
    """report.json with ``file_name`` (a file in StaticResources/RegisteredResources) registered.
    item_type is "CustomTheme" or "Image". Returns a new document; the input is not changed."""
    out = copy.deepcopy(dict(report))
    pkg = _package(out, "RegisteredResources", "RegisteredResources")
    if not any(i.get("name") == file_name for i in pkg["items"]):
        pkg["items"].append({"name": file_name, "path": file_name, "type": item_type})
    return out


def unregister_resource(report: Mapping[str, Any], file_name: str) -> Json:
    out = copy.deepcopy(dict(report))
    pkg = _package(out, "RegisteredResources", "RegisteredResources", create=False)
    if pkg:
        pkg["items"] = [i for i in pkg["items"] if i.get("name") != file_name]
    return out


def set_custom_theme(report: Mapping[str, Any], file_name: str) -> Json:
    """report.json using ``file_name`` as its custom theme (registered as a resource as well).
    The version triple is copied from the base theme when there is one, as Desktop does."""
    out = register_resource(report, file_name, "CustomTheme")
    collection = out.setdefault("themeCollection", {})
    version = (collection.get("baseTheme") or {}).get("reportVersionAtImport")
    if not isinstance(version, dict):
        version = dict(THEME_VERSION_AT_IMPORT)
    collection["customTheme"] = {"name": file_name, "reportVersionAtImport": copy.deepcopy(version),
                                 "type": "RegisteredResources"}
    return out


def register_public_visual(report: Mapping[str, Any], visual_id: str) -> Json:
    """report.json listing an AppSource visual (HTML Content, Deneb, ...) the report uses."""
    out = copy.deepcopy(dict(report))
    listed = out.setdefault("publicCustomVisuals", [])
    if visual_id not in listed:
        listed.append(visual_id)
    return out


def unregister_public_visual(report: Mapping[str, Any], visual_id: str) -> Json:
    out = copy.deepcopy(dict(report))
    if visual_id in out.get("publicCustomVisuals", []):
        out["publicCustomVisuals"] = [v for v in out["publicCustomVisuals"] if v != visual_id]
    return out


def custom_visual_package(guid: str) -> Json:
    """The resourcePackages entry of a private custom visual unpacked under CustomVisuals/<guid>/."""
    meta = f"{guid}.pbiviz.json"
    return {"name": guid, "type": "CustomVisual",
            "items": [{"name": meta, "path": meta, "type": "CustomVisualMetadata"}]}


def register_custom_visual(report: Mapping[str, Any], guid: str) -> Json:
    out = copy.deepcopy(dict(report))
    packages = out.setdefault("resourcePackages", [])
    if not any(p.get("name") == guid for p in packages):
        packages.append(custom_visual_package(guid))
    return out


def unregister_custom_visual(report: Mapping[str, Any], guid: str) -> Json:
    out = copy.deepcopy(dict(report))
    if "resourcePackages" in out:
        out["resourcePackages"] = [p for p in out["resourcePackages"] if p.get("name") != guid]
    return out
