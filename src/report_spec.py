"""
Report as code: describe a Power BI report in one JSON-serialisable spec and apply it to a
PBIR-Enhanced report folder (<Name>.Report with a definition/ folder).

    errors = validate_report_spec(spec)
    result = apply_report_spec(report_dir, spec, model_fields=None, prune=False, dry_run=False)

Applying is idempotent: every page, element and bookmark has a stable ``name``, so a second run
updates the same files in place and an unchanged spec rewrites nothing. Nothing is written
unless the whole spec validates, and all writes land together or not at all. Pages, visuals and
bookmarks the spec did not create are never rewritten or removed.

THE SPEC

  {
    "theme":          {...},        optional custom theme (see below)
    "custom_visuals": [...],        optional AppSource ids and .pbiviz packages
    "page_defaults":  {...},        optional width / height / background / wallpaper for every page
    "pages":          [...],        the pages, each with its elements
    "shared":         [...],        optional elements repeated on several pages (masthead, navigation)
    "bookmarks":      [...],        optional show/hide bookmarks
    "page_order":     [...],        optional page names, first to last
    "active_page":    "name"        optional page the report opens on
  }

Names (pages, elements, bookmarks) use letters, digits, "_" and "-", 50 characters at most. They
are folder names, so they are compared without regard to case.

page
  name, display_name, width, height (default 1280 x 720), display_option ("FitToPage"),
  background ("#RRGGBB" | {color, transparency} | {image, scaling}), wallpaper ("#RRGGBB"),
  hidden (true keeps it out of the reader's page list), tooltip (true makes it a tooltip page),
  elements [...], popups [...].
  "existing": true means the page is already in the report and only elements are added to it:
  its page.json is left alone. Without it, a page of that name must not exist yet (or must have
  been created by an earlier apply).

element (every type)
  name, type, x, y, width, height, and optionally z (default: declaration order), tab_order,
  hidden, and the frame: title, subtitle, background, border, shadow, padding, header, alt_text,
  tooltip, lock_aspect. Text, shapes, images, buttons, slicers and HTML get no frame unless you
  give one ("chrome": "theme" hands the frame back to the theme). "objects" adds formatting in
  compact form, {"legend": {"show": false, "fontSize": 10}}, and "container" does the same for
  frame objects not listed above. An element must fit inside its page ("allow_overflow": true
  lifts that).

  textbox  text (or paragraphs: [{runs: [{text, font, size, color, bold}], align}]), font, size,
           color, bold, align
  shape    fill, transparency, radius, outline, outline_weight, action
  image    resource (a file already in StaticResources/RegisteredResources) or measure (a measure
           returning an image URL or an SVG data URI), scaling, action
  button   text, font, size, color, bold, fill, fill_transparency, outline, radius, action,
           states {hover | press | selected | disabled: {fill, fill_transparency, color, text}},
           active {...} (overrides used on the page the button itself points at)
  slicer   field "Table.Column", mode ("dropdown" | "list" | "tiles" | "between"), single_select,
           select_all, header, items {color, background, size, font, outline, outline_color},
           sync_group (a name, or true for one named after the field)
  html     measure "Table.Measure" (the HTML Content visual), visual_id
  deneb    fields [...], spec {...}, config {...}, provider, version, selection, tooltips
  visual   visual_type ("barChart", "tableEx", "card", ...) or custom (the alias of a .pbiviz in
           custom_visuals), fields {role: field | [fields]}
  group    display_name, hidden, elements [...] (members keep page coordinates)

  action   {"type": "page" | "bookmark" | "back" | "url" | "clear_slicers", "target": ..., "tooltip": ...}

field
  "Table.Field", "'Table Name'.Field", or {"measure": "Table.Name"} / {"column": "Table.Name"},
  optionally with "aggregation": "sum" | "average" | "count" | "distinct_count" | "min" | "max".
  With model_fields every field is checked against the model, and a plain string is resolved
  to a measure or a column from it. Without it, a plain string in a chart is read from its role
  (a measure in Y or Values, a column in Category), and a table, a Deneb visual or a custom
  visual needs the object form, since one role holds both kinds there.

popup (per page, or inside a shared entry)
  name, box {x, y, width, height, fill, radius, outline}, title, veil {color, transparency},
  close {text, width, height, ...button keys}, elements [...], open_button {...a button...}.
  Expands into a hidden group <name> holding a veil over the page, the box, a close button and
  your elements, plus two bookmarks, <page>_<name>_open and <page>_<name>_close, that only show
  and hide that group (filters and slicers keep their state). Clicking the veil closes it.

shared
  {"name": ..., "pages": "*" | [names], "exclude": [names], "elements": [...], "popups": [...]}
  The elements are added to each page. "{page}" and "{page_title}" inside any text are replaced
  with the page's name and display name, so one button can open "{page}_filters_open" everywhere.

bookmark
  {"name", "display_name", "page", "show": [element names], "hide": [element names]}
  Shows and hides those elements on that page and touches nothing else. "targeted": false makes
  an ordinary bookmark that also opens the page.

theme
  {"name", "file" (default <name>.json), "colors": {data: [...], background, page, wallpaper,
   foreground, muted, subtle, border, accent, good, neutral, bad}, "fonts": {title | label |
   callout | header: {family, size, color}}, "card": {radius, border, shadow, padding, header},
   "extra": {...raw theme JSON...}}

custom_visuals
  [{"appsource": "<visual id>"}, {"pbiviz": "path/to/visual.pbiviz", "alias": "home"}]
  A .pbiviz is unpacked into CustomVisuals/<guid>/ and registered; a relative path is taken from
  the folder that holds the report folder. HTML Content and Deneb are registered automatically
  when an element uses them.

EXAMPLE

  {
    "theme": {"name": "House", "colors": {"data": ["#1F6FEB", "#F0883E"], "background": "#FFFFFF",
              "page": "#F6F8FA", "foreground": "#1F2328", "border": "#D0D7DE"}, "card": {"radius": 8}},
    "page_defaults": {"width": 1280, "height": 720},
    "shared": [{"name": "nav", "pages": "*", "elements": [
      {"name": "nav_overview", "type": "button", "x": 24, "y": 16, "width": 120, "height": 36,
       "text": "Overview", "action": {"type": "page", "target": "overview"}, "active": {"bold": true}},
      {"name": "nav_detail", "type": "button", "x": 152, "y": 16, "width": 120, "height": 36,
       "text": "Detail", "action": {"type": "page", "target": "detail"}, "active": {"bold": true}},
      {"name": "nav_filters", "type": "button", "x": 1136, "y": 16, "width": 120, "height": 36,
       "text": "Filters", "fill": "#1F6FEB", "color": "#FFFFFF", "radius": 18,
       "action": {"type": "bookmark", "target": "{page}_filters_open"}}],
      "popups": [{"name": "filters", "title": "Filters",
        "box": {"x": 340, "y": 160, "width": 600, "height": 400},
        "elements": [{"name": "f_year", "type": "slicer", "x": 372, "y": 240, "width": 536, "height": 72,
                      "field": "Calendar.Year", "mode": "tiles", "sync_group": true}]}]}],
    "pages": [
      {"name": "overview", "display_name": "Overview", "elements": [
        {"name": "heading", "type": "textbox", "x": 24, "y": 72, "width": 600, "height": 40,
         "text": "Sales overview", "size": 20, "bold": true},
        {"name": "by_month", "type": "visual", "visual_type": "lineChart", "x": 24, "y": 128, "width": 1232,
         "height": 560, "title": "Sales by month",
         "fields": {"Category": "Calendar.Month", "Y": "Sales.Total Sales"}}]},
      {"name": "detail", "display_name": "Detail", "elements": [
        {"name": "orders", "type": "visual", "visual_type": "tableEx", "x": 24, "y": 72, "width": 1232,
         "height": 616,
         "fields": {"Values": [{"column": "Customer.Name"}, {"measure": "Sales.Total Sales"}]}}]}],
    "page_order": ["overview", "detail"],
    "active_page": "overview"
  }

APPLYING

  apply_report_spec(report_dir, spec, *, model_fields=None, prune=False, dry_run=False,
                    adopt=False, manifest_path=None)

  report_dir    an existing report folder that contains definition.pbir. An empty one (no
                definition/ yet) gets report.json, version.json and pages.json created.
  model_fields  the model's fields, {table: {field names}} or {table: {field: {"kind":
                "column" | "measure", "dataType": ...}}}, as the PBIP connector produces them.
  prune         also remove pages, elements and bookmarks that an earlier apply created and the
                spec no longer has. Only those: what is managed is recorded in a manifest,
                "<report folder name>.spec-manifest.json", kept beside the report folder (not in
                it, so it is never uploaded with the report).
  dry_run       return what would be written and removed without touching the disk.
  adopt         take over pages, elements or bookmarks that already exist under a name the spec
                uses, instead of refusing. Use it when the manifest was lost.

  Returns {"success", "written": [...], "removed": [...], "unchanged": n, "warnings": [...],
  "errors": [...]}; paths are relative to the report folder.
"""
import copy
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Set, Tuple

import pbir_authoring
import pbir_builders as pb
import pbir_packages
import safe_io

Json = Dict[str, Any]

MANIFEST_SUFFIX = ".spec-manifest.json"
MANIFEST_VERSION = 1

ELEMENT_TYPES = ("textbox", "shape", "image", "button", "slicer", "html", "deneb", "visual", "group")
ACTION_TYPES = ("page", "bookmark", "back", "url", "clear_slicers")
_PLAIN_TYPES = {"textbox", "shape", "image", "button", "slicer", "html"}
_LINKABLE = {"button", "shape", "image"}
_CHROME_KEYS = ("title", "subtitle", "background", "border", "shadow", "padding", "header", "alt_text",
                "lock_aspect", "tooltip", "keep_layer_order")
_COMMON_KEYS = {"name", "type", "x", "y", "width", "height", "z", "position", "tab_order", "hidden", "chrome",
                "container", "objects", "allow_overflow", "action", "active", *_CHROME_KEYS}
_TYPE_KEYS = {
    "textbox": {"text", "paragraphs", "font", "size", "color", "bold", "italic", "underline", "align"},
    "shape": {"fill", "transparency", "radius", "outline", "outline_weight"},
    "image": {"resource", "measure", "scaling", "transparency"},
    "button": {"text", "font", "size", "color", "bold", "fill", "fill_transparency", "outline",
               "outline_weight",
               "radius", "states"},
    "slicer": {"field", "mode", "single_select", "strict_single_select", "select_all", "header", "items",
               "sync_group"},
    "html": {"measure", "visual_id"},
    "deneb": {"fields", "spec", "config", "provider", "version", "selection", "tooltips", "max_selected"},
    "visual": {"visual_type", "custom", "fields"},
    "group": {"display_name", "elements"},
}
_TOP_KEYS = {"theme", "custom_visuals", "page_defaults", "pages", "shared", "bookmarks", "page_order",
             "active_page"}
_PAGE_KEYS = {"name", "display_name", "width", "height", "display_option", "background", "wallpaper",
              "hidden",
              "tooltip", "existing", "elements", "popups"}
_NAME = re.compile(r"^[A-Za-z0-9_-]{1,50}$")
_RESERVED = {"con", "prn", "aux", "nul", *(f"com{i}" for i in range(1, 10)),
             *(f"lpt{i}" for i in range(1, 10))}
_AGGREGATIONS = {"sum": pbir_authoring.AGG_SUM, "average": pbir_authoring.AGG_AVERAGE,
                 "avg": pbir_authoring.AGG_AVERAGE,
                 "distinct_count": pbir_authoring.AGG_DISTINCT_COUNT, "min": pbir_authoring.AGG_MIN,
                 "max": pbir_authoring.AGG_MAX, "count": pbir_authoring.AGG_COUNT_NONNULL,
                 "median": pbir_authoring.AGG_MEDIAN}
_NUMERIC_TYPES = {"int64", "double", "decimal", "int", "integer", "number"}

# Layering bands. Page content sits lowest, shared chrome (a masthead) above it, pop-ups on top.
# Pop-up members sit below their group's own z, the arrangement seen in reports that open.
_Z_CONTENT, _Z_SHARED, _Z_POPUP_ITEMS, _Z_POPUP_GROUP, _Z_STEP = 1000, 20000, 40000, 50000, 10

# Keys of a page.json / visual.json that the spec decides. Anything else found in an existing
# file (filters added in Desktop, annotations) is carried over rather than dropped.
_PAGE_OWNED = {"$schema", "name", "displayName", "displayOption", "height", "width", "objects", "visibility"}
_VISUAL_OWNED = {"$schema", "name", "position", "visual", "visualGroup", "parentGroupName", "isHidden"}


# ---------------------------------------------------------------------------------------------
# compiled form
# ---------------------------------------------------------------------------------------------
@dataclass
class _Element:
    page: str
    name: str
    type: str
    spec: Json
    where: str
    x: float = 0
    y: float = 0
    width: float = 0
    height: float = 0
    z: int = 0
    parent: Optional[str] = None
    hidden: bool = False
    doc: Optional[Json] = None


@dataclass
class _Page:
    name: str
    display_name: str
    width: float
    height: float
    existing: bool
    spec: Json
    elements: List[_Element] = field(default_factory=list)
    doc: Optional[Json] = None


@dataclass
class _Compiled:
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    pages: List[_Page] = field(default_factory=list)
    bookmarks: List[Json] = field(default_factory=list)       # built bookmark documents, in order
    theme_file: Optional[str] = None
    theme_doc: Optional[Json] = None
    public_ids: List[str] = field(default_factory=list)
    images: List[str] = field(default_factory=list)
    page_order: List[str] = field(default_factory=list)
    active_page: Optional[str] = None


class _Fields:
    """Resolves field references against the model, when one was supplied."""

    def __init__(self, model_fields: Optional[Mapping[str, Any]]):
        self.known = model_fields is not None
        self.guessed: List[str] = []       # fields whose kind was taken from the role they sit in
        self.tables: Dict[str, Tuple[str, Dict[str, Tuple[str, Optional[Mapping[str, Any]]]]]] = {}
        for table, fields in (model_fields or {}).items():
            entries: Dict[str, Tuple[str, Optional[Mapping[str, Any]]]] = {}
            if isinstance(fields, Mapping):
                for name, meta in fields.items():
                    entries[str(name).lower()] = (str(name), meta if isinstance(meta, Mapping) else None)
            else:
                for name in fields:
                    entries[str(name).lower()] = (str(name), None)
            self.tables[str(table).lower()] = (str(table), entries)

    def resolve(self, ref: Any, where: str, errors: List[str], *, want: Optional[str] = None,
                role: Optional[str] = None, need_kind: bool = False) -> Optional[Json]:
        """A field reference -> {"table", "field", "kind", "nativeQueryRef", "aggregation"?}.

        want: the kind this slot requires ("Measure" for an HTML measure, "Column" for a slicer).
        role: the query role of a chart, which decides the fallback kind and whether a plain
              column is aggregated, the way the connector's own add_visual does.
        need_kind: refuse a reference whose kind cannot be told. Deneb and tables hold columns
              and measures side by side in one role, so the role says nothing there; and a
              table lists a column's values, so nothing is aggregated unless asked for.
        """
        kind: Optional[str] = None
        aggregation: Any = None
        if isinstance(ref, Mapping):
            table, name = pb.split_field(ref)
            if ref.get("kind"):
                kind = "Measure" if str(ref["kind"]).lower().startswith("meas") else "Column"
            elif "measure" in ref:
                kind = "Measure"
            elif "column" in ref:
                kind = "Column"
            aggregation = ref.get("aggregation")
        elif isinstance(ref, str):
            table, name = pbir_authoring.split_table_field(ref)
        else:
            errors.append(f"{where}: a field must be 'Table.Field' or an object, not {type(ref).__name__}")
            return None
        if not table or not name:
            errors.append(f"{where}: {ref!r} is not a field; write it as 'Table.Field'")
            return None

        meta: Optional[Mapping[str, Any]] = None
        if self.known:
            found = self.tables.get(table.lower())
            hit = found[1].get(name.lower()) if found else None
            if not hit:
                reason = "no such table" if not found else "no such field in that table"
                errors.append(f"{where}: {table}[{name}] is not in the model ({reason})")
                return None
            table, (name, meta) = found[0], hit
            model_kind = str((meta or {}).get("kind", "")).lower()
            if model_kind in ("measure", "column"):
                actual = "Measure" if model_kind == "measure" else "Column"
                if kind and kind != actual:
                    errors.append(f"{where}: {table}[{name}] is a {actual.lower()} in the model, "
                                  f"not a {kind.lower()}")
                    return None
                kind = actual
        if want and kind and kind != want:
            errors.append(f"{where}: {table}[{name}] is a {kind.lower()}; a {want.lower()} is needed here")
            return None
        aggregating = bool(role) and role.lower() in pbir_authoring.AGGREGATING_ROLES
        if kind is None:
            if want:
                kind = want
            elif need_kind:
                errors.append(f"{where}: cannot tell whether {table}[{name}] is a column or a measure; write "
                              f'{{"measure": "{table}.{name}"}} or {{"column": ...}}, or pass model_fields '
                              "with field kinds")
                return None
            else:
                kind = "Measure" if aggregating else "Column"
                self.guessed.append(f"{table}[{name}]")
        out: Json = {"table": table, "field": name, "kind": kind, "nativeQueryRef": name}
        if aggregation is not None:
            if isinstance(aggregation, str):
                if aggregation.lower() not in _AGGREGATIONS:
                    errors.append(f"{where}: unknown aggregation {aggregation!r}; "
                                  f"use one of {sorted(_AGGREGATIONS)}")
                    return None
                aggregation = _AGGREGATIONS[aggregation.lower()]
            if kind == "Measure":
                errors.append(f"{where}: {table}[{name}] is a measure and cannot take an aggregation")
                return None
            out["aggregation"] = int(aggregation)
        elif kind == "Column" and aggregating and meta and meta.get("dataType"):
            numeric = str(meta["dataType"]).lower() in _NUMERIC_TYPES
            out["aggregation"] = pbir_authoring.AGG_SUM if numeric else pbir_authoring.AGG_COUNT_NONNULL
        return out


# ---------------------------------------------------------------------------------------------
# small helpers
# ---------------------------------------------------------------------------------------------
def _is_num(v: Any) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def _name_error(value: Any, what: str) -> Optional[str]:
    if not isinstance(value, str) or not value:
        return f"{what}: a name is required"
    if not _NAME.match(value):
        return (f"{what}: {value!r} is not a usable name (letters, digits, '_' and '-' only, 50 characters "
                "at most; it becomes a folder or file name)")
    if value.lower() in _RESERVED:
        return f"{what}: {value!r} is a reserved file name on Windows"
    return None


def _subst(value: Any, tokens: Mapping[str, str], skip: Sequence[str] = ("spec", "config")) -> Any:
    """Replace {page} style tokens in every string. Deneb specs are left alone: braces are their syntax."""
    if isinstance(value, str):
        for token, text in tokens.items():
            value = value.replace("{" + token + "}", text)
        return value
    if isinstance(value, list):
        return [_subst(v, tokens, skip) for v in value]
    if isinstance(value, dict):
        return {k: (copy.deepcopy(v) if k in skip else _subst(v, tokens, skip)) for k, v in value.items()}
    return value


def _top_level_errors(spec: Any) -> List[str]:
    """Shape problems that make the rest of the spec unreadable."""
    if not isinstance(spec, Mapping):
        return ["the spec must be an object"]
    errors = [f"unknown key {k!r} at the top of the spec; known keys: {sorted(_TOP_KEYS)}"
              for k in spec if k not in _TOP_KEYS]
    for key in ("pages", "shared", "bookmarks", "custom_visuals", "page_order"):
        if key in spec and not isinstance(spec[key], list):
            errors.append(f"{key} must be a list")
    for key in ("theme", "page_defaults"):
        if key in spec and not isinstance(spec[key], Mapping):
            errors.append(f"{key} must be an object")
    for key in ("pages", "shared", "bookmarks", "custom_visuals"):
        for i, item in enumerate(spec.get(key) or [] if isinstance(spec.get(key), list) else []):
            if not isinstance(item, Mapping):
                errors.append(f"{key}[{i}] must be an object")
    return errors


def _custom_aliases(spec: Mapping[str, Any], errors: List[str]) -> Tuple[Dict[str, str], List[str]]:
    """({alias: path of the .pbiviz}, [AppSource ids]) from custom_visuals."""
    aliases: Dict[str, str] = {}
    public: List[str] = []
    for i, item in enumerate(spec.get("custom_visuals") or []):
        where = f"custom_visuals[{i}]"
        if isinstance(item.get("appsource"), str) and item["appsource"]:
            public.append(item["appsource"])
        elif isinstance(item.get("pbiviz"), str) and item["pbiviz"]:
            alias = item.get("alias") or os.path.splitext(os.path.basename(item["pbiviz"]))[0]
            if alias in aliases:
                errors.append(f"{where}: the alias {alias!r} is used twice")
            aliases[str(alias)] = item["pbiviz"]
        else:
            errors.append(f'{where}: give either "appsource": "<visual id>" or "pbiviz": "<path>"')
    return aliases, public


# ---------------------------------------------------------------------------------------------
# expansion: shared elements, pop-ups, groups, layering
# ---------------------------------------------------------------------------------------------
def _geometry(el: Mapping[str, Any]) -> Json:
    pos = dict(el.get("position") or {}) if isinstance(el.get("position"), Mapping) else {}
    for key in ("x", "y", "width", "height", "z"):
        if key in el:
            pos[key] = el[key]
    return pos


def _popup_parts(popup: Mapping[str, Any], page: _Page, where: str, errors: List[str]) -> Optional[Json]:
    """A pop-up as plain elements: the group, the veil, the box, a title, a close button, the
    caller's own elements, and the two bookmarks that show and hide the group."""
    err = _name_error(popup.get("name"), where)
    if err:
        errors.append(err)
        return None
    name = popup["name"]
    box = popup.get("box")
    if not isinstance(box, Mapping) or not all(_is_num(box.get(k)) for k in ("x", "y", "width", "height")):
        errors.append(f"{where}: box needs numeric x, y, width and height")
        return None
    open_name = popup.get("open_bookmark") or f"{page.name}_{name}_open"
    close_name = popup.get("close_bookmark") or f"{page.name}_{name}_close"
    for bm in (open_name, close_name):
        err = _name_error(bm, f"{where}: bookmark")
        if err:
            errors.append(err + '; set "open_bookmark" / "close_bookmark" to shorter names')
            return None
    close_action = {"type": "bookmark", "target": close_name, "tooltip": "Close"}
    veil = dict(popup.get("veil") or {})
    members: List[Json] = [
        {"name": f"{name}_veil", "type": "shape", "x": 0, "y": 0, "width": page.width, "height": page.height,
         "fill": veil.get("color", "#000000"), "transparency": veil.get("transparency", 60),
         "action": close_action},
        {"name": f"{name}_card", "type": "shape", "x": box["x"], "y": box["y"], "width": box["width"],
         "height": box["height"], "fill": box.get("fill", "#FFFFFF"), "radius": box.get("radius", 12),
         **({"outline": box["outline"]} if box.get("outline") else {})},
    ]
    close = dict(popup.get("close") or {})
    cw, ch = close.pop("width", 72), close.pop("height", 32)
    title = popup.get("title")
    if title:
        t = {"text": title} if isinstance(title, str) else dict(title)
        members.append({"name": f"{name}_title", "type": "textbox", "x": box["x"] + 24, "y": box["y"] + 16,
                        "width": max(1, box["width"] - 64 - cw), "height": 32, "size": 14, "bold": True, **t})
    members.append({"name": f"{name}_close", "type": "button", "x": box["x"] + box["width"] - 16 - cw,
                    "y": box["y"] + 16, "width": cw, "height": ch, "text": "Close", **close,
                    "action": close_action})
    own = popup.get("elements") or []
    if not isinstance(own, list):
        errors.append(f"{where}: elements must be a list")
        return None
    out: Json = {"group": {"name": name, "type": "group", "display_name": popup.get("display_name", name),
                           "hidden": popup.get("hidden", True)},
                 "members": members, "own": own, "open": open_name, "close": close_name, "button": None}
    if popup.get("open_button"):
        if not isinstance(popup["open_button"], Mapping):
            errors.append(f"{where}: open_button must be an object")
            return None
        out["button"] = {"type": "button", **popup["open_button"],
                         "action": {"type": "bookmark", "target": open_name}}
    return out


class _Expander:
    """Turns one page's declared elements into a flat, layered list of _Element."""

    def __init__(self, page: _Page, errors: List[str], warnings: List[str]):
        self.page, self.errors, self.warnings = page, errors, warnings
        self.seen: Dict[str, str] = {}

    def add(self, spec: Any, where: str, z: int, parent: Optional[str] = None,
            hidden_default: bool = False) -> Optional[_Element]:
        if not isinstance(spec, Mapping):
            self.errors.append(f"{where} must be an object")
            return None
        err = _name_error(spec.get("name"), where)
        if err:
            self.errors.append(err)
            return None
        name, kind = spec["name"], spec.get("type")
        where = f"page {self.page.name!r}, element {name!r}"
        if kind == "actionButton":
            kind = "button"
        if kind not in ELEMENT_TYPES:
            self.errors.append(f"{where}: unknown type {kind!r}; use one of {list(ELEMENT_TYPES)}")
            return None
        if name.lower() in self.seen:
            self.errors.append(f"{where}: the name is already used on this page ({self.seen[name.lower()]})")
            return None
        self.seen[name.lower()] = where
        for key in spec:
            if key not in _COMMON_KEYS and key not in _TYPE_KEYS[kind]:
                self.warnings.append(f"{where}: {key!r} is not a setting of a {kind} and was ignored")
        el = _Element(self.page.name, name, kind, dict(spec), where, parent=parent,
                      hidden=bool(spec.get("hidden", hidden_default)))
        geo = _geometry(spec)
        el.z = int(geo["z"]) if _is_num(geo.get("z")) else z
        if kind == "group":
            # A group spans the page and its members keep page coordinates: the one arrangement
            # seen in reports that open, so member positions never depend on the group's own box.
            el.x, el.y, el.width, el.height = 0, 0, self.page.width, self.page.height
        else:
            missing = [k for k in ("x", "y", "width", "height") if not _is_num(geo.get(k))]
            if missing:
                self.errors.append(f"{where}: needs numeric {', '.join(missing)}")
                return None
            el.x, el.y, el.width, el.height = geo["x"], geo["y"], geo["width"], geo["height"]
            if el.width <= 0 or el.height <= 0:
                self.errors.append(f"{where}: width and height must be greater than 0")
                return None
            outside = (el.x < 0 or el.y < 0 or el.x + el.width > self.page.width + 0.5
                       or el.y + el.height > self.page.height + 0.5)
            if outside and not spec.get("allow_overflow"):
                self.errors.append(f"{where}: at ({el.x}, {el.y}) size {el.width} x {el.height} it does not "
                                   f"fit the {self.page.width} x {self.page.height} page "
                                   '(set "allow_overflow": true if that is intended)')
                return None
        self.page.elements.append(el)
        return el

    def add_group(self, spec: Mapping[str, Any], where: str, z: int) -> int:
        """Add a group and its members. Returns the next free z."""
        group = self.add(spec, where, z)
        members = spec.get("elements") or []
        if not isinstance(members, list):
            self.errors.append(f"{where}: elements must be a list")
            members = []
        if group:
            for j, member in enumerate(members):
                if isinstance(member, Mapping) and member.get("type") == "group":
                    self.errors.append(f"{where}: a group cannot hold another group")
                    continue
                self.add(member, f"{where}, member {j}", group.z + 1 + j, parent=group.name)
        return z + _Z_STEP * (len(members) // _Z_STEP + 1)

    def add_list(self, specs: Sequence[Any], where: str, z: int) -> int:
        for i, spec in enumerate(specs):
            if isinstance(spec, Mapping) and spec.get("type") == "group":
                z = self.add_group(spec, f"{where}[{i}]", z)
            else:
                self.add(spec, f"{where}[{i}]", z)
                z += _Z_STEP
        return z


def _expand(spec: Mapping[str, Any], out: _Compiled) -> List[Json]:
    """Fill out.pages with layered elements. Returns the pop-up bookmarks to build later."""
    errors, warnings = out.errors, out.warnings
    defaults = dict(spec.get("page_defaults") or {})
    pages: List[_Page] = []
    seen: Dict[str, int] = {}
    for i, p in enumerate(spec.get("pages") or []):
        where = f"pages[{i}]"
        err = _name_error(p.get("name"), where)
        if err:
            errors.append(err)
            continue
        if p["name"].lower() in seen:
            errors.append(f"{where}: the page name {p['name']!r} is used twice")
            continue
        seen[p["name"].lower()] = i
        for key in p:
            if key not in _PAGE_KEYS:
                errors.append(f"page {p['name']!r}: unknown key {key!r}; known keys: {sorted(_PAGE_KEYS)}")
        unused = sorted(k for k in p if k in _PAGE_KEYS - {"name", "existing", "elements", "popups"})
        if p.get("existing") and unused:
            warnings.append(f'page {p["name"]!r} is marked "existing", so its page.json is left alone and '
                            f"{', '.join(unused)} had no effect")
        merged = {**defaults, **p}
        width, height = merged.get("width", 1280), merged.get("height", 720)
        if not _is_num(width) or not _is_num(height) or width <= 0 or height <= 0:
            errors.append(f"page {p['name']!r}: width and height must be numbers greater than 0")
            continue
        for key in ("elements", "popups"):
            if key in p and not isinstance(p[key], list):
                errors.append(f"page {p['name']!r}: {key} must be a list")
                merged[key] = []
        pages.append(_Page(p["name"], str(merged.get("display_name") or p["name"]), width, height,
                           bool(p.get("existing")), merged))
    out.pages = pages

    shared_by_page: Dict[str, List[Json]] = {p.name: [] for p in pages}
    for i, sh in enumerate(spec.get("shared") or []):
        where = f"shared[{i}]"
        targets = sh.get("pages", "*")
        if targets == "*":
            targets = [p.name for p in pages]
        if not isinstance(targets, list):
            errors.append(f'{where}: pages must be "*" or a list of page names')
            continue
        for key in ("elements", "popups"):
            if key in sh and not isinstance(sh[key], list):
                errors.append(f"{where}: {key} must be a list")
        excluded = {str(x).lower() for x in sh.get("exclude") or []}
        for target in targets:
            if target not in shared_by_page:
                errors.append(f"{where}: page {target!r} is not a page of this spec")
            elif str(target).lower() not in excluded:
                shared_by_page[target].append(sh)

    popup_bookmarks: List[Json] = []
    for page in pages:
        tokens = {"page": page.name, "page_title": page.display_name}
        ex = _Expander(page, errors, warnings)
        ex.add_list(page.spec.get("elements") or [], f"page {page.name!r}, elements", _Z_CONTENT)
        popups = [(pp, f"page {page.name!r}, popups[{j}]") for j,
                  pp in enumerate(page.spec.get("popups") or [])]
        z = _Z_SHARED
        for sh in shared_by_page[page.name]:
            label = f"shared {sh.get('name', '')!r} on page {page.name!r}"
            if isinstance(sh.get("elements"), list):
                z = ex.add_list(_subst(sh["elements"], tokens), label, z)
            if isinstance(sh.get("popups"), list):
                popups += [(_subst(pp, tokens), f"{label}, popups[{j}]") for j, pp in enumerate(sh["popups"])]
        for k, (popup, where) in enumerate(popups):
            if not isinstance(popup, Mapping):
                errors.append(f"{where} must be an object")
                continue
            parts = _popup_parts(popup, page, where, errors)
            if not parts:
                continue
            group = ex.add(parts["group"], where, _Z_POPUP_GROUP + 1000 * k)
            if not group:
                continue
            base = _Z_POPUP_ITEMS + 1000 * k
            for j, member in enumerate(parts["members"]):
                ex.add(member, where, base + j, parent=group.name)
            for j, member in enumerate(parts["own"]):
                if isinstance(member, Mapping) and member.get("type") == "group":
                    errors.append(f"{where}: a pop-up cannot hold a group")
                    continue
                ex.add(member, f"{where}, elements[{j}]", base + 10 + j, parent=group.name)
            if parts["button"]:
                ex.add(parts["button"], f"{where}, open_button", z)
                z += _Z_STEP
            title = f"{page.display_name}: {group.name}"
            popup_bookmarks.append({"name": parts["open"], "display_name": f"{title} (open)",
                                    "page": page.name,
                                    "show": [group.name], "hide": []})
            popup_bookmarks.append({"name": parts["close"], "display_name": f"{title} (closed)",
                                    "page": page.name,
                                    "show": [], "hide": [group.name]})
        zs: Dict[int, str] = {}
        for el in page.elements:
            if el.z in zs:
                warnings.append(f"page {page.name!r}: {el.name!r} and {zs[el.z]!r} share z {el.z}; "
                                "which one is on top is then up to Power BI")
            zs[el.z] = el.name
    return popup_bookmarks


# ---------------------------------------------------------------------------------------------
# building documents
# ---------------------------------------------------------------------------------------------
@dataclass
class _Context:
    fields: _Fields
    schemas: Mapping[str, str]
    guids: Optional[Mapping[str, str]]          # alias -> guid; None when no package was read
    aliases: Mapping[str, str]
    page_names: Set[str]
    bookmark_names: Set[str]


def _action_link(el: _Element, ctx: _Context, errors: List[str]) -> Optional[List[Json]]:
    action = el.spec.get("action")
    if action is None:
        return None
    if el.type not in _LINKABLE:
        errors.append(f"{el.where}: only a button, shape or image can carry an action")
        return None
    if not isinstance(action, Mapping) or action.get("type") not in ACTION_TYPES:
        errors.append(f'{el.where}: action needs "type", one of {list(ACTION_TYPES)}')
        return None
    kind, target = action["type"], action.get("target")
    if kind in ("page", "bookmark", "url") and (not isinstance(target, str) or not target):
        errors.append(f'{el.where}: a {kind} action needs "target"')
        return None
    if kind == "page" and target.lower() not in ctx.page_names:
        errors.append(f"{el.where}: the action opens page {target!r}, which does not exist")
        return None
    if kind == "bookmark" and target.lower() not in ctx.bookmark_names:
        errors.append(f"{el.where}: the action applies bookmark {target!r}, which does not exist")
        return None
    if kind == "url" and not re.match(r"^https?://", target):
        errors.append(f"{el.where}: a url action needs an http:// or https:// address")
        return None
    return pb.action_link(kind, target, tooltip=action.get("tooltip"))


def _container(el: _Element, link: Optional[List[Json]]) -> Json:
    spec = el.spec
    plain = spec.get("chrome") == "plain" or (el.type in _PLAIN_TYPES and spec.get("chrome") != "theme")
    kw: Json = dict(pb.PLAIN_CHROME) if plain else {}
    for key in _CHROME_KEYS:
        if key in spec:
            kw[key] = spec[key]
    return pb.container_objects(**kw, link=link, extra=pb.simple_objects(spec.get("container")))


def _paragraphs(el: _Element, errors: List[str]) -> Optional[List[Json]]:
    spec = el.spec
    style = {k: spec[k] for k in ("font", "size", "color", "bold", "italic", "underline") if k in spec}
    if "paragraphs" in spec:
        if not isinstance(spec["paragraphs"], list):
            errors.append(f"{el.where}: paragraphs must be a list")
            return None
        out = []
        for para in spec["paragraphs"]:
            para = {"text": para} if isinstance(para, str) else dict(para)
            built = []
            for run in para.get("runs") or [{"text": para.get("text", "")}]:
                run = {"text": run} if isinstance(run, str) else dict(run)
                own = {k: run[k] for k in ("font", "size", "color", "bold", "italic",
                                           "underline") if k in run}
                built.append(pb.text_run(run.get("text", ""), **{**style, **own}))
            out.append(pb.paragraph(built, para.get("align", spec.get("align"))))
        return out
    if not isinstance(spec.get("text"), str):
        errors.append(f'{el.where}: a textbox needs "text" or "paragraphs"')
        return None
    return [pb.paragraph([pb.text_run(line, **style)],
                         spec.get("align")) for line in spec["text"].split("\n")]


def _build_element(el: _Element, page: _Page, ctx: _Context, out: _Compiled) -> Optional[Json]:
    errors = out.errors
    before = len(errors)
    spec = el.spec
    tab = spec.get("tab_order") if _is_num(spec.get("tab_order")) else None
    pos = pb.position(el.x, el.y, el.width, el.height, el.z, tab)
    schema = ctx.schemas.get("visual")
    if el.type == "group":
        return pb.visual_group(el.name, pos, str(spec.get("display_name") or el.name), hidden=el.hidden,
                               schema_url=schema)
    link = _action_link(el, ctx, errors)
    frame = _container(el, link)
    common = {"container": frame, "schema_url": schema, "parent_group": el.parent, "hidden": el.hidden}
    objects = pb.simple_objects(spec.get("objects")) if isinstance(spec.get("objects"), Mapping) else None
    if "objects" in spec and objects is None:
        errors.append(f"{el.where}: objects must be an object of formatting objects")

    def merged(doc: Json) -> Json:
        if objects:
            doc["visual"].setdefault("objects", {}).update(objects)
        return doc

    if el.type == "textbox":
        paragraphs = _paragraphs(el, errors)
        return merged(pb.textbox(el.name, pos, paragraphs, **common)) if paragraphs is not None else None
    if el.type == "shape":
        return merged(pb.shape(el.name, pos, fill=spec.get("fill", "#FFFFFF"),
                               transparency=spec.get("transparency", 0),
                               radius=spec.get("radius", 0), outline=spec.get("outline"),
                               outline_weight=spec.get("outline_weight", 1), **common))
    if el.type == "image":
        if bool(spec.get("resource")) == bool(spec.get("measure")):
            errors.append(f'{el.where}: an image needs exactly one of "resource" or "measure"')
            return None
        measure = None
        if spec.get("measure"):
            measure = ctx.fields.resolve(spec["measure"], el.where, errors, want="Measure")
            if not measure:
                return None
        else:
            out.images.append(str(spec["resource"]))
        return merged(pb.image(el.name, pos, resource=spec.get("resource"), measure=measure,
                               scaling=spec.get("scaling"), transparency=spec.get("transparency", 0),
                               **common))
    if el.type == "button":
        style = {k: spec[k] for k in ("text", "font", "size", "color", "bold", "fill", "fill_transparency",
                                      "outline", "outline_weight", "radius", "states") if k in spec}
        action = spec.get("action") if isinstance(spec.get("action"), Mapping) else {}
        own_page = action.get("type") == "page" and str(action.get("target", "")).lower() == page.name.lower()
        if own_page and isinstance(spec.get("active"), Mapping):
            # The button for the page you are on: restyled, and no longer a link to itself.
            style.update({k: v for k, v in spec["active"].items() if k in _TYPE_KEYS["button"]})
            common["container"] = _container(el, None)
        if "states" in style and not isinstance(style["states"], Mapping):
            errors.append(f"{el.where}: states must be an object")
            return None
        return merged(pb.action_button(el.name, pos, **style, **common))
    if el.type == "slicer":
        column = ctx.fields.resolve(spec.get("field"), el.where, errors, want="Column")
        if not column:
            return None
        sync = spec.get("sync_group")
        if sync is True:
            sync = re.sub(r"[^A-Za-z0-9_]+", "_", f"{column['table']}_{column['field']}")
        return merged(pb.slicer(el.name, pos, column, mode=spec.get("mode", "dropdown"),
                                single_select=spec.get("single_select"),
                                strict_single_select=spec.get("strict_single_select"),
                                select_all=spec.get("select_all"), header=spec.get("header"),
                                items=spec.get("items"),
                                sync_group=sync or None, **common))
    if el.type == "html":
        measure = ctx.fields.resolve(spec.get("measure"), el.where, errors, want="Measure")
        if not measure:
            return None
        visual_id = str(spec.get("visual_id") or pb.HTML_CONTENT_ID)
        out.public_ids.append(visual_id)
        return merged(pb.html_visual(el.name, pos, measure, visual_type=visual_id, **common))
    if el.type == "deneb":
        refs = spec.get("fields")
        if not isinstance(refs, list) or not refs:
            errors.append(f'{el.where}: a Deneb visual needs "fields", the list of columns and measures '
                          "it reads")
            return None
        if not isinstance(spec.get("spec"), (Mapping, str)):
            errors.append(f'{el.where}: a Deneb visual needs "spec", the Vega-Lite (or Vega) specification')
            return None
        resolved = [ctx.fields.resolve(r, el.where, errors, need_kind=True) for r in refs]
        if None in resolved:
            return None
        out.public_ids.append(pb.DENEB_ID)
        return merged(pb.deneb_visual(el.name, pos, resolved, spec["spec"], config=spec.get("config"),
                                      provider=spec.get("provider", "vegaLite"), version=spec.get("version"),
                                      tooltips=spec.get("tooltips", True),
                                      selection=bool(spec.get("selection", False)),
                                      max_selected=spec.get("max_selected", 50), **common))
    # a native visual, an AppSource visual by id, or a private custom visual by alias
    custom = spec.get("custom")
    if custom:
        if custom not in ctx.aliases:
            errors.append(f"{el.where}: custom visual {custom!r} is not declared in custom_visuals")
            return None
        visual_type = (ctx.guids or {}).get(custom, str(custom))
    else:
        visual_type = spec.get("visual_type")
        if not isinstance(visual_type, str) or not visual_type:
            errors.append(f'{el.where}: a visual needs "visual_type" (or "custom")')
            return None
    roles = spec.get("fields") or {}
    if not isinstance(roles, Mapping):
        errors.append(f"{el.where}: fields must be an object of role: field or [fields]")
        return None
    table = visual_type == "tableEx"
    resolved_roles: Json = {}
    for role, refs in roles.items():
        items = [ctx.fields.resolve(r, f"{el.where}, role {role!r}", errors, role=None if table else role,
                                    need_kind=table or bool(custom))
                 for r in (refs if isinstance(refs, list) else [refs])]
        if None not in items and items:
            resolved_roles[role] = items
    if len(errors) > before:
        return None
    return merged(pb.data_visual(el.name, visual_type, pos, resolved_roles,
                                 mark_active=not (table or custom), **common))


def _build_bookmark(bm: Mapping[str, Any], where: str, out: _Compiled,
                    schema: Optional[str]) -> Optional[Json]:
    err = _name_error(bm.get("name"), where)
    if err:
        out.errors.append(err)
        return None
    page = next((p for p in out.pages if p.name.lower() == str(bm.get("page", "")).lower()), None)
    if not page:
        out.errors.append(f"{where}: page {bm.get('page')!r} is not a page of this spec")
        return None
    by_name = {el.name.lower(): el for el in page.elements}
    groups: Dict[str, bool] = {}
    visuals: Dict[str, Json] = {}
    for key, hidden in (("show", False), ("hide", True)):
        names = bm.get(key) or []
        if not isinstance(names, list):
            out.errors.append(f"{where}: {key} must be a list of element names")
            return None
        for name in names:
            el = by_name.get(str(name).lower())
            if not el:
                out.errors.append(f"{where}: {key} names {name!r}, which is not an element of "
                                  f"page {page.name!r}")
                return None
            if el.type == "group":
                groups[el.name] = hidden
            elif el.doc:
                visuals[el.name] = {"visual_type": el.doc["visual"]["visualType"], "hidden": hidden}
    display = str(bm.get("display_name") or bm["name"])
    if bm.get("targeted", True):
        if not groups and not visuals:
            out.errors.append(f'{where}: a bookmark needs "show" or "hide" '
                              '(or "targeted": false to only open a page)')
            return None
        return pb.visibility_bookmark(bm["name"], display, page.name, groups=groups, visuals=visuals,
                                      schema_url=schema)
    return pb.bookmark(bm["name"], display, page.name, groups=groups, visuals=visuals, schema_url=schema)


def _compile(spec: Any, *, model_fields: Optional[Mapping[str, Any]] = None,
             schemas: Optional[Mapping[str, str]] = None, guids: Optional[Mapping[str, str]] = None,
             existing_pages: Optional[Sequence[str]] = None,
             existing_bookmarks: Optional[Sequence[str]] = None) -> _Compiled:
    """Validate the spec and build every document it describes. Reads nothing from disk."""
    out = _Compiled()
    out.errors.extend(_top_level_errors(spec))
    if out.errors:
        return out
    schemas = schemas or pb.DEFAULT_SCHEMAS
    aliases, public = _custom_aliases(spec, out.errors)
    out.public_ids.extend(public)
    popup_bookmarks = _expand(spec, out)

    declared = popup_bookmarks + [b for b in spec.get("bookmarks") or []]
    names: Dict[str, str] = {}
    for i, bm in enumerate(declared):
        key = str(bm.get("name", "")).lower()
        if key and key in names:
            out.errors.append(f"bookmark {bm.get('name')!r} is defined twice")
        names[key] = str(bm.get("name", ""))
    ctx = _Context(_Fields(model_fields), schemas, guids, aliases,
                   {p.name.lower() for p in out.pages} | {str(p).lower() for p in existing_pages or []},
                   set(names) | {str(b).lower() for b in existing_bookmarks or []})

    for page in out.pages:
        if not page.existing:
            try:
                background = page.spec.get("background")
                if isinstance(background, Mapping) and background.get("image"):
                    out.images.append(str(background["image"]))
                page.doc = pb.page(page.name, page.display_name, page.width, page.height,
                                   display_option=page.spec.get("display_option", "FitToPage"),
                                   background=background, wallpaper=page.spec.get("wallpaper"),
                                   hidden=bool(page.spec.get("hidden")),
                                   tooltip=bool(page.spec.get("tooltip")),
                                   schema_url=schemas.get("page"))
            except (KeyError, TypeError, ValueError, AttributeError) as e:
                out.errors.append(f"page {page.name!r}: {e}")
        for el in page.elements:
            try:
                el.doc = _build_element(el, page, ctx, out)
            except (KeyError, TypeError, ValueError, AttributeError) as e:
                out.errors.append(f"{el.where}: {e}")
    for i, bm in enumerate(declared):
        where = f"bookmark {bm.get('name')!r}" if bm.get("name") else f"bookmarks[{i - len(popup_bookmarks)}]"
        doc = _build_bookmark(bm, where, out, schemas.get("bookmark"))
        if doc:
            out.bookmarks.append(doc)

    theme = spec.get("theme")
    if theme is not None:
        if not isinstance(theme.get("name"), str) or not theme["name"]:
            out.errors.append('theme: "name" is required')
        else:
            file_name = theme.get("file") or (re.sub(r"[^A-Za-z0-9_-]+", "-",
                                                     theme["name"]).strip("-") or "theme") + ".json"
            if not re.match(r"^[A-Za-z0-9 _.-]+\.json$", str(file_name)):
                out.errors.append(f"theme: {file_name!r} is not a usable file name (it must end in .json)")
            else:
                try:
                    out.theme_doc = pb.theme(theme["name"], colors=theme.get("colors"),
                                             fonts=theme.get("fonts"),
                                             card=theme.get("card"),
                                             plain_types=theme.get("plain_types", pb.THEME_PLAIN_TYPES),
                                             extra=theme.get("extra"))
                    out.theme_file = str(file_name)
                except (KeyError, TypeError, ValueError, AttributeError) as e:
                    out.errors.append(f"theme: {e}")

    order = spec.get("page_order") or []
    for name in order:
        if str(name).lower() not in ctx.page_names:
            out.errors.append(f"page_order names {name!r}, which is not a page")
    out.page_order = [str(n) for n in order]
    active = spec.get("active_page")
    if active is not None:
        if str(active).lower() not in ctx.page_names:
            out.errors.append(f"active_page is {active!r}, which is not a page")
        out.active_page = str(active)
    out.public_ids = list(dict.fromkeys(out.public_ids))
    out.images = list(dict.fromkeys(out.images))
    guessed = list(dict.fromkeys(ctx.fields.guessed))
    if guessed:
        shown = ", ".join(guessed[:5]) + (", ..." if len(guessed) > 5 else "")
        out.warnings.append(f"{len(guessed)} field(s) were read as a measure or a column from the role they "
                            f"sit in ({shown}); pass model_fields, or write "
                            '{"measure": ...} / {"column": ...}, to be sure')
    return out


def validate_report_spec(spec: Any, *, model_fields: Optional[Mapping[str, Any]] = None,
                         existing_pages: Optional[Sequence[str]] = None,
                         existing_bookmarks: Optional[Sequence[str]] = None) -> List[str]:
    """Problems that would stop the spec being applied. An empty list means it can be applied.

    This looks at the spec alone, so a button may only point at a page or bookmark the spec
    defines, unless the report's own are passed as ``existing_pages`` / ``existing_bookmarks``.
    To check a spec against a real report folder (name clashes, registered images, custom
    visual packages), call apply_report_spec(..., dry_run=True) instead.
    """
    return _compile(spec, model_fields=model_fields, existing_pages=existing_pages,
                    existing_bookmarks=existing_bookmarks).errors


# ---------------------------------------------------------------------------------------------
# the report folder as it is
# ---------------------------------------------------------------------------------------------
@dataclass
class _State:
    pages: Dict[str, Set[str]] = field(default_factory=dict)        # page folder -> visual folder names
    page_json: Set[str] = field(default_factory=set)                # page folders that hold a page.json
    bookmarks: Set[str] = field(default_factory=set)
    schemas: Dict[str, str] = field(default_factory=dict)
    visual_types: Dict[str, str] = field(default_factory=dict)      # "page/visual" (lower case) -> visualType
    report: Optional[Json] = None
    pages_meta: Optional[Json] = None
    bookmarks_meta: Optional[Json] = None
    manifest: Json = field(default_factory=dict)


_SCHEMA_IN_TEXT = re.compile(r'"\$schema"\s*:\s*"([^"]+)"')
_TYPE_IN_TEXT = re.compile(r'"visualType"\s*:\s*"([^"]+)"')
_VERSION_IN_URL = re.compile(r"/(\d+)\.(\d+)\.(\d+)/schema\.json$")


def _newest(urls: Sequence[str], default: str) -> str:
    """The highest schema version among sibling files. Taking the newest keeps a rerun stable:
    files this module wrote carry that same version, so the answer does not move."""
    best, best_key = default, None
    for url in urls:
        m = _VERSION_IN_URL.search(url)
        if m:
            key = tuple(int(g) for g in m.groups())
            if best_key is None or key > best_key:
                best, best_key = url, key
    return best


def _read_json_file(path: Path, what: str, errors: List[str]) -> Optional[Json]:
    try:
        data = safe_io.read_json(path)
    except (OSError, ValueError) as e:
        errors.append(f"{what} cannot be read as JSON ({e}); fix it before applying a spec")
        return None
    if not isinstance(data, dict):
        errors.append(f"{what} is not a JSON object")
        return None
    return data


def _read_state(root: Path, manifest_file: Path, errors: List[str]) -> _State:
    state = _State()
    definition = root / "definition"
    found: Dict[str, List[str]] = {"visual": [], "page": [], "bookmark": []}
    pages_dir = definition / "pages"
    if pages_dir.is_dir():
        for page_dir in sorted(p for p in pages_dir.iterdir() if p.is_dir()):
            visuals: Set[str] = set()
            if (page_dir / "page.json").is_file():
                state.page_json.add(page_dir.name)
                m = _SCHEMA_IN_TEXT.search(_head(page_dir / "page.json"))
                if m:
                    found["page"].append(m.group(1))
            visuals_dir = page_dir / "visuals"
            if visuals_dir.is_dir():
                for visual_dir in sorted(v for v in visuals_dir.iterdir() if v.is_dir()):
                    visuals.add(visual_dir.name)
                    vfile = visual_dir / "visual.json"
                    if vfile.is_file():
                        text = _text(vfile)
                        m = _SCHEMA_IN_TEXT.search(text[:600])
                        if m:
                            found["visual"].append(m.group(1))
                        t = _TYPE_IN_TEXT.search(text)
                        if t:
                            state.visual_types[f"{page_dir.name}/{visual_dir.name}".lower()] = t.group(1)
            state.pages[page_dir.name] = visuals
    bookmarks_dir = definition / "bookmarks"
    if bookmarks_dir.is_dir():
        for f in sorted(bookmarks_dir.glob("*.bookmark.json")):
            state.bookmarks.add(f.name[:-len(".bookmark.json")])
            m = _SCHEMA_IN_TEXT.search(_head(f))
            if m:
                found["bookmark"].append(m.group(1))
    state.schemas = dict(pb.DEFAULT_SCHEMAS)
    for kind, urls in found.items():
        state.schemas[kind] = _newest(urls, pb.DEFAULT_SCHEMAS[kind])
    for attr, path, what in (("report", definition / "report.json", "definition/report.json"),
                             ("pages_meta", pages_dir / "pages.json", "definition/pages/pages.json"),
                             ("bookmarks_meta", bookmarks_dir / "bookmarks.json",
                              "definition/bookmarks/bookmarks.json")):
        if path.is_file():
            setattr(state, attr, _read_json_file(path, what, errors))
    if manifest_file.is_file():
        manifest = _read_json_file(manifest_file, f"the manifest {manifest_file.name}", errors)
        if manifest is not None:
            if manifest.get("version") != MANIFEST_VERSION:
                errors.append(f"the manifest {manifest_file.name} has version {manifest.get('version')!r}; "
                              f"this server writes version {MANIFEST_VERSION}")
            else:
                state.manifest = manifest
    return state


def _text(path: Path) -> str:
    try:
        return safe_io.read_text(path)[0]
    except (OSError, UnicodeDecodeError):
        return ""


def _head(path: Path) -> str:
    return _text(path)[:600]


def _lower(names: Any) -> Set[str]:
    return {str(n).lower() for n in names or []}


# ---------------------------------------------------------------------------------------------
# apply
# ---------------------------------------------------------------------------------------------
def _carry_over(doc: Json, path: Path, owned: Set[str]) -> Json:
    """Keep the keys of an existing file that the spec has no say over."""
    if not path.is_file():
        return doc
    try:
        current = safe_io.read_json(path)
    except (OSError, ValueError):
        return doc
    if not isinstance(current, dict):
        return doc
    out = dict(doc)
    for key, value in current.items():
        if key not in owned and key not in out:
            out[key] = value
    return out


def _same_json(path: Path, doc: Any) -> bool:
    """True when the file already holds this document, whatever its formatting. Such a file is
    left alone, so a file Desktop re-saved with its own layout is not rewritten for nothing."""
    if not path.is_file():
        return False
    try:
        return safe_io.read_json(path) == doc
    except (OSError, ValueError):
        return False


def _remove_empty_dirs(paths: Sequence[Path], stop: Path) -> None:
    """Drop folders left empty by removed files, up to (not including) ``stop``. rmdir only
    ever removes an empty folder, so nothing that still holds a file can go."""
    for path in sorted({p.parent for p in paths}, key=lambda p: len(p.parts), reverse=True):
        try:
            (path / "visuals").rmdir()          # a page whose last visual went earlier
        except OSError:
            pass
        current = path
        while current != stop and stop in current.parents:
            try:
                current.rmdir()
            except OSError:
                break
            current = current.parent


def _strip_bookmarks(items: List[Any], gone: Set[str]) -> List[Any]:
    out = []
    for item in items:
        if isinstance(item, dict) and str(item.get("name", "")).lower() in gone:
            continue
        if isinstance(item, dict) and isinstance(item.get("children"), list):
            item = {**item, "children": [c for c in item["children"] if str(c).lower() not in gone]}
        out.append(item)
    return out


def _listed_bookmarks(items: List[Any]) -> Set[str]:
    names: Set[str] = set()
    for item in items:
        if isinstance(item, dict):
            names.add(str(item.get("name", "")).lower())
            names |= {str(c).lower() for c in item.get("children") or []}
    return names


def apply_report_spec(report_dir: Any, spec: Any, *, model_fields: Optional[Mapping[str, Any]] = None,
                      prune: bool = False, dry_run: bool = False, adopt: bool = False,
                      manifest_path: Any = None) -> Json:
    """Make the report folder match the spec. See the module docstring for the spec and the options."""
    result: Json = {"success": False, "written": [], "removed": [], "unchanged": 0, "warnings": [],
                    "errors": [],
                    "dry_run": bool(dry_run)}
    errors, warnings = result["errors"], result["warnings"]
    root = Path(os.path.abspath(os.fspath(report_dir)))
    if not root.is_dir() or not (root / "definition.pbir").is_file():
        errors.append(f"{root} is not a report folder: it must exist and contain definition.pbir. Create the "
                      "report in Power BI Desktop (File > Save as > Power BI project) first; this tool fills "
                      "a report in, it does not connect one to a model.")
        return result
    definition = root / "definition"
    if not definition.is_dir() and (root / "report.json").is_file():
        errors.append("this report is in the older single-file format (report.json beside definition.pbir). "
                      "Save it from Power BI Desktop in the PBIR format first; a spec can only be applied "
                      "to that.")
        return result
    errors.extend(_top_level_errors(spec))
    if errors:
        return result
    manifest_file = Path(os.path.abspath(os.fspath(manifest_path))) if manifest_path \
        else root.parent / (root.name + MANIFEST_SUFFIX)
    result["manifest"] = _rel(manifest_file, root)

    state = _read_state(root, manifest_file, errors)
    aliases, _ = _custom_aliases(spec, [])
    packages: Dict[str, Json] = {}
    guids: Dict[str, str] = {}
    for alias, path in aliases.items():
        full = path if os.path.isabs(path) else str(root.parent / path)
        try:
            package = pbir_packages.read_pbiviz(full)
        except pbir_packages.PbivizError as e:
            errors.append(str(e))
            continue
        guids[alias] = package["guid"]
        packages[package["guid"]] = package
    if errors:
        return result

    compiled = _compile(spec, model_fields=model_fields, schemas=state.schemas, guids=guids,
                        existing_pages=sorted(state.page_json), existing_bookmarks=sorted(state.bookmarks))
    errors.extend(compiled.errors)
    warnings.extend(compiled.warnings)
    if errors:
        return result

    old = state.manifest
    old_pages: Json = old.get("pages") or {}
    old_page = {str(k).lower(): v for k, v in old_pages.items()}
    old_bookmarks = _lower(old.get("bookmarks"))
    disk_pages = {name.lower(): name for name in state.pages}
    disk_bookmarks = _lower(state.bookmarks)

    # ---- what is already there under a name the spec uses
    for page in compiled.pages:
        key = page.name.lower()
        managed = old_page.get(key) or {}
        on_disk = key in disk_pages and disk_pages[key] in state.page_json
        if page.existing and not on_disk:
            errors.append(f'page {page.name!r} is marked "existing" but is not in the report')
            continue
        if not page.existing and on_disk and not managed.get("owned") and not adopt:
            errors.append(f"page {page.name!r} already exists and was not created by an earlier apply. Mark "
                          'it "existing": true to add elements to it as it is, pick another name, or pass '
                          "adopt=True to take it over.")
            continue
        existing_visuals = _lower(state.pages.get(disk_pages.get(key, ""), set()))
        managed_visuals = _lower(managed.get("elements"))
        for el in page.elements:
            if el.name.lower() in existing_visuals and el.name.lower() not in managed_visuals and not adopt:
                errors.append(f"{el.where}: a visual of that name is already on the page and was not created "
                              "by an earlier apply. Pick another name, or pass adopt=True to take it over.")
    for doc in compiled.bookmarks:
        if doc["name"].lower() in disk_bookmarks and doc["name"].lower() not in old_bookmarks and not adopt:
            errors.append(f"bookmark {doc['name']!r} already exists and was not created by an earlier apply. "
                          "Pick another name, or pass adopt=True to take it over.")
    resources = root / "StaticResources" / "RegisteredResources"
    old_theme: Json = old.get("theme") or {}
    if compiled.theme_file and (resources / compiled.theme_file).is_file() \
            and str(old_theme.get("file", "")).lower() != compiled.theme_file.lower() and not adopt:
        errors.append(f"the theme file {compiled.theme_file!r} already exists and was not created by an "
                      'earlier apply. Give the theme another "file" name, or pass adopt=True to '
                      "overwrite it.")
    for image_name in compiled.images:
        if not (resources / image_name).is_file():
            errors.append(f"image {image_name!r} is not in StaticResources/RegisteredResources. Copy the "
                          "file there first; a spec registers images, it does not create them.")
    old_custom: Json = old.get("custom_visuals") or {}
    custom_files: Dict[str, Tuple[str, safe_io.TextFormat]] = {}
    for guid, package in packages.items():
        target = root / "CustomVisuals" / guid
        if target.is_dir() and any(target.iterdir()) and guid not in old_custom and not adopt:
            errors.append(f"custom visual {guid} is already unpacked in CustomVisuals and was not put there "
                          "by an earlier apply. Pass adopt=True to replace it with this package.")
            continue
        files = pbir_packages.custom_visual_files(guid, package["files"])
        too_long = pbir_packages.path_length_problems(str(root), files)
        if too_long:
            errors.append(f"custom visual {guid}: the path of {too_long[0]} under {root} would be "
                          f"{pbir_packages.WINDOWS_PATH_LIMIT} characters or more, and Power BI Desktop "
                          "drops a visual it cannot open. Move the project to a shorter folder.")
            continue
        for rel, data in files.items():
            try:
                custom_files[rel] = safe_io.decode(data)
            except UnicodeDecodeError:
                errors.append(f"custom visual {guid}: {rel} is not text; only text members can be unpacked")
    if errors:
        return result

    # ---- the documents to have on disk
    docs: Dict[Path, Any] = {}                     # JSON documents
    texts: Dict[Path, Tuple[str, Optional[safe_io.TextFormat]]] = {}
    deletes: List[Path] = []
    pages_dir = definition / "pages"
    bookmarks_dir = definition / "bookmarks"
    if not (definition / "version.json").is_file():
        docs[definition / "version.json"] = pb.version_metadata()

    new_manifest: Json = {"version": MANIFEST_VERSION, "pages": {}, "bookmarks": [], "custom_visuals": {},
                          "public_visuals": [], "images": [], "stale_files": []}
    spec_pages = {p.name.lower(): p for p in compiled.pages}
    removed_pages: Set[str] = set()
    removed_visuals: Set[str] = set()              # "page/visual", lower case
    rewritten_visuals: Set[str] = set()            # same form: visuals this apply writes

    def drop_visual(folder: Path, visual_folder: str) -> None:
        # Everything in a visual's folder belongs to that visual (visual.json, a mobile layout).
        target = folder / "visuals" / visual_folder
        deletes.extend(sorted(f for f in target.iterdir() if f.is_file()))
        removed_visuals.add(f"{folder.name}/{visual_folder}".lower())

    for page in compiled.pages:
        folder = pages_dir / disk_pages.get(page.name.lower(), page.name)
        if page.doc is not None:
            docs[folder / "page.json"] = _carry_over(page.doc, folder / "page.json", _PAGE_OWNED)
        wanted = {el.name.lower() for el in page.elements}
        on_disk = {v.lower(): v for v in state.pages.get(folder.name, set())}
        for el in page.elements:
            if el.doc is not None:
                vfile = folder / "visuals" / on_disk.get(el.name.lower(), el.name) / "visual.json"
                docs[vfile] = _carry_over(el.doc, vfile, _VISUAL_OWNED)
                rewritten_visuals.add(f"{folder.name}/{el.name}".lower())
        kept = [el.name for el in page.elements]
        for name in (old_page.get(page.name.lower()) or {}).get("elements") or []:
            if name.lower() in wanted or name.lower() not in on_disk:
                continue
            if prune:
                drop_visual(folder, on_disk[name.lower()])
            else:
                kept.append(name)
        new_manifest["pages"][page.name] = {"owned": not page.existing,
                                            "elements": sorted(kept, key=str.lower)}

    for name, info in old_pages.items():
        key = str(name).lower()
        if key in spec_pages or key not in disk_pages:
            continue
        folder = pages_dir / disk_pages[key]
        on_disk = {v.lower(): v for v in state.pages.get(folder.name, set())}
        managed = [e for e in info.get("elements") or [] if e.lower() in on_disk]
        if not prune:
            new_manifest["pages"][name] = {"owned": bool(info.get("owned")),
                                           "elements": sorted(managed, key=str.lower)}
            continue
        for element in managed:
            drop_visual(folder, on_disk[element.lower()])
        if info.get("owned"):
            others = sorted(set(on_disk) - {e.lower() for e in managed})
            if others:
                theirs = ", ".join(on_disk[o] for o in others[:5])
                warnings.append(f"page {name!r} is no longer in the spec, but it holds visuals this tool did "
                                f"not create ({theirs}), so the page was kept")
                new_manifest["pages"][name] = {"owned": True, "elements": []}
            else:
                deletes.append(folder / "page.json")
                removed_pages.add(key)

    # pages.json
    meta = copy.deepcopy(state.pages_meta) if state.pages_meta is not None else \
        pbir_authoring.build_pages_metadata([], None, state.schemas.get("pagesMetadata"))
    order = [p for p in meta.get("pageOrder") or [] if str(p).lower() not in removed_pages]
    listed = _lower(order)
    for page in compiled.pages:
        if page.name.lower() not in listed:
            order.append(disk_pages.get(page.name.lower(), page.name))
            listed.add(page.name.lower())
    if compiled.page_order:
        by_key = {str(p).lower(): p for p in order}
        head = [by_key[n.lower()] for n in compiled.page_order if n.lower() in by_key]
        order = head + [p for p in order if p not in head]
    active = compiled.active_page or meta.get("activePageName") or ""
    active = next((p for p in order if str(p).lower() == str(active).lower()), order[0] if order else "")
    meta["pageOrder"] = order
    if order:
        meta["activePageName"] = active
    else:
        warnings.append("the report has no pages; Power BI Desktop needs at least one to open it")
    docs[pages_dir / "pages.json"] = meta

    # bookmarks
    wanted_bookmarks = {doc["name"].lower() for doc in compiled.bookmarks}
    on_disk_bm = {b.lower(): b for b in state.bookmarks}
    for doc in compiled.bookmarks:
        docs[bookmarks_dir / f"{on_disk_bm.get(doc['name'].lower(), doc['name'])}.bookmark.json"] = doc
    gone_bookmarks: Set[str] = set()
    kept_bookmarks = [doc["name"] for doc in compiled.bookmarks]
    for name in old.get("bookmarks") or []:
        if name.lower() in wanted_bookmarks or name.lower() not in on_disk_bm:
            continue
        if prune:
            deletes.append(bookmarks_dir / f"{on_disk_bm[name.lower()]}.bookmark.json")
            gone_bookmarks.add(name.lower())
        else:
            kept_bookmarks.append(name)
    new_manifest["bookmarks"] = sorted(kept_bookmarks, key=str.lower)
    if compiled.bookmarks or gone_bookmarks:
        bmeta = copy.deepcopy(state.bookmarks_meta) if state.bookmarks_meta is not None else \
            pb.bookmarks_metadata([], state.schemas.get("bookmarksMetadata"))
        items = _strip_bookmarks(list(bmeta.get("items") or []), gone_bookmarks)
        present = _listed_bookmarks(items)
        items += [{"name": doc["name"]} for doc in compiled.bookmarks if doc["name"].lower() not in present]
        bmeta["items"] = items
        docs[bookmarks_dir / "bookmarks.json"] = bmeta

    # report.json: theme, images, AppSource ids, custom visual packages
    report = copy.deepcopy(state.report) if state.report is not None else pb.new_report()
    # Files of ours that are no longer used (a theme file after a rename). They wait here until
    # a prune, like every other leftover.
    stale = [f for f in old.get("stale_files") or [] if root.joinpath(*f.split("/")).is_file()]
    if compiled.theme_file:
        current = (report.get("themeCollection") or {}).get("customTheme")
        old_file = str(old_theme.get("file", "")).lower()
        ours = isinstance(current, dict) and str(current.get("name", "")).lower() == old_file
        # The theme the report had before the spec set one, so a prune can put it back.
        previous = old_theme.get("previous") if ours else current
        if str(old_theme.get("file", compiled.theme_file)).lower() != compiled.theme_file.lower():
            report = pb.unregister_resource(report, old_theme["file"])
            stale.append(_rel(resources / old_theme["file"], root))
        report = pb.set_custom_theme(report, compiled.theme_file)
        docs[resources / compiled.theme_file] = compiled.theme_doc
        new_manifest["theme"] = {"file": compiled.theme_file, "previous": previous}
        stale = [f for f in stale if f.lower() != _rel(resources / compiled.theme_file, root).lower()]
    elif old_theme.get("file"):
        if prune:
            report = pb.unregister_resource(report, old_theme["file"])
            collection = report.setdefault("themeCollection", {})
            current = collection.get("customTheme")
            if isinstance(current, dict) and str(current.get("name",
                                                             "")).lower() == old_theme["file"].lower():
                if old_theme.get("previous"):
                    collection["customTheme"] = old_theme["previous"]
                else:
                    collection.pop("customTheme", None)
            stale.append(_rel(resources / old_theme["file"], root))
        else:
            new_manifest["theme"] = old_theme
    stale = sorted({f for f in stale if root.joinpath(*f.split("/")).is_file()})
    if prune:
        deletes.extend(root.joinpath(*f.split("/")) for f in stale)
    else:
        new_manifest["stale_files"] = stale

    registered = {str(i.get("name")) for pkg in report.get("resourcePackages") or []
                  if pkg.get("name") == "RegisteredResources" for i in pkg.get("items") or []}
    ours_images = list(old.get("images") or [])
    for image_name in compiled.images:
        if image_name not in registered:
            report = pb.register_resource(report, image_name, "Image")
            ours_images.append(image_name)
    for image_name in list(ours_images):
        if prune and image_name not in compiled.images:
            report = pb.unregister_resource(report, image_name)
            ours_images.remove(image_name)
    new_manifest["images"] = sorted(set(ours_images))

    # Visual types still on the pages after this apply: decides whether an AppSource id or a
    # custom visual package can be let go.
    in_use = {t for key, t in state.visual_types.items() if key not in removed_visuals
              and key not in rewritten_visuals and key.split("/")[0] not in removed_pages}
    in_use |= {el.doc["visual"]["visualType"] for p in compiled.pages for el in p.elements
               if el.doc and "visual" in el.doc}
    ours_public = list(old.get("public_visuals") or [])
    for visual_id in compiled.public_ids:
        if visual_id not in (report.get("publicCustomVisuals") or []):
            report = pb.register_public_visual(report, visual_id)
            ours_public.append(visual_id)
    for visual_id in list(ours_public):
        if prune and visual_id not in compiled.public_ids and visual_id not in in_use:
            report = pb.unregister_public_visual(report, visual_id)
            ours_public.remove(visual_id)
    new_manifest["public_visuals"] = sorted(set(ours_public))

    for guid in packages:
        report = pb.register_custom_visual(report, guid)
    for rel, (text, fmt) in custom_files.items():
        texts[root.joinpath(*rel.split("/"))] = (text, fmt)
    for guid, package in packages.items():
        new_manifest["custom_visuals"][guid] = sorted(
            pbir_packages.custom_visual_files(guid, package["files"]))
    for guid, files in old_custom.items():
        current_files = set(new_manifest["custom_visuals"].get(guid, []))
        leftover = [f for f in files if f not in current_files and root.joinpath(*f.split("/")).is_file()]
        if guid in packages:
            deletes.extend(root.joinpath(*f.split("/")) for f in leftover)   # dropped by the new version
        elif prune and guid not in in_use:
            deletes.extend(root.joinpath(*f.split("/")) for f in leftover)
            report = pb.unregister_custom_visual(report, guid)
        else:
            if prune:
                warnings.append(f"custom visual {guid} is still used by a visual in the report, "
                                "so it was kept")
            new_manifest["custom_visuals"][guid] = sorted(files)
    docs[definition / "report.json"] = report

    managed_anything = any(new_manifest[k] for k in ("pages", "bookmarks", "custom_visuals", "public_visuals",
                                                     "images", "stale_files")) or "theme" in new_manifest
    if managed_anything or manifest_file.is_file():
        docs[manifest_file] = new_manifest

    # ---- one transaction
    tx = safe_io.FileTransaction()
    planned: Dict[str, Path] = {_key(definition / "version.json"): definition / "version.json"}
    for path, doc in docs.items():
        planned[_key(path)] = path
        if not _same_json(path, doc):
            tx.write_json(path, doc)
    for path, (text, fmt) in texts.items():
        planned[_key(path)] = path
        tx.write(path, text, fmt)
    for path in deletes:
        tx.delete(path)
    changed = {_key(p) for p in tx.paths}
    delete_keys = {_key(p) for p in deletes}
    written = sorted((p for k, p in planned.items() if k in changed and k not in delete_keys),
                     key=lambda p: _rel(p, root))
    removed = sorted((p for p in deletes if _key(p) in changed), key=lambda p: _rel(p, root))
    result["written"] = [_rel(p, root) for p in written]
    result["removed"] = [_rel(p, root) for p in removed]
    result["unchanged"] = len(planned) - len(written)
    long_paths = pbir_packages.path_length_problems(str(root), result["written"], limit=260)
    if long_paths:
        warnings.append(f"{len(long_paths)} file path(s) reach 260 characters, which Windows may refuse "
                        f"(first: {long_paths[0]}); a shorter project folder or shorter names avoid it")
    if dry_run:
        tx.discard()
        result["success"] = True
        return result

    # Folders this apply will bring into being. If the commit fails the files are rolled back
    # by the transaction, and these are taken away again so no empty visual folder is left.
    new_dirs = {d for path in written for d in [path.parent, *path.parent.parents] if not d.exists()}
    try:
        tx.commit()
    except OSError as e:
        for folder in sorted(new_dirs, key=lambda p: len(p.parts), reverse=True):
            try:
                folder.rmdir()
            except OSError:
                pass
        errors.append(f"nothing was changed: writing failed ({e})")
        result["written"], result["removed"], result["unchanged"] = [], [], 0
        return result
    _remove_empty_dirs(removed, root)
    result["success"] = True
    return result


def _key(path: Any) -> str:
    return os.path.normcase(os.path.abspath(os.fspath(path)))


def _rel(path: Path, root: Path) -> str:
    return os.path.relpath(path, root).replace(os.sep, "/")
