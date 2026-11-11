"""
Report as code: describe a Power BI report in one JSON-serialisable spec and apply it to a
PBIR-Enhanced report folder (<Name>.Report with a definition/ folder).

    errors = validate_report_spec(spec)
    result = apply_report_spec(report_dir, spec, model_fields=None, prune=False, dry_run=False)

Applying is idempotent: every page, element and bookmark has a stable ``name``, so a second run
updates the same files in place and an unchanged spec rewrites nothing. Nothing is written
unless the whole spec validates, and all writes land together or not at all. Pages, visuals and
bookmarks the spec did not create are never rewritten or removed. The same holds inside a file
the spec writes: its filters, interactions and bookmark groups are replaced or removed with the
spec, and the ones someone added in Power BI Desktop stay.

THE SPEC

  {
    "theme":           {...},       optional custom theme (see below)
    "custom_visuals":  [...],       optional AppSource ids, .pbiviz packages and visuals to build
    "page_defaults":   {...},       optional width / height / background / wallpaper for every page
    "filters":         [...],       optional filters on the whole report (see filter)
    "pages":           [...],       the pages, each with its elements
    "shared":          [...],       optional elements repeated on several pages (masthead, navigation)
    "bookmarks":       [...],       optional show/hide bookmarks
    "bookmark_groups": [...],       optional groups of those bookmarks
    "page_order":      [...],       optional page names, first to last
    "active_page":     "name"       optional page the report opens on
  }

Names (pages, elements, bookmarks) use letters, digits, "_" and "-", 50 characters at most. They
are folder names, so they are compared without regard to case.

page
  name, display_name, width, height (default 1280 x 720), display_option ("FitToPage"),
  background ("#RRGGBB" | {color, transparency} | {image, scaling}), wallpaper ("#RRGGBB"),
  hidden (true keeps it out of the reader's page list), elements [...], popups [...],
  filters [...] (see filter), interactions [...] (see interaction), and drillthrough {...} or
  tooltip (see drillthrough and tooltip pages).
  "existing": true means the page is already in the report and only elements are added to it:
  its page.json is left alone, so page settings other than elements and popups have no effect.
  Without it, a page of that name must not exist yet (or must have been created by an earlier
  apply).

element (every type)
  name, type, x, y, width, height, and optionally z (default: declaration order), tab_order,
  hidden, and the frame: title, subtitle, background, border, shadow, padding, header, alt_text,
  tooltip, lock_aspect. Text, shapes, images, buttons, slicers and HTML get no frame unless you
  give one ("chrome": "theme" hands the frame back to the theme). "objects" adds formatting in
  compact form, {"legend": {"show": false, "fontSize": 10}}, and "container" does the same for
  frame objects not listed above. An element must fit inside its page ("allow_overflow": true
  lifts that). "mobile": {x, y, width, height, z} puts the element on the page's phone layout;
  an element without it is not on the phone layout. The phone canvas is 323 wide (Microsoft
  Learn: "323 pt is the maximum screen width on the mobile layout canvas") and as tall as its
  content, so x + width may not pass 323 and y is free.

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
           sync_group (a name, or true for one named after the field), filters
  html     measure "Table.Measure" (the HTML Content visual), filters, and edition: "regular"
           (the default: not certified; it runs scripts, which a JavaScript app in a measure
           needs) or "secure" (the certified edition; it sanitizes the HTML, so static HTML
           works in both). visual_id names an edition by its id instead.
  deneb    fields [...], spec {...}, config {...}, provider, version, tooltips, filters,
           tooltip_page, and Deneb's own switches: selection (a click cross-filters the other
           visuals; max_selected, 1 to 250, is how many selected rows Deneb passes on),
           cross_highlight (a selection elsewhere highlights this one; the spec then reads
           "<measure>__highlight"), context_menu (the right-click menu, where a reader finds
           drill through; on unless switched off). "objects": {"vega": {...}} adds settings to
           the ones these keys write ({"renderMode": "canvas"}); it cannot repeat one of them.
  visual   visual_type ("barChart", "tableEx", "card", ...) or custom (the alias of a visual in
           custom_visuals), fields {role: field | [fields]}, filters, tooltip_page,
           drill_filter_other_visuals (false: a drill in another visual does not filter this one)
  group    display_name, hidden, elements [...], and optionally x, y, width, height. Members
           are placed in page coordinates, like everything else in a spec, and must lie inside
           the group's box. With no box the group spans the page. (In the files a member's
           position counts from its group's corner, not the page's: Desktop draws a member
           written at (40, 40) in a group at (300, 200) at (340, 240). The spec does that sum.)

  action   {"type": "page" | "bookmark" | "back" | "url" | "clear_slicers" | "drillthrough",
            "target": ..., "tooltip": ...}
           A drillthrough action names a drillthrough page. Power BI enables the button once the
           reader has picked a data point that carries one of that page's fields; until then it
           is drawn in its "disabled" state and shows "disabled_tooltip".
  tooltip_page  the name of a tooltip page to show in place of the visual's default tooltip
           (the frame key "tooltip" only switches the tooltip on or off). It is stored as the
           frame's visualTooltip with "show" and "section" and no "type", as in visuals Desktop
           saved; with a type of 'ReportPage' Power BI was seen to show the default tooltip.

field
  "Table.Field", "'Table Name'.Field", or {"measure": "Table.Name"} / {"column": "Table.Name"},
  optionally with "aggregation": "sum" | "average" | "count" | "distinct_count" | "min" | "max" |
  "median", and with "active": true | false to say which level of a drill role is shown (left
  out, the first field of a chart's Category is, and the first of several in a role a custom
  visual's package lists under capabilities.drilldown.roles).
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

bookmark_groups
  [{"name", "display_name", "bookmarks": [names of bookmarks of this spec]}]
  The bookmarks pane shows the group in place of its bookmarks. A bookmark sits in one group
  at most; group names and bookmark names share one set of names.

filter (top-level "filters": the whole report; page "filters": one page; element "filters":
        one slicer, html, deneb or visual element)
  {"field": F, "in": [values]}                     is one of; "not_in": [values] is none of
  {"field": F, "op": OP, "value": V}               OP is = != > >= < <= contains not_contains
                                                   starts_with not_starts_with
      ... "and": {"op", "value"} | "or": {...}     a second condition on the same field
  {"field": F, "between": [low, high]}             both ends included, lowest first
  {"field": F, "blank": true | false}              is blank, is not blank
  {"field": F, "top": {"n": 5, "by": M, "direction": "top" | "bottom"}}
                                                   the N values of F that rank highest or lowest
                                                   by M (a measure, or a column, which is
                                                   aggregated as in a value well: summed when
                                                   it holds numbers, counted otherwise)
  {"field": F, "relative_date": {"last": 3, "unit": "months", "include_today": true}}
                                                   or "next": n, or "this": true; unit is days,
                                                   weeks, months or years
  {"field": F}                                     an empty filter card for the reader to fill
  Options: display_name, hidden (not shown to readers), locked (shown, not changeable),
  single_select (with "in" or an empty card), and name: a filter's name is unique across the
  report and is how a later apply finds it again; left out, it is derived from where the filter
  sits and the field it filters, so it is the same on every run.
  A measure, or a column with an "aggregation", can only filter an element, and only with op,
  between or blank; "top" is for an element as well. With model_fields the values are checked
  against the column: text for a text column, numbers for a number column, true/false, and
  "YYYY-MM-DD" (or "YYYY-MM-DDTHH:MM:SS") for a date column, which must be a date that exists.
  Without it a value's own type decides. {"literal": "datetime'2026-01-31T00:00:00'"} writes a
  literal out as it stands, provided it is one Power BI reads: 'text', 12L, 1.5D, 1.5M, true,
  false, null or datetime'...'. A value Desktop cannot read (NaN, a whole number beyond 64
  bits, 30 February) is refused here, because there it costs the visual or the whole report.

drillthrough (page key)
  {"fields": ["Table.Column", ...], "keep_all_filters": true, "cross_report": false,
   "back_button": true | {button settings}}
  Makes the page a drillthrough page: right-clicking a data point that carries one of the
  fields offers "Drill through" to it, and the page opens filtered to that value.
  keep_all_filters false stops the other filters on that data point from coming along;
  cross_report lets other reports drill through to the page. back_button adds a button named
  drill_back (top left unless you place it) that returns to the page the reader came from.
  A field someone adds to the page's drillthrough well in Desktop is kept by later applies.

tooltip (page key)
  true, or {"fields": ["Table.Column", ...]}: a tooltip page, hidden from the page list and
  320 x 240 unless the page gives its own width and height (page_defaults do not apply to it).
  An element shows it with "tooltip_page": "<page name>"; "fields" are the columns a visual must
  carry for Power BI to pick the page as its tooltip by itself.

changed for a spec written for 3.8
  Three things, on purpose. A tooltip page that gives no size is 320 x 240, where it used to
  take the size of the other pages: give it "width" and "height" to keep that (an apply that
  resizes an existing page says so, and an element that no longer fits says why). A deneb
  element is stamped with Vega-Lite 6.4.3, the version Deneb 2.0 writes, not 6.4.1. And input
  that 3.8 wrote out as given although Desktop cannot use it is now an error: an aggregation
  number that is no function, NaN or Infinity anywhere, a display_option the format does not
  have, a max_selected outside 1 to 250, "objects": {"vega": ...} that would replace a Deneb
  visual's specification, and a "tooltip" that is neither true nor an object. Everything else
  such a spec produces is byte for byte what 3.8 wrote.

interaction (page "interactions")
  {"from": "<element>", "to": "<element>" | [elements] | "*", "type": "filter" | "highlight" | "none"}
  What picking a data point in "from" does to "to". Both are slicer, html, deneb or visual
  elements: nothing else takes part in interactions. "*" is every other such element of the
  page. A slicer can be filtered but not highlighted, so "highlight" to a slicer is refused and
  "highlight" to "*" leaves the slicers out. A pair that is not listed keeps Power BI's
  default, and a later entry overrides an earlier one, so "none" to "*" followed by one
  exception reads as written.

theme
  {"name", "file" (default <name>.json), "colors": {data: [...], background, page, wallpaper,
   foreground, muted, subtle, border, accent, good, neutral, bad}, "fonts": {title | label |
   callout | header: {family, size, color}}, "card": {radius, border, shadow, padding, header},
   "extra": {...raw theme JSON...}}

custom_visuals
  [{"appsource": "<visual id>"}, {"pbiviz": "path/to/visual.pbiviz", "alias": "home"},
   {"definition": "path/to/definition.json", "alias": "bars"}]
  A .pbiviz is unpacked into CustomVisuals/<guid>/ and registered; a relative path is taken from
  the folder that holds the report folder. A definition is built into such a package first
  (see custom_visual.build_custom_visual for what a definition holds) and then treated the same
  way: a definition that does not build is an error of the spec, and an unchanged one builds
  to the same files, so a second apply writes nothing. An element uses either by its alias (by
  default the file's name, or the folder's name for a file called definition.json). HTML
  Content and Deneb are registered automatically when an element uses them.

EXAMPLE

  {
    "theme": {"name": "House", "colors": {"data": ["#1F6FEB", "#F0883E"], "background": "#FFFFFF",
              "page": "#F6F8FA", "foreground": "#1F2328", "border": "#D0D7DE"}, "card": {"radius": 8}},
    "page_defaults": {"width": 1280, "height": 720},
    "filters": [{"field": "Calendar.Year", "in": [2025, 2026], "display_name": "Years"}],
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
      {"name": "overview", "display_name": "Overview",
       "interactions": [{"from": "by_segment", "to": "by_month", "type": "filter"}],
       "elements": [
        {"name": "heading", "type": "textbox", "x": 24, "y": 72, "width": 600, "height": 40,
         "text": "Sales overview", "size": 20, "bold": true},
        {"name": "by_month", "type": "visual", "visual_type": "lineChart", "x": 24, "y": 128, "width": 800,
         "height": 560, "title": "Sales by month",
         "fields": {"Category": "Calendar.Month", "Y": "Sales.Total Sales"},
         "mobile": {"x": 0, "y": 0, "width": 323, "height": 220}},
        {"name": "by_segment", "type": "visual", "visual_type": "barChart", "x": 840, "y": 128, "width": 416,
         "height": 560, "title": "Top segments",
         "fields": {"Category": "Customer.Segment", "Y": "Sales.Total Sales"},
         "filters": [{"field": "Customer.Segment", "top": {"n": 5, "by": "Sales.Total Sales"}}]}]},
      {"name": "detail", "display_name": "Detail",
       "drillthrough": {"fields": ["Customer.Segment"], "back_button": {"x": 1016, "y": 16}},
       "filters": [{"field": "Sales.Amount", "op": ">", "value": 0, "hidden": true}],
       "elements": [
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
                it, so it is never uploaded with the report). The manifest also names the
                filters, interactions, page bindings, phone positions and bookmark groups an
                apply made. Those follow the spec without prune: taken out of the spec, they are
                taken out of the files on the next apply, and nothing else in those files is.
                A manifest that is damaged is an error, not a guess.
  dry_run       return what would be written and removed without touching the disk.
  adopt         take over pages, elements or bookmarks that already exist under a name the spec
                uses, instead of refusing. Use it when the manifest was lost.

  Returns {"success", "written": [...], "removed": [...], "unchanged": n, "warnings": [...],
  "errors": [...]}; paths are relative to the report folder.

  Two things an apply refuses rather than repair. A file it has to write that is there but
  cannot be read as JSON (a merge left half done): written over, whatever a person had in it
  would be gone, so mend it first. And a filter name the spec gives that a filter in another
  file of the report already has: a filter's name is unique across the report.
"""
import copy
import hashlib
import json
import math
import os
import re
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Set, Tuple

import custom_visual
import pbir_authoring
import pbir_builders as pb
import pbir_filters as pf
import pbir_packages
import safe_io

Json = Dict[str, Any]

MANIFEST_SUFFIX = ".spec-manifest.json"
MANIFEST_VERSION = 1

ELEMENT_TYPES = ("textbox", "shape", "image", "button", "slicer", "html", "deneb", "visual", "group")
ACTION_TYPES = ("page", "bookmark", "back", "url", "clear_slicers", "drillthrough")
_PLAIN_TYPES = {"textbox", "shape", "image", "button", "slicer", "html"}
_LINKABLE = {"button", "shape", "image"}
_DATA_TYPES = {"slicer", "html", "deneb", "visual"}      # elements that query the model
_CHROME_KEYS = ("title", "subtitle", "background", "border", "shadow", "padding", "header", "alt_text",
                "lock_aspect", "tooltip", "keep_layer_order")
_COMMON_KEYS = {"name", "type", "x", "y", "width", "height", "z", "position", "tab_order", "hidden", "chrome",
                "container", "objects", "allow_overflow", "action", "active", "mobile", *_CHROME_KEYS}
_TYPE_KEYS = {
    "textbox": {"text", "paragraphs", "font", "size", "color", "bold", "italic", "underline", "align"},
    "shape": {"fill", "transparency", "radius", "outline", "outline_weight"},
    "image": {"resource", "measure", "scaling", "transparency"},
    "button": {"text", "font", "size", "color", "bold", "fill", "fill_transparency", "outline",
               "outline_weight",
               "radius", "states"},
    "slicer": {"field", "mode", "single_select", "strict_single_select", "select_all", "header", "items",
               "sync_group", "filters"},
    "html": {"measure", "visual_id", "edition", "filters"},
    "deneb": {"fields", "spec", "config", "provider", "version", "selection", "tooltips", "max_selected",
              "cross_highlight", "context_menu", "filters", "tooltip_page"},
    "visual": {"visual_type", "custom", "fields", "filters", "tooltip_page", "drill_filter_other_visuals"},
    "group": {"display_name", "elements"},
}
_TOP_KEYS = {"theme", "custom_visuals", "page_defaults", "pages", "shared", "bookmarks", "bookmark_groups",
             "filters", "page_order", "active_page"}
_PAGE_KEYS = {"name", "display_name", "width", "height", "display_option", "background", "wallpaper",
              "hidden",
              "tooltip", "drillthrough", "filters", "interactions", "existing", "elements", "popups"}
_DRILLTHROUGH_KEYS = {"fields", "keep_all_filters", "cross_report", "back_button"}
# PageDisplayOption of the page schema. Another word here stops Desktop reading the page.
_DISPLAY_OPTIONS = ("FitToPage", "FitToWidth", "ActualSize", "ActualSizeTopLeft", "DeprecatedDynamic")
_TOOLTIP_SIZE = (320, 240)        # the size Desktop gives a page of type "Tooltip"
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
_MOBILE_OWNED = {"$schema", "position"}
# Keys of a page.json that are the spec's only while the spec declares them (a tooltip or a
# drillthrough page). The manifest remembers that an apply set them, so a later one can take
# them away again; a binding made in Desktop on a page the spec says nothing about is kept.
_BINDING_KEYS = ("pageBinding", "type")


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
    filters: Optional[List[Json]] = None      # visual-level filters; None when the spec declares none
    mobile: Optional[Json] = None             # the mobile.json document of an element on the phone layout
    origin: Tuple[float, float] = (0, 0)      # where its group sits: a member is written relative to it


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
    filters: Optional[List[Json]] = None      # page-level filters, drillthrough and tooltip fields included
    interactions: Optional[List[Json]] = None
    tooltip_sized: bool = False               # its size is the tooltip default, not one the spec gave


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
    report_filters: Optional[List[Json]] = None
    bookmark_groups: Optional[List[Json]] = None              # bookmarks.json group entries


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
        if isinstance(ref, Mapping) and ref.get("active") is not None:
            if not isinstance(ref["active"], bool):
                errors.append(f'{where}: "active" of {table}[{name}] must be true or false')
                return None
            out["active"] = ref["active"]
        if aggregation is not None:
            # The number lands in the file as the query's aggregate function. One that is not a
            # function stops Desktop drawing any page of the report, so only known ones pass.
            known = aggregation.lower() in _AGGREGATIONS if isinstance(aggregation, str) else \
                _is_num(aggregation) and aggregation in set(_AGGREGATIONS.values())
            if not known:
                errors.append(f"{where}: unknown aggregation {aggregation!r}; "
                              f"use one of {sorted(_AGGREGATIONS)}")
                return None
            if isinstance(aggregation, str):
                aggregation = _AGGREGATIONS[aggregation.lower()]
            if kind == "Measure":
                errors.append(f"{where}: {table}[{name}] is a measure and cannot take an aggregation")
                return None
            out["aggregation"] = int(aggregation)
        elif kind == "Column" and aggregating and meta and meta.get("dataType"):
            numeric = str(meta["dataType"]).lower() in _NUMERIC_TYPES
            out["aggregation"] = pbir_authoring.AGG_SUM if numeric else pbir_authoring.AGG_COUNT_NONNULL
        return out

    def data_type(self, resolved: Mapping[str, Any]) -> Optional[str]:
        """The model's data type of a resolved field, when the model gives one."""
        found = self.tables.get(str(resolved["table"]).lower())
        hit = found[1].get(str(resolved["field"]).lower()) if found else None
        meta = hit[1] if hit else None
        return str(meta["dataType"]) if meta and meta.get("dataType") else None


# ---------------------------------------------------------------------------------------------
# small helpers
# ---------------------------------------------------------------------------------------------
def _is_num(v: Any) -> bool:
    """A real, finite number: NaN and Infinity parse as JSON numbers but no report file can hold them."""
    return isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v)


def _declared(value: Any) -> bool:
    """True for a page setting that is switched on: true, or an object of options (even an empty one)."""
    return value is True or isinstance(value, Mapping)


def _digest(*parts: Any) -> str:
    """A 20-character name, the length Desktop gives its own filters and bindings, that is the
    same on every apply: names made here must not move between two runs of one spec."""
    return hashlib.sha1("|".join(str(p).lower() for p in parts).encode("utf-8")).hexdigest()[:20]


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


def _not_numbers(node: Any, path: str) -> List[str]:
    """Where a spec holds NaN or Infinity. Python reads both from JSON text and writes both
    back, but they are not JSON: a report file with one in it is a file Desktop cannot open."""
    if isinstance(node, float) and not math.isfinite(node):
        return [f"{path} is {node!r}: a report file cannot hold NaN or Infinity"]
    if isinstance(node, Mapping):
        return [e for key, value in node.items() for e in _not_numbers(value, f"{path}.{key}")]
    if isinstance(node, list):
        return [e for i, value in enumerate(node) for e in _not_numbers(value, f"{path}[{i}]")]
    return []


def _top_level_errors(spec: Any) -> List[str]:
    """Shape problems that make the rest of the spec unreadable."""
    if not isinstance(spec, Mapping):
        return ["the spec must be an object"]
    errors = [f"unknown key {k!r} at the top of the spec; known keys: {sorted(_TOP_KEYS)}"
              for k in spec if k not in _TOP_KEYS]
    errors += [e for key, value in spec.items() for e in _not_numbers(value, str(key))]
    for key in ("pages", "shared", "bookmarks", "bookmark_groups", "filters", "custom_visuals", "page_order"):
        if key in spec and not isinstance(spec[key], list):
            errors.append(f"{key} must be a list")
    for key in ("theme", "page_defaults"):
        if key in spec and not isinstance(spec[key], Mapping):
            errors.append(f"{key} must be an object")
    for key in ("pages", "shared", "bookmarks", "bookmark_groups", "filters", "custom_visuals"):
        for i, item in enumerate(spec.get(key) or [] if isinstance(spec.get(key), list) else []):
            if not isinstance(item, Mapping):
                errors.append(f"{key}[{i}] must be an object")
    return errors


def _custom_aliases(spec: Mapping[str, Any],
                    errors: List[str]) -> Tuple[Dict[str, str], List[str], Set[str]]:
    """({alias: path}, [AppSource ids], {aliases whose path is a definition to build}) from
    custom_visuals. The other paths are .pbiviz packages."""
    aliases: Dict[str, str] = {}
    public: List[str] = []
    built: Set[str] = set()
    for i, item in enumerate(spec.get("custom_visuals") or []):
        where = f"custom_visuals[{i}]"
        if isinstance(item.get("appsource"), str) and item["appsource"]:
            public.append(item["appsource"])
            continue
        source = next((k for k in ("pbiviz", "definition") if isinstance(item.get(k), str) and item[k]), None)
        if not source:
            errors.append(f'{where}: give either "appsource": "<visual id>", "pbiviz": "<path>" or '
                          '"definition": "<path>"')
            continue
        path = item[source]
        stem = os.path.splitext(os.path.basename(path))[0]
        if source == "definition" and stem.lower() == "definition":
            stem = os.path.basename(os.path.dirname(path.replace("\\", "/"))) or stem
        alias = str(item.get("alias") or stem)
        if alias in aliases:
            errors.append(f"{where}: the alias {alias!r} is used twice")
        aliases[alias] = path
        if source == "definition":
            built.add(alias)
    return aliases, public, built


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

    def add(self, spec: Any, where: str, z: int, parent: Optional[_Element] = None,
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
        el = _Element(self.page.name, name, kind, dict(spec), where,
                      parent=parent.name if parent else None,
                      hidden=bool(spec.get("hidden", hidden_default)))
        geo = _geometry(spec)
        el.z = int(geo["z"]) if _is_num(geo.get("z")) else z
        if kind == "group" and not any(k in geo for k in ("x", "y", "width", "height")):
            # With no box of its own a group spans the page, so the position Desktop reads for
            # a member (relative to its group) is the member's place on the page.
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
            place = f"{where}: at ({el.x}, {el.y}) size {el.width} x {el.height} it does not fit"
            outside = (el.x < 0 or el.y < 0 or el.x + el.width > self.page.width + 0.5
                       or el.y + el.height > self.page.height + 0.5)
            if outside and not spec.get("allow_overflow"):
                if self.page.tooltip_sized:
                    self.errors.append(f"{place} the page, which is {self.page.width} x {self.page.height} "
                                       "because it is a tooltip page and gives no size; give the page its "
                                       'own "width" and "height" for a larger tooltip')
                else:
                    self.errors.append(f"{place} the {self.page.width} x {self.page.height} page "
                                       '(set "allow_overflow": true if that is intended)')
                return None
            if parent and not spec.get("allow_overflow") and (
                    el.x < parent.x - 0.5 or el.y < parent.y - 0.5
                    or el.x + el.width > parent.x + parent.width + 0.5
                    or el.y + el.height > parent.y + parent.height + 0.5):
                self.errors.append(f"{place} its group {parent.name!r}, which is at ({parent.x}, {parent.y}) "
                                   f"size {parent.width} x {parent.height}; members are placed in page "
                                   "coordinates")
                return None
        if parent:
            el.origin = (parent.x, parent.y)
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
                self.add(member, f"{where}, member {j}", group.z + 1 + j, parent=group)
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
        tooltip_sized = _declared(p.get("tooltip")) and not ("width" in p and "height" in p)
        if _declared(p.get("tooltip")):
            # A tooltip page is a small card beside the pointer: it takes its own size, never
            # the size the other pages share.
            merged["width"] = p.get("width", _TOOLTIP_SIZE[0])
            merged["height"] = p.get("height", _TOOLTIP_SIZE[1])
        width, height = merged.get("width", 1280), merged.get("height", 720)
        if not _is_num(width) or not _is_num(height) or width <= 0 or height <= 0:
            errors.append(f"page {p['name']!r}: width and height must be numbers greater than 0")
            continue
        for key in ("elements", "popups"):
            if key in p and not isinstance(p[key], list):
                errors.append(f"page {p['name']!r}: {key} must be a list")
                merged[key] = []
        pages.append(_Page(p["name"], str(merged.get("display_name") or p["name"]), width, height,
                           bool(p.get("existing")), merged, tooltip_sized=tooltip_sized))
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
        drill = page.spec.get("drillthrough")
        back = drill.get("back_button") if isinstance(drill, Mapping) and not page.existing else None
        if back:
            # The way back to the page the reader drilled from, layered with the shared chrome.
            if back is not True and not isinstance(back, Mapping):
                errors.append(f"page {page.name!r}: drillthrough back_button must be true or an object of "
                              "button settings")
            else:
                own = dict(back) if isinstance(back, Mapping) else {}
                ex.add({"name": "drill_back", "x": 16, "y": 16, "width": 96, "height": 32, "text": "Back",
                        "outline": "#8A8886", "radius": 4, **own, "type": "button",
                        "action": {"type": "back"}},
                       f"page {page.name!r}, drillthrough back_button", z)
                z += _Z_STEP
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
                ex.add(member, where, base + j, parent=group)
            for j, member in enumerate(parts["own"]):
                if isinstance(member, Mapping) and member.get("type") == "group":
                    errors.append(f"{where}: a pop-up cannot hold a group")
                    continue
                ex.add(member, f"{where}, elements[{j}]", base + 10 + j, parent=group)
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
    spelling: Dict[str, str] = field(default_factory=dict)       # page name, lower case -> as written
    own_pages: Set[str] = field(default_factory=set)             # pages whose page.json the spec writes
    tooltip_pages: Set[str] = field(default_factory=set)
    drill_pages: Set[str] = field(default_factory=set)
    filter_names: Dict[str, str] = field(default_factory=dict)   # filter name, lower case -> where it is
    # What the pages already in the report are used for (lower-case name -> "Drillthrough",
    # "Tooltip" or None). None for the whole mapping when no report folder was read.
    page_kinds: Optional[Mapping[str, Optional[str]]] = None
    drill_roles: Mapping[str, Set[str]] = field(default_factory=dict)   # custom visual alias -> drill roles


def _page_of_kind(target: Any, kinds: Set[str], what: str, setting: str, where: str, ctx: _Context,
                  errors: List[str]) -> Optional[str]:
    """The name of the tooltip or drillthrough page an element points at, spelled as the page
    spells it. A page the spec builds is checked against the spec, a page the report already
    has against its own page.json; only when no report was read is the name taken on trust."""
    key = str(target).lower()
    if not isinstance(target, str) or key not in ctx.page_names:
        errors.append(f"{where}: {target!r} is not a page")
        return None
    if key in ctx.own_pages:
        if key not in kinds:
            errors.append(f"{where}: page {target!r} is not a {what} page; give it {setting}")
            return None
    elif ctx.page_kinds is not None and (ctx.page_kinds.get(key) or "").lower() != what:
        errors.append(f"{where}: page {target!r} is in the report but is not a {what} page there; make it "
                      f"one in Power BI Desktop, or let the spec build the page with {setting}")
        return None
    return ctx.spelling.get(key, target)


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
    if kind in ("page", "bookmark", "url", "drillthrough") and (not isinstance(target, str) or not target):
        errors.append(f'{el.where}: a {kind} action needs "target"')
        return None
    if kind == "drillthrough":
        target = _page_of_kind(target, ctx.drill_pages, "drillthrough", '"drillthrough": {"fields": [...]}',
                               f"{el.where}, drillthrough action", ctx, errors)
        if target is None:
            return None
        return pb.action_link(kind, target, tooltip=action.get("tooltip"),
                              disabled_tooltip=action.get("disabled_tooltip"))
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


def _container(el: _Element, link: Optional[List[Json]], tooltip_page: Optional[str] = None) -> Json:
    spec = el.spec
    plain = spec.get("chrome") == "plain" or (el.type in _PLAIN_TYPES and spec.get("chrome") != "theme")
    kw: Json = dict(pb.PLAIN_CHROME) if plain else {}
    for key in _CHROME_KEYS:
        if key in spec:
            kw[key] = spec[key]
    return pb.container_objects(**kw, tooltip_page=tooltip_page, link=link,
                                extra=pb.simple_objects(spec.get("container")))


# Deneb keeps its specification, its config and every switch in one formatting object, "vega".
# These of its properties are written from the element's own keys.
_DENEB_KEYS = {"jsonSpec": "spec", "jsonConfig": "config", "provider": "provider", "version": "version",
               "enableTooltips": "tooltips", "enableContextMenu": "context_menu",
               "enableHighlight": "cross_highlight", "enableSelection": "selection",
               "selectionMaxDataPoints": "max_selected"}


def _add_deneb_settings(el: _Element, properties: Json, entries: Any, errors: List[str]) -> None:
    """Add the caller's objects.vega to the vega object the element built, property by property.
    Replacing the object, as "objects" does for every other one, would drop the specification
    and leave an empty visual; and a property an element key already writes is refused, so one
    setting cannot be said twice in two ways."""
    if not isinstance(el.spec["objects"].get("vega"), Mapping):
        errors.append(f'{el.where}: objects.vega must be an object of settings, {{"renderMode": "canvas"}}: '
                      "the element writes the rest of it")
        return
    for name, value in entries[0]["properties"].items():
        if name in properties and name in _DENEB_KEYS:
            errors.append(f"{el.where}: objects.vega.{name} is what the element's own "
                          f'"{_DENEB_KEYS[name]}" sets; give it there')
        else:
            properties[name] = value


def _switch(el: _Element, key: str, errors: List[str]) -> Optional[bool]:
    """An on/off setting of an element: its value, or None when the spec leaves it out."""
    if key not in el.spec:
        return None
    if not isinstance(el.spec[key], bool):
        errors.append(f"{el.where}: {key} must be true or false")
        return None
    return el.spec[key]


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
    # Desktop reads the position of a group's member from the group's corner, not the page's.
    pos = pb.position(el.x - el.origin[0], el.y - el.origin[1], el.width, el.height, el.z, tab)
    schema = ctx.schemas.get("visual")
    if el.type == "group":
        return pb.visual_group(el.name, pos, str(spec.get("display_name") or el.name), hidden=el.hidden,
                               schema_url=schema)
    link = _action_link(el, ctx, errors)
    tooltip_page = None
    if spec.get("tooltip_page") is not None and el.type in ("visual", "deneb"):
        tooltip_page = _page_of_kind(spec["tooltip_page"], ctx.tooltip_pages, "tooltip", '"tooltip": true',
                                     f"{el.where}, tooltip_page", ctx, errors)
        if tooltip_page and tooltip_page.lower() == page.name.lower():
            errors.append(f"{el.where}, tooltip_page: a tooltip page cannot be the tooltip of its own "
                          "visuals")
            tooltip_page = None
    frame = _container(el, link, tooltip_page)
    common = {"container": frame, "schema_url": schema, "parent_group": el.parent, "hidden": el.hidden}
    objects = pb.simple_objects(spec.get("objects")) if isinstance(spec.get("objects"), Mapping) else None
    if "objects" in spec and objects is None:
        errors.append(f"{el.where}: objects must be an object of formatting objects")

    def merged(doc: Json) -> Json:
        if not objects:
            return doc
        own = doc["visual"].setdefault("objects", {})
        for name, entries in objects.items():
            if el.type == "deneb" and name == "vega":
                _add_deneb_settings(el, own["vega"][0]["properties"], entries, errors)
            else:
                own[name] = entries
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
        edition = spec.get("edition")
        if edition is not None and edition not in pb.HTML_CONTENT_EDITIONS:
            errors.append(f'{el.where}: edition must be "regular" or "secure", not {edition!r}')
            return None
        if edition and spec.get("visual_id"):
            errors.append(f"{el.where}: give edition or visual_id, not both")
            return None
        visual_id = str(spec.get("visual_id") or pb.HTML_CONTENT_EDITIONS.get(edition, pb.HTML_CONTENT_ID))
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
                                      max_selected=spec.get("max_selected", 50),
                                      cross_highlight=_switch(el, "cross_highlight", errors),
                                      context_menu=_switch(el, "context_menu", errors), **common))
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
    drill_filter = _switch(el, "drill_filter_other_visuals", errors)
    if len(errors) > before:
        return None
    return merged(pb.data_visual(el.name, visual_type, pos, resolved_roles,
                                 mark_active=not (table or custom),
                                 drill_roles=ctx.drill_roles.get(custom, ()) if custom else (),
                                 drill_filter_other_visuals=drill_filter is not False, **common))


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
    unbuilt = False          # it names an element that failed to build, which has its own error
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
            else:
                unbuilt = True
    display = str(bm.get("display_name") or bm["name"])
    if bm.get("targeted", True):
        if not groups and not visuals:
            if not unbuilt:
                out.errors.append(f'{where}: a bookmark needs "show" or "hide" '
                                  '(or "targeted": false to only open a page)')
            return None
        return pb.visibility_bookmark(bm["name"], display, page.name, groups=groups, visuals=visuals,
                                      schema_url=schema)
    return pb.bookmark(bm["name"], display, page.name, groups=groups, visuals=visuals, schema_url=schema)


# ---------------------------------------------------------------------------------------------
# filters, drillthrough and tooltip pages, interactions, the phone layout, bookmark groups
# ---------------------------------------------------------------------------------------------
def _claim_filter_name(name: str, where: str, ctx: _Context, errors: List[str]) -> bool:
    """Filter names are unique across a whole report, so every level shares one register."""
    taken = ctx.filter_names.get(name.lower())
    if taken:
        errors.append(f"{where}: the filter name {name!r} is already used by {taken}; a filter name is "
                      "unique across the report")
        return False
    ctx.filter_names[name.lower()] = where
    return True


def _shows(doc: Optional[Json], expr: Json) -> bool:
    """True when a visual already has this field in one of its wells."""
    state = ((doc or {}).get("visual") or {}).get("query", {}).get("queryState", {})
    return any(p.get("field") == expr for role in state.values() for p in role.get("projections", []))


def _filter_list(specs: Any, where: str, scope: str, ctx: _Context, errors: List[str],
                 visual: Optional[Json] = None) -> Optional[List[Json]]:
    """The filters declared at one level, built. ``scope`` keeps generated names apart between
    levels. ``visual`` is the visual.json the filters are for: there a measure can be filtered
    and a top N asked for, while a report or a page can only filter columns."""
    if not isinstance(specs, list):
        errors.append(f"{where} must be a list")
        return None
    on_visual = visual is not None
    out: List[Json] = []
    seen: Dict[str, int] = {}
    for i, item in enumerate(specs):
        at = f"{where}[{i}]"
        if not isinstance(item, Mapping):
            errors.append(f"{at} must be an object")
            continue
        if "field" not in item:
            errors.append(f'{at}: a filter needs "field"')
            continue
        guesses = len(ctx.fields.guessed)
        target = ctx.fields.resolve(item["field"], at, errors)
        if not target:
            continue
        label = f"{at} on {target['table']}[{target['field']}]"
        calculated = target["kind"] == "Measure" or target.get("aggregation") is not None
        if not on_visual:
            del ctx.fields.guessed[guesses:]         # only a column can sit here, so nothing was guessed
            if calculated or "top" in item:
                what = "a top N filter" if "top" in item and not calculated else \
                    "a measure or an aggregated column"
                errors.append(f"{label}: {what} can only filter one visual; put this filter on an element")
                continue
        order_by = None
        top = item.get("top")
        if isinstance(top, Mapping) and "by" in top:
            order_by = ctx.fields.resolve(top["by"], f"{label}, top by", errors, role="Y")
            if not order_by:
                continue
            if order_by["kind"] != "Measure" and order_by.get("aggregation") is None:
                errors.append(f"{label}: top ranks by a measure or an aggregated column; "
                              f"{order_by['table']}[{order_by['field']}] is a plain column, so give it an "
                              '"aggregation"')
                continue
        nth = seen.get(f"{target['table']}|{target['field']}".lower(), 0)
        seen[f"{target['table']}|{target['field']}".lower()] = nth + 1
        name = item.get("name")
        if name is None:
            name = _digest("filter", scope, target["table"], target["field"], nth)
        elif not isinstance(name, str) or not _NAME.match(name):
            errors.append(f"{label}: {name!r} is not a usable filter name (letters, digits, '_' and '-' "
                          "only, 50 characters at most)")
            continue
        if not _claim_filter_name(name, at, ctx, errors):
            continue
        shown = on_visual and _shows(visual, pf.field_expr(target))
        try:
            out.append(pf.filter_container(name, target, item, data_type=ctx.fields.data_type(target),
                                           order_by=order_by, how_created="Auto" if shown else "User"))
        except ValueError as e:
            errors.append(f"{label}: {e}")
    return out


def _binding(page: _Page, ctx: _Context,
             errors: List[str]) -> Tuple[Optional[str], Optional[Json], List[Json]]:
    """(kind, pageBinding, the filters it is bound to) for a drillthrough or a tooltip page;
    (None, None, []) for an ordinary one. A tooltip page with no fields has no binding at all:
    that is how Desktop saves one (page type and visibility only)."""
    where = f"page {page.name!r}"
    tooltip, drill = page.spec.get("tooltip"), page.spec.get("drillthrough")
    if tooltip in (None, False) and drill is None:
        return None, None, []
    if tooltip not in (None, False) and drill is not None:
        errors.append(f"{where}: a page is a tooltip page or a drillthrough page, not both")
        return None, None, []
    options: Mapping[str, Any] = {}
    if drill is not None:
        kind, known = "Drillthrough", _DRILLTHROUGH_KEYS
        if not isinstance(drill, Mapping) or not isinstance(drill.get("fields"), list) or not drill["fields"]:
            errors.append(f'{where}: drillthrough needs "fields", the columns a data point must carry for '
                          'Power BI to offer this page: {"fields": ["Table.Column"]}')
            return None, None, []
        options = drill
    else:
        kind, known = "Tooltip", {"fields"}
        if tooltip is not True and not isinstance(tooltip, Mapping):
            errors.append(f'{where}: tooltip must be true or {{"fields": [...]}}')
            return None, None, []
        options = tooltip if isinstance(tooltip, Mapping) else {}
        if not isinstance(options.get("fields", []), list):
            errors.append(f"{where}: tooltip fields must be a list of columns")
            return None, None, []
    for key in options:
        if key not in known:
            errors.append(f"{where}: unknown {kind.lower()} key {key!r}; known keys: {sorted(known)}")
    for key in ("keep_all_filters", "cross_report"):
        if key in options and not isinstance(options[key], bool):
            errors.append(f"{where}: drillthrough {key} must be true or false")
    filters: List[Json] = []
    bound: List[Json] = []
    seen: Set[str] = set()
    for i, ref in enumerate(options.get("fields") or []):
        at = f"{where}, {kind.lower()} fields[{i}]"
        column = ctx.fields.resolve(ref, at, errors, want="Column")
        if not column:
            continue
        key = f"{column['table']}|{column['field']}".lower()
        if column.get("aggregation") is not None or key in seen:
            reason = "is listed twice" if key in seen else "must be a plain column, without an aggregation"
            errors.append(f"{at}: {column['table']}[{column['field']}] {reason}")
            continue
        seen.add(key)
        name = _digest("binding", page.name, column["table"], column["field"])
        if not _claim_filter_name(name, at, ctx, errors):
            continue
        filters.append(pf.binding_filter(name, column))
        bound.append({"filter": name, "expr": pf.field_expr(column),
                      "name": _digest("binding parameter", page.name, column["table"], column["field"])})
    if kind == "Tooltip" and not bound:
        return kind, None, []
    binding = pb.page_binding(_digest("page binding", page.name), kind, bound,
                              cross_report=options.get("cross_report") is True,
                              keep_all_filters=options.get("keep_all_filters") is not False)
    return kind, binding, filters


def _interactions(page: _Page, errors: List[str]) -> Optional[List[Json]]:
    """The page's visualInteractions from {"from", "to", "type"} entries. A later entry
    overrides an earlier one for the same pair, so "none to everything, except ..." reads naturally.
    Only elements that query the model take part: Power BI has no interaction to or from a
    textbox, a shape, an image or a button."""
    specs = page.spec.get("interactions")
    if specs is None:
        return None
    where = f"page {page.name!r}, interactions"
    if not isinstance(specs, list):
        errors.append(f"{where} must be a list")
        return None
    by_name = {el.name.lower(): el for el in page.elements}

    def element(name: Any, at: str, end: str) -> Optional[_Element]:
        el = by_name.get(str(name).lower()) if isinstance(name, str) else None
        if not el:
            errors.append(f'{at}: "{end}" names {name!r}, which is not an element of this page')
        elif el.type == "group":
            errors.append(f'{at}: "{end}" names {el.name!r}, which is a group; name the visuals inside it')
            return None
        elif el.type not in _DATA_TYPES:
            errors.append(f'{at}: "{end}" names {el.name!r}, which is a {el.type}; only visuals and slicers '
                          "take part in interactions")
            return None
        return el

    pairs: Dict[Tuple[str, str], str] = {}
    for i, item in enumerate(specs):
        at = f"{where}[{i}]"
        if not isinstance(item, Mapping):
            errors.append(f"{at} must be an object")
            continue
        unknown = sorted(set(item) - {"from", "to", "type"})
        if unknown:
            errors.append(f"{at}: unknown key(s) {unknown}; known keys: from, to, type")
        kind = item.get("type")
        if kind not in pb.INTERACTION_TYPES:
            errors.append(f'{at}: "type" must be one of {list(pb.INTERACTION_TYPES)}, not {kind!r}')
            continue
        source = element(item.get("from"), at, "from")
        if not source:
            continue
        to = item.get("to")
        if to == "*":
            # A slicer shows values to pick from, so it can be filtered but not highlighted:
            # "highlight" to everything means everything that can be.
            targets = [el for el in page.elements if el is not source and el.type in _DATA_TYPES
                       and not (kind == "highlight" and el.type == "slicer")]
        else:
            names = to if isinstance(to, list) else [to]
            if not names:
                errors.append(f'{at}: "to" names no element')
            targets = []
            for name in names:
                target = element(name, at, "to")
                if target is source:
                    errors.append(f"{at}: {source.name!r} cannot interact with itself")
                elif target and kind == "highlight" and target.type == "slicer":
                    errors.append(f'{at}: {target.name!r} is a slicer, which can be filtered but not '
                                  'highlighted; use "filter" or "none"')
                elif target:
                    targets.append(target)
        for target in targets:
            pairs.pop((source.name, target.name), None)
            pairs[(source.name, target.name)] = kind
    return [pb.visual_interaction(source, target, kind) for (source, target), kind in pairs.items()]


def _mobile(el: _Element, ctx: _Context, errors: List[str]) -> Optional[Json]:
    """The mobile.json of an element that has a place on the phone layout."""
    box = el.spec.get("mobile")
    if box is None:
        return None
    if el.type == "group":
        errors.append(f'{el.where}: a group has no place on the phone layout; give "mobile" to its members')
        return None
    if not isinstance(box, Mapping):
        errors.append(f"{el.where}: mobile must be an object with x, y, width and height")
        return None
    unknown = sorted(set(box) - {"x", "y", "width", "height", "z"})
    if unknown:
        errors.append(f"{el.where}: unknown mobile key(s) {unknown}; known keys: x, y, width, height, z")
        return None
    missing = [k for k in ("x", "y", "width", "height") if not _is_num(box.get(k))]
    if missing:
        errors.append(f"{el.where}: mobile needs numeric {', '.join(missing)}")
        return None
    if box["x"] < 0 or box["y"] < 0 or box["width"] <= 0 or box["height"] <= 0:
        errors.append(f"{el.where}: mobile x and y cannot be negative, and width and height must be "
                      "greater than 0")
        return None
    if box["x"] + box["width"] > pb.PHONE_WIDTH + 0.5:
        errors.append(f"{el.where}: mobile x {box['x']} and width {box['width']} reach past the phone "
                      f"canvas, which is {pb.PHONE_WIDTH} wide (it has no fixed height: the page scrolls)")
        return None
    if "z" in box and not _is_num(box["z"]):
        errors.append(f"{el.where}: mobile z must be a number")
        return None
    z = int(box["z"]) if "z" in box else el.z
    return pb.mobile_state(pb.position(box["x"], box["y"], box["width"], box["height"], z),
                           ctx.schemas.get("mobile"))


def _bookmark_groups(spec: Mapping[str, Any], bookmarks: Mapping[str, str], taken: Set[str],
                     errors: List[str]) -> Optional[List[Json]]:
    """bookmark_groups as bookmarks.json entries. ``bookmarks`` maps the spec's bookmark names,
    lower case, to their spelling; ``taken`` holds every bookmark name there is, the report's too."""
    if "bookmark_groups" not in spec:
        return None
    out: List[Json] = []
    seen: Set[str] = set()
    placed: Dict[str, str] = {}
    for i, group in enumerate(spec["bookmark_groups"]):
        where = f"bookmark_groups[{i}]"
        err = _name_error(group.get("name"), where)
        if err:
            errors.append(err)
            continue
        name = group["name"]
        where = f"bookmark group {name!r}"
        for key in group:
            if key not in ("name", "display_name", "bookmarks"):
                errors.append(f"{where}: unknown key {key!r}; known keys: bookmarks, display_name, name")
        if name.lower() in seen:
            errors.append(f"{where}: the name is used twice")
            continue
        seen.add(name.lower())
        if name.lower() in taken:
            errors.append(f"{where}: the name is also the name of a bookmark; groups and bookmarks share "
                          "one set of names")
            continue
        children = group.get("bookmarks")
        if not isinstance(children, list) or not children:
            errors.append(f'{where}: needs "bookmarks", the names of the bookmarks it holds')
            continue
        members: List[str] = []
        for child in children:
            key = str(child).lower()
            if key not in bookmarks:
                errors.append(f"{where}: {child!r} is not a bookmark of this spec")
            elif key in placed:
                errors.append(f"{where}: bookmark {child!r} is already in group {placed[key]!r}")
            else:
                placed[key] = name
                members.append(bookmarks[key])
        out.append(pb.bookmark_group(name, str(group.get("display_name") or name), members))
    return out


def _compile(spec: Any, *, model_fields: Optional[Mapping[str, Any]] = None,
             schemas: Optional[Mapping[str, str]] = None, guids: Optional[Mapping[str, str]] = None,
             existing_pages: Optional[Sequence[str]] = None,
             existing_bookmarks: Optional[Sequence[str]] = None,
             page_kinds: Optional[Mapping[str, Optional[str]]] = None,
             drill_roles: Optional[Mapping[str, Set[str]]] = None) -> _Compiled:
    """Validate the spec and build every document it describes. Reads nothing from disk:
    what the caller read there (schemas, package guids and drill roles, the report's own pages
    and what they are used for) comes in as arguments."""
    out = _Compiled()
    out.errors.extend(_top_level_errors(spec))
    if out.errors:
        return out
    schemas = schemas or pb.DEFAULT_SCHEMAS
    aliases, public, _ = _custom_aliases(spec, out.errors)
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
    ctx.spelling = {str(p).lower(): str(p) for p in existing_pages or []}
    ctx.spelling.update({p.name.lower(): p.name for p in out.pages if not p.existing})
    ctx.own_pages = {p.name.lower() for p in out.pages if not p.existing}
    ctx.tooltip_pages = {p.name.lower() for p in out.pages if _declared(p.spec.get("tooltip"))}
    ctx.drill_pages = {p.name.lower() for p in out.pages if isinstance(p.spec.get("drillthrough"), Mapping)}
    ctx.page_kinds = page_kinds
    ctx.drill_roles = drill_roles or {}

    if "filters" in spec:
        out.report_filters = _filter_list(spec["filters"], "filters", "report", ctx, out.errors)
    for page in out.pages:
        if not page.existing:
            try:
                background = page.spec.get("background")
                if isinstance(background, Mapping) and background.get("image"):
                    out.images.append(str(background["image"]))
                kind, binding, bound = _binding(page, ctx, out.errors)
                display_option = page.spec.get("display_option", "FitToPage")
                if display_option not in _DISPLAY_OPTIONS:
                    out.errors.append(f"page {page.name!r}: display_option must be one of "
                                      f"{list(_DISPLAY_OPTIONS)}, not {display_option!r}")
                page.doc = pb.page(page.name, page.display_name, page.width, page.height,
                                   display_option=display_option,
                                   background=background, wallpaper=page.spec.get("wallpaper"),
                                   hidden=bool(page.spec.get("hidden")),
                                   tooltip=kind == "Tooltip", drillthrough=kind == "Drillthrough",
                                   binding=binding, schema_url=schemas.get("page"))
                if bound or "filters" in page.spec:
                    own = _filter_list(page.spec.get("filters", []), f"page {page.name!r}, filters",
                                       f"page/{page.name}", ctx, out.errors)
                    page.filters = bound + (own or [])
                page.interactions = _interactions(page, out.errors)
            except (KeyError, TypeError, ValueError, AttributeError) as e:
                out.errors.append(f"page {page.name!r}: {e}")
        for el in page.elements:
            try:
                el.doc = _build_element(el, page, ctx, out)
                if "filters" in el.spec and el.type in _DATA_TYPES and el.doc is not None:
                    el.filters = _filter_list(el.spec["filters"], f"{el.where}, filters",
                                              f"visual/{page.name}/{el.name}", ctx, out.errors,
                                              visual=el.doc)
                el.mobile = _mobile(el, ctx, out.errors)
            except (KeyError, TypeError, ValueError, AttributeError) as e:
                out.errors.append(f"{el.where}: {e}")
    for i, bm in enumerate(declared):
        where = f"bookmark {bm.get('name')!r}" if bm.get("name") else f"bookmarks[{i - len(popup_bookmarks)}]"
        doc = _build_bookmark(bm, where, out, schemas.get("bookmark"))
        if doc:
            out.bookmarks.append(doc)
    out.bookmark_groups = _bookmark_groups(spec, {k: v for k, v in names.items() if k},
                                           {str(b).lower() for b in existing_bookmarks or []} | set(names),
                                           out.errors)

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
    # page folder (lower case) -> "Drillthrough", "Tooltip" or None: what the page is used for
    page_kinds: Dict[str, Optional[str]] = field(default_factory=dict)
    # filter name (lower case) -> the files that hold a filter of that name, as relative paths
    filter_names: Dict[str, List[str]] = field(default_factory=dict)


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


def _manifest_problem(manifest: Mapping[str, Any]) -> Optional[str]:
    """What is wrong with the shape of a manifest, or None. It is this module's own record, but
    it lives in a folder that people edit and merge, and a list where an object belongs would
    otherwise surface as an AttributeError half way through an apply."""
    def names(value: Any) -> bool:
        return isinstance(value, list) and all(isinstance(v, str) for v in value)

    pages = manifest.get("pages", {})
    if not isinstance(pages, dict):
        return '"pages" is not an object'
    for name, entry in pages.items():
        if not isinstance(entry, dict):
            return f"the entry of page {name!r} is not an object"
        for key in ("elements", "filters", "keys", "mobile"):
            if key in entry and not names(entry[key]):
                return f'"{key}" of page {name!r} is not a list of names'
        by_element = entry.get("element_filters", {})
        if not isinstance(by_element, dict) or not all(names(v) for v in by_element.values()):
            return f'"element_filters" of page {name!r} is not an object of name lists'
        pairs = entry.get("interactions", [])
        if not isinstance(pairs, list) or not all(names(p) and len(p) == 2 for p in pairs):
            return f'"interactions" of page {name!r} is not a list of [from, to] pairs'
    for key in ("bookmarks", "public_visuals", "images", "stale_files", "report_filters", "bookmark_groups"):
        if key in manifest and not names(manifest[key]):
            return f'"{key}" is not a list of names'
    packages = manifest.get("custom_visuals", {})
    if not isinstance(packages, dict) or not all(names(v) for v in packages.values()):
        return '"custom_visuals" is not an object of file lists'
    if "theme" in manifest and not isinstance(manifest["theme"], dict):
        return '"theme" is not an object'
    return None


def _page_kind(doc: Mapping[str, Any]) -> Optional[str]:
    """What a page.json says the page is used for, by its type or its binding: one of
    pb.BINDING_TYPES, or None for an ordinary page."""
    binding = doc.get("pageBinding")
    for value in (doc.get("type"), binding.get("type") if isinstance(binding, Mapping) else None):
        if value in pb.BINDING_TYPES:
            return value
    return None


def _read_state(root: Path, manifest_file: Path, errors: List[str]) -> _State:
    state = _State()
    definition = root / "definition"
    found: Dict[str, List[str]] = {"visual": [], "page": [], "bookmark": [], "mobile": []}

    def note(path: Path, text: str) -> None:
        # Only a file that mentions the keys is parsed. One that cannot be parsed says nothing
        # here; it is refused later if this apply has to write it.
        if not any(key in text for key in ('"filterConfig"', '"pageBinding"', '"type"')):
            return
        try:
            doc = safe_io.read_json(path)
        except (OSError, ValueError):
            return
        if not isinstance(doc, dict):
            return
        for name in pf.filter_names(doc.get("filterConfig")):
            state.filter_names.setdefault(name.lower(), []).append(_rel(path, root))
        if path.name == "page.json":
            state.page_kinds[path.parent.name.lower()] = _page_kind(doc)

    pages_dir = definition / "pages"
    if pages_dir.is_dir():
        for page_dir in sorted(p for p in pages_dir.iterdir() if p.is_dir()):
            visuals: Set[str] = set()
            if (page_dir / "page.json").is_file():
                state.page_json.add(page_dir.name)
                state.page_kinds[page_dir.name.lower()] = None
                text = _text(page_dir / "page.json")
                m = _SCHEMA_IN_TEXT.search(text[:600])
                if m:
                    found["page"].append(m.group(1))
                note(page_dir / "page.json", text)
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
                        if '"filterConfig"' in text:
                            note(vfile, text)
                    if (visual_dir / "mobile.json").is_file():
                        m = _SCHEMA_IN_TEXT.search(_head(visual_dir / "mobile.json"))
                        if m:
                            found["mobile"].append(m.group(1))
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
    for name in pf.filter_names((state.report or {}).get("filterConfig")):
        state.filter_names.setdefault(name.lower(), []).append("definition/report.json")
    if manifest_file.is_file():
        manifest = _read_json_file(manifest_file, f"the manifest {manifest_file.name}", errors)
        if manifest is not None:
            problem = _manifest_problem(manifest)
            if manifest.get("version") != MANIFEST_VERSION:
                errors.append(f"the manifest {manifest_file.name} has version {manifest.get('version')!r}; "
                              f"this server writes version {MANIFEST_VERSION}")
            elif problem:
                errors.append(f"the manifest {manifest_file.name} is damaged ({problem}). Restore it from "
                              "source control, or delete it and pass adopt=True to take the spec's pages "
                              "over again.")
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
def _existing(path: Path, root: Path, errors: List[str]) -> Json:
    """The JSON object a file this apply is about to write holds now; {} when there is no file.

    A file that is there but cannot be read (half a merge, a stray edit) is an error, not an
    empty file: writing over it would lose whatever a person had in it.
    """
    if not path.is_file():
        return {}
    return _read_json_file(path, _rel(path, root), errors) or {}


def _carry_over(doc: Json, current: Mapping[str, Any], owned: Set[str],
                shared: Optional[Mapping[str, Any]] = None) -> Json:
    """Keep the keys of an existing file that the spec has no say over.

    ``shared`` holds keys with entries from both sides (a filter list with the spec's filters
    and ones added in Desktop), already merged: each takes the place the key has in the file,
    and None drops the key.
    """
    out = dict(doc)
    pending = dict(shared or {})
    for key, value in current.items():
        if key in pending:
            value = pending.pop(key)
            if value is not None:
                out[key] = value
        elif key not in owned and key not in out:
            out[key] = value
    out.update({key: value for key, value in pending.items() if value is not None})
    return out


def _foreign_filters(config: Any, ours: Sequence[Mapping[str, Any]], previous: Any, where: str,
                     errors: List[str]) -> None:
    """Refuse a filter name that a filter someone else put in the same file already has."""
    mine = _lower(previous)
    there = _lower(pf.filter_names(config))
    for item in ours:
        if item["name"].lower() in there and item["name"].lower() not in mine:
            errors.append(f"{where}: a filter named {item['name']!r} is already there and was not created "
                          'by an earlier apply. Give the filter another "name", or pass adopt=True to '
                          "replace it.")


def _merge_interactions(current: Any, ours: Sequence[Mapping[str, Any]],
                        previous: Any) -> Optional[List[Any]]:
    """A page's visualInteractions with this apply's pairs in place of an earlier apply's.
    A pair set in Desktop that the spec never named stays."""
    def pair(item: Any) -> Tuple[str, str]:
        if isinstance(item, Mapping):
            return str(item.get("source", "")).lower(), str(item.get("target", "")).lower()
        if isinstance(item, (list, tuple)) and len(item) == 2:
            return str(item[0]).lower(), str(item[1]).lower()
        return "", ""

    mine = {pair(i): i for i in ours}
    gone = {pair(p) for p in previous or []} - set(mine)
    kept: List[Any] = []
    placed: Set[Tuple[str, str]] = set()
    for item in (current if isinstance(current, list) else []):
        key = pair(item)
        if key in mine:
            if key not in placed:          # ours keeps the place it has in the file
                kept.append(dict(mine[key]))
                placed.add(key)
        elif key not in gone:
            kept.append(item)
    return (kept + [dict(i) for key, i in mine.items() if key not in placed]) or None


def _page_records(info: Mapping[str, Any], elements: Sequence[str]) -> Json:
    """What the manifest remembers about a page beyond its elements, narrowed to ``elements``:
    the filters, interactions and binding keys an apply put in page.json, and the filters and
    phone positions it gave those elements."""
    out: Json = {k: info[k] for k in ("filters", "interactions", "keys") if info.get(k)}
    names = _lower(elements)
    filters = {k: v for k, v in (info.get("element_filters") or {}).items() if str(k).lower() in names and v}
    if filters:
        out["element_filters"] = filters
    mobile = [m for m in info.get("mobile") or [] if str(m).lower() in names]
    if mobile:
        out["mobile"] = mobile
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


def _group_bookmarks(items: List[Any], groups: Sequence[Mapping[str, Any]], old_groups: Set[str],
                     spec_bookmarks: Set[str]) -> List[Any]:
    """bookmarks.json items with the spec's groups in place.

    A group an earlier apply made and the spec dropped is opened up: its bookmarks stay where
    the group was. A bookmark the spec puts in a group leaves wherever else it was listed. A
    group made in Desktop is left alone apart from that, and so is a bookmark someone moved
    into one of the spec's groups.
    """
    declared = {g["name"].lower(): g for g in groups}
    grouped = {str(c).lower() for g in groups for c in g["children"]}
    out: List[Any] = []
    placed: Set[str] = set()
    for item in items:
        children = item.get("children") if isinstance(item, dict) else None
        key = str(item.get("name", "")).lower() if isinstance(item, dict) else ""
        if not isinstance(children, list):
            if key not in grouped:
                out.append(item)
        elif key in declared:
            # The spec's bookmarks in the spec's order; anything else that sits in the group stays.
            others = [c for c in children if str(c).lower() not in grouped | spec_bookmarks]
            out.append({**item, "displayName": declared[key]["displayName"],
                        "children": list(declared[key]["children"]) + others})
            placed.add(key)
        elif key in old_groups:
            out.extend({"name": c} for c in children if str(c).lower() not in grouped)
        else:
            kept = [c for c in children if str(c).lower() not in grouped]
            out.append(item if len(kept) == len(children) else {**item, "children": kept})
    return out + [dict(g) for g in groups if g["name"].lower() not in placed]


def _listed_bookmarks(items: List[Any]) -> Set[str]:
    names: Set[str] = set()
    for item in items:
        if isinstance(item, dict):
            names.add(str(item.get("name", "")).lower())
            names |= {str(c).lower() for c in item.get("children") or []}
    return names


def _built_package(path: str, alias: str, errors: List[str], warnings: List[str]) -> Optional[Json]:
    """The package a custom visual definition builds, read back the way a .pbiviz file is.
    A definition that does not build is a spec error like any other. The build is deterministic,
    so an unchanged definition unpacks to the same files on every apply. Its one write goes to
    a temporary folder, never to the report."""
    try:
        definition, base = custom_visual.load_definition(path)
    except custom_visual.DefinitionError as e:
        errors.extend(f"custom visual {alias!r}: {problem}" for problem in e.errors)
        return None
    with tempfile.TemporaryDirectory() as folder:
        target = os.path.join(folder, "visual.pbiviz")
        built = custom_visual.build_custom_visual(definition, target, base_dir=base)
        errors.extend(f"custom visual {alias!r}: {problem}" for problem in built["errors"])
        warnings.extend(f"custom visual {alias!r}: {note}" for note in built["warnings"])
        return pbir_packages.read_pbiviz(target) if built["success"] else None


def _drill_roles(package: Mapping[str, Any]) -> Set[str]:
    """The roles a custom visual drills on: capabilities.drilldown.roles of its package."""
    try:
        meta = json.loads(package["files"][f"resources/{package['guid']}.pbiviz.json"].decode("utf-8-sig"))
        roles = meta["capabilities"]["drilldown"]["roles"]
    except (KeyError, TypeError, ValueError):
        return set()
    return {str(r) for r in roles} if isinstance(roles, list) else set()


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
    aliases, _, built = _custom_aliases(spec, [])
    packages: Dict[str, Json] = {}
    guids: Dict[str, str] = {}
    drill_roles: Dict[str, Set[str]] = {}
    for alias, path in aliases.items():
        full = path if os.path.isabs(path) else str(root.parent / path)
        try:
            package = _built_package(full, alias, errors, warnings) if alias in built \
                else pbir_packages.read_pbiviz(full)
        except pbir_packages.PbivizError as e:
            errors.append(str(e))
            continue
        if package is None:
            continue
        guids[alias] = package["guid"]
        packages[package["guid"]] = package
        drill_roles[alias] = _drill_roles(package)
    if errors:
        return result

    compiled = _compile(spec, model_fields=model_fields, schemas=state.schemas, guids=guids,
                        existing_pages=sorted(state.page_json), existing_bookmarks=sorted(state.bookmarks),
                        page_kinds=state.page_kinds, drill_roles=drill_roles)
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

    # Every filter name an earlier apply of this manifest made, wherever it put it.
    made_before = _lower(old.get("report_filters"))
    for info in old_pages.values():
        made_before |= _lower(info.get("filters"))
        made_before |= {n.lower() for names in (info.get("element_filters") or {}).values() for n in names}

    def unique_in_report(filters: Sequence[Mapping[str, Any]], file: Path, where: str) -> None:
        # The schema makes a filter's name unique across the whole report, not just its file.
        here = _rel(file, root).lower()
        for item in filters:
            elsewhere = [f for f in state.filter_names.get(item["name"].lower(), []) if f.lower() != here]
            if elsewhere and item["name"].lower() not in made_before:
                errors.append(f"{where}: a filter named {item['name']!r} is already in {elsewhere[0]}, and a "
                              'filter name is unique across the report. Give this one another "name".')

    for page in compiled.pages:
        folder = pages_dir / disk_pages.get(page.name.lower(), page.name)
        managed = old_page.get(page.name.lower()) or {}
        # What this apply owns inside the page's files. A filter, an interaction or a binding is
        # the spec's only while the spec declares it; the manifest carries the names forward so
        # the next apply can tell them from ones made in Desktop and remove just those.
        record: Json = {}
        if page.doc is None:
            record = _page_records(managed, [])
        else:
            current = _existing(folder / "page.json", root, errors)
            size_before = (current.get("width"), current.get("height"))
            if page.tooltip_sized and all(_is_num(v) for v in size_before) \
                    and size_before != (page.width, page.height):
                warnings.append(f"page {page.name!r} is a tooltip page and gives no size of its own, so it "
                                f"becomes {page.width} x {page.height} (it was {size_before[0]} x "
                                f'{size_before[1]}). Give the page "width" and "height" to keep another '
                                "size.")
            shared: Json = {}
            declared = page.doc.get("pageBinding")
            had_keys = set(managed.get("keys") or [])
            owned = set(_PAGE_OWNED)
            kept_by_hand = False
            if declared is not None or "pageBinding" in had_keys:
                # The binding is the spec's, but a field someone bound in Desktop is not: its
                # parameter stays, and with it the page stays what it is.
                shared["pageBinding"] = pb.merge_binding(current.get("pageBinding"), declared,
                                                         managed.get("filters") or [])
                kept_by_hand = declared is None and shared["pageBinding"] is not None
            if "type" in had_keys and not kept_by_hand:
                owned.add("type")
            if page.filters is not None or managed.get("filters"):
                ours = page.filters or []
                if not adopt:
                    _foreign_filters(current.get("filterConfig"), ours, managed.get("filters"),
                                     f"page {page.name!r}", errors)
                unique_in_report(ours, folder / "page.json", f"page {page.name!r}")
                shared["filterConfig"] = pf.merge_filters(current.get("filterConfig"), ours,
                                                          managed.get("filters") or [])
                if ours:
                    record["filters"] = [f["name"] for f in ours]
            if page.interactions is not None or managed.get("interactions"):
                pairs = page.interactions or []
                shared["visualInteractions"] = _merge_interactions(current.get("visualInteractions"), pairs,
                                                                   managed.get("interactions"))
                if pairs:
                    record["interactions"] = [[i["source"], i["target"]] for i in pairs]
            docs[folder / "page.json"] = _carry_over(
                {k: v for k, v in page.doc.items() if k != "pageBinding"}, current, owned, shared)
            binding_keys = [k for k in _BINDING_KEYS if k in page.doc]
            if binding_keys:
                record["keys"] = binding_keys
        wanted = {el.name.lower() for el in page.elements}
        on_disk = {v.lower(): v for v in state.pages.get(folder.name, set())}
        old_filters = {str(k).lower(): v for k, v in (managed.get("element_filters") or {}).items()}
        old_mobile = _lower(managed.get("mobile"))
        element_filters: Json = {}
        on_phone: List[str] = []
        for el in page.elements:
            if el.doc is None:
                continue
            visual_dir = folder / "visuals" / on_disk.get(el.name.lower(), el.name)
            current = _existing(visual_dir / "visual.json", root, errors)
            shared = {}
            previous = old_filters.get(el.name.lower()) or []
            if el.filters is not None or previous:
                ours = el.filters or []
                if not adopt:
                    _foreign_filters(current.get("filterConfig"), ours, previous, el.where, errors)
                unique_in_report(ours, visual_dir / "visual.json", el.where)
                shared["filterConfig"] = pf.merge_filters(current.get("filterConfig"), ours, previous)
                if ours:
                    element_filters[el.name] = [f["name"] for f in ours]
            docs[visual_dir / "visual.json"] = _carry_over(el.doc, current, _VISUAL_OWNED, shared)
            rewritten_visuals.add(f"{folder.name}/{el.name}".lower())
            mobile_file = visual_dir / "mobile.json"
            if el.mobile is not None:
                docs[mobile_file] = _carry_over(el.mobile, _existing(mobile_file, root, errors),
                                                _MOBILE_OWNED)
                on_phone.append(el.name)
            elif el.name.lower() in old_mobile and mobile_file.is_file():
                deletes.append(mobile_file)        # ours, and the spec took the element off the phone layout
        kept = [el.name for el in page.elements]
        for name in managed.get("elements") or []:
            if name.lower() in wanted or name.lower() not in on_disk:
                continue
            if prune:
                drop_visual(folder, on_disk[name.lower()])
            else:
                kept.append(name)
                if old_filters.get(name.lower()):
                    element_filters[name] = old_filters[name.lower()]
                if name.lower() in old_mobile:
                    on_phone.append(name)
        entry: Json = {"owned": not page.existing, "elements": sorted(kept, key=str.lower), **record}
        if element_filters:
            entry["element_filters"] = {k: element_filters[k] for k in sorted(element_filters, key=str.lower)}
        if on_phone:
            entry["mobile"] = sorted(on_phone, key=str.lower)
        new_manifest["pages"][page.name] = entry

    for name, info in old_pages.items():
        key = str(name).lower()
        if key in spec_pages or key not in disk_pages:
            continue
        folder = pages_dir / disk_pages[key]
        on_disk = {v.lower(): v for v in state.pages.get(folder.name, set())}
        managed = [e for e in info.get("elements") or [] if e.lower() in on_disk]
        if not prune:
            new_manifest["pages"][name] = {"owned": bool(info.get("owned")),
                                           "elements": sorted(managed, key=str.lower),
                                           **_page_records(info, managed)}
            continue
        for element in managed:
            drop_visual(folder, on_disk[element.lower()])
        if info.get("owned"):
            others = sorted(set(on_disk) - {e.lower() for e in managed})
            if others:
                theirs = ", ".join(on_disk[o] for o in others[:5])
                warnings.append(f"page {name!r} is no longer in the spec, but it holds visuals this tool did "
                                f"not create ({theirs}), so the page was kept")
                new_manifest["pages"][name] = {"owned": True, "elements": [], **_page_records(info, [])}
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
    groups = compiled.bookmark_groups or []
    old_groups = _lower(old.get("bookmark_groups"))
    listed_groups = {str(i.get("name", "")).lower() for i in (state.bookmarks_meta or {}).get("items") or []
                     if isinstance(i, dict) and isinstance(i.get("children"), list)}
    for group in groups:
        if group["name"].lower() in listed_groups and group["name"].lower() not in old_groups and not adopt:
            errors.append(f"bookmark group {group['name']!r} already exists and was not created by an "
                          "earlier apply. Pick another name, or pass adopt=True to take it over.")
    if compiled.bookmarks or gone_bookmarks or groups or old_groups:
        bmeta = copy.deepcopy(state.bookmarks_meta) if state.bookmarks_meta is not None else \
            pb.bookmarks_metadata([], state.schemas.get("bookmarksMetadata"))
        items = _strip_bookmarks(list(bmeta.get("items") or []), gone_bookmarks)
        items = _group_bookmarks(items, groups, old_groups, wanted_bookmarks)
        present = _listed_bookmarks(items)
        items += [{"name": doc["name"]} for doc in compiled.bookmarks if doc["name"].lower() not in present]
        bmeta["items"] = items
        docs[bookmarks_dir / "bookmarks.json"] = bmeta
    if groups:
        new_manifest["bookmark_groups"] = sorted((g["name"] for g in groups), key=str.lower)

    # report.json: report-level filters, theme, images, AppSource ids, custom visual packages
    report = copy.deepcopy(state.report) if state.report is not None else pb.new_report()
    old_report_filters = old.get("report_filters") or []
    if compiled.report_filters is not None or old_report_filters:
        report_filters = compiled.report_filters or []
        if not adopt:
            _foreign_filters(report.get("filterConfig"), report_filters, old_report_filters, "filters",
                             errors)
        unique_in_report(report_filters, definition / "report.json", "filters")
        merged_filters = pf.merge_filters(report.get("filterConfig"), report_filters, old_report_filters)
        if merged_filters is None:
            report.pop("filterConfig", None)
        else:
            report["filterConfig"] = merged_filters
        if report_filters:
            new_manifest["report_filters"] = [f["name"] for f in report_filters]
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

    managed_anything = any(new_manifest.get(k) for k in (
        "pages", "bookmarks", "custom_visuals", "public_visuals", "images", "stale_files", "report_filters",
        "bookmark_groups")) or "theme" in new_manifest
    if managed_anything or manifest_file.is_file():
        docs[manifest_file] = new_manifest
    if errors:
        return result

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
