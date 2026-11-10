# Report as code: the report spec

`pbir_apply_spec` builds or updates a PBIP report from one declarative JSON document. It is the
same idea as building a report with a script, without writing the script: the spec says what
the report contains, and the server writes the PBIR files Power BI Desktop reads.

This page is generated from the docstring of [`src/report_spec.py`](../src/report_spec.py)
(`python docs/build_report_spec_doc.py`), so it describes exactly what the code accepts.

## What applying a spec guarantees

- **Idempotent.** Every page, element and bookmark has a stable `name`. Applying the same spec
  twice rewrites nothing; changing one element rewrites that element's file.
- **All or nothing.** The whole spec is validated first: names, layout (elements inside the
  page), navigation targets, and every field against the model. If anything is wrong, no file
  is written. Valid changes land in one transaction.
- **Non-destructive.** Pages, visuals and bookmarks the spec did not create are never rewritten
  or removed. `prune` removes only what an earlier apply created.
- **Faithful files.** New files are UTF-8 without a byte order mark and use LF; existing files
  keep their own encoding and line endings.

## Workflow

```
pbip_load_project        load the .pbip (the report must be in PBIR format)
pbir_apply_spec          dry_run=true first: returns the files it would write, or the errors
pbir_apply_spec          apply
audit_report_design      structure, design and governance findings for the result
bridge_reload            hot-reload the open Desktop (never over unsaved changes)
bridge_screenshot        look at the rendered pages
```

## The spec

```
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
```

## Example

```json
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
```

## Applying

```
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
```

## Measures that drive HTML, SVG and script elements

The `html`, `image` and `deneb` elements display what a measure returns. Generate those measures
with the matching tools, save them with `pbip_add_measures` (use `mode: "upsert"` so a rebuild
replaces them in place), then reference them from the spec:

| Element | Measure from |
|---|---|
| `html` (static HTML: KPI cards, headline sentences, bar lists, tables) | `generate_html_measure` |
| `html` (a JavaScript app) | `build_js_app_measure`, fed by `generate_payload_measure` |
| `image` with `measure` (an SVG data URI) | `generate_svg_measure` |

## Limits

- The JSON shapes were taken from report files that open in Power BI Desktop, and the tests
  check the emitted structure against them. Generated reports have not been opened in Desktop
  by the test suite: reload and look (`bridge_reload`, `bridge_screenshot`) after a build.
- Not covered by the spec: drillthrough, report-level and page-level filters, visual
  interactions, mobile layout, bookmark groups, and data state inside bookmarks.
- Images are registered, not created: the file must already be in
  `StaticResources/RegisteredResources`.
- HTML Content, Deneb and private custom visuals are not certified visuals. A tenant that only
  allows certified visuals will not render them; `audit_report_design` lists them.
