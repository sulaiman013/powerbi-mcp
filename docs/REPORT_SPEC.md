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
  or removed. `prune` removes only what an earlier apply created. Inside a file the spec writes,
  a filter, an interaction or a phone position made in Power BI Desktop is kept; the spec's own
  are told apart by the names recorded in the manifest.
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
```

## Example

```json
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
```

## Measures that drive HTML, SVG and script elements

The `html`, `image` and `deneb` elements display what a measure returns. Generate those measures
with the matching tools, save them with `pbip_add_measures` (use `mode: "upsert"` so a rebuild
replaces them in place), then reference them from the spec:

| Element | Measure from |
|---|---|
| `html` (static HTML: KPI cards, headline sentences, bar lists, tables; either edition) | `generate_html_measure` |
| `html` (a JavaScript app; needs `"edition": "regular"`, the default) | `build_js_app_measure`, fed by `generate_payload_measure` |
| `image` with `measure` (an SVG data URI) | `generate_svg_measure` |

## Limits

- The JSON shapes were taken from report files that open in Power BI Desktop, and the tests
  check the emitted structure against them. Generated reports have not been opened in Desktop
  by the test suite: reload and look (`bridge_reload`, `bridge_screenshot`) after a build.
- Filters, drillthrough and tooltip pages, interactions, the phone layout and bookmark groups
  follow the public PBIR schemas and were loaded into Power BI Desktop. Filters were seen to
  change the numbers, and the filter pane showed their names, locks and hidden marks. A tooltip
  page was seen to appear, filtered to the row, for a visual that names it. What a click does
  otherwise (drilling through, one visual filtering another, the phone view) was not observed:
  Desktop accepted the files and drew the pages.
- Not covered by the spec: data state inside bookmarks (a bookmark shows and hides elements; it
  does not set slicers or filters), drillthrough on a measure or a summarized column, relative
  time filters and calendar-aligned relative dates, phone-specific formatting, and the look of
  the filter pane (set that in the theme).
- A filter pins values into the report files. Anyone who can read the report definition can
  read them, and a filter is not a security boundary: use row-level security for that.
- Images are registered, not created: the file must already be in
  `StaticResources/RegisteredResources`.
- Certification differs by visual. Deneb's AppSource edition and HTML Content Secure are
  certified. HTML Content Regular (the edition that runs scripts, which a JavaScript app in a
  measure needs) and private `.pbiviz` visuals, built here or not, are not: a tenant that only
  allows certified visuals will not render those. `audit_report_design` lists them.
