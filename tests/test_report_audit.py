"""
Tests for src/report_audit.py on synthetic PBIR report definitions built in a temp folder:
a clean report gives no findings; each structural, design, governance and interactivity
check fires on the defect it is for and stays quiet on the deliberate pattern next to it; a
report that uses drillthrough, tooltip pages, filters, visual interactions, a phone layout,
bookmark groups and custom visuals correctly gives no warning and no error; hostile files
and arguments are reported or refused, never raised from inside; the audit changes no file.
Every report file is written by hand as JSON, so the audit is judged on files and not on a
generator. No Power BI required. Run: python tests/test_report_audit.py
"""
import copy
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))
import report_audit as ra  # noqa: E402

_failures = []
SCHEMA = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/{}/{}/schema.json"


def check(name, cond, detail=""):
    line = f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f": {detail}" if detail and not cond else "")
    print(line.encode("ascii", "backslashreplace").decode("ascii"))
    if not cond:
        _failures.append(name)


def eq(name, actual, expected):
    check(name, actual == expected, f"got {actual!r}, expected {expected!r}")


# ---------------------------------------------------------------- building a report

def lit(value):
    return {"expr": {"Literal": {"Value": value}}}


def column(table, name):
    return {"field": {"Column": {"Expression": {"SourceRef": {"Entity": table}}, "Property": name}},
            "queryRef": f"{table}.{name}"}


def measure(table, name):
    return {"field": {"Measure": {"Expression": {"SourceRef": {"Entity": table}}, "Property": name}},
            "queryRef": f"{table}.{name}"}


def visual(name, vtype="barChart", x=0, y=0, w=300, h=200, fields=None, title="A title", alt="What it shows",
           parent=None, hidden=False, link=None, sync=None, fill=None, version="2.7.0"):
    container = {"general": [{"properties": {"keepLayerOrder": lit("true")}}]}
    if alt is not None:
        container["general"][0]["properties"]["altText"] = alt if isinstance(alt, dict) else lit(f"'{alt}'")
    if title is not None:
        container["title"] = [{"properties": {"show": lit("true" if title else "false"),
                                              **({"text": lit(f"'{title}'")} if title else {})}}]
    if link:
        container["visualLink"] = [{"properties": {"show": lit("true"), **{k: lit(f"'{v}'") for k, v in link.items()}}}]
    body = {"visualType": vtype, "objects": {}, "visualContainerObjects": container, "drillFilterOtherVisuals": True}
    if fields:
        body["query"] = {"queryState": {"Values": {"projections": fields}}}
    if sync:
        body["syncGroup"] = {"groupName": sync, "fieldChanges": True, "filterChanges": True}
    if fill:
        body["objects"]["dataPoint"] = [{"properties": {"fill": {"solid": {"color": fill}}}}]
    doc = {"$schema": SCHEMA.format("visualContainer", version), "name": name,
           "position": {"x": x, "y": y, "z": 0, "width": w, "height": h, "tabOrder": 0}, "visual": body}
    if parent:
        doc["parentGroupName"] = parent
    if hidden:
        doc["isHidden"] = True
    return doc


def group(name, x=0, y=0, w=400, h=300, hidden=False):
    doc = {"$schema": SCHEMA.format("visualContainer", "2.7.0"), "name": name,
           "position": {"x": x, "y": y, "z": 10, "width": w, "height": h, "tabOrder": 10},
           "visualGroup": {"displayName": name, "groupMode": "ScaleMode"}}
    if hidden:
        doc["isHidden"] = True
    return doc


def page(name, visuals, width=1280, height=720, **extra):
    doc = {"$schema": SCHEMA.format("page", "2.0.0"), "name": name, "displayName": name.title(),
           "displayOption": "FitToPage", "width": width, "height": height, "objects": {}}
    doc.update(extra)
    return {"doc": doc, "visuals": visuals}


def dump(path, doc):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        json.dump(doc, f, indent=2)


def write_report(pages, bookmarks=None, public=None, private=None, order=None, active=None, bookmark_items=None,
                 report_extra=None, packages=None, landing=None):
    """Write a report folder. A visual may carry "_folder" (its folder name) and "_mobile" (the
    mobile.json to put beside it: a dict, or text written as it is). ``packages`` maps a custom
    visual id to its pbiviz.json (a dict, or text written as it is). ``landing`` is written as
    pages.json landingPageName."""
    root = os.path.join(tempfile.mkdtemp(), "Demo.Report")
    d = os.path.join(root, "definition")
    dump(os.path.join(d, "report.json"), {"$schema": SCHEMA.format("report", "3.0.0"), "publicCustomVisuals": public or [],
                                          **(report_extra or {})})
    dump(os.path.join(d, "version.json"), {"version": "2.0.0"})
    names = [p["doc"]["name"] for p in pages]
    dump(os.path.join(d, "pages", "pages.json"), {"$schema": SCHEMA.format("pagesMetadata", "1.1.0"),
                                                  "pageOrder": order if order is not None else names,
                                                  "activePageName": active or names[0],
                                                  **({"landingPageName": landing} if landing else {})})
    for p in pages:
        folder = os.path.join(d, "pages", p.get("folder") or p["doc"]["name"])
        dump(os.path.join(folder, "page.json"), p["doc"])
        for v in p["visuals"]:
            vfolder = os.path.join(folder, "visuals", v.get("_folder") or v["name"])
            dump(os.path.join(vfolder, "visual.json"), {k: val for k, val in v.items() if not k.startswith("_")})
            if isinstance(v.get("_mobile"), str):
                with open(os.path.join(vfolder, "mobile.json"), "w", encoding="utf-8") as f:
                    f.write(v["_mobile"])
            elif "_mobile" in v:
                dump(os.path.join(vfolder, "mobile.json"), v["_mobile"])
    if bookmarks is not None:
        items = bookmark_items if bookmark_items is not None else [{"name": b["name"]} for b in bookmarks]
        dump(os.path.join(d, "bookmarks", "bookmarks.json"), {"items": items})
        for b in bookmarks:
            dump(os.path.join(d, "bookmarks", b["name"] + ".bookmark.json"), b)
    for name in private or []:
        dump(os.path.join(root, "CustomVisuals", name, "package.json"), {"visual": {"guid": name}})
    for guid, meta in (packages or {}).items():
        dump(os.path.join(root, "CustomVisuals", guid, "package.json"), {"visual": {"guid": guid}})
        path = os.path.join(root, "CustomVisuals", guid, "resources", guid + ".pbiviz.json")
        if isinstance(meta, str):
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w", encoding="utf-8") as f:
                f.write(meta)
        else:
            dump(path, meta)
    return root, d


def ref(table, name, kind="Column"):
    return {kind: {"Expression": {"SourceRef": {"Entity": table}}, "Property": name}}


def flt(name, table, field, values=None, hidden=False, locked=False, how=None, display=None, kind="Column"):
    """One filter of a filterConfig; ``values`` gives it a condition (an In list)."""
    doc = {"name": name, "field": ref(table, field, kind), "type": "Categorical"}
    if values:
        doc["filter"] = {"Version": 2, "From": [{"Name": "t", "Entity": table, "Type": 0}],
                         "Where": [{"Condition": {"In": {
                             "Expressions": [{"Column": {"Expression": {"SourceRef": {"Source": "t"}}, "Property": field}}],
                             "Values": [[{"Literal": {"Value": f"'{v}'"}}] for v in values]}}}]}
    if hidden:
        doc["isHiddenInViewMode"] = True
    if locked:
        doc["isLockedInViewMode"] = True
    if how:
        doc["howCreated"] = how
    if display:
        doc["displayName"] = display
    return doc


def drill_page(name, visuals, fields, **extra):
    """A drillthrough page as Desktop saves one: the page type, a binding of the same type whose
    parameters carry the fields, and one page filter per field marked howCreated 'Drillthrough'.
    A field is (table, column) or a ready field expression."""
    exprs = [f if isinstance(f, dict) else ref(*f) for f in fields]
    binding = {"name": "bind_" + name, "type": "Drillthrough",
               "parameters": [{"name": f"param_{name}_{i}", "boundFilter": f"filter_{name}_{i}", "asAggregation": False,
                               "qnaSingleSelectRequired": False, "fieldExpr": e} for i, e in enumerate(exprs)]}
    filters = [{"name": f"filter_{name}_{i}", "field": e, "type": "Categorical", "howCreated": "Drillthrough"}
               for i, e in enumerate(exprs)]
    extra.setdefault("visibility", "HiddenInViewMode")
    return page(name, visuals, type="Drillthrough", pageBinding=binding, filterConfig={"filters": filters}, **extra)


def tooltip(doc, section=None, show=None, kind=None, header=False):
    """Set a visual's (or its header's) tooltip to a report page."""
    props = {}
    if show is not None:
        props["show"] = lit("true" if show else "false")
    if kind:
        props["type"] = lit(f"'{kind}'")
    if section:
        props["section"] = lit(f"'{section}'")
    doc["visual"]["visualContainerObjects"]["visualHeaderTooltip" if header else "visualTooltip"] = [{"properties": props}]
    return doc


def phone(x, y, w, h):
    return {"$schema": SCHEMA.format("visualContainerMobileState", "2.3.0"),
            "position": {"x": x, "y": y, "z": 0, "width": w, "height": h, "tabOrder": 0}}


def pbiviz(name, capabilities, script, api="5.11.0"):
    """The metadata file of an unpacked custom visual: capabilities and script in one JSON."""
    return {"visual": {"name": name, "displayName": name.title(), "guid": name, "version": "1.0.0.0"},
            "apiVersion": api, "capabilities": capabilities, "content": {"js": script, "css": ""}}


def bookmark(name, active, sections=None, targets=None):
    doc = {"name": name, "displayName": name, "explorationState": {"version": "1.0", "activeSection": active,
                                                                   "sections": sections or {}}}
    if targets is not None:
        doc["options"] = {"targetVisualNames": targets}
    return doc


def snapshot(root):
    out = {}
    for folder, _, names in os.walk(root):
        for n in names:
            with open(os.path.join(folder, n), "rb") as f:
                out[os.path.join(folder, n)] = f.read()
    return out


def codes(result, code=None, severity=None):
    return [f for f in result["findings"] if (code is None or f["code"] == code) and (severity is None or f["severity"] == severity)]


def has(result, code, fragment, severity=None, page=None, visual=None):
    return any(fragment.lower() in f["message"].lower() and (severity is None or f["severity"] == severity)
               and (page is None or f["page"] == page) and (visual is None or f["visual"] == visual)
               for f in codes(result, code))


SALES = [column("Dim Product", "Category"), measure("Sales", "Total Sales")]


def clean_pages():
    return [
        page("home", [
            visual("chart", fields=SALES, x=20, y=20),
            visual("table", "tableEx", fields=SALES, x=340, y=20),
            visual("go", "actionButton", x=20, y=600, w=120, h=40, title=None, alt="Open the detail page",
                   link={"type": "PageNavigation", "navigationSection": "detail"}),
        ]),
        page("detail", [
            visual("detailchart", "lineChart", fields=SALES, x=20, y=20),
            visual("back", "actionButton", x=20, y=600, w=120, h=40, title=None, alt="Back to home",
                   link={"type": "PageNavigation", "navigationSection": "home"}),
        ]),
    ]


def test_clean_report():
    print("\n== a clean report ==")
    root, d = write_report(clean_pages())
    before = snapshot(root)
    r = ra.audit_report(root)
    eq("no findings", r["findings"], [])
    eq("the audit changes no file", snapshot(root), before)
    s = r["summary"]
    eq("counts", (s["pages"], s["visuals"], s["groups"], s["bookmarks"]), (2, 5, 0, 0))
    eq("visual types", s["visual_types"], {"actionButton": 2, "barChart": 1, "tableEx": 1, "lineChart": 1})
    eq("finding totals", (s["findings"]["total"], s["findings"]["error"]), (0, 0))
    check("the field check is reported as skipped", s["field_check"].startswith("skipped"))
    check("limits are stated", any("nothing was opened" in x for x in s["limits"]))
    eq("the definition folder is accepted too", ra.audit_report(d)["findings"], [])
    eq("page sizes", s["page_sizes"], {"1280x720": ["home", "detail"]})
    eq("nothing to report on the native features it does not use",
       (s["drillthrough_pages"], s["tooltip_pages"], s["bookmark_groups"], s["custom_visual_capabilities"],
        s["filters"]["report"] + s["filters"]["page"] + s["filters"]["visual"], s["visual_interactions"]["total"],
        s["path_lengths"]["too_long"], s["certification"]),
       ([], [], [], [], 0, 0, [], {"certified": [], "not_certified": [], "unknown": []}))
    eq("no phone layout is a fact in the summary, not a finding", s["mobile_layout"],
       {"pages": {}, "visuals": 0, "canvas_width": 323.0})
    try:
        ra.audit_report(os.path.dirname(root))
        check("a folder that is not a report is refused", False)
    except ValueError as e:
        check("a folder that is not a report is refused", "not a PBIR report definition" in str(e))


def test_structure():
    print("\n== structure ==")
    pages = clean_pages()
    home = pages[0]["visuals"]
    home.append(visual("outside", fields=SALES, x=1200, y=600, w=300, h=200))
    home.append(visual("flat", fields=SALES, w=0, h=50))
    noposition = visual("noposition", fields=SALES)
    del noposition["position"]
    home.append(noposition)
    rooted = visual("rooted", fields=SALES, x=700, y=20)
    rooted["drillFilterOtherVisuals"] = True
    rooted["visual"]["position"] = {"x": 1}
    home.append(rooted)
    both = visual("both", fields=SALES, x=700, y=300)
    both["visualGroup"] = {"displayName": "x"}
    home.append(both)
    typeless = visual("typeless", x=700, y=520, w=100, h=50)
    del typeless["visual"]["visualType"]
    home.append(typeless)
    home.append(visual("orphan", "card", fields=[measure("Sales", "Total Sales")], x=900, y=520, w=120, h=60, parent="nogroup"))
    home.append(visual("newer", "card", fields=[measure("Sales", "Total Sales")], x=1040, y=520, w=120, h=60, version="2.14.0"))
    noschema = visual("noschema", "card", fields=[measure("Sales", "Total Sales")], x=900, y=600, w=120, h=60)
    del noschema["$schema"]
    home.append(noschema)
    home.append(visual("custom", "fancyChartAB12CD34EF56AB12CD34EF56AB12CD34", fields=SALES, x=20, y=300))
    home.append(dict(visual("twin", "card", fields=[measure("Sales", "Total Sales")], x=340, y=300, w=100, h=60), _folder="twin_a"))
    home.append(dict(visual("twin", "card", fields=[measure("Sales", "Total Sales")], x=460, y=300, w=100, h=60), _folder="twin_b"))
    home.append(dict(visual("renamed", "card", fields=[measure("Sales", "Total Sales")], x=580, y=300, w=100, h=60), _folder="friendly name"))
    pages[1]["folder"] = "Detail page"
    root, d = write_report(pages, public=["unusedVisual0123456789ABCDEF0123456789ABCDEF"], order=["home", "ghost"], active="nowhere")
    hv = os.path.join(d, "pages", "home", "visuals")
    with open(os.path.join(hv, "chart", "visual.json"), "rb") as f:
        body = f.read()
    with open(os.path.join(hv, "chart", "visual.json"), "wb") as f:
        f.write(b"\xef\xbb\xbf" + body)
    os.makedirs(os.path.join(hv, "broken"))
    with open(os.path.join(hv, "broken", "visual.json"), "w", encoding="utf-8") as f:
        f.write('{"name": "broken", ')
    os.makedirs(os.path.join(hv, "empty"))
    os.makedirs(os.path.join(d, "pages", "leftover"))
    os.remove(os.path.join(d, "version.json"))

    r = ra.audit_report(root)
    check("a byte order mark", has(r, "STRUCT_BOM", "byte order mark", "warning", "home", "chart"))
    check("a file that is not JSON", has(r, "STRUCT_JSON", "not valid json", "error", "home", "broken"))
    check("a visual folder without visual.json", has(r, "STRUCT_FILE", "no visual.json", "error", "home", "empty"))
    check("a page folder without page.json", has(r, "STRUCT_FILE", "no page.json", "error", "leftover"))
    check("a missing version.json", has(r, "STRUCT_FILE", "version.json is missing", "warning"))
    check("two visuals with one name", has(r, "STRUCT_NAME", "two visuals", "error", "home", "twin"))
    renamed = [f for f in codes(r, "STRUCT_NAME", "info")]
    check("renamed folders are one note, not an error each",
          len(renamed) == 1 and "4 folder(s)" in renamed[0]["message"], [f["message"] for f in codes(r, "STRUCT_NAME")])
    check("pageOrder names a page that does not exist", has(r, "STRUCT_PAGES", "'ghost'", "error"))
    check("a page missing from pageOrder", has(r, "STRUCT_PAGES", "not listed", "error", "detail"))
    check("activePageName is not a page", has(r, "STRUCT_PAGES", "'nowhere'", "error"))
    check("a custom visual that is not registered", has(r, "STRUCT_CUSTOM_VISUAL", "fancyChartAB12CD34EF56AB12CD34EF56AB12CD34", "error"))
    check("a registered visual nobody uses is a note", has(r, "STRUCT_CUSTOM_VISUAL", "no visual uses it", "info"))
    check("a visual outside the page", has(r, "STRUCT_POSITION", "outside the 1280 x 720 page", "warning", "home", "outside"))
    check("a visual with no size", has(r, "STRUCT_POSITION", "0 x 50", "error", "home", "flat"))
    check("a visual with no position", has(r, "STRUCT_POSITION", "no position", "error", "home", "noposition"))
    check("drillFilterOtherVisuals at the root", has(r, "STRUCT_MISPLACED", "'drillFilterOtherVisuals' is at the root", "error", "home", "rooted"))
    check("position inside visual", has(r, "STRUCT_MISPLACED", "'position' is inside 'visual'", "warning", "home", "rooted"))
    check("visual and visualGroup together", has(r, "STRUCT_MISPLACED", "both", "error", "home", "both"))
    check("no visualType", has(r, "STRUCT_MISPLACED", "no visualType", "error", "home", "typeless"))
    check("a parent group that does not exist", has(r, "STRUCT_GROUP", "'nogroup'", "error", "home", "orphan"))
    check("mixed schema versions are a note", has(r, "STRUCT_SCHEMA", "2 schema versions: 2.14.0 (1), 2.7.0", "info"))
    check("a file with no $schema", has(r, "STRUCT_SCHEMA", "no recognisable $schema", "warning", "home", "noschema"))
    check("the grouped visual is not also reported as outside the page", not has(r, "STRUCT_POSITION", "", page="home", visual="orphan"))
    order = [f["severity"] for f in r["findings"]]
    check("findings are ordered by severity", order == sorted(order, key=["error", "warning", "info"].index), order[:12])
    check("every finding carries a recommendation",
          all(set(f) >= {"code", "severity", "page", "visual", "message", "recommendation"} and f["recommendation"]
              for f in r["findings"]))
    eq("the summary counts by code", r["summary"]["findings"]["by_code"]["STRUCT_MISPLACED"], 4)


def test_field_check():
    print("\n== fields against the model ==")
    pages = clean_pages()
    pages[0]["visuals"].append(visual("wrongfield", fields=[column("Dim Product", "Colour"), measure("Sales", "Total Sales")], x=700))
    pages[0]["visuals"].append(visual("wrongtable", "card", fields=[measure("Ghost", "M")], x=700, y=300, w=100, h=60))
    root, _ = write_report(pages)
    model = {"dim product": ["category", "Brand"], "Sales": {"columns": ["Amount"], "measures": ["TOTAL SALES"]}}
    r = ra.audit_report(root, model_fields=model)
    eq("only the two bad references are reported", len(codes(r, "STRUCT_FIELD")), 2)
    check("a field that is not in its table", has(r, "STRUCT_FIELD", "Dim Product[Colour], which is not in the model", "error", "home", "wrongfield"))
    check("a table that is not in the model", has(r, "STRUCT_FIELD", "no table 'Ghost'", "error", "home", "wrongtable"))
    eq("the summary says the check ran", r["summary"]["field_check"], "done")
    eq("without model_fields nothing is assumed", codes(ra.audit_report(root), "STRUCT_FIELD"), [])


def test_design():
    print("\n== design ==")
    card = [measure("Sales", "Total Sales")]
    home = [
        visual("canvas", "sceneAB12CD34EF56AB12CD34EF56AB12CD34", fields=card, x=0, y=0, w=1280, h=720, title=""),
        visual("over1", fields=SALES, x=20, y=20, alt=None),
        visual("over2", "lineChart", fields=SALES, x=150, y=80, alt=None),
        visual("clear", "card", fields=card, x=700, y=20, w=200, h=100, title="", alt={"expr": {"Measure": {"Property": "Alt"}}}),
        visual("button", "actionButton", x=20, y=20, w=300, h=200, title=None,
               link={"type": "PageNavigation", "navigationSection": "missing"}),
        group("panel", x=700, y=300, w=400, h=300),
        visual("inpanel1", fields=SALES, x=700, y=300, parent="panel"),
        visual("inpanel2", fields=SALES, x=720, y=320, parent="panel"),
        visual("tiny", "card", fields=card, x=1000, y=20, w=8, h=60),
        visual("rule", "shape", x=20, y=700, w=600, h=2, title=None, alt=None),
        visual("dot", "shape", x=640, y=700, w=4, h=4, title=None, alt=None),
        visual("slicer_a", "slicer", fields=[column("Dim Date", "Year")], x=1000, y=100, w=200, h=60),
        visual("slicer_b", "slicer", fields=[column("Dim Region", "Region")], x=1000, y=180, w=200, h=60, sync="region"),
        visual("slicer_c", "slicer", fields=[column("Dim Product", "Brand")], x=1000, y=260, w=200, h=60, sync="brand_a"),
        visual("tobookmark", "actionButton", x=340, y=600, w=120, h=40, title=None,
               link={"type": "Bookmark", "bookmark": "gone"}),
        visual("tohidden", "actionButton", x=480, y=600, w=120, h=40, title=None,
               link={"type": "PageNavigation", "navigationSection": "linked"}),
    ]
    busy = [visual(f"v{i}", "card", fields=card, x=10 + 100 * (i % 12), y=10 + 80 * (i // 12), w=90, h=70) for i in range(13)]
    second = [
        visual("slicer_a2", "slicer", fields=[column("Dim Date", "Year")], x=20, y=600, w=200, h=60, hidden=True),
        visual("slicer_b2", "slicer", fields=[column("Dim Region", "Region")], x=240, y=600, w=200, h=60, sync="region"),
        visual("slicer_c2", "slicer", fields=[column("Dim Product", "Brand")], x=460, y=600, w=200, h=60, sync="brand_b"),
    ]
    pages = [
        page("home", home),
        page("busy", busy + second),
        page("wide", [], width=1920, height=1080),
        page("tip", [], width=320, height=240, type="Tooltip", visibility="HiddenInViewMode"),
        page("drill", [], type="Drillthrough", visibility="HiddenInViewMode"),
        page("lost", [], visibility="HiddenInViewMode"),
        page("linked", [], visibility="HiddenInViewMode"),
        page("viabookmark", [], visibility="HiddenInViewMode"),
    ]
    bookmarks = [
        bookmark("ok", "viabookmark"),
        bookmark("stale", "home", sections={"home": {"visualContainers": {"over1": {}, "deleted": {}},
                                                     "visualContainerGroups": {"panel": {}, "oldgroup": {}}},
                                            "removedpage": {}}, targets=["over1", "nothing"]),
        bookmark("astray", "nopage"),
    ]
    root, _ = write_report(pages, bookmarks=bookmarks, public=["sceneAB12CD34EF56AB12CD34EF56AB12CD34"],
                           bookmark_items=[{"name": "ok"}, {"name": "grp", "displayName": "Group", "children": ["stale", "missingfile"]}])
    r = ra.audit_report(root)

    overlaps = codes(r, "DESIGN_OVERLAP")
    eq("only the two free charts overlap", [(f["page"], f["visual"]) for f in overlaps], [("home", "over1 + over2")])
    check("the overlap says how much", "overlap:" in overlaps[0]["message"] and overlaps[0]["severity"] == "warning", overlaps[0]["message"])
    check("grouped visuals, a button over a chart and a full-page canvas are deliberate",
          not any(n in f["visual"] for f in overlaps for n in ("inpanel", "button", "canvas")))
    check("a crowded page", has(r, "DESIGN_CROWDED", "13 data visuals", "warning", "busy"))
    check("a page under the threshold is not crowded", not codes(ra.audit_report(root, max_data_visuals_per_page=13), "DESIGN_CROWDED"))
    check("many elements is a note when asked for", has(ra.audit_report(root, max_data_visuals_per_page=50, max_visuals_per_page=14),
                                                         "DESIGN_CROWDED", "visible elements", "info", "busy"))
    alt = [f for f in codes(r, "DESIGN_ALT_TEXT") if f["page"] == "home"]
    eq("missing alt text is one finding per page, naming the visuals", [sorted(f["visuals"]) for f in alt], [["over1", "over2"]])
    check("alt text driven by an expression counts as present", "clear" not in alt[0]["visuals"])
    titles = [f for f in codes(r, "DESIGN_TITLE") if f["page"] == "home"]
    eq("titles switched off are a note", [(f["severity"], sorted(f["visuals"])) for f in titles], [("info", ["canvas", "clear"])])
    eq("tiny elements: a narrow card and a dot, not a thin rule",
       sorted(f["visual"] for f in codes(r, "DESIGN_TINY")), ["dot", "tiny"])
    sizes = codes(r, "DESIGN_PAGE_SIZE")
    check("page sizes differ, tooltip page aside", len(sizes) == 1 and "2 sizes" in sizes[0]["message"] and "320" not in sizes[0]["message"],
          [f["message"] for f in sizes])
    eq("only the hidden page nothing opens is reported", [f["page"] for f in codes(r, "DESIGN_HIDDEN_PAGE")], ["lost"])
    sync = codes(r, "DESIGN_SYNC")
    check("a field sliced on two pages without a sync group", any("Dim Date[Year]" in f["message"] and "not in a sync group" in f["message"] for f in sync))
    check("two different sync groups for one field", any("Dim Product[Brand]" in f["message"] and "2 different sync groups" in f["message"] for f in sync))
    check("a shared sync group is fine", not any("Dim Region" in f["message"] for f in sync))
    check("a button to a page that does not exist", has(r, "DESIGN_BROKEN_LINK", "'missing', which is not a page", "error", "home", "button"))
    check("a button to a bookmark that does not exist", has(r, "DESIGN_BROKEN_LINK", "'gone', which is not a bookmark", "error", "home", "tobookmark"))
    check("bookmarks.json lists a file that is not there", has(r, "DESIGN_BROKEN_LINK", "'missingfile'", "error"))
    check("a bookmark that opens a missing page", has(r, "DESIGN_BROKEN_LINK", "'nopage'", "error", visual="astray"))
    check("a bookmark holding state for a removed page", has(r, "DESIGN_BROKEN_LINK", "'removedpage'", "warning", visual="stale"))
    check("a bookmark referring to deleted visuals", has(r, "DESIGN_BROKEN_LINK", "no longer on the page: deleted", "warning", "home", "stale"))
    check("and to a deleted group", has(r, "DESIGN_BROKEN_LINK", "oldgroup", "warning", "home", "stale"))
    check("a bookmark targeting a visual that does not exist", has(r, "DESIGN_BROKEN_LINK", "do not exist: nothing", "warning", visual="stale"))
    check("a bookmark file missing from bookmarks.json is a note", has(r, "STRUCT_FILE", "'astray' is not listed", "info"))
    eq("bookmarks are counted", r["summary"]["bookmarks"], 3)
    eq("groups are counted apart from visuals", r["summary"]["groups"], 1)


def test_colours_and_governance():
    print("\n== colours and governance ==")
    theme = {"expr": {"ThemeDataColor": {"ColorId": 1, "Percent": 0}}}
    pages = [page("home", [
        visual("red", fields=SALES, x=20, fill=lit("'#FF0000'")),
        visual("alsored", fields=SALES, x=340, fill=lit("'#ff0000'")),
        visual("themed", fields=SALES, x=660, fill=theme),
        visual("html", "htmlContent0123456789ABCDEF0123456789ABCDEF", fields=[measure("M", "Page")], x=20, y=300, fill=lit("'#00AA00'")),
        visual("mine", "ownVisual0123456789ABCDEF0123456789ABCDEF", fields=SALES, x=340, y=300),
        visual("store", "storeVisualFEDCBA9876543210FEDCBA9876543210", fields=SALES, x=660, y=300),
    ])]
    pages[0]["doc"]["objects"] = {"background": [{"properties": {"color": {"solid": {"color": lit("'#FAFAFA'")}}}}]}
    root, _ = write_report(pages, public=["htmlContent0123456789ABCDEF0123456789ABCDEF", "storeVisualFEDCBA9876543210FEDCBA9876543210"],
                           private=["ownVisual0123456789ABCDEF0123456789ABCDEF"])
    r = ra.audit_report(root)
    c = r["summary"]["hard_coded_colours"]
    eq("literal colours are counted, case folded", (c["total"], c["distinct"], c["theme_references"]), (4, 3, 1))
    eq("most used first", c["most_used"][0], ["#FF0000", 2])
    eq("per page", c["by_page"], {"home": 4})
    check("one note for the report", len(codes(r, "DESIGN_COLOURS")) == 1 and codes(r, "DESIGN_COLOURS")[0]["severity"] == "info")
    eq("custom visuals in the summary", r["summary"]["custom_visuals"],
       {"public": ["htmlContent0123456789ABCDEF0123456789ABCDEF", "storeVisualFEDCBA9876543210FEDCBA9876543210"],
        "private": ["ownVisual0123456789ABCDEF0123456789ABCDEF"]})
    gov = [f for f in r["findings"] if f["code"].startswith("GOV_")]
    eq("three governance notes", sorted(f["code"] for f in gov), ["GOV_HTML_VISUAL", "GOV_PRIVATE_VISUAL", "GOV_PUBLIC_VISUAL"])
    check("governance is information, never an error", all(f["severity"] == "info" for f in gov))
    check("the private visual is named", has(r, "GOV_PRIVATE_VISUAL", "ownVisual0123456789ABCDEF0123456789ABCDEF (1)"))
    check("store visuals: certification cannot be read", has(r, "GOV_PUBLIC_VISUAL", "cannot be read"))
    check("the HTML visual is named", has(r, "GOV_HTML_VISUAL", "htmlContent0123456789ABCDEF0123456789ABCDEF (1)"))
    check("a registered custom visual is not a structure error", not codes(r, "STRUCT_CUSTOM_VISUAL"))
    eq("no errors and no warnings beyond the design notes", r["summary"]["findings"]["error"], 0)
    eq("certification: a private visual is never certified, an unknown store visual is unknown",
       r["summary"]["certification"],
       {"certified": [], "not_certified": ["ownVisual0123456789ABCDEF0123456789ABCDEF"],
        "unknown": ["htmlContent0123456789ABCDEF0123456789ABCDEF", "storeVisualFEDCBA9876543210FEDCBA9876543210"]})
    check("an HTML visual the audit does not know is not called script-capable",
          has(r, "GOV_HTML_VISUAL", "runs scripts cannot be read") and not has(r, "GOV_HTML_VISUAL", "also runs"))


def test_unreadable_parts():
    print("\n== unreadable parts do not stop the audit ==")
    root, d = write_report(clean_pages(), bookmarks=[bookmark("b", "home")])
    for rel in ("report.json", os.path.join("pages", "pages.json"), os.path.join("pages", "detail", "page.json"),
                os.path.join("bookmarks", "bookmarks.json"), os.path.join("bookmarks", "b.bookmark.json")):
        with open(os.path.join(d, rel), "w", encoding="utf-8") as f:
            f.write("{ not json")
    r = ra.audit_report(root)
    eq("each broken file is reported once", len(codes(r, "STRUCT_JSON")), 5)
    eq("the readable page is still audited", r["summary"]["pages"], 1)
    check("links into the unreadable page are reported, not crashed on", has(r, "DESIGN_BROKEN_LINK", "'detail'", "error", "home", "go"))
    os.remove(os.path.join(d, "report.json"))
    os.remove(os.path.join(d, "pages", "pages.json"))
    r = ra.audit_report(root)
    check("missing report.json and pages.json are errors", has(r, "STRUCT_FILE", "report.json is missing", "error")
          and has(r, "STRUCT_PAGES", "pages.json is missing", "error"))
    weird = copy.deepcopy(clean_pages())
    weird[0]["visuals"][0]["visual"]["query"] = "not an object"
    weird[0]["visuals"][1]["position"] = {"x": "left", "width": None}
    weird[0]["visuals"][2]["visual"]["visualContainerObjects"] = ["a list"]
    root, _ = write_report(weird)
    try:
        r = ra.audit_report(root)
        check("odd shapes are tolerated", r["summary"]["visuals"] == 5)
    except Exception as e:  # noqa: BLE001
        check("odd shapes are tolerated", False, f"{type(e).__name__}: {e}")


# ---------------------------------------------------------------- native report features

CARD = [measure("Sales", "Total Sales")]
CUSTOMER = [column("Dim Customer", "Name"), measure("Sales", "Total Sales")]
DENEB = "deneb7E15AEF80B9E4D4F8E12924291ECE89A"
HTML_SECURE = "htmlContent443BE3AD55E043BF878BED274D3A6865"
HTML_REGULAR = "htmlContent443BE3AD55E043BF878BED274D3A6855"
FULL, PLAIN, MENU, PICK, KIT, MUTE, NOISE, KEYED, ASSEMBLED, UNSPLIT, BARE, OLD, BROKEN, SPARE = (
    name + "VisualAB12CD34EF56AB12CD34EF56AB12CD34"
    for name in ("full", "plain", "menu", "pick", "kit", "mute", "noise", "keyed", "assembled", "unsplit", "bare", "old",
                 "broken", "spare"))
# A role that takes a column and one that takes a measure (the kind as a name, and as its number).
ROLES = [{"name": "category", "kind": "Grouping"}, {"name": "values", "kind": 1}]
MEASURES = [{"name": "values", "kind": "Measure"}]
# A visual that does what a native one does: it selects, opens the context menu on a data
# point, shows tooltips with the point's identity and reads the highlight values.
FULL_CAPS = {"dataRoles": ROLES, "supportsHighlight": True, "supportsKeyboardFocus": True, "supportsLandingPage": True,
             "supportsMultiVisualSelection": True,
             "tooltips": {"supportedTypes": {"default": True, "canvas": True}, "roles": ["tooltips"]},
             "drilldown": {"roles": ["category"]},
             "privileges": [{"name": "WebAccess", "essential": True, "parameters": ["https://*.example.com"]},
                            {"name": "LocalStorage", "essential": False}, {"name": "ExportContent", "essential": True}]}
FULL_JS = ("class V{constructor(o){this.h=o.host;this.s=this.h.createSelectionManager();}"
           "update(o){const c=o.dataViews[0].categorical;const lit=c.values[0].highlights;"
           "const id=this.h.createSelectionIdBuilder().withCategory(c.categories[0],0).createSelectionId();"
           "this.el.onclick=()=>this.s.select(id);"
           "this.el.oncontextmenu=e=>this.s.showContextMenu(id,{x:e.clientX,y:e.clientY});"
           "this.el.onmouseover=e=>this.h.tooltipService.show({coordinates:[e.clientX,e.clientY],identities:[id],dataItems:[]});}}")
PLAIN_CAPS = {"dataRoles": ROLES, "supportsLandingPage": True, "privileges": []}
MEASURE_CAPS = {"dataRoles": MEASURES, "supportsHighlight": True, "supportsLandingPage": True, "privileges": [],
                "tooltips": {"supportedTypes": {"default": True, "canvas": True}}}
MUTE_JS = "class V{update(o){this.el.textContent=o.dataViews[0].single.value;this.h.eventService.renderingFinished(o);}}"
MENU_JS = ("class V{constructor(o){this.s=o.host.createSelectionManager();}"
           "update(){this.el.oncontextmenu=e=>this.s.showContextMenu({},{x:e.clientX,y:e.clientY});}}")
PICK_JS = "class V{update(){this.h.applyJsonFilter(this.f,'general','filter',0);}}"
# Every member is named, and none is called: the names sit in comments and in strings.
NOISE_JS = ("/* never calls createSelectionManager or createSelectionIdBuilder; showContextMenu and tooltipService are unused */"
            "class V{update(o){this.log('applyJsonFilter is not used, highlights are ignored');"
            "this.el.title=\"see host.tooltipService\";this.el.textContent=o.dataViews[0].single.value;} // showContextMenu\n}")
# The members are reached through quoted names in brackets, which is a call all the same. In
# front of them stands a regular expression that, read as anything else, starts a comment.
KEYED_JS = ('class V{constructor(o){this.h=o.host;this.s=this.h["createSelectionManager"]();}'
            'update(o){const t=String(o.t).replace(/^https?:\\/\\//,"");'
            'const id=this.h["createSelectionIdBuilder"]().withCategory(o.c,0).createSelectionId();'
            'this.el.onclick=()=>this.s.select(id);}}')
# The names are put together at run time: no search for them finds a thing.
ASSEMBLED_JS = ("class V{constructor(o){this.h=o.host;this.s=this.h['create'+'SelectionManager']();}"
                "update(o){const id=this.h['createSelection'+'IdBuilder']().withCategory(o.c,0).createSelectionId();"
                "this.el.onclick=()=>this.s.select(id);}}")
# A regular expression in a place where the split takes it for a division: the quote in it
# swallows code, the brackets stop matching, and the whole text is searched instead.
UNSPLIT_JS = "class V{update(o){if(o.a)/'/.test(o.b)&&this.log(');');/* createSelectionManager */}}"


def back_button(name="back"):
    return visual(name, "actionButton", x=20, y=640, w=120, h=40, title=None, alt="Back", link={"type": "Back"})


def nav_button(name, target):
    return visual(name, "actionButton", x=160, y=640, w=120, h=40, title=None, alt="Open a page",
                  link={"type": "PageNavigation", "navigationSection": target})


def drill_button(name, x, target=None):
    link = {"type": "Drillthrough", **({"drillthroughSection": target} if target else {})}
    return visual(name, "actionButton", x=x, y=580, w=120, h=40, title=None, alt="Drill through", link=link)


def test_drillthrough():
    print("\n== drillthrough pages ==")
    level_expr = {"HierarchyLevel": {"Expression": {"Hierarchy": {"Expression": {"SourceRef": {"Entity": "Dim Store"}},
                                                                  "Hierarchy": "Geography"}}, "Level": "City"}}
    level = {"field": level_expr, "queryRef": "Dim Store.Geography.City"}
    counted = {"field": {"Aggregation": {"Expression": ref("Dim Product", "Brand"), "Function": 5}},
               "queryRef": "CountNonNull(Dim Product.Brand)"}
    detail = [visual("d", "tableEx", fields=CUSTOMER, x=20, y=20)]
    marked = page("markedonly", detail + [back_button()], visibility="HiddenInViewMode",
                  filterConfig={"filters": [flt("m1", "Dim Customer", "Name", how="Drillthrough")]})
    crossreport = drill_page("crossreport", [back_button()], [("Dim Supplier", "Name")])
    crossreport["doc"]["pageBinding"]["referenceScope"] = "CrossReport"
    good = drill_page("good", detail + [visual("segments", fields=[column("Dim Customer", "Segment")] + CARD, x=340, y=20),
                                        back_button()], [("Dim Customer", "Name")])
    # A filter that came along with a drillthrough is marked like one, but the binding names the fields.
    good["doc"]["filterConfig"]["filters"].append(flt("kept", "Dim Date", "Year", values=["2024"], how="Drillthrough"))
    summarized = drill_page("summarized", [back_button()], [("Dim Product", "Brand")])
    summarized["doc"]["pageBinding"]["parameters"][0]["asAggregation"] = True
    off = drill_button("switched_off", 580, "nowhere")
    off["visual"]["visualContainerObjects"]["visualLink"][0]["properties"]["show"] = lit("false")

    def channel(name):
        return drill_page(name, [visual("bychannel", fields=[column("Dim Channel", "Channel")] + CARD, x=20, y=20), back_button()],
                          [("Dim Channel", "Channel")])

    pages = [
        page("home", [visual("bycustomer", fields=CUSTOMER, x=20, y=20),
                      visual("byplace", "lineChart", fields=[level, measure("Sales", "Total Sales")], x=340, y=20),
                      visual("brands", "card", fields=[counted], x=660, y=20, w=200, h=100),
                      visual("years", "slicer", fields=[column("Dim Date", "Year")], x=880, y=20, w=200, h=60),
                      drill_button("to_good", 20, "good"), drill_button("to_nowhere", 160, "nowhere"),
                      drill_button("to_ordinary", 300, "home"), drill_button("no_destination", 440), off]),
        good,
        page("nofield", [back_button()], type="Drillthrough", visibility="HiddenInViewMode",
             pageBinding={"name": "empty", "type": "Drillthrough", "parameters": []}),
        drill_page("noback", detail, [("Dim Customer", "Name")]),
        drill_page("navonly", detail + [nav_button("tohome", "home")], [("Dim Customer", "Name")]),
        drill_page("hiddenback", detail + [dict(back_button(), isHidden=True)], [("Dim Customer", "Name")]),
        # The field is only on the drillthrough page itself, so nothing leads there.
        drill_page("orphan", [visual("own", fields=[column("Dim Product", "Colour")] + CARD, x=20, y=20), back_button()],
                   [("Dim Product", "Colour")]),
        # Used on a tooltip page only, and a tooltip is not something a reader can right-click.
        drill_page("viatip", [back_button()], [("Dim Region", "Region")]),
        # Another page uses a hierarchy of the same table: only the model knows its columns.
        drill_page("bystore", [back_button()], [("Dim Store", "City")]),
        # The drillthrough field is itself a hierarchy level: bound, and not for the files to judge.
        drill_page("bylevel", [back_button()], [level_expr]),
        # Home holds the year in a slicer only, and the brand only as a count.
        drill_page("viaslicer", [back_button()], [("Dim Date", "Year")]),
        drill_page("viacount", [back_button()], [("Dim Product", "Brand")]),
        # ... which is what a drillthrough on the summarized field asks for.
        summarized,
        drill_page("onmeasure", [back_button()], [ref("Sales", "Total Sales", "Measure")]),
        # Its field is only on 'good', a drillthrough page a reader can get to.
        drill_page("second", [back_button()], [("Dim Customer", "Segment")]),
        # Two drillthrough pages that lead to each other and that nothing else leads to.
        channel("pair_a"), channel("pair_b"),
        crossreport,
        marked,
        page("tip", [visual("t", "card", fields=[column("Dim Region", "Region")], x=10, y=10, w=200, h=80)],
             width=320, height=240, type="Tooltip", visibility="HiddenInViewMode"),
    ]
    root, _ = write_report(pages)
    r = ra.audit_report(root)
    eq("a drillthrough page with no field; one bound to a hierarchy level has a field",
       [(f["page"], f["severity"]) for f in codes(r, "DESIGN_DRILL_FIELD")], [("nofield", "warning")])
    eq("no Back button: a warning with no way out, a note beside other navigation, and a hidden button does not count",
       {f["page"]: f["severity"] for f in codes(r, "DESIGN_DRILL_BACK")},
       {"noback": "warning", "navonly": "info", "hiddenback": "warning"})
    eq("pages nothing leads to: the field is nowhere else, only on a tooltip page, only in a slicer, only summarized, or "
       "only on another page that is out of reach itself",
       sorted((f["page"], f["severity"]) for f in codes(r, "DESIGN_DRILL_UNREACHABLE")),
       [(p, "warning") for p in ("orphan", "pair_a", "pair_b", "viacount", "viaslicer", "viatip")])
    check("the unreachable field is named", has(r, "DESIGN_DRILL_UNREACHABLE", "Dim Product[Colour]", page="orphan"))
    eq("the report opens on an ordinary page", codes(r, "DESIGN_DRILL_LANDING"), [])
    eq("a hidden drillthrough page is not a hidden page nothing opens", codes(r, "DESIGN_HIDDEN_PAGE"), [])
    by_page = {d["page"]: d for d in r["summary"]["drillthrough_pages"]}
    eq("every drillthrough page is listed, however it is marked", sorted(by_page),
       sorted(["bylevel", "bystore", "crossreport", "good", "hiddenback", "markedonly", "navonly", "noback", "nofield",
               "onmeasure", "orphan", "pair_a", "pair_b", "second", "summarized", "viacount", "viaslicer", "viatip"]))
    eq("the good page in the summary", by_page["good"],
       {"page": "good", "displayName": "Good", "fields": ["Dim Customer[Name]"], "back_button": True, "hidden": True,
        "cross_report": False, "reachable": True})
    eq("reach: marked by its filter only, summarized as asked, a measure, through a reachable drillthrough page",
       [by_page[p]["reachable"] for p in ("markedonly", "summarized", "onmeasure", "second")], [True] * 4)
    eq("reach not judged: a hierarchy on the same table, a hierarchy level as the field, cross-report, no field",
       [by_page[p]["reachable"] for p in ("bystore", "bylevel", "crossreport", "nofield")], [None] * 4)
    eq("a hierarchy level is listed as the page's field", by_page["bylevel"]["fields"], ["Dim Store[Geography.City]"])
    eq("the correct drillthrough page has no finding at all", [f["code"] for f in r["findings"] if f["page"] == "good"], [])
    eq("drillthrough buttons: a page that is not there, a page that is not a drillthrough page, no destination; "
       "the good one and the one switched off give nothing",
       sorted((f["visual"], f["severity"]) for f in codes(r, "DESIGN_BROKEN_LINK") if f["page"] == "home"),
       [("no_destination", "warning"), ("to_nowhere", "error"), ("to_ordinary", "warning")])
    check("the missing page is named", has(r, "DESIGN_BROKEN_LINK", "drillthrough action points at 'nowhere', which is not a page"))
    check("the ordinary page is named", has(r, "DESIGN_BROKEN_LINK", "'home', which is a page but not a drillthrough page"))

    landed = codes(ra.audit_report(write_report(pages, active="good")[0]), "DESIGN_DRILL_LANDING")
    check("a drillthrough page that was open when the report was saved is where readers start, hidden or not",
          [(f["page"], f["severity"]) for f in landed] == [("good", "warning")] and "activePageName" in landed[0]["message"],
          landed)
    r = ra.audit_report(write_report(pages, active="good", landing="home")[0])
    eq("landingPageName overrides activePageName: an ordinary landing page is fine", codes(r, "DESIGN_DRILL_LANDING"), [])
    landed = codes(ra.audit_report(write_report(pages, landing="good")[0]), "DESIGN_DRILL_LANDING")
    check("a drillthrough page set as the landing page", [(f["page"], f["severity"]) for f in landed] == [("good", "warning")]
          and "landingPageName" in landed[0]["message"], landed)
    r = ra.audit_report(write_report(pages, landing="nopage")[0])
    check("a landingPageName that names no page is reported like a dangling activePageName",
          has(r, "STRUCT_PAGES", "landingPageName is 'nopage', which is not a page", "error") and not codes(r, "DESIGN_DRILL_LANDING"))


def test_tooltip_pages():
    print("\n== tooltip pages ==")

    def tip(name, w=320, h=240, hidden=True, **extra):
        if hidden:
            extra["visibility"] = "HiddenInViewMode"
        return page(name, [visual("tipcard", "card", fields=CARD, x=10, y=10, w=200, h=80)], width=w, height=h,
                    type="Tooltip", **extra)

    def chart(name, x, y):
        return visual(name, fields=SALES, x=x, y=y, w=200, h=150)

    # The tooltip type is written 'Canvas' by some writers and 'ReportPage' by others, and files
    # saved by Desktop often carry none: the verdict must be the same for all three.
    home = [
        tooltip(chart("uses_tip", 20, 20), "tip", show=True, kind="Canvas"),
        tooltip(chart("uses_tip_report_page", 240, 20), "tip_report_page", show=True, kind="ReportPage"),
        tooltip(chart("uses_tip_no_type", 460, 20), "tip_no_type", show=True),
        tooltip(chart("uses_big", 680, 20), "tipbig"),
        tooltip(chart("uses_visible", 900, 20), "tipvisible"),
        tooltip(chart("to_missing", 20, 190), "nopage", show=True),
        tooltip(chart("to_missing_canvas", 240, 190), "nopage", show=True, kind="Canvas"),
        tooltip(chart("to_missing_report_page", 460, 190), "nopage", show=True, kind="ReportPage"),
        tooltip(chart("to_ordinary", 680, 190), "other", show=True),
        tooltip(chart("automatic", 900, 190), "___AUTO___"),
        tooltip(chart("switched_off", 20, 360), "nopage", show=False),
        tooltip(chart("default_type", 240, 360), "nopage", show=True, kind="Default"),
        tooltip(chart("header_help", 460, 360), "tipheader", kind="Canvas", header=True),
    ]
    matched = {"name": "tb", "type": "Tooltip", "parameters": [{"name": "tp", "boundFilter": "tf",
                                                               "fieldExpr": ref("Dim Product", "Category")}]}
    pages = [
        page("home", home),
        page("other", [visual("o", fields=SALES, x=20, y=20)]),
        tip("tip"), tip("tip_report_page"), tip("tip_no_type"),
        tip("tipbig", w=1280, h=720),
        tip("tipvisible", hidden=False),
        tip("tipunused"),
        tip("tipheader"),
        # Exactly twice the tooltip size is still allowed; its fields let Power BI match it by itself.
        tip("tipmatched", w=640, h=480, pageBinding=matched, filterConfig={"filters": [flt("tf", "Dim Product", "Category")]}),
        # Marked by its binding only.
        page("tipbinding", [], width=320, height=240, visibility="HiddenInViewMode", pageBinding=dict(matched, name="tb2")),
        # An ordinary page filter is not a tooltip field: nothing shows this page.
        tip("tipfiltered", filterConfig={"filters": [flt("tf2", "Dim Product", "Category")]}),
    ]
    root, _ = write_report(pages)
    r = ra.audit_report(root)
    eq("a tooltip page that is not hidden", [(f["page"], f["severity"]) for f in codes(r, "DESIGN_TOOLTIP_VISIBLE")],
       [("tipvisible", "warning")])
    eq("a tooltip page the size of a report page", [(f["page"], f["severity"]) for f in codes(r, "DESIGN_TOOLTIP_SIZE")],
       [("tipbig", "warning")])
    eq("tooltip pages nothing shows: no visual names them and they have no tooltip fields",
       sorted((f["page"], f["severity"]) for f in codes(r, "DESIGN_TOOLTIP_UNUSED")),
       [("tipfiltered", "warning"), ("tipunused", "warning")])
    eq("tooltips that point at a missing page (whatever the type literal) and at an ordinary page",
       sorted((f["visual"], f["severity"]) for f in codes(r, "DESIGN_TOOLTIP_LINK")),
       [(v, "warning") for v in ("to_missing", "to_missing_canvas", "to_missing_report_page", "to_ordinary")])
    check("the missing page is named", has(r, "DESIGN_TOOLTIP_LINK", "'nopage', which is not a page", visual="to_missing"))
    check("the ordinary page is named", has(r, "DESIGN_TOOLTIP_LINK", "'Other', which is not a tooltip page", visual="to_ordinary"))
    by_page = {t["page"]: t for t in r["summary"]["tooltip_pages"]}
    eq("tooltip pages in the summary", sorted(by_page),
       sorted(["tip", "tip_no_type", "tip_report_page", "tipbig", "tipbinding", "tipfiltered", "tipheader", "tipmatched",
               "tipunused", "tipvisible"]))
    eq("the good tooltip page", by_page["tip"],
       {"page": "tip", "displayName": "Tip", "size": "320x240", "hidden": True, "used_by": 1, "auto_fields": []})
    eq("a page is used whichever way the visual writes the tooltip type",
       [by_page[p]["used_by"] for p in ("tip", "tip_report_page", "tip_no_type")], [1, 1, 1])
    eq("a header tooltip counts as use; tooltip fields are listed, a page filter is not one",
       (by_page["tipheader"]["used_by"], by_page["tipmatched"]["auto_fields"], by_page["tipfiltered"]["auto_fields"]),
       (1, ["Dim Product[Category]"], []))
    eq("the correct tooltip pages have no finding",
       [f["code"] for f in r["findings"] if f["page"] in ("tip", "tip_report_page", "tip_no_type", "tipheader", "tipmatched",
                                                          "tipbinding")], [])


def test_interactions():
    print("\n== visual interactions ==")

    def link(source, target, kind="NoFilter"):
        return {"source": source, "target": target, "type": kind}

    def visuals():
        return [visual("slicer", "slicer", fields=[column("Dim Date", "Year")], x=20, y=20, w=200, h=60),
                visual("a", fields=SALES, x=20, y=100),
                visual("b", "lineChart", fields=SALES, x=340, y=100),
                visual("c", "card", fields=CARD, x=660, y=100, w=200, h=100)]

    pages = [
        page("bad", visuals(), visualInteractions=[
            link("slicer", "a"), link("slicer", "b"), link("slicer", "c"),      # off for every data visual
            link("a", "deleted"), link("ghost", "b", "DataFilter"),             # visuals that are not on the page
            link("a", "a", "DataFilter"),                                       # a visual and itself
            link("a", "b", "DataFilter"), link("a", "b", "DataFilter"),         # twice, the same
            link("b", "a", "DataFilter"), link("b", "a", "NoFilter"),           # twice, and they disagree
            link("c", "a", "Filter"),                                           # not a type the schema has
        ]),
        page("good", visuals(), visualInteractions=[link("slicer", "a"), link("a", "b", "HighlightFilter"),
                                                    link("b", "a", "Default"), link("c", "b", "DataFilter")]),
    ]
    root, _ = write_report(pages)
    r = ra.audit_report(root)
    struct = codes(r, "STRUCT_INTERACTION")
    eq("three structural findings, all on the bad page and all warnings",
       sorted((f["page"], f["severity"]) for f in struct), [("bad", "warning")] * 3)
    check("visuals that are not on the page", has(r, "STRUCT_INTERACTION", "2 of 11 visual interaction(s) name a visual that is not on the page (deleted, ghost)"))
    check("a pair listed twice with different types", has(r, "STRUCT_INTERACTION", "b -> a (DataFilter, NoFilter)"))
    check("a type the schema does not have", has(r, "STRUCT_INTERACTION", "'Filter'"))
    notes = codes(r, "DESIGN_INTERACTION")
    eq("two notes on the bad page", sorted((f["page"], f["severity"]) for f in notes), [("bad", "info")] * 2)
    check("the same visual as source and target", has(r, "DESIGN_INTERACTION", "same visual as source and target (a)"))
    check("a pair listed twice with the same type", has(r, "DESIGN_INTERACTION", "a -> b"))
    eq("a slicer switched off for its whole page is a note",
       [(f["page"], f["visual"], f["severity"]) for f in codes(r, "DESIGN_INTERACTION_OFF")], [("bad", "slicer", "info")])
    eq("correct interactions give nothing", [f["code"] for f in r["findings"] if f["page"] == "good" and "INTERACTION" in f["code"]], [])
    eq("interactions in the summary", r["summary"]["visual_interactions"],
       {"total": 15, "pages": 2, "by_type": {"DataFilter": 6, "NoFilter": 6, "Filter": 1, "HighlightFilter": 1, "Default": 1}})


def test_filters():
    print("\n== filters ==")
    report_filters = {"filterConfig": {"filters": [
        flt("r_hidden", "Dim Region", "Region", values=["North"], hidden=True, locked=True),
        flt("r_ghost", "Ghost", "X", values=["a"]),
        flt("r_locked", "Dim Product", "Category", values=["Bikes"], locked=True),      # locked but visible
    ]}}
    chart = dict(visual("chart", fields=SALES, x=20, y=20), filterConfig={"filters": [
        flt("v_card", "Dim Product", "Category", hidden=True),                          # hides the card of a field it shows
        flt("v_dead", "Dim Date", "Year", hidden=True, how="User"),                     # filters nothing, seen by nobody
        flt("v_hidden", "Dim Product", "Brand", values=["Acme"], hidden=True, locked=True),
        {"field": ref("Dim Product", "Category"), "type": "Categorical"},               # no name: the schema requires one
    ]})
    card = dict(visual("card", "card", fields=CARD, x=660, y=20, w=200, h=100),
                filterConfig={"filters": [flt("v_ghost", "Ghost", "Y", values=["a"])]})
    home = page("home", [chart, visual("table", "tableEx", fields=SALES, x=340, y=20), card], filterConfig={"filters": [
        flt("p_dead", "Dim Date", "Year", hidden=True, how="User"),
        flt("p_open", "Dim Date", "Month"),                                             # an unset card readers can use
        flt("p_wrong", "Dim Product", "Colour", values=["Red"]),
        flt("twice", "Dim Date", "Year", values=["2024"]),
        flt("twice", "Dim Date", "Quarter", values=["Q1"]),
    ]})
    detail = drill_page("detail", [back_button(), visual("d", "tableEx", fields=SALES, x=20, y=20)], [("Dim Product", "Category")])
    # A drillthrough filter has no condition until someone drills through, hidden or not.
    detail["doc"]["filterConfig"]["filters"][0]["isHiddenInViewMode"] = True
    # The same name in another container is not a duplicate within one.
    detail["doc"]["filterConfig"]["filters"].append(flt("twice", "Dim Date", "Year", values=["2023"]))
    # The same holds for a filter that carries only the drillthrough mark, and for one that only a binding names.
    marked = page("marked", [back_button(), visual("m", "tableEx", fields=SALES, x=20, y=20)], visibility="HiddenInViewMode",
                  filterConfig={"filters": [flt("m_drill", "Dim Product", "Category", hidden=True, how="Drillthrough")]})
    matched = page("matched", [visual("t", "card", fields=CARD, x=10, y=10, w=200, h=80)], width=320, height=240, type="Tooltip",
                   visibility="HiddenInViewMode", filterConfig={"filters": [flt("t_bound", "Dim Product", "Category", hidden=True)]},
                   pageBinding={"name": "tb", "type": "Tooltip", "parameters": [{"name": "tp", "boundFilter": "t_bound",
                                                                                 "fieldExpr": ref("Dim Product", "Category")}]})
    root, _ = write_report([home, detail, marked, matched], report_extra=report_filters)
    model = {"Dim Product": ["Category", "Brand"], "Dim Date": ["Year", "Month", "Quarter"], "Dim Region": ["Region"],
             "Sales": ["Total Sales"]}
    r = ra.audit_report(root, model_fields=model)
    eq("a filter on a field that is not in the model, once per filter, at each level",
       sorted([(f["page"], f["visual"], f["severity"]) for f in codes(r, "STRUCT_FIELD")], key=str),
       sorted([(None, None, "error"), ("home", None, "error"), ("home", "card", "error")], key=str))
    check("the report-level one", has(r, "STRUCT_FIELD", "A report-level filter uses column Ghost[X]; the model has no table 'Ghost'"))
    check("the page-level one", has(r, "STRUCT_FIELD", "A page-level filter uses column Dim Product[Colour], which is not in the model", page="home"))
    eq("without model_fields no filter field is judged", codes(ra.audit_report(root), "STRUCT_FIELD"), [])
    hidden = codes(r, "GOV_HIDDEN_FILTER")
    eq("hidden filters that restrict the data are one governance note", [(f["severity"], f["page"]) for f in hidden], [("info", None)])
    eq("it lists the hidden ones and not the visible locked one",
       [(x["scope"], x["page"], x["visual"], x["filter"], x["locked"]) for x in hidden[0]["filters"]],
       [("report", None, None, "Dim Region[Region]", True), ("visual", "home", "chart", "Dim Product[Brand]", True)])
    dead = codes(r, "DESIGN_FILTER_DEAD")
    eq("hidden filters with no condition, on the page and on the visual",
       sorted([(f["page"], f["visual"], f["severity"]) for f in dead], key=str),
       sorted([("home", None, "info"), ("home", "chart", "info")], key=str))
    check("a hidden card on a field the visual shows is not dead weight",
          all("Dim Product[Category]" not in f["message"] for f in dead) and has(r, "DESIGN_FILTER_DEAD", "1 hidden filter(s) with no condition (Dim Date[Year])", visual="chart"))
    eq("two filters with one name in one container is a warning; a filter with no name, which the schema requires, an error",
       sorted([(f["page"], f["visual"], f["severity"]) for f in codes(r, "STRUCT_FILTER_NAME")], key=str),
       sorted([("home", None, "warning"), ("home", "chart", "error")], key=str))
    eq("filters in the summary", {k: v for k, v in r["summary"]["filters"].items() if k != "hidden_with_condition"},
       {"report": 3, "page": 9, "visual": 5, "hidden": 8, "locked": 3})
    eq("filters that drillthrough or a tooltip fills in give nothing, hidden as they are",
       [f["code"] for f in r["findings"] if f["page"] in ("detail", "marked", "matched")], [])


def test_mobile_layout():
    print("\n== phone layout ==")

    def placed(doc, x, y, w, h):
        return dict(doc, _mobile=phone(x, y, w, h))

    pages = [
        page("full", [placed(visual("kpi", "card", fields=CARD, x=20, y=20, w=200, h=100), 0, 0, 158, 100),
                      placed(visual("chart", fields=SALES, x=240, y=20), 0, 110, 323, 180),
                      # The canvas scrolls, so a visual far down is still on it.
                      placed(visual("far_down", "lineChart", fields=SALES, x=560, y=20), 0, 5000, 323, 400),
                      visual("rule", "shape", x=20, y=700, w=600, h=2, title=None, alt=None),   # decoration may stay out
                      visual("popup", fields=SALES, x=880, y=20, hidden=True)]),                # and so may a hidden visual
        page("partial", [placed(visual("kpi", "card", fields=CARD, x=20, y=20, w=200, h=100), 0, 0, 158, 100),
                         visual("left_out", fields=SALES, x=240, y=20),
                         visual("also_out", "tableEx", fields=SALES, x=560, y=20)]),
        page("broken", [placed(visual("too_wide", fields=SALES, x=20, y=20), 200, 0, 200, 100),
                        placed(visual("negative", fields=SALES, x=340, y=20), -10, 0, 100, 100),
                        placed(visual("flat", fields=SALES, x=660, y=20), 0, 200, 0, 100),
                        dict(visual("nowhere", fields=SALES, x=20, y=240),
                             _mobile={"$schema": SCHEMA.format("visualContainerMobileState", "2.3.0")}),
                        dict(visual("unreadable", fields=SALES, x=340, y=240), _mobile="{ not json")]),
        page("desktop_only", [visual("x", fields=SALES, x=20, y=20)]),
    ]
    root, _ = write_report(pages)
    r = ra.audit_report(root)
    partial = codes(r, "DESIGN_MOBILE_PARTIAL")
    eq("a page whose phone layout leaves data visuals out is a note",
       [(f["page"], f["severity"], sorted(f["visuals"])) for f in partial], [("partial", "info", ["also_out", "left_out"])])
    eq("positions outside the phone canvas are warnings; a mobile.json without its required position is an error",
       sorted((f["page"], f["visual"], f["severity"]) for f in codes(r, "STRUCT_MOBILE_POSITION")),
       [("broken", "flat", "warning"), ("broken", "negative", "warning"), ("broken", "nowhere", "error"),
        ("broken", "too_wide", "warning")])
    check("the width of the canvas is stated", has(r, "STRUCT_MOBILE_POSITION", "from 0 to 323 across", visual="too_wide"))
    check("a mobile.json that is not JSON", has(r, "STRUCT_JSON", "mobile.json is not valid JSON", "error", "broken", "unreadable"))
    eq("the phone layout in the summary", r["summary"]["mobile_layout"],
       {"pages": {"full": 3, "partial": 1, "broken": 5}, "visuals": 9, "canvas_width": 323.0})
    check("phone layout files count in the schema versions", r["summary"]["schema_versions"].get("visualContainerMobileState") == {"2.3.0": 8},
          r["summary"]["schema_versions"])
    eq("the complete layout has no finding", [f["code"] for f in r["findings"] if f["page"] == "full"], [])

    r = ra.audit_report(write_report(pages[3:] + clean_pages())[0])
    eq("a report with no phone layout at all is not a finding: the summary says it",
       ([f["code"] for f in r["findings"] if "MOBILE" in f["code"]], r["summary"]["mobile_layout"]["pages"]), ([], {}))


def test_bookmark_groups():
    print("\n== bookmark groups ==")
    marks = [bookmark(n, "home") for n in ("one", "two", "three", "four", "five")]
    items = [{"name": "one"}, {"name": "five"},
             {"name": "g_views", "displayName": "Views", "children": ["two", "three"]},
             {"name": "g_empty", "displayName": "Nothing", "children": []},
             {"name": "g_stale", "displayName": "Old", "children": ["four", "gone"]},
             {"name": "g_again", "displayName": "Again", "children": ["three", "one"]}]
    root, _ = write_report(clean_pages(), bookmarks=marks, bookmark_items=items)
    r = ra.audit_report(root)
    check("a group that lists a bookmark with no file", has(r, "DESIGN_BROKEN_LINK", "The bookmark group 'Old' lists 'gone', which has no bookmark file", "error"))
    groups = codes(r, "STRUCT_BOOKMARK_GROUP")
    eq("an empty group is a note; a bookmark listed twice is a warning",
       sorted((f["severity"], f["message"].split(". ")[0].rstrip(".")) for f in groups),
       [("info", "The bookmark group 'Nothing' holds no bookmark"),
        ("warning", "bookmarks.json lists the bookmark 'one' 2 times"),
        ("warning", "bookmarks.json lists the bookmark 'three' 2 times")])
    eq("the groups in the summary", [(g["name"], g["displayName"], g["bookmarks"]) for g in r["summary"]["bookmark_groups"]],
       [("g_views", "Views", ["two", "three"]), ("g_empty", "Nothing", []), ("g_stale", "Old", ["four", "gone"]),
        ("g_again", "Again", ["three", "one"])])

    named = [{"name": "one"},
             {"name": "g_a", "displayName": "A", "children": ["two"]},
             {"name": "g_a", "displayName": "A again", "children": ["three"]},       # two groups, one name
             {"name": "four", "displayName": "Like a bookmark", "children": ["four"]},  # a group named like a bookmark
             {"name": "g_plain", "children": ["five"]},                              # no displayName: the schema requires it
             {}]                                                                     # neither a bookmark nor a group
    r = ra.audit_report(write_report(clean_pages(), bookmarks=marks, bookmark_items=named)[0])
    eq("names: a required one that is missing is an error, a group name used twice a warning, one shared with a bookmark a note",
       sorted((f["severity"], f["message"].split(". ")[0].split(";")[0].split(",")[0].rstrip(".")) for f in codes(r, "STRUCT_BOOKMARK_GROUP")),
       [("error", "An entry of bookmarks.json has neither a name nor children"),
        ("error", "The bookmark group 'g_plain' has no displayName"),
        ("info", "A bookmark group and a bookmark are both named 'four'"),
        ("warning", "2 bookmark groups have the name 'g_a'")])

    root, _ = write_report(clean_pages(), bookmarks=marks[:4], bookmark_items=[items[0], items[2], {"name": "four"}])
    r = ra.audit_report(root)
    eq("a correct group gives nothing", codes(r, "STRUCT_BOOKMARK_GROUP") + codes(r, "DESIGN_BROKEN_LINK"), [])


def test_custom_visual_capabilities():
    print("\n== what private custom visuals can do ==")

    def custom(name, vtype, x=20, y=20, category=True):
        """A custom visual with a measure in its 'values' role and, unless told not to, a column in 'category'."""
        doc = visual(name, vtype, x=x, y=y)
        state = {"values": {"projections": CARD}}
        if category:
            state["category"] = {"projections": [column("Dim Product", "Category")]}
        doc["visual"]["query"] = {"queryState": state}
        return doc

    pages = [
        page("busy", [visual("chart", fields=SALES, x=20, y=20), custom("full1", FULL, 340), custom("plain1", PLAIN, 660),
                      custom("menu1", MENU, 980), custom("pick1", PICK, 20, 240),
                      custom("kit1", KIT, 340, 240, category=False), custom("mute1", MUTE, 660, 240, category=False)]),
        # The same display-only visual with nothing but a measure in it: no data point, so nothing to click.
        page("measure_only", [visual("chart", fields=SALES, x=20, y=20), custom("plain2", PLAIN, 340, category=False)]),
        page("alone", [custom("plain3", PLAIN), nav_button("tobusy", "busy")]),
        page("twins", [custom("plain4", PLAIN), custom("plain5", PLAIN, 340)]),
        page("scripts", [visual("chart", fields=SALES, x=20, y=20), custom("noise1", NOISE, 340), custom("keyed1", KEYED, 660),
                         custom("assembled1", ASSEMBLED, 980), custom("unsplit1", UNSPLIT, 20, 240, category=False)]),
        page("unknown", [visual("chart", fields=SALES, x=20, y=20), custom("bare1", BARE, 340), custom("broken1", BROKEN, 660),
                         custom("old1", OLD, 980)]),
    ]
    packages = {FULL: pbiviz(FULL, FULL_CAPS, FULL_JS), PLAIN: pbiviz(PLAIN, PLAIN_CAPS, MUTE_JS),
                MENU: pbiviz(MENU, PLAIN_CAPS, MENU_JS), PICK: pbiviz(PICK, dict(PLAIN_CAPS, supportsHighlight=True), PICK_JS),
                # Measures only: one built on a kit that names the whole selection API, one that only draws.
                KIT: pbiviz(KIT, MEASURE_CAPS, FULL_JS), MUTE: pbiviz(MUTE, {"dataRoles": MEASURES, "privileges": []}, MUTE_JS),
                NOISE: pbiviz(NOISE, FULL_CAPS, NOISE_JS), KEYED: pbiviz(KEYED, PLAIN_CAPS, KEYED_JS),
                ASSEMBLED: pbiviz(ASSEMBLED, PLAIN_CAPS, ASSEMBLED_JS), UNSPLIT: pbiviz(UNSPLIT, PLAIN_CAPS, UNSPLIT_JS),
                OLD: pbiviz(OLD, {"dataRoles": []}, MUTE_JS, api="3.8.0"),
                BROKEN: "{ not json", SPARE: pbiviz(SPARE, PLAIN_CAPS, MUTE_JS)}
    root, _ = write_report(pages, private=[BARE], packages=packages)
    r = ra.audit_report(root)
    caps = {c["guid"]: c for c in r["summary"]["custom_visual_capabilities"]}
    eq("every package in the report folder is read", sorted(caps), sorted(list(packages) + [BARE]))
    eq("what the full visual declares", caps[FULL]["declares"],
       {"highlight": True, "tooltips": True, "report_page_tooltips": True, "drilldown": True, "keyboard_focus": True,
        "landing_page": True, "multi_visual_selection": True, "sync_slicers": False})
    eq("what its script calls", caps[FULL]["script"],
       {"selection_manager": True, "selection_ids": True, "filter_api": False, "context_menu": True, "tooltip_service": True,
        "reads_highlights": True})
    eq("what follows", caps[FULL]["behaviour"],
       {"cross_filter": True, "cross_highlight": True, "drillthrough": True, "tooltips": True, "report_page_tooltips": True})
    eq("its data roles, whether the kind is a name or a number, and the ones that take a column",
       (caps[FULL]["data_roles"], caps[FULL]["column_roles"]), ({"category": "Grouping", "values": "Measure"}, ["category"]))
    eq("its privileges", [(p["name"], p["essential"], p["parameters"]) for p in caps[FULL]["privileges"]],
       [("WebAccess", True, ["https://*.example.com"]), ("LocalStorage", False, []), ("ExportContent", True, [])])
    eq("identity and use", (caps[FULL]["name"], caps[FULL]["version"], caps[FULL]["api_version"], caps[FULL]["visuals"], caps[PLAIN]["visuals"], caps[SPARE]["visuals"]),
       (FULL.title(), "1.0.0.0", "5.11.0", 1, 5, 0))
    eq("a visual that takes only measures has nothing to select, though its script names the whole selection API",
       (caps[KIT]["column_roles"], caps[KIT]["script"]["selection_manager"], caps[KIT]["script"]["selection_ids"],
        caps[KIT]["script"]["context_menu"], caps[KIT]["behaviour"]),
       ([], True, True, True, {"cross_filter": False, "cross_highlight": True, "drillthrough": False, "tooltips": True,
                               "report_page_tooltips": True}))
    eq("a visual that only draws", (caps[PLAIN]["declares"]["landing_page"], sorted(set(caps[PLAIN]["script"].values())), sorted(set(caps[PLAIN]["behaviour"].values()))),
       (True, [False], [False]))
    eq("a context menu without a data point is not drillthrough",
       (caps[MENU]["script"]["context_menu"], caps[MENU]["script"]["selection_ids"], caps[MENU]["behaviour"]["drillthrough"], caps[MENU]["behaviour"]["cross_filter"]),
       (True, False, False, False))
    eq("the filter API filters without a selection", (caps[PICK]["script"]["filter_api"], caps[PICK]["behaviour"]["cross_filter"]), (True, True))
    eq("highlight data that the script never reads highlights nothing",
       (caps[PICK]["declares"]["highlight"], caps[PICK]["script"]["reads_highlights"], caps[PICK]["behaviour"]["cross_highlight"]),
       (True, False, False))
    eq("a name in a comment or in a string is not a call", (sorted(set(caps[NOISE]["script"].values())), caps[NOISE]["script_read"]),
       ([False], "code"))
    eq("a quoted name in brackets is a call, and a regular expression in front of it does not hide it",
       (caps[KEYED]["script"]["selection_manager"], caps[KEYED]["script"]["selection_ids"], caps[KEYED]["behaviour"]["cross_filter"],
        caps[KEYED]["script_read"]), (True, True, True, "code"))
    eq("names put together at run time are not found, and that is known",
       (sorted(set(caps[ASSEMBLED]["script"].values())), caps[ASSEMBLED]["assembled_calls"], caps[KEYED]["assembled_calls"]),
       ([False], True, False))
    eq("a script that cannot be split into code and text is searched whole",
       (caps[UNSPLIT]["script_read"], caps[UNSPLIT]["script"]["selection_manager"]), ("text", True))
    check("a package with no metadata file cannot be judged", caps[BARE]["behaviour"] is None and "no resources" in caps[BARE]["unread"], caps[BARE])
    check("nor one whose metadata is not JSON, which is an error of its own",
          caps[BROKEN]["behaviour"] is None and has(r, "STRUCT_JSON", BROKEN + ".pbiviz.json is not valid JSON", "error"))
    eq("privileges that are not declared", caps[OLD]["privileges"], None)

    notes = codes(r, "INTERACT_CAPABILITIES")
    eq("one note per package in use, none for the spare one", sorted((f["package"], f["severity"]) for f in notes),
       sorted((g, "info") for g in list(packages) + [BARE] if g != SPARE))
    by_package = {f["package"]: f["message"] for f in notes}
    check("the note states what a click does", "a click can filter other visuals" in by_package[FULL]
          and "other visuals can highlight it" in by_package[FULL] and "drillthrough from its context menu is possible" in by_package[FULL]
          and "Power BI tooltips and report page tooltips" in by_package[FULL], by_package[FULL])
    check("and the data roles and the privileges", "category (Grouping), values (Measure)" in by_package[FULL]
          and "WebAccess (https://*.example.com); LocalStorage, optional; ExportContent" in by_package[FULL], by_package[FULL])
    check("and the opposite", "a click cannot filter other visuals" in by_package[PLAIN] and "Privileges requested: none" in by_package[PLAIN]
          and "shows no Power BI tooltips" in by_package[PLAIN], by_package[PLAIN])
    check("a visual that takes only measures is told so, and not that a click filters",
          "it takes only measures" in by_package[KIT] and "a click can filter" not in by_package[KIT], by_package[KIT])
    check("an old API needs no declaration for web access", "before 4.6.0" in by_package[OLD], by_package[OLD])
    check("what cannot be read is said", "could not be read" in by_package[BARE] and "could not be read" in by_package[BROKEN])
    check("so is a script read whole, and one that assembles names",
          "could not be split" in by_package[UNSPLIT] and "put together at run time" in by_package[ASSEMBLED])

    silent = codes(r, "INTERACT_NO_SELECTION")
    eq("clicks that will not filter the page: a visual with data points, no selection for them, and other data visuals "
       "beside it; one finding per page and visual type",
       sorted([(f["page"], f["visual"], f["severity"], f["visuals"]) for f in silent], key=str),
       sorted([("busy", "plain1", "warning", ["plain1"]), ("busy", "menu1", "warning", ["menu1"]),
               ("twins", None, "warning", ["plain4", "plain5"]), ("scripts", "noise1", "warning", ["noise1"])], key=str))
    check("nothing is said of a visual that takes only measures, of one with only a measure in it, of one on its own, "
          "or of one that cannot be judged",
          not any(set(f["visuals"]) & {"kit1", "mute1", "plain2", "plain3", "assembled1", "unsplit1", "bare1", "broken1", "old1"}
                  for f in silent))
    menus = {f["package"]: f for f in codes(r, "INTERACT_NO_CONTEXT_MENU")}
    eq("no drillthrough is a note per visual type that shows data points",
       sorted((g, f["severity"], f["pages"]) for g, f in menus.items()),
       sorted([(PLAIN, "info", ["busy", "measure_only", "alone", "twins"]), (MENU, "info", ["busy"]), (PICK, "info", ["busy"]),
               (NOISE, "info", ["scripts"]), (KEYED, "info", ["scripts"]), (UNSPLIT, "info", ["scripts"])]))
    check("a menu with no data point is told apart from no menu",
          "builds no selection id" in menus[MENU]["message"] and "never opens the context menu" in menus[PLAIN]["message"])


def test_store_visual_interactivity():
    print("\n== Deneb and HTML Content ==")

    def deneb(name, x, **switches):
        doc = visual(name, DENEB, x=x, y=20)
        doc["visual"]["query"] = {"queryState": {"dataset": {"projections": SALES}}}
        doc["visual"]["objects"]["vega"] = [{"properties": {"jsonSpec": lit("'{}'"), "selectionMode": lit("'simple'"),
                                                            **{k: lit("true" if v else "false") for k, v in switches.items()}}}]
        return doc

    def html(name, vtype, x, context=None, tips=None, enabled=None):
        doc = visual(name, vtype, x=x, y=300, w=190)
        state = {"content": {"projections": [measure("M", "Page")]}}
        if context:
            state["sampling"] = {"projections": context}
        if tips:
            state["tooltips"] = {"projections": tips}
        doc["visual"]["query"] = {"queryState": state}
        if enabled is not None:
            doc["visual"]["objects"]["crossFilter"] = [{"properties": {"enabled": lit("true" if enabled else "false")}}]
        return doc

    category = [column("Dim Product", "Category")]
    level = [{"field": {"HierarchyLevel": {"Expression": {"Hierarchy": {"Expression": {"SourceRef": {"Entity": "Dim Store"}},
                                                                         "Hierarchy": "Geography"}}, "Level": "City"}},
              "queryRef": "Dim Store.Geography.City"}]
    home = page("home", [
        deneb("d_default", 20),                                                  # nothing stored: Deneb's defaults
        deneb("d_on", 340, enableSelection=True, enableHighlight=True),
        deneb("d_off", 660, enableTooltips=False, enableContextMenu=False),
        html("h_measure", HTML_SECURE, 20),                                      # one measure, no row to select
        html("h_filter", HTML_REGULAR, 220, context=category, enabled=True),
        html("h_not_enabled", HTML_REGULAR, 420, context=category),
        html("h_measure_context", HTML_SECURE, 620, context=CARD, enabled=True),  # Enable needs a column in Context
        html("h_tips", HTML_SECURE, 820, tips=CARD),
        html("h_level", HTML_SECURE, 1020, context=level, enabled=True),          # a hierarchy level stands for a column
    ])
    root, _ = write_report([home], public=[DENEB, HTML_SECURE, HTML_REGULAR])
    r = ra.audit_report(root)
    store = r["summary"]["store_visual_interactivity"]
    switches = ("enableSelection", "enableHighlight", "enableTooltips", "enableContextMenu", "enableContextMenuSelector")
    eq("Deneb's switches, stored or at their documented default",
       {d["visual"]: tuple(d[s] for s in switches) for d in store["deneb"]["items"]},
       {"d_default": (False, False, True, True, True), "d_on": (True, True, True, True, True),
        "d_off": (False, False, False, False, True)})
    for fragment in ("Deneb is used 3 time(s), 3 of them in the AppSource edition, which is certified", "(enableSelection) in 1",
                     "(enableHighlight) in 1", "(enableTooltips) in 2", "(enableContextMenu) in 2",
                     "(enableContextMenuSelector) in 2", "Cross-filtering is off in: home / d_default, home / d_off"):
        check(f"the Deneb note says: {fragment}", has(r, "INTERACT_DENEB", fragment, "info"))
    eq("HTML Content, per visual: edition, certified, runs scripts, cross-filter, context menu, tooltips",
       {h["visual"]: (h["edition"], h["certified"], h["runs_scripts"], h["cross_filter"], h["context_menu"], h["tooltips"])
        for h in store["html_content"]["items"]},
       {"h_measure": ("Secure", True, False, False, False, False), "h_filter": ("Regular", False, True, True, True, True),
        "h_not_enabled": ("Regular", False, True, False, True, True),
        "h_measure_context": ("Secure", True, False, False, True, True), "h_tips": ("Secure", True, False, False, False, True),
        "h_level": ("Secure", True, False, True, True, True)})
    for fragment in ("HTML Content is used 6 time(s) (Regular edition, not certified: 2; Secure edition, certified: 4",
                     "under cross-filtering: 2 of 6", "fields in Context: 4 of 6", "do not show): 5 of 6",
                     "The Secure edition removes scripts and inline event handlers from the HTML; the Regular edition runs them"):
        check(f"the HTML Content note says: {fragment}", has(r, "INTERACT_HTML_CONTENT", fragment, "info"))
    eq("both are notes, and nothing here is a warning about clicks", codes(r, "INTERACT_NO_SELECTION"), [])
    check("each names where its facts come from", "deneb.guide" in store["deneb"]["source"] and "html-content.com" in store["html_content"]["source"])
    eq("certification as the vendors' documentation states it: Deneb and HTML Content Secure are certified, Regular is not",
       (r["summary"]["certification"], [d["certified"] for d in store["deneb"]["items"]]),
       ({"certified": [DENEB, HTML_SECURE], "not_certified": [HTML_REGULAR], "unknown": []}, [True, True, True]))
    check("the store note says which is which, and that nothing is left to look up",
          has(r, "GOV_PUBLIC_VISUAL", DENEB + " (3, certified)") and has(r, "GOV_PUBLIC_VISUAL", HTML_SECURE + " (4, certified)")
          and has(r, "GOV_PUBLIC_VISUAL", HTML_REGULAR + " (2, not certified)") and not has(r, "GOV_PUBLIC_VISUAL", "cannot be read"))
    check("script-capable HTML is the Regular edition only",
          has(r, "GOV_HTML_VISUAL", "HTML Content Regular (the id ending 6855) is not certified and also runs the scripts")
          and has(r, "GOV_HTML_VISUAL", "HTML Content Secure (the id ending 6865) is certified and removes scripts"))

    secure_only = page("home", [html("h1", HTML_SECURE, 20)])
    r = ra.audit_report(write_report([secure_only], public=[HTML_SECURE])[0])
    check("a report on the Secure edition alone is not told that its HTML runs scripts",
          not has(r, "GOV_HTML_VISUAL", "runs") and not has(r, "INTERACT_HTML_CONTENT", "runs")
          and has(r, "INTERACT_HTML_CONTENT", "The Secure edition removes scripts and inline event handlers from the HTML.")
          and r["summary"]["certification"]["not_certified"] == [],
          [f["message"] for f in codes(r, "GOV_HTML_VISUAL") + codes(r, "INTERACT_HTML_CONTENT")])
    r = ra.audit_report(write_report([page("home", [html("h1", HTML_REGULAR, 20)])], public=[HTML_REGULAR])[0])
    check("a report on the Regular edition alone is told that it does, and that the edition is not certified",
          has(r, "INTERACT_HTML_CONTENT", "The Regular edition runs the scripts and inline event handlers in the HTML.")
          and has(r, "GOV_HTML_VISUAL", "is not certified and also runs") and not has(r, "GOV_HTML_VISUAL", "Secure")
          and r["summary"]["certification"] == {"certified": [], "not_certified": [HTML_REGULAR], "unknown": []})

    plain = ra.audit_report(write_report(clean_pages())[0])
    eq("a report with neither says nothing about them",
       ([f["code"] for f in plain["findings"] if f["code"].startswith("INTERACT_")],
        plain["summary"]["store_visual_interactivity"]["deneb"]["items"], plain["summary"]["store_visual_interactivity"]["html_content"]["items"]),
       ([], [], []))


def test_path_length():
    print("\n== path length ==")
    root, d = write_report(clean_pages())
    base = len(os.path.abspath(root))
    check("the temp folder leaves room to build paths of an exact length", base < 180, base)
    listing, on_windows = ra._list_paths, ra._ON_WINDOWS

    def below(length, name=""):
        """A path below the report folder whose full length is exactly ``length``."""
        head = "StaticResources/RegisteredResources/"
        return head + "x" * (length - base - 1 - len(head) - len(name)) + name

    def audit(entries, windows=True):
        """Audit the report as if its folders held ``entries``, on Windows or elsewhere."""
        ra._list_paths, ra._ON_WINDOWS = (lambda report_dir, definition: iter(entries)), windows
        try:
            return ra.audit_report(root)
        finally:
            ra._list_paths, ra._ON_WINDOWS = listing, on_windows

    short = [("definition", True), (below(247), True), (below(259, ".png"), False)]
    r = audit(short)
    eq("259 and 247 characters are the longest Windows takes", codes(r, "STRUCT_PATH_LENGTH"), [])
    eq("and the summary says how close the report is", r["summary"]["path_lengths"],
       {"longest_file": 259, "longest_folder": 247, "file_limit": 260, "folder_limit": 248, "too_long": [], "judged": True})

    r = audit(short + [(below(260, ".png"), False)])
    found = codes(r, "STRUCT_PATH_LENGTH")
    eq("a file path of 260 characters is an error", [(f["severity"], f["page"], f["paths"]) for f in found],
       [("error", None, [below(260, ".png")])])
    check("it counts what is too long", "1 file(s) and 0 folder(s)" in found[0]["message"] and "260 and 247" in found[0]["message"], found[0]["message"])

    r = audit(short + [(below(248), True), (below(255, ".json"), False)])
    found = codes(r, "STRUCT_PATH_LENGTH")
    check("a folder path of 248 characters is one too, though the file in it is under 260",
          len(found) == 1 and "0 file(s) and 1 folder(s)" in found[0]["message"] and found[0]["paths"] == [below(248) + "/"],
          [f["message"] for f in found])
    eq("the rest of the report is still audited", r["summary"]["pages"], 2)

    r = audit(short + [(below(300, ".png"), False), (below(280), True)], windows=False)
    eq("the limits are Windows' own: off Windows the lengths are reported and not judged",
       (codes(r, "STRUCT_PATH_LENGTH"), r["summary"]["path_lengths"]["too_long"], r["summary"]["path_lengths"]["judged"],
        r["summary"]["path_lengths"]["longest_file"]), ([], [], False, 300))

    listed = sorted(ra._list_paths(Path(root), Path(d)))
    check("what is listed: the folders and files of the report, by their path below it",
          ("definition", True) in listed and ("definition/report.json", False) in listed
          and ("definition/pages/home/visuals/chart/visual.json", False) in listed and ("definition/pages/home", True) in listed,
          listed[:6])

    # A link back to its own parent folder. Followed, it lists the same files again and again
    # under ever longer paths, until a report with short paths looks like one that is too long.
    visuals = os.path.join(root, "CustomVisuals")
    dump(os.path.join(visuals, "someVisual", "package.json"), {})
    loop = os.path.join(visuals, "loop")
    try:
        if os.name == "nt":
            subprocess.run(["cmd", "/c", "mklink", "/J", loop, visuals], check=True, capture_output=True)
        else:
            os.symlink(visuals, loop, target_is_directory=True)
    except (OSError, subprocess.CalledProcessError) as e:
        print(f"  [SKIP] a link or junction cannot be made here, so the loop is not tried: {e}")
    else:
        listed = sorted(ra._list_paths(Path(root), Path(d)))
        r = ra.audit_report(root)
        check("a link or junction that loops back is not followed",
              not any("loop" in path for path, _ in listed) and ("CustomVisuals/someVisual/package.json", False) in listed
              and not codes(r, "STRUCT_PATH_LENGTH") and r["summary"]["path_lengths"]["longest_file"] < 220,
              (r["summary"]["path_lengths"], [f["message"] for f in codes(r, "STRUCT_PATH_LENGTH")]))
        # Take the link away again (the folder it points at stays), so the temp folder holds no loop.
        os.unlink(loop) if os.path.islink(loop) else os.rmdir(loop)
    if os.name == "nt":
        spelled = ra.audit_report("\\\\?\\" + os.path.abspath(root))["summary"]["path_lengths"]
        eq("a report given in the extended-length spelling is measured as the same paths",
           (spelled["longest_file"], spelled["longest_folder"]),
           tuple(ra.audit_report(root)["summary"]["path_lengths"][k] for k in ("longest_file", "longest_folder")))


def good_report():
    """A report that uses every native feature the audit reads, and uses each one correctly."""
    def placed(doc, y, h):
        return dict(doc, _mobile=phone(0, y, 323, h))

    def with_query(doc, state, objects):
        doc["visual"]["query"] = {"queryState": state}
        doc["visual"]["objects"].update(objects)
        return doc

    deneb = with_query(visual("deneb", DENEB, x=20, y=320), {"dataset": {"projections": SALES}},
                       {"vega": [{"properties": {"jsonSpec": lit("'{}'"), "enableSelection": lit("true"), "enableHighlight": lit("true")}}]})
    html = with_query(visual("html", HTML_REGULAR, x=340, y=320),
                      {"content": {"projections": [measure("M", "Page")]}, "sampling": {"projections": [column("Dim Product", "Category")]}},
                      {"crossFilter": [{"properties": {"enabled": lit("true")}}]})
    custom = with_query(visual("custom", FULL, x=660, y=100),
                        {"category": {"projections": [column("Dim Product", "Category")]}, "values": {"projections": CARD}}, {})
    home = page("home", [
        placed(visual("year", "slicer", fields=[column("Dim Date", "Year")], x=20, y=20, w=200, h=60), 0, 60),
        placed(tooltip(visual("chart", fields=SALES, x=20, y=100), "tip", show=True, kind="Canvas"), 70, 180),
        placed(visual("table", "tableEx", fields=CUSTOMER, x=340, y=100), 260, 270),
        placed(custom, 540, 180),
        placed(deneb, 730, 180),
        placed(html, 920, 180),
        visual("views", "bookmarkNavigator", x=660, y=320, w=300, h=40, title=None, alt="Saved views"),
        visual("details", "actionButton", x=660, y=380, w=120, h=40, title=None, alt="Customer details",
               link={"type": "Drillthrough", "drillthroughSection": "detail"}),
    ], visualInteractions=[{"source": "year", "target": "table", "type": "NoFilter"},
                           {"source": "chart", "target": "table", "type": "HighlightFilter"}],
        filterConfig={"filters": [flt("p_region", "Dim Region", "Region")]})
    home["visuals"][-2]["visual"]["objects"]["bookmarks"] = [{"properties": {"bookmarkGroup": lit("'g_views'")}}]
    detail = drill_page("detail", [visual("orders", "tableEx", fields=CUSTOMER, x=20, y=20), back_button()], [("Dim Customer", "Name")])
    tip = page("tip", [visual("tipcard", "card", fields=CARD, x=10, y=10, w=200, h=80)], width=320, height=240, type="Tooltip",
               visibility="HiddenInViewMode")
    return dict(
        pages=[home, detail, tip], landing="home",
        bookmarks=[bookmark("all", "home"), bookmark("top", "home")],
        bookmark_items=[{"name": "g_views", "displayName": "Views", "children": ["all", "top"]}],
        public=[DENEB, HTML_REGULAR], packages={FULL: pbiviz(FULL, FULL_CAPS, FULL_JS)},
        report_extra={"filterConfig": {"filters": [flt("r_scope", "Dim Region", "Region", values=["North"], hidden=True, locked=True)]}})


def test_every_feature_used_correctly():
    print("\n== an ordinary good report that uses every feature ==")
    root, _ = write_report(**good_report())
    before = snapshot(root)
    model = {"Dim Date": ["Year"], "Dim Product": ["Category"], "Dim Customer": ["Name"], "Dim Region": ["Region"],
             "Sales": ["Total Sales"], "M": ["Page"]}
    r = ra.audit_report(root, model_fields=model)
    eq("no warning and no error", [(f["code"], f["page"], f["visual"], f["message"]) for f in r["findings"] if f["severity"] != "info"], [])
    eq("what is left are facts for whoever governs the tenant and for whoever builds the visuals",
       sorted(f["code"] for f in r["findings"]),
       ["GOV_HIDDEN_FILTER", "GOV_HTML_VISUAL", "GOV_PRIVATE_VISUAL", "GOV_PUBLIC_VISUAL", "INTERACT_CAPABILITIES",
        "INTERACT_DENEB", "INTERACT_HTML_CONTENT"])
    eq("the audit changes no file", snapshot(root), before)
    s = r["summary"]
    eq("every feature was seen",
       ([d["page"] for d in s["drillthrough_pages"]], [t["page"] for t in s["tooltip_pages"]], s["filters"]["report"], s["filters"]["page"],
        s["visual_interactions"]["total"], s["mobile_layout"]["pages"], [g["name"] for g in s["bookmark_groups"]],
        [c["guid"] for c in s["custom_visual_capabilities"]], s["store_visual_interactivity"]["deneb"]["visuals"],
        s["store_visual_interactivity"]["html_content"]["visuals"], s["field_check"]),
       (["detail"], ["tip"], 1, 2, 2, {"home": 6}, ["g_views"], [FULL], 1, 1, "done"))
    eq("and every part of it is in order",
       (s["drillthrough_pages"][0]["reachable"], s["drillthrough_pages"][0]["back_button"], s["tooltip_pages"][0]["used_by"],
        s["custom_visual_capabilities"][0]["behaviour"]["cross_filter"], s["store_visual_interactivity"]["html_content"]["items"][0]["cross_filter"],
        s["store_visual_interactivity"]["deneb"]["items"][0]["enableSelection"]), (True, True, 1, True, True, True))
    eq("who is certified", s["certification"], {"certified": [DENEB], "not_certified": sorted([FULL, HTML_REGULAR]), "unknown": []})


def test_hostile_input():
    print("\n== hostile files and arguments are reported or refused, never raised from inside ==")
    # Far deeper than any Python version's parser goes, whatever its limit.
    deep = '{"a":' * 100000 + "1" + "}" * 100000

    def audit(change=None, **arguments):
        """Audit a small report after ``change(report folder)``; the result, or the exception as text."""
        pages = clean_pages()
        pages[0]["visuals"][0]["_mobile"] = phone(0, 0, 300, 200)
        pages[0]["visuals"].append(visual("cv", FULL, fields=SALES, x=660, y=20))
        root, _ = write_report(pages, packages={FULL: pbiviz(FULL, FULL_CAPS, FULL_JS)}, bookmarks=[bookmark("b", "home")])
        if change:
            change(root)
        try:
            return ra.audit_report(root, **arguments)
        except Exception as e:  # noqa: BLE001
            return f"{type(e).__name__}: {e}"

    def overwrite(*parts, text):
        def change(root):
            with open(os.path.join(root, *parts), "w", encoding="utf-8") as f:
                f.write(text)
        return change

    def edit(*parts, old, new):
        def change(root):
            path = os.path.join(root, *parts)
            with open(path, encoding="utf-8") as f:
                text = f.read()
            assert old in text, (old, path)
            with open(path, "w", encoding="utf-8") as f:
                f.write(text.replace(old, new))
        return change

    package = ("CustomVisuals", FULL, "resources", FULL + ".pbiviz.json")
    chart = ("definition", "pages", "home", "visuals", "chart")
    for label, parts in (("report.json", ("definition", "report.json")), ("pages.json", ("definition", "pages", "pages.json")),
                         ("page.json", ("definition", "pages", "home", "page.json")), ("visual.json", chart + ("visual.json",)),
                         ("mobile.json", chart + ("mobile.json",)), ("bookmarks.json", ("definition", "bookmarks", "bookmarks.json")),
                         ("a bookmark", ("definition", "bookmarks", "b.bookmark.json")), ("a custom visual's metadata", package)):
        r = audit(overwrite(*parts, text=deep))
        check(f"{label} nested 100,000 deep is a file that cannot be read, not a crash",
              isinstance(r, dict) and has(r, "STRUCT_JSON", "nested too deeply", "error"), r if isinstance(r, str) else "no STRUCT_JSON")
    for value in ("NaN", "Infinity", "-Infinity", "1e999"):
        r = audit(edit(*package, old='"kind": "Grouping"', new='"kind": ' + value))
        check(f"a data role kind of {value} is reported as invalid JSON and the audit carries on",
              isinstance(r, dict) and has(r, "STRUCT_JSON", FULL + ".pbiviz.json is not valid JSON", "error") and r["summary"]["pages"] == 2,
              r if isinstance(r, str) else [f["message"] for f in codes(r, "STRUCT_JSON")])
    r = audit(edit(*chart, "mobile.json", old='"width": 300', new='"width": NaN'))
    check("NaN is not JSON in a report file either", isinstance(r, dict) and has(r, "STRUCT_JSON", "mobile.json is not valid JSON: NaN", "error", "home", "chart"), r)
    r = audit(edit(*chart, "visual.json", old='"width": 300', new='"width": 1' + "0" * 400))
    check("a number of 400 digits is no size, and no crash", isinstance(r, dict) and has(r, "STRUCT_POSITION", "a visual needs a width and a height", "error", "home", "chart"), r)
    r = audit(edit(*package, old='"kind": 1', new='"kind": 7'))
    check("a role kind that is not one of the three is no kind",
          isinstance(r, dict) and r["summary"]["custom_visual_capabilities"][0]["data_roles"] == {"category": "Grouping", "values": None}, r)

    for label, fields in (("a list", ["Sales"]), ("a number for a table", {"Sales": 5}), ("text for a table", {"Sales": "Total Sales"}),
                          ("a number in a group", {"Sales": {"measures": 3}})):
        r = audit(model_fields=fields)
        check(f"model_fields as {label} is refused with a ValueError that names it", isinstance(r, str) and r.startswith("ValueError: model_fields"), r)
    r = audit(model_fields={"Sales": None, "Dim Product": {"columns": ["Category"], "measures": None}})
    check("a table with no fields is still a table", isinstance(r, dict) and r["summary"]["field_check"] == "done", r)
    for name, value in (("max_data_visuals_per_page", None), ("max_visuals_per_page", "40"), ("min_element_size", True),
                        ("overlap_ratio", None)):
        r = audit(**{name: value})
        check(f"{name}={value!r} is refused with a ValueError that names it", isinstance(r, str) and r.startswith(f"ValueError: {name} must be a number"), r)


def test_odd_shapes_in_native_features():
    print("\n== odd shapes in the native features do not stop the audit ==")
    weird = clean_pages()
    weird[0]["doc"].update(visualInteractions="all", filterConfig=["x"], pageBinding="Drillthrough", type=7, visibility=None)
    weird[1]["doc"].update(
        visualInteractions=[1, None, {"source": 5}, {"source": ["a"], "target": {"b": 1}, "type": []}],
        filterConfig={"filters": [1, {"name": ["n"], "field": "f", "filter": [], "isHiddenInViewMode": "yes"}, {"name": None},
                                  {"name": "h", "isHiddenInViewMode": True, "field": {"Column": {"Property": 3}}}]},
        pageBinding={"type": "Drillthrough", "referenceScope": 1, "parameters": [3, {"boundFilter": {"x": 1}, "fieldExpr": []}]})
    weird[0]["visuals"][0]["_mobile"] = ["not", "an", "object"]
    weird[0]["visuals"][1]["_mobile"] = {"position": "top"}
    weird[0]["visuals"][1]["filterConfig"] = {"filters": "none"}
    weird[0]["visuals"][0]["visual"]["visualContainerObjects"]["visualTooltip"] = [{"properties": {"section": {"expr": {"Literal": {"Value": 5}}}}}, "x"]
    weird[0]["visuals"][0]["visual"]["visualType"] = DENEB
    weird[0]["visuals"][0]["visual"]["objects"] = {"vega": "on"}
    weird[0]["visuals"][1]["visual"]["visualType"] = HTML_SECURE
    weird[0]["visuals"][1]["visual"]["query"] = {"queryState": {"sampling": {"projections": [7, {"field": []}]}, "tooltips": 3,
                                                                "content": {"projections": "x", "fieldParameters": 4}}}
    weird[0]["visuals"][2]["visual"]["visualContainerObjects"]["visualLink"] = [{"properties": {"type": lit("'Drillthrough'"),
                                                                                             "drillthroughSection": lit("7")}}]
    odd = {"capabilities": {"dataRoles": ["x", {"name": "a", "kind": []}, {"name": 5}], "privileges": [1, {"name": 5, "parameters": "p"}],
                            "tooltips": [], "drilldown": 4},
           "content": {"js": 12}, "visual": "v", "apiVersion": 5}
    try:
        root, d = write_report(weird, public=[DENEB, HTML_SECURE], report_extra={"filterConfig": "x"},
                               bookmarks=[bookmark("b", "home")],
                               bookmark_items=[{"name": "g", "children": [1, None, ["x"]]}, {"children": []}, 7, {"name": {"a": 1}}],
                               packages={FULL: odd, MENU: "[1, 2]", PICK: {"capabilities": []}})
        dump(os.path.join(d, "pages", "pages.json"), {"pageOrder": "home", "activePageName": ["home"], "landingPageName": {"a": 1}})
        r = ra.audit_report(root, model_fields={"Sales": ["Total Sales"]})
        check("odd shapes are tolerated", r["summary"]["pages"] == 2 and len(r["summary"]["custom_visual_capabilities"]) == 3)
        check("and what can still be said is said",
              has(r, "STRUCT_MOBILE_POSITION", "no position", "error", "home") and has(r, "STRUCT_BOOKMARK_GROUP", "holds no bookmark", "info"))
    except Exception as e:  # noqa: BLE001
        check("odd shapes are tolerated", False, f"{type(e).__name__}: {e}")


if __name__ == "__main__":
    for fn in (test_clean_report, test_structure, test_field_check, test_design, test_colours_and_governance,
               test_unreadable_parts, test_drillthrough, test_tooltip_pages, test_interactions, test_filters,
               test_mobile_layout, test_bookmark_groups, test_custom_visual_capabilities, test_store_visual_interactivity,
               test_path_length, test_every_feature_used_correctly, test_hostile_input, test_odd_shapes_in_native_features):
        fn()
    print()
    if _failures:
        print(f"FAILED: {len(_failures)} check(s): {_failures}")
        sys.exit(1)
    print("ALL REPORT AUDIT TESTS PASSED")
