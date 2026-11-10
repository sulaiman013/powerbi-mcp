"""
Tests for src/report_audit.py on synthetic PBIR report definitions built in a temp folder:
a clean report gives no findings; each structural, design and governance check fires on the
defect it is for and stays quiet on the deliberate pattern next to it; the audit changes no
file. No Power BI required. Run: python tests/test_report_audit.py
"""
import copy
import json
import os
import sys
import tempfile

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


def write_report(pages, bookmarks=None, public=None, private=None, order=None, active=None, bookmark_items=None):
    root = os.path.join(tempfile.mkdtemp(), "Demo.Report")
    d = os.path.join(root, "definition")
    dump(os.path.join(d, "report.json"), {"$schema": SCHEMA.format("report", "3.0.0"), "publicCustomVisuals": public or []})
    dump(os.path.join(d, "version.json"), {"version": "2.0.0"})
    names = [p["doc"]["name"] for p in pages]
    dump(os.path.join(d, "pages", "pages.json"), {"$schema": SCHEMA.format("pagesMetadata", "1.0.0"),
                                                  "pageOrder": order if order is not None else names,
                                                  "activePageName": active or names[0]})
    for p in pages:
        folder = os.path.join(d, "pages", p.get("folder") or p["doc"]["name"])
        dump(os.path.join(folder, "page.json"), p["doc"])
        for v in p["visuals"]:
            dump(os.path.join(folder, "visuals", v.get("_folder") or v["name"], "visual.json"),
                 {k: val for k, val in v.items() if k != "_folder"})
    if bookmarks is not None:
        items = bookmark_items if bookmark_items is not None else [{"name": b["name"]} for b in bookmarks]
        dump(os.path.join(d, "bookmarks", "bookmarks.json"), {"items": items})
        for b in bookmarks:
            dump(os.path.join(d, "bookmarks", b["name"] + ".bookmark.json"), b)
    for name in private or []:
        dump(os.path.join(root, "CustomVisuals", name, "package.json"), {"visual": {"guid": name}})
    return root, d


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


if __name__ == "__main__":
    for fn in (test_clean_report, test_structure, test_field_check, test_design, test_colours_and_governance,
               test_unreadable_parts):
        fn()
    print()
    if _failures:
        print(f"FAILED: {len(_failures)} check(s): {_failures}")
        sys.exit(1)
    print("ALL REPORT AUDIT TESTS PASSED")
