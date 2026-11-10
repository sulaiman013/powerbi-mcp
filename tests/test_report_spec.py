"""
Tests for the declarative report engine (src/report_spec.py): validation, applying a spec to a
report folder, idempotence, dry run, prune, hand-made pages left alone, and a structural check
of everything one full spec writes. Report folders are built in a temp directory.
No Power BI required.
Run: python tests/test_report_spec.py
"""
import copy
import io
import json
import os
import sys
import tempfile
import zipfile

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))
import pbir_builders as pb  # noqa: E402
import report_spec as rs  # noqa: E402

_failures = []
BOM = b"\xef\xbb\xbf"
GUID = "homeVisualABCDEF0123456789"


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f": {detail}" if detail and not cond else ""))
    if not cond:
        _failures.append(name)


def eq(name, actual, expected):
    check(name, actual == expected, f"got {actual!r}, expected {expected!r}")


def has(name, messages, fragment):
    check(name, any(fragment in m for m in messages), f"no message contains {fragment!r} in {messages!r}")


# ---------------------------------------------------------------------------------------------
# fixtures
# ---------------------------------------------------------------------------------------------
def make_report(base, name="Demo.Report"):
    """An empty report folder: only definition.pbir, as it is before any page exists."""
    root = os.path.join(base, name)
    os.makedirs(root)
    with io.open(os.path.join(root, "definition.pbir"), "w", encoding="utf-8", newline="\n") as f:
        f.write(json.dumps({"version": "4.0",
                            "datasetReference": {"byPath": {"path": "../Demo.SemanticModel"}}}))
    return root


def write_raw(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        f.write(data)


def load(path):
    with io.open(path, encoding="utf-8") as f:
        return json.load(f)


def snapshot(base):
    out = {}
    for folder, _dirs, files in os.walk(base):
        for name in files:
            p = os.path.join(folder, name)
            with open(p, "rb") as f:
                out[os.path.relpath(p, base).replace(os.sep, "/")] = f.read()
    return out


def make_pbiviz(path, guid=GUID, extra_member=None):
    package = {"version": "1.2.0.0", "visual": {"name": "homeVisual", "guid": guid, "version": "1.2.0.0"},
               "resources": [{"resourceId": "rId0", "sourceType": 5,
                              "file": f"resources/{guid}.pbiviz.json"}]}
    with zipfile.ZipFile(path, "w") as z:
        z.writestr("package.json", json.dumps(package, indent="\t").replace("\n", "\r\n"))
        z.writestr(f"resources/{guid}.pbiviz.json", json.dumps({"visual": package["visual"],
                                                                "content": {"js": "x"}}))
        if extra_member:
            z.writestr(extra_member, "{}")


def hand_made(root):
    """A page and two visuals as Desktop might have saved them: CRLF, 4-space indent, extra keys."""
    pages = os.path.join(root, "definition", "pages")
    page = {"$schema": "https://example.test/definition/page/2.1.0/schema.json", "name": "legacy",
            "displayName": "Legacy", "displayOption": "FitToPage", "height": 720, "width": 1280}
    write_raw(os.path.join(pages, "legacy", "page.json"), json.dumps(page, indent=4).replace("\n",
                                                                                             "\r\n").encode())
    for name, vtype in (("oldChart", "barChart"), ("oldText", "textbox")):
        visual = {"$schema": "https://example.test/definition/visualContainer/2.14.0/schema.json",
                  "name": name,
                  "position": {"x": 0, "y": 0, "z": 0, "width": 100, "height": 100},
                  "visual": {"visualType": vtype}, "howCreated": "InsertVisualButton"}
        write_raw(os.path.join(pages, "legacy", "visuals", name, "visual.json"),
                  json.dumps(visual, indent=4).replace("\n", "\r\n").encode())
    meta = {"$schema": "https://example.test/definition/pagesMetadata/1.1.0/schema.json",
            "pageOrder": ["legacy"],
            "activePageName": "legacy", "landingPageName": "legacy"}
    write_raw(os.path.join(pages, "pages.json"), json.dumps(meta, indent=2).replace("\n", "\r\n").encode())
    report = {"$schema": "https://example.test/definition/report/3.3.0/schema.json",
              "themeCollection": {"customTheme": {"name": "handmade.json", "type": "RegisteredResources"}},
              "settings": {"exportDataMode": "AllowSummarized"}}
    write_raw(os.path.join(root, "definition", "report.json"), json.dumps(report, indent=2).encode())
    bm = {"$schema": "https://example.test/definition/bookmark/2.1.0/schema.json", "displayName": "Mine",
          "name": "mine",
          "options": {"targetVisualNames": []},
          "explorationState": {"version": "1.3", "activeSection": "legacy",
                               "sections": {"legacy": {"visualContainers": {}}}}}
    write_raw(os.path.join(root, "definition", "bookmarks", "mine.bookmark.json"), json.dumps(bm,
                                                                                           indent=2).encode())
    write_raw(os.path.join(root, "definition", "bookmarks", "bookmarks.json"), json.dumps(
        {"$schema": "https://example.test/definition/bookmarksMetadata/1.0.0/schema.json",
         "items": [{"name": "mine"}]},
        indent=2).encode())


MODEL = {
    "Calendar": {"Year": {"kind": "column", "dataType": "int64"}, "Month": {"kind": "column",
                                                                            "dataType": "string"}},
    "Customer": {"Name": {"kind": "column", "dataType": "string"}, "Segment": {"kind": "column",
                                                                               "dataType": "string"}},
    "Sales": {"Amount": {"kind": "column", "dataType": "double"}, "Total Sales": {"kind": "measure",
                                                                                  "dataType": None},
              "Summary HTML": {"kind": "measure", "dataType": None}, "KPI Card": {"kind": "measure",
                                                                                  "dataType": None},
              "Home Payload": {"kind": "measure", "dataType": None}},
}
CATALOG = {t: set(fs) for t, fs in MODEL.items()}


def small_spec():
    return {"pages": [{"name": "overview", "display_name": "Overview", "elements": [
        {"name": "heading", "type": "textbox", "x": 24, "y": 24, "width": 400, "height": 40,
         "text": "Overview"},
        {"name": "by_month", "type": "visual", "visual_type": "lineChart", "x": 24, "y": 80, "width": 800,
         "height": 400,
         "fields": {"Category": "Calendar.Month", "Y": "Sales.Total Sales"}}]}]}


def full_spec(pbiviz):
    """One spec that uses every element type and every convenience."""
    return {
        "theme": {"name": "House Style", "colors": {"data": ["#1F6FEB", "#F0883E"], "background": "#FFFFFF",
                                                    "page": "#F6F8FA", "foreground": "#1F2328",
                                                    "border": "#D0D7DE"},
                  "fonts": {"title": {"family": "Segoe UI Semibold", "size": 12}}, "card": {"radius": 8,
                                                                                            "padding": 12}},
        "custom_visuals": [{"pbiviz": pbiviz, "alias": "home"}, {"appsource": "someVisual0123456789ABCDEF"}],
        "page_defaults": {"width": 1280, "height": 720, "wallpaper": "#EAEEF2"},
        "shared": [{"name": "masthead", "pages": "*", "exclude": ["Home"], "elements": [
            {"name": "brand", "type": "textbox", "x": 24, "y": 12, "width": 300, "height": 40,
             "text": "{page_title}",
             "size": 16, "bold": True},
            {"name": "nav_overview", "type": "button", "x": 400, "y": 12, "width": 110, "height": 36,
             "text": "Overview",
             "radius": 18, "action": {"type": "page", "target": "Overview"}, "active": {"fill": "#1F6FEB",
                                                                                        "color": "#FFFFFF"}},
            {"name": "nav_detail", "type": "button", "x": 520, "y": 12, "width": 110, "height": 36,
             "text": "Detail",
             "radius": 18, "action": {"type": "page", "target": "detail"}, "active": {"fill": "#1F6FEB",
                                                                                      "color": "#FFFFFF"}},
            {"name": "open_filters", "type": "button", "x": 1136, "y": 12, "width": 120, "height": 36,
             "text": "Filters",
             "fill": "#1F6FEB", "color": "#FFFFFF", "action": {"type": "bookmark",
                                                               "target": "{page}_filters_open"}},
            {"name": "clear", "type": "button", "x": 1000, "y": 12, "width": 120, "height": 36,
             "text": "Clear",
             "action": {"type": "clear_slicers"}}],
            "popups": [{"name": "filters", "title": "Filters", "box": {"x": 340, "y": 160, "width": 600,
                                                                       "height": 400},
                        "elements": [
                            {"name": "f_year", "type": "slicer", "x": 372, "y": 240, "width": 536,
                             "height": 72,
                             "field": "Calendar.Year", "mode": "tiles", "sync_group": True, "header": "Year"},
                            {"name": "f_segment", "type": "slicer", "x": 372, "y": 330, "width": 260,
                             "height": 72,
                             "field": "Customer.Segment", "mode": "dropdown", "single_select": True}]}]}],
        "pages": [
            {"name": "Home", "display_name": "Home", "background": {"image": "ground.png"}, "elements": [
                {"name": "scene", "type": "visual", "custom": "home", "x": 0, "y": 0, "width": 1280,
                 "height": 720,
                 "chrome": "plain", "alt_text": "The home scene",
                 "fields": {"payload": {"measure": "Sales.Home Payload"}},
                 "objects": {"motion": {"animate": True, "speed": 1}}},
                {"name": "go_overview", "type": "button", "x": 60, "y": 560, "width": 300, "height": 120,
                 "action": {"type": "page", "target": "Overview", "tooltip": "Open Overview"}},
                {"name": "docs", "type": "button", "x": 1100, "y": 20, "width": 150, "height": 36,
                 "text": "Docs",
                 "action": {"type": "url", "target": "https://example.com/docs"}}]},
            {"name": "Overview", "display_name": "Overview", "background": "#F6F8FA", "elements": [
                {"name": "panel", "type": "shape", "x": 24, "y": 64, "width": 1232, "height": 120,
                 "fill": "#FFFFFF",
                 "radius": 12, "outline": "#D0D7DE"},
                {"name": "kpi", "type": "image", "x": 40, "y": 76, "width": 280, "height": 96,
                 "measure": "Sales.KPI Card",
                 "background": "#FFFFFF", "border": {"color": "#D0D7DE", "radius": 8}},
                {"name": "logo", "type": "image", "x": 1180, "y": 76, "width": 60, "height": 60,
                 "resource": "logo.png",
                 "scaling": "Fill", "lock_aspect": True, "action": {"type": "page", "target": "Home"}},
                {"name": "summary", "type": "html", "x": 340, "y": 76, "width": 800, "height": 96,
                 "measure": "Sales.Summary HTML"},
                {"name": "bars", "type": "deneb", "x": 24, "y": 200, "width": 600, "height": 500,
                 "title": "Sales by segment", "subtitle": {"text": "Click a bar to filter", "size": 9},
                 "fields": ["Customer.Segment", {"measure": "Sales.Total Sales"}], "selection": True,
                 "spec": {"data": {"name": "dataset"}, "mark": "bar",
                          "encoding": {"y": {"field": "Segment", "type": "nominal"},
                                       "x": {"field": "Total Sales", "type": "quantitative"}}},
                 "config": {"view": {"stroke": "transparent"}}},
                {"name": "trend", "type": "visual", "visual_type": "lineChart", "x": 640, "y": 200,
                 "width": 616,
                 "height": 240, "title": "Sales by month", "fields": {"Category": "Calendar.Month",
                                                                      "Y": "Sales.Total Sales"},
                 "objects": {"legend": {"show": False}}},
                {"name": "details", "type": "group", "display_name": "Details", "elements": [
                    {"name": "amounts", "type": "visual", "visual_type": "barChart", "x": 640, "y": 456,
                     "width": 616,
                     "height": 244, "fields": {"Category": "Customer.Segment", "Y": "Sales.Amount"}},
                    {"name": "note", "type": "textbox", "x": 660, "y": 460, "width": 300, "height": 24,
                     "paragraphs": [{"runs": [{"text": "Amounts ", "bold": True}, {"text": "by segment"}],
                                     "align": "right"}]}]},
                {"name": "back", "type": "button", "x": 24, "y": 688, "width": 80, "height": 28,
                 "text": "Back",
                 "action": {"type": "back"}, "allow_overflow": True}]},
            {"name": "detail", "display_name": "Detail", "hidden": True, "elements": [
                {"name": "orders", "type": "visual", "visual_type": "tableEx", "x": 24, "y": 64,
                 "width": 1232, "height": 640,
                 "fields": {"Values": ["Customer.Name", "Sales.Amount", "Sales.Total Sales"]}}]}],
        "bookmarks": [
            {"name": "ShowDetails", "display_name": "Show details", "page": "Overview", "show": ["details"]},
            {"name": "HideDetails", "display_name": "Hide details", "page": "Overview", "hide": ["details",
                                                                                                 "panel"]},
            {"name": "Start", "display_name": "Start here", "page": "Overview", "targeted": False}],
        "page_order": ["Home", "Overview", "detail"],
        "active_page": "Home",
    }


def prepared(base):
    """An empty report folder with the two image files the full spec refers to, and its package."""
    root = make_report(base)
    for image in ("ground.png", "logo.png"):
        write_raw(os.path.join(root, "StaticResources", "RegisteredResources", image), b"\x89PNG not really")
    pbiviz = os.path.join(base, "home.pbiviz")
    make_pbiviz(pbiviz)
    return root, pbiviz


# ---------------------------------------------------------------------------------------------
# a structural check of a report folder, independent of the code under test
# ---------------------------------------------------------------------------------------------
def _walk(node):
    if isinstance(node, dict):
        yield node
        for v in node.values():
            yield from _walk(v)
    elif isinstance(node, list):
        for v in node:
            yield from _walk(v)


def self_check(root):
    problems = []
    d = os.path.join(root, "definition")
    for required in ("report.json", "version.json", os.path.join("pages", "pages.json")):
        if not os.path.isfile(os.path.join(d, required)):
            problems.append(f"missing {required}")
    if problems:
        return problems
    report = load(os.path.join(d, "report.json"))
    if "themeCollection" not in report or "$schema" not in report:
        problems.append("report.json lacks $schema or themeCollection")
    if load(os.path.join(d, "version.json")).get("version") != "2.0.0":
        problems.append("version.json")
    pages_dir = os.path.join(d, "pages")
    page_names = sorted(n for n in os.listdir(pages_dir) if os.path.isdir(os.path.join(pages_dir, n)))
    meta = load(os.path.join(pages_dir, "pages.json"))
    if sorted(meta.get("pageOrder", [])) != page_names:
        problems.append(f"pageOrder {meta.get('pageOrder')} does not match the folders {page_names}")
    if meta.get("activePageName") not in page_names:
        problems.append("activePageName is not a page")
    bdir = os.path.join(d, "bookmarks")
    suffix = ".bookmark.json"
    bookmark_names = sorted(f[:-len(suffix)] for f in os.listdir(bdir) if f.endswith(suffix)) \
        if os.path.isdir(bdir) else []
    if bookmark_names:
        listed = sorted(i["name"] for i in load(os.path.join(bdir, "bookmarks.json"))["items"])
        if listed != bookmark_names:
            problems.append(f"bookmarks.json lists {listed}, the files are {bookmark_names}")
    registered = {i["name"]: i["type"] for p in report.get("resourcePackages",
                                                           []) if p["name"] == "RegisteredResources"
                  for i in p["items"]}
    packages = {p["name"] for p in report.get("resourcePackages", []) if p["type"] == "CustomVisual"}
    public = set(report.get("publicCustomVisuals", []))
    resources = os.path.join(root, "StaticResources", "RegisteredResources")
    for name in registered:
        if not os.path.isfile(os.path.join(resources, name)):
            problems.append(f"registered resource {name} has no file")
    custom_theme = (report["themeCollection"].get("customTheme") or {}).get("name")
    if custom_theme and registered.get(custom_theme) != "CustomTheme":
        problems.append("the custom theme is not registered")
    for guid in packages:
        if not os.path.isfile(os.path.join(root, "CustomVisuals", guid, "resources", f"{guid}.pbiviz.json")):
            problems.append(f"custom visual {guid} has no metadata file")

    groups_by_page, visuals_by_page = {}, {}
    for page_name in page_names:
        page = load(os.path.join(pages_dir, page_name, "page.json"))
        if page.get("name") != page_name:
            problems.append(f"page folder {page_name} holds a page named {page.get('name')}")
        if not all(isinstance(page.get(k), (int, float)) for k in ("width",
                                                                   "height")) or not page.get("displayName"):
            problems.append(f"page {page_name} lacks a size or a display name")
        vdir = os.path.join(pages_dir, page_name, "visuals")
        visuals = {}
        for name in sorted(os.listdir(vdir)) if os.path.isdir(vdir) else []:
            path = os.path.join(vdir, name, "visual.json")
            if not os.path.isfile(path):
                problems.append(f"{page_name}/{name} has no visual.json")
                continue
            visuals[name] = load(path)
        visuals_by_page[page_name] = visuals
        groups_by_page[page_name] = {n for n, v in visuals.items() if "visualGroup" in v}
        zs = [v["position"]["z"] for v in visuals.values()]
        if len(zs) != len(set(zs)):
            problems.append(f"page {page_name} has two visuals on one z")
        for name, v in visuals.items():
            where = f"{page_name}/{name}"
            if v.get("name") != name:
                problems.append(f"{where} is named {v.get('name')}")
            if not str(v.get("$schema", "")).endswith("/schema.json"):
                problems.append(f"{where} has no $schema")
            pos = v.get("position", {})
            if not all(isinstance(pos.get(k), (int, float)) for k in ("x", "y", "z", "width", "height",
                                                                      "tabOrder")):
                problems.append(f"{where} has an incomplete position")
            elif (pos["x"] < 0 or pos["y"] < 0 or pos["x"] + pos["width"] > page["width"]
                  or pos["y"] + pos["height"] > page["height"]):
                problems.append(f"{where} is outside the page")
            if ("visual" in v) == ("visualGroup" in v):
                problems.append(f"{where} must be either a visual or a group")
            parent = v.get("parentGroupName")
            if parent and parent not in groups_by_page[page_name]:
                problems.append(f"{where} belongs to a group that is not on the page: {parent}")
            vtype = v.get("visual", {}).get("visualType")
            if "visual" in v and not vtype:
                problems.append(f"{where} has no visualType")
            if vtype and vtype.startswith(("htmlContent", "deneb")) and vtype not in public:
                problems.append(f"{where} uses {vtype}, which report.json does not list")
            if vtype == GUID and GUID not in packages:
                problems.append(f"{where} uses a custom visual that is not registered")
            for node in _walk(v):
                if "Literal" in node and not (isinstance(node["Literal"],
                                                         dict) and isinstance(node["Literal"].get("Value"),
                                                                              str)):
                    problems.append(f"{where} has a malformed literal")
                if "projections" in node:
                    for p in node["projections"]:
                        kinds = set(p.get("field", {})) & {"Column", "Measure", "Aggregation"}
                        if len(kinds) != 1 or not p.get("queryRef") or not p.get("nativeQueryRef"):
                            problems.append(f"{where} has a malformed projection")
                item = node.get("ResourcePackageItem")
                if item and item["ItemName"] not in registered:
                    problems.append(f"{where} uses an unregistered resource")
            for link in v.get("visual", {}).get("visualContainerObjects", {}).get("visualLink", []):
                p = link["properties"]
                kind = p["type"]["expr"]["Literal"]["Value"]
                target = p.get("navigationSection") or p.get("bookmark")
                target = target["expr"]["Literal"]["Value"].strip("'") if target else ""
                if kind == "'PageNavigation'" and target not in page_names:
                    problems.append(f"{where} links to a page that does not exist")
                if kind == "'Bookmark'" and target not in bookmark_names:
                    problems.append(f"{where} links to a bookmark that does not exist")
    for name in bookmark_names:
        bm = load(os.path.join(bdir, f"{name}.bookmark.json"))
        if bm.get("name") != name:
            problems.append(f"bookmark file {name} holds a bookmark named {bm.get('name')}")
        state = bm["explorationState"]
        if state["activeSection"] not in page_names:
            problems.append(f"bookmark {name} is on a page that does not exist")
            continue
        for section, body in state["sections"].items():
            for g in body.get("visualContainerGroups", {}):
                if g not in groups_by_page.get(section, set()):
                    problems.append(f"bookmark {name} toggles a group that is not on the page: {g}")
            for vname in body.get("visualContainers", {}):
                if vname not in visuals_by_page.get(section, {}):
                    problems.append(f"bookmark {name} names a visual that is not on the page: {vname}")
        for target in bm["options"].get("targetVisualNames", []):
            if target not in visuals_by_page[state["activeSection"]]:
                problems.append(f"bookmark {name} targets {target}, which is not on the page")
    return problems


# ---------------------------------------------------------------------------------------------
# tests
# ---------------------------------------------------------------------------------------------
def test_validation():
    print("\n== validation ==")
    eq("a small spec is valid", rs.validate_report_spec(small_spec()), [])
    eq("the full spec is valid", rs.validate_report_spec(full_spec("home.pbiviz"), model_fields=MODEL), [])
    eq("and with only names known", rs.validate_report_spec(small_spec(), model_fields=CATALOG), [])
    has("the spec must be an object", rs.validate_report_spec([]), "must be an object")
    has("an unknown top-level key", rs.validate_report_spec({"page": []}), "unknown key 'page'")
    has("pages must be a list", rs.validate_report_spec({"pages": {}}), "pages must be a list")

    def broken(change):
        spec = small_spec()
        change(spec)
        return rs.validate_report_spec(spec)

    def el(spec, i=0):
        return spec["pages"][0]["elements"][i]

    has("a name with a space", broken(lambda s: el(s).update(name="my heading")), "not a usable name")
    has("a reserved name", broken(lambda s: el(s).update(name="NUL")), "reserved file name")
    has("a name used twice, whatever the case", broken(lambda s: el(s, 1).update(name="HEADING")),
        "already used on this page")
    has("an unknown type", broken(lambda s: el(s).update(type="sparkle")), "unknown type 'sparkle'")
    has("a missing size", broken(lambda s: el(s).pop("width")), "needs numeric width")
    has("a zero size", broken(lambda s: el(s).update(height=0)), "greater than 0")
    has("an element off the page", broken(lambda s: el(s).update(x=1000)), "does not fit the 1280 x 720 page")
    eq("which can be allowed", broken(lambda s: el(s).update(x=1000, allow_overflow=True)), [])
    has("a textbox with no text", broken(lambda s: el(s).pop("text")), 'needs "text" or "paragraphs"')
    has("a visual with no type", broken(lambda s: el(s, 1).pop("visual_type")), 'needs "visual_type"')
    has("a field that is not Table.Field", broken(lambda s: el(s, 1)["fields"].update(Y="Total")),
        "write it as 'Table.Field'")
    has("an unknown page key", broken(lambda s: s["pages"][0].update(colour="#FFFFFF")),
        "unknown key 'colour'")
    has("a page twice", broken(lambda s: s["pages"].append(dict(s["pages"][0], name="OVERVIEW"))),
        "used twice")
    has("a page with no size", broken(lambda s: s["pages"][0].update(width="wide")),
        "width and height must be numbers")
    has("an action on a textbox", broken(lambda s: el(s).update(action={"type": "back"})),
        "only a button, shape or image")
    has("page_order naming a stranger", broken(lambda s: s.update(page_order=["nowhere"])),
        "page_order names 'nowhere'")
    has("active_page naming a stranger", broken(lambda s: s.update(active_page="nowhere")),
        "active_page is 'nowhere'")
    has("a theme with no name", broken(lambda s: s.update(theme={"colors": {}})), '"name" is required')
    has("a theme file that is not .json", broken(lambda s: s.update(theme={"name": "T", "file": "t.txt"})),
        "must end in .json")
    has("custom_visuals needs a source", broken(lambda s: s.update(custom_visuals=[{"alias": "x"}])),
        "give either")

    button = {"name": "go", "type": "button", "x": 0, "y": 0, "width": 10, "height": 10}

    def with_button(**kw):
        spec = small_spec()
        spec["pages"][0]["elements"].append({**button, **kw})
        return spec

    has("a button to a page that is not there", rs.validate_report_spec(with_button(action={"type": "page",
                                                                                       "target": "nowhere"})),
        "opens page 'nowhere', which does not exist")
    eq("unless the report has it", rs.validate_report_spec(with_button(action={"type": "page",
                                                                               "target": "nowhere"}),
                                                         existing_pages=["Nowhere"]), [])
    has("a button to a bookmark that is not there",
        rs.validate_report_spec(with_button(action={"type": "bookmark", "target": "ghost"})),
        "applies bookmark 'ghost'")
    eq("unless the report has it too", rs.validate_report_spec(with_button(action={"type": "bookmark",
                                                                                   "target": "ghost"}),
                                                               existing_bookmarks=["ghost"]), [])
    has("an unknown action", rs.validate_report_spec(with_button(action={"type": "teleport"})),
        'action needs "type"')
    has("an action with no target", rs.validate_report_spec(with_button(action={"type": "page"})),
        'needs "target"')
    has("a url that is not http", rs.validate_report_spec(with_button(action={"type": "url",
                                                                              "target": "file:///c:/x"})),
        "http:// or https://")
    has("an unknown button state", rs.validate_report_spec(with_button(states={"wiggle": {}})),
        "unknown button state")

    def with_bookmark(**kw):
        spec = small_spec()
        spec["bookmarks"] = [{"name": "bm", "page": "overview", **kw}]
        return spec

    has("a bookmark with nothing to do", rs.validate_report_spec(with_bookmark()), 'needs "show" or "hide"')
    has("a bookmark naming a stranger", rs.validate_report_spec(with_bookmark(show=["ghost"])),
        "not an element of page")
    has("a bookmark on a page that is not in the spec",
        rs.validate_report_spec(with_bookmark(page="elsewhere", show=["x"])),
        "is not a page of this spec")
    eq("a bookmark on single visuals", rs.validate_report_spec(with_bookmark(hide=["heading"])), [])
    spec = with_bookmark(hide=["heading"])
    spec["bookmarks"].append(dict(spec["bookmarks"][0], name="BM"))
    has("a bookmark defined twice", rs.validate_report_spec(spec), "defined twice")

    spec = small_spec()
    spec["pages"][0]["popups"] = [{"name": "filters"}]
    has("a pop-up with no box", rs.validate_report_spec(spec), "box needs numeric")
    spec["pages"][0]["popups"] = [{"name": "x" * 40, "box": {"x": 1, "y": 1, "width": 10, "height": 10}}]
    has("pop-up bookmark names that get too long", rs.validate_report_spec(spec), "open_bookmark")
    spec = small_spec()
    spec["shared"] = [{"name": "s", "pages": ["nowhere"], "elements": []}]
    has("shared elements for a page that is not there", rs.validate_report_spec(spec),
        "not a page of this spec")
    spec = small_spec()
    spec["pages"][0]["elements"].append({"name": "g", "type": "group", "elements": [{"name": "h",
                                                                                     "type": "group"}]})
    has("a group inside a group", rs.validate_report_spec(spec), "cannot hold another group")


def test_documented_example():
    print("\n== the example in the module docstring ==")
    doc = rs.__doc__
    text = doc[doc.index("EXAMPLE") + len("EXAMPLE"):doc.index("APPLYING")]
    try:
        example = json.loads(text)
    except ValueError as e:
        check("the example is valid JSON", False, str(e))
        return
    check("the example is valid JSON", True)
    eq("it validates on its own", rs.validate_report_spec(example), [])
    eq("and against a model", rs.validate_report_spec(example, model_fields=MODEL), [])
    with tempfile.TemporaryDirectory() as base:
        root = make_report(base)
        result = rs.apply_report_spec(root, example, model_fields=MODEL)
        eq("it applies to an empty report folder", [result["success"], result["errors"]], [True, []])
        eq("and what it writes is sound", self_check(root), [])
    for key in rs._TOP_KEYS:
        check(f"the docstring explains {key!r}", f'"{key}"' in doc or f"\n{key}\n" in doc)


def test_fields():
    print("\n== fields against the model ==")

    def one(element, model=MODEL):
        spec = {"pages": [{"name": "p", "elements": [{"name": "e", "x": 0, "y": 0, "width": 100,
                                                      "height": 100, **element}]}]}
        compiled = rs._compile(spec, model_fields=model)
        doc = compiled.pages[0].elements[0].doc if compiled.pages and compiled.pages[0].elements else None
        return compiled, doc

    def projections(doc, role):
        return doc["visual"]["query"]["queryState"][role]["projections"]

    c, doc = one({"type": "visual", "visual_type": "barChart", "fields": {"Category": "customer.segment",
                                                                         "Y": ["Sales.Amount",
                                                                               "sales.total sales"]}})
    eq("a chart resolves", c.errors, [])
    eq("names take the model's spelling", projections(doc, "Category")[0]["queryRef"], "Customer.Segment")
    eq("a numeric column in a value role is summed", projections(doc, "Y")[0]["queryRef"],
       "Sum(Sales.Amount)")
    eq("a measure stays a measure", list(projections(doc, "Y")[1]["field"]), ["Measure"])
    eq("nothing was guessed", c.warnings, [])
    c, doc = one({"type": "visual", "visual_type": "barChart", "fields": {"Y": "Customer.Name"}})
    eq("a text column in a value role is counted", projections(doc, "Y")[0]["queryRef"],
       "CountNonNull(Customer.Name)")
    c, doc = one({"type": "visual", "visual_type": "barChart", "fields": {"Y": {"column": "Sales.Amount",
                                                                                "aggregation": "max"}}})
    eq("an aggregation can be chosen", projections(doc, "Y")[0]["queryRef"], "Max(Sales.Amount)")
    c, doc = one({"type": "visual", "visual_type": "tableEx", "fields": {"Values": ["Customer.Name",
                                                                                    "Sales.Amount",
                                                                                    "Sales.Total Sales"]}})
    eq("a table lists columns as they are", [list(p["field"]) for p in projections(doc, "Values")],
       [["Column"], ["Column"], ["Measure"]])

    c, _ = one({"type": "visual", "visual_type": "barChart", "fields": {"Y": "Sales.Missing"}})
    has("a field that is not in the model", c.errors,
        "Sales[Missing] is not in the model (no such field in that table)")
    c, _ = one({"type": "visual", "visual_type": "barChart", "fields": {"Y": "Nope.Missing"}})
    has("a table that is not in the model", c.errors, "no such table")
    c, _ = one({"type": "slicer", "field": "Sales.Total Sales"})
    has("a slicer on a measure", c.errors, "a column is needed here")
    c, _ = one({"type": "html", "measure": "Customer.Name"})
    has("HTML from a column", c.errors, "a measure is needed here")
    c, _ = one({"type": "image", "measure": "Customer.Name"})
    has("an image from a column", c.errors, "a measure is needed here")
    c, _ = one({"type": "visual", "visual_type": "barChart", "fields": {"Y": {"measure": "Sales.Amount"}}})
    has("a column called a measure", c.errors, "is a column in the model, not a measure")
    c, _ = one({"type": "visual", "visual_type": "barChart", "fields": {"Y": {"measure": "Sales.Total Sales",
                                                                              "aggregation": "sum"}}})
    has("a measure cannot be aggregated", c.errors, "cannot take an aggregation")
    c, _ = one({"type": "visual", "visual_type": "barChart", "fields": {"Y": {"column": "Sales.Amount",
                                                                              "aggregation": "mode"}}})
    has("an unknown aggregation", c.errors, "unknown aggregation 'mode'")

    c, doc = one({"type": "visual", "visual_type": "barChart", "fields": {"Category": "Customer.Segment",
                                                                          "Y": "Sales.Total Sales"}},
                 model=CATALOG)
    eq("with names only, a chart falls back on the role", [c.errors, list(projections(doc, "Y")[0]["field"])],
       [[], ["Measure"]])
    has("and says so", c.warnings, "from the role they sit in")
    c, _ = one({"type": "deneb", "fields": ["Customer.Segment"], "spec": {}}, model=CATALOG)
    has("Deneb needs to know which is which", c.errors,
        "cannot tell whether Customer[Segment] is a column or a measure")
    c, doc = one({"type": "deneb", "fields": [{"column": "Customer.Segment"},
                                              {"measure": "Sales.Total Sales"}], "spec": {}},
                 model=None)
    eq("which the object form says", [c.errors, [list(p["field"]) for p in projections(doc, "dataset")]],
       [[], [["Column"], ["Measure"]]])
    c, _ = one({"type": "visual", "visual_type": "tableEx", "fields": {"Values": ["Customer.Name"]}},
               model=None)
    has("a table needs it too", c.errors, "cannot tell whether")
    c, doc = one({"type": "slicer", "field": "Calendar.Year", "sync_group": True}, model=None)
    eq("a slicer needs no model", [c.errors, doc["visual"]["syncGroup"]["groupName"]], [[], "Calendar_Year"])
    c, _ = one({"type": "deneb", "fields": [], "spec": {}})
    has("Deneb with no fields", c.errors, 'needs "fields"')
    c, _ = one({"type": "deneb", "fields": ["Customer.Segment"]})
    has("Deneb with no spec", c.errors, 'needs "spec"')


def test_full_spec():
    print("\n== one spec with everything ==")
    with tempfile.TemporaryDirectory() as base:
        root, pbiviz = prepared(base)
        spec = full_spec(pbiviz)
        untouched = copy.deepcopy(spec)
        result = rs.apply_report_spec(root, spec, model_fields=MODEL)
        eq("it applies", [result["success"], result["errors"]], [True, []])
        eq("the caller's spec is not changed", spec, untouched)
        eq("the structural check finds nothing", self_check(root), [])
        files = snapshot(base)
        written = {k: v for k, v in files.items() if k.endswith(".json") and "CustomVisuals" not in k}
        check("new files have no BOM", not any(v.startswith(BOM) for v in written.values()))
        check("new files use LF", not any(b"\r" in v for v in written.values()))
        check("everything written is listed", sorted(result["written"]) == sorted(
            os.path.relpath(os.path.join(base, k), root).replace(os.sep, "/") for k in files
            if not k.endswith((".png", ".pbiviz", "definition.pbir"))))
        eq("the report folder holds only report parts", sorted(os.listdir(root)),
           ["CustomVisuals", "StaticResources", "definition", "definition.pbir"])
        eq("the manifest sits beside the report folder", [result["manifest"], os.path.isfile(
            os.path.join(base, "Demo.Report.spec-manifest.json"))], ["../Demo.Report.spec-manifest.json",
                                                                     True])
        d = os.path.join(root, "definition")
        eq("definition holds what a report has", sorted(os.listdir(d)), ["bookmarks", "pages", "report.json",
                                                                         "version.json"])

        # names keep their letter case on disk, whatever the file system does with it
        eq("page folders", sorted(os.listdir(os.path.join(d, "pages"))), ["Home", "Overview", "detail",
                                                                          "pages.json"])
        check("bookmark files", {"ShowDetails.bookmark.json", "HideDetails.bookmark.json",
                                 "Start.bookmark.json",
                                 "Overview_filters_open.bookmark.json"} <= set(os.listdir(os.path.join(d,
                                                                                               "bookmarks"))))
        eq("the unpacked visual", sorted(os.listdir(os.path.join(root, "CustomVisuals", GUID, "resources"))),
           [f"{GUID}.pbiviz.json"])
        eq("the theme file is named after the theme", [f for f in os.listdir(os.path.join(
            root, "StaticResources", "RegisteredResources")) if f.endswith(".json")], ["House-Style.json"])

        with zipfile.ZipFile(pbiviz) as z:
            packed = {n: z.read(n) for n in z.namelist()}
        eq("package members are copied byte for byte", {k[len(f"Demo.Report/CustomVisuals/{GUID}/"):]: v
                                                       for k, v in files.items() if "/CustomVisuals/" in k},
                                                       packed)
        report = load(os.path.join(d, "report.json"))
        eq("AppSource visuals in use are listed", sorted(report["publicCustomVisuals"]),
           sorted([pb.DENEB_ID, pb.HTML_CONTENT_ID, "someVisual0123456789ABCDEF"]))
        eq("the images are registered",
           sorted(i["name"] for i in report["resourcePackages"][0]["items"] if i["type"] == "Image"),
           ["ground.png", "logo.png"])
        meta = load(os.path.join(d, "pages", "pages.json"))
        eq("page order and the opening page", [meta["pageOrder"], meta["activePageName"]], [["Home",
                                                                                             "Overview",
                                                                                             "detail"],
                                                                                             "Home"])
        eq("a hidden page", load(os.path.join(d, "pages", "detail", "page.json")).get("visibility"),
           "HiddenInViewMode")
        eq("page defaults reach every page", "outspace" in load(os.path.join(d, "pages", "detail",
                                                                             "page.json"))["objects"], True)

        def visual(page, name):
            return load(os.path.join(d, "pages", page, "visuals", name, "visual.json"))

        eq("the custom visual is used by its guid", visual("Home", "scene")["visual"]["visualType"], GUID)
        eq("its compact formatting", visual("Home", "scene")["visual"]["objects"]["motion"],
           [{"properties": {"animate": pb.lit_bool(True), "speed": pb.lit_num(1)}}])
        check("shared elements skip an excluded page", not os.path.isdir(os.path.join(d, "pages", "Home",
                                                                                      "visuals", "brand")))
        eq("a shared textbox takes the page's title",
           visual("detail", "brand")["visual"]["objects"]["general"][0]["properties"]["paragraphs"][0]
           ["textRuns"][0]["value"],
           "Detail")
        own = visual("Overview", "nav_overview")["visual"]
        other = visual("detail", "nav_overview")["visual"]
        check("the tab of the current page is not a link", "visualLink" not in own["visualContainerObjects"])
        check("and is restyled", own["objects"]["fill"][1]["properties"]["fillColor"] == pb.color("#1F6FEB"))
        check("on other pages it is a link", "visualLink" in other["visualContainerObjects"])
        eq("a shared button opens that page's own pop-up",
           visual("detail", "open_filters")["visual"]["visualContainerObjects"]["visualLink"][0]
           ["properties"]["bookmark"],
           pb.lit_str("detail_filters_open"))

        group = visual("Overview", "filters")
        eq("a pop-up is a hidden group over the page", [group["isHidden"], group["position"]["width"],
                                                        group["position"]["height"]],
           [True, 1280, 720])
        veil = visual("Overview", "filters_veil")
        eq("its veil closes it",
           veil["visual"]["visualContainerObjects"]["visualLink"][0]["properties"]["bookmark"],
           pb.lit_str("Overview_filters_close"))
        members = [visual("Overview", n) for n in ("filters_veil", "filters_card", "filters_title",
                                                   "filters_close", "f_year",
                                                   "f_segment")]
        check("its members belong to it", all(m["parentGroupName"] == "filters" for m in members))
        zs = [m["position"]["z"] for m in members]
        check("they are layered in order, above the page", zs == sorted(zs) and min(zs) >= 40000)
        check("page content sits below shared chrome", visual("Overview", "trend")["position"]["z"]
              < visual("Overview", "brand")["position"]["z"] < min(zs))
        opened = load(os.path.join(d, "bookmarks", "Overview_filters_open.bookmark.json"))
        eq("the open bookmark only shows the group",
           [opened["options"]["targetVisualNames"], opened["options"]["suppressData"],
            opened["explorationState"]["sections"]["Overview"]["visualContainerGroups"]],
           [["filters"], True, {"filters": {"isHidden": False}}])
        hide = load(os.path.join(d, "bookmarks",
                                 "HideDetails.bookmark.json"))["explorationState"]["sections"]["Overview"]
        eq("a bookmark can hide a group and a single visual", [hide["visualContainerGroups"],
                                                               hide["visualContainers"]],
           [{"details": {"isHidden": True}}, {"panel": {"singleVisual": {"visualType": "shape",
                                                                         "display": {"mode": "hidden"}}}}])
        eq("an untargeted bookmark opens its page", load(os.path.join(d, "bookmarks",
                                                                      "Start.bookmark.json"))["options"],
           {"targetVisualNames": [], "suppressData": True})
        eq("a group member", [visual("Overview", "amounts")["parentGroupName"],
                              visual("Overview", "amounts")["position"]["z"] - visual("Overview",
                                                                                 "details")["position"]["z"]],
           ["details", 1])
        eq("a table's fields", [list(p["field"])
                                for p in visual("detail", "orders")["visual"]["query"]["queryState"]["Values"]
                                ["projections"]], [["Column"], ["Column"], ["Measure"]])
        eq("a tile slicer synced by its field", visual("Overview",
                                                       "f_year")["visual"]["syncGroup"]["groupName"],
                                                       "Calendar_Year")
        eq("the manifest lists what is managed", sorted(load(os.path.join(base,
                                                              "Demo.Report.spec-manifest.json"))["bookmarks"],
                                                        key=str.lower)[:3], ["detail_filters_close",
                                                                             "detail_filters_open",
                                                                             "HideDetails"])

        again = rs.apply_report_spec(root, spec, model_fields=MODEL)
        eq("a second apply writes nothing", [again["success"], again["written"], again["removed"]], [True, [],
                                                                                                     []])
        eq("and counts every file as unchanged", again["unchanged"], len(result["written"]))
        eq("the folder is byte for byte the same", snapshot(base), files)


def test_dry_run_and_failures():
    print("\n== dry run, failed validation, folder checks ==")
    with tempfile.TemporaryDirectory() as base:
        root, pbiviz = prepared(base)
        before = snapshot(base)
        plan = rs.apply_report_spec(root, full_spec(pbiviz), model_fields=MODEL, dry_run=True)
        eq("a dry run succeeds", [plan["success"], plan["dry_run"], plan["errors"]], [True, True, []])
        eq("and changes nothing", snapshot(base), before)
        check("no folder was made either", not os.path.exists(os.path.join(root, "definition")))
        real = rs.apply_report_spec(root, full_spec(pbiviz), model_fields=MODEL)
        eq("the plan is what an apply then writes", plan["written"], real["written"])

    with tempfile.TemporaryDirectory() as base:
        root = make_report(base)
        hand_made(root)
        before = snapshot(base)
        spec = small_spec()
        spec["pages"].append({"name": "second", "elements": [{"name": "bad", "type": "html", "x": 0, "y": 0,
                                                              "width": 10,
                                                              "height": 10, "measure": "Sales.Missing"}]})
        failed = rs.apply_report_spec(root, spec, model_fields=MODEL)
        eq("one bad field fails the whole apply", failed["success"], False)
        has("and says which", failed["errors"], "Sales[Missing] is not in the model")
        eq("nothing was written", [failed["written"], snapshot(base)], [[], before])
        failed = rs.apply_report_spec(root, {"pages": "all of them"})
        eq("a malformed spec fails cleanly", [failed["success"], snapshot(base) == before], [False, True])
        spec = small_spec()
        spec["pages"][0]["elements"].append({"name": "logo", "type": "image", "x": 0, "y": 0, "width": 10,
                                             "height": 10,
                                             "resource": "absent.png"})
        failed = rs.apply_report_spec(root, spec)
        has("an image file that is not there", failed["errors"],
            "'absent.png' is not in StaticResources/RegisteredResources")
        spec = small_spec()
        spec["custom_visuals"] = [{"pbiviz": "absent.pbiviz"}]
        has("a package that is not there", rs.apply_report_spec(root, spec)["errors"],
            "custom visual package not found")
        eq("still nothing written", snapshot(base), before)

    with tempfile.TemporaryDirectory() as base:
        missing = rs.apply_report_spec(os.path.join(base, "Absent.Report"), small_spec())
        has("a folder that does not exist", missing["errors"], "is not a report folder")
        bare = os.path.join(base, "Bare.Report")
        os.makedirs(bare)
        has("a folder with no definition.pbir", rs.apply_report_spec(bare, small_spec())["errors"],
            "contain definition.pbir")
        eq("which is not created for you", os.listdir(bare), [])
        legacy = make_report(base, "Legacy.Report")
        write_raw(os.path.join(legacy, "report.json"), b"{}")
        has("the older single-file format", rs.apply_report_spec(legacy, small_spec())["errors"],
            "older single-file format")
        long_guid = "v" + "x" * 130
        make_pbiviz(os.path.join(base, "long.pbiviz"), guid=long_guid)
        root = make_report(base, "Long.Report")
        spec = small_spec()
        spec["custom_visuals"] = [{"pbiviz": "long.pbiviz", "alias": "long"}]
        failed = rs.apply_report_spec(root, spec)
        has("a custom visual whose path would be too long", failed["errors"], "characters or more")
        check("and nothing was unpacked", not os.path.exists(os.path.join(root, "CustomVisuals")))
        broken = make_report(base, "Broken.Report")
        write_raw(os.path.join(broken, "definition", "report.json"), b"{ not json")
        has("an unreadable report.json", rs.apply_report_spec(broken, small_spec())["errors"],
            "cannot be read as JSON")
        eq("which is left as it was", snapshot(broken)["definition/report.json"], b"{ not json")

    with tempfile.TemporaryDirectory() as base:
        # A file sits where a visual's folder has to go, so the write fails part of the way through.
        root = make_report(base)
        rs.apply_report_spec(root, small_spec())
        spec = small_spec()
        spec["pages"][0]["elements"][0]["text"] = "Changed"
        box = {"type": "shape", "x": 0, "y": 0, "width": 10, "height": 10}
        spec["pages"][0]["elements"].append({"name": "extra", **box})
        spec["pages"].append({"name": "second", "elements": [
            {"name": "inner", "type": "shape", "x": 0, "y": 0, "width": 10, "height": 10}]})
        write_raw(os.path.join(root, "definition", "pages", "overview", "visuals", "extra"), b"in the way")
        before = snapshot(base)
        failed = rs.apply_report_spec(root, spec)
        eq("a write that fails reports it", [failed["success"], failed["written"]], [False, []])
        has("in plain words", failed["errors"], "nothing was changed: writing failed")
        eq("every file is as it was", snapshot(base), before)
        check("and no folder was left behind for the page that never arrived",
              not os.path.exists(os.path.join(root, "definition", "pages", "second")))
        tmp_files = [n for _f, _d, names in os.walk(base) for n in names if n.endswith(".tmp")]
        eq("nor a temp file", tmp_files, [])


def test_existing_report():
    print("\n== a report that already has hand-made pages ==")
    with tempfile.TemporaryDirectory() as base:
        root = make_report(base)
        hand_made(root)
        before = snapshot(root)
        spec = small_spec()
        spec["theme"] = {"name": "House"}
        spec["pages"].append({"name": "legacy", "existing": True, "elements": [
            {"name": "banner", "type": "shape", "x": 0, "y": 0, "width": 1280, "height": 48,
             "fill": "#1F6FEB"},
            {"name": "to_overview", "type": "button", "x": 8, "y": 8, "width": 100, "height": 32,
             "text": "Overview",
             "action": {"type": "page", "target": "overview"}}]})
        spec["pages"][0]["elements"].append({"name": "to_legacy", "type": "button", "x": 900, "y": 24,
                                             "width": 100,
                                             "height": 32, "text": "Legacy", "action": {"type": "page",
                                                                                        "target": "LEGACY"}})
        spec["pages"][0]["elements"].append({"name": "reset", "type": "button", "x": 1010, "y": 24,
                                             "width": 100,
                                             "height": 32, "text": "Mine", "action": {"type": "bookmark",
                                                                                      "target": "mine"}})
        spec["bookmarks"] = [{"name": "hide_heading", "page": "overview", "hide": ["heading"]}]
        result = rs.apply_report_spec(root, spec)
        eq("it applies", [result["success"], result["errors"]], [True, []])
        after = snapshot(root)
        for path in ("definition/pages/legacy/page.json",
                     "definition/pages/legacy/visuals/oldChart/visual.json",
                     "definition/pages/legacy/visuals/oldText/visual.json",
                     "definition/bookmarks/mine.bookmark.json"):
            check(f"{path} is untouched", after[path] == before[path])
        check("elements were added to the hand-made page",
              "definition/pages/legacy/visuals/banner/visual.json" in after)
        pages_json = after["definition/pages/pages.json"]
        check("pages.json keeps its CRLF line ends",
              b"\r\n" in pages_json and b"\n" not in pages_json.replace(b"\r\n", b""))
        meta = json.loads(pages_json)
        eq("existing pages stay first and its other keys stay", [meta["pageOrder"], meta["activePageName"],
                                                                 meta["landingPageName"]],
           [["legacy", "overview"], "legacy", "legacy"])
        eq("new visuals take the newest schema in the project",
           load(os.path.join(root, "definition", "pages", "overview", "visuals", "heading",
                             "visual.json"))["$schema"],
           "https://example.test/definition/visualContainer/2.14.0/schema.json")
        eq("new pages too", load(os.path.join(root, "definition", "pages", "overview",
                                              "page.json"))["$schema"],
           "https://example.test/definition/page/2.1.0/schema.json")
        eq("and bookmarks", load(os.path.join(root, "definition", "bookmarks",
                                              "hide_heading.bookmark.json"))["$schema"],
           "https://example.test/definition/bookmark/2.1.0/schema.json")
        eq("bookmarks.json keeps the hand-made one first", [i["name"] for i in load(os.path.join(
            root, "definition", "bookmarks", "bookmarks.json"))["items"]], ["mine", "hide_heading"])
        report = load(os.path.join(root, "definition", "report.json"))
        eq("report.json keeps its settings and takes the theme", [report["settings"],
                                                            report["themeCollection"]["customTheme"]["name"]],
           [{"exportDataMode": "AllowSummarized"}, "House.json"])
        eq("a link to a hand-made page uses the name as given", load(os.path.join(
            root, "definition", "pages", "overview", "visuals", "to_legacy", "visual.json"))
            ["visual"]["visualContainerObjects"]["visualLink"][0]["properties"]["navigationSection"],
            pb.lit_str("LEGACY"))
        manifest = load(os.path.join(base, "Demo.Report.spec-manifest.json"))
        eq("the manifest knows the page is not its own", manifest["pages"]["legacy"],
           {"owned": False, "elements": ["banner", "to_overview"]})
        eq("and remembers the theme that was there", manifest["theme"]["previous"],
           {"name": "handmade.json", "type": "RegisteredResources"})
        state = snapshot(base)
        eq("applying again changes nothing", [rs.apply_report_spec(root, spec)["written"], snapshot(base)],
           [[], state])

        # prune with the hand-made things still in the way
        pruned = rs.apply_report_spec(root, {"pages": [{"name": "legacy", "existing": True}]}, prune=True)
        eq("prune succeeds", [pruned["success"], pruned["errors"]], [True, []])
        after = snapshot(root)
        check("the hand-made page and its visuals survive a prune", all(after.get(p) == before[p] for p in (
            "definition/pages/legacy/page.json", "definition/pages/legacy/visuals/oldChart/visual.json",
            "definition/pages/legacy/visuals/oldText/visual.json",
            "definition/bookmarks/mine.bookmark.json")))
        check("what the spec added is gone",
              not any("banner" in p or "overview" in p or "hide_heading" in p for p in after))
        eq("the hand-made theme is back", load(os.path.join(root, "definition",
                                                            "report.json"))["themeCollection"]["customTheme"],
           {"name": "handmade.json", "type": "RegisteredResources"})
        eq("pages.json and bookmarks.json list what is left",
           [json.loads(after["definition/pages/pages.json"])["pageOrder"],
            [i["name"] for i in json.loads(after["definition/bookmarks/bookmarks.json"])["items"]]],
           [["legacy"], ["mine"]])
        eq("no empty folder is left behind", sorted(os.listdir(os.path.join(root, "definition", "pages"))),
           ["legacy", "pages.json"])
        eq("nor an empty visual folder", sorted(os.listdir(os.path.join(root, "definition", "pages", "legacy",
                                                                        "visuals"))),
           ["oldChart", "oldText"])

    with tempfile.TemporaryDirectory() as base:
        root = make_report(base)
        hand_made(root)
        before = snapshot(base)
        clash = {"pages": [{"name": "Legacy", "elements": []}]}
        failed = rs.apply_report_spec(root, clash)
        has("a page name that is taken", failed["errors"],
            "already exists and was not created by an earlier apply")
        clash = {"pages": [{"name": "legacy", "existing": True, "elements": [
            {"name": "OLDCHART", "type": "shape", "x": 0, "y": 0, "width": 10, "height": 10}]}]}
        has("a visual name that is taken", rs.apply_report_spec(root, clash)["errors"],
            "a visual of that name is already on the page")
        clash = small_spec()
        clash["bookmarks"] = [{"name": "Mine", "page": "overview", "hide": ["heading"]}]
        has("a bookmark name that is taken", rs.apply_report_spec(root, clash)["errors"],
            "bookmark 'Mine' already exists")
        has('a page marked "existing" that is not there', rs.apply_report_spec(root,
                                                                               {"pages": [{"name": "ghost",
                                                                                          "existing": True}]})
            ["errors"], 'is marked "existing" but is not in the report')
        eq("none of that wrote anything", snapshot(base), before)
        adopted = rs.apply_report_spec(root, {"pages": [{"name": "legacy", "display_name": "Taken over",
                                                         "elements": [
            {"name": "oldChart", "type": "shape", "x": 0, "y": 0, "width": 10, "height": 10}]}]}, adopt=True)
        eq("adopt takes the page over", [adopted["success"], load(os.path.join(root, "definition", "pages",
                                                                               "legacy", "page.json"))
                                         ["displayName"]], [True, "Taken over"])
        taken = load(os.path.join(root, "definition", "pages", "legacy", "visuals", "oldChart",
                                  "visual.json"))
        eq("and the visual, keeping what the spec has no say over", [taken["visual"]["visualType"],
                                                                     taken["howCreated"]],
           ["shape", "InsertVisualButton"])
        check("a file it replaced keeps its CRLF line ends",
              b"\r\n" in snapshot(root)["definition/pages/legacy/visuals/oldChart/visual.json"])


def test_prune():
    print("\n== prune ==")
    with tempfile.TemporaryDirectory() as base:
        root, pbiviz = prepared(base)
        first = full_spec(pbiviz)
        rs.apply_report_spec(root, first, model_fields=MODEL)
        d = os.path.join(root, "definition")
        # someone adds a visual of their own to a page the spec created
        mine = {"$schema": pb.DEFAULT_SCHEMAS["visual"], "name": "mine", "position": {"x": 0, "y": 0, "z": 7,
                                                                                      "width": 10,
                                                                                    "height": 10,
                                                                                    "tabOrder": 7},
                "visual": {"visualType": "card"}}
        write_raw(os.path.join(d, "pages", "detail", "visuals", "mine", "visual.json"),
                  json.dumps(mine).encode())
        write_raw(os.path.join(d, "pages", "Overview", "visuals", "trend", "mobile.json"), b"{}")

        second = copy.deepcopy(first)
        del second["theme"], second["custom_visuals"], second["shared"]
        second["pages"] = [p for p in second["pages"] if p["name"] == "Overview"]
        second["pages"][0]["elements"] = [e for e in second["pages"][0]["elements"] if e["name"] in ("panel",
                                                                                                     "kpi")]
        second["bookmarks"] = [{"name": "HideDetails", "display_name": "Hide panel", "page": "Overview",
                                "hide": ["panel"]}]
        second["page_order"], second["active_page"] = ["Overview"], "Overview"

        kept = rs.apply_report_spec(root, second, model_fields=MODEL)
        eq("without prune nothing is removed", [kept["success"], kept["removed"]], [True, []])
        check("the dropped page is still there", os.path.isfile(os.path.join(d, "pages", "Home",
                                                                             "page.json")))
        manifest = load(os.path.join(base, "Demo.Report.spec-manifest.json"))
        check("and still tracked, for a later prune",
              "Home" in manifest["pages"] and "trend" in manifest["pages"]["Overview"]["elements"]
              and "ShowDetails" in manifest["bookmarks"] and GUID in manifest["custom_visuals"])
        eq("leftovers do not break the report", self_check(root), [])

        plan = rs.apply_report_spec(root, second, model_fields=MODEL, prune=True, dry_run=True)
        check("a prune dry run removes nothing", os.path.isfile(os.path.join(d, "pages", "Home",
                                                                             "page.json")))
        pruned = rs.apply_report_spec(root, second, model_fields=MODEL, prune=True)
        eq("prune succeeds", [pruned["success"], pruned["errors"]], [True, []])
        eq("and removes what the dry run said", pruned["removed"], plan["removed"])
        eq("pages left", sorted(os.listdir(os.path.join(d, "pages"))), ["Overview", "detail", "pages.json"])
        eq("visuals left on the kept page", sorted(os.listdir(os.path.join(d, "pages", "Overview",
                                                                           "visuals"))), ["kpi", "panel"])
        check("a visual's other files go with it",
              "definition/pages/Overview/visuals/trend/mobile.json" in pruned["removed"])
        has("a page holding someone else's visual is kept", pruned["warnings"],
            "holds visuals this tool did not create (mine)")
        eq("with only that visual on it", sorted(os.listdir(os.path.join(d, "pages", "detail", "visuals"))),
           ["mine"])
        eq("bookmarks left", sorted(os.listdir(os.path.join(d, "bookmarks"))), ["HideDetails.bookmark.json",
                                                                                "bookmarks.json"])
        report = load(os.path.join(d, "report.json"))
        eq("the theme, the package and unused AppSource ids are unregistered",
           [report["themeCollection"], [p["name"] for p in report["resourcePackages"]],
            report["publicCustomVisuals"],
            [i["name"] for i in report["resourcePackages"][0]["items"]]],
           [{}, ["RegisteredResources"], [], []])
        check("their files are gone", not os.path.exists(os.path.join(root, "CustomVisuals"))
              and os.listdir(os.path.join(root, "StaticResources", "RegisteredResources")) == ["ground.png",
                                                                                               "logo.png"])
        meta = load(os.path.join(d, "pages", "pages.json"))
        eq("pages.json follows", [meta["pageOrder"], meta["activePageName"]], [["Overview", "detail"],
                                                                               "Overview"])
        eq("the report is still sound", self_check(root), [])
        state = snapshot(base)
        again = rs.apply_report_spec(root, second, model_fields=MODEL, prune=True)
        eq("pruning again does nothing", [again["written"], again["removed"], snapshot(base)], [[], [],
                                                                                                state])

        os.remove(os.path.join(d, "pages", "detail", "visuals", "mine", "visual.json"))
        os.rmdir(os.path.join(d, "pages", "detail", "visuals", "mine"))
        final = rs.apply_report_spec(root, second, model_fields=MODEL, prune=True)
        eq("once it is empty the page goes too", [final["removed"], sorted(os.listdir(os.path.join(d,
                                                                                                  "pages")))],
           [["definition/pages/detail/page.json"], ["Overview", "pages.json"]])

    with tempfile.TemporaryDirectory() as base:
        root = make_report(base)
        spec = small_spec()
        spec["theme"] = {"name": "First"}
        rs.apply_report_spec(root, spec)
        spec["theme"] = {"name": "Second"}
        renamed = rs.apply_report_spec(root, spec)
        resources = os.path.join(root, "StaticResources", "RegisteredResources")
        eq("a renamed theme leaves the old file until a prune", [renamed["removed"],
                                                                 sorted(os.listdir(resources))],
           [[], ["First.json", "Second.json"]])
        eq("but no longer registers it", [i["name"] for i in load(os.path.join(root, "definition",
                                                                               "report.json"))
                                         ["resourcePackages"][0]["items"]], ["Second.json"])
        pruned = rs.apply_report_spec(root, spec, prune=True)
        eq("the prune removes it", [pruned["removed"], os.listdir(resources)],
           [["StaticResources/RegisteredResources/First.json"], ["Second.json"]])

    with tempfile.TemporaryDirectory() as base:
        root = make_report(base)
        elsewhere = os.path.join(base, "state", "my-manifest.json")
        os.makedirs(os.path.dirname(elsewhere))
        result = rs.apply_report_spec(root, small_spec(), manifest_path=elsewhere)
        eq("the manifest can be kept elsewhere", [result["success"], os.path.isfile(elsewhere),
                                                  os.listdir(base)],
           [True, True, ["Demo.Report", "state"]])
        lost = rs.apply_report_spec(root, small_spec())
        has("without its manifest the spec's own pages look foreign", lost["errors"],
            "was not created by an earlier apply")
        eq("adopt recovers from that", rs.apply_report_spec(root, small_spec(), adopt=True)["success"], True)


def test_carry_over():
    print("\n== what Desktop added to a managed file ==")
    with tempfile.TemporaryDirectory() as base:
        root = make_report(base)
        rs.apply_report_spec(root, small_spec())
        vfile = os.path.join(root, "definition", "pages", "overview", "visuals", "by_month", "visual.json")
        pfile = os.path.join(root, "definition", "pages", "overview", "page.json")
        visual, page = load(vfile), load(pfile)
        visual["filterConfig"] = {"filters": [{"name": "f1"}]}
        visual["visual"]["objects"] = {"legend": [{"properties": {"show": pb.lit_bool(True)}}]}
        page["filterConfig"] = {"filters": [{"name": "p1"}]}
        page["displayName"] = "Renamed in Desktop"
        # re-saved the way Desktop lays a file out, which is not this module's layout
        write_raw(vfile, json.dumps(visual, indent=4).replace("\n", "\r\n").encode())
        write_raw(pfile, json.dumps(page, indent=4).replace("\n", "\r\n").encode())
        heading = os.path.join(root, "definition", "pages", "overview", "visuals", "heading", "visual.json")
        write_raw(heading, json.dumps(load(heading), indent=4).replace("\n", "\r\n").encode())
        reformatted = snapshot(root)["definition/pages/overview/visuals/heading/visual.json"]

        result = rs.apply_report_spec(root, small_spec())
        eq("only the two changed files are rewritten", result["written"], [
            "definition/pages/overview/page.json", "definition/pages/overview/visuals/by_month/visual.json"])
        eq("a file that was only reformatted is left alone",
           snapshot(root)["definition/pages/overview/visuals/heading/visual.json"], reformatted)
        visual, page = load(vfile), load(pfile)
        eq("a visual-level filter survives", visual["filterConfig"], {"filters": [{"name": "f1"}]})
        check("formatting the spec does not have is reset", "objects" not in visual["visual"])
        eq("a page-level filter survives, the spec's name wins", [page["filterConfig"], page["displayName"]],
           [{"filters": [{"name": "p1"}]}, "Overview"])
        check("the rewritten files keep the line ends they had",
              b"\r\n" in snapshot(root)["definition/pages/overview/page.json"])
        eq("and a further apply is quiet", rs.apply_report_spec(root, small_spec())["written"], [])


def test_layering_and_tokens():
    print("\n== layering ==")
    spec = {"pages": [{"name": "p", "elements": [
        {"name": "a", "type": "shape", "x": 0, "y": 0, "width": 10, "height": 10},
        {"name": "g", "type": "group", "elements": [
            {"name": f"m{i}", "type": "shape", "x": 0, "y": 0, "width": 10,
             "height": 10} for i in range(12)]},
        {"name": "b", "type": "shape", "x": 0, "y": 0, "width": 10, "height": 10},
        {"name": "top", "type": "shape", "x": 0, "y": 0, "width": 10, "height": 10, "z": 99999,
         "tab_order": 1}]}]}
    compiled = rs._compile(spec)
    z = {el.name: el.z for el in compiled.pages[0].elements}
    eq("declaration order is layer order", [z["a"], z["g"], z["m0"], z["m11"], z["b"]], [1000, 1010, 1011,
                                                                                         1022, 1030])
    eq("an explicit z is kept", z["top"], 99999)
    eq("and the tab order can differ", compiled.pages[0].elements[-1].doc["position"]["tabOrder"], 1)
    eq("a group spans the page", [compiled.pages[0].elements[1].doc["position"][k] for k in ("x", "y",
                                                                                             "width",
                                                                                             "height")],
       [0, 0, 1280, 720])
    spec["pages"][0]["elements"][2]["z"] = 1000
    has("two elements on one z are reported", rs._compile(spec).warnings, "share z 1000")
    spec = small_spec()
    spec["pages"].append({"name": "kept", "existing": True, "background": "#FFFFFF"})
    has("a setting that cannot apply to an existing page is reported", rs._compile(spec).warnings,
        "background had no effect")
    spec = small_spec()
    spec["pages"][0]["elements"][0]["colour"] = "#FFFFFF"
    has("a misspelt setting is reported, not swallowed", rs._compile(spec).warnings,
        "'colour' is not a setting of a textbox")
    spec = small_spec()
    spec["shared"] = [{"name": "s", "pages": "*", "elements": [
        {"name": "d", "type": "deneb", "x": 0, "y": 0, "width": 10, "height": 10,
         "fields": [{"column": "T.C"}],
         "title": "{page_title}", "spec": {"title": "{page}"}}]}]
    doc = rs._compile(spec).pages[0].elements[-1].doc
    eq("tokens are replaced in settings",
       doc["visual"]["visualContainerObjects"]["title"][0]["properties"]["text"],
       pb.lit_str("Overview"))
    check("but a Deneb spec is left alone",
          '{"title":"{page}"}' in doc["visual"]["objects"]["vega"][0]["properties"]["jsonSpec"]
          ["expr"]["Literal"]["Value"])


if __name__ == "__main__":
    for fn in (test_validation, test_documented_example, test_fields, test_full_spec,
               test_dry_run_and_failures, test_existing_report, test_prune, test_carry_over,
               test_layering_and_tokens):
        fn()
    print()
    if _failures:
        print(f"FAILED: {len(_failures)} check(s): {_failures}")
        sys.exit(1)
    print("ALL REPORT SPEC TESTS PASSED")
