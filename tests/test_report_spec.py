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


def make_pbiviz(path, guid=GUID, extra_member=None, capabilities=None):
    package = {"version": "1.2.0.0", "visual": {"name": "homeVisual", "guid": guid, "version": "1.2.0.0"},
               "resources": [{"resourceId": "rId0", "sourceType": 5,
                              "file": f"resources/{guid}.pbiviz.json"}]}
    meta = {"visual": package["visual"], "content": {"js": "x"}}
    if capabilities is not None:
        meta["capabilities"] = capabilities
    with zipfile.ZipFile(path, "w") as z:
        z.writestr("package.json", json.dumps(package, indent="\t").replace("\n", "\r\n"))
        z.writestr(f"resources/{guid}.pbiviz.json", json.dumps(meta))
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
        # a group stands in for the bookmarks it holds
        listed = sorted(name for i in load(os.path.join(bdir, "bookmarks.json"))["items"]
                        for name in i.get("children", [i["name"]]))
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
    filter_names = []
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
                target = p.get("navigationSection") or p.get("bookmark") or p.get("drillthroughSection")
                target = target["expr"]["Literal"]["Value"].strip("'") if target else ""
                if kind in ("'PageNavigation'", "'Drillthrough'") and target not in page_names:
                    problems.append(f"{where} links to a page that does not exist")
                if kind == "'Bookmark'" and target not in bookmark_names:
                    problems.append(f"{where} links to a bookmark that does not exist")
            for tip in v.get("visual", {}).get("visualContainerObjects", {}).get("visualTooltip", []):
                section = tip["properties"].get("section")
                if section and section["expr"]["Literal"]["Value"].strip("'") not in page_names:
                    problems.append(f"{where} shows a tooltip page that does not exist")
            mobile = os.path.join(vdir, name, "mobile.json")
            phone = load(mobile).get("position") if os.path.isfile(mobile) else None
            if phone is not None and not all(isinstance(phone.get(k), (int, float))
                                             for k in ("x", "y", "width", "height")):
                problems.append(f"{where} has an incomplete phone position")
        filter_names += [f["name"] for v in visuals.values()
                         for f in v.get("filterConfig", {}).get("filters", [])]
        own = [f["name"] for f in page.get("filterConfig", {}).get("filters", [])]
        filter_names += own
        for parameter in page.get("pageBinding", {}).get("parameters", []):
            if parameter["boundFilter"] not in own:
                problems.append(f"page {page_name} binds a filter it does not have")
        bound_as = page.get("pageBinding", {}).get("type")
        if page.get("type") == "Drillthrough" and bound_as != "Drillthrough":
            problems.append(f"page {page_name} is a drillthrough page without a drillthrough binding")
        if bound_as in ("Drillthrough", "Tooltip") and page.get("type") != bound_as:
            problems.append(f"page {page_name} has a {bound_as} binding and is not that type of page")
        for pair in page.get("visualInteractions", []):
            ends = (pair["source"], pair["target"])
            if ends[0] == ends[1] or not all(end in visuals for end in ends):
                problems.append(f"page {page_name} has an interaction between visuals that are not there")
    filter_names += [f["name"] for f in report.get("filterConfig", {}).get("filters", [])]
    if len(filter_names) != len({n.lower() for n in filter_names}):
        problems.append("two filters share a name")
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
        check("the custom visual's files are gone", not os.path.exists(os.path.join(root, "CustomVisuals")))
        eq("and the theme file, leaving the images",
           sorted(os.listdir(os.path.join(root, "StaticResources", "RegisteredResources"))),
           ["ground.png", "logo.png"])
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
                                                  sorted(os.listdir(base))],
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


# ---------------------------------------------------------------------------------------------
# native report features: filters, drillthrough, tooltip pages, interactions, phone layout, groups
# ---------------------------------------------------------------------------------------------
def chart(name, x=24, y=24, **more):
    return {"name": name, "type": "visual", "visual_type": "barChart", "x": x, "y": y, "width": 300,
            "height": 200, "fields": {"Category": "Customer.Segment", "Y": "Sales.Total Sales"}, **more}


def native_spec():
    """One spec that uses every native report feature."""
    return {
        "filters": [{"name": "years", "field": "Calendar.Year", "in": [2025, 2026]}],
        "pages": [
            {"name": "overview", "display_name": "Overview",
             "filters": [{"field": "Customer.Segment", "not_in": ["Internal"], "display_name": "Segments",
                          "locked": True}],
             "interactions": [{"from": "by_segment", "to": "*", "type": "none"},
                              {"from": "by_segment", "to": "by_month", "type": "filter"},
                              {"from": "by_month", "to": ["by_segment"], "type": "highlight"}],
             "elements": [
                 chart("by_segment", tooltip_page="TIP", mobile={"x": 0, "y": 0, "width": 323, "height": 200},
                       filters=[{"field": "Customer.Segment", "top": {"n": 3, "by": "Sales.Total Sales"}},
                                {"field": "Sales.Amount", "op": ">=", "value": 100, "hidden": True}]),
                 {"name": "by_month", "type": "visual", "visual_type": "lineChart", "x": 340, "y": 24,
                  "width": 300, "height": 200, "drill_filter_other_visuals": False,
                  "fields": {"Category": "Calendar.Month", "Y": "Sales.Total Sales"},
                  "filters": [{"field": {"measure": "Sales.Total Sales"}, "op": ">", "value": 0}]},
                 {"name": "total", "type": "visual", "visual_type": "card", "x": 660, "y": 24, "width": 300,
                  "height": 200, "fields": {"Values": "Sales.Total Sales"},
                  "mobile": {"x": 0, "y": 200, "width": 323, "height": 100, "z": 3}},
                 {"name": "open_detail", "type": "button", "x": 24, "y": 240, "width": 200, "height": 40,
                  "text": "Detail", "action": {"type": "drillthrough", "target": "DETAIL", "tooltip": "Open",
                                               "disabled_tooltip": "Pick a segment"}}]},
            {"name": "detail", "display_name": "Detail",
             "drillthrough": {"fields": ["Customer.Segment", "Calendar.Year"], "back_button": True},
             "elements": [{"name": "orders", "type": "visual", "visual_type": "tableEx", "x": 24, "y": 64,
                           "width": 600, "height": 400,
                           "fields": {"Values": [{"column": "Customer.Name"},
                                                 {"measure": "Sales.Total Sales"}]}}]},
            {"name": "tip", "display_name": "Tip", "tooltip": {"fields": ["Customer.Segment"]},
             "elements": [{"name": "tip_total", "type": "visual", "visual_type": "card", "x": 0, "y": 0,
                           "width": 320, "height": 240, "fields": {"Values": "Sales.Total Sales"}}]}],
        "bookmarks": [
            {"name": "hide_total", "page": "overview", "hide": ["total"]},
            {"name": "show_total", "page": "overview", "show": ["total"]},
            {"name": "alone", "page": "overview", "hide": ["by_month"]}],
        "bookmark_groups": [{"name": "views", "display_name": "Views", "bookmarks": ["show_total",
                                                                                     "hide_total"]}],
    }


NEW_KEYS = ("filterConfig", "pageBinding", "visualInteractions", "report_filters", "bookmark_groups",
            "element_filters", "enableHighlight", "enableContextMenu")


def test_old_specs_are_unchanged():
    print("\n== a spec without the new keys writes none of them ==")
    with tempfile.TemporaryDirectory() as base:
        root, pbiviz = prepared(base)
        rs.apply_report_spec(root, full_spec(pbiviz), model_fields=MODEL)
        files = snapshot(base)
        texts = {k: v.decode("utf-8") for k, v in files.items() if k.endswith(".json")}
        for key in NEW_KEYS:
            check(f"no file mentions {key}", not any(f'"{key}"' in t for t in texts.values()))
        check("no page took a page type", not any('"type": "Tooltip"' in t or '"type": "Drillthrough"' in t
                                                  for k, t in texts.items() if k.endswith("page.json")))
        check("no phone layout was written", not any(k.endswith("mobile.json") for k in files))
        manifest = load(os.path.join(base, "Demo.Report.spec-manifest.json"))
        eq("the manifest has the keys it always had", sorted(manifest),
           ["bookmarks", "custom_visuals", "images", "pages", "public_visuals", "stale_files", "theme",
            "version"])
        eq("and so does each page in it", {tuple(sorted(p)) for p in manifest["pages"].values()},
           {("elements", "owned")})
        eq("every visual still follows a drill elsewhere",
           {json.loads(t)["visual"]["drillFilterOtherVisuals"] for k, t in texts.items()
            if k.endswith("visual.json") and "visualGroup" not in t}, {True})


def test_filters():
    print("\n== filters on the report, a page and a visual ==")

    def compiled(change=None, model=MODEL):
        spec = native_spec()
        if change:
            change(spec)
        return rs._compile(spec, model_fields=model)

    def page_el(spec, i=0):
        return spec["pages"][0]["elements"][i]

    c = compiled()
    eq("the full spec compiles", c.errors, [])
    eq("and guesses nothing", c.warnings, [])
    overview = c.pages[0]
    eq("a report filter keeps the name it was given", [f["name"] for f in c.report_filters], ["years"])
    eq("whole numbers for an integer column", c.report_filters[0]["filter"]["Where"][0]["Condition"]["In"]
       ["Values"], [[{"Literal": {"Value": "2025L"}}], [{"Literal": {"Value": "2026L"}}]])
    page_filter = overview.filters[0]
    eq("a page filter", [page_filter["type"], page_filter["displayName"], page_filter["isLockedInViewMode"],
                         page_filter["howCreated"], "Not" in page_filter["filter"]["Where"][0]["Condition"]],
       ["Categorical", "Segments", True, "User", True])
    check("a generated name is 20 hex digits", bool(rs.re.match(r"^[0-9a-f]{20}$", page_filter["name"])))
    top, amount = overview.elements[0].filters
    eq("a top N filter on a field the visual shows is that field's own card",
       [top["type"], top["howCreated"]],
       ["TopN", "Auto"])
    eq("a filter on a field the visual does not show is the reader's", [amount["type"], amount["howCreated"],
                                                                        amount["isHiddenInViewMode"]],
       ["Advanced", "User", True])
    eq("a double column takes a double", amount["filter"]["Where"][0]["Condition"]["Comparison"]["Right"],
       {"Literal": {"Value": "100D"}})
    measure = overview.elements[1].filters[0]
    eq("a measure can filter a visual", [list(measure["field"]), measure["howCreated"]],
       [["Measure"], "Auto"])
    names = [f["name"] for f in c.report_filters + overview.filters + [top, amount, measure]]
    eq("every filter has its own name", len(set(names)), len(names))
    again = compiled()
    eq("the names are the same on every run", [f["name"] for f in again.pages[0].elements[0].filters],
       [top["name"], amount["name"]])
    check("an element with no filters declares none", c.pages[0].elements[2].filters is None)

    def two_on_one_field(s):
        page_el(s)["filters"] = [{"field": "Customer.Segment", "in": ["A"]},
                                 {"field": "Customer.Segment", "op": "contains", "value": "B"}]
    c = compiled(two_on_one_field)
    eq("two filters on one field get two names", [c.errors, len({f["name"] for f in c.pages[0].elements[0]
                                                                .filters})], [[], 2])

    def errors(change, model=MODEL):
        return compiled(change, model).errors

    has("a measure at page level", errors(lambda s: s["pages"][0]["filters"].append(
        {"field": "Sales.Total Sales", "op": ">", "value": 1})),
        "page 'overview', filters[1] on Sales[Total Sales]: a measure or an aggregated column can only "
        "filter one visual")
    has("a measure at report level", errors(lambda s: s["filters"].append(
        {"field": {"measure": "Sales.Total Sales"}, "op": ">", "value": 1})),
        "filters[1] on Sales[Total Sales]")
    has("top N at page level", errors(lambda s: s["pages"][0]["filters"].append(
        {"field": "Customer.Segment", "top": {"n": 3, "by": "Sales.Total Sales"}})),
        "a top N filter can only filter one visual; put this filter on an element")
    has("an aggregated column at report level", errors(lambda s: s["filters"].append(
        {"field": {"column": "Sales.Amount", "aggregation": "sum"}, "op": ">", "value": 1})),
        "a measure or an aggregated column can only filter one visual")
    has("a field that is not in the model", errors(lambda s: s["filters"].append(
        {"field": "Calendar.Decade", "in": [1990]})), "filters[1]: Calendar[Decade] is not in the model")
    has("a value that does not fit the column", errors(lambda s: s["filters"].append(
        {"field": "Calendar.Year", "in": ["this year"]})),
        "filters[1] on Calendar[Year]: 'this year' is text, but the field holds number values (int64)")
    has("a number for a text column, on an element", errors(lambda s: page_el(s)["filters"].append(
        {"field": "Customer.Name", "op": "=", "value": 7})),
        "page 'overview', element 'by_segment', filters[2] on Customer[Name]: 7 is a number")
    has("a filter with no field", errors(lambda s: s["filters"].append({"in": [1]})),
        'filters[1]: a filter needs "field"')
    has("a filter that is not an object", errors(lambda s: s["filters"].append("Calendar.Year")),
        "filters[1] must be an object")
    has("filters that are not a list", errors(lambda s: s["pages"][0].update(filters={})),
        "page 'overview', filters must be a list")
    has("an unknown filter key", errors(lambda s: s["filters"][0].update(values=[1])),
        "unknown filter key(s) ['values']")
    has("a filter name that cannot be used", errors(lambda s: s["filters"][0].update(name="my filter")),
        "'my filter' is not a usable filter name")
    has("one name on two filters, whatever the level", errors(lambda s: s["pages"][0]["filters"][0].update(
        name="YEARS")), "the filter name 'YEARS' is already used by filters[0]")
    has("top N by a plain column", errors(lambda s: page_el(s)["filters"][0]["top"].update(
        by={"column": "Customer.Name"}), model=None),
        "top ranks by a measure or an aggregated column; Customer[Name] is a plain column")
    has("top N with no measure", errors(lambda s: page_el(s)["filters"][0]["top"].pop("by")), 'needs "by"')
    has("a relative date on a column that is not a date", errors(lambda s: s["filters"].append(
        {"field": "Calendar.Month", "relative_date": {"last": 3, "unit": "months"}})),
        "needs a date column, and this one is string")
    c = compiled(lambda s: page_el(s)["filters"][0]["top"].update(by="Sales.Amount"))
    ranked = c.pages[0].elements[0].filters[0]["filter"]["From"][0]["Expression"]["Subquery"]["Query"]
    eq("a column to rank by is summed, as in a value well",
       [c.errors, list(ranked["OrderBy"][0]["Expression"])], [[], ["Aggregation"]])
    c = compiled(lambda s: s["pages"][0]["elements"].append(
        {"name": "note", "type": "textbox", "x": 0, "y": 300, "width": 10, "height": 10, "text": "x",
         "filters": [{"field": "Customer.Segment", "in": ["A"]}]}))
    has("filters on something that shows no data are reported", c.warnings,
        "'filters' is not a setting of a textbox")
    c = compiled(model=CATALOG)
    eq("with names only the spec still compiles", c.errors, [])
    eq("and report filters are taken as columns without a word",
       [w for w in c.warnings if "Calendar[Year]" in w],
       [])

    with tempfile.TemporaryDirectory() as base:
        root = make_report(base)
        spec = native_spec()
        result = rs.apply_report_spec(root, spec, model_fields=MODEL)
        eq("it applies", [result["success"], result["errors"]], [True, []])
        eq("what it writes is sound", self_check(root), [])
        d = os.path.join(root, "definition")
        report_file = os.path.join(d, "report.json")
        page_file = os.path.join(d, "pages", "overview", "page.json")
        visual_file = os.path.join(d, "pages", "overview", "visuals", "by_segment", "visual.json")

        def filters_of(path):
            return [f["name"] for f in load(path).get("filterConfig", {}).get("filters", [])]

        eq("the report filter is in report.json", filters_of(report_file), ["years"])
        eq("the page filter in page.json", len(filters_of(page_file)), 1)
        eq("the visual's filters in visual.json", len(filters_of(visual_file)), 2)
        manifest_file = os.path.join(base, "Demo.Report.spec-manifest.json")
        manifest = load(manifest_file)
        eq("the manifest names every filter it made",
           [manifest["report_filters"], manifest["pages"]["overview"]["filters"],
            manifest["pages"]["overview"]["element_filters"]["by_segment"]],
           [["years"], filters_of(page_file), filters_of(visual_file)])
        state = snapshot(base)
        again = rs.apply_report_spec(root, spec, model_fields=MODEL)
        eq("a second apply writes nothing", [again["written"], again["removed"], snapshot(base)],
           [[], [], state])

        # someone adds filters of their own in Desktop, at all three levels
        for path, theirs in ((report_file, "desktop1"), (page_file, "desktop2"), (visual_file, "desktop3")):
            doc = load(path)
            doc["filterConfig"]["filters"].insert(0, {"name": theirs, "type": "Categorical",
                                                      "howCreated": "User"})
            doc["filterConfig"]["filterSortOrder"] = "Custom"
            write_raw(path, json.dumps(doc, indent=2).encode())
        spec2 = native_spec()
        spec2["filters"] = [{"name": "years", "field": "Calendar.Year", "in": [2024]},
                            {"name": "months", "field": "Calendar.Month", "in": ["May"]}]
        spec2["pages"][0]["filters"][0]["locked"] = False
        spec2["pages"][0]["elements"][0]["filters"].pop(0)
        plan = rs.apply_report_spec(root, spec2, model_fields=MODEL, dry_run=True)
        changed = rs.apply_report_spec(root, spec2, model_fields=MODEL)
        eq("a changed filter rewrites only the files that hold it", changed["written"], [
            "../Demo.Report.spec-manifest.json", "definition/pages/overview/page.json",
            "definition/pages/overview/visuals/by_segment/visual.json", "definition/report.json"])
        eq("as the dry run said", plan["written"], changed["written"])
        eq("the Desktop filter keeps its place, ours is updated beside it, a new one goes last",
           filters_of(report_file), ["desktop1", "years", "months"])
        eq("the new values are in", load(report_file)["filterConfig"]["filters"][1]["filter"]["Where"][0]
           ["Condition"]["In"]["Values"], [[{"Literal": {"Value": "2024L"}}]])
        eq("a filter the spec dropped is gone from the visual, the Desktop one stays",
           filters_of(visual_file), ["desktop3", amount["name"]])
        check("an option switched off leaves the file", "isLockedInViewMode" not in load(page_file)
              ["filterConfig"]["filters"][1])
        eq("the file's other filter settings stay", load(page_file)["filterConfig"]["filterSortOrder"],
           "Custom")
        eq("the manifest follows", load(manifest_file)["report_filters"], ["years", "months"])

        clash = native_spec()
        clash["filters"] = [{"name": "Desktop1", "field": "Calendar.Year", "in": [2024]}]
        before = snapshot(base)
        failed = rs.apply_report_spec(root, clash, model_fields=MODEL)
        has("a name a Desktop filter already has is refused", failed["errors"],
            "filters: a filter named 'Desktop1' is already there and was not created by an earlier apply")
        eq("and nothing is written", [failed["written"], snapshot(base)], [[], before])
        clash["filters"] = []
        clash["pages"][0]["elements"][0]["filters"] = [{"name": "desktop3", "field": "Customer.Segment",
                                                        "in": ["A"]}]
        has("on a visual too", rs.apply_report_spec(root, clash, model_fields=MODEL)["errors"],
            "page 'overview', element 'by_segment': a filter named 'desktop3' is already there")
        # a name is unique across the whole report: a Desktop filter in another file has it too
        clash = native_spec()
        clash["filters"] = [{"name": "Desktop3", "field": "Calendar.Year", "in": [2024]}]
        elsewhere = rs.apply_report_spec(root, clash, model_fields=MODEL)
        has("a name a Desktop filter has in another file is refused as well", elsewhere["errors"],
            "filters: a filter named 'Desktop3' is already in "
            "definition/pages/overview/visuals/by_segment/visual.json, and a filter name is unique across "
            "the report")
        eq("with adopt too, since the other filter would still be there",
           [rs.apply_report_spec(root, clash, model_fields=MODEL, adopt=True)["success"], snapshot(base)],
           [False, before])
        moved = native_spec()
        moved["filters"] = []
        moved["pages"][0]["filters"].append({"name": "years", "field": "Calendar.Year", "in": [2024]})
        eq("one of the spec's own filters can move to another file under its name",
           rs.apply_report_spec(root, moved, model_fields=MODEL, dry_run=True)["errors"], [])

        # the spec stops declaring filters: ours go, theirs stay
        spec3 = native_spec()
        del spec3["filters"], spec3["pages"][0]["filters"]
        for el in spec3["pages"][0]["elements"]:
            el.pop("filters", None)
        cleared = rs.apply_report_spec(root, spec3, model_fields=MODEL)
        eq("it applies without prune", [cleared["success"], cleared["removed"]], [True, []])
        eq("only the Desktop filters are left", [filters_of(report_file), filters_of(page_file),
                                                 filters_of(visual_file)],
           [["desktop1"], ["desktop2"], ["desktop3"]])
        check("a visual whose only filters were ours has no filter list left",
              "filterConfig" not in load(os.path.join(d, "pages", "overview", "visuals", "by_month",
                                                      "visual.json")))
        manifest = load(manifest_file)
        check("and the manifest forgets them", "report_filters" not in manifest
              and "filters" not in manifest["pages"]["overview"]
              and "element_filters" not in manifest["pages"]["overview"])
        state = snapshot(base)
        eq("applying again is quiet", [rs.apply_report_spec(root, spec3, model_fields=MODEL)["written"],
                                       snapshot(base)], [[], state])

        adopted = native_spec()
        adopted["filters"] = [{"name": "desktop1", "field": "Calendar.Year", "in": [2030]}]
        taken = rs.apply_report_spec(root, adopted, model_fields=MODEL, adopt=True)
        eq("adopt takes a Desktop filter over, in place", [taken["success"], filters_of(report_file),
                                                           load(report_file)["filterConfig"]["filters"][0]
                                                           ["type"]], [True, ["desktop1"], "Categorical"])
        check("with the spec's condition", "filter" in load(report_file)["filterConfig"]["filters"][0])

    with tempfile.TemporaryDirectory() as base:
        # a filter left on an element that the spec drops without prune stays tracked
        root = make_report(base)
        rs.apply_report_spec(root, native_spec(), model_fields=MODEL)
        smaller = native_spec()
        smaller["pages"][0]["elements"] = smaller["pages"][0]["elements"][2:]
        smaller["pages"][0]["interactions"] = []
        rs.apply_report_spec(root, smaller, model_fields=MODEL)
        manifest = load(os.path.join(base, "Demo.Report.spec-manifest.json"))
        eq("a leftover element keeps its record", [sorted(manifest["pages"]["overview"]["element_filters"]),
                                                   manifest["pages"]["overview"]["mobile"]],
           [["by_month", "by_segment"], ["by_segment", "total"]])
        back = rs.apply_report_spec(root, native_spec(), model_fields=MODEL)
        eq("and the spec can take the element back", [back["success"], back["errors"]], [True, []])

    # an element shared by several pages: each copy gets its own filter names
    spec = small_spec()
    spec["pages"].append({"name": "second", "elements": []})
    shared_filters = [{"field": "Customer.Segment", "in": ["A"]},
                      {"name": "{page}_named", "field": "Calendar.Year", "in": [2025]}]
    spec["shared"] = [{"name": "s", "pages": "*", "elements": [
        chart("shared_chart", y=500, filters=shared_filters,
              mobile={"x": 0, "y": 0, "width": 323, "height": 200})]}]
    c = rs._compile(spec, model_fields=MODEL)
    copies = [p.elements[-1].filters for p in c.pages]
    eq("a shared element's filters compile on every page", [c.errors, [len(f) for f in copies]], [[], [2, 2]])
    check("with a name of their own on each", copies[0][0]["name"] != copies[1][0]["name"])
    eq("and a given name can carry the page", [copies[0][1]["name"], copies[1][1]["name"]],
       ["overview_named", "second_named"])
    spec["shared"][0]["elements"][0]["filters"][1]["name"] = "same"
    has("one given name on two pages is refused", rs._compile(spec, model_fields=MODEL).errors,
        "the filter name 'same' is already used by")

    with tempfile.TemporaryDirectory() as base:
        # elements added to a page the report already had: the page file is still left alone
        root = make_report(base)
        hand_made(root)
        before = snapshot(root)
        spec = {"pages": [{"name": "legacy", "existing": True,
                           "filters": [{"field": "Calendar.Year", "in": [2025]}],
                           "elements": [chart("added", filters=[{"field": "Calendar.Year", "in": [2025]}],
                                              mobile={"x": 0, "y": 0, "width": 323, "height": 200})]}]}
        result = rs.apply_report_spec(root, spec, model_fields=MODEL)
        eq("it applies", [result["success"], result["errors"]], [True, []])
        has("page filters of an existing page are reported as unused", result["warnings"],
            "filters had no effect")
        after = snapshot(root)
        check("its page.json is untouched", after["definition/pages/legacy/page.json"]
              == before["definition/pages/legacy/page.json"])
        added = os.path.join(root, "definition", "pages", "legacy", "visuals", "added")
        eq("the added element has its filter and its phone position",
           [len(load(os.path.join(added, "visual.json"))["filterConfig"]["filters"]),
            os.path.isfile(os.path.join(added, "mobile.json"))], [1, True])
        entry = load(os.path.join(base, "Demo.Report.spec-manifest.json"))["pages"]["legacy"]
        eq("and the manifest records both under the page it does not own",
           [entry["owned"], list(entry["element_filters"]), entry["mobile"], "filters" in entry],
           [False, ["added"], ["added"], False])


def test_drillthrough_and_tooltip_pages():
    print("\n== drillthrough and tooltip pages ==")
    c = rs._compile(native_spec(), model_fields=MODEL)
    overview, detail, tip = c.pages
    segment = {"Column": {"Expression": {"SourceRef": {"Entity": "Customer"}}, "Property": "Segment"}}
    bound = [f["name"] for f in detail.filters]
    eq("a drillthrough page",
       [detail.doc["type"], detail.doc["pageBinding"]["type"], "visibility" in detail.doc],
       ["Drillthrough", "Drillthrough", False])
    eq("its binding points each field at a filter of the page",
       detail.doc["pageBinding"]["parameters"],
       [{"name": detail.doc["pageBinding"]["parameters"][0]["name"], "boundFilter": bound[0],
         "asAggregation": False, "qnaSingleSelectRequired": False, "fieldExpr": segment},
        {"name": detail.doc["pageBinding"]["parameters"][1]["name"], "boundFilter": bound[1],
         "asAggregation": False, "qnaSingleSelectRequired": False,
         "fieldExpr": {"Column": {"Expression": {"SourceRef": {"Entity": "Calendar"}}, "Property": "Year"}}}])
    check("a parameter has a name of its own, 20 hex digits like the ones Desktop gives",
          all(rs.re.match(r"^[0-9a-f]{20}$", p["name"]) and p["name"] != p["boundFilter"]
              for p in detail.doc["pageBinding"]["parameters"]))
    eq("those filters wait for the value a drill brings", detail.filters[0],
       {"name": bound[0], "field": segment, "type": "Categorical", "howCreated": "Drillthrough"})
    check("keeping all filters and staying in the report are the defaults, so nothing is written for them",
          not {"acceptsFilterContext", "referenceScope"} & set(detail.doc["pageBinding"]))
    back = next(el for el in detail.elements if el.name == "drill_back")
    back_link = back.doc["visual"]["visualContainerObjects"]["visualLink"][0]["properties"]
    eq("the back button", [back.doc["visual"]["visualType"], back_link["type"], back.z >= 20000],
       ["actionButton", pb.lit_str("Back"), True])
    link = overview.elements[3].doc["visual"]["visualContainerObjects"]["visualLink"][0]["properties"]
    eq("a drillthrough button names the page as the page spells it",
       [link["type"], link["drillthroughSection"], link["enabledTooltip"], link["disabledTooltip"]],
       [pb.lit_str("Drillthrough"), pb.lit_str("detail"), pb.lit_str("Open"), pb.lit_str("Pick a segment")])

    eq("a tooltip page is small and hidden", [tip.doc["type"], tip.doc["visibility"], tip.doc["width"],
                                              tip.doc["height"]], ["Tooltip", "HiddenInViewMode", 320, 240])
    eq("its binding", [tip.doc["pageBinding"]["type"], len(tip.doc["pageBinding"]["parameters"])],
       ["Tooltip", 1])
    eq("a visual that shows it",
       overview.elements[0].doc["visual"]["visualContainerObjects"]["visualTooltip"],
       [{"properties": {"show": pb.lit_bool(True), "section": pb.lit_str("tip")}}])
    names = {detail.doc["pageBinding"]["name"], tip.doc["pageBinding"]["name"]}
    eq("each binding has its own name, the same on every run",
       [len(names), rs._compile(native_spec(), model_fields=MODEL).pages[1].doc["pageBinding"]["name"]],
       [2, detail.doc["pageBinding"]["name"]])

    def compiled(change):
        spec = native_spec()
        change(spec)
        return rs._compile(spec, model_fields=MODEL)

    c = compiled(lambda s: (s.update(page_defaults={"width": 1600, "height": 900}),
                            s["pages"][2].update(tooltip=True)))
    eq("page defaults do not stretch a tooltip page", [c.errors, c.pages[2].width, c.pages[0].width],
       [[], 320, 1600])
    eq("with no fields a tooltip page is its type and nothing more, as Desktop saves one",
       [c.pages[2].doc["type"], c.pages[2].doc["visibility"], "pageBinding" in c.pages[2].doc,
        c.pages[2].filters], ["Tooltip", "HiddenInViewMode", False, None])
    c = compiled(lambda s: s["pages"][2].update(tooltip={}))
    check("an empty object says the same", c.errors == [] and "pageBinding" not in c.pages[2].doc)
    c = compiled(lambda s: (s["pages"][2].update(width=400, height=300),
                            s["pages"][2]["elements"][0].update(width=400)))
    eq("its own size wins", [c.errors, c.pages[2].doc["width"], c.pages[2].doc["height"]], [[], 400, 300])
    c = compiled(lambda s: s["pages"][1]["drillthrough"].update(keep_all_filters=False, cross_report=True,
                                                                back_button={"name": "go_back", "x": 500,
                                                                             "text": "Return"}))
    binding = c.pages[1].doc["pageBinding"]
    eq("the drillthrough options", [c.errors, binding["acceptsFilterContext"], binding["referenceScope"]],
       [[], "None", "CrossReport"])
    back = next(el for el in c.pages[1].elements if el.name == "go_back")
    eq("a back button of your own making", [back.x, back.y, back.doc["visual"]["objects"]["text"][1]
                                            ["properties"]["text"]], [500, 16, pb.lit_str("Return")])
    c = compiled(lambda s: s["pages"][1]["drillthrough"].pop("back_button"))
    check("no back button unless asked for", [c.errors, [el.name for el in c.pages[1].elements]]
          == [[], ["orders"]])

    def errors(change):
        return compiled(change).errors

    has("drillthrough with no fields", errors(lambda s: s["pages"][1].update(drillthrough={})),
        "page 'detail': drillthrough needs \"fields\"")
    has("a measure as a drillthrough field", errors(lambda s: s["pages"][1]["drillthrough"].update(
        fields=["Sales.Total Sales"])),
        "page 'detail', drillthrough fields[0]: Sales[Total Sales] is a measure")
    has("an aggregated drillthrough field", errors(lambda s: s["pages"][1]["drillthrough"].update(
        fields=[{"column": "Sales.Amount", "aggregation": "sum"}])), "must be a plain column")
    has("a field listed twice", errors(lambda s: s["pages"][1]["drillthrough"].update(
        fields=["Customer.Segment", "customer.segment"])), "Customer[Segment] is listed twice")
    has("an unknown drillthrough key", errors(lambda s: s["pages"][1]["drillthrough"].update(keep=True)),
        "unknown drillthrough key 'keep'")
    has("an option that is not true or false", errors(lambda s: s["pages"][1]["drillthrough"].update(
        cross_report="yes")), "drillthrough cross_report must be true or false")
    has("a back button that is neither", errors(lambda s: s["pages"][1]["drillthrough"].update(
        back_button="yes")), "back_button must be true or an object")
    has("a page that is both", errors(lambda s: s["pages"][1].update(tooltip=True)),
        "page 'detail': a page is a tooltip page or a drillthrough page, not both")
    has("tooltip that is neither true nor an object", errors(lambda s: s["pages"][2].update(tooltip="yes")),
        "tooltip must be true or")
    has("a drillthrough button to an ordinary page", errors(lambda s: s["pages"][0]["elements"][3]["action"]
                                                             .update(target="overview")),
        "page 'overview', element 'open_detail', drillthrough action: page 'overview' is not a drillthrough "
        "page")
    has("a drillthrough button to nowhere", errors(lambda s: s["pages"][0]["elements"][3]["action"]
                                                   .update(target="nowhere")), "'nowhere' is not a page")
    has("a drillthrough button with no target", errors(lambda s: s["pages"][0]["elements"][3]["action"]
                                                       .pop("target")),
        'a drillthrough action needs "target"')
    has("a tooltip page that is not one", errors(lambda s: s["pages"][0]["elements"][0].update(
        tooltip_page="detail")),
        "tooltip_page: page 'detail' is not a tooltip page; give it \"tooltip\": true")
    has("a tooltip page that is not there", errors(lambda s: s["pages"][0]["elements"][0].update(
        tooltip_page="ghost")), "tooltip_page: 'ghost' is not a page")
    has("a tooltip page on a visual with tooltips off", errors(lambda s: s["pages"][0]["elements"][0].update(
        tooltip=False)), "its tooltip switched off cannot show a tooltip page")
    spec = native_spec()
    spec["pages"][0]["elements"][0]["tooltip_page"] = "Theirs"
    spec["pages"][0]["elements"][3]["action"]["target"] = "theirs"
    c = rs._compile(spec, model_fields=MODEL, existing_pages=["Theirs"])
    eq("a page the report already has is taken on trust, spelled its way",
       [c.errors, c.pages[0].elements[3].doc["visual"]["visualContainerObjects"]["visualLink"][0]
        ["properties"]["drillthroughSection"]], [[], pb.lit_str("Theirs")])
    spec = native_spec()
    spec["pages"][0].update(existing=True)
    has("page settings of an existing page are reported as unused", rs._compile(spec, model_fields=MODEL)
        .warnings, "filters, interactions had no effect")

    with tempfile.TemporaryDirectory() as base:
        root = make_report(base)
        result = rs.apply_report_spec(root, native_spec(), model_fields=MODEL)
        eq("it applies", [result["success"], result["errors"]], [True, []])
        d = os.path.join(root, "definition")
        detail_file = os.path.join(d, "pages", "detail", "page.json")
        manifest_file = os.path.join(base, "Demo.Report.spec-manifest.json")
        eq("the manifest knows the binding is the spec's", load(manifest_file)["pages"]["detail"]["keys"],
           ["pageBinding", "type"])
        check("the back button is a file like any element", os.path.isfile(os.path.join(
            d, "pages", "detail", "visuals", "drill_back", "visual.json")))
        page = load(detail_file)
        page["filterConfig"]["filters"].append({"name": "desktop1", "type": "Categorical"})
        page["annotations"] = [{"name": "note", "value": "kept"}]
        write_raw(detail_file, json.dumps(page, indent=2).encode())
        plain = native_spec()
        del plain["pages"][1]["drillthrough"]
        plain["pages"][0]["elements"].pop(3)
        ordinary = rs.apply_report_spec(root, plain, model_fields=MODEL)
        eq("dropping drillthrough from the spec applies", [ordinary["success"], ordinary["errors"]],
           [True, []])
        page = load(detail_file)
        check("the page is an ordinary page again", not {"type", "pageBinding"} & set(page))
        eq("its drillthrough filters went, a Desktop filter and a note stayed",
           [[f["name"] for f in page["filterConfig"]["filters"]], page["annotations"]],
           [["desktop1"], [{"name": "note", "value": "kept"}]])
        check("and the manifest no longer claims the binding", "keys" not in load(manifest_file)["pages"]
              ["detail"])
        check("the back button waits for a prune, like any element the spec dropped",
              os.path.isfile(os.path.join(d, "pages", "detail", "visuals", "drill_back", "visual.json")))

        # a page someone made a tooltip page in Desktop, which the spec says nothing about, stays one
        page.update({"type": "Tooltip", "pageBinding": {"name": "theirs", "type": "Tooltip"}})
        write_raw(detail_file, json.dumps(page, indent=2).encode())
        rs.apply_report_spec(root, plain, model_fields=MODEL)
        eq("a binding made in Desktop is not the spec's to remove", [load(detail_file)["type"],
                                                                    load(detail_file)["pageBinding"]["name"]],
           ["Tooltip", "theirs"])
        again = rs.apply_report_spec(root, native_spec(), model_fields=MODEL)
        eq("until the spec declares one of its own", [again["success"], load(detail_file)["type"],
                                                      load(detail_file)["pageBinding"]["type"]],
           [True, "Drillthrough", "Drillthrough"])
        eq("and the report is sound", self_check(root), [])


def test_interactions():
    print("\n== visual interactions ==")
    c = rs._compile(native_spec(), model_fields=MODEL)
    eq("from, to and type become source, target and the format's own words", c.pages[0].interactions, [
        {"source": "by_segment", "target": "total", "type": "NoFilter"},
        {"source": "by_segment", "target": "by_month", "type": "DataFilter"},
        {"source": "by_month", "target": "by_segment", "type": "HighlightFilter"}])
    check("a page that declares none has none", c.pages[1].interactions is None)

    def compiled(interactions, more=None):
        spec = native_spec()
        spec["pages"][0]["interactions"] = interactions
        if more:
            more(spec)
        return rs._compile(spec, model_fields=MODEL)

    c = compiled([{"from": "BY_SEGMENT", "to": "*", "type": "highlight"}])
    eq('"*" is every other element that shows data, named as the page names them',
       [(i["source"], i["target"]) for i in c.pages[0].interactions],
       [("by_segment", "by_month"), ("by_segment", "total")])
    eq("an empty list is a list", compiled([]).pages[0].interactions, [])

    def errors(interactions, more=None):
        return compiled(interactions, more).errors

    has("an element that is not on the page", errors([{"from": "ghost", "to": "total", "type": "none"}]),
        "page 'overview', interactions[0]: \"from\" names 'ghost', which is not an element of this page")
    has("a target that is not on the page", errors([{"from": "total", "to": ["by_month", "ghost"],
                                                     "type": "none"}]), "\"to\" names 'ghost'")
    has("an element with itself", errors([{"from": "total", "to": "TOTAL", "type": "none"}]),
        "'total' cannot interact with itself")
    has("an unknown type", errors([{"from": "total", "to": "by_month", "type": "Filter"}]),
        "\"type\" must be one of ['filter', 'highlight', 'none'], not 'Filter'")
    has("a stray key", errors([{"from": "total", "to": "by_month", "type": "none", "source": "x"}]),
        "unknown key(s) ['source']")
    has("not a list", errors({"from": "total"}), "page 'overview', interactions must be a list")
    has("an entry that is not an object", errors(["total"]), "interactions[0] must be an object")
    has("a group", errors([{"from": "g", "to": "total", "type": "none"}], lambda s: s["pages"][0]["elements"]
                          .append({"name": "g", "type": "group", "elements": []})),
        "\"from\" names 'g', which is a group; name the visuals inside it")

    with tempfile.TemporaryDirectory() as base:
        root = make_report(base)
        rs.apply_report_spec(root, native_spec(), model_fields=MODEL)
        page_file = os.path.join(root, "definition", "pages", "overview", "page.json")
        manifest_file = os.path.join(base, "Demo.Report.spec-manifest.json")
        eq("the page holds them", len(load(page_file)["visualInteractions"]), 3)
        eq("the manifest lists the pairs", load(manifest_file)["pages"]["overview"]["interactions"],
           [["by_segment", "total"], ["by_segment", "by_month"], ["by_month", "by_segment"]])
        page = load(page_file)
        theirs = {"source": "total", "target": "by_month", "type": "NoFilter"}
        page["visualInteractions"].insert(0, theirs)
        page["visualInteractions"][1]["type"] = "DataFilter"          # they changed one of ours as well
        write_raw(page_file, json.dumps(page, indent=2).encode())
        spec = native_spec()
        spec["pages"][0]["interactions"] = [{"from": "by_segment", "to": "total", "type": "none"}]
        rs.apply_report_spec(root, spec, model_fields=MODEL)
        eq("a pair set in Desktop stays; the spec's pairs are the spec's",
           load(page_file)["visualInteractions"],
           [theirs, {"source": "by_segment", "target": "total", "type": "NoFilter"}])
        state = snapshot(base)
        eq("applying again is quiet", [rs.apply_report_spec(root, spec, model_fields=MODEL)["written"],
                                       snapshot(base)], [[], state])
        del spec["pages"][0]["interactions"]
        rs.apply_report_spec(root, spec, model_fields=MODEL)
        eq("without the key only theirs is left", load(page_file)["visualInteractions"], [theirs])
        check("and the manifest lets go", "interactions" not in load(manifest_file)["pages"]["overview"])
        page = load(page_file)
        del page["visualInteractions"]
        write_raw(page_file, json.dumps(page, indent=2).encode())
        rs.apply_report_spec(root, native_spec(), model_fields=MODEL)
        spec = native_spec()
        del spec["pages"][0]["interactions"]
        rs.apply_report_spec(root, spec, model_fields=MODEL)
        check("with none of theirs, the key goes with the last of ours",
              "visualInteractions" not in load(page_file))


def test_phone_layout():
    print("\n== the phone layout ==")
    c = rs._compile(native_spec(), model_fields=MODEL)
    by_segment, _by_month, total = c.pages[0].elements[:3]
    eq("a phone position", by_segment.mobile,
       {"$schema": pb.DEFAULT_SCHEMAS["mobile"],
        "position": {"x": 0, "y": 0, "z": by_segment.z, "width": 323, "height": 200,
                     "tabOrder": by_segment.z}})
    eq("with its own layer", total.mobile["position"]["z"], 3)
    check("an element without one is not on the phone", _by_month.mobile is None)

    def errors(mobile, element=0, more=None):
        spec = native_spec()
        spec["pages"][0]["elements"][element]["mobile"] = mobile
        if more:
            more(spec)
        return rs._compile(spec, model_fields=MODEL).errors

    has("a missing size", errors({"x": 0, "y": 0, "width": 323}),
        "page 'overview', element 'by_segment': mobile needs numeric height")
    has("a negative position", errors({"x": -1, "y": 0, "width": 10, "height": 10}), "cannot be negative")
    has("a stray key", errors({"x": 0, "y": 0, "width": 10, "height": 10, "w": 5}),
        "unknown mobile key(s) ['w']")
    has("not an object", errors(True), "mobile must be an object")
    spec = native_spec()
    spec["pages"][0]["elements"].append({"name": "g", "type": "group", "elements": [], "mobile": {}})
    has("a group", rs._compile(spec, model_fields=MODEL).errors,
        "a group has no place on the phone layout")

    with tempfile.TemporaryDirectory() as base:
        root = make_report(base)
        visuals = os.path.join(root, "definition", "pages", "overview", "visuals")
        plan = rs.apply_report_spec(root, native_spec(), model_fields=MODEL, dry_run=True)
        result = rs.apply_report_spec(root, native_spec(), model_fields=MODEL)
        mobiles = sorted(p for p in result["written"] if p.endswith("mobile.json"))
        eq("the phone files are written, and listed", mobiles, [
            "definition/pages/overview/visuals/by_segment/mobile.json",
            "definition/pages/overview/visuals/total/mobile.json"])
        eq("the dry run listed them too", plan["written"], result["written"])
        eq("the file", load(os.path.join(visuals, "total", "mobile.json")),
           {"$schema": pb.DEFAULT_SCHEMAS["mobile"],
            "position": {"x": 0, "y": 200, "z": 3, "width": 323, "height": 100, "tabOrder": 3}})
        manifest_file = os.path.join(base, "Demo.Report.spec-manifest.json")
        eq("the manifest names them", load(manifest_file)["pages"]["overview"]["mobile"],
           ["by_segment", "total"])

        # Desktop: phone formatting on one of ours, and a phone position for a visual the spec left off
        theirs = {"$schema": "https://example.test/definition/visualContainerMobileState/2.5.0/schema.json",
                  "position": {"x": 1, "y": 2, "width": 3, "height": 4}}
        write_raw(os.path.join(visuals, "by_month", "mobile.json"), json.dumps(theirs).encode())
        ours = load(os.path.join(visuals, "total", "mobile.json"))
        ours["objects"] = {"labels": [{"properties": {}}]}
        write_raw(os.path.join(visuals, "total", "mobile.json"), json.dumps(ours, indent=2).encode())
        spec = native_spec()
        spec["pages"][0]["elements"][2]["mobile"]["height"] = 150
        del spec["pages"][0]["elements"][0]["mobile"]
        plan = rs.apply_report_spec(root, spec, model_fields=MODEL, dry_run=True)
        check("a dry run removes nothing", os.path.isfile(os.path.join(visuals, "by_segment", "mobile.json")))
        moved = rs.apply_report_spec(root, spec, model_fields=MODEL)
        eq("an element taken off the phone loses its file, without prune", moved["removed"],
           ["definition/pages/overview/visuals/by_segment/mobile.json"])
        eq("as the dry run said", [plan["removed"], plan["written"]], [moved["removed"], moved["written"]])
        check("the visual itself stays", os.path.isfile(os.path.join(visuals, "by_segment", "visual.json")))
        eq("a phone position made in Desktop is not touched", load(os.path.join(visuals, "by_month",
                                                                                "mobile.json")), theirs)
        changed = load(os.path.join(visuals, "total", "mobile.json"))
        eq("a changed position keeps the phone formatting Desktop added", [changed["position"]["height"],
                                                                           changed["objects"]],
           [150, {"labels": [{"properties": {}}]}])
        eq("the manifest follows", load(manifest_file)["pages"]["overview"]["mobile"], ["total"])
        state = snapshot(base)
        eq("applying again is quiet", [rs.apply_report_spec(root, spec, model_fields=MODEL)["written"],
                                       snapshot(base)], [[], state])
        spec["pages"][0]["elements"][1]["mobile"] = {"x": 0, "y": 0, "width": 100, "height": 100}
        rs.apply_report_spec(root, spec, model_fields=MODEL)
        eq("a new phone file takes the newest schema in the project",
           load(os.path.join(visuals, "by_month", "mobile.json"))["$schema"], theirs["$schema"])

        gone = native_spec()
        gone["pages"][0]["elements"] = gone["pages"][0]["elements"][:1]
        gone["pages"][0]["interactions"] = []
        gone["bookmarks"], gone["bookmark_groups"] = [], []
        pruned = rs.apply_report_spec(root, gone, model_fields=MODEL, prune=True)
        check("pruning an element takes its phone file along",
              "definition/pages/overview/visuals/total/mobile.json" in pruned["removed"]
              and not os.path.exists(os.path.join(visuals, "total")))


def test_bookmark_groups():
    print("\n== bookmark groups ==")
    c = rs._compile(native_spec(), model_fields=MODEL)
    eq("a group", c.bookmark_groups, [{"name": "views", "displayName": "Views",
                                       "children": ["show_total", "hide_total"]}])
    check("a spec with no groups declares none", rs._compile(small_spec()).bookmark_groups is None)

    def errors(groups):
        spec = native_spec()
        spec["bookmark_groups"] = groups
        return rs._compile(spec, model_fields=MODEL, existing_bookmarks=["Mine"]).errors

    has("a bookmark that is not in the spec", errors([{"name": "g", "bookmarks": ["mine"]}]),
        "bookmark group 'g': 'mine' is not a bookmark of this spec")
    has("a bookmark in two groups", errors([{"name": "g", "bookmarks": ["alone"]},
                                            {"name": "h", "bookmarks": ["ALONE"]}]),
        "bookmark group 'h': bookmark 'ALONE' is already in group 'g'")
    has("a group with no bookmarks", errors([{"name": "g", "bookmarks": []}]), 'needs "bookmarks"')
    has("a group named like a bookmark of the spec", errors([{"name": "Alone", "bookmarks": ["alone"]}]),
        "the name is also the name of a bookmark")
    has("or like one the report has", errors([{"name": "mine", "bookmarks": ["alone"]}]),
        "the name is also the name of a bookmark")
    has("a group twice", errors([{"name": "g", "bookmarks": ["alone"]}, {"name": "G", "bookmarks":
                                                                         ["show_total"]}]), "used twice")
    has("a name that cannot be used", errors([{"name": "my group", "bookmarks": ["alone"]}]),
        "not a usable name")
    has("a stray key", errors([{"name": "g", "bookmarks": ["alone"], "children": []}]),
        "unknown key 'children'")
    has("groups that are not a list", rs.validate_report_spec(dict(native_spec(), bookmark_groups={})),
        "bookmark_groups must be a list")

    with tempfile.TemporaryDirectory() as base:
        root = make_report(base)
        hand_made(root)
        meta_file = os.path.join(root, "definition", "bookmarks", "bookmarks.json")
        meta = load(meta_file)
        meta["items"].append({"name": "theirs", "displayName": "Their group", "children": []})
        write_raw(meta_file, json.dumps(meta, indent=2).encode())
        result = rs.apply_report_spec(root, native_spec(), model_fields=MODEL)
        eq("it applies", [result["success"], result["errors"]], [True, []])
        views = {"name": "views", "displayName": "Views", "children": ["show_total", "hide_total"]}
        theirs = {"name": "theirs", "displayName": "Their group", "children": []}
        eq("the group holds its bookmarks; the rest stand alone, after what was there",
           load(meta_file)["items"], [{"name": "mine"}, theirs, views, {"name": "alone"}])
        manifest_file = os.path.join(base, "Demo.Report.spec-manifest.json")
        eq("the manifest names the group", load(manifest_file)["bookmark_groups"], ["views"])
        state = snapshot(base)
        again = rs.apply_report_spec(root, native_spec(), model_fields=MODEL)
        eq("applying again is quiet", [again["written"], snapshot(base)], [[], state])

        # in Desktop: their own bookmark dragged into our group, and one of ours into theirs
        meta = load(meta_file)
        meta["items"] = [theirs | {"children": ["alone"]}, views | {"children": ["hide_total", "mine",
                                                                                 "show_total"]}]
        write_raw(meta_file, json.dumps(meta, indent=2).encode())
        spec = native_spec()
        spec["bookmark_groups"] = [{"name": "views", "display_name": "Two views",
                                    "bookmarks": ["hide_total", "alone"]}]
        rs.apply_report_spec(root, spec, model_fields=MODEL)
        eq("the spec's bookmarks go where the spec says; a bookmark someone else put in the group stays",
           load(meta_file)["items"],
           [theirs,
            {"name": "views", "displayName": "Two views", "children": ["hide_total", "alone", "mine"]},
            {"name": "show_total"}])

        del spec["bookmark_groups"]
        rs.apply_report_spec(root, spec, model_fields=MODEL)
        eq("a group the spec drops is opened up where it stood", load(meta_file)["items"],
           [theirs, {"name": "hide_total"}, {"name": "alone"}, {"name": "mine"}, {"name": "show_total"}])
        check("and the manifest forgets it", "bookmark_groups" not in load(manifest_file))
        eq("the bookmarks themselves are all still there",
           sorted(f for f in os.listdir(os.path.dirname(meta_file)) if f.endswith(".bookmark.json")),
           ["alone.bookmark.json", "hide_total.bookmark.json", "mine.bookmark.json",
            "show_total.bookmark.json"])

        clash = native_spec()
        clash["bookmark_groups"] = [{"name": "Theirs", "bookmarks": ["alone"]}]
        before = snapshot(base)
        failed = rs.apply_report_spec(root, clash, model_fields=MODEL)
        has("a group someone else made is not taken over", failed["errors"],
            "bookmark group 'Theirs' already exists and was not created by an earlier apply")
        eq("nothing is written", snapshot(base), before)
        adopted = rs.apply_report_spec(root, clash, model_fields=MODEL, adopt=True)
        eq("unless adopt says so", [adopted["success"], load(meta_file)["items"][0]],
           [True, {"name": "theirs", "displayName": "Theirs", "children": ["alone"]}])

    with tempfile.TemporaryDirectory() as base:
        root = make_report(base)
        rs.apply_report_spec(root, native_spec(), model_fields=MODEL)
        meta_file = os.path.join(root, "definition", "bookmarks", "bookmarks.json")
        spec = native_spec()
        spec["bookmarks"] = spec["bookmarks"][2:]
        del spec["bookmark_groups"]
        rs.apply_report_spec(root, spec, model_fields=MODEL)
        eq("bookmarks the spec dropped stay listed until a prune", load(meta_file)["items"],
           [{"name": "show_total"}, {"name": "hide_total"}, {"name": "alone"}])
        rs.apply_report_spec(root, spec, model_fields=MODEL, prune=True)
        eq("and go with it", load(meta_file)["items"], [{"name": "alone"}])


def test_interactivity_switches():
    print("\n== interactivity switches ==")

    def one(element):
        spec = {"pages": [{"name": "p", "elements": [{"name": "e", "x": 0, "y": 0, "width": 100,
                                                      "height": 100, **element}]}]}
        compiled = rs._compile(spec, model_fields=MODEL)
        return compiled, compiled.pages[0].elements[0].doc

    deneb = {"type": "deneb", "fields": ["Customer.Segment", "Sales.Total Sales"], "spec": {}}
    c, doc = one(deneb)
    vega = doc["visual"]["objects"]["vega"][0]["properties"]
    check("Deneb's highlight and context menu are left to Deneb unless asked for",
          c.errors == [] and not {"enableHighlight", "enableContextMenu"} & set(vega))
    c, doc = one({**deneb, "selection": True, "cross_highlight": True, "context_menu": False})
    vega = doc["visual"]["objects"]["vega"][0]["properties"]
    eq("asked for, they are written", [c.errors, vega["enableSelection"], vega["enableHighlight"],
                                       vega["enableContextMenu"]],
       [[], pb.lit_bool(True), pb.lit_bool(True), pb.lit_bool(False)])
    c, _ = one({**deneb, "cross_highlight": "yes"})
    has("a switch must be true or false", c.errors,
        "page 'p', element 'e': cross_highlight must be true or false")
    c, doc = one(chart("e"))
    eq("a visual follows a drill elsewhere by default", doc["visual"]["drillFilterOtherVisuals"], True)
    c, doc = one(chart("e", drill_filter_other_visuals=False))
    eq("which can be switched off", [c.errors, doc["visual"]["drillFilterOtherVisuals"]], [[], False])
    c, _ = one(chart("e", drill_filter_other_visuals=0))
    has("but only with true or false", c.errors, "drill_filter_other_visuals must be true or false")
    c, _ = one({"type": "textbox", "text": "x", "cross_highlight": True})
    has("a switch on the wrong element is reported", c.warnings,
        "'cross_highlight' is not a setting of a textbox")


def test_native_spec_end_to_end():
    print("\n== one spec with every native feature ==")
    with tempfile.TemporaryDirectory() as base:
        root = make_report(base)
        hand_made(root)
        before = snapshot(root)
        spec = native_spec()
        untouched = copy.deepcopy(spec)
        plan = rs.apply_report_spec(root, spec, model_fields=MODEL, dry_run=True)
        eq("a dry run changes nothing", snapshot(root), before)
        result = rs.apply_report_spec(root, spec, model_fields=MODEL)
        eq("it applies", [result["success"], result["errors"], result["warnings"]], [True, [], []])
        eq("the dry run said what would be written", plan["written"], result["written"])
        eq("the caller's spec is not changed", spec, untouched)
        after = snapshot(root)
        check("hand-made pages, visuals and bookmarks are untouched",
              all(after[p] == before[p] for p in before if "/legacy/" in p or "mine." in p))
        files = snapshot(base)
        again = rs.apply_report_spec(root, spec, model_fields=MODEL)
        eq("a second apply writes nothing", [again["written"], again["removed"], snapshot(base)],
           [[], [], files])

        pruned = rs.apply_report_spec(root, {"pages": []}, model_fields=MODEL, prune=True)
        eq("an empty spec with prune removes it all", [pruned["success"], pruned["errors"]], [True, []])
        left = snapshot(root)
        eq("the files that were there before the spec are left, and the version file a report needs",
           sorted(left), sorted([*before, "definition/version.json"]))
        changed = sorted(p for p in before if left[p] != before[p])
        eq("and they hold what they held", [json.loads(left[p]) == json.loads(before[p]) for p in changed],
           [True] * len(changed))


# ---------------------------------------------------------------------------------------------
# what a review of the native features found: hand edits that were lost, values that were
# written as given, damaged files and manifests, and the questions only Desktop could settle
# ---------------------------------------------------------------------------------------------
def compile_one(element, model=MODEL, **more):
    """A page 'p' holding the one element 'e'. Returns (compiled, the element's document)."""
    spec = {"pages": [{"name": "p", "elements": [{"name": "e", "x": 0, "y": 0, "width": 100, "height": 100,
                                                  **element}]}], **more}
    compiled = rs._compile(spec, model_fields=model)
    return compiled, (compiled.pages[0].elements[0].doc if compiled.pages else None)


def test_hand_edits_inside_a_binding_and_the_interactions():
    print("\n== a drillthrough field and an interaction added by hand ==")
    with tempfile.TemporaryDirectory() as base:
        root = make_report(base)
        spec = native_spec()
        rs.apply_report_spec(root, spec, model_fields=MODEL)
        manifest_file = os.path.join(base, "Demo.Report.spec-manifest.json")
        detail_file = os.path.join(root, "definition", "pages", "detail", "page.json")
        page = load(detail_file)
        ours = copy.deepcopy(page["pageBinding"]["parameters"])
        # in Desktop someone drags one more field into the drillthrough well; it lands between ours
        month = {"Column": {"Expression": {"SourceRef": {"Entity": "Calendar"}}, "Property": "Month"}}
        by_hand = {"name": "ffff0000ffff0000ffff", "boundFilter": "aaaabbbbccccddddeeee",
                   "asAggregation": False, "qnaSingleSelectRequired": False, "fieldExpr": month}
        page["filterConfig"]["filters"].insert(1, {"name": "aaaabbbbccccddddeeee", "field": month,
                                                   "type": "Categorical", "howCreated": "Drillthrough"})
        page["pageBinding"]["parameters"].insert(1, by_hand)
        write_raw(detail_file, json.dumps(page, indent=2).encode())
        state = snapshot(base)
        again = rs.apply_report_spec(root, spec, model_fields=MODEL)
        eq("an unchanged spec leaves a page with a hand-bound field alone",
           [again["written"], snapshot(base)], [[], state])

        changed = native_spec()
        changed["pages"][1]["drillthrough"]["fields"] = ["Customer.Segment"]        # Calendar.Year goes
        changed["pages"][1]["display_name"] = "Detail page"
        rs.apply_report_spec(root, changed, model_fields=MODEL)
        page = load(detail_file)
        eq("a changed spec keeps the hand-bound parameter in its place and drops only its own",
           page["pageBinding"]["parameters"], [ours[0], by_hand])
        eq("and keeps the filter it is bound to", [f["name"] for f in page["filterConfig"]["filters"]],
           [ours[0]["boundFilter"], "aaaabbbbccccddddeeee"])

        plain = native_spec()
        del plain["pages"][1]["drillthrough"]
        plain["pages"][0]["elements"].pop(3)
        rs.apply_report_spec(root, plain, model_fields=MODEL)
        page = load(detail_file)
        eq("when the spec stops making it a drillthrough page, the hand-bound field keeps it one",
           [page.get("type"), page["pageBinding"]["type"], page["pageBinding"]["parameters"],
            [f["name"] for f in page["filterConfig"]["filters"]]],
           ["Drillthrough", "Drillthrough", [by_hand], ["aaaabbbbccccddddeeee"]])
        check("which the manifest no longer claims", "keys" not in load(manifest_file)["pages"]["detail"])
        state = snapshot(base)
        eq("and a further apply is quiet", [rs.apply_report_spec(root, plain, model_fields=MODEL)["written"],
                                            snapshot(base)], [[], state])

    with tempfile.TemporaryDirectory() as base:
        root = make_report(base)
        spec = native_spec()
        rs.apply_report_spec(root, spec, model_fields=MODEL)
        page_file = os.path.join(root, "definition", "pages", "overview", "page.json")
        page = load(page_file)
        theirs = {"source": "total", "target": "by_month", "type": "NoFilter"}
        page["visualInteractions"].insert(1, theirs)           # Desktop wrote it in the middle
        write_raw(page_file, json.dumps(page, indent=2).encode())
        state = snapshot(base)
        again = rs.apply_report_spec(root, spec, model_fields=MODEL)
        eq("an unchanged spec does not rewrite a page for an interaction set by hand",
           [again["written"], snapshot(base)], [[], state])
        eq("the pair is where Desktop put it", load(page_file)["visualInteractions"][1], theirs)
        spec["pages"][0]["interactions"][1]["type"] = "none"
        rs.apply_report_spec(root, spec, model_fields=MODEL)
        eq("a changed pair is changed in place, the hand-set one untouched",
           [(i["source"], i["target"], i["type"]) for i in load(page_file)["visualInteractions"]],
           [("by_segment", "total", "NoFilter"), ("total", "by_month", "NoFilter"),
            ("by_segment", "by_month", "NoFilter"), ("by_month", "by_segment", "HighlightFilter")])


def test_values_that_would_break_the_report():
    print("\n== values that Desktop cannot read are refused ==")

    def errors(change, **more):
        spec = native_spec()
        change(spec)
        return rs._compile(spec, model_fields=MODEL, **more).errors

    def el(spec, i=0):
        return spec["pages"][0]["elements"][i]

    # an aggregation is written into the file as a number: only numbers that are functions pass
    has("an aggregation that is no function, in a filter's field", errors(lambda s: el(s)["filters"].append(
        {"field": {"column": "Sales.Amount", "aggregation": 99}, "op": ">", "value": 1})),
        "unknown aggregation 99; use one of")
    has("true is not an aggregation", errors(lambda s: el(s)["filters"].append(
        {"field": {"column": "Sales.Amount", "aggregation": True}, "op": ">", "value": 1})),
        "unknown aggregation True")
    has("nor is a list", errors(lambda s: el(s)["filters"].append(
        {"field": {"column": "Sales.Amount", "aggregation": ["sum"]}, "op": ">", "value": 1})),
        "unknown aggregation ['sum']")
    has("in what a top N ranks by", errors(lambda s: el(s)["filters"][0]["top"].update(
        by={"column": "Sales.Amount", "aggregation": 42})), "top by: unknown aggregation 42")
    has("in a well of a visual", errors(lambda s: el(s)["fields"].update(
        Y={"column": "Sales.Amount", "aggregation": -1})), "role 'Y': unknown aggregation -1")
    for given, function in (("max", 4), (4, 4), ("Median", 6), (0, 0)):
        spec = native_spec()
        el(spec)["filters"].append({"field": {"column": "Sales.Amount", "aggregation": given}, "op": ">",
                                    "value": 1})
        c = rs._compile(spec, model_fields=MODEL)
        eq(f"the aggregation {given!r} is written as function {function}",
           [c.errors, c.pages[0].elements[0].filters[2]["field"]["Aggregation"]["Function"]], [[], function])

    # NaN and Infinity come out of a JSON parser as numbers and are not JSON: nowhere in a spec
    has("a position that is not a number", errors(lambda s: el(s).update(x=float("nan"))),
        "pages[0].elements[0].x is nan: a report file cannot hold NaN or Infinity")
    has("an infinite size in a theme", errors(lambda s: s.update(theme={"name": "T", "card": {
        "radius": float("inf")}})), "theme.card.radius is inf")
    has("inside a Deneb spec too", rs.validate_report_spec({"pages": [{"name": "p", "elements": [
        {"name": "d", "type": "deneb", "x": 0, "y": 0, "width": 10, "height": 10,
         "fields": [{"column": "T.C"}], "spec": {"mark": {"opacity": float("-inf")}}}]}]}),
        "pages[0].elements[0].spec.mark.opacity is -inf")
    has("a display option the format does not have", errors(lambda s: s["pages"][0].update(
        display_option="Fit")), "page 'overview': display_option must be one of")
    eq("the documented ones pass", errors(lambda s: s["pages"][0].update(display_option="ActualSize")), [])
    has("a range the wrong way round, in a page filter", errors(lambda s: s["pages"][0]["filters"].append(
        {"field": "Calendar.Year", "between": [9, 1]})), "needs the lowest value first: 9 is above 1")
    has("a number no file can hold, in a filter", errors(lambda s: s["filters"][0].update(
        {"in": [float("inf")]})), "filters[0].in[0] is inf")
    has("a whole number too large for the model", errors(lambda s: s["filters"][0].update(
        {"in": [10 ** 30]})), "filters[0] on Calendar[Year]: 1000000000000000000000000000000 is outside the "
        "range of a whole number")
    has("a literal that is not one", errors(lambda s: s["filters"][0].update({"in": [{"literal": "2025"}]})),
        "'2025' is not a literal Power BI reads")

    # interactions: only elements that query the model take part
    def with_extras(s):
        s["pages"][0]["elements"] += [
            {"name": "note", "type": "textbox", "x": 0, "y": 300, "width": 50, "height": 20, "text": "x"},
            {"name": "panel", "type": "shape", "x": 60, "y": 300, "width": 50, "height": 20},
            {"name": "pick", "type": "slicer", "x": 120, "y": 300, "width": 100, "height": 40,
             "field": "Customer.Segment"}]
        s["pages"][0]["popups"] = [{"name": "pop", "box": {"x": 300, "y": 300, "width": 200, "height": 200}}]

    def interaction(**entry):
        def change(s):
            with_extras(s)
            s["pages"][0]["interactions"] = [entry]
        return change

    for name, kind in (("note", "textbox"), ("panel", "shape"), ("open_detail", "button"),
                       ("pop_veil", "shape")):
        has(f"an interaction from a {kind} ({name})", errors(interaction(**{"from": name, "to": "total",
                                                                           "type": "none"})),
            f"\"from\" names {name!r}, which is a {kind}; only visuals and slicers take part in interactions")
        has(f"or to one ({name})", errors(interaction(**{"from": "total", "to": name, "type": "filter"})),
            f"\"to\" names {name!r}, which is a {kind}")
    has("highlighting a slicer", errors(interaction(**{"from": "total", "to": "pick", "type": "highlight"})),
        "'pick' is a slicer, which can be filtered but not highlighted; use \"filter\" or \"none\"")
    eq("filtering one is fine", errors(interaction(**{"from": "by_segment", "to": "pick", "type": "filter"})),
       [])
    def to_everything(kind):
        spec = native_spec()
        interaction(**{"from": "by_segment", "to": "*", "type": kind})(spec)
        c = rs._compile(spec, model_fields=MODEL)
        return [c.errors, [i["target"] for i in c.pages[0].interactions]]

    eq('highlight to "*" is everything that can be highlighted, so not the slicer',
       to_everything("highlight"), [[], ["by_month", "total"]])
    eq('none to "*" does reach it', to_everything("none"), [[], ["by_month", "total", "pick"]])
    has("an interaction to nothing", errors(interaction(**{"from": "total", "to": [], "type": "none"})),
        '"to" names no element')

    # pages used as something they are not
    has("a tooltip page as the tooltip of its own visual",
        errors(lambda s: s["pages"][2]["elements"][0].update(tooltip_page="tip")),
        "page 'tip', element 'tip_total', tooltip_page: a tooltip page cannot be the tooltip of its own "
        "visuals")

    def pointing_at_theirs(s):
        el(s)["tooltip_page"] = "Theirs"
        el(s, 3)["action"]["target"] = "theirs"

    ordinary = errors(pointing_at_theirs, existing_pages=["Theirs"], page_kinds={"theirs": None})
    has("a tooltip_page that is an ordinary page of the report", ordinary,
        "tooltip_page: page 'Theirs' is in the report but is not a tooltip page there")
    has("a drillthrough button to an ordinary page of the report", ordinary,
        "drillthrough action: page 'theirs' is in the report but is not a drillthrough page there")
    has("a tooltip page is not a drillthrough page", errors(pointing_at_theirs, existing_pages=["Theirs"],
                                                          page_kinds={"theirs": "Tooltip"}),
        "is not a drillthrough page there")
    eq("with no report read, a name is still taken on trust", errors(pointing_at_theirs,
                                                                    existing_pages=["Theirs"]), [])

    # the phone layout
    has("a phone position wider than the phone", errors(lambda s: el(s).update(
        mobile={"x": 0, "y": 0, "width": 324, "height": 100})),
        "mobile x 0 and width 324 reach past the phone canvas, which is 323 wide")
    has("or pushed off its edge", errors(lambda s: el(s).update(
        mobile={"x": 200, "y": 0, "width": 200, "height": 100})), "reach past the phone canvas")
    eq("the full width fits, however tall", errors(lambda s: el(s).update(
        mobile={"x": 0, "y": 5000, "width": 323, "height": 4000})), [])
    has("a layer that is not a number", errors(lambda s: el(s).update(
        mobile={"x": 0, "y": 0, "width": 100, "height": 100, "z": "top"})), "mobile z must be a number")

    # one failure, one message
    spec = native_spec()
    el(spec, 2)["fields"] = {"Values": "Sales.Missing"}
    found = rs._compile(spec, model_fields=MODEL).errors
    has("an element that cannot be built is reported", found, "Sales[Missing] is not in the model")
    check("and the bookmarks that hide it do not add a complaint of their own",
          not any('needs "show" or "hide"' in e for e in found), str(found))
    spec = native_spec()
    spec["bookmarks"].append({"name": "empty", "page": "overview"})
    has("a bookmark that really names nothing still says so", rs._compile(spec, model_fields=MODEL).errors,
        'bookmark \'empty\': a bookmark needs "show" or "hide"')

    with tempfile.TemporaryDirectory() as base:
        # at apply time the report's own pages are read, so a wrong target is caught there
        root = make_report(base)
        hand_made(root)
        before = snapshot(base)
        spec = small_spec()
        spec["pages"][0]["elements"].append(
            {"name": "drill", "type": "button", "x": 900, "y": 24, "width": 100, "height": 32, "text": "Go",
             "action": {"type": "drillthrough", "target": "legacy"}})
        spec["pages"][0]["elements"][1]["tooltip_page"] = "Legacy"
        failed = rs.apply_report_spec(root, spec)
        has("a drillthrough button to a hand-made page that is no drillthrough page", failed["errors"],
            "page 'legacy' is in the report but is not a drillthrough page there")
        has("and a tooltip_page that is no tooltip page", failed["errors"],
            "page 'Legacy' is in the report but is not a tooltip page there")
        eq("nothing is written", snapshot(base), before)
        legacy = os.path.join(root, "definition", "pages", "legacy", "page.json")
        page = load(legacy)
        page.update({"type": "Drillthrough", "pageBinding": {"name": "theirs", "type": "Drillthrough"}})
        write_raw(legacy, json.dumps(page, indent=2).encode())
        del spec["pages"][0]["elements"][1]["tooltip_page"]
        made = rs.apply_report_spec(root, spec)
        eq("once the page is a drillthrough page in the report, the button is accepted",
           [made["success"], made["errors"]], [True, []])
        page.update({"type": "Tooltip", "pageBinding": {"name": "theirs", "type": "Tooltip"}})
        del page["pageBinding"]
        write_raw(legacy, json.dumps(page, indent=2).encode())
        spec["pages"][0]["elements"].pop()
        spec["pages"][0]["elements"][1]["tooltip_page"] = "Legacy"
        made = rs.apply_report_spec(root, spec)
        chart_file = os.path.join(root, "definition", "pages", "overview", "visuals", "by_month",
                                  "visual.json")
        shown = load(chart_file)["visual"]["visualContainerObjects"]["visualTooltip"][0]["properties"]
        eq("and a page Desktop saved as a tooltip page (a type, no binding) can be a tooltip_page",
           [made["errors"], shown["section"]], [[], pb.lit_str("legacy")])


def test_damaged_files_and_manifests():
    print("\n== files and manifests that cannot be read ==")
    with tempfile.TemporaryDirectory() as base:
        root = make_report(base)
        spec = native_spec()
        rs.apply_report_spec(root, spec, model_fields=MODEL)
        changed = native_spec()
        changed["pages"][0]["display_name"] = "Changed"
        changed["pages"][0]["elements"][0]["title"] = "Changed"
        changed["pages"][0]["elements"][0]["mobile"]["height"] = 222
        for rel in ("pages/overview/page.json", "pages/overview/visuals/by_segment/visual.json",
                    "pages/overview/visuals/by_segment/mobile.json"):
            path = os.path.join(root, "definition", *rel.split("/"))
            with open(path, "rb") as f:
                good = f.read()
            conflicted = b"<<<<<<< HEAD\n" + good + b"\n=======\n{\"mine\": true}\n>>>>>>> theirs\n"
            write_raw(path, conflicted)
            before = snapshot(base)
            for label, attempt in (("a changed spec", changed), ("the same spec", spec)):
                failed = rs.apply_report_spec(root, attempt, model_fields=MODEL)
                has(f"{rel} with merge markers, {label}: refused", failed["errors"],
                    f"definition/{rel} cannot be read as JSON")
                has("and it says what to do", failed["errors"], "fix it before applying a spec")
                eq("nothing is written, and the hand-made content is still there",
                   [failed["success"], failed["written"], snapshot(base)], [False, [], before])
            write_raw(path, b"[1, 2]")
            has(f"{rel} that holds a list", rs.apply_report_spec(root, changed, model_fields=MODEL)["errors"],
                f"definition/{rel} is not a JSON object")
            write_raw(path, good)
        eq("with the files mended the spec applies",
           rs.apply_report_spec(root, changed, model_fields=MODEL)["errors"], [])
        # a broken file the apply has no reason to write is not its business
        other = os.path.join(root, "definition", "pages", "overview", "visuals", "total", "mobile.json")
        gone = native_spec()
        del gone["pages"][0]["elements"][2]["mobile"]
        rs.apply_report_spec(root, gone, model_fields=MODEL)
        write_raw(other, b"{ not json")
        eq("a damaged phone file of an element the spec keeps off the phone is left alone",
           [rs.apply_report_spec(root, gone, model_fields=MODEL)["errors"], snapshot(root)
            ["definition/pages/overview/visuals/total/mobile.json"]], [[], b"{ not json"])

    with tempfile.TemporaryDirectory() as base:
        root = make_report(base)
        spec = native_spec()
        rs.apply_report_spec(root, spec, model_fields=MODEL)
        manifest_file = os.path.join(base, "Demo.Report.spec-manifest.json")
        good = load(manifest_file)
        changed = native_spec()
        changed["pages"][0]["display_name"] = "Changed"
        for label, damage, says in (
                ("pages is a list", lambda m: m.update(pages=[]), '"pages" is not an object'),
                ("a page entry is a list", lambda m: m["pages"].update(overview=[]),
                 "the entry of page 'overview' is not an object"),
                ("element_filters is a list", lambda m: m["pages"]["overview"].update(element_filters=[]),
                 '"element_filters" of page \'overview\' is not an object of name lists'),
                ("elements hold numbers", lambda m: m["pages"]["overview"].update(elements=[1, 2]),
                 '"elements" of page \'overview\' is not a list of names'),
                ("interactions are not pairs", lambda m: m["pages"]["overview"].update(interactions=["a"]),
                 '"interactions" of page \'overview\' is not a list of [from, to] pairs'),
                ("bookmarks is an object", lambda m: m.update(bookmarks={}),
                 '"bookmarks" is not a list of names'),
                ("custom_visuals is a list", lambda m: m.update(custom_visuals=[]),
                 '"custom_visuals" is not an object of file lists')):
            manifest = copy.deepcopy(good)
            damage(manifest)
            write_raw(manifest_file, json.dumps(manifest, indent=2).encode())
            before = snapshot(base)
            for attempt, options in ((spec, {}), (changed, {}), ({"pages": []}, {"prune": True})):
                try:
                    failed = rs.apply_report_spec(root, attempt, model_fields=MODEL, **options)
                except Exception as e:                                   # the bug this guards against
                    failed = {"success": None, "errors": [f"{type(e).__name__}: {e}"], "written": None}
                has(f"a manifest where {label}: a clear error", failed["errors"],
                    f"the manifest Demo.Report.spec-manifest.json is damaged ({says})")
                eq("and nothing is written", [failed["success"], failed["written"], snapshot(base)],
                   [False, [], before])
        write_raw(manifest_file, json.dumps(good, indent=2).encode())
        eq("the manifest restored, the spec applies again",
           rs.apply_report_spec(root, spec, model_fields=MODEL)["errors"], [])


def test_tooltip_page_size():
    print("\n== the size of a tooltip page ==")

    def card(width, height):
        return {"name": "c", "type": "visual", "visual_type": "card", "x": 0, "y": 0, "width": width,
                "height": height, "fields": {"Values": "Sales.Total Sales"}}

    spec = {"pages": [{"name": "tip", "tooltip": True, "elements": [card(600, 400)]}]}
    found = rs.validate_report_spec(spec, model_fields=MODEL)
    has("an element too large for a tooltip page is told why the page is small", found,
        "page 'tip', element 'c': at (0, 0) size 600 x 400 it does not fit the page, which is 320 x 240 "
        "because it is a tooltip page and gives no size")
    has("and what to do about it", found, 'give the page its own "width" and "height" for a larger tooltip')
    check("letting the element overflow is not the advice", not any("allow_overflow" in e for e in found))
    spec["page_defaults"] = {"width": 1000, "height": 500}
    has("page_defaults do not change that", rs.validate_report_spec(spec, model_fields=MODEL),
        "which is 320 x 240 because it is a tooltip page")
    spec["pages"][0].update(width=640, height=480)
    eq("the page's own size is the remedy", rs.validate_report_spec(spec, model_fields=MODEL), [])
    spec["pages"][0]["elements"] = [card(700, 400)]
    has("a page that gave its size gets the usual message", rs.validate_report_spec(spec, model_fields=MODEL),
        "it does not fit the 640 x 480 page (set \"allow_overflow\": true if that is intended)")

    with tempfile.TemporaryDirectory() as base:
        # a tooltip page an earlier release built at the size of the other pages
        root = make_report(base)
        before = {"page_defaults": {"width": 1000, "height": 500},
                  "pages": [{"name": "tip", "tooltip": True, "width": 1000, "height": 500,
                             "elements": [card(300, 200)]}]}
        rs.apply_report_spec(root, before, model_fields=MODEL)
        now = copy.deepcopy(before)
        del now["pages"][0]["width"], now["pages"][0]["height"]
        plan = rs.apply_report_spec(root, now, model_fields=MODEL, dry_run=True)
        real = rs.apply_report_spec(root, now, model_fields=MODEL)
        has("an apply that resizes an existing tooltip page says so", real["warnings"],
            "page 'tip' is a tooltip page and gives no size of its own, so it becomes 320 x 240 (it was "
            "1000 x 500). Give the page \"width\" and \"height\" to keep another size.")
        eq("the dry run said it first", plan["warnings"], real["warnings"])
        page = load(os.path.join(root, "definition", "pages", "tip", "page.json"))
        eq("the page is the tooltip size, typed and hidden as Desktop saves one",
           [page["width"], page["height"], page["type"], page["visibility"], "pageBinding" in page],
           [320, 240, "Tooltip", "HiddenInViewMode", False])
        eq("with nothing left to change there is nothing to warn about",
           rs.apply_report_spec(root, now, model_fields=MODEL)["warnings"], [])
        ordinary = {"pages": [{"name": "tip", "elements": [card(300, 200)]}]}
        rs.apply_report_spec(root, ordinary, model_fields=MODEL)
        check("a page the spec no longer calls a tooltip page loses the type the spec gave it",
              "type" not in load(os.path.join(root, "definition", "pages", "tip", "page.json")))


def test_custom_visual_drill_roles_and_definitions():
    print("\n== custom visuals: drill roles, and building from a definition ==")
    levels = {"category": [{"column": "Customer.Segment"}, {"column": "Customer.Name"}],
              "value": [{"measure": "Sales.Total Sales"}]}

    def placed(name, custom, fields, y=0):
        return {"name": name, "type": "visual", "custom": custom, "x": 0, "y": y, "width": 300, "height": 100,
                "fields": fields}

    with tempfile.TemporaryDirectory() as base:
        root = make_report(base)
        make_pbiviz(os.path.join(base, "drill.pbiviz"), guid="drillVisualABCDEF0123456789",
                    capabilities={"dataRoles": [{"name": "category", "kind": "Grouping"},
                                                {"name": "value", "kind": "Measure"}],
                                  "drilldown": {"roles": ["category"]}})
        make_pbiviz(os.path.join(base, "flat.pbiviz"), guid="flatVisualABCDEF0123456789")
        spec = {"custom_visuals": [{"pbiviz": "drill.pbiviz"}, {"pbiviz": "flat.pbiviz"}],
                "pages": [{"name": "p", "elements": [
                    placed("two", "drill", levels),
                    placed("one", "drill", {"category": [{"column": "Customer.Segment"}],
                                            "value": [{"measure": "Sales.Total Sales"}]}, 110),
                    placed("chosen", "drill", {"category": [{"column": "Customer.Segment", "active": False},
                                                            {"column": "Customer.Name", "active": True}],
                                               "value": [{"measure": "Sales.Total Sales"}]}, 220),
                    placed("flat", "flat", levels, 330)]}]}
        result = rs.apply_report_spec(root, spec, model_fields=MODEL)
        eq("it applies", [result["success"], result["errors"]], [True, []])

        def active(name, role):
            doc = load(os.path.join(root, "definition", "pages", "p", "visuals", name, "visual.json"))
            return [p.get("active", False) for p in doc["visual"]["query"]["queryState"][role]["projections"]]

        eq("two fields in a role the package drills on: the first is the level shown",
           [active("two", "category"), active("two", "value")], [[True, False], [False]])
        eq("one field in it needs no mark", active("one", "category"), [False])
        eq("a field can say for itself which level is active", active("chosen", "category"), [False, True])
        eq("a package that names no drill role is written as before", active("flat", "category"),
           [False, False])
    has("active must be true or false", rs.validate_report_spec({"pages": [{"name": "p", "elements": [chart(
        "e", fields={"Category": {"column": "Customer.Segment", "active": "yes"}})]}]}, model_fields=MODEL),
        "\"active\" of Customer[Segment] must be true or false")

    import custom_visual                      # the builder is another module; it is only called here
    definition = {
        "name": "tinyList", "display_name": "Tiny list", "version": "1.0.0.0",
        "author": {"name": "Tests", "email": "tests@example.test"},
        "roles": [{"name": "category", "kind": "grouping"}, {"name": "value", "kind": "measure", "max": 1}],
        "features": {"drilldown": "category"},
        "css": ".row { padding: 2px 4px; }",
        "js": "function render(ctx) { return ctx.rows.map(function (row) { return '<div class=\"row\" "
              "data-row=\"' + row.index + '\">' + ctx.escape(row.text.category) + '</div>'; }).join(''); }"}
    with tempfile.TemporaryDirectory() as base:
        root = make_report(base)
        folder = os.path.join(base, "visuals", "tiny_list")
        write_raw(os.path.join(folder, "definition.json"), json.dumps(definition, indent=2).encode())
        spec = {"custom_visuals": [{"definition": "visuals/tiny_list/definition.json"}],
                "pages": [{"name": "p", "elements": [placed("list", "tiny_list", levels)]}]}
        before = snapshot(base)
        plan = rs.apply_report_spec(root, spec, model_fields=MODEL, dry_run=True)
        eq("a dry run builds the visual and writes nothing",
           [plan["success"], plan["errors"], snapshot(base)], [True, [], before])
        result = rs.apply_report_spec(root, spec, model_fields=MODEL)
        eq("a definition is built and placed in one apply", [result["success"], result["errors"]], [True, []])
        eq("as the dry run said", plan["written"], result["written"])
        guid = custom_visual.visual_guid("tinyList")
        eq("the package is unpacked like any .pbiviz",
           sorted(os.listdir(os.path.join(root, "CustomVisuals"))), [guid])
        check("with its metadata file", os.path.isfile(os.path.join(root, "CustomVisuals", guid, "resources",
                                                                    f"{guid}.pbiviz.json")))
        report = load(os.path.join(root, "definition", "report.json"))
        eq("and registered in report.json", [p["name"] for p in report["resourcePackages"]
                                             if p["type"] == "CustomVisual"], [guid])
        doc = load(os.path.join(root, "definition", "pages", "p", "visuals", "list", "visual.json"))
        eq("the element is the visual, by its alias (the definition's folder)", doc["visual"]["visualType"],
           guid)
        eq("its drill role, read from the built package, starts on the first of two fields",
           [p.get("active", False) for p in doc["visual"]["query"]["queryState"]["category"]["projections"]],
           [True, False])
        eq("the structural check finds nothing", self_check(root), [])
        state = snapshot(base)
        again = rs.apply_report_spec(root, spec, model_fields=MODEL)
        eq("the build is the same every time, so a second apply writes nothing",
           [again["written"], again["removed"], snapshot(base)], [[], [], state])

        definition["css"] = ".row { padding: 6px; }"
        write_raw(os.path.join(folder, "definition.json"), json.dumps(definition, indent=2).encode())
        rebuilt = rs.apply_report_spec(root, spec, model_fields=MODEL)
        eq("a changed definition rewrites the unpacked visual and nothing else", rebuilt["written"],
           [f"CustomVisuals/{guid}/resources/{guid}.pbiviz.json"])

        broken = {k: v for k, v in definition.items() if k not in ("roles", "js")}
        write_raw(os.path.join(base, "visuals", "bad", "definition.json"), json.dumps(broken).encode())
        spec["custom_visuals"].append({"definition": "visuals/bad/definition.json"})
        state = snapshot(base)
        failed = rs.apply_report_spec(root, spec, model_fields=MODEL)
        check("a definition that does not build is a spec error that names the visual",
              not failed["success"] and any(e.startswith("custom visual 'bad': ") for e in failed["errors"]),
              str(failed["errors"]))
        eq("and nothing is written", [failed["written"], snapshot(base)], [[], state])
        spec["custom_visuals"][1] = {"definition": "visuals/absent/definition.json", "alias": "gone"}
        has("a definition file that is not there",
            rs.apply_report_spec(root, spec, model_fields=MODEL)["errors"],
            "custom visual 'gone': definition file could not be read")
        pruned = rs.apply_report_spec(root, {"pages": []}, model_fields=MODEL, prune=True)
        check("a prune takes a built visual away like a packaged one",
              pruned["success"] and not os.path.exists(os.path.join(root, "CustomVisuals")))
    has("custom_visuals still needs a source", rs.validate_report_spec(
        {"custom_visuals": [{"alias": "x"}], "pages": []}),
        'give either "appsource": "<visual id>", "pbiviz": "<path>" or "definition": "<path>"')
    eq("a definition entry is enough to validate a spec that uses it", rs.validate_report_spec(
        {"custom_visuals": [{"definition": "x/definition.json", "alias": "mine"}],
         "pages": [{"name": "p", "elements": [placed("list", "mine", levels)]}]}, model_fields=MODEL), [])


def test_deneb_settings_and_html_editions():
    print("\n== Deneb's own settings, and the editions of HTML Content ==")
    deneb = {"type": "deneb", "fields": ["Customer.Segment", "Sales.Total Sales"], "spec": {"mark": "bar"},
             "config": {"view": {"stroke": "transparent"}}, "selection": True}
    c, doc = compile_one({**deneb, "objects": {
        "vega": {"renderMode": "canvas", "enableContextMenuSelector": True, "selectionMode": "advanced"},
        "display": {"scrollbarRadius": 4}}})
    vega = doc["visual"]["objects"]["vega"][0]["properties"]
    eq("objects.vega is added to the element's own vega object", c.errors, [])
    check("the specification, the config and the switches are all still there",
          {"jsonSpec", "jsonConfig", "provider", "version", "enableTooltips", "enableSelection",
           "selectionMaxDataPoints"} <= set(vega), str(sorted(vega)))
    eq("the spec is the element's", vega["jsonSpec"], pb.json_literal({"mark": "bar"}))
    eq("the settings given are in",
       [vega["renderMode"], vega["enableContextMenuSelector"], vega["selectionMode"]],
       [pb.lit_str("canvas"), pb.lit_bool(True), pb.lit_str("advanced")])
    eq("any other object is the caller's, as before", doc["visual"]["objects"]["display"],
       [{"properties": {"scrollbarRadius": pb.lit_num(4)}}])
    c, _ = compile_one({**deneb, "objects": {"vega": {"jsonSpec": "{}"}}})
    has("a property the element's own key writes is refused", c.errors,
        "page 'p', element 'e': objects.vega.jsonSpec is what the element's own \"spec\" sets; give it there")
    c, _ = compile_one({**deneb, "objects": {"vega": {"enableSelection": False}}})
    has("selection too", c.errors,
        "objects.vega.enableSelection is what the element's own \"selection\" sets")
    c, doc = compile_one({**deneb, "objects": {"vega": {"enableHighlight": True}}})
    eq("a switch the element did not set can be set there",
       [c.errors, doc["visual"]["objects"]["vega"][0]["properties"]["enableHighlight"]],
       [[], pb.lit_bool(True)])
    c, _ = compile_one({**deneb, "cross_highlight": True, "objects": {"vega": {"enableHighlight": False}}})
    has("but not said twice", c.errors, "objects.vega.enableHighlight is what the element's own "
        "\"cross_highlight\" sets")
    c, _ = compile_one({**deneb, "objects": {"vega": [{"properties": {"renderMode": pb.lit_str("svg")}}]}})
    has("a whole replacement of vega is refused", c.errors, "objects.vega must be an object of settings")
    for bad in (0, 251, 2.5, "many", True):
        c, _ = compile_one({**deneb, "max_selected": bad})
        has(f"max_selected {bad!r} is outside what Deneb allows", c.errors,
            "page 'p', element 'e': max_selected must be a whole number from 1 to 250")
    c, doc = compile_one({**deneb, "max_selected": 250})
    eq("250 is the most",
       [c.errors, doc["visual"]["objects"]["vega"][0]["properties"]["selectionMaxDataPoints"]],
       [[], pb.lit_num(250)])
    c, doc = compile_one(deneb)
    eq("the version stamped is the one Deneb 2.0 writes", doc["visual"]["objects"]["vega"][0]["properties"]
       ["version"], pb.lit_str("6.4.3"))

    html = {"type": "html", "measure": "Sales.Summary HTML"}
    regular, secure = pb.HTML_CONTENT_EDITIONS["regular"], pb.HTML_CONTENT_EDITIONS["secure"]
    c, doc = compile_one(html)
    eq("an html element with no edition is the regular one, as it always was",
       [doc["visual"]["visualType"], c.public_ids], [regular, [regular]])
    c, doc = compile_one({**html, "edition": "secure"})
    eq("the secure edition by name", [c.errors, doc["visual"]["visualType"], c.public_ids],
       [[], secure, [secure]])
    c, doc = compile_one({**html, "edition": "regular"})
    eq("the regular edition by name", [c.errors, doc["visual"]["visualType"]], [[], regular])
    c, doc = compile_one({**html, "visual_id": secure})
    eq("visual_id still works", [c.errors, doc["visual"]["visualType"]], [[], secure])
    c, _ = compile_one({**html, "edition": "lite"})
    has("an unknown edition", c.errors, "page 'p', element 'e': edition must be \"regular\" or \"secure\", "
        "not 'lite'")
    c, _ = compile_one({**html, "edition": "secure", "visual_id": regular})
    has("an edition and an id together", c.errors, "give edition or visual_id, not both")
    with tempfile.TemporaryDirectory() as base:
        root = make_report(base)
        spec = {"pages": [{"name": "p", "elements": [
            {"name": "a", "x": 0, "y": 0, "width": 100, "height": 100, **html, "edition": "secure"},
            {"name": "b", "x": 0, "y": 110, "width": 100, "height": 100, **html, "edition": "regular"}]}]}
        rs.apply_report_spec(root, spec, model_fields=MODEL)
        eq("both editions in use are registered", sorted(load(os.path.join(root, "definition", "report.json"))
                                                         ["publicCustomVisuals"]), sorted([regular, secure]))


def test_groups_with_a_place_of_their_own():
    print("\n== a group that is not at the page's corner ==")
    box = {"type": "shape", "width": 120, "height": 80}
    spec = {"pages": [{"name": "p", "elements": [
        {"name": "placed", "type": "group", "x": 300, "y": 200, "width": 500, "height": 400, "elements": [
            {"name": "inner", "x": 340, "y": 240, **box},
            {"name": "corner", "x": 300, "y": 200, **box}]},
        {"name": "wide", "type": "group", "elements": [{"name": "free", "x": 340, "y": 240, **box}]}]}]}
    c = rs._compile(spec)
    eq("it compiles", c.errors, [])
    at = {el.name: [el.doc["position"][k] for k in ("x", "y", "width", "height")]
          for el in c.pages[0].elements}
    eq("a group with a box of its own is written where the spec puts it", at["placed"], [300, 200, 500, 400])
    eq("its member, placed on the page in the spec, is written from the group's corner (Desktop draws a "
       "member there)", at["inner"], [40, 40, 120, 80])
    eq("a member in the group's corner is at its origin", at["corner"], [0, 0, 120, 80])
    eq("a group with no box spans the page", at["wide"], [0, 0, 1280, 720])
    eq("so its member's position is its place on the page, as before", at["free"], [340, 240, 120, 80])
    eq("members still belong to their group", [el.doc.get("parentGroupName") for el in c.pages[0].elements],
       [None, "placed", "placed", None, "wide"])

    def errors(change):
        changed = copy.deepcopy(spec)
        change(changed["pages"][0]["elements"][0])
        return rs._compile(changed).errors

    has("a member outside its group's box", errors(lambda g: g["elements"][0].update(x=900)),
        "page 'p', element 'inner': at (900, 240) size 120 x 80 it does not fit its group 'placed', which is "
        "at (300, 200) size 500 x 400; members are placed in page coordinates")
    eq("unless it is meant to hang over", errors(lambda g: g["elements"][0].update(x=750,
                                                                                   allow_overflow=True)), [])
    has("a group box that is half given", errors(lambda g: g.pop("width")),
        "page 'p', element 'placed': needs numeric width")
    has("a group box off the page", errors(lambda g: g.update(x=1000)), "does not fit the 1280 x 720 page")
    with tempfile.TemporaryDirectory() as base:
        root = make_report(base)
        spec["pages"][0]["popups"] = [{"name": "pop",
                                       "box": {"x": 400, "y": 300, "width": 300, "height": 200},
                                       "elements": [{"name": "in_pop", "x": 420, "y": 380, **box}]}]
        result = rs.apply_report_spec(root, spec)
        eq("it applies", [result["success"], result["errors"]], [True, []])
        eq("and the structure is sound", self_check(root), [])

        def position(name):
            pos = load(os.path.join(root, "definition", "pages", "p", "visuals", name, "visual.json"))
            return [pos["position"]["x"], pos["position"]["y"]]

        eq("a pop-up's group is the page, so its members are where the spec says",
           [position("pop"), position("pop_card"), position("in_pop")], [[0, 0], [400, 300], [420, 380]])
        eq("on disk the placed group's member is relative to it", position("inner"), [40, 40])
        eq("a second apply writes nothing", rs.apply_report_spec(root, spec)["written"], [])


if __name__ == "__main__":
    for fn in (test_validation, test_documented_example, test_fields, test_full_spec,
               test_dry_run_and_failures, test_existing_report, test_prune, test_carry_over,
               test_layering_and_tokens, test_old_specs_are_unchanged, test_filters,
               test_drillthrough_and_tooltip_pages, test_interactions, test_phone_layout,
               test_bookmark_groups, test_interactivity_switches, test_native_spec_end_to_end,
               test_hand_edits_inside_a_binding_and_the_interactions, test_values_that_would_break_the_report,
               test_damaged_files_and_manifests, test_tooltip_page_size,
               test_custom_visual_drill_roles_and_definitions, test_deneb_settings_and_html_editions,
               test_groups_with_a_place_of_their_own):
        fn()
    print()
    if _failures:
        print(f"FAILED: {len(_failures)} check(s): {_failures}")
        sys.exit(1)
    print("ALL REPORT SPEC TESTS PASSED")
