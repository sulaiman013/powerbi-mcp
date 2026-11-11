"""
Tests for src/custom_visual.py (definition -> .pbiviz, validator, safety pass, inspector) and
for the runtime in src/visual_kit, which is run in Node against a model of the visual host.
No Power BI required; the Node checks print [SKIP] when node is not on PATH.
Run: python tests/test_custom_visual.py
"""
import base64
import copy
import html
import json
import os
import re
import shutil
import struct
import sys
import tempfile
import zipfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))
import custom_visual as cv  # noqa: E402
import js_app_measure as jm  # noqa: E402
import pbir_packages  # noqa: E402

NODE = shutil.which("node")
EXAMPLES = os.path.join(ROOT, "examples", "custom_visuals")
_failures = []

# Key sets of a package built by the official toolchain (powerbi-visuals-tools 7, API 5.11.0)
# that Power BI Desktop loads. A package written here must carry the same ones.
REAL_PACKAGE_KEYS = ["version", "author", "resources", "visual", "metadata"]
REAL_METADATA_KEYS = ["visual", "author", "apiVersion", "style", "stringResources", "capabilities", "content",
                      "visualEntryPoint", "externalJS", "assets"]
REAL_VISUAL_KEYS = ["name", "displayName", "guid", "visualClassName", "version", "description", "supportUrl",
                    "gitHubUrl"]

JS = """
function render(ctx) {
  return ctx.rows.map(function (row) {
    return '<div class="item" data-row="' + row.index + '">' + ctx.escape(row.text.category) + '=' +
      ctx.escape(row.text.value) + '</div>';
  }).join("") + '<p class="gap">space</p>';
}
"""
DATA = {"columns": [{"role": "category", "name": "city", "kind": "grouping"},
                    {"role": "value", "name": "Customers", "kind": "measure", "format": "#,##0"}],
        "rows": [["Mumbai", 1078], ["Chennai", 834], ["Bengaluru", 751]]}
KEYS = ['t.city="Mumbai"', 't.city="Chennai"', 't.city="Bengaluru"']


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f": {detail}" if detail and not cond else ""))
    if not cond:
        _failures.append(name)


def eq(name, got, want):
    check(name, got == want, f"got {got!r}, want {want!r}")


def definition(**over):
    base = {"name": "testList", "display_name": "Test list",
            "roles": [{"name": "category", "kind": "grouping"}, {"name": "value", "kind": "measure"}],
            "js": JS, "css": ".item { padding: 2px }"}
    base.update(over)
    return {k: v for k, v in base.items() if v is not None}


def build(defn, folder, name="v.pbiviz", **kw):
    path = os.path.join(folder, name)
    kw.setdefault("check_with_node", False)
    return cv.build_custom_visual(defn, path, **kw), path


def built(defn, folder, name, **kw):
    """The path of a package that must build: a refusal is a failed check, not a later crash."""
    result, path = build(defn, folder, name, **kw)
    if not result["success"]:
        check(f"{name} builds", False, str(result["errors"]))
    return path


def metadata(path):
    with zipfile.ZipFile(path) as z:
        name = [n for n in z.namelist() if n.endswith(".pbiviz.json")][0]
        return json.loads(z.read(name).decode("utf-8"))


def members(path):
    with zipfile.ZipFile(path) as z:
        return z.namelist()


def read_bytes(path):
    with open(path, "rb") as f:
        return f.read()


def calls(sim, name, step=None):
    return [e for e in sim["log"] if e["call"] == name and (step is None or e["step"] == step)]


def classes(state, row):
    return [r for r in state["rows"] if r["row"] == str(row)][0]["classes"]


def errors_of(defn, base_dir=None):
    return cv.validate_definition(defn, base_dir)["errors"]


# ---------------------------------------------------------------- pure Python

def test_guid():
    print("\n== guid ==")
    guid = cv.visual_guid("barList")
    check("the guid is the name followed by 32 hex digits", re.fullmatch(r"barList[0-9A-F]{32}", guid) is not None, guid)
    eq("the guid depends only on the name", cv.visual_guid("barList"), guid)
    check("another name gives another guid", cv.visual_guid("barList2")[-32:] != guid[-32:])
    with tempfile.TemporaryDirectory() as d:
        first, _ = build(definition(), d, "a.pbiviz")
        changed, _ = build(definition(js=JS + "\n// changed", display_name="Renamed", version="2.1.0"), d, "b.pbiviz")
        eq("a rebuild with other code, title and version keeps the guid", changed["guid"], first["guid"])
        eq("a three-part version gains its fourth part", changed["version"], "2.1.0.0")
        given, _ = build(definition(guid="myVisual0123456789ABCDEF"), d, "c.pbiviz")
        eq("a guid given in the definition is used as it is", given["guid"], "myVisual0123456789ABCDEF")
        check("the given guid names the resource file", "resources/myVisual0123456789ABCDEF.pbiviz.json"
              in members(os.path.join(d, "c.pbiviz")))


def test_package_structure():
    print("\n== package structure ==")
    with tempfile.TemporaryDirectory() as d:
        result, path = build(definition(author={"name": "A. Author", "email": "a@example.com"},
                                        description="What it shows"), d)
        check("the build succeeds", result["success"] and result["written"], str(result["errors"]))
        guid = result["guid"]
        with zipfile.ZipFile(path) as z:
            names = z.namelist()
            package = json.loads(z.read("package.json").decode("utf-8"))
            raw = z.read(f"resources/{guid}.pbiviz.json")
            infos = z.infolist()
        eq("archive members, in the official order", names,
           ["package.json", "resources/", f"resources/{guid}.pbiviz.json"])
        eq("package.json has the keys of a real package", list(package), REAL_PACKAGE_KEYS)
        eq("package.json points at the metadata file", package["resources"],
           [{"resourceId": "rId0", "sourceType": 5, "file": f"resources/{guid}.pbiviz.json"}])
        eq("package.json names the resource as the pbiviz json", package["metadata"],
           {"pbivizjson": {"resourceId": "rId0"}})
        check("the metadata file is UTF-8 without a byte order mark", raw[:1] == b"{")
        meta = json.loads(raw.decode("utf-8"))
        eq("the metadata has the keys of a real package, in order", list(meta), REAL_METADATA_KEYS)
        eq("the visual block has the keys of a real package", list(meta["visual"]), REAL_VISUAL_KEYS)
        eq("both files describe the same visual", package["visual"], meta["visual"])
        eq("visualClassName", meta["visual"]["visualClassName"], "Visual")
        eq("apiVersion", meta["apiVersion"], cv.API_VERSION)
        eq("author travels to both files", (meta["author"], package["author"]),
           ({"name": "A. Author", "email": "a@example.com"},) * 2)
        eq("content holds script, stylesheet and icon", sorted(meta["content"]), ["css", "iconBase64", "js"])
        eq("stringResources / externalJS / entry point as the packager leaves them",
           (meta["stringResources"], meta["externalJS"], meta["visualEntryPoint"]), ({}, [], ""))
        icon = meta["content"]["iconBase64"]
        png = base64.b64decode(icon.split(",", 1)[1])
        check("the icon is a PNG data URI", icon.startswith("data:image/png;base64,") and png[:8] == cv._PNG_SIGNATURE)
        eq("the default icon is 20 by 20", struct.unpack(">II", png[16:24]), (20, 20))

        js = meta["content"]["js"]
        check("the script declares the global named after the guid", js.startswith(f"var {guid};"))
        check("the script registers the plugin under its guid",
              f'powerbi.visuals.plugins["{guid}"]=plugin' in js.replace(" ", ""))
        check("the script exports the plugin as the default of that global", f'{guid} = {{ "default": plugin }};' in js)
        check("the plugin names its class, API version and factory",
              all(s in js for s in ('"class":"Visual"', f'apiVersion:"{cv.API_VERSION}"', "return new Visual(options)",
                                    "custom:true")), js[-600:])
        check("the class contract is there: update, getFormattingModel, destroy",
              all(f"Visual.prototype.{m}=function" in js.replace(" ", "") for m in ("update", "getFormattingModel", "destroy")))
        check("the author's code sits in its own function", "KIT.load = function (APP) {" in js)
        check("the kit marks its output", cv.KIT_VERSION in js)
        check("the stylesheet holds the kit's rules and the author's", ".vk-root" in meta["content"]["css"]
              and ".item{padding: 2px}" in meta["content"]["css"], meta["content"]["css"][-80:])
        check("every capabilities key is one the API defines", set(meta["capabilities"]) <= cv.CAPABILITY_KEYS,
              str(set(meta["capabilities"]) - cv.CAPABILITY_KEYS))
        eq("no privilege is asked for", meta["capabilities"]["privileges"], [])
        eq("the report holds the capabilities that were written", result["capabilities"], meta["capabilities"])
        eq("size_bytes is the size of the file", result["size_bytes"], os.path.getsize(path))

        read = pbir_packages.read_pbiviz(path)
        eq("the report builder's reader accepts the package", (read["guid"], read["name"], read["version"]),
           (guid, "testList", "1.0.0.0"))
        placed = pbir_packages.custom_visual_files(guid, read["files"])
        eq("and unpacks it where a report keeps custom visuals", sorted(placed),
           [f"CustomVisuals/{guid}/package.json", f"CustomVisuals/{guid}/resources/{guid}.pbiviz.json"])
        check("member timestamps and attributes are fixed", all(
            i.date_time == (1980, 1, 1, 0, 0, 0) and i.create_system == 0 for i in infos))

        plain, plain_path = build(definition(), d, "plain.pbiviz", minify=False)
        raw_js = metadata(plain_path)["content"]["js"]
        check("minify=False ships the sources as written", "/* ==== visual.js ==== */" in raw_js
              and "// The visual the host talks to." in raw_js and plain["script_chars"] > result["script_chars"])

        icon_path = os.path.join(d, "icon.png")
        with open(icon_path, "wb") as f:
            f.write(cv._default_icon()[:-12] + b"\0" * 4 + cv._default_icon()[-12:])
        own, own_path = build(definition(icon_file="icon.png"), d, "icon.pbiviz", base_dir=d)
        check("an icon file is embedded as given", own["success"] and base64.b64decode(
            metadata(own_path)["content"]["iconBase64"].split(",", 1)[1]) == read_bytes(icon_path))
        with open(icon_path, "wb") as f:
            f.write(b"GIF89a")
        check("an icon that is not a PNG is refused", any("not a PNG" in e for e in errors_of(definition(icon_file="icon.png"), d)))

        nothing = cv.build_custom_visual(definition(), check_with_node=False)
        check("without an output path nothing is written, and the report says so",
              nothing["success"] and nothing["written"] is False and nothing["path"] is None)


def test_determinism():
    print("\n== determinism ==")
    with tempfile.TemporaryDirectory() as d:
        a, pa = build(definition(), d, "a.pbiviz")
        b, pb = build(copy.deepcopy(definition()), os.path.join(d, "elsewhere"), "b.pbiviz")
        eq("the same definition gives the same bytes", read_bytes(pa), read_bytes(pb))
        eq("and the same digest in the report", a["sha256"], b["sha256"])
        with open(os.path.join(d, "render.js"), "w", encoding="utf-8", newline="\r\n") as f:
            f.write(JS)
        c, pc = build(definition(js=None, js_files=["render.js"]), d, "c.pbiviz", base_dir=d)
        eq("the script read from a file (with CRLF line ends) gives the same bytes as inline",
           read_bytes(pc), read_bytes(pa))
        e, _ = build(definition(js=JS.replace("space", "gap")), d, "e.pbiviz")
        check("a change in the script changes the bytes, not the guid", e["sha256"] != a["sha256"] and e["guid"] == a["guid"])
        again = cv.build_custom_visual(definition(), pa, check_with_node=False)
        eq("building over an existing file leaves the same bytes", again["sha256"], a["sha256"])
        check("no temp file is left beside the package", sorted(os.listdir(os.path.join(d, "elsewhere"))) == ["b.pbiviz"])


def test_capabilities():
    print("\n== capabilities per feature ==")
    caps = cv.build_custom_visual(definition(), check_with_node=False)["capabilities"]
    eq("data roles", caps["dataRoles"], [{"displayName": "Category", "name": "category", "kind": "Grouping"},
                                         {"displayName": "Value", "name": "value", "kind": "Measure"}])
    eq("one categorical mapping: grouping roles as categories, measures as values", caps["dataViewMappings"], [
        {"categorical": {"categories": {"for": {"in": "category"}, "dataReductionAlgorithm": {"top": {"count": 1001}}},
                         "values": {"select": [{"for": {"in": "value"}}]}}}])
    eq("defaults: every native feature that is harmless is on",
       {k: caps.get(k) for k in ("supportsHighlight", "supportsKeyboardFocus", "supportsLandingPage",
                                 "supportsEmptyDataView", "supportsMultiVisualSelection", "sorting", "tooltips")},
       {"supportsHighlight": True, "supportsKeyboardFocus": True, "supportsLandingPage": True,
        "supportsEmptyDataView": True, "supportsMultiVisualSelection": True, "sorting": {"default": {}},
        "tooltips": {"supportedTypes": {"default": True, "canvas": True}, "supportEnhancedTooltips": True}})
    check("drill-down is off unless asked for", "drilldown" not in caps)

    switches = {"highlight": ["supportsHighlight"], "keyboard": ["supportsKeyboardFocus"],
                "landing_page": ["supportsLandingPage", "supportsEmptyDataView"],
                "multi_visual_selection": ["supportsMultiVisualSelection"], "sorting": ["sorting"],
                "tooltips": ["tooltips"]}
    for feature, keys in sorted(switches.items()):
        off = cv.build_custom_visual(definition(features={feature: False}), check_with_node=False)
        check(f"features.{feature}: false removes {', '.join(keys)}",
              off["success"] and not any(k in off["capabilities"] for k in keys) and
              set(caps) - set(off["capabilities"]) == set(keys), str(sorted(off.get("capabilities", {}))))
    no_select = cv.build_custom_visual(definition(features={"selection": False}), check_with_node=False)
    check("features.selection: false also drops multi-visual selection and bookmarks",
          "supportsMultiVisualSelection" not in no_select["capabilities"] and not no_select["features"]["selection"]
          and not no_select["features"]["bookmarks"])
    check("features.selection: false is carried into the script's configuration",
          '"selection":false' in cv.assemble_script(cv._normalize(definition(features={"selection": False}), None)[0]))
    no_menu = cv.build_custom_visual(definition(features={"context_menu": False}), check_with_node=False)
    check("features.context_menu: false is a script switch (no capability exists for it)",
          no_menu["capabilities"] == caps and no_menu["features"]["context_menu"] is False)

    drill = cv.build_custom_visual(definition(
        roles=[{"name": "category", "kind": "grouping", "max": 3}, {"name": "value", "kind": "measure", "max": 1},
               {"name": "extra", "kind": "measure", "tooltips": True, "display_name": "Tooltips",
                "description": "More measures"}],
        features={"drilldown": True}), check_with_node=False)
    eq("drilldown names the role", drill["capabilities"]["drilldown"], {"roles": ["category"]})
    eq("the drill role is limited to one field, as the host requires; other limits are kept",
       drill["capabilities"]["dataViewMappings"][0]["conditions"], [{"category": {"max": 1}, "value": {"max": 1}}])
    check("and the build says it changed the limit", any("max is set to 1" in w for w in drill["warnings"]))
    eq("a tooltips role is listed for the tooltip well", drill["capabilities"]["tooltips"]["roles"], ["extra"])
    eq("role display name and description are carried", drill["capabilities"]["dataRoles"][2],
       {"displayName": "Tooltips", "name": "extra", "kind": "Measure", "description": "More measures"})
    eq("the feature report names the drill role", drill["features"]["drilldown"], "category")

    two = cv.build_custom_visual(definition(roles=[{"name": "a", "kind": "grouping"}, {"name": "b", "kind": "grouping"},
                                                   {"name": "m", "kind": "measure"}, {"name": "n", "kind": "measure"}]),
                                 check_with_node=False)["capabilities"]["dataViewMappings"][0]["categorical"]
    eq("several grouping roles are all categories", two["categories"]["select"], [{"for": {"in": "a"}}, {"for": {"in": "b"}}])
    eq("several measure roles are all values", two["values"], {"select": [{"for": {"in": "m"}}, {"for": {"in": "n"}}]})

    measures = cv.build_custom_visual(definition(roles=[{"name": "values", "kind": "measure"}]), check_with_node=False)
    check("measures only: no categories, no selection, still highlights",
          "categories" not in measures["capabilities"]["dataViewMappings"][0]["categorical"]
          and measures["features"]["selection"] is False and measures["capabilities"].get("supportsHighlight") is True
          and "supportsMultiVisualSelection" not in measures["capabilities"])
    columns = cv.build_custom_visual(definition(roles=[{"name": "category", "kind": "grouping"}]), check_with_node=False)
    check("columns only: no values, no highlights, still selection",
          "values" not in columns["capabilities"]["dataViewMappings"][0]["categorical"]
          and "supportsHighlight" not in columns["capabilities"] and columns["features"]["selection"] is True)

    def reduction(rows):
        built = cv.build_custom_visual(definition(rows=rows), check_with_node=False)
        return (built["capabilities"]["dataViewMappings"][0]["categorical"]["categories"]["dataReductionAlgorithm"],
                built["features"]["fetch_more_data"])

    eq("below one window of rows: a top-N limit of one row more than the visual takes, so a cut shows",
       reduction(29999), ({"top": {"count": 30000}}, False))
    eq("exactly one window: rows are served in a window, which says when more exist", reduction(30000),
       ({"window": {"count": 30000}}, False))
    eq("more than one window: served in windows, and the visual is told to fetch more", reduction(30001),
       ({"window": {"count": 30000}}, True))

    pane = cv.build_custom_visual(definition(format=[{"name": "look", "display_name": "Look", "properties": [
        {"name": "show", "type": "toggle", "default": True},
        {"name": "fill", "type": "color", "default": "#112233"},
        {"name": "gap", "type": "number", "default": 4, "min": 0, "max": 10, "description": "Space between items"},
        {"name": "size", "type": "font_size"},
        {"name": "caption", "type": "text", "default": "Hello"},
        {"name": "order", "type": "dropdown", "items": ["asc", {"value": "desc", "display_name": "Largest first"}]}]}]),
        check_with_node=False)
    props = pane["capabilities"]["objects"]["look"]["properties"]
    eq("format card", pane["capabilities"]["objects"]["look"]["displayName"], "Look")
    eq("property types of the format pane", {k: v["type"] for k, v in props.items()}, {
        "show": {"bool": True}, "fill": {"fill": {"solid": {"color": True}}}, "gap": {"numeric": True},
        "size": {"formatting": {"fontSize": True}}, "caption": {"text": True},
        "order": {"enumeration": [{"value": "asc", "displayName": "Asc"},
                                  {"value": "desc", "displayName": "Largest first"}]}})
    eq("a description is carried to the pane", props["gap"].get("description"), "Space between items")
    eq("the report lists the pane", pane["format_pane"], [{"card": "look", "properties": [
        "show (toggle)", "fill (color)", "gap (number)", "size (font_size)", "caption (text)", "order (dropdown)"]}])


def test_validation():
    print("\n== validation messages ==")
    eq("a valid definition", cv.validate_definition(definition()), {"valid": True, "errors": [], "warnings": []})
    cases = [
        ("not an object", [1, 2], "must be a JSON object"),
        ("no name", definition(name=None), "name: must start with a letter"),
        ("a name with a dash", definition(name="bar-list"), "name: must start with a letter"),
        ("no roles", definition(roles=[]), "roles: give at least one data role"),
        ("an unknown role kind", definition(roles=[{"name": "x", "kind": "axis"}]),
         "roles[0].kind: must be 'grouping' (columns) or 'measure', got 'axis'"),
        ("a role declared twice", definition(roles=[{"name": "x", "kind": "measure"}, {"name": "x", "kind": "measure"}]),
         "roles[1].name: the role 'x' is declared twice"),
        ("a role limit of zero", definition(roles=[{"name": "x", "kind": "measure", "max": 0}]), "roles[0].max"),
        ("an unknown key", definition(colour="red"), "definition: unknown key 'colour'"),
        ("an unknown role key", definition(roles=[{"name": "x", "kind": "measure", "minimum": 1}]),
         "roles[0]: unknown key 'minimum'"),
        ("an unknown feature", definition(features={"zoom": True}), "features: unknown key 'zoom'"),
        ("a feature that is not a switch", definition(features={"tooltips": "yes"}),
         "features.tooltips: must be true or false, got 'yes'"),
        ("selection without a grouping role", definition(roles=[{"name": "m", "kind": "measure"}],
                                                         features={"selection": True}),
         "features.selection: needs a grouping role"),
        ("highlight without a measure", definition(roles=[{"name": "c", "kind": "grouping"}],
                                                   features={"highlight": True}),
         "features.highlight: needs a measure role"),
        ("drill-down on a measure", definition(features={"drilldown": "value"}),
         "features.drilldown: 'value' is not a grouping role (grouping roles: category)"),
        ("drill-down: true with two grouping roles",
         definition(roles=[{"name": "a", "kind": "grouping"}, {"name": "b", "kind": "grouping"}],
                    features={"drilldown": True}), "features.drilldown: name the grouping role"),
        ("an unknown property type", definition(format=[{"name": "c", "properties": [{"name": "p", "type": "slider"}]}]),
         "format[0].properties[0].type: must be one of toggle, color, number, font_size, text, dropdown, got 'slider'"),
        ("a card without properties", definition(format=[{"name": "c", "properties": []}]),
         "format[0].properties: a card needs at least one property"),
        ("a dropdown without items", definition(format=[{"name": "c", "properties": [{"name": "p", "type": "dropdown"}]}]),
         "format[0].properties[0].items: a dropdown needs its choices"),
        ("a dropdown default that is not an item", definition(format=[{"name": "c", "properties": [
            {"name": "p", "type": "dropdown", "items": ["a", "b"], "default": "z"}]}]),
         "format[0].properties[0].default: 'z' is not one of the items (a, b)"),
        ("a colour that is not hex", definition(format=[{"name": "c", "properties": [
            {"name": "p", "type": "color", "default": "red"}]}]), "format[0].properties[0].default: a colour is '#RRGGBB'"),
        ("a number outside its limits", definition(format=[{"name": "c", "properties": [
            {"name": "p", "type": "number", "default": 50, "min": 0, "max": 10}]}]),
         "format[0].properties[0].default: 50 is outside 0 to 10"),
        ("limits the wrong way round", definition(format=[{"name": "c", "properties": [
            {"name": "p", "type": "number", "min": 5, "max": 1}]}]), "min (5) is greater than max (1)"),
        ("limits on a toggle", definition(format=[{"name": "c", "properties": [
            {"name": "p", "type": "toggle", "min": 1}]}]), "only a number or a font_size has limits"),
        ("a property declared twice", definition(format=[{"name": "c", "properties": [
            {"name": "p", "type": "toggle"}, {"name": "p", "type": "text"}]}]), "'p' is declared twice in this card"),
        ("too many rows", definition(rows=2000000), "rows: must be a whole number from 1 to 1048576"),
        ("a bad version", definition(version="v1"), "version: must be three or four numbers"),
        ("a bad guid", definition(guid="my visual"), "guid: must start with a letter"),
        ("no script", definition(js=None), "js: give the script once"),
        ("the script given twice", definition(js_files=["a.js"]), "js: give the script once"),
        ("an empty script", definition(js="  \n"), "js: the script is empty"),
        ("a script file that is missing", definition(js=None, js_files=["nowhere.js"]),
         "js_files[0]: 'nowhere.js' could not be read"),
        ("a script folder that is missing", definition(js=None, js_folder="nowhere"), "js_folder: 'nowhere' is not a folder"),
        ("the stylesheet given twice", definition(css_file="a.css"), "css: give the stylesheet once"),
        ("an author that is not an object", definition(author="me"), "author: must be an object"),
    ]
    for label, bad, want in cases:
        found = errors_of(bad, tempfile.gettempdir())
        check(f"{label}: says {want[:60]!r}", any(want in e for e in found), str(found))
    several = errors_of(definition(name="1x", roles=[{"name": "c", "kind": "axis"}], rows=0, version="x"))
    check("every problem is reported at once", len(several) >= 4, str(several))

    with tempfile.TemporaryDirectory() as d:
        result, path = build(definition(name="1x"), d)
        check("a definition with errors builds nothing", not result["success"] and not result["written"]
              and not os.path.exists(path) and "Not built" in cv.describe_build(result))
        with open(os.path.join(d, "definition.json"), "w", encoding="utf-8") as f:
            json.dump(definition(), f)
        loaded, base = cv.load_definition(os.path.join(d, "definition.json"))
        eq("a definition file is read with the folder its file names are relative to", (loaded, base),
           (definition(), os.path.abspath(d)))
        with open(os.path.join(d, "broken.json"), "w", encoding="utf-8") as f:
            f.write("{ nope")
        try:
            cv.load_definition(os.path.join(d, "broken.json"))
            check("a definition file that is not JSON is refused", False)
        except cv.DefinitionError as e:
            check("a definition file that is not JSON is refused", "not valid JSON" in str(e), str(e))


def test_safety_pass():
    print("\n== safety pass ==")
    refused = [
        ('fetch("https://example.com/x")', "uses fetch"),
        ("window.fetch(url)", "uses fetch"),
        ("var r = new XMLHttpRequest();", "uses XMLHttpRequest"),
        ('new WebSocket("wss://example.com")', "uses WebSocket"),
        ("navigator.sendBeacon(url, data)", "uses sendBeacon"),
        ('var w = new Worker("w.js")', "uses Worker"),
        ('eval("1 + 1")', "uses eval"),
        ('var f = new Function("return 1")', "builds a function from text"),
        ('x.constructor("return this")()', "calls .constructor(...)"),
        ('setTimeout("draw()", 10)', "passes text to setTimeout"),
        ('import("./more.js")', "uses import"),
        ('window.parent.postMessage("hi", "*")', "reaches for parent"),
        ("var d = parent.document;", "reaches for parent"),
        ('top.location = "https://example.com"', "reaches for top"),
        ("window.opener.focus()", "reaches for opener"),
        ("self.postMessage(1)", "uses postMessage"),
        ('localStorage.setItem("a", 1)', "uses localStorage"),
        ("var c = document.cookie;", "document.cookie"),
        ('document.write("<b>x</b>")', "calls document.write"),
        ('location.href = "https://example.com"', "uses location"),
        ('window.open("https://example.com")', "calls window.open"),
        ('var s = document.createElement("script");', "creates a <script> element"),
        ("var t = `total ${fetch(url)}`;", "uses fetch"),
    ]
    for source, want in refused:
        errors, _ = cv.check_script_safety(source, "m.js")
        check(f"refused: {source[:44]}", any(want in e for e in errors), str(errors))
    allowed = [
        "// fetch(url) would be refused\nvar a = 1;",
        "/* eval(x) */ var b = 2;",
        'var label = "fetch the data with eval";',
        "var r = client.fetch(1);",
        "var parent = node.parentNode; parent.appendChild(child);",
        "function place(top, left) { return top.y + left.x; }",
        "var box = { top: 0, parent: null }; var y = rect.top + box.top;",
        "items.forEach(function (parent) { parent.count += 1; });",
        "var open = function () {}; panel.open();",
        "var grow = (top) => top.size;",
        "const { top, left } = rect; draw(top.x);",
        "setTimeout(function () { draw(); }, 10);",
        'var d = document.createElement("div");',
        'var quoted = /"fetch\\(/.test(text);',
        "class Box { constructor(options) { this.options = options; } }",
        "window.requestAnimationFrame(draw);",
    ]
    for source in allowed:
        errors, _ = cv.check_script_safety(source, "m.js")
        check(f"allowed: {source[:44]!r}", errors == [], str(errors))
    errors, _ = cv.check_script_safety("var a = 1;\n/* two\n lines */\nvar b = `x\ny`;\n  fetch(u);\n", "draw.js")
    check("an error names the module and the line, counted past comments and multi-line text",
          bool(errors) and errors[0].startswith("draw.js: line 6: uses fetch"), str(errors))
    _, warnings = cv.check_script_safety('el.innerHTML = a;\nel.innerHTML = b;\nvar m = "<a onclick=\\"go()\\">x</a>";', "m.js")
    check("writing markup by hand is a warning, given once", len([w for w in warnings if "innerHTML" in w]) == 1
          and "ctx.escape()" in warnings[0], str(warnings))
    check("an inline event handler in markup is a warning", any("inline event handler" in w for w in warnings), str(warnings))

    check("a script that closes its script element is refused", any("'</script'" in e for e in errors_of(
        definition(js="function render() { return 'x</scr' + 'ipt>'; }\nvar t = '</script>';"))))
    check("a stylesheet that closes its style element is refused",
          any("'</style'" in e for e in errors_of(definition(css="a{}</style><b>"))))
    check("a stylesheet that loads from the web is warned about",
          any("external resource" in w for w in cv.validate_definition(
              definition(css="@import url(https://example.com/a.css);"))["warnings"]))
    with tempfile.TemporaryDirectory() as d:
        result, path = build(definition(js=JS + "\nfetch('https://example.com');"), d)
        check("a refused script builds nothing, and the error names the line",
              not result["success"] and not os.path.exists(path) and "js: line 9: uses fetch" in result["errors"][0],
              str(result["errors"]))
    for name in cv._KIT_MODULES + ("register.js",):
        errors, _ = cv.check_script_safety(cv._kit_source(name), name)
        check(f"the kit's own {name} passes its own safety pass", errors == [], str(errors))


def foreign_package(folder):
    """A package shaped like one from the official toolchain, with nothing native wired."""
    guid = "foreignViz0123456789ABCDEF0123456789ABCDEF"
    visual = {"name": "foreignViz", "displayName": "Foreign", "guid": guid, "visualClassName": "Visual",
              "version": "2.3.0.0", "description": "", "supportUrl": "", "gitHubUrl": ""}
    meta = {"visual": visual, "author": {"name": "Someone", "email": "s@example.com"}, "apiVersion": "5.3.0",
            "style": "style/visual.less", "stringResources": {},
            "capabilities": {"dataRoles": [{"displayName": "Rows", "name": "rows", "kind": "GroupingOrMeasure"}],
                             "dataViewMappings": [{"table": {"rows": {"for": {"in": "rows"},
                                                                     "dataReductionAlgorithm": {"window": {"count": 100}}}}}],
                             "objects": {"general": {"properties": {"filter": {"type": {"filter": True}}}}},
                             "supportsHighlight": True, "futureKey": 1,
                             "privileges": [{"name": "WebAccess", "essential": True, "parameters": ["https://*.example.com"]}]},
            "content": {"js": f"var {guid};(()=>{{class V{{update(o){{this.m=this.host.createSelectionManager();"
                              f"this.m.select(i);fetch(u);this.host.fetchMoreData();this.e.renderingStarted(o);"
                              f"this.e.renderingFinished(o)}}getFormattingModel(){{return{{cards:[]}}}}}}"
                              f"powerbi.visuals.plugins.{guid}={{}}}})();", "css": "", "iconBase64": ""},
            "visualEntryPoint": "", "externalJS": [], "assets": {"icon": "assets/icon.png"}}
    package = {"version": "2.3.0.0", "author": meta["author"],
               "resources": [{"resourceId": "rId0", "sourceType": 5, "file": f"resources/{guid}.pbiviz.json"}],
               "visual": visual, "metadata": {"pbivizjson": {"resourceId": "rId0"}}}
    path = os.path.join(folder, "foreign.pbiviz")
    with zipfile.ZipFile(path, "w") as z:
        z.writestr("package.json", json.dumps(package))
        z.writestr(f"resources/{guid}.pbiviz.json", json.dumps(meta))
    return path, guid


def test_inspector():
    print("\n== inspector ==")
    with tempfile.TemporaryDirectory() as d:
        result, path = build(definition(
            roles=[{"name": "category", "kind": "grouping"}, {"name": "value", "kind": "measure"}],
            features={"drilldown": "category"}, version="1.2.3.4", description="Shows things", rows=40000,
            format=[{"name": "look", "properties": [{"name": "show", "type": "toggle"}, {"name": "fill", "type": "color"}]}]), d)
        info = cv.inspect_custom_visual(path)
        eq("name, guid, version, API", (info["name"], info["display_name"], info["guid"], info["version"], info["api_version"]),
           ("testList", "Test list", result["guid"], "1.2.3.4", cv.API_VERSION))
        eq("data roles", info["data_roles"], [{"name": "category", "display_name": "Category", "kind": "Grouping"},
                                              {"name": "value", "display_name": "Value", "kind": "Measure"}])
        eq("data mapping", info["data_mappings"], ["categorical"])
        eq("format pane", info["format_pane"], {"look": ["fill", "show"]})
        eq("privileges", info["privileges"], [])
        on = sorted(k for k, v in info["features"].items() if v)
        eq("every native feature the kit wires is found in its own output", on, sorted(info["features"]))
        eq("drill roles", info["features"]["drilldown"], ["category"])
        check("the kit's output is recognised, and its plugin registration found",
              info["built_by_kit"] and info["registers_plugin"] and info["warnings"] == [] and info["script_mentions"] == [])
        text = cv.describe_inspection(info)
        check("the summary names the visual, its roles and the drill role",
              "Test list (" in text and "category (Grouping), value (Measure)" in text and "drill roles: category" in text
              and "Privileges: none" in text, text)

        unpacked = os.path.join(d, "Report", "CustomVisuals", result["guid"])
        for rel, data in pbir_packages.read_pbiviz(path)["files"].items():
            os.makedirs(os.path.dirname(os.path.join(unpacked, rel)), exist_ok=True)
            with open(os.path.join(unpacked, rel), "wb") as f:
                f.write(data)
        eq("the folder a report keeps the visual in reads the same as the archive", cv.inspect_custom_visual(unpacked), info)

        plain = cv.inspect_custom_visual(build(definition(features={
            "selection": False, "highlight": False, "tooltips": False, "keyboard": False, "landing_page": False,
            "sorting": False}), d, "plain.pbiviz")[1])
        eq("capability switches that are off read as off", {k: plain["features"][k] for k in (
            "cross_highlight", "report_page_tooltips", "modern_tooltips", "keyboard", "landing_page",
            "multi_visual_selection", "sorting", "drilldown", "fetch_more_data", "format_pane")},
           {"cross_highlight": False, "report_page_tooltips": False, "modern_tooltips": False, "keyboard": False,
            "landing_page": False, "multi_visual_selection": False, "sorting": False, "drilldown": [],
            "fetch_more_data": False, "format_pane": False})

        eq("script switches read as they are set: the code is in every package, the configuration decides",
           {k: plain["features"][k] for k in ("selection", "bookmarks", "tooltips", "context_menu")},
           {"selection": False, "bookmarks": False, "tooltips": False, "context_menu": True})

        foreign, guid = foreign_package(d)
        other = cv.inspect_custom_visual(foreign)
        eq("a package from elsewhere: identity", (other["name"], other["guid"], other["api_version"], other["built_by_kit"]),
           ("foreignViz", guid, "5.3.0", False))
        eq("its privileges", other["privileges"], ["WebAccess"])
        eq("what its script mentions", other["script_mentions"], ["network"])
        eq("its data mapping", other["data_mappings"], ["table"])
        eq("features told from capabilities and script", {k: other["features"][k] for k in (
            "selection", "cross_highlight", "context_menu", "tooltips", "bookmarks", "fetch_more_data", "format_pane",
            "rendering_events", "launch_url", "keyboard")},
           {"selection": True, "cross_highlight": True, "context_menu": False, "tooltips": False, "bookmarks": False,
            "fetch_more_data": True, "format_pane": True, "rendering_events": True, "launch_url": False,
            "keyboard": False})
        notes = " | ".join(other["warnings"])
        check("warnings: highlights on a table mapping, no context menu, no bookmark restore, unknown key",
              all(s in notes for s in ("table data mapping never carries highlights", "never opens the context menu",
                                       "bookmark cannot restore", "futureKey")), notes)
        check("a registered plugin is found in dotted form too", other["registers_plugin"])

        junk = os.path.join(d, "junk.pbiviz")
        with open(junk, "wb") as f:
            f.write(b"not a zip")
        for label, target in (("a file that is not a zip", junk), ("a folder that is not a visual", d),
                              ("a path that does not exist", os.path.join(d, "none.pbiviz"))):
            try:
                cv.inspect_custom_visual(target)
                check(f"{label} is refused", False)
            except pbir_packages.PbivizError:
                check(f"{label} is refused", True)


def test_report():
    print("\n== build report ==")
    with tempfile.TemporaryDirectory() as d:
        result, path = build(definition(), d)
        text = cv.describe_build(result)
        check("the summary names the visual, where it went, roles, features and the node check",
              all(s in text for s in (result["guid"], path, "category (grouping), value (measure)",
                                      "  cross-filter and cross-highlight other visuals on click: yes",
                                      "  drill-down: no, not asked for (features.drilldown names the role)",
                                      "  format pane: no, the definition declares no format cards",
                                      "Script check (node): skipped")), text)
        eq("usage: how a report spec registers the package", result["usage"]["custom_visuals"],
           [{"pbiviz": path, "alias": "testList"}])
        eq("usage: an element with one field per role", result["usage"]["element"]["fields"],
           {"category": {"column": "<Table>.<Field>"}, "value": {"measure": "<Table>.<Field>"}})
        check("the notes say what was not verified", any("Not verified here" in n for n in result["notes"]))
        check("an empty author is pointed out", any("author.name is empty" in w for w in result["warnings"]))
        silent, _ = build(definition(js="var draw = 1;"), d, "silent.pbiviz")
        check("a script without render or main is warned about",
              silent["success"] and any("No render(ctx) or main(app)" in w for w in silent["warnings"]))
        if not NODE:
            print("  [SKIP] syntax check of the assembled script (node is not on PATH)")
            return
        checked = cv.build_custom_visual(definition(), os.path.join(d, "checked.pbiviz"))
        eq("the assembled script parses under node", checked["node_check"], "passed")
        eq("and unminified too", cv.build_custom_visual(definition(), minify=False)["node_check"], "passed")
        with open(os.path.join(d, "good.js"), "w", encoding="utf-8") as f:
            f.write("var helper = 1;\n")
        with open(os.path.join(d, "bad.js"), "w", encoding="utf-8") as f:
            f.write("function render(ctx) { return ctx.rows.map(function (row) { return 'x'; ); }\n")
        broken = cv.build_custom_visual(definition(js=None, js_files=["good.js", "bad.js"]),
                                        os.path.join(d, "broken.pbiviz"), base_dir=d)
        check("a script that does not parse builds nothing, and the error names the module",
              not broken["success"] and not os.path.exists(os.path.join(d, "broken.pbiviz"))
              and "bad.js:" in broken["errors"][0], str(broken["errors"]))


def test_examples():
    print("\n== example definitions ==")
    names = sorted(n for n in os.listdir(EXAMPLES) if os.path.isfile(os.path.join(EXAMPLES, n, "definition.json")))
    check("the examples are there", names == ["bar_list", "card_list", "kpi_tiles", "ranked_app"], str(names))
    with tempfile.TemporaryDirectory() as d:
        for name in names:
            defn, base = cv.load_definition(os.path.join(EXAMPLES, name, "definition.json"))
            found = cv.validate_definition(defn, base)
            result = cv.build_custom_visual(defn, os.path.join(d, name + ".pbiviz"), base_dir=base, check_with_node=False)
            check(f"{name}: validates and builds", found["valid"] and result["success"], str(found["errors"]))
            eq(f"{name}: its guid follows from its name", result["guid"], cv.visual_guid(defn["name"]))
    folder = os.path.join(EXAMPLES, "ranked_app", "modules")
    with open(os.path.join(EXAMPLES, "ranked_app", "style.css"), encoding="utf-8") as f:
        css = f.read()
    page = jm.build_js_app_measures("Ranked", css=css, js_modules=folder, payload_measures=[("Ranked Payload", "ITEMS")],
                                    check_with_node=False)
    check("ranked_app: the same module folder also builds as a measure-driven page",
          [m["name"] for m in page["measures"]] == ["Ranked Styles", "Ranked Runtime", "Ranked"]
          and "readItems" in page["measures"][1]["expression"])


# ---------------------------------------------------------------- the kit, run in Node

def test_kit_selection(d):
    print("\n== kit: rows, selection, context menu, tooltips (node) ==")
    path = built(definition(), d, "sel.pbiviz")
    steps = [
        {"op": "update", "data": None},                                   # 0 landing
        {"op": "update", "data": DATA},                                   # 1
        {"op": "click", "row": None},                                     # 2 empty space, nothing selected
        {"op": "click", "row": 1},                                        # 3
        {"op": "click", "row": 2, "ctrl": True},                          # 4
        {"op": "click", "row": 0, "meta": True},                          # 5
        {"op": "update", "data": DATA},                                   # 6 the selection outlives a redraw
        {"op": "click", "row": 0},                                        # 7 a plain click replaces the selection
        {"op": "click", "row": 0},                                        # 8 and a second one clears it (host rule)
        {"op": "click", "row": 2},                                        # 9
        {"op": "click", "selector": ".gap"},                              # 10 empty space clears
        {"op": "contextmenu", "row": 1, "x": 31, "y": 47},                # 11
        {"op": "contextmenu", "row": None, "x": 5, "y": 6},               # 12
        {"op": "hover", "row": 0, "x": 112, "y": 74},                    # 13
        {"op": "hover", "row": 0, "x": 114, "y": 76},                    # 14
        {"op": "hover", "row": 2, "x": 20, "y": 60},                      # 15
        {"op": "hover", "selector": ".gap", "x": 1, "y": 1},              # 16
        {"op": "hover", "row": 1, "x": 3, "y": 3},                        # 17
        {"op": "leave"},                                                  # 18
        {"op": "host_select", "rows": [2]},                               # 19 a bookmark restores a selection
        {"op": "host_select", "rows": []},                                # 20 another visual took the selection
        {"op": "destroy"},                                                # 21
        {"op": "click", "row": 1},                                        # 22
    ]
    sim = cv.simulate(path, steps)
    if not sim.get("ok"):
        check("the visual runs under node", False, str(sim.get("error")))
        return
    s = sim["states"]
    guid = cv.visual_guid("testList")
    eq("the plugin the host finds", sim["plugin"], {"name": guid, "displayName": "Test list", "class": "Visual",
                                                    "apiVersion": cv.API_VERSION, "custom": True, "exported": True})
    eq("the object the factory returns", sim["methods"], ["update", "getFormattingModel", "destroy"])
    check("the selection manager is created once and listens for the host's selection",
          len(calls(sim, "createSelectionManager")) == 1 and len(calls(sim, "registerOnSelectCallback")) == 1)
    eq("no console output", sim["console"], [])

    check("no data: the landing page, with the wells to fill", s[0]["noticeClasses"] == ["vk-landing", "vk-notice"]
          and "Add fields to: Category, Value" in s[0]["noticeText"] and "vk-hidden" in s[0]["rootClasses"], str(s[0]))
    for step in (0, 1, 6):
        eq(f"step {step}: rendering started and finished, with the update's own options",
           [(e["call"], e["sameOptions"]) for e in sim["log"] if e["step"] == step and e["call"].startswith("rendering")],
           [("renderingStarted", True), ("renderingFinished", True)])
    eq("rows are drawn from the data roles, formatted", [r["text"] for r in s[1]["rows"]],
       ["Mumbai=1,078", "Chennai=834", "Bengaluru=751"])
    check("with data the landing page is gone", "vk-hidden" in s[1]["noticeClasses"] and "vk-hidden" not in s[1]["rootClasses"])
    eq("each row can take the keyboard focus and says what it is",
       [(r["tabindex"], r["role"], r["ariaSelected"]) for r in s[1]["rows"]], [("0", "option", "false")] * 3)
    eq("the list of rows says what it is", s[1]["rootRole"], "listbox")
    eq("the theme reaches the stylesheet as variables", {k: s[1]["style"].get(k) for k in ("--vk-foreground", "--vk-accent")},
       {"--vk-foreground": "#252423", "--vk-accent": "#118DFF"})

    eq("a click on empty space with nothing selected asks the host for nothing", [e for e in sim["log"] if e["step"] == 2], [])
    eq("a click selects that row's identity", calls(sim, "select", 3), [
        {"call": "select", "ids": [KEYS[1]], "multiSelect": False, "step": 3}])
    eq("the row is marked, the others dimmed", [classes(s[3], r) for r in (0, 1, 2)],
       [["item", "vk-dimmed"], ["item", "vk-selected"], ["item", "vk-dimmed"]])
    check("and the root and the accessibility state follow", "vk-has-selection" in s[3]["rootClasses"]
          and [r["ariaSelected"] for r in s[3]["rows"]] == ["false", "true", "false"])
    eq("Ctrl+click adds to the selection", calls(sim, "select", 4), [
        {"call": "select", "ids": [KEYS[2]], "multiSelect": True, "step": 4}])
    eq("Cmd+click does too", calls(sim, "select", 5)[0]["multiSelect"], True)
    eq("three rows selected, none dimmed", [classes(s[5], r) for r in (0, 1, 2)], [["item", "vk-selected"]] * 3)
    eq("the selection is still shown after the visual is drawn again", [classes(s[6], r) for r in (0, 1, 2)],
       [["item", "vk-selected"]] * 3)
    eq("a plain click replaces a multiple selection", [classes(s[7], r) for r in (0, 1, 2)],
       [["item", "vk-selected"], ["item", "vk-dimmed"], ["item", "vk-dimmed"]])
    check("clicking the only selected row again clears it", [classes(s[8], r) for r in (0, 1, 2)] == [["item"]] * 3
          and "vk-has-selection" not in s[8]["rootClasses"])
    eq("a click on empty space clears the selection", [e["call"] for e in sim["log"] if e["step"] == 10], ["clear"])
    eq("and nothing is marked any more", [classes(s[10], r) for r in (0, 1, 2)], [["item"]] * 3)

    eq("right-click on a row: the context menu for that data point, at the pointer", calls(sim, "showContextMenu", 11), [
        {"call": "showContextMenu", "id": KEYS[1], "argument": "identity", "emptySpace": False, "x": 31, "y": 47,
         "step": 11}])
    eq("right-click on empty space: the visual's own menu", calls(sim, "showContextMenu", 12), [
        {"call": "showContextMenu", "id": None, "argument": "empty object", "emptySpace": True, "x": 5, "y": 6,
         "step": 12}])
    check("the browser's own menu is suppressed", s[11]["result"]["defaultPrevented"] and s[12]["result"]["defaultPrevented"])

    eq("hover: the host tooltip, placed relative to the visual, with the row's fields and its identity",
       calls(sim, "tooltip.show", 13), [
        {"call": "tooltip.show", "coordinates": [12, 34], "isTouchEvent": False, "identities": [KEYS[0]],
         "dataItems": [{"displayName": "city", "value": "Mumbai"}, {"displayName": "Customers", "value": "1,078"}],
         "step": 13}])
    eq("moving over the same row moves the tooltip", [(e["call"], e["coordinates"]) for e in sim["log"] if e["step"] == 14],
       [("tooltip.move", [14, 36])])
    eq("another row shows its own", [(e["call"], e["identities"]) for e in sim["log"] if e["step"] == 15],
       [("tooltip.show", [KEYS[2]])])
    eq("off the rows the tooltip is hidden", [e for e in sim["log"] if e["step"] == 16],
       [{"call": "tooltip.hide", "immediately": True, "isTouchEvent": False, "step": 16}])
    eq("leaving the visual hides it", [e["call"] for e in sim["log"] if e["step"] in (17, 18)], ["tooltip.show", "tooltip.hide"])

    eq("a bookmark's selection is shown without the visual asking for it",
       ([e for e in sim["log"] if e["step"] == 19], [classes(s[19], r) for r in (0, 1, 2)]),
       ([], [["item", "vk-dimmed"], ["item", "vk-dimmed"], ["item", "vk-selected"]]))
    eq("when another visual takes the selection, this one lets go", [classes(s[20], r) for r in (0, 1, 2)], [["item"]] * 3)
    eq("after destroy the visual no longer answers", [e for e in sim["log"] if e["step"] == 22], [])


def test_kit_keyboard_and_static(d):
    print("\n== kit: keyboard, no interactions, switches (node) ==")
    path = built(definition(), d, "keys.pbiviz")
    sim = cv.simulate(path, [
        {"op": "update", "data": DATA},
        {"op": "key", "row": 0, "key": "Enter"},                          # 1
        {"op": "key", "row": 1, "key": " ", "ctrl": True},                # 2
        {"op": "key", "row": 2, "key": "ContextMenu"},                    # 3
        {"op": "key", "row": 2, "key": "F10", "shift": True},             # 4
        {"op": "key", "row": 0, "key": "ArrowDown"},                      # 5
        {"op": "key", "row": 1, "key": "End"},                            # 6
        {"op": "key", "row": 2, "key": "ArrowUp"},                        # 7
        {"op": "update", "data": DATA},                                   # 8 focus survives a redraw
        {"op": "key", "row": 0, "key": "a"},                              # 9
    ])
    if not sim.get("ok"):
        check("the visual runs under node", False, str(sim.get("error")))
        return
    s = sim["states"]
    eq("Enter selects the focused row", calls(sim, "select", 1), [{"call": "select", "ids": [KEYS[0]], "multiSelect": False, "step": 1}])
    eq("Ctrl+Space adds a row", calls(sim, "select", 2), [{"call": "select", "ids": [KEYS[1]], "multiSelect": True, "step": 2}])
    check("the keys do not scroll the page", s[1]["result"]["defaultPrevented"] and s[2]["result"]["defaultPrevented"])
    eq("the menu key opens the context menu at the middle of the row", calls(sim, "showContextMenu", 3), [
        {"call": "showContextMenu", "id": KEYS[2], "argument": "identity", "emptySpace": False, "x": 50, "y": 10,
         "step": 3}])
    eq("Shift+F10 does the same", len(calls(sim, "showContextMenu", 4)), 1)
    eq("arrow keys, Home and End move the focus between rows", [s[k]["focusedRow"] for k in (5, 6, 7)], ["1", "2", "1"])
    eq("the focused row keeps the focus when the visual is drawn again", s[8]["focusedRow"], "1")
    eq("other keys are left alone", ([e for e in sim["log"] if e["step"] == 9], s[9]["result"]["defaultPrevented"]), ([], False))

    frozen = cv.simulate(path, [
        {"op": "update", "data": DATA},
        {"op": "click", "row": 0}, {"op": "contextmenu", "row": 0}, {"op": "key", "row": 0, "key": "Enter"},
        {"op": "hover", "row": 0, "x": 1, "y": 2},
    ], host={"allowInteractions": False})
    eq("when the host allows no interactions: no selection, no menu, no keyboard; the tooltip still shows",
       [e["call"] for e in frozen["log"] if e["step"] >= 1], ["tooltip.show"])
    check("and the rows say so: no focus stop, the static class", "vk-static" in frozen["states"][0]["rootClasses"]
          and [r["tabindex"] for r in frozen["states"][0]["rows"]] == [None] * 3)

    off = built(definition(features={"selection": False, "context_menu": False, "tooltips": False, "keyboard": False}),
                   d, "off.pbiviz")
    quiet = cv.simulate(off, [{"op": "update", "data": DATA}, {"op": "click", "row": 0},
                              {"op": "contextmenu", "row": 0}, {"op": "hover", "row": 0},
                              {"op": "key", "row": 0, "key": "Enter"}])
    eq("features switched off in the definition make no host calls",
       [e["call"] for e in quiet["log"] if e["step"] >= 1], [])
    check("and a right-click is left to the host", quiet["states"][2]["result"]["defaultPrevented"] is False
          and [r["role"] for r in quiet["states"][0]["rows"]] == [None] * 3)


def test_kit_highlights_and_settings(d):
    print("\n== kit: highlights, format pane, theme (node) ==")
    defn, base = cv.load_definition(os.path.join(EXAMPLES, "bar_list", "definition.json"))
    path = os.path.join(d, "bars.pbiviz")
    cv.build_custom_visual(defn, path, base_dir=base, check_with_node=False)
    data = {"columns": [{"role": "category", "name": "city", "kind": "grouping"},
                        {"role": "value", "name": "Spend", "kind": "measure", "format": "\\$#,0"},
                        {"role": "tooltips", "name": "Share", "kind": "measure", "format": "0.0%"}],
            "rows": [["Chennai", 1000, 0.25], ["Mumbai", 2000, 0.5], ["Pune", 500, 0.125]]}
    lit = dict(data, highlights=[[None, None, None], [None, 500, 0.1], [None, 500, 0.3]])
    styled = dict(data, objects={"bars": {"color": {"solid": {"color": "#AA0000"}}, "height": 99, "order": "model"},
                                 "labels": {"show": False, "fontSize": 20}})
    sim = cv.simulate(path, [
        {"op": "update", "data": data},                                   # 0
        {"op": "update", "data": lit},                                    # 1 cross-highlighted by another visual
        {"op": "hover", "row": 1, "x": 1, "y": 1},                        # 2
        {"op": "update", "data": data},                                   # 3 highlight gone
        {"op": "formatting_model"},                                       # 4 defaults
        {"op": "update", "data": styled},                                 # 5
        {"op": "formatting_model"},                                       # 6
        {"op": "update", "data": data, "host": {"highContrast": True}},   # 7
        {"op": "update", "data": {"columns": data["columns"], "rows": []}, "host": {"highContrast": False}},  # 8
    ])
    if not sim.get("ok"):
        check("the bar list runs under node", False, str(sim.get("error")))
        return
    s = sim["states"]
    eq("no console output", sim["console"], [])
    eq("bar list: largest first, values formatted with the column's format string",
       [r["text"] for r in s[0]["rows"]], ["Mumbai$2K", "Chennai$1K", "Pune$500"])
    check("without highlights nothing is marked", "vk-has-highlights" not in s[0]["rootClasses"]
          and all(c == ["bl-row"] for c in [classes(s[0], r) for r in (0, 1, 2)]))
    eq("highlighted rows are marked, the others dimmed", [classes(s[1], r) for r in (0, 1, 2)],
       [["bl-row", "vk-dimmed"], ["bl-row", "vk-highlighted"], ["bl-row", "vk-highlighted"]])
    check("the root says highlights are in play", "vk-has-highlights" in s[1]["rootClasses"])
    check("the render code got the highlighted part per row: a quarter of Mumbai's bar, all of Pune's, none of Chennai's",
          [m for m in re.findall(r'bl-fill" style="width:([0-9.]+)%', s[1]["html"])] == ["25.00", "0.00", "100.00"],
          str(re.findall(r'bl-fill" style="width:([0-9.]+)%', s[1]["html"])))
    eq("the tooltip adds the highlighted value under each measure", calls(sim, "tooltip.show", 2)[0]["dataItems"], [
        {"displayName": "city", "value": "Mumbai"}, {"displayName": "Spend", "value": "$2,000"},
        {"displayName": "Highlighted", "value": "$500"}, {"displayName": "Share", "value": "50.0%"},
        {"displayName": "Highlighted", "value": "10.0%"}])
    check("when the highlight goes, so do the marks", "vk-has-highlights" not in s[3]["rootClasses"]
          and all(classes(s[3], r) == ["bl-row"] for r in (0, 1, 2)))

    cards = s[4]["result"]["cards"]
    eq("format pane: one card per object", [(c["displayName"], c["uid"]) for c in cards],
       [("Bars", "bars-card"), ("Value labels", "labels-card")])
    slices = cards[0]["groups"][0]["slices"]
    eq("slices: a control per property type", [(x["uid"], x["displayName"], x["control"]["type"]) for x in slices],
       [("bars-color", "Colour", "ColorPicker"), ("bars-height", "Bar height", "NumUpDown"), ("bars-order", "Order", "Dropdown")])
    eq("every slice names the object and property it writes",
       [x["control"]["properties"]["descriptor"] for x in slices],
       [{"objectName": "bars", "propertyName": p} for p in ("color", "height", "order")])
    eq("a colour left to the theme shows the theme's first data colour", slices[0]["control"]["properties"]["value"],
       {"value": "#118DFF"})
    eq("a number carries its limits", slices[1]["control"]["properties"],
       {"descriptor": {"objectName": "bars", "propertyName": "height"}, "value": 14,
        "options": {"minValue": {"type": 0, "value": 4}, "maxValue": {"type": 1, "value": 40}}})
    eq("a dropdown carries its items and the chosen one", (slices[2]["control"]["properties"]["value"],
                                                           len(slices[2]["control"]["properties"]["items"])),
       ({"value": "value", "displayName": "Largest first"}, 2))
    eq("a toggle named show becomes the card's own switch", (cards[1]["topLevelToggle"]["uid"],
                                                             cards[1]["topLevelToggle"]["control"]["type"],
                                                             cards[1]["topLevelToggle"]["control"]["properties"]["value"],
                                                             [x["uid"] for x in cards[1]["groups"][0]["slices"]]),
       ("labels-show", "ToggleSwitch", True, ["labels-fontSize", "labels-units"]))
    eq("reset to default covers every property", cards[1]["revertToDefaultDescriptors"],
       [{"objectName": "labels", "propertyName": p} for p in ("show", "fontSize", "units")])

    check("settings reach the render code: colour, the report's own order, labels off, text size",
          "background:#AA0000" in s[5]["html"] and "font-size:20pt" in s[5]["html"] and "bl-value" not in s[5]["html"]
          and [r["text"] for r in s[5]["rows"]] == ["Chennai", "Mumbai", "Pune"], s[5]["html"][:300])
    check("a number outside its limits is brought back inside", "height:40px" in s[5]["html"])
    changed = s[6]["result"]["cards"]
    eq("the pane shows the stored values", (changed[0]["groups"][0]["slices"][0]["control"]["properties"]["value"],
                                            changed[0]["groups"][0]["slices"][2]["control"]["properties"]["value"]["value"],
                                            changed[1]["topLevelToggle"]["control"]["properties"]["value"]),
       ({"value": "#AA0000"}, "model", False))
    check("high contrast: the root says so and colours fall back to the system foreground",
          "vk-high-contrast" in s[7]["rootClasses"] and "background:#FFFFFF" in s[7]["html"]
          and s[7]["style"]["--vk-foreground"] == "#FFFFFF", s[7]["html"][:200])
    check("fields but no rows: the empty message, not the landing page", s[8]["noticeClasses"] == ["vk-empty", "vk-notice"]
          and s[8]["noticeText"] == "No data for the current filters." and "vk-high-contrast" not in s[8]["rootClasses"])


FORMAT_JS = r"""
function render(ctx) {
  var day = new Date(2026, 0, 5, 14, 7, 9);
  var cases = [
    [1078, "#,##0"], [1078, "0"], [0.428, "0.0%"], [0.5, "0%;-0%;0%"], [-0.25, "0%;-0%;0%"],
    [14667452.4, "\\₹#,0"], [-1234.5, "\\$#,0;(\\$#,0);\\$#,0"], [0, "#,0;(#,0);\"-\""], [-0.2, "0"],
    [1234567, "#,0,.0"], [1234.5678, "N2"], [0.1234, "P1"], [5, "D3"], [1234.5, ""], [3, "0.00 \"kg\""],
    [null, "0"], ["text", "0"], [true, ""], [88482959, "\\₹#,0", {units: "auto"}], [1500, "0", {units: "auto"}],
    [999, "0", {units: "auto"}], [0.42, "0%", {units: "auto"}], [2500000, "0", {units: "thousands"}],
    [3.14159, "0.00", {decimals: 1}], [1234.5, "#,0.00", {locale: "de-DE"}], [null, "0", {blank: "-"}],
    [day, "yyyy-MM-dd"], [day, "dd MMM yyyy"], [day, "MMMM d, yyyy 'at' HH:mm:ss"], [day, "h:mm tt"]
  ];
  var out = cases.map(function (item) { return ctx.format(item[0], item[1], item[2]); });
  out.push(ctx.format(1234.5, ctx.columns.value[0]));
  out.push(ctx.escape("<b a=\"1\">&'</b>"));
  return "<pre>" + ctx.escape(JSON.stringify(out)) + "</pre>";
}
"""


def test_kit_format(d):
    print("\n== kit: value formatting (node) ==")
    path = built(definition(js=FORMAT_JS), d, "fmt.pbiviz")
    sim = cv.simulate(path, [{"op": "update", "data": DATA}])
    if not sim.get("ok"):
        check("the visual runs under node", False, str(sim.get("error")))
        return
    got = json.loads(html.unescape(re.search(r"<pre>(.*)</pre>", sim["states"][0]["html"], re.S).group(1)))
    want = ["1,078", "1078", "42.8%", "50%", "-25%", "₹14,667,452", "($1,235)", "-", "0", "1,234.6", "1,234.57",
            "12.3%", "005", "1234.5", "3.00 kg", "", "text", "True", "₹88.5M", "1.5K", "999", "42%", "2500K", "3.1",
            "1.234,50", "-", "2026-01-05", "05 Jan 2026", "January 5, 2026 at 14:07:09", "2:07 PM",
            "1,235", "&lt;b a=&quot;1&quot;&gt;&amp;&#39;&lt;/b&gt;"]
    labels = ["thousands separator", "plain digits", "percent with a decimal", "sections: positive", "sections: negative",
              "an escaped currency symbol", "negative in brackets", "a literal zero section", "minus zero is zero",
              "a scaling comma", "N2", "P1", "D3", "no format string", "a quoted suffix", "blank", "text passes through",
              "boolean", "units: auto, millions", "units: auto, thousands", "units: auto, below a thousand",
              "units leave a percentage alone", "units: thousands", "decimals override", "another locale", "blank text",
              "date: yyyy-MM-dd", "date: dd MMM yyyy", "date: names, quoted text and time", "date: 12-hour clock",
              "a column object carries its format", "escape"]
    for label, g, w in zip(labels, got, want):
        eq(f"format: {label}", g, w)
    eq("every case was checked", (len(got), len(labels)), (len(want), len(want)))


HOOKS_JS = """
var changes = 0;
function tooltip(row, ctx) {
  if (row.index === 2) { return []; }
  return [{ displayName: "Place", value: row.text.category, color: "#123456", header: "Mine" }];
}
function selectionChanged(ctx) { changes += 1; ctx.root.setAttribute("data-changes", String(changes)); }
function render(ctx) {
  ctx.state.runs = ctx.runs;
  ctx.on(ctx.root, "dblclick", "pick", function (event) { ctx.select(ctx.rows[1]); });
  ctx.on(ctx.root, "dblclick", "pick", function (event) { ctx.select([ctx.rows[0], ctx.rows[2]], true); });
  ctx.on(ctx.root, "wheel", "menu", function (event) { ctx.contextMenu(ctx.rows[1], { x: 7, y: 9 }); });
  ctx.on(ctx.root, "focusin", "tip", function (event) {
    ctx.showTooltip([{ displayName: "Note", value: 5 }], { x: 140, y: 90 });
  });
  ctx.on(ctx.root, "focusout", "untip", function (event) { ctx.hideTooltip(); ctx.clearSelection(); });
  return ctx.rows.map(function (row) {
    return '<div class="item" data-row="' + row.index + '">' + ctx.escape(row.text.category) + '</div>';
  }).join("") + '<a class="web" href="https://example.com/a?b=1">site</a>' +
    '<span class="mail" data-url="mailto:someone@example.com">mail</span>' +
    '<a class="bad" href="java' + 'script:void(0)">bad</a><button class="own">sort</button>' +
    '<i class="state">' + ctx.runs + '/' + ctx.state.runs + '/' + ctx.loadingMore + '/' + ctx.hasMoreRows + '</i>';
}
"""


def test_kit_explicit(d):
    print("\n== kit: explicit calls, hooks, links, more rows, failures (node) ==")
    path = built(definition(js=HOOKS_JS, rows=70000), d, "hooks.pbiviz")
    more = dict(DATA, segment=True)
    sim = cv.simulate(path, [
        {"op": "update", "data": more},                                   # 0 first window, more waiting
        {"op": "update", "data": DATA},                                   # 1 last window
        {"op": "hover", "row": 0, "x": 2, "y": 3},                        # 2 tooltip hook
        {"op": "hover", "row": 2, "x": 2, "y": 3},                        # 3 hook returns no lines
        {"op": "click", "selector": ".web"},                              # 4
        {"op": "click", "selector": ".mail"},                             # 5
        {"op": "click", "selector": ".bad"},                              # 6
        {"op": "click", "row": 1},                                        # 7
        {"op": "click", "selector": ".own"},                              # 8 the author's own button keeps the selection
        {"op": "event", "type": "dblclick", "selector": ".vk-root"},      # 9 ctx.select
        {"op": "event", "type": "wheel", "selector": ".vk-root"},         # 10 ctx.contextMenu
        {"op": "event", "type": "focusin", "selector": ".vk-root"},       # 11 ctx.showTooltip
        {"op": "event", "type": "focusout", "selector": ".vk-root"},      # 12 ctx.hideTooltip, ctx.clearSelection
    ])
    if not sim.get("ok"):
        check("the visual runs under node", False, str(sim.get("error")))
        return
    s = sim["states"]
    eq("no console output", sim["console"], [])
    eq("more rows than one window: the visual asks the host for the next one, once per update that has more",
       [(e["step"], e["aggregateSegments"]) for e in calls(sim, "fetchMoreData")], [(0, True)])
    check("the render code is told rows are still coming, then that they are all there",
          ">1/1/true/true<" in s[0]["html"] and ">2/2/false/false<" in s[1]["html"], s[1]["html"][-80:])
    eq("both updates report rendering finished", len(calls(sim, "renderingFinished")), 2)
    eq("tooltip(row, ctx) supplies the lines", calls(sim, "tooltip.show", 2)[0]["dataItems"],
       [{"displayName": "Place", "value": "Mumbai", "color": "#123456", "header": "Mine"}])
    eq("no lines, no tooltip", [e["call"] for e in sim["log"] if e["step"] == 3], ["tooltip.hide"])
    eq("a link is opened by the host", [e for e in sim["log"] if e["step"] == 4],
       [{"call": "launchUrl", "url": "https://example.com/a?b=1", "step": 4}])
    eq("data-url works the same", [e.get("url") for e in sim["log"] if e["step"] == 5], ["mailto:someone@example.com"])
    eq("a javascript: link is not handed to the host", [e for e in sim["log"] if e["step"] == 6], [])
    check("link clicks do not navigate the frame", all(s[k]["result"]["defaultPrevented"] for k in (4, 5, 6)))
    eq("selectionChanged(ctx) runs after each change of the selection, not before",
       [s[k]["rootAttributes"].get("data-changes") for k in (6, 7, 9, 12)], [None, "1", "2", "3"])
    eq("a click on the author's own control does not clear the selection",
       ([e for e in sim["log"] if e["step"] == 8], classes(s[8], 1)), ([], ["item", "vk-selected"]))
    eq("ctx.select(rows, true) adds rows; ctx.on kept only the last handler given for a key",
       [e for e in sim["log"] if e["step"] == 9],
       [{"call": "select", "ids": [KEYS[0], KEYS[2]], "multiSelect": True, "step": 9}])
    eq("all three rows are now selected", [classes(s[9], r) for r in (0, 1, 2)], [["item", "vk-selected"]] * 3)
    eq("ctx.contextMenu(row, point)", [e for e in sim["log"] if e["step"] == 10],
       [{"call": "showContextMenu", "id": KEYS[1], "argument": "identity", "emptySpace": False, "x": 7, "y": 9,
         "step": 10}])
    eq("ctx.showTooltip(lines, point)", [e for e in sim["log"] if e["step"] == 11],
       [{"call": "tooltip.show", "coordinates": [40, 50], "isTouchEvent": False, "identities": [],
         "dataItems": [{"displayName": "Note", "value": "5"}], "step": 11}])
    eq("ctx.hideTooltip() and ctx.clearSelection()", [e["call"] for e in sim["log"] if e["step"] == 12],
       ["tooltip.hide", "clear"])

    small_path = built(definition(js=HOOKS_JS), d, "small.pbiviz")
    eq("a definition within one window never fetches", calls(cv.simulate(small_path, [{"op": "update", "data": more}]),
                                                            "fetchMoreData"), [])

    tile_definition, tile_base = cv.load_definition(os.path.join(EXAMPLES, "kpi_tiles", "definition.json"))
    tiles = built(tile_definition, d, "tiles.pbiviz", base_dir=tile_base)
    kpi = {"columns": [{"role": "values", "name": "Customers", "kind": "measure", "format": "#,##0"},
                       {"role": "values", "name": "Share", "kind": "measure", "format": "0.0%"}], "rows": [[4000, 0.428]]}
    run = cv.simulate(tiles, [
        {"op": "update", "data": kpi},
        {"op": "hover", "selector": "[data-tile=\"1\"]", "x": 109, "y": 48},     # 1
        {"op": "hover", "selector": "[data-tile=\"0\"]", "x": 10, "y": 8},       # 2
        {"op": "hover", "selector": ".kt", "x": 0, "y": 0},                        # 3
        {"op": "contextmenu", "selector": "[data-tile=\"0\"]", "x": 4, "y": 4},  # 4
        {"op": "click", "selector": "[data-tile=\"0\"]"},                         # 5
        {"op": "update", "data": dict(kpi, highlights=[[1000, 0.5]])},             # 6
        {"op": "update", "data": kpi}, {"op": "update", "data": kpi},              # 7, 8
        {"op": "hover", "selector": "[data-tile=\"1\"]", "x": 1, "y": 1},        # 9
    ])
    if not run.get("ok"):
        check("the KPI tiles run under node", False, str(run.get("error")))
        return
    t = run["states"]
    eq("KPI tiles: one tile per measure, no rows to select", (re.findall(r'kt-value"[^>]*>([^<]+)<', t[0]["html"]), t[0]["rows"]),
       (["4K", "42.8%"], []))
    eq("ctx.showTooltip: the script's own lines, no identity", [e for e in run["log"] if e["step"] == 1], [
        {"call": "tooltip.show", "coordinates": [9, 8], "isTouchEvent": False, "identities": [],
         "dataItems": [{"displayName": "Share", "value": "42.8%"}], "step": 1}])
    eq("the kit does not hide a tooltip the script opened; it follows the pointer",
       [(e["call"], e["dataItems"][0]["displayName"]) for e in run["log"] if e["step"] == 2], [("tooltip.move", "Customers")])
    eq("ctx.hideTooltip", [e["call"] for e in run["log"] if e["step"] == 3], ["tooltip.hide"])
    eq("a visual without data points still gets the visual's context menu",
       [(e["call"], e["emptySpace"]) for e in run["log"] if e["step"] == 4], [("showContextMenu", True)])
    eq("and never selects", [e for e in run["log"] if e["step"] == 5], [])
    eq("cross-highlighted tiles show the highlighted part of each measure",
       re.findall(r'kt-value"[^>]*>([^<]+)<|kt-note">([^<]+)<', t[6]["html"]),
       [("1K", ""), ("", "of 4K"), ("50.0%", ""), ("", "of 42.8%")])
    eq("ctx.on keeps one listener however often the visual is drawn",
       [e["call"] for e in run["log"] if e["step"] == 9], ["tooltip.show"])

    crash = built(definition(js="function render(ctx) { return ctx.rows[9].text.category; }"), d, "crash.pbiviz")
    failed = cv.simulate(crash, [{"op": "update", "data": DATA}, {"op": "update", "data": None}])
    eq("a render function that throws: rendering failed is reported in place of finished",
       [e["call"] for e in failed["log"] if e["step"] == 0 and e["call"].startswith("rendering")],
       ["renderingStarted", "renderingFailed"])
    check("with the reason", "undefined" in calls(failed, "renderingFailed", 0)[0]["reason"],
          calls(failed, "renderingFailed", 0)[0]["reason"])
    check("and the visual shows the error instead of staying blank",
          failed["states"][0]["noticeClasses"] == ["vk-error", "vk-notice"]
          and "could not be drawn" in failed["states"][0]["noticeText"], str(failed["states"][0]["noticeText"]))
    eq("it recovers on the next update", failed["states"][1]["noticeClasses"], ["vk-landing", "vk-notice"])
    broken = built(definition(js="var x = missing.name;\nfunction render() { return ''; }"), d, "load.pbiviz")
    eq("a script that fails while loading is reported the same way",
       [e["call"] for e in cv.simulate(broken, [{"op": "update", "data": DATA}])["log"] if e["call"].startswith("rendering")],
       ["renderingStarted", "renderingFailed"])
    none = built(definition(js="var nothing = 1;"), d, "none.pbiviz")
    check("a script with no render function says so in the visual",
          "neither render(ctx) nor main(app)" in cv.simulate(none, [{"op": "update", "data": DATA}])["states"][0]["noticeText"])


def test_kit_app_contract(d):
    print("\n== kit: a measure-page script as a visual (node) ==")
    defn, base = cv.load_definition(os.path.join(EXAMPLES, "ranked_app", "definition.json"))
    path = os.path.join(d, "ranked.pbiviz")
    result = cv.build_custom_visual(defn, path, base_dir=base)
    eq("the module folder builds and parses", (result["success"], result["node_check"]), (True, "passed"))
    data = {"columns": [{"role": "category", "name": "category", "kind": "grouping"},
                        {"role": "value", "name": "Spend", "kind": "measure", "format": "0"}],
            "rows": [["Food", 7300000], ["Bills", 17500000], ["Travel", 900]]}
    sim = cv.simulate(path, [{"op": "update", "data": data}, {"op": "click", "row": 0},
                             {"op": "contextmenu", "row": 1, "x": 1, "y": 2}, {"op": "update", "data": data}])
    if not sim.get("ok"):
        check("the ported app runs under node", False, str(sim.get("error")))
        return
    s = sim["states"]
    eq("main(app) draws from the data roles, ranked", [r["text"] for r in s[0]["rows"]],
       ["1Bills17.5M", "2Food7.3M", "3Travel900"])
    eq("its items are data points: a click selects the row behind the item", calls(sim, "select", 1),
       [{"call": "select", "ids": ['t.category="Food"'], "multiSelect": False, "step": 1}])
    eq("the kit marks the item; the app's own highlight stays out of it",
       [r["classes"] for r in s[1]["rows"]], [["ra-item", "vk-dimmed"], ["ra-item", "vk-selected"], ["ra-item", "vk-dimmed"]])
    eq("and the context menu works on them", calls(sim, "showContextMenu", 2)[0]["id"], 't.category="Bills"')
    eq("the selection is kept when main runs again", [r["classes"] for r in s[3]["rows"]][1], ["ra-item", "vk-selected"])
    eq("no console output", sim["console"], [])

    page = jm.build_js_app_measures("Ranked", js_modules=os.path.join(EXAMPLES, "ranked_app", "modules"),
                                    payload_measures=[("Ranked Payload", "ITEMS")],
                                    sample_payloads={"ITEMS": "Food|7300000;Bills|17500000"})
    eq("the same modules pass the measure builder's own node check", page["node_check"], "passed")


def test_kit_examples(d):
    print("\n== kit: the examples draw sample rows (node) ==")
    samples = {
        "card_list": {"columns": [{"role": "category", "name": "occupation", "kind": "grouping"},
                                  {"role": "value", "name": "Monthly Spend", "kind": "measure", "format": "\\₹#,0"},
                                  {"role": "details", "name": "Customers", "kind": "measure", "format": "#,##0"},
                                  {"role": "details", "name": "Share", "kind": "measure", "format": "0.0%"}],
                      "rows": [["Freelancers", 12589773, 784, 0.458], ["Business Owners", 14667452, 630, 0.332]]},
    }
    defn, base = cv.load_definition(os.path.join(EXAMPLES, "card_list", "definition.json"))
    path = os.path.join(d, "cards.pbiviz")
    cv.build_custom_visual(defn, path, base_dir=base, check_with_node=False)
    data = samples["card_list"]
    sim = cv.simulate(path, [
        {"op": "update", "data": data},
        {"op": "click", "row": 1},
        {"op": "update", "data": dict(data, highlights=[[None, 6000000, 300, 0.4], [None, None, None, None]])},
        {"op": "update", "data": dict(data, objects={"details": {"show": False}, "heading": {"text": "By <job>"},
                                                    "cards": {"units": "none", "minWidth": 200}})},
    ])
    if not sim.get("ok"):
        check("the card list runs under node", False, str(sim.get("error")))
        return
    s = sim["states"]
    eq("card list: a card per row with headline and details", [r["text"] for r in s[0]["rows"]],
       ["Freelancers₹12.6MCustomers784Share45.8%", "Business Owners₹14.7MCustomers630Share33.2%"])
    eq("a click on a card selects it", calls(sim, "select", 1)[0]["ids"], ['t.occupation="Business Owners"'])
    check("cross-highlighted: the highlighted part against the total, on the highlighted card only",
          [r["text"] for r in s[2]["rows"]][0].startswith("Freelancers₹6M of ₹12.6M")
          and " of " not in [r["text"] for r in s[2]["rows"]][1], str([r["text"] for r in s[2]["rows"]]))
    check("settings: details off, a heading (escaped), full numbers, card width",
          [r["text"] for r in s[3]["rows"]] == ["Freelancers₹12,589,773", "Business Owners₹14,667,452"]
          and "By &lt;job&gt;" in s[3]["html"] and "--cl-min:200px" in s[3]["html"], s[3]["html"][:200])
    eq("no console output", sim["console"], [])


# ---------------------------------------------------------------- review round

SECRET = "hunter2-secret-0002"


def link_folder(target, link):
    """Make ``link`` a link to the folder ``target``: a symbolic link, or on Windows, where that
    needs a privilege, a junction. False when this account can make neither."""
    try:
        os.symlink(target, link, target_is_directory=True)
        return True
    except (OSError, NotImplementedError, AttributeError):
        pass
    try:
        import _winapi
        _winapi.CreateJunction(target, link)
        return True
    except (ImportError, OSError, AttributeError):
        return False


def test_paths():
    print("\n== files a definition may read ==")
    with tempfile.TemporaryDirectory() as root:
        base = os.path.join(root, "project", "visual")
        os.makedirs(os.path.join(base, "mods"))
        with open(os.path.join(root, "secret.js"), "w", encoding="utf-8") as f:
            f.write(f"var TOKEN = '{SECRET}';\nfunction render(ctx) {{ return TOKEN; }}\n")
        with open(os.path.join(root, "secret.txt"), "w", encoding="utf-8") as f:
            f.write(f"password = {SECRET}\napi_key: ABCDEF\n")
        with open(os.path.join(root, "secret.png"), "wb") as f:
            f.write(cv._default_icon())
        with open(os.path.join(base, "render.js"), "w", encoding="utf-8") as f:
            f.write(JS)
        with open(os.path.join(base, "notes.txt"), "w", encoding="utf-8") as f:
            f.write(f"var first = 1;\npassword: {SECRET} = api key\n")
        out = os.path.join(base, "out.pbiviz")
        outside = [
            ("js_files with ..", definition(js=None, js_files=[os.path.join("..", "..", "secret.js")]), "js_files[0]"),
            ("js_files with an absolute path", definition(js=None, js_files=[os.path.join(root, "secret.js")]), "js_files[0]"),
            ("css_file with an absolute path", definition(css=None, css_file=os.path.join(root, "secret.txt")), "css_file"),
            ("css_file with ..", definition(css=None, css_file="../../secret.txt"), "css_file"),
            ("js_folder outside", definition(js=None, js_folder=root), "js_folder"),
            ("icon_file outside", definition(icon_file=os.path.join(root, "secret.png")), "icon_file"),
        ]
        for label, defn, where in outside:
            result = cv.build_custom_visual(defn, out, base_dir=base, check_with_node=False)
            check(f"{label}: refused, nothing written, and the definition is not called valid",
                  not result["success"] and not os.path.exists(out) and not cv.validate_definition(defn, base)["valid"]
                  and any(e.startswith(where) and "outside the definition's folder" in e for e in result["errors"]),
                  str(result["errors"]))
            check(f"{label}: the file's content is in no message", SECRET not in json.dumps(result))
        here = errors_of(definition(css=None, css_file=os.path.join(root, "secret.txt")), base)
        gone = errors_of(definition(css=None, css_file=os.path.join(root, "nothing.txt")), base)
        eq("a path outside gets the same answer whether the file exists or not",
           [e.replace("secret.txt", "X") for e in here], [e.replace("nothing.txt", "X") for e in gone])

        inside = cv.build_custom_visual(definition(js=None, js_files=["render.js"]), out, base_dir=base, check_with_node=False)
        check("a file inside the folder is read as before", inside["success"] and os.path.exists(out), str(inside["errors"]))
        os.remove(out)
        dotted = cv.build_custom_visual(definition(js=None, js_files=[os.path.join("mods", "..", "render.js")]), None,
                                        base_dir=base, check_with_node=False)
        check("a name that wanders but ends inside the folder is fine", dotted["success"], str(dotted["errors"]))
        missing = errors_of(definition(js=None, js_files=["nowhere.js"]), base)
        check("a missing file inside is named as given, without the folder's path",
              missing and "nowhere.js" in missing[0] and root not in " ".join(missing), str(missing))

        before = os.getcwd()
        os.chdir(base)
        try:
            for label, defn in (("js_files", definition(js=None, js_files=["render.js"])),
                                ("css_file", definition(css=None, css_file="notes.txt")),
                                ("js_folder", definition(js=None, js_folder="mods")),
                                ("icon_file", definition(icon_file="icon.png"))):
                found = errors_of(defn)
                check(f"without a base folder {label} is not read from the working directory",
                      any("no folder of its own" in e for e in found), str(found))
        finally:
            os.chdir(before)

        outer = os.path.join(root, "outer")
        os.makedirs(outer)
        shutil.copy(os.path.join(root, "secret.js"), os.path.join(outer, "secret.js"))
        if not link_folder(outer, os.path.join(base, "linked")):
            print("  [SKIP] a link that points out of the folder (this account cannot create links)")
        else:
            for label, defn in (("a file reached through a link", definition(js=None, js_files=[os.path.join("linked", "secret.js")])),
                                ("a linked folder", definition(js=None, js_folder="linked"))):
                linked = cv.build_custom_visual(defn, None, base_dir=base, check_with_node=False)
                check(f"{label} that points out of the folder is refused", not linked["success"]
                      and "outside the definition's folder" in linked["errors"][0] and SECRET not in json.dumps(linked),
                      str(linked["errors"]))

        if not NODE:
            print("  [SKIP] a text file given as a script (node is not on PATH)")
            return
        quoted = cv.build_custom_visual(definition(js=None, js_files=["notes.txt"]), out, base_dir=base)
        check("a file that is not JavaScript is refused by name and line", not quoted["success"] and not os.path.exists(out)
              and "notes.txt: line 2: is not valid JavaScript (SyntaxError)" in quoted["errors"][0], str(quoted["errors"]))
        check("and its text is quoted nowhere in the result", SECRET not in json.dumps(quoted) and "password" not in json.dumps(quoted),
              json.dumps(quoted)[:400])


def test_safety_ordinary_code():
    print("\n== safety pass: ordinary code, and code that only looks ordinary ==")
    ordinary = [
        "function Node(parent) { var self = this; self.parent = parent; self.top = 0; }",
        "function Panel() { var self = this; self.open = function () {}; self.open(); }",
        "function Pin() { var self = this; self.location = [1, 2]; }",
        "function Store() { var self = this; self.fetch = function () { return 1; }; return self.fetch(); }",
        "function Anim() { var self = this; self.frames = []; self.frames.push(1); }",
        "var api = { fetch: function () { return 1; } };",
        "var api = { fetch() { return 1; } };",
        "class Store { fetch() { return 1; } static open() {} get top() { return 1; } }",
        "var ops = { eval: 1, parse: 2 };",
        "var keys = { localStorage: 1, parent: null, top: 0, location: 'x' };",
        "function clone() { return new this.constructor(this.x); }",
        "var copy = arr.constructor(3); var same = a.constructor === b.constructor;",
        "if (url.indexOf('javascript:') === 0) { return; }",
        "var help = 'This visual cannot host an <iframe> or <objectives>.';",
        "function load(fetch) { return fetch(1); }",
        "var fetch = function (rows) { return rows; }; fetch([]);",
        "function f(...parent) { return parent.length; }",
        "top: for (var i = 0; i < 3; i++) { continue top; }",
        "class A { #top = 1; static parent = null; get value() { return this.#top; } }",
        "var port = channel.port1; port.postMessage(1);",
        "var v = window?.innerWidth; var w = row?.parent?.id;",
    ]
    for source in ordinary:
        errors, _ = cv.check_script_safety(source, "m.js")
        check(f"allowed: {source[:52]!r}", errors == [], str(errors))
    still_refused = [
        ("function f() { return self.parent.document; }", "reaches for parent"),
        ("var self = this; window.parent.focus();", "reaches for parent"),
        ("var api = { fetch: 1 }; fetch(url);", "uses fetch"),
        ("var pick = ok ? fetch : null;", "uses fetch"),
        ("var api = { load() { return fetch(url); } };", "uses fetch"),
        ("var f = (0, eval)('1');", "uses eval"),
        ("var g = x.constructor.constructor('return this')();", "calls .constructor(...) to build code"),
        ("var g = [].map.constructor(`return ${a}`)();", "calls .constructor(...) to build code"),
        ("window?.parent.postMessage(1, '*');", "reaches for parent"),
        ("window.setTimeout('go()', 5);", "passes text to setTimeout"),
        ("var store = window.localStorage;", "uses localStorage"),
        ("postMessage({ a: 1 }, '*');", "uses postMessage"),
    ]
    for source, want in still_refused:
        errors, _ = cv.check_script_safety(source, "m.js")
        check(f"refused: {source[:52]}", any(want in e for e in errors), str(errors))
    for source, sign in (("var h = '<scr' + 'ipt>'; var t = '<script src=x>';", "'<script'"),
                         ('el.title = "<iframe src=x></iframe>"', "'<iframe'"),
                         ('a.href = "javascript:alert(1)"', "'javascript:'")):
        errors, warnings = cv.check_script_safety(source, "m.js")
        check(f"text that may be markup is warned about, not refused: {sign}",
              errors == [] and any(f"a string holds {sign}" in w for w in warnings), str((errors, warnings)))
    errors, _ = cv.check_script_safety("var a = 1;\nvar t = `l2\nl3 ${ fetch(x) } l3\nl4 ${ `in ${ eval(y) }` }`;\nfetch(u);", "m.js")
    eq("a refusal inside a template literal names the line it is on, also when templates nest",
       [e.split(": ")[1] + ": " + e.split(": ")[2].split(".")[0] for e in errors],
       ["line 3: uses fetch", "line 4: uses eval", "line 5: uses fetch"])
    errors, _ = cv.check_script_safety("var parent = el.parentNode;\nvar t = `${parent.id} ${top.id}`;", "m.js")
    eq("code inside a template sees the names declared around it", [e.split(". ")[0] for e in errors],
       ["m.js: line 2: reaches for top"])


def test_bad_input():
    print("\n== bad input gives a message, never an exception ==")
    cases = [
        ("a role kind given as a list", definition(roles=[{"name": "c", "kind": ["grouping"]}]), "roles[0].kind: must be"),
        ("a role kind given as an object", definition(roles=[{"name": "c", "kind": {"a": 1}}]), "roles[0].kind: must be"),
        ("a blank js_folder", definition(js=None, js_folder=" "), "js_folder: must be a folder name"),
        ("a number default that is not a number (NaN)", definition(format=[{"name": "c", "properties": [
            {"name": "p", "type": "number", "default": float("nan")}]}]), "default: must be a finite number"),
        ("an infinite number default", definition(format=[{"name": "c", "properties": [
            {"name": "p", "type": "number", "default": float("inf")}]}]), "default: must be a finite number"),
        ("an infinite limit", definition(format=[{"name": "c", "properties": [
            {"name": "p", "type": "number", "min": float("-inf")}]}]), "min: must be a finite number"),
        ("a number too large for a script", definition(format=[{"name": "c", "properties": [
            {"name": "p", "type": "number", "default": 10 ** 400}]}]), "default: must be a finite number"),
        ("a guid that is a reserved word", definition(guid="function"), "guid: 'function' is a reserved word of JavaScript"),
        ("another reserved word", definition(guid="debugger"), "guid: 'debugger' is a reserved word"),
        ("a guid that is a built-in visual", definition(guid="clusteredBarChart"),
         "guid: 'clusteredBarChart' is the name of a visual built into Power BI"),
        ("a built-in visual in other letter case", definition(guid="PIECHART"), "is the name of a visual built into Power BI"),
        ("a name too long for any report folder", definition(name="n" * 80), "name: the guid is 112 characters"),
    ]
    for label, bad, want in cases:
        try:
            found = cv.validate_definition(bad, tempfile.gettempdir())["errors"]
            built = cv.build_custom_visual(bad, None, base_dir=tempfile.gettempdir(), check_with_node=False)
        except Exception as e:  # noqa: BLE001  (the point of the check)
            check(f"{label}: no exception", False, f"{type(e).__name__}: {e}")
            continue
        check(f"{label}: says {want[:52]!r}", any(want in e for e in found) and not built["success"], str(found))

    long_name = cv.build_custom_visual(definition(name="a" * 30), check_with_node=False)
    check("a name of 30 characters builds, with a warning that says how long a name may be",
          long_name["success"] and any("A name of 29 characters or fewer" in w and "refuses to open" in w
                                       for w in long_name["warnings"]), str(long_name["warnings"]))
    check("a name of 29 characters is not warned about",
          not any(w.startswith("name:") for w in cv.build_custom_visual(definition(name="a" * 29), check_with_node=False)["warnings"]))

    with tempfile.TemporaryDirectory() as d:
        blocker = os.path.join(d, "a-file")
        with open(blocker, "w", encoding="utf-8") as f:
            f.write("x")
        folder = os.path.join(d, "a-folder.pbiviz")
        os.makedirs(folder)
        for label, target in (("a path under a file", os.path.join(blocker, "x.pbiviz")),
                              ("a path that is a folder", folder), ("a path that is not text", 5)):
            try:
                result = cv.build_custom_visual(definition(), target, check_with_node=False)
            except Exception as e:  # noqa: BLE001
                check(f"output_path, {label}: no exception", False, f"{type(e).__name__}: {e}")
                continue
            check(f"output_path, {label}: the result says it could not be written",
                  not result["success"] and not result["written"] and result["errors"][0].startswith("output_path:")
                  and "Not built" in cv.describe_build(result), str(result["errors"]))
        eq("and no temporary file is left behind", sorted(os.listdir(d)), ["a-file", "a-folder.pbiviz"])


def repack(source, name, change):
    """A copy of the package at ``source`` with its metadata passed through ``change``."""
    path = os.path.join(os.path.dirname(source), name)
    with zipfile.ZipFile(source) as src, zipfile.ZipFile(path, "w") as dst:
        for info in src.infolist():
            data = src.read(info)
            if info.filename.endswith(".pbiviz.json"):
                doc = json.loads(data)
                doc = change(doc) or doc
                data = json.dumps(doc).encode("utf-8")
            if not info.is_dir():
                dst.writestr(info.filename, data)
    return path


def test_inspector_trust():
    print("\n== inspector: wrong shapes, the kit marker, visuals without a data point ==")
    with tempfile.TemporaryDirectory() as d:
        _, good = build(definition(), d, "good.pbiviz")
        for label, key, value in (("capabilities is a list", "capabilities", [1]), ("capabilities is text", "capabilities", "x"),
                                  ("content is text", "content", "x"), ("visual is a list", "visual", [1])):
            try:
                cv.inspect_custom_visual(repack(good, "shape.pbiviz", lambda doc, k=key, v=value: doc.update({k: v})))
                check(f"{label}: refused as not a package", False, "no error raised")
            except pbir_packages.PbivizError as e:
                check(f"{label}: refused as not a package", key in str(e), str(e))
            except Exception as e:  # noqa: BLE001
                check(f"{label}: refused as not a package", False, f"{type(e).__name__}: {e}")

        def caps(change):
            return lambda doc: change(doc["capabilities"])

        odd = [("objects is a list", caps(lambda c: c.update(objects=[1])), "capabilities.objects"),
               ("a card's properties are a list of numbers", caps(lambda c: c.update(objects={"a": {"properties": [1, 2]}})),
                "capabilities.objects.a.properties"),
               ("a card is text", caps(lambda c: c.update(objects={"a": "x"})), "capabilities.objects.a"),
               ("tooltips is text", caps(lambda c: c.update(tooltips="yes")), "capabilities.tooltips"),
               ("tooltips.supportedTypes is a list", caps(lambda c: c.update(tooltips={"supportedTypes": [1]})),
                "capabilities.tooltips.supportedTypes"),
               ("drilldown is a list", caps(lambda c: c.update(drilldown=["category"])), "capabilities.drilldown"),
               ("drilldown.roles is text", caps(lambda c: c.update(drilldown={"roles": "category"})), "capabilities.drilldown.roles"),
               ("dataRoles is an object", caps(lambda c: c.update(dataRoles={"a": 1})), "capabilities.dataRoles"),
               ("privileges is text", caps(lambda c: c.update(privileges="WebAccess")), "capabilities.privileges"),
               ("content.js is a number", lambda doc: doc.update(content={"js": 5}), "no script")]
        for label, change, where in odd:
            try:
                info = cv.inspect_custom_visual(repack(good, "odd.pbiviz", change))
                text = cv.describe_inspection(info)
            except Exception as e:  # noqa: BLE001
                check(f"{label}: read around, with a warning", False, f"{type(e).__name__}: {e}")
                continue
            check(f"{label}: read around, with a warning", any(where in w for w in info["warnings"]) and info["guid"] in text,
                  str(info["warnings"]))

        guid = "fakeGuid00000001"
        marker = f'kit: "{cv.KIT_VERSION}", config: '
        fake = os.path.join(d, "fake.pbiviz")
        with zipfile.ZipFile(fake, "w") as z:
            z.writestr("package.json", json.dumps({"visual": {"name": "fake", "guid": guid}}))
            z.writestr(f"resources/{guid}.pbiviz.json", json.dumps({
                "visual": {"name": "fake", "guid": guid}, "apiVersion": "5.3.0",
                "capabilities": {"dataRoles": [{"name": "c", "kind": "Grouping"}], "privileges": []},
                "content": {"js": f'var x = "{marker}"; // {marker}' + '{"features": {"selection": true, "contextMenu": true, '
                                  '"tooltips": true}, "format": [{}]}'}}))
        info = cv.inspect_custom_visual(fake)
        eq("a script that only carries the kit's marker is not taken for the kit's",
           (info["built_by_kit"], [k for k in ("selection", "context_menu", "drillthrough", "tooltips", "bookmarks",
                                               "format_pane") if info["features"][k]]), (False, []))
        tampered = cv.inspect_custom_visual(repack(good, "tampered.pbiviz", lambda doc: doc["content"].update(
            js=doc["content"]["js"].replace('config: {"name"', "config: {name"))))
        check("a kit script whose configuration no longer reads is not called the kit's", tampered["built_by_kit"] is False)
        check("the kit's own output is still recognised, minified or not",
              cv.inspect_custom_visual(good)["built_by_kit"] and cv.inspect_custom_visual(
                  build(definition(), d, "plain.pbiviz", minify=False)[1])["built_by_kit"])

        _, measures = build(definition(roles=[{"name": "values", "kind": "measure"}]), d, "measures.pbiviz")
        info = cv.inspect_custom_visual(measures)
        eq("measures only: no cross-filtering, drillthrough or report-page tooltip is claimed",
           {k: info["features"][k] for k in ("selection", "drillthrough", "report_page_tooltips", "multi_visual_selection",
                                             "bookmarks", "context_menu", "tooltips", "cross_highlight")},
           {"selection": False, "drillthrough": False, "report_page_tooltips": False, "multi_visual_selection": False,
            "bookmarks": False, "context_menu": True, "tooltips": True, "cross_highlight": True})
        check("and the inspection says why", any("No grouping role" in n for n in info["notes"])
              and "Note: No grouping role" in cv.describe_inspection(info), str(info["notes"]))
        full = cv.inspect_custom_visual(good)
        eq("with a grouping role they are reported", (full["features"]["selection"], full["features"]["drillthrough"],
                                                      full["features"]["report_page_tooltips"], full["notes"]),
           (True, True, True, []))

        script = metadata(good)["content"]["js"]
        stolen = repack(good, "stolen.pbiviz", lambda doc: doc["capabilities"].update(
            dataRoles=[{"name": "values", "displayName": "V", "kind": "Measure"}]))
        info = cv.inspect_custom_visual(stolen)
        check("the kit's whole script over capabilities with no grouping role still claims no data point",
              "createSelectionManager" in script and not info["features"]["selection"]
              and not info["features"]["drillthrough"] and not info["features"]["report_page_tooltips"])


def test_feature_notes():
    print("\n== build report: what this visual does, feature by feature ==")
    full = cv.build_custom_visual(definition(features={"drilldown": "category"}, format=[
        {"name": "look", "properties": [{"name": "show", "type": "toggle"}]}]), check_with_node=False)
    eq("every feature of the report has its plain statement", [n["feature"] for n in full["feature_notes"]], list(full["features"]))
    check("a visual with a grouping role and a measure does all of it but fetching more",
          [n["feature"] for n in full["feature_notes"] if not n["applies"]] == ["fetch_more_data"]
          and full["features"]["drilldown"] == "category", str([n for n in full["feature_notes"] if not n["applies"]]))
    measures = cv.build_custom_visual(definition(roles=[{"name": "values", "kind": "measure"}]), check_with_node=False)
    by = {n["feature"]: n for n in measures["feature_notes"]}
    eq("measures only: what cannot apply is reported as not applying",
       {k: measures["features"][k] for k in ("selection", "multi_visual_selection", "bookmarks", "drillthrough",
                                             "report_page_tooltips", "row_limit_notice", "context_menu", "tooltips",
                                             "cross_highlight")},
       {"selection": False, "multi_visual_selection": False, "bookmarks": False, "drillthrough": False,
        "report_page_tooltips": False, "row_limit_notice": False, "context_menu": True, "tooltips": True,
        "cross_highlight": True})
    check("each with its reason", "no grouping role" in by["selection"]["why"] and "no grouping role" in by["drillthrough"]["why"]
          and "no grouping role" in by["report_page_tooltips"]["why"] and by["tooltips"]["why"] == "", str(by["selection"]))
    text = cv.describe_build(measures)
    check("the summary states it per feature",
          "  cross-filter and cross-highlight other visuals on click: no, the visual has no grouping role" in text
          and "  drillthrough, include and exclude from a data point's context menu: no, the visual has no grouping role" in text
          and "  report-page tooltips: no, the visual has no grouping role" in text
          and "  host tooltips on hover: yes" in text and "  context menu on right-click: yes" in text, text)
    switched = cv.build_custom_visual(definition(features={"selection": False, "context_menu": False, "tooltips": False,
                                                           "limit_notice": False}), check_with_node=False)
    by = {n["feature"]: n for n in switched["feature_notes"]}
    eq("a feature switched off says so", [by[k]["why"] for k in ("selection", "context_menu", "tooltips", "row_limit_notice")],
       ["switched off in the definition"] * 4)
    eq("and what hangs on it follows", (by["drillthrough"]["why"], by["report_page_tooltips"]["why"],
                                        by["multi_visual_selection"]["why"]),
       ("the context menu is switched off in the definition", "tooltips are switched off in the definition",
        "there is no selection"))
    columns = cv.build_custom_visual(definition(roles=[{"name": "category", "kind": "grouping"}]), check_with_node=False)
    check("no measure role: being cross-highlighted is reported as not applying",
          columns["features"]["cross_highlight"] is False and "no measure role" in
          {n["feature"]: n for n in columns["feature_notes"]}["cross_highlight"]["why"])
    check("the notes document the row limit", any("at most 1,000 rows" in n and "ctx.truncated" in n for n in full["notes"])
          and not any("at most" in n for n in measures["notes"]), str(full["notes"]))


LIMIT_JS = """
function render(ctx) {
  var total = 0;
  ctx.rows.forEach(function (row) { total += row.values.value; });
  return '<p class="flags">' + ctx.rows.length + '/' + ctx.truncated + '/' + ctx.rowLimit + '/' + ctx.hasMoreRows + '/' +
    ctx.loadingMore + '/' + total + '</p>' + ctx.rows.slice(0, 5).map(function (row) {
      return '<i data-row="' + row.index + '">' + ctx.escape(row.text.category) + '</i>';
    }).join("");
}
"""


def flags(state):
    return re.search(r'class="flags">([^<]*)<', state["html"]).group(1)


def test_kit_row_limit(d):
    print("\n== kit: rows cut at the limit (node) ==")
    def data(count, **more):
        return dict(DATA, rows=[["k%d" % i, 1] for i in range(count)], **more)

    path = built(definition(js=LIMIT_JS, rows=2), d, "limit.pbiviz")
    eq("the package asks the host for one row more than it takes", metadata(path)["capabilities"]["dataViewMappings"][0][
        "categorical"]["categories"]["dataReductionAlgorithm"], {"top": {"count": 3}})
    sim = cv.simulate(path, [{"op": "update", "data": data(3)}, {"op": "click", "row": 1},
                             {"op": "update", "data": data(2)}, {"op": "update", "data": data(1)},
                             {"op": "update", "data": data(0)}])
    if not sim.get("ok"):
        check("the visual runs under node", False, str(sim.get("error")))
        return
    s = sim["states"]
    eq("more rows than the limit: the visual takes the limit and is told the data was cut", flags(s[0]), "2/true/2/false/false/2")
    eq("the row beyond the limit is not drawn", [r["row"] for r in s[0]["rows"]], ["0", "1"])
    eq("and the visual says so itself", s[0]["limitText"], "Showing the first 2 rows")
    check("a click on a row still selects with the note showing", calls(sim, "select", 1) != [])
    eq("exactly the limit: nothing was cut, nothing is said", (flags(s[2]), s[2]["limitText"]), ("2/false/2/false/false/2", ""))
    eq("fewer rows than the limit", (flags(s[3]), s[3]["limitText"]), ("1/false/2/false/false/1", ""))
    eq("no rows: the empty message, without the note", (s[4]["noticeClasses"], s[4]["limitText"]), (["vk-empty", "vk-notice"], ""))

    quiet = cv.simulate(built(definition(js=LIMIT_JS, rows=2, features={"limit_notice": False}), d, "quiet.pbiviz"),
                        [{"op": "update", "data": data(3)}])
    eq("features.limit_notice: false leaves the note out; the render code is still told",
       (flags(quiet["states"][0]), quiet["states"][0]["limitText"]), ("2/true/2/false/false/2", ""))

    window = cv.simulate(built(definition(js=LIMIT_JS, rows=30000), d, "window.pbiviz"),
                         [{"op": "update", "data": data(3, segment=True)}, {"op": "update", "data": data(3)}])
    eq("exactly one window wanted and the host has more: cut, and no more is fetched",
       (flags(window["states"][0]), window["states"][0]["limitText"], calls(window, "fetchMoreData")),
       ("3/true/30000/true/false/3", "Showing the first 3 rows", []))
    eq("the host has no more: not cut", flags(window["states"][1]), "3/false/30000/false/false/3")

    many = cv.simulate(built(definition(js=LIMIT_JS, rows=30002), d, "many.pbiviz"), [
        {"op": "update", "data": data(4, segment=True)},
        {"op": "update", "data": data(30005, segment=True)},
        {"op": "update", "data": data(7)}], timeout=180)
    if not many.get("ok"):
        check("the visual takes 30,005 rows under node", False, str(many.get("error"))[:300])
        return
    m = many["states"]
    eq("windows still arriving: not cut yet, more is asked for", (flags(m[0]), m[0]["limitText"],
                                                                  [e["step"] for e in calls(many, "fetchMoreData")]),
       ("4/false/30002/true/true/4", "", [0]))
    eq("the windows pass the limit: the visual keeps the limit, is told, and stops asking",
       (flags(m[1]), m[1]["limitText"]), ("30002/true/30002/true/false/30002", "Showing the first 30,002 rows"))
    eq("all rows arrived within the limit: not cut", (flags(m[2]), m[2]["limitText"]), ("7/false/30002/false/false/7", ""))


TWO_JS = """
function render(ctx) {
  return ctx.rows.map(function (row) {
    return '<div class="item" data-row="' + row.index + '">' + ctx.escape(row.text.a) + '/' + ctx.escape(row.text.b) + '=' +
      ctx.escape(String(row.values.a)) + '</div>';
  }).join("") + '<p class="gap">space</p>';
}
"""
CALLS_JS = JS + """
function selectionChanged(ctx) {}
(function () {
  APP.on(APP.root, "dblclick", "pick", function () { APP.select(APP.rows[1]); });
  APP.on(APP.root, "wheel", "drop", function () { APP.clearSelection(); });
  APP.on(APP.root, "focusin", "menu", function () { APP.contextMenu(APP.rows[0], { x: 3, y: 4 }); });
}());
"""
OWN = json.dumps('<p data-row="0">own</p>')          # markup as a JavaScript string literal
RETURNS = [
    ("nothing, after drawing into ctx.root", "function render(ctx) { ctx.root.innerHTML = " + OWN + "; }", "own"),
    ("ctx.root itself", "function render(ctx) { ctx.root.innerHTML = " + OWN + "; return ctx.root; }", "own"),
    ("an element", "function render(ctx) { var d = document.createElement('div'); d.textContent = 'node'; return d; }", "node"),
    ("a fragment", "function render(ctx) { var f = document.createDocumentFragment(), b = document.createElement('b');"
                   " b.textContent = 'piece'; f.appendChild(b); return f; }", "piece"),
    ("null, after drawing", "function render(ctx) { ctx.root.textContent = 'drawn'; return null; }", "drawn"),
]


def test_kit_review_fixes(d):
    print("\n== kit: review round (node) ==")
    roles = [{"name": "a", "kind": "grouping"}, {"name": "b", "kind": "grouping"}, {"name": "m", "kind": "measure"}]
    two = {"columns": [{"role": "a", "name": "city", "kind": "grouping"}, {"role": "b", "name": "job", "kind": "grouping"},
                       {"role": "m", "name": "N", "kind": "measure"}],
           "rows": [["Mumbai", "A", 1], ["Mumbai", "B", 2], ["Pune", "A", 3], [None, "", 4]]}
    sim = cv.simulate(built(definition(js=TWO_JS, roles=roles), d, "two.pbiviz"), [
        {"op": "update", "data": two},
        {"op": "click", "row": 1},                                        # 1
        {"op": "hover", "row": 1, "x": 150, "y": 90},                     # 2
        {"op": "update", "data": two},                                    # 3 the tooltip must not outlive the redraw
        {"op": "hover", "row": 3, "x": 150, "y": 90},                     # 4
        {"op": "contextmenu", "row": 2, "x": 0, "y": 47},                 # 5
    ])
    if not sim.get("ok"):
        check("the visual runs under node", False, str(sim.get("error")))
        return
    s = sim["states"]
    eq("two category columns: a row's identity names both, so rows that share the first are told apart",
       calls(sim, "select", 1), [{"call": "select", "ids": ['t.city="Mumbai"|t.job="B"'], "multiSelect": False, "step": 1}])
    eq("and only the clicked row is marked", [classes(s[1], r) for r in (0, 1, 2)],
       [["item", "vk-dimmed"], ["item", "vk-selected"], ["item", "vk-dimmed"]])
    eq("the tooltip position is the pointer's, measured from the visual's own corner",
       calls(sim, "tooltip.show", 2)[0]["coordinates"], [50, 50])
    eq("drawing again hides the tooltip of a row that may be gone", [e["call"] for e in sim["log"] if e["step"] == 3
                                                                      and e["call"].startswith("tooltip")], ["tooltip.hide"])
    eq("a category without a value reads (Blank), in the row and in the tooltip",
       ([r["text"] for r in s[0]["rows"]][3], calls(sim, "tooltip.show", 4)[0]["dataItems"][:2]),
       ("(Blank)/(Blank)=null", [{"displayName": "city", "value": "(Blank)"}, {"displayName": "job", "value": "(Blank)"}]))
    eq("a pointer at the left edge still gives its position to the menu", [(e["x"], e["y"]) for e in calls(sim, "showContextMenu", 5)],
       [(0, 47)])

    frozen = cv.simulate(built(definition(js=CALLS_JS), d, "calls.pbiviz"), [
        {"op": "update", "data": DATA},
        {"op": "host_select", "rows": [0]},                               # 1 the host holds a selection
        {"op": "click", "row": 1},                                        # 2
        {"op": "click", "selector": ".gap"},                              # 3 empty space
        {"op": "event", "type": "dblclick", "selector": ".vk-root"},      # 4 ctx.select
        {"op": "event", "type": "wheel", "selector": ".vk-root"},         # 5 ctx.clearSelection
        {"op": "event", "type": "focusin", "selector": ".vk-root"},       # 6 ctx.contextMenu
        {"op": "key", "row": 1, "key": "Enter"},                          # 7
    ], host={"allowInteractions": False})
    eq("no interactions allowed: a click, a click on empty space, a key and the script's own ctx.select, "
       "ctx.clearSelection and ctx.contextMenu ask the host for nothing",
       [(e["step"], e["call"]) for e in frozen["log"] if e["step"] >= 2], [])
    eq("and the selection the host holds stays", classes(frozen["states"][7], 0), ["item", "vk-selected"])
    allowed = cv.simulate(built(definition(js=CALLS_JS), d, "calls.pbiviz"), [
        {"op": "update", "data": DATA}, {"op": "event", "type": "dblclick", "selector": ".vk-root"},
        {"op": "event", "type": "wheel", "selector": ".vk-root"}, {"op": "event", "type": "focusin", "selector": ".vk-root"}])
    eq("the same calls with interactions allowed do reach the host", [e["call"] for e in allowed["log"] if e["step"] >= 1],
       ["select", "clear", "showContextMenu"])

    pane = [{"name": "look", "properties": [{"name": "order", "type": "dropdown", "items": ["asc", "desc"], "default": "desc"},
                                            {"name": "n", "type": "number", "default": 3},
                                            {"name": "c", "type": "color", "default": "#112233"}]}]
    show = "function render(ctx) { return '<pre>' + ctx.escape(JSON.stringify(ctx.settings.look)) + '</pre>'; }"
    path = built(definition(js=show, format=pane), d, "pane.pbiviz")
    for label, stored, want in (
            ("a dropdown value that is not one of the items", {"order": "sideways"}, {"order": "desc", "n": 3, "c": "#112233"}),
            ("a dropdown value of another type", {"order": 5}, {"order": "desc", "n": 3, "c": "#112233"}),
            ("a number stored as true", {"n": True}, {"order": "desc", "n": 3, "c": "#112233"}),
            ("an empty colour", {"c": {"solid": {"color": ""}}}, {"order": "desc", "n": 3, "c": "#112233"}),
            ("a colour that is markup", {"c": {"solid": {"color": 'red" onload="x'}}}, {"order": "desc", "n": 3, "c": "#112233"}),
            ("a colour by name, and a number as text", {"c": {"solid": {"color": "rebeccapurple"}}, "n": "7"},
             {"order": "desc", "n": 7, "c": "rebeccapurple"})):
        run = cv.simulate(path, [{"op": "update", "data": dict(DATA, objects={"look": stored})}, {"op": "formatting_model"}])
        got = json.loads(html.unescape(re.search(r"<pre>(.*)</pre>", run["states"][0]["html"], re.S).group(1)))
        slices = {x["uid"]: x["control"]["properties"]["value"] for x in run["states"][1]["result"]["cards"][0]["groups"][0]["slices"]}
        eq(f"settings: {label} falls back to the default", got, want)
        eq(f"settings: {label}: the pane shows what the render code gets",
           (slices["look-order"]["value"], slices["look-n"], slices["look-c"]["value"]), (got["order"], got["n"], got["c"]))

    for label, js, text in RETURNS:
        run = cv.simulate(built(definition(js=js), d, "returns.pbiviz"), [{"op": "update", "data": DATA}, {"op": "update", "data": DATA}])
        check(f"render may return {label}", text in run["states"][1]["html"] and run["states"][1]["html"].count(text) == 1
              and [e["call"] for e in run["log"] if e["call"].startswith("rendering")][-1] == "renderingFinished",
              run["states"][1]["html"][:120])
    for label, js, word in (("a number", "function render(ctx) { return 42; }", "a number"),
                            ("an object", "function render(ctx) { return { html: 'x' }; }", "an object"),
                            ("true", "function render(ctx) { return true; }", "a boolean")):
        run = cv.simulate(built(definition(js=js), d, "returns.pbiviz"), [{"op": "update", "data": DATA}])
        eq(f"render returning {label}: reported as failed, not as drawn",
           [e["call"] for e in run["log"] if e["call"].startswith("rendering")], ["renderingStarted", "renderingFailed"])
        check(f"render returning {label}: the visual says what was returned and what is expected",
              run["states"][0]["noticeClasses"] == ["vk-error", "vk-notice"] and f"returned {word}" in run["states"][0]["noticeText"]
              and "Return the markup as text" in run["states"][0]["noticeText"], run["states"][0]["noticeText"])

    odd = "function render(ctx) { return " + json.dumps("".join(
        '<i class="o%d" data-row="%s">x</i>' % (k, value) for k, value in enumerate(("1.9", "2abc", "-1", " 2 ", "01")))) + "; }"
    run = cv.simulate(built(definition(js=odd), d, "odd.pbiviz"), [{"op": "update", "data": DATA}] + [
        {"op": "click", "selector": ".o%d" % k} for k in range(5)])
    eq("data-row holds a whole number or names no row: 1.9, 2abc and -1 select nothing",
       [[e.get("ids") for e in run["log"] if e["step"] == step and e["call"] == "select"] for step in range(1, 6)],
       [[], [], [], [[KEYS[2]]], [[KEYS[1]]]])


FORMAT_MORE_JS = r"""
function render(ctx) {
  var cases = [
    [1234567, ""], [1234.5678, ""], [0.000123, ""], [-1234.5, ""], [0.1 + 0.2, ""], [1e21, ""], [0.00001234, ""],
    [1234.5678, "G"], [1234567, "General Number"], [0.5, "Percent"], [1234.5, "Currency"], [1234.5, "Fixed"],
    [1234.5, "Standard"], [12345.678, "Scientific"], [12345.678, "0.00E+00"], [0.00012, "0.0E-0"], [99999, "0.0E+0"],
    [12345.678, "E2"], [0, "0.00E+00"], [1234.5, "#,0.0,"], [1, "Yes/No"], [0, "On/Off"], [true, "Yes/No"],
    [1234.5, "Foo"], [1234.5, "X"], [1234.5, "\"n/a\""], [0.0125, "0.0" + String.fromCharCode(0x2030)],
    [1234.5, "", {units: "auto"}], [1234.5678, "", {decimals: 1}]
  ];
  return "<pre>" + ctx.escape(JSON.stringify(cases.map(function (c) { return ctx.format(c[0], c[1], c[2]); }))) + "</pre>";
}
"""


def test_kit_format_general(d):
    print("\n== kit: General, named and exponent formats (node) ==")
    sim = cv.simulate(built(definition(js=FORMAT_MORE_JS), d, "fmt2.pbiviz"), [{"op": "update", "data": DATA}])
    if not sim.get("ok"):
        check("the visual runs under node", False, str(sim.get("error")))
        return
    got = json.loads(html.unescape(re.search(r"<pre>(.*)</pre>", sim["states"][0]["html"], re.S).group(1)))
    cases = [("no format: no thousands separators", "1234567"), ("no format: the decimals the number has", "1234.5678"),
             ("no format: a small number is not cut to zero", "0.000123"), ("no format: negative", "-1234.5"),
             ("no format: no floating-point noise", "0.3"), ("no format: very large takes an exponent", "1E+21"),
             ("no format: very small takes an exponent", "1.234E-05"), ("G", "1234.5678"),
             ("General Number", "1234567"), ("Percent", "50.00%"), ("Currency (no symbol is known)", "1,234.50"),
             ("Fixed", "1234.50"), ("Standard", "1,234.50"), ("Scientific", "1.23E+04"), ("0.00E+00", "1.23E+04"),
             ("0.0E-0: a sign only when negative", "1.2E-4"), ("0.0E+0: rounding carries into the exponent", "1.0E+5"),
             ("E2", "1.23E+004"), ("zero with an exponent", "0.00E+00"),
             ("a comma after the decimals is not printed", "1,234.5"), ("Yes/No", "Yes"), ("On/Off", "Off"),
             ("Yes/No on a boolean", "Yes"), ("an unknown format name prints the number, not the name", "1234.5"),
             ("a standard letter that is not supported prints the number", "1234.5"),
             ("a pattern without a digit placeholder prints the number", "1234.5"),
             ("per mille", "12.5" + chr(0x2030)), ("no format, with units", "1.2K"), ("no format, with decimals", "1234.6")]
    for (label, want), value in zip(cases, got):
        eq(f"format: {label}", value, want)
    eq("every case was checked", len(got), len(cases))


if __name__ == "__main__":
    for fn in (test_guid, test_package_structure, test_determinism, test_capabilities, test_validation,
               test_safety_pass, test_inspector, test_report, test_examples, test_paths,
               test_safety_ordinary_code, test_bad_input, test_inspector_trust, test_feature_notes):
        fn()
    node_suites = (test_kit_selection, test_kit_keyboard_and_static, test_kit_highlights_and_settings, test_kit_format,
                   test_kit_explicit, test_kit_app_contract, test_kit_examples, test_kit_row_limit,
                   test_kit_review_fixes, test_kit_format_general)
    if NODE:
        with tempfile.TemporaryDirectory() as folder:
            for fn in node_suites:
                fn(folder)
    else:
        print(f"\n  [SKIP] {len(node_suites)} suites that run the visual (node is not on PATH)")
    print()
    if _failures:
        print(f"FAILED: {len(_failures)} check(s): {_failures}")
        sys.exit(1)
    print("ALL CUSTOM VISUAL TESTS PASSED")
