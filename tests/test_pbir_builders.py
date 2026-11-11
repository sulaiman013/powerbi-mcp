"""
Tests for the PBIR emitters (src/pbir_builders.py) and the custom visual package reader
(src/pbir_packages.py). The expected dicts follow the structure of report files that open in
Power BI Desktop; the values are made up. No Power BI required.
Run: python tests/test_pbir_builders.py
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
import pbir_packages  # noqa: E402

_failures = []


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f": {detail}" if detail and not cond else ""))
    if not cond:
        _failures.append(name)


def eq(name, actual, expected):
    detail = ""
    if actual != expected:
        got, want = (json.dumps(v, sort_keys=True)[:700] for v in (actual, expected))
        detail = f"got {got} expected {want}"
    check(name, actual == expected, detail)


def L(value):
    return {"expr": {"Literal": {"Value": value}}}


def C(hex_color):
    return {"solid": {"color": L(f"'{hex_color}'")}}


OFF, ON = [{"properties": {"show": L("false")}}], [{"properties": {"show": L("true")}}]
PAD0 = [{"properties": {s: L("0D") for s in ("top", "bottom", "left", "right")}}]
KEEP = [{"properties": {"keepLayerOrder": L("true")}}]
PLAIN = {"title": OFF, "background": OFF, "border": OFF, "dropShadow": OFF, "visualHeader": OFF,
         "padding": PAD0,
         "general": KEEP}
POS = {"x": 10, "y": 20, "z": 300, "width": 400, "height": 50, "tabOrder": 300}


def test_literals():
    print("\n== literals ==")
    eq("text is single-quoted", pb.lit_str("Sales"), L("'Sales'"))
    eq("an apostrophe is doubled", pb.lit_str("this year's"), L("'this year''s'"))
    eq("a whole number", pb.lit_num(12), L("12D"))
    eq("a whole float drops its decimals", pb.lit_num(12.0), L("12D"))
    eq("a fraction keeps them", pb.lit_num(10.5), L("10.5D"))
    eq("an integer literal", pb.lit_int(2), L("2L"))
    eq("true", pb.lit_bool(True), L("true"))
    eq("false", pb.lit_bool(0), L("false"))
    eq("a colour", pb.color("#112233"), C("#112233"))
    eq("lit picks by type", [pb.lit(True), pb.lit(3), pb.lit("a")], [L("true"), L("3D"), L("'a'")])
    eq("lit passes an expression through", pb.lit({"expr": {"x": 1}}), {"expr": {"x": 1}})
    try:
        pb.lit_num(True)
        check("a bool is not a number", False)
    except TypeError:
        check("a bool is not a number", True)
    eq("a measure as a property value", pb.measure_expr("'Key Figures'.Total"),
       {"expr": {"Measure": {"Expression": {"SourceRef": {"Entity": "Key Figures"}}, "Property": "Total"}}})
    eq("a registered file as a property value", pb.resource_expr("logo.png"),
       {"expr": {"ResourcePackageItem": {"PackageName": "RegisteredResources", "PackageType": 1,
                                         "ItemName": "logo.png"}}})
    eq("position keeps whole numbers whole", pb.position(10, 20.0, 400, 50, 300), POS)
    eq("position rounds fractions and takes a tab order", pb.position(1.234, 2, 3, 4, 5, 9),
       {"x": 1.23, "y": 2, "z": 5, "width": 3, "height": 4, "tabOrder": 9})


def test_objects():
    print("\n== formatting objects ==")
    eq("the plain frame", pb.container_objects(**pb.PLAIN_CHROME), PLAIN)
    eq("nothing given leaves only layer order", pb.container_objects(), {"general": KEEP})
    eq("layer order can be left out", pb.container_objects(keep_layer_order=False), {})
    framed = pb.container_objects(title="Revenue", subtitle={"text": "By month", "color": "#445566",
                                                             "size": 9.5,
                                                             "font": "Segoe UI"},
                                  background="#FFFFFF", border={"color": "#DDDDDD", "radius": 8},
                                  shadow=False,
                                  padding={"top": 12, "bottom": 10, "left": 16, "right": 16}, header=True,
                                  alt_text="Revenue by month", tooltip=False, lock_aspect=True)
    eq("a full frame", framed, {
        "title": [{"properties": {"show": L("true"), "text": L("'Revenue'")}}],
        "subTitle": [{"properties": {"show": L("true"), "text": L("'By month'"), "fontColor": C("#445566"),
                                     "fontSize": L("9.5D"), "fontFamily": L("'Segoe UI'")}}],
        "background": [{"properties": {"show": L("true"), "color": C("#FFFFFF"), "transparency": L("0D")}}],
        "border": [{"properties": {"show": L("true"), "color": C("#DDDDDD"), "radius": L("8D")}}],
        "dropShadow": OFF,
        "visualHeader": ON,
        "padding": [{"properties": {"top": L("12D"), "bottom": L("10D"), "left": L("16D"),
                                    "right": L("16D")}}],
        "visualTooltip": OFF,
        "lockAspect": ON,
        "general": [{"properties": {"altText": L("'Revenue by month'"), "keepLayerOrder": L("true")}}],
    })
    eq("compact objects", pb.simple_objects({"legend": {"show": False, "fontSize": 10,
                                                        "labelColor": "#333333",
                                                        "position": "Top"},
                                             "raw": [{"properties": {"a": L("1D")},
                                                      "selector": {"id": "x"}}]}),
       {"legend": [{"properties": {"show": L("false"), "fontSize": L("10D"), "labelColor": C("#333333"),
                                   "position": L("'Top'")}}],
        "raw": [{"properties": {"a": L("1D")}, "selector": {"id": "x"}}]})
    eq("page navigation", pb.action_link("page", "detail", tooltip="Open detail"),
       [{"properties": {"show": L("true"), "type": L("'PageNavigation'"), "navigationSection": L("'detail'"),
                        "tooltip": L("'Open detail'")}}])
    eq("bookmark action", pb.action_link("bookmark", "filters_open", default_tooltip=False),
       [{"properties": {"show": L("true"), "type": L("'Bookmark'"), "bookmark": L("'filters_open'"),
                        "showDefaultTooltip": L("false")}}])
    eq("clear slicers", pb.action_link("clear_slicers"),
       [{"properties": {"show": L("true"), "type": L("'ClearAllSlicers'")}}])
    eq("back", pb.action_link("back"), [{"properties": {"show": L("true"), "type": L("'Back'")}}])
    eq("web address", pb.action_link("url", "https://example.com"),
       [{"properties": {"show": L("true"), "type": L("'WebUrl'"), "webUrl": L("'https://example.com'")}}])
    for bad in (("page", None), ("teleport", "x")):
        try:
            pb.action_link(*bad)
            check(f"action {bad} is refused", False)
        except ValueError:
            check(f"action {bad} is refused", True)


def test_decoration():
    print("\n== textbox, shape, image ==")
    box = pb.textbox("heading", POS, [
        pb.paragraph([pb.text_run("Northwind", font="Georgia", size=19.5, color="#221133", bold=True)]),
        pb.paragraph([pb.text_run("Quarterly review", font="Segoe UI", size=7, color="#667788")],
                     align="right")])
    eq("textbox", box, {
        "$schema": pb.DEFAULT_SCHEMAS["visual"], "name": "heading", "position": POS,
        "visual": {"visualType": "textbox",
                   "objects": {"general": [{"properties": {"paragraphs": [
                       {"textRuns": [{"value": "Northwind", "textStyle": {"fontFamily": "Georgia",
                                                                          "fontSize": "19.5pt",
                                                                         "color": "#221133",
                                                                         "fontWeight": "bold"}}]},
                       {"textRuns": [{"value": "Quarterly review", "textStyle": {"fontFamily": "Segoe UI",
                                                                                 "fontSize": "7pt",
                                                                                "color": "#667788"}}],
                        "horizontalTextAlignment": "right"}]}}]},
                   "visualContainerObjects": PLAIN, "drillFilterOtherVisuals": True}})
    eq("a run with no style has no textStyle", pb.text_run("plain"), {"value": "plain"})

    close = pb.action_link("bookmark", "filters_close", tooltip="Close")
    veil = pb.shape("veil", POS, fill="#101820", transparency=58, link=close, parent_group="filters",
                    schema_url="https://example.test/visualContainer/2.7.0/schema.json")
    eq("shape used as a veil", veil, {
        "$schema": "https://example.test/visualContainer/2.7.0/schema.json", "name": "veil", "position": POS,
        "visual": {"visualType": "shape",
                   "objects": {"fill": [{"properties": {"fillColor": C("#101820"), "transparency": L("58D")},
                                         "selector": {"id": "default"}}],
                               "outline": OFF,
                               "shape": [{"properties": {"tileShape": L("'rectangleRoundedByPixel'"),
                                                         "roundEdge": L("0D")}}]},
                   "visualContainerObjects": {**{k: v for k, v in PLAIN.items() if k != "general"},
                                              "visualLink": close, "general": KEEP},
                   "drillFilterOtherVisuals": True},
        "parentGroupName": "filters"})
    card = pb.shape("card", POS, fill="#FFFFFF", radius=18, outline="#E0E0E0")
    eq("shape outline", card["visual"]["objects"]["outline"],
       [{"properties": {"show": L("true")}},
        {"properties": {"lineColor": C("#E0E0E0"), "weight": L("1D")}, "selector": {"id": "default"}}])
    eq("shape radius", card["visual"]["objects"]["shape"],
       [{"properties": {"tileShape": L("'rectangleRoundedByPixel'"), "roundEdge": L("18D")}}])

    kpi = pb.image("kpi", POS, measure="Figures.KPI Card",
                   container=pb.container_objects(title=False, shadow=False, header=False,
                                                  background="#FFFFFF",
                                                  border={"color": "#EEEEEE", "radius": 8}, padding=0))
    eq("image from a measure", kpi["visual"]["objects"], {"image": [{"properties": {
        "sourceType": L("'imageData'"),
        "sourceField": {"expr": {"Measure": {"Expression": {"SourceRef": {"Entity": "Figures"}},
                                             "Property": "KPI Card"}}},
        "transparency": L("0D")}}]})
    eq("image keeps the frame it was given", kpi["visual"]["visualContainerObjects"]["border"],
       [{"properties": {"show": L("true"), "color": C("#EEEEEE"), "radius": L("8D")}}])
    logo = pb.image("logo", POS, resource="logo.png", scaling="Fill", link=pb.action_link("page", "home"),
                    hidden=True)
    eq("image from a registered file", logo["visual"]["objects"], {
        "general": [{"properties": {"imageUrl": pb.resource_expr("logo.png")}}],
        "imageScaling": [{"properties": {"imageScalingType": L("'Fill'")}}]})
    check("a hidden visual says so", logo.get("isHidden") is True)
    check("the link is in the frame", "visualLink" in logo["visual"]["visualContainerObjects"])
    for kwargs in ({}, {"resource": "a.png", "measure": "T.M"}):
        try:
            pb.image("bad", POS, **kwargs)
            check(f"image with {sorted(kwargs)} is refused", False)
        except ValueError:
            check(f"image with {sorted(kwargs)} is refused", True)


def test_button_and_slicer():
    print("\n== button, slicer ==")
    link = pb.action_link("page", "overview", tooltip="Open Overview")
    tab = pb.action_button("tab0", POS, text="Overview", link=link, font="Segoe UI", size=10, color="#303040",
                           radius=20,
                           states={"hover": {"fill": "#5020B0", "fill_transparency": 88, "color": "#101020"},
                                   "press": {"fill": "#5020B0", "fill_transparency": 80}})
    eq("a tab button", tab["visual"]["objects"], {
        "icon": OFF,
        "fill": [{"properties": {"show": L("true")}},
                 {"properties": {"fillColor": C("#FFFFFF"), "transparency": L("100D")},
                  "selector": {"id": "default"}},
                 {"properties": {"fillColor": C("#5020B0"), "transparency": L("88D")},
                  "selector": {"id": "hover"}},
                 {"properties": {"fillColor": C("#5020B0"), "transparency": L("80D")},
                  "selector": {"id": "press"}}],
        "outline": OFF,
        "shape": [{"properties": {"tileShape": L("'rectangleRounded'")}}] + [
            {"properties": {"roundEdge": L("20D")}, "selector": {"id": s}} for s in ("default", "hover",
                                                                                     "press", "selected")],
        "text": [{"properties": {"show": L("true")}},
                 {"properties": {"text": L("'Overview'"), "fontColor": C("#303040"),
                                 "fontFamily": L("'Segoe UI'"),
                                 "fontSize": L("10D")}, "selector": {"id": "default"}},
                 {"properties": {"fontColor": C("#101020")}, "selector": {"id": "hover"}}]})
    eq("its frame carries the link", tab["visual"]["visualContainerObjects"],
       {**{k: v for k, v in PLAIN.items() if k != "general"}, "visualLink": link, "general": KEEP})
    eq("the button type", tab["visual"]["visualType"], "actionButton")
    hit = pb.action_button("hit", POS, link=link)
    eq("a hit area is transparent and has no text", [hit["visual"]["objects"]["fill"][1],
                                                     hit["visual"]["objects"]["text"]],
       [{"properties": {"fillColor": C("#FFFFFF"), "transparency": L("100D")}, "selector": {"id": "default"}},
        OFF])
    check("a hit area has no shape object", "shape" not in hit["visual"]["objects"])
    solid = pb.action_button("go", POS, text="Go", fill="#102030", outline="#405060", bold=True)
    eq("a filled, outlined button", [solid["visual"]["objects"]["fill"][1],
                                     solid["visual"]["objects"]["outline"]],
       [{"properties": {"fillColor": C("#102030"), "transparency": L("0D")}, "selector": {"id": "default"}},
        [{"properties": {"show": L("true")}},
         {"properties": {"lineColor": C("#405060"), "weight": L("1D")}, "selector": {"id": "default"}}]])
    try:
        pb.action_button("bad", POS, states={"wiggle": {}})
        check("an unknown state is refused", False)
    except ValueError:
        check("an unknown state is refused", True)

    s = pb.slicer("city", POS, "Customers.City", mode="tiles", single_select=False, select_all=False,
                  header={"text": "City", "color": "#667788", "font": "Segoe UI Semibold", "size": 9},
                  items={"color": "#303040", "background": "#F4F0FB", "size": 10, "font": "Segoe UI",
                         "outline": "Frame",
                         "outline_color": "#E6E1F0"}, sync_group="customers_city", parent_group="filters")
    eq("a tile slicer", s, {
        "$schema": pb.DEFAULT_SCHEMAS["visual"], "name": "city", "position": POS,
        "visual": {
            "visualType": "slicer",
            "query": {"queryState": {"Values": {"projections": [{
                "field": {"Column": {"Expression": {"SourceRef": {"Entity": "Customers"}},
                                     "Property": "City"}},
                "queryRef": "Customers.City", "nativeQueryRef": "City", "active": True}]}}},
            "objects": {
                "data": [{"properties": {"mode": L("'Basic'")}}],
                "header": [{"properties": {"show": L("true"), "text": L("'City'"), "fontColor": C("#667788"),
                                           "fontFamily": L("'Segoe UI Semibold'"), "textSize": L("9D")}}],
                "items": [{"properties": {"fontColor": C("#303040"), "background": C("#F4F0FB"),
                                          "textSize": L("10D"),
                                          "fontFamily": L("'Segoe UI'"), "outline": L("'Frame'"),
                                          "outlineColor": C("#E6E1F0")}}],
                "selection": [{"properties": {"selectAllCheckboxEnabled": L("false"),
                                              "singleSelect": L("false")}}],
                "general": [{"properties": {"orientation": L("1D")}}]},
            "visualContainerObjects": PLAIN,
            "syncGroup": {"groupName": "customers_city", "fieldChanges": True, "filterChanges": True},
            "drillFilterOtherVisuals": True},
        "parentGroupName": "filters"})
    modes = {m: pb.slicer("s", POS, "T.C", mode=m)["visual"]["objects"]["data"][0]["properties"]["mode"]
             for m in ("list", "dropdown", "between")}
    eq("slicer modes", modes, {"list": L("'Basic'"), "dropdown": L("'Dropdown'"), "between": L("'Between'")})
    check("only tiles are laid out sideways", "general" not in pb.slicer("s", POS, "T.C",
                                                                         mode="list")["visual"]["objects"])
    eq("strict single select", pb.slicer("s", POS, "T.C", single_select=True, strict_single_select=True)
       ["visual"]["objects"]["selection"],
       [{"properties": {"singleSelect": L("true"), "strictSingleSelect": L("true")}}])
    try:
        pb.slicer("s", POS, "T.C", mode="wheel")
        check("an unknown mode is refused", False)
    except ValueError:
        check("an unknown mode is refused", True)


def test_data_visuals():
    print("\n== HTML, Deneb, data visuals ==")
    html = pb.html_visual("chips", POS, {"table": "_measures", "measure": "HTML Summary"})
    eq("HTML Content", html["visual"], {
        "visualType": pb.HTML_CONTENT_ID,
        "query": {"queryState": {"content": {"projections": [{
            "field": {"Measure": {"Expression": {"SourceRef": {"Entity": "_measures"}},
                                  "Property": "HTML Summary"}},
            "queryRef": "_measures.HTML Summary", "nativeQueryRef": "HTML Summary"}]}}},
        "visualContainerObjects": PLAIN, "drillFilterOtherVisuals": True})
    eq("the other HTML Content id", pb.html_visual("h", POS, "T.M",
                                                  visual_type=pb.HTML_CONTENT_ID_ALT)["visual"]["visualType"],
       pb.HTML_CONTENT_ID_ALT)

    spec = {"data": {"name": "dataset"}, "mark": "bar", "encoding": {"x": {"field": "Category"},
                                                                    "tooltip": {"value": "it's here"}}}
    deneb = pb.deneb_visual("bars", POS, ["Sales.Category", {"measure": "Sales.Share"}], spec,
                            config={"view": {"stroke": "transparent"}}, selection=True,
                            container=pb.container_objects(title="Share by category"))
    vega = deneb["visual"]["objects"]["vega"][0]["properties"]
    eq("Deneb type", deneb["visual"]["visualType"], pb.DENEB_ID)
    eq("Deneb settings", {k: v for k, v in vega.items() if k not in ("jsonSpec", "jsonConfig")},
       {"provider": L("'vegaLite'"), "version": L(f"'{pb.DENEB_VEGA_LITE_VERSION}'"),
        "enableTooltips": L("true"),
        "enableSelection": L("true"), "selectionMode": L("'simple'"), "selectionMaxDataPoints": L("50D")})
    text = vega["jsonSpec"]["expr"]["Literal"]["Value"]
    check("the spec is one compact literal",
          text.startswith("'{\"data\":{\"name\":\"dataset\"}") and text.endswith("}'"))
    check("an apostrophe in the spec is doubled", "it''s here" in text)
    eq("the spec survives the round trip", json.loads(text[1:-1].replace("''", "'")), spec)
    eq("the config too", vega["jsonConfig"], L("'{\"view\":{\"stroke\":\"transparent\"}}'"))
    eq("Deneb fields are never active", deneb["visual"]["query"]["queryState"]["dataset"]["projections"], [
        {"field": {"Column": {"Expression": {"SourceRef": {"Entity": "Sales"}}, "Property": "Category"}},
         "queryRef": "Sales.Category", "nativeQueryRef": "Category"},
        {"field": {"Measure": {"Expression": {"SourceRef": {"Entity": "Sales"}}, "Property": "Share"}},
         "queryRef": "Sales.Share", "nativeQueryRef": "Share"}])
    eq("a spec given as text is kept as it is", pb.deneb_visual("d", POS, ["T.C"], '{"mark":"line"}')
       ["visual"]["objects"]["vega"][0]["properties"]["jsonSpec"], L("'{\"mark\":\"line\"}'"))
    try:
        pb.deneb_visual("d", POS, ["T.C"], {}, provider="vega")
        check("provider vega needs a version", False)
    except ValueError:
        check("provider vega needs a version", True)

    chart = pb.data_visual("chart", "barChart", POS, {
        "Category": ["Calendar.Month", "Calendar.Day"], "Series": "Product.Family",
        "Y": [{"measure": "Sales.Total"}, {"table": "Sales", "column": "Amount", "aggregation": 0}]},
        objects=pb.simple_objects({"legend": {"show": False}}))
    qs = chart["visual"]["query"]["queryState"]
    eq("only the first Category column is active",
       [[p.get("active", False) for p in qs[r]["projections"]] for r in ("Category", "Series", "Y")],
       [[True, False], [False], [False, False]])
    eq("an aggregated column", qs["Y"]["projections"][1], {
        "field": {"Aggregation": {"Expression": {"Column": {"Expression": {"SourceRef": {"Entity": "Sales"}},
                                                            "Property": "Amount"}}, "Function": 0}},
        "queryRef": "Sum(Sales.Amount)", "nativeQueryRef": "Amount"})
    eq("a plain string in a value role is a measure",
       list(pb.query_state({"Y": "Sales.Total"})["Y"]["projections"][0]["field"]),
       ["Measure"])
    eq("a data visual leaves its frame to the theme", chart["visual"]["visualContainerObjects"],
       {"general": KEEP})
    eq("its formatting", chart["visual"]["objects"], {"legend": [{"properties": {"show": L("false")}}]})
    table = pb.data_visual("t", "tableEx", POS, {"Values": [{"column": "Customer.Name"},
                                                            {"measure": "Sales.Total"}]})
    check("table columns are never active",
          not any("active" in p for p in table["visual"]["query"]["queryState"]["Values"]["projections"]))
    custom = pb.data_visual("home", "homeVisualABCDEF", POS, {"payload": {"measure": "Figures.Payload"},
                                                              "Category": {"column": "T.C"}},
                                                              mark_active=False,
                            container=pb.container_objects(**pb.PLAIN_CHROME, alt_text="Home"))
    check("a custom visual's fields are not active when told so",
          not any("active" in p for r in custom["visual"]["query"]["queryState"].values()
                  for p in r["projections"]))
    eq("a custom visual's type is its guid", custom["visual"]["visualType"], "homeVisualABCDEF")


def test_group_page_bookmark():
    print("\n== group, page, bookmark ==")
    eq("a hidden full-page group", pb.visual_group("filters", pb.position(0, 0, 1600, 1000, 50000),
                                                   "Filters pop-up",
                                                   hidden=True),
       {"$schema": pb.DEFAULT_SCHEMAS["visual"], "name": "filters",
        "position": {"x": 0, "y": 0, "z": 50000, "width": 1600, "height": 1000, "tabOrder": 50000},
        "visualGroup": {"displayName": "Filters pop-up", "groupMode": "ScaleMode",
                        "objects": {"background": OFF}},
        "isHidden": True})

    eq("a plain page", pb.page("overview", "Overview"), {
        "$schema": pb.DEFAULT_SCHEMAS["page"], "name": "overview", "displayName": "Overview",
        "displayOption": "FitToPage", "height": 720, "width": 1280})
    eq("a page on an image, with a wallpaper", pb.page("card", "Card", 1600, 1000,
                                                       background={"image": "ground.png"},
                                                       wallpaper="#ECECEC"), {
        "$schema": pb.DEFAULT_SCHEMAS["page"], "name": "card", "displayName": "Card",
        "displayOption": "FitToPage",
        "height": 1000, "width": 1600,
        "objects": {
            "background": [{"properties": {
                "image": {"image": {"name": L("'ground.png'"), "url": pb.resource_expr("ground.png"),
                                    "scaling": L("'Fill'")}},
                "transparency": L("0D")}}],
            "outspace": [{"properties": {"color": C("#ECECEC"), "transparency": L("0D")}}]}})
    coloured = pb.page("p", "P", background="#FAFAFA", vertical_alignment="Middle", hidden=True,
                       display_option="FitToWidth")
    eq("a coloured, hidden page", [coloured["objects"], coloured["visibility"], coloured["displayOption"]],
       [{"displayArea": [{"properties": {"verticalAlignment": L("'Middle'")}}],
         "background": [{"properties": {"color": C("#FAFAFA"), "transparency": L("0D")}}]},
         "HiddenInViewMode", "FitToWidth"])
    tip = pb.page("tip", "Tip", 400, 300, tooltip=True)
    eq("a tooltip page", [tip["type"], tip["visibility"]], ["Tooltip", "HiddenInViewMode"])

    eq("a show/hide bookmark", pb.visibility_bookmark("filters_open", "Card: filters open", "card",
                                                      groups={"filters": False}), {
        "$schema": pb.DEFAULT_SCHEMAS["bookmark"], "displayName": "Card: filters open",
        "name": "filters_open",
        "options": {"applyOnlyToTargetVisuals": True, "targetVisualNames": ["filters"],
                    "suppressActiveSection": True,
                    "suppressData": True},
        "explorationState": {"version": "1.0", "activeSection": "card", "sections": {"card": {
            "visualContainers": {}, "visualContainerGroups": {"filters": {"isHidden": False}}}}}})
    single = pb.visibility_bookmark("hide_note", "Hide note", "card",
                                    visuals={"note": {"visual_type": "textbox", "hidden": True},
                                             "tip": {"visual_type": "textbox", "hidden": False}})
    eq("single visuals", single["explorationState"]["sections"]["card"], {"visualContainers": {
        "note": {"singleVisual": {"visualType": "textbox", "display": {"mode": "hidden"}}},
        "tip": {"singleVisual": {"visualType": "textbox"}}}})
    eq("their names are the targets", single["options"]["targetVisualNames"], ["note", "tip"])
    plain = pb.bookmark("start", "Start", "card", groups={"filters": True})
    eq("an ordinary bookmark leaves data alone", plain["options"], {"targetVisualNames": [],
                                                                    "suppressData": True})
    eq("and opens its page", plain["explorationState"]["activeSection"], "card")
    eq("bookmarks.json", pb.bookmarks_metadata(["a", "b"]),
       {"$schema": pb.DEFAULT_SCHEMAS["bookmarksMetadata"], "items": [{"name": "a"}, {"name": "b"}]})
    eq("version.json", pb.version_metadata(), {"$schema": pb.DEFAULT_SCHEMAS["version"], "version": "2.0.0"})


def test_theme_and_report():
    print("\n== theme, report.json ==")
    theme = pb.theme("House", colors={"data": ["#111111", "#222222"], "background": "#FFFFFF",
                                      "page": "#F5F5F5",
                                      "wallpaper": "#EEEEEE", "foreground": "#101010", "muted": "#606060",
                                      "border": "#DDDDDD", "accent": "#0055AA", "good": "#007700",
                                      "bad": "#AA0000"},
                     fonts={"title": {"family": "Segoe UI Semibold", "size": 11, "color": "#101010"},
                            "callout": {"family": "Georgia", "size": 26}},
                     card={"radius": 16, "padding": 14},
                     extra={"visualStyles": {"slicer": {"*": {"items": [{"textSize": 10}]}}}})
    eq("theme colours", {k: theme[k] for k in ("name", "dataColors", "background", "backgroundLight",
                                               "backgroundNeutral",
                                               "foreground", "foregroundNeutralSecondary", "tableAccent",
                                               "hyperlink",
                                               "good", "bad")},
       {"name": "House", "dataColors": ["#111111", "#222222"], "background": "#FFFFFF",
        "backgroundLight": "#F5F5F5",
        "backgroundNeutral": "#DDDDDD", "foreground": "#101010", "foregroundNeutralSecondary": "#606060",
        "tableAccent": "#0055AA", "hyperlink": "#0055AA", "good": "#007700", "bad": "#AA0000"})
    eq("theme text classes", theme["textClasses"], {"callout": {"fontFace": "Georgia", "fontSize": 26},
                                                    "title": {"fontFace": "Segoe UI Semibold", "fontSize": 11,
                                                              "color": "#101010"}})
    eq("the frame every visual gets", theme["visualStyles"]["*"]["*"], {
        "background": [{"show": True, "color": {"solid": {"color": "#FFFFFF"}}, "transparency": 0}],
        "border": [{"show": True, "color": {"solid": {"color": "#DDDDDD"}}, "radius": 16}],
        "dropShadow": [{"show": False}],
        "general": [{"keepLayerOrder": True}],
        "padding": [{"top": 14, "bottom": 14, "left": 14, "right": 14}],
        "title": [{"show": True, "fontFamily": "Segoe UI Semibold", "fontSize": 11,
                   "fontColor": {"solid": {"color": "#101010"}}}],
        "visualHeader": [{"show": False}]})
    eq("the page style", theme["visualStyles"]["page"], {"*": {
        "background": [{"color": {"solid": {"color": "#F5F5F5"}}, "transparency": 0}],
        "outspace": [{"color": {"solid": {"color": "#EEEEEE"}}, "transparency": 0}]}})
    check("text has no frame", theme["visualStyles"]["textbox"]["*"]["background"] == [{"show": False}])
    check("extra is merged without losing what was there",
          theme["visualStyles"]["slicer"]["*"]["items"] == [{"textSize": 10}]
          and theme["visualStyles"]["slicer"]["*"]["border"] == [{"show": False}])
    eq("a theme can be almost empty", pb.theme("Bare", plain_types=())["visualStyles"], {"*": {"*": {
        "border": [{"show": False}], "dropShadow": [{"show": False}], "general": [{"keepLayerOrder": True}],
        "title": [{"show": True}], "visualHeader": [{"show": False}]}}})

    base = {"$schema": "https://example.test/report/3.3.0/schema.json",
            "themeCollection": {"baseTheme": {"name": "CY24SU06", "type": "SharedResources",
                                              "reportVersionAtImport": {"visual": "1.8.1", "report": "2.0.1",
                                                                        "page": "1.3.1"}}},
            "resourcePackages": [{"name": "SharedResources", "type": "SharedResources",
                                  "items": [{"name": "CY24SU06", "path": "BaseThemes/CY24SU06.json",
                                             "type": "BaseTheme"}]}],
            "settings": {"exportDataMode": "AllowSummarized"}}
    before = copy.deepcopy(base)
    out = pb.set_custom_theme(base, "house.json")
    eq("the input report is not changed", base, before)
    eq("the custom theme takes the base theme's versions", out["themeCollection"]["customTheme"],
       {"name": "house.json", "reportVersionAtImport": {"visual": "1.8.1", "report": "2.0.1",
                                                        "page": "1.3.1"},
        "type": "RegisteredResources"})
    eq("and is registered as a resource", out["resourcePackages"][1],
       {"name": "RegisteredResources", "type": "RegisteredResources",
        "items": [{"name": "house.json", "path": "house.json", "type": "CustomTheme"}]})
    eq("registering twice changes nothing", pb.set_custom_theme(out, "house.json"), out)
    eq("with no base theme a fixed triple is used", pb.set_custom_theme(pb.new_report(), "t.json")
       ["themeCollection"]["customTheme"]["reportVersionAtImport"], pb.THEME_VERSION_AT_IMPORT)
    out = pb.register_resource(out, "ground.png", "Image")
    eq("an image beside the theme", out["resourcePackages"][1]["items"][1],
       {"name": "ground.png", "path": "ground.png", "type": "Image"})
    eq("unregistering removes just that file", [i["name"] for i in pb.unregister_resource(out, "house.json")
                                                 ["resourcePackages"][1]["items"]], ["ground.png"])
    out = pb.register_public_visual(pb.register_public_visual(out, pb.DENEB_ID), pb.DENEB_ID)
    eq("an AppSource visual is listed once", out["publicCustomVisuals"], [pb.DENEB_ID])
    eq("and can be taken out", pb.unregister_public_visual(out, pb.DENEB_ID)["publicCustomVisuals"], [])
    out = pb.register_custom_visual(out, "homeVisualABCDEF")
    eq("a private visual's package entry", out["resourcePackages"][-1], {
        "name": "homeVisualABCDEF", "type": "CustomVisual",
        "items": [{"name": "homeVisualABCDEF.pbiviz.json", "path": "homeVisualABCDEF.pbiviz.json",
                   "type": "CustomVisualMetadata"}]})
    eq("removing it leaves the others", [p["name"] for p in pb.unregister_custom_visual(out,
                                                                                        "homeVisualABCDEF")
                                         ["resourcePackages"]], ["SharedResources", "RegisteredResources"])
    eq("settings are kept", out["settings"], {"exportDataMode": "AllowSummarized"})
    eq("the smallest report.json", pb.new_report(), {"$schema": pb.DEFAULT_SCHEMAS["report"],
                                                     "themeCollection": {}})


def _pbiviz(path, guid="homeVisualABCDEF", members=None):
    package = {"version": "1.0.0.0", "visual": {"name": "homeVisual", "guid": guid, "version": "1.0.0.0"},
               "resources": [{"resourceId": "rId0", "sourceType": 5,
                              "file": f"resources/{guid}.pbiviz.json"}]}
    with zipfile.ZipFile(path, "w") as z:
        z.writestr("package.json", json.dumps(package, indent="\t"))
        z.writestr(f"resources/{guid}.pbiviz.json", json.dumps({"visual": package["visual"],
                                                                "content": {"js": "x"}}))
        for name, data in (members or {}).items():
            z.writestr(name, data)


def test_packages():
    print("\n== custom visual packages ==")
    with tempfile.TemporaryDirectory() as tmp:
        good = os.path.join(tmp, "home.pbiviz")
        _pbiviz(good)
        pkg = pbir_packages.read_pbiviz(good)
        eq("guid and version", [pkg["guid"], pkg["version"]], ["homeVisualABCDEF", "1.0.0.0"])
        eq("members", sorted(pkg["files"]), ["package.json", "resources/homeVisualABCDEF.pbiviz.json"])
        eq("where they go", sorted(pbir_packages.custom_visual_files(pkg["guid"], pkg["files"])),
           ["CustomVisuals/homeVisualABCDEF/package.json",
            "CustomVisuals/homeVisualABCDEF/resources/homeVisualABCDEF.pbiviz.json"])
        cases = {}
        evil = os.path.join(tmp, "evil.pbiviz")
        _pbiviz(evil, members={"../outside.txt": "x"})
        cases["a member outside the folder"] = evil
        notzip = os.path.join(tmp, "notzip.pbiviz")
        io.open(notzip, "w", encoding="utf-8").write("not a zip")
        cases["a file that is not a zip"] = notzip
        nopkg = os.path.join(tmp, "nopkg.pbiviz")
        with zipfile.ZipFile(nopkg, "w") as z:
            z.writestr("readme.txt", "x")
        cases["a zip with no package.json"] = nopkg
        nometa = os.path.join(tmp, "nometa.pbiviz")
        with zipfile.ZipFile(nometa, "w") as z:
            z.writestr("package.json", json.dumps({"visual": {"guid": "abc"}}))
        cases["a package with no metadata file"] = nometa
        badguid = os.path.join(tmp, "badguid.pbiviz")
        _pbiviz(badguid, guid="..")
        cases["a guid that is not a folder name"] = badguid
        cases["a missing file"] = os.path.join(tmp, "absent.pbiviz")
        for label, path in cases.items():
            try:
                pbir_packages.read_pbiviz(path)
                check(f"{label} is refused", False)
            except pbir_packages.PbivizError:
                check(f"{label} is refused", True)
        root = os.path.join(tmp, "R")
        short, long_ = "CustomVisuals/a/b.json", "CustomVisuals/" + "x" * 300 + "/b.json"
        eq("only the long path is reported", pbir_packages.path_length_problems(root, [short, long_]),
           [long_])
        eq("the limit can be set", pbir_packages.path_length_problems(root, [short], limit=5), [short])


def test_native_features():
    print("\n== drillthrough, tooltip pages, interactions, the phone layout, bookmark groups ==")
    city = {"Column": {"Expression": {"SourceRef": {"Entity": "Customer"}}, "Property": "City"}}
    binding = pb.page_binding("bind1", "Drillthrough", [{"filter": "f1", "expr": city, "name": "p1"}])
    parameter = {"name": "p1", "boundFilter": "f1", "asAggregation": False, "qnaSingleSelectRequired": False,
                 "fieldExpr": city}
    eq("a drillthrough binding points each field at its filter, the way Desktop saves one", binding,
       {"name": "bind1", "type": "Drillthrough", "parameters": [parameter]})
    eq("cross-report, and without the source's other filters",
       pb.page_binding("b", "Drillthrough", [{"filter": "f1", "expr": city}], cross_report=True,
                       keep_all_filters=False),
       {"name": "b", "type": "Drillthrough", "referenceScope": "CrossReport",
        "parameters": [dict(parameter, name="Param_f1")], "acceptsFilterContext": "None"})

    # a binding someone added a field to in Desktop
    theirs = {"name": "hand", "boundFilter": "desk", "asAggregation": False, "fieldExpr": city}
    on_disk = {"name": "bind1", "type": "Drillthrough", "parameters": [theirs, parameter]}
    eq("a parameter bound by hand stays where it is", pb.merge_binding(on_disk, binding, ["f1"]), on_disk)
    changed = pb.page_binding("bind1", "Drillthrough", [{"filter": "f2", "expr": city, "name": "p2"}],
                              keep_all_filters=False)
    eq("ours are replaced: the one the spec dropped goes, the new one is added, theirs stays",
       pb.merge_binding(on_disk, changed, ["f1"]),
       {"name": "bind1", "type": "Drillthrough",
        "parameters": [theirs, dict(parameter, name="p2", boundFilter="f2")], "acceptsFilterContext": "None"})
    eq("when the spec declares no binding any more, a hand-bound field keeps the binding alive",
       pb.merge_binding(on_disk, None, ["f1"]),
       {"name": "bind1", "type": "Drillthrough", "parameters": [theirs]})
    eq("and with only ours in it, the binding goes", pb.merge_binding(binding, None, ["F1"]), None)
    eq("a page that had no binding takes ours", pb.merge_binding(None, binding), binding)
    eq("a binding with no parameters left has no parameter list",
       pb.merge_binding(binding, pb.page_binding("t", "Tooltip"), ["f1"]), {"name": "t", "type": "Tooltip"})
    kept = pb.merge_binding(on_disk, binding, ["f1"])
    kept["parameters"][1]["fieldExpr"]["Column"]["Property"] = "changed"
    eq("the result shares nothing with the binding it was given",
       binding["parameters"][0]["fieldExpr"]["Column"]["Property"], "City")
    eq("a tooltip binding with no fields", pb.page_binding("t", "Tooltip"), {"name": "t", "type": "Tooltip"})
    try:
        pb.page_binding("x", "Popup")
        check("an unknown binding type is refused", False)
    except ValueError:
        check("an unknown binding type is refused", True)
    drill = pb.page("detail", "Detail", drillthrough=True, binding=binding)
    eq("a drillthrough page", [drill["type"], drill["pageBinding"], "visibility" in drill],
       ["Drillthrough", binding, False])
    binding["parameters"].clear()
    eq("the page keeps its own copy of the binding", len(drill["pageBinding"]["parameters"]), 1)
    tip = pb.page("tip", "Tip", 320, 240, tooltip=True, binding=pb.page_binding("t", "Tooltip"))
    eq("a tooltip page with its binding", [tip["type"], tip["visibility"], tip["pageBinding"]],
       ["Tooltip", "HiddenInViewMode", {"name": "t", "type": "Tooltip"}])
    check("an ordinary page has neither", not {"type", "pageBinding"} & set(pb.page("p", "P")))
    try:
        pb.page("p", "P", tooltip=True, drillthrough=True)
        check("a page cannot be both", False)
    except ValueError:
        check("a page cannot be both", True)

    # show and section only, as in visuals Desktop saved; with a "type" of 'ReportPage' Power BI
    # was seen to fall back to the default tooltip
    eq("a visual that shows a tooltip page", pb.container_objects(tooltip_page="tip")["visualTooltip"],
       [{"properties": {"show": L("true"), "section": L("'tip'")}}])
    eq("with tooltip: true as well", pb.container_objects(tooltip=True, tooltip_page="tip")["visualTooltip"],
       [{"properties": {"show": L("true"), "section": L("'tip'")}}])
    check("no tooltip type is written",
          "ReportPage" not in json.dumps(pb.container_objects(tooltip_page="tip")))
    try:
        pb.container_objects(tooltip=False, tooltip_page="tip")
        check("a tooltip page on a visual with tooltips off is refused", False)
    except ValueError:
        check("a tooltip page on a visual with tooltips off is refused", True)

    eq("a drillthrough button", pb.action_link("drillthrough", "detail", tooltip="Open detail",
                                               disabled_tooltip="Pick a city first"),
       [{"properties": {"show": L("true"), "type": L("'Drillthrough'"), "drillthroughSection": L("'detail'"),
                        "enabledTooltip": L("'Open detail'"), "disabledTooltip": L("'Pick a city first'")}}])
    try:
        pb.action_link("drillthrough")
        check("a drillthrough action needs its page", False)
    except ValueError:
        check("a drillthrough action needs its page", True)
    check("other actions keep their single tooltip",
          "enabledTooltip" not in pb.action_link("page", "p", tooltip="Go")[0]["properties"])

    eq("interactions", [pb.visual_interaction("a", "b", k) for k in ("filter", "highlight", "none")],
       [{"source": "a", "target": "b", "type": "DataFilter"},
        {"source": "a", "target": "b", "type": "HighlightFilter"},
        {"source": "a", "target": "b", "type": "NoFilter"}])
    try:
        pb.visual_interaction("a", "b", "Filter")
        check("an unknown interaction is refused", False)
    except ValueError:
        check("an unknown interaction is refused", True)

    eq("a phone position", pb.mobile_state(pb.position(0, 120, 323, 200, 7)),
       {"$schema": pb.DEFAULT_SCHEMAS["mobile"],
        "position": {"x": 0, "y": 120, "z": 7, "width": 323, "height": 200, "tabOrder": 7}})
    check("the mobile schema is its own", "visualContainerMobileState" in pb.DEFAULT_SCHEMAS["mobile"])
    eq("with the project's schema", pb.mobile_state(POS, "https://example.test/m.json")["$schema"],
       "https://example.test/m.json")
    eq("a bookmark group", pb.bookmark_group("views", "Views", ("a", "b")),
       {"name": "views", "displayName": "Views", "children": ["a", "b"]})

    eq("Deneb's version literal is the one Deneb 2.0 writes", pb.DENEB_VEGA_LITE_VERSION, "6.4.3")
    for bad in (0, 251, 2.5, True, "50", float("nan")):
        try:
            pb.deneb_visual("d", POS, ["T.C"], {}, max_selected=bad)
            check(f"max_selected {bad!r} is refused", False)
        except ValueError as e:
            check(f"max_selected {bad!r} is refused", "from 1 to 250" in str(e), str(e))
    eq("the ends of the range pass",
       [pb.deneb_visual("d", POS, ["T.C"], {}, max_selected=n)["visual"]["objects"]["vega"][0]["properties"]
        ["selectionMaxDataPoints"] for n in (1, 250)], [L("1D"), L("250D")])
    deneb = pb.deneb_visual("d", POS, ["T.C"], {}, selection=True, cross_highlight=True, context_menu=False)
    eq("Deneb's own switches for the context menu and cross-highlight",
       [k for k in deneb["visual"]["objects"]["vega"][0]["properties"] if k.startswith("enable")],
       ["enableTooltips", "enableContextMenu", "enableHighlight", "enableSelection"])
    vega = deneb["visual"]["objects"]["vega"][0]["properties"]
    eq("and their values", [vega["enableContextMenu"], vega["enableHighlight"]], [L("false"), L("true")])
    def active(doc, role):
        return [p.get("active", False) for p in doc["visual"]["query"]["queryState"][role]["projections"]]

    levels = {"category": [{"column": "T.A"}, {"column": "T.B"}, {"column": "T.C"}],
              "value": [{"measure": "T.M"}]}
    custom = pb.data_visual("v", "myVisual123", POS, levels, mark_active=False, drill_roles=["category"])
    eq("a custom visual's drill role with several fields starts on the first",
       [active(custom, "category"), active(custom, "value")], [[True, False, False], [False]])
    plain = pb.data_visual("v", "myVisual123", POS, levels, mark_active=False)
    eq("without the role named as one it drills on, nothing is marked", active(plain, "category"),
       [False, False, False])
    single = pb.data_visual("v", "myVisual123", POS, {"category": [{"column": "T.A"}]}, mark_active=False,
                            drill_roles=["category"])
    eq("one field needs no mark", active(single, "category"), [False])
    chosen = pb.data_visual("v", "myVisual123", POS, {"category": [
        {"column": "T.A", "active": True}, {"column": "T.B", "active": True}, {"column": "T.C"}]},
        mark_active=False, drill_roles=["category"])
    eq("a field can say for itself whether it is active", active(chosen, "category"), [True, True, False])
    native = pb.data_visual("v", "barChart", POS, {"Category": [{"column": "T.A", "active": False},
                                                                {"column": "T.B", "active": True}]})
    eq("on a native visual too, in place of the first-field rule", active(native, "Category"), [False, True])
    eq("a field that says nothing leaves the rule alone",
       active(pb.data_visual("v", "barChart", POS, {"Category": [{"column": "T.A"}, {"column": "T.B"}]}),
              "Category"), [True, False])
    written = chosen["visual"]["query"]["queryState"]["category"]["projections"]
    check("no stray key reaches the projection",
          all(set(p) <= {"field", "queryRef", "nativeQueryRef", "active"} for p in written))

    eq("the two editions of HTML Content", pb.HTML_CONTENT_EDITIONS,
       {"regular": "htmlContent443BE3AD55E043BF878BED274D3A6855",
        "secure": "htmlContent443BE3AD55E043BF878BED274D3A6865"})
    eq("the default is still the regular one", pb.HTML_CONTENT_ID, pb.HTML_CONTENT_EDITIONS["regular"])
    eq("the phone canvas is 323 wide", pb.PHONE_WIDTH, 323)
    for bad in (float("nan"), float("inf")):
        try:
            pb.lit_num(bad)
            check(f"{bad!r} is not written as a number", False)
        except ValueError:
            check(f"{bad!r} is not written as a number", True)
    quiet = pb.data_visual("v", "barChart", POS, {"Category": "T.C"}, drill_filter_other_visuals=False)
    eq("a visual that a drill elsewhere does not filter",
       [quiet["visual"]["drillFilterOtherVisuals"],
        pb.data_visual("v", "barChart", POS, {"Category": "T.C"})["visual"]["drillFilterOtherVisuals"]],
       [False, True])


def test_purity():
    print("\n== emitters do not share state ==")
    frame = pb.container_objects(**pb.PLAIN_CHROME)
    before = copy.deepcopy(frame)
    a = pb.shape("a", POS, link=pb.action_link("back"), container=frame)
    eq("the caller's frame is not changed", frame, before)
    b = pb.shape("b", POS, container=frame)
    a["visual"]["visualContainerObjects"]["title"][0]["properties"]["show"] = "changed"
    check("two visuals built from one frame are independent",
          b["visual"]["visualContainerObjects"]["title"] == OFF)
    eq("PLAIN_CHROME is still what it was", pb.PLAIN_CHROME, {"title": False, "background": False,
                                                              "border": False,
                                                             "shadow": False, "header": False, "padding": 0})
    for doc in (a, b, pb.page("p", "P"), pb.theme("T"), pb.bookmark("x", "X", "p")):
        json.dumps(doc)
    check("every document is JSON-serialisable", True)


if __name__ == "__main__":
    for fn in (test_literals, test_objects, test_decoration, test_button_and_slicer, test_data_visuals,
               test_group_page_bookmark, test_theme_and_report, test_packages, test_native_features,
               test_purity):
        fn()
    print()
    if _failures:
        print(f"FAILED: {len(_failures)} check(s): {_failures}")
        sys.exit(1)
    print("ALL PBIR BUILDER TESTS PASSED")
