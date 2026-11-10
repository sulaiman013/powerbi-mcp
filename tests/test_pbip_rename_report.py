"""
PBIP rename, report side (PBIR): references are rewritten structurally in every report file
that can hold one (visuals, mobile layouts, page and report filters, bookmarks, report-level
measures, and the JSON documents the legacy report.json keeps inside strings), and field names
inside Deneb / Vega specs follow the fields they name.

The fixtures use the real PBIR shapes: Property beside Expression on a Column / Measure node,
Entity under Expression.SourceRef, filters that reach the table through a From alias,
Aggregation and HierarchyLevel wrappers, selector metadata, sort definitions.
No Power BI required. Run: python tests/test_pbip_rename_report.py
"""
import json
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import pbip_fixtures as fx  # noqa: E402

_failures = []


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f": {detail}" if detail and not cond else ""))
    if not cond:
        _failures.append(name)


def eq(name, actual, expected):
    check(name, actual == expected, f"got {actual!r}, expected {expected!r}")


def warned(result, *words):
    warnings = result.warnings if hasattr(result, "warnings") else result.get("warnings", [])
    return any(all(w in warning for w in words) for warning in warnings)


def projections(visual, role):
    return visual["visual"]["query"]["queryState"][role]["projections"]


def measure_filter(name, entity, prop, alias):
    """An advanced filter on a measure, compared through a From alias."""
    return {
        "name": name, "field": fx.measure(entity, prop), "type": "Advanced",
        "filter": {"Version": 2, "From": [{"Name": alias, "Entity": entity, "Type": 0}],
                   "Where": [{"Condition": {"Comparison": {
                       "ComparisonKind": 1,
                       "Left": {"Measure": {"Expression": {"SourceRef": {"Source": alias}}, "Property": prop}},
                       "Right": {"Literal": {"Value": "0D"}}}}}]},
    }


def chart():
    return fx.visual("chart", "clusteredColumnChart", {
        "Category": [fx.projection(fx.column("Customers", "Region"), "Customers.Region", "Region", active=True)],
        "Y": [fx.projection(fx.aggregation("Sales", "Revenue"), "Sum(Sales.Revenue)", "Sum of Revenue"),
              fx.projection(fx.measure("_Measures", "Revenue"), "_Measures.Revenue", "Revenue"),
              fx.projection(fx.measure("_Measures", "Rev x2"), "_Measures.Rev x2", display="Doubled")],
        "Tooltips": [fx.projection(fx.column("Sales", "Revenue"), "Sales.Revenue", "Revenue")],
    }, objects={
        "dataPoint": [
            {"properties": {"fill": {"solid": {"color": fx.literal("#118DFF")}}},
             "selector": {"metadata": "_Measures.Revenue"}},
            {"properties": {"fill": {"solid": {"color": fx.literal("#12239E")}}},
             "selector": {"metadata": "Sum(Sales.Revenue)"}},
        ],
        "labels": [{"properties": {"color": {"solid": {"color": {"expr": {"FillRule": {
            "Input": fx.measure("_Measures", "Revenue"),
            "FillRule": {"linearGradient2": {"min": {"color": fx.literal("#fff")},
                                             "max": {"color": fx.literal("#000")}}}}}}}}}}],
        "title": [{"properties": {"text": fx.literal("Revenue by region")}}],
    }, filters=[fx.categorical_filter("f1", "Sales", "Region", "s", ["East"]),
                measure_filter("f2", "_Measures", "Revenue", "m")],
        sort=[{"field": fx.measure("_Measures", "Revenue"), "direction": "Descending"}])


def grid():
    return fx.visual("grid", "tableEx", {"Values": [
        fx.projection(fx.column("Sales", "ID"), "Sales.ID", "ID"),
        fx.projection(fx.column("Customers", "ID"), "Customers.ID", "ID"),
        fx.projection(fx.hierarchy_level("Customers", "Geography", "Region"), "Customers.Geography.Region"),
        fx.projection(fx.variation_level("Sales", "Order Date", "Year"),
                      "Sales.Order Date.Variation.Date Hierarchy.Year"),
        fx.projection({"Measure": {"Expression": {"SourceRef": {"Schema": "extension", "Entity": "Sales"}},
                                   "Property": "Report Margin"}}, "Sales.Report Margin"),
    ]}, objects={"columnWidth": [
        {"properties": {"value": {"expr": {"Literal": {"Value": "120D"}}}}, "selector": {"metadata": "Sales.ID"}},
        {"properties": {"value": {"expr": {"Literal": {"Value": "80D"}}}}, "selector": {"metadata": "Customers.ID"}},
    ]})


def report_files():
    flt = [fx.categorical_filter("pf1", "Sales", "Revenue", "s", ["1"]),
           fx.categorical_filter("pf2", "Customers", "Region", "c", ["East"])]
    bookmark = {
        "$schema": "bookmark/1.4.0", "displayName": "East only", "name": "Bm1",
        "explorationState": {
            "version": "1.3", "activeSection": "overview",
            "filters": {"byExpr": [{"name": "bf1", "type": "Categorical",
                                    "expression": fx.column("Sales", "Region"),
                                    "filter": fx.categorical_filter("x", "Sales", "Region", "s", ["East"])["filter"]}]},
            "sections": {"overview": {"visualContainers": {"chart": {"singleVisual": {
                "visualType": "clusteredColumnChart",
                "orderBy": [{"Direction": 2, "Expression": fx.measure("_Measures", "Revenue")}],
                "activeProjections": {"Category": [fx.column("Customers", "Region")]},
            }, "highlight": {"selection": [{"dataMap": {"Sales.Region": [{"scopeId": {"Comparison": {
                "ComparisonKind": 0, "Left": fx.column("Sales", "Region"),
                "Right": {"Literal": {"Value": "'East'"}}}}}]}, "metadata": ["_Measures.Revenue"]}]}}}}},
        },
    }
    return {
        "pages/overview/page.json": {"$schema": "page/2.0.0", "name": "overview", "displayName": "Overview",
                                     "displayOption": "FitToPage", "height": 720, "width": 1280,
                                     "filterConfig": {"filters": [flt[0]]}},
        "report.json": {"$schema": "report/3.0.0", "themeCollection": {}, "filterConfig": {"filters": [flt[1]]}},
        "bookmarks/bookmarks.json": {"$schema": "bookmarksMetadata/1.0.0", "items": [{"name": "Bm1"}]},
        "bookmarks/Bm1.bookmark.json": bookmark,
        "pages/overview/visuals/chart/mobile.json": {
            "$schema": "visualContainerMobileState/2.0.0", "position": {"x": 0, "y": 0, "width": 300, "height": 200},
            "objects": {"dataPoint": [{"properties": {}, "selector": {"metadata": "Sum(Sales.Revenue)"}},
                                      {"properties": {}, "selector": {"metadata": "_Measures.Revenue"}}]}},
        "reportExtensions.json": {"$schema": "reportExtension/1.0.0", "name": "extension", "entities": [
            {"name": "Sales", "measures": [{
                "name": "Report Margin", "dataType": "Double",
                "expression": "SUM(Sales[Revenue]) - SUM(Sales[Revenue]) * 0.1",
                "references": {"measures": [{"entity": "_Measures", "name": "Revenue"}]}}]}]},
    }


def build(tmp, **kwargs):
    kwargs.setdefault("visuals", {"overview/chart": chart(), "overview/grid": grid()})
    kwargs.setdefault("report_files", report_files())
    return fx.build_project(tmp, **kwargs)


def defn(p, rel):
    return fx.load_json(p.report_definition / rel)


def test_column_rename():
    print("\n== column rename across the report ==")
    with tempfile.TemporaryDirectory() as tmp:
        p = build(tmp)
        r = p.connector.rename_column_in_files("Sales", "Revenue", "Net Revenue")
        check("rename succeeds", r.success, r.message)
        v = p.visual("overview", "chart")
        agg, m1, _ = projections(v, "Y")
        eq("aggregated column: Property under Aggregation.Expression.Column",
           agg["field"]["Aggregation"]["Expression"]["Column"]["Property"], "Net Revenue")
        eq("aggregated column: queryRef keeps its Sum(...) form", agg["queryRef"], "Sum(Sales.Net Revenue)")
        eq("aggregated column: its own display name is left", agg["nativeQueryRef"], "Sum of Revenue")
        tip = projections(v, "Tooltips")[0]
        eq("plain column: Property", tip["field"]["Column"]["Property"], "Net Revenue")
        eq("plain column: Entity untouched", tip["field"]["Column"]["Expression"]["SourceRef"]["Entity"], "Sales")
        eq("plain column: queryRef", tip["queryRef"], "Sales.Net Revenue")
        eq("plain column: nativeQueryRef", tip["nativeQueryRef"], "Net Revenue")
        eq("measure of the same name: Property untouched", m1["field"]["Measure"]["Property"], "Revenue")
        eq("measure of the same name: queryRef untouched", m1["queryRef"], "_Measures.Revenue")
        eq("measure of the same name: nativeQueryRef untouched", m1["nativeQueryRef"], "Revenue")
        selectors = [o["selector"]["metadata"] for o in v["visual"]["objects"]["dataPoint"]]
        eq("selector metadata: only the column's", selectors, ["_Measures.Revenue", "Sum(Sales.Net Revenue)"])
        eq("conditional formatting on the measure untouched",
           v["visual"]["objects"]["labels"][0]["properties"]["color"]["solid"]["color"]["expr"]["FillRule"]["Input"],
           fx.measure("_Measures", "Revenue"))
        eq("sort on the measure untouched", v["visual"]["query"]["sortDefinition"]["sort"][0]["field"],
           fx.measure("_Measures", "Revenue"))
        page = defn(p, "pages/overview/page.json")["filterConfig"]["filters"][0]
        eq("page filter: field", page["field"]["Column"]["Property"], "Net Revenue")
        aliased = page["filter"]["Where"][0]["Condition"]["In"]["Expressions"][0]["Column"]
        eq("page filter: condition reached through the From alias", aliased["Property"], "Net Revenue")
        eq("page filter: alias itself untouched", aliased["Expression"]["SourceRef"]["Source"], "s")
        mobile = defn(p, "pages/overview/visuals/chart/mobile.json")
        eq("mobile layout selectors", [o["selector"]["metadata"] for o in mobile["objects"]["dataPoint"]],
           ["Sum(Sales.Net Revenue)", "_Measures.Revenue"])
        ext = defn(p, "reportExtensions.json")["entities"][0]["measures"][0]
        eq("report-level measure DAX", ext["expression"], "SUM(Sales[Net Revenue]) - SUM(Sales[Net Revenue]) * 0.1")
        eq("report-level measure's reference to the model measure untouched",
           ext["references"]["measures"][0], {"entity": "_Measures", "name": "Revenue"})
        eq("bindings all resolve", [e.message for e in p.connector.validate_report_bindings()], [])
        check("free text is reported, not rewritten",
              v["visual"]["objects"]["title"][0]["properties"]["text"] == fx.literal("Revenue by region")
              and warned(r, "chart/visual.json", "still contain 'Revenue'"), str(r.warnings))


def test_measure_rename():
    print("\n== measure rename across the report ==")
    with tempfile.TemporaryDirectory() as tmp:
        p = build(tmp)
        r = p.connector.rename_measure_in_files("Revenue", "Total Revenue")
        check("rename succeeds", r.success, r.message)
        v = p.visual("overview", "chart")
        agg, m1, m2 = projections(v, "Y")
        eq("measure projection: Property", m1["field"]["Measure"]["Property"], "Total Revenue")
        eq("measure projection: queryRef", m1["queryRef"], "_Measures.Total Revenue")
        eq("measure projection: nativeQueryRef", m1["nativeQueryRef"], "Total Revenue")
        eq("other measure untouched", (m2["field"]["Measure"]["Property"], m2["queryRef"], m2["displayName"]),
           ("Rev x2", "_Measures.Rev x2", "Doubled"))
        eq("column of the same name: aggregated projection untouched",
           (agg["field"]["Aggregation"]["Expression"]["Column"]["Property"], agg["queryRef"]),
           ("Revenue", "Sum(Sales.Revenue)"))
        tip = projections(v, "Tooltips")[0]
        eq("column of the same name: plain projection untouched",
           (tip["field"]["Column"]["Property"], tip["queryRef"], tip["nativeQueryRef"]),
           ("Revenue", "Sales.Revenue", "Revenue"))
        eq("selector metadata: only the measure's",
           [o["selector"]["metadata"] for o in v["visual"]["objects"]["dataPoint"]],
           ["_Measures.Total Revenue", "Sum(Sales.Revenue)"])
        eq("conditional formatting input",
           v["visual"]["objects"]["labels"][0]["properties"]["color"]["solid"]["color"]["expr"]["FillRule"]["Input"],
           fx.measure("_Measures", "Total Revenue"))
        eq("sort definition", v["visual"]["query"]["sortDefinition"]["sort"][0]["field"],
           fx.measure("_Measures", "Total Revenue"))
        f2 = v["filterConfig"]["filters"][1]
        eq("measure filter: field", f2["field"]["Measure"]["Property"], "Total Revenue")
        eq("measure filter: condition through the From alias",
           f2["filter"]["Where"][0]["Condition"]["Comparison"]["Left"]["Measure"]["Property"], "Total Revenue")
        eq("page filter on the same-named column untouched",
           defn(p, "pages/overview/page.json")["filterConfig"]["filters"][0]["field"], fx.column("Sales", "Revenue"))
        bm = defn(p, "bookmarks/Bm1.bookmark.json")["explorationState"]["sections"]["overview"]
        chart_state = bm["visualContainers"]["chart"]
        eq("bookmark: orderBy", chart_state["singleVisual"]["orderBy"][0]["Expression"],
           fx.measure("_Measures", "Total Revenue"))
        eq("bookmark: highlight metadata list", chart_state["highlight"]["selection"][0]["metadata"],
           ["_Measures.Total Revenue"])
        eq("mobile layout selectors",
           [o["selector"]["metadata"] for o in defn(p, "pages/overview/visuals/chart/mobile.json")["objects"]["dataPoint"]],
           ["Sum(Sales.Revenue)", "_Measures.Total Revenue"])
        ext = defn(p, "reportExtensions.json")["entities"][0]["measures"][0]
        eq("report-level measure's reference list", ext["references"]["measures"][0],
           {"entity": "_Measures", "name": "Total Revenue"})
        eq("report-level measure DAX keeps the column", ext["expression"],
           "SUM(Sales[Revenue]) - SUM(Sales[Revenue]) * 0.1")
        eq("bindings all resolve", [e.message for e in p.connector.validate_report_bindings()], [])


def test_table_rename():
    print("\n== table rename across the report ==")
    with tempfile.TemporaryDirectory() as tmp:
        p = build(tmp)
        r = p.connector.rename_table_in_files("Sales", "Sales Data")
        check("rename succeeds", r.success, r.message)
        v = p.visual("overview", "chart")
        agg = projections(v, "Y")[0]
        eq("Entity under Aggregation", agg["field"]["Aggregation"]["Expression"]["Column"]["Expression"]["SourceRef"],
           {"Entity": "Sales Data"})
        eq("queryRef inside Sum(...)", agg["queryRef"], "Sum(Sales Data.Revenue)")
        eq("other table untouched", projections(v, "Category")[0]["queryRef"], "Customers.Region")
        f1 = v["filterConfig"]["filters"][0]
        eq("filter: From entity renamed, alias kept", f1["filter"]["From"], [{"Name": "s", "Entity": "Sales Data", "Type": 0}])
        eq("filter: aliased reference still points at the alias",
           f1["filter"]["Where"][0]["Condition"]["In"]["Expressions"][0]["Column"]["Expression"], {"SourceRef": {"Source": "s"}})
        eq("selector metadata", [o["selector"]["metadata"] for o in v["visual"]["objects"]["dataPoint"]],
           ["_Measures.Revenue", "Sum(Sales Data.Revenue)"])
        g = projections(p.visual("overview", "grid"), "Values")
        eq("grid queryRefs", [x["queryRef"] for x in g],
           ["Sales Data.ID", "Customers.ID", "Customers.Geography.Region",
            "Sales Data.Order Date.Variation.Date Hierarchy.Year", "Sales Data.Report Margin"])
        eq("variation source entity",
           g[3]["field"]["HierarchyLevel"]["Expression"]["Hierarchy"]["Expression"]["PropertyVariationSource"]["Expression"],
           {"SourceRef": {"Entity": "Sales Data"}})
        eq("report-level measure reference keeps its schema, follows the table",
           g[4]["field"]["Measure"]["Expression"]["SourceRef"], {"Schema": "extension", "Entity": "Sales Data"})
        ext = defn(p, "reportExtensions.json")["entities"][0]
        eq("report extension entity", ext["name"], "Sales Data")
        eq("report extension DAX", ext["measures"][0]["expression"],
           "SUM('Sales Data'[Revenue]) - SUM('Sales Data'[Revenue]) * 0.1")
        hl = defn(p, "bookmarks/Bm1.bookmark.json")["explorationState"]["sections"]["overview"][
            "visualContainers"]["chart"]["highlight"]["selection"][0]
        eq("bookmark dataMap key", list(hl["dataMap"]), ["Sales Data.Region"])
        eq("bookmark filter expression",
           defn(p, "bookmarks/Bm1.bookmark.json")["explorationState"]["filters"]["byExpr"][0]["expression"],
           fx.column("Sales Data", "Region"))
        eq("report filter on another table untouched", defn(p, "report.json")["filterConfig"]["filters"][0]["field"],
           fx.column("Customers", "Region"))
        eq("bindings all resolve", [e.message for e in p.connector.validate_report_bindings()], [])


def test_hierarchies_and_variations():
    print("\n== hierarchy levels keep their names; a date variation follows its column ==")
    with tempfile.TemporaryDirectory() as tmp:
        p = build(tmp)
        r = p.connector.rename_column_in_files("Customers", "Region", "Area")
        check("column behind a hierarchy level: rename succeeds", r.success, r.message)
        g = projections(p.visual("overview", "grid"), "Values")
        eq("hierarchy level reference untouched (the level is still called Region)",
           (g[2]["field"], g[2]["queryRef"]),
           (fx.hierarchy_level("Customers", "Geography", "Region"), "Customers.Geography.Region"))
        eq("the plain column reference follows",
           projections(p.visual("overview", "chart"), "Category")[0]["field"], fx.column("Customers", "Area"))
        eq("bookmark activeProjections follow",
           defn(p, "bookmarks/Bm1.bookmark.json")["explorationState"]["sections"]["overview"]["visualContainers"][
               "chart"]["singleVisual"]["activeProjections"]["Category"], [fx.column("Customers", "Area")])
        eq("report-level filter follows", defn(p, "report.json")["filterConfig"]["filters"][0]["field"],
           fx.column("Customers", "Area"))

        r = p.connector.rename_column_in_files("Sales", "Order Date", "Sold On")
        check("date column with a generated hierarchy: rename succeeds", r.success, r.message)
        g = projections(p.visual("overview", "grid"), "Values")
        eq("variation source column",
           g[3]["field"]["HierarchyLevel"]["Expression"]["Hierarchy"]["Expression"]["PropertyVariationSource"]["Property"],
           "Sold On")
        eq("variation queryRef", g[3]["queryRef"], "Sales.Sold On.Variation.Date Hierarchy.Year")
        eq("level name untouched", g[3]["field"]["HierarchyLevel"]["Level"], "Year")
        eq("bindings all resolve", [e.message for e in p.connector.validate_report_bindings()], [])


def test_same_named_columns_and_dotted_tables():
    print("\n== only the owning table's references; a dotted table name is not a prefix match ==")
    with tempfile.TemporaryDirectory() as tmp:
        p = build(tmp)
        r = p.connector.rename_column_in_files("Sales", "ID", "SalesID")
        check("rename succeeds", r.success, r.message)
        v = p.visual("overview", "grid")
        g = projections(v, "Values")
        eq("Sales.ID follows", (g[0]["field"], g[0]["queryRef"], g[0]["nativeQueryRef"]),
           (fx.column("Sales", "SalesID"), "Sales.SalesID", "SalesID"))
        eq("Customers.ID untouched", (g[1]["field"], g[1]["queryRef"], g[1]["nativeQueryRef"]),
           (fx.column("Customers", "ID"), "Customers.ID", "ID"))
        eq("selectors", [o["selector"]["metadata"] for o in v["visual"]["objects"]["columnWidth"]],
           ["Sales.SalesID", "Customers.ID"])
    files = dict(fx.MODEL_FILES)
    files["tables/Sales.EU.tmdl"] = "table 'Sales.EU'\n\tcolumn Amount\n\t\tdataType: double\n\t\tsourceColumn: Amount\n"
    files["model.tmdl"] = fx.MODEL + "ref table 'Sales.EU'\n"
    eu = fx.visual("eu", "tableEx", {"Values": [
        fx.projection(fx.column("Sales.EU", "Amount"), "Sales.EU.Amount", "Amount"),
        fx.projection(fx.column("Sales", "Revenue"), "Sales.Revenue", "Revenue")]},
        objects={"columnWidth": [{"properties": {}, "selector": {"metadata": "Sales.EU.Amount"}},
                                 {"properties": {}, "selector": {"metadata": "Sales.Revenue"}}]})
    with tempfile.TemporaryDirectory() as tmp:
        p = fx.build_project(tmp, model=files, visuals={"overview/eu": eu})
        r = p.connector.rename_table_in_files("Sales", "Orders")
        check("table rename succeeds", r.success, r.message)
        v = p.visual("overview", "eu")
        eq("projections", [(x["field"]["Column"]["Expression"]["SourceRef"]["Entity"], x["queryRef"])
                           for x in projections(v, "Values")],
           [("Sales.EU", "Sales.EU.Amount"), ("Orders", "Orders.Revenue")])
        eq("selector with no expression beside it is attributed by the model",
           [o["selector"]["metadata"] for o in v["visual"]["objects"]["columnWidth"]],
           ["Sales.EU.Amount", "Orders.Revenue"])


SPEC = {
    "data": {"name": "dataset"},
    "transform": [
        {"filter": "datum.Revenue > 0 && datum['Region'] != 'none' && datum[\"Revenue\"] < 9"},
        {"calculate": "datum.Revenue * 2", "as": "double"},
        {"aggregate": [{"op": "sum", "field": "Revenue", "as": "total"}], "groupby": ["Region"]},
    ],
    "mark": "bar",
    "encoding": {
        "x": {"field": "Region", "type": "nominal", "title": "Region of the customer"},
        "y": {"field": "Revenue", "type": "quantitative"},
        "tooltip": [{"field": "Region"}, {"field": "Revenue", "format": ","}],
    },
}
CONFIG = {"view": {"stroke": "transparent"}, "font": "Segoe UI"}


def deneb(native_region="Region", display_region=None, spec=None):
    return fx.visual("spec", "deneb7E15AEF80B9E4D4F8E12924291ECE89A", {"dataset": [
        fx.projection(fx.column("Customers", "Region"), "Customers.Region", native_region, display=display_region),
        fx.projection(fx.measure("_Measures", "Revenue"), "_Measures.Revenue", "Revenue"),
    ]}, objects=fx.deneb_objects(spec or SPEC, CONFIG, [display_region or native_region, "Revenue"]))


def spec_of(visual):
    props = visual["visual"]["objects"]["vega"][0]["properties"]
    return fx.literal_json(props["jsonSpec"])


def test_deneb_spec_follows_its_fields():
    print("\n== Deneb: field names inside the spec follow a rename ==")
    with tempfile.TemporaryDirectory() as tmp:
        p = fx.build_project(tmp, visuals={"overview/spec": deneb()})
        raw_before = fx.read(p.visual_path("overview", "spec"))
        r = p.connector.rename_measure_in_files("Revenue", "Total Revenue")
        check("measure rename succeeds", r.success, r.message)
        v = p.visual("overview", "spec")
        s = spec_of(v)
        eq("datum.x, datum['x'] and datum[\"x\"] in an expression",
           s["transform"][0]["filter"],
           "datum['Total Revenue'] > 0 && datum['Region'] != 'none' && datum[\"Total Revenue\"] < 9")
        eq("calculate expression", s["transform"][1]["calculate"], "datum['Total Revenue'] * 2")
        eq("the name a transform outputs is its own", s["transform"][1]["as"], "double")
        eq("aggregate field", s["transform"][2]["aggregate"][0]["field"], "Total Revenue")
        eq("encoding field", s["encoding"]["y"]["field"], "Total Revenue")
        eq("tooltip fields", [t["field"] for t in s["encoding"]["tooltip"]], ["Region", "Total Revenue"])
        props = v["visual"]["objects"]
        eq("field-tracking keys", list(fx.literal_json(props["stateManagement"][0]["properties"]["supportFieldConfiguration"])),
           ["Region", "Total Revenue"])
        eq("config untouched", fx.literal_json(props["vega"][0]["properties"]["jsonConfig"]), CONFIG)
        raw_after = fx.read(p.visual_path("overview", "spec"))
        check("the spec literal keeps its compact form and '' quoting",
              "datum[''Total Revenue''] > 0 && datum[''Region''] != ''none''" in raw_after, raw_after[:400])
        check("lines the rename did not concern are identical",
              [ln for ln in raw_before.splitlines() if "Revenue" not in ln]
              == [ln for ln in raw_after.splitlines() if "Revenue" not in ln])

        r = p.connector.rename_column_in_files("Customers", "Region", "Area")
        check("column rename succeeds", r.success, r.message)
        s = spec_of(p.visual("overview", "spec"))
        eq("identifier-safe new name keeps the dot / quote style of each reference",
           s["transform"][0]["filter"],
           "datum['Total Revenue'] > 0 && datum['Area'] != 'none' && datum[\"Total Revenue\"] < 9")
        eq("groupby list", s["transform"][2]["groupby"], ["Area"])
        eq("encoding field; its free-text title is left", (s["encoding"]["x"]["field"], s["encoding"]["x"]["title"]),
           ("Area", "Region of the customer"))
        check("the leftover title text is reported", warned(r, "spec/visual.json", "still contain 'Region'"),
              str(r.warnings))


def test_deneb_cases_that_must_not_be_rewritten():
    print("\n== Deneb: custom display names, derived fields and broken specs are left alone ==")
    with tempfile.TemporaryDirectory() as tmp:
        p = fx.build_project(tmp, visuals={"overview/spec": deneb(display_region="Region")})
        r = p.connector.rename_column_in_files("Customers", "Region", "Area")
        check("rename succeeds", r.success, r.message)
        v = p.visual("overview", "spec")
        eq("projection follows the model", projections(v, "dataset")[0]["field"], fx.column("Customers", "Area"))
        eq("a field shown under a custom display name keeps that name in the spec", spec_of(v), SPEC)
    derived = json.loads(json.dumps(SPEC))
    derived["transform"].append({"calculate": "upper(datum.Region)", "as": "Region"})
    with tempfile.TemporaryDirectory() as tmp:
        p = fx.build_project(tmp, visuals={"overview/spec": deneb(spec=derived)})
        r = p.connector.rename_column_in_files("Customers", "Region", "Area")
        check("rename succeeds", r.success, r.message)
        eq("a spec that derives its own field of that name is untouched", spec_of(p.visual("overview", "spec")), derived)
        check("and reported", warned(r, "creates its own field named 'Region'"), str(r.warnings))
    with tempfile.TemporaryDirectory() as tmp:
        broken = deneb()
        broken["visual"]["objects"]["vega"][0]["properties"]["jsonSpec"] = fx.literal('{"mark": "bar", "field": "Region"')
        p = fx.build_project(tmp, visuals={"overview/spec": broken})
        r = p.connector.rename_column_in_files("Customers", "Region", "Area")
        check("rename succeeds with an unparseable spec", r.success, r.message)
        eq("the spec text is untouched",
           p.visual("overview", "spec")["visual"]["objects"]["vega"][0]["properties"]["jsonSpec"],
           fx.literal('{"mark": "bar", "field": "Region"'))
        check("and the visual is named in a warning", warned(r, "spec/visual.json", "not parseable"), str(r.warnings))


def legacy_report():
    """A PBIR-Legacy report.json: per visual, config / query / dataTransforms / filters are
    JSON documents stored as strings."""
    def container(name, config, filters):
        proto = config["singleVisual"]["prototypeQuery"]
        query = {"Commands": [{"SemanticQueryDataShapeCommand": {"Query": proto}}]}
        entity_of = {src["Name"]: src["Entity"] for src in proto["From"]}

        def by_entity(sel):
            # dataTransforms repeats each select with the table named directly, not by alias
            kind = "Column" if "Column" in sel else "Measure"
            alias = sel[kind]["Expression"]["SourceRef"]["Source"]
            return {kind: {"Expression": fx.source(entity_of[alias]), "Property": sel[kind]["Property"]}}

        transforms = {"selects": [{"displayName": sel["Name"].split(".")[-1], "queryName": sel["Name"],
                                   "expr": by_entity(sel)} for sel in proto["Select"]],
                      "projectionOrdering": {"Values": [0]}}
        return {"x": 0, "y": 0, "width": 300, "height": 200, "config": json.dumps(config),
                "filters": json.dumps(filters), "query": json.dumps(query), "dataTransforms": json.dumps(transforms)}

    def aliased(kind, alias, prop):
        return {kind: {"Expression": {"SourceRef": {"Source": alias}}, "Property": prop}}

    card = {"name": "card1", "singleVisual": {
        "visualType": "card",
        "projections": {"Values": [{"queryRef": "_Measures.Revenue"}]},
        "prototypeQuery": {"Version": 2, "From": [{"Name": "m", "Entity": "_Measures", "Type": 0}],
                           "Select": [dict(aliased("Measure", "m", "Revenue"), Name="_Measures.Revenue")]},
        "columnProperties": {"_Measures.Revenue": {"displayName": "Rev"}},
    }}
    table = {"name": "table1", "singleVisual": {
        "visualType": "tableEx",
        "projections": {"Values": [{"queryRef": "Sales.Revenue"}, {"queryRef": "Customers.Region"}]},
        "prototypeQuery": {"Version": 2,
                           "From": [{"Name": "s", "Entity": "Sales", "Type": 0}, {"Name": "c", "Entity": "Customers", "Type": 0}],
                           "Select": [dict(aliased("Column", "s", "Revenue"), Name="Sales.Revenue"),
                                      dict(aliased("Column", "c", "Region"), Name="Customers.Region")],
                           "OrderBy": [{"Direction": 2, "Expression": aliased("Column", "s", "Revenue")}]},
        "objects": {"columnWidth": [{"properties": {}, "selector": {"metadata": "Sales.Revenue"}}]},
    }}
    table_filters = [{"name": "f", "expression": fx.column("Sales", "Region"), "type": "Categorical",
                      "filter": fx.categorical_filter("f", "Sales", "Region", "s", ["East"])["filter"]}]
    return {
        "config": json.dumps({"version": "5.50", "themeCollection": {}}),
        "filters": json.dumps([{"name": "rf", "expression": fx.column("Customers", "Region"), "type": "Categorical"}]),
        "sections": [{
            "name": "ReportSection1", "displayName": "Page 1", "config": "{}",
            "filters": json.dumps([{"name": "pf", "expression": fx.column("Sales", "Revenue"), "type": "Advanced"}]),
            "visualContainers": [container("card1", card, []), container("table1", table, table_filters)],
        }],
    }


def nested(container, key):
    return json.loads(container[key])


def test_legacy_report_json():
    print("\n== PBIR-Legacy: JSON documents nested in strings are rewritten in place ==")
    with tempfile.TemporaryDirectory() as tmp:
        p = fx.build_project(tmp, legacy_report=legacy_report())
        check("project is read as PBIR-Legacy", p.connector.get_project_info()["report_format"] == "PBIR-Legacy")
        path = p.report / "report.json"
        card_before = fx.load_json(path)["sections"][0]["visualContainers"][0]
        r = p.connector.rename_column_in_files("Sales", "Revenue", "Net Revenue")
        check("column rename succeeds", r.success, r.message)
        report = fx.load_json(path)
        card, table = report["sections"][0]["visualContainers"]
        check("a visual the rename does not concern keeps its strings byte for byte", card == card_before)
        sv = nested(table, "config")["singleVisual"]
        eq("config: Select through the alias", sv["prototypeQuery"]["Select"][0],
           {"Column": {"Expression": {"SourceRef": {"Source": "s"}}, "Property": "Net Revenue"}, "Name": "Sales.Net Revenue"})
        eq("config: OrderBy", sv["prototypeQuery"]["OrderBy"][0]["Expression"]["Column"]["Property"], "Net Revenue")
        eq("config: projection list", sv["projections"]["Values"],
           [{"queryRef": "Sales.Net Revenue"}, {"queryRef": "Customers.Region"}])
        eq("config: selector", sv["objects"]["columnWidth"][0]["selector"]["metadata"], "Sales.Net Revenue")
        eq("query string", nested(table, "query")["Commands"][0]["SemanticQueryDataShapeCommand"]["Query"]["Select"][0]["Name"],
           "Sales.Net Revenue")
        eq("dataTransforms string", nested(table, "dataTransforms")["selects"][0]["queryName"], "Sales.Net Revenue")
        eq("page filter string", nested(report["sections"][0], "filters")[0]["expression"], fx.column("Sales", "Net Revenue"))
        eq("bindings all resolve", [e.message for e in p.connector.validate_report_bindings()], [])

        r = p.connector.rename_measure_in_files("Revenue", "Total Revenue")
        check("measure rename succeeds", r.success, r.message)
        card = fx.load_json(path)["sections"][0]["visualContainers"][0]
        sv = nested(card, "config")["singleVisual"]
        eq("measure Select", sv["prototypeQuery"]["Select"][0],
           {"Measure": {"Expression": {"SourceRef": {"Source": "m"}}, "Property": "Total Revenue"},
            "Name": "_Measures.Total Revenue"})
        eq("columnProperties is keyed by query name", sv["columnProperties"], {"_Measures.Total Revenue": {"displayName": "Rev"}})

        r = p.connector.rename_table_in_files("Sales", "Sales Data")
        check("table rename succeeds", r.success, r.message)
        report = fx.load_json(path)
        table = report["sections"][0]["visualContainers"][1]
        sv = nested(table, "config")["singleVisual"]
        eq("From entity, alias kept", sv["prototypeQuery"]["From"][0], {"Name": "s", "Entity": "Sales Data", "Type": 0})
        eq("Select names", [s["Name"] for s in sv["prototypeQuery"]["Select"]], ["Sales Data.Net Revenue", "Customers.Region"])
        eq("visual filter string", nested(table, "filters")[0]["expression"], fx.column("Sales Data", "Region"))
        eq("report filter on another table untouched", nested(report, "filters")[0]["expression"],
           fx.column("Customers", "Region"))
        eq("bindings all resolve", [e.message for e in p.connector.validate_report_bindings()], [])
        eq("no broken references", p.connector.scan_broken_references()["has_broken_refs"], False)


def test_stale_kinds_extension_measures_and_visual_calculations():
    print("\n== node kinds, report-level measures, visual calculations ==")
    v = fx.visual("mix", "tableEx", {"Values": [
        # a measure stored under a Column node (a stale kind): the table and name still identify it
        fx.projection(fx.column("_Measures", "Revenue"), "_Measures.Revenue", "Revenue"),
        # a report-level measure that happens to share the model measure's name
        fx.projection({"Measure": {"Expression": {"SourceRef": {"Schema": "extension", "Entity": "_Measures"}},
                                   "Property": "Revenue"}}, "_Measures.Revenue1"),
        fx.projection({"NativeVisualCalculation": {"Language": "dax", "Name": "Running",
                                                   "Expression": "RUNNINGSUM([Revenue]) + [Other]"}}, "Running"),
        # an alias with no From clause in scope: the table cannot be known, so it is not guessed
        fx.projection({"Measure": {"Expression": {"SourceRef": {"Source": "zz"}}, "Property": "Revenue"}}, "zz.Revenue"),
    ]})
    with tempfile.TemporaryDirectory() as tmp:
        p = fx.build_project(tmp, visuals={"overview/mix": v})
        r = p.connector.rename_measure_in_files("Revenue", "Total Revenue")
        check("rename succeeds", r.success, r.message)
        g = projections(p.visual("overview", "mix"), "Values")
        eq("reference with an unresolvable table untouched", (g[3]["field"]["Measure"]["Property"], g[3]["queryRef"]),
           ("Revenue", "zz.Revenue"))
        check("and reported", warned(r, "no resolvable table"), str(r.warnings))
        eq("stale Column kind for a measure is still that measure", (g[0]["field"], g[0]["queryRef"]),
           (fx.column("_Measures", "Total Revenue"), "_Measures.Total Revenue"))
        eq("report-level measure of the same name untouched", (g[1]["field"]["Measure"]["Property"], g[1]["queryRef"]),
           ("Revenue", "_Measures.Revenue1"))
        eq("visual calculation follows the field's new display name",
           g[2]["field"]["NativeVisualCalculation"]["Expression"], "RUNNINGSUM([Total Revenue]) + [Other]")


def test_round_trip_and_untouched_files():
    print("\n== report files: there and back is byte-identical; unrelated files never change ==")
    with tempfile.TemporaryDirectory() as tmp:
        p = build(tmp, visuals={"overview/chart": chart(), "overview/grid": grid(), "overview/spec": deneb()})
        before = fx.snapshot(p.root)
        r1 = p.connector.rename_column_in_files("Sales", "Region", "Zone")
        after_one = fx.snapshot(p.root)
        changed = sorted(k for k in before if before[k] != after_one[k])
        check("rename succeeds", r1.success, r1.message)
        check("files with no reference to Sales[Region] are byte-identical",
              all("Zone" in after_one[k].decode("utf-8") for k in changed)
              and not any(k.endswith(("grid/visual.json", "spec/visual.json", "report.json", "pages.json")) for k in changed),
              str(changed))
        r2 = p.connector.rename_column_in_files("Sales", "Zone", "Region")
        check("rename back succeeds", r2.success, r2.message)
        check("project is byte-identical again", fx.snapshot(p.root) == before)


def test_repair_after_model_only_rename():
    print("\n== fix_broken_visual_references: report repaired after a model-only rename ==")
    with tempfile.TemporaryDirectory() as tmp:
        p = build(tmp)
        # the state a TOM rename leaves: the model says Orders, the report still says Sales
        for rel in ("tables/Sales.tmdl", "tables/_Measures.tmdl", "tables/Dim Date.tmdl", "roles/Reader.tmdl",
                    "relationships.tmdl", "model.tmdl", "perspectives/Core.tmdl", "tables/Customers.tmdl"):
            path = p.definition / rel
            text = fx.read(path).replace("table Sales\n", "table Orders\n").replace("Sales[", "Orders[")
            text = text.replace("Sales.", "Orders.").replace("ref table Sales", "ref table Orders")
            text = text.replace("perspectiveTable Sales", "perspectiveTable Orders")
            text = text.replace("tablePermission Sales", "tablePermission Orders").replace("= Sales\n", "= Orders\n")
            text = text.replace("{Sales, Joined}", "{Orders, Joined}").replace('{"ID"}, Sales,', '{"ID"}, Orders,')
            fx.write(path, text)
        scan = p.connector.scan_broken_references()
        check("scan reports the orphaned table", scan["orphaned_table_names"] == ["Sales"] and scan["has_broken_refs"],
              str(scan["orphaned_table_names"]))
        bad = p.connector.fix_broken_visual_references("Sales", "Nope")
        check("refused when the target table does not exist", bad["success"] is False and "does not exist" in bad["message"],
              str(bad))
        bad = p.connector.fix_broken_visual_references("Customers", "Orders")
        check("refused when the 'old' table still exists", bad["success"] is False and "still exists" in bad["message"],
              str(bad))
        res = p.connector.fix_broken_visual_references("Sales", "Orders")
        check("repair succeeds and counts what it fixed", res["success"] and res["references_fixed"] > 10, str(res))
        eq("report now binds cleanly", [e.message for e in p.connector.validate_report_bindings()], [])
        check("linguistic metadata and diagram followed",
              '"ConceptualEntity": "Orders"' in p.tmdl("cultures/en-US.tmdl")
              and fx.load_json(p.model / "diagramLayout.json")["diagrams"][0]["nodes"][0]["nodeIndex"] == "Orders")
        again = p.connector.fix_broken_visual_references("Sales", "Orders")
        check("a second run finds nothing and says so", again["success"] is False and again["references_fixed"] == 0,
              str(again))


if __name__ == "__main__":
    print("=" * 70)
    print("  PBIP RENAME: REPORT (PBIR) TESTS")
    print("=" * 70)
    for fn in (test_column_rename, test_measure_rename, test_table_rename, test_hierarchies_and_variations,
               test_same_named_columns_and_dotted_tables, test_deneb_spec_follows_its_fields,
               test_deneb_cases_that_must_not_be_rewritten, test_legacy_report_json,
               test_stale_kinds_extension_measures_and_visual_calculations,
               test_round_trip_and_untouched_files, test_repair_after_model_only_rename):
        fn()
    print("\n" + "=" * 70)
    if _failures:
        print(f"  {len(_failures)} CHECK(S) FAILED: {', '.join(_failures)}")
        sys.exit(1)
    print("  ALL PBIP REPORT RENAME CHECKS PASSED")
    print("=" * 70)
