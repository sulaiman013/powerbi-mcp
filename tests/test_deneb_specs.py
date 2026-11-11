"""
Tests for src/deneb_specs.py.

As data: which field sits on which channel of every kind, the transforms (the stack, the top-N
ranking, the order of a second column), that each interactivity option adds and removes exactly
its own parts, the tooltip, the number formats, the parts that depend on the visual's size, field
names that need escaping, names checked against a model, the returned ``deneb`` element against
the report spec (validated, and written into a report folder), determinism and the messages for
input that cannot be used.

Drawn: every kind is compiled by Vega-Lite and rendered headless by Vega, in Node.js, and the
marks are measured: a bar's length against its value, a segment against the one below it, a
tick against the target. A click and a tooltip are resolved the way Deneb resolves them (ports
of its interactivity/data-point.ts and tooltip.ts). That part needs ``node`` on PATH and the
``vega`` and ``vega-lite`` packages; it prints [SKIP] without them. They are looked for in the
folder named by the environment variable DENEB_VEGA_MODULES (the folder that holds
``node_modules``, or ``node_modules`` itself), then in this repository's own ``node_modules``:

    npm install --prefix <folder> vega@6 vega-lite@6
    DENEB_VEGA_MODULES=<folder> python tests/test_deneb_specs.py

No Power BI required. Run: python tests/test_deneb_specs.py
"""
import copy
import inspect
import io
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))
import deneb_specs as ds  # noqa: E402
import pbip_json  # noqa: E402
import report_spec as rs  # noqa: E402

_failures = []


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f": {detail}" if detail and not cond else ""))
    if not cond:
        _failures.append(name)


def eq(name, actual, expected):
    check(name, actual == expected, f"got {ascii(actual)}, expected {ascii(expected)}")


def refused(name, fragments, kind, fields, **options):
    """The call is refused with a ValueError whose message contains every fragment."""
    try:
        ds.generate_deneb_spec(kind, fields, **options)
    except ValueError as exc:
        missing = [f for f in fragments if f not in str(exc)]
        check(name, not missing, f"message {ascii(str(exc))} lacks {missing!r}")
    except Exception as exc:  # anything else is a hole: the tool answers ValueError only
        check(name, False, f"raised {type(exc).__name__}: {exc}")
    else:
        check(name, False, "no error was raised")


# ---------------------------------------------------------------------------------------------
# fixtures
# ---------------------------------------------------------------------------------------------
V, T, U = "Total Sales", "Target", "Units"
PLAIN = {"category": "Product.Category", "series": "Geo.Region", "facet": "Geo.Country",
         "value": {"measure": "Sales.Total Sales"}, "target": {"measure": "Sales.Target"},
         "size": {"measure": "Sales.Units"}}
# A dot, brackets, a double quote, a backslash, single quotes (one of them leading), spaces and
# letters outside ASCII; a table whose own name holds a dot.
AWKWARD = {"category": {"column": "'Dim.Product'.Product [Group]"},
           "series": {"table": "Geo", "column": "'Région' d'été"},
           "facet": {"table": "Geo", "column": "País \"ES\""},
           "value": {"table": "Sales", "measure": "Sales.Net (€)"},
           "target": {"table": "Sales", "measure": "Target\\Plan"},
           "size": {"table": "Sales", "measure": "Units' sold"}}
AWKWARD_DATA = {"category": "Product _Group_", "series": "'Région' d'été", "facet": "País _ES_",
                "value": "Sales_Net (€)", "target": "Target_Plan", "size": "Units' sold"}
ROLES = {"bar": ("category", "value"), "column": ("category", "value"), "lollipop": ("category", "value"),
         "stacked_bar": ("category", "series", "value"), "stacked_column": ("category", "series", "value"),
         "line": ("category", "series", "value"), "area": ("category", "series", "value"),
         "bullet": ("category", "value", "target"), "heatmap": ("x", "y", "value"),
         "scatter": ("category", "series", "x", "y", "size")}
FACETED = ("bar", "column", "stacked_bar", "stacked_column", "line", "area", "lollipop", "scatter")
VERTICAL = ("column", "stacked_column")
ALL_OFF = {"cross_filter": False, "cross_highlight": False, "tooltips": False, "context_menu": False}
HEX = re.compile(r"#[0-9A-Fa-f]{3,8}\b")
SELECTED = {"field": "__selected__", "equal": "off"}
SHOWN = "isDefined(datum['{0}__highlight']) ? datum['{0}__highlight'] : datum['{0}']"


def fields_for(kind, names=PLAIN, facet=False):
    alias = {"x": "category", "y": "series"} if kind == "heatmap" else {"x": "value", "y": "target"}
    out = {role: names[alias.get(role, role)] for role in ROLES[kind]}
    if facet:
        out["facet"] = names["facet"]
    return out


def gen(kind, names=PLAIN, facet=False, **options):
    return ds.generate_deneb_spec(kind, fields_for(kind, names, facet), **options)


def view(spec):
    """The layered view: the spec itself, or what a facet repeats."""
    return spec.get("spec", spec)


def text(node):
    return json.dumps(node, ensure_ascii=False)


def values(result):
    return {s["property"]: s["value"] for s in result["interactivity"]}


def setting(result, prop):
    return next((s for s in result["interactivity"] if s["property"] == prop), None)


def unit(scale):
    largest = f"max(abs(domain('{scale}')[0]), abs(domain('{scale}')[1]))"
    return f"{largest} >= 1000 ? {{value: {largest}}} : {{}}"


def drawn(layer):
    """What one layer draws: (mark, the opacity fixed on the mark, {channel: where its value comes
    from}). A path (line, area) is told apart: it reads an aggregate of copies, not the rows."""
    mark, enc = layer["mark"], layer["encoding"]
    source = {}
    for channel, definition in enc.items():
        if channel in ("opacity", "tooltip", "text"):
            continue
        source[channel] = definition.get("field", definition.get("datum", definition.get("value")))
    if "transform" in layer:
        (aggregate,) = layer["transform"]
        source = {"path": [(a["op"], a["field"], a["as"]) for a in aggregate["aggregate"]],
                  "groupby": aggregate["groupby"], **source}
    return mark["type"], mark.get("opacity"), source


def opacity(layer):
    """(the tests that hide or dim the mark, each with its value; the value otherwise)."""
    channel = layer["encoding"].get("opacity")
    if channel is None:
        return [], layer["mark"].get("opacity", 1)
    conditions = channel["condition"] if isinstance(channel["condition"], list) else [channel["condition"]]
    return [(c["test"], c["value"]) for c in conditions], channel["value"]


def page_with(element):
    return {"pages": [{"name": "p", "display_name": "P", "elements": [
        {**element, "name": "chart", "x": 24, "y": 24, "width": 640, "height": 420}]}]}


def split_access_path(path):
    """Vega's splitAccessPath (vega-util), which Vega-Lite uses to read a ``field`` value:
    the parts of a property path, with quotes, brackets and backslash escapes resolved."""
    parts, n = [], len(path)
    quote, bracket, pending, i, j = None, 0, "", 0, 0

    def push():
        nonlocal pending, i
        parts.append(pending + path[i:j])
        pending, i = "", j + 1

    while j < n:
        c = path[j]
        if c == "\\":
            pending += path[i:j]
            j += 1
            i = j
        elif c == quote:
            push()
            quote, bracket = None, -1
        elif quote:
            pass
        elif i == bracket and c in "\"'":
            i, quote = j + 1, c
        elif c == "." and not bracket:
            if j > i:
                push()
            else:
                i = j + 1
        elif c == "[":
            if j > i:
                push()
            bracket = i = j + 1
        elif c == "]":
            if not bracket:
                raise ValueError("Access path missing open bracket: " + path)
            if bracket > 0:
                push()
            bracket, i = 0, j + 1
        j += 1
    if bracket:
        raise ValueError("Access path missing closing bracket: " + path)
    if quote:
        raise ValueError("Access path missing closing quote: " + path)
    if j > i:
        j += 1
        push()
    return parts


# ---------------------------------------------------------------------------------------------
# which field sits on which channel
# ---------------------------------------------------------------------------------------------
def path_of(field, series=True):
    return {"path": [("max", field, "path_y")], "groupby": ["path_x"] + (["path_series"] if series else []),
            "x": "path_x", "y": "path_y", **({"color": "path_series"} if series else {})}


HEAT_FRONT = {"expr": "scale('color', datum['shown_value'])"}
# kind -> the marks of one pass and where each channel's value comes from. ``f`` names the field
# of a role: the measure itself for the full values, "shown_<role>" for the highlighted ones.
MARKS = {
    "bar": lambda f: [("bar", {"x": f("value")})],
    "column": lambda f: [("bar", {"y": f("value")})],
    "stacked_bar": lambda f: [("bar", {"x": "stack_start", "x2": f("end")})],
    "stacked_column": lambda f: [("bar", {"y": "stack_start", "y2": f("end")})],
    "line": lambda f: [("line", path_of(f("value"))), ("point", {"y": f("value")})],
    "area": lambda f: [("area", path_of(f("value"))), ("line", path_of(f("value"))), ("point", {"y": f("value")})],
    "scatter": lambda f: [("point", {"x": "value", "y": "target"})],            # a point does not move
    "heatmap": lambda f: [("rect", {"color": "value" if f("value") == "value" else HEAT_FRONT})],
    "lollipop": lambda f: [("rule", {"x": f("value"), "x2": 0}), ("point", {"x": f("value")})],
    "bullet": lambda f: [("bar", {"x": f("value")}), ("tick", {"x": f("target")})],
}
ROLE_FIELD = {"value": V, "target": T, "end": "stack_end"}


def layers_of(kind, highlight=True):
    """The layers a kind must have: with a highlight, every mark twice, the full values dimmed
    behind (an area fainter still) and the highlighted ones in front."""
    def fixed(mark, behind):
        return {True: 0.1, False: 0.3}[behind] if mark == "area" else (ds.DIMMED if behind else None)
    full, shown = MARKS[kind](lambda f: f), MARKS[kind](lambda f: "shown_" + f)
    if not highlight:
        return [(m, fixed(m, False), named(s)) for m, s in full]
    return [(m, fixed(m, True), named(s)) for m, s in full] + [(m, fixed(m, False), named(s)) for m, s in shown]


def named(source):
    """An expectation with its role names replaced by the plain fixture's field names."""
    if isinstance(source, dict):
        return {k: named(v) for k, v in source.items()}
    if isinstance(source, list):
        return [named(v) for v in source]
    if isinstance(source, tuple):
        return tuple(named(v) for v in source)
    return ROLE_FIELD.get(source, source)


def test_channels():
    print("\n== every kind: which field sits on which channel ==")
    eq("the kinds", set(ds.KINDS), set(ROLES))
    for kind in ds.KINDS:
        r = gen(kind)
        spec = r["spec"]
        enc = spec["encoding"]
        eq(f"{kind}: reads Deneb's dataset, and nothing is embedded", spec["data"], {"name": "dataset"})
        eq(f"{kind}: the layers and their fields", [drawn(layer) for layer in spec["layer"]], layers_of(kind))
        eq(f"{kind}: with cross-highlight off, the marks once, on the measures themselves",
           [drawn(layer) for layer in gen(kind, cross_highlight=False)["spec"]["layer"]], layers_of(kind, False))
        shown = [t for t in spec.get("transform", []) if str(t.get("as", "")).startswith("shown_")]
        if kind == "scatter":
            eq("scatter: a point does not move under a highlight, so no shown value is worked out", shown, [])
        else:
            measures = [V, T] if kind == "bullet" else [V]
            expected = [{"calculate": SHOWN.format(m), "as": f"shown_{role}"}
                        for m, role in zip(measures, ("value", "target"))]
            if kind in ("stacked_bar", "stacked_column"):
                expected.append({"calculate": "datum['stack_start'] + datum['shown_value']", "as": "shown_end"})
            eq(f"{kind}: the highlighted value falls back to the full one", shown, expected)
        eq(f"{kind}: the fields to bind, in order",
           r["fields"], [fields_for(kind)[role] if isinstance(fields_for(kind)[role], dict)
                         else {"column": fields_for(kind)[role]} for role in ROLES[kind]])
        check(f"{kind}: no fixed colour anywhere", not HEX.search(text(spec) + text(r["config"])))

    # the category axis, the value axis and what colours the marks
    for kind in ("bar", "stacked_bar", "lollipop", "bullet", "column", "stacked_column", "line", "area"):
        enc = gen(kind)["spec"]["encoding"]
        k, v = ("x", "y") if kind in VERTICAL + ("line", "area") else ("y", "x")
        eq(f"{kind}: the category is on {k}", (enc[k]["field"], enc[k]["type"]),
           ("Category", "ordinal" if kind in ("line", "area") else "nominal"))
        eq(f"{kind}: the value axis is {v}, titled with the measure (a bullet's carries two, so none)",
           (enc[v]["type"], enc[v]["axis"]["title"], "field" in enc[v]), ("quantitative", None if kind == "bullet" else V, False))
        if "series" in ROLES[kind]:
            eq(f"{kind}: the series colours the marks from the theme palette",
               (enc["color"]["field"], enc["color"]["scale"], enc["color"]["title"]),
               ("Region", {"scheme": "pbiColorNominal"}, "Region"))
        else:
            check(f"{kind}: one theme colour, on the marks", "color" not in enc and all(
                layer["mark"]["color"]["expr"].startswith("pbiColor(0") for layer in gen(kind)["spec"]["layer"]))
    for kind in ("bar", "column", "bullet"):
        v = "y" if kind == "column" else "x"
        eq(f"{kind}: Vega-Lite is told not to stack the bars",
           gen(kind)["spec"]["encoding"][v].get("stack", "not said"), None)
    eq("area: series overlap rather than stack", gen("area")["spec"]["encoding"]["y"].get("stack", "not said"), None)
    check("line: nothing to stack", "stack" not in gen("line")["spec"]["encoding"]["y"])
    eq("bullet: the tick is darker than the bar, both from the theme",
       [layer["mark"]["color"]["expr"] for layer in gen("bullet")["spec"]["layer"][:2]],
       ["pbiColor(0)", "pbiColor(0, -0.5)"])
    eq("bullet: the bar is half a band high, in Vega-Lite's own property",
       gen("bullet")["spec"]["layer"][0]["mark"]["height"], {"band": 0.5})

    enc = gen("scatter")["spec"]["encoding"]
    eq("scatter: both axes are measures, neither starts at zero, each titled with its own",
       [(enc[c]["type"], enc[c].get("scale"), enc[c]["axis"]["title"]) for c in "xy"],
       [("quantitative", {"zero": False}, V), ("quantitative", {"zero": False}, T)])
    eq("scatter: the size is the size measure, a blank one drawn smallest, with no legend",
       (enc["size"], gen("scatter")["spec"]["transform"]),
       ({"field": "size_value", "type": "quantitative", "legend": None},
        [{"calculate": "isValid(datum['Units']) ? datum['Units'] : 0", "as": "size_value"}]))
    eq("scatter: the series colours the points", enc["color"]["field"], "Region")
    check("scatter without a size has no size channel", "size" not in ds.generate_deneb_spec(
        "scatter", {"x": PLAIN["value"], "y": PLAIN["target"]})["spec"]["encoding"])
    r = ds.generate_deneb_spec("scatter", {"x": {"column": "P.Price"}, "y": {"column": "P.Weight"}})
    eq("scatter of two columns: one layer, no highlight (columns have none), and a note on the missing measure",
       ([drawn(layer) for layer in r["spec"]["layer"]], values(r)["enableHighlight"],
        any("binds no measure" in n for n in r["notes"]), any("left out" in n for n in r["notes"])),
       ([("point", None, {"x": "Price", "y": "Weight"})], False, True, True))

    r = gen("heatmap")
    enc, colour = r["spec"]["encoding"], r["spec"]["layer"][0]["encoding"]["color"]
    eq("heatmap: x and y are the two columns, in that order", [(enc[c]["field"], enc[c]["type"]) for c in "xy"],
       [("Category", "ordinal"), ("Region", "ordinal")])
    eq("heatmap: the colour is the measure on the theme's gradient, kept inside its ends",
       (colour["field"], colour["type"], colour["scale"]),
       (V, "quantitative", {"scheme": "pbiColorLinear", "clamp": True}))
    eq("heatmap: the legend is titled with the measure, and no longer than the plot is wide",
       (colour["legend"].get("title"), colour["legend"]["gradientLength"]), (V, {"expr": "clamp(width, 40, 200)"}))
    eq("heatmap: color=\"divergent\" is the theme's three-colour gradient",
       gen("heatmap", color="divergent")["spec"]["layer"][0]["encoding"]["color"]["scale"]["scheme"],
       "pbiColorDivergent")
    eq("heatmap: the second column is ordered by where its values arrive within the first",
       (r["spec"]["transform"][0], enc["y"]["sort"], enc["x"]["sort"]),
       ({"window": [{"op": "row_number", "as": "y_position"}], "groupby": ["Category"]},
        {"field": "y_position", "op": "max", "order": "ascending"}, None))
    r = gen("heatmap", sort="category_desc")
    eq("heatmap: a category sort orders both columns by name, and no position is worked out",
       ([r["spec"]["encoding"][c]["sort"] for c in "xy"],
        [t for t in r["spec"].get("transform", []) if "window" in t]), (["descending", "descending"], []))


def test_stack_and_rank():
    print("\n== the stack transform and the top-N ranking ==")
    for kind in ("stacked_bar", "stacked_column"):
        r = gen(kind)
        total = {"joinaggregate": [{"op": "sum", "field": V, "as": "category_total"}], "groupby": ["Category"]}
        stack = {"stack": V, "groupby": ["Category"], "offset": "zero",
                 "sort": [{"field": "Region", "order": "ascending"}], "as": ["stack_start", "stack_end"]}
        eq(f"{kind}: categories are totalled, segments stacked from zero in series order",
           r["spec"]["transform"][:2], [total, stack])
        k = "x" if kind == "stacked_column" else "y"
        eq(f"{kind}: sorted by the category's total", r["spec"]["encoding"][k]["sort"],
           {"field": "category_total", "op": "max", "order": "descending"})
        r = gen(kind, sort="none")
        eq(f"{kind}: with no value sort and no top_n there is no total", r["spec"]["transform"][0], stack)
        r = gen(kind, facet=True)
        eq(f"{kind}, faceted: totalled and stacked within the panel",
           [t["groupby"] for t in r["spec"]["transform"] if "joinaggregate" in t or "stack" in t],
           [["Category", "Country"]] * 2)

    window = lambda r: [t for t in r["spec"]["transform"] if "window" in t or "filter" in t]  # noqa: E731
    for kind in ("bar", "column", "lollipop", "bullet"):
        r = gen(kind, top_n=3)
        eq(f"{kind}: top_n ranks by the value, largest first, and keeps the first N", window(r),
           [{"window": [{"op": "rank", "as": "value_rank"}], "sort": [{"field": V, "order": "descending"}]},
            {"filter": "datum['value_rank'] <= 3"}])
        k = "x" if kind == "column" else "y"
        eq(f"{kind}: and the axis is sorted by the same field", r["spec"]["encoding"][k]["sort"],
           {"field": V, "op": "max", "order": "descending"})
    for kind in ("stacked_bar", "stacked_column"):
        eq(f"{kind}: top_n ranks whole categories by their total", window(gen(kind, top_n=2, sort="none")),
           [{"window": [{"op": "dense_rank", "as": "value_rank"}],
             "sort": [{"field": "category_total", "order": "descending"}]}, {"filter": "datum['value_rank'] <= 2"}])
    eq("faceted: ranked within the panel", window(gen("bar", facet=True, top_n=2))[0]["groupby"], ["Country"])
    eq("faceted stack: ranked within the panel", window(gen("stacked_bar", facet=True, top_n=2))[0]["groupby"],
       ["Country"])
    check("without top_n nothing is ranked or filtered", not window(gen("bar")))
    check("top_n adds its note", any("top_n hides rows" in n for n in gen("bar", top_n=3)["notes"])
          and not any("top_n" in n for n in gen("bar")["notes"]))

    sorts = {"value_desc": {"field": V, "op": "max", "order": "descending"},
             "value_asc": {"field": V, "op": "max", "order": "ascending"},
             "category_asc": "ascending", "category_desc": "descending", "none": None}
    for name, expected in sorts.items():
        eq(f"bar sort {name}", gen("bar", sort=name)["spec"]["encoding"]["y"]["sort"], expected)
    eq("bars sort by value by default, a line keeps the delivered order",
       (gen("column")["spec"]["encoding"]["x"]["sort"], gen("line")["spec"]["encoding"]["x"]["sort"]),
       (sorts["value_desc"], None))
    eq("a line sorted by category: by the copy its paths are drawn from, which both layers hold",
       [gen(kind, sort="category_desc", facet=facet)["spec"].get("spec", gen(kind, sort="category_desc")["spec"])
        ["encoding"]["x"]["sort"] for kind in ("line", "area") for facet in (False, True)],
       [{"field": "path_x", "op": "min", "order": "descending"}] * 4)
    r = gen("line", sort="none", facet=True)
    eq("small multiples: the category is the second column, so its delivered order is worked out",
       ([t for t in r["spec"]["transform"] if "window" in t], view(r["spec"])["encoding"]["x"]["sort"]),
       ([{"window": [{"op": "row_number", "as": "category_position"}], "groupby": ["Country"]}],
        {"field": "category_position", "op": "max", "order": "ascending"}))
    eq("and a path carries that order along with its value",
       view(r["spec"])["layer"][0]["transform"][0]["aggregate"],
       [{"op": "max", "field": V, "as": "path_y"},
        {"op": "max", "field": "category_position", "as": "category_position"}])


# ---------------------------------------------------------------------------------------------
# interactivity
# ---------------------------------------------------------------------------------------------
def test_each_option_owns_its_parts():
    print("\n== each interactivity option adds and removes exactly its own parts ==")
    for kind in ds.KINDS:
        full, bare = gen(kind), gen(kind, **ALL_OFF)
        eq(f"{kind}: everything on by default",
           {k: values(full)[k] for k in ("enableTooltips", "enableContextMenu", "enableSelection", "enableHighlight")},
           dict.fromkeys(("enableTooltips", "enableContextMenu", "enableSelection", "enableHighlight"), True))
        blob = text(bare["spec"])
        check(f"{kind}: all off leaves no trace of the supporting fields but the format",
              "__selected__" not in blob and "__highlight" not in blob and "tooltip" not in blob
              and "__row__" not in blob.replace('"field": "__row__"', ""))
        eq(f"{kind}: all off, the settings say so", values(bare),
           {"enableTooltips": False, "enableContextMenu": False, "enableSelection": False, "enableHighlight": False,
            **({"supportFieldConfiguration": values(bare)["supportFieldConfiguration"]}
               if "supportFieldConfiguration" in values(bare) else {})})

        # cross-filter: an opacity test on __selected__, on the marks drawn one per row
        for label, options in (("with the pair", {}), ("without it", {"cross_highlight": False})):
            r = gen(kind, **options)
            for layer in r["spec"]["layer"]:
                mark, fixed, _ = drawn(layer)
                back = fixed == ds.DIMMED and "opacity" not in layer["encoding"]
                tests, otherwise = opacity(layer)
                selected = [value for test, value in tests if test == SELECTED]
                if mark in ("line", "area"):
                    eq(f"{kind} {label}: a {mark} takes no part in the selection", selected, [])
                elif back:
                    eq(f"{kind} {label}: the dimmed twin stays as it is", (tests, otherwise), ([], ds.DIMMED))
                else:
                    paired = r["spec"]["layer"].index(layer) > 0 and values(r)["enableHighlight"] and mark != "text"
                    eq(f"{kind} {label}: an unselected {mark} " + ("gives way to its twin" if paired else "is dimmed"),
                       selected, [0 if paired else ds.DIMMED])
        off = gen(kind, cross_filter=False)
        check(f"{kind}: cross_filter off removes the test and the selection settings",
              "__selected__" not in text(off["spec"]) and values(off)["enableSelection"] is False
              and setting(off, "selectionMode") is None and setting(off, "selectionMaxDataPoints") is None)
        eq(f"{kind}: cross_filter on brings the mode and the limit",
           (values(full)["selectionMode"], values(full)["selectionMaxDataPoints"]), ("simple", 50))

        # tooltips: one channel shared by the layers; a path opts out
        check(f"{kind}: tooltips on, one shared channel", "tooltip" in full["spec"]["encoding"])
        eq(f"{kind}: only a line or an area opts out of it",
           [drawn(layer)[0] for layer in full["spec"]["layer"] if "tooltip" in layer["encoding"]],
           [m for m, _, _ in layers_of(kind) if m in ("line", "area")])
        check(f"{kind}: tooltips off, no channel at all", "tooltip" not in text(gen(kind, tooltips=False)["spec"]))

        # the context menu is a setting and nothing else
        no_menu = gen(kind, context_menu=False)
        eq(f"{kind}: context_menu off changes the settings, not the spec",
           (no_menu["spec"], values(no_menu)["enableContextMenu"], setting(no_menu, "enableContextMenuSelector")),
           (full["spec"], False, None))
        eq(f"{kind}: with the menu on, the selector that resolves the row is on too",
           values(full)["enableContextMenuSelector"], True)

        # the supporting fields the spec reads
        support = values(full)["supportFieldConfiguration"]
        reads = lambda suffix: sorted({m for m in support if f"{m}{suffix}" in text(full["spec"])})  # noqa: E731
        eq(f"{kind}: the supporting fields listed are the ones the spec reads",
           (sorted(m for m, flags in support.items() if flags["highlight"]),
            sorted(m for m, flags in support.items() if flags["format"])),
           (reads("__highlight"), reads("__format")))

    eq("max_selected reaches the setting and the element",
       (values(gen("bar", max_selected=120))["selectionMaxDataPoints"], gen("bar", max_selected=120)["element"]["max_selected"]),
       (120, 120))
    r = gen("scatter")
    hide = "isDefined(datum['Target__highlight']) && !isValid(datum['Target__highlight'])"
    eq("scatter: the front point is left out for a row outside the highlight, and nothing else changes",
       (opacity(r["spec"]["layer"][1]), opacity(r["spec"]["layer"][0])),
       (([(SELECTED, 0), (hide, 0)], 0.8), ([], ds.DIMMED)))
    r = gen("heatmap")
    eq("heatmap: the front cell is left out where there is no highlighted value",
       opacity(r["spec"]["layer"][1]), ([(SELECTED, 0), ("!isValid(datum['shown_value'])", 0)], 1))

    for kind in ("line", "area"):
        r = gen(kind)
        blob = text([layer for layer in r["spec"]["layer"] if drawn(layer)[0] in ("line", "area")])
        check(f"{kind}: a path names no dataset field and carries no row index, so a click resolves to nothing",
              not any(f'"{name}"' in blob for name in ("Category", "Region", "__row__", "__selected__")))
        eq(f"{kind}: it is drawn from copies of the category and the series",
           r["spec"]["transform"][:2], [{"calculate": "datum['Category']", "as": "path_x"},
                                        {"calculate": "datum['Region']", "as": "path_series"}])
        check(f"{kind}: the note says a click on the path selects nothing",
              any("between the points selects nothing" in n for n in r["notes"]))
    check("a bar's note says what a click elsewhere does",
          any("A click anywhere else" in n and "clears the selection" in n for n in gen("bar")["notes"]))


def test_tooltip():
    print("\n== the tooltip ==")
    blank = "isValid(datum['{0}']) ? datum['{0}'] : '(Blank)'"
    entry = lambda name: f"'{name}': " + blank.format(name)  # noqa: E731
    more = ("isValid(datum['{0}__highlight']) && datum['{0}__highlight'] !== datum['{0}'] ? "
            "{{'{1}': pbiFormat(datum['{0}__highlight'], (datum['{0}__format'] || ''))}} : {{}}")
    eq("bar: every bound field raw under its own name, and the highlighted value when it differs",
       gen("bar")["spec"]["encoding"]["tooltip"],
       {"value": {"expr": "merge({" + entry("Category") + ", " + entry(V) + "}, " + more.format(V, "Highlighted") + ")"}})
    eq("bar without cross-highlight: the fields alone",
       gen("bar", cross_highlight=False)["spec"]["encoding"]["tooltip"],
       {"value": {"expr": "{" + entry("Category") + ", " + entry(V) + "}"}})
    eq("bullet: two measures, each with its own highlighted entry",
       gen("bullet")["spec"]["encoding"]["tooltip"]["value"]["expr"],
       "merge({" + ", ".join(entry(n) for n in ("Category", V, T)) + "}, "
       + more.format(V, f"Highlighted {V}") + ", " + more.format(T, f"Highlighted {T}") + ")")
    for kind in ds.KINDS:
        for facet in (False, True) if kind in FACETED else (False,):
            r = gen(kind, facet=facet, value_format={"x": "0.0", "y": "0.0"} if kind == "scatter" else "0.0")
            expr = view(r["spec"])["encoding"]["tooltip"]["value"]["expr"]
            names = [d["name"] for d in r["dataset"]]
            eq(f"{kind}{' faceted' if facet else ''}: the tooltip lists every bound field once, in binding order",
               re.findall(r"'([^']+)': isValid", expr), names)
            check(f"{kind}{' faceted' if facet else ''}: value_format does not reach the tooltip",
                  "'0.0'" not in expr)
    r = gen("line", category_type="temporal")
    check("a date category is written as a date", "'Category': isValid(datum['Category']) ? "
          "timeFormat(datum['Category'], '%b %d, %Y') : '(Blank)'" in r["spec"]["encoding"]["tooltip"]["value"]["expr"])
    note = next(n for n in gen("bar", value_format="0.0")["notes"] if n.startswith("Tooltips:"))
    check("the note says tooltips keep the model's format, also with value_format",
          "always take the model's format string, also when value_format is given" in note
          and "highlighted value is listed beside it" in note)
    check("and without cross-highlight it does not speak of a highlighted value", "highlighted" not in next(
        n for n in gen("bar", cross_highlight=False)["notes"] if n.startswith("Tooltips:")))
    r = ds.generate_deneb_spec("bar", {"category": "P.Highlighted", "value": PLAIN["value"]})
    check("a field called Highlighted keeps its entry: the extra one gets another key",
          "{'Highlighted ': pbiFormat(" in r["spec"]["encoding"]["tooltip"]["value"]["expr"])


def test_formats():
    print("\n== numbers: axes, labels, legends ==")
    param = {"name": "valueFormat", "expr": "length(data('dataset')) && isString(data('dataset')[0]"
             "['Total Sales__format']) ? data('dataset')[0]['Total Sales__format'] : ''"}
    for kind in ds.KINDS:
        if kind in ("scatter", "heatmap"):
            continue
        v = "y" if kind in VERTICAL + ("line", "area") else "x"
        r = gen(kind)
        eq(f"{kind}: the axis takes the model's format, one unit for all its ticks",
           (r["spec"]["encoding"][v]["axis"]["labelExpr"], r["spec"]["params"][0]),
           (f"pbiFormat(datum.value, valueFormat, {unit(v)})", param))
        r = gen(kind, value_format="#,0.0")
        eq(f"{kind}: a given format is used exactly, with no unit",
           r["spec"]["encoding"][v]["axis"]["labelExpr"], "pbiFormat(datum.value, '#,0.0')")
        check(f"{kind}: and the model's is then not asked for by the axis",
              not any(p["name"] == "valueFormat" for p in r["spec"]["params"]))
    r = gen("scatter")
    eq("scatter: each axis has its own measure's format",
       ([r["spec"]["encoding"][c]["axis"]["labelExpr"] for c in "xy"], [p["name"] for p in r["spec"]["params"]][:2]),
       ([f"pbiFormat(datum.value, xFormat, {unit('x')})", f"pbiFormat(datum.value, yFormat, {unit('y')})"],
        ["xFormat", "yFormat"]))
    check("scatter: the parameters read the right fields", "Total Sales__format" in r["spec"]["params"][0]["expr"]
          and "Target__format" in r["spec"]["params"][1]["expr"])
    r = gen("scatter", value_format={"y": "0.0%"})
    eq("scatter: value_format per role",
       [r["spec"]["encoding"][c]["axis"]["labelExpr"] for c in "xy"],
       [f"pbiFormat(datum.value, xFormat, {unit('x')})", "pbiFormat(datum.value, '0.0%')"])
    eq("heatmap: the legend takes the model's format and one unit",
       gen("heatmap")["spec"]["layer"][0]["encoding"]["color"]["legend"]["labelExpr"],
       f"pbiFormat(datum.value, valueFormat, {unit('color')})")
    r = gen("line", category_type="quantitative")
    eq("a number category is written without separators or decimals (a year)",
       (r["spec"]["encoding"]["x"]["type"], r["spec"]["encoding"]["x"]["axis"]["labelExpr"]),
       ("quantitative", "pbiFormat(datum.value, '')"))
    check("and a date category is left to Vega-Lite", gen("line", category_type="temporal")["spec"]["encoding"]["x"]
          == {"field": "Category", "type": "temporal", "axis": {"title": None}})

    row = "(datum['Total Sales__format'] || '')"
    full = f"isValid(datum['{V}']) ? pbiFormat(datum['{V}'], {row}) : ''"
    for kind, channel in (("bar", "x"), ("lollipop", "x"), ("bullet", "x")):
        label = gen(kind, labels=True)["spec"]["layer"][-1]
        eq(f"{kind}: a label is the value in the model's format, in full; a blank value has none",
           (label["mark"]["type"], label["encoding"]["text"]), ("text", {"value": {"expr": full}}))
        eq(f"{kind}: label_units writes it in the unit of the axis",
           gen(kind, labels=True, label_units=True)["spec"]["layer"][-1]["encoding"]["text"]["value"]["expr"],
           f"isValid(datum['{V}']) ? pbiFormat(datum['{V}'], {row}, {unit(channel)}) : ''")
        eq(f"{kind}: with value_format the label uses it",
           gen(kind, labels=True, value_format="0.0")["spec"]["layer"][-1]["encoding"]["text"]["value"]["expr"],
           f"isValid(datum['{V}']) ? pbiFormat(datum['{V}'], '0.0') : ''")
    eq("column: a label wider than its column is left out",
       gen("column", labels=True)["spec"]["layer"][-1]["encoding"]["text"]["value"]["expr"],
       f"length({full}) * 6 > bandwidth('x') ? '' : {full}")
    eq("column in small multiples with top_n: measured against the panel's own scale",
       view(gen("column", labels=True, facet=True, top_n=2)["spec"])["layer"][-1]["encoding"]["text"]["value"]["expr"],
       f"length({full}) * 6 > bandwidth('child_x') ? '' : {full}")
    eq("heatmap: a label that does not fit its cell is left out",
       gen("heatmap", labels=True)["spec"]["layer"][-1]["encoding"]["text"]["value"]["expr"],
       f"length({full}) * 6 > bandwidth('x') || bandwidth('y') < 12 ? '' : {full}")
    eq("heatmap: label_units takes the unit of the colour scale",
       gen("heatmap", labels=True, label_units=True)["spec"]["layer"][-1]["encoding"]["text"]["value"]["expr"]
       .split(" ? '' : ")[1], f"isValid(datum['{V}']) ? pbiFormat(datum['{V}'], {row}, {unit('color')}) : ''")
    eq("heatmap: the label is white on a dark cell, black on a light or a dimmed one",
       gen("heatmap", labels=True)["spec"]["layer"][-1]["mark"]["color"],
       {"expr": "!isValid(datum['shown_value']) || datum['__selected__'] === 'off' || "
                "luminance(scale('color', datum['shown_value'])) > 0.18 ? 'black' : 'white'"})
    eq("heatmap with nothing interactive: the ink goes by the cell's own colour",
       gen("heatmap", labels=True, **ALL_OFF)["spec"]["layer"][-1]["mark"]["color"],
       {"expr": "luminance(scale('color', datum['Total Sales'])) > 0.18 ? 'black' : 'white'"})

    # where a label stands
    for kind, channel, place in (("bar", "x", {"align": "left", "baseline": "middle", "dx": 4}),
                                 ("column", "y", {"align": "center", "baseline": "bottom", "dy": -4}),
                                 ("lollipop", "x", {"align": "left", "baseline": "middle", "dx": 8})):
        r = gen(kind, labels=True)
        label = r["spec"]["layer"][-1]
        eq(f"{kind}: the label stands past the value end, a negative one at zero",
           ({k: v for k, v in label["mark"].items() if k != "type"}, label["encoding"][channel],
            r["spec"]["transform"][-1]),
           (place, {"field": "label_at"}, {"calculate": f"max(datum['{V}'], 0)", "as": "label_at"}))
        eq(f"{kind}: an unselected label is dimmed, and it has no twin", opacity(label), ([(SELECTED, ds.DIMMED)], 1))
        eq(f"{kind}: with labels the value axis ends at the largest value, so the overhang is the label's own",
           (r["spec"]["encoding"][channel].get("scale"), "scale" in gen(kind)["spec"]["encoding"][channel]),
           ({"nice": False}, False))
    eq("bullet: the label clears the target's tick as well",
       gen("bullet", labels=True)["spec"]["transform"][-1],
       {"calculate": f"max(datum['{V}'], datum['{T}'], 0)", "as": "label_at"})
    reads = lambda r: next((n for n in r["notes"] if n.startswith("The model's format string reaches")), "")  # noqa: E731
    check("the note names what is written with the model's format: axis, labels, highlighted tooltip value",
          "reaches the axis, the labels and a highlighted value in the tooltip through" in reads(gen("bar", labels=True)))
    check("only the axis, with labels and cross-highlight off",
          "reaches the axis through" in reads(gen("bar", cross_highlight=False)))
    check("the legend, for a heatmap", "reaches the legend and a highlighted value in the tooltip through"
          in reads(gen("heatmap")))
    eq("no such note when nothing reads it", reads(gen("bar", value_format="0", cross_highlight=False)), "")


def test_layout():
    print("\n== what depends on the visual's size ==")
    width = {"name": "visualWidth", "expr": "isFinite(containerSize()[0]) ? containerSize()[0] : width"}
    height = {"name": "visualHeight", "expr": "isFinite(containerSize()[1]) ? containerSize()[1] : height"}
    share = "clamp(visualWidth * 0.4, 40, 180)"
    for kind in ds.KINDS:
        r = gen(kind)
        enc, params = r["spec"]["encoding"], r["spec"]["params"]
        check(f"{kind}: the visual's width is asked of the container, not of the fitted plot", width in params)
        labelled = kind in ("bar", "stacked_bar", "lollipop", "bullet", "heatmap")
        beside = f"({share} + 8)" if labelled else "72"
        if labelled:
            eq(f"{kind}: category names beside the plot take at most two fifths of the visual",
               enc["y"]["axis"]["labelLimit"], {"expr": share})
        if kind in VERTICAL + ("line", "area", "heatmap"):
            narrow = f"(visualWidth - {beside}) / max(1, length(domain('x'))) < 48"
            eq(f"{kind}: the categories under the plot lie flat, cut to their slot, or stand upright in a narrow one",
               {k: enc["x"]["axis"][k] for k in ("labelAngle", "labelAlign", "labelBaseline", "labelLimit")},
               {"labelAngle": {"expr": f"{narrow} ? 270 : 0"}, "labelAlign": {"expr": f"{narrow} ? 'right' : 'center'"},
                "labelBaseline": {"expr": f"{narrow} ? 'middle' : 'top'"},
                "labelLimit": {"expr": f"{narrow} ? clamp(visualHeight * 0.3, 30, 90) : "
                                       "width / max(1, length(domain('x')))"}})
            check(f"{kind}: and the height comes from the container too", height in params)
        else:
            check(f"{kind}: no height is asked for", height not in params)
        if "series" in ROLES[kind]:
            eq(f"{kind}: the legend wraps to the rows that fit above the plot",
               enc["color"]["legend"], {"symbolOpacity": 1, "columns": {
                   "expr": f"max(1, floor((visualWidth - {beside}) / 124))"}})
    r = ds.generate_deneb_spec("scatter", {"x": PLAIN["value"], "y": PLAIN["target"]})
    check("a chart with nothing to fit asks for no size", not any(
        p["name"].startswith("visual") for p in r["spec"]["params"]))
    for name in ("temporal", "quantitative"):
        check(f"a {name} axis is left to Vega-Lite's own labels",
              "labelAngle" not in gen("line", category_type=name)["spec"]["encoding"]["x"]["axis"])
    config = gen("bar")["config"]
    eq("config: fitted to the visual; the plot's background is nothing a click can land on",
       (config["autosize"], config["view"]), ({"type": "fit", "contains": "padding"}, {"stroke": None, "fill": None}))
    eq("config: overlapping axis labels are dropped, numbers keep a gap, and one at the end of a horizontal "
       "axis stays inside the plot",
       (config["axis"]["labelOverlap"], config["axisQuantitative"]["labelSeparation"],
        config["axisXQuantitative"], config["axisX"]),
       ("greedy", 6, {"labelFlush": 24}, {"labelAngle": 0}))
    eq("config: the legend sits on top, its labels cut at the width an entry is reckoned at",
       config["legend"], {"orient": "top", "labelFontSize": 11, "labelLimit": 100})
    r = gen("bar", config={"axis": {"labelFontSize": 13}, "background": "transparent"})
    eq("a given config is merged over the standard one",
       (r["config"]["axis"]["labelFontSize"], r["config"]["axis"]["labelOverlap"], r["config"]["background"],
        r["element"]["config"]), (13, "greedy", "transparent", r["config"]))
    check("and the standard one is not changed by it", gen("bar")["config"]["axis"]["labelFontSize"] == 11)


def test_small_multiples():
    print("\n== small multiples ==")
    for kind in FACETED:
        r = gen(kind, facet=True)
        spec = r["spec"]
        eq(f"{kind}: panels split on a copy of the facet column, in the order its rows arrive",
           (spec["transform"][0], spec["facet"]),
           ({"calculate": "datum['Country']", "as": "panel"},
            {"field": "panel", "type": "nominal", "header": {"title": None},
             "sort": {"field": "__row__", "op": "min", "order": "ascending"}}))
        eq(f"{kind}: three panels of 180 x 120 a row by default, sharing their scales",
           (spec["columns"], spec["spacing"], spec["spec"]["width"], spec["spec"]["height"], "resolve" in spec),
           (3, 16, 180, 120, False))
        check(f"{kind}: the view holds the layers; \"fit\" does not apply to a faceted view",
              "layer" not in spec and "layer" in spec["spec"] and "autosize" not in r["config"])
        eq(f"{kind}: the facet is bound first, so Power BI delivers the panels in the model's order",
           r["fields"][0], {"column": "Geo.Country"})
        expected = layers_of(kind)
        if kind in ("line", "area"):                 # a path brings the order of its categories along
            order = ("max", "category_position", "category_position")
            expected = [(m, fixed, dict(source, path=source["path"] + [order]) if "path" in source else source)
                        for m, fixed, source in expected]
        eq(f"{kind}: the same layers on the same fields as the single view",
           [drawn(layer) for layer in spec["spec"]["layer"]], expected)
        note = next((n for n in r["notes"] if n.startswith("Small multiples:")), "")
        check(f"{kind}: the note says the view is not fitted, how to fit it, and what the height does",
              "Pass width" in note and "panel_height whatever the visual's height" in note
              and "the facet is bound first" in note)
    for kind in ("heatmap", "bullet"):
        refused(f"{kind} takes no facet", ["no \"facet\" role"], kind, dict(fields_for(kind), facet="Geo.Country"))

    r = gen("bar", facet=True, top_n=3, width=640)
    eq("top_n: each panel has its own category axis", r["spec"].get("resolve"), {"scale": {"y": "independent"}})
    eq("top_n in columns: the x axis is the one set free",
       gen("column", facet=True, top_n=3)["spec"].get("resolve"), {"scale": {"x": "independent"}})
    eq("width with top_n on bars: every panel pays for its own labels",
       r["spec"]["spec"]["width"], (640 - 40 - 2 * 16 - 3 * 98) // 3)
    eq("width, shared axis, bars: one set of labels a row",
       gen("bar", facet=True, width=640)["spec"]["spec"]["width"], (640 - 40 - 2 * 16 - 98) // 3)
    eq("width, columns: one value axis a row",
       gen("column", facet=True, width=640, facet_columns=4)["spec"]["spec"]["width"], (640 - 40 - 3 * 16 - 72) // 4)
    eq("a given panel_width wins over width", gen("bar", facet=True, width=640, panel_width=77)["spec"]["spec"]["width"], 77)
    eq("panel_height and facet_columns are taken as given",
       [gen("bar", facet=True, panel_height=90, facet_columns=2)["spec"][k] if k == "columns"
        else gen("bar", facet=True, panel_height=90, facet_columns=2)["spec"]["spec"][k] for k in ("columns", "height")],
       [2, 90])
    eq("a panel never gets narrower than 40", gen("bar", facet=True, width=100)["spec"]["spec"]["width"], 40)
    eq("bars: category names in a panel are cut at 90",
       view(gen("bar", facet=True)["spec"])["encoding"]["y"]["axis"]["labelLimit"], 90)
    axis = view(gen("column", facet=True, width=640)["spec"])["encoding"]["x"]["axis"]
    eq("columns: a category's slot is reckoned from the panel's width",
       axis["labelAngle"], {"expr": f"{(640 - 40 - 2 * 16 - 72) // 3} / max(1, length(domain('x'))) < 48 ? 270 : 0"})
    eq("with top_n the slot is counted on the panel's own scale",
       view(gen("column", facet=True, top_n=2)["spec"])["encoding"]["x"]["axis"]["labelAngle"],
       {"expr": "180 / max(1, length(domain('child_x'))) < 48 ? 270 : 0"})
    eq("outside Deneb the visual's width is the given one, or the row of panels",
       ([p["expr"] for p in gen("line", facet=True, width=640)["spec"]["params"] if p["name"] == "visualWidth"],
        [p["expr"] for p in gen("line", facet=True)["spec"]["params"] if p["name"] == "visualWidth"]),
       (["isFinite(containerSize()[0]) ? containerSize()[0] : 640"],
        [f"isFinite(containerSize()[0]) ? containerSize()[0] : {3 * 180 + 40 + 2 * 16 + 72}"]))
    fitted = next((n for n in gen("bar", facet=True, width=640)["notes"] if n.startswith("Small multiples:")), "")
    check("the note says the panels were sized, and what a value sort means on a shared axis",
          "sized to fit the given width" in fitted and "largest value in any panel" in fitted)
    check("and with top_n that each panel has its own top categories",
          "its own top categories" in next((n for n in r["notes"] if n.startswith("Small multiples:")), ""))
    for option in ("facet_columns", "panel_width", "panel_height", "width"):
        refused(f"{option} without a facet", [option, "no \"facet\" field"], "bar", fields_for("bar"), **{option: 3})
        refused(f"{option} must be a whole number", [option, "whole number"], "bar", fields_for("bar", facet=True),
                **{option: 2.5})
    refused("facet_columns has its own ceiling", ["1 to 50"], "bar", fields_for("bar", facet=True), facet_columns=51)


# ---------------------------------------------------------------------------------------------
# names
# ---------------------------------------------------------------------------------------------
def test_field_names():
    print("\n== field names ==")
    for kind in ds.KINDS:
        r = gen(kind, AWKWARD, facet=kind in FACETED, labels=kind in ds._LABEL_KINDS)
        names = {d["role"]: d["name"] for d in r["dataset"]}
        alias = {"x": "category", "y": "series"} if kind == "heatmap" else {"x": "value", "y": "target"}
        eq(f"{kind}: the dataset names follow Deneb's rule", names,
           {role: AWKWARD_DATA[alias.get(role, role)] for role in names})
        derived = {t["as"] if isinstance(t.get("as"), str) else None for t in r["spec"].get("transform", [])}
        fields = set()

        def walk(node):
            if isinstance(node, dict):
                if isinstance(node.get("field"), str):
                    fields.add(node["field"])
                for key in ("groupby",):
                    fields.update(node.get(key, []))
                for value in node.values():
                    walk(value)
            elif isinstance(node, list):
                for value in node:
                    walk(value)

        walk(r["spec"])
        known = set(names.values()) | derived | {"__row__", "__selected__", "stack_start", "stack_end", "path_y",
                                                 "value_rank", "category_position", "y_position", "category_total"}
        bad = [f for f in sorted(fields) if split_access_path(f) != [f.replace("\\'", "'")]
               or f.replace("\\'", "'") not in known]
        eq(f"{kind}: every \"field\" reads one whole name, as Vega-Lite splits it", bad, [])
        blob = text(r["spec"])
        for name in names.values():
            if "'" in name or "\\" in name:
                quoted = "datum['" + name.replace("\\", "\\\\").replace("'", "\\'") + "']"
                check(f"{kind}: {name!r} is escaped inside expressions", json.dumps(quoted, ensure_ascii=False)[1:-1] in blob)
        check(f"{kind}: the note lists the renamed fields", any(n.startswith("Inside the spec") for n in r["notes"]))
    eq("datum() and vl_field()", (ds.datum("a'b\\c"), ds.vl_field("Units' sold")), ("datum['a\\'b\\\\c']", "Units\\' sold"))

    # a bound field that has a name the spec would make for itself
    r = ds.generate_deneb_spec("bullet", {"category": "P.label_at", "value": {"measure": "S.shown_value"},
                                          "target": {"measure": "S.shown_target"}}, labels=True, top_n=2)
    made = [t["as"] for t in r["spec"]["transform"] if isinstance(t.get("as"), str)] + ["value_rank"]
    eq("names the spec makes give way to the bound fields'", sorted(made),
       ["label_at_", "shown_target_", "shown_value_", "value_rank"])
    r = ds.generate_deneb_spec("line", {"category": "P.panel", "series": "P.path_x", "value": {"measure": "S.path_y"},
                                        "facet": "G.path_series"})
    eq("also for the panel and the path", [t["as"] for t in r["spec"]["transform"] if "calculate" in t][:3],
       ["panel_", "path_x_", "path_series_"])
    refused("two fields with one dataset name", ["would both be called", "rename one"], "bar",
            {"category": "A.Name", "value": {"measure": "B.Name"}})
    eq("the same field in two roles is bound once",
       ds.generate_deneb_spec("scatter", {"x": PLAIN["value"], "y": PLAIN["value"]})["fields"], [PLAIN["value"]])
    for name in sorted(ds._OBJECT_MEMBERS):
        refused(f"a field called {name}", [name, "cannot be drawn", "Rename"], "bar",
                {"category": "P.Cat", "value": {"measure": f"S.{name}"}})
    eq("the members refused are the ones every JavaScript object has",
       sorted(ds._OBJECT_MEMBERS), sorted(["constructor", "hasOwnProperty", "isPrototypeOf", "propertyIsEnumerable",
                                           "toLocaleString", "toString", "valueOf", "__proto__", "__defineGetter__",
                                           "__defineSetter__", "__lookupGetter__", "__lookupSetter__"]))


def test_model_fields():
    print("\n== names checked against the model ==")
    shapes = {"lists": {"Product": ["Category", "Price"], "Sales": ["Total Sales"]},
              "objects": {"Product": {"Category": {"kind": "column"}, "Price": {"kind": "column"}},
                          "Sales": {"Total Sales": {"kind": "measure"}}}}
    for label, model in shapes.items():
        r = ds.generate_deneb_spec("bar", {"category": "PRODUCT.category", "value": {"measure": "sales.TOTAL SALES"}},
                                   model_fields=model)
        eq(f"{label}: names are written the way the model spells them, whatever the case given",
           (r["fields"], [d["name"] for d in r["dataset"]], r["spec"]["layer"][0]["encoding"]["x"]["field"]),
           ([{"column": "Product.Category"}, {"measure": "Sales.Total Sales"}], ["Category", V], V))
        check(f"{label}: and the note on unchecked names is gone", not any("not checked" in n for n in r["notes"]))
        refused(f"{label}: a field the model lacks", ["Product[Colour]", "not in the model", "no such field"], "bar",
                {"category": "Product.Colour", "value": {"measure": "Sales.Total Sales"}}, model_fields=model)
        refused(f"{label}: a table the model lacks", ["not in the model", "no such table"], "bar",
                {"category": "Geo.Region", "value": {"measure": "Sales.Total Sales"}}, model_fields=model)
    model = shapes["objects"]
    refused("a column given as a measure", ["is a column in the model, not a measure"], "bar",
            {"category": "Product.Category", "value": {"measure": "Product.Price"}}, model_fields=model)
    refused("a measure in a column's role", ["is a measure", "takes a column"], "bar",
            {"category": "Sales.Total Sales", "value": {"measure": "Sales.Total Sales"}}, model_fields=model)
    r = ds.generate_deneb_spec("scatter", {"x": "product.price", "y": "sales.total sales"}, model_fields=model)
    eq("the model says which of a scatter's fields is a column", r["fields"],
       [{"column": "Product.Price"}, {"measure": "Sales.Total Sales"}])
    refused("lists cannot say that", ["cannot tell whether"], "scatter", {"x": "Product.Price", "y": "Sales.Total Sales"},
            model_fields=shapes["lists"])
    check("without a model the note says the names were not checked, and that case matters",
          any("not checked against the model" in n and "case-sensitive" in n for n in gen("bar")["notes"]))
    refused("model_fields of the wrong shape", ["model_fields must be"], "bar", fields_for("bar"), model_fields=["Sales"])
    refused("a table of the wrong shape", ["model_fields['Sales']"], "bar", fields_for("bar"), model_fields={"Sales": 5})


# ---------------------------------------------------------------------------------------------
# input
# ---------------------------------------------------------------------------------------------
def test_input():
    print("\n== input that cannot be used ==")
    bar = fields_for("bar")
    signature = inspect.signature(ds.generate_deneb_spec).parameters
    keyword_only = [p.name for p in signature.values() if p.kind is p.KEYWORD_ONLY and p.name != "model_fields"]
    eq("OPTIONS is the keyword options, in order; model_fields is the caller's own", list(ds.OPTIONS), keyword_only)
    eq("ARGUMENTS is what a tool may be given", ds.ARGUMENTS, ("kind", "fields") + ds.OPTIONS)
    refused("an option that does not exist", ["unknown option(s) ['legend', 'topN']", "the options are"], "bar", bar,
            topN=3, legend=True)
    refused("an unknown kind", ["unknown kind 'pie'"], "pie", bar)
    refused("a kind that is not text", ["unknown kind"], ["bar"], bar)
    refused("no fields", ["fields must be an object"], "bar", None)
    refused("fields as a list", ["fields must be an object"], "bar", ["Product.Category"])
    refused("a missing role", ["fields.value is required"], "bar", {"category": "Product.Category"})
    refused("a role the kind lacks", ["no \"series\" role"], "bar", dict(bar, series="Geo.Region"))
    for flag in ("labels", "label_units", "cross_filter", "cross_highlight", "tooltips", "context_menu"):
        for value in ("false", 0, 1, None, "yes", []):
            refused(f"{flag}={value!r} is not true or false", [f"{flag} must be true or false"], "bar", bar, **{flag: value})
    field = lambda **ref: {"category": "Product.Category", "value": ref}  # noqa: E731
    refused("a field named twice", ["name the field once", "['column', 'measure']"], "bar",
            field(column="Sales.Amount", measure="Sales.Total"))
    refused("a field with no name", ["name the field once", "neither"], "bar", field(table="Sales"))
    refused("a field with keys it does not have", ["unknown key(s) ['format', 'name']"], "bar",
            field(measure="Sales.Total", name="x", format="0"))
    refused("a field of an unknown kind", ["kind 'banana'"], "bar", field(measure="Sales.Total", kind="banana"))
    refused("a kind that contradicts the key", ["agree with the key"], "bar", field(measure="Sales.Total", kind="column"))
    refused("an aggregated column", ["aggregated column cannot be used", "Create a measure"], "bar",
            field(column="Sales.Amount", aggregation="Sum"))
    refused("a field that is a number", ["not int"], "bar", {"category": "Product.Category", "value": 5})
    refused("a name that is not text", ["must be text"], "bar", field(measure=5))
    refused("a field without a table", ["write it as 'Table.Field'"], "bar", {"category": "Category", "value": bar["value"]})
    refused("a control character in a name", ["control character"], "bar", field(measure="Sales.To\ttal"))
    refused("a column where a measure belongs", ["takes a measure", "create a measure"], "bar",
            {"category": "Product.Category", "value": {"column": "Sales.Amount"}})
    refused("a scatter axis that could be either", ["cannot tell whether"], "scatter", {"x": "S.A", "y": "S.B"})
    eq("\"field\" with a kind, and \"entity\" for the table, are understood",
       ds.generate_deneb_spec("bar", {"category": {"entity": "Product", "field": "Category", "kind": "column"},
                                      "value": {"table": "Sales", "property": "Total Sales"}})["fields"],
       [{"column": "Product.Category"}, {"measure": "Sales.Total Sales"}])

    refused("an unknown sort", ["unknown sort 'biggest'"], "bar", bar, sort="biggest")
    refused("a value sort on a line", ["does not apply to a line chart"], "line", fields_for("line"), sort="value_desc")
    refused("a value sort on a heatmap", ["does not apply to a heatmap chart"], "heatmap", fields_for("heatmap"),
            sort="value_asc")
    refused("any sort on a scatter", ["does not apply to a scatter"], "scatter", fields_for("scatter"), sort="category_asc")
    for value in (0, -1, 2.5, "3", True, float("nan"), float("inf"), 10001):
        refused(f"top_n={value!r}", ["top_n must be a whole number from 1 to 10000"], "bar", bar, top_n=value)
    eq("a whole number that arrives as a float is taken", [t for t in gen("bar", top_n=3.0)["spec"]["transform"]
                                                        if "filter" in t], [{"filter": "datum['value_rank'] <= 3"}])
    refused("top_n on a line", ["top_n does not apply to a line chart"], "line", fields_for("line"), top_n=3)
    for value in (0, 251, 1.5, "50", True):
        refused(f"max_selected={value!r}", ["max_selected must be a whole number from 1 to 250"], "bar", bar,
                max_selected=value)
    refused("labels on a line", ["labels are not drawn on a line chart"], "line", fields_for("line"), labels=True)
    refused("label_units without labels", ["needs labels to be on"], "bar", bar, label_units=True)
    refused("label_units with value_format", ["put the unit in it"], "bar", bar, labels=True, label_units=True,
            value_format="0")
    refused("an unknown category_type", ["unknown category_type 'date'"], "line", fields_for("line"), category_type="date")
    refused("a category_type that is not text", ["unknown category_type"], "line", fields_for("line"), category_type=["ordinal"])
    refused("a temporal category on a bar", ["applies to line and area charts"], "bar", bar, category_type="temporal")
    refused("a sort on a temporal axis", ["ordered by its own values"], "line", fields_for("line"),
            category_type="temporal", sort="category_asc")
    for kind in ds.KINDS:
        check(f"{kind}: category_type=\"ordinal\", the default, is accepted",
              gen(kind, category_type="ordinal")["spec"] == gen(kind)["spec"])
    refused("a title that is not text", ["title must be text"], "bar", bar, title=5)
    refused("a config that is not an object", ["config must be an object"], "bar", bar, config="dark")
    refused("a config that is not JSON", ["config must be plain JSON"], "bar", bar, config={"a": {1, 2}})
    refused("a config with a number JSON lacks", ["config must be plain JSON"], "bar", bar, config={"a": float("nan")})

    refused("value_format of the wrong type", ["value_format must be"], "bar", bar, value_format=5)
    refused("an empty value_format", ["value_format must be"], "bar", bar, value_format="  ")
    refused("a value_format for a role that draws no number", ["value_format.category", "['value']"], "bar", bar,
            value_format={"category": "0"})
    refused("one value_format for a scatter", ["per role"], "scatter", fields_for("scatter"), value_format="0")

    refused("a fixed colour", ["not a theme colour", "stop following the report theme"], "bar", bar, color="#1F6FEB")
    refused("a theme colour index out of range", ["0 to 99"], "bar", bar, color=100)
    refused("a negative index", ["0 to 99"], "bar", bar, color=-1)
    refused("true as a colour", ["not a theme colour"], "bar", bar, color=True)
    refused("an unknown colour name", ["not a theme colour"], "bar", bar, color="brand")
    refused("a colour object with other keys", ["color as an object is"], "bar", bar, color={"theme": 1, "tint": 0.2})
    refused("a shade out of range", ["-1 (darker) to 1 (lighter)"], "bar", bar, color={"theme": 1, "shade": 2})
    refused("a colour beside a series", ["a series is coloured by the theme palette"], "stacked_bar",
            fields_for("stacked_bar"), color=2)
    refused("a heatmap colour that is not a scheme", ["['divergent', 'linear']"], "heatmap", fields_for("heatmap"), color=2)
    eq("a theme colour by index, by name, and shaded",
       [gen("bar", color=c, cross_highlight=False)["spec"]["layer"][0]["mark"]["color"]["expr"]
        for c in (3, "good", {"theme": 2, "shade": -0.25}, {"theme": "bad", "shade": 0.5})],
       ["pbiColor(3)", "pbiColor('good')", "pbiColor(2, -0.25)", "pbiColor('bad', 0.5)"])
    check("an index is given with the warning that the theme must reach it",
          any("Theme colour 3 must exist" in n and "black" in n for n in gen("bar", color=3)["notes"])
          and not any("must exist" in n for n in gen("bar")["notes"] + gen("bar", color="good")["notes"]))


# ---------------------------------------------------------------------------------------------
# settings, the element, the text
# ---------------------------------------------------------------------------------------------
def make_report(base):
    root = os.path.join(base, "Demo.Report")
    os.makedirs(root)
    with io.open(os.path.join(root, "definition.pbir"), "w", encoding="utf-8", newline="\n") as f:
        f.write(json.dumps({"version": "4.0", "datasetReference": {"byPath": {"path": "../Demo.SemanticModel"}}}))
    return root


def test_settings():
    print("\n== the Deneb settings ==")
    r = gen("bar", max_selected=75)
    eq("each setting: where Deneb stores it, the literal PBIR holds, Deneb's default, the element key",
       [(s["path"], s["literal"], s["default"], s["element_key"]) for s in r["interactivity"][:7]],
       [("visual.objects.vega[0].properties.enableTooltips", "true", True, "tooltips"),
        ("visual.objects.vega[0].properties.enableContextMenu", "true", True, "context_menu"),
        ("visual.objects.vega[0].properties.enableContextMenuSelector", "true", True, None),
        ("visual.objects.vega[0].properties.enableSelection", "true", False, "selection"),
        ("visual.objects.vega[0].properties.selectionMode", "'simple'", "simple", None),
        ("visual.objects.vega[0].properties.selectionMaxDataPoints", "75D", 50, "max_selected"),
        ("visual.objects.vega[0].properties.enableHighlight", "true", False, "cross_highlight")])
    support = setting(r, "supportFieldConfiguration")
    eq("the supporting fields, as Deneb's per-field flags",
       (support["path"], support["value"], json.loads(support["literal"][1:-1])),
       ("visual.objects.stateManagement[0].properties.supportFieldConfiguration",
        {V: {"highlight": True, "highlightStatus": False, "highlightComparator": False, "format": True,
             "formatted": False}}, support["value"]))
    eq("the element keys are the report spec's", ds.ELEMENT_KEYS,
       {"enableTooltips": "tooltips", "enableContextMenu": "context_menu", "enableSelection": "selection",
        "selectionMaxDataPoints": "max_selected", "enableHighlight": "cross_highlight"})
    check("the report spec knows every one of them", set(ds.ELEMENT_KEYS.values()) <= set(rs._ELEMENT_KEYS["deneb"])
          if hasattr(rs, "_ELEMENT_KEYS") else True)


def test_element():
    print("\n== the element against the report spec ==")
    with tempfile.TemporaryDirectory() as tmp:
        root = make_report(tmp)
        for kind in ds.KINDS:
            for label, names, facet in (("plain", PLAIN, False), ("awkward names", AWKWARD, kind in FACETED)):
                r = gen(kind, names, facet=facet, title="A title")
                element = r["element"]
                eq(f"{kind}, {label}: the element validates", rs.validate_report_spec(page_with(element)), [])
                # A key the element does not have is only a warning, so the dry run is asked too.
                dry = rs.apply_report_spec(root, page_with(element), dry_run=True)
                eq(f"{kind}, {label}: a dry run accepts it, every key included",
                   (dry["success"], dry["errors"], [w for w in dry["warnings"] if "is not a setting" in w]),
                   (True, [], []))
                check(f"{kind}, {label}: no name and no position",
                      not {"name", "x", "y", "width", "height"} & set(element))
                eq(f"{kind}, {label}: it carries the spec, the config and the fields",
                   (element["type"], element["spec"], element["config"], element["fields"], element["provider"]),
                   ("deneb", r["spec"], r["config"], r["fields"], "vegaLite"))
    on, off = gen("bar", max_selected=75)["element"], gen("bar", **ALL_OFF)["element"]
    eq("the settings the element carries, on", {k: on[k] for k in ds.ELEMENT_KEYS.values()},
       {"tooltips": True, "context_menu": True, "selection": True, "max_selected": 75, "cross_highlight": True})
    eq("and off", {k: off.get(k) for k in ds.ELEMENT_KEYS.values()},
       {"tooltips": False, "context_menu": False, "selection": False, "max_selected": None,
        "cross_highlight": False})
    eq("a title goes on the element, a blank one nowhere",
       (gen("bar", title="Sales")["element"]["title"], "title" in gen("bar", title="  ")["element"],
        "title" in gen("bar", title="Sales")["spec"]), ("Sales", False, False))

    model = {"Product": {"Category": {"kind": "column"}}, "Geo": {"Region": {"kind": "column"}},
             "Sales": {"Total Sales": {"kind": "measure"}}}
    element = gen("stacked_bar")["element"]
    eq("the element validates against the model", rs.validate_report_spec(page_with(element), model_fields=model), [])
    del model["Geo"]
    check("and a field the model lacks is reported", any("Geo[Region]" in e for e in rs.validate_report_spec(
        page_with(element), model_fields=model)))

    with tempfile.TemporaryDirectory() as tmp:
        root = make_report(tmp)
        r = gen("lollipop", AWKWARD, labels=True, max_selected=30, title="Net sales")
        result = rs.apply_report_spec(root, page_with(r["element"]))
        eq("the element applies", (result["success"], result["errors"]), (True, []))
        with io.open(os.path.join(root, "definition", "pages", "p", "visuals", "chart", "visual.json"),
                     encoding="utf-8") as f:
            visual = json.load(f)["visual"]
        vega = visual["objects"]["vega"][0]["properties"]
        literal = lambda prop: vega[prop]["expr"]["Literal"]["Value"]  # noqa: E731
        eq("the stored spec reads back as the generated one (quotes in names included)",
           json.loads(pbip_json.literal_unwrap(literal("jsonSpec"))), r["spec"])
        eq("so does the config", json.loads(pbip_json.literal_unwrap(literal("jsonConfig"))), r["config"])
        stored = [s for s in r["interactivity"] if s["element_key"]]
        eq("the element sets tooltips, the menu, cross-filter with its limit, and cross-highlight",
           [s["property"] for s in stored], ["enableTooltips", "enableContextMenu", "enableSelection",
                                             "selectionMaxDataPoints", "enableHighlight"])
        for s in stored:
            eq(f"{s['property']} is stored where and as the generator says", literal(s["property"]), s["literal"])
        eq("selectionMode is written by the element itself", literal("selectionMode"),
           setting(r, "selectionMode")["literal"])
        check("the visual carries no Deneb version stamp, which is why it gets every supporting field",
              not any("denebMeta" in key for key in vega))
        projections = visual["query"]["queryState"]["dataset"]["projections"]
        eq("the fields are bound in order, under the names the spec uses",
           [ds.deneb_field_name(p["nativeQueryRef"]) for p in projections], [d["name"] for d in r["dataset"]])
        eq("as a column and a measure", [next(iter(p["field"])) for p in projections], ["Column", "Measure"])
        eq("the title is the visual's own", visual["visualContainerObjects"]["title"][0]["properties"]["text"],
           {"expr": {"Literal": {"Value": "'Net sales'"}}})


def test_determinism_and_text():
    print("\n== determinism, and the text a tool returns ==")
    for kind in ds.KINDS:
        options = {"labels": True} if kind in ds._LABEL_KINDS else {}
        fields = fields_for(kind, AWKWARD, kind in FACETED)
        before = copy.deepcopy(fields)
        first = ds.generate_deneb_spec(kind, fields, **options)
        second = ds.generate_deneb_spec(kind, fields, **options)
        eq(f"{kind}: the same input gives the same output, byte for byte",
           json.dumps(first, ensure_ascii=False), json.dumps(second, ensure_ascii=False))
        eq(f"{kind}: the input is not changed", fields, before)
        first["element"]["spec"]["data"]["name"] = "changed"
        check(f"{kind}: the element holds its own copy of the spec", first["spec"]["data"]["name"] == "dataset")
        eq(f"{kind}: the result's keys", sorted(second),
           ["config", "dataset", "element", "fields", "interactivity", "kind", "notes", "provider", "spec"])
    r = gen("stacked_bar", AWKWARD, top_n=3)
    out = ds.to_text(r)
    check("the text names the kind, the fields with their dataset names, the settings, the notes and the element",
          out.startswith("=== Deneb spec: stacked_bar (vegaLite) ===") and "Product _Group_" in out
          and "vega.enableSelection = true  (element key \"selection\")" in out
          and "vega.selectionMode = 'simple'  (Deneb's default)" in out
          and all(f"  - {note}" in out for note in r["notes"]))
    eq("its last line is the element, as JSON", json.loads(out.rstrip("\n").rsplit("\n", 1)[1]), r["element"])
    with io.open(os.path.join(ROOT, "src", "deneb_specs.py"), encoding="utf-8") as f:
        source = f.read()
    eq("the module: no line over 110 columns",
       [n for n, line in enumerate(source.split("\n"), 1) if len(line) > 110], [])
    check("the module: plain ASCII, so no invisible character can hide in it", all(ord(ch) < 127 for ch in source))


# ---------------------------------------------------------------------------------------------
# drawn: compiled by Vega-Lite and rendered by Vega, in Node.js
# ---------------------------------------------------------------------------------------------
RENDER_JS = r"""
// Compile each case the way Deneb prepares a Vega-Lite spec, render it headless, and print what
// was drawn: every mark item with its geometry, the row a click on it resolves to and the
// tooltip it hands over; the axes, the legends, the groups a click could land on, the scales.
//   node render.mjs <cases.json> <node_modules>
import fs from 'node:fs';
import path from 'node:path';
import { pathToFileURL } from 'node:url';

const [casesFile, modules] = process.argv.slice(2);
const load = (name) => {
  const dir = path.join(modules, name);
  const entry = JSON.parse(fs.readFileSync(path.join(dir, 'package.json'), 'utf8')).exports['.'];
  return import(pathToFileURL(path.join(dir, typeof entry === 'string' ? entry : entry.default)).href);
};
const version = (name) => JSON.parse(fs.readFileSync(path.join(modules, name, 'package.json'), 'utf8')).version;
const vega = await load('vega');
const vegaLite = await load('vega-lite');

// Stand-ins for what Deneb registers: a theme of plain colours, and a formatter that writes
// what it was asked for as "<format>|<unit>|<value>".
const PALETTE = ['#118DFF', '#12239E', '#E66C37', '#6B007B', '#E044A7', '#744EC2', '#D9B300', '#D64550'];
vega.expressionFunction('pbiColor', (value) => PALETTE[parseInt(`${value}`, 10) || 0]);
vega.scheme('pbiColorNominal', PALETTE);
vega.scheme('pbiColorLinear', ['#DEEFFF', '#0B3C6E']);
vega.scheme('pbiColorDivergent', ['#DEEFFF', '#C8C8C8', '#0B3C6E']);
const unit = (v) => (typeof v !== 'number' ? '' : Math.abs(v) >= 1e6 ? 'M' : Math.abs(v) >= 1e3 ? 'K' : '');
const isObject = (v) => v !== null && typeof v === 'object';
vega.expressionFunction('pbiFormat', (value, format, options = {}) => `${isObject(format) ? '' : format}|${unit(options.value)}|${value}`);
// Without a canvas Vega takes every character for 0.8 em; Segoe UI sets them about this wide.
const charWidth = (ch) => (/[il.,:;'|!]/.test(ch) ? 0.24 : /[ fjrt()\-\[\]]/.test(ch) ? 0.33 : /[0-9$]/.test(ch) ? 0.54
  : /[mwMW%@]/.test(ch) ? 0.82 : /[A-Z&+]/.test(ch) ? 0.63 : /[a-z]/.test(ch) ? 0.51 : 0.6);
vega.textMetrics.width = (item, text) => {
  const size = item.fontSize != null ? +item.fontSize || 0 : 11;
  const width = [...(text == null ? '' : `${text}`.trim())].reduce((sum, ch) => sum + charWidth(ch), 0) * size;
  return item.limit > 0 ? Math.min(width, +item.limit) : width;
};

// ---- Deneb: interactivity/data-point.ts
const ROW = '__row__';
const validRow = (value, n) => typeof value === 'number' && Number.isInteger(value) && value >= 0 && value < n;
const rowNumbers = (data, n) => {
  const out = [];
  data.forEach((d) => { const r = d[ROW]; if ((n === undefined ? r !== undefined : validRow(r, n)) && !out.includes(r)) out.push(r); });
  return out;
};
const matched = (data, fields, values) => {
  const wanted = {};
  for (const key of Object.keys(fields)) if (key in (data[0] ?? {})) wanted[key] = fields[key].numeric ? Number.parseFloat(data[0][key]) : data[0][key];
  return values.filter((row) => Object.entries(wanted).every(([k, v]) => row[k] === v));
};
function resolveRows(data, fields, values) {
  if (!data || data.length === 0) return [];
  if (data.length === 1 && validRow(data[0]?.[ROW], values.length)) return [data[0][ROW]];
  if (data.every((d) => Object.prototype.hasOwnProperty.call(d, ROW))) {
    const rows = rowNumbers(data, values.length);
    if (rows.length > 0) return rows;
  }
  const known = Object.fromEntries(Object.entries(fields).filter(([k]) => Object.keys(data[0] || {}).includes(k)));
  let found = matched(data, known, values);
  if (!found.length) found = matched(data, Object.fromEntries(Object.entries(known).filter(([, f]) => !f.isMeasure)), values);
  if (found.length === values.length) return [];
  return rowNumbers(found);
}
const datumOf = (item) => (item?.context?.data?.facet?.values?.value ? item.context.data.facet.values.value.slice() : [{ ...item?.datum }]);
// ---- Deneb: interactivity/tooltip.ts. A number under a dataset field's name is formatted by
// Deneb with the model's format ("auto"); anything else is shown as the spec gave it.
const shownTooltip = (value, fields) => Object.entries(JSON.parse(JSON.stringify(value))).map(([key, val]) =>
  [key, key in fields && fields[key].numeric && !isNaN(val) ? 'auto' : 'as given', val]);

const round = (v) => (typeof v === 'number' ? Math.round(v * 100) / 100 : v);
const out = { versions: { vega: version('vega'), 'vega-lite': version('vega-lite') }, cases: [] };
for (const c of JSON.parse(fs.readFileSync(casesFile, 'utf8'))) {
  const entry = { id: c.id, warnings: [], marks: [], axes: [], legends: [], groups: [], scales: {} };
  try {
    const spec = structuredClone(c.spec);
    // Deneb: a single or layered view is sized to its container (patch-vega-lite.ts).
    if (!spec.facet) { spec.width ??= 'container'; spec.height ??= 'container'; }
    spec.params = [...(spec.params || []), { name: 'denebContainer', value: { width: c.width, height: c.height } }];
    spec.datasets = { dataset: structuredClone(c.rows) };
    spec.config = { background: 'transparent', customFormatTypes: true, ...structuredClone(c.config) };
    const note = (kind) => (...a) => { entry.warnings.push(`${kind}: ${a.join(' ')}`); return logger; };
    const logger = { level() { return logger; }, error: note('vega-lite error'), warn: note('vega-lite warning'), info() { return logger; }, debug() { return logger; } };
    const { spec: vg } = vegaLite.compile(spec, { logger });
    const view = new vega.View(vega.parse(vg), { renderer: 'none' });
    view.logger({ level() { return this; }, error: note('vega error'), warn: note('vega warning'), info() {}, debug() {} });
    view._el = { clientWidth: c.width, clientHeight: c.height, style: {} };    // what containerSize() reads
    await view.runAsync();
    (function names(node) {
      for (const s of node.scales || []) { try { entry.scales[s.name] = view.scale(s.name).domain(); } catch (e) { entry.scales[s.name] = null; } }
      for (const m of node.marks || []) if (m.type === 'group') names(m);
    })(vg);
    const fields = c.fields, values = view.data('dataset');
    const reach = { x1: Infinity, y1: Infinity, x2: -Infinity, y2: -Infinity };
    (function walk(node, panel, dx, dy) {
      if (!node.marktype) return;
      if (node.marktype === 'group') {
        for (const item of node.items || []) {
          const pickable = node.interactive !== false && !!(item.fill || item.stroke);
          if (node.role === 'scope' || node.role === 'frame') entry.groups.push({ role: node.role, pickable, click: pickable ? resolveRows(datumOf(item), fields, values) : null });
          const key = node.role === 'scope' && item.datum ? Object.values(item.datum)[0] : panel;
          if (node.role === 'axis') entry.axes.push({ scale: item.datum && item.datum.scale, orient: item.orient, panel: key, labels: [], angles: [] });
          if (node.role === 'legend') entry.legends.push({ labels: [], title: null });
          for (const child of item.items || []) walk(child, key, dx + (item.x || 0), dy + (item.y || 0));
        }
        return;
      }
      for (const item of node.items) {      // what can be seen, for the reach of the drawing
        const b = item.bounds;
        if (item.opacity === 0 || !b || !isFinite(b.x1) || (node.marktype === 'text' && !item.text)) continue;
        reach.x1 = Math.min(reach.x1, dx + b.x1); reach.y1 = Math.min(reach.y1, dy + b.y1);
        reach.x2 = Math.max(reach.x2, dx + b.x2); reach.y2 = Math.max(reach.y2, dy + b.y2);
      }
      if (node.role === 'mark') {
        const isPath = node.marktype === 'line' || node.marktype === 'area';
        const picked = isPath && node.items.length ? resolveRows(datumOf(node.items[0]), fields, values) : null;   // a path is picked as its first item
        entry.marks.push({ name: node.name, type: node.marktype, panel, items: node.items.map((i) => {
          const d = i.datum || {};
          const o = { row: d[ROW], named: Object.keys(d).filter((k) => k in fields), click: isPath ? picked : resolveRows(datumOf(i), fields, values) };
          for (const k of ['x', 'y', 'width', 'height', 'x2', 'y2', 'size', 'opacity', 'fill', 'stroke', 'text', 'align']) if (i[k] !== undefined) o[k] = round(i[k]);
          if (i.tooltip != null) o.tooltip = shownTooltip(i.tooltip, fields);
          return o;
        }) });
      } else if (node.role === 'axis-label') {
        const axis = entry.axes.at(-1);
        axis.labels = node.items.filter((i) => i.opacity !== 0).map((i) => i.text); axis.angles = node.items.map((i) => i.angle || 0);
      } else if (node.role === 'legend-label') { entry.legends.at(-1).labels.push(...node.items.map((i) => i.text));
      } else if (node.role === 'legend-title') { entry.legends.at(-1).title = node.items[0] && node.items[0].text; }
    })(view.scenegraph().root, null, 0, 0);
    const pad = view.padding(), [ox, oy] = view.origin();
    entry.room = [c.width - pad.left - pad.right, c.height - pad.top - pad.bottom];
    entry.reach = [round(ox + reach.x1), round(oy + reach.y1), round(ox + reach.x2), round(oy + reach.y2)];
    entry.plot = spec.facet ? null : [round(view.width()), round(view.height())];
    view.finalize();
  } catch (e) {
    entry.error = String((e && e.stack) || e).split('\n').slice(0, 2).join(' | ');
  }
  out.cases.push(entry);
}
process.stdout.write(JSON.stringify(out));
"""
MODULES_VARIABLE = "DENEB_VEGA_MODULES"
FORMATS = {V: "#,0", T: "0.0", U: "0"}
CATEGORIES = ["Bikes", "Clothing", "Parts", "Helmets"]
REGIONS = ["North", "South"]
COUNTRIES = ["UK", "France"]


def vega_modules():
    """The node_modules folder that holds vega and vega-lite, or None."""
    given = os.environ.get(MODULES_VARIABLE)
    for base in ([given] if given else []) + [ROOT]:
        for folder in (base, os.path.join(base, "node_modules")):
            if all(os.path.isfile(os.path.join(folder, name, "package.json")) for name in ("vega", "vega-lite")):
                return folder
    return None


def sample_rows(result, highlight=None, selected=None, bare=False):
    """Rows as Deneb hands them to the spec: the bound fields under their dataset names, with
    __row__, and, as the settings ask, __selected__, <measure>__highlight and __format."""
    settings = values(result)
    columns = [d for d in result["dataset"] if d["kind"] == "column"]
    measures = [d["name"] for d in result["dataset"] if d["kind"] == "measure"]
    domain = {"category": CATEGORIES, "series": REGIONS, "facet": COUNTRIES, "x": CATEGORIES, "y": REGIONS}
    combos = [[]]
    for d in columns:
        combos = [combo + [value] for combo in combos for value in domain[d["role"]]]
    rows = []
    for i, combo in enumerate(combos):
        row = {"__row__": i}
        if settings["enableSelection"] and not bare:
            row["__selected__"] = selected(i) if selected else "neutral"
        row.update({d["name"]: value for d, value in zip(columns, combo)})
        for n, m in enumerate(measures):
            # distinct, never in order of the categories, and different for each measure
            row[m] = ((i * 7 + 3) % 11 + 2) * 150 + n * 475 - (i % 3) * 20
            if not bare:
                if settings["enableHighlight"]:
                    row[m + "__highlight"] = highlight(i, row[m]) if highlight else row[m]
                row[m + "__format"] = FORMATS.get(m, "0.00")
        rows.append(row)
    return rows


def render(cases):
    """Run the cases through Node; {id: what was drawn}, or None when it cannot be done here."""
    node, modules = shutil.which("node"), vega_modules()
    if not node or not modules:
        why = "node is not on PATH" if not node else (
            f"the vega and vega-lite packages were not found; name their folder in {MODULES_VARIABLE}")
        print(f"  [SKIP] {len(cases)} charts compiled by Vega-Lite and drawn by Vega ({why})")
        return None
    with tempfile.TemporaryDirectory() as tmp:
        script, data = os.path.join(tmp, "render.mjs"), os.path.join(tmp, "cases.json")
        with io.open(script, "w", encoding="utf-8", newline="\n") as f:
            f.write(RENDER_JS)
        with io.open(data, "w", encoding="utf-8", newline="\n") as f:
            json.dump(cases, f, ensure_ascii=False)
        done = subprocess.run([node, script, data, modules], capture_output=True)
    if done.returncode != 0 or not done.stdout:
        check("the Node renderer runs", False, done.stderr.decode("utf-8", "replace")[-1500:])
        return None
    out = json.loads(done.stdout.decode("utf-8"))
    print(f"  (vega {out['versions']['vega']}, vega-lite {out['versions']['vega-lite']}, from {modules})")
    return {entry["id"]: entry for entry in out["cases"]}


def case(case_id, result, rows, width=520, height=360):
    fields = {d["name"]: {"numeric": d["kind"] == "measure", "isMeasure": d["kind"] == "measure"}
              for d in result["dataset"]}
    return {"id": case_id, "spec": result["spec"], "config": result["config"], "rows": rows, "fields": fields,
            "width": width, "height": height}


def js_number(value):
    """A number as JavaScript writes it: 375, not 375.0."""
    return str(int(value)) if float(value).is_integer() else repr(float(value))


def near(a, b, tolerance=0.02):
    return abs(a - b) <= tolerance * max(1.0, abs(a), abs(b))


def proportional(pairs_):
    """True when every (pixels, value) pair has the same pixels per unit of value."""
    rates = [pixels / value for pixels, value in pairs_ if value]
    return bool(rates) and all(near(rate, rates[0]) for rate in rates)


def length(item, channel):
    """How far a bar or a rule reaches along a channel, from Vega's item."""
    if channel == "x":
        return item["width"] if "width" in item else abs(item["x2"] - item["x"])
    return item["height"] if "height" in item else abs(item["y2"] - item["y"])


def test_drawn():
    print("\n== drawn: compiled by Vega-Lite, rendered by Vega, resolved the way Deneb does ==")
    half = lambda i, value: value * 0.5 if i % 2 == 0 else None  # noqa: E731
    cases, then = [], []

    def add(case_id, result, rows, verify, **size):
        cases.append(case(case_id, result, rows, **size))
        then.append((case_id, result, rows, verify))

    def common(cid, e, r, rows, scenario):
        settings = values(r)
        row_marks = [m for m in e["marks"] if m["type"] not in ("line", "area")]
        paths = [m for m in e["marks"] if m["type"] in ("line", "area")]
        kept = {i["row"] for m in row_marks for i in m["items"]}
        check(f"{cid}: every mark drawn for a row resolves a click to that row, and to no other",
              bool(row_marks) and all(i["click"] == [i["row"]] for m in row_marks for i in m["items"]),
              str([(m["name"], [i["click"] for i in m["items"]][:4]) for m in row_marks][:3]))
        check(f"{cid}: a line or an area resolves a click to nothing",
              all(not i["named"] and i.get("row") is None and i["click"] == [] for m in paths for i in m["items"]))
        check(f"{cid}: no panel or plot background can be clicked", all(not g["pickable"] for g in e["groups"]))
        if settings["enableTooltips"]:
            names = [d["name"] for d in r["dataset"]]
            measures = [d["name"] for d in r["dataset"] if d["kind"] == "measure"]
            tips = [i["tooltip"] for m in row_marks for i in m["items"]]
            check(f"{cid}: every such mark hands over each bound field, a measure as a raw number for Deneb "
                  "to format", all([t[0] for t in tip][:len(names)] == names and all(
                      how == "auto" and value == rows[i["row"]][key] for key, how, value in tip if key in measures)
                      for m in row_marks for i, tip in ((i, i["tooltip"]) for i in m["items"])), str(tips[:1]))
            extra = {i["row"]: [t for t in i["tooltip"] if t[0].startswith("Highlighted")]
                     for m in row_marks for i in m["items"]}
            if scenario == "highlighted" and settings["enableHighlight"]:
                want = {row: [f"{FORMATS.get(m, '0.00')}||{js_number(rows[row][m + '__highlight'])}" for m in measures]
                        if rows[row][measures[0] + "__highlight"] is not None else [] for row in kept}
                eq(f"{cid}: a highlighted row lists its highlighted value too, in the model's format",
                   {row: [t[2] for t in entries] for row, entries in extra.items()}, want)
            else:
                check(f"{cid}: no highlighted entry without a highlight", not any(extra.values()))
            check(f"{cid}: a path has no tooltip", all("tooltip" not in i for m in paths for i in m["items"]))
        else:
            check(f"{cid}: no tooltip anywhere", all("tooltip" not in i for m in e["marks"] for i in m["items"]))
        if scenario == "selected":
            off = [i for m in row_marks for i in m["items"] if rows[i["row"]]["__selected__"] == "off"]
            on = [i for m in row_marks for i in m["items"] if rows[i["row"]]["__selected__"] == "on"]
            check(f"{cid}: unselected rows are dimmed or give way, selected ones are drawn in full",
                  off and on and all(i["opacity"] <= ds.DIMMED for i in off)
                  and all(any(i["opacity"] >= 0.8 for i in on if i["row"] == row) for row in {i["row"] for i in on}))
        labels = [t for a in e["axes"] for t in a["labels"]] + [t for g in e["legends"] for t in g["labels"]]
        texts = [i["text"] for m in e["marks"] if m["type"] == "text" for i in m["items"]]
        check(f"{cid}: no text reads undefined, null or NaN",
              not any(w in str(t) for t in labels + texts for w in ("undefined", "null", "NaN")), str((labels + texts)[:6]))
        if e["plot"]:
            x1, y1, x2, y2 = e["reach"]
            over = max(-x1, -y1, x2 - e["room"][0], y2 - e["room"][1])
            check(f"{cid}: all of it is inside the visual", over <= 5 and min(e["plot"]) >= 40,
                  f"reach {e['reach']} in {e['room']}, plot {e['plot']}")

    def front(e, mark, nth=0):
        """The items of the nth mark of a type among the front layers (the last half), by row."""
        found = [m for m in e["marks"] if m["type"] == mark]
        chosen = found[len(found) // 2:][nth] if len(found) > 1 else found[0]
        return {i["row"]: i for i in chosen["items"]}

    def back(e, mark, nth=0):
        return {i["row"]: i for i in [m for m in e["marks"] if m["type"] == mark][nth]["items"]}

    scenarios = {"neutral": {}, "bare": {"bare": True}, "highlighted": {"highlight": half},
                 "selected": {"selected": lambda i: "on" if i % 2 else "off"}}

    def bars(kind, channel):
        def verify(cid, e, r, rows, scenario):
            name = next(d["name"] for d in r["dataset"] if d["role"] == "value")
            full = back(e, "rect")
            check(f"{cid}: a bar is as long as its value", len(full) == len(rows) and proportional(
                [(length(i, channel), rows[row][name]) for row, i in full.items()]))
            other = "y" if channel == "x" else "x"
            order = [row for row, _ in sorted(full.items(), key=lambda kv: kv[1][other])]
            by_value = sorted(full, key=lambda row: -rows[row][name])
            eq(f"{cid}: largest first" + (" (left to right)" if channel == "y" else " (top down)"), order, by_value)
            if scenario == "highlighted":
                shown = front(e, "rect")
                check(f"{cid}: in front, the highlighted rows at their highlighted length, the others not drawn",
                      all(near(length(shown[row], channel), length(full[row], channel) * 0.5) for row in full if row % 2 == 0)
                      and all(row not in shown or not length(shown[row], channel) for row in full if row % 2))
        return verify

    def stacked(kind, channel):
        def verify(cid, e, r, rows, scenario):
            names = {d["role"]: d["name"] for d in r["dataset"]}
            full, shown = back(e, "rect"), front(e, "rect")
            start = lambda i: i[channel] if channel == "x" else i["y"] + i["height"]  # noqa: E731  (a column grows upward)
            by_category = {}
            for row, item in full.items():
                by_category.setdefault(rows[row][names["category"]], []).append((rows[row][names["series"]], row, item))
            ok = True
            for members in by_category.values():
                members.sort()                       # in series order, each segment starts where the last ended
                edge = start(members[0][2])
                for _, row, item in members:
                    ok = ok and near(start(item), edge, 0.005)
                    edge += length(item, channel) * (1 if channel == "x" else -1)
            check(f"{cid}: segments stack in series order, each on the one before", ok)
            check(f"{cid}: a segment is as long as its value (not a share of the whole)", proportional(
                [(length(i, channel), rows[row][names["value"]]) for row, i in full.items()]))
            if scenario == "highlighted":
                check(f"{cid}: the highlighted part starts where its own segment starts",
                      all(near(start(shown[row]), start(full[row]), 0.005)
                          and near(length(shown[row], channel), length(full[row], channel) * 0.5)
                          for row in full if row % 2 == 0))
        return verify

    def trend(kind):
        def verify(cid, e, r, rows, scenario):
            names = {d["role"]: d["name"] for d in r["dataset"]}
            points = back(e, "symbol")
            zero = max(i["y2"] for m in e["marks"] if m["type"] == "area" for i in m["items"]) if kind == "area" else None
            for mark in ("line",) + (("area",) if kind == "area" else ()):
                path = [m for m in e["marks"] if m["type"] == mark][0]
                check(f"{cid}: the {mark} passes through every point (series overlap, none stacked)",
                      len(points) == len(rows) and {(round(i["x"]), round(i["y"])) for m in e["marks"]
                                                    if m["name"] == path["name"] for i in m["items"]}
                      >= {(round(i["x"]), round(i["y"])) for i in points.values()})
            xs = {}
            for row, item in points.items():
                xs.setdefault(rows[row][names["category"]], set()).add(round(item["x"]))
            check(f"{cid}: the points of one category stand on one x, in the order delivered",
                  all(len(v) == 1 for v in xs.values())
                  and [c for c, _ in sorted(xs.items(), key=lambda kv: min(kv[1]))] == CATEGORIES)
            lowest, highest = min(points, key=lambda row: rows[row][names["value"]]), max(points, key=lambda row: rows[row][names["value"]])
            check(f"{cid}: a larger value stands higher", points[highest]["y"] < points[lowest]["y"])
            if zero is not None:
                check(f"{cid}: the area's height is its value", proportional(
                    [(zero - i["y"], rows[row][names["value"]]) for row, i in points.items()]))
        return verify

    def scatter(cid, e, r, rows, scenario):
        names = {d["role"]: d["name"] for d in r["dataset"]}
        points = back(e, "symbol")
        a, b = sorted(points)[0], sorted(points)[-1]
        rate = lambda c, role: (points[b][c] - points[a][c]) / (rows[b][names[role]] - rows[a][names[role]])  # noqa: E731
        check(f"{cid}: x follows the x measure and y the y measure",
              all(near(points[row]["x"] - points[a]["x"], rate("x", "x") * (rows[row][names["x"]] - rows[a][names["x"]]), 0.01)
                  and near(points[row]["y"] - points[a]["y"], rate("y", "y") * (rows[row][names["y"]] - rows[a][names["y"]]), 0.01)
                  for row in points) and rate("x", "x") > 0 > rate("y", "y"))
        sizes = sorted(points, key=lambda row: rows[row][names["size"]])
        check(f"{cid}: a larger size measure draws a larger point",
              all(points[lo]["size"] <= points[hi]["size"] for lo, hi in zip(sizes, sizes[1:]))
              and points[sizes[0]]["size"] < points[sizes[-1]]["size"])
        check(f"{cid}: neither axis is stretched down to zero", e["scales"]["x"][0] > 0 and e["scales"]["y"][0] > 0)
        if scenario == "highlighted":
            shown = front(e, "symbol")
            check(f"{cid}: under a highlight no point moves; the rows outside it show only their dimmed twin",
                  all((shown[row]["x"], shown[row]["y"]) == (points[row]["x"], points[row]["y"]) for row in points)
                  and all(shown[row]["opacity"] == (0.8 if row % 2 == 0 else 0) for row in points))

    def heatmap(cid, e, r, rows, scenario):
        names = {d["role"]: d["name"] for d in r["dataset"]}
        cells = back(e, "rect")
        columns = {rows[row][names["x"]]: set() for row in cells}
        lines = {rows[row][names["y"]]: set() for row in cells}
        for row, item in cells.items():
            columns[rows[row][names["x"]]].add(item["x"])
            lines[rows[row][names["y"]]].add(item["y"])
        check(f"{cid}: the first column runs across and the second down, both in the order delivered",
              all(len(v) == 1 for v in list(columns.values()) + list(lines.values()))
              and sorted(columns, key=lambda k: min(columns[k])) == CATEGORIES
              and sorted(lines, key=lambda k: min(lines[k])) == REGIONS)
        low, high = min(cells, key=lambda row: rows[row][names["value"]]), max(cells, key=lambda row: rows[row][names["value"]])
        eq(f"{cid}: the smallest and the largest value take the two ends of the theme's gradient",
           (cells[low]["fill"], cells[high]["fill"]), ("rgb(222, 239, 255)", "rgb(11, 60, 110)"))
        if scenario == "highlighted":
            shown = front(e, "rect")
            eq(f"{cid}: the colour scale stays on the full values", e["scales"]["color"],
               [min(rows[row][names["value"]] for row in cells), max(rows[row][names["value"]] for row in cells)])
            check(f"{cid}: the front cell takes its highlighted value's colour, or is not shown",
                  all(shown[row]["opacity"] == (1 if row % 2 == 0 else 0) for row in cells)
                  and any(shown[row].get("fill") != cells[row]["fill"] for row in cells if row % 2 == 0))

    def lollipop(cid, e, r, rows, scenario):
        name = next(d["name"] for d in r["dataset"] if d["role"] == "value")
        stems, heads = back(e, "rule"), back(e, "symbol")
        check(f"{cid}: a stem runs from zero to its value, and the head sits on its end",
              proportional([(length(i, "x"), rows[row][name]) for row, i in stems.items()])
              and len({min(i["x"], i["x2"]) for i in stems.values()}) == 1
              and all(near(heads[row]["x"], max(stems[row]["x"], stems[row]["x2"]), 0.005) for row in stems))

    def bullet(cid, e, r, rows, scenario):
        names = {d["role"]: d["name"] for d in r["dataset"]}
        barsdrawn, ticks = back(e, "rect", 0), back(e, "rect", 1)
        origin = min(i["x"] for i in barsdrawn.values())
        check(f"{cid}: the bar is the actual and the tick stands at the target",
              proportional([(i["width"], rows[row][names["value"]]) for row, i in barsdrawn.items()])
              and proportional([(i["x"] + i["width"] / 2 - origin, rows[row][names["target"]]) for row, i in ticks.items()])
              and all(i["width"] <= 2 for i in ticks.values()))
        if scenario == "highlighted":
            shown = [m for m in e["marks"] if m["type"] == "rect"][3]
            check(f"{cid}: the target is highlighted with the actual",
                  all(near(i["x"] + i["width"] / 2 - origin,
                           (ticks[i["row"]]["x"] + ticks[i["row"]]["width"] / 2 - origin) * 0.5)
                      for i in shown["items"] if i["row"] % 2 == 0) and len(shown["items"]) >= len(rows) // 2)

    verifiers = {"bar": bars("bar", "x"), "column": bars("column", "y"), "stacked_bar": stacked("stacked_bar", "x"),
                 "stacked_column": stacked("stacked_column", "y"), "line": trend("line"), "area": trend("area"),
                 "scatter": scatter, "heatmap": heatmap, "lollipop": lollipop, "bullet": bullet}
    for kind in ds.KINDS:
        for names, label in ((PLAIN, "plain"), (AWKWARD, "awkward names")):
            r = gen(kind, names)
            for scenario, how in scenarios.items():
                if label == "awkward names" and scenario in ("bare", "selected"):
                    continue
                rows = sample_rows(r, **how)
                add(f"{kind}, {label}, {scenario}", r, rows,
                    lambda cid, e, r, rows, scenario=scenario, kind=kind: (
                        common(cid, e, r, rows, scenario), verifiers[kind](cid, e, r, rows, scenario)))
        r = gen(kind, **ALL_OFF)
        add(f"{kind}, nothing interactive", r, sample_rows(r),
            lambda cid, e, r, rows, kind=kind: (common(cid, e, r, rows, "neutral"), verifiers[kind](cid, e, r, rows, "neutral")))
        r = gen(kind, labels=True) if kind in ds._LABEL_KINDS else gen(kind)
        add(f"{kind}, in a small visual", r, sample_rows(r, highlight=half),
            lambda cid, e, r, rows: common(cid, e, r, rows, "highlighted"), width=224, height=290)

    # numbers as text: the stand-in formatter writes "<format>|<unit>|<value>"
    def labelled(cid, e, r, rows, _=None):
        texts = {i["row"]: i["text"] for m in e["marks"] if m["type"] == "text" for i in m["items"]}
        eq(f"{cid}: each label is its row's value in the model's format, in full",
           texts, {row["__row__"]: f"#,0||{row[V]}" for row in rows})
        axis = next(a for a in e["axes"] if a["scale"] == "x" and a["labels"])
        check(f"{cid}: the axis writes every tick in the model's format with one unit",
              all(t.startswith("#,0|K|") for t in axis["labels"]) and len(axis["labels"]) >= 2, str(axis["labels"]))
        ends = {i["row"]: i for i in back(e, "rect").values()}
        check(f"{cid}: a label starts where its bar ends",
              all(near(i["x"], ends[i["row"]]["x"] + ends[i["row"]]["width"], 0.005)
                  for m in e["marks"] if m["type"] == "text" for i in m["items"]))
    r = gen("bar", labels=True)
    add("bar with labels", r, sample_rows(r), labelled)

    def units(cid, e, r, rows):
        texts = [i["text"] for m in e["marks"] if m["type"] == "text" for i in m["items"]]
        check(f"{cid}: label_units gives every label the one unit of the axis", texts and all(
            t.split("|")[1] == "K" for t in texts), str(texts))
    r = gen("lollipop", labels=True, label_units=True)
    add("lollipop with label_units", r, sample_rows(r), units)

    def given(cid, e, r, rows):
        axis = next(a for a in e["axes"] if a["scale"] == "y" and a["labels"])
        check(f"{cid}: value_format is used as given, with no unit", all(t.startswith("0.0%||") for t in axis["labels"]))
        tip = e["marks"][0]["items"][0]["tooltip"]
        check(f"{cid}: and the tooltip still hands Deneb the raw number", [how for key, how, _ in tip if key == V] == ["auto"])
    r = gen("column", value_format="0.0%")
    add("column with value_format", r, sample_rows(r), given)

    def small(cid, e, r, rows):
        tall = [a for a in e["axes"] if a["orient"] == "bottom" and a["labels"]]
        check(f"{cid}: under a narrow plot the categories stand upright and are written whole",
              tall and all(angle == 270 for a in tall for angle in a["angles"])
              and all(set(a["labels"]) == set(CATEGORIES) for a in tall), str(tall))
        check(f"{cid}: every legend entry is listed", [g["labels"] for g in e["legends"] if g["labels"]] == [REGIONS])
    r = gen("stacked_column")
    add("stacked_column, 224 px wide", r, sample_rows(r), small, width=224, height=290)

    def wide(cid, e, r, rows):
        flat = [a for a in e["axes"] if a["orient"] == "bottom" and a["labels"]]
        check(f"{cid}: under a wide plot they lie flat", flat and all(angle == 0 for a in flat for angle in a["angles"]))
    add("stacked_column, 520 px wide", r, sample_rows(r), wide)

    def sorted_line(order):
        def verify(cid, e, r, rows):
            names = {d["role"]: d["name"] for d in r["dataset"]}
            for mark in [m for m in e["marks"] if m["type"] == "symbol"][:1] + [m for m in e["marks"] if m["type"] == "line"][:1]:
                key = names["category"] if mark["type"] == "symbol" else None
                across = sorted({(round(i["x"]), rows[i["row"]][key] if key else None) for i in mark["items"]})
                if key:
                    eq(f"{cid}: the categories run {order}", list(dict.fromkeys(c for _, c in across)),
                       sorted(CATEGORIES, reverse=order == "descending"))
                else:
                    check(f"{cid}: and the line stands on the same places", {x for x, _ in across} == {
                        round(i["x"]) for m in e["marks"] if m["type"] == "symbol" for i in m["items"]})
        return verify
    for kind in ("line", "area"):
        for order in ("ascending", "descending"):
            r = gen(kind, sort="category_" + order[:-6])
            add(f"{kind} sorted by category, {order}", r, sample_rows(r), sorted_line(order))
    r = gen("line", facet=True, sort="category_desc")
    add("line, small multiples, sorted by category, descending", r, sample_rows(r), sorted_line("descending"))

    # top_n: the rows that are drawn
    def top(role, total=False):
        def verify(cid, e, r, rows):
            names = {d["role"]: d["name"] for d in r["dataset"]}
            score = {}
            for row in rows:
                key = (row.get(names.get("facet")), row[names["category"]])
                score[key] = score.get(key, 0) + row[names[role]] if total else max(score.get(key, 0), row[names[role]])
            keep = set()
            for panel in {key[0] for key in score}:
                ranked = sorted((k for k in score if k[0] == panel), key=lambda k: -score[k])[:2]
                keep.update(ranked)
            drawn_ = {(rows[i["row"]].get(names.get("facet")), rows[i["row"]][names["category"]])
                      for m in e["marks"] if m["type"] == "rect" or m["type"] == "rule" for i in m["items"]}
            eq(f"{cid}: the two largest categories" + (" of each panel" if "facet" in names else "") + " are drawn",
               drawn_, keep)
        return verify
    for kind, facet, total in (("bar", False, False), ("lollipop", False, False), ("bullet", False, False),
                               ("stacked_bar", False, True), ("column", True, False), ("stacked_column", True, True)):
        r = gen(kind, facet=facet, top_n=2)
        add(f"{kind}{' faceted' if facet else ''}, top 2", r, sample_rows(r), top("value", total))

    # small multiples
    def panels(cid, e, r, rows):
        common(cid, e, r, rows, "neutral")
        eq(f"{cid}: one set of marks a panel, panels in the order their rows arrive",
           list(dict.fromkeys(m["panel"] for m in e["marks"] if m["type"] not in ("line", "area"))), COUNTRIES)
        check(f"{cid}: with width given, a row of panels fits the visual", e["reach"][2] <= e["room"][0] + 5,
              str(e["reach"]))
    for kind in FACETED:
        r = gen(kind, facet=True, width=640)
        add(f"{kind}, small multiples", r, sample_rows(r), panels, width=640, height=420)

    def own_axes(cid, e, r, rows):
        axes = [a for a in e["axes"] if a["scale"] == "child_y" and a["labels"]]
        check(f"{cid}: with top_n every panel lists its own categories", len(axes) == 2 and all(
            len(a["labels"]) == 2 for a in axes), str(axes))
    r = gen("bar", facet=True, top_n=2, width=640)
    add("bar, small multiples with top_n", r, sample_rows(r), own_axes, width=640, height=420)

    drawn_cases = render(cases)
    if drawn_cases is None:
        return
    for case_id, result, rows, verify in then:
        e = drawn_cases[case_id]
        # Vega says "Infinite extent" of a field with no number in it, which a highlight that
        # touches none of the rows produces; nothing else is let through.
        problems = [w for w in e["warnings"] if 'Infinite extent for field "shown_' not in w]
        if e.get("error") or problems:
            check(f"{case_id}: compiles and renders without an error or a warning", False,
                  (e.get("error") or "") + " " + " ; ".join(problems[:2]))
            continue
        check(f"{case_id}: compiles and renders without an error or a warning", True)
        try:
            verify(case_id, e, result, rows)
        except Exception as exc:  # a check that cannot run has not passed
            check(f"{case_id}: could be measured", False, f"{type(exc).__name__}: {exc}")


def main():
    test_channels()
    test_stack_and_rank()
    test_each_option_owns_its_parts()
    test_tooltip()
    test_formats()
    test_layout()
    test_small_multiples()
    test_field_names()
    test_model_fields()
    test_input()
    test_settings()
    test_element()
    test_determinism_and_text()
    test_drawn()
    print()
    if _failures:
        print(f"{len(_failures)} check(s) FAILED:")
        for name in _failures:
            print(f"  - {name}")
        sys.exit(1)
    print("All checks passed.")


if __name__ == "__main__":
    main()
