"""
Tests for the SVG micro-visual DAX measure generators. Each generated measure is checked three
ways: the DAX lexes and is clean under our own linter; the string concatenation is evaluated
here (literals joined, VARs substituted) and the resulting data URI is decoded and parsed as
XML; and the blank-handling and validation contracts hold. No Power BI required.
Run: python tests/test_svg_measures.py
"""
import os
import sys
import urllib.parse
import xml.dom.minidom

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))
import svg_measures  # noqa: E402
import dax_lint  # noqa: E402
import dax_tokens as dt  # noqa: E402

_failures = []


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f": {detail}" if detail and not cond else ""))
    if not cond:
        _failures.append(name)


def raises(fn, *needles):
    try:
        fn()
    except ValueError as e:
        return all(n in str(e) for n in needles)
    except Exception:
        return False
    return False


def var_body(dax, var):
    """Source text of 'VAR <var> = ...' up to the next top-level VAR / RETURN line."""
    lines = dax.split("\n")
    start = next(i for i, ln in enumerate(lines) if ln.startswith(f"VAR {var} ="))
    body = [lines[start].split("=", 1)[1]]
    for ln in lines[start + 1:]:
        if ln.startswith("VAR ") or ln.startswith("RETURN"):
            break
        body.append(ln)
    return "\n".join(body)


def concat(expr, values):
    """Evaluate a DAX string concatenation: string literals as written, names from ``values``.
    Anything else (IF, NOT, ISBLANK, parentheses, '&') is structure and contributes nothing."""
    out = []
    for t in dt.tokenize(expr):
        if t.kind == dt.STRING:
            out.append(t.text[1:-1].replace('""', '"'))
        elif t.kind == dt.IDENT and t.text in values:
            out.append(str(values[t.text]))
    return "".join(out)


def render(dax, values):
    """The data URI the measure would return for the given VAR values, and its parsed SVG."""
    values = dict(values)
    if "VAR _marker =" in dax:
        values["_marker"] = concat(var_body(dax, "_marker"), {k: v for k, v in values.items() if k == "_tx"})
    uri = concat(var_body(dax, "_svg"), values)
    assert uri.startswith(svg_measures.URI_PREFIX), uri[:40]
    payload = uri[len(svg_measures.URI_PREFIX):]
    return uri, xml.dom.minidom.parseString(urllib.parse.unquote(payload)).documentElement


def lint_ids(dax):
    return {f["rule_id"] for f in dax_lint.lint_expression("svg", dax)}


def test_each_kind():
    print("\n== each kind emits a well-formed data URI ==")
    prog = svg_measures.generate("progress", value_measure="Margin %", max_value=1)
    uri, svg = render(prog["dax"], {"_w": 40})
    check("progress references measure", "VAR _v = [Margin %]" in prog["dax"])
    check("progress: parses as SVG with track + fill rects", svg.tagName == "svg" and len(svg.getElementsByTagName("rect")) == 2)
    check("progress: the fill width is the computed value", svg.getElementsByTagName("rect")[1].getAttribute("width") == "40")
    check("progress: size attributes", svg.getAttribute("width") == "100" and svg.getAttribute("height") == "16")

    bul = svg_measures.generate("bullet", value_measure="Sales", target_measure="Target", max_value=1000)
    uri, svg = render(bul["dax"], {"_vw": 60, "_tx": 90})
    rects = svg.getElementsByTagName("rect")
    check("bullet references both measures", "VAR _v = [Sales]" in bul["dax"] and "VAR _t = [Target]" in bul["dax"])
    check("bullet: track, value bar and target marker", len(rects) == 3 and rects[1].getAttribute("width") == "60"
          and rects[2].getAttribute("x") == "90" and rects[2].getAttribute("width") == "2")
    check("bullet: marker stays on the canvas at 100%", "* 118, 0 )" in bul["dax"])

    pill = svg_measures.generate("status_pill", value_measure="KPI")
    uri, svg = render(pill["dax"], {"_color": "%23D64550", "_label": "Low"})
    check("pill uses SWITCH for color + label", pill["dax"].count("SWITCH( TRUE()") == 2)
    check("pill: rect takes the colour, text takes the label",
          svg.getElementsByTagName("rect")[0].getAttribute("fill") == "#D64550"
          and svg.getElementsByTagName("text")[0].firstChild.data == "Low")

    spark = svg_measures.generate("sparkline", axis_column="'Date'[Month]", value_measure="Sales")
    uri, svg = render(spark["dax"], {"_points": "20,260 600,140 1180,20"})
    line = svg.getElementsByTagName("polyline")[0]
    check("sparkline references axis", "VALUES( 'Date'[Month] )" in spark["dax"])
    check("sparkline: polyline carries the points", line.getAttribute("points") == "20,260 600,140 1180,20"
          and line.getAttribute("stroke") == "#118DFF")
    check("sparkline: 10x viewBox for whole-number coordinates", svg.getAttribute("viewBox") == "0 0 1200 280"
          and svg.getAttribute("width") == "120")
    for kind, d in (("progress", prog), ("bullet", bul), ("status_pill", pill), ("sparkline", spark)):
        check(f"{kind}: balanced DAX string literals", all(t.text.endswith('"') and len(t.text) >= 2
              for t in dt.tokenize(d["dax"]) if t.kind == dt.STRING))
        check(f"{kind}: balanced parentheses", d["dax"].count("(") == d["dax"].count(")"))


def test_blank_handling():
    print("\n== defect 22: BLANK in, BLANK out ==")
    cases = {
        "progress": (svg_measures.progress_bar("M"), "IF( NOT ISBLANK( _v ), _svg )"),
        "bullet": (svg_measures.bullet("M", "T", 10), "IF( NOT ISBLANK( _v ), _svg )"),
        "status_pill": (svg_measures.status_pill("M"), "IF( NOT ISBLANK( _v ), _svg )"),
        "sparkline": (svg_measures.sparkline("'D'[M]", "M"), "IF( NOT ISEMPTY( _data ), _svg )"),
    }
    for kind, (dax, guard) in cases.items():
        check(f"{kind}: returns the SVG only when there is data", dax.rstrip().endswith("RETURN\n    " + guard), dax[-80:])
        check(f"{kind}: IF has no else branch (so the result is BLANK)", ", _svg )" in dax and "_svg, " not in dax)
    check("bullet: no marker when the target is blank", "VAR _marker = IF( NOT ISBLANK( _t )," in cases["bullet"][0])
    check("sparkline: points without a value are skipped", "NOT ISBLANK( [@y] )" in cases["sparkline"][0])
    check("sparkline: a flat or single-point line is centred, not divided by zero",
          cases["sparkline"][0].count(", 0.5 )") == 2)
    check("notes tell the user about BLANK", "BLANK" in svg_measures.generate("progress", value_measure="M")["notes"])


def test_xml_and_uri_escaping():
    print("\n== defect 23 / 25: labels are XML-escaped, # and % are URI-escaped ==")
    for label in ("R&D", "<50%", "a > b", "100% #1", 'say "hi"', "it's", "%20", "&amp;"):
        dax = svg_measures.status_pill("KPI", thresholds=[{"max": None, "color": "#3FA34D", "label": label}])
        literal = next(t.text for t in dt.tokenize(var_body(dax, "_label")) if t.kind == dt.STRING)
        value = literal[1:-1].replace('""', '"')
        try:
            uri, svg = render(dax, {"_color": "%233FA34D", "_label": value})
            shown = svg.getElementsByTagName("text")[0].firstChild.data
        except Exception as e:  # noqa: BLE001 - a parse failure is the defect being tested
            shown = f"XML ERROR {e}"
        check(f"label {label!r} survives the round trip", shown == label, shown)
    dax = svg_measures.status_pill("KPI")
    check("no raw # anywhere in the generated measure", "#" not in dax, dax)
    check("colours are written %23RRGGBB", '"%23D64550"' in dax and "fill='%23FFFFFF'" in dax)
    check("the ;utf8, prefix is kept", svg_measures.URI_PREFIX == "data:image/svg+xml;utf8," and '"data:image/svg+xml;utf8,"' in dax)
    for dax in (svg_measures.progress_bar("M"), svg_measures.bullet("M", "T", 5), svg_measures.sparkline("D.M", "M")):
        check("no raw # in " + dax.split("\n")[0], "#" not in dax)
    pct = svg_measures.status_pill("KPI", thresholds=[{"max": None, "color": "red", "label": "50%"}])
    check("% in a label is written %25", '"50%25"' in pct, pct)


def test_measure_references():
    print("\n== defect 25: measure references are escaped through dax_tokens ==")
    ref = svg_measures._measure_ref
    check("plain name", ref("Total Sales") == "[Total Sales]")
    check("bracketed reference passes through", ref(" [Total Sales] ") == "[Total Sales]")
    check("] in a name is doubled", ref("Sales [Net]") == "[Sales [Net]]]" and ref("A]B") == "[A]]B]")
    check("escaped reference passes through", ref("[A]]B]") == "[A]]B]")
    check("table-qualified measure", ref("Sales[Total]") == "Sales[Total]" and ref("'Sales Data'[Total]") == "'Sales Data'[Total]")
    check("parentheses in a name are fine", ref("Sales (USD)") == "[Sales (USD)]")
    check("an aggregation is not a measure name", raises(lambda: ref("SUM(Sales[Amount])"), "not a measure name"))
    check("an expression is not a measure name", raises(lambda: ref("[x] + 1"), "not a measure name"))
    check("empty / non-text", raises(lambda: ref(" "), "required") and raises(lambda: ref(None), "required"))
    dax = svg_measures.progress_bar("Sales [Net]")
    check("the generated DAX lexes back to the measure name",
          [t.name for t in dt.tokenize(dax) if t.kind == dt.BRACKET] == ["Sales [Net]"], dax.split("\n")[0])


def test_parameter_validation():
    print("\n== defect 25: numeric and colour parameters are validated ==")
    check("max_value text", raises(lambda: svg_measures.progress_bar("M", max_value="abc"), "max_value", "number"))
    check("width that is DAX, not a number", raises(lambda: svg_measures.progress_bar("M", width="100) + [Secret] + (1"), "width"))
    check("width zero / negative / fractional", raises(lambda: svg_measures.progress_bar("M", width=0), "width")
          and raises(lambda: svg_measures.progress_bar("M", width=-5), "width")
          and raises(lambda: svg_measures.progress_bar("M", width=10.5), "whole number"))
    check("boolean is not a number", raises(lambda: svg_measures.progress_bar("M", height=True), "height"))
    check("NaN / infinity", raises(lambda: svg_measures.progress_bar("M", max_value=float("nan")), "max_value")
          and raises(lambda: svg_measures.bullet("M", "T", float("inf")), "max_value"))
    check("max must exceed min", raises(lambda: svg_measures.progress_bar("M", max_value=1, min_value=1), "greater than min_value"))
    check("bullet max must be positive", raises(lambda: svg_measures.bullet("M", "T", 0), "greater than 0"))
    check("colour injection is rejected", raises(lambda: svg_measures.progress_bar("M", fill="red' onload='x"), "fill", "colour"))
    check("empty colour is rejected", raises(lambda: svg_measures.sparkline("D.M", "M", stroke=""), "stroke"))
    for good in ("#fff", "#118DFF", "#118DFF80", "red", "rgb(17, 141, 255)", "hsl(210 100% 50% / 0.5)"):
        try:
            svg_measures.progress_bar("M", fill=good)
            ok = True
        except ValueError:
            ok = False
        check(f"colour {good!r} is accepted", ok)
    check("numeric strings are accepted as numbers", "* 80, 0 )" in svg_measures.progress_bar("M", width="80", max_value="2.5"))
    check("numbers are written without exponent notation",
          "DIVIDE( _v, 0.00001 )" in svg_measures.progress_bar("M", max_value=1e-05)
          and "DIVIDE( _v - 10, 90 )" in svg_measures.progress_bar("M", min_value=10, max_value=100))
    check("pad too large", raises(lambda: svg_measures.sparkline("D.M", "M", width=3, height=3, pad=2), "pad"))
    check("thresholds must ascend", raises(lambda: svg_measures.status_pill("M", thresholds=[
        {"max": 0.8, "color": "red"}, {"max": 0.5, "color": "red"}]), "ascending"))
    check("the max = null band must be last", raises(lambda: svg_measures.status_pill("M", thresholds=[
        {"max": None, "color": "green"}, {"max": 0.5, "color": "red"}]), "last"))
    check("threshold colour is validated", raises(lambda: svg_measures.status_pill("M", thresholds=[
        {"max": None, "color": "'/><script>"}]), "thresholds[0].color"))
    check("threshold max is validated", raises(lambda: svg_measures.status_pill("M", thresholds=[
        {"max": "high", "color": "red"}]), "thresholds[0].max"))
    check("thresholds must be a list of objects", raises(lambda: svg_measures.status_pill("M", thresholds=["red"]), "object")
          and raises(lambda: svg_measures.status_pill("M", thresholds=[]), "non-empty"))
    no_top = svg_measures.status_pill("M", thresholds=[{"max": 0.5, "color": "red", "label": "L"},
                                                       {"max": 0.8, "color": "orange", "label": "H"}])
    check("without a max = null band the last band is the else branch",
          var_body(no_top, "_color").replace(" ", "").replace("\n", "") == 'SWITCH(TRUE(),_v<=0.5,"red","orange")', var_body(no_top, "_color"))
    single = svg_measures.generate("status_pill", value_measure="M", thresholds=[{"max": None, "color": "#3FA34D", "label": "OK"}])["dax"]
    check("a single band needs no SWITCH", "SWITCH( TRUE()" not in single and 'VAR _color = "%233FA34D"' in single)


def test_sparkline_sort_column():
    print("\n== defect 24: a sort column that is in row context ==")
    dax = svg_measures.sparkline("'Date'[Month Name]", "Sales", sort_column="'Date'[Month No]")
    check("the axis is the distinct (axis, sort) pairs",
          "VAR _axis = SUMMARIZE( 'Date', 'Date'[Month Name], 'Date'[Month No] )" in dax, dax.split("\n")[0])
    check("the rank reads the sort column from that row context", "RANKX( _axis, 'Date'[Month No], , ASC, DENSE )" in dax)
    check("no extension column inside SUMMARIZE (so no DL007)", "DL007" not in lint_ids(dax))
    same = svg_measures.sparkline("Date.Month", "Sales", sort_column="'Date'[Month]")
    check("sorting by the axis itself keeps the simple form", "VAR _axis = VALUES( 'Date'[Month] )" in same)
    check("default orders by the axis", "RANKX( _axis, 'Date'[Month], , ASC, DENSE )" in svg_measures.sparkline("Date.Month", "S"))
    check("Table.Column spelling is accepted like the measure generator's", "VALUES( 'Date'[Month] )" in svg_measures.sparkline("Date.Month", "S"))
    check("quoting and escaping of awkward names",
          "SUMMARIZE( 'O''Brien', 'O''Brien'[Mon]]th], 'O''Brien'[No] )" in svg_measures.sparkline("O'Brien.Mon]th", "S", sort_column="O'Brien.No"))
    check("sort column in another table is rejected",
          raises(lambda: svg_measures.sparkline("Date.Month", "S", sort_column="Other.No"), "same table"))
    check("axis without a table is rejected", raises(lambda: svg_measures.sparkline("Month", "S"), "axis_column", "has no table"))
    check("points are plotted in rank order with whole-number coordinates",
          "[@x], ASC" in dax and "ROUND(" in dax and ", 1 )" not in dax)


def test_lint_clean():
    print("\n== generated DAX is clean under our own linter ==")
    for kind, kw in [("progress", {"value_measure": "M", "max_value": 1}),
                     ("bullet", {"value_measure": "M", "target_measure": "T", "max_value": 10}),
                     ("status_pill", {"value_measure": "M"}),
                     ("sparkline", {"axis_column": "'D'[M]", "value_measure": "M"}),
                     ("sparkline", {"axis_column": "D.M", "value_measure": "Net -- 6\" [x]", "sort_column": "D.N"})]:
        dax = svg_measures.generate(kind, **kw)["dax"]
        check(f"{kind}: no lint finding of any severity", lint_ids(dax) == set(), str(lint_ids(dax)))


def test_dispatch_and_errors():
    print("\n== dispatch + error handling ==")
    check("status alias maps to status_pill", svg_measures.generate("status", value_measure="M")["kind"] == "status_pill")
    check("custom name preserved", svg_measures.generate("progress", name="Bar", value_measure="M", max_value=1)["name"] == "Bar")
    check("result keys", set(svg_measures.generate("progress", value_measure="M")) == {"name", "kind", "dax", "notes"})
    check("unknown kind raises", raises(lambda: svg_measures.generate("piechart", value_measure="M"), "Unknown SVG measure kind"))
    try:
        svg_measures.generate("progress", value_measure="M", bogus=1)
        check("unexpected parameter is a TypeError (the server reports it)", False)
    except TypeError:
        check("unexpected parameter is a TypeError (the server reports it)", True)


if __name__ == "__main__":
    print("=" * 70)
    print("  SVG MEASURE GENERATOR TESTS")
    print("=" * 70)
    test_each_kind()
    test_blank_handling()
    test_xml_and_uri_escaping()
    test_measure_references()
    test_parameter_validation()
    test_sparkline_sort_column()
    test_lint_clean()
    test_dispatch_and_errors()
    print("\n" + "=" * 70)
    if _failures:
        print(f"  {len(_failures)} CHECK(S) FAILED: {', '.join(_failures)}")
        sys.exit(1)
    print("  ALL SVG MEASURE CHECKS PASSED")
    print("=" * 70)
