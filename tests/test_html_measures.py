"""
Tests for src/html_measures.py: every kind emits well-formed, lint-clean DAX with balanced
markup, references built through dax_tokens, model text HTML-escaped inside the DAX, blank
handling, and validated style parameters. The DAX is never evaluated; no Power BI required.
Run: python tests/test_html_measures.py
"""
import os
import re
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))
import dax_lint  # noqa: E402
import dax_tokens as dt  # noqa: E402
import html_measures as hm  # noqa: E402

_failures = []


def check(name, cond, detail=""):
    line = f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f": {detail}" if detail and not cond else "")
    print(line.encode("ascii", "backslashreplace").decode("ascii"))
    if not cond:
        _failures.append(name)


def eq(name, actual, expected):
    check(name, actual == expected, f"got {actual!r}, expected {expected!r}")


def raises(name, fn, fragment=""):
    try:
        fn()
    except ValueError as e:
        check(name, fragment.lower() in str(e).lower(), f"message was {e}")
    else:
        check(name, False, "no ValueError raised")


def literals(dax):
    """The text of every DAX string literal, in order."""
    return [t.text[1:-1].replace('""', '"') for t in dt.tokenize(dax) if t.kind == dt.STRING]


def well_formed(dax):
    depth = 0
    for tok in dt.tokenize(dax):
        if tok.kind == dt.STRING and (len(tok.text) < 2 or not tok.text.endswith('"')):
            return False
        if tok.kind == dt.OP and tok.text in "()":
            depth += 1 if tok.text == "(" else -1
            if depth < 0:
                return False
    return depth == 0


def balanced_markup(dax):
    """Each element opened in the literals is closed in them. Every conditional branch in
    these generators is balanced on its own, so the totals must agree."""
    text = "".join(literals(dax))
    for tag in ("div", "span", "b", "table", "thead", "tbody", "tr", "th", "td"):
        opened = len(re.findall(rf"<{tag}[ >]", text))
        closed = text.count(f"</{tag}>")
        if opened != closed:
            return f"<{tag}> opened {opened}, closed {closed}"
    return ""


def sound(label, dax):
    check(f"{label}: well-formed DAX", well_formed(dax), dax)
    eq(f"{label}: clean under the DAX linter", dax_lint.lint_expression(label, dax), [])
    eq(f"{label}: markup is balanced", balanced_markup(dax), "")
    check(f"{label}: ASCII only, no backslash", all(ch == "\n" or 32 <= ord(ch) < 127 for ch in dax) and chr(92) not in dax)
    check(f"{label}: no string literal spans two lines", all(line.count('"') % 2 == 0 for line in dax.split("\n")))
    attrs = re.findall(r"style=(.)", "".join(literals(dax)))
    check(f"{label}: attributes are single-quoted", attrs and set(attrs) == {"'"}, attrs[:5])


def test_kpi_card():
    print("\n== kpi card ==")
    dax = hm.kpi_card("Total Sales", label="Sales <net> & more", format_string="#,0")
    sound("kpi_card", dax)
    check("reads the measure once", dax.count("[Total Sales]") == 1, dax)
    check("the label is escaped at build time", "Sales &lt;net&gt; &amp; more" in dax and "<net>" not in dax)
    check("blank shows the placeholder", 'IF ( ISBLANK ( _v0 ), "n/a", FORMAT ( _v0, "#,0" ) )' in dax)
    check("no comparison block when none is asked for", "_delta0" not in dax)

    cmp_dax = hm.kpi_card("[Margin %]", format_string="0.0%", comparison_measure="Margin % PY",
                          comparison_label="vs last year", good_colour="#008000", bad_colour="#CC0000")
    sound("kpi_card with comparison", cmp_dax)
    check("the change divides safely", "DIVIDE ( _v0 - _c0, ABS ( _c0 ) )" in cmp_dax)
    check("no comparison value hides the change line", 'ISBLANK ( _delta0 ),\n        "",' in cmp_dax, cmp_dax)
    check("good and bad colours follow the sign", 'IF ( _delta0 >= 0, "#008000", "#CC0000" )' in cmp_dax)
    check("arrows are written by code point", "UNICHAR ( 9650 )" in cmp_dax and "UNICHAR ( 9660 )" in cmp_dax)
    check("the default label is the measure name", ">Margin %</div>" in cmp_dax)
    lower = hm.kpi_card("Defects", comparison_measure="Defects PY", higher_is_better=False)
    check("lower-is-better flips the colour test only", "IF ( _delta0 <= 0," in lower and "IF ( _delta0 >= 0, UNICHAR ( 9650 )" in lower)

    odd = hm.kpi_card("Net]Sales", format_string='0 "<units>"')
    sound("kpi_card with awkward names", odd)
    check("a ] in the measure name is doubled", "[Net]]Sales]" in odd)
    check("a format string that can write markup is escaped",
          'SUBSTITUTE ( FORMAT ( _v0, "0 ""<units>""" ), "&", "&amp;" )' in odd, odd)
    custom = hm.kpi_card("X", blank_text="<none>", accent="rgb(10, 20, 30)", font_family="Inter, Arial")
    check("blank text is escaped", '"&lt;none&gt;"' in custom)
    check("style parameters reach the markup", "border-left:4px solid rgb(10, 20, 30)" in custom and "font-family:Inter, Arial" in custom)


def test_kpi_strip():
    print("\n== kpi strip ==")
    dax = hm.kpi_strip([{"measure": "Total Sales", "label": "Sales"},
                        {"measure": "Margin %", "format_string": "0.0%", "comparison_measure": "Margin % PY"},
                        {"measure": "Orders"}], gap=8)
    sound("kpi_strip", dax)
    check("one tile per item", dax.count("flex:1 1 0") == 3 and "gap:8px" in dax)
    check("each item has its own variables", all(v in dax for v in ("_v0", "_v1", "_v2", "_delta1")) and "_delta0" not in dax)
    raises("an empty strip is refused", lambda: hm.kpi_strip([]), "at least one")
    raises("an item needs a measure", lambda: hm.kpi_strip([{"label": "x"}]), "needs a measure")


def test_headline():
    print("\n== headline ==")
    dax = hm.headline("Sales reached {sales} in {year}: {sales} is {growth} above <last> year. {{sic}}",
                      {"sales": {"measure": "Total Sales", "format_string": "$#,0"},
                       "year": {"measure": "Selected Year", "text": True},
                       "growth": {"measure": "Growth %", "format_string": "0.0%"}})
    sound("headline", dax)
    check("a repeated placeholder reads its measure once", dax.count("[Total Sales]") == 1 and dax.count("& _h0 &") == 2, dax)
    check("the sentence text is escaped", "above &lt;last&gt; year. {sic}" in dax)
    check("a text measure is escaped in DAX, & first",
          'SUBSTITUTE ( SUBSTITUTE ( _h1v, "&", "&amp;" ), "<", "&lt;" )' in dax, dax)
    check("figures are emphasised", dax.count("<b style='font-weight:600;color:#2563EB'>") == 4)
    raises("an unknown placeholder", lambda: hm.headline("{a} {b}", {"a": {"measure": "A"}}), "matching entry")
    raises("no placeholder at all", lambda: hm.headline("static text", {}), "nothing live")
    raises("an unused value", lambda: hm.headline("{a}", {"a": {"measure": "A"}, "b": {"measure": "B"}}), "never uses")
    raises("a format specification is not a name", lambda: hm.headline("{a:>10}", {"a": {"measure": "A"}}), "plain names")
    raises("an unbalanced brace", lambda: hm.headline("{a", {"a": {"measure": "A"}}), "literal braces")


def test_bar_list():
    print("\n== bar list ==")
    dax = hm.bar_list("'Dim Product'[Category]", "Total Sales", top_n=5, title="Top <5>", bar_colour="#0A0")
    sound("bar_list", dax)
    check("rows with a blank value are left out", 'NOT ISBLANK ( [@v] )' in dax)
    check("top N with a stable tie-break", "TOPN ( 5, _base, [@v], DESC, 'Dim Product'[Category], ASC )" in dax)
    check("the width is a clamped whole-number percentage",
          'FORMAT ( ROUND ( MIN ( MAX ( DIVIDE ( [@v], _max ), 0 ), 1 ) * 100, 0 ), "0" )' in dax)
    check("the label is escaped in DAX and blank-safe",
          "IF ( ISBLANK ( 'Dim Product'[Category] ), \"(blank)\", SUBSTITUTE ( SUBSTITUTE ( SUBSTITUTE ( SUBSTITUTE ( SUBSTITUTE ( "
          "'Dim Product'[Category], \"&\", \"&amp;\" )" in dax, dax)
    check("an empty list shows the empty text", 'ISBLANK ( _max ),' in dax and "No data for the current filters." in dax)
    check("the title is escaped", "Top &lt;5&gt;" in dax)
    other = hm.bar_list(("O'Brien's", "A]B"), "M", empty_text="Nothing & nobody")
    sound("bar_list with awkward names", other)
    check("tuple reference is quoted and escaped", "'O''Brien''s'[A]]B]" in other)
    check("empty text is escaped", "Nothing &amp; nobody" in other)
    raises("a bare column name is refused", lambda: hm.bar_list("Category", "M"), "column reference")
    raises("top_n is bounded", lambda: hm.bar_list("T[C]", "M", top_n=0), "between")


def test_table():
    print("\n== table ==")
    cols = [{"measure": "Total Sales", "header": "Sales <k>"}, {"measure": "Margin %", "format_string": "0.0%", "align": "center"}]
    dax = hm.table("Dim Site[Site Name]", cols, category_header="Site", top_n=15)
    sound("table", dax)
    check("measures are added once, as extension columns", '"@m0", [Total Sales], "@m1", [Margin %]' in dax)
    check("rows are the top N by the first measure", "TOPN ( 15, FILTER ( _base, NOT ISBLANK ( [@m0] ) ), [@m0], DESC," in dax)
    check("headers are escaped", "Sales &lt;k&gt;</th>" in dax and ">Site</th>" in dax and ">Margin %</th>" in dax)
    check("alignment per column", "text-align:center" in dax and dax.count("text-align:right") >= 2)
    check("each cell is blank-safe", 'IF ( ISBLANK ( [@m1] ), "n/a", FORMAT ( [@m1], "0.0%" ) )' in dax)
    check("no stripes unless asked", "RANKX" not in dax)
    striped = hm.table(("Dim Site", "Site Name"), cols, stripe_colour="#F9FAFB")
    sound("striped table", striped)
    check("stripes use the row position", 'IF ( MOD ( _n, 2 ) = 0, "#F9FAFB", "transparent" )' in striped)
    check("the category header defaults to the column name", ">Site Name</th>" in striped)
    raises("a table needs a measure column", lambda: hm.table("T[C]", []), "at least one")
    raises("align is checked", lambda: hm.table("T[C]", [{"measure": "M", "align": "top"}]), "align")


def test_validation_and_dispatch():
    print("\n== validation and dispatch ==")
    raises("a colour that closes the attribute", lambda: hm.kpi_card("M", accent="red;' onmouseover='x"), "not a colour")
    raises("a colour with markup", lambda: hm.kpi_card("M", background="#fff'><script>"), "not a colour")
    raises("a font with a quote", lambda: hm.kpi_card("M", font_family="Arial'; x:"), "font_family")
    raises("a multi-line format string", lambda: hm.kpi_card("M", format_string="0\n0"), "one-line")
    raises("an unknown style parameter", lambda: hm.kpi_card("M", colour="#000"), "unknown parameter")
    raises("a missing measure", lambda: hm.kpi_card("  "), "measure name")
    raises("value_size is bounded", lambda: hm.kpi_card("M", value_size=0), "between")
    for colour in ("#abc", "#aabbccdd", "rgba(0, 0, 0, 0.5)", "hsl(120 50% 50%)", "transparent", "rebeccapurple"):
        try:
            hm.kpi_card("M", accent=colour)
            check(f"colour accepted: {colour}", True)
        except ValueError as e:
            check(f"colour accepted: {colour}", False, str(e))

    r = hm.generate("kpi_card", value_measure="Total Sales")
    eq("generate returns name, kind, dax, notes", sorted(r), ["dax", "kind", "name", "notes"])
    eq("default name", r["name"], "kpi_card (HTML)")
    check("notes say what was not verified", "not been evaluated" in r["notes"] and "no script" in r["notes"])
    eq("a given name is kept", hm.generate("table", name="Site table", category="T[C]", columns=[{"measure": "M"}])["name"], "Site table")
    eq("aliases", [hm.generate(k, **p)["kind"] for k, p in (("kpi", {"value_measure": "M"}), ("bars", {"category": "T[C]", "value_measure": "M"}),
                                                           ("KPI-Strip", {"items": [{"measure": "M"}]}))],
       ["kpi_card", "bar_list", "kpi_strip"])
    eq("every kind is reachable", sorted(hm.KINDS), ["bar_list", "headline", "kpi_card", "kpi_strip", "table"])
    raises("unknown kind", lambda: hm.generate("gauge"), "unknown html measure kind")
    raises("a missing argument is a ValueError", lambda: hm.generate("bar_list", category="T[C]"), "bar_list")
    eq("escape_html order", hm.escape_html("x").count("SUBSTITUTE"), 5)
    check("escape_html escapes & before the entities it writes",
          hm.escape_html("x").startswith('SUBSTITUTE ( SUBSTITUTE ( SUBSTITUTE ( SUBSTITUTE ( SUBSTITUTE ( x, "&", "&amp;" )'))


if __name__ == "__main__":
    for fn in (test_kpi_card, test_kpi_strip, test_headline, test_bar_list, test_table, test_validation_and_dispatch):
        fn()
    print()
    if _failures:
        print(f"FAILED: {len(_failures)} check(s): {_failures}")
        sys.exit(1)
    print("ALL HTML MEASURE TESTS PASSED")
