"""
Tests for the SVG micro-visual DAX measure generators: each kind emits well-formed DAX (balanced
string literals, valid SVG fragments, data URI), references its inputs, and is clean under our own
DAX linter. No Power BI required.
Run: python tests/test_svg_measures.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))
import svg_measures  # noqa: E402
import dax_lint  # noqa: E402

_failures = []


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f": {detail}" if detail and not cond else ""))
    if not cond:
        _failures.append(name)


def _wellformed(dax):
    return (dax.count('"') % 2 == 0                 # balanced DAX string literals
            and svg_measures.URI_PREFIX in dax       # data URI present
            and "<svg" in dax and "</svg>" in dax    # svg envelope
            and '"' not in dax.split("<svg", 1)[1].split("'http", 1)[0])  # svg attrs single-quoted


def test_each_kind():
    print("\n== each kind emits well-formed DAX ==")
    prog = svg_measures.generate("progress", value_measure="Margin %", max_value=1)
    check("progress well-formed", _wellformed(prog["dax"]), prog["dax"])
    check("progress references measure", "[Margin %]" in prog["dax"])
    check("progress has track + fill rects", prog["dax"].count("<rect") == 2)

    bul = svg_measures.generate("bullet", value_measure="Sales", target_measure="Target", max_value=1000)
    check("bullet well-formed", _wellformed(bul["dax"]), bul["dax"])
    check("bullet references both measures", "[Sales]" in bul["dax"] and "[Target]" in bul["dax"])
    check("bullet has target marker var", "_tx" in bul["dax"])

    pill = svg_measures.generate("status_pill", value_measure="KPI")
    check("pill well-formed", _wellformed(pill["dax"]), pill["dax"])
    check("pill uses SWITCH for color + label", pill["dax"].count("SWITCH( TRUE()") == 2)
    check("pill has text element", "<text" in pill["dax"] and "</text>" in pill["dax"])

    spark = svg_measures.generate("sparkline", axis_column="'Date'[Month]", value_measure="Sales")
    check("sparkline well-formed", _wellformed(spark["dax"]), spark["dax"])
    check("sparkline references axis", "'Date'[Month]" in spark["dax"])
    check("sparkline builds polyline via CONCATENATEX", "<polyline" in spark["dax"] and "CONCATENATEX" in spark["dax"])


def test_lint_clean():
    print("\n== generated DAX is clean under our own linter (no bad division / unknown funcs) ==")
    for kind, kw in [("progress", {"value_measure": "M", "max_value": 1}),
                     ("bullet", {"value_measure": "M", "target_measure": "T", "max_value": 10}),
                     ("status_pill", {"value_measure": "M"}),
                     ("sparkline", {"axis_column": "'D'[M]", "value_measure": "M"})]:
        dax = svg_measures.generate(kind, **kw)["dax"]
        rules = {f["rule_id"] for f in dax_lint.lint_expression(kind, dax)}
        check(f"{kind}: no bare-division DL003", "DL003" not in rules, str(rules))
        check(f"{kind}: no unknown-function DL008", "DL008" not in rules, str(rules))


def test_dispatch_and_errors():
    print("\n== dispatch + error handling ==")
    check("status alias maps to status_pill", svg_measures.generate("status", value_measure="M")["kind"] == "status_pill")
    check("custom name preserved", svg_measures.generate("progress", name="Bar", value_measure="M", max_value=1)["name"] == "Bar")
    try:
        svg_measures.generate("piechart", value_measure="M")
        check("unknown kind raises", False)
    except ValueError:
        check("unknown kind raises", True)


if __name__ == "__main__":
    print("=" * 70)
    print("  SVG MEASURE GENERATOR TESTS")
    print("=" * 70)
    test_each_kind()
    test_lint_clean()
    test_dispatch_and_errors()
    print("\n" + "=" * 70)
    if _failures:
        print(f"  {len(_failures)} CHECK(S) FAILED: {', '.join(_failures)}")
        sys.exit(1)
    print("  ALL SVG MEASURE CHECKS PASSED")
    print("=" * 70)
