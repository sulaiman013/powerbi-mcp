"""
Tests for src/payload_measures.py: the text-escaping chains (modelled in Python and decoded
with the standard library), the shape and quoting of the generated DAX, input validation, the
size estimate, and the generated JavaScript parsers.

The DAX is never evaluated here. The parser checks run under Node when it is on PATH and are
skipped otherwise. Run: python tests/test_payload_measures.py
"""
import json
import os
import shutil
import subprocess
import sys
import tempfile
import urllib.parse

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))
import dax_tokens as dt  # noqa: E402
import payload_measures as pm  # noqa: E402

_failures = []
NODE = shutil.which("node")
LS, PS, BS = chr(0x2028), chr(0x2029), chr(92)


def check(name, cond, detail=""):
    line = f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f": {detail}" if detail and not cond else "")
    # Test data holds characters a Windows console code page cannot print.
    print(line.encode("ascii", "backslashreplace").decode("ascii"))
    if not cond:
        _failures.append(name)


def eq(name, actual, expected):
    check(name, actual == expected, f"got {actual!r}, expected {expected!r}")


def raises(name, fn, fragment=""):
    try:
        fn()
    except pm.PayloadError as e:
        check(name, fragment.lower() in str(e).lower(), f"message was {e}")
    else:
        check(name, False, "no PayloadError raised")


def well_formed(dax):
    """Every string, quoted name and bracket is closed and the parentheses balance."""
    depth = 0
    closers = {dt.STRING: '"', dt.QNAME: "'", dt.BRACKET: "]"}
    for tok in dt.tokenize(dax):
        if tok.kind in closers and (len(tok.text) < 2 or not tok.text.endswith(closers[tok.kind])):
            return False
        if tok.kind == dt.OP and tok.text in "()":
            depth += 1 if tok.text == "(" else -1
            if depth < 0:
                return False
    return depth == 0


def plain_ascii(text):
    return all(ch in "\n" or 32 <= ord(ch) < 127 for ch in text) and BS not in text


NASTY = [
    "plain",
    'say "hi"',
    "O'Brien",
    "back" + BS + "slash" + BS + BS,
    "100% sure, 50%25",
    "a,b;c^d#e=f",
    "line1\nline2\r\nline3",
    "tab\there",
    "</script><script>alert(1)</script>",
    "<!-- comment -->",
    "sep" + LS + "and" + PS + "end",
    "caf" + chr(0xE9) + " " + chr(0xFC) + chr(0xF1) + " " + chr(0x4E2D) + chr(0x6587),
    "",
    "%",
    BS + '"' + BS + "n",
]


def test_escape_chains():
    print("\n== escape chains (Python model of the SUBSTITUTE chain) ==")
    steps = pm.text_escape_steps("delimited", (",", ";", "^", "#"))
    eq("delimited: the escape character is encoded first", steps[0], ("%", "%25"))
    forbidden = set('"' + "'" + BS + "<\r\n,;^#" + LS + PS)
    for s in NASTY:
        wire = pm.apply_escape(s, steps)
        check(f"delimited round trip {s[:18]!r}", urllib.parse.unquote(wire, errors="strict") == s, wire)
        check(f"delimited output is inert {s[:18]!r}", not (forbidden & set(wire)), wire)
    jsteps = pm.text_escape_steps("json")
    eq("json: the backslash is escaped first", jsteps[0], (BS, BS + BS))
    for s in NASTY:
        wire = pm.apply_escape(s, jsteps)
        try:
            back = json.loads('"' + wire + '"')
        except ValueError as e:
            back = f"<invalid JSON: {e}>"
        check(f"json round trip {s[:18]!r}", back == s, wire)
        check(f"json output cannot close a script {s[:18]!r}",
              "<" not in wire and LS not in wire and PS not in wire and "\n" not in wire, wire)
    raises("unknown format is refused", lambda: pm.text_escape_steps("xml"), "unknown format")


FIELDS = [
    {"column": "OrderKey", "type": "int", "name": "key"},
    {"column": "Order Date", "type": "date", "name": "day"},
    {"table": "Dim Product", "column": "Name", "type": "text", "name": "product"},
    {"column": "Amount", "type": "decimal", "scale": 2, "name": "amount"},
    {"column": "Is Online", "type": "bool", "name": "online"},
]


def test_row_grain_dax():
    print("\n== row grain ==")
    r = pm.generate_payload_measure("Sales Facts", "Fact Sales", FIELDS, order_by=["key"],
                                    where="'Fact Sales'[Qty] > 0", display_folder="Payload")
    dax = r["dax"]
    check("well-formed DAX", well_formed(dax), dax)
    check("no backslash and no non-ASCII character in the measure text", plain_ascii(dax))
    check("iterates the filtered base table", "FILTER ( 'Fact Sales', 'Fact Sales'[Qty] > 0 )" in dax)
    check("integer goes through FORMAT 0", "FORMAT ( 'Fact Sales'[OrderKey], \"0\" )" in dax)
    check("date is built by arithmetic, not a format string",
          "YEAR ( 'Fact Sales'[Order Date] ) * 10000 + MONTH ( 'Fact Sales'[Order Date] ) * 100" in dax)
    check("decimal travels as a scaled integer", "ROUND ( 'Fact Sales'[Amount] * 100, 0 )" in dax)
    check("a related column uses RELATED", "RELATED ( 'Dim Product'[Name] )" in dax)
    check("text is wrapped in the SUBSTITUTE chain", dax.count("SUBSTITUTE (") == 11, dax.count("SUBSTITUTE ("))
    check("line separators are named by code point", "UNICHAR ( 8232 )" in dax and "UNICHAR ( 8233 )" in dax)
    check("ordered by the key", dax.rstrip().endswith("'Fact Sales'[OrderKey], ASC\n)"), dax[-60:])
    eq("expression mirrors dax", r["expression"], dax)
    eq("display folder kept", r["display_folder"], "Payload")
    eq("embed hint", r["embed_as"], "string")
    eq("grain", r["grain"], "rows")
    eq("parser name", r["parser_name"], "parseSalesFacts")
    check("description is one line", "\n" not in r["description"])
    check("notes say the ceiling is not a documented limit",
          any("not a documented Power BI limit" in n for n in r["notes"]))
    check("notes say the DAX was not evaluated", any("not been evaluated" in n for n in r["notes"]))

    q = pm.generate_payload_measure("P", "O'Brien's Data", [{"column": "Net]Sales", "type": "int"}])
    check("table apostrophe and column bracket are escaped", "'O''Brien''s Data'[Net]]Sales]" in q["dax"], q["dax"])
    z = pm.generate_payload_measure("P", "T", [{"column": "Qty", "type": "int", "blank": "zero"}])
    check("blank as zero drops the ISBLANK test", "ISBLANK" not in z["dax"] and "( 'T'[Qty] + 0 )" in z["dax"], z["dax"])
    s = pm.generate_payload_measure("P", "T", ["Qty:int", ("Name", "text"), {"column": "Dim[Label]", "type": "text"}])
    check("shorthand field forms", "'T'[Qty]" in s["dax"] and "'T'[Name]" in s["dax"]
          and "RELATED ( 'Dim'[Label] )" in s["dax"], s["dax"])
    unordered = pm.generate_payload_measure("P", "T", ["Qty:int"])
    check("no order_by is called out", any("order_by" in n for n in unordered["notes"]))
    check("a related column is called out, with what it needs", any("many-to-one" in n and "Dim Product" in n for n in r["notes"]))
    check("no relationship note when every column is the table's own", not any("many-to-one" in n for n in unordered["notes"]))
    loose = pm.generate_payload_measure("P", "Fact Sales", [{"column": "Dim Site[Site Name]", "type": "text"},
                                                           {"column": "'Fact Sales'[Qty]", "type": "int"},
                                                           {"column": "[Net]", "type": "int"}])
    check("a table written without its quotes is still read as one name", "RELATED ( 'Dim Site'[Site Name] )" in loose["dax"], loose["dax"])
    check("a quoted own-table reference and a bare [column] need no RELATED",
          "FORMAT ( 'Fact Sales'[Qty], \"0\" )" in loose["dax"] and "FORMAT ( 'Fact Sales'[Net], \"0\" )" in loose["dax"])
    raises("a reference that is not a column", lambda: pm.generate_payload_measure("P", "T", [{"column": "SUM(T[a])", "type": "int"}]),
           "column reference")


def test_group_grain_dax():
    print("\n== group grain ==")
    fields = [
        {"column": "MonthKey", "type": "int", "name": "month"},
        {"table": "Dim Site", "column": "SiteKey", "type": "int", "name": "site"},
        {"measure": "Total Hours", "type": "decimal", "scale": 1, "name": "hours"},
        {"expression": "SUM ( 'Fact Hours'[Cost] )", "aggregate": True, "type": "int", "name": "cost"},
    ]
    r = pm.generate_payload_measure("Hours", "Fact Hours", fields, order_by=[("month", "DESC"), "hours"],
                                    where="'Fact Hours'[Hours] > 0")
    dax = r["dax"]
    check("well-formed DAX", well_formed(dax), dax)
    eq("grain is inferred from the measure field", r["grain"], "groups")
    check("groups by the column fields", "SUMMARIZE ( 'Fact Hours', 'Fact Hours'[MonthKey], 'Dim Site'[SiteKey] )" in dax, dax)
    check("measure is added as written", '"@f2", [Total Hours]' in dax)
    check("raw aggregate is wrapped in CALCULATE", '"@f3", CALCULATE ( SUM ( \'Fact Hours\'[Cost] ) )' in dax)
    check("the condition is a filter argument, so measures see it",
          "CALCULATETABLE (" in dax and dax.index("CALCULATETABLE") < dax.index("ADDCOLUMNS"))
    check("group columns are read without RELATED", "RELATED" not in dax)
    check("computed values are read from their extension columns", "ROUND ( [@f2] * 10, 0 )" in dax, dax)
    check("order uses the group column and the extension column",
          "'Fact Hours'[MonthKey], DESC" in dax and "[@f2], ASC" in dax)
    total = pm.generate_payload_measure("Totals", "Fact Hours", [{"measure": "[Total Hours]", "type": "int"}])
    check("measures only: one group for the whole table", 'ROW ( "@all", 1 )' in total["dax"], total["dax"])
    distinct = pm.generate_payload_measure("Sites", "Fact Hours", ["SiteKey:int"], grain="groups")
    check("explicit groups with no measure is a distinct list",
          "SUMMARIZE ( 'Fact Hours', 'Fact Hours'[SiteKey] )" in distinct["dax"] and "ADDCOLUMNS" not in distinct["dax"])


def test_json_dax():
    print("\n== json format ==")
    r = pm.generate_payload_measure("Sales Facts", "Fact Sales", FIELDS, format="json", order_by=["key"])
    dax = r["dax"]
    check("well-formed DAX", well_formed(dax), dax)
    check("no backslash and no non-ASCII character in the measure text", plain_ascii(dax))
    check("blank numbers become null", 'IF ( ISBLANK ( \'Fact Sales\'[OrderKey] ), "null"' in dax)
    check("booleans are true/false", '"true", "false"' in dax)
    check("text is quoted and null when blank", '"null", """" & SUBSTITUTE' in dax, dax)
    check("the backslash is written with UNICHAR", "UNICHAR ( 92 ) & UNICHAR ( 92 )" in dax)
    check("rows are wrapped in an array", '"[" & _rows & "]"' in dax and "BLANK ()" in dax)
    eq("embed hint", r["embed_as"], "json")


def test_dimension_dax():
    print("\n== dimension payload ==")
    dims = [
        {"name": "site", "table": "Dim Site", "key": "SiteKey", "labels": ["Site Name", "Region"]},
        {"name": "shift", "table": "Dim Shift", "key": "ShiftKey", "labels": ["Shift"], "all_rows": True,
         "where": "'Dim Shift'[Active] = TRUE ()"},
    ]
    r = pm.generate_dimension_payload("App Names", dims, expected_rows={"site": 40, "shift": 3})
    dax = r["dax"]
    check("well-formed DAX", well_formed(dax), dax)
    check("each table is named", '"site=" & CONCATENATEX ( \'Dim Site\',' in dax and '"shift=" & CONCATENATEX' in dax, dax)
    check("tables are joined by the table separator", '& "#" & "shift="' in dax)
    check("all_rows ignores the filter context", "FILTER ( ALL ( 'Dim Shift' ), 'Dim Shift'[Active] = TRUE () )" in dax)
    check("rows are ordered by key", "'Dim Site'[SiteKey], ASC" in dax)
    check("text escapes cover all three separators", '"^", "%5E"' in dax and '";", "%3B"' in dax and '"#", "%23"' in dax)
    eq("per-dimension widths", sorted(r["estimated_chars_per_row"]), ["shift", "site"])
    check("budget totals the dimensions", r["budget"]["fits"] and r["budget"]["estimated_chars"] > 0, r["budget"])
    j = pm.generate_dimension_payload("App Names", dims, format="json")
    check("json: well-formed DAX", well_formed(j["dax"]), j["dax"])
    check("json: an object of arrays", j["dax"].startswith('"{" & """site"":["') and j["dax"].endswith('& "}"'), j["dax"][:40])
    raises("a dimension name must be an identifier",
           lambda: pm.generate_dimension_payload("N", [{"name": "bad name", "table": "T", "key": "K"}]), "letters")
    raises("duplicate dimension names are refused",
           lambda: pm.generate_dimension_payload("N", [{"table": "T", "key": "K"}, {"table": "T", "key": "K"}]), "share")
    raises("unknown dimension in expected_rows",
           lambda: pm.generate_dimension_payload("N", [{"table": "T", "key": "K"}], expected_rows={"x": 1}), "unknown")


def test_validation():
    print("\n== validation ==")
    one = [{"column": "A", "type": "int"}]
    raises("unknown format", lambda: pm.generate_payload_measure("M", "T", one, format="csv"), "unknown format")
    raises("a digit cannot be a separator", lambda: pm.generate_payload_measure("M", "T", one, field_sep="1"), "not allowed")
    raises("a percent sign cannot be a separator", lambda: pm.generate_payload_measure("M", "T", one, field_sep="%"), "not allowed")
    raises("separators must differ", lambda: pm.generate_payload_measure("M", "T", one, field_sep=";"), "different")
    raises("a field needs a type", lambda: pm.generate_payload_measure("M", "T", [{"column": "A"}]), "needs a type")
    raises("a field needs one source",
           lambda: pm.generate_payload_measure("M", "T", [{"column": "A", "measure": "B", "type": "int"}]), "exactly one")
    raises("duplicate field names", lambda: pm.generate_payload_measure("M", "T", one + one), "both named")
    raises("order_by must name a field", lambda: pm.generate_payload_measure("M", "T", one, order_by=["Z"]), "not one of the fields")
    raises("unbalanced where", lambda: pm.generate_payload_measure("M", "T", one, where="T[A] > ( 1"), "unclosed")
    raises("unterminated string in where", lambda: pm.generate_payload_measure("M", "T", one, where='T[A] = "x'), "unterminated")
    raises("a comment in an expression is refused",
           lambda: pm.generate_payload_measure("M", "T", [{"expression": "1 -- x", "type": "int"}]), "comment")
    raises("a measure needs group grain",
           lambda: pm.generate_payload_measure("M", "T", [{"measure": "X", "type": "int"}], grain="rows"), "grain")
    raises("scale is bounded",
           lambda: pm.generate_payload_measure("M", "T", [{"column": "A", "type": "decimal", "scale": 12}]), "scale")
    raises("no fields", lambda: pm.generate_payload_measure("M", "T", []), "at least one field")
    raises("bad parser name", lambda: pm.generate_payload_measure("M", "T", one, parser_name="2bad"), "identifier")


def test_budget():
    print("\n== budget ==")
    b = pm.estimate_budget(50, rows=10_000)
    eq("capacity at the default ceiling", b["max_rows_at_ceiling"], 42_000)
    eq("estimated size", b["estimated_chars"], 500_000)
    check("fits", b["fits"] and abs(b["share_of_ceiling"] - 0.2381) < 1e-4, b)
    big = pm.estimate_budget(50, rows=100_000, ceiling=1_000_000)
    check("over the ceiling is reported, not hidden", big["fits"] is False and big["share_of_ceiling"] == 5.0, big)
    r = pm.generate_payload_measure("M", "T", [{"column": "A", "type": "int", "avg_chars": 7}, "B:date"],
                                    expected_rows=1000, ceiling=10_000)
    eq("chars per row: 7 + 8 + field separator + record separator", r["estimated_chars_per_row"], 17.0)
    eq("budget uses the caller's ceiling", (r["budget"]["ceiling"], r["budget"]["fits"]), (10_000, False))


# ---------------------------------------------------------------- parsers under Node

ROWS = [
    {"key": 1, "day": "2024-03-09", "product": 'Widget "A", large; 5% off', "amount": 12.5, "online": True},
    {"key": -20, "day": "1999-12-31", "product": "O'Brien </script> " + BS + LS, "amount": -0.07, "online": False},
    {"key": None, "day": None, "product": "", "amount": None, "online": None},
    {"key": 4000000000, "day": "2030-01-01", "product": "line1\nline2 ^ # =", "amount": 1234567.89, "online": True},
]


def wire(field, value, fmt, steps):
    """What the generated DAX writes for one value (the test's own, independent encoder)."""
    if value is None:
        return "null" if fmt == "json" else ""
    kind = field["type"]
    if kind == "int":
        return str(int(value))
    if kind == "decimal":
        return str(int(round(value * 10 ** field.get("scale", 2))))
    if kind == "date":
        return value.replace("-", "")
    if kind == "bool":
        return ("true" if value else "false") if fmt == "json" else ("1" if value else "0")
    text = pm.apply_escape(value, steps)
    return '"' + text + '"' if fmt == "json" else text


def expected_row(row):
    out = dict(row)
    out["day"] = None if row["day"] is None else int(row["day"].replace("-", ""))
    out["product"] = row["product"] or ""
    return out


def run_node(script):
    d = tempfile.mkdtemp()
    path = os.path.join(d, "check.js")
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        f.write(script)
    r = subprocess.run([NODE, path], capture_output=True, text=True, encoding="utf-8", timeout=60)
    if r.returncode != 0:
        return None, (r.stderr or r.stdout)[:600]
    return json.loads(r.stdout), ""


DUMP = ("function dump(t) { var a = [], i; for (i = 0; i < t.length; i++) { a.push(t.row(i)); } return a; }\n"
        "function clean(k, v) { return typeof v === 'number' && isNaN(v) ? null : v; }\n")


def test_parsers():
    print("\n== generated parsers ==")
    for fmt in ("delimited", "json"):
        r = pm.generate_payload_measure("Sales Facts", "Fact Sales", FIELDS, format=fmt)
        steps = pm.text_escape_steps(fmt, (",", ";") if fmt == "delimited" else ())
        records = [[wire(f, row[f["name"]], fmt, steps) for f in FIELDS] for row in ROWS]
        if fmt == "delimited":
            text = ";".join(",".join(rec) for rec in records)
        else:
            text = "[" + ",".join("[" + ",".join(rec) + "]" for rec in records) + "]"
            try:
                check("json payload is valid JSON", len(json.loads(text)) == len(ROWS))
            except ValueError as e:
                check("json payload is valid JSON", False, str(e))
        check(f"{fmt}: payload text cannot close a script or a string",
              "<" not in text and "\n" not in text and LS not in text
              and (fmt == "json" or '"' not in text), text[:120])
        if not NODE:
            print(f"  [SKIP] {fmt}: parser run (node is not on PATH)")
            continue
        script = (r["js_parser"] + "\n" + DUMP
                  + f"var t = {r['parser_name']}({json.dumps(text)});\n"
                  + f"var empty = {r['parser_name']}('');\n"
                  + "process.stdout.write(JSON.stringify({rows: dump(t), n: t.length, empty: empty.length, "
                    "keyType: Object.prototype.toString.call(t.columns.key)}, clean));\n")
        out, err = run_node(script)
        if out is None:
            check(f"{fmt}: parser runs under node", False, err)
            continue
        eq(f"{fmt}: rows survive the round trip", out["rows"], [expected_row(x) for x in ROWS])
        eq(f"{fmt}: record count", out["n"], len(ROWS))
        eq(f"{fmt}: empty payload gives no rows", out["empty"], 0)
        eq(f"{fmt}: numeric column is a typed array", out["keyType"], "[object Float64Array]")

    dims = [{"name": "site", "table": "Dim Site", "key": "SiteKey", "labels": ["Site Name", "Region"]},
            {"name": "shift", "table": "Dim Shift", "key": "ShiftKey", "labels": ["Shift"]}]
    data = {"site": [(1, "North; yard #2", "EU"), (2, "South^East=1", 'A "B"')], "shift": []}
    for fmt in ("delimited", "json"):
        r = pm.generate_dimension_payload("App Names", dims, format=fmt)
        steps = pm.text_escape_steps(fmt, ("^", ";", "#") if fmt == "delimited" else ())
        if fmt == "delimited":
            text = "#".join(
                name + "=" + ";".join("^".join([str(row[0])] + [pm.apply_escape(v, steps) for v in row[1:]])
                                      for row in rows) for name, rows in data.items())
        else:
            text = "{" + ",".join(
                '"' + name + '":[' + ",".join(
                    "[" + ",".join([str(row[0])] + ['"' + pm.apply_escape(v, steps) + '"' for v in row[1:]]) + "]"
                    for row in rows) + "]" for name, rows in data.items()) + "}"
        if not NODE:
            print(f"  [SKIP] {fmt}: dimension parser run (node is not on PATH)")
            continue
        script = (r["js_parser"] + "\n"
                  + f"var d = {r['parser_name']}({json.dumps(text)});\n"
                  + f"var none = {r['parser_name']}('');\n"
                  + "process.stdout.write(JSON.stringify({rows: d.site.rows, two: d.site.byKey[2], "
                    "shift: d.shift.length, none: [none.site.length, none.shift.length]}));\n")
        out, err = run_node(script)
        if out is None:
            check(f"{fmt}: dimension parser runs under node", False, err)
            continue
        eq(f"{fmt}: dimension rows", out["rows"],
           [{"key": 1, "Site Name": "North; yard #2", "Region": "EU"},
            {"key": 2, "Site Name": "South^East=1", "Region": 'A "B"'}])
        eq(f"{fmt}: lookup by key", out["two"]["Site Name"], "South^East=1")
        eq(f"{fmt}: an empty dimension is still present", out["shift"], 0)
        eq(f"{fmt}: a blank payload gives empty dimensions", out["none"], [0, 0])


if __name__ == "__main__":
    for fn in (test_escape_chains, test_row_grain_dax, test_group_grain_dax, test_json_dax,
               test_dimension_dax, test_validation, test_budget, test_parsers):
        fn()
    print()
    if _failures:
        print(f"FAILED: {len(_failures)} check(s): {_failures}")
        sys.exit(1)
    print("ALL PAYLOAD MEASURE TESTS PASSED")
