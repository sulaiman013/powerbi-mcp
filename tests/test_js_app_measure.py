"""
Tests for src/js_app_measure.py: the JavaScript and CSS minifiers (the twelve regex-versus-
division cases, strings, templates, and line breaks that act as semicolons), the checks that
fail a build, the DAX the three measures hold, and the bootstrap's behaviour when the script
is evaluated more than once.

The DAX is never evaluated. Checks that run JavaScript need Node on PATH and are skipped
otherwise. Run: python tests/test_js_app_measure.py
"""
import json
import os
import shutil
import subprocess
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))
import dax_tokens as dt  # noqa: E402
import js_app_measure as jm  # noqa: E402
import tmdl_authoring  # noqa: E402

_failures = []
NODE = shutil.which("node")


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
    except jm.BuildError as e:
        check(name, fragment.lower() in str(e).lower(), f"message was {e}")
    else:
        check(name, False, "no BuildError raised")


def run_node(script):
    d = tempfile.mkdtemp()
    path = os.path.join(d, "check.js")
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        f.write(script)
    r = subprocess.run([NODE, path], capture_output=True, text=True, encoding="utf-8", timeout=60)
    if r.returncode != 0:
        return None, (r.stderr or r.stdout)[:800]
    return json.loads(r.stdout), ""


def dax_value(expr):
    """The text a constant measure evaluates to: a string literal, optionally inside the
    one SUBSTITUTE that restores line breaks."""
    toks = [t for t in dt.tokenize(expr) if t.kind != dt.WS]
    strings = [t.text[1:-1].replace('""', '"') for t in toks if t.kind == dt.STRING]
    if toks[0].kind == dt.STRING and len(toks) == 1:
        return strings[0]
    assert toks[0].text == "SUBSTITUTE" and len(strings) == 2 and "UNICHAR ( 10 )" in expr, expr[:80]
    return strings[0].replace(strings[1], "\n")


# Each case is a function body: (what it shows, source, expected minified text).
# The trailing comments hold quotes and slashes on purpose: a scanner that loses its place
# inside the code before them would not strip them cleanly.
TWELVE = [
    ("a regex holding a double quote",
     r'''var t = 'a"b"c'; return t.replace(/"/g, '""');  // csv "quote"''',
     r'''var t='a"b"c';return t.replace(/"/g,'""');'''),
    ("a regex holding an apostrophe",
     r'''var s = "it's"; return s.replace(/'/g, "x");  // it's''',
     r'''var s="it's";return s.replace(/'/g,"x");'''),
    ("division after a name",
     r'''var a = 8, b = 2, c = 2; return a / b / c;  // a/b/c''',
     r'''var a=8,b=2,c=2;return a/b/c;'''),
    ("division after )",
     r'''var a = 3, b = 5; return (a + b) / 2;  // "half"''',
     r'''var a=3,b=5;return(a+b)/2;'''),
    ("division after ]",
     r'''var v = [9, 3]; return v[0] / v[1];  // 'q' ''',
     r'''var v=[9,3];return v[0]/v[1];'''),
    ("division after a number",
     r'''return 100 / 5 / 2;  // 10''',
     r'''return 100/5/2;'''),
    ("a regex after =",
     r'''var re = /ab+c/gi; return "xABBCx".replace(re, "-");''',
     r'''var re=/ab+c/gi;return"xABBCx".replace(re,"-");'''),
    ("a regex after (",
     r'''return "a  b   c".split(/\s+/).length;''',
     r'''return"a  b   c".split(/\s+/).length;'''),
    ("a regex after a keyword",
     r'''function f(s) { return /^\d+$/.test(s); } return [f("12"), f("1a")];''',
     r'''function f(s){return/^\d+$/.test(s);}return[f("12"),f("1a")];'''),
    ("an escaped slash inside a regex",
     r'''var p = /a\/b/.test("a/b"); return p;  // a/b''',
     r'''var p=/a\/b/.test("a/b");return p;'''),
    ("a character class holding a slash and both quotes",
     r'''return "a/b\"c'd".replace(/[/"']/g, "_");  // class''',
     r'''return"a/b\"c'd".replace(/[/"']/g,"_");'''),
    ("a regex holding // after a comma",
     r'''function g(x, r, n) { return r.test(x) ? n : 0; } return g("x//y", /x\/\/y/, 1);  // tail''',
     r'''function g(x,r,n){return r.test(x)?n:0;}return g("x//y",/x\/\/y/,1);'''),
]

MORE = [
    ("postfix ++ then division", "var i = 4; var z = i++ / 2; return [i, z];", "var i=4;var z=i++/2;return[i,z];"),
    ("template text is kept, its expressions are minified",
     "var x = 6, y = 3; return `a // b ${ x / y } /* c */`;  // real comment",
     "var x=6,y=3;return`a // b ${x/y} /* c */`;"),
    ("nested template", "var a = 1, b = 4; return `${ a ? `in ${ b / 2 }` : '' }`;",
     "var a=1,b=4;return`${a?`in ${b/2}`:''}`;"),
    ("unary operators keep their space", "var b = 1, c = 2; return [b + +c, b - -c, b + ++c];",
     "var b=1,c=2;return[b+ +c,b- -c,b+ ++c];"),
    ("< and !-- never touch", "var a = 1, b = 5; return a < !--b;", "var a=1,b=5;return a< !--b;"),
    ("a number keeps the space before a dot", "return 1 .toFixed(2);", "return 1 .toFixed(2);"),
    ("word operators keep their spaces", "var o = {k: 1}; return 'k' in o && o instanceof Object;",
     "var o={k:1};return'k'in o&&o instanceof Object;"),
    ("a string holding comment markers", 'return "// not" + \'/* nor */\';', 'return"// not"+\'/* nor */\';'),
]

LINES = [
    ("a break that acts as a semicolon is kept", "var a = 1\nvar b = 2\nreturn a + b\n", "var a=1\nvar b=2\nreturn a+b"),
    ("breaks inside brackets and after ; are removed",
     "var a = [\n  1,\n  2\n];\nvar s = a.reduce(function (p, q) {\n  return p + q;\n}, 0);\nreturn s;\n",
     "var a=[1,2];var s=a.reduce(function(p,q){return p+q;},0);return s;"),
    ("a break after return is kept (it ends the statement)",
     "function g() {\n  return\n  1;\n}\nreturn g();", "function g(){return\n1;}\nreturn g();"),
    ("a leading operator continues the line", "var a = 1, b = 2\nvar x = a\n  + b\n  - 1;\nreturn x;",
     "var a=1,b=2\nvar x=a+b-1;return x;"),
    ("a call or index on the next line continues, as it does in JavaScript",
     "var f = function (n) { return [n, n * 2]; }\nvar r = f\n  (3)\n  [1];\nreturn r;",
     "var f=function(n){return[n,n*2];}\nvar r=f(3)[1];return r;"),
    ("++ on a new line starts a statement", "var a = 1, b = 5\na\n++b\nreturn [a, b]", "var a=1,b=5\na\n++b\nreturn[a,b]"),
    ("a comment spanning lines still ends the line", "var a = 1 /* one\n two */ var b = 2\nreturn a + b",
     "var a=1\nvar b=2\nreturn a+b"),
    ("a line comment at the end of the input", "return 7 // done", "return 7"),
    ("carriage returns are normalised", "var a = 1;\r\nreturn a;\r\n", "var a=1;return a;"),
]


def test_minifier_cases():
    print("\n== the twelve regex / division cases ==")
    eq("there are twelve", len(TWELVE), 12)
    for name, src, expected in TWELVE:
        eq(name, jm.minify_js(src), expected)
    print("\n== strings, templates, operators ==")
    for name, src, expected in MORE:
        eq(name, jm.minify_js(src), expected)
    print("\n== line breaks ==")
    for name, src, expected in LINES:
        eq(name, jm.minify_js(src), expected)
    eq("empty input", jm.minify_js(""), "")
    eq("a string that is never closed stops at the line end", jm.minify_js("var a = 'x\nvar b = 2;"), "var a='x\nvar b=2;")
    eq("minifying twice changes nothing", jm.minify_js(jm.minify_js(LINES[1][1])), LINES[1][2])


def test_minifier_semantics():
    print("\n== minified code behaves like the source (node) ==")
    cases = TWELVE + MORE + LINES
    if not NODE:
        print(f"  [SKIP] {len(cases)} snippets (node is not on PATH)")
        return
    payload = [{"name": n, "src": s, "min": jm.minify_js(s)} for n, s, _ in cases]
    script = ("var cases = " + json.dumps(payload) + ";\n"
              "var out = cases.map(function (c) {\n"
              "  var a, b;\n"
              "  try { a = String(JSON.stringify(new Function(c.src)())); } catch (e) { a = 'throws ' + e.message; }\n"
              "  try { b = String(JSON.stringify(new Function(c.min)())); } catch (e) { b = 'throws ' + e.message; }\n"
              "  return { name: c.name, source: a, minified: b };\n"
              "});\n"
              "process.stdout.write(JSON.stringify(out));\n")
    out, err = run_node(script)
    if out is None:
        check("snippets run under node", False, err)
        return
    for item in out:
        check(f"same result: {item['name']}",
              item["source"] == item["minified"] and not str(item["source"]).startswith("throws"),
              f"source {item['source']} / minified {item['minified']}")


def test_css_minifier():
    print("\n== css ==")
    css = ("/* card */\n.card , .tile {\n  color : #222 ;\n  background: url( img/a b.png ) ;\n}\n"
           "@media (min-width: 10px) and (max-width: 20px) {\n  .x::after { content: \" ; } /* x */ \" ; }\n}\n")
    out = jm.minify_css(css)
    check("one line", "\n" not in out, out)
    check("comment removed", "card */" not in out and "/* x */" in out, out)
    check("string kept exactly", '" ; } /* x */ "' in out, out)
    check("unquoted url kept exactly", "url( img/a b.png )" in out, out)
    check("spaces around braces, semicolons and commas are dropped", out.startswith(".card,.tile{color : #222;"), out)
    check("a descendant combinator keeps its space", jm.minify_css("a  b > c { }") == "a b > c{}")
    check("a media query keeps its spaces", "@media (min-width: 10px) and (max-width: 20px){" in out, out)
    eq("empty input", jm.minify_css(""), "")


def test_safety_checks():
    print("\n== checks that fail the build ==")
    ok = [("a.js", "function main(app) { app.root.textContent = 'ok'; }")]
    build = lambda **kw: jm.build_js_app_measures("P", js_modules=kw.pop("js", ok), check_with_node=False, **kw)  # noqa: E731
    raises("</script in a string", lambda: build(js=[("a.js", "var s = '</script>';")]), "</script")
    raises("</SCRIPT in any case", lambda: build(js=[("a.js", "var s = '</SCRIPT >';")]), "</script")
    raises("<!-- in the script", lambda: build(js=[("a.js", "var s = '<!--';")]), "<!--")
    raises("the error names the module", lambda: build(js=[("00-a.js", "var a;"), ("01-b.js", "var s = '</script>';")]), "01-b.js")
    raises("a control character", lambda: build(js=[("a.js", "var s = 'a" + chr(0x15) + "b';")]), "0x15")
    raises("a Unicode line separator", lambda: build(js=[("a.js", "var s = 'a" + chr(0x2028) + "b';")]), "U+2028")
    raises("a Unicode paragraph separator in css", lambda: build(css="a{content:'" + chr(0x2029) + "'}"), "U+2029")
    raises("</style in the stylesheet", lambda: build(css="a{} </style><script>"), "</style")
    raises("a control character in css", lambda: build(css="a{content:'" + chr(0x0C) + "'}"), "0x0C")
    try:
        r = build(js=[("a.js", "/* </script> is only mentioned here */\nvar a = 1;")])
        check("</script inside a comment is fine once the comment is stripped", "</script" not in r["measures"][1]["expression"].lower())
    except jm.BuildError as e:
        check("</script inside a comment is fine once the comment is stripped", False, str(e))
    raises("but not when comments ship", lambda: build(js=[("a.js", "/* </script> */\nvar a = 1;")], minify=False), "</script")
    r = build(js=[("a.js", "var s = 'zero" + chr(0x200B) + "width';")])
    check("an invisible character is reported, not refused", any("U+200B" in w for w in r["warnings"]), r["warnings"])
    raises("root_id must be a plain id", lambda: build(root_id="a b"), "root_id")
    raises("prefix is required", lambda: jm.build_js_app_measures(" ", js_modules=ok), "prefix")
    raises("modules are required", lambda: jm.build_js_app_measures("P", js_modules=[]), "empty")
    raises("a global must be an identifier", lambda: build(payload_measures=[("M", "not valid")]), "identifier")
    raises("a global cannot be a reserved word", lambda: build(payload_measures=[("M", "class")]), "identifier")
    raises("a global is used once", lambda: build(payload_measures=[("M", "G"), ("N", "G")]), "twice")
    raises("embed_as is checked", lambda: build(payload_measures=[("M", "G", "xml")]), "embed_as")


MODULES = [
    ("00-format.js", "// helpers\nfunction esc(s) {\n  return String(s).replace(/[&<>\"]/g, function (c) {\n"
                     "    return '&#' + c.charCodeAt(0) + ';';\n  });\n}\n"),
    ("01-main.js", "function main(app) {\n  var facts = app.data.FACTS || ''\n"
                   "  app.root.textContent = 'rows: ' + (facts ? facts.split(';').length : 0)\n}\n"),
]
CSS = "/* page */\n#stage { font: 14px 'Segoe UI', sans-serif; }\n.is-empty { opacity: .6; }\n"


def well_formed(dax):
    depth = 0
    for tok in dt.tokenize(dax):
        if tok.kind == dt.STRING and (len(tok.text) < 2 or not tok.text.endswith('"')):
            return False
        if tok.kind == dt.OP and tok.text in "()":
            depth += 1 if tok.text == "(" else -1
    return depth == 0


def test_measures():
    print("\n== the three measures ==")
    r = jm.build_js_app_measures(
        "Demo", css=CSS, js_modules=MODULES, root_id="stage", display_folder="Page",
        payload_measures=[("Demo Facts", "FACTS"), {"measure": "[Demo Names]", "global": "NAMES", "embed_as": "json"},
                          {"measure": "Caption", "global": "CAPTION", "embed_as": "text"}],
        empty_message="Nothing <here> & now", check_with_node=False,
        sample_payloads={"FACTS": "1,2;3,4", "NAMES": '{"a":[[1,"x"]]}', "CAPTION": 'say "hi"'})
    styles, runtime, page = r["measures"]
    eq("names", [m["name"] for m in r["measures"]], ["Demo Styles", "Demo Runtime", "Demo"])
    check("measure dicts carry what a bulk writer needs",
          all(set(m) == {"name", "expression", "description", "display_folder"} and m["display_folder"] == "Page"
              and "\n" not in m["description"] for m in r["measures"]))
    check("each constant is one physical line", "\n" not in styles["expression"] and "\n" not in runtime["expression"])
    check("all three are well-formed DAX", all(well_formed(m["expression"]) for m in r["measures"]))
    eq("styles decode to the minified css", dax_value(styles["expression"]), jm.minify_css(CSS))
    js = dax_value(runtime["expression"])
    eq("runtime decodes to the assembled script",
       js, jm.assemble_runtime(MODULES, namespace="__Demo", root_id="stage", data_globals=["FACTS", "NAMES", "CAPTION"]))
    check("the script is one IIFE in strict mode", js.startswith('(function(){"use strict";') and js.endswith("})();"), js[:40])
    check("line breaks the modules rely on are restored by SUBSTITUTE",
          "var facts=app.data.FACTS||''\napp.root.textContent=" in js and runtime["expression"].startswith("SUBSTITUTE ( "))
    check("comments are gone", "helpers" not in js)
    check("the singleton is named after the prefix", "window.__Demo||(window.__Demo={})" in js and r["namespace"] == "__Demo")
    check("payload globals are handed to the modules", "APP.data={FACTS:window.FACTS,NAMES:window.NAMES,CAPTION:window.CAPTION}" in js)

    doc = page["expression"]
    check("no string literal spans two lines", all(line.count('"') % 2 == 0 for line in doc.split("\n")), doc)
    check("payloads are read once into variables", "VAR _p0 = [Demo Facts]" in doc and "VAR _p1 = [Demo Names]" in doc)
    check("styles and runtime are referenced, not repeated", "[Demo Styles]" in doc and "& [Demo Runtime]" in doc and "function" not in doc)
    check("the required payload gates the script", 'IF (\n        ISBLANK ( _p0 ) || _p0 = "",' in doc, doc)
    check("the empty state holds no script and an escaped message",
          "class='is-empty'>Nothing &lt;here&gt; &amp; now</div>\"," in doc, doc)
    check("a string payload sits between double quotes", '& "window.FACTS=""" & _p0 & """;"' in doc, doc)
    check("a json payload is a value, null when blank", '& "window.NAMES=" & IF ( ISBLANK ( _p1 ) || _p1 = "", "null", _p1 ) & ";"' in doc)
    check("a text payload is escaped in DAX", '& "window.CAPTION=""" & SUBSTITUTE ( ' in doc and "UNICHAR ( 8232 )" in doc)
    check("script order: globals, then the runtime", doc.index("window.FACTS") < doc.index("[Demo Runtime]") < doc.index("</script>"))
    check("no backslash in the document measure", chr(92) not in doc)

    tmdl = tmdl_authoring.render_measures(r["measures"])
    check("the measure writer keeps each constant on its declaration line",
          "\tmeasure 'Demo Runtime' = SUBSTITUTE ( \"(function(){" in tmdl and "\tmeasure 'Demo Styles' = \"#stage{" in tmdl)

    html = r["preview_html"]
    check("the preview follows the measure", html.startswith("<style>" + jm.minify_css(CSS) + "</style><div id='stage'></div><script>window.FACTS=\"1,2;3,4\";"), html[:120])
    check("the preview escapes a text payload", 'window.CAPTION="say \\"hi\\"";' in html, html)
    blank = jm.build_js_app_measures("Demo", css=CSS, js_modules=MODULES, root_id="stage", payload_measures=[("Demo Facts", "FACTS")],
                                     check_with_node=False, sample_payloads={})
    check("a blank required payload previews the empty state", "<script" not in blank["preview_html"] and "is-empty" in blank["preview_html"])

    static = jm.build_js_app_measures("Static", js_modules=MODULES, check_with_node=False)
    check("no payload: no IF, and a warning says so", "IF (" not in static["measures"][2]["expression"]
          and any("static" in w for w in static["warnings"]))
    raw = jm.build_js_app_measures("Demo", css=CSS, js_modules=MODULES, root_id="stage", minify=False, check_with_node=False)
    raw_js = dax_value(raw["measures"][1]["expression"])
    check("without minification the sources ship as written, still on one line",
          "// helpers" in raw_js and "/* ==== 00-format.js ==== */" in raw_js and "\n" not in raw["measures"][1]["expression"])
    eq("unminified css round trip", dax_value(raw["measures"][0]["expression"]), CSS.strip("\n"))


def test_sizes_and_inputs():
    print("\n== sizes, warnings, folder input ==")
    r = jm.build_js_app_measures("Demo", css=CSS, js_modules=MODULES, payload_measures=[("F", "FACTS")], check_with_node=False)
    s = r["sizes"]
    eq("value size of the styles", s["measures"]["Demo Styles"]["value_chars"], len(jm.minify_css(CSS)))
    check("expression size counts the doubled quotes",
          s["measures"]["Demo Runtime"]["expression_chars"] > s["measures"]["Demo Runtime"]["value_chars"])
    check("headroom is the ceiling minus the fixed part",
          s["measures"]["Demo"]["payload_headroom_chars"] == s["ceiling"] - s["measures"]["Demo"]["fixed_chars"])
    eq("per-module sizes", [m["name"] for m in s["modules"]], ["00-format.js", "01-main.js"])
    check("minification shrinks a module", s["modules"][0]["shipped_chars"] < s["modules"][0]["source_chars"])
    check("kept line breaks are counted and explained",
          s["runtime_line_breaks_kept"] > 0 and any("line break" in w for w in r["warnings"]))
    check("notes say what was not verified", any("not been evaluated" in n for n in r["notes"])
          and any("not a documented" in n for n in r["notes"]))
    eq("node check can be turned off", r["node_check"], "skipped")
    tight = jm.build_js_app_measures("Demo", css=CSS, js_modules=MODULES, ceiling=500, check_with_node=False)
    check("over the ceiling is a warning", any("over the 500-character ceiling" in w for w in tight["warnings"]), tight["warnings"])

    d = tempfile.mkdtemp()
    for name, text in (("10-b.js", "function main(app) { app.root.textContent = label(); }\r\n"),
                       ("02-a.js", chr(0xFEFF) + "function label() { return 'from files'; }\n"),
                       ("notes.txt", "not a module")):
        with open(os.path.join(d, name), "w", encoding="utf-8", newline="") as f:
            f.write(text)
    folder = jm.build_js_app_measures("Files", js_modules=d, check_with_node=False)
    eq("a folder is read in filename order, .js only", [m["name"] for m in folder["sizes"]["modules"]], ["02-a.js", "10-b.js"])
    js = dax_value(folder["measures"][1]["expression"])
    check("a byte order mark and CRLF do not reach the script", chr(0xFEFF) not in js and "\r" not in js
          and js.index("function label") < js.index("function main"))
    raises("a missing folder is refused", lambda: jm.build_js_app_measures("X", js_modules=os.path.join(d, "none")), "not a folder")
    eq("placeholder skips characters the script uses", jm.dax_constant("a@b\nc#d"), 'SUBSTITUTE ( "a@b~c#d", "~", UNICHAR ( 10 ) )')
    eq("a quote is doubled", jm.dax_string('say "hi"'), '"say ""hi"""')


HARNESS = r"""
var log = [];
function element() {
  return { textContent: "", listeners: [], addEventListener: function (t, f) { this.listeners.push([t, f]); } };
}
var root = element(), frames = [], windowListeners = [];
globalThis.window = globalThis;
globalThis.addEventListener = function (t, f) { windowListeners.push([t, f]); };
globalThis.requestAnimationFrame = function (f) { frames.push(f); return frames.length; };
globalThis.document = { getElementById: function () { return root; }, addEventListener: function () {} };
globalThis.log = log;
function run(code) { (0, eval)(code); }
var out = {};
window.FACTS = "a;b;c";
run(RUNTIME);                                   // first evaluation
run(RUNTIME);                                   // the measure is evaluated again, same element
out.windowListeners = windowListeners.length;
out.rootListeners = root.listeners.length;
windowListeners[0][1]({});
root.listeners[0][1].call(root, {});
out.framesRequested = frames.length;
frames[0]();
out.afterTwoRuns = log.slice();
out.state = window.__Guard.state.count;
out.runs = window.__Guard.runs;
out.data = window.__Guard.data.FACTS;
var first = root;
root = element();                               // the visual rebuilt the stage
run(RUNTIME);
out.newRootListeners = root.listeners.length;
out.oldRootListeners = first.listeners.length;
out.windowListenersAfterThree = windowListeners.length;
root.listeners[0][1].call(root, {});
out.lastClick = log[log.length - 1];
run(BROKEN);
out.error = root.textContent;
run(ASSIGNED);
out.assigned = root.textContent;
process.stdout.write(JSON.stringify(out));
"""

GUARD_MODULE = """
function main(app) {
  var run = app.runs
  app.state.count = (app.state.count || 0) + 1
  app.on(window, "resize", "fit", function () { log.push("resize handled by run " + run) })
  app.on(app.root, "click", "pick", function () { log.push("click handled by run " + run) })
  app.schedule(function () { log.push("first paint of run " + run) })
  app.schedule(function () { log.push("last paint of run " + run) })
}
"""


def test_reevaluation_guard():
    print("\n== re-evaluation in the same window (node) ==")
    runtime = jm.assemble_runtime([("guard.js", GUARD_MODULE)], namespace="__Guard", root_id="stage", data_globals=["FACTS"])
    broken = jm.assemble_runtime([("broken.js", "function main(app) { throw new Error('boom'); }")],
                                 namespace="__Guard", root_id="stage")
    assigned = jm.assemble_runtime([("late.js", "(function () { APP.main = function (app) { app.root.textContent = 'set'; }; })();")],
                                   namespace="__Guard", root_id="stage")
    check("the bootstrap parses under node --check", jm.node_syntax_check(runtime) in ("passed", "skipped"), jm.node_syntax_check(runtime))
    if not NODE:
        print("  [SKIP] bootstrap behaviour (node is not on PATH)")
        return
    eq("a syntax error is reported, not raised", jm.node_syntax_check("function ( {").split(":")[0], "failed")
    script = (f"var RUNTIME = {json.dumps(runtime)}, BROKEN = {json.dumps(broken)}, ASSIGNED = {json.dumps(assigned)};\n"
              + HARNESS)
    out, err = run_node(script)
    if out is None:
        check("the harness runs", False, err)
        return
    eq("one window listener after two evaluations", out["windowListeners"], 1)
    eq("one listener on the stage after two evaluations", out["rootListeners"], 1)
    eq("one animation frame for four schedule calls", out["framesRequested"], 1)
    eq("events and the frame reach the newest handlers",
       out["afterTwoRuns"], ["resize handled by run 2", "click handled by run 2", "last paint of run 2"])
    eq("state survives re-evaluation", (out["state"], out["runs"]), (2, 2))
    eq("payload globals are exposed", out["data"], "a;b;c")
    eq("a rebuilt stage gets its own listener", (out["newRootListeners"], out["oldRootListeners"]), (1, 1))
    eq("the window listener is still wired once", out["windowListenersAfterThree"], 1)
    eq("the new stage calls the newest handler", out["lastClick"], "click handled by run 3")
    eq("an exception is shown on the stage instead of a blank page", out["error"], "Script error: boom")
    eq("a module may set APP.main instead of declaring main", out["assigned"], "set")
    full = jm.build_js_app_measures("Guard", js_modules=[("guard.js", GUARD_MODULE)], payload_measures=[("F", "FACTS")])
    eq("the build runs the node check by default", full["node_check"], "passed")
    bad = jm.build_js_app_measures("Guard", js_modules=[("bad.js", "function main( {")])
    check("a script node rejects is reported in node_check and in warnings",
          bad["node_check"].startswith("failed") and any("Node rejected" in w for w in bad["warnings"]), bad["node_check"])


def test_payload_to_page():
    """The whole chain with hostile text: the escaping the payload measure applies (modelled
    in Python), the document the page measure assembles, the browser's script parser (here:
    Node's), the generated parsers, and the module that paints."""
    print("\n== from payload text to the painted stage (node) ==")
    import payload_measures as pm
    nasty = 'A "quoted", name; with </script><script>x</script> ' + chr(92) + " and " + chr(0x2028) + "line\nbreak"
    facts = pm.generate_payload_measure(
        "Facts", "Sales", [{"column": "Key", "type": "int", "name": "key"}, {"column": "Label", "type": "text", "name": "label"},
                           {"column": "Amount", "type": "decimal", "name": "amount"}], parser_name="parseFacts")
    names = pm.generate_dimension_payload("Names", [{"name": "site", "table": "Site", "key": "SiteKey", "labels": ["Name"]}],
                                          format="json", parser_name="parseNames")
    steps = pm.text_escape_steps("delimited", (",", ";"))
    facts_text = ";".join(["1," + pm.apply_escape(nasty, steps) + ",1250", "2," + pm.apply_escape("plain", steps) + ",-5"])
    names_text = '{"site":[[7,"' + pm.apply_escape(nasty, pm.text_escape_steps("json")) + '"]]}'
    main = ("function main(app) {\n"
            "  var t = parseFacts(app.data.FACTS), d = parseNames(app.data.NAMES)\n"
            "  app.state.result = { rows: t.length, label: t.row(0).label, amount: t.columns.amount[0], site: d.site.byKey[7].Name }\n"
            "  app.root.textContent = 'rows ' + t.length\n"
            "}\n")
    r = jm.build_js_app_measures(
        "Chain", js_modules=[("00-parsers.js", facts["js_parser"] + "\n" + names["js_parser"]), ("01-main.js", main)],
        payload_measures=[{"measure": "Facts", "global": "FACTS", "embed_as": facts["embed_as"]},
                          {"measure": "Names", "global": "NAMES", "embed_as": names["embed_as"]},
                          {"measure": "Caption", "global": "CAPTION", "embed_as": "text"}],
        root_id="stage", sample_payloads={"FACTS": facts_text, "NAMES": names_text, "CAPTION": nasty})
    html = r["preview_html"]
    eq("the embed hints are the ones the page builder expects", (facts["embed_as"], names["embed_as"]), ("string", "json"))
    eq("the data never closes the script element: one opener, one closer", (html.count("<script"), html.lower().count("</script")), (1, 1))
    check("no raw line break or line separator reaches the payload part of the script",
          "\n" not in html[html.index("<script>"):html.index("(function(){")] and chr(0x2028) not in html)
    if not NODE:
        print("  [SKIP] running the page script (node is not on PATH)")
        return
    eq("the generated parsers passed the build's own node check", r["node_check"], "passed")
    script = html[html.index("<script>") + len("<script>"):html.rindex("</script>")]
    harness = ("var root = { textContent: '', addEventListener: function () {} };\n"
               "globalThis.window = globalThis;\n"
               "globalThis.document = { getElementById: function () { return root; } };\n"
               "globalThis.requestAnimationFrame = function () { return 1; };\n"
               f"(0, eval)({json.dumps(script)});\n"
               "process.stdout.write(JSON.stringify({ stage: root.textContent, result: window.__Chain.state.result, "
               "caption: window.CAPTION, error: String(window.__Chain.error) }));\n")
    out, err = run_node(harness)
    if out is None:
        check("the page script runs", False, err)
        return
    eq("the stage is painted", out["stage"], "rows 2")
    eq("no script error", out["error"], "null")
    eq("hostile text arrives intact through the delimited payload", out["result"]["label"], nasty)
    eq("and through the JSON payload", out["result"]["site"], nasty)
    eq("and through a text payload escaped in the document measure", out["caption"], nasty)
    eq("a scaled decimal is divided back", (out["result"]["rows"], out["result"]["amount"]), (2, 12.5))


if __name__ == "__main__":
    for fn in (test_minifier_cases, test_minifier_semantics, test_css_minifier, test_safety_checks,
               test_measures, test_sizes_and_inputs, test_reevaluation_guard, test_payload_to_page):
        fn()
    print()
    if _failures:
        print(f"FAILED: {len(_failures)} check(s): {_failures}")
        sys.exit(1)
    print("ALL JS APP MEASURE TESTS PASSED")
