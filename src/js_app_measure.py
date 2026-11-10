"""
Assemble a JavaScript / CSS application into DAX measures for an HTML visual.

Some HTML visuals render a text measure as a document and run the inline script it holds.
That makes a measure a delivery vehicle for a small application: the stylesheet and the
script are constants, the data is a payload measure (see payload_measures), and one more
measure glues them into the document the visual binds to:

    <Prefix> Styles    the stylesheet, a constant
    <Prefix> Runtime   the script, a constant: an IIFE holding a small bootstrap and the modules
    <Prefix>           <style> + stage element + <script> payload globals, then the runtime

What this module takes care of, because each one has broken a real build:

  * One physical line per constant. A measure writer re-indents the lines of an expression,
    which would change the inside of a string literal. Line breaks the script still needs are
    carried as a placeholder character and restored by one SUBSTITUTE.
  * A minifier that is safe by construction. It strips comments and redundant white space
    and nothing else. It knows string, template and regular-expression literals (a regex
    holding a quote would otherwise desynchronise everything after it), and it only joins two
    lines where automatic semicolon insertion could not have applied; otherwise it keeps the
    line break. Telling a regex from a division uses the usual rule: after a value (a name,
    a number, ")", "]", "}") a slash divides; after an operator, a punctuator or a keyword
    such as return it opens a regex.
  * Checks that fail the build: "</script" or "<!--" in the script, "</style" in the
    stylesheet, control characters, and the two Unicode line separators.
  * Re-evaluation. The visual runs the whole script again on every measure evaluation, in
    the same window. The bootstrap keeps one singleton per application and routes every event
    listener through it, so a second run replaces a handler instead of adding another.

The script is checked with Node when it is on PATH. Nothing here can evaluate the DAX or
render the page; see the notes returned with each build.

Module contract (what the modules can rely on): they are concatenated in order inside one
function, in strict mode. ``APP`` is the singleton: APP.root (the stage element), APP.data
(the payload globals), APP.state (survives re-evaluation), APP.runs (1 on the first run),
APP.on(target, type, key, handler) and APP.schedule(fn). If a module defines
``function main(app)`` (or sets APP.main), it is called once per evaluation after all
modules have loaded.
"""
import html
import os
import re
import shutil
import subprocess
import tempfile
import unicodedata
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

import dax_tokens
import payload_measures as _payload      # aliased: build_js_app_measures has a parameter of that name
import safe_io

DEFAULT_CEILING = _payload.DEFAULT_CEILING

_BS = chr(92)
_LINE_SEPARATORS = (chr(0x2028), chr(0x2029))
# After these words a slash starts a regex, and a statement cannot end.
_OPERAND_WORDS = frozenset(
    "return typeof instanceof in of new delete void do else case yield await throw".split())
# A line break after these words ends the statement (a "restricted production").
_RESTRICTED_WORDS = frozenset("return break continue throw yield async await".split())
# After these words more of the same statement must follow, so a line break changes nothing.
_NO_END_WORDS = frozenset(
    "else typeof new delete void in instanceof case var const do try finally function".split())
_EMBED_KINDS = ("string", "json", "text")


class BuildError(ValueError):
    """The sources cannot be shipped safely inside a measure."""


# ---------------------------------------------------------------- JavaScript minifier

class _Tok:
    __slots__ = ("kind", "text", "nl", "ws")

    def __init__(self, kind: str, text: str, nl: bool, ws: bool):
        self.kind, self.text, self.nl, self.ws = kind, text, nl, ws


def _is_word_char(ch: str) -> bool:
    return ch.isalnum() or ch in "_$"


def _is_value(tok: _Tok) -> bool:
    """True when ``tok`` can be the last token of an expression."""
    if tok.kind == "word":
        return tok.text not in _OPERAND_WORDS
    return tok.kind in ("string", "template", "regex") or tok.text in ")]}"


def _slash_is_regex(toks: List[_Tok]) -> bool:
    if not toks:
        return True
    prev = toks[-1]
    if prev.kind != "punct":
        return not _is_value(prev)
    if prev.text in ")]}":
        return False
    if (prev.text in "+-" and len(toks) >= 3 and not prev.ws and not prev.nl
            and toks[-2].kind == "punct" and toks[-2].text == prev.text and _is_value(toks[-3])):
        return False                      # a++ / b: the slash follows a postfix operator
    return True


def _scan_string(js: str, i: int) -> int:
    quote, n = js[i], len(js)
    i += 1
    while i < n:
        ch = js[i]
        if ch == _BS and i + 1 < n:
            i += 2
            continue
        if ch == quote:
            return i + 1
        if ch == "\n":
            return i                      # unterminated: stop before the line break
        i += 1
    return n


def _scan_regex(js: str, i: int) -> int:
    """Index just past the regex literal opening at ``i``, or -1 if the line ends first."""
    n, in_class = len(js), False
    j = i + 1
    while j < n:
        ch = js[j]
        if ch == "\n":
            return -1
        if ch == _BS and j + 1 < n:
            j += 2
            continue
        if ch == "[":
            in_class = True
        elif ch == "]":
            in_class = False
        elif ch == "/" and not in_class:
            j += 1
            while j < n and (js[j].isalpha()):
                j += 1
            return j
        j += 1
    return -1


def _scan_template(js: str, i: int) -> Tuple[str, int]:
    """Minified text of the template literal opening at ``i`` and the index just past it.
    The literal text is kept exactly; only the code inside ${ } is minified."""
    n = len(js)
    out = ["`"]
    i += 1
    while i < n:
        ch = js[i]
        if ch == _BS and i + 1 < n:
            out.append(js[i:i + 2])
            i += 2
        elif ch == "`":
            out.append(ch)
            return "".join(out), i + 1
        elif ch == "$" and js.startswith("${", i):
            toks, i = _lex(js, i + 2, True)
            out.append("${" + _emit(toks) + "}")
        else:
            out.append(ch)
            i += 1
    return "".join(out), n


def _lex(js: str, i: int, in_template: bool) -> Tuple[List[_Tok], int]:
    """Tokens from ``i`` to the end, or to the brace that closes a template ${ } expression."""
    toks: List[_Tok] = []
    n, depth = len(js), 0
    nl = ws = False

    def add(kind: str, text: str) -> None:
        nonlocal nl, ws
        toks.append(_Tok(kind, text, nl, ws))
        nl = ws = False

    while i < n:
        ch = js[i]
        nxt = js[i + 1] if i + 1 < n else ""
        if ch == "\n":
            nl = ws = True
            i += 1
        elif ch.isspace() or ch == chr(0xFEFF):
            ws = True
            i += 1
        elif ch == "/" and nxt == "/":
            while i < n and js[i] != "\n":
                i += 1
            ws = True
        elif ch == "/" and nxt == "*":
            end = js.find("*/", i + 2)
            end = n if end < 0 else end + 2
            nl = nl or "\n" in js[i:end]  # a comment that spans lines ends the line for ASI
            ws = True
            i = end
        elif ch in "\"'":
            end = _scan_string(js, i)
            add("string", js[i:end])
            i = end
        elif ch == "`":
            text, i = _scan_template(js, i)
            add("template", text)
        elif ch == "/" and _slash_is_regex(toks) and (end := _scan_regex(js, i)) > 0:
            add("regex", js[i:end])
            i = end
        elif _is_word_char(ch):
            end = i + 1
            while end < n and _is_word_char(js[end]):
                end += 1
            add("word", js[i:end])
            i = end
        else:
            if ch == "{":
                depth += 1
            elif ch == "}":
                if in_template and depth == 0:
                    return toks, i + 1
                depth -= 1
            add("punct", ch)
            i += 1
    return toks, i


def _doubled(toks: List[_Tok], k: int) -> bool:
    """True when toks[k] and toks[k + 1] are an adjacent ++ or --."""
    return (k + 1 < len(toks) and toks[k].kind == "punct" and toks[k].text in "+-"
            and toks[k + 1].kind == "punct" and toks[k + 1].text == toks[k].text
            and not toks[k + 1].ws and not toks[k + 1].nl)


def _may_end_statement(toks: List[_Tok], k: int) -> bool:
    """Could a statement end at toks[k], so that a following line break might be a semicolon?"""
    tok = toks[k]
    if tok.kind == "word":
        return tok.text not in _NO_END_WORDS
    if tok.kind != "punct":
        return True
    if tok.text in ")]}":
        return True
    return k > 0 and _doubled(toks, k - 1)          # a++ at the end of a line


def _continues(toks: List[_Tok], k: int) -> bool:
    """Is toks[k] a token that can only carry on the previous expression (or is an error with
    and without the line break alike)? Then the break before it is not a semicolon."""
    tok = toks[k]
    if tok.kind == "template":
        return True                                  # a tagged template continues across lines
    if tok.kind == "word":
        return tok.text in ("in", "instanceof")
    if tok.kind != "punct":
        return False
    nxt = toks[k + 1] if k + 1 < len(toks) else None
    adjacent = nxt is not None and not nxt.ws and not nxt.nl
    if tok.text == ".":
        return not (adjacent and nxt.kind == "word" and nxt.text[:1].isdigit())   # .5 is a number
    if tok.text in "+-":
        return not _doubled(toks, k)                 # ++x on a new line starts a statement
    if tok.text == "!":
        return adjacent and nxt.kind == "punct" and nxt.text == "="
    return tok.text in ",;)]}?:=*%&|^<>(["


def _needs_space(prev: _Tok, tok: _Tok) -> bool:
    a, b = prev.text[-1], tok.text[0]
    if _is_word_char(a) and _is_word_char(b):
        return True
    if prev.kind == "word" and a.isdigit() and b == ".":
        return True                                  # 1 .toFixed() must not become 1.toFixed()
    # Pairs that would read as one token, a comment or an HTML comment if they touched.
    return (a, b) in (("+", "+"), ("-", "-"), ("/", "/"), ("/", "*"), ("<", "!"))


def _emit(toks: List[_Tok]) -> str:
    out: List[str] = []
    for k, tok in enumerate(toks):
        if k:
            prev = toks[k - 1]
            if tok.nl and (prev.kind == "word" and prev.text in _RESTRICTED_WORDS
                           or (_may_end_statement(toks, k - 1) and not _continues(toks, k))):
                out.append("\n")
            elif (tok.nl or tok.ws) and _needs_space(prev, tok):
                out.append(" ")
        out.append(tok.text)
    return "".join(out)


def minify_js(js: str) -> str:
    """``js`` without comments and redundant white space.

    Semantics are kept, not guessed: a line break is removed only where it cannot have been
    acting as a semicolon, so the result may still span several lines. Not handled: a regex
    literal directly after ")" or "}" (as in ``if (x) /re/.test(y)``) is read as a division,
    which is harmless unless that regex holds a quote, a "//" or a run of spaces.
    """
    toks, _ = _lex((js or "").replace("\r\n", "\n").replace("\r", "\n"), 0, False)
    return _emit(toks)


# ---------------------------------------------------------------- CSS minifier

def minify_css(css: str) -> str:
    """``css`` on one line, without comments. Strings and unquoted url() bodies are kept
    exactly; spaces are only dropped around { } ; and , where they never carry meaning."""
    s = (css or "").replace("\r\n", "\n").replace("\r", "\n")
    out: List[str] = []
    i, n, skip = 0, len(s), True
    while i < n:
        ch = s[i]
        if ch in "\"'":
            end = _scan_string(s, i)
            out.append(s[i:end])
            i, skip = end, False
        elif s.startswith("/*", i):
            end = s.find("*/", i + 2)
            i = n if end < 0 else end + 2
            if not skip and out and out[-1] != " ":
                out.append(" ")
        elif s[i:i + 4].lower() == "url(" and s[i + 4:].lstrip()[:1] not in ("'", '"'):
            end = s.find(")", i)
            end = n if end < 0 else end + 1
            out.append(s[i:end].replace("\n", ""))
            i, skip = end, False
        elif ch.isspace():
            if not skip and out and out[-1] != " ":
                out.append(" ")
            i += 1
        elif ch in "{};,":
            if out and out[-1] == " ":
                out.pop()
            out.append(ch)
            i, skip = i + 1, True
        else:
            out.append(ch)
            i, skip = i + 1, False
    return "".join(out).strip()


# ---------------------------------------------------------------- safety checks

def check_source(text: str, label: str, language: str, warnings: Optional[List[str]] = None) -> None:
    """Raise BuildError if ``text`` cannot sit inside a measure and the element it ships in.

    language is 'js' (inside <script>) or 'css' (inside <style>).
    """
    _check_characters(text, label, warnings)
    _check_closers(text, label, language)


def _check_characters(text: str, label: str, warnings: Optional[List[str]] = None) -> None:
    line, col = 1, 0
    for ch in text:
        if ch == "\n":
            line, col = line + 1, 0
            continue
        col += 1
        code = ord(ch)
        if ch in _LINE_SEPARATORS:
            raise BuildError(
                f"{label}: line {line}, column {col} holds U+{code:04X}, a Unicode line separator. It "
                "ends a line in JavaScript and in a measure definition. Write it as an escape "
                f"({_BS}u{code:04X}) or build it with String.fromCharCode({code}).")
        if (code < 32 and ch != "\t") or 0x7F <= code <= 0x9F:
            raise BuildError(
                f"{label}: line {line}, column {col} holds the control character 0x{code:02X}. A model "
                "rejects a measure that contains one. A numeric escape was probably read by a quoting "
                f"layer on the way here; build the character with String.fromCharCode({code}) instead.")
        if warnings is not None and unicodedata.category(ch) == "Cf":
            warnings.append(f"{label}: line {line}, column {col} holds the invisible character "
                            f"U+{code:04X}. It ships as written.")


def _check_closers(text: str, label: str, language: str) -> None:
    low = text.lower()
    closers = ("</script", "<!--") if language == "js" else ("</style", "</script")
    for bad in closers:
        at = low.find(bad)
        if at >= 0:
            where = text.count("\n", 0, at) + 1
            element = "script" if language == "js" else "style"
            raise BuildError(
                f"{label}: line {where} contains {bad!r}, which would end or corrupt the <{element}> "
                "element the code ships in. Split the text (for example '<' + '/script') or escape the "
                "slash.")


# ---------------------------------------------------------------- DAX text

def dax_string(text: str) -> str:
    """A DAX string literal. DAX has no backslash escapes: only the quote is doubled."""
    return '"' + text.replace('"', '""') + '"'


def _placeholder(text: str) -> str:
    """A single character that does not occur in ``text``, to stand in for a line break."""
    candidates = list("@#~^|$`") + [chr(c) for c in (0xA7, 0xB6, 0xA4, 0xA6, 0xAC, 0xB1, 0xD7, 0xF7)]
    candidates += [chr(c) for c in range(0x2460, 0x2500)]
    for ch in candidates:
        if ch not in text:
            return ch
    raise BuildError("No free placeholder character for line breaks: the source uses every candidate.")


def dax_constant(text: str) -> str:
    """A one-line DAX expression whose value is ``text``, line breaks included."""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    if "\n" not in text:
        return dax_string(text)
    mark = _placeholder(text)
    return f"SUBSTITUTE ( {dax_string(text.replace(chr(10), mark))}, {dax_string(mark)}, UNICHAR ( 10 ) )"


# ---------------------------------------------------------------- bootstrap

_BOOTSTRAP_HEAD = """\
(function () {
"use strict";
var APP = window.NAMESPACE || (window.NAMESPACE = {});
APP.state = APP.state || {};
APP.handlers = APP.handlers || {};
APP.runs = (APP.runs || 0) + 1;
APP.root = document.getElementById(ROOT_ID);
APP.data = DATA;
APP.main = null;
APP.error = null;
APP.pending = null;
// One real listener per (target, type, key). A later run only swaps the handler it calls,
// so re-evaluating the measure never wires an event twice and never leaves a stale closure.
APP.on = function (target, type, key, handler, options) {
  var shared = target === window || target === document;
  var store = shared ? APP.handlers : (target.appHandlers || (target.appHandlers = {}));
  var id = (target === window ? "w|" : target === document ? "d|" : "e|") + type + "|" + key;
  var wired = Object.prototype.hasOwnProperty.call(store, id);
  store[id] = handler;
  if (!wired) {
    target.addEventListener(type, function (event) { return store[id].call(this, event); }, options);
  }
};
// At most one repaint per frame; the last function asked for wins.
APP.schedule = function (fn) {
  APP.pending = fn;
  if (APP.frame) { return; }
  APP.frame = window.requestAnimationFrame(function () {
    var next = APP.pending;
    APP.frame = 0;
    APP.pending = null;
    if (next) { next(APP); }
  });
};
try {
"""

_BOOTSTRAP_TAIL = """\
var entry = typeof main === "function" ? main : APP.main;
if (typeof entry === "function") { entry(APP); }
} catch (error) {
  APP.error = error;
  if (APP.root) {
    APP.root.textContent = "Script error: " + (error && error.message ? error.message : error);
  }
}
})();
"""


def _js_string(text: str) -> str:
    """A double-quoted JavaScript string literal for trusted build-time text."""
    out = ['"']
    for ch in text:
        if ch in (_BS, '"'):
            out.append(_BS + ch)
        elif ch == "<" or ord(ch) < 32 or ord(ch) > 126:
            out.append(_BS + "u%04X" % ord(ch))
        else:
            out.append(ch)
    return "".join(out) + '"'


def _unix(text: str) -> str:
    return (text or "").replace("\r\n", "\n").replace("\r", "\n")


def _shipped_modules(js_modules: Sequence[Tuple[str, str]], minify: bool,
                     warnings: Optional[List[str]] = None) -> List[Tuple[str, str]]:
    """(name, body as it ships) per module, each one checked under its own name so an error
    points at a file. The closers are looked for in what ships: a comment the minifier
    removed can no longer end the script element."""
    out = []
    for name, source in js_modules:
        text = _unix(source)
        _check_characters(text, name, warnings)
        if minify:
            body = minify_js(text)
        else:
            title = re.sub(r"[^A-Za-z0-9._ -]", "_", name)
            body = f"/* ==== {title} ==== */\n" + text.strip("\n")
        _check_closers(body, name, "js")
        out.append((name, body))
    return out


def _wrap(bodies: Sequence[str], namespace: str, root_id: str, data_globals: Sequence[str],
          minify: bool) -> str:
    data = "{" + ", ".join(f"{g}: window.{g}" for g in data_globals) + "}"
    head = (_BOOTSTRAP_HEAD.replace("NAMESPACE", namespace).replace("ROOT_ID", _js_string(root_id))
            .replace("DATA", data))
    parts = [minify_js(head) if minify else head.rstrip("\n")]
    parts += [b for b in bodies if b]
    parts.append(minify_js(_BOOTSTRAP_TAIL) if minify else _BOOTSTRAP_TAIL.rstrip("\n"))
    return "\n".join(parts)


def assemble_runtime(js_modules: Sequence[Tuple[str, str]], *, namespace: str, root_id: str,
                     data_globals: Sequence[str] = (), minify: bool = True) -> str:
    """The script that ships: bootstrap head, the modules in order, bootstrap tail.
    Raises BuildError when a module cannot be shipped."""
    bodies = [body for _, body in _shipped_modules(js_modules, minify)]
    return _wrap(bodies, namespace, root_id, data_globals, minify)


# ---------------------------------------------------------------- inputs

def _read_modules(js_modules: Any) -> List[Tuple[str, str]]:
    if isinstance(js_modules, (str, os.PathLike)):
        folder = os.fspath(js_modules)
        if not os.path.isdir(folder):
            raise BuildError(f"js_modules: '{folder}' is not a folder. Pass a folder of .js files or a "
                             "list of (name, source) pairs.")
        names = sorted(f for f in os.listdir(folder) if f.lower().endswith(".js"))
        if not names:
            raise BuildError(f"js_modules: no .js files in '{folder}'.")
        return [(f, safe_io.read_text(os.path.join(folder, f))[0]) for f in names]
    out = []
    for i, item in enumerate(js_modules or []):
        if isinstance(item, dict):
            item = (item.get("name"), item.get("source"))
        if not isinstance(item, (tuple, list)) or len(item) != 2 or not isinstance(item[1], str):
            raise BuildError(f"js_modules #{i + 1} must be a (name, source) pair.")
        out.append((str(item[0] or f"module-{i + 1}"), item[1]))
    if not out:
        raise BuildError("js_modules is empty: the page needs at least one script module.")
    return out


def _payloads(specs: Optional[Sequence[Any]]) -> List[Dict[str, Any]]:
    out, seen = [], set()
    for i, spec in enumerate(specs or []):
        if isinstance(spec, (tuple, list)):
            spec = dict(zip(("measure", "global", "embed_as"), spec))
        if not isinstance(spec, dict):
            raise BuildError(f"payload_measures #{i + 1} must be a dict or a (measure, global) pair.")
        measure = dax_tokens.unbracket(str(spec.get("measure") or spec.get("name") or "").strip())
        target = str(spec.get("global") or "").strip()
        embed = str(spec.get("embed_as") or spec.get("as") or "string").lower()
        if not measure:
            raise BuildError(f"payload_measures #{i + 1} needs the name of a measure.")
        if not _payload.is_js_identifier(target):
            raise BuildError(f"payload_measures #{i + 1}: 'global' must be a JavaScript identifier, "
                             f"got {target!r}.")
        if target in seen:
            raise BuildError(f"payload_measures: the global '{target}' is used twice.")
        if embed not in _EMBED_KINDS:
            raise BuildError(f"payload_measures #{i + 1}: embed_as must be one of {', '.join(_EMBED_KINDS)}.")
        seen.add(target)
        out.append({"measure": measure, "global": target, "embed_as": embed,
                    "required": bool(spec.get("required", i == 0))})
    return out


def _identifier(prefix: str) -> str:
    words = re.findall(r"[A-Za-z0-9]+", prefix)
    ident = "".join(w[:1].upper() + w[1:] for w in words)
    if not ident:
        raise BuildError("prefix must contain at least one letter or digit.")
    return "__" + ident


# ---------------------------------------------------------------- node

def node_syntax_check(js: str, timeout: int = 60) -> str:
    """'passed', 'failed: <first lines of the error>' or 'skipped' (node is not on PATH)."""
    node = shutil.which("node")
    if not node:
        return "skipped"
    folder = tempfile.mkdtemp(prefix="js-app-measure-")
    path = os.path.join(folder, "runtime.js")
    try:
        safe_io.write_text(path, js)
        done = subprocess.run([node, "--check", path], capture_output=True, text=True,
                              encoding="utf-8", errors="replace", timeout=timeout)
    except (OSError, subprocess.SubprocessError) as e:
        return f"skipped ({type(e).__name__}: node could not be run)"
    finally:
        shutil.rmtree(folder, ignore_errors=True)
    if done.returncode == 0:
        return "passed"
    detail = (done.stderr or done.stdout or "").replace(path, "runtime.js").strip().splitlines()
    return "failed: " + " | ".join(line.strip() for line in detail[:6] if line.strip())


# ---------------------------------------------------------------- the build

def _document_dax(styles_ref: str, runtime_ref: str, payloads: List[Dict[str, Any]], root_id: str,
                  empty_html: str) -> str:
    stage = f"<div id='{root_id}'></div>"
    lines = [f"VAR _p{i} = {dax_tokens.bracket(p['measure'])}" for i, p in enumerate(payloads)]
    lines.append(f'VAR _style = "<style>" & {styles_ref} & "</style>"')
    script = ['& "<script>"']
    for i, p in enumerate(payloads):
        target = f"window.{p['global']}="
        if p["embed_as"] == "json":
            script.append(f'& "{target}" & IF ( ISBLANK ( _p{i} ) || _p{i} = "", "null", _p{i} ) & ";"')
        else:
            value = f"_p{i}"
            if p["embed_as"] == "text":      # arbitrary text: make it a JavaScript string body first
                value = _payload.dax_escape(value, _payload.text_escape_steps("json"))
            script.append(f'& "{target}""" & {value} & """;"')
    script += [f"& {runtime_ref}", '& "</script>"']
    page = ["_style", f"    & {dax_string(stage)}"] + ["    " + s for s in script]
    required = [f'ISBLANK ( _p{i} ) || _p{i} = ""' for i, p in enumerate(payloads) if p["required"]]
    lines.append("RETURN")
    if not required:
        lines += ["    " + s for s in page]
    else:
        lines.append("    IF (")
        lines.append("        " + " || ".join(required) + ",")
        lines.append(f"        _style & {dax_string(empty_html)},")
        lines += ["        " + s for s in page]
        lines.append("    )")
    return "\n".join(lines)


def build_js_app_measures(
        prefix: str, *, css: str = "",
        js_modules: Union[Sequence[Tuple[str, str]], str, os.PathLike, None] = None,
        payload_measures: Optional[Sequence[Any]] = None, root_id: str = "app-root",
        empty_message: str = "No data for the current filters.", minify: bool = True,
        display_folder: Optional[str] = None, ceiling: int = DEFAULT_CEILING,
        check_with_node: Optional[bool] = None,
        sample_payloads: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    """Build the three measures of a script-driven HTML page.

    prefix: measure name stem. The document measure is ``prefix`` itself.
    css: the stylesheet source.
    js_modules: ordered (name, source) pairs, or a folder whose .js files are taken in
        filename order (00-format.js, 01-state.js, ...).
    payload_measures: each {"measure", "global", "embed_as"?, "required"?} or a
        (measure, global[, embed_as]) tuple. embed_as: 'string' for text that is already inert
        between double quotes (a delimited payload), 'json' for a JSON value, 'text' for
        arbitrary text (escaped in DAX first). The page shows ``empty_message`` instead of
        running the script when a required payload is blank; by default only the first is.
    check_with_node: None checks the script with Node when it is on PATH; False never checks.
    sample_payloads: {global: text}. When given, the result also holds ``preview_html``, the
        document the measure would return for those values, for a browser or a test runner.

    Returns {measures: [{name, expression, description, display_folder}], sizes, warnings,
    node_check, notes, namespace, root_id}. Raises BuildError when a source cannot be shipped.
    """
    stem = str(prefix or "").strip()
    if not stem or "\n" in stem or "\r" in stem:
        raise BuildError("prefix must be a one-line measure name stem.")
    if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_-]*", root_id or ""):
        raise BuildError("root_id must start with a letter and hold only letters, digits, '-' and '_'.")
    namespace = _identifier(stem)
    modules = _read_modules(js_modules)
    payloads = _payloads(payload_measures)
    warnings: List[str] = []

    css_source = _unix(css)
    _check_characters(css_source, "css", warnings)
    css_text = minify_css(css_source) if minify else css_source.strip("\n")
    _check_closers(css_text, "css", "css")
    shipped = _shipped_modules(modules, minify, warnings)
    runtime = _wrap([body for _, body in shipped], namespace, root_id,
                    [p["global"] for p in payloads], minify)

    node_check = "skipped" if check_with_node is False else node_syntax_check(runtime)
    if node_check.startswith("failed"):
        warnings.append("Node rejected the assembled script, so the page would stay blank: " + node_check[8:])
    elif node_check.startswith("skipped") and check_with_node is not False:
        warnings.append("The script was not syntax-checked: node is not available on this machine.")

    styles_name, runtime_name = f"{stem} Styles", f"{stem} Runtime"
    empty_html = f"<div id='{root_id}' class='is-empty'>{html.escape(empty_message or '', quote=True)}</div>"
    document = _document_dax(dax_tokens.bracket(styles_name), dax_tokens.bracket(runtime_name), payloads,
                             root_id, empty_html)
    listed = ", ".join(n for n, _ in modules[:6]) + (", ..." if len(modules) > 6 else "")
    sources = ", ".join(p["measure"] for p in payloads) or "no payload"
    measures = [
        {"name": styles_name, "expression": dax_constant(css_text),
         "description": f"Stylesheet for '{stem}'. A constant with no model dependency. "
                        "Generated: change the source and rebuild.",
         "display_folder": display_folder},
        {"name": runtime_name, "expression": dax_constant(runtime),
         "description": f"Script for '{stem}' ({len(modules)} modules: {listed}). A constant "
                        "with no model dependency. Generated: change the modules and rebuild.",
         "display_folder": display_folder},
        {"name": stem, "expression": document,
         "description": f"The '{stem}' document for an HTML visual that runs inline scripts: styles, "
                        f"the stage element '{root_id}', the data ({sources}) and the script. Bind the "
                        "visual to this measure.",
         "display_folder": display_folder},
    ]

    kept_breaks = runtime.count("\n")
    fixed = len(css_text) + len(runtime) + len("<style></style><script></script>") + len(root_id) + 20
    fixed += sum(len(p["global"]) + 12 for p in payloads)
    sizes = {
        "ceiling": int(ceiling),
        "measures": {
            styles_name: _size(len(css_text), len(measures[0]["expression"]), ceiling),
            runtime_name: _size(len(runtime), len(measures[1]["expression"]), ceiling),
            stem: {"fixed_chars": fixed, "expression_chars": len(document),
                   "share_of_ceiling": round(fixed / ceiling, 4) if ceiling else None,
                   "payload_headroom_chars": int(ceiling) - fixed},
        },
        "modules": [{"name": name, "source_chars": len(source), "shipped_chars": len(body)}
                    for (name, source), (_, body) in zip(modules, shipped)],
        "css_source_chars": len(css_source),
        "runtime_line_breaks_kept": kept_breaks,
    }
    if fixed > ceiling:
        warnings.append(f"The styles and the script alone are {fixed:,} characters, over the "
                        f"{ceiling:,}-character ceiling: the document measure cannot return them.")
    elif fixed > ceiling * 0.5:
        warnings.append(f"The styles and the script use {fixed / ceiling:.0%} of the {ceiling:,}-character "
                        "ceiling, which leaves little room for data.")
    if kept_breaks:
        warnings.append(f"{kept_breaks} line break(s) are kept in the script, because removing them "
                        "could change what it does (a statement may end there without a semicolon). "
                        "They are restored by one SUBSTITUTE in the runtime measure.")
    if not payloads:
        warnings.append("No payload measure: the page is static and never shows the empty message.")

    result: Dict[str, Any] = {
        "measures": measures, "sizes": sizes, "warnings": warnings, "node_check": node_check,
        "namespace": namespace, "root_id": root_id, "notes": _notes(ceiling, payloads),
    }
    if sample_payloads is not None:
        result["preview_html"] = _preview(css_text, runtime, payloads, root_id, empty_html, sample_payloads)
    return result


def _size(value_chars: int, expression_chars: int, ceiling: int) -> Dict[str, Any]:
    return {"value_chars": value_chars, "expression_chars": expression_chars,
            "share_of_ceiling": round(value_chars / ceiling, 4) if ceiling else None}


def _notes(ceiling: int, payloads: List[Dict[str, Any]]) -> List[str]:
    notes = [
        "Bind an HTML visual that runs inline <script> to the document measure. Many HTML visuals, "
        "certified ones in particular, remove scripts: test the visual you plan to use first.",
        "Everything in a measure can be read by anyone who can open the report or the model. Never "
        "put a key, a token or an endpoint that relies on secrecy in the script or the styles.",
        f"The {ceiling:,}-character ceiling is an empirical figure from one project, not a documented "
        "Power BI limit. Splitting the page into several measures does not raise it: the document "
        "measure still returns one string.",
        "Not verified here: the DAX has not been evaluated and the page has not been rendered. The "
        "script was only parsed (see node_check).",
        "The visual cannot hand a selection back to Power BI, so the page filters itself: slicers "
        "and other visuals filter it (the measure is evaluated again), never the other way round.",
    ]
    if any(p["embed_as"] == "string" for p in payloads):
        notes.append("A payload embedded as 'string' is placed between double quotes as it is. It must "
                     "not contain a double quote, a backslash, a line break or '<'; a delimited payload "
                     "from generate_payload_measure never does. Use embed_as 'text' for any other text.")
    return notes


def _preview(css_text: str, runtime: str, payloads: List[Dict[str, Any]], root_id: str, empty_html: str,
             samples: Dict[str, str]) -> str:
    """The document the measure returns for ``samples`` (global name -> measure text), following
    the DAX step for step."""
    style = f"<style>{css_text}</style>"
    values = [samples.get(p["global"]) or "" for p in payloads]
    if any(p["required"] and not v for p, v in zip(payloads, values)):
        return style + empty_html
    parts = [style, f"<div id='{root_id}'></div>", "<script>"]
    json_steps = _payload.text_escape_steps("json")
    for p, value in zip(payloads, values):
        if p["embed_as"] == "json":
            parts.append(f"window.{p['global']}={value or 'null'};")
        else:
            if p["embed_as"] == "text":
                value = _payload.apply_escape(value, json_steps)
            parts.append(f'window.{p["global"]}="{value}";')
    parts += [runtime, "</script>"]
    return "".join(parts)
