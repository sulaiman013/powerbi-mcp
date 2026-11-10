"""
DAX as a data transport: text measures that pack model rows for a JavaScript consumer.

A visual that is given one text measure (an HTML visual running an inline script, or a custom
visual with a single measure role) can receive a whole table of rows that way: the measure
concatenates one record per row, and the script rebuilds columns from the string. The model
stays the source of truth, so slicers, row-level security and refresh keep working, while the
script gets row-level data it can filter and aggregate without another query.

Two wire formats:

  delimited  records joined by ``record_sep``, fields by ``field_sep``. Numbers are digits
             only; text is percent-encoded, so one parser serves both consumers (the string
             read raw by a custom visual, and the same string placed between double quotes
             in a script).
  json       an array of row arrays. Valid JSON and a valid JavaScript expression.

What keeps the string well formed whatever the data and the workspace locale:

  * numbers go through FORMAT ( x, "0" ), so no locale can insert a decimal comma or a group
    separator. Decimals travel as integers scaled by 10^scale and are divided back in the
    parser. Dates travel as yyyymmdd integers built by arithmetic, not by a format string.
  * text goes through a SUBSTITUTE chain whose order matters (the escape character first, or
    every later escape would be escaped again). The chain covers the separators, both quote
    styles the string may sit in, the backslash, line breaks, the two Unicode line separators
    that end a JavaScript line, and "<" so the data can never close a script element.

Nothing here evaluates DAX. ``apply_escape`` is a Python model of the chain so the escaping can
be tested without an engine; the parsers are plain JavaScript and can be checked with Node.

Entry points:
    generate_payload_measure(name, table, fields, ...)   rows at a chosen grain
    generate_dimension_payload(name, dimensions, ...)    key-to-label lookup tables
    estimate_budget(chars_per_row, rows, ceiling)        size against a measure ceiling
"""
import json
import re
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

import dax_tokens

# One measure returned about this many characters before failing in the project this
# technique comes from. It is an observation, not a documented Power BI limit.
DEFAULT_CEILING = 2_100_000

FIELD_TYPES = ("int", "decimal", "date", "bool", "text")
FORMATS = ("delimited", "json")

# A separator must be one printable ASCII character that can never appear in a number, in the
# percent escapes, or as a quote: so digits, letters, "%", "-", ".", quotes, "\" and "<" are out.
ALLOWED_SEPARATORS = frozenset(";,|^#~:!*+@$?&/")

_TYPED_ARRAYS = {
    "float64": "Float64Array", "float32": "Float32Array", "int32": "Int32Array",
    "int16": "Int16Array", "int8": "Int8Array", "uint32": "Uint32Array",
    "uint16": "Uint16Array", "uint8": "Uint8Array",
}
_DEFAULT_WIDTH = {"int": 4.0, "decimal": 6.0, "date": 8.0, "bool": 1.0, "text": 12.0}
_BS = chr(92)
_LINE_SEP, _PARA_SEP = chr(0x2028), chr(0x2029)
_JS_WORDS = frozenset(
    "break case catch class const continue debugger default delete do else enum export extends "
    "false finally for function if import in instanceof new null return super switch this throw "
    "true try typeof var void while with yield let static await".split())


class PayloadError(ValueError):
    """The request cannot be turned into a safe payload measure."""


# ---------------------------------------------------------------- escaping

def text_escape_steps(fmt: str, separators: Sequence[str] = ()) -> List[Tuple[str, str]]:
    """Ordered (find, replacement) pairs applied to every text value.

    delimited: percent-encoding. "%" goes first, so the "%" written by later steps is never
    encoded again; decodeURIComponent undoes the whole chain.
    json: JSON string escapes. The backslash goes first for the same reason. "<" becomes a
    six-character Unicode escape (valid in JSON and in JavaScript), which keeps "</script"
    and "<!--" out of the document.
    """
    if fmt == "delimited":
        finds = ["%", _BS, '"', "'", "<", "\r", "\n", _LINE_SEP, _PARA_SEP]
        finds += [s for s in separators if s not in finds]
        return [(ch, "".join("%%%02X" % b for b in ch.encode("utf-8"))) for ch in finds]
    if fmt == "json":
        return [
            (_BS, _BS + _BS), ('"', _BS + '"'), ("<", _BS + "u003C"),
            ("\r", _BS + "r"), ("\n", _BS + "n"), ("\t", _BS + "t"),
            (_LINE_SEP, _BS + "u2028"), (_PARA_SEP, _BS + "u2029"),
        ]
    raise PayloadError(f"Unknown format '{fmt}'. Use one of: {', '.join(FORMATS)}")


def apply_escape(value: str, steps: Sequence[Tuple[str, str]]) -> str:
    """What the generated SUBSTITUTE chain returns for ``value``.

    SUBSTITUTE replaces every occurrence, case-sensitively, one step after another, which is
    exactly str.replace in sequence. Used by the tests and for previews; it is not a DAX engine.
    """
    for find, replacement in steps:
        value = value.replace(find, replacement)
    return value


def _dax_text(s: str) -> str:
    """A DAX expression that evaluates to the string ``s``.

    The backslash and every character outside printable ASCII are written as UNICHAR ( n ), so
    the measure text holds no invisible character and no backslash for a quoting layer between
    here and the model to consume.
    """
    parts: List[str] = []
    run: List[str] = []

    def flush() -> None:
        if run:
            parts.append('"' + "".join(run).replace('"', '""') + '"')
            run.clear()

    for ch in s:
        if 32 <= ord(ch) < 127 and ch != _BS:
            run.append(ch)
        else:
            flush()
            parts.append(f"UNICHAR ( {ord(ch)} )")
    flush()
    return " & ".join(parts) if parts else '""'


def dax_escape(expr: str, steps: Sequence[Tuple[str, str]]) -> str:
    """``expr`` wrapped in one SUBSTITUTE per step, innermost first (the order of ``steps``)."""
    for find, replacement in steps:
        expr = f"SUBSTITUTE ( {expr}, {_dax_text(find)}, {_dax_text(replacement)} )"
    return expr


# ---------------------------------------------------------------- input checks

def _check_separators(*seps: str) -> None:
    for s in seps:
        if not isinstance(s, str) or len(s) != 1 or s not in ALLOWED_SEPARATORS:
            raise PayloadError(
                f"Separator {s!r} is not allowed. Use one character from: "
                + " ".join(sorted(ALLOWED_SEPARATORS)))
    if len(set(seps)) != len(seps):
        raise PayloadError("Each separator must be a different character.")


def _check_fragment(dax: str, what: str) -> str:
    """Reject a caller-supplied DAX fragment that would unbalance the measure around it."""
    text = (dax or "").strip()
    if not text:
        raise PayloadError(f"{what} is empty.")
    depth = 0
    closers = {dax_tokens.STRING: '"', dax_tokens.QNAME: "'", dax_tokens.BRACKET: "]"}
    for tok in dax_tokens.tokenize(text):
        if tok.kind in closers and (len(tok.text) < 2 or not tok.text.endswith(closers[tok.kind])):
            raise PayloadError(f"{what} has an unterminated {tok.kind}: {tok.text[:40]!r}")
        if tok.kind == dax_tokens.COMMENT:
            raise PayloadError(f"{what} must not contain a comment (it would swallow the rest of a line).")
        if tok.kind == dax_tokens.OP and tok.text == "(":
            depth += 1
        elif tok.kind == dax_tokens.OP and tok.text == ")":
            depth -= 1
            if depth < 0:
                raise PayloadError(f"{what} closes a parenthesis it did not open.")
    if depth:
        raise PayloadError(f"{what} has {depth} unclosed parenthesis(es).")
    return text


def _split_column(ref: str, default_table: str) -> Tuple[str, str]:
    """'Col', '[Col]', "Table[Col]" or "'My Table'[Col]" -> (table, column)."""
    text = str(ref).strip()
    if "[" not in text:
        return default_table, text
    sig = [t for t in dax_tokens.tokenize(text) if t.kind != dax_tokens.WS]
    # The column is the bracketed name that ends the text; what precedes it is the table,
    # quoted or not (callers often write Dim Site[Name] without the quotes DAX requires).
    if sig and sig[-1].kind == dax_tokens.BRACKET and sig[-1].end == len(text) and text.endswith("]"):
        table = dax_tokens.unquote_table(text[:sig[-1].start].strip())
        if not table:
            return default_table, sig[-1].name
        if "[" not in table:
            return table, sig[-1].name
    raise PayloadError(f"Cannot read the column reference {ref!r}. Use 'Column' or 'Table[Column]'.")


def _js_identifier(text: str, fallback: str) -> str:
    words = re.findall(r"[A-Za-z0-9]+", text or "")
    ident = "".join(w[:1].upper() + w[1:] for w in words) or fallback
    return ident if not ident[0].isdigit() else "_" + ident


def is_js_identifier(name: str) -> bool:
    return bool(re.fullmatch(r"[A-Za-z_$][A-Za-z0-9_$]*", name or "")) and name not in _JS_WORDS


# ---------------------------------------------------------------- fields

class _Field:
    """One value carried per record, resolved against the table being iterated."""

    def __init__(self, spec: Any, table: str, index: int):
        if isinstance(spec, str):
            head, _, kind = spec.rpartition(":")
            spec = {"column": head, "type": kind} if head else {"column": spec}
        elif isinstance(spec, (tuple, list)) and len(spec) == 2:
            spec = {"column": spec[0], "type": spec[1]}
        if not isinstance(spec, dict):
            raise PayloadError(f"Field #{index + 1} must be a dict, 'Column:type' or (column, type).")
        self.type = str(spec.get("type") or "").lower()
        if self.type not in FIELD_TYPES:
            raise PayloadError(
                f"Field #{index + 1} needs a type: one of {', '.join(FIELD_TYPES)}. A transport "
                "contract cannot guess whether a column is a number or text.")
        sources = [k for k in ("column", "measure", "expression") if spec.get(k)]
        if len(sources) != 1:
            raise PayloadError(f"Field #{index + 1} needs exactly one of: column, measure, expression.")
        self.kind = sources[0]
        self.index = index
        self.table = table
        self.column: Optional[str] = None
        self.raw: Optional[str] = None
        if self.kind == "column":
            self.table, self.column = _split_column(spec["column"], spec.get("table") or table)
            default_name = self.column
        elif self.kind == "measure":
            self.raw = dax_tokens.bracket(dax_tokens.unbracket(str(spec["measure"]).strip()))
            default_name = dax_tokens.unbracket(self.raw)
        else:
            self.raw = _check_fragment(spec["expression"], f"Field #{index + 1} expression")
            default_name = f"f{index}"
        self.name = str(spec.get("name") or default_name)
        self.aggregate = self.kind == "measure" or bool(spec.get("aggregate"))
        self.scale = int(spec.get("scale", 2)) if self.type == "decimal" else 0
        if not 0 <= self.scale <= 9:
            raise PayloadError(f"Field '{self.name}': scale must be between 0 and 9.")
        self.blank = str(spec.get("blank") or "empty").lower()
        if self.blank not in ("empty", "zero"):
            raise PayloadError(f"Field '{self.name}': blank must be 'empty' or 'zero'.")
        self.array = str(spec.get("array") or "float64").lower()
        if self.array not in _TYPED_ARRAYS:
            raise PayloadError(f"Field '{self.name}': array must be one of {', '.join(_TYPED_ARRAYS)}.")
        self.width = float(spec.get("avg_chars") or _DEFAULT_WIDTH[self.type])
        self.ext = f"@f{index}"          # extension column name when the value is computed per group

    def source(self, base_table: str, grouped: bool) -> str:
        """DAX giving this field's value in the row context of the iterated table."""
        if self.kind == "column":
            ref = dax_tokens.column_ref(self.table, self.column)
            if grouped or dax_tokens.names_equal(self.table, base_table):
                return ref
            return f"RELATED ( {ref} )"     # a row of the base table reaches its dimension row
        return dax_tokens.bracket(self.ext) if grouped else self.raw

    def value(self, src: str, fmt: str, steps: Sequence[Tuple[str, str]]) -> str:
        """DAX giving this field's text on the wire."""
        as_json = fmt == "json"
        empty = '"null"' if as_json else '""'
        if self.type == "text":
            body = dax_escape(src, steps)
            return f'IF ( ISBLANK ( {src} ), "null", """" & {body} & """" )' if as_json else body
        if self.type == "bool":
            yes, no = ('"true"', '"false"') if as_json else ('"1"', '"0"')
            return f"IF ( ISBLANK ( {src} ), {empty}, IF ( {src}, {yes}, {no} ) )"
        zero = self.blank == "zero"
        num = f"( {src} + 0 )" if zero else src
        if self.type == "date":
            digits = f'FORMAT ( YEAR ( {src} ) * 10000 + MONTH ( {src} ) * 100 + DAY ( {src} ), "0" )'
            return f"IF ( ISBLANK ( {src} ), {empty}, {digits} )"
        if self.type == "decimal" and self.scale:
            digits = f'FORMAT ( ROUND ( {num} * {10 ** self.scale}, 0 ), "0" )'
        else:
            # No ROUND here: FORMAT already writes whole digits, and ROUND would pass a large
            # integer key through a floating-point number.
            digits = f'FORMAT ( {num}, "0" )'
        return digits if zero else f"IF ( ISBLANK ( {src} ), {empty}, {digits} )"

    def spec(self) -> List[Any]:
        """The parser's description of this field: [name, type, divisor, typed array or null]."""
        numeric = self.type in ("int", "decimal", "date")
        return [self.name, self.type, 10 ** self.scale if self.type == "decimal" else 1,
                _TYPED_ARRAYS[self.array] if numeric else None]

    def chars(self, fmt: str) -> float:
        return self.width + (2 if fmt == "json" and self.type == "text" else 0)


def _fields(specs: Sequence[Any], table: str) -> List[_Field]:
    if not specs:
        raise PayloadError("At least one field is required.")
    out = [_Field(s, table, i) for i, s in enumerate(specs)]
    seen = set()
    for f in out:
        if f.name in seen:
            raise PayloadError(f"Two fields are both named '{f.name}'. Give one a different name.")
        seen.add(f.name)
    return out


def _order_args(order_by: Optional[Sequence[Any]], fields: List[_Field], table: str,
                grouped: bool) -> List[str]:
    """CONCATENATEX order arguments, one "expression, direction" string per sort key."""
    if not order_by:
        return []
    by_name = {f.name: f for f in fields}
    out = []
    for item in ([order_by] if isinstance(order_by, str) else order_by):
        name, direction = (item, "ASC") if isinstance(item, str) else (item[0], str(item[1]).upper())
        if name not in by_name:
            raise PayloadError(f"order_by names '{name}', which is not one of the fields.")
        if direction not in ("ASC", "DESC"):
            raise PayloadError(f"order_by direction for '{name}' must be ASC or DESC.")
        out.append(f"{by_name[name].source(table, grouped)}, {direction}")
    return out


def _concatenatex(source: str, record: str, separator: str, order: Sequence[str]) -> str:
    args = [_indent(source, 4), "    " + record, f'    "{separator}"'] + ["    " + o for o in order]
    return "CONCATENATEX (\n" + ",\n".join(args) + "\n)"


def _indent(text: str, spaces: int) -> str:
    pad = " " * spaces
    return "\n".join(pad + line if line else line for line in text.split("\n"))


def _table_expression(table: str, fields: List[_Field], grouped: bool, where: Optional[str]) -> str:
    """The table CONCATENATEX iterates: the base rows, or one row per distinct group."""
    base = dax_tokens.quote_table(table, force=True)
    row_filter = f"FILTER ( {base}, {where} )" if where else None
    if not grouped:
        return row_filter or base
    groups = [f.source(table, True) for f in fields if not f.aggregate]
    # With no column field there is one group: the whole (filtered) table.
    summary = f"SUMMARIZE ( {', '.join([base] + groups)} )" if groups else 'ROW ( "@all", 1 )'
    computed = [f for f in fields if f.aggregate]
    if computed:
        # CALCULATE forces the context transition for a raw expression; a measure reference
        # already carries one, so it is left as written.
        cols = ",\n".join(
            f'    "{f.ext}", ' + (f.raw if f.kind == "measure" else f"CALCULATE ( {f.raw} )")
            for f in computed)
        summary = f"ADDCOLUMNS (\n    {summary},\n{cols}\n)"
    if row_filter:
        # As a filter argument the condition reaches both the grouping and the measures.
        summary = f"CALCULATETABLE (\n{_indent(summary, 4)},\n    {row_filter}\n)"
    return summary


# ---------------------------------------------------------------- parsers

# build(recs, spec) turns an array of records into one column per field. The four upper-case
# words are the only parts that differ between the two wire formats.
_BUILD_JS = """\
  function build(recs, spec) {
    var n = recs.length, cols = {}, c = [], i, j, s, p, v;
    for (j = 0; j < spec.length; j++) {
      s = spec[j];
      c[j] = s[1] === "text" || s[1] === "bool" ? new Array(n) : new ARRAYS[s[3]](n);
      cols[s[0]] = c[j];
    }
    for (i = 0; i < n; i++) {
      p = FIELDS_OF_RECORD;
      for (j = 0; j < spec.length; j++) {
        s = spec[j];
        v = p[j];
        if (s[1] === "text") { c[j][i] = TEXT_VALUE; }
        else if (s[1] === "bool") { c[j][i] = IS_MISSING ? null : BOOL_VALUE; }
        else { c[j][i] = IS_MISSING ? NaN : +v / s[2]; }
      }
    }
    return {
      length: n,
      columns: cols,
      row: function (k) {
        var o = {}, q;
        for (q = 0; q < spec.length; q++) { o[spec[q][0]] = c[q][k]; }
        return o;
      }
    };
  }"""

# rows: one object per record; byKey: the same objects under the value of the first field.
_LOOKUP_JS = """\
  function lookup(recs, spec) {
    var t = build(recs, spec), i;
    t.rows = [];
    t.byKey = {};
    for (i = 0; i < t.length; i++) {
      t.rows.push(t.row(i));
      t.byKey[t.rows[i][spec[0][0]]] = t.rows[i];
    }
    return t;
  }"""

_ARRAYS_JS = ("  var ARRAYS = { " + ", ".join(f"{v}: {v}" for v in sorted(set(_TYPED_ARRAYS.values())))
              + " };")


def _build_js(fmt: str, field_sep: str) -> str:
    if fmt == "delimited":
        subs = {"FIELDS_OF_RECORD": f"recs[i].split({json.dumps(field_sep)})",
                "TEXT_VALUE": 'v ? decodeURIComponent(v) : ""',
                "IS_MISSING": '(v === undefined || v === "")',
                "BOOL_VALUE": 'v === "1"'}
    else:
        subs = {"FIELDS_OF_RECORD": "recs[i] || []",
                "TEXT_VALUE": 'v == null ? "" : String(v)',
                "IS_MISSING": "v == null",
                "BOOL_VALUE": "!!v"}
    body = _BUILD_JS
    for key, text in subs.items():
        body = body.replace(key, text)
    return body


def _rows_parser(function_name: str, fields: List[_Field], fmt: str, record_sep: str,
                 field_sep: str) -> str:
    spec = json.dumps([f.spec() for f in fields], ensure_ascii=True)
    if fmt == "delimited":
        arg = "text"
        read = f"text ? String(text).split({json.dumps(record_sep)}) : []"
    else:
        arg = "input"
        read = 'typeof input === "string" ? (input ? JSON.parse(input) : []) : (input || [])'
    return "\n".join([
        f"function {function_name}({arg}) {{",
        f"  var spec = {spec};",
        _ARRAYS_JS,
        _build_js(fmt, field_sep),
        f"  return build({read}, spec);",
        "}",
    ])


def _dims_parser(function_name: str, dims: List[Tuple[str, List[_Field]]], fmt: str,
                 table_sep: str, record_sep: str, field_sep: str) -> str:
    specs = json.dumps({name: [f.spec() for f in fs] for name, fs in dims}, ensure_ascii=True)
    own = "Object.prototype.hasOwnProperty.call(specs, name)"
    if fmt == "delimited":
        arg = "text"
        tables, records = json.dumps(table_sep), json.dumps(record_sep)
        read = [
            f"  var parts = text ? String(text).split({tables}) : [], out = {{}}, i, eq, name, body;",
            "  for (i = 0; i < parts.length; i++) {",
            '    eq = parts[i].indexOf("=");',
            '    name = eq < 0 ? "" : parts[i].slice(0, eq);',
            '    body = eq < 0 ? "" : parts[i].slice(eq + 1);',
            f"    if ({own}) {{ out[name] = lookup(body ? body.split({records}) : [], specs[name]); }}",
            "  }",
        ]
    else:
        arg = "input"
        read = [
            '  var src = typeof input === "string" ? (input ? JSON.parse(input) : {}) : (input || {});',
            "  var out = {}, name;",
            "  for (name in src) {",
            f"    if ({own}) {{ out[name] = lookup(src[name] || [], specs[name]); }}",
            "  }",
        ]
    return "\n".join([
        f"function {function_name}({arg}) {{",
        f"  var specs = {specs};",
        _ARRAYS_JS,
        _build_js(fmt, field_sep),
        _LOOKUP_JS,
        *read,
        "  for (name in specs) {",
        f"    if ({own} && !out[name]) {{ out[name] = lookup([], specs[name]); }}",
        "  }",
        "  return out;",
        "}",
    ])


# ---------------------------------------------------------------- budget and notes

def estimate_budget(chars_per_row: float, rows: Optional[int] = None,
                    ceiling: int = DEFAULT_CEILING) -> Dict[str, Any]:
    """Size of a payload against a measure ceiling. ``rows`` may be None (capacity only)."""
    per_row = max(float(chars_per_row), 1.0)
    out: Dict[str, Any] = {"ceiling": int(ceiling), "estimated_chars_per_row": round(per_row, 1),
                           "max_rows_at_ceiling": int(ceiling // per_row)}
    if rows is not None:
        total = int(round(per_row * rows))
        out.update({"expected_rows": int(rows), "estimated_chars": total,
                    "share_of_ceiling": round(total / ceiling, 4) if ceiling else None,
                    "fits": total <= ceiling})
    return out


def _row_chars(fields: List[_Field], fmt: str, record_sep: str, field_sep: str) -> float:
    widths = sum(f.chars(fmt) for f in fields)
    if fmt == "json":
        return widths + (len(fields) - 1) + 3          # commas, two brackets, the row comma
    return widths + (len(fields) - 1) * len(field_sep) + len(record_sep)


def _relationship_note(fields: List[_Field], table: str) -> List[str]:
    others = sorted({f.table for f in fields
                     if f.kind == "column" and not dax_tokens.names_equal(f.table, table)})
    if not others:
        return []
    return [f"Columns of {', '.join(others)} are reached through the model's relationships from {table} "
            "(RELATED per row, or as group columns). Each needs a many-to-one relationship from "
            f"{table}; without one the measure fails."]


def _notes(fmt: str, fields: List[_Field], ceiling: int, ordered: bool) -> List[str]:
    notes = [
        "Numbers are written with FORMAT ( x, \"0\" ), decimals as integers scaled by 10^scale and "
        "dates as yyyymmdd integers, so the string is the same in every workspace locale.",
        f"The {ceiling:,}-character ceiling is an empirical figure from one project (the report "
        "gallery this technique comes from), not a documented Power BI limit. Measure a real "
        "payload in your own tenant before relying on it; estimated_chars_per_row is a guess "
        "built from average field widths.",
        "Not verified here: the DAX has not been evaluated against a model. Check it in DAX query "
        "view, and compare the record count with COUNTROWS of the same table.",
    ]
    if any(f.type == "text" for f in fields):
        if fmt == "delimited":
            notes.append("Text is percent-encoded (the parser uses decodeURIComponent), so the string "
                         "needs no further escaping between double quotes in a script. Each text field "
                         "costs a chain of SUBSTITUTE calls per row: send keys in large payloads and "
                         "put the labels in a dimension payload.")
        else:
            notes.append("Text is JSON-escaped, with \"<\" written as a Unicode escape so the data cannot "
                         "close a script element. Control characters other than tab, CR and LF are not "
                         "escaped: JSON.parse rejects them, a script that takes the array as a literal "
                         "does not.")
        notes.append("A text field must be a text column. A number or date passed as text is converted "
                     "with the workspace locale; declare it as int, decimal or date instead.")
    if fmt == "json":
        notes.append("The measure returns BLANK when there are no rows, otherwise a JSON array of row "
                     "arrays in field order; a blank value is null.")
    else:
        notes.append("The measure returns BLANK when there are no rows; a blank value is an empty field "
                     "(NaN in a numeric column, null in a bool column, \"\" in a text column).")
    if not ordered:
        notes.append("No order_by was given, so record order is whatever the engine returns and may "
                     "change between evaluations.")
    return notes


# ---------------------------------------------------------------- generators

def generate_payload_measure(
        name: str, table: str, fields: Sequence[Any], *, format: str = "delimited",
        record_sep: str = ";", field_sep: str = ",",
        order_by: Optional[Sequence[Union[str, Tuple[str, str]]]] = None, where: Optional[str] = None,
        display_folder: Optional[str] = None, grain: Optional[str] = None,
        expected_rows: Optional[int] = None, ceiling: int = DEFAULT_CEILING,
        parser_name: Optional[str] = None) -> Dict[str, Any]:
    """A text measure that packs rows of ``table`` for a JavaScript consumer.

    fields: each a dict (or 'Column:type', or (column, type)) with a ``type`` of int, decimal,
        date, bool or text and exactly one source:
          column      a column of ``table``, or of a related table ("Table[Column]" or ``table`` key)
          measure     a measure, evaluated once per group
          expression  raw DAX; with ``aggregate: true`` it is evaluated once per group
        Optional keys: name (the JavaScript property), scale (decimal places, default 2),
        blank ('empty' or 'zero'), array (typed array for a numeric column), avg_chars.
    grain: 'rows' sends one record per row of ``table``; 'groups' sends one record per distinct
        combination of the column fields, with the measure and aggregate fields computed for
        it. Default: 'groups' when any field is a measure or an aggregate, otherwise 'rows'.
    where: a row-level DAX condition on ``table`` (row context), for example Sales[Qty] > 0.
    order_by: field names, or (name, 'ASC' | 'DESC') pairs, for a stable record order.

    Returns {name, dax, expression, description, display_folder, format, embed_as, js_parser,
    parser_name, fields, estimated_chars_per_row, budget, notes}. ``dax`` and ``expression`` hold
    the same text, so the dict can go straight to a bulk measure writer.
    """
    fmt = str(format or "").lower()
    if fmt not in FORMATS:
        raise PayloadError(f"Unknown format '{format}'. Use one of: {', '.join(FORMATS)}")
    if not str(name or "").strip() or not str(table or "").strip():
        raise PayloadError("A measure name and a table are both required.")
    if fmt == "delimited":
        _check_separators(record_sep, field_sep)
    parsed = _fields(fields, table)
    condition = _check_fragment(where, "where") if where else None
    if grain not in (None, "rows", "groups"):
        raise PayloadError("grain must be 'rows', 'groups' or None.")
    grouped = grain == "groups" or (grain is None and any(f.aggregate for f in parsed))
    if not grouped and any(f.kind == "measure" for f in parsed):
        raise PayloadError("A measure field needs grain='groups': a measure is computed per group, "
                           "not per row.")

    steps = text_escape_steps(fmt, (field_sep, record_sep) if fmt == "delimited" else ())
    values = [f.value(f.source(table, grouped), fmt, steps) for f in parsed]
    source = _table_expression(table, parsed, grouped, condition)
    order = _order_args(order_by, parsed, table, grouped)
    if fmt == "delimited":
        record = f'\n        & "{field_sep}" & '.join(values)
        dax = _concatenatex(source, record, record_sep, order)
    else:
        record = '"[" & ' + '\n        & "," & '.join(values) + ' & "]"'
        dax = ("VAR _rows =\n" + _indent(_concatenatex(source, record, ",", order), 4) + "\n"
               "RETURN\n"
               '    IF ( ISBLANK ( _rows ) || _rows = "", BLANK (), "[" & _rows & "]" )')

    per_row = _row_chars(parsed, fmt, record_sep, field_sep)
    function_name = parser_name or "parse" + _js_identifier(name, "Payload")
    if not is_js_identifier(function_name):
        raise PayloadError(f"parser_name {function_name!r} is not a JavaScript identifier.")
    grain_text = "one record per group" if grouped else "one record per row"
    description = (f"Data payload for a script: {grain_text} of {table}, {len(parsed)} fields "
                   f"({', '.join(f.name for f in parsed)}), {fmt} format. Read it with {function_name}().")
    return {
        "name": name, "dax": dax, "expression": dax, "description": description,
        "display_folder": display_folder, "format": fmt,
        "embed_as": "string" if fmt == "delimited" else "json",
        "js_parser": _rows_parser(function_name, parsed, fmt, record_sep, field_sep),
        "parser_name": function_name,
        "fields": [{"name": f.name, "type": f.type, "scale": f.scale} for f in parsed],
        "grain": "groups" if grouped else "rows",
        "estimated_chars_per_row": round(per_row, 1),
        "budget": estimate_budget(per_row, expected_rows, ceiling),
        "notes": _notes(fmt, parsed, ceiling, bool(order)) + _relationship_note(parsed, table),
    }


def _dimension(spec: Dict[str, Any], index: int) -> Tuple[str, str, List[_Field], Optional[str], bool]:
    if not isinstance(spec, dict) or not spec.get("table"):
        raise PayloadError(f"Dimension #{index + 1} must be a dict with at least a table.")
    table = str(spec["table"])
    name = str(spec.get("name") or table)
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise PayloadError(f"Dimension name {name!r} must be letters, digits and underscores: it is "
                           "the lookup key in the script.")
    specs = list(spec.get("fields") or [])
    if not specs:
        if not spec.get("key"):
            raise PayloadError(f"Dimension '{name}' needs a key column (or a fields list).")
        specs = [{"column": spec["key"], "type": spec.get("key_type") or "int", "name": "key"}]
        specs += [{"column": c, "type": "text"} for c in (spec.get("labels") or [])]
    where = _check_fragment(spec["where"], f"Dimension '{name}' where") if spec.get("where") else None
    return name, table, _fields(specs, table), where, bool(spec.get("all_rows"))


def generate_dimension_payload(
        name: str, dimensions: Sequence[Dict[str, Any]], *, format: str = "delimited",
        table_sep: str = "#", record_sep: str = ";", field_sep: str = "^",
        display_folder: Optional[str] = None, expected_rows: Optional[Dict[str, int]] = None,
        ceiling: int = DEFAULT_CEILING, parser_name: Optional[str] = None) -> Dict[str, Any]:
    """One text measure holding several key-to-label lookup tables.

    dimensions: each {"table", "name"?, "key", "key_type"? (default int), "labels"? [columns]},
        or {"table", "name"?, "fields": [field specs]} where the first field is the key.
        Optional: "where" (row condition) and "all_rows" (ignore the filter context, so the
        script can list members the current selection has filtered out).

    delimited shape:  site=1^North^EU;2^South^EU#shift=1^Day;2^Night
    json shape:       {"site":[[1,"North","EU"],[2,"South","EU"]],"shift":[[1,"Day"],[2,"Night"]]}
    The parser returns, per dimension, {length, columns, row(i), rows, byKey}.
    expected_rows: {dimension name: row count}, to size the whole measure against ``ceiling``.

    Returns the same keys as generate_payload_measure; estimated_chars_per_row is a dict of
    dimension name to characters per row.
    """
    fmt = str(format or "").lower()
    if fmt not in FORMATS:
        raise PayloadError(f"Unknown format '{format}'. Use one of: {', '.join(FORMATS)}")
    if not str(name or "").strip():
        raise PayloadError("A measure name is required.")
    if not dimensions:
        raise PayloadError("At least one dimension is required.")
    if fmt == "delimited":
        _check_separators(table_sep, record_sep, field_sep)
    dims = [_dimension(d, i) for i, d in enumerate(dimensions)]
    if len({d[0] for d in dims}) != len(dims):
        raise PayloadError("Two dimensions share a name. Give each a different name.")
    steps = text_escape_steps(fmt, (field_sep, record_sep, table_sep) if fmt == "delimited" else ())

    parts, per_row, all_fields = [], {}, []
    for dim_name, table, fields, where, all_rows in dims:
        base = dax_tokens.quote_table(table, force=True)
        source = f"ALL ( {base} )" if all_rows else base
        if where:
            source = f"FILTER ( {source}, {where} )"
        values = [f.value(f.source(table, False), fmt, steps) for f in fields]
        key = fields[0].source(table, False)
        if fmt == "delimited":
            record = f' & "{field_sep}" & '.join(values)
            parts.append(f'"{dim_name}=" & CONCATENATEX ( {source}, {record}, "{record_sep}", {key}, ASC )')
        else:
            record = '"[" & ' + ' & "," & '.join(values) + ' & "]"'
            parts.append(f'"""{dim_name}"":[" & CONCATENATEX ( {source}, {record}, ",", {key}, ASC ) & "]"')
        per_row[dim_name] = round(_row_chars(fields, fmt, record_sep, field_sep), 1)
        all_fields += fields
    if fmt == "delimited":
        dax = f'\n    & "{table_sep}" & '.join(parts)
    else:
        dax = '"{" & ' + '\n    & "," & '.join(parts) + ' & "}"'

    function_name = parser_name or "parse" + _js_identifier(name, "Dimensions")
    if not is_js_identifier(function_name):
        raise PayloadError(f"parser_name {function_name!r} is not a JavaScript identifier.")
    budget: Dict[str, Any] = {"ceiling": int(ceiling)}
    if expected_rows:
        unknown = sorted(set(expected_rows) - set(per_row))
        if unknown:
            raise PayloadError(f"expected_rows names unknown dimension(s): {', '.join(unknown)}")
        total = int(round(sum(per_row[n] * rows + len(n) + 2 for n, rows in expected_rows.items())))
        budget.update({"expected_rows": dict(expected_rows), "estimated_chars": total,
                       "share_of_ceiling": round(total / ceiling, 4) if ceiling else None,
                       "fits": total <= ceiling})
    notes = _notes(fmt, all_fields, ceiling, True)
    notes.append("Every dimension is read in the current filter context unless all_rows is set, so the "
                 "labels always cover the keys of the rows a fact payload sends from the same visual.")
    return {
        "name": name, "dax": dax, "expression": dax,
        "description": (f"Lookup tables for a script ({', '.join(d[0] for d in dims)}), {fmt} format. "
                        f"Read it with {function_name}()."),
        "display_folder": display_folder, "format": fmt,
        "embed_as": "string" if fmt == "delimited" else "json",
        "js_parser": _dims_parser(function_name, [(d[0], d[2]) for d in dims], fmt,
                                  table_sep, record_sep, field_sep),
        "parser_name": function_name,
        "dimensions": [{"name": d[0], "table": d[1],
                        "fields": [{"name": f.name, "type": f.type, "scale": f.scale} for f in d[2]]}
                       for d in dims],
        "estimated_chars_per_row": per_row,
        "budget": budget,
        "notes": notes,
    }
