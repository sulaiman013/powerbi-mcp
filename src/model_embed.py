"""
Make an import model self-contained: carry table data inside the partition itself.

A model that reads a local file or a local database cannot refresh once it is published:
the service has no path to that source without a gateway. For small reference and demo
tables the fix is to put the data in the Power Query expression, the same way Power BI's
own "Enter data" does it: the CSV is compressed with raw deflate, written as base64 text,
and read back with

    Csv.Document(Binary.Decompress(Binary.FromText("<base64>", BinaryEncoding.Base64),
                                   Compression.Deflate), ...)

The published model then refreshes with no data source, no credentials and no gateway.

    embed_csv_partition(csv_text, ...)    the M expression for one table
    embed_tables(definition_dir, {...})   rewrite partitions in a PBIP model's TMDL files

embed_tables changes only the lines under each partition's ``source =``. Everything else in
a file comes back byte for byte (encoding, BOM and line endings included; a file that mixes
line endings comes back with one style), and every file is written through one transaction:
either all of them change or none does. A parameter holding a file path or folder that
only the replaced queries used is removed with them: it would otherwise publish a path from
the author's machine that no query reads any more. Parameters of any other kind, shared
queries and functions are never removed.

Nothing here runs Power Query. What is checked in Python: the embedded text inflates back
to the CSV that went in (decode_embedded_csv), and the edited files keep their structure.
"""
import base64
import csv
import io
import json
import os
import re
import zlib
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple, Union

import safe_io

PathLike = Union[str, os.PathLike]

# Embedding is for reference and demo tables. A larger file belongs in a real source.
DEFAULT_MAX_BYTES = 8 * 1024 * 1024

_M_TYPES = {
    "text": "type text", "string": "type text",
    "int64": "Int64.Type", "int": "Int64.Type", "integer": "Int64.Type", "whole": "Int64.Type",
    "double": "type number", "number": "type number", "float": "type number",
    "decimal": "Currency.Type", "currency": "Currency.Type", "fixed": "Currency.Type",
    "percentage": "Percentage.Type",
    "boolean": "type logical", "logical": "type logical", "bool": "type logical",
    "date": "type date", "datetime": "type datetime", "datetimezone": "type datetimezone",
    "time": "type time", "duration": "type duration",
}
_RAW_M_TYPE = re.compile(r"type [a-z]+|[A-Za-z][A-Za-z0-9]*\.Type")
_BASE64_CALL = re.compile(r'Binary\.FromText\("([A-Za-z0-9+/=]*)",\s*BinaryEncoding\.Base64\)')
# Functions that reach outside the model. Their presence after embedding means a refresh in
# the service still needs a connection.
_EXTERNAL = re.compile(
    r"\b(File\.Contents|Folder\.Files|Folder\.Contents|Web\.Contents|Web\.Page|Web\.BrowserContents"
    r"|Sql\.Databases?|Odbc\.DataSource|Odbc\.Query|OleDb\.DataSource|OleDb\.Query|OData\.Feed"
    r"|SharePoint\.Files|SharePoint\.Contents|SharePoint\.Tables|Access\.Database"
    r"|MySQL\.Database|PostgreSQL\.Database|Oracle\.Database|Snowflake\.Databases"
    r"|AnalysisServices\.Databases?|AzureStorage\.[A-Za-z]+|Lakehouse\.Contents"
    r"|PowerPlatform\.Dataflows|GoogleBigQuery\.Database|Databricks\.Catalogs)\b")


class EmbedError(ValueError):
    """The data cannot be embedded as asked. Nothing has been written when this is raised."""


# ---------------------------------------------------------------- CSV to M

def _m_type(name: str) -> str:
    text = str(name or "").strip()
    known = _M_TYPES.get(text.lower())
    if known:
        return known
    if _RAW_M_TYPE.fullmatch(text):
        return text
    raise EmbedError(f"Unknown column type {name!r}. Use one of: {', '.join(sorted(set(_M_TYPES)))}, "
                     "or an M type such as 'type text' or 'Int64.Type'.")


def _m_string(text: str) -> str:
    """An M text literal. Control characters use M's #(...) escapes."""
    for char, escape in (('"', '""'), ("\t", "#(tab)"), ("\r", "#(cr)"), ("\n", "#(lf)")):
        text = text.replace(char, escape)
    return '"' + text + '"'


def _read_csv(csv_text: str, delimiter: str) -> Tuple[List[str], List[List[str]]]:
    if not isinstance(delimiter, str) or len(delimiter) != 1 or delimiter in '"\r\n':
        raise EmbedError("delimiter must be a single character other than a quote or a line break.")
    text = (csv_text or "").lstrip(chr(0xFEFF))
    try:
        rows = [r for r in csv.reader(io.StringIO(text, newline=""), delimiter=delimiter) if r]
    except csv.Error as e:
        raise EmbedError(f"The CSV cannot be read: {e}.") from None
    if not rows:
        raise EmbedError("The CSV is empty: there is no header row.")
    header = [h.strip() for h in rows[0]]
    if any(not h for h in header):
        raise EmbedError("The CSV header has an empty column name.")
    seen: Set[str] = set()
    for h in header:
        if h.lower() in seen:
            raise EmbedError(f"The CSV header repeats the column '{h}'.")
        seen.add(h.lower())
    for n, row in enumerate(rows[1:], start=2):
        if len(row) != len(header):
            raise EmbedError(f"CSV line {n} has {len(row)} field(s); the header has {len(header)}.")
    return header, rows[1:]


def _write_csv(header: Sequence[str], rows: Iterable[Sequence[str]], delimiter: str) -> str:
    out = io.StringIO(newline="")
    writer = csv.writer(out, delimiter=delimiter, lineterminator="\n")
    writer.writerow(header)
    writer.writerows(rows)
    return out.getvalue()


_INT = re.compile(r"-?(?:0|[1-9][0-9]{0,17})")
_NUMBER = re.compile(r"-?(?:0|[1-9][0-9]*)\.[0-9]+(?:[eE][-+]?[0-9]+)?|-?(?:0|[1-9][0-9]*)[eE][-+]?[0-9]+")
_DATE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")
_DATETIME = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}[T ][0-9]{2}:[0-9]{2}(?::[0-9]{2}(?:\.[0-9]+)?)?")


def infer_column_types(csv_text: str, delimiter: str = ",") -> Dict[str, str]:
    """A cautious type per column, from the values: int64, double, date, datetime, boolean or
    text. A column is only typed when every non-empty value fits, and a value with a leading
    zero (a postcode, a product code) keeps the column as text."""
    header, rows = _read_csv(csv_text, delimiter)
    return _infer(header, rows)


def _infer(header: Sequence[str], rows: Sequence[Sequence[str]]) -> Dict[str, str]:
    out = {}
    for i, name in enumerate(header):
        values = [r[i] for r in rows if r[i] != ""]
        if not values:
            kind = "text"
        elif all(_INT.fullmatch(v) for v in values):
            kind = "int64"
        elif all(_INT.fullmatch(v) or _NUMBER.fullmatch(v) for v in values):
            kind = "double"
        elif all(_DATE.fullmatch(v) for v in values):
            kind = "date"
        elif all(_DATE.fullmatch(v) or _DATETIME.fullmatch(v) for v in values):
            kind = "datetime"
        elif all(v.lower() in ("true", "false") for v in values):
            kind = "boolean"
        else:
            kind = "text"
        out[name] = kind
    return out


def _deflate64(data: bytes) -> str:
    # wbits -15 gives a raw deflate stream (no zlib header), which is what Compression.Deflate reads.
    packer = zlib.compressobj(9, zlib.DEFLATED, -15)
    return base64.b64encode(packer.compress(data) + packer.flush()).decode("ascii")


def _m_lines(header: Sequence[str], rows: Sequence[Sequence[str]], types: Mapping[str, str],
             delimiter: str) -> Tuple[List[str], Dict[str, Any]]:
    data = _write_csv(header, rows, delimiter).encode("utf-8")
    b64 = _deflate64(data)
    m_types = {h: _m_type(types.get(h, "text")) for h in header}
    pairs = ", ".join("{" + _m_string(h) + ", " + m_types[h] + "}" for h in header)
    typed = [h for h in header if m_types[h] != "type text"]
    lines = [
        "let",
        f'    Source = Csv.Document(Binary.Decompress(Binary.FromText("{b64}", BinaryEncoding.Base64), '
        f"Compression.Deflate), [Delimiter={_m_string(delimiter)}, Columns={len(header)}, Encoding=65001, "
        "QuoteStyle=QuoteStyle.Csv]),",
        '    #"Promoted Headers" = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),',
    ]
    previous = '#"Promoted Headers"'
    if typed:
        # An empty field must become null before the type change, or a number column fails on "".
        names = "{" + ", ".join(_m_string(h) for h in typed) + "}"
        lines.append(f'    #"Blank To Null" = Table.ReplaceValue({previous}, "", null, '
                     f"Replacer.ReplaceValue, {names}),")
        previous = '#"Blank To Null"'
    # A fixed culture: the CSV writes numbers with "." and dates as ISO text, whatever the
    # locale of the machine or the service that refreshes the model.
    lines += [
        f'    #"Changed Type" = Table.TransformColumnTypes({previous}, {{{pairs}}}, "en-US")',
        "in",
        '    #"Changed Type"',
    ]
    stats = {"rows": len(rows), "columns": len(header), "csv_bytes": len(data),
             "compressed_bytes": len(base64.b64decode(b64)), "embedded_chars": len(b64),
             "column_types": {h: m_types[h] for h in header}}
    return lines, stats


def embed_csv_partition(csv_text: str, *, column_types: Optional[Mapping[str, str]] = None,
                        delimiter: str = ",", infer_types: bool = True) -> str:
    """The M expression that returns ``csv_text`` as a typed table, with the data embedded.

    column_types: {column: type}. Types: text, int64, double, decimal (fixed decimal), date,
        datetime, boolean, percentage, time, or an M type written out ('type text',
        'Int64.Type'). A column that is not listed is typed from its values when
        ``infer_types`` is true (see infer_column_types), otherwise it stays text.
    The header row gives the column names. Numbers must use "." as the decimal separator and
    dates the ISO form yyyy-mm-dd: the type step uses a fixed culture so that a refresh gives
    the same result everywhere.
    """
    header, rows = _read_csv(csv_text, delimiter)
    given = dict(column_types or {})
    unknown = [c for c in given if c not in header]
    if unknown:
        raise EmbedError(f"column_types names column(s) the CSV does not have: {', '.join(unknown)}")
    types = _infer(header, rows) if infer_types else {}
    types.update(given)
    lines, _ = _m_lines(header, rows, types, delimiter)
    return "\n".join(lines)


def decode_embedded_csv(m_expression: str) -> str:
    """The CSV text carried by an expression built here: base64 decoded, then inflated."""
    found = _BASE64_CALL.search(m_expression or "")
    if not found:
        raise EmbedError("No Binary.FromText(\"...\", BinaryEncoding.Base64) call in the expression.")
    return zlib.decompress(base64.b64decode(found.group(1)), -15).decode("utf-8")


# ---------------------------------------------------------------- reading TMDL

_TABLE_DECL = re.compile(r"^table\s+(?:'((?:[^']|'')+)'|(\S+))", re.MULTILINE)
_COLUMN_DECL = re.compile(r"^([ \t]+)column\s+(?:'((?:[^']|'')+)'|([^\s=]+))\s*(=.*)?$")
_PARTITION_DECL = re.compile(r"^([ \t]+)partition\s+(?:'((?:[^']|'')+)'|([^\s=]+))\s*=\s*(\w+)\s*$")
_SOURCE_LINE = re.compile(r"^([ \t]+)source\s*=(.*)$")
_EXPRESSION_DECL = re.compile(r"^expression\s+(?:'((?:[^']|'')+)'|([^\s=]+))\s*=")
_TEXT_PARAMETER = re.compile(r'\s*"((?:[^"]|"")*)"\s*(?:meta\b|$)')
_PATH_LIKE = re.compile(r"^[A-Za-z]:|^\\\\|^https?://|[\\/]", re.IGNORECASE)
_QUERY_ORDER = re.compile(r"^(annotation\s+PBI_QueryOrder\s*=\s*)(\[.*\])[ \t]*$", re.MULTILINE)


def _unquote(quoted: Optional[str], bare: Optional[str]) -> str:
    return quoted.replace("''", "'") if quoted is not None else (bare or "")


def _lead(line: str) -> str:
    return line[:len(line) - len(line.lstrip(" \t"))]


def _block_end(lines: List[str], start: int, indent: str) -> int:
    """Index just past the last non-blank line that is indented deeper than ``indent``,
    scanning from ``start``. Blank lines after it are left to whatever follows."""
    end = start
    for i in range(start, len(lines)):
        line = lines[i]
        if not line.strip():
            continue
        lead = _lead(line)
        if len(lead) > len(indent) and lead.startswith(indent):
            end = i + 1
        else:
            break
    return end


def _columns(lines: List[str]) -> List[Dict[str, Any]]:
    """Data columns of a table: name, dataType, sourceColumn, and whether the model computes
    them (a calculated column has an expression and no source column)."""
    out = []
    for i, line in enumerate(lines):
        decl = _COLUMN_DECL.match(line)
        if not decl:
            continue
        col = {"name": _unquote(decl.group(2), decl.group(3)), "data_type": "string",
               "source_column": None, "calculated": bool(decl.group(4)), "date_only": False}
        for prop in lines[i + 1:_block_end(lines, i + 1, decl.group(1))]:
            text = prop.strip()
            if text.startswith("dataType:"):
                col["data_type"] = text.split(":", 1)[1].strip()
            elif text.startswith("sourceColumn:"):
                value = text.split(":", 1)[1].strip()
                if len(value) >= 2 and value[0] == value[-1] == '"':
                    value = value[1:-1].replace('""', '"')
                col["source_column"] = value
            elif text.startswith("type:") and "calculated" in text:
                col["calculated"] = True
            elif text.replace(" ", "") == "annotationUnderlyingDateTimeDataType=Date":
                col["date_only"] = True
        # [Name] is how a calculated table names its columns: there is no source query to feed.
        if col["source_column"] and not col["source_column"].startswith("["):
            out.append(col)
    return out


def _partition_source(lines: List[str], table: str, path: str) -> Tuple[int, int, str, str]:
    """(first line, line after the last, indent of the ``source`` line, old M text) of the one
    import partition's source block."""
    partitions = [(i, _PARTITION_DECL.match(line)) for i, line in enumerate(lines)]
    partitions = [(i, m) for i, m in partitions if m]
    if not partitions:
        raise EmbedError(f"Table '{table}' has no partition in {path}.")
    if len(partitions) > 1:
        raise EmbedError(f"Table '{table}' has {len(partitions)} partitions. Embedding replaces one "
                         "query; merge the partitions first.")
    at, decl = partitions[0]
    kind = decl.group(4)
    if kind != "m":
        raise EmbedError(f"Table '{table}' has a '{kind}' partition. Only Power Query (m) partitions "
                         "hold a query that can be replaced with embedded data.")
    stop = _block_end(lines, at + 1, decl.group(1))
    for i in range(at + 1, stop):
        found = _SOURCE_LINE.match(lines[i])
        if not found:
            continue
        indent, rest = found.group(1), found.group(2).strip()
        if rest.startswith("```") and rest.count("```") == 1:
            # A fenced expression runs to the line that holds only the closing fence.
            end = next((j + 1 for j in range(i + 1, len(lines)) if lines[j].strip() == "```"), None)
            if end is None:
                raise EmbedError(f"Table '{table}': the fenced source expression is never closed.")
        else:
            end = max(_block_end(lines, i + 1, indent), i + 1)
        old = "\n".join([rest] + lines[i + 1:end]).replace("```", "")
        return i, end, indent, old
    raise EmbedError(f"Table '{table}': the partition has no 'source =' expression.")


def _m_names(m_text: str) -> Set[str]:
    """Identifiers used in M code: bare names and #"quoted names". Text inside string
    literals and comments is skipped, so a path that merely mentions a name does not count."""
    names: Set[str] = set()
    s, i, n = m_text or "", 0, len(m_text or "")
    while i < n:
        ch = s[i]
        if s.startswith('#"', i) or ch == '"':
            start = i + (2 if ch == "#" else 1)
            j = start
            while j < n:
                if s[j] == '"':
                    if j + 1 < n and s[j + 1] == '"':
                        j += 2
                        continue
                    break
                j += 1
            if ch == "#":
                names.add(s[start:j].replace('""', '"'))
            i = j + 1
        elif s.startswith("//", i):
            end = s.find("\n", i)
            i = n if end < 0 else end
        elif s.startswith("/*", i):
            end = s.find("*/", i + 2)
            i = n if end < 0 else end + 2
        elif ch.isalpha() or ch == "_":
            j = i + 1
            while j < n and (s[j].isalnum() or s[j] in "_."):
                j += 1
            names.add(s[i:j])
            i = j
        else:
            i += 1
    return names


def _without_strings(m_text: str) -> str:
    """M code with string literals and comments blanked, for looking at the calls it makes."""
    out, s, i, n = [], m_text or "", 0, len(m_text or "")
    while i < n:
        if s[i] == '"':
            j = i + 1
            while j < n and not (s[j] == '"' and s[j + 1:j + 2] != '"'):
                j += 2 if s[j] == '"' else 1
            i = j + 1
            out.append('""')
        elif s.startswith("//", i):
            end = s.find("\n", i)
            i = n if end < 0 else end
        elif s.startswith("/*", i):
            end = s.find("*/", i + 2)
            i = n if end < 0 else end + 2
        else:
            out.append(s[i])
            i += 1
    return "".join(out)


def _expressions(text: str) -> List[Dict[str, Any]]:
    """Shared expressions of expressions.tmdl: name, the line span of the whole block (its
    /// description, its properties and the blank lines after it) and its text."""
    lines = text.split("\n")
    out = []
    for i, line in enumerate(lines):
        decl = _EXPRESSION_DECL.match(line)
        if not decl:
            continue
        start = i
        while start > 0 and lines[start - 1].startswith("///"):
            start -= 1
        end = len(lines)
        for j in range(i + 1, len(lines)):
            if lines[j].strip() and not lines[j][0] in " \t":
                end = j
                break
        body = "\n".join(lines[i:end])
        literal = _TEXT_PARAMETER.match(line[decl.end():])
        out.append({"name": _unquote(decl.group(1), decl.group(2)), "start": start, "end": end, "text": body,
                    "parameter": "isparameterquery=true" in body.replace(" ", "").lower(),
                    # A text literal that reads like a drive, a share, a relative path or a URL.
                    "path": bool(literal and _PATH_LIKE.search(literal.group(1).replace('""', '"')))})
    return out


def _drop_from_query_order(text: str, name: str) -> str:
    """model.tmdl without ``name`` in the PBI_QueryOrder annotation; other text untouched."""
    found = _QUERY_ORDER.search(text)
    if not found:
        return text
    try:
        order = json.loads(found.group(2))
    except ValueError:
        return text
    if name not in order:
        return text
    token = re.escape(json.dumps(name, ensure_ascii=False))
    array = found.group(2)
    for pattern in (r"\s*,\s*" + token, token + r"\s*,\s*", token):
        edited, count = re.subn(pattern, "", array, count=1)
        if count:
            break
    wanted = [x for x in order if x != name]
    try:
        intact = json.loads(edited) == wanted
    except ValueError:
        intact = False
    if not intact:                                   # an unusual layout: write the array afresh
        edited = json.dumps(wanted, ensure_ascii=False, separators=(",", ":"))
    return text[:found.start(2)] + edited + text[found.end(2):]


# ---------------------------------------------------------------- embedding into a model

def _definition_dir(path: PathLike) -> Path:
    root = Path(path)
    for candidate in (root, root / "definition"):
        if (candidate / "tables").is_dir():
            return candidate
    raise EmbedError(f"'{path}' is not a TMDL model definition: there is no 'tables' folder in it or "
                     "in its 'definition' subfolder.")


def _table_files(tables_dir: Path) -> Dict[str, Path]:
    found: Dict[str, Path] = {}
    for path in sorted(tables_dir.glob("*.tmdl")):
        try:
            text, _ = safe_io.read_text(path)
        except (OSError, UnicodeDecodeError):
            continue
        decl = _TABLE_DECL.search(text)
        if decl:
            found[_unquote(decl.group(1), decl.group(2)).lower()] = path
    return found


def _column_type(col: Dict[str, Any], values: Sequence[str]) -> str:
    kind = col["data_type"]
    if kind == "dateTime":
        # The model type covers both. A date-only column must not be read as midnight datetimes
        # in one place and dates in another, so the values decide.
        if col["date_only"] or all(_DATE.fullmatch(v) for v in values if v != ""):
            return "date"
        return "datetime"
    return {"string": "text", "int64": "int64", "double": "double", "decimal": "decimal",
            "boolean": "boolean"}.get(kind, "text")


def embed_tables(model_definition_dir: PathLike, tables: Mapping[str, PathLike], *, dry_run: bool = False,
                 max_bytes: int = DEFAULT_MAX_BYTES, delimiter: str = ",") -> Dict[str, Any]:
    """Replace the query of each named table with its CSV, embedded in the partition.

    model_definition_dir: the model's ``definition`` folder (or the .SemanticModel folder).
    tables: {table name: path of a UTF-8 CSV}. The header must use the table's source column
        names; column types come from the model, so the CSV carries no type information.
    dry_run: work everything out and report it, write nothing.
    max_bytes: largest CSV accepted, per table.

    Returns {success, dry_run, tables: [...sizes...], removed_expressions, kept_expressions,
    external_sources, self_contained, changed_files, totals, notes}.
    Raises EmbedError, before anything is written, when a table cannot be embedded.
    """
    definition = _definition_dir(model_definition_dir)
    if not tables:
        raise EmbedError("tables is empty: name at least one table and its CSV file.")
    files = _table_files(definition / "tables")
    tx = safe_io.FileTransaction()
    report: List[Dict[str, Any]] = []
    old_sources: List[str] = []

    for table, csv_path in tables.items():
        path = files.get(str(table).lower())
        if path is None:
            raise EmbedError(f"Table '{table}' is not in the model. Tables with a file: "
                             + ", ".join(sorted(p.stem for p in files.values())))
        csv_file = Path(csv_path)
        if not csv_file.is_file():
            raise EmbedError(f"Table '{table}': the CSV file '{csv_path}' does not exist.")
        size = csv_file.stat().st_size
        if size > max_bytes:
            raise EmbedError(
                f"Table '{table}': '{csv_file.name}' is {size:,} bytes, over the {max_bytes:,}-byte limit "
                "(max_bytes). Embedding is meant for small reference and demo tables; keep a table this "
                "size in a source the service can reach, or raise max_bytes on purpose.")
        try:
            csv_text, _ = safe_io.read_text(csv_file)
        except UnicodeDecodeError:
            raise EmbedError(f"Table '{table}': '{csv_file.name}' is not UTF-8 text.") from None
        header, rows = _read_csv(csv_text, delimiter)

        lines = tx.read(path).split("\n")
        columns = _columns(lines)
        if not columns:
            raise EmbedError(f"Table '{table}' has no column with a sourceColumn: it is a calculated "
                             "table, and has no query to replace.")
        wanted = [c["source_column"] for c in columns]
        missing = [c for c in wanted if c not in header]
        if missing:
            raise EmbedError(
                f"Table '{table}': the CSV has no column(s) {', '.join(missing)}. Its header must use the "
                f"table's source column names: {', '.join(wanted)}.")
        warnings = []
        extra = [h for h in header if h not in wanted]
        if extra:
            warnings.append(f"CSV column(s) not in the table were left out: {', '.join(extra)}")
        keep = [header.index(c) for c in wanted]
        rows = [[r[i] for i in keep] for r in rows]
        types = {c["source_column"]: _column_type(c, [r[k] for r in rows]) for k, c in enumerate(columns)}
        if not rows:
            warnings.append("The CSV has a header and no rows: the table will be empty.")

        first, last, indent, old = _partition_source(lines, str(table), path.name)
        body = next((_lead(x) for x in lines[first + 1:last] if x.strip()), None)
        # Desktop writes an expression two tabs deeper than its property; keep the file's own
        # depth when there is one to copy.
        if body is None or len(body) <= len(indent):
            body = indent + "\t\t"
        m_lines, stats = _m_lines(wanted, rows, types, delimiter)
        new_lines = lines[:first] + [f"{indent}source ="] + [body + x for x in m_lines] + lines[last:]
        tx.write(path, "\n".join(new_lines))
        old_sources.append(old)
        stats.update({"table": str(table), "file": str(path), "csv_path": str(csv_file),
                      "compression_ratio": round(stats["csv_bytes"] / max(1, stats["compressed_bytes"]), 2),
                      "warnings": warnings})
        report.append(stats)

    removed, kept = _drop_unused_parameters(definition, tx, files, old_sources)
    external = _external_sources(definition, tx, files)
    changed = tx.paths
    if not dry_run:
        changed = tx.commit()
    return {
        "success": True, "dry_run": bool(dry_run), "definition_dir": str(definition),
        "tables": report, "removed_expressions": removed, "kept_expressions": kept,
        "external_sources": external, "self_contained": not external,
        "changed_files": [str(p) for p in changed],
        "totals": {"tables": len(report), "rows": sum(t["rows"] for t in report),
                   "csv_bytes": sum(t["csv_bytes"] for t in report),
                   "embedded_chars": sum(t["embedded_chars"] for t in report)},
        "notes": [
            "Publishing uploads the definition, not the data: refresh the model once after publishing "
            "(or after opening it in Desktop). No credentials or gateway are needed for the embedded "
            "tables.",
            "The embedded text was checked by inflating it back in Python. The Power Query itself was "
            "not run: open the model and refresh before relying on it.",
            "An empty CSV field becomes null in a typed column and an empty string in a text column.",
        ] + (["Other queries still reach outside the model (see external_sources), so the service "
              "will still ask for a connection."] if external else []),
    }


def _all_sources(tx: safe_io.FileTransaction, files: Mapping[str, Path]) -> Dict[str, str]:
    """Partition M per table file, as staged. A table whose partition cannot be read
    contributes its whole text, so nothing it mentions is ever taken for unused."""
    out = {}
    for path in files.values():
        text = tx.read(path)
        try:
            _, _, _, m_text = _partition_source(text.split("\n"), path.stem, path.name)
        except EmbedError:
            m_text = text if "partition" in text else ""
        out[path.stem] = m_text
    return out


def _drop_unused_parameters(definition: Path, tx: safe_io.FileTransaction, files: Mapping[str, Path],
                            old_sources: Sequence[str]) -> Tuple[List[str], List[Dict[str, str]]]:
    """Remove each path parameter that the replaced queries used and nothing uses now."""
    path = definition / "expressions.tmdl"
    if not tx.exists(path):
        return [], []
    used_before: Set[str] = set()
    for m_text in old_sources:
        used_before |= _m_names(m_text)
    removed: List[str] = []
    kept: List[Dict[str, str]] = []
    candidates = [e["name"] for e in _expressions(tx.read(path)) if e["name"] in used_before]
    for name in candidates:
        text = tx.read(path)
        blocks = _expressions(text)
        block = next(b for b in blocks if b["name"] == name)
        if not block["parameter"]:
            kept.append({"name": name,
                         "reason": "not a parameter: shared queries and functions are left alone"})
            continue
        users = [t for t, m_text in _all_sources(tx, files).items() if name in _m_names(m_text)]
        users += [f"expression {b['name']}" for b in blocks
                  if b["name"] != name and name in _m_names(b["text"])]
        if users:
            kept.append({"name": name, "reason": "still used by " + ", ".join(sorted(users))})
            continue
        if not block["path"]:
            kept.append({"name": name,
                         "reason": "a parameter nothing uses now, but not a file path or folder: "
                                   "left in place"})
            continue
        lines = text.split("\n")
        rest = "\n".join(lines[:block["start"]] + lines[block["end"]:])
        if rest.strip():
            tx.write(path, rest)
        else:
            tx.delete(path)
        model = definition / "model.tmdl"
        if tx.exists(model):
            model_text = _drop_from_query_order(tx.read(model), name)
            # Some serializers also list shared expressions as refs.
            model_text = re.sub(r"(?m)^ref expression (?:'" + re.escape(name.replace("'", "''")) + r"'|"
                                + re.escape(name) + r")[ \t]*\n", "", model_text)
            tx.write(model, model_text)
        removed.append(name)
        if not tx.exists(path):
            break
    return removed, kept


def _external_sources(definition: Path, tx: safe_io.FileTransaction,
                      files: Mapping[str, Path]) -> List[Dict[str, Any]]:
    found = []
    for table, m_text in sorted(_all_sources(tx, files).items()):
        calls = sorted(set(_EXTERNAL.findall(_without_strings(m_text))))
        if calls:
            found.append({"object": f"table {table}", "functions": calls})
    path = definition / "expressions.tmdl"
    if tx.exists(path):
        for block in _expressions(tx.read(path)):
            calls = sorted(set(_EXTERNAL.findall(_without_strings(block["text"]))))
            if calls:
                found.append({"object": f"expression {block['name']}", "functions": calls})
    return found
