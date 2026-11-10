"""
Structural scanner for TMDL text.

TMDL is indentation based: an object is declared on one line (``measure 'Total' = ...``), its
properties and child objects sit one level deeper, and a multi-line expression sits two levels
deeper than the line that opens it. A rename has to know which of those a given line is, and
which table it belongs to, before it may touch a name on it: the same word can be a column of
this table, a column of another table, a hierarchy level, text inside a DAX string, or part of
an M query. Regexes over the whole file cannot tell them apart, so every TMDL reader and
rewriter in this server works from the tree this module builds.

    doc = scan(text)
    for node in doc.walk():
        node.keyword, node.name, node.name_span      # 'column', 'Order Date', (start, end)
        node.value, node.value_span                  # for 'sortByColumn: Month Number'
        node.expr_span, doc.expression(node)         # for '= <expression>'
        node.table                                   # name of the enclosing table, if any

Spans are offsets into the scanned text, so a rewrite is a set of splices and everything else
(indentation, comments, line endings, unknown properties) is left exactly as it was. The
scanner never raises on odd input: a line it cannot classify is kept as a node whose
``malformed`` field says why, for the validator to report.
"""
import re
from dataclasses import dataclass, field
from typing import Callable, Dict, Iterator, List, Optional, Tuple

Span = Tuple[int, int]

_HEAD = re.compile(r"(?:(ref)[ \t]+)?([A-Za-z_][A-Za-z0-9_]*)")
_FENCE = "```"

# Characters that force single quotes around a TMDL object name (TMDL language reference).
_NAME_NEEDS_QUOTES = re.compile(r"[\s.=:']")

# Declarations whose text after '=' is DAX.
DAX_OBJECTS = frozenset({"measure", "column", "calculationItem", "tablePermission", "function"})
# Expression-valued properties that hold DAX wherever they appear.
DAX_PROPERTIES = frozenset({
    "formatStringDefinition", "detailRowsDefinition", "defaultDetailRowsDefinition",
    "targetExpression", "statusExpression", "trendExpression",
    "noSelectionExpression", "multipleOrEmptySelectionExpression",
})
M_PROPERTIES = frozenset({"sourceExpression", "pollingExpression"})
# Properties whose value is a (possibly quoted, possibly dotted) reference to a model object.
TABLE_SCOPES = frozenset({"table", "perspectiveTable"})


@dataclass(eq=False)
class TmdlNode:
    keyword: str
    level: int
    line: int                              # 1-based
    start: int                             # offset of the first non-blank character of the line
    end: int                               # offset just past the line's last character
    is_ref: bool = False
    name: Optional[str] = None             # unquoted object name
    name_span: Optional[Span] = None       # span of the name token, quotes included
    value: Optional[str] = None            # text after 'key:'
    value_span: Optional[Span] = None
    expr_span: Optional[Span] = None       # text after '=' (may span several lines)
    malformed: Optional[str] = None
    parent: Optional["TmdlNode"] = None
    children: List["TmdlNode"] = field(default_factory=list)

    def ancestor(self, *keywords: str) -> Optional["TmdlNode"]:
        node = self.parent
        while node is not None:
            if node.keyword in keywords and not node.is_ref:
                return node
            node = node.parent
        return None

    @property
    def table(self) -> Optional[str]:
        """Name of the table this node belongs to: the enclosing ``table`` (or perspective
        table), or the table a ``tablePermission`` filters."""
        if self.keyword == "tablePermission":
            return self.name
        owner = self.ancestor("table", "perspectiveTable", "tablePermission")
        return owner.name if owner is not None else None

    def child(self, keyword: str) -> Optional["TmdlNode"]:
        for c in self.children:
            if c.keyword == keyword:
                return c
        return None

    def children_of(self, keyword: str) -> List["TmdlNode"]:
        return [c for c in self.children if c.keyword == keyword and not c.is_ref]


@dataclass
class TmdlDocument:
    text: str
    roots: List[TmdlNode]

    def walk(self) -> Iterator[TmdlNode]:
        stack = list(reversed(self.roots))
        while stack:
            node = stack.pop()
            yield node
            stack.extend(reversed(node.children))

    def expression(self, node: TmdlNode) -> str:
        return self.text[node.expr_span[0]:node.expr_span[1]] if node.expr_span else ""

    def language(self, node: TmdlNode) -> Optional[str]:
        """'dax', 'm', 'json' or None for the expression a node carries."""
        if node.expr_span is None or node.is_ref:
            return None
        kw = node.keyword
        if node.name is not None:
            if kw in DAX_OBJECTS:
                return "dax"
            if kw == "expression":
                return "m"
            return None
        if kw in DAX_PROPERTIES:
            return "dax"
        if kw in M_PROPERTIES:
            return "m"
        if kw == "linguisticMetadata":
            return "json"
        if kw == "source" and node.parent is not None and node.parent.keyword == "partition":
            kind = self.expression(node.parent).strip().lower()
            return {"m": "m", "calculated": "dax"}.get(kind)
        if kw == "expression" and node.parent is not None and node.parent.keyword in (
                "dataCoverageDefinition", "calculationExpression"):
            return "dax"
        return None


# ---------------------------------------------------------------- names

def needs_quotes(name: str) -> bool:
    return not name or bool(_NAME_NEEDS_QUOTES.search(name))


def quote_name(name: str, force: bool = False) -> str:
    """TMDL token for an object name: single quoted (with '' escapes) when required."""
    if force or needs_quotes(name):
        return "'" + name.replace("'", "''") + "'"
    return name


def unquote_name(token: str) -> str:
    if len(token) >= 2 and token[0] == "'" and token[-1] == "'":
        return token[1:-1].replace("''", "'")
    return token


def _scan_name(content: str, k: int) -> Tuple[str, int]:
    """(unquoted name, index just past its token) for the name token starting at ``k``."""
    if content[k] == "'":
        i = k + 1
        while i < len(content):
            if content[i] == "'":
                if content[i + 1:i + 2] == "'":
                    i += 2
                    continue
                return content[k + 1:i].replace("''", "'"), i + 1
            i += 1
        return content[k + 1:].replace("''", "'"), len(content)
    i = k
    while i < len(content) and not content[i].isspace() and content[i] not in "=:":
        i += 1
    return content[k:i], i


def split_object_path(value: str) -> List[Tuple[str, int, int]]:
    """Split a reference such as ``'Dim Date'.'Month Name'`` or ``Sales.Amount`` into
    [(unquoted part, start, end)] with spans relative to ``value``. Returns [] when the text
    is not a well-formed dotted name."""
    parts: List[Tuple[str, int, int]] = []
    i, n = 0, len(value)
    while i < n and value[i].isspace():
        i += 1
    while i < n:
        if value[i] == "'":
            name, j = _scan_name(value, i)
            if j <= i + 1 or value[j - 1] != "'":
                return []
        else:
            j = i
            while j < n and value[j] not in ".'" and not value[j].isspace():
                j += 1
            name = value[i:j]
        if not name:
            return []
        parts.append((name, i, j))
        i = j
        if i < n and value[i] == ".":
            i += 1
            continue
        break
    if value[i:].strip():
        return []
    return parts


# ---------------------------------------------------------------- scanning

def _line_table(text: str) -> List[Tuple[int, int]]:
    """[(start, end)] per line; ``end`` excludes the newline and a trailing carriage return."""
    out: List[Tuple[int, int]] = []
    pos, n = 0, len(text)
    while pos <= n:
        nl = text.find("\n", pos)
        end = n if nl == -1 else nl
        stop = end - 1 if end > pos and text[end - 1] == "\r" else end
        out.append((pos, stop))
        if nl == -1:
            break
        pos = nl + 1
    return out


def _indent_measure(text: str, lines: List[Tuple[int, int]]) -> Callable[[int, int], Tuple[int, int]]:
    """Returns f(start, end) -> (level, content_start). Tabs are one level each (the form
    Power BI Desktop writes); a file indented only with spaces uses its narrowest indent as
    the unit, so hand-written files still nest correctly."""
    uses_tabs = any(text.startswith("\t", s) for s, _ in lines)
    unit = 0
    if not uses_tabs:
        widths = []
        for s, e in lines:
            w = 0
            while s + w < e and text[s + w] == " ":
                w += 1
            if 0 < w < e - s:
                widths.append(w)
        unit = min(widths) if widths else 1

    def measure(s: int, e: int) -> Tuple[int, int]:
        i = s
        if uses_tabs:
            while i < e and text[i] == "\t":
                i += 1
            level = i - s
            while i < e and text[i] in " \t":
                i += 1
            return level, i
        while i < e and text[i] in " \t":
            i += 1
        return (i - s) // unit, i

    return measure


def scan(text: str) -> TmdlDocument:
    """Build the object tree of one TMDL file."""
    lines = _line_table(text)
    measure = _indent_measure(text, lines)
    total = len(lines)
    blank = [not text[s:e].strip() for s, e in lines]
    levels = [measure(s, e) for s, e in lines]

    def block_end(first: int, min_level: int) -> int:
        """Index just past the last line of the run starting at ``first`` whose lines are
        blank or indented to at least ``min_level``; trailing blank lines are not included."""
        last = first - 1
        j = first
        while j < total:
            if blank[j]:
                j += 1
                continue
            if levels[j][0] < min_level:
                break
            last = j
            j += 1
        return last + 1

    roots: List[TmdlNode] = []
    stack: List[TmdlNode] = []
    i = 0
    while i < total:
        s, e = lines[i]
        if blank[i]:
            i += 1
            continue
        level, cs = levels[i]
        content = text[cs:e]
        if content.startswith("///"):
            i += 1
            continue
        nxt = i + 1
        m = _HEAD.match(content)
        if not m:
            node = TmdlNode(keyword="", level=level, line=i + 1, start=cs, end=e,
                            malformed="line does not start with a TMDL keyword")
        else:
            node = TmdlNode(keyword=m.group(2), level=level, line=i + 1, start=cs, end=e,
                            is_ref=bool(m.group(1)))
            k = m.end()
            while k < len(content) and content[k] in " \t":
                k += 1
            eq = -1
            if k >= len(content):
                pass
            elif content[k] == ":" and not node.is_ref:
                v = k + 1
                while v < len(content) and content[v] in " \t":
                    v += 1
                node.value = content[v:].rstrip()
                node.value_span = (cs + v, cs + v + len(node.value))
            elif content[k] == "=":
                eq = k
            else:
                name, name_end = _scan_name(content, k)
                node.name, node.name_span = name, (cs + k, cs + name_end)
                p = name_end
                while p < len(content) and content[p] in " \t":
                    p += 1
                if p < len(content):
                    if content[p] == "=":
                        eq = p
                    else:
                        node.malformed = ("unexpected text after the object name (a name with "
                                          "spaces must be wrapped in single quotes)")
            if eq >= 0:
                after = content[eq + 1:]
                stripped = after.strip()
                lead = len(after) - len(after.lstrip())
                first = cs + eq + 1 + lead
                if stripped.startswith(_FENCE):
                    close = stripped.find(_FENCE, len(_FENCE))
                    if close != -1:
                        node.expr_span = (first + len(_FENCE), first + close)
                    else:
                        j = i + 1
                        while j < total and text[lines[j][0]:lines[j][1]].strip() != _FENCE:
                            j += 1
                        body_start = lines[i + 1][0] if i + 1 < total else e
                        body_end = lines[j - 1][1] if j - 1 > i else body_start
                        node.expr_span = (body_start, max(body_start, body_end))
                        if j >= total:
                            node.malformed = "expression opened with ``` is never closed"
                        nxt = min(j + 1, total)
                elif not stripped:
                    stop = block_end(i + 1, level + 2)
                    if stop > i + 1:
                        node.expr_span = (lines[i + 1][0], lines[stop - 1][1])
                    else:
                        node.expr_span = (e, e)
                    nxt = stop
                else:
                    # An expression that starts on the declaration line normally ends there;
                    # lines indented two levels deeper can only be its continuation.
                    stop = block_end(i + 1, level + 2)
                    node.expr_span = (first, lines[stop - 1][1] if stop > i + 1 else e)
                    nxt = stop
        while stack and stack[-1].level >= level:
            stack.pop()
        if stack:
            node.parent = stack[-1]
            stack[-1].children.append(node)
        else:
            roots.append(node)
        stack.append(node)
        i = max(nxt, i + 1)
    return TmdlDocument(text=text, roots=roots)


# ---------------------------------------------------------------- model catalog

@dataclass
class ColumnInfo:
    name: str
    data_type: Optional[str] = None
    calculated: bool = False
    sort_by: Optional[str] = None


@dataclass
class TableInfo:
    name: str
    file: Optional[str] = None
    columns: Dict[str, ColumnInfo] = field(default_factory=dict)       # casefolded name -> info
    measures: Dict[str, str] = field(default_factory=dict)             # casefolded name -> name
    hierarchies: Dict[str, List[str]] = field(default_factory=dict)    # casefolded name -> level names
    hierarchy_names: Dict[str, str] = field(default_factory=dict)
    is_calculation_group: bool = False

    def has_field(self, name: str) -> bool:
        key = name.casefold()
        return key in self.columns or key in self.measures


class ModelCatalog:
    """Tables, columns, measures and hierarchies declared by a set of TMDL files. Lookups are
    case-insensitive, as the engine's are."""

    def __init__(self) -> None:
        self.tables: Dict[str, TableInfo] = {}
        self.duplicates: List[str] = []

    def add_document(self, doc: TmdlDocument, file: Optional[str] = None) -> None:
        for node in doc.roots:
            if node.keyword != "table" or node.is_ref or node.name is None:
                continue
            key = node.name.casefold()
            if key in self.tables:
                self.duplicates.append(f"table '{node.name}' is declared more than once")
                info = self.tables[key]
            else:
                info = self.tables[key] = TableInfo(name=node.name, file=file)
            for child in node.children:
                if child.is_ref or child.name is None:
                    continue
                ckey = child.name.casefold()
                if child.keyword == "column":
                    if ckey in info.columns or ckey in info.measures:
                        self.duplicates.append(f"'{node.name}'[{child.name}] is declared more than once")
                    dt = child.child("dataType")
                    sb = child.child("sortByColumn")
                    info.columns[ckey] = ColumnInfo(
                        name=child.name, data_type=dt.value if dt is not None else None,
                        calculated=child.expr_span is not None,
                        sort_by=unquote_name(sb.value) if sb is not None and sb.value else None)
                elif child.keyword == "measure":
                    if ckey in info.columns or ckey in info.measures:
                        self.duplicates.append(f"'{node.name}'[{child.name}] is declared more than once")
                    info.measures[ckey] = child.name
                elif child.keyword == "hierarchy":
                    info.hierarchy_names[ckey] = child.name
                    info.hierarchies[ckey] = [lv.name for lv in child.children_of("level") if lv.name]
                elif child.keyword == "calculationGroup":
                    info.is_calculation_group = True
            if node.child("calculationGroup") is not None:
                info.is_calculation_group = True

    @classmethod
    def from_texts(cls, texts: Dict[str, str]) -> "ModelCatalog":
        cat = cls()
        for path, text in texts.items():
            cat.add_document(scan(text), path)
        return cat

    def table(self, name: Optional[str]) -> Optional[TableInfo]:
        return self.tables.get(name.casefold()) if name is not None else None

    def has_table(self, name: Optional[str]) -> bool:
        return self.table(name) is not None

    def has_column(self, table: Optional[str], name: Optional[str]) -> bool:
        t = self.table(table)
        return t is not None and name is not None and name.casefold() in t.columns

    def has_measure(self, table: Optional[str], name: Optional[str]) -> bool:
        t = self.table(table)
        return t is not None and name is not None and name.casefold() in t.measures

    def has_field(self, table: Optional[str], name: Optional[str]) -> bool:
        t = self.table(table)
        return t is not None and name is not None and t.has_field(name)

    def has_member(self, table: Optional[str], name: Optional[str]) -> bool:
        """True for a column, measure, hierarchy or hierarchy level of the table (everything a
        report can bind to)."""
        t = self.table(table)
        if t is None or name is None:
            return False
        key = name.casefold()
        if t.has_field(name) or key in t.hierarchies:
            return True
        return any(key == lv.casefold() for levels in t.hierarchies.values() for lv in levels)

    def measure_tables(self, name: str) -> List[str]:
        key = name.casefold()
        return [t.name for t in self.tables.values() if key in t.measures]

    def column_tables(self, name: str) -> List[str]:
        key = name.casefold()
        return [t.name for t in self.tables.values() if key in t.columns]

    def all_measure_names(self) -> Dict[str, List[str]]:
        out: Dict[str, List[str]] = {}
        for t in self.tables.values():
            for key in t.measures:
                out.setdefault(key, []).append(t.name)
        return out
