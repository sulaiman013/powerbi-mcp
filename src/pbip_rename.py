"""
Transactional rename engine for PBIP projects (TMDL model + PBIR report).

A rename is planned completely in memory and written all-or-nothing:

  1. read the model (tmdl_scan) and build the catalog of tables, columns and measures;
  2. refuse up front when the old name does not exist or the new name collides;
  3. rewrite every TMDL file from its object tree: declarations scoped to the owning table,
     DAX through the shared tokenizer (dax_tokens), M through pbip_mquery, structural
     properties (relationships, sortByColumn, hierarchy levels, perspectives, roles, query
     order, linguistic metadata) from their parsed values;
  4. rewrite every report file structurally (pbip_report);
  5. validate the staged result: every JSON still parses, and the model and the report
     bindings have no broken reference that was not already broken before;
  6. commit through safe_io.FileTransaction, which replaces all files or none.

Several renames given together are applied in order to the staged state and committed once.
Nothing here swallows an error: an unreadable file, a failed check or a failed write ends the
whole operation with ``success=False`` and the project untouched. What the engine cannot
decide (an unqualified [Name] that might be something else, free text that still contains the
old name) is reported in ``warnings`` rather than guessed.
"""
import json
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Set, Tuple

import dax_tokens
import pbip_json
import pbip_mquery
import pbip_report
import tmdl_scan
from pbip_report import RenameOp
from safe_io import FileTransaction
from tmdl_scan import ModelCatalog, TmdlDocument, TmdlNode

_eq = dax_tokens.names_equal

# Properties whose value is a Table.Column path.
_COLUMN_PATH_PROPERTIES = frozenset({"fromColumn", "toColumn", "defaultColumn", "baseColumn"})
_OTHER_PATH_PROPERTIES = frozenset({"defaultHierarchy"})
_TABLE_PROPERTIES = frozenset({"baseTable"})
# Properties that name a column of the table they sit in.
_SAME_TABLE_COLUMN_PROPERTIES = frozenset({"sortByColumn", "groupByColumn"})
MAX_LISTED = 12

Problem = Tuple[str, str, str]             # (kind, table casefolded, name casefolded)
BeforeCommit = Callable[[], Optional[str]]


class RenameError(Exception):
    """A rename that must not start (missing object, name collision, invalid name)."""


@dataclass
class Issue:
    file: str
    line: int
    kind: str
    message: str
    context: str = ""


@dataclass
class EngineResult:
    success: bool
    message: str
    files_modified: List[str] = field(default_factory=list)
    references_updated: int = 0
    warnings: List[str] = field(default_factory=list)
    issues: List[Issue] = field(default_factory=list)
    operations: List[Dict[str, object]] = field(default_factory=list)
    originals: Dict[str, Optional[bytes]] = field(default_factory=dict)


# ---------------------------------------------------------------- TMDL rewriting

def _line_bounds(text: str, offset: int) -> Tuple[int, int]:
    """(start, end) of the line containing ``offset``, the end including its newline."""
    start = text.rfind("\n", 0, offset) + 1
    nl = text.find("\n", offset)
    return start, len(text) if nl == -1 else nl + 1


def _name_token(new: str) -> str:
    """TMDL token for a name: quoted only when TMDL requires it, the way Desktop serializes."""
    return tmdl_scan.quote_name(new)


def relationship_renames(docs: Iterable[TmdlDocument], op: RenameOp) -> Dict[str, str]:
    """Auto-style relationship names ('Sales to Product') that carry the renamed table."""
    out: Dict[str, str] = {}
    if op.kind != "table":
        return out
    for doc in docs:
        for node in doc.roots:
            if node.keyword != "relationship" or node.is_ref or not node.name or " to " not in node.name:
                continue
            left, right = node.name.split(" to ", 1)
            new_left = op.new if _eq(left, op.old) else left
            new_right = op.new if _eq(right, op.old) else right
            if (new_left, new_right) != (left, right):
                out[node.name] = f"{new_left} to {new_right}"
    return out


def rewrite_tmdl(text: str, op: RenameOp, catalog: ModelCatalog, label: str,
                 rel_renames: Optional[Dict[str, str]] = None) -> Tuple[str, int, List[str]]:
    """Apply one rename to one TMDL file. Returns (new_text, replacements, warnings)."""
    rel_renames = rel_renames or {}
    doc = tmdl_scan.scan(text)
    edits = pbip_json.Edits(text)
    warnings: List[str] = []
    count = 0

    def set_name(node: TmdlNode) -> None:
        nonlocal count
        edits.replace_span(node.name_span[0], node.name_span[1], _name_token(op.new))
        count += 1

    def where(node: TmdlNode) -> str:
        owner = node if node.name else (node.parent or node)
        named = f"{owner.keyword} '{owner.name}'" if owner.name else owner.keyword
        return f"{label}: {named} (line {node.line})"

    for node in doc.walk():
        kw = node.keyword

        # ---- names on declarations and ref lines
        if node.name is not None and node.name_span is not None:
            if op.kind == "table":
                if kw in ("table", "perspectiveTable", "tablePermission") and _eq(node.name, op.old):
                    set_name(node)
                elif (kw == "partition" and not node.is_ref and _eq(node.name, op.old)
                      and _eq(node.table, op.old)):
                    set_name(node)
                elif kw == "relationship" and node.name in rel_renames:
                    new_name = rel_renames[node.name]
                    edits.replace_span(node.name_span[0], node.name_span[1], _name_token(new_name))
                    count += 1
            elif _eq(node.name, op.old) and _eq(node.table, op.table) and not node.is_ref:
                if op.kind == "column" and kw in ("column", "perspectiveColumn", "columnPermission"):
                    set_name(node)
                    inferred = node.child("isNameInferred")
                    if inferred is not None and kw == "column":
                        # The engine re-derives the name of a column flagged isNameInferred
                        # from its source on the next refresh, which would undo the rename.
                        start, end = _line_bounds(text, inferred.start)
                        edits.replace_span(start, end, "")
                        warnings.append(f"{where(node)}: removed isNameInferred so the new name is kept")
                elif op.kind == "measure" and kw in ("measure", "perspectiveMeasure"):
                    set_name(node)

        # ---- properties that refer to model objects
        if node.value is not None and node.value_span is not None:
            vs = node.value_span[0]
            if kw in _COLUMN_PATH_PROPERTIES or kw in _OTHER_PATH_PROPERTIES:
                parts = tmdl_scan.split_object_path(node.value)
                if len(parts) >= 2:
                    (tname, ts, te), (member, ms, me) = parts[0], parts[1]
                    if op.kind == "table" and _eq(tname, op.old):
                        edits.replace_span(vs + ts, vs + te, _name_token(op.new))
                        count += 1
                    elif (op.kind == "column" and kw in _COLUMN_PATH_PROPERTIES
                          and _eq(tname, op.table) and _eq(member, op.old)):
                        edits.replace_span(vs + ms, vs + me, _name_token(op.new))
                        count += 1
            elif kw in _TABLE_PROPERTIES:
                if op.kind == "table" and _eq(tmdl_scan.unquote_name(node.value), op.old):
                    edits.replace_span(vs, node.value_span[1], _name_token(op.new))
                    count += 1
            elif kw in _SAME_TABLE_COLUMN_PROPERTIES or (
                    kw == "column" and node.parent is not None and node.parent.keyword == "level"):
                if (op.kind == "column" and _eq(tmdl_scan.unquote_name(node.value), op.old)
                        and _eq(node.table, op.table)):
                    edits.replace_span(vs, node.value_span[1], _name_token(op.new))
                    count += 1
            elif kw == "relationship" and tmdl_scan.unquote_name(node.value) in rel_renames:
                new_name = rel_renames[tmdl_scan.unquote_name(node.value)]
                edits.replace_span(vs, node.value_span[1], _name_token(new_name))
                count += 1

        # ---- expressions
        if node.expr_span is None or node.is_ref:
            continue
        es, ee = node.expr_span
        expr = text[es:ee]
        lang = doc.language(node)
        if lang == "dax":
            new_expr, n, notes = pbip_report.rename_in_dax(
                expr, op, catalog, node.table, row_context=kw in ("column", "tablePermission"))
            if n:
                edits.replace_span(es, ee, new_expr)
                count += n
            warnings.extend(f"{where(node)}: {note}" for note in notes)
            mention = _mention_in_text_tokens(new_expr, op)
            if mention:
                warnings.append(f"{where(node)}: {mention}")
        elif lang == "m" and op.kind == "table":
            new_expr, n, shadowed = pbip_mquery.rename_query_references(expr, op.old, op.new)
            if n:
                edits.replace_span(es, ee, new_expr)
                count += n
            if shadowed:
                warnings.append(f"{where(node)}: the query defines its own '{op.old}' step or parameter, "
                                "so its uses were left unchanged")
        elif lang == "json":
            n, note = _linguistic_metadata(expr, es, edits, op)
            count += n
            if note:
                warnings.append(f"{where(node)}: {note}")
        elif kw == "annotation" and node.name == "PBI_QueryOrder" and op.kind == "table":
            count += _query_order(expr, es, edits, op)

    return edits.apply(), count, warnings


def _mention_in_text_tokens(expr: str, op: RenameOp) -> Optional[str]:
    """A note when a DAX string literal or comment still spells out a reference to the old
    name (commented-out code, DAX built as text). These are not rewritten."""
    if op.kind == "table":
        shown = dax_tokens.quote_table(op.old, force=True)
        pattern = re.compile(re.escape(shown) + r"|(?<![\w'])" + re.escape(op.old) + r"\s*\[", re.IGNORECASE)
    else:
        shown = dax_tokens.bracket(op.old)
        pattern = re.compile(re.escape(shown), re.IGNORECASE)
    hits = sum(1 for t in dax_tokens.tokenize(expr)
               if t.kind in (dax_tokens.STRING, dax_tokens.COMMENT) and pattern.search(t.text))
    if not hits:
        return None
    return (f"{hits} string literal(s) or comment(s) still mention {shown}; they are text, "
            "so they were left unchanged")


def _linguistic_metadata(expr: str, offset: int, edits: pbip_json.Edits,
                         op: RenameOp) -> Tuple[int, Optional[str]]:
    """Rename ConceptualEntity / ConceptualProperty bindings in a culture's linguistic schema."""
    if op.old.casefold() not in expr.casefold():
        return 0, None
    try:
        doc = pbip_json.parse(expr)
    except pbip_json.JsonSyntaxError:
        return 0, (f"the linguistic metadata is not JSON and mentions '{op.old}'; "
                   "its Q&A bindings were left unchanged")
    count = 0
    stack = [doc]
    while stack:
        node = stack.pop()
        if isinstance(node, list):
            stack.extend(node)
            continue
        if not isinstance(node, dict):
            continue
        entity = node.get("ConceptualEntity")
        prop = node.get("ConceptualProperty")
        target = None
        if isinstance(entity, pbip_json.JStr):
            if op.kind == "table" and _eq(entity, op.old):
                target = entity
            elif (op.kind != "table" and _eq(entity, op.table)
                  and isinstance(prop, pbip_json.JStr) and _eq(prop, op.old)):
                target = prop
        if target is not None:
            token = pbip_json.encode_string(op.new, expr[target.start:target.end])
            edits.replace_span(offset + target.start, offset + target.end, token)
            count += 1
        stack.extend(node.values())
    return count, None


def _query_order(expr: str, offset: int, edits: pbip_json.Edits, op: RenameOp) -> int:
    try:
        doc = pbip_json.parse(expr)
    except pbip_json.JsonSyntaxError:
        return 0
    count = 0
    for item in doc if isinstance(doc, list) else []:
        if isinstance(item, pbip_json.JStr) and _eq(item, op.old):
            edits.replace_span(offset + item.start, offset + item.end,
                               pbip_json.encode_string(op.new, expr[item.start:item.end]))
            count += 1
    return count


# ---------------------------------------------------------------- unquoted multi-word tables

def quote_unquoted_tables(expr: str, table_names: Iterable[str]) -> Tuple[str, int]:
    """Quote table names containing spaces that a DAX expression uses without quotes, as in
    ``SUM(Leads Sales Data[Amount])``. Only whole-token matches outside strings, comments,
    quoted names and brackets are touched, and only where a table can stand: directly before
    a [column], or as a complete function argument."""
    names = sorted({n for n in table_names if n and any(c.isspace() for c in n)}, key=len, reverse=True)
    if not names:
        return expr, 0
    tokens = dax_tokens.tokenize(expr)
    by_end = {t.end: i for i, t in enumerate(tokens)}
    taken: Set[int] = set()
    spans: List[Tuple[int, int, str]] = []
    plain = (dax_tokens.IDENT, dax_tokens.NUMBER, dax_tokens.OP, dax_tokens.WS)
    for name in names:
        for i, t in enumerate(tokens):
            if t.kind not in (dax_tokens.IDENT, dax_tokens.NUMBER) or i in taken:
                continue
            end = t.start + len(name)
            if expr[t.start:end].casefold() != name.casefold() or end not in by_end:
                continue
            j = by_end[end]
            covered = range(i, j + 1)
            if any(k in taken or tokens[k].kind not in plain for k in covered):
                continue
            before = _neighbour(tokens, i, -1)
            after = _neighbour(tokens, j, +1)
            qualifies = after is not None and after.kind == dax_tokens.BRACKET
            argument = (before is not None and before.kind == dax_tokens.OP and before.text in "(,"
                        and after is not None and after.kind == dax_tokens.OP and after.text in ",)")
            if not (qualifies or argument):
                continue
            taken.update(covered)
            spans.append((t.start, end, dax_tokens.quote_table(name, force=True)))
    if not spans:
        return expr, 0
    out, cursor = [], 0
    for start, end, repl in sorted(spans):
        out.append(expr[cursor:start])
        out.append(repl)
        cursor = end
    out.append(expr[cursor:])
    return "".join(out), len(spans)


def _neighbour(tokens: List[dax_tokens.Token], index: int, step: int) -> Optional[dax_tokens.Token]:
    k = index + step
    while 0 <= k < len(tokens):
        if tokens[k].kind not in (dax_tokens.WS, dax_tokens.COMMENT):
            return tokens[k]
        k += step
    return None


# ---------------------------------------------------------------- validation

def model_problems(docs: Dict[str, TmdlDocument], catalog: ModelCatalog) -> Dict[Problem, str]:
    """Broken references inside the model: {(kind, table, name): where it was seen}. Covers
    relationship endpoints, sortByColumn, hierarchy level columns, model 'ref table' lines and
    table-qualified DAX references. Unqualified DAX names cannot be resolved offline and are
    not judged."""
    out: Dict[Problem, str] = {}

    def add(kind: str, table: str, name: str, where: str) -> None:
        out.setdefault((kind, table.casefold(), name.casefold()), where)

    spaced_tables = [t.name for t in catalog.tables.values() if any(c.isspace() for c in t.name)]
    for label, doc in docs.items():
        for node in doc.walk():
            kw = node.keyword
            at = f"{label}:{node.line}"
            if node.is_ref:
                if kw == "table" and node.name and not catalog.has_table(node.name):
                    add("ref table", node.name, "", at)
                continue
            if node.value is not None:
                if kw in ("fromColumn", "toColumn"):
                    parts = tmdl_scan.split_object_path(node.value)
                    if len(parts) == 2 and not catalog.has_column(parts[0][0], parts[1][0]):
                        add("relationship column", parts[0][0], parts[1][0], at)
                elif kw == "sortByColumn" or (kw == "column" and node.parent is not None
                                              and node.parent.keyword == "level"):
                    owner = node.ancestor("table")
                    name = tmdl_scan.unquote_name(node.value)
                    if owner is not None and owner.level == 0 and not catalog.has_column(owner.name, name):
                        add("sort or level column", owner.name or "", name, at)
            if doc.language(node) == "dax":
                # Read a multi-word table written without quotes as the table it means, so it
                # is reported once (as unquoted) and not again as an unknown table.
                expr = quote_unquoted_tables(doc.expression(node), spaced_tables)[0]
                for ref in dax_tokens.references(expr):
                    if ref.kind != "column" or ref.table is None or ref.name is None:
                        continue
                    if not catalog.has_table(ref.table):
                        add("DAX table", ref.table, "", at)
                    elif not catalog.has_field(ref.table, ref.name):
                        add("DAX reference", ref.table, ref.name, at)
    return out


def binding_problems(report_texts: Dict[str, str], catalog: ModelCatalog) -> Dict[Problem, str]:
    """Report references to tables or fields the model does not have."""
    out: Dict[Problem, str] = {}
    for label, text in report_texts.items():
        try:
            refs, tables = pbip_report.references_in_text(text)
        except pbip_json.JsonSyntaxError as e:
            raise RenameError(f"{label} is not valid JSON ({e})") from None
        for table in tables:
            if not catalog.has_table(table):
                out.setdefault(("report table", table.casefold(), ""), label)
        for table, name in refs:
            if catalog.has_table(table) and not catalog.has_member(table, name):
                out.setdefault(("report field", table.casefold(), name.casefold()), label)
    return out


def _map_problem(problem: Problem, op: RenameOp) -> Problem:
    kind, table, name = problem
    if op.kind == "table":
        return (kind, op.new.casefold(), name) if table == op.old.casefold() else problem
    if table == (op.table or "").casefold() and name == op.old.casefold():
        return kind, table, op.new.casefold()
    return problem


# ---------------------------------------------------------------- the engine

class RenameEngine:
    """Plans and commits renames over one PBIP project's files."""

    def __init__(self, root: Path, tmdl_files: Iterable[Path], report_files: Iterable[Path],
                 diagram_layout: Optional[Path] = None) -> None:
        self.root = Path(root)
        self.tmdl_files = [Path(p) for p in tmdl_files]
        self.report_files = [Path(p) for p in report_files]
        self.diagram_layout = Path(diagram_layout) if diagram_layout else None
        self.tx = FileTransaction()

    # -- helpers

    def label(self, path) -> str:
        try:
            return Path(os.path.relpath(path, self.root)).as_posix()
        except ValueError:
            return str(path)

    def _tmdl_texts(self) -> Dict[Path, str]:
        return {p: self.tx.read(p) for p in self.tmdl_files}

    def _report_texts(self) -> Dict[Path, str]:
        return {p: self.tx.read(p) for p in self.report_files}

    def catalog(self) -> ModelCatalog:
        cat = ModelCatalog()
        for path, text in self._tmdl_texts().items():
            cat.add_document(tmdl_scan.scan(text), str(path))
        return cat

    def _problems(self) -> Tuple[Dict[Problem, str], ModelCatalog]:
        docs = {self.label(p): tmdl_scan.scan(t) for p, t in self._tmdl_texts().items()}
        cat = ModelCatalog()
        for label, doc in docs.items():
            cat.add_document(doc, label)
        problems = model_problems(docs, cat)
        problems.update(binding_problems({self.label(p): t for p, t in self._report_texts().items()}, cat))
        for dup in cat.duplicates:
            problems.setdefault(("duplicate", dup.casefold(), ""), "model")
        return problems, cat

    # -- preconditions

    @staticmethod
    def resolve(op: RenameOp, catalog: ModelCatalog) -> RenameOp:
        """Check that ``op`` can start and fill in a measure's home table. Raises RenameError."""
        new = op.new
        if not isinstance(new, str) or not new.strip():
            raise RenameError("the new name is empty")
        if new != new.strip():
            raise RenameError(f"the new name {new!r} starts or ends with whitespace")
        if any(ord(c) < 32 for c in new):
            raise RenameError("the new name contains a line break or control character")
        if op.kind == "table":
            if not catalog.has_table(op.old):
                raise RenameError(f"table '{op.old}' does not exist in the model")
            if new == catalog.table(op.old).name:
                raise RenameError(f"table '{op.old}' already has that name")
            if catalog.has_table(new) and not _eq(new, op.old):
                raise RenameError(f"a table named '{new}' already exists")
            return RenameOp("table", catalog.table(op.old).name, new)
        if op.kind == "column":
            table = catalog.table(op.table)
            if table is None:
                raise RenameError(f"table '{op.table}' does not exist in the model")
            if op.old.casefold() not in table.columns:
                is_measure = op.old.casefold() in table.measures
                hint = " (it is a measure: use the measure rename)" if is_measure else ""
                raise RenameError(f"column '{table.name}'[{op.old}] does not exist{hint}")
            current = table.columns[op.old.casefold()].name
            if new == current:
                raise RenameError(f"column '{table.name}'[{current}] already has that name")
            if table.has_field(new) and not _eq(new, op.old):
                raise RenameError(f"'{table.name}' already has a column or measure named '{new}'")
            return RenameOp("column", current, new, table.name)
        if op.kind == "measure":
            homes = catalog.measure_tables(op.old)
            if op.table is not None:
                homes = [t for t in homes if _eq(t, op.table)]
            if not homes:
                raise RenameError(f"measure '{op.old}' does not exist in the model")
            if len(homes) > 1:
                raise RenameError(f"measure '{op.old}' is declared in more than one table "
                                  f"({', '.join(homes)}); fix the model first")
            table = catalog.table(homes[0])
            current = table.measures[op.old.casefold()]
            if new == current:
                raise RenameError(f"measure '{current}' already has that name")
            if not _eq(new, op.old):
                taken = catalog.measure_tables(new)
                if taken:
                    raise RenameError(f"a measure named '{new}' already exists in '{taken[0]}'")
                if new.casefold() in table.columns:
                    raise RenameError(f"'{table.name}' already has a column named '{new}'")
            return RenameOp("measure", current, new, table.name)
        raise RenameError(f"unknown rename kind '{op.kind}'")

    # -- one operation on the staged state

    def _apply(self, op: RenameOp, catalog: ModelCatalog) -> Tuple[int, List[str]]:
        count = 0
        warnings: List[str] = []
        tmdl = self._tmdl_texts()
        rel = relationship_renames((tmdl_scan.scan(t) for t in tmdl.values()), op)
        for path, text in tmdl.items():
            new_text, n, notes = rewrite_tmdl(text, op, catalog, self.label(path), rel)
            count += n
            warnings.extend(notes)
            if new_text != text:
                self.tx.write(path, new_text)
        if op.kind == "table" and self.diagram_layout is not None and self.tx.exists(self.diagram_layout):
            count += self._diagram(op)
        rewriter = pbip_report.ReportRewriter(op, catalog)
        leftovers: List[str] = []
        for path, text in self._report_texts().items():
            label = self.label(path)
            result = self._rewrite_report(rewriter, text, label)
            count += result.count
            warnings.extend(result.warnings)
            if result.text != text:
                self.tx.write(path, result.text)
            hits = pbip_report.leftover_mentions(result.text, op.old, op.new)
            if hits:
                where, snippet = hits[0]
                leftovers.append(f"{label}: {len(hits)} text value(s) still contain '{op.old}' and were "
                                 f"left as written (first at {where}: \"{snippet.strip()}\")")
        warnings.extend(leftovers[:MAX_LISTED])
        if len(leftovers) > MAX_LISTED:
            warnings.append(f"... and {len(leftovers) - MAX_LISTED} more report file(s) with text that "
                            f"still contains '{op.old}'")
        return count, warnings

    @staticmethod
    def _rewrite_report(rewriter: pbip_report.ReportRewriter, text: str,
                        label: str) -> pbip_report.RewriteResult:
        try:
            return rewriter.rewrite(text, label)
        except pbip_json.JsonSyntaxError as e:
            raise RenameError(f"{label} is not valid JSON ({e})") from None

    def _diagram(self, op: RenameOp) -> int:
        text = self.tx.read(self.diagram_layout)
        if op.old.casefold() not in text.casefold():
            return 0
        doc = pbip_json.parse(text)
        edits = pbip_json.Edits(text)
        count = 0
        stack = [doc]
        while stack:
            node = stack.pop()
            if isinstance(node, dict):
                token = node.get("nodeIndex")
                if isinstance(token, pbip_json.JStr) and _eq(token, op.old):
                    count += edits.set_string(token, op.new)
                stack.extend(node.values())
            elif isinstance(node, list):
                stack.extend(node)
        if count:
            self.tx.write(self.diagram_layout, edits.apply())
        return count

    # -- public operations

    def rename(self, ops: List[RenameOp], before_commit: Optional[BeforeCommit] = None) -> EngineResult:
        """Apply ``ops`` in order as one transaction. ``before_commit`` runs after the plan has
        been validated and just before anything is written; returning a message stops the
        commit (used to take a backup only for renames that will really happen)."""
        if not ops:
            return EngineResult(False, "Nothing was changed: no renames were given.")
        return self._guarded(lambda: self._run(ops, False, before_commit))

    def repair_report_table(self, old: str, new: str,
                            before_commit: Optional[BeforeCommit] = None) -> EngineResult:
        """Point report references at table ``new`` where they still name ``old``, a table the
        model no longer has (the state a model-only rename leaves behind)."""
        def run() -> EngineResult:
            catalog = self.catalog()
            if not catalog.has_table(new):
                raise RenameError(f"table '{new}' does not exist in the model")
            if catalog.has_table(old):
                raise RenameError(f"table '{old}' still exists in the model; use a table rename instead")
            return self._run([RenameOp("table", old, catalog.table(new).name)], True, before_commit)
        return self._guarded(run)

    def _guarded(self, run: Callable[[], EngineResult]) -> EngineResult:
        """Turn an expected failure into a result that says nothing was written. A failed
        commit lands here too: the transaction has already restored every file it replaced."""
        try:
            return run()
        except RenameError as e:
            self.tx.discard()
            return EngineResult(False, f"Nothing was changed: {e}.")
        except (OSError, UnicodeError, pbip_json.JsonSyntaxError) as e:
            self.tx.discard()
            return EngineResult(False, f"Nothing was changed: {type(e).__name__}: {e}")

    def _run(self, ops: List[RenameOp], repair: bool, before_commit: Optional[BeforeCommit]) -> EngineResult:
        before, catalog = self._problems()
        expected: Set[Problem] = set(before)
        total = 0
        warnings: List[str] = []
        operations: List[Dict[str, object]] = []
        for raw in ops:
            if repair:
                op = raw
                count, notes = self._apply_repair(op, catalog)
            else:
                op = self.resolve(raw, catalog)
                count, notes = self._apply(op, catalog)
            expected = {_map_problem(p, op) for p in expected}
            catalog = self.catalog()
            if not repair:
                self._assert_renamed(op, catalog)
            total += count
            warnings.extend(notes)
            operations.append({"kind": op.kind, "table": op.table, "old": op.old, "new": op.new,
                               "references_updated": count})
        warnings = _dedupe(warnings)

        issues = self._validate(expected, repair)
        if issues:
            self.tx.discard()
            listed = "; ".join(i.message for i in issues[:MAX_LISTED])
            more = f" (+{len(issues) - MAX_LISTED} more)" if len(issues) > MAX_LISTED else ""
            return EngineResult(False, "Nothing was changed: the result would not be consistent: "
                                       f"{listed}{more}", issues=issues, warnings=warnings,
                                operations=operations)

        changed = self.tx.paths
        if repair and not changed:
            return EngineResult(False, f"Nothing was changed: no report reference to table "
                                       f"'{ops[0].old}' was found.", warnings=warnings, operations=operations)
        originals: Dict[str, Optional[bytes]] = {path: _read_bytes(path) for path in changed}
        if before_commit is not None:
            stop = before_commit()
            if stop:
                self.tx.discard()
                return EngineResult(False, f"Nothing was changed: {stop}.", warnings=warnings,
                                    operations=operations)
        try:
            committed = self.tx.commit()
        except OSError as e:
            # The transaction puts back what it had already replaced, but that restore can
            # fail for the same reason the write did. Report what is really on disk.
            stuck = {p: data for p, data in originals.items() if _read_bytes(p) != data}
            if not stuck:
                raise
            names = ", ".join(self.label(p) for p in sorted(stuck)[:MAX_LISTED])
            return EngineResult(
                False, f"The write failed ({type(e).__name__}: {e}) and {len(stuck)} file(s) could not be "
                       f"put back as they were: {names}. The project is partly renamed; restore those "
                       "files (undo, a backup or source control) before doing anything else.",
                warnings=warnings, operations=operations, originals=stuck)
        return EngineResult(True, "", files_modified=sorted(committed), references_updated=total,
                            warnings=warnings, operations=operations, originals=originals)

    def _apply_repair(self, op: RenameOp, catalog: ModelCatalog) -> Tuple[int, List[str]]:
        """Report layer, linguistic metadata and diagram only: the model already has the new name."""
        count = 0
        warnings: List[str] = []
        for path, text in self._tmdl_texts().items():
            doc = tmdl_scan.scan(text)
            edits = pbip_json.Edits(text)
            for node in doc.walk():
                if doc.language(node) == "json" and node.expr_span:
                    n, note = _linguistic_metadata(doc.expression(node), node.expr_span[0], edits, op)
                    count += n
                    if note:
                        warnings.append(f"{self.label(path)}: {note}")
            if len(edits):
                self.tx.write(path, edits.apply())
        if self.diagram_layout is not None and self.tx.exists(self.diagram_layout):
            count += self._diagram(op)
        rewriter = pbip_report.ReportRewriter(op, catalog)
        for path, text in self._report_texts().items():
            result = self._rewrite_report(rewriter, text, self.label(path))
            count += result.count
            warnings.extend(result.warnings)
            if result.text != text:
                self.tx.write(path, result.text)
        return count, warnings

    @staticmethod
    def _assert_renamed(op: RenameOp, catalog: ModelCatalog) -> None:
        """The staged model must now hold the new name and not the old one."""
        if op.kind == "table":
            ok = catalog.has_table(op.new) and (_eq(op.old, op.new) or not catalog.has_table(op.old))
        elif op.kind == "column":
            ok = catalog.has_column(op.table, op.new) and (
                _eq(op.old, op.new) or not catalog.has_column(op.table, op.old))
        else:
            ok = catalog.has_measure(op.table, op.new) and (
                _eq(op.old, op.new) or not catalog.has_measure(op.table, op.old))
        if not ok:
            raise RenameError(f"the declaration of {op.describe()} could not be rewritten")

    def _validate(self, expected: Set[Problem], repair: bool) -> List[Issue]:
        issues: List[Issue] = []
        for path in self.tx.paths:
            if path.lower().endswith(".json"):
                try:
                    json.loads(self.tx.read(path))
                except ValueError as e:
                    issues.append(Issue(path, 0, "INVALID_JSON",
                                        f"{self.label(path)} would not be valid JSON ({e})"))
        after, _ = self._problems()
        for problem, where in after.items():
            if problem in expected:
                continue
            kind, table, name = problem
            if repair and kind not in ("report table", "report field"):
                continue
            target = f"'{table}'[{name}]" if name else f"'{table}'"
            issues.append(Issue(re.sub(r":\d+$", "", where), _line_of(where), "BROKEN_REFERENCE",
                                f"{kind} {target} would be left unresolved ({where})"))
        return issues


def _read_bytes(path: str) -> Optional[bytes]:
    try:
        with open(path, "rb") as f:
            return f.read()
    except FileNotFoundError:
        return None


def _line_of(where: str) -> int:
    m = re.search(r":(\d+)$", where)
    return int(m.group(1)) if m else 0


def _dedupe(items: List[str]) -> List[str]:
    seen: Set[str] = set()
    out: List[str] = []
    for item in items:
        if item not in seen:
            seen.add(item)
            out.append(item)
    return out
