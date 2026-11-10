"""
Report-layer (PBIR) reference reading and rewriting.

A Power BI report refers to model objects through a small expression grammar that appears in
many places: visual projections, sort definitions, filters (visual, page and report level),
conditional formatting, bookmarks, and the mobile layout. The same grammar is used by the
enhanced format (one JSON file per visual) and by the legacy single report.json, where each
visual's config, query, filters and dataTransforms are JSON documents stored as strings.

    {"Column":  {"Expression": {"SourceRef": {"Entity": "Sales"}}, "Property": "Amount"}}
    {"Measure": {"Expression": {"SourceRef": {"Source": "s"}},     "Property": "Total"}}   + From alias
    {"Aggregation": {"Expression": {"Column": {...}}, "Function": 0}}
    {"HierarchyLevel": {"Expression": {"Hierarchy": {...}}, "Level": "Month"}}

The table name sits under Expression.SourceRef (directly, or as an alias declared by a From
clause at an enclosing scope), never beside Property, so a rename has to resolve it
structurally. This module walks the parsed JSON, decides per node whether it refers to the
object being renamed, and records token edits (see pbip_json) so that nothing else in the file
changes. Names that only label a reference (queryRef, selector metadata, legacy Select names)
are kept consistent with it, and field names used inside Deneb / Vega specs follow when the
field's display name changes.
"""
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple

import dax_tokens
import pbip_json
from pbip_json import Edits, JStr
from tmdl_scan import ModelCatalog

# Legacy report.json keeps these documents as JSON text inside a string.
NESTED_JSON_KEYS = frozenset({"config", "filters", "query", "dataTransforms", "parameters"})
_EXPR_KEYS = ("Column", "Measure", "Aggregation", "HierarchyLevel", "Hierarchy")
# Strings that repeat a projection's query name. A dict under one of the *_KEYED names is
# keyed by query names instead.
_QUERY_NAME_KEYS = ("queryRef", "Name", "queryName")
# Without an expression beside it, "Name" is too general a key to read as a query name (From
# aliases and variations use it too), so only these are followed on their own.
_LOOSE_QUERY_NAME_KEYS = ("queryRef", "queryName")
_DISPLAY_NAME_KEYS = ("nativeQueryRef", "NativeReferenceName")
_QUERY_KEYED_DICTS = frozenset({"columnProperties", "dataMap"})
_WRAPPED_QUERY_REF = re.compile(r"([A-Za-z][A-Za-z0-9]*)\((.*)\)\Z", re.DOTALL)
_JS_IDENTIFIER = re.compile(r"[A-Za-z_$][A-Za-z0-9_$]*\Z")
# Deneb replaces these characters with an underscore when it names dataset fields.
_DENEB_SPECIAL = re.compile(r'[\\".\[\]]')
# Vega / Vega-Lite keys whose string value (or list of strings) can only be field names.
_VEGA_FIELD_KEYS = frozenset({"field", "pivot", "impute", "regression", "loess", "density", "quantile",
                              "lookup", "key"})
_VEGA_FIELD_LIST_KEYS = frozenset({"groupby", "fields", "fold", "flatten", "partitionby"})
_STRUCTURAL_KEYS = frozenset({"$schema", "Entity", "Property", "Hierarchy", "Level", "Source", "queryRef",
                              "queryName", "nativeQueryRef", "NativeReferenceName", "metadata"})


@dataclass(frozen=True)
class RenameOp:
    """One rename. ``table`` is the owning table for a column or measure."""
    kind: str                      # 'table' | 'column' | 'measure'
    old: str
    new: str
    table: Optional[str] = None

    def describe(self) -> str:
        if self.kind == "table":
            return f"table '{self.old}'"
        return f"{self.kind} '{self.table}'[{self.old}]"


def _eq(a: Optional[str], b: Optional[str]) -> bool:
    return dax_tokens.names_equal(a, b)


# ---------------------------------------------------------------- file inventory

def report_files(report_folder: Optional[Path], enhanced: bool) -> List[Path]:
    """Every report file that can hold a field reference. Enhanced format: all JSON under
    definition/ (visuals, mobile layouts, pages, report, bookmarks, report extensions).
    Legacy format: the single report.json."""
    if report_folder is None:
        return []
    out: List[Path] = []
    definition = report_folder / "definition"
    if enhanced and definition.is_dir():
        out.extend(sorted(definition.rglob("*.json")))
    legacy = report_folder / "report.json"
    if legacy.is_file():
        out.append(legacy)
    return out


# ---------------------------------------------------------------- expression helpers

def _source(expr: Any, alias: Dict[str, str]) -> Tuple[Optional[str], bool]:
    """(table, is_report_extension) an Expression node points at. Follows a From alias and
    the Hierarchy / variation wrappers; (None, False) when it cannot be resolved here."""
    if not isinstance(expr, dict):
        return None, False
    sr = expr.get("SourceRef")
    if isinstance(sr, dict):
        ext = sr.get("Schema") == "extension"
        if isinstance(sr.get("Entity"), str):
            return str(sr["Entity"]), ext
        if isinstance(sr.get("Source"), str):
            return alias.get(str(sr["Source"])), ext
        return None, ext
    for wrapper in ("Hierarchy", "PropertyVariationSource"):
        inner = expr.get(wrapper)
        if isinstance(inner, dict):
            return _source(inner.get("Expression"), alias)
    return None, False


def _holder(obj: Dict[Any, Any]) -> Optional[Dict[Any, Any]]:
    """The dict that carries the field expression of a select-like item (a projection, a
    legacy Select entry, a dataTransforms select), or None."""
    if any(k in obj for k in _EXPR_KEYS):
        return obj
    for key in ("field", "expr"):
        inner = obj.get(key)
        if isinstance(inner, dict) and any(k in inner for k in _EXPR_KEYS):
            return inner
    return None


@dataclass
class _Field:
    kind: str                       # 'Column' | 'Measure' | 'Hierarchy'
    table: Optional[str]
    name: Optional[str]             # column / measure name (the variation column for a hierarchy)
    extension: bool = False
    aggregated: bool = False


def _primary(holder: Dict[Any, Any], alias: Dict[str, str]) -> Optional[_Field]:
    """What a field expression is about: the column or measure at its core."""
    for kind in ("Column", "Measure"):
        node = holder.get(kind)
        if isinstance(node, dict) and isinstance(node.get("Property"), str):
            table, ext = _source(node.get("Expression"), alias)
            return _Field(kind, table, str(node["Property"]), ext)
    agg = holder.get("Aggregation")
    if isinstance(agg, dict) and isinstance(agg.get("Expression"), dict):
        inner = _primary(agg["Expression"], alias)
        if inner is not None:
            inner.aggregated = True
        return inner
    level = holder.get("HierarchyLevel")
    hierarchy = holder.get("Hierarchy")
    if isinstance(level, dict) and isinstance(level.get("Expression"), dict):
        hierarchy = level["Expression"].get("Hierarchy")
    if isinstance(hierarchy, dict):
        table, ext = _source(hierarchy.get("Expression"), alias)
        return _Field("Hierarchy", table, _variation_column(hierarchy), ext)
    return None


def _variation_column(hierarchy: Any) -> Optional[str]:
    """The date column a generated date hierarchy belongs to, or None for a model hierarchy."""
    source = hierarchy.get("Expression") if isinstance(hierarchy, dict) else None
    variation = source.get("PropertyVariationSource") if isinstance(source, dict) else None
    if isinstance(variation, dict) and isinstance(variation.get("Property"), str):
        return str(variation["Property"])
    return None


# ---------------------------------------------------------------- reading references

def _nested_documents(obj: Dict[Any, Any]) -> Iterator[Tuple[str, Any]]:
    """(key, parsed document) for every legacy JSON-in-a-string value of ``obj``."""
    for key, value in obj.items():
        if key in NESTED_JSON_KEYS and isinstance(value, str) and value.lstrip()[:1] in ("{", "["):
            try:
                yield str(key), pbip_json.parse(value)
            except pbip_json.JsonSyntaxError:
                continue


def walk_references(obj: Any, refs: Set[Tuple[str, str]], tables: Optional[Set[str]] = None,
                    alias: Optional[Dict[str, str]] = None) -> None:
    """Collect (table, field) references from a parsed report document into ``refs`` (and
    every referenced table name into ``tables``). Columns, measures, hierarchies and hierarchy
    levels are attributed to the table their own expression resolves to; references to
    report-level (extension) measures are skipped because they are not model objects."""
    alias = dict(alias or {})
    if isinstance(obj, dict):
        frm = obj.get("From")
        if isinstance(frm, list):
            for src in frm:
                if isinstance(src, dict) and isinstance(src.get("Name"), str) and isinstance(
                        src.get("Entity"), str):
                    alias[str(src["Name"])] = str(src["Entity"])
                    if tables is not None:
                        tables.add(str(src["Entity"]))
        sr = obj.get("SourceRef")
        if tables is not None and isinstance(sr, dict) and isinstance(sr.get("Entity"), str):
            tables.add(str(sr["Entity"]))
        for kind, namekey in (("Column", "Property"), ("Measure", "Property"),
                              ("Hierarchy", "Hierarchy"), ("HierarchyLevel", "Level")):
            node = obj.get(kind)
            if not (isinstance(node, dict) and isinstance(node.get(namekey), str)):
                continue
            table, ext = _source(node.get("Expression", {}), alias)
            if not table or ext:
                continue
            name = str(node[namekey])
            # The date hierarchy Desktop generates for a date column ("variation") lives in
            # a hidden table; what such a reference depends on in the model is the column.
            hierarchy = node if kind == "Hierarchy" else node.get("Expression", {}).get("Hierarchy")
            variation = _variation_column(hierarchy)
            if variation is not None:
                name = variation
            refs.add((table, name))
        for _, nested in _nested_documents(obj):
            walk_references(nested, refs, tables)
        for value in obj.values():
            walk_references(value, refs, tables, alias)
    elif isinstance(obj, list):
        for item in obj:
            walk_references(item, refs, tables, alias)


def references_in_text(text: str) -> Tuple[Set[Tuple[str, str]], Set[str]]:
    """((table, field) references, table names) used by one report JSON file."""
    refs: Set[Tuple[str, str]] = set()
    tables: Set[str] = set()
    walk_references(pbip_json.parse(text), refs, tables)
    return refs, tables


# ---------------------------------------------------------------- rewriting

@dataclass
class RewriteResult:
    text: str
    count: int = 0
    warnings: List[str] = field(default_factory=list)


class ReportRewriter:
    """Applies one RenameOp to report JSON text. ``catalog`` is the model BEFORE the rename."""

    def __init__(self, op: RenameOp, catalog: ModelCatalog) -> None:
        self.op = op
        self.catalog = catalog

    # -- entry points

    def may_touch(self, text: str) -> bool:
        """Cheap pre-check: a file that never mentions the old name needs no parsing. Names
        with characters JSON may escape are always parsed."""
        old = self.op.old
        if any(c in '"\\/' or ord(c) > 126 or ord(c) < 32 for c in old):
            return True
        return old.casefold() in text.casefold()

    def rewrite(self, text: str, label: str) -> RewriteResult:
        result = RewriteResult(text=text)
        if not self.may_touch(text):
            return result
        doc = pbip_json.parse(text)
        edits = Edits(text)
        self._document(doc, edits, label, result, is_extensions=label.endswith("reportExtensions.json"))
        result.text = edits.apply()
        return result

    # -- one document (a file, or a JSON document nested in a string)

    def _document(self, doc: Any, edits: Edits, label: str, result: RewriteResult,
                  is_extensions: bool = False) -> None:
        self._walk(doc, edits, {}, label, result)
        if is_extensions:
            self._extensions(doc, edits, label, result)
        for container in self._visual_containers(doc):
            self._field_names_in_specs(container, edits, label, result)

    def _walk(self, node: Any, edits: Edits, alias: Dict[str, str], label: str,
              result: RewriteResult) -> None:
        op = self.op
        if isinstance(node, list):
            for item in node:
                self._walk(item, edits, alias, label, result)
            return
        if not isinstance(node, dict):
            return
        frm = node.get("From")
        if isinstance(frm, list):
            alias = dict(alias)
            for src in frm:
                if isinstance(src, dict) and isinstance(src.get("Name"), str) and isinstance(
                        src.get("Entity"), JStr):
                    alias[str(src["Name"])] = str(src["Entity"])
                    if op.kind == "table" and _eq(src["Entity"], op.old):
                        result.count += edits.set_string(src["Entity"], op.new)
        sr = node.get("SourceRef")
        if (op.kind == "table" and isinstance(sr, dict) and isinstance(sr.get("Entity"), JStr)
                and _eq(sr["Entity"], op.old)):
            result.count += edits.set_string(sr["Entity"], op.new)
        if op.kind != "table":
            self._field_node(node, edits, alias, label, result)
        has_expression = _holder(node) is not None
        if has_expression:
            self._select_names(node, edits, alias, result)
        for key, value in node.items():
            if isinstance(value, JStr):
                if key in NESTED_JSON_KEYS and value.lstrip()[:1] in ("{", "["):
                    self._nested_json(value, edits, f"{label} > {key}", result)
                elif key == "metadata" or (key in _LOOSE_QUERY_NAME_KEYS and not has_expression):
                    self._loose_query_name(value, edits, result)
            elif key == "metadata" and isinstance(value, list):
                for item in value:
                    if isinstance(item, JStr):
                        self._loose_query_name(item, edits, result)
            elif key in _QUERY_KEYED_DICTS and isinstance(value, dict):
                for k in value:
                    if isinstance(k, JStr):
                        self._loose_query_name(k, edits, result)
                self._walk(value, edits, alias, label, result)
            else:
                self._walk(value, edits, alias, label, result)

    def _field_node(self, node: Dict[Any, Any], edits: Edits, alias: Dict[str, str], label: str,
                    result: RewriteResult) -> None:
        """Rename Property on a Column / Measure / variation node that refers to the object."""
        op = self.op
        for kind in ("Column", "Measure", "PropertyVariationSource"):
            inner = node.get(kind)
            if not (isinstance(inner, dict) and isinstance(inner.get("Property"), JStr)
                    and _eq(inner["Property"], op.old)):
                continue
            table, ext = _source(inner.get("Expression"), alias)
            if ext:
                continue                      # a report-level measure of the same name
            if table is None:
                _warn(result, f"{label}: a {kind} reference to [{op.old}] has no resolvable table "
                              "and was left unchanged")
                continue
            if _eq(table, op.table):
                # (table, name) is unique across a table's columns and measures, so a node of
                # the other kind with this pair can only be a stale label for the same object.
                result.count += edits.set_string(inner["Property"], op.new)
            elif op.kind == "measure" and kind == "Measure" and not self.catalog.has_field(table, op.old):
                _warn(result, f"{label}: measure reference '{table}'[{op.old}] points at a table that "
                              f"does not hold the measure (it lives in '{op.table}'); left unchanged")

    def _matches(self, f: Optional[_Field]) -> bool:
        op = self.op
        if f is None or f.table is None:
            return False
        if op.kind == "table":
            return _eq(f.table, op.old)      # a report-level measure is still attached to the table
        return not f.extension and _eq(f.table, op.table) and _eq(f.name, op.old)

    def _select_names(self, node: Dict[Any, Any], edits: Edits, alias: Dict[str, str],
                      result: RewriteResult) -> None:
        """Keep the names that label a select item in step with its field expression."""
        f = _primary(_holder(node), alias)
        if not self._matches(f):
            return
        for key in _QUERY_NAME_KEYS:
            token = node.get(key)
            if isinstance(token, JStr):
                new = self._renamed_query_name(str(token), f.table)
                if new is not None:
                    result.count += edits.set_string(token, new)
        if self.op.kind != "table" and f.kind != "Hierarchy" and not f.aggregated:
            for key in _DISPLAY_NAME_KEYS:
                token = node.get(key)
                if isinstance(token, JStr) and str(token) == self.op.old:
                    result.count += edits.set_string(token, self.op.new)

    def _renamed_query_name(self, name: str, table: Optional[str]) -> Optional[str]:
        """New text for a query name known to belong to ``table``'s renamed object, or None
        when the name is not in one of the forms Power BI writes (it is then only a label and
        stays as it is)."""
        op = self.op
        prefix, core, suffix = "", name, ""
        m = _WRAPPED_QUERY_REF.match(name)
        if m:
            prefix, core, suffix = m.group(1) + "(", m.group(2), ")"
        if op.kind == "table":
            head = op.old + "."
            if core[:len(head)].casefold() == head.casefold():
                return prefix + op.new + core[len(op.old):] + suffix
            return None
        head = f"{table}.{op.old}"
        if core[:len(head)].casefold() != head.casefold():
            return None
        rest = core[len(head):]
        if rest and not rest.startswith("."):
            return None
        return f"{prefix}{core[:len(table)]}.{op.new}{rest}{suffix}"

    def _loose_query_name(self, token: JStr, edits: Edits, result: RewriteResult) -> None:
        """A query name with no expression beside it (selector metadata, legacy projection
        lists, dictionaries keyed by query name). The owning table is worked out from the
        model so that 'Sales.EU.Amount' is not mistaken for a field of table 'Sales'."""
        name = str(token)
        m = _WRAPPED_QUERY_REF.match(name)
        core = m.group(2) if m else name
        owner, rest = self._owner_of(core)
        if owner is None:
            return
        op = self.op
        if op.kind == "table":
            if not _eq(owner, op.old):
                return
        else:
            if not _eq(owner, op.table):
                return
            if not (_eq(rest, op.old) or (rest[:len(op.old) + 1].casefold() == (op.old + ".").casefold()
                                          and not self.catalog.has_member(owner, rest))):
                return
        new = self._renamed_query_name(name, owner)
        if new is not None:
            result.count += edits.set_string(token, new)

    def _owner_of(self, core: str) -> Tuple[Optional[str], str]:
        """(table, remainder) for a 'Table.member' query name: the longest table name that
        prefixes it and owns the remainder, else the longest table name that prefixes it."""
        best: Optional[str] = None
        best_owns = False
        folded = core.casefold()
        names = [t.name for t in self.catalog.tables.values()]
        if self.op.kind == "table" and not self.catalog.has_table(self.op.old):
            names.append(self.op.old)        # repairing a report after a model-only rename
        for table in names:
            head = table.casefold() + "."
            if not folded.startswith(head):
                continue
            rest = core[len(head):]
            owns = self.catalog.has_member(table, rest) or self.catalog.has_member(
                table, rest.split(".", 1)[0])
            if best is None or (owns, len(table)) > (best_owns, len(best)):
                best, best_owns = table, owns
        if best is None:
            return None, ""
        return best, core[len(best) + 1:]

    # -- JSON documents stored inside strings

    def _nested_json(self, token: JStr, edits: Edits, label: str, result: RewriteResult) -> None:
        inner_text = str(token)
        if not self.may_touch(inner_text):
            return
        try:
            inner = pbip_json.parse(inner_text)
        except pbip_json.JsonSyntaxError:
            _warn(result, f"{label}: embedded JSON could not be parsed and still mentions "
                          f"'{self.op.old}'; it was left unchanged")
            return
        inner_edits = Edits(inner_text)
        self._document(inner, inner_edits, label, result)
        if len(inner_edits):
            edits.set_string(token, inner_edits.apply())

    # -- report-level measures (reportExtensions.json)

    def _extensions(self, doc: Any, edits: Edits, label: str, result: RewriteResult) -> None:
        op = self.op
        if not isinstance(doc, dict) or not isinstance(doc.get("entities"), list):
            return
        for entity in doc["entities"]:
            if not isinstance(entity, dict):
                continue
            home = str(entity["name"]) if isinstance(entity.get("name"), str) else None
            if op.kind == "table" and isinstance(entity.get("name"), JStr) and _eq(home, op.old):
                result.count += edits.set_string(entity["name"], op.new)
            for measure in entity.get("measures") or []:
                if not isinstance(measure, dict):
                    continue
                expr = measure.get("expression")
                if isinstance(expr, JStr):
                    new_expr, n, notes = rename_in_dax(str(expr), op, self.catalog, home)
                    if n:
                        result.count += n
                        edits.set_string(expr, new_expr)
                    for note in notes:
                        _warn(result, f"{label}: report measure '{measure.get('name')}': {note}")
                self._extension_references(measure.get("references"), edits, result)

    def _extension_references(self, node: Any, edits: Edits, result: RewriteResult) -> None:
        op = self.op
        if isinstance(node, list):
            for item in node:
                self._extension_references(item, edits, result)
        elif isinstance(node, dict):
            ent, name = node.get("entity"), node.get("name")
            if isinstance(ent, JStr) and isinstance(name, JStr) and node.get("schema") != "extension":
                if op.kind == "table" and _eq(ent, op.old):
                    result.count += edits.set_string(ent, op.new)
                elif op.kind != "table" and _eq(ent, op.table) and _eq(name, op.old):
                    result.count += edits.set_string(name, op.new)
            for value in node.values():
                self._extension_references(value, edits, result)

    # -- field names inside visual specs (Deneb / Vega, visual calculations)

    @staticmethod
    def _visual_containers(doc: Any) -> Iterator[Dict[Any, Any]]:
        """Dicts that describe one visual: they hold its query and its formatting objects
        (the 'visual' node of visual.json, 'singleVisual' of a legacy config)."""
        stack = [doc]
        while stack:
            node = stack.pop()
            if isinstance(node, dict):
                if "visualType" in node and any(
                        k in node for k in ("query", "prototypeQuery", "projections")):
                    yield node
                    continue
                stack.extend(v for v in node.values() if isinstance(v, (dict, list)))
            elif isinstance(node, list):
                stack.extend(v for v in node if isinstance(v, (dict, list)))

    def _display_renames(self, container: Dict[Any, Any], label: str,
                         result: RewriteResult) -> Dict[str, str]:
        """{old display name: new display name} for the fields of one visual whose name, as
        the visual sees it, changes with this rename. A field shown under a custom display
        name keeps that name, so nothing inside the visual needs to follow."""
        op = self.op
        if op.kind == "table":
            return {}
        overrides: Dict[str, str] = {}
        props = container.get("columnProperties")
        if isinstance(props, dict):
            for qref, cfg in props.items():
                if isinstance(cfg, dict) and isinstance(cfg.get("displayName"), str):
                    overrides[str(qref)] = str(cfg["displayName"])
        renames: Dict[str, str] = {}
        stack: List[Tuple[Any, Dict[str, str]]] = [
            (container.get(k), {}) for k in ("query", "prototypeQuery") if k in container]
        while stack:
            node, alias = stack.pop()
            if isinstance(node, list):
                stack.extend((item, alias) for item in node)
                continue
            if not isinstance(node, dict):
                continue
            frm = node.get("From")
            if isinstance(frm, list):
                alias = dict(alias)
                for src in frm:
                    if isinstance(src, dict) and isinstance(src.get("Name"), str) and isinstance(
                            src.get("Entity"), str):
                        alias[str(src["Name"])] = str(src["Entity"])
            holder = _holder(node)
            # Only real select items count (they carry a query name); a sort or filter entry
            # repeats the field expression without being a column of the visual's dataset.
            if holder is not None and any(isinstance(node.get(k), str) for k in ("queryRef", "Name")):
                f = _primary(holder, alias)
                if self._matches(f):
                    custom = node.get("displayName")
                    if not isinstance(custom, str):
                        custom = overrides.get(str(node.get("queryRef") or node.get("Name") or ""))
                    shown = next((str(node[k]) for k in _DISPLAY_NAME_KEYS if isinstance(node.get(k), str)),
                                 op.old)
                    if isinstance(custom, str):
                        pass
                    elif f.aggregated or shown != op.old:
                        _warn(result, f"{label}: the renamed field is shown as '{shown}' here; if a "
                                      "visual spec refers to it by that name, check it by hand")
                    else:
                        renames[op.old] = op.new
            stack.extend((v, alias) for v in node.values() if isinstance(v, (dict, list)))
        return renames

    def _field_names_in_specs(self, container: Dict[Any, Any], edits: Edits, label: str,
                              result: RewriteResult) -> None:
        objects = container.get("objects")
        has_spec = isinstance(objects, dict) and isinstance(objects.get("vega"), list)
        calcs = list(_visual_calculations(container))
        if not has_spec and not calcs:
            return
        renames = self._display_renames(container, label, result)
        if not renames:
            return
        for calc in calcs:
            expr = calc.get("Expression")
            if isinstance(expr, JStr):
                new_expr = str(expr)
                for old, new in renames.items():
                    new_expr, n = dax_tokens.rename_measure(new_expr, old, new)
                    result.count += n
                edits.set_string(expr, new_expr)
        if not has_spec:
            return
        fields = {deneb_field_name(o): deneb_field_name(n) for o, n in renames.items()}
        if any(deneb_field_name(n) != n for n in list(renames) + list(renames.values())):
            _warn(result, f"{label}: the field name contains characters Deneb replaces with '_'; "
                          "the spec was updated on that assumption, check it renders")
        for group, keyed in (("vega", False), ("stateManagement", True)):
            for entry in objects.get(group) or []:
                props = entry.get("properties") if isinstance(entry, dict) else None
                if not isinstance(props, dict):
                    continue
                for prop_name, prop in props.items():
                    token = _literal_token(prop)
                    if token is None:
                        continue
                    inner_text = pbip_json.literal_unwrap(str(token))
                    if inner_text is None or inner_text.lstrip()[:1] not in ("{", "["):
                        continue
                    if not any(o in inner_text for o in fields):
                        continue
                    try:
                        inner = pbip_json.parse(inner_text)
                    except pbip_json.JsonSyntaxError:
                        _warn(result, f"{label}: {group}.{prop_name} is not parseable JSON and still "
                                      f"mentions '{self.op.old}'; it was left unchanged")
                        continue
                    inner_edits = Edits(inner_text)
                    if keyed:
                        n = _rename_keys(inner, inner_edits, fields)
                    else:
                        defined = _defines_field(inner, set(fields))
                        if defined:
                            _warn(result, f"{label}: {group}.{prop_name} creates its own field named "
                                          f"'{defined}'; the spec was left unchanged, check it by hand")
                            continue
                        n = _rewrite_vega(inner, inner_edits, fields)
                    if n:
                        result.count += n
                        edits.set_string(token, pbip_json.literal_wrap(inner_edits.apply()))


def _warn(result: RewriteResult, message: str) -> None:
    if message not in result.warnings:
        result.warnings.append(message)


def _literal_token(prop: Any) -> Optional[JStr]:
    """The Value token of {"expr": {"Literal": {"Value": "..."}}}."""
    if isinstance(prop, dict) and isinstance(prop.get("expr"), dict):
        lit = prop["expr"].get("Literal")
        if isinstance(lit, dict) and isinstance(lit.get("Value"), JStr):
            return lit["Value"]
    return None


def _visual_calculations(container: Dict[Any, Any]) -> Iterator[Dict[Any, Any]]:
    stack: List[Any] = [container.get("query"), container.get("prototypeQuery")]
    while stack:
        node = stack.pop()
        if isinstance(node, dict):
            calc = node.get("NativeVisualCalculation")
            if isinstance(calc, dict):
                yield calc
            stack.extend(node.values())
        elif isinstance(node, list):
            stack.extend(node)


# ---------------------------------------------------------------- Deneb / Vega specs

def deneb_field_name(name: str) -> str:
    """The dataset field name Deneb gives a Power BI field."""
    return _DENEB_SPECIAL.sub("_", name)


def _rename_keys(doc: Any, edits: Edits, fields: Dict[str, str]) -> int:
    """Rename top-level keys of a field-configuration map."""
    if not isinstance(doc, dict):
        return 0
    return sum(1 for old, new in fields.items() if edits.rename_key(doc, old, new))


def _defines_field(doc: Any, names: Set[str]) -> Optional[str]:
    """A name from ``names`` that the spec itself produces with an "as" output. Such a spec
    may be referring to its own derived field, so it is not safe to rewrite."""
    stack = [doc]
    while stack:
        node = stack.pop()
        if isinstance(node, dict):
            out = node.get("as")
            for candidate in (out if isinstance(out, list) else [out]):
                if isinstance(candidate, str) and candidate in names:
                    return candidate
            stack.extend(node.values())
        elif isinstance(node, list):
            stack.extend(node)
    return None


def _js_string(name: str, quote: str) -> str:
    return quote + name.replace("\\", "\\\\").replace(quote, "\\" + quote) + quote


def rename_datum_references(expr: str, old: str, new: str) -> Tuple[str, int]:
    """Rename datum.old, datum['old'] and datum["old"] in a Vega expression string."""
    count = 0

    def replacement(quote: str):
        def repl(_m: "re.Match") -> str:
            nonlocal count
            count += 1
            if quote == "." and _JS_IDENTIFIER.match(new):
                return "datum." + new
            return "datum[" + _js_string(new, "'" if quote == "." else quote) + "]"
        return repl

    if "datum" not in expr:
        return expr, 0
    out = expr
    if _JS_IDENTIFIER.match(old):
        out = re.sub(r"(?<![\w$])datum\." + re.escape(old) + r"(?![\w$])", replacement("."), out)
    for quote in ("'", '"'):
        pattern = r"(?<![\w$])datum\[\s*" + re.escape(_js_string(old, quote)) + r"\s*\]"
        out = re.sub(pattern, replacement(quote), out)
    return out, count


def _rewrite_vega(doc: Any, edits: Edits, fields: Dict[str, str]) -> int:
    """Rename dataset field names in a parsed Vega / Vega-Lite spec. Returns edits made."""
    count = 0

    def field_token(token: Any) -> None:
        nonlocal count
        if isinstance(token, JStr) and str(token) in fields:
            count += edits.set_string(token, fields[str(token)])

    def expression_token(token: JStr) -> None:
        nonlocal count
        text = str(token)
        total = 0
        for old, new in fields.items():
            text, n = rename_datum_references(text, old, new)
            total += n
        if total:
            count += total
            edits.set_string(token, text)

    def strings(node: Any) -> None:
        if isinstance(node, JStr):
            field_token(node)
        elif isinstance(node, list):
            for item in node:
                strings(item)
        elif isinstance(node, dict):
            for item in node.values():
                strings(item)

    def walk(node: Any) -> None:
        if isinstance(node, list):
            for item in node:
                if isinstance(item, JStr):
                    expression_token(item)
                else:
                    walk(item)
            return
        if not isinstance(node, dict):
            return
        for key, value in node.items():
            if key == "as":
                continue
            if key in _VEGA_FIELD_KEYS and isinstance(value, JStr):
                field_token(value)
            elif key in _VEGA_FIELD_LIST_KEYS and isinstance(value, (list, JStr)):
                strings(value)
            elif key == "repeat" and isinstance(value, (list, dict)):
                strings(value)
            elif isinstance(value, JStr):
                expression_token(value)
            else:
                walk(value)

    walk(doc)
    return count


# ---------------------------------------------------------------- DAX shared by report and model

def rename_in_dax(expr: str, op: RenameOp, catalog: ModelCatalog, home_table: Optional[str],
                  row_context: bool = False) -> Tuple[str, int, List[str]]:
    """Apply a rename to one DAX expression. Returns (new_expr, replacements, notes); a note
    describes a reference that could not be decided and was left for a person to check.

    ``home_table`` is the table the expression belongs to. ``row_context`` says it is evaluated
    row by row over that table (a calculated column or a row-level security filter), where an
    unqualified [Name] normally means a column of that table.
    """
    notes: List[str] = []
    if op.kind == "table":
        new_expr, n = dax_tokens.rename_table(expr, op.old, op.new)
        return new_expr, n, notes
    if op.kind == "measure":
        tokens = dax_tokens.tokenize(expr)
        bare = sum(1 for r in dax_tokens.iter_references(tokens) if r.kind == "bare" and _eq(r.name, op.old))
        if bare and row_context and catalog.has_column(home_table, op.old):
            # [old] here is far more likely this table's own column than the measure.
            repl = {r.index: dax_tokens.bracket(op.new) for r in dax_tokens.iter_references(tokens)
                    if r.kind == "column" and _eq(r.name, op.old) and _eq(r.table, op.table)
                    and not catalog.has_column(r.table, r.name)}
            notes.append(f"{bare} unqualified [{op.old}] reference(s) left unchanged because "
                         f"'{home_table}' has a column of that name; rename them by hand if they "
                         "mean the measure")
            if not repl:
                return expr, 0, notes
            return "".join(repl.get(i, t.text) for i, t in enumerate(tokens)), len(repl), notes
        new_expr, n = dax_tokens.rename_measure(expr, op.old, op.new, is_column=catalog.has_column,
                                                measure_table=op.table)
        if bare and catalog.column_tables(op.old):
            owners = ", ".join(f"'{t}'" for t in catalog.column_tables(op.old))
            notes.append(f"{bare} unqualified [{op.old}] reference(s) were renamed as the measure, "
                         f"but a column of that name also exists in {owners}; check them")
        return new_expr, n, notes
    # column: an unqualified [old] is only this column when nothing else can claim the name
    other_measure = bool(catalog.measure_tables(op.old))
    twins = [t for t in catalog.column_tables(op.old) if not _eq(t, op.table)]
    tokens = dax_tokens.tokenize(expr)
    redefined = any(t.kind == dax_tokens.STRING and _eq(t.text[1:-1].replace('""', '"'), op.old)
                    for t in tokens)
    used_tables = {r.table.casefold() for r in dax_tokens.iter_references(tokens) if r.table}
    twin_in_scope = any(t.casefold() in used_tables for t in twins)
    safe = (not other_measure and not redefined
            and (not twins or (_eq(home_table, op.table) and not twin_in_scope)))
    new_expr, n, skipped = dax_tokens.rename_column(expr, op.table, op.old, op.new, unqualified=safe)
    if skipped:
        why = ("a measure has the same name" if other_measure
               else "the expression also defines a column of that name" if redefined
               else "another table has a column of the same name")
        notes.append(f"{skipped} unqualified [{op.old}] reference(s) left unchanged because {why}; "
                     "rename them by hand if they mean this column")
    return new_expr, n, notes


# ---------------------------------------------------------------- leftover mentions

def leftover_mentions(text: str, old: str, new: str) -> List[Tuple[str, str]]:
    """[(json path, snippet)] for strings that still contain ``old`` as a whole word after a
    rename, looking inside nested JSON and expression literals too. These are free text
    (titles, labels, hand-written specs): possibly references, possibly not."""
    if old not in text:
        return []
    word = re.compile(r"(?<![\w])" + re.escape(old) + r"(?![\w])")
    hits: List[Tuple[str, str]] = []

    def leaf(path: str, value: str) -> None:
        stripped = value.replace(new, "") if new and old in new else value
        m = word.search(stripped)
        if m:
            start = max(0, m.start() - 30)
            hits.append((path, stripped[start:m.end() + 30].replace("\n", " ")))

    def visit(node: Any, path: str) -> None:
        if isinstance(node, dict):
            for key, value in node.items():
                # Names of model objects and query names are handled structurally; a word they
                # share with the old name ("Store" in the measure "Store CS") is not a leftover.
                if key in _STRUCTURAL_KEYS and isinstance(value, str):
                    continue
                visit(value, f"{path}.{key}" if path else str(key))
        elif isinstance(node, list):
            for i, item in enumerate(node):
                visit(item, f"{path}[{i}]")
        elif isinstance(node, str):
            if old not in node:
                return
            inner = pbip_json.literal_unwrap(node)
            candidate = inner if inner is not None else node
            if candidate.lstrip()[:1] in ("{", "["):
                try:
                    visit(pbip_json.parse(candidate), path)
                    return
                except pbip_json.JsonSyntaxError:
                    pass
            leaf(path, candidate)

    try:
        visit(pbip_json.parse(text), "")
    except pbip_json.JsonSyntaxError:
        return []
    return hits
