"""
Read-only audit of a PBIR report definition (the ``definition`` folder of a .Report).

Power BI Desktop reports a broken project with an error that seldom names the cause, and a
report can be perfectly valid and still be hard to use. This module reads the JSON files and
returns findings in three groups:

  structure   what stops a report opening or silently drops a visual: unparseable JSON, a
              byte order mark, two objects that declare the same ``name`` (and, as a note,
              folders named differently from the name inside), pages.json out of step with
              the pages, a custom visual that is used but not registered, a visual outside
              its page, a property at the wrong level, mixed schema versions, a field that
              is not in the model (when ``model_fields`` is given).
  design      what makes a page harder to read or to navigate: data visuals that overlap,
              crowded pages, missing alt text, titles switched off, tiny elements, page
              sizes that differ, hidden pages nothing leads to, the same field sliced on
              several pages without a sync group, buttons and bookmarks that point at
              something that no longer exists, colours written as literals instead of
              theme colours.
  governance  facts a tenant administrator may want to know, reported as information and
              never as errors: private custom visuals, visuals from the public store (their
              certification cannot be read from the files), and HTML visuals, which render
              markup supplied by a measure.

Nothing is written, and nothing is opened in Power BI: every finding comes from the files
as they are on disk. Severity is "error" (Desktop is likely to reject or mis-render it),
"warning" (worth fixing) or "info".

Entry point:
    audit_report(report_definition_dir, *, model_fields=None, ...) ->
        {"summary": {...}, "findings": [{code, severity, page, visual, message, recommendation}]}
"""
import json
import os
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Mapping, Optional, Set, Tuple, Union

PathLike = Union[str, os.PathLike]
_BOM = b"\xef\xbb\xbf"
_SEVERITY_ORDER = {"error": 0, "warning": 1, "info": 2}

# Keys that belong inside "visual" and keys that belong beside it, on the container.
_VISUAL_LEVEL = ("visualType", "query", "objects", "visualContainerObjects", "drillFilterOtherVisuals",
                 "syncGroup", "expansionStates")
_CONTAINER_LEVEL = ("position", "parentGroupName", "isHidden", "filterConfig")
# Types that draw decoration or navigation rather than data.
_DECORATION = frozenset({"shape", "basicShape", "image", "textbox", "actionButton", "pageNavigator",
                         "bookmarkNavigator"})
_CUSTOM_ID = re.compile(r"[A-Za-z][A-Za-z0-9_]*?[0-9A-F]{12,}")
_SCHEMA = re.compile(r"/([A-Za-z]+)/(\d+(?:\.\d+)*)/schema\.json$")
_HEX_COLOUR = re.compile(r"#(?:[0-9A-Fa-f]{3,4}|[0-9A-Fa-f]{6}|[0-9A-Fa-f]{8})")

_RECOMMEND = {
    "STRUCT_JSON": "Fix the JSON syntax; Desktop cannot load a report with a file it cannot parse.",
    "STRUCT_BOM": "Save the file as UTF-8 without a byte order mark.",
    "STRUCT_FILE": "Restore the missing file or delete the leftover folder.",
    "STRUCT_NAME": "Give every page and visual its own 'name'; keep folder and name the same where scripts "
                   "rely on it.",
    "STRUCT_PAGES": "Bring pages.json in step with the pages that exist.",
    "STRUCT_CUSTOM_VISUAL": "Register the visual: list it in report.json 'publicCustomVisuals', or add its "
                            "package under the report's CustomVisuals folder.",
    "STRUCT_POSITION": "Give the visual a position and a size inside the page.",
    "STRUCT_MISPLACED": "Move the property to the level it belongs to.",
    "STRUCT_SCHEMA": "Keep generated files on one schema version, or open and save the report in "
                     "Desktop so it rewrites them.",
    "STRUCT_GROUP": "Point 'parentGroupName' at a group on the same page, or remove it.",
    "STRUCT_FIELD": "Fix or remove the reference; the field is not in the model.",
    "DESIGN_OVERLAP": "Move or resize one of them, or group them if the overlap is intended.",
    "DESIGN_CROWDED": "Split the page, or move detail into a drill-through or tooltip page.",
    "DESIGN_ALT_TEXT": "Write alt text that says what the visual shows (Format > General > Alt text).",
    "DESIGN_TITLE": "Turn the title on, or make sure a text box next to the visual names it.",
    "DESIGN_TINY": "Enlarge it; small targets are hard to hit and hard to read.",
    "DESIGN_PAGE_SIZE": "Use one page size, unless the difference is deliberate.",
    "DESIGN_HIDDEN_PAGE": "Add a button or bookmark that opens the page, or delete it.",
    "DESIGN_SYNC": "Sync the slicers (View > Sync slicers) so a choice carries across pages.",
    "DESIGN_BROKEN_LINK": "Point it at something that exists, or remove the action.",
    "DESIGN_COLOURS": "Prefer theme colours, so one theme change restyles the report.",
    "GOV_PRIVATE_VISUAL": "Tell the tenant administrator: an organisation can block visuals that are "
                          "not from the store or not certified.",
    "GOV_PUBLIC_VISUAL": "Check each visual's certification in the store if your tenant only allows "
                         "certified visuals.",
    "GOV_HTML_VISUAL": "Review who can edit the measures behind it: their text is rendered as markup, "
                       "and in visuals that run scripts, as code.",
}


# ---------------------------------------------------------------- reading

class _Audit:
    def __init__(self) -> None:
        self.findings: List[Dict[str, Any]] = []

    def add(self, code: str, severity: str, message: str, page: Optional[str] = None,
            visual: Optional[str] = None, **extra: Any) -> None:
        item = {"code": code, "severity": severity, "page": page, "visual": visual, "message": message,
                "recommendation": _RECOMMEND.get(code, "")}
        item.update(extra)
        self.findings.append(item)

    def load(self, path: Path, page: Optional[str] = None, visual: Optional[str] = None) -> Optional[Any]:
        """Parsed JSON of ``path``, or None after recording why it could not be read."""
        try:
            raw = path.read_bytes()
        except OSError as e:
            self.add("STRUCT_FILE", "error", f"{path.name} cannot be read: {e.strerror or e}.", page, visual)
            return None
        if raw.startswith(_BOM):
            self.add("STRUCT_BOM", "warning", f"{path.name} starts with a byte order mark.", page, visual)
            raw = raw[len(_BOM):]
        try:
            return json.loads(raw.decode("utf-8"))
        except (ValueError, UnicodeDecodeError) as e:
            self.add("STRUCT_JSON", "error", f"{path.name} is not valid JSON: {e}.", page, visual)
            return None


def _locate(path: PathLike) -> Tuple[Path, Path]:
    """(report folder, definition folder) for a .Report folder or its definition folder."""
    root = Path(path)
    if (root / "definition").is_dir():
        return root, root / "definition"
    if (root / "pages").is_dir() or (root / "report.json").is_file():
        return root.parent, root
    raise ValueError(f"'{path}' is not a PBIR report definition: it has no 'definition' folder, no "
                     "'pages' folder and no report.json.")


# The files are read as they are, and a hand-edited or generated file can hold any JSON
# value anywhere. These three accessors turn "the wrong type" into "absent", so a check
# reports what it can and never stops the audit.

def _dict(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _list(value: Any) -> List[Any]:
    return value if isinstance(value, list) else []


def _text(value: Any) -> Optional[str]:
    return value if isinstance(value, str) else None


def _number(value: Any, default: float = 0.0) -> float:
    return float(value) if isinstance(value, (int, float)) and not isinstance(value, bool) else default


def _literal(node: Any) -> Any:
    """The Python value of a PBIR literal expression ({"expr": {"Literal": {"Value": ...}}}),
    or None when the node is anything else (a measure, a theme colour, a missing property)."""
    if isinstance(node, dict) and "expr" in node:
        node = node["expr"]
    value = _dict(_dict(node).get("Literal")).get("Value")
    if not isinstance(value, str):
        return None
    if len(value) >= 2 and value[0] == value[-1] == "'":
        return value[1:-1].replace("''", "'")
    if value in ("true", "false"):
        return value == "true"
    number = re.fullmatch(r"(-?[0-9]+(?:[.][0-9]+)?)[DLM]?", value)
    return float(number.group(1)) if number else value


def _properties(objects: Any, name: str) -> Iterator[Dict[str, Any]]:
    """The property bags of object ``name`` in an objects / visualContainerObjects block."""
    for item in _list(_dict(objects).get(name)):
        bag = _dict(item).get("properties")
        if isinstance(bag, dict):
            yield bag


def _first(objects: Any, name: str, prop: str) -> Any:
    for bag in _properties(objects, name):
        if prop in bag:
            return _literal(bag[prop])
    return None


def _walk(node: Any) -> Iterator[Dict[str, Any]]:
    """Every dict in a JSON tree."""
    if isinstance(node, dict):
        yield node
        for value in node.values():
            yield from _walk(value)
    elif isinstance(node, list):
        for value in node:
            yield from _walk(value)


def _field_refs(node: Any) -> Iterator[Tuple[str, str, str]]:
    """(kind, table, field) for every column and measure reference under ``node``."""
    for item in _walk(node):
        for kind in ("Column", "Measure"):
            ref = _dict(item.get(kind))
            entity = _dict(_dict(ref.get("Expression")).get("SourceRef")).get("Entity")
            if isinstance(entity, str) and isinstance(ref.get("Property"), str):
                yield kind.lower(), entity, ref["Property"]


def _projections(visual: Mapping[str, Any]) -> List[Tuple[str, str, str]]:
    out: List[Tuple[str, str, str]] = []
    for role in _dict(_dict(visual.get("query")).get("queryState")).values():
        for projection in _list(_dict(role).get("projections")):
            out.extend(_field_refs(projection))
    return out


def _schema_version(doc: Any) -> Tuple[Optional[str], Optional[str]]:
    found = _SCHEMA.search(_text(_dict(doc).get("$schema")) or "")
    return (found.group(1), found.group(2)) if found else (None, None)


def _page_size(doc: Mapping[str, Any]) -> Tuple[float, float]:
    """Width and height of a page; Power BI's default canvas when the file does not say."""
    return _number(doc.get("width"), 1280.0), _number(doc.get("height"), 720.0)


class _Visual:
    """One visual.json, with the facts the checks need."""

    def __init__(self, folder: str, doc: Dict[str, Any]):
        self.folder = folder
        self.doc = doc
        self.name = doc.get("name") if isinstance(doc.get("name"), str) else folder
        self.is_group = isinstance(doc.get("visualGroup"), dict)
        self.visual: Dict[str, Any] = doc.get("visual") if isinstance(doc.get("visual"), dict) else {}
        self.type: Optional[str] = _text(self.visual.get("visualType"))
        self.parent: Optional[str] = _text(doc.get("parentGroupName"))
        self.hidden = doc.get("isHidden") is True
        pos = doc.get("position") if isinstance(doc.get("position"), dict) else None
        self.has_position = pos is not None
        self.x, self.y = _number((pos or {}).get("x")), _number((pos or {}).get("y"))
        self.w, self.h = _number((pos or {}).get("width")), _number((pos or {}).get("height"))
        self.fields = _projections(self.visual)
        kind = (self.type or "").lower()
        self.is_slicer = "slicer" in kind or "datepicker" in kind
        self.is_html = "html" in kind
        self.is_data = bool(self.fields) and not self.is_slicer and self.type not in _DECORATION
        self.container = self.visual.get("visualContainerObjects")

    def label(self) -> str:
        title = _first(self.container, "title", "text")
        named = f", '{title}'" if isinstance(title, str) and title else ""
        return f"{self.name} ({self.type or 'group'}{named})"


def _model_index(model_fields: Optional[Mapping[str, Any]]) -> Optional[Dict[str, Set[str]]]:
    """{table (case-folded): {field (case-folded)}} from either accepted shape."""
    if model_fields is None:
        return None
    index: Dict[str, Set[str]] = {}
    for table, fields in model_fields.items():
        names: Iterable[Any]
        if isinstance(fields, Mapping):
            names = [n for group in fields.values() for n in (group or [])]
        else:
            names = fields or []
        index[str(table).casefold()] = {str(n).casefold() for n in names}
    return index


# ---------------------------------------------------------------- the audit

def audit_report(report_definition_dir: PathLike, *, model_fields: Optional[Mapping[str, Any]] = None,
                 max_data_visuals_per_page: int = 12, max_visuals_per_page: int = 40,
                 min_element_size: int = 12, overlap_ratio: float = 0.25) -> Dict[str, Any]:
    """Audit a PBIR report definition without changing it.

    report_definition_dir: the .Report folder or its ``definition`` folder.
    model_fields: optional {table: [column and measure names]} (or {table: {"columns": [...],
        "measures": [...]}}). When given, every field a visual uses is checked against it.
    max_data_visuals_per_page / max_visuals_per_page: crowding thresholds (visible visuals).
    min_element_size: a visible element narrower or shorter than this many pixels is "tiny".
    overlap_ratio: two data visuals overlap when the shared area is at least this share of
        the smaller one.

    Returns {"summary": {...}, "findings": [...]}, findings ordered by severity, then page.
    Raises ValueError when the folder is not a report definition at all.
    """
    report_dir, definition = _locate(report_definition_dir)
    audit = _Audit()
    index = _model_index(model_fields)

    report = audit.load(definition / "report.json") if (definition / "report.json").is_file() else None
    if not (definition / "report.json").is_file():
        audit.add("STRUCT_FILE", "error", "report.json is missing from the definition folder.")
    if not (definition / "version.json").is_file():
        audit.add("STRUCT_FILE", "warning", "version.json is missing from the definition folder.")
    report = report if isinstance(report, dict) else {}
    public = [v for v in _list(report.get("publicCustomVisuals")) if isinstance(v, str)]
    private_dir = report_dir / "CustomVisuals"
    private = sorted(p.name for p in private_dir.iterdir() if p.is_dir()) if private_dir.is_dir() else []

    pages = _read_pages(audit, definition)
    _check_pages_json(audit, definition, pages)
    bookmarks, bookmark_groups = _read_bookmarks(audit, definition)

    types: Counter = Counter()
    versions: Dict[str, Counter] = defaultdict(Counter)
    colours: Counter = Counter()
    colours_by_page: Counter = Counter()
    theme_refs = 0
    nav_targets: Set[str] = set()
    slicers: Dict[Tuple[str, str], List[Tuple[str, _Visual]]] = defaultdict(list)
    for kind, version in [_schema_version(report)] + [_schema_version(p["doc"]) for p in pages]:
        if kind:
            versions[kind][version] += 1

    for page in pages:
        name, visuals = page["name"], page["visuals"]
        by_name = {v.name: v for v in visuals}
        groups = {v.name for v in visuals if v.is_group}
        hidden = _hidden_names(visuals, by_name)
        width, height = _page_size(page["doc"])
        for v in visuals:
            types[v.type or "(group)"] += 1
            kind, version = _schema_version(v.doc)
            if kind:
                versions[kind][version] += 1
            else:
                audit.add("STRUCT_SCHEMA", "warning", "The file has no recognisable $schema.", name, v.name)
            _check_container(audit, name, v, groups, width, height)
            if index is not None:
                _check_fields(audit, name, v, index)
            _check_links(audit, name, v, {p["name"] for p in pages}, bookmarks, bookmark_groups, nav_targets)
            found = [c.upper() for c in _hard_coded_colours(v.doc)]
            colours.update(found)
            colours_by_page[name] += len(found)
            theme_refs += sum(1 for d in _walk(v.doc) if "ThemeDataColor" in d)
            # Hidden slicers count too: filter pop-ups are hidden until a button shows them.
            if v.is_slicer and v.fields:
                slicers[(v.fields[0][1], v.fields[0][2])].append((name, v))
        found = [c.upper() for c in _hard_coded_colours(page["doc"].get("objects"))]
        colours.update(found)
        colours_by_page[name] += len(found)
        _check_page_design(audit, name, visuals, hidden, width * height, max_data_visuals_per_page,
                           max_visuals_per_page, min_element_size, overlap_ratio)

    _check_schema_mix(audit, versions)
    _check_custom_visuals(audit, types, public, private)
    _check_bookmarks(audit, bookmarks, pages)
    for bookmark in bookmarks.values():
        state = bookmark.get("explorationState") if isinstance(bookmark.get("explorationState"), dict) else {}
        options = bookmark.get("options") if isinstance(bookmark.get("options"), dict) else {}
        suppressed = options.get("suppressActiveSection") is True
        if isinstance(state.get("activeSection"), str) and not suppressed:
            nav_targets.add(state["activeSection"])
    _check_report_design(audit, pages, nav_targets, slicers)
    if colours:
        worst = ", ".join(f"{p} ({n})" for p, n in colours_by_page.most_common(3) if n)
        audit.add("DESIGN_COLOURS", "info",
                  f"{sum(colours.values())} colour value(s) are written as literals and bypass the theme "
                  f"({len(colours)} distinct; {theme_refs} theme colour reference(s) elsewhere). "
                  f"Most on: {worst}.")

    order = {p["name"]: i for i, p in enumerate(pages)}
    audit.findings.sort(key=lambda f: (_SEVERITY_ORDER.get(f["severity"], 9), order.get(f["page"], -1),
                                       f["code"], str(f["visual"] or "")))
    by_severity = Counter(f["severity"] for f in audit.findings)
    sizes: Dict[str, List[str]] = defaultdict(list)
    for page in pages:
        page_w, page_h = _page_size(page["doc"])
        sizes[f"{page_w:g}x{page_h:g}"].append(page["name"])
    summary = {
        "definition_dir": str(definition),
        "pages": len(pages),
        "visuals": sum(1 for p in pages for v in p["visuals"] if not v.is_group),
        "groups": sum(1 for p in pages for v in p["visuals"] if v.is_group),
        "bookmarks": len(bookmarks),
        "visual_types": dict(types.most_common()),
        "custom_visuals": {"public": sorted(public), "private": private},
        "schema_versions": {k: dict(v) for k, v in sorted(versions.items())},
        "page_sizes": dict(sizes),
        "hard_coded_colours": {"total": sum(colours.values()), "distinct": len(colours),
                               "theme_references": theme_refs,
                               "by_page": {p: n for p, n in colours_by_page.items() if n},
                               "most_used": [list(x) for x in colours.most_common(8)]},
        "findings": {"total": len(audit.findings), "error": by_severity.get("error", 0),
                     "warning": by_severity.get("warning", 0), "info": by_severity.get("info", 0),
                     "by_code": dict(sorted(Counter(f["code"] for f in audit.findings).items()))},
        "field_check": ("done" if index is not None
                        else "skipped: pass model_fields to check field references"),
        "limits": [
            "Read from the files only: nothing was opened or rendered in Power BI.",
            "Visuals inside a group are left out of the overlap and outside-the-page checks (their "
            "coordinates are read relative to the group by some versions and to the page by others).",
            "A missing title object means the theme default applies, which this audit cannot see; only "
            "titles that are explicitly switched off are reported.",
            "Whether a store visual is certified cannot be read from a report definition.",
        ],
    }
    return {"summary": summary, "findings": audit.findings}


def _read_pages(audit: _Audit, definition: Path) -> List[Dict[str, Any]]:
    pages_dir = definition / "pages"
    out: List[Dict[str, Any]] = []
    renamed: List[str] = []
    seen_pages: Set[str] = set()
    if not pages_dir.is_dir():
        audit.add("STRUCT_FILE", "error", "The definition has no 'pages' folder.")
        return out
    for folder in sorted(p for p in pages_dir.iterdir() if p.is_dir()):
        page_json = folder / "page.json"
        if not page_json.is_file():
            audit.add("STRUCT_FILE", "error", "The page folder has no page.json.", folder.name)
            continue
        doc = audit.load(page_json, folder.name)
        if not isinstance(doc, dict):
            continue
        # The name inside the file is the page's identity: pages.json, buttons and bookmarks
        # all use it. The folder may be renamed, so a difference is reported once, as a note.
        name = doc.get("name")
        if not isinstance(name, str) or not name:
            audit.add("STRUCT_NAME", "error", "page.json has no 'name'.", folder.name)
            name = folder.name
        elif name != folder.name:
            renamed.append(f"page folder '{folder.name}' holds '{name}'")
        if name in seen_pages:
            audit.add("STRUCT_NAME", "error", f"Two page folders declare the name '{name}'.", name)
        seen_pages.add(name)
        visuals: List[_Visual] = []
        seen: Set[str] = set()
        visuals_dir = folder / "visuals"
        for vfolder in sorted(p for p in visuals_dir.iterdir() if p.is_dir()) if visuals_dir.is_dir() else []:
            visual_json = vfolder / "visual.json"
            if not visual_json.is_file():
                audit.add("STRUCT_FILE", "error", "The visual folder has no visual.json.", name, vfolder.name)
                continue
            vdoc = audit.load(visual_json, name, vfolder.name)
            if not isinstance(vdoc, dict):
                continue
            inner = vdoc.get("name")
            if not isinstance(inner, str) or not inner:
                audit.add("STRUCT_NAME", "error", "visual.json has no 'name'.", name, vfolder.name)
            elif inner != vfolder.name:
                renamed.append(f"visual folder '{folder.name}/{vfolder.name}' holds '{inner}'")
            visual = _Visual(vfolder.name, vdoc)
            if visual.name in seen:
                audit.add("STRUCT_NAME", "error",
                          f"Two visuals on the page declare the name '{visual.name}'.", name, visual.name)
            seen.add(visual.name)
            visuals.append(visual)
        out.append({"name": name, "folder": folder.name, "doc": doc, "visuals": visuals})
    if renamed:
        audit.add("STRUCT_NAME", "info",
                  f"{len(renamed)} folder(s) are named differently from the 'name' inside them (for example "
                  f"{renamed[0]}). The format allows it and Power BI uses the name inside; a script that "
                  "looks objects up by folder will not find them.")
    return out


def _check_pages_json(audit: _Audit, definition: Path, pages: List[Dict[str, Any]]) -> None:
    path = definition / "pages" / "pages.json"
    if not path.is_file():
        if (definition / "pages").is_dir():
            audit.add("STRUCT_PAGES", "error", "pages.json is missing from the pages folder.")
        return
    meta = audit.load(path)
    if not isinstance(meta, dict):
        return
    names = [p["name"] for p in pages]
    order = [n for n in _list(meta.get("pageOrder")) if isinstance(n, str)]
    for name in order:
        if name not in names:
            audit.add("STRUCT_PAGES", "error", f"pageOrder lists '{name}', which no readable page declares.")
    for name in names:
        if name not in order:
            audit.add("STRUCT_PAGES", "error", "The page is not listed in pages.json pageOrder.", name)
    if len(order) != len(set(order)):
        audit.add("STRUCT_PAGES", "error", "pageOrder lists a page more than once.")
    active = meta.get("activePageName")
    if isinstance(active, str) and active not in names:
        audit.add("STRUCT_PAGES", "error", f"activePageName is '{active}', which is not a page.")
    # Put the pages in report order, so findings read the way the report does.
    rank = {n: i for i, n in enumerate(order)}
    pages.sort(key=lambda p: (rank.get(p["name"], len(rank)), p["name"]))


def _hidden_names(visuals: List[_Visual], by_name: Mapping[str, _Visual]) -> Set[str]:
    """Names of visuals that are hidden themselves or sit in a hidden group (a pop-up)."""
    hidden: Set[str] = set()
    for v in visuals:
        node: Optional[_Visual] = v
        seen: Set[str] = set()
        while node is not None and node.name not in seen:
            seen.add(node.name)
            if node.hidden:
                hidden.add(v.name)
                break
            node = by_name.get(node.parent) if node.parent else None
    return hidden


def _check_container(audit: _Audit, page: str, v: _Visual, groups: Set[str], width: float,
                     height: float) -> None:
    doc = v.doc
    has_visual, has_group = isinstance(doc.get("visual"), dict), v.is_group
    if has_visual and has_group:
        audit.add("STRUCT_MISPLACED", "error", "The container holds both 'visual' and 'visualGroup'.",
                  page, v.name)
    elif not has_visual and not has_group:
        audit.add("STRUCT_MISPLACED", "error", "The container holds neither 'visual' nor 'visualGroup'.",
                  page, v.name)
    for key in _VISUAL_LEVEL:
        if key in doc:
            audit.add("STRUCT_MISPLACED", "error",
                      f"'{key}' is at the root of visual.json; it belongs inside 'visual'. At the root it is "
                      "ignored or invalidates the container.", page, v.name)
    for key in _CONTAINER_LEVEL:
        if key in v.visual:
            audit.add("STRUCT_MISPLACED", "warning",
                      f"'{key}' is inside 'visual'; it belongs at the root of visual.json.", page, v.name)
    if has_visual and not v.type:
        audit.add("STRUCT_MISPLACED", "error", "'visual' has no visualType.", page, v.name)
    if v.parent and v.parent not in groups:
        audit.add("STRUCT_GROUP", "error",
                  f"parentGroupName is '{v.parent}', which is not a group on this page.", page, v.name)
    if not v.has_position:
        audit.add("STRUCT_POSITION", "error", "The container has no position.", page, v.name)
    elif v.w <= 0 or v.h <= 0:
        audit.add("STRUCT_POSITION", "error",
                  f"The size is {v.w:g} x {v.h:g}; a visual needs a width and a height.", page, v.name)
    elif not v.parent and (v.x < 0 or v.y < 0 or v.x + v.w > width + 1 or v.y + v.h > height + 1):
        audit.add("STRUCT_POSITION", "warning",
                  f"At ({v.x:g}, {v.y:g}) with size {v.w:g} x {v.h:g} it reaches outside the "
                  f"{width:g} x {height:g} page.", page, v.name)


def _check_fields(audit: _Audit, page: str, v: _Visual, index: Mapping[str, Set[str]]) -> None:
    seen: Set[Tuple[str, str]] = set()
    for kind, table, field in _field_refs(v.doc):
        key = (table.casefold(), field.casefold())
        if key in seen:
            continue
        seen.add(key)
        if key[0] not in index:
            audit.add("STRUCT_FIELD", "error",
                      f"It uses {kind} {table}[{field}]; the model has no table '{table}'.", page, v.name)
        elif key[1] not in index[key[0]]:
            audit.add("STRUCT_FIELD", "error", f"It uses {kind} {table}[{field}], which is not in the model.",
                      page, v.name)


def _check_links(audit: _Audit, page: str, v: _Visual, page_names: Set[str], bookmarks: Mapping[str, Any],
                 bookmark_groups: Set[str], nav_targets: Set[str]) -> None:
    for bag in _properties(v.container, "visualLink"):
        if _literal(bag.get("show")) is False:
            continue
        action = _literal(bag.get("type"))
        if action == "PageNavigation":
            target = _literal(bag.get("navigationSection"))
            if isinstance(target, str):
                nav_targets.add(target)
                if target not in page_names:
                    audit.add("DESIGN_BROKEN_LINK", "error",
                              f"Its page navigation points at '{target}', which is not a page.", page, v.name)
            elif "navigationSection" not in bag:
                audit.add("DESIGN_BROKEN_LINK", "warning", "Its page navigation has no destination.",
                          page, v.name)
        elif action == "Bookmark":
            target = _literal(bag.get("bookmark"))
            if isinstance(target, str) and target not in bookmarks:
                audit.add("DESIGN_BROKEN_LINK", "error",
                          f"Its bookmark action points at '{target}', which is not a bookmark.", page, v.name)
            elif "bookmark" not in bag:
                audit.add("DESIGN_BROKEN_LINK", "warning", "Its bookmark action names no bookmark.",
                          page, v.name)
    if v.type == "bookmarkNavigator":
        for item in _walk(v.visual.get("objects")):
            group = _literal(item.get("bookmarkGroup")) if "bookmarkGroup" in item else None
            if isinstance(group, str) and group and group not in bookmark_groups:
                audit.add("DESIGN_BROKEN_LINK", "warning",
                          f"The bookmark navigator shows the group '{group}', which is not a bookmark "
                          "group.", page, v.name)


def _hard_coded_colours(node: Any) -> List[str]:
    out = []
    for item in _walk(node):
        value = item.get("Literal", {}).get("Value") if isinstance(item.get("Literal"), dict) else None
        quoted = isinstance(value, str) and len(value) >= 2 and value[0] == value[-1] == "'"
        if quoted and _HEX_COLOUR.fullmatch(value[1:-1]):
            out.append(value[1:-1])
    return out


def _has_alt_text(v: _Visual) -> bool:
    """True for non-empty literal alt text, and for alt text driven by an expression (a
    measure or a conditional rule), whose value the files cannot show."""
    for bag in _properties(v.container, "general"):
        if "altText" in bag:
            text = _literal(bag["altText"])
            return text is None or (isinstance(text, str) and bool(text.strip()))
    return False


def _overlap(a: _Visual, b: _Visual) -> float:
    """Shared area as a share of the smaller visual."""
    w = min(a.x + a.w, b.x + b.w) - max(a.x, b.x)
    h = min(a.y + a.h, b.y + b.h) - max(a.y, b.y)
    smaller = min(a.w * a.h, b.w * b.h)
    return (w * h) / smaller if w > 0 and h > 0 and smaller > 0 else 0.0


def _check_page_design(audit: _Audit, page: str, visuals: List[_Visual], hidden: Set[str], page_area: float,
                       max_data: int, max_all: int, min_size: int, overlap_ratio: float) -> None:
    shown = [v for v in visuals if not v.is_group and v.name not in hidden]
    data = [v for v in shown if v.is_data]
    if len(data) > max_data:
        audit.add("DESIGN_CROWDED", "warning",
                  f"{len(data)} data visuals are visible at once (more than {max_data}). A reader cannot "
                  "take that many in, and every one is a query.", page)
    elif len(shown) > max_all:
        audit.add("DESIGN_CROWDED", "info",
                  f"{len(shown)} visible elements on the page (more than {max_all}).", page)

    # A visual that covers most of the page is a canvas other things are laid over on purpose
    # (a full-page scene with cards on it), so it is not compared.
    free = [v for v in data if not v.parent and v.w > 0 and v.h > 0 and v.w * v.h < 0.5 * page_area]
    for i, a in enumerate(free):
        for b in free[i + 1:]:
            share = _overlap(a, b)
            if share >= overlap_ratio:
                audit.add("DESIGN_OVERLAP", "warning",
                          f"{a.label()} and {b.label()} overlap: {share:.0%} of the smaller one is covered. "
                          "Neither is in a group.", page, f"{a.name} + {b.name}")

    no_alt = [v for v in data if not _has_alt_text(v)]
    if no_alt:
        audit.add("DESIGN_ALT_TEXT", "warning",
                  f"{len(no_alt)} of {len(data)} data visual(s) have no alt text: "
                  + ", ".join(v.label() for v in no_alt[:8]) + (" ..." if len(no_alt) > 8 else "") + ".",
                  page, visuals=[v.name for v in no_alt])
    untitled = [v for v in data if _first(v.container, "title", "show") is False]
    if untitled:
        audit.add("DESIGN_TITLE", "info",
                  f"{len(untitled)} data visual(s) have the title switched off: "
                  + ", ".join(v.label() for v in untitled[:8]) + (" ..." if len(untitled) > 8 else "") + ".",
                  page, visuals=[v.name for v in untitled])
    for v in shown:
        if v.w <= 0 or v.h <= 0:
            continue
        thin = min(v.w, v.h) < min_size
        # A thin shape or image is a rule or a divider; only flag decoration when it is small both ways.
        if (thin and v.type not in ("shape", "basicShape", "image")) or max(v.w, v.h) < min_size:
            audit.add("DESIGN_TINY", "warning", f"{v.label()} is {v.w:g} x {v.h:g} pixels.", page, v.name)


def _check_schema_mix(audit: _Audit, versions: Mapping[str, Counter]) -> None:
    for kind, counts in sorted(versions.items()):
        if len(counts) > 1:
            listed = ", ".join(f"{ver} ({n})" for ver, n in sorted(counts.items()))
            audit.add("STRUCT_SCHEMA", "info", f"{kind} files use {len(counts)} schema versions: {listed}.")


def _check_custom_visuals(audit: _Audit, types: Counter, public: List[str], private: List[str]) -> None:
    registered = set(public) | set(private)
    used = [t for t in types if t in registered or _CUSTOM_ID.fullmatch(t or "")]
    for t in sorted(used):
        if t not in registered:
            audit.add("STRUCT_CUSTOM_VISUAL", "error",
                      f"The custom visual '{t}' is used {types[t]} time(s) but is not registered: it is not "
                      "in report.json publicCustomVisuals and has no package in the CustomVisuals folder.")
    for t in sorted(set(public) - set(used)):
        audit.add("STRUCT_CUSTOM_VISUAL", "info",
                  f"'{t}' is listed in publicCustomVisuals but no visual uses it.")
    in_use_private = sorted(t for t in used if t in private)
    in_use_public = sorted(t for t in used if t in public and t not in private)
    if in_use_private:
        audit.add("GOV_PRIVATE_VISUAL", "info",
                  "Private custom visuals (packaged inside the report, not from the store, so not certified) "
                  "are in use: " + ", ".join(f"{t} ({types[t]})" for t in in_use_private) + ".")
    if in_use_public:
        audit.add("GOV_PUBLIC_VISUAL", "info",
                  "Store custom visuals are in use: " + ", ".join(f"{t} ({types[t]})" for t in in_use_public)
                  + ". Whether each is certified cannot be read from the report files.")
    html_types = sorted(t for t in types if t and "html" in t.lower())
    if html_types:
        audit.add("GOV_HTML_VISUAL", "info",
                  "Visuals that render HTML supplied by a measure are in use: "
                  + ", ".join(f"{t} ({types[t]})" for t in html_types)
                  + ". Some HTML visuals also run scripts contained in that HTML.")


def _read_bookmarks(audit: _Audit, definition: Path) -> Tuple[Dict[str, Dict[str, Any]], Set[str]]:
    folder = definition / "bookmarks"
    bookmarks: Dict[str, Dict[str, Any]] = {}
    groups: Set[str] = set()
    if not folder.is_dir():
        return bookmarks, groups
    for path in sorted(folder.glob("*.bookmark.json")):
        doc = audit.load(path)
        if not isinstance(doc, dict):
            continue
        # As with pages and visuals, the name inside the file is what buttons refer to.
        stem = path.name[:-len(".bookmark.json")]
        name = doc.get("name") if isinstance(doc.get("name"), str) and doc.get("name") else stem
        if name in bookmarks:
            audit.add("STRUCT_NAME", "error", f"Two bookmark files declare the name '{name}'.")
        bookmarks[name] = doc
    meta_path = folder / "bookmarks.json"
    if not meta_path.is_file():
        if bookmarks:
            audit.add("STRUCT_FILE", "error", "The bookmarks folder has no bookmarks.json.")
        return bookmarks, groups
    meta = audit.load(meta_path)
    listed: Set[str] = set()
    for item in _list(_dict(meta).get("items")):
        if not isinstance(item, dict):
            continue
        children = item.get("children")
        if isinstance(children, list):
            if isinstance(item.get("name"), str):
                groups.add(item["name"])
            if isinstance(item.get("displayName"), str):
                groups.add(item["displayName"])
            listed.update(c for c in children if isinstance(c, str))
        elif isinstance(item.get("name"), str):
            listed.add(item["name"])
    for name in sorted(listed - set(bookmarks)):
        audit.add("DESIGN_BROKEN_LINK", "error",
                  f"bookmarks.json lists '{name}', which has no bookmark file.")
    for name in sorted(set(bookmarks) - listed):
        audit.add("STRUCT_FILE", "info", f"The bookmark file '{name}' is not listed in bookmarks.json.")
    return bookmarks, groups


def _check_bookmarks(audit: _Audit, bookmarks: Mapping[str, Dict[str, Any]],
                     pages: List[Dict[str, Any]]) -> None:
    on_page = {p["name"]: {v.name for v in p["visuals"]} for p in pages}
    for name, doc in sorted(bookmarks.items()):
        state = doc.get("explorationState") if isinstance(doc.get("explorationState"), dict) else {}
        shown = doc.get("displayName") if isinstance(doc.get("displayName"), str) else name
        active = state.get("activeSection")
        if isinstance(active, str) and active not in on_page:
            audit.add("DESIGN_BROKEN_LINK", "error",
                      f"Bookmark '{shown}' opens the page '{active}', which is not a page.", visual=name)
        sections = state.get("sections") if isinstance(state.get("sections"), dict) else {}
        for section, body in sections.items():
            if section not in on_page:
                audit.add("DESIGN_BROKEN_LINK", "warning", f"Bookmark '{shown}' stores state for the page "
                          f"'{section}', which is not a page.", visual=name)
                continue
            for key in ("visualContainers", "visualContainerGroups"):
                stored = body.get(key) if isinstance(body, dict) else None
                gone = sorted(n for n in _dict(stored) if n not in on_page[section])
                if gone:
                    audit.add("DESIGN_BROKEN_LINK", "warning",
                              f"Bookmark '{shown}' refers to {len(gone)} visual(s) or group(s) that are no "
                              f"longer on the page: {', '.join(gone[:6])}{' ...' if len(gone) > 6 else ''}.",
                              section, name)
        options = doc.get("options") if isinstance(doc.get("options"), dict) else {}
        targets = options.get("targetVisualNames")
        everywhere = set().union(*on_page.values()) if on_page else set()
        gone = sorted(n for n in _list(targets) if isinstance(n, str) and n not in everywhere)
        if gone:
            audit.add("DESIGN_BROKEN_LINK", "warning",
                      f"Bookmark '{shown}' targets {len(gone)} visual(s) or group(s) that do not exist: "
                      f"{', '.join(gone[:6])}{' ...' if len(gone) > 6 else ''}.", visual=name)


def _check_report_design(audit: _Audit, pages: List[Dict[str, Any]], nav_targets: Set[str],
                         slicers: Mapping[Tuple[str, str], List[Tuple[str, _Visual]]]) -> None:
    normal = [p for p in pages if p["doc"].get("type") not in ("Tooltip", "Drillthrough")]
    sizes: Dict[Tuple[float, float], List[str]] = defaultdict(list)
    for p in normal:
        sizes[_page_size(p["doc"])].append(p["name"])
    if len(sizes) > 1:
        listed = "; ".join(f"{w:g} x {h:g}: {', '.join(names[:4])}{' ...' if len(names) > 4 else ''}"
                           for (w, h), names in sorted(sizes.items(), key=lambda kv: -len(kv[1])))
        audit.add("DESIGN_PAGE_SIZE", "info", f"Pages come in {len(sizes)} sizes ({listed}).")
    for p in normal:
        if p["doc"].get("visibility") == "HiddenInViewMode" and p["name"] not in nav_targets:
            shown = p["doc"].get("displayName") or p["name"]
            audit.add("DESIGN_HIDDEN_PAGE", "warning",
                      f"The page '{shown}' is hidden and no button or bookmark opens it.", p["name"])
    for (table, field), found in sorted(slicers.items()):
        on_pages = sorted({page for page, _ in found})
        if len(on_pages) < 2:
            continue
        synced = [(page, _text(v.visual["syncGroup"].get("groupName"))) for page, v in found
                  if isinstance(v.visual.get("syncGroup"), dict)]
        group_names = {group for _, group in synced}
        loose = sorted({page for page, v in found if not isinstance(v.visual.get("syncGroup"), dict)})
        if loose:
            audit.add("DESIGN_SYNC", "info",
                      f"{table}[{field}] is sliced on {len(on_pages)} pages ({', '.join(on_pages[:6])}"
                      f"{' ...' if len(on_pages) > 6 else ''}); the slicer(s) on {', '.join(loose[:6])} are "
                      "not in a sync group, so a choice does not carry across.",
                      visuals=[v.name for _, v in found])
        elif len(group_names) > 1:
            audit.add("DESIGN_SYNC", "info",
                      f"{table}[{field}] is sliced on {len(on_pages)} pages through {len(group_names)} "
                      f"different sync groups ({', '.join(sorted(str(g) for g in group_names))}).",
                      visuals=[v.name for _, v in found])
