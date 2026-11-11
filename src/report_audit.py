"""
Read-only audit of a PBIR report definition (the ``definition`` folder of a .Report).

Power BI Desktop reports a broken project with an error that seldom names the cause, and a
report can be perfectly valid and still be hard to use. This module reads the JSON files and
returns findings in four groups:

  structure   what stops a report opening or silently drops a visual: unparseable JSON, a
              byte order mark, two objects that declare the same ``name`` (and, as a note,
              folders named differently from the name inside), pages.json out of step with
              the pages, a custom visual that is used but not registered, a visual outside
              its page, a property at the wrong level, mixed schema versions, a field that
              is not in the model (when ``model_fields`` is given), a path too long for
              Windows, visual interactions and phone positions that point nowhere, filter
              names used twice, bookmarks listed twice.
  design      what makes a page harder to read or to navigate: data visuals that overlap,
              crowded pages, missing alt text, titles switched off, tiny elements, page
              sizes that differ, hidden pages nothing leads to, the same field sliced on
              several pages without a sync group, buttons and bookmarks that point at
              something that no longer exists, colours written as literals instead of
              theme colours; drillthrough pages nothing can reach or that offer no way
              back, tooltip pages that are visible, oversized or unused, slicers switched
              off for their whole page, phone layouts that leave data visuals out.
  governance  facts a tenant administrator may want to know, reported as information and
              never as errors: private custom visuals, which are never certified; visuals
              from the public store, whose certification cannot be read from the files and
              is stated only where the vendor's documentation gives it for the visual id
              (Deneb and HTML Content Secure are certified, HTML Content Regular is not);
              HTML visuals, which render markup supplied by a measure, and of which HTML
              Content Regular also runs scripts; hidden filters that restrict what readers
              see.
  interact    what each custom visual can do that a native visual does: filter the page
              when clicked, be highlighted by other visuals, open the context menu that
              leads to drillthrough, show Power BI tooltips. For a private visual this is
              read from its package (capabilities, data roles and script); for Deneb and
              HTML Content, whose code is not in the report, from the settings stored in
              each visual and from what their own documentation states.

Nothing is written, and nothing is opened in Power BI: every finding comes from the files
as they are on disk. Severity is "error" (Desktop is likely to reject or mis-render it),
"warning" (worth fixing: a reader meets the defect) or "info".

Where each shape comes from (the checks name their own):
    pages.json          .../report/definition/pagesMetadata/1.1.0/schema.json (pageOrder,
                        activePageName, landingPageName)
    page.json           .../report/definition/page/2.1.0/schema.json (type, visibility,
                        pageBinding, visualInteractions)
    filters             .../report/definition/filterConfiguration/1.3.0/schema.json
    mobile.json         .../report/definition/visualContainerMobileState/2.3.0/schema.json
    bookmarks.json      .../report/definition/bookmarksMetadata/1.0.0/schema.json
    visualLink/Tooltip  .../report/definition/visualConfiguration/2.3.0/schema-embedded.json for
                        the property names; the values (a link's action 'Back', 'Bookmark',
                        'Drillthrough', 'PageNavigation', 'WebUrl'; a tooltip's type 'Default'
                        or 'Canvas') from the pbir command line's property catalogue
    custom visuals      powerbi-visuals-api 5.11.0: schema.capabilities.json and the typings
                        of IVisualHost, ISelectionManager and ITooltipService
    Deneb               https://deneb.guide/docs/pbir-guide (objects.vega, the certified id)
    HTML Content        https://www.html-content.com/docs/pbir-guide, /docs/interactivity and
                        /docs/visual-editions (roles, crossFilter, the two editions)

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

# Page schema, VisualInteractionFilterType.
_INTERACTION_TYPES = ("Default", "DataFilter", "HighlightFilter", "NoFilter")
# "Tooltip" page size offered by Desktop (Microsoft Learn, "Extend visuals with report page
# tooltips": 320 pixels wide, 240 high; a larger page "can obscure the visuals on the source
# page"). Twice that on either side is reported.
_TOOLTIP_WIDTH, _TOOLTIP_HEIGHT = 320.0, 240.0
# What Desktop stores in visualTooltip "section" when the tooltip page is chosen automatically
# (seen in reports saved by Desktop; it is not a page name).
_TOOLTIP_AUTO = "___AUTO___"
# "323 pt is the maximum screen width on the mobile layout canvas" (Microsoft Learn, "Best
# practices for creating mobile-optimized reports"). The canvas scrolls, so it has no height.
_PHONE_CANVAS_WIDTH = 323.0
# Windows: "The fully qualified file name must be less than 260 characters, and the directory
# name must be less than 248 characters." Desktop refuses to open a report that breaks either.
# The limits are Windows' own, so the rule is applied where the audit runs on Windows: a path on
# another system says nothing about where the report will sit on the machine that opens it.
_MAX_FILE_PATH, _MAX_FOLDER_PATH = 260, 248
_ON_WINDOWS = os.name == "nt"
# The reparse tag of a directory junction (IO_REPARSE_TAG_MOUNT_POINT).
_JUNCTION = 0xA0000003

# Deneb keeps its interactivity switches in visual.objects.vega[0].properties; a switch that is
# not stored is at the default listed here (Deneb documentation, PBIR implementation guide,
# which also names this id as "the AppSource (certified) version of Deneb").
_DENEB_ID = "deneb7E15AEF80B9E4D4F8E12924291ECE89A"
_DENEB_SWITCHES = (("enableSelection", False), ("enableHighlight", False), ("enableTooltips", True),
                   ("enableContextMenu", True), ("enableContextMenuSelector", True))
# The two AppSource editions of HTML Content (HTML Content documentation: PBIR implementation
# guide, "Visual GUIDs", and "Visual editions"). Secure is the certified one: its sanitizer
# removes <script> elements and inline event handlers. Regular is not certified and runs them.
_HTML_SECURE = "htmlContent443BE3AD55E043BF878BED274D3A6865"
_HTML_REGULAR = "htmlContent443BE3AD55E043BF878BED274D3A6855"
_HTML_CONTENT_EDITIONS = {_HTML_SECURE: "Secure", _HTML_REGULAR: "Regular"}
# What the vendors' documentation states about the store visuals this audit knows by id. For
# any other store visual, certification cannot be read from a report.
_CERTIFIED = {_DENEB_ID: True, _HTML_SECURE: True, _HTML_REGULAR: False}
# Members of the Power BI visuals API a script has to call for each native behaviour. They are
# properties of objects the host supplies, so a minifier cannot rename them.
_SCRIPT_CALLS = {"selection_manager": "createSelectionManager", "selection_ids": "createSelectionIdBuilder",
                 "filter_api": "applyJsonFilter", "context_menu": "showContextMenu",
                 "tooltip_service": "tooltipService", "reads_highlights": "highlights"}
# What is not code in a script: comments, strings, template literals and regular expression
# literals. A name that only appears in one of those is not a call. Two things are kept: a
# quoted property name in brackets (host["createSelectionManager"]), which is a member access,
# and the operator in front of a regular expression, which is how one is told from a division.
_JS_NOT_CODE = re.compile(r"""
      (?P<key>\[\s*(?P<quote>["'])[A-Za-z_$][\w$]*(?P=quote)\s*\])
    | //[^\n]*
    | /\*.*?\*/
    | "(?:\\.|[^"\\\n])*"
    | '(?:\\.|[^'\\\n])*'
    | `(?:\\.|[^`\\])*`
    | (?P<before>[(,=:\[!&|?{;]|\b(?:return|typeof|case|in|of|do|else|void|throw|new|delete)\b)\s*
      /(?![/*])(?:\\.|\[(?:\\.|[^\]\\\n])*\]|[^/\\\n\[])+/[A-Za-z]*
""", re.S | re.X)
# What is left of host["create" + "SelectionManager"]() once the strings are set aside: a
# member called by a name that is put together at run time, which no search for names finds.
_JS_ASSEMBLED_CALL = re.compile(r"\[[\s+]+\]\s*\(")
# VisualDataRoleKind, for a package that stores the number instead of the name.
_ROLE_KINDS = {0: "Grouping", 1: "Measure", 2: "GroupingOrMeasure"}
# The kinds of data role that take a column, and so give the visual data points to select.
_COLUMN_ROLES = ("grouping", "groupingormeasure")

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
    "STRUCT_PATH_LENGTH": "Move the project to a shorter folder path or shorten the folder names; Windows "
                          "must be able to open every file below 260 characters.",
    "STRUCT_INTERACTION": "Name visuals that are on the page, use one entry per pair, and one of the types "
                          "Default, DataFilter, HighlightFilter or NoFilter; or remove the entry.",
    "STRUCT_FILTER_NAME": "Give each filter its own name.",
    "STRUCT_MOBILE_POSITION": "Place the visual inside the phone canvas in Desktop's mobile layout view, or "
                              "delete its mobile.json to leave it out of the phone layout.",
    "STRUCT_BOOKMARK_GROUP": "List each bookmark once in bookmarks.json, give every group its own name and "
                             "a displayName, and remove groups that hold nothing.",
    "DESIGN_DRILL_FIELD": "Add a field to the page's drillthrough well, or turn the page back into an "
                          "ordinary page.",
    "DESIGN_DRILL_BACK": "Add a button with the action 'Back' (Desktop adds one when a drillthrough field "
                         "is set; it is worth keeping).",
    "DESIGN_DRILL_UNREACHABLE": "Use the drillthrough field in a visual on another page, or change the "
                                "drillthrough field to one the report's visuals show.",
    "DESIGN_DRILL_LANDING": "Open the report on an ordinary page: set another page as the landing page, or "
                            "save the report with another page open.",
    "DESIGN_TOOLTIP_VISIBLE": "Hide the page (right-click the tab > Hide), so readers do not land on it.",
    "DESIGN_TOOLTIP_SIZE": "Set the page size to Tooltip (320 x 240) or a custom size not much larger.",
    "DESIGN_TOOLTIP_LINK": "Point the visual's tooltip at a page that exists and has 'Allow use as tooltip' "
                           "switched on, or set the tooltip type back to Default.",
    "DESIGN_TOOLTIP_UNUSED": "Choose the page in a visual's Tooltip settings (type Report page), give the "
                             "page tooltip fields so it is matched automatically, or delete it.",
    "DESIGN_INTERACTION": "Remove the entries that do nothing.",
    "DESIGN_INTERACTION_OFF": "Check Format > Edit interactions for the slicer: a slicer that filters "
                              "nothing on its page is usually a mistake, unless it only feeds other pages.",
    "DESIGN_FILTER_DEAD": "Remove the filter, or give it a condition.",
    "DESIGN_MOBILE_PARTIAL": "Add the visuals that matter on a phone to the mobile layout; leaving the "
                             "others out is fine when it is deliberate.",
    "GOV_HIDDEN_FILTER": "Make sure each hidden filter is intended and documented: readers cannot see that "
                         "the data is restricted.",
    "INTERACT_CAPABILITIES": "Compare with what the report needs; a behaviour the package lacks has to be "
                             "added to the visual's code and capabilities.",
    "INTERACT_NO_SELECTION": "Build a selection id for each data point and call the selection manager on "
                             "click, or tell readers the visual is display only.",
    "INTERACT_NO_CONTEXT_MENU": "Call selectionManager.showContextMenu with the data point's selection id "
                                "on right-click, so the context menu can offer drillthrough.",
    "INTERACT_DENEB": "Change the interactivity settings in Deneb's editor where a visual should behave "
                      "differently.",
    "INTERACT_HTML_CONTENT": "To let an HTML Content visual filter the page, put a column in its Context "
                             "role and switch on Enable under cross-filtering.",
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
            return _parse_json(raw)
        except ValueError as e:
            self.add("STRUCT_JSON", "error", f"{path.name} is not valid JSON: {e}.", page, visual)
            return None


def _not_json(name: str) -> Any:
    raise ValueError(f"{name} is not a JSON value")


def _finite(text: str) -> float:
    value = float(text)
    if value in (float("inf"), float("-inf")):
        raise ValueError(f"the number {text[:24]} is too large to read")
    return value


def _parse_json(raw: bytes) -> Any:
    """``raw`` parsed as strict JSON. Everything a report tool cannot read raises ValueError:
    wrong encoding, bad syntax, the NaN and Infinity that Python would let through, a number
    too large to hold, and nesting too deep to walk."""
    try:
        return json.loads(raw.decode("utf-8"), parse_constant=_not_json, parse_float=_finite)
    except RecursionError:
        raise ValueError("it is nested too deeply to read") from None


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
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return default
    try:
        return float(value)
    except OverflowError:  # an integer of hundreds of digits
        return default


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
    """Every dict in a JSON tree, parents before children, in document order. It keeps its
    own stack: a file may be nested as deep as the parser accepts."""
    pending = [node]
    while pending:
        node = pending.pop()
        if isinstance(node, dict):
            yield node
            pending.extend(reversed(list(node.values())))
        elif isinstance(node, list):
            pending.extend(reversed(node))


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


def _filters(doc: Any) -> List[Dict[str, Any]]:
    """The filters of a report, a page or a visual: filterConfig.filters[] in all three files."""
    return [f for f in _list(_dict(_dict(doc).get("filterConfig")).get("filters")) if isinstance(f, dict)]


def _has_condition(flt: Mapping[str, Any]) -> bool:
    """True when the filter restricts anything: its definition has a non-empty Where."""
    return bool(_list(_dict(flt.get("filter")).get("Where")))


def _field_label(refs: List[Tuple[str, str, str]]) -> Optional[str]:
    return f"{refs[0][1]}[{refs[0][2]}]" if refs else None


def _page_kind(doc: Mapping[str, Any]) -> Optional[str]:
    """'Drillthrough' or 'Tooltip' for a page used as one. Desktop writes the page "type" and,
    for drillthrough, a pageBinding of the same type and page filters marked howCreated
    'Drillthrough'; a file that carries only one of the marks is recognised by it."""
    for value in (doc.get("type"), _dict(doc.get("pageBinding")).get("type")):
        if value in ("Drillthrough", "Tooltip"):
            return value
    return "Drillthrough" if any(f.get("howCreated") == "Drillthrough" for f in _filters(doc)) else None


def _source(ref: Any) -> Optional[Tuple[str, str]]:
    """(table, name) of a Column or Measure reference: {"Expression": {"SourceRef": {"Entity"}},
    "Property"}."""
    ref = _dict(ref)
    entity, name = _dict(_dict(ref.get("Expression")).get("SourceRef")).get("Entity"), ref.get("Property")
    return (entity, name) if isinstance(entity, str) and isinstance(name, str) else None


def _expression(expr: Any) -> Optional[Tuple[str, str, str]]:
    """How one field expression uses the model: ("column" | "aggregate" | "measure" |
    "hierarchy", table, name), or None for any other expression.

    The shapes are those of the semanticQuery schema: Column and Measure; Aggregation, which
    wraps a column in {"Expression", "Function"}; HierarchyLevel, {"Expression": {"Hierarchy":
    {"Expression": <table>, "Hierarchy"}}, "Level"}, which stands for a column only the model
    can name.
    """
    expr = _dict(expr)
    for key, kind in (("Column", "column"), ("Measure", "measure")):
        found = _source(expr.get(key))
        if found:
            return kind, found[0], found[1]
    summed = _source(_dict(_dict(expr.get("Aggregation")).get("Expression")).get("Column"))
    if summed:
        return "aggregate", summed[0], summed[1]
    level = _dict(expr.get("HierarchyLevel"))
    hierarchy = _dict(_dict(level.get("Expression")).get("Hierarchy"))
    tables = [e for e in (_dict(d.get("SourceRef")).get("Entity") for d in _walk(hierarchy.get("Expression")))
              if isinstance(e, str)]
    if tables and isinstance(level.get("Level"), str):
        return "hierarchy", tables[0], f"{_text(hierarchy.get('Hierarchy')) or '?'}.{level['Level']}"
    return None


def _binding(doc: Mapping[str, Any]) -> List[Dict[str, Any]]:
    """What a drillthrough or tooltip page is bound to: one {"label", "use"} per pageBinding
    parameter, read from its fieldExpr or from the field of the filter it names in boundFilter.

    "use" is how a visual must hold the field for the page to be offered from it: ("column",
    table, field) for a field used as a category, ("aggregate", ...) for one that drillthrough
    takes when it is summarized (the parameter's asAggregation), ("measure", ...); table and
    field case-folded. It is None when the files cannot say: a hierarchy level, an expression
    of another kind, a parameter with no field. A page with no parameter at all is read by its
    filters marked howCreated 'Drillthrough'.
    """
    def bound(found: Optional[Tuple[str, str, str]], summarized: bool) -> Dict[str, Any]:
        if found is None:
            return {"label": None, "use": None}
        kind, table, field = found
        if kind == "hierarchy":
            return {"label": f"{table}[{field}]", "use": None}
        if kind == "column" and summarized:
            kind = "aggregate"
        return {"label": f"{table}[{field}]", "use": (kind, table.casefold(), field.casefold())}

    by_name = {_text(f.get("name")): f for f in _filters(doc)}
    entries = []
    for parameter in _list(_dict(doc.get("pageBinding")).get("parameters")):
        if isinstance(parameter, dict):
            named = _dict(by_name.get(_text(parameter.get("boundFilter")) or "")).get("field")
            entries.append(bound(_expression(parameter.get("fieldExpr")) or _expression(named),
                                 parameter.get("asAggregation") is True))
    if not entries:
        entries = [bound(_expression(f.get("field")), False) for f in _filters(doc)
                   if f.get("howCreated") == "Drillthrough"]
    return entries


def _labels(entries: List[Dict[str, Any]]) -> List[str]:
    """The fields of a binding by name, each once."""
    return list(dict.fromkeys(e["label"] for e in entries if e["label"]))


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
        # The same fields by data role and by the way each is used: role -> [(use, table, name)].
        self.roles: Dict[str, List[Tuple[str, str, str]]] = {}
        self.field_parameters = False
        for role, state in _dict(_dict(self.visual.get("query")).get("queryState")).items():
            found = (_expression(_dict(p).get("field")) for p in _list(_dict(state).get("projections")))
            self.roles[role] = [f for f in found if f]
            self.field_parameters = self.field_parameters or bool(_list(_dict(state).get("fieldParameters")))
        self.uses = {(use, table.casefold(), name.casefold())
                     for found in self.roles.values() for use, table, name in found}
        kind = (self.type or "").lower()
        self.is_slicer = "slicer" in kind or "datepicker" in kind
        self.is_html = "html" in kind
        self.is_data = bool(self.fields) and not self.is_slicer and self.type not in _DECORATION
        self.container = self.visual.get("visualContainerObjects")
        # The phone layout, when the visual folder has a mobile.json (set by the page reader).
        self.has_mobile = False
        self.mobile: Optional[Dict[str, Any]] = None

    def label(self) -> str:
        title = _first(self.container, "title", "text")
        named = f", '{title}'" if isinstance(title, str) and title else ""
        return f"{self.name} ({self.type or 'group'}{named})"

    def actions(self) -> Set[str]:
        """Action types of the links that are switched on (visualContainerObjects.visualLink
        "type": 'Back', 'PageNavigation', 'Bookmark', 'Drillthrough', 'WebUrl', ...)."""
        found: Set[str] = set()
        for bag in _properties(self.container, "visualLink"):
            action = _literal(bag.get("type"))
            if isinstance(action, str) and _literal(bag.get("show")) is not False:
                found.add(action)
        return found

    def tooltip_pages(self) -> List[str]:
        """Pages named as this visual's report page tooltip: "section" of visualTooltip and of
        visualHeaderTooltip, unless the tooltip is switched off or of type 'Default'."""
        found: List[str] = []
        for name in ("visualTooltip", "visualHeaderTooltip"):
            for bag in _properties(self.container, name):
                section = _literal(bag.get("section"))
                if (isinstance(section, str) and section and _literal(bag.get("show")) is not False
                        and _literal(bag.get("type")) != "Default"):
                    found.append(section)
        return found


def _model_index(model_fields: Optional[Mapping[str, Any]]) -> Optional[Dict[str, Set[str]]]:
    """{table (case-folded): {field (case-folded)}} from either accepted shape. Anything else
    is refused here with a ValueError, before a file is read."""
    if model_fields is None:
        return None
    shape = "{table: [field names]} or {table: {'columns': [...], 'measures': [...]}}"
    if not isinstance(model_fields, Mapping):
        raise ValueError(f"model_fields must be {shape}, not {type(model_fields).__name__}.")
    index: Dict[str, Set[str]] = {}
    for table, fields in model_fields.items():
        groups = list(fields.values()) if isinstance(fields, Mapping) else [fields]
        for group in groups:
            if group is not None and (isinstance(group, (str, bytes)) or not isinstance(group, Iterable)):
                raise ValueError(f"model_fields['{table}'] must list field names ({shape}); it holds "
                                 f"{type(group).__name__} {group!r}.")
        index[str(table).casefold()] = {str(n).casefold() for group in groups for n in (group or [])}
    return index


def _check_thresholds(**thresholds: Any) -> None:
    """Refuse a threshold that is not a number: None, text or a flag would fail far from here."""
    for name, value in thresholds.items():
        if isinstance(value, bool) or not isinstance(value, (int, float)) or value != value:
            raise ValueError(f"{name} must be a number, not {value!r}.")


# ---------------------------------------------------------------- the audit

def audit_report(report_definition_dir: PathLike, *, model_fields: Optional[Mapping[str, Any]] = None,
                 max_data_visuals_per_page: int = 12, max_visuals_per_page: int = 40,
                 min_element_size: int = 12, overlap_ratio: float = 0.25) -> Dict[str, Any]:
    """Audit a PBIR report definition without changing it.

    report_definition_dir: the .Report folder or its ``definition`` folder.
    model_fields: optional {table: [column and measure names]} (or {table: {"columns": [...],
        "measures": [...]}}). When given, every field a visual or a filter uses is checked
        against it.
    max_data_visuals_per_page / max_visuals_per_page: crowding thresholds (visible visuals).
    min_element_size: a visible element narrower or shorter than this many pixels is "tiny".
    overlap_ratio: two data visuals overlap when the shared area is at least this share of
        the smaller one.

    Returns {"summary": {...}, "findings": [...]}, findings ordered by severity, then page.
    Beside the counts, the summary describes what the report uses: "drillthrough_pages",
    "tooltip_pages", "filters", "visual_interactions", "mobile_layout", "bookmark_groups",
    "custom_visual_capabilities" (one entry per private visual package: what it declares,
    which Power BI API members its script names, and what follows for clicks, highlighting,
    drillthrough and tooltips), "store_visual_interactivity" (the same for each Deneb and
    HTML Content visual), "certification" and "path_lengths".
    Raises ValueError when the folder is not a report definition at all, when model_fields
    has another shape, or when a threshold is not a number.
    """
    _check_thresholds(max_data_visuals_per_page=max_data_visuals_per_page,
                      max_visuals_per_page=max_visuals_per_page, min_element_size=min_element_size,
                      overlap_ratio=overlap_ratio)
    index = _model_index(model_fields)
    report_dir, definition = _locate(report_definition_dir)
    audit = _Audit()

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
    opens_on = _check_pages_json(audit, definition, pages)
    bookmarks, bookmark_groups, group_members = _read_bookmarks(audit, definition)

    types: Counter = Counter()
    versions: Dict[str, Counter] = defaultdict(Counter)
    colours: Counter = Counter()
    colours_by_page: Counter = Counter()
    theme_refs = 0
    nav_targets: Set[str] = set()
    slicers: Dict[Tuple[str, str], List[Tuple[str, _Visual]]] = defaultdict(list)
    filters: Dict[str, Any] = {"report": 0, "page": 0, "visual": 0, "hidden": 0, "locked": 0,
                               "restricting": []}
    interactions: Dict[str, Any] = {"total": 0, "pages": 0, "by_type": Counter()}
    phone: Dict[str, int] = {}
    page_kinds = {p["name"]: p["kind"] for p in pages}
    for kind, version in [_schema_version(report)] + [_schema_version(p["doc"]) for p in pages]:
        if kind:
            versions[kind][version] += 1

    _check_filters(audit, filters, "report", report, index=index)
    for page in pages:
        name, visuals = page["name"], page["visuals"]
        by_name = {v.name: v for v in visuals}
        groups = {v.name for v in visuals if v.is_group}
        hidden = page["hidden"] = _hidden_names(visuals, by_name)
        width, height = _page_size(page["doc"])
        _check_filters(audit, filters, "page", page["doc"], name, index=index)
        for v in visuals:
            types[v.type or "(group)"] += 1
            kind, version = _schema_version(v.doc)
            if kind:
                versions[kind][version] += 1
            else:
                audit.add("STRUCT_SCHEMA", "warning", "The file has no recognisable $schema.", name, v.name)
            kind, version = _schema_version(v.mobile)
            if kind:
                versions[kind][version] += 1
            _check_container(audit, name, v, groups, width, height)
            if index is not None:
                _check_fields(audit, name, v, index)
            # A visual's own filters are checked against the model with its other fields, above.
            _check_filters(audit, filters, "visual", v.doc, name, v)
            _check_links(audit, name, v, page_kinds, bookmarks, bookmark_groups, nav_targets)
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
        _check_interactions(audit, interactions, name, page["doc"], visuals, hidden)
        placed = _check_mobile(audit, name, visuals, hidden)
        if placed:
            phone[name] = placed

    _check_schema_mix(audit, versions)
    certification = _check_custom_visuals(audit, types, public, private)
    packages = _read_packages(audit, private_dir, private, types)
    _check_package_behaviour(audit, pages, packages)
    store_visuals = _check_store_visuals(audit, pages)
    drillthrough = _check_drillthrough(audit, pages, opens_on)
    tooltips = _check_tooltip_pages(audit, pages)
    if filters["restricting"]:
        locked = sum(1 for f in filters["restricting"] if f["locked"])
        listed = "; ".join(_filter_place(f) for f in filters["restricting"][:8])
        audit.add("GOV_HIDDEN_FILTER", "info",
                  f"{len(filters['restricting'])} hidden filter(s) restrict what readers see without showing "
                  f"in the filter pane ({locked} also locked): {listed}"
                  f"{' ...' if len(filters['restricting']) > 8 else ''}.", filters=filters["restricting"])
    paths = _check_path_lengths(audit, report_dir, definition)
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
        "drillthrough_pages": drillthrough,
        "tooltip_pages": tooltips,
        "filters": {"report": filters["report"], "page": filters["page"], "visual": filters["visual"],
                    "hidden": filters["hidden"], "locked": filters["locked"],
                    "hidden_with_condition": filters["restricting"]},
        "visual_interactions": {"total": interactions["total"], "pages": interactions["pages"],
                                "by_type": dict(interactions["by_type"].most_common())},
        # An empty "pages" means no page has a phone layout: the mobile app then shows every
        # page in its desktop layout. That is a fact about the report, not a defect to fix.
        "mobile_layout": {"pages": phone, "visuals": sum(phone.values()),
                          "canvas_width": _PHONE_CANVAS_WIDTH},
        "bookmark_groups": group_members,
        "custom_visual_capabilities": [packages[guid] for guid in sorted(packages)],
        "store_visual_interactivity": store_visuals,
        "certification": certification,
        "path_lengths": paths,
        "limits": [
            "Read from the files only: nothing was opened or rendered in Power BI.",
            "Visuals inside a group are left out of the overlap and outside-the-page checks (their "
            "coordinates are read relative to the group by some versions and to the page by others).",
            "A missing title object means the theme default applies, which this audit cannot see; only "
            "titles that are explicitly switched off are reported.",
            "Whether a store visual is certified cannot be read from a report definition. It is stated "
            "for Deneb and for the two editions of HTML Content, from their own documentation.",
            "What a private custom visual can do is read from its capabilities, its data roles and the "
            "Power BI API members named in the code of its script (not in a comment or a string). The "
            "script was not run: a call that is present but never reached still counts, and a member "
            "whose name is put together at run time (host['create' + 'SelectionManager']) is missed.",
            "Deneb and HTML Content are judged by the settings stored in each visual and by their "
            "documentation; whether a Deneb specification keeps the row identity that cross-filtering "
            "needs cannot be read from the settings.",
            "Whether a drillthrough page can be reached is not judged when its field is a hierarchy "
            "level, when a page that leads there uses a field parameter or a hierarchy of the same "
            "table, or when the page accepts cross-report drillthrough.",
            "The phone canvas width is Microsoft's documented 323 points; the files do not state it, and "
            "the canvas has no fixed height.",
            "Path lengths are judged on Windows only, where the report is now; the same report in a "
            "deeper folder can cross the limit.",
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
            # The phone layout is one mobile.json per visual, beside visual.json.
            if (vfolder / "mobile.json").is_file():
                visual.has_mobile = True
                mobile = audit.load(vfolder / "mobile.json", name, visual.name)
                # None when it could not be read (already reported); an empty layout when it
                # holds something other than an object.
                visual.mobile = None if mobile is None else _dict(mobile)
            visuals.append(visual)
        out.append({"name": name, "folder": folder.name, "doc": doc, "visuals": visuals,
                    "kind": _page_kind(doc), "hidden": set()})
    if renamed:
        audit.add("STRUCT_NAME", "info",
                  f"{len(renamed)} folder(s) are named differently from the 'name' inside them (for example "
                  f"{renamed[0]}). The format allows it and Power BI uses the name inside; a script that "
                  "looks objects up by folder will not find them.")
    return out


def _check_pages_json(audit: _Audit, definition: Path,
                      pages: List[Dict[str, Any]]) -> Tuple[Optional[str], str]:
    """Check pages.json against the pages, put the pages in report order, and return the page
    the report opens on with the key that says so.

    pagesMetadata schema: landingPageName, when set, is the page "the report will always open
    on ... for all users, overriding activePageName"; otherwise it opens on activePageName,
    and without that on the first page in pageOrder. A hidden page opens just the same
    (Microsoft Learn, "Set the landing page for a Power BI report" and "Use Report view",
    Hide report pages).
    """
    path = definition / "pages" / "pages.json"
    if not path.is_file():
        if (definition / "pages").is_dir():
            audit.add("STRUCT_PAGES", "error", "pages.json is missing from the pages folder.")
        return None, ""
    meta = audit.load(path)
    if not isinstance(meta, dict):
        return None, ""
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
    active, landing = meta.get("activePageName"), meta.get("landingPageName")
    if isinstance(active, str) and active not in names:
        audit.add("STRUCT_PAGES", "error", f"activePageName is '{active}', which is not a page.")
    if isinstance(landing, str) and landing not in names:
        audit.add("STRUCT_PAGES", "error", f"landingPageName is '{landing}', which is not a page.")
    # Put the pages in report order, so findings read the way the report does.
    rank = {n: i for i, n in enumerate(order)}
    pages.sort(key=lambda p: (rank.get(p["name"], len(rank)), p["name"]))
    for key, value in (("landingPageName", landing), ("activePageName", active)):
        if isinstance(value, str) and value in names:
            return value, key
    return next((n for n in order if n in names), None), "the first page of pageOrder"


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


def _check_links(audit: _Audit, page: str, v: _Visual, page_names: Mapping[str, Optional[str]],
                 bookmarks: Mapping[str, Any], bookmark_groups: Set[str], nav_targets: Set[str]) -> None:
    """The actions of a button, shape or image. ``page_names`` maps each page to its kind
    ('Drillthrough', 'Tooltip' or None)."""
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
        elif action == "Drillthrough":
            # A drillthrough button names its destination in "drillthroughSection" (VisualLink in
            # the visualConfiguration schema); the destination is a drillthrough page of this report.
            target = _literal(bag.get("drillthroughSection"))
            if isinstance(target, str) and target not in page_names:
                audit.add("DESIGN_BROKEN_LINK", "error",
                          f"Its drillthrough action points at '{target}', which is not a page.", page, v.name)
            elif isinstance(target, str) and page_names[target] != "Drillthrough":
                audit.add("DESIGN_BROKEN_LINK", "warning",
                          f"Its drillthrough action points at '{target}', which is a page but not a "
                          "drillthrough page.", page, v.name)
            elif "drillthroughSection" not in bag:
                audit.add("DESIGN_BROKEN_LINK", "warning", "Its drillthrough action has no destination.",
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


def _check_custom_visuals(audit: _Audit, types: Counter, public: List[str],
                          private: List[str]) -> Dict[str, List[str]]:
    """Registration and governance of the custom visuals in use. Returns which of them are
    certified: a private visual never is; a store visual is "unknown" unless its vendor's
    documentation says which (Deneb, HTML Content Secure and HTML Content Regular)."""
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
    unknown = [t for t in in_use_public if t not in _CERTIFIED]
    if in_use_public:
        stated = {True: ", certified", False: ", not certified"}
        audit.add("GOV_PUBLIC_VISUAL", "info",
                  "Store custom visuals are in use: "
                  + ", ".join(f"{t} ({types[t]}{stated.get(_CERTIFIED.get(t), '')})" for t in in_use_public)
                  + "."
                  + (" Certified or not is what the visual's own documentation states for that id."
                     if len(unknown) < len(in_use_public) else "")
                  + (f" Whether {_some(unknown)} is certified cannot be read from the report files."
                     if unknown else ""))
    html_types = sorted(t for t in types if t and "html" in t.lower())
    if html_types:
        other = [t for t in html_types if t not in _HTML_CONTENT_EDITIONS]
        audit.add("GOV_HTML_VISUAL", "info",
                  "Visuals that render HTML supplied by a measure are in use: "
                  + ", ".join(f"{t} ({types[t]})" for t in html_types) + "."
                  + (" HTML Content Regular (the id ending 6855) is not certified and also runs the scripts "
                     "and inline event handlers in that HTML." if _HTML_REGULAR in html_types else "")
                  + (" HTML Content Secure (the id ending 6865) is certified and removes scripts and inline "
                     "event handlers." if _HTML_SECURE in html_types else "")
                  + (f" Whether {_some(other)} runs scripts cannot be read from the report files."
                     if other else ""))
    uncertified = [t for t in in_use_public if _CERTIFIED.get(t) is False]
    return {"certified": [t for t in in_use_public if _CERTIFIED.get(t) is True],
            "not_certified": sorted(in_use_private + uncertified), "unknown": unknown}


def _read_bookmarks(audit: _Audit, definition: Path
                    ) -> Tuple[Dict[str, Dict[str, Any]], Set[str], List[Dict[str, Any]]]:
    """(bookmarks by name, every name a group answers to, the groups with their bookmarks)."""
    folder = definition / "bookmarks"
    bookmarks: Dict[str, Dict[str, Any]] = {}
    groups: Set[str] = set()
    members: List[Dict[str, Any]] = []
    if not folder.is_dir():
        return bookmarks, groups, members
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
        return bookmarks, groups, members
    meta = audit.load(meta_path)
    # bookmarks.json items are either {"name"} (a bookmark on its own) or a group:
    # {"name", "displayName", "children": [bookmark names]}, all three required, and every name
    # "unique across the report" (bookmarksMetadata schema). A required property that is missing
    # is one of the errors that stop Desktop loading a report (Microsoft Learn, "Power BI
    # Desktop project report folder", PBIR format: blocking errors).
    listed: Counter = Counter()
    group_of: Dict[str, str] = {}
    group_names: Counter = Counter()
    for item in _list(_dict(meta).get("items")):
        if not isinstance(item, dict):
            continue
        children = item.get("children")
        name, shown = _text(item.get("name")), _text(item.get("displayName"))
        if isinstance(children, list):
            if isinstance(item.get("name"), str):
                groups.add(item["name"])
            if isinstance(item.get("displayName"), str):
                groups.add(item["displayName"])
            label = shown or name or "(unnamed)"
            inside = [c for c in children if isinstance(c, str)]
            members.append({"name": name, "displayName": shown, "bookmarks": inside})
            missing = [key for key, value in (("name", name), ("displayName", shown)) if not value]
            if missing:
                audit.add("STRUCT_BOOKMARK_GROUP", "error",
                          f"The bookmark group '{label}' has no {' and no '.join(missing)}, which "
                          "bookmarks.json requires of every group.")
            if name:
                group_names[name] += 1
            if not inside:
                audit.add("STRUCT_BOOKMARK_GROUP", "info", f"The bookmark group '{label}' holds no bookmark.")
            for child in inside:
                listed[child] += 1
                group_of.setdefault(child, label)
        elif name:
            listed[name] += 1
        else:
            audit.add("STRUCT_BOOKMARK_GROUP", "error",
                      "An entry of bookmarks.json has neither a name nor children, so it is neither a "
                      "bookmark nor a group.")
    for name in sorted(set(listed) - set(bookmarks)):
        lister = f"The bookmark group '{group_of[name]}'" if name in group_of else "bookmarks.json"
        audit.add("DESIGN_BROKEN_LINK", "error", f"{lister} lists '{name}', which has no bookmark file.")
    for name in sorted(n for n, count in listed.items() if count > 1):
        audit.add("STRUCT_BOOKMARK_GROUP", "warning",
                  f"bookmarks.json lists the bookmark '{name}' {listed[name]} times. A bookmark belongs in "
                  "one place, on its own or in one group; which entry Power BI keeps is not defined.")
    for name in sorted(n for n, count in group_names.items() if count > 1):
        audit.add("STRUCT_BOOKMARK_GROUP", "warning",
                  f"{group_names[name]} bookmark groups have the name '{name}'. A group name has to be "
                  "unique in the report; a bookmark navigator that shows the group cannot tell them apart.")
    for name in sorted(n for n in group_names if n in bookmarks):
        audit.add("STRUCT_BOOKMARK_GROUP", "info",
                  f"A bookmark group and a bookmark are both named '{name}'; the schema asks for names "
                  "that are unique across the report.")
    for name in sorted(set(bookmarks) - set(listed)):
        audit.add("STRUCT_FILE", "info", f"The bookmark file '{name}' is not listed in bookmarks.json.")
    return bookmarks, groups, members


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
    normal = [p for p in pages if p["kind"] is None]
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


def _some(items: List[str], limit: int = 6) -> str:
    """The first few of a list, for a message that must stay readable."""
    return ", ".join(items[:limit]) + (" ..." if len(items) > limit else "")


# ---------------------------------------------------------------- filters

def _filter_place(entry: Mapping[str, Any]) -> str:
    if entry["scope"] == "report":
        return f"report: {entry['filter']}"
    if entry["scope"] == "page":
        return f"page {entry['page']}: {entry['filter']}"
    return f"{entry['page']} / {entry['visual']}: {entry['filter']}"


def _check_filters(audit: _Audit, stats: Dict[str, Any], scope: str, doc: Any, page: Optional[str] = None,
                   visual: Optional[_Visual] = None, index: Optional[Mapping[str, Set[str]]] = None) -> None:
    """The filters of one report, page or visual. Each is {"name", "field", "type", "filter",
    "howCreated", "isHiddenInViewMode", "isLockedInViewMode", ...} (filterConfiguration schema).

    ``index`` is passed for the report and the pages only: a visual's filters are checked
    against the model together with its other fields.
    """
    filters = _filters(doc)
    if not filters:
        return
    owner = {"report": "The report", "page": "The page", "visual": "The visual"}[scope]
    visual_name = visual.name if visual is not None else None
    stats[scope] += len(filters)
    counts = Counter(_text(f.get("name")) or None for f in filters)
    repeated = sorted(n for n, count in counts.items() if n and count > 1)
    if repeated:
        audit.add("STRUCT_FILTER_NAME", "warning",
                  f"{owner} has more than one filter named {_some([repr(n) for n in repeated])}. Bookmarks "
                  "and drillthrough bindings refer to a filter by its name, so they cannot tell them apart.",
                  page, visual_name)
    if counts[None]:
        # "name" is the one property a filter is required to have, and a missing required
        # property stops Desktop loading the report (see the note on bookmarks.json).
        audit.add("STRUCT_FILTER_NAME", "error",
                  f"{owner} has {counts[None]} filter(s) with no name, which every filter is required to "
                  "have.", page, visual_name)
    # A filter that a drillthrough or tooltip binding fills in at run time has no condition by design.
    bound = {_text(_dict(p).get("boundFilter"))
             for p in _list(_dict(_dict(doc).get("pageBinding")).get("parameters"))} - {None}
    own = {(t.casefold(), f.casefold()) for _, t, f in visual.fields} if visual is not None else set()
    dead: List[str] = []
    for flt in filters:
        refs = list(_field_refs(flt.get("field")))
        label = _text(flt.get("displayName")) or _field_label(refs) or _text(flt.get("name")) or "(unnamed)"
        hidden, locked = flt.get("isHiddenInViewMode") is True, flt.get("isLockedInViewMode") is True
        stats["hidden"] += hidden
        stats["locked"] += locked
        for kind, table, field in refs if index is not None else []:
            used = f"A {scope}-level filter uses {kind} {table}[{field}]"
            if table.casefold() not in index:
                audit.add("STRUCT_FIELD", "error", f"{used}; the model has no table '{table}'.", page)
            elif field.casefold() not in index[table.casefold()]:
                audit.add("STRUCT_FIELD", "error", f"{used}, which is not in the model.", page)
        if not hidden:
            continue
        if _has_condition(flt):
            stats["restricting"].append({"scope": scope, "page": page, "visual": visual_name, "filter": label,
                                         "locked": locked})
        # A hidden card with no condition on a field the visual shows is a way to keep that
        # card out of the reader's filter pane, so it is left alone.
        elif (flt.get("howCreated") != "Drillthrough" and _text(flt.get("name")) not in bound
              and not (refs and all((t.casefold(), f.casefold()) in own for _, t, f in refs))):
            dead.append(label)
    if dead:
        audit.add("DESIGN_FILTER_DEAD", "info",
                  f"{owner} has {len(dead)} hidden filter(s) with no condition ({_some(dead)}): they filter "
                  "nothing and no reader can see or set them.", page, visual_name)


# ---------------------------------------------------------------- interactions

def _check_interactions(audit: _Audit, stats: Dict[str, Any], page: str, doc: Mapping[str, Any],
                        visuals: List[_Visual], hidden: Set[str]) -> None:
    """page.json "visualInteractions": [{"source", "target", "type"}], where the type is one of
    Default, DataFilter, HighlightFilter, NoFilter (page schema, VisualInteraction)."""
    entries = [e for e in _list(doc.get("visualInteractions")) if isinstance(e, dict)]
    if not entries:
        return
    stats["total"] += len(entries)
    stats["pages"] += 1
    names = {v.name for v in visuals}
    pairs: Dict[Tuple[str, str], List[str]] = defaultdict(list)
    stray = 0
    gone: List[str] = []
    selfs: List[str] = []
    unknown: List[str] = []
    for entry in entries:
        source, target = _text(entry.get("source")), _text(entry.get("target"))
        kind = _text(entry.get("type")) or "(none)"
        stats["by_type"][kind] += 1
        if kind not in _INTERACTION_TYPES and kind not in unknown:
            unknown.append(kind)
        missing = [n or "(none)" for n in (source, target) if n not in names]
        if missing:
            stray += 1
            gone.extend(n for n in missing if n not in gone)
        elif source == target:
            selfs.append(source)
        else:
            pairs[(source, target)].append(kind)
    if stray:
        audit.add("STRUCT_INTERACTION", "warning",
                  f"{stray} of {len(entries)} visual interaction(s) name a visual that is not on the page "
                  f"({_some(gone)}), so they change nothing.", page)
    if unknown:
        audit.add("STRUCT_INTERACTION", "warning",
                  f"Visual interaction type(s) {_some([repr(k) for k in unknown])} are not defined by the "
                  f"page schema, which allows {', '.join(_INTERACTION_TYPES)}.", page)
    clashes = sorted(f"{s} -> {t} ({', '.join(sorted(set(kinds)))})" for (s, t), kinds in pairs.items()
                     if len(set(kinds)) > 1)
    if clashes:
        audit.add("STRUCT_INTERACTION", "warning",
                  f"{len(clashes)} pair(s) of visuals are listed more than once with different types: "
                  f"{_some(clashes)}. Which entry applies is not defined.", page)
    repeats = sorted(f"{s} -> {t}" for (s, t), kinds in pairs.items()
                     if len(kinds) > 1 and len(set(kinds)) == 1)
    if repeats:
        audit.add("DESIGN_INTERACTION", "info",
                  f"{len(repeats)} visual interaction(s) are listed more than once with the same type: "
                  f"{_some(repeats)}.", page)
    if selfs:
        audit.add("DESIGN_INTERACTION", "info",
                  f"{len(selfs)} visual interaction(s) have the same visual as source and target "
                  f"({_some(sorted(set(selfs)))}); a visual does not filter itself.", page)

    shown = [v for v in visuals if not v.is_group and v.name not in hidden]
    data = [v for v in shown if v.is_data]
    for slicer in (v for v in shown if v.is_slicer and v.fields):
        if data and all(set(pairs.get((slicer.name, d.name), ())) == {"NoFilter"} for d in data):
            synced = isinstance(slicer.visual.get("syncGroup"), dict)
            audit.add("DESIGN_INTERACTION_OFF", "info",
                      f"The slicer {slicer.label()} is switched off (NoFilter) for all {len(data)} data "
                      "visual(s) on its page, so a choice in it changes none of them."
                      + (" It is in a sync group, so it can still act on other pages." if synced else ""),
                      page, slicer.name)


# ---------------------------------------------------------------- phone layout

def _check_mobile(audit: _Audit, page: str, visuals: List[_Visual], hidden: Set[str]) -> int:
    """The phone layout of one page; returns how many of its visuals are on the phone canvas.

    A visual is in the phone layout when its folder has a mobile.json, which holds
    {"$schema", "position": {"x", "y", "z", "width", "height", "tabOrder"}} in phone canvas
    coordinates ("position" is required: visualContainerMobileState schema).
    """
    placed = [v for v in visuals if v.has_mobile]
    if not placed:
        return 0
    for v in placed:
        if v.mobile is None:  # unreadable, and reported when it was loaded
            continue
        position = v.mobile.get("position")
        if not isinstance(position, dict):
            # A required property that is missing stops Desktop loading the report (see the
            # note on bookmarks.json).
            audit.add("STRUCT_MOBILE_POSITION", "error",
                      "mobile.json has no position, which the file is required to have: the phone layout "
                      "places the visual nowhere.", page, v.name)
            continue
        x, y = _number(position.get("x")), _number(position.get("y"))
        w, h = _number(position.get("width")), _number(position.get("height"))
        if w <= 0 or h <= 0:
            audit.add("STRUCT_MOBILE_POSITION", "warning",
                      f"Its phone layout size is {w:g} x {h:g}; a visual needs a width and a height.",
                      page, v.name)
        elif x < 0 or y < 0 or x + w > _PHONE_CANVAS_WIDTH + 1:
            audit.add("STRUCT_MOBILE_POSITION", "warning",
                      f"In the phone layout it is at ({x:g}, {y:g}) and {w:g} wide, which reaches outside "
                      f"the phone canvas (from 0 to {_PHONE_CANVAS_WIDTH:g} across, and from 0 downwards).",
                      page, v.name)
    left_out = [v for v in visuals
                if not v.is_group and v.name not in hidden and v.is_data and not v.has_mobile]
    if left_out:
        audit.add("DESIGN_MOBILE_PARTIAL", "info",
                  f"The page has a phone layout with {len(placed)} visual(s), and {len(left_out)} visible "
                  "data visual(s) are not in it, so a phone does not show them: "
                  f"{_some([v.label() for v in left_out], 8)}.", page, visuals=[v.name for v in left_out])
    return len(placed)


# ---------------------------------------------------------------- drillthrough and tooltip pages

def _drill_reach(pages: List[Dict[str, Any]], bound: Mapping[str, List[Dict[str, Any]]],
                 open_to_others: Set[str]) -> Dict[str, Optional[bool]]:
    """For each drillthrough page in ``bound`` (name -> its binding): True when a reader can
    drill through to it, False when nothing leads there, None when the files cannot tell.

    The context menu offers a drillthrough page from a visual that holds its field ("The
    field must exist in both the source visual and the drillthrough filter well") and holds
    it the way the binding asks: not summarized, unless the drillthrough is set for the
    summarized field ("Check that the field isn't aggregated ... in the source visual";
    both from Microsoft Learn, "Drillthrough in Power BI reports"). Slicers are not counted
    as sources.

    Only a page a reader can be on leads anywhere: an ordinary page, or a drillthrough page
    that is itself within reach, so two drillthrough pages that lead only to each other are
    both out of reach. A tooltip page is not interactive. What stays unknown: a page whose
    field is a hierarchy level or of some other kind, a page other reports can drill through
    to (``open_to_others``), and a source that uses a field parameter or a hierarchy of the
    same table, whose columns only the model can name.
    """
    wanted = {name: {e["use"] for e in entries if e["use"]} for name, entries in bound.items()}
    by_name = {p["name"]: p for p in pages}

    def leads(source: str, target: str) -> Optional[bool]:
        tables = {table for _, table, _ in wanted[target]}
        unknown = False
        for v in by_name[source]["visuals"]:
            if v.is_slicer:
                continue
            if v.uses & wanted[target]:
                return True
            unknown = unknown or v.field_parameters or any(
                use == "hierarchy" and table in tables for use, table, _ in v.uses)
        return None if unknown else False

    sure = {p["name"] for p in pages if p["kind"] is None}
    maybe = sure | {name for name, entries in bound.items()
                    if name in open_to_others or any(e["use"] is None for e in entries)}
    grew = True
    while grew:
        grew = False
        for target in bound:
            if target not in sure and any(leads(s, target) is True for s in sorted(sure - {target})):
                sure.add(target)
                maybe.add(target)
                grew = True
            if target not in maybe and any(leads(s, target) is not False for s in sorted(maybe - {target})):
                maybe.add(target)
                grew = True
    return {name: True if name in sure else None if name in maybe else False for name in bound}


def _check_drillthrough(audit: _Audit, pages: List[Dict[str, Any]],
                        opens_on: Tuple[Optional[str], str]) -> List[Dict[str, Any]]:
    """Drillthrough pages: page "type" 'Drillthrough', a pageBinding of that type whose
    parameters carry the drillthrough fields, and page filters marked howCreated
    'Drillthrough' (page and filterConfiguration schemas; pages Desktop saves have all three).
    Desktop adds a Back button with the first drillthrough field, and Microsoft's guidance is
    to keep it ("Use report page drillthrough", Recommendations)."""
    drill = [p for p in pages if p["kind"] == "Drillthrough"]
    bound = {p["name"]: _binding(p["doc"]) for p in drill}
    cross_report = {p["name"] for p in drill
                    if _dict(p["doc"].get("pageBinding")).get("referenceScope") == "CrossReport"}
    # A page with no binding at all has nothing to be reached by.
    reach = _drill_reach(pages, {name: entries for name, entries in bound.items() if entries}, cross_report)
    out: List[Dict[str, Any]] = []
    for page in drill:
        doc, name = page["doc"], page["name"]
        title = _text(doc.get("displayName")) or name
        labels = _labels(bound[name])
        shown = [v for v in page["visuals"] if v.name not in page["hidden"]]
        actions: Set[str] = set()
        for v in shown:
            actions |= v.actions()
        reachable = reach.get(name)
        if not bound[name]:
            audit.add("DESIGN_DRILL_FIELD", "warning",
                      f"The page '{title}' is set up for drillthrough but has no drillthrough field, so "
                      "nothing can drill through to it.", name)
        elif reachable is False:
            audit.add("DESIGN_DRILL_UNREACHABLE", "warning",
                      f"No visual on a page readers can be on holds {_some(labels)}, the drillthrough "
                      f"field(s) of '{title}', the way drillthrough needs it (in a visual that is not a "
                      "slicer, and summarized only if the drillthrough field is), so the context menu "
                      "never offers this page.", name)
        if "Back" not in actions:
            # Without a Back button a reader can still leave through a page navigation button.
            other = "PageNavigation" in actions or any(v.type == "pageNavigator" for v in shown)
            audit.add("DESIGN_DRILL_BACK", "info" if other else "warning",
                      f"The drillthrough page '{title}' has no Back button (a button whose action is Back)"
                      + ("; readers leave through its other navigation and do not return to where they "
                         "drilled from." if other else " and no other navigation button, so a reader who "
                         "drills through has no button to return with."), name)
        if name == opens_on[0]:
            audit.add("DESIGN_DRILL_LANDING", "warning",
                      f"The report opens on '{title}' ({opens_on[1]} in pages.json), which is a drillthrough "
                      "page: readers start on a detail page that no drillthrough has filtered.", name)
        out.append({"page": name, "displayName": title, "fields": labels, "back_button": "Back" in actions,
                    "hidden": doc.get("visibility") == "HiddenInViewMode",
                    "cross_report": name in cross_report, "reachable": reachable})
    return out


def _check_tooltip_pages(audit: _Audit, pages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Tooltip pages: page "type" 'Tooltip' (page schema), shown by a visual whose
    visualTooltip names the page in "section", or matched automatically through the fields in
    the page's binding parameters. Microsoft's guidance ("Extend visuals with report page
    tooltips", Recommendations): keep the page small, and always hide it. A visual whose
    tooltip page is not available falls back to the default tooltip ("Visual tooltips in
    Power BI", Options)."""
    by_name = {p["name"]: p for p in pages}
    used: Counter = Counter()
    for page in pages:
        for v in page["visuals"]:
            for section in v.tooltip_pages():
                if section == _TOOLTIP_AUTO:
                    continue
                used[section] += 1
                target = by_name.get(section)
                if target is None:
                    audit.add("DESIGN_TOOLTIP_LINK", "warning",
                              f"Its report page tooltip is '{section}', which is not a page, so the default "
                              "tooltip shows instead.", page["name"], v.name)
                elif target["kind"] != "Tooltip":
                    title = _text(target["doc"].get("displayName")) or section
                    audit.add("DESIGN_TOOLTIP_LINK", "warning",
                              f"Its report page tooltip is the page '{title}', which is not a tooltip page "
                              "(its type is not 'Tooltip'); Power BI only uses tooltip pages as tooltips.",
                              page["name"], v.name)
    out: List[Dict[str, Any]] = []
    for page in pages:
        if page["kind"] != "Tooltip":
            continue
        doc, name = page["doc"], page["name"]
        title = _text(doc.get("displayName")) or name
        width, height = _page_size(doc)
        hidden = doc.get("visibility") == "HiddenInViewMode"
        # Tooltip fields, which let Power BI pick the page by itself, are the parameters of
        # the page's binding. An ordinary page filter is not one.
        matched = [p for p in _list(_dict(doc.get("pageBinding")).get("parameters")) if isinstance(p, dict)]
        auto = _labels(_binding(doc)) if matched else []
        if not hidden:
            audit.add("DESIGN_TOOLTIP_VISIBLE", "warning",
                      f"The tooltip page '{title}' is not hidden, so it shows among the report's pages and "
                      "readers can open it on its own.", name)
        if width > 2 * _TOOLTIP_WIDTH or height > 2 * _TOOLTIP_HEIGHT:
            audit.add("DESIGN_TOOLTIP_SIZE", "warning",
                      f"The tooltip page '{title}' is {width:g} x {height:g}, more than twice the "
                      f"{_TOOLTIP_WIDTH:g} x {_TOOLTIP_HEIGHT:g} tooltip size; it will cover the visual it "
                      "explains.", name)
        if not used[name] and not matched:
            audit.add("DESIGN_TOOLTIP_UNUSED", "warning",
                      f"No visual uses the tooltip page '{title}', and it has no tooltip fields to be "
                      "matched by automatically, so it never shows.", name)
        out.append({"page": name, "displayName": title, "size": f"{width:g}x{height:g}", "hidden": hidden,
                    "used_by": used[name], "auto_fields": auto})
    return out


# ---------------------------------------------------------------- what custom visuals can do

def _yes(value: Optional[bool]) -> str:
    return "yes" if value else "no"


def _read_package(audit: _Audit, folder: Path, guid: str, uses: int) -> Dict[str, Any]:
    """What one unpacked private visual declares and calls.

    <guid>/resources/<guid>.pbiviz.json holds the visual's metadata, its "capabilities" (the
    capabilities.json it was built from; its keys are described in Microsoft Learn,
    "Capabilities and properties of Power BI visuals") and its script in content.js.
    "behaviour" is what follows from the two together, and stays None when the package
    cannot be read.

    The data roles decide what there is to click. A data point comes from a column, so a
    visual whose roles all take measures ("column_roles" empty) has nothing to select and
    nothing to drill through from, whatever its script calls.
    """
    info: Dict[str, Any] = {"guid": guid, "name": guid, "version": None, "api_version": None, "visuals": uses,
                            "data_roles": {}, "column_roles": [], "declares": None, "script": None,
                            "script_read": None, "assembled_calls": False, "privileges": None,
                            "behaviour": None, "unread": None}
    resources = folder / "resources"
    found = sorted(resources.glob("*.pbiviz.json")) if resources.is_dir() else []
    path = next((p for p in found if p.name == f"{guid}.pbiviz.json"), found[0] if found else None)
    if path is None:
        info["unread"] = "the package has no resources/*.pbiviz.json"
        return info
    try:
        raw = path.read_bytes()
        meta = _parse_json(raw[len(_BOM):] if raw.startswith(_BOM) else raw)
    except OSError as e:
        info["unread"] = f"{path.name} cannot be read ({e.strerror or e})"
        return info
    except ValueError as e:
        audit.add("STRUCT_JSON", "error", f"{path.name} is not valid JSON: {e}.")
        info["unread"] = f"{path.name} is not valid JSON"
        return info
    meta = _dict(meta)
    described = _dict(meta.get("visual"))
    info["name"] = _text(described.get("displayName")) or _text(described.get("name")) or guid
    info["version"] = _text(described.get("version"))
    info["api_version"] = _text(meta.get("apiVersion"))
    if not isinstance(meta.get("capabilities"), dict):
        info["unread"] = f"{path.name} has no capabilities"
        return info
    caps = meta["capabilities"]
    for role in _list(caps.get("dataRoles")):
        kind = _dict(role).get("kind")
        if isinstance(_dict(role).get("name"), str):
            # The kind is written as a name, or as its number in the VisualDataRoleKind enum.
            numbered = isinstance(kind, (int, float)) and not isinstance(kind, bool)
            info["data_roles"][role["name"]] = kind if isinstance(kind, str) else (
                _ROLE_KINDS.get(kind) if numbered else None)
    columns = info["column_roles"] = [name for name, kind in info["data_roles"].items()
                                      if (kind or "").lower() in _COLUMN_ROLES]
    tooltips = caps.get("tooltips")
    declares = info["declares"] = {
        "highlight": caps.get("supportsHighlight") is True,
        "tooltips": isinstance(tooltips, dict),
        "report_page_tooltips": _dict(_dict(tooltips).get("supportedTypes")).get("canvas") is True,
        "drilldown": bool(_list(_dict(caps.get("drilldown")).get("roles"))),
        "keyboard_focus": caps.get("supportsKeyboardFocus") is True,
        "landing_page": caps.get("supportsLandingPage") is True,
        "multi_visual_selection": caps.get("supportsMultiVisualSelection") is True,
        "sync_slicers": caps.get("supportsSynchronizingFilterState") is True,
    }
    if isinstance(caps.get("privileges"), list):
        # WebAccess lists the addresses it may reach; AADAuthentication maps clouds to audiences.
        info["privileges"] = [{"name": _text(p.get("name")), "essential": p.get("essential") is True,
                               "parameters": _list(p.get("parameters")) or _dict(p.get("parameters")) or []}
                              for p in caps["privileges"] if isinstance(p, dict)]
    script = _text(_dict(meta.get("content")).get("js"))
    if script is None:
        info["unread"] = f"{path.name} holds no script (content.js)"
        return info
    code, split = _script_code(script)
    info["script_read"] = "code" if split else "text"
    # What such a script does not seem to call, it may still call: nothing is concluded from
    # a name that is missing.
    info["assembled_calls"] = split and bool(_JS_ASSEMBLED_CALL.search(code))
    calls = info["script"] = {flag: member in code for flag, member in _SCRIPT_CALLS.items()}
    info["behaviour"] = {
        # A click filters other visuals through a selection (selection manager plus selection
        # ids for the data points) or, for a slicer-like visual, through the filter API.
        "cross_filter": bool(columns) and ((calls["selection_manager"] and calls["selection_ids"])
                                           or calls["filter_api"]),
        "cross_highlight": declares["highlight"] and calls["reads_highlights"],
        # Drillthrough is offered in the context menu, for the data point it is opened on.
        "drillthrough": bool(columns) and calls["context_menu"] and calls["selection_ids"],
        "tooltips": calls["tooltip_service"],
        "report_page_tooltips": declares["report_page_tooltips"] and calls["tooltip_service"],
    }
    return info


def _script_code(script: str) -> Tuple[str, bool]:
    """(the code of ``script``, True): comments, strings, template literals and regular
    expression literals set aside, so a member that is only mentioned in one of them does not
    count as called. (the whole text, False) when the script could not be split reliably.

    The split is checked on its result: in code that was split correctly every bracket that
    opens also closes. Where it went wrong (a regular expression taken for a division hands a
    quote to the code, and from there strings and code change places) the brackets stop
    matching, and the whole text is searched instead.
    """
    code = _JS_NOT_CODE.sub(r"\g<key>\g<before> ", script)
    balanced = all(code.count(opened) == code.count(closed) for opened, closed in ("()", "[]", "{}"))
    return (code, True) if balanced else (script, False)


def _privilege_text(privileges: Optional[List[Dict[str, Any]]], api_version: Optional[str]) -> str:
    if privileges is None:
        old = re.match(r"(\d+)\.(\d+)", api_version or "")
        # "From API v4.6.0, privileges must be specified ... In earlier versions, remote access
        # is automatically granted" (Microsoft Learn, capabilities of Power BI visuals).
        if old and (int(old.group(1)), int(old.group(2))) < (4, 6):
            return "not declared (built for an API before 4.6.0, where web access needs no declaration)"
        return "not declared"
    if not privileges:
        return "none"
    parts = []
    for p in privileges:
        scope = p["parameters"]
        listed = f" ({', '.join(str(x) for x in scope)})" if isinstance(scope, list) and scope else ""
        parts.append(f"{p['name'] or '(unnamed)'}{listed}{'' if p['essential'] else ', optional'}")
    return "; ".join(parts)


def _read_packages(audit: _Audit, private_dir: Path, private: List[str],
                   types: Counter) -> Dict[str, Dict[str, Any]]:
    """Read every private visual package, and state as a note what each one in use can do."""
    packages: Dict[str, Dict[str, Any]] = {}
    for guid in private:
        info = packages[guid] = _read_package(audit, private_dir / guid, guid, types.get(guid, 0))
        if not info["visuals"]:
            continue
        audit.add("INTERACT_CAPABILITIES", "info", _package_text(info), package=guid)
    return packages


def _package_text(info: Mapping[str, Any]) -> str:
    """One package as a sentence a reader can check against the visual's capabilities.json."""
    text = (f"Custom visual '{info['name']}' ({info['guid']}, API {info['api_version'] or 'unknown'}), used "
            f"{info['visuals']} time(s). ")
    declares, calls, can = info["declares"], info["script"], info["behaviour"]
    if declares is None:
        return text + f"What it can do could not be read: {info['unread']}."
    roles = ", ".join(f"{name} ({kind or 'kind not readable'})" for name, kind in info["data_roles"].items())
    text += (f"Its data roles: {roles or 'none'}. Its capabilities declare: highlighting by other visuals "
             f"(supportsHighlight) {_yes(declares['highlight'])}, tooltips {_yes(declares['tooltips'])} "
             f"(report page tooltips {_yes(declares['report_page_tooltips'])}), drill-down "
             f"{_yes(declares['drilldown'])}, keyboard focus {_yes(declares['keyboard_focus'])}, landing "
             f"page {_yes(declares['landing_page'])}, multi-visual selection "
             f"{_yes(declares['multi_visual_selection'])}. Privileges requested: "
             f"{_privilege_text(info['privileges'], info['api_version'])}. ")
    if calls is None or can is None:
        return text + f"Its script could not be read: {info['unread']}."
    if info["column_roles"]:
        clicks = (f"a click {'can' if can['cross_filter'] else 'cannot'} filter other visuals; drillthrough "
                  f"from its context menu is {'possible' if can['drillthrough'] else 'not possible'}")
    else:
        clicks = ("it takes only measures, so it has no data point: a click cannot filter other visuals and "
                  "no drillthrough starts from it, whatever its script calls")
    return text + (
        f"Its script calls: the selection manager {_yes(calls['selection_manager'])}, selection ids "
        f"{_yes(calls['selection_ids'])}, the filter API {_yes(calls['filter_api'])}, the context menu "
        f"(showContextMenu) {_yes(calls['context_menu'])}, the tooltip service "
        f"{_yes(calls['tooltip_service'])}"
        + ("" if info["script_read"] == "code" else " (the script could not be split into code and text, so "
           "a name in a comment or a string counts too)")
        + (" (it also calls members by names put together at run time, so what it does not seem to call "
           "it may still call)" if info["assembled_calls"] else "")
        + f". So, as far as the files tell: {clicks}; other visuals "
        f"{'can' if can['cross_highlight'] else 'cannot'} highlight it; it shows "
        f"{'Power BI tooltips' if can['tooltips'] else 'no Power BI tooltips'}"
        f"{' and report page tooltips' if can['report_page_tooltips'] else ''}.")


def _check_package_behaviour(audit: _Audit, pages: List[Dict[str, Any]],
                             packages: Mapping[str, Dict[str, Any]]) -> None:
    """Where a private visual that shows data points cannot select them, or cannot open the
    context menu on them. A visual that takes only measures has no data point, so neither
    is said of it: there is nothing in the report or in the visual to fix."""
    no_menu: Dict[str, List[str]] = defaultdict(list)
    for page in pages:
        shown = [v for v in page["visuals"] if not v.is_group and v.name not in page["hidden"]]
        data = [v for v in shown if v.is_data]
        placed: Dict[str, List[_Visual]] = defaultdict(list)
        for v in data:
            known = packages.get(v.type or "")
            if known and known["behaviour"] is not None and not known["assembled_calls"]:
                placed[known["guid"]].append(v)
        for guid, found in sorted(placed.items()):
            can, columns = packages[guid]["behaviour"], packages[guid]["column_roles"]
            if columns and not can["drillthrough"]:
                no_menu[guid].append(page["name"])
            # The ones that have data points here: a column, or a hierarchy level, in a role
            # that takes columns.
            mute = [v for v in found if any(use in ("column", "hierarchy")
                                            for role in columns for use, _, _ in v.roles.get(role, ()))]
            if mute and not can["cross_filter"] and len(data) > 1:
                audit.add("INTERACT_NO_SELECTION", "warning",
                          f"Clicks in the custom visual '{packages[guid]['name']}' "
                          f"({_some([v.name for v in mute])}) will not filter the page, which holds "
                          f"{len(data)} data visuals: it shows data points, and its script builds no "
                          "selection for them (no selection manager with selection ids, and no filter API).",
                          page["name"], mute[0].name if len(mute) == 1 else None,
                          visuals=[v.name for v in mute])
    for guid, on_pages in sorted(no_menu.items()):
        calls = packages[guid]["script"]
        why = ("its script opens the context menu but builds no selection id, so the menu has no data "
               "point to drill through from" if calls["context_menu"]
               else "its script never opens the context menu (showContextMenu)")
        audit.add("INTERACT_NO_CONTEXT_MENU", "info",
                  f"No drillthrough from the custom visual '{packages[guid]['name']}': {why}. It is on "
                  f"{_some(on_pages)}.", package=guid, pages=on_pages)


def _check_store_visuals(audit: _Audit, pages: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Deneb and HTML Content come from AppSource, so the report holds no package to read.
    What each visual can do is taken from the settings stored in it and from the visuals' own
    documentation (the two guides named at the top of this module), never guessed."""
    deneb: List[Dict[str, Any]] = []
    html: List[Dict[str, Any]] = []
    for page in pages:
        for v in page["visuals"]:
            kind, objects = v.type or "", v.visual.get("objects")
            # The other editions of Deneb carry the same id behind a prefix and keep the same
            # settings; only the AppSource one is certified.
            if kind.endswith(_DENEB_ID):
                item: Dict[str, Any] = {"page": page["name"], "visual": v.name,
                                        "certified": kind == _DENEB_ID}
                for switch, default in _DENEB_SWITCHES:
                    stored = _first(objects, "vega", switch)
                    item[switch] = stored if isinstance(stored, bool) else default
                deneb.append(item)
            elif kind in _HTML_CONTENT_EDITIONS:
                # Roles: "content" (Values), "sampling" (Context), "tooltips" (Tooltips). A column
                # in Context gives the rows; a hierarchy level stands for a column.
                state = _dict(_dict(v.visual.get("query")).get("queryState"))
                context = _list(_dict(state.get("sampling")).get("projections"))
                extra = _list(_dict(state.get("tooltips")).get("projections"))
                column = any(use in ("column", "hierarchy") for use, _, _ in v.roles.get("sampling", ()))
                enabled = _first(objects, "crossFilter", "enabled") is True
                html.append({"page": page["name"], "visual": v.name, "edition": _HTML_CONTENT_EDITIONS[kind],
                             "certified": _CERTIFIED[kind], "runs_scripts": kind == _HTML_REGULAR,
                             "context_fields": len(context), "tooltip_fields": len(extra),
                             "cross_filter": enabled and column, "context_menu": bool(context),
                             "tooltips": bool(context or extra)})
    if deneb:
        total = len(deneb)

        def on(switch: str) -> int:
            return sum(1 for d in deneb if d[switch])

        drill = sum(1 for d in deneb if d["enableContextMenu"] and d["enableContextMenuSelector"])
        off = [f"{d['page']} / {d['visual']}" for d in deneb if not d["enableSelection"]]
        store = sum(1 for d in deneb if d["certified"])
        audit.add("INTERACT_DENEB", "info",
                  f"Deneb is used {total} time(s), {store} of them in the AppSource edition, which is "
                  "certified. By the settings stored in each visual (one that is not "
                  "stored is at Deneb's documented default): a click cross-filters other visuals "
                  f"(enableSelection) in {on('enableSelection')}; highlighting by other visuals reaches the "
                  f"specification (enableHighlight) in {on('enableHighlight')}; Power BI tooltips "
                  f"(enableTooltips) in {on('enableTooltips')}; the context menu (enableContextMenu) in "
                  f"{on('enableContextMenu')}, with the data point resolved for drillthrough "
                  f"(enableContextMenuSelector) in {drill}. Whether a specification keeps the row identity "
                  "those need is not in the settings."
                  + (f" Cross-filtering is off in: {_some(off, 8)}." if off else ""))
    if html:
        total = len(html)
        editions = Counter(h["edition"] for h in html)
        stated = {"Secure": "Secure edition, certified", "Regular": "Regular edition, not certified"}
        # Scripts are a matter of the edition, and only of the Regular one; nothing is said
        # about an edition the report does not use.
        removes = "The Secure edition removes scripts and inline event handlers from the HTML"
        scripts = {("Regular", "Secure"): removes + "; the Regular edition runs them.",
                   ("Regular",): "The Regular edition runs the scripts and inline event handlers in the "
                                 "HTML.",
                   ("Secure",): removes + "."}
        audit.add("INTERACT_HTML_CONTENT", "info",
                  f"HTML Content is used {total} time(s) ("
                  + "; ".join(f"{stated[name]}: {count}" for name, count in sorted(editions.items()))
                  + "; the edition is told from the visual id, as its documentation lists them). By that "
                  "documentation a visual cross-filters only with a column in its Context role and Enable "
                  "switched on under cross-filtering: "
                  f"{sum(1 for h in html if h['cross_filter'])} of {total}. Its context menu offers data "
                  "point actions, drillthrough included, only with fields in Context: "
                  f"{sum(1 for h in html if h['context_menu'])} of {total}. Power BI tooltips need fields "
                  "in Context or Tooltips (or tooltipEnabled markup in the HTML, which the files do not "
                  "show): "
                  f"{sum(1 for h in html if h['tooltips'])} of {total}. A visual fed by one measure has no "
                  f"row to select, so a click in it filters nothing. {scripts[tuple(sorted(editions))]}")
    return {"deneb": {"visuals": len(deneb), "items": deneb, "source": "https://deneb.guide/docs/pbir-guide"},
            "html_content": {"visuals": len(html), "items": html,
                             "source": "https://www.html-content.com/docs/interactivity and "
                                       "https://www.html-content.com/docs/visual-editions"}}


# ---------------------------------------------------------------- path length

def _extended(path: str) -> str:
    """``path`` in the form Windows opens at any length. The audit has to be able to list a
    folder that Desktop cannot open, or it could not say which one it is."""
    if os.name != "nt" or path.startswith("\\\\?\\"):
        return path
    return "\\\\?\\UNC\\" + path[2:] if path.startswith("\\\\") else "\\\\?\\" + path


def _plain(path: str) -> str:
    """``path`` without the extended-length prefix: the spelling Desktop is given, and the
    one whose length counts."""
    if path.startswith("\\\\?\\UNC\\"):
        return "\\\\" + path[8:]
    return path[4:] if path.startswith("\\\\?\\") else path


def _is_link(entry: Any) -> bool:
    """True for a symbolic link and for a Windows directory junction (an os.DirEntry)."""
    try:
        tag = getattr(entry.stat(follow_symlinks=False), "st_reparse_tag", 0)
        return entry.is_symlink() or tag == _JUNCTION
    except OSError:
        return False


def _list_paths(report_dir: Path, definition: Path) -> Iterator[Tuple[str, bool]]:
    """Every folder and file of the report as (its path below the report folder, written
    with forward slashes; True for a folder). The definition, the unpacked custom visuals and
    the resources are listed.

    Links and junctions are not entered. One that points back at a folder above it would be
    followed round and round until Windows gives up, and each turn lists the same files
    again under a longer path.
    """
    base = _plain(os.path.abspath(report_dir))
    pending = ["StaticResources", "CustomVisuals", definition.name]
    while pending:
        folder = pending.pop()
        try:
            with os.scandir(_extended(os.path.join(base, folder))) as found:
                entries = sorted(found, key=lambda entry: entry.name)
        except OSError:  # not there, or not a folder
            continue
        yield folder.replace(os.sep, "/"), True
        for entry in entries:
            below = os.path.join(folder, entry.name)
            try:
                if _is_link(entry):
                    continue
                if entry.is_dir(follow_symlinks=False):
                    pending.append(below)
                    continue
            except OSError:
                continue
            yield below.replace(os.sep, "/"), False


def _check_path_lengths(audit: _Audit, report_dir: Path, definition: Path) -> Dict[str, Any]:
    """Every file and folder of the report against the Windows path limits Desktop enforces
    ("The fully qualified file name must be less than 260 characters, and the directory name
    must be less than 248 characters"). A custom visual's metadata file repeats its id, so it
    gets there first.

    The lengths are those of the paths as they are here. They are judged only on Windows:
    elsewhere the folder the report sits in is not the one Desktop will open it from.
    """
    base = len(_plain(os.path.abspath(report_dir)))
    longest_file = longest_folder = 0
    files: List[str] = []
    folders: List[str] = []
    for below, is_folder in _list_paths(report_dir, definition):
        length = base + 1 + len(below)
        if is_folder:
            longest_folder = max(longest_folder, length)
            if length >= _MAX_FOLDER_PATH:
                folders.append(below + "/")
        else:
            longest_file = max(longest_file, length)
            if length >= _MAX_FILE_PATH:
                files.append(below)
    too_long = sorted(files) + sorted(folders) if _ON_WINDOWS else []
    if too_long:
        audit.add("STRUCT_PATH_LENGTH", "error",
                  f"{len(files)} file(s) and {len(folders)} folder(s) of the report have a path too long "
                  f"for Windows: a file path must stay under {_MAX_FILE_PATH} characters and a folder path "
                  f"under {_MAX_FOLDER_PATH}, and the longest here are {longest_file} and {longest_folder}. "
                  f"Power BI Desktop refuses to open such a report. First: {_some(too_long, 3)}.",
                  paths=too_long)
    return {"longest_file": longest_file, "longest_folder": longest_folder, "file_limit": _MAX_FILE_PATH,
            "folder_limit": _MAX_FOLDER_PATH, "too_long": too_long, "judged": _ON_WINDOWS}
