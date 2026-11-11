"""
Build a real Power BI custom visual (.pbiviz) from a compact definition.

A .pbiviz is a zip: package.json (the manifest) and resources/<guid>.pbiviz.json, which holds
the capabilities and, as strings, the script and the stylesheet. The script registers a plugin
on ``powerbi.visuals.plugins[<guid>]`` whose ``create(options)`` returns an object with
``update``, ``getFormattingModel`` and ``destroy``. The official route to that file is the
pbiviz toolchain (TypeScript, webpack, an npm install of several hundred megabytes). This
module writes the same package directly: a hand-written runtime (src/visual_kit) does the host
work, and the author supplies a stylesheet and a render function.

What the author writes:

    function render(ctx) {                      // or main(app), the measure-app contract
      return ctx.rows.map(function (row) {
        return '<div class="item" data-row="' + row.index + '">' +
               ctx.escape(row.text.category) + ' ' + ctx.escape(row.text.value) + '</div>';
      }).join("");
    }

Any element carrying ``data-row="<row index>"`` is selectable (click, Ctrl/Cmd for more,
click on empty space to clear), opens the host's context menu (drillthrough, drill,
include/exclude, copy, show as a table), shows the host tooltip with the row's identity (so
report-page tooltips work), takes keyboard focus, and carries the classes ``vk-selected`` /
``vk-dimmed`` / ``vk-highlighted``. The context also offers the explicit calls: ``select``,
``clearSelection``, ``contextMenu``, ``showTooltip``, ``hideTooltip``, ``openUrl``, ``format``,
``color``, ``escape``, plus ``rows``, ``columns``, ``settings``, ``viewport``, ``colors``.

Optional hooks, picked up by name: ``tooltip(row, ctx)`` returns the tooltip lines for a row,
``selectionChanged(ctx)`` runs after the selection changed (for a canvas that paints its own).

Data shape: every grouping role becomes categories and every measure role values of one
categorical data view (the mapping that carries highlights). A row is one combination of the
grouping fields: ``row.values[role]``, ``row.text[role]`` (formatted; a category without a
value reads "(Blank)"), ``row.highlights[role]`` (the cross-highlighted part, or null),
``row.cells[role]`` (every field of the role, each {value, text, highlight, column}),
``row.selected``, ``row.highlighted``, ``row.dimmed``. A visual of measures only has one row
and no data point: it cannot cross-filter, offer drillthrough or feed a report-page tooltip.

Rows: a visual takes at most ``rows`` rows (default 1000). Power BI cuts the data to a visual's
limit without telling it, so a total summed over ``ctx.rows`` can be the total of the first
rows only. The kit makes the cut visible: ``ctx.truncated`` is true (``ctx.rowLimit`` holds the
limit) and the visual shows "Showing the first N rows" unless ``features.limit_notice`` is
false. ``ctx.hasMoreRows`` / ``ctx.loadingMore`` tell about windows still on their way when
``rows`` is above 30,000.

Files: a definition names its script, stylesheet and icon by file name. They are read from the
definition's own folder (``base_dir``) and nowhere else: whatever a definition names ends up
inside the package, so a path that leaves the folder is refused.

Sources of each structure written here:
  * package.json / *.pbiviz.json key sets and the plugin object: a package built by the
    official toolchain (API 5.11.0) that Power BI Desktop loads, and schema.pbiviz.json of the
    powerbi-visuals-api package.
  * capabilities: schema.capabilities.json of powerbi-visuals-api 5.11.0.
  * formatting model, selection, tooltips, context menu, highlights, drill-down, fetch more
    data, rendering events, landing page: the visuals API typings (5.11.0) and Microsoft Learn,
    "Power BI visuals API" (power-bi/developer/visuals/...).

Seen in Power BI Desktop (November 2026): packages built here load and draw their rows; the
format strings, the theme's colours, the settings stored in a report file, the landing page,
the row windows and the drill levels arrive as the kit expects; and, called from a visual's
own code, ``ctx.select`` cross-filters and cross-highlights the built-in visuals,
``ctx.showTooltip`` draws the host tooltip and the context menu offers drillthrough. Pointer
input cannot be driven from outside Desktop, so what a click, a hover and a key lead to is
covered by ``simulate``, which runs a package in Node against a model of the host. The build
itself never runs the author's script: it reads it (safety pass) and has Node parse it.
"""
import base64
import hashlib
import io
import json
import math
import os
import re
import shutil
import struct
import subprocess
import tempfile
import uuid
import zipfile
import zlib
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple, Union

import js_app_measure as jm
import payload_measures
import pbir_packages
import safe_io

API_VERSION = "5.11.0"          # the API the kit is written against, and that Desktop was seen to load
KIT_VERSION = "visual-kit/1"    # marker in the script, so the inspector can tell its own output
DEFAULT_ROWS = 1000
WINDOW_ROWS = 30000             # the largest window of rows the host serves at a time
MAX_ROWS = 1048576              # the documented ceiling of a data view

ROLE_KINDS = {"grouping": "Grouping", "measure": "Measure"}
PROPERTY_TYPES = ("toggle", "color", "number", "font_size", "text", "dropdown")
FEATURES = ("selection", "highlight", "context_menu", "tooltips", "drilldown", "keyboard",
            "landing_page", "multi_visual_selection", "sorting", "limit_notice")
# Top-level keys of a capabilities document (schema.capabilities.json, API 5.11.0).
CAPABILITY_KEYS = frozenset((
    "privileges", "dataRoles", "dataViewMappings", "objects", "tooltips", "sorting", "drilldown",
    "expandCollapse", "suppressDefaultTitle", "supportsKeyboardFocus", "supportsHighlight",
    "supportsSynchronizingFilterState", "advancedEditModeSupport", "supportsLandingPage",
    "supportsEmptyDataView", "supportsMultiVisualSelection", "subtotals", "migration",
    "keepAllMetadataColumns", "dataReductionCustomization", "supportsOnObjectFormatting",
    "enablePointerEventsFormatMode"))

_DEFINITION_KEYS = {"name", "display_name", "description", "version", "guid", "author", "support_url",
                    "github_url", "icon_file", "roles", "rows", "features", "format", "css", "css_file",
                    "js", "js_files", "js_folder", "landing", "empty_message"}
_ROLE_KEYS = {"name", "display_name", "description", "kind", "max", "tooltips"}
_CARD_KEYS = {"name", "display_name", "properties"}
_PROPERTY_KEYS = {"name", "display_name", "description", "type", "default", "min", "max", "items",
                  "placeholder"}
# Type names of the visuals built into Power BI (pbir-format reference, "Visual Types"), lower
# case: a custom visual's guid must not be one of them, or a report could not tell them apart.
_NATIVE_VISUAL_TYPES = frozenset(t.lower() for t in (
    "lineChart", "areaChart", "stackedAreaChart", "hundredPercentStackedAreaChart", "barChart",
    "clusteredBarChart", "hundredPercentStackedBarChart", "columnChart", "clusteredColumnChart",
    "hundredPercentStackedColumnChart", "lineClusteredColumnComboChart", "lineStackedColumnComboChart",
    "scatterChart", "pieChart", "donutChart", "waterfallChart", "ribbonChart", "funnel", "treemap", "gauge",
    "filledMap", "map", "azureMap", "shapeMap", "tableEx", "pivotTable", "multiRowCard", "kpi", "cardVisual",
    "card", "scorecard", "slicer", "advancedSlicerVisual", "listSlicer", "aiNarratives",
    "decompositionTreeVisual", "keyDriversVisual", "qnaVisual", "pythonVisual", "scriptVisual", "rdlVisual",
    "group", "basicShape", "shape", "image", "actionButton", "pageNavigator", "bookmarkNavigator", "textbox",
    "filter"))
# Room a report folder's own path needs under the Windows limit ("C:\Users\<name>\Documents\
# <project>\<report>.Report" is commonly 60 to 90 characters).
_REPORT_FOLDER_ROOM = 90
_KIT_DIR = Path(__file__).resolve().parent / "visual_kit"
_KIT_MODULES = ("format.js", "model.js", "visual.js")
_GUID_NAMESPACE = uuid.UUID("6f1d2a5e-93c4-5b7a-8e21-4d0c9b7a3f15")
_IDENTIFIER = re.compile(r"[A-Za-z][A-Za-z0-9_]*")
_BS = chr(92)
_PNG_SIGNATURE = bytes((0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A))

Json = Dict[str, Any]


class DefinitionError(ValueError):
    """The definition cannot be turned into a package. ``errors`` lists every problem found."""

    def __init__(self, errors: Sequence[str]):
        super().__init__("; ".join(errors))
        self.errors = list(errors)


# ---------------------------------------------------------------- identity

def visual_guid(name: str) -> str:
    """The guid a visual of this name gets: the name followed by 32 hex digits derived from it.

    It is the key a report stores for the visual (visualType in every visual.json, the folder
    under CustomVisuals), so it must not change when the visual is built again.
    """
    return name + uuid.uuid5(_GUID_NAMESPACE, name).hex.upper()


# ---------------------------------------------------------------- reading a definition

def load_definition(path: Union[str, os.PathLike]) -> Tuple[Json, str]:
    """(definition, the folder its relative file names are read from) for a definition file."""
    full = os.path.abspath(os.fspath(path))
    try:
        definition = safe_io.read_json(full)
    except OSError as e:
        raise DefinitionError([f"definition file could not be read: {e}"]) from e
    except ValueError as e:
        raise DefinitionError([f"{os.path.basename(full)} is not valid JSON: {e}"]) from e
    if not isinstance(definition, dict):
        raise DefinitionError([f"{os.path.basename(full)} must hold a JSON object"])
    return definition, os.path.dirname(full)


def _inside(base_dir: Optional[str], name: Any, where: str, errors: List[str], kind: str = "file") -> Optional[str]:
    """The full path of ``name`` when it lies inside ``base_dir``; otherwise None, with an error.

    A definition can come from anywhere and whatever it names ends up inside the package, so it
    may only name files in its own folder. Links are resolved first, and a path outside gets
    the same refusal whether it exists or not: the message must not tell what a machine holds.
    """
    if not isinstance(name, str) or not name.strip():
        errors.append(f"{where}: must be a {kind} name")
        return None
    if base_dir is None:
        errors.append(f"{where}: {name!r} cannot be read, because this definition has no folder of its own. "
                      "Load the definition from a file or give base_dir; only files inside that folder are read.")
        return None
    try:
        base = os.path.realpath(base_dir)
        full = os.path.realpath(os.path.join(base, name))       # an absolute name replaces the base here
        inside = os.path.commonpath([os.path.normcase(base), os.path.normcase(full)]) == os.path.normcase(base)
    except (ValueError, OSError):                                # another drive, or a name no file system takes
        inside = False
    if not inside:
        errors.append(f"{where}: {name!r} is outside the definition's folder; only files inside it are read")
        return None
    return full


def _reason(error: OSError) -> str:
    """Why a file could not be used, without the path the operating system puts in its message."""
    return error.strerror or type(error).__name__


def _read_source(base_dir: Optional[str], name: Any, where: str, errors: List[str]) -> str:
    full = _inside(base_dir, name, where, errors)
    if full is None:
        return ""
    try:
        return safe_io.read_text(full)[0]
    except UnicodeDecodeError:
        errors.append(f"{where}: {name!r} could not be read (it is not UTF-8 text)")
    except OSError as e:
        errors.append(f"{where}: {name!r} could not be read ({_reason(e)})")
    return ""


def _text(value: Any, where: str, errors: List[str], default: str = "") -> str:
    if value is None:
        return default
    if not isinstance(value, str):
        errors.append(f"{where}: must be text, got {type(value).__name__}")
        return default
    return value


def _unknown_keys(item: Mapping[str, Any], known: set, where: str, errors: List[str]) -> None:
    for key in sorted(set(item) - known):
        errors.append(f"{where}: unknown key {key!r} (known: {', '.join(sorted(known))})")


def _title(name: str) -> str:
    """'avgSpend' / 'avg_spend' -> 'Avg spend': a display name when none was given."""
    words = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", name).replace("_", " ").split()
    return " ".join(words).capitalize() if words else name


def _roles(definition: Mapping[str, Any], errors: List[str]) -> List[Json]:
    raw = definition.get("roles")
    if not isinstance(raw, list) or not raw:
        errors.append('roles: give at least one data role, for example {"name": "category", "kind": "grouping"}')
        return []
    out: List[Json] = []
    seen = set()
    for i, role in enumerate(raw):
        where = f"roles[{i}]"
        if not isinstance(role, dict):
            errors.append(f"{where}: must be an object with name and kind")
            continue
        _unknown_keys(role, _ROLE_KEYS, where, errors)
        name = role.get("name")
        if not isinstance(name, str) or not _IDENTIFIER.fullmatch(name):
            errors.append(f"{where}.name: must start with a letter and hold only letters, digits and '_', "
                          f"got {name!r}")
            continue
        if name in seen:
            errors.append(f"{where}.name: the role {name!r} is declared twice")
            continue
        seen.add(name)
        kind = role.get("kind")
        if not isinstance(kind, str) or kind not in ROLE_KINDS:
            errors.append(f"{where}.kind: must be 'grouping' (columns) or 'measure', got {kind!r}")
            continue
        limit = role.get("max")
        if limit is not None and (isinstance(limit, bool) or not isinstance(limit, int) or limit < 1):
            errors.append(f"{where}.max: must be a whole number of fields, 1 or more, got {limit!r}")
            limit = None
        tooltips = role.get("tooltips", False)
        if not isinstance(tooltips, bool):
            errors.append(f"{where}.tooltips: must be true or false")
            tooltips = False
        out.append({"name": name, "kind": kind, "max": limit, "tooltips": tooltips,
                    "display_name": _text(role.get("display_name"), f"{where}.display_name", errors) or _title(name),
                    "description": _text(role.get("description"), f"{where}.description", errors)})
    return out


def _features(definition: Mapping[str, Any], roles: List[Json], errors: List[str],
              warnings: List[str]) -> Json:
    raw = definition.get("features") or {}
    if not isinstance(raw, dict):
        errors.append("features: must be an object of switches, for example {\"tooltips\": false}")
        raw = {}
    _unknown_keys(raw, set(FEATURES), "features", errors)
    groupings = [r["name"] for r in roles if r["kind"] == "grouping"]
    measures = [r["name"] for r in roles if r["kind"] == "measure"]
    out: Json = {}
    for name in FEATURES:
        if name == "drilldown":
            continue
        value = raw.get(name)
        if value is not None and not isinstance(value, bool):
            errors.append(f"features.{name}: must be true or false, got {value!r}")
            value = None
        out[name] = value
    # A selection needs an identity, and only a grouping field gives a row one.
    if out["selection"] and not groupings:
        errors.append("features.selection: needs a grouping role; a visual of measures only has no data "
                      "point to select")
    if out["highlight"] and not measures:
        errors.append("features.highlight: needs a measure role; highlights arrive on measure values")
    out["selection"] = bool(groupings) if out["selection"] is None else out["selection"] and bool(groupings)
    out["highlight"] = bool(measures) if out["highlight"] is None else out["highlight"] and bool(measures)
    if out["multi_visual_selection"] and not out["selection"]:
        errors.append("features.multi_visual_selection: needs features.selection")
    out["multi_visual_selection"] = (out["selection"] if out["multi_visual_selection"] is None
                                     else out["multi_visual_selection"] and out["selection"])
    for name in ("context_menu", "tooltips", "keyboard", "landing_page", "sorting", "limit_notice"):
        out[name] = True if out[name] is None else out[name]

    drill = raw.get("drilldown")
    out["drilldown"] = None
    if drill is True:
        if len(groupings) == 1:
            out["drilldown"] = groupings[0]
        else:
            errors.append("features.drilldown: name the grouping role to drill on (true only works with "
                          f"exactly one grouping role; this definition has {len(groupings)})")
    elif isinstance(drill, str):
        if drill in groupings:
            out["drilldown"] = drill
        else:
            errors.append(f"features.drilldown: {drill!r} is not a grouping role "
                          f"(grouping roles: {', '.join(groupings) or 'none'})")
    elif drill not in (None, False):
        errors.append(f"features.drilldown: must be a grouping role name, true or false, got {drill!r}")
    for role in roles:
        if role["name"] == out["drilldown"]:
            # The host asks for a limit of one field on a drillable role; a hierarchy still drops in.
            if role["max"] not in (None, 1):
                warnings.append(f"roles: '{role['name']}' is the drill role, so its max is set to 1 "
                                f"(was {role['max']}); a hierarchy can still be dropped on it")
            role["max"] = 1
    return out


def _number(value: Any) -> bool:
    """A finite number: NaN and the infinities have no JSON form, and a script cannot count with them."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    try:
        return math.isfinite(value)
    except OverflowError:                 # an integer too large to be a JavaScript number
        return False


def _property(prop: Any, where: str, errors: List[str]) -> Optional[Json]:
    if not isinstance(prop, dict):
        errors.append(f"{where}: must be an object with name and type")
        return None
    _unknown_keys(prop, _PROPERTY_KEYS, where, errors)
    name, kind = prop.get("name"), prop.get("type")
    if not isinstance(name, str) or not _IDENTIFIER.fullmatch(name):
        errors.append(f"{where}.name: must start with a letter and hold only letters, digits and '_', got {name!r}")
        return None
    if kind not in PROPERTY_TYPES:
        errors.append(f"{where}.type: must be one of {', '.join(PROPERTY_TYPES)}, got {kind!r}")
        return None
    out: Json = {"name": name, "type": kind,
                 "displayName": _text(prop.get("display_name"), f"{where}.display_name", errors) or _title(name)}
    description = _text(prop.get("description"), f"{where}.description", errors)
    if description:
        out["description"] = description
    default = prop.get("default")
    for key in ("min", "max"):
        if prop.get(key) is not None:
            if kind not in ("number", "font_size"):
                errors.append(f"{where}.{key}: only a number or a font_size has limits")
            elif not _number(prop[key]):
                errors.append(f"{where}.{key}: must be a finite number, got {prop[key]!r:.40}")
            else:
                out[key] = prop[key]
    if kind == "toggle":
        if default is not None and not isinstance(default, bool):
            errors.append(f"{where}.default: a toggle is true or false, got {default!r}")
        out["value"] = bool(default)
    elif kind == "color":
        if default is not None and not (isinstance(default, str) and re.fullmatch(r"#[0-9A-Fa-f]{6}", default)):
            errors.append(f"{where}.default: a colour is '#RRGGBB' (leave it out to follow the report theme), "
                          f"got {default!r}")
            default = None
        out["value"] = default            # null: the theme's first data colour, read at run time
    elif kind in ("number", "font_size"):
        if kind == "font_size":
            out.setdefault("min", 6)
            out.setdefault("max", 72)
        if default is not None and not _number(default):
            errors.append(f"{where}.default: must be a finite number, got {default!r:.40}")
            default = None
        out["value"] = (12 if kind == "font_size" else 0) if default is None else default
        low, high = out.get("min"), out.get("max")
        if low is not None and high is not None and low > high:
            errors.append(f"{where}: min ({low}) is greater than max ({high})")
        elif (low is not None and out["value"] < low) or (high is not None and out["value"] > high):
            errors.append(f"{where}.default: {out['value']} is outside {low} to {high}")
    elif kind == "text":
        out["value"] = _text(default, f"{where}.default", errors)
        out["placeholder"] = _text(prop.get("placeholder"), f"{where}.placeholder", errors)
    else:
        items = prop.get("items")
        if not isinstance(items, list) or not items:
            errors.append(f"{where}.items: a dropdown needs its choices, for example "
                          '[{"value": "asc", "display_name": "Ascending"}]')
            return None
        choices = []
        for k, item in enumerate(items):
            if isinstance(item, str):
                item = {"value": item}
            if not isinstance(item, dict) or not isinstance(item.get("value"), str) or not item["value"]:
                errors.append(f"{where}.items[{k}]: must be text or an object with a text 'value'")
                continue
            if any(c["value"] == item["value"] for c in choices):
                errors.append(f"{where}.items[{k}]: the value {item['value']!r} is listed twice")
                continue
            choices.append({"value": item["value"],
                            "displayName": _text(item.get("display_name"), f"{where}.items[{k}].display_name",
                                                 errors) or _title(item["value"])})
        if not choices:
            return None
        if default is not None and default not in [c["value"] for c in choices]:
            errors.append(f"{where}.default: {default!r} is not one of the items "
                          f"({', '.join(c['value'] for c in choices)})")
            default = None
        out["items"] = choices
        out["value"] = choices[0]["value"] if default is None else default
    if prop.get("items") is not None and kind != "dropdown":
        errors.append(f"{where}.items: only a dropdown has items")
    if prop.get("placeholder") is not None and kind != "text":
        errors.append(f"{where}.placeholder: only a text property has a placeholder")
    return out


def _format_cards(definition: Mapping[str, Any], errors: List[str]) -> List[Json]:
    raw = definition.get("format") or []
    if not isinstance(raw, list):
        errors.append("format: must be a list of cards, each {name, display_name, properties: [...]}")
        return []
    cards: List[Json] = []
    seen = set()
    for i, card in enumerate(raw):
        where = f"format[{i}]"
        if not isinstance(card, dict):
            errors.append(f"{where}: must be an object with name and properties")
            continue
        _unknown_keys(card, _CARD_KEYS, where, errors)
        name = card.get("name")
        if not isinstance(name, str) or not _IDENTIFIER.fullmatch(name):
            errors.append(f"{where}.name: must start with a letter and hold only letters, digits and '_', "
                          f"got {name!r}")
            continue
        if name in seen:
            errors.append(f"{where}.name: the card {name!r} is declared twice")
            continue
        seen.add(name)
        props = card.get("properties")
        if not isinstance(props, list) or not props:
            errors.append(f"{where}.properties: a card needs at least one property")
            continue
        built: List[Json] = []
        for k, prop in enumerate(props):
            item = _property(prop, f"{where}.properties[{k}]", errors)
            if item is None:
                continue
            if any(p["name"] == item["name"] for p in built):
                errors.append(f"{where}.properties[{k}].name: {item['name']!r} is declared twice in this card")
                continue
            built.append(item)
        cards.append({"name": name, "properties": built,
                      "displayName": _text(card.get("display_name"), f"{where}.display_name", errors) or _title(name)})
    return cards


def _sources(definition: Mapping[str, Any], base_dir: Optional[str],
             errors: List[str]) -> Tuple[str, List[Tuple[str, str]]]:
    """(stylesheet, [(module name, source)]) as the definition gives them, inline or from files."""
    given = [k for k in ("css", "css_file") if definition.get(k) is not None]
    if len(given) > 1:
        errors.append("css: give the stylesheet once, as 'css' (text) or 'css_file' (a file name)")
    css = _text(definition.get("css"), "css", errors)
    if definition.get("css_file") is not None:
        css = _read_source(base_dir, definition["css_file"], "css_file", errors)

    given = [k for k in ("js", "js_files", "js_folder") if definition.get(k) is not None]
    modules: List[Tuple[str, str]] = []
    if len(given) != 1:
        errors.append("js: give the script once, as 'js' (text), 'js_files' (file names, in order) or "
                      "'js_folder' (a folder whose .js files are taken in name order)"
                      + (f"; got {', '.join(given)}" if given else ""))
    elif given[0] == "js":
        modules = [("js", _text(definition["js"], "js", errors))]
    elif given[0] == "js_files":
        names = definition["js_files"]
        if not isinstance(names, list) or not names:
            errors.append("js_files: must be a list of file names")
        else:
            modules = [(os.path.basename(str(n)), _read_source(base_dir, n, f"js_files[{i}]", errors))
                       for i, n in enumerate(names)]
    else:
        folder = definition["js_folder"]
        full = _inside(base_dir, folder, "js_folder", errors, "folder")
        if full is not None and not os.path.isdir(full):
            errors.append(f"js_folder: {folder!r} is not a folder")
        elif full is not None:
            names = sorted(f for f in os.listdir(full) if f.lower().endswith(".js"))
            if not names:
                errors.append(f"js_folder: no .js files in {folder!r}")
            modules = [(n, _read_source(full, n, f"js_folder/{n}", errors)) for n in names]
    if modules and not any(source.strip() for _, source in modules) and not errors:
        errors.append("js: the script is empty; it must define render(ctx) or main(app)")
    return css, modules


def _icon(definition: Mapping[str, Any], base_dir: Optional[str], errors: List[str]) -> bytes:
    name = definition.get("icon_file")
    if name is None:
        return _default_icon()
    full = _inside(base_dir, name, "icon_file", errors)
    if full is None:
        return _default_icon()
    try:
        with open(full, "rb") as f:
            data = f.read()
    except OSError as e:
        errors.append(f"icon_file: {name!r} could not be read ({_reason(e)})")
        return _default_icon()
    if not data.startswith(_PNG_SIGNATURE):
        errors.append(f"icon_file: {name!r} is not a PNG image")
        return _default_icon()
    return data


def _path_length(name: str, guid: str, errors: List[str], warnings: List[str]) -> None:
    """A report keeps the visual at CustomVisuals/<guid>/resources/<guid>.pbiviz.json: the guid
    twice. Power BI Desktop refuses to open a report in which that path passes the Windows
    limit, so a long name is caught here, before a report depends on it."""
    stored = len(f"CustomVisuals/{guid}/resources/{guid}.pbiviz.json")
    room = pbir_packages.WINDOWS_PATH_LIMIT - stored - 1          # what is left for the report folder
    # The longest derived guid (name + 32 digits) that still leaves the usual folder its room.
    safe = (pbir_packages.WINDOWS_PATH_LIMIT - _REPORT_FOLDER_ROOM - 1 - len("CustomVisuals//resources/.pbiviz.json")) // 2 - 32
    if room < 1:
        errors.append(f"name: the guid is {len(guid)} characters and a report stores it twice in one path "
                      f"({stored} characters), which is over the Windows limit by itself. Use a name of "
                      f"{safe} characters or fewer.")
    elif room < _REPORT_FOLDER_ROOM:
        warnings.append(f"name: the guid is {len(guid)} characters and a report stores it twice in one path "
                        f"({stored} characters: CustomVisuals/<guid>/resources/<guid>.pbiviz.json). That leaves "
                        f"{room} characters for the report folder's own path before the Windows limit, and "
                        f"Power BI Desktop refuses to open a report that passes it. A name of {safe} characters "
                        f"or fewer leaves room for a folder path of {_REPORT_FOLDER_ROOM}.")


def _normalize(definition: Any, base_dir: Optional[str]) -> Tuple[Json, List[str], List[str]]:
    """(the definition with every default filled in, errors, warnings). Reads the source files."""
    errors: List[str] = []
    warnings: List[str] = []
    if not isinstance(definition, dict):
        return {}, ["the definition must be a JSON object"], warnings
    _unknown_keys(definition, _DEFINITION_KEYS, "definition", errors)

    name = definition.get("name")
    if not isinstance(name, str) or not _IDENTIFIER.fullmatch(name):
        errors.append("name: must start with a letter and hold only letters, digits and '_' (it becomes part "
                      f"of the visual's guid and of a CSS class), got {name!r}")
        name = "visual"
    guid = definition.get("guid")
    if guid is None:
        guid = visual_guid(name)
    elif not isinstance(guid, str) or not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{7,}", guid):
        errors.append("guid: must start with a letter and hold only letters, digits and '_', 8 characters or "
                      f"more (it is a folder name and a script identifier), got {guid!r:.60}")
        guid = visual_guid(name)
    elif not payload_measures.is_js_identifier(guid) or guid.lower() in _NATIVE_VISUAL_TYPES:
        # The script declares a variable of this name, and a report tells visuals apart by it.
        errors.append(f"guid: {guid!r} is " + ("the name of a visual built into Power BI"
                                               if guid.lower() in _NATIVE_VISUAL_TYPES else "a reserved word of JavaScript")
                      + "; leave it out to get one derived from the name")
        guid = visual_guid(name)
    _path_length(name, guid, errors, warnings)
    version = definition.get("version", "1.0.0.0")
    if not isinstance(version, str) or not re.fullmatch(r"\d+(\.\d+){2,3}", version):
        errors.append(f"version: must be three or four numbers separated by dots, like 1.0.0.0, got {version!r}")
        version = "1.0.0.0"
    elif version.count(".") == 2:
        version += ".0"
    author = definition.get("author") or {}
    if not isinstance(author, dict):
        errors.append('author: must be an object, {"name": ..., "email": ...}')
        author = {}
    _unknown_keys(author, {"name", "email"}, "author", errors)

    rows = definition.get("rows", DEFAULT_ROWS)
    if isinstance(rows, bool) or not isinstance(rows, int) or not 1 <= rows <= MAX_ROWS:
        errors.append(f"rows: must be a whole number from 1 to {MAX_ROWS}, got {rows!r}")
        rows = DEFAULT_ROWS
    roles = _roles(definition, errors)
    features = _features(definition, roles, errors, warnings)
    cards = _format_cards(definition, errors)
    css, modules = _sources(definition, base_dir, errors)
    landing = definition.get("landing") or {}
    if not isinstance(landing, dict):
        errors.append('landing: must be an object, {"title": ..., "text": ...}')
        landing = {}
    _unknown_keys(landing, {"title", "text"}, "landing", errors)
    display_name = _text(definition.get("display_name"), "display_name", errors) or _title(name)
    wells = ", ".join(r["display_name"] for r in roles)

    if rows > WINDOW_ROWS and not any(r["kind"] == "grouping" for r in roles):
        warnings.append(f"rows: {rows} asks for more than one window of rows, but a visual of measures only "
                        "has a single row; the setting has no effect")
    return {
        "name": name, "guid": guid, "version": version, "display_name": display_name,
        "description": _text(definition.get("description"), "description", errors),
        "author": {"name": _text(author.get("name"), "author.name", errors),
                   "email": _text(author.get("email"), "author.email", errors)},
        "support_url": _text(definition.get("support_url"), "support_url", errors),
        "github_url": _text(definition.get("github_url"), "github_url", errors),
        "roles": roles, "rows": rows, "features": features, "format": cards, "css": css, "modules": modules,
        "landing": {"title": _text(landing.get("title"), "landing.title", errors) or display_name,
                    "text": _text(landing.get("text"), "landing.text", errors) or f"Add fields to: {wells}"},
        "empty_message": _text(definition.get("empty_message"), "empty_message", errors)
        or "No data for the current filters.",
        "icon": _icon(definition, base_dir, errors),
    }, errors, warnings


# ---------------------------------------------------------------- safety pass

_GLOBALS = frozenset(("window", "self", "globalThis", "frames"))
_NETWORK = frozenset(("fetch", "XMLHttpRequest", "WebSocket", "EventSource", "sendBeacon", "importScripts",
                      "Worker", "SharedWorker", "RTCPeerConnection", "ActiveXObject"))
_FRAME_NAMES = frozenset(("parent", "top", "opener", "frameElement", "frames"))
_STORAGE = frozenset(("localStorage", "sessionStorage", "indexedDB"))
_HTML_SINKS = frozenset(("innerHTML", "outerHTML", "insertAdjacentHTML"))
_LOADING_TAGS = frozenset(("script", "iframe", "object", "embed", "link"))
_RISKY_TEXT = ("<script", "<iframe", "<object", "<embed", "javascript:")
_MEMBER_STARTERS = frozenset(("static", "async", "get", "set"))

_NO_NETWORK = ("A packaged visual asks for no web access privilege, so the host refuses network calls; bring "
               "the data in through a data role")
_NO_FRAME = ("The visual runs in a sandboxed frame and must not reach out of it; everything it may ask of "
             "Power BI is on the render context")


def _token_spans(source: str, toks: Sequence[Any]) -> List[Tuple[int, int, int]]:
    """(1-based line, start, end) of each token in ``source``. Tokens come in source order and only
    white space and comments sit between them, so one walk places them all."""
    spans: List[Tuple[int, int, int]] = []
    pos, line, n = 0, 1, len(source)
    for tok in toks:
        while pos < n:
            if source.startswith("//", pos):
                end = source.find("\n", pos)
                pos = n if end < 0 else end
            elif source.startswith("/*", pos):
                end = source.find("*/", pos + 2)
                end = n if end < 0 else end + 2
                line += source.count("\n", pos, end)
                pos = end
            elif source[pos].isspace() or source[pos] == chr(0xFEFF):
                line += source[pos] == "\n"
                pos += 1
            else:
                break
        end = jm._scan_template(source, pos)[1] if tok.kind == "template" else pos + len(tok.text)
        spans.append((line, pos, end))
        line += source.count("\n", pos, end)
        pos = end
    return spans


def _template_expressions(source: str, start: int, end: int) -> List[Tuple[int, str]]:
    """(offset in ``source``, code) of each ${ } expression of the template literal at start..end."""
    out: List[Tuple[int, str]] = []
    i = start + 1
    while i < end - 1:
        if source[i] == _BS:
            i += 2
        elif source.startswith("${", i):
            _, after = jm._lex(source, i + 2, True)              # reads to the brace that closes it
            out.append((i + 2, source[i + 2:max(i + 2, after - 1)]))
            i = max(after, i + 2)
        else:
            i += 1
    return out


def _is(tok: Any, text: str) -> bool:
    return tok is not None and tok.kind == "punct" and tok.text == text


def _listed_names(toks: Sequence[Any], start: int, stop_at_semicolon: bool) -> set:
    """Words that open an item of the comma-separated list starting at ``start``: the names of a
    parameter list (``start`` just past its bracket) or of a var/let/const statement. Names
    inside a destructuring pattern count too."""
    names, depth, expecting = set(), 0, True
    pattern = 0                       # depth inside a {..} or [..] pattern that is being declared
    for j in range(start, len(toks)):
        tok = toks[j]
        if tok.kind == "punct" and tok.text in "([{":
            if expecting and tok.text in "[{" and not pattern:
                pattern = depth + 1
            depth += 1
        elif tok.kind == "punct" and tok.text in ")]}":
            depth -= 1
            if depth < 0:
                break
            if pattern and depth < pattern:
                pattern, expecting = 0, False
        elif depth == 0 and _is(tok, ";") and stop_at_semicolon:
            break
        elif depth == 0 and _is(tok, ","):
            expecting = True
        elif tok.kind == "word" and (pattern or (expecting and depth == 0)):
            if stop_at_semicolon and tok.text in ("of", "in") and not expecting:
                break
            names.add(tok.text)
            expecting = False
        elif not pattern and not _is(tok, "."):                   # "...rest" keeps waiting for its name
            expecting = False
    return names


def _declared(toks: Sequence[Any]) -> set:
    """Names the script itself declares (var/let/const/function/class, parameters, catch): a local
    'parent', 'top' or 'self' is a different thing from the global of that name. Scopes are not
    followed: a name declared anywhere in the module counts everywhere in it."""
    names = set()
    n = len(toks)
    for k, tok in enumerate(toks):
        if tok.kind == "word" and tok.text in ("var", "let", "const"):
            names |= _listed_names(toks, k + 1, True)
        elif tok.kind == "word" and tok.text in ("function", "class", "catch"):
            j = k + 1
            if j < n and _is(toks[j], "*"):                      # function* name
                j += 1
            if j < n and toks[j].kind == "word":
                names.add(toks[j].text)                          # the function's or class's own name
                j += 1
            if j < n and _is(toks[j], "(") and tok.text != "class":
                names |= _listed_names(toks, j + 1, False)
        elif _is(tok, "=") and k + 1 < n and _is(toks[k + 1], ">") and not toks[k + 1].ws and k:
            if toks[k - 1].kind == "word":                       # x => ...
                names.add(toks[k - 1].text)
            elif _is(toks[k - 1], ")"):                          # (a, b) => ...
                depth, j = 0, k - 1
                while j >= 0:
                    depth += _is(toks[j], ")") - _is(toks[j], "(")
                    if depth == 0:
                        break
                    j -= 1
                if j >= 0:
                    names |= _listed_names(toks, j + 1, False)
    return names


def _closes_before_block(toks: Sequence[Any], open_at: int) -> bool:
    """True when the bracket opening at ``open_at`` closes right before a '{': name(...) { is a
    method being defined, never a call."""
    depth = 0
    for j in range(open_at, len(toks)):
        depth += _is(toks[j], "(") - _is(toks[j], ")")
        if depth == 0:
            return j + 1 < len(toks) and _is(toks[j + 1], "{")
    return False


def check_script_safety(source: str, label: str = "js") -> Tuple[List[str], List[str]]:
    """(errors, warnings) for one script module: what a visual must not do inside a report.

    Refused: network access, running text as code, reaching the parent frame or other frames,
    browser storage, navigating the frame, creating elements that load code. Warned about:
    writing HTML by hand (escape model values), inline event handlers, and text that looks like
    markup which loads code (a string cannot be told from markup, so that is never a refusal).

    It reads the tokens of the script. A word in a comment or a string is not a call; a name
    the script declares itself (``var self = this``, a parameter called ``parent``) is not the
    global of that name; a property key, a method name and a member of the script's own object
    (``api.fetch()``) are not uses. It is a review aid, not a sandbox: code that hides a name
    (window["fe" + "tch"]) passes, and scopes are not followed. The sandboxed frame Power BI
    runs every custom visual in stays the real boundary.
    """
    text = (source or "").replace("\r\n", "\n").replace("\r", "\n")
    errors: List[str] = []
    warnings: List[str] = []
    _scan_tokens(text, label, errors, warnings, 1, set(), set())
    return errors, warnings


def _scan_tokens(text: str, label: str, errors: List[str], warnings: List[str], first_line: int,
                 outer: set, warned: set) -> None:
    """Check ``text``, whose first line is line ``first_line`` of the module. ``outer`` holds the
    names declared around it (for the code inside a template literal)."""
    toks, _ = jm._lex(text, 0, False)
    spans = _token_spans(text, toks)
    declared = outer | _declared(toks)
    n = len(toks)

    def line(k: int) -> int:
        return first_line + spans[k][0] - 1

    def fail(k: int, what: str, why: str) -> None:
        errors.append(f"{label}: line {line(k)}: {what}. {why}.")

    def warn(k: int, key: str, message: str) -> None:
        if key not in warned:
            warned.add(key)
            warnings.append(f"{label}: line {line(k)}: {message}")

    for k, tok in enumerate(toks):
        prev = toks[k - 1] if k else None
        nxt = toks[k + 1] if k + 1 < n else None
        after = nxt.text if nxt is not None and nxt.kind == "punct" else ""

        if tok.kind in ("string", "template"):
            low = tok.text.lower()
            for sign in _RISKY_TEXT:
                if sign in low:
                    warn(k, sign, f"a string holds {sign!r}. If it is markup, it would load or run code inside the "
                         "visual, which the sandbox blocks; build elements for the data and let the kit wire them.")
            if re.search(r"\son[a-z]+\s*=\s*[\"'\\]", low):
                warn(k, "handler", "an inline event handler (on...=) in markup. Mark the element with "
                     "data-row, or wire it with ctx.on, so the handler is real code that was checked.")
            if tok.kind == "template":
                for offset, code in _template_expressions(text, spans[k][1], spans[k][2]):
                    _scan_tokens(code, label, errors, warnings, first_line + text.count("\n", 0, offset),
                                 declared, warned)
            continue
        if tok.kind != "word":
            continue
        word = tok.text
        if _is(prev, "#"):
            continue                                              # a private class member, #name
        member = _is(prev, ".")
        owner_at = k - 3 if member and k >= 3 and _is(toks[k - 2], "?") else k - 2
        owner = toks[owner_at] if member and owner_at >= 0 and toks[owner_at].kind == "word" else None
        owner_bare = owner is not None and not (owner_at and _is(toks[owner_at - 1], "."))
        # A member of the global object, as far as the tokens show: window.x, self.x, parent.x,
        # unless the script declared that name itself (var self = this).
        on_global = (owner_bare and owner.text in (_GLOBALS | _FRAME_NAMES) and owner.text not in declared)
        on_document = owner_bare and owner.text == "document" and "document" not in declared
        key_or_label = after == ":" and (prev is None or (prev.kind == "punct" and prev.text in "{,;}"))
        defines_method = (after == "(" and _closes_before_block(toks, k + 1) and (
            prev is None or (prev.kind == "punct" and prev.text in "{,;}*")
            or (prev.kind == "word" and prev.text in _MEMBER_STARTERS)))
        # A use of the global of this name: not a member, a key, a method being defined or a local.
        use = not member and not key_or_label and not defines_method and word not in declared
        literal_argument = after == "(" and k + 2 < n and toks[k + 2].kind in ("string", "template")

        if word in _NETWORK and (use or on_global or (word == "sendBeacon" and owner_bare
                                                      and owner.text == "navigator")):
            fail(k, f"uses {word}", _NO_NETWORK)
        elif word == "eval" and (use or on_global):
            fail(k, "uses eval", "Running text as code is refused in a visual (and by certification)")
        elif word == "Function" and use and (after == "(" or (prev is not None and prev.text == "new")):
            fail(k, "builds a function from text (Function)", "Running text as code is refused in a visual")
        elif word == "constructor" and member and (
                literal_argument or (after == "(" and owner is not None and owner.text == "constructor"
                                     and owner_at and _is(toks[owner_at - 1], "."))):
            fail(k, "calls .constructor(...) to build code", "It reaches the Function constructor, which runs "
                 "text as code")
        elif word in ("setTimeout", "setInterval") and (use or on_global) and literal_argument:
            fail(k, f"passes text to {word}", "A timer given text runs it as code; pass a function")
        elif word == "import" and use:
            fail(k, "uses import", "A visual's script is one file with no module loader; put the code in the "
                 "definition's script files")
        elif word in _FRAME_NAMES and (on_global or (use and after in (".", "["))):
            fail(k, f"reaches for {word}", _NO_FRAME)
        elif word == "postMessage" and (on_global or (use and after == "(")):
            fail(k, "uses postMessage", _NO_FRAME)
        elif word in _STORAGE and (use or on_global):
            fail(k, f"uses {word}", "Browser storage is not available to a visual without a privilege; keep "
                 "state in ctx.state while the report is open")
        elif word == "cookie" and member and on_document:
            fail(k, "reads or writes document.cookie", "A sandboxed visual has no cookies to use")
        elif word in ("write", "writeln") and member and on_document:
            fail(k, f"calls document.{word}", "It replaces the document the visual lives in; build elements "
                 "inside ctx.root")
        elif word == "location" and (on_global or (use and after in (".", "=", "["))):
            fail(k, "uses location", "A visual cannot navigate its frame; open links with ctx.openUrl(url)")
        elif word == "open" and on_global:
            fail(k, "calls window.open", "Pop-ups are blocked in the sandbox; open links with ctx.openUrl(url)")
        elif word == "createElement" and after == "(" and k + 2 < n and toks[k + 2].kind == "string" \
                and toks[k + 2].text[1:-1].lower() in _LOADING_TAGS:
            fail(k, f"creates a <{toks[k + 2].text[1:-1].lower()}> element",
                 "Elements that load code or other documents are not allowed in a visual")
        elif word in _HTML_SINKS and member:
            warn(k, "html", f"writes markup through {word}. Pass every value that comes from the model "
                 "through ctx.escape() first, or the data can inject markup.")


# ---------------------------------------------------------------- capabilities

def _property_type(prop: Json) -> Json:
    kind = prop["type"]
    if kind == "toggle":
        return {"bool": True}
    if kind == "color":
        return {"fill": {"solid": {"color": True}}}
    if kind == "number":
        return {"numeric": True}
    if kind == "font_size":
        return {"formatting": {"fontSize": True}}
    if kind == "text":
        return {"text": True}
    return {"enumeration": [{"value": i["value"], "displayName": i["displayName"]} for i in prop["items"]]}


def build_capabilities(spec: Json) -> Json:
    """The capabilities document of a normalized definition (the contents of capabilities.json)."""
    roles, features = spec["roles"], spec["features"]
    groupings = [r["name"] for r in roles if r["kind"] == "grouping"]
    measures = [r["name"] for r in roles if r["kind"] == "measure"]
    data_roles = []
    for role in roles:
        item = {"displayName": role["display_name"], "name": role["name"], "kind": ROLE_KINDS[role["kind"]]}
        if role["description"]:
            item["description"] = role["description"]
        data_roles.append(item)

    categorical: Json = {}
    if groupings:
        categories: Json = ({"for": {"in": groupings[0]}} if len(groupings) == 1
                            else {"select": [{"for": {"in": name}} for name in groupings]})
        # The host cuts the data to the limit without a word, so the visual has to be able to tell.
        # Below one window it asks for one row more than it will use: if that row arrives, rows
        # were cut. From one window up, rows come in windows and the host says when more exist.
        categories["dataReductionAlgorithm"] = ({"window": {"count": WINDOW_ROWS}} if spec["rows"] >= WINDOW_ROWS
                                                else {"top": {"count": spec["rows"] + 1}})
        categorical["categories"] = categories
    if measures:
        categorical["values"] = {"select": [{"for": {"in": name}} for name in measures]}
    mapping: Json = {}
    limits = {r["name"]: {"max": r["max"]} for r in roles if r["max"] is not None}
    if limits:
        mapping["conditions"] = [limits]
    mapping["categorical"] = categorical

    objects: Json = {}
    for card in spec["format"]:
        objects[card["name"]] = {"displayName": card["displayName"], "properties": {
            p["name"]: {"displayName": p["displayName"], **({"description": p["description"]}
                                                           if p.get("description") else {}),
                        "type": _property_type(p)} for p in card["properties"]}}

    caps: Json = {"privileges": [], "dataRoles": data_roles, "dataViewMappings": [mapping], "objects": objects}
    if features["tooltips"]:
        caps["tooltips"] = {"supportedTypes": {"default": True, "canvas": True}, "supportEnhancedTooltips": True}
        wells = [r["name"] for r in roles if r["tooltips"]]
        if wells:
            caps["tooltips"]["roles"] = wells
    if features["sorting"]:
        caps["sorting"] = {"default": {}}
    if features["drilldown"]:
        caps["drilldown"] = {"roles": [features["drilldown"]]}
    if features["highlight"]:
        caps["supportsHighlight"] = True
    if features["keyboard"]:
        caps["supportsKeyboardFocus"] = True
    if features["landing_page"]:
        caps["supportsLandingPage"] = True
        caps["supportsEmptyDataView"] = True
    if features["multi_visual_selection"]:
        caps["supportsMultiVisualSelection"] = True
    return caps


# ---------------------------------------------------------------- the script

def _js_literal(value: Any) -> str:
    """``value`` as a JavaScript literal that is safe wherever the host puts the script."""
    return json.dumps(value, ensure_ascii=True, separators=(",", ":")).replace("<", _BS + "u003c")


def _kit_source(name: str) -> str:
    return safe_io.read_text(_KIT_DIR / name)[0]


def _config(spec: Json) -> Json:
    features = spec["features"]
    return {
        "name": spec["name"], "displayName": spec["display_name"], "rows": spec["rows"],
        "roles": [{"name": r["name"], "displayName": r["display_name"], "kind": r["kind"],
                   "drill": r["name"] == features["drilldown"]} for r in spec["roles"]],
        "format": spec["format"],
        "features": {"selection": features["selection"], "contextMenu": features["context_menu"],
                     "tooltips": features["tooltips"], "keyboard": features["keyboard"],
                     "landingPage": features["landing_page"], "limitNotice": features["limit_notice"],
                     "fetchMore": _fetches_more(spec)},
        "landing": spec["landing"], "emptyMessage": spec["empty_message"],
    }


def _fetches_more(spec: Json) -> bool:
    return spec["rows"] > WINDOW_ROWS and any(r["kind"] == "grouping" for r in spec["roles"])


_HOOKS = ("render", "main", "tooltip", "selectionChanged")


def assemble_script(spec: Json, minify: bool = True) -> str:
    """The script of the package: the kit, the author's modules in one function, the registration."""
    def ship(name: str, source: str) -> str:
        text = source.replace("\r\n", "\n").replace("\r", "\n")
        return jm.minify_js(text) if minify else f"/* ==== {name} ==== */\n" + text.strip("\n")

    guid = spec["guid"]
    register = (_kit_source("register.js")
                .replace('"__VK_GUID__"', _js_literal(guid))
                .replace('"__VK_DISPLAY_NAME__"', _js_literal(spec["display_name"]))
                .replace('"__VK_API_VERSION__"', _js_literal(API_VERSION)))
    hooks = ",".join(f'{h}:typeof {h}==="function"?{h}:null' for h in _HOOKS)
    parts = [f"var {guid};", "(function () {", '"use strict";',
             f"var KIT = {{ kit: {_js_literal(KIT_VERSION)}, config: {_js_literal(_config(spec))} }};"]
    parts += [ship(name, _kit_source(name)) for name in _KIT_MODULES]
    # The author's code gets a scope of its own: its names cannot collide with the kit's, and
    # every visual on a page gets its own copy of the module state.
    parts.append("KIT.load = function (APP) {")
    parts += [ship(name, source) for name, source in spec["modules"] if source.strip()]
    parts.append(f"return {{{hooks}}};")
    parts.append("};")
    parts.append(ship("register.js", register))
    parts.append(f'{guid} = {{ "default": plugin }};')
    parts.append("}());")
    return "\n".join(parts) + "\n"


def assemble_css(spec: Json, minify: bool = True) -> str:
    kit, own = _kit_source("kit.css"), spec["css"].replace("\r\n", "\n").replace("\r", "\n")
    if minify:
        return jm.minify_css(kit) + jm.minify_css(own)
    return kit.rstrip("\n") + "\n\n" + own.strip("\n") + "\n"


# ---------------------------------------------------------------- the package

def _default_icon() -> bytes:
    """A 20 by 20 PNG: three bars. Built here so the repository carries no binary file."""
    size, bars = 20, ((3, 15), (8, 10), (13, 17))
    rows = []
    for y in range(size):
        row = bytearray([0])                                       # filter type: none
        for x in range(size):
            on = any(top <= y < top + 3 and 2 <= x < 2 + width for top, width in bars)
            row += bytes((0x4B, 0x4F, 0x58, 0xFF)) if on else bytes(4)
        rows.append(bytes(row))

    def chunk(kind: bytes, data: bytes) -> bytes:
        return struct.pack(">I", len(data)) + kind + data + struct.pack(">I", zlib.crc32(kind + data) & 0xFFFFFFFF)

    header = struct.pack(">IIBBBBB", size, size, 8, 6, 0, 0, 0)    # 8-bit RGBA
    return (_PNG_SIGNATURE + chunk(b"IHDR", header) + chunk(b"IDAT", zlib.compress(b"".join(rows), 9))
            + chunk(b"IEND", b""))


def _archive(members: Sequence[Tuple[str, bytes]]) -> bytes:
    """A zip whose bytes depend only on its members: fixed timestamps, attributes and order."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as z:
        for name, data in members:
            info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
            info.create_system = 0                 # otherwise 0 on Windows and 3 elsewhere
            info.external_attr = 0x10 if name.endswith("/") else 0
            info.compress_type = zipfile.ZIP_STORED if name.endswith("/") else zipfile.ZIP_DEFLATED
            z.writestr(info, data, compresslevel=9)
    return buffer.getvalue()


def package_files(spec: Json, script: str, css: str) -> List[Tuple[str, bytes]]:
    """The members of the .pbiviz, in archive order, as the official packager lays them out."""
    guid = spec["guid"]
    visual = {"name": spec["name"], "displayName": spec["display_name"], "guid": guid,
              "visualClassName": "Visual", "version": spec["version"], "description": spec["description"],
              "supportUrl": spec["support_url"], "gitHubUrl": spec["github_url"]}
    metadata = {
        "visual": visual, "author": spec["author"], "apiVersion": API_VERSION, "style": "style/visual.less",
        "stringResources": {}, "capabilities": build_capabilities(spec),
        "content": {"js": script, "css": css,
                    "iconBase64": "data:image/png;base64," + base64.b64encode(spec["icon"]).decode("ascii")},
        "visualEntryPoint": "", "externalJS": [], "assets": {"icon": "assets/icon.png"},
    }
    resource = f"resources/{guid}.pbiviz.json"
    package = {"version": spec["version"], "author": spec["author"],
               "resources": [{"resourceId": "rId0", "sourceType": 5, "file": resource}],
               "visual": visual, "metadata": {"pbivizjson": {"resourceId": "rId0"}}}
    return [("package.json", (json.dumps(package, indent=2, ensure_ascii=False) + "\n").encode("utf-8")),
            ("resources/", b""),
            (resource, json.dumps(metadata, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))]


def _feature_notes(spec: Json) -> List[Json]:
    """One entry per native behaviour: {feature, label, applies, why}. ``applies`` is whether this
    visual does it, not whether the kit can: a visual of measures only has no data point, so it
    cannot cross-filter, offer drillthrough or feed a report-page tooltip whatever is switched on."""
    features = spec["features"]
    grouping = any(r["kind"] == "grouping" for r in spec["roles"])
    measure = any(r["kind"] == "measure" for r in spec["roles"])
    off = "switched off in the definition"
    no_point = "the visual has no grouping role, so it has no data point to act on"
    notes: List[Json] = []

    def add(feature: str, label: str, applies: Any, why: str = "") -> None:
        notes.append({"feature": feature, "label": label, "applies": bool(applies), "why": "" if applies else why})

    add("selection", "cross-filter and cross-highlight other visuals on click", features["selection"],
        off if grouping else no_point)
    add("multi_visual_selection", "Ctrl+click selection across visuals", features["multi_visual_selection"],
        off if features["selection"] else "there is no selection" if grouping else no_point)
    add("bookmarks", "selection restored by bookmarks", features["selection"], off if grouping else no_point)
    add("cross_highlight", "is cross-highlighted by other visuals", features["highlight"],
        off if measure else "the visual has no measure role, and highlights arrive on measure values")
    add("context_menu", "context menu on right-click", features["context_menu"], off)
    add("drillthrough", "drillthrough, include and exclude from a data point's context menu",
        features["context_menu"] and grouping, no_point if features["context_menu"] else "the context menu is " + off)
    add("tooltips", "host tooltips on hover", features["tooltips"], off)
    add("report_page_tooltips", "report-page tooltips", features["tooltips"] and grouping,
        no_point.replace("to act on", "whose identity a tooltip page could be filtered by")
        if features["tooltips"] else "tooltips are " + off)
    add("drilldown", "drill-down" + (f" on the role '{features['drilldown']}'" if features["drilldown"] else ""),
        features["drilldown"], "not asked for (features.drilldown names the role)")
    add("keyboard", "keyboard focus, arrow keys, Enter and Space", features["keyboard"], off)
    add("landing_page", "landing page while no field is bound", features["landing_page"], off)
    add("sorting", "sort by a field from the visual's menu", features["sorting"], off)
    add("fetch_more_data", f"fetches rows beyond the first window of {WINDOW_ROWS:,}", _fetches_more(spec),
        f"rows is {spec['rows']:,}, which one window holds" if grouping else "a visual of measures only has one row")
    add("row_limit_notice", f"says so when the data was cut at the limit of {spec['rows']:,} rows",
        features["limit_notice"] and grouping, off if grouping else "a visual of measures only has one row")
    add("format_pane", "format pane", spec["format"], "the definition declares no format cards")
    for feature, label in (("rendering_events", "rendering started / finished / failed events"),
                           ("launch_url", "links opened by the host"), ("theme_colors", "theme colours"),
                           ("high_contrast", "high-contrast mode")):
        add(feature, label, True)
    return notes


def _feature_report(spec: Json) -> Json:
    """{feature: on?} from the notes; drilldown carries the role it applies to."""
    report = {note["feature"]: note["applies"] for note in _feature_notes(spec)}
    report["drilldown"] = spec["features"]["drilldown"] or False
    return report


def validate_definition(definition: Any, base_dir: Optional[str] = None) -> Json:
    """Check a definition without building it: {"valid", "errors", "warnings"}.

    Reads the script and stylesheet files the definition names (relative to ``base_dir``) and
    runs the safety pass over the script, so everything a build would refuse is listed here.
    """
    spec, errors, warnings = _normalize(definition, base_dir)
    if spec:
        _check_sources(spec, errors, warnings)
    return {"valid": not errors, "errors": errors, "warnings": warnings}


def _check_sources(spec: Json, errors: List[str], warnings: List[str]) -> None:
    # The host may place the script and the stylesheet inside <script> and <style> elements, so
    # text that closes one is refused, as it is for a measure-driven page.
    for name, source in spec["modules"]:
        try:
            jm._check_closers(source.replace("\r\n", "\n").replace("\r", "\n"), name, "js")
        except jm.BuildError as e:
            errors.append(str(e))
        bad, hints = check_script_safety(source, name)
        errors.extend(bad)
        warnings.extend(hints)
    try:
        jm._check_closers(spec["css"].replace("\r\n", "\n").replace("\r", "\n"), "css", "css")
    except jm.BuildError as e:
        errors.append(str(e))
    if re.search(r"@import|url\(\s*['\"]?\s*(https?:)?//", spec["css"], re.IGNORECASE):
        warnings.append("css: refers to an external resource (@import or a web url). The visual has no web "
                        "access, so it will not load; embed it as a data: URI instead.")


def _failing_module(spec: Json) -> str:
    """Which of the author's modules Node refuses, and on which line, checked one at a time.

    Node's own message quotes the offending line. That text must not travel into an error: a
    definition may name any text file in its folder as a script, and the error would then read
    the file out. So only the module, the line and the kind of error are reported.
    """
    for name, source in spec["modules"]:
        # Checked the way it ships: inside a function, in strict mode. One line of wrapper.
        check = jm.node_syntax_check('(function () { "use strict";\n' + source + "\n});")
        if check.startswith("failed"):
            at = re.search(r"runtime\.js:(\d+)", check)
            kind = re.search(r"\b([A-Z][A-Za-z]*Error)\b", check)
            return (f"{name}: " + (f"line {int(at.group(1)) - 1}: " if at else "") + "is not valid JavaScript"
                    + (f" ({kind.group(1)})" if kind else ""))
    return ""


def build_custom_visual(definition: Any, output_path: Union[str, os.PathLike, None] = None, *,
                        base_dir: Optional[str] = None, minify: bool = True,
                        check_with_node: Optional[bool] = None) -> Json:
    """Build the .pbiviz of a definition. Writes ``output_path`` when given; reports either way.

    definition: {name, roles: [{name, kind: 'grouping' | 'measure', display_name?, max?, tooltips?}],
        js | js_files | js_folder, css | css_file, display_name?, description?, version?, guid?,
        author?: {name, email}, rows?, features?: {selection, highlight, context_menu, tooltips,
        drilldown, keyboard, landing_page, multi_visual_selection, sorting, limit_notice}, format?:
        [{name, display_name?, properties: [{name, type: toggle | color | number | font_size | text
        | dropdown, default?, min?, max?, items?}]}], landing?: {title, text}, empty_message?,
        icon_file?}
      rows: the most rows the visual takes (default 1000, up to 1,048,576). The host cuts the
        data to the limit silently; the visual then gets ctx.truncated = true and, unless
        features.limit_notice is false, shows "Showing the first N rows". From 30,000 up, rows
        arrive a window of 30,000 at a time and the visual asks for more until it has ``rows``.
    base_dir: the folder the definition's file names are read from. Files outside it are never
        read, and without it (an inline definition) no file is read at all.
    check_with_node: None checks the script with Node when it is on PATH; False never checks.

    Returns {success, errors, warnings, path, written, guid, name, display_name, version,
    api_version, size_bytes, sha256, script_chars, css_chars, capabilities, features,
    feature_notes, roles, format_pane, privileges, node_check, usage, notes}. ``features`` says
    for each native behaviour whether THIS visual does it; ``feature_notes`` adds the reason when
    it does not. Never raises for a bad definition or an unwritable path: ``success`` is False and
    ``errors`` says why, and nothing is written. The same definition and sources give the same
    bytes, and the guid follows from the name, so a report that uses the visual keeps working
    when it is built again.
    """
    spec, errors, warnings = _normalize(definition, base_dir)
    result: Json = {"success": False, "errors": errors, "warnings": warnings, "written": False, "path": None}
    if output_path is not None:
        try:
            result["path"] = os.path.abspath(os.fspath(output_path))
        except (TypeError, ValueError):
            errors.append(f"output_path: must be a file path, got {output_path!r:.60}")
    if not spec:
        return result
    _check_sources(spec, errors, warnings)
    result.update(guid=spec["guid"], name=spec["name"], display_name=spec["display_name"],
                  version=spec["version"], api_version=API_VERSION)
    if errors:
        return result

    script, css = assemble_script(spec, minify), assemble_css(spec, minify)
    node_check = "skipped" if check_with_node is False else jm.node_syntax_check(script)
    if node_check.startswith("failed"):
        errors.append("the script does not parse, so the visual would stay blank: "
                      + (_failing_module(spec) or "node refuses the assembled script"))
        result["node_check"] = "failed"
        return result
    if node_check.startswith("skipped") and check_with_node is not False:
        warnings.append("The script was not syntax-checked: node is not available on this machine.")
    if not any(re.search(r"\b(function|var|let|const)\s+(render|main)\b|\bAPP\.main\s*=", source)
               for _, source in spec["modules"]):
        warnings.append("No render(ctx) or main(app) function was found in the script: the visual will show "
                        "an error instead of drawing.")
    if not spec["author"]["name"]:
        warnings.append("author.name is empty. Power BI shows the author when a visual is imported from a "
                        "file, and AppSource requires one.")

    data = _archive(package_files(spec, script, css))
    if output_path is not None:
        target = Path(result["path"])
        try:
            target.parent.mkdir(parents=True, exist_ok=True)
            safe_io.write_bytes(target, data)
        except (OSError, ValueError) as e:
            errors.append(f"output_path: {result['path']} could not be written "
                          f"({_reason(e) if isinstance(e, OSError) else 'it is not a usable path'})")
            return result
        result["written"] = True
    result.update(
        success=True, size_bytes=len(data), sha256=hashlib.sha256(data).hexdigest(),
        script_chars=len(script), css_chars=len(css), capabilities=build_capabilities(spec),
        features=_feature_report(spec), feature_notes=_feature_notes(spec), privileges=[], node_check=node_check,
        roles=[{"name": r["name"], "display_name": r["display_name"], "kind": r["kind"], "max": r["max"]}
               for r in spec["roles"]],
        format_pane=[{"card": c["name"], "properties": [f"{p['name']} ({p['type']})" for p in c["properties"]]}
                     for c in spec["format"]],
        usage=_usage(spec, result["path"]), notes=_notes(spec))
    return result


def _usage(spec: Json, path: Optional[str]) -> Json:
    """How to place the visual with a report spec (pbir_apply_spec)."""
    return {"custom_visuals": [{"pbiviz": path or f"{spec['name']}.pbiviz", "alias": spec["name"]}],
            "element": {"type": "visual", "custom": spec["name"], "name": spec["name"], "x": 24, "y": 24,
                        "width": 480, "height": 320,
                        "fields": {r["name"]: {"column" if r["kind"] == "grouping" else "measure":
                                               "<Table>.<Field>"} for r in spec["roles"]}}}


def _notes(spec: Json) -> List[str]:
    notes = [
        "Built without the pbiviz toolchain. Not verified here: the package has not been loaded by Power BI; "
        "reload the report and look at a screenshot.",
        "A private custom visual is not certified: a tenant that allows only certified visuals blocks it, and "
        "it is not rendered by export to PDF or PowerPoint or in e-mail subscriptions.",
        "The script was checked for network access, eval and reaching out of its frame, by reading its "
        "tokens. That is a review aid, not a sandbox.",
    ]
    if spec["features"]["drilldown"]:
        notes.append(f"Drill-down is on for '{spec['features']['drilldown']}': drop a hierarchy or several "
                     "columns on that role. The visual always receives one column for it: the level shown, or, "
                     "with levels expanded, the levels joined into one value. In a report file (PBIR) the "
                     "projections of the level shown must carry \"active\": true, or Desktop reports too many "
                     "columns in the role.")
    if any(r["kind"] == "grouping" for r in spec["roles"]):
        notes.append(f"The visual takes at most {spec['rows']:,} rows ('rows' in the definition). The host cuts "
                     "the data there without a word; the render code then sees ctx.truncated = true"
                     + (", and the visual shows \"Showing the first N rows\"." if spec["features"]["limit_notice"]
                        else " (the visual's own notice is switched off)."))
    if _fetches_more(spec):
        notes.append(f"Rows arrive {WINDOW_ROWS:,} at a time; the visual draws each time more have arrived "
                     f"and asks again until it holds {spec['rows']:,} or the host has no more.")
    return notes


# ---------------------------------------------------------------- inspector

_SCRIPT_SIGNS = {
    "selection": ("createSelectionManager",),
    "context_menu": ("showContextMenu",),
    "tooltips": ("tooltipService",),
    "bookmarks": ("registerOnSelectCallback",),
    "rendering_events": ("renderingStarted", "renderingFinished"),
    "launch_url": ("launchUrl",),
    "high_contrast": ("isHighContrast",),
    "interaction_check": ("allowInteractions",),
    "fetch_more_data": ("fetchMoreData",),
    "drill_api": (".drill(",),
    "persist_properties": ("persistProperties",),
    "json_filter": ("applyJsonFilter",),
}
_SCRIPT_RISKS = {
    "network": ("fetch(", "XMLHttpRequest", "WebSocket", "EventSource", "sendBeacon"),
    "eval": ("eval(", "new Function("),
    "storage": ("localStorage", "sessionStorage", "indexedDB"),
}
# Code every script built here holds, whatever its definition switched on.
_KIT_SIGNS = ("KIT.load = function (APP) {", "createSelectionManager", "registerOnSelectCallback",
              "showContextMenu", "tooltipService", "getFormattingModel", "renderingFinished")


def _shaped(value: Any, kind: type, where: str, warnings: List[str]) -> Any:
    """``value`` when it has the shape the format gives it, else an empty one (and a warning): a
    package from elsewhere may hold anything, and a wrong part must not hide the rest."""
    if isinstance(value, kind):
        return value
    if value is not None:
        warnings.append(f"{where} is not {'an object' if kind is dict else 'a list'}; it is ignored.")
    return kind()


def _mapping_kinds(mappings: List[Any]) -> List[str]:
    kinds: List[str] = []
    for mapping in mappings:
        kinds += [k for k in ("categorical", "table", "matrix", "single", "scriptResult")
                  if isinstance(mapping, dict) and k in mapping and k not in kinds]
    return kinds


def _windowed(node: Any) -> bool:
    if isinstance(node, dict):
        reduction = node.get("dataReductionAlgorithm")
        return (isinstance(reduction, dict) and "window" in reduction) or any(_windowed(v) for v in node.values())
    return isinstance(node, list) and any(_windowed(v) for v in node)


def _kit_config(script: str, guid: str) -> Optional[Json]:
    """The configuration of a script built here, or None for any other script.

    The kit's code is the same in every package and its configuration says which parts are in
    use, so for such a script the inspector can be exact. The marker is only text, though, and
    anyone can write it: it counts where the build puts it (the opening of the script) and only
    together with the kit's own code.
    """
    head = f'var {guid};\n(function () {{\n"use strict";\nvar KIT = {{ kit: {_js_literal(KIT_VERSION)}, config: '
    if not script.startswith(head) or not all(sign in script for sign in _KIT_SIGNS) \
            or f'{guid} = {{ "default": plugin }};' not in script:
        return None
    try:
        config = json.JSONDecoder().raw_decode(script, len(head))[0]
    except ValueError:
        return None
    return config if isinstance(config, dict) and isinstance(config.get("features"), dict) else None


def inspect_custom_visual(path: Union[str, os.PathLike]) -> Json:
    """Describe a custom visual from its .pbiviz, or from the folder a report keeps it unpacked in.

    Returns {name, display_name, guid, version, api_version, author, description, data_roles,
    data_mappings, format_pane, privileges, features, script_mentions, built_by_kit, sizes,
    notes, warnings}. ``features`` says which native behaviours the visual supports as far as
    its capabilities and its script show: a capability switch is a fact, a host call found in
    the script only shows that the code is there, not that it is reached. For a package built
    here the script switches are read from the configuration it carries. Whatever the script
    holds, a visual without a grouping role has no data point, so cross-filtering, drillthrough
    and report-page tooltips are never reported for it.
    Raises pbir_packages.PbivizError when the path is not a custom visual package.
    """
    full = os.fspath(path)
    package = pbir_packages.read_unpacked(full) if os.path.isdir(full) else pbir_packages.read_pbiviz(full)
    guid = package["guid"]
    where = os.path.basename(full)
    try:
        meta = json.loads(package["files"][f"resources/{guid}.pbiviz.json"].decode("utf-8-sig"))
        manifest = json.loads(package["files"]["package.json"].decode("utf-8-sig"))
    except ValueError as e:
        raise pbir_packages.PbivizError(f"{where}: the package metadata is not valid JSON ({e})") from e
    if not isinstance(meta, dict):
        raise pbir_packages.PbivizError(f"{where}: resources/{guid}.pbiviz.json must hold an object")
    visual = meta.get("visual") if meta.get("visual") is not None else manifest.get("visual")
    caps, content = meta.get("capabilities"), meta.get("content")
    for name, part in (("visual", visual), ("capabilities", caps), ("content", content)):
        if part is not None and not isinstance(part, dict):
            raise pbir_packages.PbivizError(f"{where}: '{name}' in resources/{guid}.pbiviz.json must hold an "
                                            f"object, not {type(part).__name__}")
    visual, caps, content = visual or {}, caps or {}, content or {}

    warnings: List[str] = []
    notes: List[str] = []
    script = content.get("js") if isinstance(content.get("js"), str) else ""
    css = content.get("css") if isinstance(content.get("css"), str) else ""
    if not script:
        warnings.append("The package holds no script (content.js), so nothing can be said about what it does.")
    tooltips = _shaped(caps.get("tooltips"), dict, "capabilities.tooltips", warnings)
    supported = _shaped(tooltips.get("supportedTypes"), dict, "capabilities.tooltips.supportedTypes", warnings)
    drill = _shaped(caps.get("drilldown"), dict, "capabilities.drilldown", warnings)
    roles = [r for r in _shaped(caps.get("dataRoles"), list, "capabilities.dataRoles", warnings) if isinstance(r, dict)]
    mappings = _shaped(caps.get("dataViewMappings"), list, "capabilities.dataViewMappings", warnings)
    objects = _shaped(caps.get("objects"), dict, "capabilities.objects", warnings)
    privileges = _shaped(caps.get("privileges"), list, "capabilities.privileges", warnings)
    kinds = _mapping_kinds(mappings)
    has = {key: all(sign in script for sign in signs) for key, signs in _SCRIPT_SIGNS.items()}
    # Only a grouping field gives a data point an identity to select, drill through or look up.
    grouping = any(r.get("kind") in ("Grouping", "GroupingOrMeasure") for r in roles)

    selects = has["selection"] and ".select(" in script
    menu, tips, restores = has["context_menu"], has["tooltips"], has["bookmarks"]
    fetches = has["fetch_more_data"] and _windowed(mappings)
    pane = bool(objects) and ("getFormattingModel" in script or "enumerateObjectInstances" in script)
    config = _kit_config(script, guid)
    if config is not None:
        switches = config["features"]
        selects = selects and bool(switches.get("selection"))
        menu = menu and bool(switches.get("contextMenu"))
        tips = tips and bool(switches.get("tooltips"))
        fetches = has["fetch_more_data"] and bool(switches.get("fetchMore"))
        pane = pane and bool(config.get("format"))
    selects = selects and grouping
    features = {
        "selection": selects,
        "cross_highlight": caps.get("supportsHighlight") is True,
        "context_menu": menu,
        "drillthrough": menu and grouping,
        "tooltips": tips,
        "report_page_tooltips": tips and supported.get("canvas") is True and grouping,
        "modern_tooltips": tooltips.get("supportEnhancedTooltips") is True,
        "drilldown": [str(r) for r in _shaped(drill.get("roles"), list, "capabilities.drilldown.roles", warnings)],
        "keyboard": caps.get("supportsKeyboardFocus") is True,
        "landing_page": caps.get("supportsLandingPage") is True,
        "multi_visual_selection": caps.get("supportsMultiVisualSelection") is True and selects,
        "sorting": "sorting" in caps,
        "bookmarks": restores and selects,
        "fetch_more_data": fetches,
        "format_pane": pane,
        "rendering_events": has["rendering_events"],
        "launch_url": has["launch_url"],
        "high_contrast": has["high_contrast"],
        "interaction_check": has["interaction_check"],
    }
    if not grouping:
        notes.append("No grouping role: the visual has no data point, so it cannot cross-filter other visuals, "
                     "offer drillthrough or feed a report-page tooltip, whatever its script holds.")
    if features["cross_highlight"] and kinds == ["table"]:
        warnings.append("supportsHighlight is set, but a table data mapping never carries highlights.")
    if selects and not menu:
        warnings.append("Selects data points but never opens the context menu: no drillthrough, drill or "
                        "include/exclude from this visual.")
    if selects and not restores:
        warnings.append("Selects data points but does not register for selection changes from the host: a "
                        "bookmark cannot restore its selection.")
    if "privileges" not in caps:
        warnings.append("No privileges entry in the capabilities (required from API 4.6.0).")
    unknown = sorted(set(caps) - CAPABILITY_KEYS)
    if unknown:
        warnings.append(f"Capabilities hold keys this API version does not define: {', '.join(unknown)}.")

    format_pane: Json = {}
    for name, card in objects.items():
        card = _shaped(card, dict, f"capabilities.objects.{name}", warnings)
        format_pane[str(name)] = sorted(str(p) for p in _shaped(
            card.get("properties"), dict, f"capabilities.objects.{name}.properties", warnings))
    author = meta.get("author") if isinstance(meta.get("author"), dict) else manifest.get("author")
    return {
        "name": str(visual.get("name", "")), "display_name": str(visual.get("displayName", "")), "guid": guid,
        "version": str(visual.get("version", "")), "api_version": str(meta.get("apiVersion", "")),
        "class_name": str(visual.get("visualClassName", "")),
        "description": str(visual.get("description", "")),
        "author": author if isinstance(author, dict) else {},
        "data_roles": [{"name": r.get("name"), "display_name": r.get("displayName"), "kind": r.get("kind")}
                       for r in roles],
        "data_mappings": kinds,
        "format_pane": format_pane,
        "privileges": [str(p.get("name")) if isinstance(p, dict) else str(p) for p in privileges],
        "features": features,
        "script_mentions": sorted(key for key, signs in _SCRIPT_RISKS.items() if any(s in script for s in signs)),
        "registers_plugin": f"plugins.{guid}" in script or f'plugins["{guid}"]' in script,
        "built_by_kit": config is not None,
        "sizes": {"script_chars": len(script), "css_chars": len(css),
                  "package_bytes": sum(len(v) for v in package["files"].values())},
        "notes": notes,
        "warnings": warnings,
    }


# ---------------------------------------------------------------- text for a tool result

def _on_off(features: Json) -> str:
    on = [k for k, v in features.items() if v]
    off = [k for k, v in features.items() if not v]
    return f"on: {', '.join(on) or 'none'}" + (f"\n  off: {', '.join(off)}" if off else "")


def describe_build(result: Json) -> str:
    """A short human-readable summary of a build result."""
    if not result.get("success"):
        lines = [f"Not built: {len(result.get('errors', []))} problem(s)."]
        lines += [f"  - {e}" for e in result.get("errors", [])]
        lines += [f"Warning: {w}" for w in result.get("warnings", [])]
        return "\n".join(lines)
    where = f"written to {result['path']}" if result["written"] else "not written (no output path)"
    lines = [f"{result['display_name']} ({result['guid']}), version {result['version']}, API "
             f"{result['api_version']}: {result['size_bytes']:,} bytes, {where}",
             "Data roles: " + ", ".join(f"{r['name']} ({r['kind']})" for r in result["roles"]),
             "What this visual does:"]
    lines += [f"  {n['label']}: " + ("yes" if n["applies"] else f"no, {n['why']}") for n in result["feature_notes"]]
    lines += ["Format pane: " + ("; ".join(f"{c['card']}: {', '.join(c['properties'])}" for c in result["format_pane"])
                                 or "none"),
              f"Script check (node): {result['node_check']}",
              "Report spec: " + json.dumps(result["usage"], ensure_ascii=False)]
    lines += [f"Warning: {w}" for w in result["warnings"]]
    lines += [f"Note: {n}" for n in result["notes"]]
    return "\n".join(lines)


def describe_inspection(info: Json) -> str:
    """A short human-readable summary of an inspection."""
    features = dict(info["features"])
    drill = features.pop("drilldown")
    features["drilldown"] = bool(drill)
    lines = [f"{info['display_name'] or info['name']} ({info['guid']}), version {info['version']}, API "
             f"{info['api_version']}" + (" [built by this server's visual kit]" if info["built_by_kit"] else ""),
             "Data roles: " + (", ".join(f"{r['name']} ({r['kind']})" for r in info["data_roles"]) or "none")
             + f"; mapping: {', '.join(info['data_mappings']) or 'none'}",
             "Features " + _on_off(features) + (f"\n  drill roles: {', '.join(drill)}" if drill else ""),
             "Format pane: " + ("; ".join(f"{k}: {', '.join(str(p) for p in v)}"
                                          for k, v in info["format_pane"].items()) or "none"),
             "Privileges: " + (", ".join(str(p) for p in info["privileges"]) or "none"),
             f"Script: {info['sizes']['script_chars']:,} characters"
             + (f"; mentions {', '.join(info['script_mentions'])}" if info["script_mentions"] else "")]
    lines += [f"Note: {n}" for n in info["notes"]]
    lines += [f"Warning: {w}" for w in info["warnings"]]
    return "\n".join(lines)


# ---------------------------------------------------------------- running a package in Node

def simulate(package_path: Union[str, os.PathLike], steps: Sequence[Json], *,
             host: Optional[Json] = None, timeout: int = 60) -> Json:
    """Run a packaged visual in Node against a model of the host and a minimal DOM.

    steps: [{"op": "update", "data": {"columns": [{"role", "name", "kind", "format"?, "type"?}],
    "rows": [[...]], "highlights"?: [[...]], "objects"?: {...}, "segment"?: bool}}, {"op": "click",
    "row": 0, "ctrl"?: bool}, {"op": "contextmenu", "row": 0 | null, "x", "y"}, {"op": "hover",
    "row": 0}, {"op": "leave"}, {"op": "key", "row": 0, "key": "Enter"}, {"op": "host_select",
    "rows": [...]}, {"op": "formatting_model"}, {"op": "destroy"}, ...]
    Returns {ok, plugin, states: [the DOM after each step], log: [every host call], console}, or
    {"ok": False, "skipped": True} when node is not on PATH.

    THIS EXECUTES THE VISUAL'S SCRIPT on this machine, outside any sandbox. It is for code you
    wrote or reviewed (the test-suite uses it); it is deliberately not part of the build.
    """
    node = shutil.which("node")
    if not node:
        return {"ok": False, "skipped": True, "error": "node is not on PATH"}
    full = os.fspath(package_path)
    package = pbir_packages.read_unpacked(full) if os.path.isdir(full) else pbir_packages.read_pbiviz(full)
    meta = json.loads(package["files"][f"resources/{package['guid']}.pbiviz.json"].decode("utf-8-sig"))
    folder = tempfile.mkdtemp(prefix="custom-visual-")
    job = os.path.join(folder, "job.json")
    try:
        safe_io.write_json(job, {"script": meta["content"]["js"], "guid": package["guid"], "host": host or {},
                                 "steps": list(steps)})
        done = subprocess.run([node, str(_KIT_DIR / "harness.js"), job], capture_output=True, text=True,
                              encoding="utf-8", errors="replace", timeout=timeout)
    except (OSError, subprocess.SubprocessError) as e:
        return {"ok": False, "error": f"{type(e).__name__}: node could not be run"}
    finally:
        shutil.rmtree(folder, ignore_errors=True)
    try:
        return json.loads(done.stdout)
    except ValueError:
        return {"ok": False, "error": (done.stderr or done.stdout or "no output from node").strip()[:2000]}


__all__ = ["API_VERSION", "DefinitionError", "visual_guid", "load_definition", "validate_definition",
           "check_script_safety", "build_capabilities", "build_custom_visual", "inspect_custom_visual",
           "describe_build", "describe_inspection", "simulate"]
