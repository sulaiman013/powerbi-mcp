"""
Power BI PBIP (Power BI Project) connector: offline editing of the files on disk.

A PBIP project is a folder holding

    <Name>.pbip                         points at the report folder
    <Name>.Report/
      definition.pbir                   points at the semantic model (byPath) or a remote one
      definition/                       PBIR-Enhanced: report.json, pages/<page>/page.json,
                                        pages/<page>/visuals/<visual>/visual.json (+ mobile.json),
                                        bookmarks/*.bookmark.json, reportExtensions.json
      report.json                       PBIR-Legacy: the whole report in one file
    <Name>.SemanticModel/
      definition/                       TMDL: model.tmdl, relationships.tmdl, tables/*.tmdl,
                                        cultures/*.tmdl, roles/*.tmdl, expressions.tmdl
      diagramLayout.json

This module is the public face of that editing: project discovery, safe renames, offline
authoring (measures, date table, calculation group, hierarchy, pages, visuals) and validation.
The heavy lifting lives in focused modules:

    tmdl_scan      object tree and model catalog for TMDL text
    pbip_report    structural reading / rewriting of report JSON
    pbip_rename    the transactional rename engine
    safe_io        atomic, format-preserving file I/O and all-or-nothing commits

Every write goes through safe_io, so a file keeps its encoding (BOM or not) and line endings,
and an operation that changes several files changes all of them or none.
"""
import json
import logging
import os
import re
import shutil
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import pbip_json
import pbip_mquery
import pbip_rename
import pbip_report
import pbir_authoring
import safe_io
import tmdl_authoring
import tmdl_scan
from pbip_report import RenameOp

logger = logging.getLogger(__name__)

BACKUP_FOLDER = ".pbip-backups"
# Namespace for the lineage tags given to measures created by an upsert, so that building the
# same measure in the same table always yields the same tag (and the same bytes on disk).
LINEAGE_NAMESPACE = uuid.UUID("6f0f2f0e-7c1b-5d8a-9b43-2a5c0e9d4b17")
MAX_WARNINGS_IN_MESSAGE = 12

# Characters that make this module quote a TMDL name. TMDL itself only requires quotes around
# whitespace, '.', '=', ':' and "'", but quoting more is always valid.
TMDL_SPECIAL_CHARS = set(' \t\n\r\'\"[]{}().,;:!@#$%^&*+-=<>?/\\|`~')
TMDL_RESERVED_WORDS = {'table', 'column', 'measure', 'relationship', 'partition', 'hierarchy', 'level',
                       'annotation', 'expression', 'from', 'to', 'true', 'false', 'null'}


def needs_tmdl_quoting(name: str) -> bool:
    """True when ``name`` should be single-quoted in TMDL or DAX: it has a space or another
    special character, starts with a digit, or is a reserved word."""
    if not name:
        return False
    if any(c in TMDL_SPECIAL_CHARS for c in name):
        return True
    if name[0].isdigit():
        return True
    return name.lower() in TMDL_RESERVED_WORDS


def quote_tmdl_name(name: str) -> str:
    """``name`` single-quoted (inner quotes doubled) when it needs it, else unchanged."""
    if needs_tmdl_quoting(name):
        return "'" + name.replace("'", "''") + "'"
    return name


def unquote_tmdl_name(name: str) -> str:
    """Remove TMDL quotes from a name if present."""
    return tmdl_scan.unquote_name(name)


def quote_mcode_name(name: str) -> str:
    """``name`` as a Power Query identifier: bare when it is a regular identifier, otherwise
    #"..." with inner double quotes doubled."""
    return pbip_mquery.quote_identifier(name)


def fix_dax_table_references(dax_expression: str, table_names: List[str]) -> str:
    """Quote table names containing spaces that a DAX expression uses without quotes:
    ``SUM(Leads Sales Data[Amount])`` becomes ``SUM('Leads Sales Data'[Amount])``. Text inside
    string literals, comments and already quoted names is never touched."""
    return pbip_rename.quote_unquoted_tables(dax_expression, table_names)[0]


@dataclass
class PBIPProject:
    """Represents a PBIP project structure"""
    root_path: Path
    pbip_file: Path
    semantic_model_folder: Optional[Path]
    report_folder: Optional[Path]
    report_json_path: Optional[Path]  # legacy: <Report>/report.json; enhanced: definition/report.json
    tmdl_files: List[Path]
    backup_path: Optional[Path] = None
    is_pbir_enhanced: bool = False
    pbir_definition_folder: Optional[Path] = None  # <Report>/definition
    visual_json_files: List[Path] = field(default_factory=list)
    cultures_files: List[Path] = field(default_factory=list)
    diagram_layout_path: Optional[Path] = None
    # Set when the report is bound to a model that is not in this folder (definition.pbir
    # byConnection), so no semantic model is edited on its behalf.
    remote_model: bool = False


@dataclass
class ValidationError:
    """One problem found by a validation pass"""
    file_path: str
    line_number: int
    error_type: str
    message: str
    context: str


@dataclass
class RenameResult:
    """Result of a PBIP rename operation. ``success`` is True only when the rename was applied
    in full; when it is False nothing was written. ``warnings`` lists what was applied but
    could not be guaranteed (ambiguous references left alone, free text that still contains
    the old name)."""
    success: bool
    message: str
    files_modified: List[str]
    references_updated: int
    details: Optional[Dict[str, Any]] = None
    validation_errors: List[ValidationError] = field(default_factory=list)
    backup_created: Optional[str] = None
    warnings: List[str] = field(default_factory=list)


def _read_json_file(path: Path) -> Optional[Any]:
    """Parsed JSON of a small project file, or None when it is missing or unreadable."""
    try:
        return safe_io.read_json(path)
    except (OSError, ValueError):
        return None


def _same_path(a: Path, b: Path) -> bool:
    return os.path.normcase(os.path.abspath(a)) == os.path.normcase(os.path.abspath(b))


def _merge_paths(found: List[Path], declared: List[Path]) -> List[Path]:
    """``found`` plus any path in ``declared`` that it lacks (compared case-insensitively
    where the file system is)."""
    seen = {os.path.normcase(os.path.abspath(p)) for p in found}
    out = list(found)
    for p in declared:
        key = os.path.normcase(os.path.abspath(p))
        if key not in seen:
            seen.add(key)
            out.append(Path(p))
    return out


class PowerBIPBIPConnector:
    """File-based editing of one loaded PBIP project.

    ``auto_backup`` is opt-in: when True, a copy of the project's own files is taken under
    <project>/.pbip-backups before each rename or repair that is about to be written, and
    before the first authoring change of a session. Without it, safety comes from the
    all-or-nothing commit and rollback_changes(), which undoes the last committed operation.
    """

    def __init__(self, auto_backup: bool = False):
        self.current_project: Optional[PBIPProject] = None
        self.auto_backup = auto_backup
        self.last_error: Optional[str] = None            # why the last load / backup failed
        self.unreadable_files: List[Tuple[str, str]] = []  # (path, error) from the last scan
        self._undo: Dict[str, Optional[bytes]] = {}       # file bytes before the last committed edit
        self._formats: Dict[str, safe_io.TextFormat] = {}

    # ==================== PROJECT DISCOVERY ====================

    @staticmethod
    def find_pbip_from_path(pbip_path: str) -> Optional[PBIPProject]:
        """Parse the project at ``pbip_path``: a .pbip file, or a folder holding exactly one.
        A folder with several .pbip files is ambiguous and returns None; pass the file."""
        path = Path(pbip_path)
        if path.is_dir():
            candidates = sorted(path.glob("*.pbip"))
            if len(candidates) != 1:
                return None
            path = candidates[0]
        if not path.is_file() or path.suffix.lower() != ".pbip":
            return None
        return PowerBIPBIPConnector._parse_pbip_project(path)

    @staticmethod
    def _report_folder_for(pbip_path: Path) -> Optional[Path]:
        """The report folder a .pbip names in its artifacts; failing that, the folder that
        shares its name; failing that, the only *.Report folder beside it."""
        root = pbip_path.parent
        data = _read_json_file(pbip_path)
        if isinstance(data, dict):
            for artifact in data.get("artifacts") or []:
                rel = artifact.get("report", {}).get("path") if isinstance(artifact, dict) else None
                if isinstance(rel, str) and (root / rel).is_dir():
                    return root / rel
        named = root / f"{pbip_path.stem}.Report"
        if named.is_dir():
            return named
        others = sorted(p for p in root.glob("*.Report") if p.is_dir())
        return others[0] if len(others) == 1 else None

    @staticmethod
    def _model_folder_for(pbip_path: Path, report_folder: Optional[Path]) -> Tuple[Optional[Path], bool]:
        """(semantic model folder, remote). The report's definition.pbir decides: byPath names
        the local model, byConnection means the model is not in this project at all."""
        root = pbip_path.parent
        if report_folder is not None:
            pbir = _read_json_file(report_folder / "definition.pbir")
            ref = pbir.get("datasetReference") if isinstance(pbir, dict) else None
            if isinstance(ref, dict):
                by_path = ref.get("byPath")
                if isinstance(by_path, dict) and isinstance(by_path.get("path"), str):
                    target = (report_folder / by_path["path"]).resolve()
                    if target.is_dir():
                        return target, False
                elif ref.get("byConnection") is not None:
                    return None, True
        for suffix in ("SemanticModel", "Dataset"):
            named = root / f"{pbip_path.stem}.{suffix}"
            if named.is_dir():
                return named, False
        others = sorted(p for s in ("SemanticModel", "Dataset") for p in root.glob(f"*.{s}") if p.is_dir())
        return (others[0] if len(others) == 1 else None), False

    @staticmethod
    def _parse_pbip_project(pbip_path: Path) -> Optional[PBIPProject]:
        """Resolve a .pbip file to its report and semantic model folders and list their files."""
        pbip_path = Path(os.path.abspath(pbip_path))      # every path reported later is absolute
        report_folder = PowerBIPBIPConnector._report_folder_for(pbip_path)
        model_folder, remote = PowerBIPBIPConnector._model_folder_for(pbip_path, report_folder)
        project = PBIPProject(
            root_path=pbip_path.parent, pbip_file=pbip_path, semantic_model_folder=model_folder,
            report_folder=report_folder, report_json_path=None, tmdl_files=[], remote_model=remote)
        PowerBIPBIPConnector._scan_project_files(project)
        return project

    @staticmethod
    def _scan_project_files(project: PBIPProject) -> None:
        """(Re)list the project's files. Paths a caller placed on the project by hand are kept."""
        report = project.report_folder
        if report is not None:
            definition = report / "definition"
            if (definition / "pages").is_dir():
                project.is_pbir_enhanced = True
                project.pbir_definition_folder = definition
                found = sorted((definition / "pages").glob("**/visuals/*/visual.json"))
                project.visual_json_files = [p for p in _merge_paths(found, project.visual_json_files)
                                             if p.is_file()]
                if (definition / "report.json").is_file():
                    project.report_json_path = definition / "report.json"
            elif (report / "report.json").is_file():
                project.report_json_path = report / "report.json"
        model = project.semantic_model_folder
        if model is not None:
            definition = model / "definition"
            base = definition if definition.is_dir() else model
            project.tmdl_files = [p for p in _merge_paths(sorted(base.glob("**/*.tmdl")), project.tmdl_files)
                                  if p.is_file()]
            cultures = definition / "cultures"
            if cultures.is_dir():
                project.cultures_files = sorted(cultures.glob("*.tmdl"))
            layout = model / "diagramLayout.json"
            if layout.is_file():
                project.diagram_layout_path = layout

    def load_project(self, pbip_path: str) -> bool:
        """Load a PBIP project for editing. Returns False (with ``last_error`` set) when the
        path does not resolve to exactly one project."""
        self.last_error = None
        project = self.find_pbip_from_path(pbip_path)
        if project is None:
            path = Path(pbip_path)
            if path.is_dir():
                found = sorted(p.name for p in path.glob("*.pbip"))
                self.last_error = (f"{len(found)} .pbip files in '{path}' ({', '.join(found)}); "
                                   "pass the path of the one to load" if len(found) > 1
                                   else f"no .pbip file in '{path}'")
            else:
                self.last_error = f"'{pbip_path}' is not a .pbip file"
            return False
        self.current_project = project
        self._undo = {}
        logger.info(f"Loaded PBIP project: {project.pbip_file}")
        return True

    def get_project_info(self) -> Dict[str, Any]:
        """Get information about the loaded project"""
        p = self.current_project
        if not p:
            return {"error": "No project loaded"}
        self._scan_project_files(p)
        return {
            "root_path": str(p.root_path),
            "pbip_file": str(p.pbip_file),
            "semantic_model_folder": str(p.semantic_model_folder) if p.semantic_model_folder else None,
            "report_folder": str(p.report_folder) if p.report_folder else None,
            "report_json_path": str(p.report_json_path) if p.report_json_path else None,
            "tmdl_file_count": len(p.tmdl_files),
            "has_report": p.report_json_path is not None,
            "report_format": "PBIR-Enhanced" if p.is_pbir_enhanced else "PBIR-Legacy",
            "is_pbir_enhanced": p.is_pbir_enhanced,
            "visual_json_count": len(p.visual_json_files),
            "cultures_file_count": len(p.cultures_files),
            "pbir_definition_folder": str(p.pbir_definition_folder) if p.pbir_definition_folder else None,
            "remote_model": p.remote_model,
        }

    # ==================== FILE I/O ====================

    def _read_text(self, file_path) -> str:
        """Text of a project file with \\n newlines and no BOM. The file's on-disk format is
        remembered so _write_text puts it back the same way."""
        text, fmt = safe_io.read_text(file_path)
        self._formats[os.path.normcase(os.path.abspath(file_path))] = fmt
        return text

    def _write_text(self, file_path, content: str) -> None:
        """Atomically write a project file in the format it was read in (a new file is UTF-8
        without BOM, LF newlines)."""
        fmt = self._formats.get(os.path.normcase(os.path.abspath(file_path)))
        safe_io.write_text(file_path, content, fmt)

    def _tmdl_files(self) -> List[Path]:
        if self.current_project:
            self._scan_project_files(self.current_project)
            return list(self.current_project.tmdl_files)
        return []

    def _report_files(self) -> List[Path]:
        p = self.current_project
        if not p:
            return []
        self._scan_project_files(p)
        found = pbip_report.report_files(p.report_folder, p.is_pbir_enhanced)
        declared = list(p.visual_json_files)
        if p.report_json_path is not None:
            declared.append(p.report_json_path)
        return _merge_paths(found, declared)

    def _catalog(self) -> tmdl_scan.ModelCatalog:
        """The model as declared by the project's TMDL files. Unreadable files are recorded in
        ``unreadable_files`` (and reported by validate_tmdl_syntax) rather than hidden."""
        catalog = tmdl_scan.ModelCatalog()
        self.unreadable_files = []
        for path in self._tmdl_files():
            try:
                catalog.add_document(tmdl_scan.scan(self._read_text(path)), str(path))
            except (OSError, UnicodeError) as e:
                self.unreadable_files.append((str(path), f"{type(e).__name__}: {e}"))
        return catalog

    # ==================== BACKUP & UNDO ====================

    def create_backup(self) -> Optional[str]:
        """Copy the project's own files (the .pbip, its report folder and its semantic model
        folder, without the local .pbi caches) into <project>/.pbip-backups/<name>-<time>-<id>.
        Returns the backup folder, or None with ``last_error`` set when the copy failed."""
        p = self.current_project
        if not p:
            return None
        self.last_error = None
        stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        target = p.root_path / BACKUP_FOLDER / f"{p.pbip_file.stem}-{stamp}-{uuid.uuid4().hex[:6]}"
        try:
            target.mkdir(parents=True)
            ignore_file = p.root_path / BACKUP_FOLDER / ".gitignore"
            if not ignore_file.exists():
                safe_io.write_text(ignore_file, "*\n")     # keep backups out of source control
            if p.pbip_file.is_file():
                shutil.copy2(p.pbip_file, target / p.pbip_file.name)
            for folder in (p.report_folder, p.semantic_model_folder):
                if folder is not None and folder.is_dir() and _same_path(folder.parent, p.root_path):
                    shutil.copytree(folder, target / folder.name, ignore=shutil.ignore_patterns(".pbi"))
        except OSError as e:
            self.last_error = f"backup failed: {type(e).__name__}: {e}"
            logger.error(self.last_error)
            shutil.rmtree(target, ignore_errors=True)
            return None
        p.backup_path = target
        logger.info(f"Created backup at: {target}")
        return str(target)

    def _backup_once(self) -> Optional[str]:
        """For authoring tools: make the session's backup before the first change. Returns an
        error message when a backup was wanted and could not be made."""
        if self.auto_backup and self.current_project and not self.current_project.backup_path:
            if self.create_backup() is None:
                return f"{self.last_error}; nothing was changed"
        return None

    def _backup_before_commit(self, made: List[str]) -> Optional[str]:
        """Hook for the rename engine: called once the plan is validated and about to be
        written, so a rename that is refused leaves no backup behind. Returns an error message
        to stop the commit when a backup was wanted and could not be made."""
        if not self.auto_backup:
            return None
        path = self.create_backup()
        if path is None:
            return self.last_error
        made.append(path)
        return None

    def rollback_changes(self) -> bool:
        """Undo the last committed rename / repair / quoting fix by restoring the bytes every
        changed file had before it. Returns False when there is nothing to undo or a file could
        not be restored (the reason is in ``last_error``)."""
        if not self._undo:
            logger.warning("No changes to rollback")
            return False
        try:
            for path, data in self._undo.items():
                if data is None:
                    if os.path.exists(path):
                        os.remove(path)
                    continue
                tmp = f"{path}.{uuid.uuid4().hex[:8]}.tmp"
                with open(tmp, "wb") as f:
                    f.write(data)
                os.replace(tmp, path)
        except OSError as e:
            self.last_error = f"rollback failed: {type(e).__name__}: {e}"
            logger.error(self.last_error)
            return False
        logger.info(f"Rolled back {len(self._undo)} file(s)")
        self._undo = {}
        return True

    # ==================== REPORT REFERENCES ====================

    @classmethod
    def _walk_report_refs(cls, obj, refs: set, alias_map=None) -> None:
        """Collect (table, field) references from a parsed PBIR JSON tree: columns, measures,
        hierarchies and hierarchy levels, each attributed to the table its own expression
        resolves to (directly or through a From-clause alias)."""
        pbip_report.walk_references(obj, refs, None, alias_map)

    def collect_report_references_by_file(self) -> Dict[str, set]:
        """{file path: {(table, field)}} for the report layer: visuals, mobile layouts, page
        and report level filters, bookmarks, and the legacy single report.json. A file that
        cannot be read or parsed is recorded in ``unreadable_files``."""
        out: Dict[str, set] = {}
        self.unreadable_files = []
        for fp in self._report_files():
            refs: set = set()
            try:
                pbip_report.walk_references(pbip_json.parse(self._read_text(fp)), refs)
            except (OSError, UnicodeError, pbip_json.JsonSyntaxError) as e:
                self.unreadable_files.append((str(fp), f"{type(e).__name__}: {e}"))
                logger.warning(f"Could not scan report references in {fp}: {e}")
                continue
            if refs or fp.name == "visual.json":
                out[str(fp)] = refs
        return out

    def collect_report_references(self) -> set:
        """Set of all (table, field) references used anywhere in the report layer."""
        refs: set = set()
        for s in self.collect_report_references_by_file().values():
            refs |= s
        return refs

    # ==================== PBIR REPORT AUTHORING (preview) ====================

    def _pages_folder(self) -> Optional[Path]:
        if self.current_project and self.current_project.pbir_definition_folder:
            return self.current_project.pbir_definition_folder / "pages"
        return None

    def _schema_from_sibling(self, filename: str, default_key: str) -> Optional[str]:
        """The $schema URL of an existing PBIR file of the same kind (schemas are bumped often,
        so the project's own version wins over the built-in default)."""
        base = self._pages_folder()
        if base and base.exists():
            for p in sorted(base.glob("**/" + filename)):
                data = _read_json_file(p)
                if isinstance(data, dict) and isinstance(data.get("$schema"), str):
                    return data["$schema"]
        return pbir_authoring.DEFAULT_SCHEMAS.get(default_key)

    def _model_field_index(self) -> Dict[str, Dict[str, Dict[str, Any]]]:
        """{table: {field: {kind, dataType}}} from the project's TMDL. kind is 'column' or
        'measure'; dataType is the column's TMDL data type (None for measures)."""
        index: Dict[str, Dict[str, Dict[str, Any]]] = {}
        for table in self._catalog().tables.values():
            fields = index.setdefault(table.name, {})
            for col in table.columns.values():
                fields[col.name] = {"kind": "column", "dataType": col.data_type}
            for name in table.measures.values():
                fields[name] = {"kind": "measure", "dataType": None}
        return index

    def model_field_catalog(self) -> Dict[str, Set[str]]:
        """{table name: {column and measure names}}."""
        return {t: set(fs.keys()) for t, fs in self._model_field_index().items()}

    # TMDL numeric data types whose default value-well aggregation is Sum (others Count).
    _NUMERIC_DATATYPES = {"int64", "double", "decimal", "int", "integer", "number"}

    def _resolve_roles(self, fields_by_role: Dict[str, Any],
                       visual_type: Optional[str] = None) -> Dict[str, Any]:
        """Turn {role: 'Table.Field' | [...]} into projection specs the way Power BI Desktop
        writes them: explicit Measure vs Column, a nativeQueryRef and, for a plain column on a
        well that summarizes (see pbir_authoring.aggregates: decided by visual type AND role),
        an Aggregation (Sum for numeric, else CountNonNull). Dicts are passed through as-is so
        a caller can specify a projection exactly."""
        index = self._model_field_index()
        lut = {t.lower(): {f.lower(): (f, meta) for f, meta in fs.items()} for t, fs in index.items()}
        resolved: Dict[str, Any] = {}
        for role, val in (fields_by_role or {}).items():
            aggregating = pbir_authoring.aggregates(visual_type, role)
            out: List[Any] = []
            for ref in (val if isinstance(val, list) else [val]):
                if not ref:
                    continue
                if isinstance(ref, dict):
                    out.append(ref)
                    continue
                table, fld = pbir_authoring.split_table_field(str(ref))
                meta = lut.get(table.lower(), {}).get(fld.lower())
                spec: Dict[str, Any] = {"table": table, "field": fld, "nativeQueryRef": fld}
                if meta and meta[1].get("kind") == "measure":
                    spec["kind"] = "Measure"
                elif meta:
                    spec["kind"] = "Column"
                    if aggregating:
                        dt = (meta[1].get("dataType") or "").lower()
                        spec["aggregation"] = (pbir_authoring.AGG_SUM if dt in self._NUMERIC_DATATYPES
                                               else pbir_authoring.AGG_COUNT_NONNULL)
                else:
                    # Unknown field (skip_validation): a summarizing well most likely takes a measure.
                    spec["kind"] = "Measure" if aggregating else "Column"
                out.append(spec)
            if out:
                resolved[role] = out
        return resolved

    def _check_fields(self, fields_by_role: Dict[str, Any]) -> List[str]:
        """Return a list of 'Table[Field]' refs that do NOT exist in the model catalog."""
        catalog = self._catalog()
        missing = []
        for _role, val in (fields_by_role or {}).items():
            for ref in (val if isinstance(val, list) else [val]):
                if not ref:
                    continue
                if isinstance(ref, dict):
                    table = ref.get("table") or ref.get("entity") or ""
                    fld = (ref.get("field") or ref.get("property") or ref.get("column")
                           or ref.get("measure") or "")
                else:
                    table, fld = pbir_authoring.split_table_field(str(ref))
                if not catalog.has_field(table, fld):
                    missing.append(f"{table}[{fld}]")
        return missing

    def _find_page_folder(self, ref: str) -> Optional[Path]:
        """A page folder by folder name, internal name or display name. ``ref`` is never used
        as a path: only a direct child of the pages folder can match."""
        pages = self._pages_folder()
        if not pages or not pages.exists() or not isinstance(ref, str) or not ref:
            return None
        if Path(ref).name == ref and ref not in (".", ".."):
            direct = pages / ref
            if direct.is_dir() and (direct / "page.json").exists():
                return direct
        for pj in sorted(pages.glob("*/page.json")):
            data = _read_json_file(pj)
            if isinstance(data, dict) and (data.get("displayName") == ref or data.get("name") == ref):
                return pj.parent
        return None

    def add_page(self, display_name: str, width: int = 1280, height: int = 720,
                 set_active: bool = False) -> Dict[str, Any]:
        """Create a new report page (PBIR-Enhanced) and register it in pages.json. Both files
        are written together or not at all."""
        if not self.current_project or not self.current_project.is_pbir_enhanced:
            return {"success": False, "message": "Requires a loaded PBIR-Enhanced PBIP project"}
        pages = self._pages_folder()
        if not pages:
            return {"success": False, "message": "No PBIR definition/pages folder found"}
        meta_path = pages / "pages.json"
        tx = safe_io.FileTransaction()
        meta: Dict[str, Any] = {}
        if meta_path.exists():
            try:
                meta = tx.read_json(meta_path)
            except (OSError, ValueError) as e:
                return {"success": False,
                        "message": f"pages.json could not be read ({e}); fix it before adding pages"}
            if not isinstance(meta, dict):
                return {"success": False, "message": "pages.json is not a JSON object; not modified"}
        backup_error = self._backup_once()
        if backup_error:
            return {"success": False, "message": backup_error}

        name = pbir_authoring.new_name()
        page_path = pages / name / "page.json"
        page_json = pbir_authoring.build_page(name, display_name, width, height,
                                              self._schema_from_sibling("page.json", "page"))
        tx.write_json(page_path, page_json)
        if "$schema" not in meta:
            meta["$schema"] = pbir_authoring.DEFAULT_SCHEMAS["pagesMetadata"]
        order = list(meta.get("pageOrder", []))
        if name not in order:
            order.append(name)
        meta["pageOrder"] = order
        if set_active or not meta.get("activePageName"):
            meta["activePageName"] = name
        tx.write_json(meta_path, meta)
        try:
            tx.commit()
        except OSError as e:
            return {"success": False, "message": f"Could not write the page ({e}); nothing was changed"}
        return {"success": True, "page_name": name, "display_name": display_name, "path": str(page_path)}

    def add_visual(self, page: str, visual_type: str, position: Optional[Dict[str, Any]] = None,
                   fields_by_role: Optional[Dict[str, Any]] = None,
                   skip_validation: bool = False) -> Dict[str, Any]:
        """Create a visual on a page (PBIR-Enhanced). fields_by_role maps a query role to
        'Table.Field' (or a list). Validates field existence against the model first."""
        if not self.current_project or not self.current_project.is_pbir_enhanced:
            return {"success": False, "message": "Requires a loaded PBIR-Enhanced PBIP project"}
        page_dir = self._find_page_folder(page)
        if not page_dir:
            return {"success": False, "message": f"Page '{page}' not found"}
        if fields_by_role and not skip_validation:
            missing = self._check_fields(fields_by_role)
            if missing:
                return {"success": False, "message": "Field(s) not found in the model; not created",
                        "missing_fields": missing}
        backup_error = self._backup_once()
        if backup_error:
            return {"success": False, "message": backup_error}

        name = pbir_authoring.new_name()
        vpath = page_dir / "visuals" / name / "visual.json"
        visual_json = pbir_authoring.build_visual(
            name, visual_type, position or {"x": 0, "y": 0, "width": 400, "height": 300},
            self._resolve_roles(fields_by_role, visual_type),
            self._schema_from_sibling("visual.json", "visual"))
        safe_io.write_json(vpath, visual_json)
        self.current_project.visual_json_files.append(vpath)
        return {"success": True, "visual_name": name, "page": page_dir.name,
                "visual_type": visual_type, "path": str(vpath)}

    def bind_fields(self, page: str, visual_name: str, fields_by_role: Dict[str, Any],
                    mode: str = "add", skip_validation: bool = False) -> Dict[str, Any]:
        """Add/replace field projections on an existing visual's query (mode: 'add' or 'replace')."""
        if not self.current_project or not self.current_project.is_pbir_enhanced:
            return {"success": False, "message": "Requires a loaded PBIR-Enhanced PBIP project"}
        if mode not in ("add", "replace"):
            return {"success": False, "message": f"Unknown mode '{mode}' (use 'add' or 'replace')"}
        page_dir = self._find_page_folder(page)
        if not page_dir:
            return {"success": False, "message": f"Page '{page}' not found"}
        if (not isinstance(visual_name, str) or Path(visual_name).name != visual_name
                or visual_name in (".", "..")):
            return {"success": False, "message": f"'{visual_name}' is not a visual name"}
        vpath = page_dir / "visuals" / visual_name / "visual.json"
        if not vpath.exists():
            return {"success": False, "message": f"Visual '{visual_name}' not found on page"}
        if not skip_validation:
            missing = self._check_fields(fields_by_role)
            if missing:
                return {"success": False, "message": "Field(s) not found in the model",
                        "missing_fields": missing}
        try:
            data = json.loads(self._read_text(vpath))
        except (OSError, ValueError) as e:
            return {"success": False, "message": f"Could not parse visual.json: {e}"}
        if not isinstance(data, dict) or not isinstance(data.get("visual", {}), dict):
            return {"success": False, "message": "visual.json does not describe a visual; not modified"}
        backup_error = self._backup_once()
        if backup_error:
            return {"success": False, "message": backup_error}

        visual = data.setdefault("visual", {})
        qs = visual.setdefault("query", {}).setdefault("queryState", {})
        resolved = self._resolve_roles(fields_by_role, visual.get("visualType"))
        if mode == "replace":
            qs.update(pbir_authoring.build_query_state(resolved))
        else:
            for role, items in resolved.items():
                role_obj = qs.setdefault(role, {"projections": []})
                projections = role_obj.setdefault("projections", [])
                existing_refs = {p.get("queryRef") for p in projections}
                for spec in items:
                    proj = pbir_authoring._projection_for(spec, "Column", active=False)
                    if proj["queryRef"] not in existing_refs:
                        projections.append(proj)
                        existing_refs.add(proj["queryRef"])
        self._write_text(vpath, safe_io.dumps_json(data))
        return {"success": True, "visual_name": visual_name, "page": page_dir.name, "mode": mode}

    def validate_report_bindings(self) -> List[ValidationError]:
        """Check that every (table, field) the report refers to exists in the model: the usual
        cause of blank visuals and 'repair' prompts after external edits. Covers visuals, page
        and report filters, bookmarks and the legacy report.json; hierarchies and their levels
        count as fields."""
        errors: List[ValidationError] = []
        if self.current_project and self.current_project.remote_model and not self._tmdl_files():
            return [ValidationError("", 0, "NO_LOCAL_MODEL", "Bindings cannot be checked offline: "
                                    + self._no_local_model(), "")]
        by_file = self.collect_report_references_by_file()
        unreadable = list(self.unreadable_files)
        catalog = self._catalog()
        for path, reason in unreadable + self.unreadable_files:
            errors.append(ValidationError(path, 0, "FILE_ERROR", f"Could not read {path}: {reason}", ""))
        for fp, refs in by_file.items():
            for (table, name) in sorted(refs):
                if not catalog.has_table(table):
                    errors.append(ValidationError(fp, 0, "MISSING_TABLE",
                        f"Report references table '{table}' that is not in the model", f"{table}[{name}]"))
                elif not catalog.has_member(table, name):
                    errors.append(ValidationError(fp, 0, "MISSING_FIELD",
                        f"Report references '{table}[{name}]' but '{name}' is not in table '{table}'",
                        f"{table}[{name}]"))
        return errors

    # ==================== TMDL MODEL AUTHORING (offline) ====================

    _INVALID_FILENAME_CHARS = re.compile(r'[<>:"/\\|?*\x00-\x1f]')
    _RESERVED_FILENAMES = {"con", "prn", "aux", "nul",
                           *(f"com{i}" for i in range(1, 10)), *(f"lpt{i}" for i in range(1, 10))}

    def _tables_folder(self) -> Optional[Path]:
        if self.current_project and self.current_project.semantic_model_folder:
            return self.current_project.semantic_model_folder / "definition" / "tables"
        return None

    def _safe_new_table_path(self, tables_dir: Path, name: str):
        """Resolve tables_dir/<name>.tmdl for a NEW table file, refusing path traversal,
        Windows-invalid characters, reserved device names, and silent overwrites of an
        existing file. Returns (path, error)."""
        clean = (name or "").strip()
        if not clean or clean in (".", "..") or self._INVALID_FILENAME_CHARS.search(clean):
            return None, f"Table name '{name}' cannot be used as a file name"
        if clean.lower() in self._RESERVED_FILENAMES:
            return None, f"Table name '{name}' is a reserved Windows device name"
        path = tables_dir / f"{clean}.tmdl"
        try:
            if path.resolve().parent != tables_dir.resolve():
                return None, f"Table name '{name}' escapes the tables folder"
        except OSError:
            return None, f"Table name '{name}' is not a valid path"
        if path.exists():
            return None, f"File '{path.name}' already exists in the tables folder"
        return path, None

    def _find_table_file(self, table_name: str) -> Optional[Path]:
        """The .tmdl file that declares the given table (names compare case-insensitively)."""
        info = self._catalog().table((table_name or "").strip())
        return Path(info.file) if info is not None and info.file else None

    @staticmethod
    def _one_line(value: Any) -> bool:
        return not (isinstance(value, str) and any(c in value for c in "\r\n"))

    def add_measures(self, table: str, measures: List[Dict[str, Any]], mode: str = "add") -> Dict[str, Any]:
        """Write measures into a table's .tmdl (offline). The whole batch is checked before
        anything is written.

        mode="add"     every measure must be new: a name that already exists anywhere in the
                       model is a collision error.
        mode="upsert"  a measure of the same name in THIS table is replaced in place (its
                       description, expression and properties come from the new definition;
                       its lineageTag is kept), the rest are added with a lineageTag derived
                       from the table and measure name. Running the same upsert again leaves
                       the file byte-identical. A measure of that name in another table, or a
                       column of that name, is still a collision error.

        Returns {"success", "path", "created": [...], "updated": [...]}.
        """
        if not self.current_project:
            return {"success": False, "message": "No PBIP project loaded. Use 'pbip_load_project' first."}
        if mode not in ("add", "upsert"):
            return {"success": False, "message": f"Unknown mode '{mode}' (use 'add' or 'upsert')"}
        catalog = self._catalog()
        info = catalog.table((table or "").strip())
        if info is None or not info.file:
            return {"success": False, "message": f"Table '{table}' not found in the model"}
        path = Path(info.file)
        problems, seen = [], set()
        for i, m in enumerate(measures):
            name = (m.get("name") or "").strip()
            if not name or not (m.get("expression") or "").strip():
                problems.append(f"#{i + 1}: needs both a name and an expression")
                continue
            # Single-line TMDL fields must not carry line breaks (they would corrupt the file).
            for key in ("name", "format_string", "display_folder"):
                if not self._one_line(m.get(key)):
                    problems.append(f"'{name}': {key} must not contain line breaks")
            key = name.casefold()
            replaceable = mode == "upsert" and key in info.measures
            if not replaceable:
                owners = [t.name for t in catalog.tables.values() if t.has_field(name)]
                if owners:
                    where = "the model" if mode == "add" else f"'{owners[0]}'"
                    problems.append(f"'{name}': already exists in {where}")
            if key in seen:
                problems.append(f"'{name}': duplicated within the batch")
            seen.add(key)
        if problems:
            return {"success": False,
                    "message": "Batch rejected; nothing was written:\n  - " + "\n  - ".join(problems)}
        backup_error = self._backup_once()
        if backup_error:
            return {"success": False, "message": backup_error}

        original = self._read_text(path)
        content = original
        created: List[str] = []
        updated: List[str] = []
        if mode == "add":
            content = tmdl_authoring.append_block(content, tmdl_authoring.render_measures(measures))
            created = [m["name"] for m in measures]
        else:
            for m in measures:
                name = m["name"].strip()
                spec = dict(m, name=name)
                block = tmdl_authoring.find_measure_block(content, info.name, name)
                if block is not None:
                    spec["lineage_tag"] = block.lineage_tag or self._lineage_tag(info.name, name)
                    rendered = tmdl_authoring.render_measure(spec)
                    content = content[:block.start] + rendered + content[block.end:]
                    updated.append(name)
                else:
                    spec["lineage_tag"] = self._lineage_tag(info.name, name)
                    content = tmdl_authoring.append_block(content, tmdl_authoring.render_measure(spec))
                    created.append(name)
        if content != original:
            self._write_text(path, content)
        return {"success": True, "created": created, "updated": updated, "path": str(path)}

    @staticmethod
    def _lineage_tag(table: str, measure: str) -> str:
        return str(uuid.uuid5(LINEAGE_NAMESPACE, f"{table.casefold()}\n{measure.casefold()}"))

    def create_date_table(self, name: str = "Date", start_date: str = "2015-01-01",
                          end_date: str = "2030-12-31",
                          fiscal_year_start_month: Optional[int] = None) -> Dict[str, Any]:
        """Create a calculated date-dimension table as a new .tmdl file (offline), marked as
        the model's date table."""
        if not self.current_project:
            return {"success": False, "message": "No PBIP project loaded. Use 'pbip_load_project' first."}
        tables_dir = self._tables_folder()
        if not tables_dir:
            return {"success": False, "message": "No semantic model definition/tables folder found"}
        if self._find_table_file(name):
            return {"success": False, "message": f"Table '{name}' already exists"}
        try:
            content = tmdl_authoring.build_date_table(name, start_date, end_date, fiscal_year_start_month)
        except ValueError as e:
            return {"success": False, "message": str(e)}
        path, path_err = self._safe_new_table_path(tables_dir, name)
        if path_err:
            return {"success": False, "message": path_err}
        backup_error = self._backup_once()
        if backup_error:
            return {"success": False, "message": backup_error}
        safe_io.write_text(path, content)
        self.current_project.tmdl_files.append(path)
        n_cols = len(re.findall(r"^\tcolumn ", content, re.MULTILINE))
        return {"success": True, "table": name, "path": str(path), "columns": n_cols}

    def add_calculation_group(self, name: str, items: List[Dict[str, Any]],
                              column_name: str = "Calculation", precedence: int = 1) -> Dict[str, Any]:
        """Create a calculation-group table as a new .tmdl file (offline). The table file and
        the model.tmdl flag it needs are written together or not at all."""
        if not self.current_project:
            return {"success": False, "message": "No PBIP project loaded. Use 'pbip_load_project' first."}
        tables_dir = self._tables_folder()
        if not tables_dir:
            return {"success": False, "message": "No semantic model definition/tables folder found"}
        if self._find_table_file(name):
            return {"success": False, "message": f"Table '{name}' already exists"}
        bad = [it for it in items if not (it.get("name") and it.get("expression"))]
        if bad:
            return {"success": False, "message": "Every calculation item needs a name and an expression"}
        crooked = [it["name"] for it in items
                   if not (self._one_line(it.get("name"))
                           and self._one_line(it.get("format_string_expression")))]
        if crooked:
            return {"success": False,
                    "message": f"Item name/format must not contain line breaks: {', '.join(crooked)}"}
        if not column_name or not self._one_line(column_name):
            return {"success": False, "message": "column_name must be a single line of text"}
        path, path_err = self._safe_new_table_path(tables_dir, name)
        if path_err:
            return {"success": False, "message": path_err}
        backup_error = self._backup_once()
        if backup_error:
            return {"success": False, "message": backup_error}

        tx = safe_io.FileTransaction()
        tx.write(path, tmdl_authoring.build_calculation_group(name, items, column_name, precedence))
        # The engine refuses a calculation group unless the model discourages implicit measures.
        flag_set = False
        definition = self.current_project.semantic_model_folder / "definition"
        model_tmdl = definition / "model.tmdl"
        if model_tmdl.exists():
            mcontent = tx.read(model_tmdl)
            doc = tmdl_scan.scan(mcontent)
            model_node = next((n for n in doc.roots if n.keyword == "model"), None)
            if model_node is not None and model_node.child("discourageImplicitMeasures") is None:
                tx.write(model_tmdl, mcontent[:model_node.end] + "\n\tdiscourageImplicitMeasures"
                         + mcontent[model_node.end:])
                flag_set = True
        try:
            tx.commit()
        except OSError as e:
            return {"success": False, "message": f"Could not write the calculation group ({e}); "
                                                 "nothing was changed"}
        self.current_project.tmdl_files.append(path)
        # Calculation groups need compatibilityLevel >= 1470; warn rather than upgrade, since a
        # level bump cannot be undone.
        compat_warning = None
        db_tmdl = definition / "database.tmdl"
        if db_tmdl.exists():
            dm = re.search(r"compatibilityLevel:\s*(\d+)", self._read_text(db_tmdl))
            if dm and int(dm.group(1)) < 1470:
                compat_warning = (f"database.tmdl compatibilityLevel is {dm.group(1)}; calculation "
                                  "groups need >= 1470 (Desktop writes 1550). Upgrade the model in "
                                  "Power BI Desktop before relying on this group.")
        return {"success": True, "table": name, "path": str(path), "items": len(items),
                "discourage_implicit_measures_set": flag_set, "compat_warning": compat_warning}

    def add_hierarchy(self, table: str, name: str, levels: List[str]) -> Dict[str, Any]:
        """Append a hierarchy to a table's .tmdl (offline). Level columns must exist."""
        if not self.current_project:
            return {"success": False, "message": "No PBIP project loaded. Use 'pbip_load_project' first."}
        if not name or not self._one_line(name):
            return {"success": False, "message": "The hierarchy name must be a single line of text"}
        info = self._catalog().table((table or "").strip())
        if info is None or not info.file:
            return {"success": False, "message": f"Table '{table}' not found in the model"}
        missing = [c for c in levels if c.casefold() not in info.columns]
        if missing:
            return {"success": False,
                    "message": f"Column(s) not found in '{table}': {', '.join(missing)}"}
        if name.casefold() in info.hierarchy_names:
            return {"success": False, "message": f"Hierarchy '{name}' already exists on '{table}'"}
        backup_error = self._backup_once()
        if backup_error:
            return {"success": False, "message": backup_error}
        path = Path(info.file)
        content = self._read_text(path)
        block = tmdl_authoring.render_hierarchy(name, [info.columns[c.casefold()].name for c in levels])
        self._write_text(path, tmdl_authoring.append_block(content, block))
        return {"success": True, "table": table, "hierarchy": name, "levels": levels, "path": str(path)}

    # ==================== VALIDATION ====================

    def validate_tmdl_syntax(self) -> List[ValidationError]:
        """Check the model's TMDL for structural problems an offline edit can introduce:
        names that need quotes, relationship endpoints that are not Table.Column, table names
        with spaces used unquoted in DAX, duplicate objects, and references (relationships,
        sortByColumn, hierarchy levels, table-qualified DAX) to objects that do not exist."""
        errors: List[ValidationError] = []
        docs: Dict[str, tmdl_scan.TmdlDocument] = {}
        for path in self._tmdl_files():
            try:
                docs[str(path)] = tmdl_scan.scan(self._read_text(path))
            except (OSError, UnicodeError) as e:
                errors.append(ValidationError(str(path), 0, "FILE_ERROR", f"{type(e).__name__}: {e}", ""))
        catalog = tmdl_scan.ModelCatalog()
        for path, doc in docs.items():
            catalog.add_document(doc, path)
        table_names = [t.name for t in catalog.tables.values()]
        for path, doc in docs.items():
            for node in doc.walk():
                context = doc.text[node.start:node.end].strip()
                if node.malformed:
                    kind = "UNQUOTED_NAME" if "object name" in node.malformed else "MALFORMED_LINE"
                    errors.append(ValidationError(path, node.line, kind, f"{node.malformed}: {context}",
                                                  context))
                if node.value is not None and node.keyword in ("fromColumn", "toColumn"):
                    if len(tmdl_scan.split_object_path(node.value)) != 2:
                        errors.append(ValidationError(
                            path, node.line, "UNQUOTED_REFERENCE",
                            "Relationship endpoint must be Table.Column, with single quotes around a "
                            f"name that has spaces: {node.value}", context))
                if doc.language(node) == "dax":
                    expr = doc.expression(node)
                    n = pbip_rename.quote_unquoted_tables(expr, table_names)[1]
                    if n:
                        errors.append(ValidationError(
                            path, node.line, "UNQUOTED_TABLE_IN_DAX",
                            f"{n} table reference(s) with spaces must be quoted in this DAX expression, "
                            "for example 'My Table'[Column]", context))
        for (kind, table, name), where in pbip_rename.model_problems(docs, catalog).items():
            target = f"'{table}'[{name}]" if name else f"'{table}'"
            file_path, _, line = where.rpartition(":")
            errors.append(ValidationError(
                file_path, int(line) if line.isdigit() else 0, "UNRESOLVED_REFERENCE",
                f"{kind} {target} does not exist in the model", target))
        for dup in catalog.duplicates:
            errors.append(ValidationError("", 0, "DUPLICATE_NAME", dup, ""))
        return errors

    def fix_all_dax_quoting(self, dry_run: bool = False) -> Dict[str, Any]:
        """Quote table names containing spaces that DAX expressions in the model use without
        quotes. Only DAX is touched (never M code, descriptions or annotations), and every
        file is written or none. With ``dry_run`` nothing is written and ``files_modified``
        lists the files that would change.

        Returns {"files_modified", "count", "tables_fixed", "errors", "dry_run"}.
        """
        result: Dict[str, Any] = {"files_modified": [], "count": 0, "tables_fixed": [], "errors": [],
                                  "dry_run": dry_run}
        if not self.current_project:
            return result
        tx = safe_io.FileTransaction()
        try:
            texts = {p: tx.read(p) for p in self._tmdl_files()}
        except (OSError, UnicodeError) as e:
            result["errors"].append({"file": getattr(e, "filename", None) or "",
                                     "error": f"{type(e).__name__}: {e}"})
            return result
        catalog = tmdl_scan.ModelCatalog.from_texts({str(p): t for p, t in texts.items()})
        names = [t.name for t in catalog.tables.values() if any(c.isspace() for c in t.name)]
        fixed_tables: Set[str] = set()
        for path, text in texts.items():
            doc = tmdl_scan.scan(text)
            edits = pbip_json.Edits(text)
            for node in doc.walk():
                if doc.language(node) != "dax":
                    continue
                expr = doc.expression(node)
                new_expr, n = pbip_rename.quote_unquoted_tables(expr, names)
                if n:
                    edits.replace_span(node.expr_span[0], node.expr_span[1], new_expr)
                    result["count"] += n
                    fixed_tables.update(
                        t for t in names
                        if new_expr.count(quote_tmdl_name(t)) > expr.count(quote_tmdl_name(t)))
            if len(edits):
                tx.write(path, edits.apply())
        result["tables_fixed"] = sorted(fixed_tables)
        changed = tx.paths
        if dry_run or not changed:
            result["files_modified"] = changed
            return result
        undo = self._snapshot(changed)
        try:
            result["files_modified"] = tx.commit()
        except OSError as e:
            return {"files_modified": [], "count": 0, "tables_fixed": [], "dry_run": dry_run,
                    "errors": [{"file": getattr(e, "filename", None) or "",
                                "error": f"{type(e).__name__}: {e}"}]}
        self._undo = undo
        return result

    @staticmethod
    def _snapshot(paths: List[str]) -> Dict[str, Optional[bytes]]:
        out: Dict[str, Optional[bytes]] = {}
        for path in paths:
            try:
                with open(path, "rb") as f:
                    out[path] = f.read()
            except FileNotFoundError:
                out[path] = None
        return out

    # ==================== RENAME OPERATIONS ====================

    def _engine(self) -> pbip_rename.RenameEngine:
        p = self.current_project
        return pbip_rename.RenameEngine(p.root_path, self._tmdl_files(), self._report_files(),
                                        p.diagram_layout_path)

    def _report_format(self) -> str:
        return "PBIR-Enhanced" if self.current_project.is_pbir_enhanced else "PBIR-Legacy"

    def _no_local_model(self) -> Optional[str]:
        """Why this project has no model that can be read offline, or None when it has one."""
        if self._tmdl_files():
            return None
        if self.current_project.remote_model:
            return "the report is bound to a semantic model that is not part of this project"
        return ("the project has no TMDL model files (a model stored as model.bim cannot be read "
                "or edited here; save it as TMDL from Power BI Desktop)")

    def _rename(self, ops: List[RenameOp], noun: str, details: Dict[str, Any]) -> RenameResult:
        """Run renames as one transaction and describe the outcome truthfully."""
        if not self.current_project:
            return RenameResult(False, "No project loaded", [], 0)
        details = dict(details, report_format=self._report_format())
        no_model = self._no_local_model()
        if no_model:
            return RenameResult(False, f"Nothing was changed: {no_model}.", [], 0,
                                details=dict(details, nothing_written=True))
        backup: List[str] = []
        outcome = self._engine().rename(ops, before_commit=lambda: self._backup_before_commit(backup))
        backup_path = backup[0] if backup else None
        details["operations"] = outcome.operations
        if not outcome.success:
            # A failed commit that could not put every file back hands over what those files
            # held, so rollback_changes() can retry the restore.
            self._undo = outcome.originals or self._undo
            details["nothing_written"] = not outcome.originals
            return RenameResult(
                False, outcome.message, [], 0, details=details,
                validation_errors=[ValidationError(i.file, i.line, i.kind, i.message, i.context)
                                   for i in outcome.issues],
                backup_created=backup_path, warnings=outcome.warnings)
        self._undo = outcome.originals
        self._scan_project_files(self.current_project)
        what = ", ".join(f"'{o['old']}' -> '{o['new']}'" for o in outcome.operations)
        message = (f"Renamed {len(outcome.operations)} {noun}(s): {what}. Updated "
                   f"{outcome.references_updated} reference(s) in {len(outcome.files_modified)} file(s) "
                   f"({details['report_format']}).")
        message += self._warning_text(outcome.warnings)
        return RenameResult(True, message, outcome.files_modified, outcome.references_updated,
                            details=details, backup_created=backup_path, warnings=outcome.warnings)

    @staticmethod
    def _warning_text(warnings: List[str]) -> str:
        if not warnings:
            return ""
        shown = warnings[:MAX_WARNINGS_IN_MESSAGE]
        more = len(warnings) - len(shown)
        return (f"\n{len(warnings)} warning(s) - review these, they were not changed automatically:\n  - "
                + "\n  - ".join(shown) + (f"\n  ... and {more} more" if more else ""))

    def rename_table_in_files(self, old_name: str, new_name: str) -> RenameResult:
        """Rename a table everywhere: its declaration, DAX, M queries that read it,
        relationships, model.tmdl, perspectives, roles, linguistic metadata, the diagram layout
        and every report reference. All files change together or none does."""
        return self._rename([RenameOp("table", old_name, new_name)], "table",
                            {"old_name": old_name, "new_name": new_name})

    def rename_column_in_files(self, table_name: str, old_name: str, new_name: str) -> RenameResult:
        """Rename a column of ``table_name`` everywhere it is referenced, and nowhere else: a
        same-named column of another table is not touched."""
        return self._rename([RenameOp("column", old_name, new_name, table_name)], "column",
                            {"table_name": table_name, "old_name": old_name, "new_name": new_name})

    def rename_measure_in_files(self, old_name: str, new_name: str,
                                table_name: Optional[str] = None) -> RenameResult:
        """Rename a measure everywhere it is referenced. Same-named columns are left alone."""
        return self._rename([RenameOp("measure", old_name, new_name, table_name)], "measure",
                            {"old_name": old_name, "new_name": new_name})

    def _batch(self, renames: List[Dict[str, str]], kind: str) -> RenameResult:
        if not self.current_project:
            return RenameResult(False, "No project loaded", [], 0)
        ops: List[RenameOp] = []
        for i, item in enumerate(renames or []):
            old, new = (item or {}).get("old_name"), (item or {}).get("new_name")
            table = (item or {}).get("table_name")
            if not old or not new or (kind == "column" and not table):
                need = "table_name, old_name and new_name" if kind == "column" else "old_name and new_name"
                return RenameResult(False, f"Nothing was changed: rename #{i + 1} needs {need}.", [], 0,
                                    details={"nothing_written": True})
            ops.append(RenameOp(kind, old, new, table if kind != "table" else None))
        if not ops:
            return RenameResult(False, "Nothing was changed: no renames were given.", [], 0,
                                details={"nothing_written": True})
        return self._rename(ops, kind, {"renames": [dict(r) for r in renames]})

    def batch_rename_tables(self, renames: List[Dict[str, str]]) -> RenameResult:
        """Rename several tables ([{"old_name", "new_name"}]) as ONE transaction: if any of
        them cannot be applied, none is."""
        return self._batch(renames, "table")

    def batch_rename_columns(self, renames: List[Dict[str, str]]) -> RenameResult:
        """Rename several columns ([{"table_name", "old_name", "new_name"}]) as one transaction."""
        return self._batch(renames, "column")

    def batch_rename_measures(self, renames: List[Dict[str, str]]) -> RenameResult:
        """Rename several measures ([{"old_name", "new_name"}], optional "table_name") as one
        transaction."""
        return self._batch(renames, "measure")

    # ==================== REPORT REPAIR & SCANS ====================

    def fix_broken_visual_references(self, old_table_name: str, new_table_name: str) -> Dict[str, Any]:
        """Repair the report after a model-only table rename: every report reference to
        ``old_table_name`` (a table the model no longer has) is pointed at ``new_table_name``
        (which must exist). Also updates linguistic metadata and the diagram layout. Nothing is
        written unless everything can be."""
        if not self.current_project:
            return {"success": False, "error": "No project loaded"}
        fmt = self._report_format()
        backup: List[str] = []
        outcome = self._engine().repair_report_table(
            old_table_name, new_table_name, before_commit=lambda: self._backup_before_commit(backup))
        backup_path = backup[0] if backup else None
        if outcome.success:
            self._undo = outcome.originals
        return {
            "success": outcome.success,
            "files_modified": outcome.files_modified,
            "references_fixed": outcome.references_updated,
            "format": fmt,
            "errors": [] if outcome.success else [outcome.message],
            "message": outcome.message,
            "warnings": outcome.warnings,
            "backup_created": backup_path,
        }

    def scan_broken_references(self) -> Dict[str, Any]:
        """Compare the report layer with the model: tables and fields the report refers to that
        the model does not have."""
        if not self.current_project:
            return {"error": "No project loaded"}
        catalog = self._catalog()
        model_tables = [t.name for t in catalog.tables.values()]
        report_tables: Set[str] = set()
        broken_refs: List[Dict[str, str]] = []
        broken_fields: List[Dict[str, str]] = []
        unreadable: List[Dict[str, str]] = [{"file": f, "error": e} for f, e in self.unreadable_files]
        for fp in self._report_files():
            try:
                refs, tables = pbip_report.references_in_text(self._read_text(fp))
            except (OSError, UnicodeError, pbip_json.JsonSyntaxError) as e:
                unreadable.append({"file": str(fp), "error": f"{type(e).__name__}: {e}"})
                continue
            kind = "visual" if fp.name == "visual.json" else fp.name
            for table in sorted(tables):
                report_tables.add(table)
                if not catalog.has_table(table):
                    broken_refs.append({"file": str(fp), "entity": table, "type": kind})
            for table, name in sorted(refs):
                if catalog.has_table(table) and not catalog.has_member(table, name):
                    broken_fields.append({"file": str(fp), "table": table, "field": name, "type": kind})
        orphaned = sorted({t for t in report_tables if not catalog.has_table(t)})
        return {
            "model_tables": model_tables,
            "report_tables": sorted(report_tables),
            "broken_references": broken_refs,
            "broken_field_references": broken_fields,
            "orphaned_table_names": orphaned,
            "unreadable_files": unreadable,
            "has_broken_refs": bool(broken_refs or broken_fields),
        }
