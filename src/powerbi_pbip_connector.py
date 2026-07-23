"""
Power BI PBIP (Power BI Project) Connector
Provides file-based editing for PBIP format to safely rename tables, columns, measures
without breaking report visuals.

Supports BOTH report formats:
  - PBIR-Legacy: Single report.json file (older format)
  - PBIR: Individual visual.json files (new enhanced format, default from Jan 2026)

PBIP Structure:
  project.pbip
  ProjectName.SemanticModel/
    definition.tmdl (or model.tmd)
    definition/
      tables/*.tmdl
      relationships/*.tmdl  <- Individual relationship files
      relationships.tmdl    <- Or single file
      cultures/*.tmdl       <- Linguistic schema (ConceptualEntity references)
      etc.
  ProjectName.Report/
    report.json             <- PBIR-Legacy: All visuals in one file
    definition.pbir         <- Points to semantic model
    definition/             <- PBIR Enhanced format (new)
      report.json           <- Report-level settings only
      pages/
        pages.json          <- Page listing
        [page_id]/
          page.json         <- Page settings
          visuals/
            [visual_id]/
              visual.json   <- Individual visual definition with Entity refs

TMDL Name Quoting Rules (Microsoft Spec):
  - Names with spaces, special chars, or reserved words MUST be quoted with single quotes
  - Examples:
    - table 'Customer Appointments'  (spaces - MUST quote)
    - table Sales                    (no spaces - no quote needed)
    - fromTable: 'My Table'          (spaces in relationship ref)

References:
  - Data Goblins: https://data-goblins.com/power-bi/programmatically-modify-reports
  - Microsoft PBIR: https://powerbi.microsoft.com/en-us/blog/power-bi-enhanced-report-format-pbir-in-power-bi-desktop-developer-mode-preview/
"""
import json
import logging
import os
import re
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, field

import pbir_authoring
import tmdl_authoring

logger = logging.getLogger(__name__)


# Characters that require quoting in TMDL names
TMDL_SPECIAL_CHARS = set(' \t\n\r\'\"[]{}().,;:!@#$%^&*+-=<>?/\\|`~')
TMDL_RESERVED_WORDS = {'table', 'column', 'measure', 'relationship', 'partition', 'hierarchy', 'level',
                       'annotation', 'expression', 'from', 'to', 'true', 'false', 'null'}


def needs_tmdl_quoting(name: str) -> bool:
    """
    Check if a TMDL name needs single quotes

    Rules:
    - Names with spaces need quotes
    - Names with special characters need quotes
    - Names starting with digits need quotes
    - Reserved words need quotes
    """
    if not name:
        return False

    # Check for spaces or special chars
    if any(c in TMDL_SPECIAL_CHARS for c in name):
        return True

    # Check if starts with digit
    if name[0].isdigit():
        return True

    # Check reserved words (case insensitive)
    if name.lower() in TMDL_RESERVED_WORDS:
        return True

    return False


def quote_tmdl_name(name: str) -> str:
    """
    Quote a TMDL name if needed

    Args:
        name: The name to potentially quote

    Returns:
        Quoted name if needed, otherwise original name
    """
    if needs_tmdl_quoting(name):
        # Escape any existing single quotes
        escaped = name.replace("'", "''")
        return f"'{escaped}'"
    return name


def unquote_tmdl_name(name: str) -> str:
    """Remove TMDL quotes from a name if present"""
    if name.startswith("'") and name.endswith("'"):
        # Remove outer quotes and unescape inner quotes
        return name[1:-1].replace("''", "'")
    return name


def quote_mcode_name(name: str) -> str:
    """
    Quote a name for M-Code/Power Query if needed.

    M-Code uses #"..." format for names with spaces or special characters.
    Names without spaces can be used directly.

    Examples:
        - Salesforce_Data -> Salesforce_Data (no quotes needed)
        - Leads Sales Data -> #"Leads Sales Data" (quotes required)
    """
    # Check if name needs quoting (has spaces or special chars)
    if ' ' in name or any(c in name for c in '!"#$%&\'()*+,-./:;<=>?@[\\]^`{|}~'):
        return f'#"{name}"'
    return name


def extract_external_refs(content: str) -> Tuple[str, Dict[str, str]]:
    """
    Extract ONLY external dataflow references from M-Code and replace with placeholders.

    External references like {[entity="TableName",version=""]} point to external
    data sources and should NEVER be modified during renames.

    Internal table references like Source = TableName SHOULD be modified.

    Returns:
        Tuple of (content_with_placeholders, {placeholder: original_ref})
    """
    external_refs = {}
    placeholder_counter = [0]

    def replace_external(match):
        placeholder = f"__EXTERNAL_REF_{placeholder_counter[0]}__"
        placeholder_counter[0] += 1
        external_refs[placeholder] = match.group(0)
        return placeholder

    # Pattern to match external dataflow references: {[entity="...",version="..."]}[Data]
    # This captures the entire external source reference pattern
    external_pattern = r'\{\[entity="[^"]+",version="[^"]*"\]\}\[Data\]'

    content_with_placeholders = re.sub(external_pattern, replace_external, content)

    return content_with_placeholders, external_refs


def restore_external_refs(content: str, external_refs: Dict[str, str]) -> str:
    """Restore external dataflow references from placeholders (unchanged)"""
    for placeholder, original in external_refs.items():
        content = content.replace(placeholder, original)
    return content


def fix_dax_table_references(dax_expression: str, table_names: List[str]) -> str:
    """
    Fix DAX expressions by quoting table names that have spaces or special chars.

    This handles cases like:
    - SUM(Leads Sales Data[Amount]) -> SUM('Leads Sales Data'[Amount])
    - CALCULATE(SUM(My Table[Col])) -> CALCULATE(SUM('My Table'[Col]))

    Args:
        dax_expression: The DAX expression to fix
        table_names: List of table names in the model (used for context)

    Returns:
        Fixed DAX expression with proper table name quoting
    """
    result = dax_expression

    for table_name in table_names:
        if needs_tmdl_quoting(table_name):
            # Find all unquoted references to this table followed by [
            # Matches: TableName[Column] but not 'TableName'[Column]
            pattern = rf"(?<!['\w]){re.escape(table_name)}(?=\s*\[)"
            replacement = quote_tmdl_name(table_name)
            result = re.sub(pattern, replacement, result)

            # Also fix function call patterns like: RELATED(TableName
            # but not: RELATED('TableName
            pattern2 = rf"(?<=[A-Za-z]\()\s*{re.escape(table_name)}(?=\s*[\[\,\)])"
            result = re.sub(pattern2, replacement, result)

    return result


@dataclass
class PBIPProject:
    """Represents a PBIP project structure"""
    root_path: Path
    pbip_file: Path
    semantic_model_folder: Optional[Path]
    report_folder: Optional[Path]
    report_json_path: Optional[Path]  # PBIR-Legacy: root report.json
    tmdl_files: List[Path]
    backup_path: Optional[Path] = None
    # PBIR Enhanced format fields
    is_pbir_enhanced: bool = False
    pbir_definition_folder: Optional[Path] = None  # Report/definition/ folder
    visual_json_files: List[Path] = field(default_factory=list)  # All visual.json files
    cultures_files: List[Path] = field(default_factory=list)  # Linguistic schema files
    # Additional semantic model files
    diagram_layout_path: Optional[Path] = None  # diagramLayout.json for model diagram


@dataclass
class ValidationError:
    """Represents a validation error found after rename"""
    file_path: str
    line_number: int
    error_type: str
    message: str
    context: str


@dataclass
class RenameResult:
    """Result of a PBIP rename operation"""
    success: bool
    message: str
    files_modified: List[str]
    references_updated: int
    details: Optional[Dict[str, Any]] = None
    validation_errors: List[ValidationError] = field(default_factory=list)
    backup_created: Optional[str] = None


class PowerBIPBIPConnector:
    """
    PBIP Connector for file-based Power BI Project editing

    Enables safe bulk renames by editing:
    - TMDL files (semantic model definitions)
    - report.json (visual field bindings)

    Key improvements in V2.1:
    - Proper TMDL quoting for names with spaces
    - Complete relationship reference updates (fromTable/toTable)
    - Automatic backup before changes
    - Post-change validation
    """

    def __init__(self, auto_backup: bool = True):
        self.current_project: Optional[PBIPProject] = None
        self.auto_backup = auto_backup
        self._original_files: Dict[str, str] = {}  # For rollback
        self._file_encodings: Dict[str, str] = {}  # Remember utf-8 vs utf-8-sig (BOM) per file

    @staticmethod
    def find_pbip_project_from_model_name(model_name: str, search_paths: Optional[List[str]] = None) -> Optional[PBIPProject]:
        """
        Find a PBIP project that matches the given model name

        Args:
            model_name: Name of the model (usually the database name from TOM)
            search_paths: Optional list of paths to search for PBIP projects

        Returns:
            PBIPProject if found, None otherwise
        """
        if not search_paths:
            # Default search locations
            search_paths = [
                os.path.expanduser("~/Documents"),
                os.path.expanduser("~/Desktop"),
                os.path.expanduser("~/Downloads"),
                "C:/",
            ]

        # Also check common development folders
        for base in ["C:/Users", os.path.expanduser("~")]:
            for folder in ["Projects", "Work", "Dev", "GitHub", "Repos"]:
                path = os.path.join(base, folder)
                if os.path.exists(path):
                    search_paths.append(path)

        for search_path in search_paths:
            if not os.path.exists(search_path):
                continue

            # Look for .pbip files
            try:
                for root, dirs, files in os.walk(search_path):
                    # Limit depth to avoid searching too deep
                    depth = root.replace(search_path, '').count(os.sep)
                    if depth > 5:
                        continue

                    for file in files:
                        if file.endswith('.pbip'):
                            pbip_path = Path(root) / file
                            project = PowerBIPBIPConnector._parse_pbip_project(pbip_path)
                            if project:
                                # Check if model name matches
                                if project.semantic_model_folder and model_name.lower() in project.semantic_model_folder.name.lower():
                                    logger.info(f"Found PBIP project for model '{model_name}' at: {pbip_path}")
                                    return project
            except PermissionError:
                continue

        return None

    @staticmethod
    def find_pbip_from_path(pbip_path: str) -> Optional[PBIPProject]:
        """
        Parse a PBIP project from a given path

        Args:
            pbip_path: Path to the .pbip file or project folder

        Returns:
            PBIPProject if valid, None otherwise
        """
        path = Path(pbip_path)

        # If it's a folder, look for .pbip file inside
        if path.is_dir():
            pbip_files = list(path.glob("*.pbip"))
            if pbip_files:
                path = pbip_files[0]
            else:
                return None

        if not path.exists() or not path.suffix == '.pbip':
            return None

        return PowerBIPBIPConnector._parse_pbip_project(path)

    @staticmethod
    def _parse_pbip_project(pbip_path: Path) -> Optional[PBIPProject]:
        """Parse a PBIP project structure, detecting both PBIR-Legacy and PBIR Enhanced formats"""
        try:
            root = pbip_path.parent

            # Find semantic model folder (.SemanticModel)
            semantic_folders = list(root.glob("*.SemanticModel"))
            semantic_model_folder = semantic_folders[0] if semantic_folders else None

            # Find report folder (.Report)
            report_folders = list(root.glob("*.Report"))
            report_folder = report_folders[0] if report_folders else None

            # Initialize PBIR fields
            report_json_path = None
            is_pbir_enhanced = False
            pbir_definition_folder = None
            visual_json_files = []
            cultures_files = []

            if report_folder:
                # Check for PBIR Enhanced format: Report/definition/pages/ structure
                definition_folder = report_folder / "definition"
                pages_folder = definition_folder / "pages"

                if pages_folder.exists():
                    # This is PBIR Enhanced format
                    is_pbir_enhanced = True
                    pbir_definition_folder = definition_folder

                    # Find all visual.json files in pages/[id]/visuals/[id]/
                    visual_json_files = list(pages_folder.glob("**/visuals/*/visual.json"))

                    # Report.json in definition folder (report-level settings only)
                    report_json = definition_folder / "report.json"
                    if report_json.exists():
                        report_json_path = report_json

                    logger.info(f"Detected PBIR Enhanced format with {len(visual_json_files)} visual files")
                else:
                    # Check for PBIR-Legacy: single report.json at root
                    report_json = report_folder / "report.json"
                    if report_json.exists():
                        report_json_path = report_json
                        logger.info("Detected PBIR-Legacy format (single report.json)")

            # Find all TMDL files
            tmdl_files = []
            diagram_layout_path = None
            if semantic_model_folder:
                tmdl_files = list(semantic_model_folder.glob("**/*.tmdl"))
                tmdl_files.extend(semantic_model_folder.glob("**/*.tmd"))

                # Find cultures files (linguistic schema with ConceptualEntity)
                cultures_folder = semantic_model_folder / "definition" / "cultures"
                if cultures_folder.exists():
                    cultures_files = list(cultures_folder.glob("*.tmdl"))

                # Find diagramLayout.json (model diagram layout)
                diagram_layout = semantic_model_folder / "diagramLayout.json"
                if diagram_layout.exists():
                    diagram_layout_path = diagram_layout
                    logger.info("Found diagramLayout.json")

            return PBIPProject(
                root_path=root,
                pbip_file=pbip_path,
                semantic_model_folder=semantic_model_folder,
                report_folder=report_folder,
                report_json_path=report_json_path,
                tmdl_files=tmdl_files,
                is_pbir_enhanced=is_pbir_enhanced,
                pbir_definition_folder=pbir_definition_folder,
                visual_json_files=visual_json_files,
                cultures_files=cultures_files,
                diagram_layout_path=diagram_layout_path
            )

        except Exception as e:
            logger.error(f"Failed to parse PBIP project: {e}")
            return None

    def load_project(self, pbip_path: str) -> bool:
        """
        Load a PBIP project for editing

        Args:
            pbip_path: Path to .pbip file or project folder

        Returns:
            True if loaded successfully
        """
        project = self.find_pbip_from_path(pbip_path)
        if project:
            self.current_project = project
            self._original_files = {}  # Clear any previous backup state
            logger.info(f"Loaded PBIP project: {project.pbip_file}")
            return True
        return False

    def get_project_info(self) -> Dict[str, Any]:
        """Get information about the loaded project"""
        if not self.current_project:
            return {"error": "No project loaded"}

        return {
            "root_path": str(self.current_project.root_path),
            "pbip_file": str(self.current_project.pbip_file),
            "semantic_model_folder": str(self.current_project.semantic_model_folder) if self.current_project.semantic_model_folder else None,
            "report_folder": str(self.current_project.report_folder) if self.current_project.report_folder else None,
            "report_json_path": str(self.current_project.report_json_path) if self.current_project.report_json_path else None,
            "tmdl_file_count": len(self.current_project.tmdl_files),
            "has_report": self.current_project.report_json_path is not None,
            # PBIR Enhanced format info
            "report_format": "PBIR-Enhanced" if self.current_project.is_pbir_enhanced else "PBIR-Legacy",
            "is_pbir_enhanced": self.current_project.is_pbir_enhanced,
            "visual_json_count": len(self.current_project.visual_json_files),
            "cultures_file_count": len(self.current_project.cultures_files),
            "pbir_definition_folder": str(self.current_project.pbir_definition_folder) if self.current_project.pbir_definition_folder else None
        }

    # ==================== BACKUP & ROLLBACK ====================

    def create_backup(self) -> Optional[str]:
        """
        Create a backup of the entire PBIP project

        Returns:
            Path to backup folder, or None if failed
        """
        if not self.current_project:
            return None

        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_name = f"{self.current_project.pbip_file.stem}_backup_{timestamp}"
            backup_path = self.current_project.root_path.parent / backup_name

            # Copy entire project folder
            shutil.copytree(self.current_project.root_path, backup_path)

            self.current_project.backup_path = backup_path
            logger.info(f"Created backup at: {backup_path}")
            return str(backup_path)

        except Exception as e:
            logger.error(f"Failed to create backup: {e}")
            return None

    @classmethod
    def _resolve_entity(cls, expr, alias_map) -> Optional[str]:
        """Resolve the table from a PBIR Expression node: SourceRef.Entity directly, or
        SourceRef.Source (a From-clause alias) resolved via alias_map, or a nested
        Hierarchy expression. Resolves from THIS node only (no whole-subtree guessing)."""
        if not isinstance(expr, dict):
            return None
        sr = expr.get("SourceRef")
        if isinstance(sr, dict):
            if isinstance(sr.get("Entity"), str):
                return sr["Entity"]
            if isinstance(sr.get("Source"), str):
                return alias_map.get(sr["Source"])
        h = expr.get("Hierarchy")
        if isinstance(h, dict):
            return cls._resolve_entity(h.get("Expression", {}), alias_map)
        return None

    @classmethod
    def _walk_report_refs(cls, obj, refs: set, alias_map=None) -> None:
        """Collect (table, field) references from a PBIR JSON tree.

        Handles Column/Measure (Property), Hierarchy (Hierarchy name), and HierarchyLevel
        (Level) nodes; resolves From-clause Source aliases used by filters/slicers; and
        resolves each reference's table from its OWN Expression so compound expressions that
        touch multiple tables are not mis-attributed."""
        alias_map = dict(alias_map or {})
        if isinstance(obj, dict):
            # A From clause at this scope defines alias -> table for descendant Source refs.
            frm = obj.get("From")
            if isinstance(frm, list):
                for src in frm:
                    if isinstance(src, dict) and isinstance(src.get("Name"), str) and isinstance(src.get("Entity"), str):
                        alias_map[src["Name"]] = src["Entity"]
            for kind, namekey in (("Column", "Property"), ("Measure", "Property"),
                                  ("Hierarchy", "Hierarchy"), ("HierarchyLevel", "Level")):
                node = obj.get(kind)
                if isinstance(node, dict) and isinstance(node.get(namekey), str):
                    entity = cls._resolve_entity(node.get("Expression", {}), alias_map)
                    if entity:
                        refs.add((entity, node[namekey]))
            ref_name = obj.get("queryRef") or obj.get("NativeReferenceName")
            # Skip aggregation/function forms like "Sum(Sales.Amount)" - their underlying
            # column is captured structurally above; partitioning them on "." would invent a
            # bogus ("Sum(Sales", "Amount)") reference.
            if (isinstance(ref_name, str) and "." in ref_name
                    and "(" not in ref_name and ")" not in ref_name):
                tbl, _, fld = ref_name.partition(".")
                if tbl and fld:
                    refs.add((tbl, fld))
            for v in obj.values():
                cls._walk_report_refs(v, refs, alias_map)
        elif isinstance(obj, list):
            for it in obj:
                cls._walk_report_refs(it, refs, alias_map)

    def collect_report_references_by_file(self) -> Dict[str, set]:
        """Scan the report layer and return {file_path: set((table, field))} references.

        Covers PBIR-Enhanced (per-visual.json) and PBIR-Legacy (single report.json)."""
        out: Dict[str, set] = {}
        if not self.current_project:
            return out
        files = []
        if self.current_project.is_pbir_enhanced and self.current_project.visual_json_files:
            files = list(self.current_project.visual_json_files)
        elif self.current_project.report_json_path:
            files = [self.current_project.report_json_path]
        for fp in files:
            refs: set = set()
            try:
                data = json.loads(self._read_text(fp))
                self._walk_report_refs(data, refs)
            except Exception as e:
                logger.warning(f"Could not scan report references in {fp}: {e}")
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
        """Read a $schema URL from an existing PBIR file of the same kind (preferred over
        hardcoding, since PBIR schemas bump over time). Falls back to a documented default."""
        base = self._pages_folder()
        if base and base.exists():
            for p in base.glob("**/" + filename):
                try:
                    data = json.loads(self._read_text(p))
                    if isinstance(data.get("$schema"), str):
                        return data["$schema"]
                except Exception:
                    continue
        return pbir_authoring.DEFAULT_SCHEMAS.get(default_key)

    def _model_field_index(self) -> Dict[str, Dict[str, Dict[str, Any]]]:
        """Parse the project's TMDL files into {table: {field: {kind, dataType}}} (one table
        per file, the PBIP convention). kind is 'column' or 'measure'; dataType is the TMDL
        data type for columns (None for measures). Drives both binding validation and the
        measure-vs-aggregated-column decision when authoring visuals."""
        index: Dict[str, Dict[str, Dict[str, Any]]] = {}
        if not self.current_project:
            return index
        for tmdl in self.current_project.tmdl_files:
            try:
                content = self._read_text(tmdl)
            except Exception:
                continue
            tmatch = re.search(r"^\s*table\s+(?:'([^']+)'|(\S+))", content, re.MULTILINE)
            if not tmatch:
                continue
            tname = (tmatch.group(1) or tmatch.group(2)).replace("''", "'")
            fields = index.setdefault(tname, {})
            current: Optional[str] = None
            for line in content.splitlines():
                cm = re.match(r"^\s*(column|measure)\s+(?:'([^']+)'|([^\s=]+))", line)
                if cm:
                    fname = (cm.group(2) or cm.group(3)).replace("''", "'")
                    fields[fname] = {"kind": cm.group(1), "dataType": None}
                    current = fname
                    continue
                if current and fields.get(current, {}).get("kind") == "column":
                    dm = re.match(r"^\s*dataType:\s*(\S+)", line)
                    if dm:
                        fields[current]["dataType"] = dm.group(1).strip()
        return index

    def model_field_catalog(self) -> Dict[str, Set[str]]:
        """{table_name: {column/measure names}} - the name-only view used to validate that
        report field bindings exist. Derived from the richer _model_field_index."""
        return {t: set(fs.keys()) for t, fs in self._model_field_index().items()}

    # TMDL numeric data types whose default value-well aggregation is Sum (others Count).
    _NUMERIC_DATATYPES = {"int64", "double", "decimal", "int", "integer", "number"}

    def _resolve_roles(self, fields_by_role: Dict[str, Any]) -> Dict[str, Any]:
        """Turn {role: 'Table.Field' | [...]} into enriched per-field projection specs that
        match how Power BI Desktop writes them: explicit Measure vs Column, a nativeQueryRef,
        and - for a plain numeric/text COLUMN dropped on an aggregating well (Y/Values/...) -
        an Aggregation (Sum for numeric, else CountNonNull). Dicts are passed through as-is so
        an advanced caller can specify a projection exactly."""
        index = self._model_field_index()
        lut = {t.lower(): {f.lower(): (f, meta) for f, meta in fs.items()} for t, fs in index.items()}
        resolved: Dict[str, Any] = {}
        for role, val in (fields_by_role or {}).items():
            aggregating = role.lower() in pbir_authoring.AGGREGATING_ROLES
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
                    # Unknown field (e.g. skip_validation): fall back to the role heuristic.
                    spec["kind"] = "Measure" if aggregating else "Column"
                out.append(spec)
            if out:
                resolved[role] = out
        return resolved

    def _check_fields(self, fields_by_role: Dict[str, Any]) -> List[str]:
        """Return a list of 'Table[Field]' refs that do NOT exist in the model catalog."""
        catalog = self.model_field_catalog()
        tables_lower = {t.lower() for t in catalog}
        fields_lower = {t.lower(): {f.lower() for f in fs} for t, fs in catalog.items()}
        missing = []
        for role, val in (fields_by_role or {}).items():
            for ref in (val if isinstance(val, list) else [val]):
                if not ref:
                    continue
                if isinstance(ref, dict):
                    table = ref.get("table") or ref.get("entity") or ""
                    fld = ref.get("field") or ref.get("property") or ref.get("column") or ref.get("measure") or ""
                else:
                    table, fld = pbir_authoring.split_table_field(str(ref))
                if table.lower() not in tables_lower or fld.lower() not in fields_lower.get(table.lower(), set()):
                    missing.append(f"{table}[{fld}]")
        return missing

    def _find_page_folder(self, ref: str) -> Optional[Path]:
        pages = self._pages_folder()
        if not pages or not pages.exists():
            return None
        direct = pages / ref
        if direct.exists() and (direct / "page.json").exists():
            return direct
        for pj in pages.glob("*/page.json"):
            try:
                data = json.loads(self._read_text(pj))
                if data.get("displayName") == ref or data.get("name") == ref:
                    return pj.parent
            except Exception:
                continue
        return None

    def add_page(self, display_name: str, width: int = 1280, height: int = 720,
                 set_active: bool = False) -> Dict[str, Any]:
        """Create a new report page (PBIR-Enhanced) and register it in pages.json."""
        if not self.current_project or not self.current_project.is_pbir_enhanced:
            return {"success": False, "message": "Requires a loaded PBIR-Enhanced PBIP project"}
        pages = self._pages_folder()
        if not pages:
            return {"success": False, "message": "No PBIR definition/pages folder found"}
        if self.auto_backup and not self.current_project.backup_path:
            self.create_backup()

        name = pbir_authoring.new_name()
        page_dir = pages / name
        page_dir.mkdir(parents=True, exist_ok=True)
        page_path = page_dir / "page.json"
        page_json = pbir_authoring.build_page(name, display_name, width, height,
                                              self._schema_from_sibling("page.json", "page"))
        self._write_text(page_path, json.dumps(page_json, indent=2))

        # Update (or create) pages.json, preserving any existing keys.
        meta_path = pages / "pages.json"
        meta: Dict[str, Any] = {}
        if meta_path.exists():
            try:
                meta = json.loads(self._read_text(meta_path))
            except Exception:
                meta = {}
        if "$schema" not in meta:
            meta["$schema"] = pbir_authoring.DEFAULT_SCHEMAS["pagesMetadata"]
        order = list(meta.get("pageOrder", []))
        if name not in order:
            order.append(name)
        meta["pageOrder"] = order
        if set_active or not meta.get("activePageName"):
            meta["activePageName"] = name
        self._write_text(meta_path, json.dumps(meta, indent=2))

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
        if self.auto_backup and not self.current_project.backup_path:
            self.create_backup()

        name = pbir_authoring.new_name()
        vdir = page_dir / "visuals" / name
        vdir.mkdir(parents=True, exist_ok=True)
        vpath = vdir / "visual.json"
        visual_json = pbir_authoring.build_visual(
            name, visual_type, position or {"x": 0, "y": 0, "width": 400, "height": 300},
            self._resolve_roles(fields_by_role), self._schema_from_sibling("visual.json", "visual"))
        self._write_text(vpath, json.dumps(visual_json, indent=2))
        self.current_project.visual_json_files.append(vpath)

        return {"success": True, "visual_name": name, "page": page_dir.name,
                "visual_type": visual_type, "path": str(vpath)}

    def bind_fields(self, page: str, visual_name: str, fields_by_role: Dict[str, Any],
                    mode: str = "add", skip_validation: bool = False) -> Dict[str, Any]:
        """Add/replace field projections on an existing visual's query (mode: 'add' or 'replace')."""
        if not self.current_project or not self.current_project.is_pbir_enhanced:
            return {"success": False, "message": "Requires a loaded PBIR-Enhanced PBIP project"}
        page_dir = self._find_page_folder(page)
        if not page_dir:
            return {"success": False, "message": f"Page '{page}' not found"}
        vpath = page_dir / "visuals" / visual_name / "visual.json"
        if not vpath.exists():
            return {"success": False, "message": f"Visual '{visual_name}' not found on page"}
        if not skip_validation:
            missing = self._check_fields(fields_by_role)
            if missing:
                return {"success": False, "message": "Field(s) not found in the model", "missing_fields": missing}
        if self.auto_backup and not self.current_project.backup_path:
            self.create_backup()

        try:
            data = json.loads(self._read_text(vpath))
        except Exception as e:
            return {"success": False, "message": f"Could not parse visual.json: {e}"}
        qs = data.setdefault("visual", {}).setdefault("query", {}).setdefault("queryState", {})
        resolved = self._resolve_roles(fields_by_role)
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
        self._write_text(vpath, json.dumps(data, indent=2))
        return {"success": True, "visual_name": visual_name, "page": page_dir.name, "mode": mode}

    def validate_report_bindings(self) -> List[ValidationError]:
        """Check that every (table, field) referenced by the report exists in the model.
        This is the #1 cause of blank visuals / 'repair' prompts after external edits."""
        errors: List[ValidationError] = []
        catalog = self.model_field_catalog()
        tables_lower = {t.lower() for t in catalog}
        fields_lower = {t.lower(): {f.lower() for f in fs} for t, fs in catalog.items()}
        for fp, refs in self.collect_report_references_by_file().items():
            for (table, field) in sorted(refs):
                tl = table.lower()
                if tl not in tables_lower:
                    errors.append(ValidationError(fp, 0, "MISSING_TABLE",
                        f"Report references table '{table}' that is not in the model", f"{table}[{field}]"))
                elif field.lower() not in fields_lower.get(tl, set()):
                    errors.append(ValidationError(fp, 0, "MISSING_FIELD",
                        f"Report references '{table}[{field}]' but '{field}' is not in table '{table}'",
                        f"{table}[{field}]"))
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
        """Locate the .tmdl file that declares the given table (one table per file).
        Handles quoted names with escaped inner quotes ('O''Brien')."""
        want = table_name.strip().lower()
        for tmdl in (self.current_project.tmdl_files if self.current_project else []):
            try:
                content = self._read_text(tmdl)
            except Exception:
                continue
            m = re.search(r"^\s*table\s+(?:'((?:[^']|'')+)'|(\S+))", content, re.MULTILINE)
            if m and (m.group(1) or m.group(2)).replace("''", "'").strip().lower() == want:
                return tmdl
        return None

    def add_measures(self, table: str, measures: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Bulk-append measures to a table's .tmdl (offline). The whole batch is checked for
        name collisions (model-wide and within the batch) BEFORE anything is written."""
        if not self.current_project:
            return {"success": False, "message": "No PBIP project loaded. Use 'pbip_load_project' first."}
        path = self._find_table_file(table)
        if not path:
            return {"success": False, "message": f"Table '{table}' not found in the model"}
        index = self._model_field_index()
        existing = {f.lower() for fields in index.values() for f in fields}
        problems, seen = [], set()
        for i, m in enumerate(measures):
            name = (m.get("name") or "").strip()
            if not name or not (m.get("expression") or "").strip():
                problems.append(f"#{i + 1}: needs both a name and an expression")
                continue
            # Single-line TMDL fields must not carry line breaks (they would corrupt the file).
            for field in ("name", "format_string", "display_folder"):
                val = m.get(field)
                if isinstance(val, str) and ("\n" in val or "\r" in val):
                    problems.append(f"'{name}': {field} must not contain line breaks")
            if name.lower() in existing:
                problems.append(f"'{name}': already exists in the model")
            if name.lower() in seen:
                problems.append(f"'{name}': duplicated within the batch")
            seen.add(name.lower())
        if problems:
            return {"success": False,
                    "message": "Batch rejected; nothing was written:\n  - " + "\n  - ".join(problems)}
        if self.auto_backup and not self.current_project.backup_path:
            self.create_backup()
        content = self._read_text(path)
        block = tmdl_authoring.render_measures(measures)
        self._write_text(path, tmdl_authoring.append_block(content, block))
        return {"success": True, "created": [m["name"] for m in measures], "path": str(path)}

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
        tables_dir.mkdir(parents=True, exist_ok=True)
        path, path_err = self._safe_new_table_path(tables_dir, name)
        if path_err:
            return {"success": False, "message": path_err}
        if self.auto_backup and not self.current_project.backup_path:
            self.create_backup()
        self._write_text(path, content)
        self.current_project.tmdl_files.append(path)
        n_cols = len(re.findall(r"^\tcolumn ", content, re.MULTILINE))
        return {"success": True, "table": name, "path": str(path), "columns": n_cols}

    def add_calculation_group(self, name: str, items: List[Dict[str, Any]],
                              column_name: str = "Calculation", precedence: int = 1) -> Dict[str, Any]:
        """Create a calculation-group table as a new .tmdl file (offline)."""
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
                   if any(isinstance(it.get(f), str) and ("\n" in it[f] or "\r" in it[f])
                          for f in ("name", "format_string_expression"))]
        if crooked:
            return {"success": False,
                    "message": f"Item name/format must not contain line breaks: {', '.join(crooked)}"}
        tables_dir.mkdir(parents=True, exist_ok=True)
        path, path_err = self._safe_new_table_path(tables_dir, name)
        if path_err:
            return {"success": False, "message": path_err}
        if self.auto_backup and not self.current_project.backup_path:
            self.create_backup()
        content = tmdl_authoring.build_calculation_group(name, items, column_name, precedence)
        self._write_text(path, content)
        self.current_project.tmdl_files.append(path)
        # Calculation groups require discourageImplicitMeasures on the model; the engine
        # refuses a calc group without it. Set the flag in model.tmdl if absent.
        flag_set = False
        model_tmdl = self.current_project.semantic_model_folder / "definition" / "model.tmdl"
        try:
            if model_tmdl.exists():
                mcontent = self._read_text(model_tmdl)
                if "discourageImplicitMeasures" not in mcontent:
                    mm = re.search(r"^model\s+\S+.*$", mcontent, re.MULTILINE)
                    if mm:
                        insert_at = mm.end()
                        mcontent = (mcontent[:insert_at] + "\n\tdiscourageImplicitMeasures"
                                    + mcontent[insert_at:])
                        self._write_text(model_tmdl, mcontent)
                        flag_set = True
        except Exception as e:
            logger.warning(f"Could not set discourageImplicitMeasures in model.tmdl: {e}")
        # Calculation groups need compatibilityLevel >= 1470; warn (never auto-upgrade,
        # since a level bump is irreversible).
        compat_warning = None
        try:
            db_tmdl = self.current_project.semantic_model_folder / "definition" / "database.tmdl"
            if db_tmdl.exists():
                dm = re.search(r"compatibilityLevel:\s*(\d+)", self._read_text(db_tmdl))
                if dm and int(dm.group(1)) < 1470:
                    compat_warning = (f"database.tmdl compatibilityLevel is {dm.group(1)}; calculation "
                                      "groups need >= 1470 (Desktop writes 1550). Upgrade the model in "
                                      "Power BI Desktop before relying on this group.")
        except Exception:
            pass
        return {"success": True, "table": name, "path": str(path), "items": len(items),
                "discourage_implicit_measures_set": flag_set, "compat_warning": compat_warning}

    def add_hierarchy(self, table: str, name: str, levels: List[str]) -> Dict[str, Any]:
        """Append a hierarchy to a table's .tmdl (offline). Level columns must exist."""
        if not self.current_project:
            return {"success": False, "message": "No PBIP project loaded. Use 'pbip_load_project' first."}
        path = self._find_table_file(table)
        if not path:
            return {"success": False, "message": f"Table '{table}' not found in the model"}
        index = self._model_field_index()
        cols = {f.lower() for f, meta in index.get(table, {}).items() if meta.get("kind") == "column"}
        # _model_field_index keys by declared table name; find case-insensitively
        for tname, fields in index.items():
            if tname.lower() == table.lower():
                cols = {f.lower() for f, meta in fields.items() if meta.get("kind") == "column"}
                break
        missing = [c for c in levels if c.lower() not in cols]
        if missing:
            return {"success": False,
                    "message": f"Column(s) not found in '{table}': {', '.join(missing)}"}
        content = self._read_text(path)
        if re.search(rf"^\s*hierarchy\s+(?:'{re.escape(name)}'|{re.escape(name)})\s*$",
                     content, re.MULTILINE | re.IGNORECASE):
            return {"success": False, "message": f"Hierarchy '{name}' already exists on '{table}'"}
        if self.auto_backup and not self.current_project.backup_path:
            self.create_backup()
        block = tmdl_authoring.render_hierarchy(name, levels)
        self._write_text(path, tmdl_authoring.append_block(content, block))
        return {"success": True, "table": table, "hierarchy": name, "levels": levels, "path": str(path)}

    def _read_text(self, file_path) -> str:
        """Read a text file, remembering its encoding (utf-8 vs utf-8-sig/BOM) so it can be
        written back faithfully. Returns text with any BOM stripped."""
        with open(file_path, 'rb') as f:
            raw = f.read()
        if raw.startswith(b'\xef\xbb\xbf'):
            self._file_encodings[str(file_path)] = 'utf-8-sig'
            return raw.decode('utf-8-sig')
        self._file_encodings[str(file_path)] = 'utf-8'
        return raw.decode('utf-8')

    def _write_text(self, file_path, content: str) -> None:
        """Atomically write text (temp file + os.replace) using the file's remembered
        encoding and without translating newlines, so a crash mid-write cannot corrupt
        the original and CRLF/LF and BOM are preserved."""
        encoding = self._file_encodings.get(str(file_path), 'utf-8')
        path = Path(file_path)
        tmp = path.with_name(path.name + '.tmp')
        with open(tmp, 'w', encoding=encoding, newline='') as f:
            f.write(content)
        os.replace(tmp, path)

    def _cache_file_content(self, file_path: Path) -> None:
        """Cache original file content for potential rollback (records encoding too)"""
        if str(file_path) not in self._original_files:
            try:
                self._original_files[str(file_path)] = self._read_text(file_path)
            except Exception as e:
                logger.warning(f"Could not cache file {file_path}: {e}")

    def rollback_changes(self) -> bool:
        """
        Rollback all changes made in this session (atomic, encoding-preserving)

        Returns:
            True if rollback successful
        """
        if not self._original_files:
            logger.warning("No changes to rollback")
            return False

        try:
            for file_path, content in self._original_files.items():
                self._write_text(file_path, content)

            logger.info(f"Rolled back {len(self._original_files)} file(s)")
            self._original_files = {}
            return True

        except Exception as e:
            logger.error(f"Rollback failed: {e}")
            return False

    # ==================== VALIDATION ====================

    def validate_tmdl_syntax(self) -> List[ValidationError]:
        """
        Validate TMDL files for common syntax errors

        Returns:
            List of validation errors found
        """
        errors = []

        if not self.current_project or not self.current_project.tmdl_files:
            return errors

        # Build set of table names that need quoting
        tables_needing_quotes = set()
        try:
            for tmdl_file in self.current_project.tmdl_files:
                with open(tmdl_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                # Find all table declarations (handle both quoted and unquoted names)
                # Pattern 1: table 'Name With Spaces'
                for match in re.finditer(r"^table\s+'([^']+)'", content, re.MULTILINE):
                    table_name = match.group(1).replace("''", "'")  # Unescape quotes
                    if needs_tmdl_quoting(table_name):
                        tables_needing_quotes.add(table_name)
                # Pattern 2: table UnquotedName
                for match in re.finditer(r"^table\s+(\w+)\s*$", content, re.MULTILINE):
                    table_name = match.group(1)
                    if needs_tmdl_quoting(table_name):
                        tables_needing_quotes.add(table_name)
        except Exception:
            pass

        for tmdl_file in self.current_project.tmdl_files:
            try:
                with open(tmdl_file, 'r', encoding='utf-8') as f:
                    lines = f.readlines()

                for i, line in enumerate(lines, 1):
                    stripped = line.strip()

                    # Check for unquoted names with spaces in declarations
                    # Pattern: "table Name With Spaces" (without quotes)
                    if stripped.startswith('table '):
                        name_part = stripped[6:].strip()
                        if ' ' in name_part and not name_part.startswith("'"):
                            errors.append(ValidationError(
                                file_path=str(tmdl_file),
                                line_number=i,
                                error_type="UNQUOTED_NAME",
                                message=f"Table name with spaces must be quoted: {name_part}",
                                context=stripped
                            ))

                    # Check fromTable/toTable references
                    for prefix in ['fromTable:', 'toTable:']:
                        if prefix in stripped:
                            # Extract the name after the prefix
                            match = re.search(rf'{prefix}\s*(.+?)(?:\s*$|\s+\w+:)', stripped)
                            if match:
                                name_part = match.group(1).strip()
                                if ' ' in name_part and not name_part.startswith("'"):
                                    errors.append(ValidationError(
                                        file_path=str(tmdl_file),
                                        line_number=i,
                                        error_type="UNQUOTED_REFERENCE",
                                        message=f"Relationship reference with spaces must be quoted: {name_part}",
                                        context=stripped
                                    ))

                    # Check for unquoted table references in DAX (measure/column expressions)
                    if 'expression:' in stripped or '=' in stripped:
                        # Check for unquoted table references that need quoting
                        for table_name in tables_needing_quotes:
                            # Pattern: unquoted TableName[Column] where table has spaces
                            if re.search(rf"(?<!['\w]){re.escape(table_name)}(?=\s*\[)", stripped):
                                errors.append(ValidationError(
                                    file_path=str(tmdl_file),
                                    line_number=i,
                                    error_type="UNQUOTED_TABLE_IN_DAX",
                                    message=f"Table '{table_name}' in DAX expression must be quoted: use '{quote_tmdl_name(table_name)}' instead of '{table_name}'",
                                    context=stripped
                                ))
                                break  # Only report once per line

            except Exception as e:
                errors.append(ValidationError(
                    file_path=str(tmdl_file),
                    line_number=0,
                    error_type="FILE_ERROR",
                    message=str(e),
                    context=""
                ))

        return errors

    def fix_all_dax_quoting(self) -> Dict[str, Any]:
        """
        Fix all DAX expressions in TMDL files by properly quoting table names with spaces.

        This scans all measures and expressions and quotes table names that have spaces
        but are referenced without quotes.

        Returns:
            Dict with files_modified, references_fixed count, and validation_errors
        """
        if not self.current_project or not self.current_project.tmdl_files:
            return {"files_modified": [], "count": 0, "errors": []}

        files_modified = []
        total_fixes = 0
        errors = []

        # First, collect all table names in the project
        table_names = set()
        try:
            for tmdl_file in self.current_project.tmdl_files:
                with open(tmdl_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                # Find all table declarations - both quoted and unquoted
                # Pattern 1: table 'Name With Spaces'
                for match in re.finditer(r"^(?:\s*)table\s+'([^']+)'", content, re.MULTILINE):
                    table_name = match.group(1).replace("''", "'")  # Unescape quotes
                    table_names.add(table_name)
                # Pattern 2: table UnquotedName
                for match in re.finditer(r"^(?:\s*)table\s+(\w+)\s*$", content, re.MULTILINE):
                    table_names.add(match.group(1))
        except Exception as e:
            logger.warning(f"Could not extract table names: {e}")

        # Filter to only tables that need quoting
        tables_needing_quotes = [t for t in table_names if needs_tmdl_quoting(t)]

        if not tables_needing_quotes:
            return {"files_modified": [], "count": 0, "errors": []}

        # Now process each file and fix DAX expressions
        for tmdl_file in self.current_project.tmdl_files:
            try:
                self._cache_file_content(tmdl_file)

                with open(tmdl_file, 'r', encoding='utf-8') as f:
                    content = f.read()

                original_content = content
                file_fixes = 0

                # Fix each table that needs quoting
                for table_name in tables_needing_quotes:
                    table_quoted = quote_tmdl_name(table_name)
                    escaped_name = re.escape(table_name)

                    # Pattern 1: TableName[Column] -> 'TableName'[Column]
                    pattern1 = rf"(?<!['\w]){escaped_name}(?=\s*\[)"
                    content_before = content
                    content = re.sub(pattern1, table_quoted, content)
                    file_fixes += len(re.findall(pattern1, content_before))

                    # Pattern 2: Handle function calls like CALCULATE(SUM(TableName[Col]))
                    # This pattern matches unquoted table names in DAX contexts
                    pattern2 = rf"(\()\s*{escaped_name}(?=\s*[\[\,\)])"
                    content_before = content
                    content = re.sub(pattern2, rf"\1{table_quoted}", content)
                    file_fixes += len(re.findall(pattern2, content_before))

                if content != original_content:
                    with open(tmdl_file, 'w', encoding='utf-8') as f:
                        f.write(content)
                    files_modified.append(str(tmdl_file))
                    total_fixes += file_fixes
                    logger.info(f"Fixed {file_fixes} DAX quote references in {tmdl_file}")

            except Exception as e:
                logger.error(f"Error fixing DAX in {tmdl_file}: {e}")
                errors.append({"file": str(tmdl_file), "error": str(e)})

        return {
            "files_modified": files_modified,
            "count": total_fixes,
            "tables_fixed": tables_needing_quotes,
            "errors": errors
        }

    # ==================== RENAME OPERATIONS ====================

    def rename_table_in_files(self, old_name: str, new_name: str) -> RenameResult:
        """
        Rename a table across all PBIP files (TMDL + report visuals)

        This is a COMPREHENSIVE rename that:
        1. Updates table declarations in TMDL files
        2. Updates ALL DAX references with proper quoting (if new name has spaces)
        3. Updates report layer (PBIR-Legacy or PBIR-Enhanced visual.json files)
        4. Updates cultures files (linguistic schema)

        Supports both PBIR-Legacy (report.json) and PBIR-Enhanced (visual.json files) formats.

        Args:
            old_name: Current table name
            new_name: New table name

        Returns:
            RenameResult with details
        """
        if not self.current_project:
            return RenameResult(False, "No project loaded", [], 0)

        files_modified = []
        total_replacements = 0
        backup_path = None

        # Create backup if enabled
        if self.auto_backup and not self.current_project.backup_path:
            backup_path = self.create_backup()

        # Transactional cascade: if any step fails, roll every file back so the model
        # and report can never be left half-renamed (inconsistent). The rollback cache is
        # scoped to THIS operation - a stale cache from an earlier operation would otherwise
        # roll files back past changes made in between (e.g. measures added by add_measures).
        self._original_files = {}
        try:
            # 1. Update TMDL files (semantic model) - auto-quoting is built-in
            tmdl_replacements = self._rename_table_in_tmdl_files(old_name, new_name)
            files_modified.extend(tmdl_replacements["files"])
            total_replacements += tmdl_replacements["count"]

            # 2. Update report layer (PBIR-Legacy or PBIR-Enhanced)
            if self.current_project.is_pbir_enhanced:
                # PBIR Enhanced: Update individual visual.json files
                visual_replacements = self._rename_table_in_visual_files(old_name, new_name)
                files_modified.extend(visual_replacements["files"])
                total_replacements += visual_replacements["count"]
            elif self.current_project.report_json_path:
                # PBIR Legacy: Update single report.json
                report_replacements = self._rename_table_in_report_json(old_name, new_name)
                if report_replacements["count"] > 0:
                    files_modified.append(str(self.current_project.report_json_path))
                    total_replacements += report_replacements["count"]

            # 3. Update cultures files (linguistic schema)
            if self.current_project.cultures_files:
                cultures_replacements = self._rename_table_in_cultures_files(old_name, new_name)
                files_modified.extend(cultures_replacements["files"])
                total_replacements += cultures_replacements["count"]

            # 4. Update diagramLayout.json (model diagram)
            if self.current_project.diagram_layout_path:
                diagram_replacements = self._rename_table_in_diagram_layout(old_name, new_name)
                files_modified.extend(diagram_replacements["files"])
                total_replacements += diagram_replacements["count"]

            # 5. Validate after changes
            validation_errors = self.validate_tmdl_syntax()
        except Exception as e:
            logger.error(f"Table rename cascade failed, rolling back: {e}")
            self.rollback_changes()
            return RenameResult(
                False,
                f"Rename of table '{old_name}' failed and all changes were rolled back: {e}",
                [], 0,
                details={"old_name": old_name, "new_name": new_name, "rolled_back": True},
                backup_created=backup_path,
            )

        report_format = "PBIR-Enhanced" if self.current_project.is_pbir_enhanced else "PBIR-Legacy"

        return RenameResult(
            success=len(validation_errors) == 0,
            message=f"Renamed table '{old_name}' to '{new_name}' in {len(files_modified)} file(s) ({report_format})" +
                    (f" with {len(validation_errors)} validation error(s)" if validation_errors else ""),
            files_modified=files_modified,
            references_updated=total_replacements,
            details={"old_name": old_name, "new_name": new_name, "report_format": report_format},
            validation_errors=validation_errors,
            backup_created=backup_path
        )

    def rename_column_in_files(self, table_name: str, old_name: str, new_name: str) -> RenameResult:
        """
        Rename a column across all PBIP files

        Supports both PBIR-Legacy and PBIR-Enhanced formats.

        Args:
            table_name: Table containing the column
            old_name: Current column name
            new_name: New column name

        Returns:
            RenameResult with details
        """
        if not self.current_project:
            return RenameResult(False, "No project loaded", [], 0)

        files_modified = []
        total_replacements = 0
        backup_path = None

        # Create backup if enabled
        if self.auto_backup and not self.current_project.backup_path:
            backup_path = self.create_backup()

        # Transactional cascade: roll back every file if any step fails. Scope the
        # rollback cache to THIS operation (see rename_table_in_files).
        self._original_files = {}
        try:
            # 1. Update TMDL files (semantic model)
            tmdl_replacements = self._rename_column_in_tmdl_files(table_name, old_name, new_name)
            files_modified.extend(tmdl_replacements["files"])
            total_replacements += tmdl_replacements["count"]

            # 2. Update report layer (PBIR-Legacy or PBIR-Enhanced)
            if self.current_project.is_pbir_enhanced:
                # PBIR Enhanced: Update individual visual.json files
                visual_replacements = self._rename_column_in_visual_files(table_name, old_name, new_name)
                files_modified.extend(visual_replacements["files"])
                total_replacements += visual_replacements["count"]
            elif self.current_project.report_json_path:
                # PBIR Legacy: Update single report.json
                report_replacements = self._rename_column_in_report_json(table_name, old_name, new_name)
                if report_replacements["count"] > 0:
                    files_modified.append(str(self.current_project.report_json_path))
                    total_replacements += report_replacements["count"]
        except Exception as e:
            logger.error(f"Column rename cascade failed, rolling back: {e}")
            self.rollback_changes()
            return RenameResult(
                False,
                f"Rename of column '{table_name}'[{old_name}] failed and all changes were rolled back: {e}",
                [], 0,
                details={"table_name": table_name, "old_name": old_name, "new_name": new_name, "rolled_back": True},
                backup_created=backup_path,
            )

        report_format = "PBIR-Enhanced" if self.current_project.is_pbir_enhanced else "PBIR-Legacy"

        return RenameResult(
            success=True,
            message=f"Renamed column '{table_name}'[{old_name}] to [{new_name}] in {len(files_modified)} file(s) ({report_format})",
            files_modified=files_modified,
            references_updated=total_replacements,
            details={"table_name": table_name, "old_name": old_name, "new_name": new_name, "report_format": report_format},
            backup_created=backup_path
        )

    def rename_measure_in_files(self, old_name: str, new_name: str) -> RenameResult:
        """
        Rename a measure across all PBIP files

        Supports both PBIR-Legacy and PBIR-Enhanced formats.

        Args:
            old_name: Current measure name
            new_name: New measure name

        Returns:
            RenameResult with details
        """
        if not self.current_project:
            return RenameResult(False, "No project loaded", [], 0)

        files_modified = []
        total_replacements = 0
        backup_path = None

        # Create backup if enabled
        if self.auto_backup and not self.current_project.backup_path:
            backup_path = self.create_backup()

        # Transactional cascade: roll back every file if any step fails. Scope the
        # rollback cache to THIS operation (see rename_table_in_files).
        self._original_files = {}
        try:
            # 1. Update TMDL files (semantic model)
            tmdl_replacements = self._rename_measure_in_tmdl_files(old_name, new_name)
            files_modified.extend(tmdl_replacements["files"])
            total_replacements += tmdl_replacements["count"]

            # 2. Update report layer (PBIR-Legacy or PBIR-Enhanced)
            if self.current_project.is_pbir_enhanced:
                # PBIR Enhanced: Update individual visual.json files
                visual_replacements = self._rename_measure_in_visual_files(old_name, new_name)
                files_modified.extend(visual_replacements["files"])
                total_replacements += visual_replacements["count"]
            elif self.current_project.report_json_path:
                # PBIR Legacy: Update single report.json
                report_replacements = self._rename_measure_in_report_json(old_name, new_name)
                if report_replacements["count"] > 0:
                    files_modified.append(str(self.current_project.report_json_path))
                    total_replacements += report_replacements["count"]
        except Exception as e:
            logger.error(f"Measure rename cascade failed, rolling back: {e}")
            self.rollback_changes()
            return RenameResult(
                False,
                f"Rename of measure '{old_name}' failed and all changes were rolled back: {e}",
                [], 0,
                details={"old_name": old_name, "new_name": new_name, "rolled_back": True},
                backup_created=backup_path,
            )

        report_format = "PBIR-Enhanced" if self.current_project.is_pbir_enhanced else "PBIR-Legacy"

        return RenameResult(
            success=True,
            message=f"Renamed measure '{old_name}' to '{new_name}' in {len(files_modified)} file(s) ({report_format})",
            files_modified=files_modified,
            references_updated=total_replacements,
            details={"old_name": old_name, "new_name": new_name, "report_format": report_format},
            backup_created=backup_path
        )

    # ==================== TMDL FILE OPERATIONS ====================

    def _rename_table_in_tmdl_files(self, old_name: str, new_name: str) -> Dict[str, Any]:
        """
        Rename table references in TMDL files with proper quoting

        Handles:
        1. Table declarations: table OldName -> table 'New Name'
        2. DAX references: 'OldName'[Column] -> 'New Name'[Column]
        3. Relationship refs: fromTable: OldName -> fromTable: 'New Name'
        4. Relationship names: 'OldName to Table' -> 'New Name to Table'
        """
        files_modified = []
        total_count = 0

        if not self.current_project or not self.current_project.tmdl_files:
            return {"files": files_modified, "count": total_count}

        # Properly quoted new name for TMDL
        new_name_quoted = quote_tmdl_name(new_name)

        # Build regex patterns for old name (handle both quoted and unquoted)
        old_name_escaped = re.escape(old_name)
        old_name_quoted_escaped = re.escape(f"'{old_name}'")

        # Build patterns list
        patterns = []

        # Pattern 1: Table declaration (both quoted and unquoted old names)
        # table OldName -> table 'New Name'
        patterns.append((
            rf'^(\s*)table\s+{old_name_escaped}\s*$',
            rf'\1table {new_name_quoted}',
            re.MULTILINE
        ))
        # table 'OldName' -> table 'New Name'
        patterns.append((
            rf"^(\s*)table\s+'{old_name_escaped}'\s*$",
            rf'\1table {new_name_quoted}',
            re.MULTILINE
        ))

        # Pattern 2: DAX references - 'TableName'[Column]
        patterns.append((
            rf"'{old_name_escaped}'\s*\[",
            f"{new_name_quoted}[",
            0
        ))

        # Pattern 3: DAX references - unquoted TableName[Column]
        # IMPORTANT: Always use the properly quoted version for the new name
        patterns.append((
            rf"(?<!['\w]){old_name_escaped}(?=\s*\[)",
            new_name_quoted,
            0
        ))

        # Pattern 4: 'TableName' in function calls (RELATED, ALL, VALUES, etc.)
        patterns.append((
            rf"'{old_name_escaped}'(?=\s*[,\)\]])",
            new_name_quoted,
            0
        ))

        # Pattern 4b: Unquoted TableName in function calls
        # This catches: FUNCTION(OldTable[, FUNCTION(OldTable,, FUNCTION(OldTable)
        # Examples: COUNTROWS(TableName), SUM(TableName[Col]), FILTER(TableName, ...)
        patterns.append((
            rf"([A-Z]+\s*\(\s*){old_name_escaped}(?=\s*[\[\,\)])",
            rf"\1{new_name_quoted}",
            re.IGNORECASE
        ))

        # Pattern 4c: Relationship names containing table name (CRITICAL for relationships)
        # Handle: relationship 'OldName to SomeTable' -> relationship 'NewName to SomeTable'
        # The entire name is in quotes, so we need to replace just the table name part inside quotes
        patterns.append((
            rf"(relationship\s+')({old_name_escaped})(\s+to\s+[^']*)'",
            rf"\1{new_name}\3'",
            re.IGNORECASE
        ))
        # Handle: relationship 'SomeTable to OldName' -> relationship 'SomeTable to NewName'
        patterns.append((
            rf"(relationship\s+')([^']*\s+to\s+)({old_name_escaped})'",
            rf"\1\2{new_name}'",
            re.IGNORECASE
        ))
        # Handle unquoted: relationship OldName to SomeTable -> relationship 'NewName' to SomeTable
        patterns.append((
            rf"(relationship\s+){old_name_escaped}(\s+to\s+)",
            rf"\1{new_name_quoted}\2",
            re.IGNORECASE
        ))
        # Handle unquoted: relationship SomeTable to OldName -> relationship SomeTable to 'NewName'
        patterns.append((
            rf"(relationship\s+)(\S+\s+to\s+){old_name_escaped}(?=\s|$)",
            rf"\1\2{new_name_quoted}",
            re.IGNORECASE | re.MULTILINE
        ))

        # Pattern 5: fromTable reference (CRITICAL for relationships)
        # fromTable: OldName -> fromTable: 'New Name'
        patterns.append((
            rf'(fromTable\s*:\s*){old_name_escaped}(?=\s*(?:$|\n|\r|toTable|fromColumn|toColumn))',
            rf'\1{new_name_quoted}',
            re.MULTILINE | re.IGNORECASE
        ))
        # fromTable: 'OldName' -> fromTable: 'New Name'
        patterns.append((
            rf"(fromTable\s*:\s*)'{old_name_escaped}'",
            rf'\1{new_name_quoted}',
            re.IGNORECASE
        ))

        # Pattern 6: toTable reference (CRITICAL for relationships)
        # toTable: OldName -> toTable: 'New Name'
        patterns.append((
            rf'(toTable\s*:\s*){old_name_escaped}(?=\s*(?:$|\n|\r|fromTable|fromColumn|toColumn))',
            rf'\1{new_name_quoted}',
            re.MULTILINE | re.IGNORECASE
        ))
        # toTable: 'OldName' -> toTable: 'New Name'
        patterns.append((
            rf"(toTable\s*:\s*)'{old_name_escaped}'",
            rf'\1{new_name_quoted}',
            re.IGNORECASE
        ))

        # Pattern 7: Partition name (partition OldName = m)
        # partition OldName -> partition 'New Name'
        patterns.append((
            rf'^(\s*)partition\s+{old_name_escaped}\s*=',
            rf'\1partition {new_name_quoted} =',
            re.MULTILINE
        ))
        # partition 'OldName' -> partition 'New Name'
        patterns.append((
            rf"^(\s*)partition\s+'{old_name_escaped}'\s*=",
            rf'\1partition {new_name_quoted} =',
            re.MULTILINE
        ))

        # Pattern 8: fromColumn with table prefix (CRITICAL for relationships)
        # Format: fromColumn: TableName.ColumnName or fromColumn: TableName.'Column Name'
        # fromColumn: OldTable.Column -> fromColumn: 'NewTable'.Column
        patterns.append((
            rf'(fromColumn\s*:\s*){old_name_escaped}\.(\w+)',
            rf'\1{new_name_quoted}.\2',
            re.MULTILINE
        ))
        # fromColumn: OldTable.'Column Name' -> fromColumn: 'NewTable'.'Column Name'
        patterns.append((
            rf"(fromColumn\s*:\s*){old_name_escaped}\.('[^']+')",
            rf'\1{new_name_quoted}.\2',
            re.MULTILINE
        ))
        # fromColumn: 'OldTable'.Column -> fromColumn: 'NewTable'.Column
        patterns.append((
            rf"(fromColumn\s*:\s*)'{old_name_escaped}'\.(\w+)",
            rf'\1{new_name_quoted}.\2',
            re.MULTILINE
        ))
        # fromColumn: 'OldTable'.'Column Name' -> fromColumn: 'NewTable'.'Column Name'
        patterns.append((
            rf"(fromColumn\s*:\s*)'{old_name_escaped}'\.('[^']+')",
            rf'\1{new_name_quoted}.\2',
            re.MULTILINE
        ))

        # Pattern 9: toColumn with table prefix (CRITICAL for relationships)
        # Same patterns as fromColumn but for toColumn
        # toColumn: OldTable.Column -> toColumn: 'NewTable'.Column
        patterns.append((
            rf'(toColumn\s*:\s*){old_name_escaped}\.(\w+)',
            rf'\1{new_name_quoted}.\2',
            re.MULTILINE
        ))
        # toColumn: OldTable.'Column Name' -> toColumn: 'NewTable'.'Column Name'
        patterns.append((
            rf"(toColumn\s*:\s*){old_name_escaped}\.('[^']+')",
            rf'\1{new_name_quoted}.\2',
            re.MULTILINE
        ))
        # toColumn: 'OldTable'.Column -> toColumn: 'NewTable'.Column
        patterns.append((
            rf"(toColumn\s*:\s*)'{old_name_escaped}'\.(\w+)",
            rf'\1{new_name_quoted}.\2',
            re.MULTILINE
        ))
        # toColumn: 'OldTable'.'Column Name' -> toColumn: 'NewTable'.'Column Name'
        patterns.append((
            rf"(toColumn\s*:\s*)'{old_name_escaped}'\.('[^']+')",
            rf'\1{new_name_quoted}.\2',
            re.MULTILINE
        ))

        # Pattern 10: ref table in model.tmdl
        # ref table OldName -> ref table 'NewName'
        patterns.append((
            rf'^(\s*)ref\s+table\s+{old_name_escaped}\s*$',
            rf'\1ref table {new_name_quoted}',
            re.MULTILINE
        ))
        # ref table 'OldName' -> ref table 'NewName'
        patterns.append((
            rf"^(\s*)ref\s+table\s+'{old_name_escaped}'\s*$",
            rf'\1ref table {new_name_quoted}',
            re.MULTILINE
        ))

        # Pattern 11: PBI_QueryOrder annotation (list of table names in JSON array format)
        # IMPORTANT: Must be specific to PBI_QueryOrder to avoid matching M-Code entity= references
        # Format: annotation PBI_QueryOrder = ["Table1","Table2",...]
        # DO NOT match: {[entity="TableName",version=""]} (M-Code/Power Query source references)
        patterns.append((
            rf'(PBI_QueryOrder\s*=\s*\[.*?")({old_name_escaped})(".*?\])',
            rf'\1{new_name}\3',
            0
        ))

        # Pattern 12: M-Code internal table references
        # These are references to other tables in the same model within Power Query
        # Format: VariableName = TableName, (inside let ... in blocks)
        # Uses #"..." quoting for names with spaces
        new_name_mcode = quote_mcode_name(new_name)

        # = OldName, (assignment with comma - most common in let blocks)
        patterns.append((
            rf'(=\s*){old_name_escaped}(\s*,)',
            rf'\1{new_name_mcode}\2',
            0
        ))
        # = #"OldName", (quoted assignment with comma)
        patterns.append((
            rf'(=\s*)#"{old_name_escaped}"(\s*,)',
            rf'\1{new_name_mcode}\2',
            0
        ))
        # = OldName at end of line (no comma - last line in let block or simple query)
        patterns.append((
            rf'(=\s*){old_name_escaped}(\s*)$',
            rf'\1{new_name_mcode}\2',
            re.MULTILINE
        ))
        # = #"OldName" at end of line
        patterns.append((
            rf'(=\s*)#"{old_name_escaped}"(\s*)$',
            rf'\1{new_name_mcode}\2',
            re.MULTILINE
        ))

        for tmdl_file in self.current_project.tmdl_files:
            try:
                # Cache original content for rollback
                self._cache_file_content(tmdl_file)

                with open(tmdl_file, 'r', encoding='utf-8') as f:
                    content = f.read()

                original_content = content
                file_count = 0

                # CRITICAL: Extract ONLY external dataflow references
                # External refs like {[entity="...",version=""]}[Data] must NEVER change
                # Internal table refs like Source = TableName SHOULD change
                content, external_refs = extract_external_refs(content)

                for pattern, replacement, flags in patterns:
                    content, count = re.subn(pattern, replacement, content, flags=flags)
                    file_count += count

                # Restore external references (unchanged)
                content = restore_external_refs(content, external_refs)

                if content != original_content:
                    with open(tmdl_file, 'w', encoding='utf-8') as f:
                        f.write(content)
                    files_modified.append(str(tmdl_file))
                    total_count += file_count
                    logger.info(f"Updated {file_count} references in {tmdl_file}")

            except Exception as e:
                logger.error(f"Error updating TMDL file {tmdl_file}: {e}")

        return {"files": files_modified, "count": total_count}

    def _rename_column_in_tmdl_files(self, table_name: str, old_name: str, new_name: str) -> Dict[str, Any]:
        """Rename column references in TMDL files with proper quoting"""
        files_modified = []
        total_count = 0

        if not self.current_project or not self.current_project.tmdl_files:
            return {"files": files_modified, "count": total_count}

        # Quote names if needed
        table_name_quoted = quote_tmdl_name(table_name)
        new_name_quoted = quote_tmdl_name(new_name)

        # For DAX, columns inside brackets don't need external quotes but may need them if they have spaces
        new_col_in_bracket = new_name if not needs_tmdl_quoting(new_name) else new_name

        # Escape for regex
        table_escaped = re.escape(table_name)
        old_escaped = re.escape(old_name)

        # Patterns for column references
        patterns = [
            # 'TableName'[OldColumn] -> 'TableName'[NewColumn]
            (rf"'{table_escaped}'\s*\[\s*{old_escaped}\s*\]", f"{table_name_quoted}[{new_col_in_bracket}]", 0),
            # TableName[OldColumn] -> TableName[NewColumn]
            (rf"(?<!['\w]){table_escaped}\s*\[\s*{old_escaped}\s*\]", f"{table_name}[{new_col_in_bracket}]", 0),
            # TMDL column definition: column OldName -> column NewName
            (rf'^(\s*)column\s+{old_escaped}\s*$', rf'\1column {new_name_quoted}', re.MULTILINE),
            (rf"^(\s*)column\s+'{old_escaped}'\s*$", rf'\1column {new_name_quoted}', re.MULTILINE),
            # fromColumn/toColumn with TableName.ColumnName format (CRITICAL for relationships)
            # fromColumn: TableName.OldColumn -> fromColumn: TableName.NewColumn
            (rf'(fromColumn\s*:\s*{table_escaped}\.)({old_escaped})(?=\s|$)', rf'\1{new_name_quoted}', re.MULTILINE),
            # fromColumn: TableName.'OldColumn' -> fromColumn: TableName.'NewColumn'
            (rf"(fromColumn\s*:\s*{table_escaped}\.)'{old_escaped}'", rf'\1{new_name_quoted}', 0),
            # fromColumn: 'TableName'.OldColumn -> fromColumn: 'TableName'.NewColumn
            (rf"(fromColumn\s*:\s*'{table_escaped}'\.)({old_escaped})(?=\s|$)", rf'\1{new_name_quoted}', re.MULTILINE),
            # fromColumn: 'TableName'.'OldColumn' -> fromColumn: 'TableName'.'NewColumn'
            (rf"(fromColumn\s*:\s*'{table_escaped}'\.)'{old_escaped}'", rf'\1{new_name_quoted}', 0),
            # toColumn: TableName.OldColumn -> toColumn: TableName.NewColumn
            (rf'(toColumn\s*:\s*{table_escaped}\.)({old_escaped})(?=\s|$)', rf'\1{new_name_quoted}', re.MULTILINE),
            # toColumn: TableName.'OldColumn' -> toColumn: TableName.'NewColumn'
            (rf"(toColumn\s*:\s*{table_escaped}\.)'{old_escaped}'", rf'\1{new_name_quoted}', 0),
            # toColumn: 'TableName'.OldColumn -> toColumn: 'TableName'.NewColumn
            (rf"(toColumn\s*:\s*'{table_escaped}'\.)({old_escaped})(?=\s|$)", rf'\1{new_name_quoted}', re.MULTILINE),
            # toColumn: 'TableName'.'OldColumn' -> toColumn: 'TableName'.'NewColumn'
            (rf"(toColumn\s*:\s*'{table_escaped}'\.)'{old_escaped}'", rf'\1{new_name_quoted}', 0),
        ]

        # Constructs that live ONLY inside the owning table's file (hierarchy levels,
        # sortByColumn wiring, calculated-table sourceColumn). Applying these globally would
        # rename same-named columns of OTHER tables, so they are scoped to this file.
        owning_file = self._find_table_file(table_name)
        owner_patterns = [
            # sortByColumn: OldName  (bare or quoted)
            (rf"^(\s*)sortByColumn:\s*{old_escaped}\s*$", rf"\1sortByColumn: {new_name_quoted}", re.MULTILINE),
            (rf"^(\s*)sortByColumn:\s*'{old_escaped}'\s*$", rf"\1sortByColumn: {new_name_quoted}", re.MULTILINE),
            # hierarchy level reference: column: OldName
            (rf"^(\s*)column:\s*{old_escaped}\s*$", rf"\1column: {new_name_quoted}", re.MULTILINE),
            (rf"^(\s*)column:\s*'{old_escaped}'\s*$", rf"\1column: {new_name_quoted}", re.MULTILINE),
            # NOTE: sourceColumn: [Name] is deliberately NOT rewritten - for calculated tables
            # it maps to the DAX output alias, which does not change when the column is renamed.
        ]

        for tmdl_file in self.current_project.tmdl_files:
            try:
                self._cache_file_content(tmdl_file)

                with open(tmdl_file, 'r', encoding='utf-8') as f:
                    content = f.read()

                original_content = content
                file_count = 0

                # Column rename patterns are specific to DAX/TMDL syntax
                # They won't accidentally match M-Code patterns
                for pattern, replacement, flags in patterns:
                    content, count = re.subn(pattern, replacement, content, flags=flags)
                    file_count += count
                if owning_file is not None and Path(tmdl_file) == Path(owning_file):
                    for pattern, replacement, flags in owner_patterns:
                        content, count = re.subn(pattern, replacement, content, flags=flags)
                        file_count += count

                if content != original_content:
                    with open(tmdl_file, 'w', encoding='utf-8') as f:
                        f.write(content)
                    files_modified.append(str(tmdl_file))
                    total_count += file_count

            except Exception as e:
                logger.error(f"Error updating TMDL file {tmdl_file}: {e}")

        return {"files": files_modified, "count": total_count}

    def _rename_measure_in_tmdl_files(self, old_name: str, new_name: str) -> Dict[str, Any]:
        """Rename measure references in TMDL files with proper quoting"""
        files_modified = []
        total_count = 0

        if not self.current_project or not self.current_project.tmdl_files:
            return {"files": files_modified, "count": total_count}

        new_name_quoted = quote_tmdl_name(new_name)
        old_escaped = re.escape(old_name)

        # Patterns for measure references. The bracket pattern must NOT touch a
        # calculated-table column mapping line (sourceColumn: [Name]) when a measure happens
        # to share a column's name - that mapping refers to the DAX-produced column, not the
        # measure. The fixed-width lookbehind covers the exact emitted/exported form.
        patterns = [
            # [MeasureName] references in DAX
            (rf"(?<!sourceColumn: )\[\s*{old_escaped}\s*\]", f"[{new_name}]", 0),
            # TMDL measure definition: measure OldName = -> measure NewName =
            (rf'^(\s*)measure\s+{old_escaped}\s*=', rf'\1measure {new_name_quoted} =', re.MULTILINE),
            (rf"^(\s*)measure\s+'{old_escaped}'\s*=", rf'\1measure {new_name_quoted} =', re.MULTILINE),
        ]

        for tmdl_file in self.current_project.tmdl_files:
            try:
                self._cache_file_content(tmdl_file)

                with open(tmdl_file, 'r', encoding='utf-8') as f:
                    content = f.read()

                original_content = content
                file_count = 0

                # Measure rename patterns are specific to DAX/TMDL syntax
                # They won't accidentally match M-Code patterns
                for pattern, replacement, flags in patterns:
                    content, count = re.subn(pattern, replacement, content, flags=flags)
                    file_count += count

                if content != original_content:
                    with open(tmdl_file, 'w', encoding='utf-8') as f:
                        f.write(content)
                    files_modified.append(str(tmdl_file))
                    total_count += file_count

            except Exception as e:
                logger.error(f"Error updating TMDL file {tmdl_file}: {e}")

        return {"files": files_modified, "count": total_count}

    # ==================== PBIR VISUAL.JSON OPERATIONS ====================

    def _rename_table_in_visual_files(self, old_name: str, new_name: str) -> Dict[str, Any]:
        """
        Rename table references in PBIR Enhanced format visual.json files.

        Each visual.json contains Entity references like:
        {
            "SourceRef": {
                "Entity": "Salesforce_Data"  <- This needs updating
            }
        }

        Returns:
            Dict with files modified and count of updates
        """
        if not self.current_project or not self.current_project.visual_json_files:
            return {"files": [], "count": 0}

        files_modified = []
        total_count = 0

        for visual_file in self.current_project.visual_json_files:
            try:
                self._cache_file_content(visual_file)

                with open(visual_file, 'r', encoding='utf-8') as f:
                    content = f.read()

                original_content = content
                file_count = 0

                # Pattern 1: "Entity": "OldTableName"
                pattern1 = rf'"Entity"\s*:\s*"{re.escape(old_name)}"'
                replacement1 = f'"Entity": "{new_name}"'
                content, c = re.subn(pattern1, replacement1, content)
                file_count += c

                # Pattern 2: queryRef patterns like "OldTableName.ColumnName"
                pattern2 = rf'"queryRef"\s*:\s*"{re.escape(old_name)}\.([^"]+)"'
                replacement2 = rf'"queryRef": "{new_name}.\1"'
                content, c = re.subn(pattern2, replacement2, content)
                file_count += c

                # Pattern 3: nativeQueryRef with table prefix
                pattern3 = rf'"nativeQueryRef"\s*:\s*"{re.escape(old_name)}\.([^"]+)"'
                replacement3 = rf'"nativeQueryRef": "{new_name}.\1"'
                content, c = re.subn(pattern3, replacement3, content)
                file_count += c

                # Pattern 4: metadata selector patterns
                pattern4 = rf'"metadata"\s*:\s*"{re.escape(old_name)}\.([^"]+)"'
                replacement4 = rf'"metadata": "{new_name}.\1"'
                content, c = re.subn(pattern4, replacement4, content)
                file_count += c

                if content != original_content:
                    with open(visual_file, 'w', encoding='utf-8') as f:
                        f.write(content)
                    files_modified.append(str(visual_file))
                    total_count += file_count
                    logger.info(f"Updated {file_count} Entity references in {visual_file.name}")

            except Exception as e:
                logger.error(f"Error updating visual file {visual_file}: {e}")

        return {"files": files_modified, "count": total_count}

    def _rename_column_in_visual_files(self, table_name: str, old_name: str, new_name: str) -> Dict[str, Any]:
        """Rename column references in PBIR Enhanced format visual.json files"""
        if not self.current_project or not self.current_project.visual_json_files:
            return {"files": [], "count": 0}

        files_modified = []
        total_count = 0

        for visual_file in self.current_project.visual_json_files:
            try:
                self._cache_file_content(visual_file)

                with open(visual_file, 'r', encoding='utf-8') as f:
                    content = f.read()

                original_content = content
                file_count = 0

                # Pattern 1: "Property": "ColumnName" (when Entity context is table_name)
                # This is tricky - we use JSON parsing for accuracy
                try:
                    visual_data = json.loads(content)
                    modified = self._deep_rename_column_in_json(visual_data, table_name, old_name, new_name)
                    if modified > 0:
                        content = json.dumps(visual_data, indent=2, ensure_ascii=False)
                        file_count += modified
                except json.JSONDecodeError:
                    pass

                # Pattern 2: queryRef patterns like "TableName.OldColumn"
                pattern2 = rf'"{re.escape(table_name)}\.{re.escape(old_name)}"'
                replacement2 = f'"{table_name}.{new_name}"'
                content, c = re.subn(pattern2, replacement2, content)
                file_count += c

                if content != original_content:
                    with open(visual_file, 'w', encoding='utf-8') as f:
                        f.write(content)
                    files_modified.append(str(visual_file))
                    total_count += file_count

            except Exception as e:
                logger.error(f"Error updating visual file {visual_file}: {e}")

        return {"files": files_modified, "count": total_count}

    def _rename_measure_in_visual_files(self, old_name: str, new_name: str) -> Dict[str, Any]:
        """Rename measure references in PBIR Enhanced format visual.json files"""
        if not self.current_project or not self.current_project.visual_json_files:
            return {"files": [], "count": 0}

        files_modified = []
        total_count = 0

        for visual_file in self.current_project.visual_json_files:
            try:
                self._cache_file_content(visual_file)

                with open(visual_file, 'r', encoding='utf-8') as f:
                    content = f.read()

                original_content = content
                file_count = 0

                # Pattern 1: "Property": "MeasureName"
                pattern1 = rf'"Property"\s*:\s*"{re.escape(old_name)}"'
                replacement1 = f'"Property": "{new_name}"'
                content, c = re.subn(pattern1, replacement1, content)
                file_count += c

                # Pattern 2: queryRef ending with measure name
                pattern2 = rf'\.{re.escape(old_name)}"'
                replacement2 = f'.{new_name}"'
                content, c = re.subn(pattern2, replacement2, content)
                file_count += c

                # Pattern 3: nativeQueryRef with measure name
                pattern3 = rf'"nativeQueryRef"\s*:\s*"{re.escape(old_name)}"'
                replacement3 = f'"nativeQueryRef": "{new_name}"'
                content, c = re.subn(pattern3, replacement3, content)
                file_count += c

                if content != original_content:
                    with open(visual_file, 'w', encoding='utf-8') as f:
                        f.write(content)
                    files_modified.append(str(visual_file))
                    total_count += file_count

            except Exception as e:
                logger.error(f"Error updating visual file {visual_file}: {e}")

        return {"files": files_modified, "count": total_count}

    def fix_broken_visual_references(self, old_table_name: str, new_table_name: str) -> Dict[str, Any]:
        """
        Fix broken visual references after a table rename.

        This is a targeted fix for the common scenario where:
        - TOM/API renamed the table in the semantic model
        - But report visuals still reference the old table name

        Works with both PBIR-Legacy and PBIR-Enhanced formats.

        Args:
            old_table_name: The old table name that visuals are still referencing
            new_table_name: The correct new table name

        Returns:
            Dict with fix results
        """
        if not self.current_project:
            return {"success": False, "error": "No project loaded"}

        files_modified = []
        total_count = 0
        errors = []

        # Fix PBIR-Legacy report.json
        if self.current_project.report_json_path and not self.current_project.is_pbir_enhanced:
            result = self._rename_table_in_report_json(old_table_name, new_table_name)
            if result.get("count", 0) > 0:
                files_modified.append(str(self.current_project.report_json_path))
                total_count += result["count"]

        # Fix PBIR-Enhanced visual.json files
        if self.current_project.is_pbir_enhanced and self.current_project.visual_json_files:
            result = self._rename_table_in_visual_files(old_table_name, new_table_name)
            files_modified.extend(result.get("files", []))
            total_count += result.get("count", 0)

        # Also fix cultures files (linguistic schema)
        if self.current_project.cultures_files:
            result = self._rename_table_in_cultures_files(old_table_name, new_table_name)
            files_modified.extend(result.get("files", []))
            total_count += result.get("count", 0)

        # Also fix diagramLayout.json
        if self.current_project.diagram_layout_path:
            result = self._rename_table_in_diagram_layout(old_table_name, new_table_name)
            files_modified.extend(result.get("files", []))
            total_count += result.get("count", 0)

        return {
            "success": total_count > 0,
            "files_modified": files_modified,
            "references_fixed": total_count,
            "format": "PBIR-Enhanced" if self.current_project.is_pbir_enhanced else "PBIR-Legacy",
            "errors": errors
        }

    def _rename_table_in_cultures_files(self, old_name: str, new_name: str) -> Dict[str, Any]:
        """
        Rename table references in cultures/linguistic schema files.

        These files contain:
        - "ConceptualEntity": "TableName" references used for Q&A
        - JSON keys like "TableName": { for linguistic entities
        """
        if not self.current_project or not self.current_project.cultures_files:
            return {"files": [], "count": 0}

        files_modified = []
        total_count = 0

        for cultures_file in self.current_project.cultures_files:
            try:
                self._cache_file_content(cultures_file)

                with open(cultures_file, 'r', encoding='utf-8') as f:
                    content = f.read()

                original_content = content
                file_count = 0

                # Pattern 1: "ConceptualEntity": "TableName"
                pattern1 = rf'"ConceptualEntity"\s*:\s*"{re.escape(old_name)}"'
                replacement1 = f'"ConceptualEntity": "{new_name}"'
                content, c = re.subn(pattern1, replacement1, content)
                file_count += c

                # Pattern 2: JSON key "TableName": { (linguistic entity definition)
                pattern2 = rf'"{re.escape(old_name)}"\s*:\s*\{{'
                replacement2 = f'"{new_name}": {{'
                content, c = re.subn(pattern2, replacement2, content)
                file_count += c

                if content != original_content:
                    with open(cultures_file, 'w', encoding='utf-8') as f:
                        f.write(content)
                    files_modified.append(str(cultures_file))
                    total_count += file_count
                    logger.info(f"Updated {file_count} linguistic schema references in {cultures_file.name}")

            except Exception as e:
                logger.error(f"Error updating cultures file {cultures_file}: {e}")

        return {"files": files_modified, "count": total_count}

    def _rename_table_in_diagram_layout(self, old_name: str, new_name: str) -> Dict[str, Any]:
        """
        Rename table references in diagramLayout.json.

        This file contains nodeIndex properties that reference table names for the model diagram.
        """
        if not self.current_project or not self.current_project.diagram_layout_path:
            return {"files": [], "count": 0}

        files_modified = []
        total_count = 0
        diagram_file = self.current_project.diagram_layout_path

        try:
            self._cache_file_content(diagram_file)

            with open(diagram_file, 'r', encoding='utf-8') as f:
                content = f.read()

            original_content = content
            file_count = 0

            # Pattern: "nodeIndex": "TableName"
            pattern = rf'"nodeIndex"\s*:\s*"{re.escape(old_name)}"'
            replacement = f'"nodeIndex": "{new_name}"'
            content, c = re.subn(pattern, replacement, content)
            file_count += c

            if content != original_content:
                with open(diagram_file, 'w', encoding='utf-8') as f:
                    f.write(content)
                files_modified.append(str(diagram_file))
                total_count += file_count
                logger.info(f"Updated {file_count} nodeIndex references in diagramLayout.json")

        except Exception as e:
            logger.error(f"Error updating diagramLayout.json: {e}")

        return {"files": files_modified, "count": total_count}

    def scan_broken_references(self) -> Dict[str, Any]:
        """
        Scan the project for potentially broken references.

        Compares table names in semantic model vs references in report layer.

        Returns:
            Dict with broken references found
        """
        if not self.current_project:
            return {"error": "No project loaded"}

        # Get table names from semantic model
        model_tables = set()
        for tmdl_file in self.current_project.tmdl_files:
            try:
                with open(tmdl_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                # Find table declarations
                for match in re.finditer(r"^(?:\s*)table\s+'([^']+)'", content, re.MULTILINE):
                    model_tables.add(match.group(1).replace("''", "'"))
                for match in re.finditer(r"^(?:\s*)table\s+(\w+)\s*$", content, re.MULTILINE):
                    model_tables.add(match.group(1))
            except Exception:
                pass

        # Get table references from report layer
        report_tables = set()
        broken_refs = []

        # Check visual files (PBIR Enhanced)
        for visual_file in self.current_project.visual_json_files:
            try:
                with open(visual_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                for match in re.finditer(r'"Entity"\s*:\s*"([^"]+)"', content):
                    entity = match.group(1)
                    report_tables.add(entity)
                    if entity not in model_tables:
                        broken_refs.append({
                            "file": str(visual_file),
                            "entity": entity,
                            "type": "visual"
                        })
            except Exception:
                pass

        # Check report.json (PBIR Legacy)
        if self.current_project.report_json_path:
            try:
                with open(self.current_project.report_json_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                for match in re.finditer(r'"Entity"\s*:\s*"([^"]+)"', content):
                    entity = match.group(1)
                    report_tables.add(entity)
                    if entity not in model_tables:
                        broken_refs.append({
                            "file": str(self.current_project.report_json_path),
                            "entity": entity,
                            "type": "report.json"
                        })
            except Exception:
                pass

        # Find orphaned tables (in report but not in model)
        orphaned = report_tables - model_tables

        return {
            "model_tables": list(model_tables),
            "report_tables": list(report_tables),
            "broken_references": broken_refs,
            "orphaned_table_names": list(orphaned),
            "has_broken_refs": len(broken_refs) > 0
        }

    # ==================== REPORT.JSON OPERATIONS (PBIR-Legacy) ====================

    def _rename_table_in_report_json(self, old_name: str, new_name: str) -> Dict[str, Any]:
        """
        Rename table references in report.json (PBIR-Legacy format)

        report.json contains visual field bindings like:
        - "Entity": "TableName"
        - "Property": "ColumnName"
        - queryRef patterns
        """
        if not self.current_project or not self.current_project.report_json_path:
            return {"count": 0}

        try:
            self._cache_file_content(self.current_project.report_json_path)

            with open(self.current_project.report_json_path, 'r', encoding='utf-8') as f:
                content = f.read()

            original_content = content
            count = 0

            # Pattern 1: "Entity": "TableName" (exact match for Entity field)
            pattern1 = rf'"Entity"\s*:\s*"{re.escape(old_name)}"'
            replacement1 = f'"Entity": "{new_name}"'
            content, c = re.subn(pattern1, replacement1, content)
            count += c

            # Pattern 2: "Table": "TableName"
            pattern2 = rf'"Table"\s*:\s*"{re.escape(old_name)}"'
            replacement2 = f'"Table": "{new_name}"'
            content, c = re.subn(pattern2, replacement2, content)
            count += c

            # Pattern 3: queryRef with table name (e.g., "TableName.ColumnName")
            pattern3 = rf'"{re.escape(old_name)}\.([^"]+)"'
            replacement3 = rf'"{new_name}.\1"'
            content, c = re.subn(pattern3, replacement3, content)
            count += c

            # Pattern 4: NativeReferenceName with table
            pattern4 = rf'"NativeReferenceName"\s*:\s*"{re.escape(old_name)}"'
            replacement4 = f'"NativeReferenceName": "{new_name}"'
            content, c = re.subn(pattern4, replacement4, content)
            count += c

            if content != original_content:
                with open(self.current_project.report_json_path, 'w', encoding='utf-8') as f:
                    f.write(content)
                logger.info(f"Updated {count} table references in report.json")

            return {"count": count}

        except Exception as e:
            logger.error(f"Error updating report.json: {e}")
            return {"count": 0, "error": str(e)}

    def _rename_column_in_report_json(self, table_name: str, old_name: str, new_name: str) -> Dict[str, Any]:
        """Rename column references in report.json"""
        if not self.current_project or not self.current_project.report_json_path:
            return {"count": 0}

        try:
            self._cache_file_content(self.current_project.report_json_path)

            with open(self.current_project.report_json_path, 'r', encoding='utf-8') as f:
                content = f.read()

            original_content = content
            count = 0

            # Pattern 1: Table.Column pattern in queryRef
            pattern1 = rf'"{re.escape(table_name)}\.{re.escape(old_name)}"'
            replacement1 = f'"{table_name}.{new_name}"'
            content, c = re.subn(pattern1, replacement1, content)
            count += c

            # Pattern 2: Use JSON parsing for Property fields with Entity context
            try:
                report_data = json.loads(content)
                modified = self._deep_rename_column_in_json(report_data, table_name, old_name, new_name)
                if modified > 0:
                    content = json.dumps(report_data, indent=2, ensure_ascii=False)
                    count += modified
            except json.JSONDecodeError:
                # Fall back to regex if JSON parsing fails
                pass

            if content != original_content:
                with open(self.current_project.report_json_path, 'w', encoding='utf-8') as f:
                    f.write(content)
                logger.info(f"Updated {count} column references in report.json")

            return {"count": count}

        except Exception as e:
            logger.error(f"Error updating report.json: {e}")
            return {"count": 0, "error": str(e)}

    def _deep_rename_column_in_json(self, obj: Any, table_name: str, old_name: str, new_name: str) -> int:
        """Recursively rename column references in JSON structure"""
        count = 0

        if isinstance(obj, dict):
            # Check if this dict has Entity=table_name and Property=old_name
            if obj.get("Entity") == table_name and obj.get("Property") == old_name:
                obj["Property"] = new_name
                count += 1

            # Also check for NativeReferenceName pattern
            if "NativeReferenceName" in obj:
                ref = obj["NativeReferenceName"]
                if isinstance(ref, str) and ref == f"{table_name}.{old_name}":
                    obj["NativeReferenceName"] = f"{table_name}.{new_name}"
                    count += 1

            # Recurse into nested dicts
            for key, value in obj.items():
                count += self._deep_rename_column_in_json(value, table_name, old_name, new_name)

        elif isinstance(obj, list):
            for item in obj:
                count += self._deep_rename_column_in_json(item, table_name, old_name, new_name)

        return count

    def _rename_measure_in_report_json(self, old_name: str, new_name: str) -> Dict[str, Any]:
        """Rename measure references in report.json"""
        if not self.current_project or not self.current_project.report_json_path:
            return {"count": 0}

        try:
            self._cache_file_content(self.current_project.report_json_path)

            with open(self.current_project.report_json_path, 'r', encoding='utf-8') as f:
                content = f.read()

            original_content = content
            count = 0

            # Measures are referenced similarly to columns
            # Pattern: "Property": "MeasureName"
            pattern1 = rf'"Property"\s*:\s*"{re.escape(old_name)}"'
            replacement1 = f'"Property": "{new_name}"'
            content, c = re.subn(pattern1, replacement1, content)
            count += c

            # Pattern: Table.MeasureName in queryRef
            pattern2 = rf'\.{re.escape(old_name)}"'
            replacement2 = f'.{new_name}"'
            content, c = re.subn(pattern2, replacement2, content)
            count += c

            if content != original_content:
                with open(self.current_project.report_json_path, 'w', encoding='utf-8') as f:
                    f.write(content)
                logger.info(f"Updated {count} measure references in report.json")

            return {"count": count}

        except Exception as e:
            logger.error(f"Error updating report.json: {e}")
            return {"count": 0, "error": str(e)}

    # ==================== BATCH OPERATIONS ====================

    def batch_rename_tables(self, renames: List[Dict[str, str]]) -> RenameResult:
        """
        Batch rename multiple tables in PBIP files

        Args:
            renames: List of {"old_name": "...", "new_name": "..."} dicts

        Returns:
            RenameResult with combined results
        """
        if not self.current_project:
            return RenameResult(False, "No project loaded", [], 0)

        all_files: Set[str] = set()
        total_refs = 0
        results = []
        all_validation_errors: List[ValidationError] = []
        backup_path = None

        # Create backup before any changes
        if self.auto_backup:
            backup_path = self.create_backup()

        for rename in renames:
            old_name = rename.get("old_name")
            new_name = rename.get("new_name")

            if not old_name or not new_name:
                continue

            result = self.rename_table_in_files(old_name, new_name)
            results.append(result)
            all_files.update(result.files_modified)
            total_refs += result.references_updated
            all_validation_errors.extend(result.validation_errors)

        # Final validation
        final_validation = self.validate_tmdl_syntax()
        all_validation_errors.extend(final_validation)

        success = len(all_validation_errors) == 0

        message = f"Renamed {len(renames)} table(s) in PBIP files. Updated {total_refs} references in {len(all_files)} file(s)."
        if not success:
            message += f" WARNING: {len(all_validation_errors)} validation error(s) found!"

        return RenameResult(
            success=success,
            message=message,
            files_modified=list(all_files),
            references_updated=total_refs,
            details={"individual_results": [r.__dict__ for r in results]},
            validation_errors=all_validation_errors,
            backup_created=backup_path
        )

    def batch_rename_columns(self, renames: List[Dict[str, str]]) -> RenameResult:
        """
        Batch rename multiple columns in PBIP files

        Args:
            renames: List of {"table_name": "...", "old_name": "...", "new_name": "..."} dicts

        Returns:
            RenameResult with combined results
        """
        if not self.current_project:
            return RenameResult(False, "No project loaded", [], 0)

        all_files: Set[str] = set()
        total_refs = 0
        results = []
        backup_path = None

        # Create backup before any changes
        if self.auto_backup:
            backup_path = self.create_backup()

        for rename in renames:
            table_name = rename.get("table_name")
            old_name = rename.get("old_name")
            new_name = rename.get("new_name")

            if not all([table_name, old_name, new_name]):
                continue

            result = self.rename_column_in_files(table_name, old_name, new_name)
            results.append(result)
            all_files.update(result.files_modified)
            total_refs += result.references_updated

        return RenameResult(
            success=True,
            message=f"Renamed {len(renames)} column(s) in PBIP files. Updated {total_refs} references in {len(all_files)} file(s).",
            files_modified=list(all_files),
            references_updated=total_refs,
            details={"individual_results": [r.__dict__ for r in results]},
            backup_created=backup_path
        )

    def batch_rename_measures(self, renames: List[Dict[str, str]]) -> RenameResult:
        """
        Batch rename multiple measures in PBIP files

        Args:
            renames: List of {"old_name": "...", "new_name": "..."} dicts

        Returns:
            RenameResult with combined results
        """
        if not self.current_project:
            return RenameResult(False, "No project loaded", [], 0)

        all_files: Set[str] = set()
        total_refs = 0
        results = []
        backup_path = None

        # Create backup before any changes
        if self.auto_backup:
            backup_path = self.create_backup()

        for rename in renames:
            old_name = rename.get("old_name")
            new_name = rename.get("new_name")

            if not old_name or not new_name:
                continue

            result = self.rename_measure_in_files(old_name, new_name)
            results.append(result)
            all_files.update(result.files_modified)
            total_refs += result.references_updated

        return RenameResult(
            success=True,
            message=f"Renamed {len(renames)} measure(s) in PBIP files. Updated {total_refs} references in {len(all_files)} file(s).",
            files_modified=list(all_files),
            references_updated=total_refs,
            details={"individual_results": [r.__dict__ for r in results]},
            backup_created=backup_path
        )
