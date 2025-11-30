"""
Power BI PBIP (Power BI Project) Connector
Provides file-based editing for PBIP format to safely rename tables, columns, measures
without breaking report visuals.

PBIP Structure:
  project.pbip
  ProjectName.SemanticModel/
    definition.tmdl (or model.tmd)
    definition/
      tables/*.tmdl
      relationships.tmdl
      etc.
  ProjectName.Report/
    report.json  <- Contains visual field bindings
    definition.pbir
"""
import json
import logging
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class PBIPProject:
    """Represents a PBIP project structure"""
    root_path: Path
    pbip_file: Path
    semantic_model_folder: Optional[Path]
    report_folder: Optional[Path]
    report_json_path: Optional[Path]
    tmdl_files: List[Path]


@dataclass
class RenameResult:
    """Result of a PBIP rename operation"""
    success: bool
    message: str
    files_modified: List[str]
    references_updated: int
    details: Optional[Dict[str, Any]] = None


class PowerBIPBIPConnector:
    """
    PBIP Connector for file-based Power BI Project editing

    Enables safe bulk renames by editing:
    - TMDL files (semantic model definitions)
    - report.json (visual field bindings)
    """

    def __init__(self):
        self.current_project: Optional[PBIPProject] = None

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
        """Parse a PBIP project structure"""
        try:
            root = pbip_path.parent

            # Find semantic model folder (.SemanticModel)
            semantic_folders = list(root.glob("*.SemanticModel"))
            semantic_model_folder = semantic_folders[0] if semantic_folders else None

            # Find report folder (.Report)
            report_folders = list(root.glob("*.Report"))
            report_folder = report_folders[0] if report_folders else None

            # Find report.json
            report_json_path = None
            if report_folder:
                report_json = report_folder / "report.json"
                if report_json.exists():
                    report_json_path = report_json

            # Find all TMDL files
            tmdl_files = []
            if semantic_model_folder:
                tmdl_files = list(semantic_model_folder.glob("**/*.tmdl"))
                tmdl_files.extend(semantic_model_folder.glob("**/*.tmd"))

            return PBIPProject(
                root_path=root,
                pbip_file=pbip_path,
                semantic_model_folder=semantic_model_folder,
                report_folder=report_folder,
                report_json_path=report_json_path,
                tmdl_files=tmdl_files
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
            "has_report": self.current_project.report_json_path is not None
        }

    # ==================== RENAME OPERATIONS ====================

    def rename_table_in_files(self, old_name: str, new_name: str) -> RenameResult:
        """
        Rename a table across all PBIP files (TMDL + report.json)

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

        # 1. Update TMDL files
        tmdl_replacements = self._rename_in_tmdl_files(old_name, new_name, "table")
        files_modified.extend(tmdl_replacements["files"])
        total_replacements += tmdl_replacements["count"]

        # 2. Update report.json
        if self.current_project.report_json_path:
            report_replacements = self._rename_table_in_report_json(old_name, new_name)
            if report_replacements["count"] > 0:
                files_modified.append(str(self.current_project.report_json_path))
                total_replacements += report_replacements["count"]

        return RenameResult(
            success=True,
            message=f"Renamed table '{old_name}' to '{new_name}' in {len(files_modified)} file(s)",
            files_modified=files_modified,
            references_updated=total_replacements,
            details={"old_name": old_name, "new_name": new_name}
        )

    def rename_column_in_files(self, table_name: str, old_name: str, new_name: str) -> RenameResult:
        """
        Rename a column across all PBIP files

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

        # 1. Update TMDL files
        tmdl_replacements = self._rename_column_in_tmdl_files(table_name, old_name, new_name)
        files_modified.extend(tmdl_replacements["files"])
        total_replacements += tmdl_replacements["count"]

        # 2. Update report.json
        if self.current_project.report_json_path:
            report_replacements = self._rename_column_in_report_json(table_name, old_name, new_name)
            if report_replacements["count"] > 0:
                files_modified.append(str(self.current_project.report_json_path))
                total_replacements += report_replacements["count"]

        return RenameResult(
            success=True,
            message=f"Renamed column '{table_name}'[{old_name}] to [{new_name}] in {len(files_modified)} file(s)",
            files_modified=files_modified,
            references_updated=total_replacements,
            details={"table_name": table_name, "old_name": old_name, "new_name": new_name}
        )

    def rename_measure_in_files(self, old_name: str, new_name: str) -> RenameResult:
        """
        Rename a measure across all PBIP files

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

        # 1. Update TMDL files
        tmdl_replacements = self._rename_measure_in_tmdl_files(old_name, new_name)
        files_modified.extend(tmdl_replacements["files"])
        total_replacements += tmdl_replacements["count"]

        # 2. Update report.json
        if self.current_project.report_json_path:
            report_replacements = self._rename_measure_in_report_json(old_name, new_name)
            if report_replacements["count"] > 0:
                files_modified.append(str(self.current_project.report_json_path))
                total_replacements += report_replacements["count"]

        return RenameResult(
            success=True,
            message=f"Renamed measure '{old_name}' to '{new_name}' in {len(files_modified)} file(s)",
            files_modified=files_modified,
            references_updated=total_replacements,
            details={"old_name": old_name, "new_name": new_name}
        )

    # ==================== TMDL FILE OPERATIONS ====================

    def _rename_in_tmdl_files(self, old_name: str, new_name: str, object_type: str) -> Dict[str, Any]:
        """Rename references in TMDL files"""
        files_modified = []
        total_count = 0

        if not self.current_project or not self.current_project.tmdl_files:
            return {"files": files_modified, "count": total_count}

        # Patterns for table references in TMDL/DAX
        patterns = [
            (rf"'{re.escape(old_name)}'\s*\[", f"'{new_name}'["),  # 'TableName'[Column]
            (rf"(?<!['\w]){re.escape(old_name)}(?=\s*\[)", new_name),  # TableName[Column]
            (rf"'{re.escape(old_name)}'(?=\s*[,\)\]])", f"'{new_name}'"),  # 'TableName' in functions
            (rf'^table\s+{re.escape(old_name)}\s*$', f'table {new_name}', re.MULTILINE),  # TMDL table definition
        ]

        for tmdl_file in self.current_project.tmdl_files:
            try:
                with open(tmdl_file, 'r', encoding='utf-8') as f:
                    content = f.read()

                original_content = content
                file_count = 0

                for pattern_tuple in patterns:
                    if len(pattern_tuple) == 3:
                        pattern, replacement, flags = pattern_tuple
                        content, count = re.subn(pattern, replacement, content, flags=flags)
                    else:
                        pattern, replacement = pattern_tuple
                        content, count = re.subn(pattern, replacement, content, flags=re.IGNORECASE)
                    file_count += count

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
        """Rename column references in TMDL files"""
        files_modified = []
        total_count = 0

        if not self.current_project or not self.current_project.tmdl_files:
            return {"files": files_modified, "count": total_count}

        # Patterns for column references
        patterns = [
            (rf"'{re.escape(table_name)}'\s*\[\s*{re.escape(old_name)}\s*\]", f"'{table_name}'[{new_name}]"),
            (rf"(?<!['\w]){re.escape(table_name)}\s*\[\s*{re.escape(old_name)}\s*\]", f"{table_name}[{new_name}]"),
            (rf'^(\s*)column\s+{re.escape(old_name)}\s*$', rf'\1column {new_name}', re.MULTILINE),  # TMDL column definition
        ]

        for tmdl_file in self.current_project.tmdl_files:
            try:
                with open(tmdl_file, 'r', encoding='utf-8') as f:
                    content = f.read()

                original_content = content
                file_count = 0

                for pattern_tuple in patterns:
                    if len(pattern_tuple) == 3:
                        pattern, replacement, flags = pattern_tuple
                        content, count = re.subn(pattern, replacement, content, flags=flags)
                    else:
                        pattern, replacement = pattern_tuple
                        content, count = re.subn(pattern, replacement, content, flags=re.IGNORECASE)
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
        """Rename measure references in TMDL files"""
        files_modified = []
        total_count = 0

        if not self.current_project or not self.current_project.tmdl_files:
            return {"files": files_modified, "count": total_count}

        # Patterns for measure references
        patterns = [
            (rf"\[\s*{re.escape(old_name)}\s*\]", f"[{new_name}]"),  # [MeasureName]
            (rf'^(\s*)measure\s+{re.escape(old_name)}\s*=', rf'\1measure {new_name} =', re.MULTILINE),  # TMDL measure definition
        ]

        for tmdl_file in self.current_project.tmdl_files:
            try:
                with open(tmdl_file, 'r', encoding='utf-8') as f:
                    content = f.read()

                original_content = content
                file_count = 0

                for pattern_tuple in patterns:
                    if len(pattern_tuple) == 3:
                        pattern, replacement, flags = pattern_tuple
                        content, count = re.subn(pattern, replacement, content, flags=flags)
                    else:
                        pattern, replacement = pattern_tuple
                        content, count = re.subn(pattern, replacement, content, flags=re.IGNORECASE)
                    file_count += count

                if content != original_content:
                    with open(tmdl_file, 'w', encoding='utf-8') as f:
                        f.write(content)
                    files_modified.append(str(tmdl_file))
                    total_count += file_count

            except Exception as e:
                logger.error(f"Error updating TMDL file {tmdl_file}: {e}")

        return {"files": files_modified, "count": total_count}

    # ==================== REPORT.JSON OPERATIONS ====================

    def _rename_table_in_report_json(self, old_name: str, new_name: str) -> Dict[str, Any]:
        """
        Rename table references in report.json

        report.json contains visual field bindings like:
        - "Entity": "TableName"
        - "Property": "ColumnName"
        - queryRef patterns
        """
        if not self.current_project or not self.current_project.report_json_path:
            return {"count": 0}

        try:
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
            with open(self.current_project.report_json_path, 'r', encoding='utf-8') as f:
                content = f.read()

            original_content = content
            count = 0

            # Pattern 1: "Property": "ColumnName" within same object that has "Entity": "TableName"
            # This is complex, so we do a simpler approach: replace Table.Column pattern
            pattern1 = rf'"{re.escape(table_name)}\.{re.escape(old_name)}"'
            replacement1 = f'"{table_name}.{new_name}"'
            content, c = re.subn(pattern1, replacement1, content)
            count += c

            # Pattern 2: Column reference with Property field
            # We need to be careful here - only replace if it's for the right table
            # For safety, we also look for standalone Property references
            pattern2 = rf'"Property"\s*:\s*"{re.escape(old_name)}"'
            # This is risky without context, so we'll be conservative
            # Only replace if we find Entity:TableName nearby (within 200 chars before)

            # Use a more sophisticated approach with JSON parsing
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

        all_files = set()
        total_refs = 0
        results = []

        for rename in renames:
            old_name = rename.get("old_name")
            new_name = rename.get("new_name")

            if not old_name or not new_name:
                continue

            result = self.rename_table_in_files(old_name, new_name)
            results.append(result)
            all_files.update(result.files_modified)
            total_refs += result.references_updated

        return RenameResult(
            success=True,
            message=f"Renamed {len(renames)} table(s) in PBIP files. Updated {total_refs} references in {len(all_files)} file(s).",
            files_modified=list(all_files),
            references_updated=total_refs,
            details={"individual_results": [r.__dict__ for r in results]}
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

        all_files = set()
        total_refs = 0
        results = []

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
            details={"individual_results": [r.__dict__ for r in results]}
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

        all_files = set()
        total_refs = 0
        results = []

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
            details={"individual_results": [r.__dict__ for r in results]}
        )
