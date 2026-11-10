"""
PBIP diagnostic utility (read-only).

    python pbip_diagnostic_tool.py <path to .pbip or project folder>
        Analyze one project: structure, TMDL validation, report bindings, and a PREVIEW of
        what the DAX quoting fix would change. Nothing is written.

    python pbip_diagnostic_tool.py
        List the .pbip files found in the usual document folders.

To apply the quoting fix, call PowerBIPBIPConnector.fix_all_dax_quoting() yourself (or use the
pbip_fix_dax_quoting tool); this script never modifies a project.
"""
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'src'))

from powerbi_pbip_connector import BACKUP_FOLDER, PowerBIPBIPConnector  # noqa: E402


def find_pbip_files(start_path: Optional[str] = None, max_depth: int = 3) -> List[Path]:
    """Find .pbip files under ``start_path`` (or the usual document folders), looking at most
    ``max_depth`` folders deep and skipping backups made by this server."""
    if start_path:
        roots = [Path(start_path)]
    else:
        roots = [
            Path.home() / "Documents",
            Path.home() / "OneDrive" / "Documents",
            Path("C:\\Users\\Public\\Documents"),
        ]
    found: List[Path] = []
    for root in roots:
        if not root.is_dir():
            continue
        base_depth = len(root.parts)
        for folder, dirs, files in os.walk(root):
            depth = len(Path(folder).parts) - base_depth
            keep = [d for d in dirs if d != BACKUP_FOLDER and not d.startswith(".")]
            dirs[:] = [] if depth >= max_depth else keep
            found.extend(Path(folder) / f for f in files if f.lower().endswith(".pbip"))
    return sorted(found)


def analyze_pbip_structure(pbip_path: str) -> Dict[str, Any]:
    """Load a project and report its structure and validation findings. Read-only."""
    connector = PowerBIPBIPConnector(auto_backup=False)
    if not connector.load_project(pbip_path):
        return {"error": f"Failed to load PBIP: {connector.last_error or pbip_path}", "pbip_path": pbip_path}
    errors = connector.validate_tmdl_syntax()
    bindings = connector.validate_report_bindings()
    by_type: Dict[str, int] = {}
    for e in errors:
        by_type[e.error_type] = by_type.get(e.error_type, 0) + 1
    return {
        "pbip_path": pbip_path,
        "project_info": connector.get_project_info(),
        "error_summary": {
            "total_errors": len(errors),
            "by_type": by_type,
            "unquoted_names": by_type.get("UNQUOTED_NAME", 0),
            "unquoted_references": by_type.get("UNQUOTED_REFERENCE", 0),
            "unquoted_dax": by_type.get("UNQUOTED_TABLE_IN_DAX", 0),
            "file_errors": by_type.get("FILE_ERROR", 0),
            "report_binding_errors": len(bindings),
        },
        "detailed_errors": [
            {"file": e.file_path, "line": e.line_number, "type": e.error_type, "message": e.message,
             "context": e.context[:100] + "..." if len(e.context) > 100 else e.context}
            for e in (errors + bindings)[:10]
        ],
        "total_detailed": len(errors) + len(bindings),
    }


def preview_dax_quoting_fix(pbip_path: str) -> Dict[str, Any]:
    """What fix_all_dax_quoting would change, computed with dry_run=True: no file is written."""
    connector = PowerBIPBIPConnector(auto_backup=False)
    if not connector.load_project(pbip_path):
        return {"error": f"Failed to load PBIP: {connector.last_error or pbip_path}", "pbip_path": pbip_path}
    before = [e for e in connector.validate_tmdl_syntax() if e.error_type == "UNQUOTED_TABLE_IN_DAX"]
    plan = connector.fix_all_dax_quoting(dry_run=True)
    return {
        "pbip_path": pbip_path,
        "files_to_modify": plan["files_modified"],
        "references_to_fix": plan["count"],
        "tables_affected": plan["tables_fixed"],
        "expressions_with_unquoted_tables": len(before),
        "errors": plan["errors"],
    }


def print_pbip_list(pbip_files: List[Path]) -> None:
    if not pbip_files:
        print("  No .pbip files found")
        return
    print(f"\nFound {len(pbip_files)} PBIP file(s):\n")
    for i, pbip in enumerate(pbip_files, 1):
        modified = datetime.fromtimestamp(pbip.stat().st_mtime).strftime("%Y-%m-%d %H:%M")
        print(f"  {i}. {pbip}")
        print(f"     Modified: {modified}\n")


def print_analysis(analysis: Dict[str, Any]) -> None:
    if "error" in analysis:
        print(f"  ERROR: {analysis['error']}")
        return
    info = analysis["project_info"]
    summary = analysis["error_summary"]
    print(f"\n  Project: {info.get('pbip_file', 'Unknown')}")
    print(f"  Report:  {info.get('report_folder') or 'none'} ({info.get('report_format')})")
    print(f"  Model:   {info.get('semantic_model_folder') or 'none (remote or missing)'}")
    print(f"  TMDL files: {info.get('tmdl_file_count', 0)}   Visuals: {info.get('visual_json_count', 0)}")
    print(f"\n  Model validation: {summary['total_errors']} finding(s)")
    for kind, count in sorted(summary["by_type"].items()):
        print(f"    - {kind}: {count}")
    print(f"  Report bindings that do not resolve: {summary['report_binding_errors']}")
    if analysis["detailed_errors"]:
        shown = analysis["detailed_errors"][:5]
        print(f"\n  First {len(shown)} of {analysis['total_detailed']}:")
        for err in shown:
            print(f"    - {err['type']}: {err['message']}")
            print(f"      {err['file']}:{err['line']}")


def print_fix_preview(result: Dict[str, Any]) -> None:
    if "error" in result:
        print(f"  ERROR: {result['error']}")
        return
    print("\n  DAX quoting fix: preview only, nothing was written")
    print(f"  Files that would change: {len(result['files_to_modify'])}")
    for f in result["files_to_modify"]:
        print(f"    - {f}")
    print(f"  References that would be quoted: {result['references_to_fix']}")
    print(f"  Tables affected: {result['tables_affected']}")
    for err in result["errors"]:
        print(f"  ERROR: {err}")


def main() -> int:
    print("\n" + "=" * 70)
    print("  PBIP DIAGNOSTIC TOOL (read-only)")
    print("=" * 70)
    if len(sys.argv) > 1:
        pbip_path = sys.argv[1]
        print(f"\nAnalyzing PBIP: {pbip_path}")
        print_analysis(analyze_pbip_structure(pbip_path))
        print("\n" + "-" * 70)
        print_fix_preview(preview_dax_quoting_fix(pbip_path))
        return 0
    print("\nSearching for PBIP files...")
    pbip_files = find_pbip_files()
    print_pbip_list(pbip_files)
    if pbip_files:
        print("Analyze one with:")
        print(f'   python pbip_diagnostic_tool.py "{pbip_files[0]}"')
    return 0


if __name__ == "__main__":
    sys.exit(main())
