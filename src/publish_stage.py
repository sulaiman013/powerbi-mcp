"""
Stage a PBIP report (and optionally its model) for publishing to the Power BI service.

A local PBIP report points at its model by folder path (definition.pbir "byPath"). The
service needs the report bound to a published semantic model instead ("byConnection").
Rewriting that binding in a COPY of the report, and importing the copy over an existing
report item, publishes a new version in place: the report keeps its id, so shared and public
links keep working.

This module only prepares the folder. It never talks to the service and never changes the
source project; the import itself is one Fabric CLI command, returned in the result.
"""
import re
import shutil
from pathlib import Path
from typing import Any, Dict, List, Optional

import safe_io

_GUID_RE = re.compile(r"^[0-9a-fA-F]{8}-(?:[0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}$")
_PBIR_SCHEMA = ("https://developer.microsoft.com/json-schemas/fabric/item/report/"
                "definitionProperties/2.0.0/schema.json")
# M functions that read from the local machine. A model whose partitions use them cannot be
# refreshed by the service without a gateway.
_LOCAL_SOURCE_RE = re.compile(r"\b(File\.Contents|Folder\.Files|Folder\.Contents|Access\.Database)\s*\(")
# Local caches and per-user settings Desktop keeps beside a project; never part of a definition.
_IGNORE = shutil.ignore_patterns(".pbi", "*.abf", "localSettings.json", "editorSettings.json", "cache.abf")


class StageError(ValueError):
    """The request cannot be staged as given (bad argument or unsafe destination)."""


def _quoted(value: str) -> str:
    """A connection-string value in double quotes, embedded quotes doubled."""
    if any(ord(c) < 32 for c in value):
        raise StageError("names must not contain control characters")
    return '"' + value.replace('"', '""') + '"'


def build_connection_string(workspace: str, model_name: str, model_id: str) -> str:
    """The byConnection string for a published semantic model."""
    if not workspace or not model_name:
        raise StageError("workspace and model_name are required")
    if not _GUID_RE.match(model_id or ""):
        raise StageError(f"model_id must be the semantic model's GUID, got {model_id!r}")
    return (f"Data Source={_quoted('powerbi://api.powerbi.com/v1.0/myorg/' + workspace)};"
            f"initial catalog={_quoted(model_name)};integrated security=ClaimsToken;"
            f"semanticmodelid={model_id.lower()}")


def _item_name(folder: Path, suffix: str) -> str:
    """'Sales.Report' -> 'Sales'."""
    return folder.name[:-len(suffix)] if folder.name.endswith(suffix) else folder.name


def _check_destination(source: Path, target: Path, overwrite: bool) -> None:
    src, dst = source.resolve(), target.resolve()
    if dst == src or src in dst.parents or dst in src.parents:
        raise StageError("the stage folder must be outside the source project folder")
    if dst.exists():
        if not overwrite:
            raise StageError(f"{dst} already exists; pass overwrite=true to replace it")
        if not (dst.is_dir() and dst.name.endswith((".Report", ".SemanticModel"))):
            raise StageError(f"refusing to replace {dst}: it is not a staged item folder")
        shutil.rmtree(dst)


def _set_display_name(item_dir: Path, display_name: Optional[str]) -> None:
    platform = item_dir / ".platform"
    if not display_name or not platform.is_file():
        return
    data = safe_io.read_json(platform)
    data.setdefault("metadata", {})["displayName"] = display_name
    safe_io.write_json(platform, data)


def stage_report(report_dir: str, stage_dir: str, *, workspace: str, model_name: str, model_id: str,
                 display_name: Optional[str] = None, overwrite: bool = False) -> Dict[str, Any]:
    """Copy ``report_dir`` (a ``*.Report`` folder) into ``stage_dir`` bound byConnection to a
    published model. Returns the staged path, the command that imports it, and warnings."""
    source = Path(report_dir)
    if not (source.is_dir() and (source / "definition.pbir").is_file()):
        raise StageError(f"{report_dir} is not a PBIP report folder (no definition.pbir)")
    connection = build_connection_string(workspace, model_name, model_id)
    item_name = display_name or _item_name(source, ".Report")
    target = Path(stage_dir) / f"{item_name}.Report"
    _check_destination(source, target, overwrite)

    original = safe_io.read_json(source / "definition.pbir")
    shutil.copytree(source, target, ignore=_IGNORE)
    binding = {
        "$schema": original.get("$schema") or _PBIR_SCHEMA,
        "version": original.get("version") or "4.0",
        "datasetReference": {"byConnection": {"connectionString": connection}},
    }
    safe_io.write_json(target / "definition.pbir", binding, safe_io.DEFAULT_FORMAT)
    _set_display_name(target, display_name)

    warnings: List[str] = []
    custom_dir = target / "CustomVisuals"
    if custom_dir.is_dir() and any(custom_dir.iterdir()):
        warnings.append("The report embeds private custom visuals; a tenant that only allows certified "
                        "visuals will not render them.")
    was = original.get("datasetReference") or {}
    if "byConnection" in was:
        warnings.append("The source report was already bound byConnection; the binding was replaced.")
    files = [p for p in target.rglob("*") if p.is_file()]
    return {
        "staged_path": str(target),
        "item_name": item_name,
        "connection_string": connection,
        "file_count": len(files),
        "import_command": f'fab import "{workspace}.Workspace/{item_name}.Report" -i "{target}" -f',
        "warnings": warnings,
    }


def stage_model(model_dir: str, stage_dir: str, *, display_name: Optional[str] = None,
                overwrite: bool = False) -> Dict[str, Any]:
    """Copy ``model_dir`` (a ``*.SemanticModel`` folder) into ``stage_dir`` without local
    caches, and report anything that would stop the service refreshing it."""
    source = Path(model_dir)
    if not (source.is_dir() and (source / "definition.pbism").is_file()):
        raise StageError(f"{model_dir} is not a PBIP semantic model folder (no definition.pbism)")
    item_name = display_name or _item_name(source, ".SemanticModel")
    target = Path(stage_dir) / f"{item_name}.SemanticModel"
    _check_destination(source, target, overwrite)
    shutil.copytree(source, target, ignore=_IGNORE)
    _set_display_name(target, display_name)

    warnings: List[str] = []
    local_sources = []
    for tmdl in sorted(target.rglob("*.tmdl")):
        text, _ = safe_io.read_text(tmdl)
        if _LOCAL_SOURCE_RE.search(text):
            local_sources.append(tmdl.relative_to(target).as_posix())
    if local_sources:
        warnings.append("These files read data from the local machine, so the service cannot refresh the "
                        "model without a gateway (embed the data first, e.g. with pbip_embed_data): "
                        + ", ".join(local_sources[:12]) + (" ..." if len(local_sources) > 12 else ""))
    warnings.append("Importing a model definition uploads no data. Query a row count afterwards and "
                    "refresh once if the tables are empty.")
    files = [p for p in target.rglob("*") if p.is_file()]
    return {
        "staged_path": str(target),
        "item_name": item_name,
        "file_count": len(files),
        "local_sources": local_sources,
        "import_command": f'fab import "<workspace>.Workspace/{item_name}.SemanticModel" -i "{target}" -f',
        "warnings": warnings,
    }


def describe(result: Dict[str, Any]) -> str:
    """A short human-readable summary of a stage result."""
    lines = [f"Staged {result['file_count']} file(s) at {result['staged_path']}"]
    if result.get("connection_string"):
        lines.append(f"Bound to: {result['connection_string']}")
    lines.append(f"To publish: {result['import_command']}")
    lines.extend(f"Warning: {w}" for w in result.get("warnings", []))
    return "\n".join(lines)


__all__ = ["StageError", "build_connection_string", "stage_report", "stage_model", "describe"]
