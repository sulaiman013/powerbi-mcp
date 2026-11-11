"""
Private custom visual packages (.pbiviz) for PBIR reports.

A .pbiviz is a zip holding package.json and resources/<guid>.pbiviz.json. A report that embeds
the visual keeps those files, unpacked, under <Report>/CustomVisuals/<guid>/ and lists the guid
in report.json resourcePackages (see pbir_builders.custom_visual_package). This module reads a
package and works out where its files go. It only reads; report_spec.py does the writing.
"""
import json
import os
import zipfile
from typing import Any, Dict, Iterable, List

# Desktop silently drops a custom visual whose unpacked files sit on a path Windows cannot
# open, and the metadata file name repeats the guid, so a deep project folder gets there fast.
# 250 leaves room under the 260-character limit for the temp names used while writing.
WINDOWS_PATH_LIMIT = 250


class PbivizError(ValueError):
    """The package is missing, unreadable, or not shaped like a .pbiviz."""


def read_pbiviz(path: str) -> Dict[str, Any]:
    """Read a .pbiviz. Returns {"guid", "name", "version", "files": {relative path: bytes}}.

    Paths use forward slashes and are relative to the package root. Members that would land
    outside the target folder are refused rather than skipped: a package like that is not one
    to unpack at all.
    """
    if not os.path.isfile(path):
        raise PbivizError(f"custom visual package not found: {path}")
    try:
        with zipfile.ZipFile(path) as z:
            files: Dict[str, bytes] = {}
            for info in z.infolist():
                if info.is_dir():
                    continue
                name = info.filename.replace("\\", "/")
                parts = name.split("/")
                if name.startswith("/") or ":" in parts[0] or any(p in ("", "..") for p in parts):
                    raise PbivizError(f"{os.path.basename(path)} has an unsafe member path: "
                                      f"{info.filename!r}")
                files[name] = z.read(info)
    except zipfile.BadZipFile as e:
        raise PbivizError(f"{os.path.basename(path)} is not a zip archive: {e}") from e
    if "package.json" not in files:
        raise PbivizError(f"{os.path.basename(path)} has no package.json, so it is not a .pbiviz")
    try:
        package = json.loads(files["package.json"].decode("utf-8-sig"))
        visual = package["visual"]
        guid = str(visual["guid"])
    except (ValueError, KeyError, TypeError) as e:
        raise PbivizError(f"{os.path.basename(path)}: package.json does not name the visual's guid "
                          f"({e})") from e
    if not guid or any(ch in guid for ch in '/\\:*?"<>|. '):
        raise PbivizError(f"{os.path.basename(path)}: the guid {guid!r} cannot be a folder name")
    meta = f"resources/{guid}.pbiviz.json"
    if meta not in files:
        raise PbivizError(f"{os.path.basename(path)} has no {meta}; report.json could not reference "
                          "the visual")
    return {"guid": guid, "name": str(visual.get("name", guid)), "version": str(visual.get("version", "")),
            "files": files}


def read_unpacked(folder: str) -> Dict[str, Any]:
    """Read a visual a report keeps unpacked (<Report>/CustomVisuals/<guid>/). Returns what
    read_pbiviz returns for the archive those files came from."""
    manifest = os.path.join(folder, "package.json")
    if not os.path.isfile(manifest):
        raise PbivizError(f"{folder} has no package.json, so it is not an unpacked custom visual")
    with open(manifest, "rb") as f:
        files: Dict[str, bytes] = {"package.json": f.read()}
    try:
        visual = json.loads(files["package.json"].decode("utf-8-sig"))["visual"]
        guid = str(visual["guid"])
    except (ValueError, KeyError, TypeError) as e:
        raise PbivizError(f"{manifest}: package.json does not name the visual's guid ({e})") from e
    meta = f"resources/{guid}.pbiviz.json"
    path = os.path.join(folder, "resources", f"{guid}.pbiviz.json")
    if not guid or any(ch in guid for ch in '/\\:*?"<>|. ') or not os.path.isfile(path):
        raise PbivizError(f"{folder} has no {meta}")
    with open(path, "rb") as f:
        files[meta] = f.read()
    return {"guid": guid, "name": str(visual.get("name", guid)), "version": str(visual.get("version", "")),
            "files": files}


def custom_visual_files(guid: str, files: Dict[str, bytes]) -> Dict[str, bytes]:
    """Where a package's files go inside the report folder: {"CustomVisuals/<guid>/<member>": bytes}."""
    return {f"CustomVisuals/{guid}/{rel}": data for rel, data in files.items()}


def path_length_problems(report_dir: str, relative_paths: Iterable[str],
                         limit: int = WINDOWS_PATH_LIMIT) -> List[str]:
    """Relative paths whose full path under ``report_dir`` is ``limit`` characters or longer."""
    base = os.path.abspath(report_dir)
    return [rel for rel in relative_paths if len(os.path.join(base, *rel.split("/"))) >= limit]
