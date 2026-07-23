"""
PBIX archive inspection and extraction.

A .pbix file is an OPC (ZIP) package. Most real-world reports start life as .pbix, not as a saved
.pbip project, so being able to crack one open, see what is inside, and pull out the report layout
is a key onboarding step before the rest of the PBIP/PBIR tooling can act on a report.

Pure stdlib (zipfile), cross-platform, no Power BI required. Zip-Slip protected on extraction.

Key entries:
  - DataModel              the imported VertiPaq model (opaque) -> a "thick" report
  - Connections            live-connection metadata          -> a "thin" report
  - Report/Layout          the legacy report layout (UTF-16-LE JSON)
  - definition/ ...        the modern PBIR report definition (rare inside .pbix)
  - Version, [Content_Types].xml, DiagramLayout, Settings, Metadata, SecurityBindings

Entry points:
    inspect(path)                 -> {type, report_format, entries, ...}
    read_layout(path)             -> the decoded Report/Layout as a dict (or None)
    extract(path, dest, ...)      -> {dest, files, layout_decoded}
"""
import json
import os
import zipfile
from typing import Any, Dict, List, Optional

_LAYOUT_NAMES = ("Report/Layout", "Report/layout")


def _layout_name(names: List[str]) -> Optional[str]:
    for n in _LAYOUT_NAMES:
        if n in names:
            return n
    return None


def _decode_layout_bytes(raw: bytes) -> Optional[Dict[str, Any]]:
    """The legacy layout is UTF-16-LE JSON (sometimes with a BOM). Decode tolerantly."""
    for enc in ("utf-16", "utf-16-le", "utf-8-sig", "utf-8"):
        try:
            text = raw.decode(enc)
            # strip any leading BOM/control noise before the first JSON brace
            brace = text.find("{")
            if brace > 0:
                text = text[brace:]
            return json.loads(text)
        except Exception:
            continue
    return None


def inspect(path: str) -> Dict[str, Any]:
    """Classify a .pbix and list its entries without extracting."""
    if not zipfile.is_zipfile(path):
        raise ValueError(f"Not a valid PBIX/ZIP package: {path}")
    with zipfile.ZipFile(path) as z:
        infos = z.infolist()
        names = [i.filename for i in infos]
        has_dm = "DataModel" in names
        has_conn = "Connections" in names
        layout = _layout_name(names)
        has_pbir = any(n.startswith("definition/") or n.startswith("Report/definition/") for n in names)

        report_format = ("PBIR (enhanced)" if has_pbir
                         else "legacy (Report/Layout)" if layout else "unknown")
        pbix_type = ("thick (imported model)" if has_dm
                     else "thin (live connection)" if has_conn else "unknown")

        page_count = None
        if layout:
            try:
                lj = _decode_layout_bytes(z.read(layout))
                if isinstance(lj, dict):
                    page_count = len(lj.get("sections", []) or [])
            except Exception:
                page_count = None

    return {
        "path": str(path),
        "type": pbix_type,
        "report_format": report_format,
        "has_data_model": has_dm,
        "has_connections": has_conn,
        "has_layout": bool(layout),
        "page_count": page_count,
        "entry_count": len(names),
        "entries": sorted(({"name": i.filename, "size": i.file_size} for i in infos),
                          key=lambda e: -e["size"]),
    }


def read_layout(path: str) -> Optional[Dict[str, Any]]:
    """Return the decoded legacy Report/Layout as a dict, or None if absent/undecodable."""
    if not zipfile.is_zipfile(path):
        raise ValueError(f"Not a valid PBIX/ZIP package: {path}")
    with zipfile.ZipFile(path) as z:
        layout = _layout_name(z.namelist())
        if not layout:
            return None
        return _decode_layout_bytes(z.read(layout))


def read_pbir_pages(path: str) -> List[Dict[str, Any]]:
    """List report pages of a .pbix whose report is stored in the embedded PBIR-enhanced
    format (Report/definition/pages/<id>/page.json inside the archive).
    Returns [{id, display_name, active, order}] ([] when the archive has no embedded PBIR)."""
    if not zipfile.is_zipfile(path):
        raise ValueError(f"Not a valid PBIX/ZIP package: {path}")
    pages: List[Dict[str, Any]] = []
    order: List[str] = []
    active = None
    with zipfile.ZipFile(path) as z:
        names = z.namelist()
        meta_name = "Report/definition/pages/pages.json"
        if meta_name in names:
            try:
                meta = json.loads(z.read(meta_name).decode("utf-8-sig"))
                order = list(meta.get("pageOrder", []))
                active = meta.get("activePageName")
            except Exception:
                pass
        for n in names:
            parts = n.split("/")
            if (len(parts) == 5 and parts[:3] == ["Report", "definition", "pages"]
                    and parts[4] == "page.json"):
                try:
                    data = json.loads(z.read(n).decode("utf-8-sig"))
                except Exception:
                    continue
                pid = data.get("name") or parts[3]
                pages.append({
                    "id": pid,
                    "display_name": data.get("displayName") or pid,
                    "active": pid == active,
                    "order": order.index(pid) if pid in order else None,
                })
    pages.sort(key=lambda p: (p["order"] is None, p["order"]))
    return pages


def _safe_target(dest: str, name: str) -> str:
    """Resolve an archive member to a path inside dest, rejecting Zip-Slip traversal."""
    dest_abs = os.path.abspath(dest)
    target = os.path.abspath(os.path.join(dest_abs, name))
    if target != dest_abs and not target.startswith(dest_abs + os.sep):
        raise ValueError(f"Unsafe path in archive (path traversal): {name}")
    return target


def extract(path: str, dest: str, decode_layout: bool = True) -> Dict[str, Any]:
    """Extract every member into dest (Zip-Slip protected). When decode_layout is set and a
    legacy layout exists, also write a UTF-8 'Report/Layout.json' for easy inspection/editing."""
    if not zipfile.is_zipfile(path):
        raise ValueError(f"Not a valid PBIX/ZIP package: {path}")
    os.makedirs(dest, exist_ok=True)
    written: List[str] = []
    with zipfile.ZipFile(path) as z:
        for info in z.infolist():
            if info.is_dir():
                continue
            target = _safe_target(dest, info.filename)
            os.makedirs(os.path.dirname(target), exist_ok=True)
            with z.open(info) as src, open(target, "wb") as out:
                out.write(src.read())
            written.append(info.filename)

        layout_decoded = False
        if decode_layout:
            layout = _layout_name(z.namelist())
            if layout:
                lj = _decode_layout_bytes(z.read(layout))
                if lj is not None:
                    out_path = _safe_target(dest, "Report/Layout.json")
                    os.makedirs(os.path.dirname(out_path), exist_ok=True)
                    with open(out_path, "w", encoding="utf-8") as f:
                        json.dump(lj, f, indent=2, ensure_ascii=False)
                    layout_decoded = True

    return {"dest": str(dest), "files": written, "file_count": len(written),
            "layout_decoded": layout_decoded}
