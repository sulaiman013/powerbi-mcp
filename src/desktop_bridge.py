"""
Power BI Desktop Bridge client (preview feature, mid-2026).

The Desktop Bridge is a JSON-RPC 2.0 server that runs INSIDE the PBIDesktop.exe process and
listens on a local named pipe `pbi-desktop-bridge-{processId}` (Content-Length framed, local
only, one operation at a time; each open Desktop window has its own pipe). It requires the
Desktop preview option "Enable external tool access to Power BI Desktop through secure local
APIs" (on by default).

Documented methods (bridge.manifest reports what a given build supports):
  - bridge.manifest              method discovery
  - application.state.get/v1     current file path + unsaved-changes flag
  - report.snapshot.capture/v1   PNG screenshot of one report page
  - file.reload/v1               hot-reload the open PBIP/PBIR from disk (no restart)

This client is pure Python: Windows named pipes are opened with plain open() - no pywin32.
Reference: learn.microsoft.com/power-bi/developer/agentic/power-bi-desktop-bridge-overview
"""
import json
import logging
import os
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

PIPE_DIR = "\\\\.\\pipe"
PIPE_PREFIX = "pbi-desktop-bridge-"
DEFAULT_TIMEOUT = 60.0  # screenshots and model reloads can take a while


# ---------------------------------------------------------------- framing (pure, testable)

def encode_frame(obj: Dict[str, Any]) -> bytes:
    """Encode one JSON-RPC message with Content-Length framing."""
    body = json.dumps(obj).encode("utf-8")
    return f"Content-Length: {len(body)}\r\n\r\n".encode("ascii") + body


def read_frame(stream) -> Dict[str, Any]:
    """Read one Content-Length framed JSON message from a byte stream."""
    header = b""
    while b"\r\n\r\n" not in header:
        ch = stream.read(1)
        if not ch:
            raise ConnectionError("Bridge pipe closed while reading response headers")
        header += ch
    length = None
    for line in header.split(b"\r\n"):
        if line.lower().startswith(b"content-length:"):
            length = int(line.split(b":", 1)[1].strip())
    if length is None:
        raise ConnectionError("Bridge response missing Content-Length header")
    body = b""
    while len(body) < length:
        chunk = stream.read(length - len(body))
        if not chunk:
            raise ConnectionError("Bridge pipe closed mid-response")
        body += chunk
    return json.loads(body.decode("utf-8"))


# ---------------------------------------------------------------- discovery

def discover_bridges() -> List[Dict[str, Any]]:
    """Find running Desktop Bridge pipes. Returns [{pid, pipe}] (one per Desktop window)."""
    out: List[Dict[str, Any]] = []
    try:
        pipes = os.listdir(PIPE_DIR + "\\")
    except OSError:
        return out
    for name in pipes:
        if name.startswith(PIPE_PREFIX):
            suffix = name[len(PIPE_PREFIX):]
            pid = int(suffix) if suffix.isdigit() else None
            out.append({"pid": pid, "pipe": PIPE_DIR + "\\" + name})
    return out


def msmdsrv_port_for_desktop_pid(desktop_pid: int) -> Optional[int]:
    """Find the local Analysis Services (msmdsrv) port belonging to a PBIDesktop process, so
    a bridge instance can be chained straight into desktop_connect for DAX/TOM work.
    Matches msmdsrv children of the Desktop pid and reads their listening port."""
    try:
        import psutil
        for proc in psutil.process_iter(["name", "ppid"]):
            try:
                if (proc.info["name"] or "").lower() != "msmdsrv.exe":
                    continue
                if proc.info["ppid"] != desktop_pid:
                    continue
                for conn in proc.net_connections(kind="tcp"):
                    if conn.status == psutil.CONN_LISTEN and conn.laddr:
                        return conn.laddr.port
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
    except Exception as e:
        logger.debug(f"msmdsrv port correlation failed: {e}")
    return None


# ---------------------------------------------------------------- client

class BridgeError(Exception):
    """A JSON-RPC error returned by the Desktop Bridge."""

    def __init__(self, code: int, message: str):
        super().__init__(f"Bridge error {code}: {message}")
        self.code = code
        self.message = message


class DesktopBridgeClient:
    """One-shot-per-call client for a Desktop Bridge pipe.

    The bridge allows a single in-flight operation; a fresh pipe handle per call keeps this
    client stateless and safe across long gaps between tool invocations."""

    def __init__(self, pipe: str, timeout: float = DEFAULT_TIMEOUT):
        self.pipe = pipe
        self.timeout = timeout
        self._id = 0

    def call(self, method: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Send one JSON-RPC request and return the `result`. Raises BridgeError on a JSON-RPC
        error and TimeoutError if the bridge does not answer within the timeout."""
        self._id += 1
        request = {"jsonrpc": "2.0", "id": self._id, "method": method, "params": params or {}}

        result: Dict[str, Any] = {}
        error: List[BaseException] = []

        def _exchange():
            try:
                with open(self.pipe, "r+b", buffering=0) as f:
                    f.write(encode_frame(request))
                    result["response"] = read_frame(f)
            except BaseException as e:  # surfaced on the caller thread
                error.append(e)

        worker = threading.Thread(target=_exchange, daemon=True)
        worker.start()
        worker.join(self.timeout)
        if worker.is_alive():
            raise TimeoutError(
                f"Desktop Bridge did not respond to {method} within {self.timeout:.0f}s "
                "(another operation may be running; the bridge is single-operation)")
        if error:
            raise error[0]

        response = result.get("response", {})
        if "error" in response:
            err = response["error"] or {}
            raise BridgeError(int(err.get("code", -1)), str(err.get("message", "unknown")))
        return response.get("result", {})

    # -------- typed wrappers over the documented methods

    def manifest(self) -> Dict[str, Any]:
        return self.call("bridge.manifest")

    def get_state(self) -> Dict[str, Any]:
        return self.call("application.state.get/v1")

    def capture_snapshot(self, page_id: str, scale: Optional[float] = None) -> Dict[str, Any]:
        params: Dict[str, Any] = {"pageId": page_id}
        if scale is not None:
            params["scale"] = float(scale)
        return self.call("report.snapshot.capture/v1", params)

    def reload_file(self, reload_model_definition: bool = True) -> Dict[str, Any]:
        return self.call("file.reload/v1", {"reloadModelDefinition": bool(reload_model_definition)})


# ---------------------------------------------------------------- PBIR page resolution

def pbir_pages_for_file(current_file_path: str) -> List[Dict[str, Any]]:
    """List the PBIR pages (name/id + displayName + order) for an open .pbip file, read from
    the sibling Report/definition/pages folder. Returns [] for .pbix or non-PBIR projects."""
    if not current_file_path or not current_file_path.lower().endswith(".pbip"):
        return []
    pbip = Path(current_file_path)
    pages_dir = pbip.parent / f"{pbip.stem}.Report" / "definition" / "pages"
    if not pages_dir.is_dir():
        return []
    order: List[str] = []
    active = None
    meta_path = pages_dir / "pages.json"
    if meta_path.exists():
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8-sig"))
            order = list(meta.get("pageOrder", []))
            active = meta.get("activePageName")
        except Exception:
            pass
    pages: List[Dict[str, Any]] = []
    for page_dir in sorted(p for p in pages_dir.iterdir() if p.is_dir()):
        pj = page_dir / "page.json"
        if not pj.exists():
            continue
        try:
            data = json.loads(pj.read_text(encoding="utf-8-sig"))
        except Exception:
            continue
        name = data.get("name") or page_dir.name
        pages.append({
            "id": name,
            "display_name": data.get("displayName") or name,
            "active": name == active,
            "order": order.index(name) if name in order else None,
        })
    pages.sort(key=lambda p: (p["order"] is None, p["order"]))
    return pages


def pages_for_file(current_file_path: str) -> List[Dict[str, Any]]:
    """List report pages for the open file: PBIR folder for .pbip, embedded legacy Layout for
    .pbix (via pbix_tools). Returns [{id, display_name, active, order}]."""
    pages = pbir_pages_for_file(current_file_path)
    if pages:
        return pages
    if current_file_path and current_file_path.lower().endswith(".pbix"):
        try:
            import pbix_tools
            # Modern .pbix: PBIR-enhanced report embedded in the archive.
            embedded = pbix_tools.read_pbir_pages(current_file_path)
            if embedded:
                return embedded
            # Legacy .pbix: single UTF-16 Report/Layout with sections.
            layout = pbix_tools.read_layout(current_file_path)
            sections = (layout or {}).get("sections", [])
            return [{"id": s.get("name") or f"section{i}",
                     "display_name": s.get("displayName") or s.get("name") or f"Page {i + 1}",
                     "active": False, "order": i}
                    for i, s in enumerate(sections)]
        except Exception as e:
            logger.warning(f"Could not read pages from pbix: {e}")
    return []


def resolve_page_id(pages: List[Dict[str, Any]], ref: str) -> Optional[str]:
    """Resolve a page reference (internal id or display name, case-insensitive) to the id."""
    want = (ref or "").strip().lower()
    for p in pages:
        if p["id"].lower() == want:
            return p["id"]
    for p in pages:
        if p["display_name"].lower() == want:
            return p["id"]
    return None
