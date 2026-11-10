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

Timeouts. A pipe opened this way does blocking I/O, so each call runs its exchange on a worker
thread and the caller waits at most ``timeout`` seconds. On a timeout the worker's pending read
is cancelled (CancelSynchronousIo) so the thread ends and the pipe handle is closed. If the
cancel is not possible the worker stays blocked until Desktop answers or closes the pipe; that
is bounded to ONE stuck worker per pipe, because further calls to the same pipe fail at once
with BridgeBusyError instead of queueing up behind it.
"""
import json
import logging
import os
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

PIPE_DIR = "\\\\.\\pipe"
PIPE_PREFIX = "pbi-desktop-bridge-"
DEFAULT_TIMEOUT = 60.0  # screenshots and model reloads can take a while
MAX_HEADER_BYTES = 8 * 1024             # a frame header is one or two short lines
MAX_BODY_BYTES = 512 * 1024 * 1024      # a full-page screenshot is a few megabytes of base64
MAX_SKIPPED_MESSAGES = 1000             # notifications tolerated while waiting for one response


# ---------------------------------------------------------------- framing (pure, testable)

def encode_frame(obj: Dict[str, Any]) -> bytes:
    """Encode one JSON-RPC message with Content-Length framing."""
    body = json.dumps(obj).encode("utf-8")
    return f"Content-Length: {len(body)}\r\n\r\n".encode("ascii") + body


def read_frame(stream, max_body: int = MAX_BODY_BYTES) -> Any:
    """Read one Content-Length framed JSON message from a byte stream. Raises ConnectionError
    when the stream ends early or the frame is malformed or larger than the limits allow."""
    header = b""
    while b"\r\n\r\n" not in header:
        ch = stream.read(1)
        if not ch:
            raise ConnectionError("Bridge pipe closed while reading response headers")
        header += ch
        if len(header) > MAX_HEADER_BYTES:
            raise ConnectionError("Bridge response header is too long to be a Content-Length frame")
    length = None
    for line in header.split(b"\r\n"):
        if line.lower().startswith(b"content-length:"):
            value = line.split(b":", 1)[1].strip()
            if not value.isdigit():
                raise ConnectionError(f"Bridge response has an invalid Content-Length: {value[:40]!r}")
            length = int(value)
    if length is None:
        raise ConnectionError("Bridge response missing Content-Length header")
    if length > max_body:
        raise ConnectionError(f"Bridge response of {length:,} bytes exceeds the {max_body:,} byte limit")
    body = b""
    while len(body) < length:
        chunk = stream.read(length - len(body))
        if not chunk:
            raise ConnectionError("Bridge pipe closed mid-response")
        body += chunk
    try:
        return json.loads(body.decode("utf-8"))
    except ValueError as e:
        raise ConnectionError(f"Bridge response is not valid JSON: {e}") from None


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
                # psutil 6 renamed Process.connections() to net_connections().
                connections = getattr(proc, "net_connections", None) or proc.connections
                for conn in connections(kind="tcp"):
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


class BridgeBusyError(TimeoutError):
    """An earlier call to this pipe timed out and is still blocked waiting for Desktop."""


def _cancel_blocked_io(thread: threading.Thread) -> bool:
    """Cancel the synchronous read a worker thread is blocked in, so it can exit and close
    its pipe handle. Windows only; returns False wherever that is not possible."""
    if os.name != "nt" or thread.native_id is None:
        return False
    try:
        import ctypes
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.OpenThread.restype = ctypes.c_void_p
        kernel32.OpenThread.argtypes = [ctypes.c_uint32, ctypes.c_int, ctypes.c_uint32]
        kernel32.CancelSynchronousIo.argtypes = [ctypes.c_void_p]
        kernel32.CloseHandle.argtypes = [ctypes.c_void_p]
        handle = kernel32.OpenThread(0x0001, False, thread.native_id)      # THREAD_TERMINATE
        if not handle:
            return False
        try:
            return bool(kernel32.CancelSynchronousIo(handle))
        finally:
            kernel32.CloseHandle(handle)
    except (OSError, AttributeError) as e:
        logger.debug(f"Could not cancel blocked bridge I/O: {e}")
        return False


class DesktopBridgeClient:
    """One-shot-per-call client for a Desktop Bridge pipe.

    The bridge allows a single in-flight operation; a fresh pipe handle per call keeps this
    client stateless and safe across long gaps between tool invocations. See the module
    docstring for what happens when a call times out."""

    _stuck: Dict[str, threading.Thread] = {}       # pipe -> worker still blocked after a timeout
    _stuck_lock = threading.Lock()

    def __init__(self, pipe: str, timeout: float = DEFAULT_TIMEOUT):
        self.pipe = pipe
        self.timeout = timeout
        self._id = 0

    def call(self, method: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Send one JSON-RPC request and return its ``result`` object.

        Messages that are not the response to this request (notifications, responses carrying
        another id) are skipped. A null result is returned as {}; a result that is not an
        object is returned as {"value": result}. Raises BridgeError on a JSON-RPC error,
        TimeoutError if the bridge does not answer within the timeout, BridgeBusyError if an
        earlier timed-out call to this pipe is still blocked, and ConnectionError for a
        malformed or truncated response."""
        with self._stuck_lock:
            previous = self._stuck.get(self.pipe)
            if previous is not None and previous.is_alive():
                raise BridgeBusyError(
                    f"Desktop Bridge is still busy with an earlier request that timed out; {method} "
                    "was not sent. Wait for Power BI Desktop to finish (or restart it) and retry.")
            self._stuck.pop(self.pipe, None)
        self._id += 1
        request_id = self._id
        request = {"jsonrpc": "2.0", "id": request_id, "method": method, "params": params or {}}

        result: Dict[str, Any] = {}
        error: List[BaseException] = []

        def _exchange():
            try:
                with open(self.pipe, "r+b", buffering=0) as f:
                    f.write(encode_frame(request))
                    for _ in range(MAX_SKIPPED_MESSAGES):
                        message = read_frame(f)
                        if not isinstance(message, dict):
                            raise ConnectionError("Bridge sent a message that is not a JSON-RPC object")
                        # A response to a request the bridge could not parse carries a null id.
                        if ("id" in message and message["id"] in (request_id, None)
                                and ("result" in message or "error" in message)):
                            result["response"] = message
                            return
                    raise ConnectionError(f"Bridge sent {MAX_SKIPPED_MESSAGES} messages without "
                                          f"answering {method}")
            except BaseException as e:  # surfaced on the caller thread
                error.append(e)

        worker = threading.Thread(target=_exchange, daemon=True, name=f"bridge-{method}")
        worker.start()
        worker.join(self.timeout)
        if worker.is_alive():
            _cancel_blocked_io(worker)
            worker.join(1.0)
            if worker.is_alive():
                with self._stuck_lock:
                    self._stuck[self.pipe] = worker
            raise TimeoutError(
                f"Desktop Bridge did not respond to {method} within {self.timeout:.0f}s "
                "(another operation may be running; the bridge is single-operation)")
        if error:
            raise error[0]

        response = result["response"]
        if response.get("error") is not None:
            err = response["error"] if isinstance(response["error"], dict) else {}
            code = err.get("code", -1)
            raise BridgeError(code if isinstance(code, int) else -1, str(err.get("message", "unknown")))
        value = response.get("result")
        if value is None:
            return {}
        return value if isinstance(value, dict) else {"value": value}

    # -------- typed wrappers over the documented methods

    def manifest(self) -> Dict[str, Any]:
        return self.call("bridge.manifest")

    def get_state(self) -> Dict[str, Any]:
        return self.call("application.state.get/v1")

    def capture_snapshot(self, page_id: str, scale: Optional[float] = None) -> Dict[str, Any]:
        # The manifest marks BOTH pageId and scale as required (scale is nullable but the key
        # must be present; omitting it null-refs inside Desktop). Learn's doc says optional -
        # trust the manifest.
        params: Dict[str, Any] = {"pageId": page_id,
                                  "scale": float(scale) if scale is not None else 1.0}
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
        if str(p["id"]).lower() == want:
            return p["id"]
    for p in pages:
        if str(p["display_name"]).lower() == want:
            return p["id"]
    return None
