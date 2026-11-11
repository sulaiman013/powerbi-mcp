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
  - report.snapshot.capture/v2   the same, optionally of one rectangle of the page (later builds)
  - file.reload/v1               hot-reload the open PBIP/PBIR from disk (no restart)

The argument shape changed between builds. The first preview builds took a method's arguments
directly as the JSON-RPC params; Desktop 2.159 (late 2026) takes {"client", "clientActivityId",
"args": {...}} and answers "Request arguments are required" (-32602) to the earlier shape. The
manifest carries each method's parameter schema, so the client reads it and sends what the
running build declares (see DesktopBridgeClient.invoke).

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
import stat
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

logger = logging.getLogger(__name__)

PIPE_DIR = "\\\\.\\pipe"
PIPE_PREFIX = "pbi-desktop-bridge-"
DEFAULT_TIMEOUT = 60.0  # screenshots and model reloads can take a while
# Desktop refuses a project file at or beyond these lengths ("The fully qualified file name
# must be less than 260 characters, and the directory name must be less than 248 characters").
MAX_FILE_PATH = 260
MAX_DIR_PATH = 248
MAX_HEADER_BYTES = 8 * 1024             # a frame header is one or two short lines
MAX_BODY_BYTES = 512 * 1024 * 1024      # a full-page screenshot is a few megabytes of base64
MAX_SKIPPED_MESSAGES = 1000             # notifications tolerated while waiting for one response
CLIENT_NAME = "powerbi-mcp"             # sent as the optional caller identifier
INVALID_PARAMS = -32602                 # JSON-RPC: the arguments did not fit the method
METHOD_NOT_FOUND = -32601
HOST_NOT_READY = -32502                 # Desktop is loading a file; it accepts no operation yet
OPERATION_IN_PROGRESS = -32511          # the bridge runs one operation at a time
RETRYABLE = (HOST_NOT_READY, OPERATION_IN_PROGRESS)
CAPTURE_TIMED_OUT = -32504               # Desktop gave up drawing a page within its own limit
RETRY_PAUSE = 2.0                       # seconds between attempts while Desktop is not ready
READY_WAIT = 20.0                       # how long a call waits for Desktop to become ready


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

    def __init__(self, pipe: str, timeout: float = DEFAULT_TIMEOUT, ready_wait: Optional[float] = None):
        self.pipe = pipe
        self.timeout = timeout
        self.ready_wait = READY_WAIT if ready_wait is None else ready_wait
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

    # -------- the argument shape this Desktop build expects

    _manifests: Dict[str, Dict[str, bool]] = {}    # pipe -> {method: arguments go in an envelope}, as declared
    _learned: Dict[str, Dict[str, bool]] = {}      # pipe -> the same, found by trying when nothing is declared

    def _enveloped(self, method: str) -> Optional[bool]:
        """Whether this build takes ``method``'s arguments inside ``{"client", "args": {...}}``
        (Desktop 2.159, late 2026) or directly as the params (the first preview builds). The
        bridge's own manifest says which; a manifest that was read is kept for the pipe, a
        failed read is not, so a bridge that was still starting is asked again. None when
        neither the manifest nor an earlier attempt says."""
        known = self._manifests.get(self.pipe)
        if known is None:
            known = {}
            try:
                for m in self.call("bridge.manifest").get("methods") or []:
                    if isinstance(m, dict) and isinstance(m.get("name"), str):
                        properties = (m.get("params") or {}).get("properties")
                        known[m["name"]] = isinstance(properties, dict) and "args" in properties
            except (BridgeError, ConnectionError) as e:
                logger.debug(f"bridge manifest unavailable, trying both argument shapes: {e}")
            if known:
                self._manifests[self.pipe] = known
        if method in known:
            return known[method]
        return self._learned.get(self.pipe, {}).get(method)

    def supports(self, method: str) -> Optional[bool]:
        """True when the running build's manifest lists ``method``, False when it does not, and
        None when the manifest could not be read."""
        self._enveloped(method)
        known = self._manifests.get(self.pipe)
        return None if known is None else method in known

    def invoke(self, method: str, args: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Call a bridge operation, passing ``args`` in the shape the running build expects.

        While Desktop answers "not ready" or "another operation is in progress" (it does for a
        few seconds after opening a file and during a reload) the call is repeated, for at most
        ``ready_wait`` seconds. When the manifest does not list the method, the current shape
        is tried first and the earlier one once more if the bridge answers "invalid params".
        """
        args = dict(args or {})
        deadline = time.monotonic() + self.ready_wait
        while True:
            try:
                return self._invoke_once(method, args)
            except BridgeError as e:
                remaining = deadline - time.monotonic()
                if e.code not in RETRYABLE or remaining <= 0:
                    raise
                time.sleep(min(RETRY_PAUSE, remaining))

    def _invoke_once(self, method: str, args: Dict[str, Any]) -> Dict[str, Any]:
        enveloped = self._enveloped(method)
        first = enveloped is not False

        def params(envelope: bool) -> Dict[str, Any]:
            return {"client": CLIENT_NAME, "args": args} if envelope else args

        try:
            return self.call(method, params(first))
        except BridgeError as refused:
            if refused.code != INVALID_PARAMS:
                raise
            if method in self._manifests.get(self.pipe, {}):
                # The shape came from the manifest, so the arguments themselves are wrong. Read
                # the manifest afresh next time in case another Desktop build now owns this pipe.
                self._manifests.pop(self.pipe, None)
                raise
            try:
                result = self.call(method, params(not first))
            except BridgeError:
                raise refused from None          # both shapes refused: report the current one's answer
            self._learned.setdefault(self.pipe, {})[method] = not first
            return result

    # -------- typed wrappers over the documented methods

    def manifest(self) -> Dict[str, Any]:
        return self.call("bridge.manifest")

    def get_state(self) -> Dict[str, Any]:
        return self.invoke("application.state.get/v1")

    def capture_snapshot(self, page_id: str, scale: Optional[float] = None,
                         region: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """PNG of a page, or of ``region`` {x, y, width, height} in the page's own coordinates
        (the ones a visual's position uses). A region needs a build with capture/v2."""
        # The manifest marks BOTH pageId and scale as required (scale is nullable but the key
        # must be present; omitting it null-refs inside Desktop). Learn's doc says optional -
        # trust the manifest.
        args: Dict[str, Any] = {"pageId": page_id, "scale": float(scale) if scale is not None else 1.0}
        method = "report.snapshot.capture/v1"
        if region is not None:
            method = "report.snapshot.capture/v2"
            if self.supports(method) is False:
                raise BridgeError(METHOD_NOT_FOUND, "this Power BI Desktop build cannot capture a region of a page "
                                                    "(it has no report.snapshot.capture/v2); capture the whole page")
            args["region"] = {k: float(region[k]) for k in ("x", "y", "width", "height")}
        try:
            return self.invoke(method, args)
        except BridgeError as e:
            if e.code != CAPTURE_TIMED_OUT:
                raise
            # The first capture of a page after a reload can run past Desktop's own 30-second
            # limit while its visuals load (seen with custom visuals fetched on first use). They
            # are loaded by the time it gives up, so one more attempt normally succeeds.
            return self.invoke(method, args)

    def reload_file(self, reload_model_definition: bool = True) -> Dict[str, Any]:
        return self.invoke("file.reload/v1", {"reloadModelDefinition": bool(reload_model_definition)})


# ---------------------------------------------------------------- opening a file

_EXTENDED = "\\\\?\\"                      # the \\?\ prefix: a Windows path of any length
_EXTENDED_UNC = "\\\\?\\UNC\\"


def _plain_path(path: str) -> str:
    """``path`` without the \\\\?\\ prefix, which is not part of what Desktop's limit counts."""
    if path.startswith(_EXTENDED_UNC):
        return "\\\\" + path[len(_EXTENDED_UNC):]
    return path[len(_EXTENDED):] if path.startswith(_EXTENDED) else path


def _listable(path: str) -> str:
    """The form of ``path`` that can be listed whatever its length. Without it, a folder beyond
    the limit cannot be opened on a machine that has no long-path support, and would be missed."""
    if os.name != "nt" or path.startswith(_EXTENDED):
        return path
    return _EXTENDED_UNC + path[2:] if path.startswith("\\\\") else _EXTENDED + path


def _is_link(entry: Any) -> bool:
    """A symbolic link or a directory junction: not followed, a loop would never end."""
    try:
        if entry.is_symlink():
            return True
        attributes = getattr(entry.stat(follow_symlinks=False), "st_file_attributes", 0)
    except OSError:
        return True
    return bool(attributes & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0))


def long_paths(roots: Iterable[Any]) -> List[Dict[str, Any]]:
    """Files and folders under ``roots`` that Desktop cannot read because of the Windows path
    limit, longest first: [{path, length, limit}]. Desktop stops opening a project at the first
    such file, with a dialog the bridge cannot see, so this is checked before a launch. Links
    and junctions are not followed."""
    found: Dict[str, Dict[str, Any]] = {}

    def look(path: str, is_dir: bool) -> None:
        limit = MAX_DIR_PATH if is_dir else MAX_FILE_PATH
        if len(path) >= limit:
            found[path] = {"path": path, "length": len(path), "limit": limit}

    for root in roots:
        if not root:
            continue
        root = _plain_path(os.path.abspath(str(root)))
        if not os.path.isdir(_listable(root)):
            look(root, False)
            continue
        pending = [root]
        while pending:
            folder = pending.pop()
            look(folder, True)
            try:
                entries = list(os.scandir(_listable(folder)))
            except OSError:
                continue
            for entry in entries:
                path = os.path.join(folder, entry.name)
                try:
                    is_dir = entry.is_dir(follow_symlinks=False)
                except OSError:
                    is_dir = False
                if is_dir and not _is_link(entry):
                    pending.append(path)
                else:
                    look(path, is_dir)
    return sorted(found.values(), key=lambda p: (-p["length"], p["path"]))


def same_file(a: str, b: str) -> bool:
    """True when both paths name the same file, however each is spelled (short names, links)."""
    try:
        return os.path.samefile(a, b)
    except OSError:
        return os.path.normcase(os.path.abspath(a)) == os.path.normcase(os.path.abspath(b))


def running_instances(path: str, timeout: float = 10.0) -> Tuple[Optional[Dict[str, Any]], List[Any]]:
    """Which running Desktop window has ``path`` open.

    Returns (the instance as {pid, pipe, state} or None, the pids of windows that gave no
    answer). A window that is loading a file or busy with an operation answers "not ready"
    instead of its state, so "no window has it" is only known when the second list is empty.
    """
    undecided: List[Any] = []
    for bridge in discover_bridges():
        try:
            state = DesktopBridgeClient(bridge["pipe"], timeout=timeout, ready_wait=0).get_state()
        except Exception as e:  # noqa: BLE001 - busy, still starting or gone: all "cannot say"
            logger.debug(f"bridge {bridge['pid']} gave no state: {e}")
            undecided.append(bridge["pid"])
            continue
        current = state.get("currentFilePath") or ""
        if current and same_file(current, path):
            return {"pid": bridge["pid"], "pipe": bridge["pipe"], "state": state}, undecided
    return None, undecided


def instance_for_file(path: str, timeout: float = 10.0) -> Optional[Dict[str, Any]]:
    """The running Desktop instance that reports ``path`` as open, as {pid, pipe, state}, or
    None. None can also mean that the window holding it is busy: see running_instances."""
    return running_instances(path, timeout)[0]


SHOW_MINIMIZED_NO_FOCUS = 7              # Windows SW_SHOWMINNOACTIVE
BUSY_WAIT = 30.0                         # how long a busy window is given to say which file it holds


def open_file(path: str, timeout: float = 120.0, poll: float = 2.0, minimized: bool = False,
              launch: Optional[Callable[[str], Any]] = None,
              sleep: Callable[[float], None] = time.sleep,
              clock: Callable[[], float] = time.monotonic) -> Dict[str, Any]:
    """Open a .pbip or .pbix in Power BI Desktop and wait until its bridge is ready for work.

    Returns {opened, already_open, pid, file, waited_seconds} or {opened: False, error, ...}.
    The file is started through its Windows file association (``os.startfile``): that is the
    only way to start a Microsoft Store install of Desktop, which has no exe path to call.
    ``minimized`` starts the window minimized without taking the keyboard focus.

    A file that is already open is not started again. A running window that is too busy to
    say which file it holds is waited for; if it stays silent the file is NOT started, because
    two windows on one project can save over each other. After a launch the file counts as
    open once the bridge has reported it twice in a row: Desktop reports the path a moment
    before it accepts operations.
    """
    path = os.path.abspath(path)
    result: Dict[str, Any] = {"opened": False, "already_open": False, "pid": None, "file": path}
    started = clock()
    running, undecided = running_instances(path)
    while not running and undecided and clock() - started < min(timeout, BUSY_WAIT):
        sleep(poll)
        running, undecided = running_instances(path)
    if running:
        return dict(result, opened=True, already_open=True, pid=running["pid"],
                    waited_seconds=round(clock() - started, 1))
    if undecided:
        return dict(result, busy_instances=undecided, waited_seconds=round(clock() - started, 1),
                    error=(f"A running Power BI Desktop window (pid {', '.join(map(str, undecided))}) is busy "
                           "and did not say which file it has open, so this file was not started: it may be "
                           "the one already open there. Try again when Desktop has finished."))
    if launch is None:
        startfile = getattr(os, "startfile", None)
        if startfile is None:
            return dict(result, error="Power BI Desktop runs on Windows only; this machine cannot open the file.")
        launch = (lambda p: startfile(p, show_cmd=SHOW_MINIMIZED_NO_FOCUS)) if minimized else startfile
    before = {b["pid"] for b in discover_bridges()}
    launched = clock()
    try:
        launch(path)
    except OSError as e:
        return dict(result, error=f"Windows could not open the file ({e}). Is Power BI Desktop installed "
                                  "and associated with this file type?")
    confirmed = 0
    while clock() - launched < timeout:
        sleep(poll)
        running = running_instances(path)[0]
        confirmed = confirmed + 1 if running else 0
        if confirmed >= 2:
            return dict(result, opened=True, pid=running["pid"], waited_seconds=round(clock() - launched, 1))
    new = sorted(p for p in {b["pid"] for b in discover_bridges()} - before if p is not None)
    if new:
        why = (f"Power BI Desktop started (pid {', '.join(map(str, new))}) but did not report the file as "
               f"open within {int(timeout)} seconds. It may still be loading, or showing an error dialog: "
               "look at the Desktop window.")
    else:
        why = (f"No Power BI Desktop Bridge appeared within {int(timeout)} seconds. Check that Desktop "
               "started and that the preview option 'Enable external tool access to Power BI Desktop "
               "through secure local APIs' is on.")
    return dict(result, error=why, new_instances=new, waited_seconds=round(clock() - launched, 1))


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


def _read_json_object(path: Path) -> Optional[Dict[str, Any]]:
    """The JSON object in ``path``, or None when the file is missing, unreadable or not an object."""
    try:
        doc = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, ValueError):
        return None
    return doc if isinstance(doc, dict) else None


def _page_folder(pbip: Path, page_id: str) -> Optional[Path]:
    """The folder of the page whose ``name`` is ``page_id``. Desktop names the folder after the
    page, but a folder renamed by hand still belongs to the page its page.json names."""
    pages = pbip.parent / f"{pbip.stem}.Report" / "definition" / "pages"
    if not pages.is_dir():
        return None
    direct = pages / page_id
    folders = sorted(p for p in pages.iterdir() if p.is_dir())
    for folder in sorted(folders, key=lambda p: p != direct):
        doc = _read_json_object(folder / "page.json")
        if doc is not None and str(doc.get("name") or folder.name) == page_id:
            return folder
    return direct if direct.is_dir() else None


def visual_region(current_file_path: str, page_id: str, visual: str, margin: Any = 8) -> Optional[Dict[str, float]]:
    """The rectangle a visual occupies on its page, widened by ``margin`` but kept inside the
    page, as {x, y, width, height} in page coordinates: what capture_snapshot takes as
    ``region`` (the bridge refuses a region that reaches past the page). The visual is found by
    its folder name or by the ``name`` in its visual.json. A visual inside a group is stored
    with a position relative to that group (verified in Desktop 2.159), so the offsets of its
    parent groups are added. None when there is no such visual, it has no position, or the open
    file is not a PBIP. ``margin`` must be a number."""
    pad = max(0.0, float(8 if margin is None else margin))
    if not current_file_path or not current_file_path.lower().endswith(".pbip") or not visual:
        return None
    page = _page_folder(Path(current_file_path), page_id)
    if page is None or not (page / "visuals").is_dir():
        return None
    docs: List[Tuple[str, Dict[str, Any]]] = []
    for folder in sorted(p for p in (page / "visuals").iterdir() if p.is_dir()):
        doc = _read_json_object(folder / "visual.json")
        if doc is not None:
            docs.append((folder.name, doc))
    wanted = visual.strip().lower()
    by_folder = [d for name, d in docs if name.lower() == wanted]
    target = (by_folder or [d for _, d in docs if str(d.get("name", "")).lower() == wanted] or [None])[0]
    if target is None:
        return None

    def box(doc: Dict[str, Any]) -> Optional[Tuple[float, float, float, float]]:
        pos = doc.get("position")
        try:
            return tuple(float(pos[k]) for k in ("x", "y", "width", "height"))  # type: ignore[return-value]
        except (TypeError, KeyError, ValueError):
            return None

    placed = box(target)
    if placed is None:
        return None
    x, y, w, h = placed
    by_name = {str(d.get("name") or folder): d for folder, d in docs}
    parent, seen = target.get("parentGroupName"), set()
    while isinstance(parent, str) and parent in by_name and parent not in seen:
        seen.add(parent)
        group = box(by_name[parent])
        if group is not None:
            x, y = x + group[0], y + group[1]
        parent = by_name[parent].get("parentGroupName")

    left, top, right, bottom = max(0.0, x - pad), max(0.0, y - pad), x + w + pad, y + h + pad
    size = _read_json_object(page / "page.json") or {}
    width, height = size.get("width"), size.get("height")
    if isinstance(width, (int, float)) and not isinstance(width, bool) and width > 0:
        right = min(right, float(width))
    if isinstance(height, (int, float)) and not isinstance(height, bool) and height > 0:
        bottom = min(bottom, float(height))
    if right <= left or bottom <= top:
        return None                                      # the visual lies outside its page
    return {"x": left, "y": top, "width": right - left, "height": bottom - top}


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
