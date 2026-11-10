"""
Tests for the Power BI Desktop Bridge client + tools: JSON-RPC framing and its limits, response
matching (ids, notifications, null results), timeout behaviour, discovery, the msmdsrv port
lookup across psutil versions, PBIR page resolution, and the reload unsaved-guard.
The pipe is always a fake stream: no Power BI and no named pipe is involved.
Run: python tests/test_desktop_bridge.py
"""
import asyncio
import builtins
import io
import json
import os
import sys
import tempfile
import threading
import time
import types
from pathlib import Path

os.environ.setdefault("POWERBI_MCP_AUDIT_DIR", __import__("tempfile").mkdtemp(prefix="pbi-mcp-audit-"))  # keep logs/ clean
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))
import desktop_bridge as db  # noqa: E402

_failures = []
FAKE = "\\\\.\\pipe\\fake-bridge-for-tests"


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f": {detail}" if detail and not cond else ""))
    if not cond:
        _failures.append(name)


def raises(name, exc_type, fn, contains=""):
    try:
        fn()
    except exc_type as e:
        check(name, contains in str(e), f"message was {e}")
        return e
    except Exception as e:  # noqa: BLE001 - the test reports any other type as a failure
        check(name, False, f"raised {type(e).__name__}: {e}")
        return None
    check(name, False, "nothing was raised")
    return None


def test_framing():
    print("\n== Content-Length framing round-trip and limits ==")
    msg = {"jsonrpc": "2.0", "id": 7, "method": "bridge.manifest", "params": {"a": chr(0xFC)}}
    frame = db.encode_frame(msg)
    check("header present", frame.startswith(b"Content-Length: "))
    body = frame.split(b"\r\n\r\n", 1)[1]
    check("length counts utf-8 bytes", int(frame.split(b":")[1].split(b"\r\n")[0]) == len(body))
    check("round-trip", db.read_frame(io.BytesIO(frame)) == msg)
    stream = io.BytesIO(db.encode_frame({"x": 1}) + db.encode_frame({"y": 2}))
    check("sequential frames", db.read_frame(stream) == {"x": 1} and db.read_frame(stream) == {"y": 2})
    raises("truncated body", ConnectionError, lambda: db.read_frame(io.BytesIO(b"Content-Length: 50\r\n\r\n{}")),
           "mid-response")
    raises("stream closed in the header", ConnectionError, lambda: db.read_frame(io.BytesIO(b"Content-Le")), "headers")
    raises("missing Content-Length", ConnectionError, lambda: db.read_frame(io.BytesIO(b"X-Other: 1\r\n\r\n{}")),
           "missing Content-Length")
    raises("non-numeric Content-Length", ConnectionError,
           lambda: db.read_frame(io.BytesIO(b"Content-Length: abc\r\n\r\n{}")), "invalid Content-Length")
    raises("negative Content-Length", ConnectionError,
           lambda: db.read_frame(io.BytesIO(b"Content-Length: -5\r\n\r\n{}")), "invalid Content-Length")
    raises("endless header is cut off", ConnectionError,
           lambda: db.read_frame(io.BytesIO(b"x" * (db.MAX_HEADER_BYTES + 100))), "too long")
    raises("oversized body is refused before it is read", ConnectionError,
           lambda: db.read_frame(io.BytesIO(b"Content-Length: 5000\r\n\r\n" + b" " * 5000), max_body=1000), "exceeds")
    raises("body that is not JSON", ConnectionError,
           lambda: db.read_frame(io.BytesIO(b"Content-Length: 3\r\n\r\n{x}")), "not valid JSON")


class FakePipe:
    """A duplex stub: collects the request, then serves the frames ``respond`` builds from it."""

    def __init__(self, respond):
        self.wrote = b""
        self.respond = respond
        self._resp = None
        self.closed = False

    def write(self, data):
        self.wrote += data
        request = json.loads(self.wrote.split(b"\r\n\r\n", 1)[1])
        self._resp = io.BytesIO(b"".join(db.encode_frame(m) for m in self.respond(request)))
        return len(data)

    def read(self, n):
        return self._resp.read(n)

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.closed = True
        return False

    @property
    def request(self):
        return json.loads(self.wrote.split(b"\r\n\r\n", 1)[1])


class PipePatch:
    """Route open() of the fake pipe path to a stub for the duration of a with-block."""

    def __init__(self, pipe):
        self.pipe = pipe

    def __enter__(self):
        self.real = builtins.open
        builtins.open = lambda path, *a, **k: self.pipe if path == FAKE else self.real(path, *a, **k)
        return self.pipe

    def __exit__(self, *a):
        builtins.open = self.real
        return False


def answer(result):
    return lambda req: [{"jsonrpc": "2.0", "id": req["id"], "result": result}]


def test_client():
    print("\n== client call / error handling (fake pipe) ==")
    c = db.DesktopBridgeClient(FAKE, timeout=5)
    with PipePatch(FakePipe(answer({"methods": [{"name": "file.reload/v1"}]}))) as pipe:
        res = c.manifest()
    check("result returned", res.get("methods", [{}])[0].get("name") == "file.reload/v1", str(res))
    check("request is JSON-RPC 2.0", pipe.request["jsonrpc"] == "2.0" and pipe.request["method"] == "bridge.manifest")
    check("pipe handle closed after the call", pipe.closed)

    error = lambda req: [{"jsonrpc": "2.0", "id": req["id"], "error": {"code": -32601, "message": "MethodNotFound"}}]  # noqa: E731
    with PipePatch(FakePipe(error)):
        e = raises("BridgeError raised", db.BridgeError, lambda: c.call("nope"), "MethodNotFound")
    check("error code kept", e is not None and e.code == -32601)

    with PipePatch(FakePipe(answer({"success": True}))) as pipe:
        r = c.reload_file(reload_model_definition=False)
    check("reload params + result", r.get("success") is True and pipe.request["params"] == {"reloadModelDefinition": False})

    # snapshot: the live manifest marks BOTH pageId and scale required (scale nullable but the
    # key must be present, else Desktop null-refs). Assert we always send scale.
    with PipePatch(FakePipe(answer({"payload": "AAAA", "pageDisplayName": "P1"}))) as pipe:
        c.capture_snapshot("page1")
    check("snapshot always sends scale (default 1.0)", pipe.request["params"] == {"pageId": "page1", "scale": 1.0},
          str(pipe.request["params"]))
    with PipePatch(FakePipe(answer({"payload": "AAAA"}))) as pipe:
        c.capture_snapshot("page1", scale=2.5)
    check("snapshot passes explicit scale", pipe.request["params"]["scale"] == 2.5)


def test_response_matching():
    print("\n== only the response to this request is accepted ==")
    c = db.DesktopBridgeClient(FAKE, timeout=5)
    noisy = lambda req: [  # noqa: E731
        {"jsonrpc": "2.0", "method": "progress", "params": {"pct": 10}},                 # a notification
        {"jsonrpc": "2.0", "id": req["id"] + 1000, "result": {"currentFilePath": "someone else's"}},
        {"jsonrpc": "2.0", "id": req["id"], "result": {"currentFilePath": "C:\\mine.pbip"}},
    ]
    with PipePatch(FakePipe(noisy)):
        res = c.get_state()
    check("notifications and foreign ids are skipped", res == {"currentFilePath": "C:\\mine.pbip"}, str(res))

    only_wrong = lambda req: [{"jsonrpc": "2.0", "id": req["id"] + 1, "result": {"x": 1}}]  # noqa: E731
    with PipePatch(FakePipe(only_wrong)):
        raises("a response with another id is never returned", ConnectionError, c.get_state)
    with PipePatch(FakePipe(lambda req: [{"jsonrpc": "2.0", "method": "progress"}])):
        raises("a notification alone is not an answer", ConnectionError, c.get_state)
    with PipePatch(FakePipe(answer(None))):
        check("null result becomes an empty object", c.get_state() == {})
    with PipePatch(FakePipe(answer([1, 2]))):
        check("non-object result is wrapped", c.call("x") == {"value": [1, 2]})
    with PipePatch(FakePipe(lambda req: [{"jsonrpc": "2.0", "id": None, "error": {"code": -32700, "message": "Parse error"}}])):
        e = raises("an error with a null id (unparseable request) is ours", db.BridgeError, c.get_state, "Parse error")
    check("its code is kept", e is not None and e.code == -32700)
    with PipePatch(FakePipe(lambda req: [{"jsonrpc": "2.0", "id": req["id"], "error": {"code": "E1", "message": "odd"}}])):
        e = raises("a non-numeric error code does not break error handling", db.BridgeError, c.get_state, "odd")
    check("it is reported as -1", e is not None and e.code == -1)
    with PipePatch(FakePipe(lambda req: [[1, 2, 3]])):
        raises("a frame that is not an object", ConnectionError, c.get_state, "not a JSON-RPC object")


class BlockingPipe:
    """Never answers until ``release`` is set."""

    def __init__(self):
        self.release = threading.Event()
        self.closed = False

    def write(self, data):
        return len(data)

    def read(self, n):
        self.release.wait(20)
        return b""

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.closed = True
        return False


def test_timeout_is_bounded():
    print("\n== a call that times out cannot pile up behind itself ==")
    c = db.DesktopBridgeClient(FAKE, timeout=0.2)
    blocked = BlockingPipe()
    before = threading.active_count()
    with PipePatch(blocked):
        started = time.time()
        raises("timeout raised", TimeoutError, c.get_state, "did not respond")
        check("the caller got control back at about the timeout", time.time() - started < 5)
        e = raises("the next call fails at once instead of queueing", db.BridgeBusyError, c.get_state, "still busy")
        check("BridgeBusyError is a TimeoutError", isinstance(e, TimeoutError))
        raises("and so does every later one", db.BridgeBusyError, lambda: c.call("anything"))
        check("only one worker is left waiting", threading.active_count() - before == 1,
              str(threading.active_count() - before))
        blocked.release.set()                    # Desktop finally lets go
        deadline = time.time() + 5
        while threading.active_count() > before and time.time() < deadline:
            time.sleep(0.02)
        check("the worker ends and closes its handle", threading.active_count() == before and blocked.closed)
    with PipePatch(FakePipe(answer({"ok": True}))):
        check("the pipe is usable again", c.call("x") == {"ok": True})

    if os.name == "nt":
        # A real blocking read (on an anonymous pipe that never gets data) is cancelled on
        # timeout, so no worker is left behind at all.
        read_fd, write_fd = os.pipe()

        class RealBlockingRead(BlockingPipe):
            def read(self, n):
                return os.read(read_fd, n)

        real = RealBlockingRead()
        try:
            with PipePatch(real):
                raises("timeout raised on a real blocking read", TimeoutError, c.get_state)
                check("the blocked read was cancelled and the handle closed",
                      threading.active_count() == before and real.closed,
                      f"threads {threading.active_count()} vs {before}, closed={real.closed}")
            with PipePatch(FakePipe(answer({"ok": True}))):
                check("no busy state is left after a cancelled read", c.call("x") == {"ok": True})
        finally:
            os.close(write_fd)
            os.close(read_fd)


def test_discovery():
    print("\n== discovery parses pipe names ==")
    orig = db.os.listdir
    db.os.listdir = lambda p: ["mojo.pipe", "pbi-desktop-bridge-4242", "pbi-desktop-bridge-x"]
    try:
        found = db.discover_bridges()
    finally:
        db.os.listdir = orig
    check("finds bridge pipes only", len(found) == 2)
    check("pid parsed", found[0]["pid"] == 4242 and found[0]["pipe"].endswith("pbi-desktop-bridge-4242"))
    check("non-numeric suffix tolerated", found[1]["pid"] is None)

    def no_pipes(_):
        raise OSError("no pipe namespace here")
    db.os.listdir = no_pipes
    try:
        check("no pipe namespace (non-Windows) is an empty result", db.discover_bridges() == [])
    finally:
        db.os.listdir = orig


def _fake_psutil(method_name):
    """A stand-in psutil whose Process exposes its connections under ``method_name``."""
    listen = "LISTEN"

    class Proc:
        def __init__(self, name, ppid, port):
            self.info = {"name": name, "ppid": ppid}
            conn = types.SimpleNamespace(status=listen, laddr=types.SimpleNamespace(port=port))
            setattr(self, method_name, lambda kind="tcp": [conn])

    mod = types.ModuleType("psutil")
    mod.CONN_LISTEN = listen
    mod.NoSuchProcess = type("NoSuchProcess", (Exception,), {})
    mod.AccessDenied = type("AccessDenied", (Exception,), {})
    mod.process_iter = lambda attrs=None: [Proc("chrome.exe", 77, 1), Proc("msmdsrv.exe", 5, 2),
                                           Proc("MSMDSRV.EXE", 77, 51234)]
    return mod


def test_msmdsrv_port_lookup():
    print("\n== msmdsrv port correlation works with psutil 5 and psutil 6+ ==")
    real = sys.modules.get("psutil")
    try:
        for label, method in (("psutil 6+ (net_connections)", "net_connections"), ("psutil 5 (connections)", "connections")):
            sys.modules["psutil"] = _fake_psutil(method)
            check(f"{label}: port of the msmdsrv child of this Desktop",
                  db.msmdsrv_port_for_desktop_pid(77) == 51234, str(db.msmdsrv_port_for_desktop_pid(77)))
            check(f"{label}: no child, no port", db.msmdsrv_port_for_desktop_pid(999) is None)
    finally:
        if real is not None:
            sys.modules["psutil"] = real
        else:
            sys.modules.pop("psutil", None)


def test_page_resolution():
    print("\n== PBIR page resolution from an open .pbip path ==")
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        pbip = root / "Demo.pbip"
        pbip.write_text("{}", encoding="utf-8")
        pages = root / "Demo.Report" / "definition" / "pages"
        for name, disp in (("abc123", "Overview"), ("def456", "Detail")):
            (pages / name).mkdir(parents=True)
            (pages / name / "page.json").write_text(json.dumps({"name": name, "displayName": disp}), encoding="utf-8")
        (pages / "pages.json").write_text(json.dumps({"pageOrder": ["def456", "abc123"], "activePageName": "abc123"}), encoding="utf-8")
        got = db.pbir_pages_for_file(str(pbip))
        check("both pages found in order", [p["id"] for p in got] == ["def456", "abc123"], str(got))
        check("active flagged", next(p for p in got if p["id"] == "abc123")["active"] is True)
        check("resolve by display name", db.resolve_page_id(got, "overview") == "abc123")
        check("resolve by id", db.resolve_page_id(got, "def456") == "def456")
        check("unknown -> None", db.resolve_page_id(got, "nope") is None)
        check("pbix path -> no pages", db.pbir_pages_for_file(str(root / "x.pbix")) == [])
        check("a numeric page id does not break resolution",
              db.resolve_page_id([{"id": 12, "display_name": "Twelve"}], "twelve") == 12)


def test_reload_guard_and_status():
    print("\n== server handlers: unsaved-change guard + graceful no-desktop ==")
    import server
    s = server.PowerBIMCPServer()
    # no bridges at all -> graceful status
    orig_disc = db.discover_bridges
    db.discover_bridges = lambda: []
    try:
        text, res = asyncio.run(s._handle_bridge_status({}))
        check("status graceful with no Desktop", res["instances"] == [] and "No bridge" in text)
        out = asyncio.run(s._handle_bridge_reload({}))
        check("reload errors cleanly with no Desktop", out.startswith("Error:"), out[:60])
    finally:
        db.discover_bridges = orig_disc
    # unsaved changes -> refuse without force
    db.discover_bridges = lambda: [{"pid": 99, "pipe": "\\\\.\\pipe\\pbi-desktop-bridge-99"}]
    calls = []

    class FakeClient:
        def __init__(self, pipe, timeout=60):
            pass

        def get_state(self):
            return {"currentFilePath": "C:\\x.pbip", "hasUnsavedChanges": True}

        def reload_file(self, reload_model_definition=True):
            calls.append("reload")
            return {"success": True}

    orig_client = db.DesktopBridgeClient
    db.DesktopBridgeClient = FakeClient
    try:
        out = asyncio.run(s._handle_bridge_reload({}))
        check("refuses over unsaved changes", out.startswith("Refused") and not calls, out[:70])
        out2 = asyncio.run(s._handle_bridge_reload({"force": True}))
        check("force reloads", "Reloaded" in out2 and calls == ["reload"], out2[:80])
    finally:
        db.DesktopBridgeClient = orig_client
        db.discover_bridges = orig_disc


if __name__ == "__main__":
    print("=" * 70)
    print("  DESKTOP BRIDGE TESTS")
    print("=" * 70)
    for fn in (test_framing, test_client, test_response_matching, test_timeout_is_bounded, test_discovery,
               test_msmdsrv_port_lookup, test_page_resolution, test_reload_guard_and_status):
        fn()
    print("\n" + "=" * 70)
    if _failures:
        print(f"  {len(_failures)} CHECK(S) FAILED: {', '.join(_failures)}")
        sys.exit(1)
    print("  ALL DESKTOP BRIDGE CHECKS PASSED")
    print("=" * 70)
