"""
Tests for the Power BI Desktop Bridge client + tools: JSON-RPC framing, discovery, the
client against a fake pipe stream, PBIR page resolution, and the reload unsaved-guard.
No Power BI required. Run: python tests/test_desktop_bridge.py
"""
import asyncio
import io
import json
import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))
import desktop_bridge as db  # noqa: E402

_failures = []


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f": {detail}" if detail and not cond else ""))
    if not cond:
        _failures.append(name)


def test_framing():
    print("\n== Content-Length framing round-trip ==")
    msg = {"jsonrpc": "2.0", "id": 7, "method": "bridge.manifest", "params": {"a": "\u00fc"}}
    frame = db.encode_frame(msg)
    check("header present", frame.startswith(b"Content-Length: "))
    body = frame.split(b"\r\n\r\n", 1)[1]
    check("length counts utf-8 bytes", int(frame.split(b":")[1].split(b"\r\n")[0]) == len(body))
    decoded = db.read_frame(io.BytesIO(frame))
    check("round-trip", decoded == msg)
    # two frames back to back read cleanly
    stream = io.BytesIO(db.encode_frame({"x": 1}) + db.encode_frame({"y": 2}))
    check("sequential frames", db.read_frame(stream) == {"x": 1} and db.read_frame(stream) == {"y": 2})
    try:
        db.read_frame(io.BytesIO(b"Content-Length: 50\r\n\r\n{}"))
        check("truncated body raises", False)
    except ConnectionError:
        check("truncated body raises", True)


class FakePipe:
    """A duplex stub: collects the request, serves a canned framed response."""
    def __init__(self, response_obj):
        self.wrote = b""
        self._resp = io.BytesIO(db.encode_frame(response_obj))
    def write(self, data): self.wrote += data
    def read(self, n): return self._resp.read(n)
    def close(self): pass
    def __enter__(self): return self
    def __exit__(self, *a): return False


def test_client(monkey_open):
    print("\n== client call / error handling (fake pipe) ==")
    ok_pipe = FakePipe({"jsonrpc": "2.0", "id": 1, "result": {"methods": [{"name": "file.reload/v1"}]}})
    monkey_open(ok_pipe)
    c = db.DesktopBridgeClient("\\\\.\\pipe\\fake", timeout=5)
    res = c.manifest()
    check("result returned", res.get("methods", [{}])[0].get("name") == "file.reload/v1", str(res))
    sent = json.loads(ok_pipe.wrote.split(b"\r\n\r\n", 1)[1])
    check("request is JSON-RPC 2.0", sent["jsonrpc"] == "2.0" and sent["method"] == "bridge.manifest")

    err_pipe = FakePipe({"jsonrpc": "2.0", "id": 1, "error": {"code": -32601, "message": "MethodNotFound"}})
    monkey_open(err_pipe)
    try:
        c.call("nope")
        check("BridgeError raised", False)
    except db.BridgeError as e:
        check("BridgeError raised", e.code == -32601 and "MethodNotFound" in e.message)

    reload_pipe = FakePipe({"jsonrpc": "2.0", "id": 1, "result": {"success": True}})
    monkey_open(reload_pipe)
    r = c.reload_file(reload_model_definition=False)
    sent = json.loads(reload_pipe.wrote.split(b"\r\n\r\n", 1)[1])
    check("reload params + result", r.get("success") is True
          and sent["params"] == {"reloadModelDefinition": False})

    # snapshot: the live manifest marks BOTH pageId and scale required (scale nullable but the
    # key must be present, else Desktop null-refs). Assert we always send scale.
    snap_pipe = FakePipe({"jsonrpc": "2.0", "id": 1, "result": {"payload": "AAAA", "pageDisplayName": "P1"}})
    monkey_open(snap_pipe)
    c.capture_snapshot("page1")
    sp = json.loads(snap_pipe.wrote.split(b"\r\n\r\n", 1)[1])["params"]
    check("snapshot always sends scale (default 1.0)", sp == {"pageId": "page1", "scale": 1.0}, str(sp))
    snap_pipe2 = FakePipe({"jsonrpc": "2.0", "id": 1, "result": {"payload": "AAAA"}})
    monkey_open(snap_pipe2)
    c.capture_snapshot("page1", scale=2.5)
    sp2 = json.loads(snap_pipe2.wrote.split(b"\r\n\r\n", 1)[1])["params"]
    check("snapshot passes explicit scale", sp2["scale"] == 2.5)


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
        def __init__(self, pipe, timeout=60): pass
        def get_state(self): return {"currentFilePath": "C:\\x.pbip", "hasUnsavedChanges": True}
        def reload_file(self, reload_model_definition=True):
            calls.append("reload"); return {"success": True}
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
    test_framing()

    _pipes = []
    _orig_open = None
    def monkey_open(pipe):
        _pipes.clear(); _pipes.append(pipe)
    import builtins
    _orig_open = builtins.open
    def fake_open(path, *a, **k):
        if isinstance(path, str) and "pbi-desktop-bridge" in path or (isinstance(path, str) and path.startswith("\\\\.\\pipe")):
            return _pipes[0]
        return _orig_open(path, *a, **k)
    builtins.open = fake_open
    try:
        test_client(monkey_open)
    finally:
        builtins.open = _orig_open

    test_discovery()
    test_page_resolution()
    test_reload_guard_and_status()
    print("\n" + "=" * 70)
    if _failures:
        print(f"  {len(_failures)} CHECK(S) FAILED: {', '.join(_failures)}")
        sys.exit(1)
    print("  ALL DESKTOP BRIDGE CHECKS PASSED")
    print("=" * 70)
