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
import subprocess
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
        self.requests = []

    def begin(self):
        """The client opens the pipe afresh for every call."""
        self.wrote = b""
        self.closed = False

    def write(self, data):
        self.wrote += data
        request = json.loads(self.wrote.split(b"\r\n\r\n", 1)[1])
        self.requests.append(request)
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

        def fake_open(path, *a, **k):
            if path != FAKE:
                return self.real(path, *a, **k)
            getattr(self.pipe, "begin", lambda: None)()
            return self.pipe
        builtins.open = fake_open
        return self.pipe

    def __exit__(self, *a):
        builtins.open = self.real
        return False


def answer(result):
    return lambda req: [{"jsonrpc": "2.0", "id": req["id"], "result": result}]


METHODS = ("application.state.get/v1", "report.snapshot.capture/v1", "file.reload/v1")


def manifest_of(enveloped, extra=()):
    """A bridge manifest in the shape of the first preview builds (a method's arguments are its
    params) or of Desktop 2.159 (they sit in an ``args`` envelope)."""
    properties = {"client": {}, "clientActivityId": {}, "args": {"type": "object"}} if enveloped else {"pageId": {}}
    return {"methods": [{"name": n, "params": {"type": "object", "properties": properties}}
                        for n in METHODS + tuple(extra)]}


def build(enveloped, result, extra=(), refuse=None):
    """A fake bridge of one kind of build: answers bridge.manifest with its manifest and every
    other call with ``result`` (or, with ``refuse``, an error when refuse(params) is true)."""
    def respond(req):
        if req["method"] == "bridge.manifest":
            return [{"jsonrpc": "2.0", "id": req["id"], "result": manifest_of(enveloped, extra)}]
        if refuse and refuse(req["params"]):
            return [{"jsonrpc": "2.0", "id": req["id"],
                     "error": {"code": -32602, "message": "Request arguments are required."}}]
        return [{"jsonrpc": "2.0", "id": req["id"], "result": result}]
    return respond


def first_preview_build():
    """Pretend the manifest was already read and described a first-preview build, so a test of
    something else exchanges exactly one request per call."""
    forget_shapes()
    db.DesktopBridgeClient._manifests[FAKE] = {n: False for n in METHODS}


def forget_shapes():
    """Drop what earlier calls learned about the fake pipe's argument shape."""
    db.DesktopBridgeClient._manifests.clear()
    db.DesktopBridgeClient._learned.clear()


def test_client():
    print("\n== client call / error handling (fake pipe) ==")
    first_preview_build()
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


def test_argument_shapes():
    print("\n== the client sends the argument shape the running build's manifest declares ==")
    envelope = lambda args: {"client": db.CLIENT_NAME, "args": args}  # noqa: E731

    forget_shapes()
    c = db.DesktopBridgeClient(FAKE, timeout=5)
    with PipePatch(FakePipe(build(True, {"success": True}, extra=("report.snapshot.capture/v2",)))) as pipe:
        c.reload_file(reload_model_definition=False)
        check("Desktop 2.159: reload arguments go in the args envelope",
              pipe.request["params"] == envelope({"reloadModelDefinition": False}), str(pipe.request["params"]))
        c.capture_snapshot("p1")
        check("so do screenshot arguments", pipe.request["method"] == "report.snapshot.capture/v1"
              and pipe.request["params"] == envelope({"pageId": "p1", "scale": 1.0}), str(pipe.request))
        c.get_state()
        check("a call without arguments sends an empty envelope", pipe.request["params"] == envelope({}))
        c.capture_snapshot("p1", scale=2, region={"x": 10, "y": 20, "width": 300, "height": 150})
        check("a region is captured with capture/v2",
              pipe.request["method"] == "report.snapshot.capture/v2" and pipe.request["params"] == envelope(
                  {"pageId": "p1", "scale": 2.0, "region": {"x": 10.0, "y": 20.0, "width": 300.0, "height": 150.0}}),
              str(pipe.request))
        asked = [r["method"] for r in pipe.requests].count("bridge.manifest")
        check("the manifest is read once for the pipe, not once per call", asked == 1, str(asked))
        check("a method the manifest lists is supported, another is not",
              c.supports("file.reload/v1") and not c.supports("file.save/v1"))

    forget_shapes()
    with PipePatch(FakePipe(build(False, {"success": True}))) as pipe:
        c.reload_file()
        check("first preview builds: the arguments are the params",
              pipe.request["params"] == {"reloadModelDefinition": True}, str(pipe.request["params"]))
        e = raises("a region on a build without capture/v2 is refused with the reason", db.BridgeError,
                   lambda: c.capture_snapshot("p1", region={"x": 0, "y": 0, "width": 1, "height": 1}), "region")
        check("and nothing was sent for it", pipe.request["method"] == "file.reload/v1" and e is not None)

    # A bridge whose manifest cannot be read: the current shape first, the earlier one once.
    def old_bridge_without_manifest(req):
        if req["method"] == "bridge.manifest":
            return [{"jsonrpc": "2.0", "id": req["id"], "error": {"code": -32601, "message": "MethodNotFound"}}]
        if "args" in req["params"]:
            return [{"jsonrpc": "2.0", "id": req["id"], "error": {"code": -32602, "message": "Invalid params"}}]
        return [{"jsonrpc": "2.0", "id": req["id"], "result": {"success": True}}]

    forget_shapes()
    with PipePatch(FakePipe(old_bridge_without_manifest)) as pipe:
        r = c.reload_file()
        sent = [("args" in q["params"]) for q in pipe.requests if q["method"] == "file.reload/v1"]
        check("with no manifest it falls back to the earlier shape after 'invalid params'",
              r == {"success": True} and sent == [True, False], str(sent))
        c.reload_file()
        sent = [("args" in q["params"]) for q in pipe.requests if q["method"] == "file.reload/v1"]
        check("and remembers which shape worked", sent == [True, False, False], str(sent))

    forget_shapes()
    with PipePatch(FakePipe(build(True, {"ok": 1}, refuse=lambda params: True))) as pipe:
        e = raises("an 'invalid params' answer from a build whose shape is known is not retried",
                   db.BridgeError, c.reload_file, "arguments are required")
        check("one attempt only", [q["method"] for q in pipe.requests].count("file.reload/v1") == 1
              and e is not None and e.code == db.INVALID_PARAMS)

        raises("and the manifest is read afresh for the next call (another build may own the pipe now)",
               db.BridgeError, c.reload_file)
        check("  it was asked again", [q["method"] for q in pipe.requests].count("bridge.manifest") == 2,
              str([q["method"] for q in pipe.requests]))

    # A manifest that could not be read is not remembered: the bridge may only be starting.
    state = {"manifest_fails": 1}

    def slow_starter(req):
        if req["method"] == "bridge.manifest":
            if state["manifest_fails"]:
                state["manifest_fails"] -= 1
                return [{"jsonrpc": "2.0", "id": req["id"], "error": {"code": -32000, "message": "starting"}}]
            return [{"jsonrpc": "2.0", "id": req["id"],
                     "result": manifest_of(True, extra=("report.snapshot.capture/v2",))}]
        return [{"jsonrpc": "2.0", "id": req["id"], "result": {"payload": "AAAA"}}]

    forget_shapes()
    with PipePatch(FakePipe(slow_starter)) as pipe:
        check("with no manifest yet, whether a method exists is unknown, not 'no'",
              c.supports("report.snapshot.capture/v2") is None)
        c.capture_snapshot("p1", region={"x": 0, "y": 0, "width": 5, "height": 5})
        check("so a region capture is tried rather than refused, and the manifest is read once it answers",
              pipe.request["method"] == "report.snapshot.capture/v2" and c.supports("report.snapshot.capture/v2") is True,
              str(pipe.request["method"]))

    def refuses_everything(req):
        if req["method"] == "bridge.manifest":
            return [{"jsonrpc": "2.0", "id": req["id"], "error": {"code": -32601, "message": "MethodNotFound"}}]
        shape = "current" if "args" in req["params"] else "earlier"
        return [{"jsonrpc": "2.0", "id": req["id"], "error": {"code": -32602, "message": f"bad {shape} arguments"}}]

    forget_shapes()
    with PipePatch(FakePipe(refuses_everything)):
        raises("when both shapes are refused, the answer to the current shape is the one reported",
               db.BridgeError, c.reload_file, "bad current arguments")

    # Desktop answers "not ready" for a few seconds after opening a file and during a reload.
    attempts = {"n": 0}

    def not_ready_twice(req):
        if req["method"] == "bridge.manifest":
            return [{"jsonrpc": "2.0", "id": req["id"], "result": manifest_of(True)}]
        attempts["n"] += 1
        if attempts["n"] <= 2:
            code = db.HOST_NOT_READY if attempts["n"] == 1 else db.OPERATION_IN_PROGRESS
            return [{"jsonrpc": "2.0", "id": req["id"], "error": {"code": code, "message": "Host is not ready"}}]
        return [{"jsonrpc": "2.0", "id": req["id"], "result": {"success": True}}]

    pause = db.RETRY_PAUSE
    db.RETRY_PAUSE = 0.01
    try:
        forget_shapes()
        with PipePatch(FakePipe(not_ready_twice)):
            check("a call waits while Desktop is not ready or busy, then succeeds",
                  db.DesktopBridgeClient(FAKE, timeout=5, ready_wait=5).reload_file() == {"success": True}
                  and attempts["n"] == 3, str(attempts))
            attempts["n"] = 0
            e = raises("with no waiting allowed, 'not ready' is reported at once", db.BridgeError,
                       db.DesktopBridgeClient(FAKE, timeout=5, ready_wait=0).reload_file, "not ready")
            check("  after one attempt", attempts["n"] == 1 and e is not None and e.code == db.HOST_NOT_READY)
    finally:
        db.RETRY_PAUSE = pause

    # The first capture of a page after a reload can exceed Desktop's own 30-second limit.
    captures = {"n": 0, "always": False}

    def slow_first_capture(req):
        if req["method"] == "bridge.manifest":
            return [{"jsonrpc": "2.0", "id": req["id"], "result": manifest_of(True)}]
        captures["n"] += 1
        if captures["n"] == 1 or captures["always"]:
            return [{"jsonrpc": "2.0", "id": req["id"], "error": {
                "code": db.CAPTURE_TIMED_OUT, "message": "report.snapshot.capture exceeded timeout of 30s."}}]
        return [{"jsonrpc": "2.0", "id": req["id"], "result": {"payload": "AAAA"}}]

    forget_shapes()
    with PipePatch(FakePipe(slow_first_capture)):
        check("a capture that timed out inside Desktop is tried once more",
              c.capture_snapshot("p1") == {"payload": "AAAA"} and captures["n"] == 2, str(captures))
        captures.update(n=0, always=True)
        e = raises("and only once: a page that cannot be drawn is still an error", db.BridgeError,
                   lambda: c.capture_snapshot("p1"), "exceeded timeout")
        check("  after two attempts", captures["n"] == 2 and e is not None and e.code == db.CAPTURE_TIMED_OUT)
    forget_shapes()


def test_visual_region():
    print("\n== the rectangle of one visual, for a focused screenshot ==")
    with tempfile.TemporaryDirectory() as tmp:
        pbip = os.path.join(tmp, "Sales.pbip")
        visuals = Path(tmp) / "Sales.Report" / "definition" / "pages" / "p1" / "visuals"
        for folder, doc in (("chart", {"name": "chart", "position": {"x": 100, "y": 50, "width": 400, "height": 300}}),
                            ("a1b2", {"name": "kpi", "position": {"x": 2, "y": 3, "width": 120, "height": 60}}),
                            ("broken", {"name": "broken"})):
            (visuals / folder).mkdir(parents=True)
            (visuals / folder / "visual.json").write_text(json.dumps(doc), encoding="utf-8")
        check("found by folder name, widened by the margin",
              db.visual_region(pbip, "p1", "chart", margin=10) == {"x": 90.0, "y": 40.0, "width": 420.0, "height": 320.0})
        check("found by the name inside visual.json; the margin stops at the top and left page edges",
              db.visual_region(pbip, "p1", "KPI", margin=8) == {"x": 0.0, "y": 0.0, "width": 130.0, "height": 71.0},
              str(db.visual_region(pbip, "p1", "KPI", margin=8)))
        check("no such visual, no such page, no position, not a PBIP: None",
              db.visual_region(pbip, "p1", "nope") is None and db.visual_region(pbip, "p9", "chart") is None
              and db.visual_region(pbip, "p1", "broken") is None
              and db.visual_region(os.path.join(tmp, "Sales.pbix"), "p1", "chart") is None)

        # The bridge refuses a region that reaches past the page, so the margin stops there too.
        page = visuals.parent
        (page / "page.json").write_text(json.dumps({"name": "p1", "width": 1280, "height": 720}), encoding="utf-8")
        for folder, pos in (("full", {"x": 0, "y": 0, "width": 1280, "height": 720}),
                            ("corner", {"x": 1100, "y": 600, "width": 180, "height": 120}),
                            ("outside", {"x": 1400, "y": 800, "width": 50, "height": 50})):
            (visuals / folder).mkdir()
            (visuals / folder / "visual.json").write_text(json.dumps({"name": folder, "position": pos}), encoding="utf-8")
        check("a full-page visual gives exactly the page",
              db.visual_region(pbip, "p1", "full", margin=8) == {"x": 0.0, "y": 0.0, "width": 1280.0, "height": 720.0},
              str(db.visual_region(pbip, "p1", "full", margin=8)))
        check("the margin stops at the right and bottom page edges",
              db.visual_region(pbip, "p1", "corner", margin=8) == {"x": 1092.0, "y": 592.0, "width": 188.0, "height": 128.0},
              str(db.visual_region(pbip, "p1", "corner", margin=8)))
        check("a visual that lies outside its page has no region", db.visual_region(pbip, "p1", "outside") is None)

        (visuals / "aslist").mkdir()
        (visuals / "aslist" / "visual.json").write_text("[1, 2]", encoding="utf-8")
        check("a visual.json that is not an object is skipped, not a crash",
              db.visual_region(pbip, "p1", "aslist") is None and db.visual_region(pbip, "p1", "chart") is not None)
        raises("a margin that is not a number is an error, not 'no such visual'", (ValueError, TypeError),
               lambda: db.visual_region(pbip, "p1", "chart", margin="wide"))

        # A visual inside a group is stored relative to the group (Desktop draws a child written
        # at (40, 40) in a group at (300, 200) at page position (340, 240)); groups can nest.
        for folder, doc in (("grp", {"name": "grp", "position": {"x": 300, "y": 200, "width": 400, "height": 300},
                                     "visualGroup": {"displayName": "g"}}),
                            ("inner", {"name": "inner", "parentGroupName": "grp",
                                       "position": {"x": 20, "y": 30, "width": 200, "height": 150},
                                       "visualGroup": {"displayName": "i"}}),
                            ("child", {"name": "child", "parentGroupName": "grp",
                                       "position": {"x": 40, "y": 40, "width": 100, "height": 50}}),
                            ("deep", {"name": "deep", "parentGroupName": "inner",
                                      "position": {"x": 5, "y": 5, "width": 60, "height": 40}}),
                            ("looped", {"name": "looped", "parentGroupName": "looped",
                                        "position": {"x": 7, "y": 9, "width": 10, "height": 10}})):
            (visuals / folder).mkdir()
            (visuals / folder / "visual.json").write_text(json.dumps(doc), encoding="utf-8")
        check("a grouped visual's region includes its group's offset",
              db.visual_region(pbip, "p1", "child", margin=0) == {"x": 340.0, "y": 240.0, "width": 100.0, "height": 50.0},
              str(db.visual_region(pbip, "p1", "child", margin=0)))
        check("nested groups add up",
              db.visual_region(pbip, "p1", "deep", margin=0) == {"x": 325.0, "y": 235.0, "width": 60.0, "height": 40.0},
              str(db.visual_region(pbip, "p1", "deep", margin=0)))
        check("a group that names itself as its parent does not loop for ever",
              db.visual_region(pbip, "p1", "looped", margin=0) == {"x": 14.0, "y": 18.0, "width": 10.0, "height": 10.0},
              str(db.visual_region(pbip, "p1", "looped", margin=0)))

        # A page folder renamed by hand still belongs to the page its page.json names.
        renamed = Path(tmp) / "Sales.Report" / "definition" / "pages" / "Overview folder"
        (renamed / "visuals" / "v").mkdir(parents=True)
        (renamed / "page.json").write_text(json.dumps({"name": "pg7", "width": 800, "height": 600}), encoding="utf-8")
        (renamed / "visuals" / "v" / "visual.json").write_text(
            json.dumps({"name": "v", "position": {"x": 10, "y": 10, "width": 100, "height": 100}}), encoding="utf-8")
        check("the page is found by the name in page.json when its folder is named differently",
              db.visual_region(pbip, "pg7", "v", margin=0) == {"x": 10.0, "y": 10.0, "width": 100.0, "height": 100.0},
              str(db.visual_region(pbip, "pg7", "v", margin=0)))


def test_long_paths_walk():
    print("\n== the path check does not follow links and reads paths of any length ==")
    with tempfile.TemporaryDirectory() as tmp:
        report = os.path.join(tmp, "R.Report")
        pages = os.path.join(report, "definition", "pages")
        os.makedirs(pages)
        with open(os.path.join(pages, "page.json"), "w", encoding="utf-8") as f:
            f.write("{}")
        loop = os.path.join(pages, "loop")
        linked = False
        try:
            os.symlink(pages, loop, target_is_directory=True)
            linked = True
        except (OSError, NotImplementedError):
            if os.name == "nt":
                made = subprocess.run(["cmd", "/c", "mklink", "/J", loop, pages], capture_output=True)
                linked = made.returncode == 0
        if not linked:
            print("  [SKIP] this file system offers neither symlinks nor junctions")
        else:
            limits = (db.MAX_FILE_PATH, db.MAX_DIR_PATH)
            db.MAX_FILE_PATH = db.MAX_DIR_PATH = len(loop) + len(os.sep + "loop") * 2
            try:
                found = db.long_paths([report])
            finally:
                db.MAX_FILE_PATH, db.MAX_DIR_PATH = limits
                if os.path.islink(loop):
                    os.unlink(loop)
                else:
                    os.rmdir(loop)
            check("a folder that links back into the report is not walked round and round",
                  found == [], str([p["path"][-60:] for p in found][:3]))

        seen = []
        real_scandir = db.os.scandir

        def scandir(path):
            seen.append(path)
            return real_scandir(path)
        db.os.scandir = scandir
        try:
            db.long_paths([report])
        finally:
            db.os.scandir = real_scandir
        if os.name == "nt":
            check("on Windows every folder is listed in the long-path form, so one beyond the limit is not missed",
                  bool(seen) and all(p.startswith("\\\\?\\") for p in seen), str(seen[:2]))
            prefixed = db.long_paths(["\\\\?\\" + os.path.abspath(report)])
            limits = db.MAX_DIR_PATH
            db.MAX_DIR_PATH = len(os.path.abspath(report))
            try:
                found = db.long_paths(["\\\\?\\" + os.path.abspath(report)])
            finally:
                db.MAX_DIR_PATH = limits
            check("a root given in the long-path form is measured without its prefix",
                  prefixed == [] and any(p["path"] == os.path.abspath(report) and p["length"] == len(os.path.abspath(report))
                                         for p in found), str(found[:2]))
        else:
            check("off Windows paths are listed as they are", bool(seen) and not any(p.startswith("\\\\?\\") for p in seen))


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


class Instances:
    """Fake Desktop windows for the open-file code: {pid: currentFilePath}. Patches discovery
    and the client, and records what was launched."""

    def __init__(self, files=None, busy=()):
        self.files = dict(files or {})
        self.busy = set(busy)                 # pids that answer "not ready" instead of their state
        self.launched = []
        self.now = 0.0

    def __enter__(self):
        self._disc, self._client = db.discover_bridges, db.DesktopBridgeClient
        db.discover_bridges = lambda: [{"pid": p, "pipe": f"pipe-{p}"} for p in sorted(self.files)]
        owner = self

        class Client:
            def __init__(self, pipe, timeout=60, ready_wait=None):
                self.pid = int(pipe.split("-")[1])

            def get_state(self):
                if self.pid in owner.busy:
                    raise db.BridgeError(db.HOST_NOT_READY, "Host is not ready to accept operations")
                return {"currentFilePath": owner.files[self.pid], "hasUnsavedChanges": False}

        db.DesktopBridgeClient = Client
        return self

    def __exit__(self, *exc):
        db.discover_bridges, db.DesktopBridgeClient = self._disc, self._client

    def clock(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


def test_open_file():
    print("\n== opening a file in Desktop: path limit, already open, wait, honest failure ==")
    with tempfile.TemporaryDirectory() as tmp:
        deep = os.path.join(tmp, "Report", "definition", "pages")
        os.makedirs(deep)
        target = os.path.join(tmp, "Sales.pbip")
        for path in (target, os.path.join(deep, "page.json")):
            with open(path, "w", encoding="utf-8") as f:
                f.write("{}")
        check("ordinary paths are fine", db.long_paths([target, os.path.join(tmp, "Report")]) == [])
        limits = (db.MAX_FILE_PATH, db.MAX_DIR_PATH)
        db.MAX_FILE_PATH, db.MAX_DIR_PATH = len(os.path.join(deep, "page.json")), len(deep) + 1
        try:
            found = db.long_paths([target, os.path.join(tmp, "Report"), None])
        finally:
            db.MAX_FILE_PATH, db.MAX_DIR_PATH = limits
        check("a file at the limit is reported, with its length and the limit it broke",
              [(os.path.basename(p["path"]), p["length"] == p["limit"]) for p in found] == [("page.json", True)],
              str(found))
        db.MAX_DIR_PATH = len(deep)
        try:
            dirs = [p["path"] for p in db.long_paths([os.path.join(tmp, "Report")])]
        finally:
            db.MAX_DIR_PATH = limits[1]
        check("so is a folder at the folder limit", dirs == [deep], str(dirs))
        check("the same file under two spellings is one file",
              db.same_file(target, os.path.join(tmp, ".", "Sales.pbip"))
              and not db.same_file(target, os.path.join(deep, "page.json")))

        with Instances({7: target}) as desk:
            r = db.open_file(target, launch=desk.launched.append, sleep=desk.sleep, clock=desk.clock)
            check("a file that is already open is not opened again",
                  r["opened"] and r["already_open"] and r["pid"] == 7 and desk.launched == [], str(r))

        with Instances({7: ""}) as desk:
            def launch(path):
                desk.launched.append(path)

            def sleep(seconds):
                desk.sleep(seconds)
                if desk.now >= 6:
                    desk.files[21] = target          # the new window finishes loading
            r = db.open_file(target, timeout=60, poll=2, launch=launch, sleep=sleep, clock=desk.clock)
            check("it launches the file and waits until the window has reported it twice in a row",
                  r["opened"] and not r["already_open"] and r["pid"] == 21 and desk.launched == [target]
                  and r["waited_seconds"] == 8.0, str(r))

        # Desktop reports the path a moment before it accepts operations, then goes quiet again.
        with Instances({}) as desk:
            def flaky_launch(path):
                desk.launched.append(path)

            def flaky_sleep(seconds):
                desk.sleep(seconds)
                if desk.now == 4:
                    desk.files[30] = target              # first report
                elif desk.now == 6:
                    desk.busy.add(30)                    # "host is not ready" right after it
                elif desk.now == 10:
                    desk.busy.discard(30)                # ready for good
            r = db.open_file(target, timeout=60, poll=2, launch=flaky_launch, sleep=flaky_sleep, clock=desk.clock)
            check("one report followed by 'not ready' does not count as open",
                  r["opened"] and r["pid"] == 30 and r["waited_seconds"] == 12.0, str(r))

        # The window that holds the file is busy (a reload, a screenshot) when we ask.
        with Instances({7: target}, busy={7}) as desk:
            def recover(seconds):
                desk.sleep(seconds)
                if desk.now >= 4:
                    desk.busy.clear()
            r = db.open_file(target, launch=desk.launched.append, sleep=recover, clock=desk.clock)
            check("a busy window is waited for, not answered with a second window on the same file",
                  r["opened"] and r["already_open"] and r["pid"] == 7 and desk.launched == [], str(r))
        with Instances({7: target, 9: ""}, busy={7}) as desk:
            r = db.open_file(target, timeout=120, launch=desk.launched.append, sleep=desk.sleep, clock=desk.clock)
            check("a window that stays busy stops the launch, and the message names it",
                  r["opened"] is False and desk.launched == [] and r["busy_instances"] == [7]
                  and "pid 7" in r["error"] and "was not started" in r["error"], str(r))
            check("  after the busy wait, not after the whole timeout", r["waited_seconds"] <= db.BUSY_WAIT + 2, str(r))
            found, undecided = db.running_instances(target)
            check("the lookup says which windows could not answer", found is None and undecided == [7])
            check("the simple lookup cannot tell 'busy' from 'not open' and says so in its name only",
                  db.instance_for_file(target) is None)

        with Instances({}) as desk:
            def launch_dialog(path):
                desk.files[33] = ""                  # a window opens but never shows the file
            r = db.open_file(target, timeout=10, poll=2, launch=launch_dialog, sleep=desk.sleep, clock=desk.clock)
            check("a window that never reports the file is a failure that names the pid",
                  r["opened"] is False and r["new_instances"] == [33] and "pid 33" in r["error"]
                  and "error dialog" in r["error"], str(r))
            desk.files.clear()
            desk.now = 0.0
            r = db.open_file(target, timeout=10, poll=2, launch=lambda p: None, sleep=desk.sleep, clock=desk.clock)
            check("no bridge at all points at the preview option",
                  r["opened"] is False and r["new_instances"] == [] and "preview option" in r["error"], str(r))

            def refuse(path):
                raise OSError("no application is associated")
            r = db.open_file(target, launch=refuse, sleep=desk.sleep, clock=desk.clock)
            check("a launch Windows refuses is reported, not raised",
                  r["opened"] is False and "associated" in r["error"], str(r))

            started = []
            real_startfile = getattr(db.os, "startfile", None)
            db.os.startfile = lambda path, **kw: started.append((path, kw))
            try:
                db.open_file(target, timeout=2, poll=2, minimized=True, sleep=desk.sleep, clock=desk.clock)
                db.open_file(target, timeout=2, poll=2, sleep=desk.sleep, clock=desk.clock)
            finally:
                if real_startfile is None:
                    del db.os.startfile
                else:
                    db.os.startfile = real_startfile
            check("minimized starts the window minimized without taking the focus; the default is a normal window",
                  started == [(target, {"show_cmd": db.SHOW_MINIMIZED_NO_FOCUS}), (target, {})], str(started))

        import server
        s = server.PowerBIMCPServer()
        text, res = asyncio.run(s._handle_bridge_open({"path": os.path.join(tmp, "missing.pbip")}))
        check("the tool refuses a path that is not a project", res["opened"] is False and "not a .pbip" in text, text)
        text, res = asyncio.run(s._handle_bridge_open({}))
        check("and a call without a path", res["opened"] is False and "Provide 'path'" in text, text)
        db.MAX_FILE_PATH = len(target)
        try:
            with Instances({}) as desk:
                text, res = asyncio.run(s._handle_bridge_open({"path": target}))
        finally:
            db.MAX_FILE_PATH = limits[0]
        check("the tool refuses an over-long path before starting Desktop",
              res["opened"] is False and "too long" in text and res["long_paths"][0]["path"] == target, text)
        port_lookup = db.msmdsrv_port_for_desktop_pid
        db.msmdsrv_port_for_desktop_pid = lambda pid: 51234      # no look at this machine's real processes
        try:
            with Instances({7: target}) as desk:
                text, res = asyncio.run(s._handle_bridge_open({"path": tmp}))
        finally:
            db.msmdsrv_port_for_desktop_pid = port_lookup
        check("the tool accepts the project folder and reports the running window",
              res["opened"] and res["already_open"] and res["pid"] == 7 and "already open" in text, text)
        check("with the engine port for desktop_connect", res.get("msmdsrv_port") == 51234 and "51234" in text, text)


if __name__ == "__main__":
    print("=" * 70)
    print("  DESKTOP BRIDGE TESTS")
    print("=" * 70)
    for fn in (test_framing, test_client, test_argument_shapes, test_response_matching, test_timeout_is_bounded,
               test_discovery, test_msmdsrv_port_lookup, test_page_resolution, test_visual_region, test_long_paths_walk,
               test_reload_guard_and_status, test_open_file):
        fn()
    print("\n" + "=" * 70)
    if _failures:
        print(f"  {len(_failures)} CHECK(S) FAILED: {', '.join(_failures)}")
        sys.exit(1)
    print("  ALL DESKTOP BRIDGE CHECKS PASSED")
    print("=" * 70)
