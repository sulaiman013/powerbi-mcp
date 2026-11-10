"""
Tests for the Power BI REST connector (src/powerbi_rest_connector.py) with a fake HTTP session,
a fake MSAL application and a fake clock: no network, no tenant.

Covers token caching/expiry/refresh, the 401 and 429/503 handling of _request, typed errors with
redacted bodies, full paging, GUID validation of ids, the Scanner helpers and Activity Events.

Run: python tests/test_connectors_rest.py
"""
import email.utils
import io
import logging
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))
import requests  # noqa: E402  (only its exception types are used; nothing is sent)

import powerbi_rest_connector as rest  # noqa: E402

# The connectors log every failure these tests provoke on purpose; keep that off stderr.
logging.getLogger().addHandler(logging.NullHandler())

_failures = []


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f": {detail}" if detail and not cond else ""))
    if not cond:
        _failures.append(name)


def eq(name, actual, expected):
    check(name, actual == expected, f"got {actual!r}, expected {expected!r}")


def raises(name, exc_type, fn, contains=None):
    try:
        fn()
    except exc_type as e:
        ok = contains is None or contains.lower() in str(e).lower()
        check(name, ok, f"raised {type(e).__name__} but message was {str(e)!r}")
        return e
    except Exception as e:  # wrong type: report it, do not crash the suite
        check(name, False, f"raised {type(e).__name__}: {e}")
        return e
    check(name, False, "did not raise")
    return None


SECRET = "S3cr3t~Value.123"
TOKEN_PREFIX = "eyJ0eXAi.TOKEN-"
BASE = "https://api.powerbi.com/v1.0/myorg"


def guid(n):
    return f"{n:08d}-0000-4000-8000-{n:012d}"


W1, W2, D1, D2, SCAN1 = guid(1), guid(2), guid(11), guid(12), guid(99)


class Clock:
    def __init__(self):
        self.now = 1_700_000_000.0
        self.sleeps = []

    def time(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


class FakeApp:
    def __init__(self, expires_in=3600, error=None):
        self.calls, self.dropped, self.expires_in, self.error = 0, 0, expires_in, error

    def acquire_token_for_client(self, scopes):
        self.calls += 1
        if self.error:
            return {"error": "invalid_client",
                    "error_description": f"AADSTS7000215: Invalid client secret provided. secret={SECRET}"}
        return {"access_token": f"{TOKEN_PREFIX}{self.calls}", "expires_in": self.expires_in}

    def remove_tokens_for_client(self):
        self.dropped += 1


class Response:
    def __init__(self, status=200, body=None, headers=None, text=None):
        self.status_code = status
        self._body = body
        self.headers = headers or {}
        self.text = text if text is not None else ("" if body is None else str(body))

    def json(self):
        if self._body is None:
            raise ValueError("no JSON")
        return self._body


class Session:
    """Answers each request from ``handler(call)``; records every call."""

    def __init__(self, handler):
        self.handler = handler
        self.calls = []

    def request(self, method, url, headers=None, json=None, timeout=None):
        call = {"method": method, "url": url, "headers": dict(headers or {}), "json": json,
                "timeout": timeout, "n": len(self.calls) + 1}
        self.calls.append(call)
        result = self.handler(call)
        if isinstance(result, Exception):
            raise result
        return result


def make(handler, app=None, clock=None):
    clock = clock or Clock()
    app = app or FakeApp()
    session = Session(handler)
    connector = rest.PowerBIRestConnector("tenant", "client", SECRET, session=session, msal_app=app,
                                          sleep=clock.sleep, clock=clock.time)
    return connector, session, app, clock


class LogCapture:
    def __enter__(self):
        self.buffer = io.StringIO()
        self.handler = logging.StreamHandler(self.buffer)
        root = logging.getLogger()
        self.previous = root.level
        root.setLevel(logging.DEBUG)
        root.addHandler(self.handler)
        return self

    def __exit__(self, *exc):
        root = logging.getLogger()
        root.removeHandler(self.handler)
        root.setLevel(self.previous)
        return False

    @property
    def text(self):
        return self.buffer.getvalue()


def value(items):
    return Response(200, {"value": items})


def test_token_lifecycle():
    print("\n== token: cached, renewed before expiry, renewed on 401 ==")
    c, s, app, clock = make(lambda call: value([{"id": W1, "name": "WS"}]))
    c.list_workspaces()
    c.list_workspaces()
    eq("one token for two calls", app.calls, 1)
    eq("bearer header carries it", s.calls[-1]["headers"]["Authorization"], f"Bearer {TOKEN_PREFIX}1")
    clock.now += 3600 - 301          # still more than the 5 minute margin left
    c.list_workspaces()
    eq("still cached with more than the margin left", app.calls, 1)
    clock.now += 2                   # inside the margin now
    c.list_workspaces()
    eq("renewed before it expires", (app.calls, s.calls[-1]["headers"]["Authorization"]),
       (2, f"Bearer {TOKEN_PREFIX}2"))
    check("MSAL's own cache was dropped so it cannot return the stale token", app.dropped == 1)

    # 401 on a token we believed valid: refresh once, retry, succeed.
    def expired_once(call):
        return Response(401, text="token expired") if call["headers"]["Authorization"].endswith("-1") \
            else value([{"id": W1, "name": "WS"}])
    c, s, app, clock = make(expired_once)
    eq("call succeeds after a transparent refresh", [w["id"] for w in c.list_workspaces()], [W1])
    eq("exactly one refresh and one retry", (app.calls, len(s.calls)), (2, 2))

    # 401 even with a fresh token: an authorization problem, reported once, no loop.
    c, s, app, clock = make(lambda call: Response(401, text="Unauthorized"))
    e = raises("persistent 401 raises PowerBIAuthError", rest.PowerBIAuthError, c.list_workspaces, "401")
    eq("  after one refresh and one retry only", (app.calls, len(s.calls)), (2, 2))
    eq("  status on the error", getattr(e, "status", None), 401)

    c, s, app, clock = make(lambda call: value([]), app=FakeApp(error=True))
    with LogCapture() as log:
        ok = c.authenticate()
        e = raises("a data call with bad credentials raises (not [])", rest.PowerBIAuthError,
                   c.list_workspaces, "invalid_client")
    check("authenticate() returns False and explains",
          ok is False and "invalid_client" in (c.last_error or ""))
    check("the secret echoed by the identity provider is masked everywhere",
          SECRET not in log.text and SECRET not in str(e) and SECRET not in c.last_error, log.text)
    eq("no request was sent without a token", len(s.calls), 0)

    class Exploding(FakeApp):
        def acquire_token_for_client(self, scopes):
            raise RuntimeError(f"authority lookup failed for client_secret={SECRET}")
    c, s, app, clock = make(lambda call: value([]), app=Exploding())
    e = raises("an MSAL exception becomes PowerBIAuthError", rest.PowerBIAuthError, c.list_workspaces)
    check("  without the secret", SECRET not in str(e), str(e))


def test_throttling():
    print("\n== 429 / 503: bounded retries honouring Retry-After ==")
    def throttle_twice(call):
        if call["n"] <= 2:
            return Response(429, headers={"Retry-After": "7"})
        return value([{"id": W1, "name": "A"}])
    c, s, app, clock = make(throttle_twice)
    eq("succeeds after waiting", len(c.list_workspaces()), 1)
    eq("waited exactly what the service asked", clock.sleeps, [7.0, 7.0])

    c, s, app, clock = make(lambda call: None)
    when = email.utils.formatdate(clock.now + 42, usegmt=True)
    s.handler = lambda call: Response(503, headers={"Retry-After": when}) if call["n"] == 1 else value([])
    c.list_workspaces()
    check("Retry-After as an HTTP date is honoured", len(clock.sleeps) == 1 and 41 <= clock.sleeps[0] <= 43,
          repr(clock.sleeps))

    c, s, app, clock = make(lambda call: Response(429) if call["n"] == 1 else value([]))
    c.list_workspaces()
    check("no Retry-After header: short backoff instead", clock.sleeps == [2.0], repr(clock.sleeps))

    c, s, app, clock = make(lambda call: Response(429, headers={"Retry-After": "nonsense"})
                            if call["n"] == 1 else value([]))
    c.list_workspaces()
    check("an unparsable Retry-After does not crash the call", clock.sleeps == [2.0], repr(clock.sleeps))

    c, s, app, clock = make(lambda call: Response(429, headers={"Retry-After": "1"}, text="slow down"))
    e = raises("persistent throttling raises PowerBIThrottledError", rest.PowerBIThrottledError,
               c.list_workspaces, "429")
    eq("  after MAX_RETRIES retries, not forever", len(s.calls), c.MAX_RETRIES + 1)
    eq("  the error says how long the service asked for", e.retry_after, 1.0)

    c, s, app, clock = make(lambda call: Response(429, headers={"Retry-After": "3600"}))
    e = raises("a wait longer than the ceiling is not slept through", rest.PowerBIThrottledError,
               c.list_workspaces)
    eq("  no sleep, one request", (clock.sleeps, len(s.calls)), ([], 1))
    eq("  retry_after reported to the caller", e.retry_after, 3600.0)

    c, s, app, clock = make(lambda call: Response(429, headers={"Retry-After": "50"}))
    raises("total waiting is bounded too", rest.PowerBIThrottledError, c.list_workspaces)
    check("  never more than MAX_TOTAL_WAIT", sum(clock.sleeps) <= c.MAX_TOTAL_WAIT, repr(clock.sleeps))


def test_errors_and_redaction():
    print("\n== errors: typed, with status and a redacted body ==")
    leaky = f'{{"error":{{"code":"ItemNotFound","message":"token Bearer {TOKEN_PREFIX}1 secret={SECRET}"}}}}'
    c, s, app, clock = make(lambda call: Response(404, text=leaky, headers={"RequestId": "req-123"}))
    with LogCapture() as log:
        e = raises("404 raises PowerBIRestError (not an empty list)", rest.PowerBIRestError,
                   lambda: c.list_datasets(W1), "404")
    check("not mistaken for an auth error", not isinstance(e, rest.PowerBIAuthError))
    eq("status and request id", (e.status, e.request_id), (404, "req-123"))
    check("body kept for diagnosis", "ItemNotFound" in e.body and "ItemNotFound" in str(e), str(e))
    check("token and secret masked in message, body and log",
          all(x not in y for x in (SECRET, TOKEN_PREFIX) for y in (str(e), e.body, log.text)), str(e))
    check("the message names the path, not the full URL with host", "/v1.0/myorg/groups/" in str(e))

    c, s, app, clock = make(lambda call: Response(403, text="forbidden"))
    raises("403 raises PowerBIAuthError", rest.PowerBIAuthError, c.list_workspaces, "403")

    c, s, app, clock = make(lambda call: Response(500, text="boom"))
    e = raises("500 raises immediately", rest.PowerBIRestError, c.list_workspaces, "500")
    eq("  without retrying", len(s.calls), 1)

    c, s, app, clock = make(lambda call: Response(200, body=None, text="<html>"))
    raises("a non-JSON success body is an error", rest.PowerBIRestError, c.list_workspaces, "not JSON")

    c, s, app, clock = make(
        lambda call: requests.exceptions.ConnectionError(f"reset; Bearer {TOKEN_PREFIX}1"))
    with LogCapture() as log:
        e = raises("network failure raises PowerBIRestError", rest.PowerBIRestError, c.list_workspaces,
                   "ConnectionError")
    eq("  GET retried a bounded number of times", len(s.calls), c.MAX_RETRIES + 1)
    check("  token masked", TOKEN_PREFIX not in str(e) and TOKEN_PREFIX not in log.text, str(e))
    check("  original exception not chained", e.__cause__ is None and e.__suppress_context__)

    c, s, app, clock = make(lambda call: requests.exceptions.Timeout("timed out"))
    out = c.trigger_refresh(W1, D1)
    eq("POST is not repeated after a network failure", len(s.calls), 1)
    check("trigger_refresh reports the failure explicitly",
          out["accepted"] is False and "Timeout" in out["message"])

    c, s, app, clock = make(lambda call: value([{"id": W1, "name": "WS"}]))
    c.list_workspaces()
    eq("every request has a timeout", s.calls[0]["timeout"], c.TIMEOUT)

    with LogCapture() as log:
        c, s, app, clock = make(lambda call: Response(429, headers={"Retry-After": "1"})
                                if call["n"] == 1 else value([{"id": W1, "name": "WS"}]))
        c.list_workspaces()
        c.list_datasets(W1)
    check("no token or secret in any log line of a normal run",
          SECRET not in log.text and TOKEN_PREFIX not in log.text, log.text)


def test_paging():
    print("\n== list endpoints page fully ==")
    c, s, app, clock = make(lambda call: value([{"id": W1, "name": "A", "type": "Workspace"}]))
    eq("list_workspaces shape", c.list_workspaces(),
       [{"id": W1, "name": "A", "type": "Workspace", "state": "Active"}])
    eq("non-admin listing is requested as before", s.calls[0]["url"], f"{BASE}/groups")

    def linked(call):
        if call["url"].endswith("/groups"):
            return Response(200, {"value": [{"id": W1, "name": "A"}],
                                  "@odata.nextLink": f"{BASE}/groups?$skiptoken=2"})
        return value([{"id": W2, "name": "B"}])
    c, s, app, clock = make(linked)
    eq("@odata.nextLink is followed", [w["name"] for w in c.list_workspaces()], ["A", "B"])

    c, s, app, clock = make(lambda call: Response(200, {
        "value": [{"id": W1, "name": "A"}], "@odata.nextLink": "https://evil.example/steal"}))
    e = raises("a next link to a foreign host is refused", rest.PowerBIRestError, c.list_workspaces,
               "unexpected host")
    eq("  and the token never went there", [call["url"] for call in s.calls], [f"{BASE}/groups"])
    check("  what was read so far is on the error", [w["id"] for w in e.partial] == [W1])

    total = 12345
    everything = [{"id": guid(1000 + i), "name": f"WS{i}"} for i in range(total)]

    def admin(call):
        query = call["url"].split("?", 1)[1]
        params = dict(p.split("=") for p in query.split("&"))
        top, skip = int(params["$top"]), int(params["$skip"])
        return value(everything[skip:skip + top])
    c, s, app, clock = make(admin)
    got = c.admin_list_workspaces()
    eq("admin listing returns every workspace", len(got), total)
    eq("  in three pages of 5000", [call["url"].split("?")[1] for call in s.calls],
       ["$top=5000&$skip=0", "$top=5000&$skip=5000", "$top=5000&$skip=10000"])
    c, s, app, clock = make(admin)
    eq("top=100 returns the first 100 with one request", (len(c.admin_list_workspaces(100)), len(s.calls)),
       (100, 1))
    eq("  asking for exactly 100", s.calls[0]["url"], f"{BASE}/admin/groups?$top=100&$skip=0")
    c, s, app, clock = make(admin)
    eq("top larger than one page", len(c.admin_list_workspaces(7000)), 7000)
    eq("top=0 asks for nothing", (c.admin_list_workspaces(0), len(s.calls)), ([], 2))

    c, s, app, clock = make(lambda call: value(everything[:3]))  # ignores $skip, always the same page
    eq("an endpoint that ignores $skip does not loop or duplicate",
       len(c.admin_list_workspaces(page_size=3)), 3)
    check("  two requests at most", len(s.calls) == 2, str(len(s.calls)))

    c, s, app, clock = make(lambda call: value([{"id": guid(call["n"]), "name": "x"}] * 1))
    c.MAX_PAGES = 5
    e = raises("a listing that never ends is an error, not a silent cut", rest.PowerBIRestError,
               lambda: c.admin_list_workspaces(page_size=1), "more than 5 pages")
    eq("  with the rows read so far attached", len(e.partial), 5)


def test_ids_are_validated():
    print("\n== ids are GUIDs before they go into a URL path ==")
    c, s, app, clock = make(lambda call: value([]))
    for bad in ("../admin/capacities?x=", "w1", "", None, guid(1) + "/datasets", "{" + guid(1) + "}"):
        raises(f"workspace id {bad!r} refused", rest.InvalidIdError, lambda b=bad: c.list_datasets(b), "GUID")
    raises("dataset id refused", rest.InvalidIdError, lambda: c.get_refresh_history(W1, "d1"))
    raises("workspace id in refresh history refused", rest.InvalidIdError,
           lambda: c.get_refresh_history("x", D1))
    raises("scan id refused", rest.InvalidIdError, lambda: c.admin_get_scan_status("1/../../groups"))
    raises("scan id refused for results", rest.InvalidIdError, lambda: c.admin_get_scan_result("abc"))
    raises("scan id refused for waiting", rest.InvalidIdError, lambda: c.admin_wait_for_scan("abc"))
    raises("workspace ids refused when starting a scan", rest.InvalidIdError,
           lambda: c.admin_start_scans([W1, "../x"]))
    raises("datasources", rest.InvalidIdError, lambda: c.get_datasources(W1, "x"))
    eq("no HTTP request was made for any of them", len(s.calls), 0)
    check("InvalidIdError is a ValueError", issubclass(rest.InvalidIdError, ValueError))
    c.list_datasets(W1.upper())
    eq("a valid GUID (any case) is used as is", s.calls[0]["url"], f"{BASE}/groups/{W1.upper()}/datasets")


def test_resolve_and_refresh():
    print("\n== resolve_dataset / refresh history ==")
    def handler(call):
        url = call["url"]
        if url.endswith("/groups"):
            return value([{"id": W1, "name": "Finance"}, {"id": W2, "name": "Sales"}])
        if url.endswith(f"/groups/{W1}/datasets"):
            return value([{"id": D1, "name": "Model", "isRefreshable": True}, {"id": D2, "name": "Twin"},
                          {"id": guid(13), "name": "Twin"}])
        if "/refreshes" in url:
            return value([{"status": "Failed", "serviceExceptionJson": "{}"}])
        return Response(404, text="nope")
    c, s, app, clock = make(handler)
    eq("resolves names to ids", c.resolve_dataset("Finance", "Model"), (W1, D1, None))
    eq("unknown workspace", c.resolve_dataset("Nope", "Model"),
       (None, None, "Workspace 'Nope' not found (or no access)"))
    eq("unknown dataset", c.resolve_dataset("Finance", "Nope"),
       (None, None, "Dataset 'Nope' not found in workspace 'Finance'"))
    wid, did, err = c.resolve_dataset("Finance", "Twin")
    check("two datasets with one name are not resolved by picking the first",
          wid is None and did is None and "2 datasets" in err, repr(err))
    eq("list_datasets shape", c.list_datasets(W1)[0],
       {"id": D1, "name": "Model", "configuredBy": "Unknown", "isRefreshable": True})
    eq("refresh history", c.get_refresh_history(W1, D1, 5),
       [{"status": "Failed", "serviceExceptionJson": "{}"}])
    eq("  $top passed", s.calls[-1]["url"], f"{BASE}/groups/{W1}/datasets/{D1}/refreshes?$top=5")

    c, s, app, clock = make(lambda call: Response(401, text="expired"))
    raises("a failed lookup raises instead of saying 'not found'", rest.PowerBIAuthError,
           lambda: c.resolve_dataset("Finance", "Model"))

    c, s, app, clock = make(lambda call: Response(202, body={}, headers={
        "Location": f"{BASE}/groups/{W1}/datasets/{D1}/refreshes/{guid(77)}"}))
    out = c.trigger_refresh(W1, D1)
    eq("trigger_refresh accepted", (out["accepted"], out["status_code"], out["request_id"]),
       (True, 202, guid(77)))
    eq("  default body", s.calls[0]["json"], {"notifyOption": "NoNotification"})
    c, s, app, clock = make(lambda call: Response(400, text="bad body"))
    out = c.trigger_refresh(W1, D1, {"type": "full"})
    check("trigger_refresh rejected is explicit", out["accepted"] is False and out["status_code"] == 400
          and "bad body" in out["message"], repr(out))


def test_scanner():
    print("\n== Scanner API: batches of 100, resumable by scan id ==")
    ids = [guid(5000 + i) for i in range(250)]
    posted = []

    def handler(call):
        if call["method"] == "POST":
            posted.append(call)
            return Response(202, {"id": guid(9000 + len(posted)), "status": "NotStarted"})
        return Response(404)
    c, s, app, clock = make(handler)
    scan_ids = c.admin_start_scans(ids + ids[:10])
    eq("three scans for 250 workspaces", scan_ids, [guid(9001), guid(9002), guid(9003)])
    eq("100 / 100 / 50 ids, duplicates dropped", [len(p["json"]["workspaces"]) for p in posted],
       [100, 100, 50])
    eq("every id was sent once, in order", [w for p in posted for w in p["json"]["workspaces"]], ids)
    check("the dataset schema (which carries RLS roles) is requested by default",
          "datasetSchema=true" in posted[0]["url"] and "lineage=true" in posted[0]["url"]
          and "datasetExpressions=false" in posted[0]["url"], posted[0]["url"])
    posted.clear()
    c.admin_post_workspace_info(ids[:2], lineage=False)
    check("the single-scan call keeps its lighter defaults",
          "lineage=false" in posted[0]["url"] and "datasetSchema=false" in posted[0]["url"], posted[0]["url"])
    raises("more than 100 ids in one scan is an error, not a silent cut", ValueError,
           lambda: c.admin_post_workspace_info(ids[:101]), "at most 100")
    raises("no ids", ValueError, lambda: c.admin_start_scans([]))

    posted.clear()

    def second_batch_fails(call):
        posted.append(call)
        if len(posted) == 1:
            return Response(202, {"id": guid(9100)})
        return Response(429, headers={"Retry-After": "900"})
    c, s, app, clock = make(second_batch_fails)
    e = raises("a batch that cannot start raises", rest.PowerBIThrottledError,
               lambda: c.admin_start_scans(ids))
    eq("  with the scans already started, so they can still be collected", e.partial, [guid(9100)])

    c, s, app, clock = make(lambda call: Response(202, {"status": "NotStarted"}))
    raises("a response without a scan id is an error", rest.PowerBIRestError,
           lambda: c.admin_start_scans(ids[:1]), "did not start")

    states = iter(["NotStarted", "Running", "Succeeded"])
    c, s, app, clock = make(lambda call: Response(200, {"id": SCAN1, "status": next(states)}))
    status = c.admin_wait_for_scan(SCAN1, timeout=300, poll_interval=10)
    eq("waits until Succeeded", (status["status"], clock.sleeps), ("Succeeded", [10.0, 10.0]))
    eq("  polling the status endpoint", s.calls[0]["url"], f"{BASE}/admin/workspaces/scanStatus/{SCAN1}")

    c, s, app, clock = make(lambda call: Response(200, {"status": "Failed", "error": {"code": "Boom"}}))
    raises("a failed scan raises ScanFailedError", rest.ScanFailedError,
           lambda: c.admin_wait_for_scan(SCAN1), "Boom")

    c, s, app, clock = make(lambda call: Response(200, {"status": "Running"}))
    e = raises("a scan that does not finish raises ScanTimeoutError", rest.ScanTimeoutError,
               lambda: c.admin_wait_for_scan(SCAN1, timeout=60, poll_interval=15))
    eq("  carrying the scan id to resume with", (e.scan_id, e.state), (SCAN1, "running"))
    check("  after waiting no longer than the timeout", sum(clock.sleeps) <= 60 and len(s.calls) <= 6,
          f"{clock.sleeps} {len(s.calls)}")
    eq("admin_scan_state is lower case", c.admin_scan_state(SCAN1), "running")

    frozen = Clock()
    frozen.sleep = lambda seconds: frozen.sleeps.append(seconds)  # a clock that never advances
    c, s, app, _ = make(lambda call: Response(200, {"status": "Running"}), clock=frozen)
    raises("a stalled clock cannot make the wait endless", rest.ScanTimeoutError,
           lambda: c.admin_wait_for_scan(SCAN1, timeout=30, poll_interval=10))
    check("  bounded number of polls", len(s.calls) <= 5, str(len(s.calls)))

    c, s, app, clock = make(lambda call: Response(200, {"workspaces": [{"id": W1}]}))
    eq("scan result by id", c.admin_get_scan_result(SCAN1), {"workspaces": [{"id": W1}]})
    eq("  from the result endpoint", s.calls[0]["url"], f"{BASE}/admin/workspaces/scanResult/{SCAN1}")
    merged = rest.PowerBIRestConnector.merge_scan_results([
        {"workspaces": [{"id": W1}], "datasourceInstances": [{"datasourceId": "a"}]},
        {"workspaces": [{"id": W2}]}, None])
    eq("results of several scans merge into one", ([w["id"] for w in merged["workspaces"]],
                                                   len(merged["datasourceInstances"])), ([W1, W2], 1))

    c, s, app, clock = make(lambda call: Response(200, [{"id": W1}, {"id": W2}]))
    eq("modified workspace ids (bare array response)", c.admin_list_modified_workspace_ids(), [W1, W2])
    check("  personal and inactive workspaces excluded by default",
          "excludePersonalWorkspaces=True" in s.calls[0]["url"]
          and "excludeInActiveWorkspaces=True" in s.calls[0]["url"], s.calls[0]["url"])
    raises("modified_since is validated", ValueError,
           lambda: c.admin_list_modified_workspace_ids("yesterday&x=1"))


def test_activity_events():
    print("\n== Activity Events: complete or an error, never a silent partial ==")
    start, end = "2026-06-20T00:00:00.000Z", "2026-06-20T00:59:59.999Z"
    cont = ("https://wabi-west-europe-redirect.analysis.windows.net/v1.0/myorg/admin/activityevents"
            "?continuationToken=abc")

    def two_pages(call):
        if call["n"] == 1:
            return Response(200, {"activityEventEntities": [{"Activity": "ViewReport"}] * 3,
                                  "continuationToken": "abc", "continuationUri": cont})
        if call["n"] == 2:  # an empty page that still has a token must not end the loop
            return Response(200, {"activityEventEntities": [], "continuationToken": "def",
                                  "continuationUri": cont + "2"})
        return Response(200, {"activityEventEntities": [{"Activity": "CreateReport"}],
                              "continuationToken": None})
    c, s, app, clock = make(two_pages)
    events = c.admin_get_activity_events(start, end, "Activity eq 'viewreport'")
    eq("all pages collected", len(events), 4)
    eq("first URL quotes the window", s.calls[0]["url"],
       f"{BASE}/admin/activityevents?startDateTime='{start}'&endDateTime='{end}'"
       "&$filter=Activity%20eq%20%27viewreport%27")
    eq("continuation URIs are followed verbatim", [call["url"] for call in s.calls[1:]], [cont, cont + "2"])

    def then_throttled(call):
        if call["n"] == 1:
            return Response(200, {"activityEventEntities": [{"Activity": "ViewReport"}] * 3,
                                  "continuationToken": "abc", "continuationUri": cont})
        return Response(429, headers={"Retry-After": "0"})
    c, s, app, clock = make(then_throttled)
    e = raises("persistent throttling raises instead of returning 3 of N events",
               rest.PowerBIThrottledError, lambda: c.admin_get_activity_events(start, end))
    eq("  with a bounded number of requests (it used to be 1000)", len(s.calls), c.MAX_RETRIES + 2)
    eq("  and the events read so far on the error", len(e.partial), 3)

    c, s, app, clock = make(lambda call: Response(200, {
        "activityEventEntities": [{"Activity": "x"}], "continuationToken": "t", "continuationUri": cont}))
    c.MAX_PAGES = 4
    e = raises("a window that never ends is an error", rest.PowerBIRestError,
               lambda: c.admin_get_activity_events(start, end), "more than 4 pages")
    eq("  with what was read", len(e.partial), 4)

    c, s, app, clock = make(lambda call: Response(200, {
        "activityEventEntities": [], "continuationToken": "t", "continuationUri": "https://evil.example/x"}))
    raises("a continuation to a foreign host is refused", rest.PowerBIRestError,
           lambda: c.admin_get_activity_events(start, end), "unexpected host")
    eq("  the token was sent to Power BI only", len(s.calls), 1)

    c, s, app, clock = make(
        lambda call: Response(200, {"activityEventEntities": [], "continuationToken": "t"}))
    raises("a token without a URI is an error", rest.PowerBIRestError,
           lambda: c.admin_get_activity_events(start, end), "continuationUri")

    for bad in ("2026-06-20", "2026-06-20T00:00:00.000Z'&$filter=x", "now"):
        raises(f"window bound {bad[:22]!r} rejected", ValueError,
               lambda b=bad: c.admin_get_activity_events(b, end))

    c, s, app, clock = make(
        lambda call: Response(200, {"activityEventEntities": [{"Activity": "ViewReport"}]}))
    day = c.admin_get_activity_events_for_day("2026-06-20")
    eq("a day is 24 hourly windows", (len(day), len(s.calls)), (24, 24))
    check("  from 00:00 to 23:59:59.999",
          "startDateTime='2026-06-20T00:00:00.000Z'" in s.calls[0]["url"]
          and "endDateTime='2026-06-20T23:59:59.999Z'" in s.calls[-1]["url"])
    for bad in ("2026-06-20T00:00:00Z'&x=1", "20260620", "yesterday", None):
        raises(f"date {str(bad)[:20]!r} rejected", ValueError,
               lambda b=bad: c.admin_get_activity_events_for_day(b))

    def fails_at_hour_two(call):
        if "T02:" in call["url"]:
            return Response(500, text="backend error")
        return Response(200, {"activityEventEntities": [{"Activity": "ViewReport"}]})
    c, s, app, clock = make(fails_at_hour_two)
    e = raises("a failed hour fails the day", rest.PowerBIRestError,
               lambda: c.admin_get_activity_events_for_day("2026-06-20"), "500")
    eq("  with the two hours already read attached", len(e.partial), 2)


if __name__ == "__main__":
    for fn in (test_token_lifecycle, test_throttling, test_errors_and_redaction, test_paging,
               test_ids_are_validated, test_resolve_and_refresh, test_scanner, test_activity_events):
        fn()
    print()
    if _failures:
        print(f"FAILED: {len(_failures)} check(s): {_failures}")
        sys.exit(1)
    print("ALL REST CONNECTOR TESTS PASSED")
