"""
Power BI REST API Connector
Workspaces, datasets, refresh history and the admin Scanner / Activity APIs, authenticated as a
service principal (MSAL client credentials).

Every call goes through ``_request``, which is the only place that knows about tokens, timeouts,
throttling and error reporting:

* the token is cached with its expiry and renewed before it runs out; a 401 renews it once and
  retries, so a long-lived server does not stop working an hour after it started;
* 429 / 503 are retried a bounded number of times, honouring ``Retry-After`` (seconds or an HTTP
  date) up to a ceiling, after which a ``PowerBIThrottledError`` says how long the service asked
  the caller to wait;
* any other failure raises ``PowerBIRestError`` with the status and a redacted body. Nothing
  here turns an HTTP error into an empty list.

Ids are validated as GUIDs before they are placed in a URL path. Neither the client secret nor
a token is ever written to a log line or an exception message.
"""
import email.utils
import logging
import re
import threading
import time
from typing import Any, Callable, Dict, Iterable, List, Optional
from urllib.parse import quote, urlsplit

import msal
import requests

from connection_strings import redact

logger = logging.getLogger(__name__)

_GUID = re.compile(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")
_ISO_INSTANT = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,7})?Z")
_ISO_DATE = re.compile(r"\d{4}-\d{2}-\d{2}")


class PowerBIRestError(Exception):
    """A Power BI REST call failed.

    ``status`` is the HTTP status (None for a network failure), ``body`` a redacted excerpt of
    the response, ``request_id`` the service's request id when it sent one. ``partial`` holds
    whatever a multi-page read had collected before it failed.
    """

    def __init__(self, message: str, status: Optional[int] = None, body: str = "",
                 request_id: Optional[str] = None, retry_after: Optional[float] = None):
        super().__init__(message)
        self.status = status
        self.body = body
        self.request_id = request_id
        self.retry_after = retry_after
        self.partial: List[Any] = []


class PowerBIAuthError(PowerBIRestError):
    """The service principal could not get a token, or the service rejected it (401 / 403)."""


class PowerBIThrottledError(PowerBIRestError):
    """The service kept answering 429 / 503 beyond the retry budget. ``retry_after`` (seconds)
    is what it last asked for, when it said."""


class ScanFailedError(PowerBIRestError):
    """A Scanner API scan ended in the Failed state."""


class ScanTimeoutError(PowerBIRestError):
    """A scan was still running when the wait ran out. ``scan_id`` lets the caller resume."""

    def __init__(self, message: str, scan_id: str, state: str):
        super().__init__(message)
        self.scan_id = scan_id
        self.state = state


class InvalidIdError(ValueError):
    """A workspace / dataset / scan id is not a GUID."""


def _guid(value: Any, what: str) -> str:
    """``value`` as a GUID string, or InvalidIdError. Ids go into URL paths, where anything else
    (``../admin/...``, a query string) would address a different endpoint."""
    text = "" if value is None else str(value).strip()
    if not _GUID.fullmatch(text):
        raise InvalidIdError(f"{what} must be a GUID, got {text[:60]!r}")
    return text


class PowerBIRestConnector:
    """Power BI connector using REST API for workspace/dataset listing and admin reads"""

    BASE_URL = "https://api.powerbi.com/v1.0/myorg"
    AUTHORITY = "https://login.microsoftonline.com/{tenant_id}"
    SCOPE = ["https://analysis.windows.net/powerbi/api/.default"]

    TIMEOUT = 30            # seconds per HTTP request
    MAX_RETRIES = 4         # retries after throttling or a transient network failure
    MAX_RETRY_WAIT = 60     # longest single wait accepted from the service, in seconds
    MAX_TOTAL_WAIT = 180    # most time one call may spend waiting between retries
    TOKEN_SKEW = 300        # renew a token this many seconds before it expires
    MAX_PAGES = 1000        # safety bound on any paged read; exceeding it is an error
    SCAN_BATCH = 100        # the Scanner API accepts at most 100 workspaces per call
    # Continuation links point at regional hosts; the bearer token only goes to these domains.
    TRUSTED_HOST_SUFFIXES = (".powerbi.com", ".analysis.windows.net")

    def __init__(self, tenant_id: str, client_id: str, client_secret: str,
                 session: Any = None, msal_app: Any = None,
                 sleep: Optional[Callable[[float], None]] = None,
                 clock: Optional[Callable[[], float]] = None):
        """Initialize connector with Azure AD credentials.

        ``session``, ``msal_app``, ``sleep`` and ``clock`` exist for tests; production code
        passes only the three credentials.
        """
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token: Optional[str] = None
        self.last_error: Optional[str] = None
        self._token_expires_at = 0.0
        self._token_lock = threading.Lock()
        self._session = session
        self._app = msal_app
        self._sleep = sleep or time.sleep
        self._clock = clock or time.time

    # ==================== AUTHENTICATION ====================

    def _redact(self, text: Any) -> str:
        return redact(text, [self.client_secret, self.access_token])

    def _token(self, force: bool = False) -> str:
        """A valid access token: the cached one while it has more than TOKEN_SKEW left, else a
        new one. ``force`` discards the cached token (used after a 401)."""
        with self._token_lock:
            now = self._clock()
            if not force and self.access_token and now < self._token_expires_at - self.TOKEN_SKEW:
                return self.access_token
            try:
                if self._app is None:
                    self._app = msal.ConfidentialClientApplication(
                        self.client_id,
                        authority=self.AUTHORITY.format(tenant_id=self.tenant_id),
                        client_credential=self.client_secret,
                    )
                if self.access_token:
                    # MSAL caches client-credential tokens itself and would return the one we
                    # just decided not to trust. (Absent from MSAL older than 1.23.)
                    drop = getattr(self._app, "remove_tokens_for_client", None)
                    if callable(drop):
                        drop()
                result = self._app.acquire_token_for_client(scopes=self.SCOPE)
            except Exception as e:
                raise PowerBIAuthError(
                    f"Authentication error: {self._redact(type(e).__name__ + ': ' + str(e))}") from None
            token = result.get("access_token") if isinstance(result, dict) else None
            if not token:
                info = result if isinstance(result, dict) else {}
                code = info.get("error", "unknown_error")
                detail = info.get("error_description", "")
                self.access_token = None
                message = f"Authentication failed ({code}): {self._redact(detail)}".rstrip(": ")
                raise PowerBIAuthError(message)
            self.access_token = token
            try:
                lifetime = float(result.get("expires_in") or 3600)
            except (TypeError, ValueError):
                lifetime = 3600.0
            self._token_expires_at = now + lifetime
            return token

    def authenticate(self) -> bool:
        """Authenticate using Service Principal and get access token.

        Returns True/False for callers that only want to test the credentials; the reason for a
        failure is in ``last_error``. Data calls authenticate on their own and raise instead.
        """
        try:
            self._token(force=True)
        except PowerBIAuthError as e:
            self.last_error = str(e)
            logger.error(self.last_error)
            return False
        self.last_error = None
        logger.info("Successfully authenticated to Power BI Service")
        return True

    def _get_headers(self) -> Dict[str, str]:
        """Get HTTP headers with authorization"""
        return {
            "Authorization": f"Bearer {self._token()}",
            "Content-Type": "application/json",
        }

    # ==================== TRANSPORT ====================

    def _http(self) -> Any:
        if self._session is None:
            self._session = requests.Session()
        return self._session

    def _trusted_url(self, url: str) -> str:
        """An absolute URL handed back by the service (a continuation or next link), checked
        before the bearer token is sent to it."""
        parts = urlsplit(str(url))
        host = (parts.hostname or "").lower()
        if parts.scheme != "https" or not any(
                host == s.lstrip(".") or host.endswith(s) for s in self.TRUSTED_HOST_SUFFIXES):
            raise PowerBIRestError(f"Refusing to follow a link to an unexpected host: {host or url!r}")
        return str(url)

    def _retry_delay(self, response: Any, attempt: int) -> float:
        """Seconds to wait before retrying: Retry-After (delta seconds or an HTTP date) when the
        service sent a usable one, else exponential backoff."""
        header = None
        headers = getattr(response, "headers", None)
        if headers is not None:
            header = headers.get("Retry-After")
        if header is not None:
            text = str(header).strip()
            try:
                return max(0.0, float(text))
            except ValueError:
                pass
            try:
                when = email.utils.parsedate_to_datetime(text)
                return max(0.0, when.timestamp() - self._clock())
            except (TypeError, ValueError, OverflowError):
                pass
        return float(min(2 ** attempt, 30))

    def _error(self, response: Any, method: str, url: str) -> PowerBIRestError:
        status = int(response.status_code)
        body = self._redact((getattr(response, "text", "") or "")[:2000])[:500]
        headers = getattr(response, "headers", None) or {}
        request_id = headers.get("RequestId") or headers.get("x-ms-request-id")
        path = urlsplit(url).path
        message = f"Power BI REST {method} {path} failed with HTTP {status}"
        if request_id:
            message += f" (request id {request_id})"
        if body.strip():
            message += f": {body.strip()}"
        if status in (401, 403):
            cls = PowerBIAuthError
        elif status in (429, 503):
            cls = PowerBIThrottledError
        else:
            cls = PowerBIRestError
        return cls(message, status=status, body=body, request_id=request_id)

    def _request(self, method: str, url: str, json_body: Any = None,
                 timeout: Optional[float] = None) -> Any:
        """Send one authenticated request and return the response of a successful call.

        Raises PowerBIAuthError, PowerBIThrottledError or PowerBIRestError; never returns a
        response with an error status.
        """
        method = method.upper()
        refreshed = False
        retries = 0
        waited = 0.0
        while True:
            headers = self._get_headers()
            try:
                response = self._http().request(method, url, headers=headers, json=json_body,
                                                timeout=timeout or self.TIMEOUT)
            except (requests.RequestException, OSError) as e:
                # Only reads are repeated: a POST may have been accepted before the link dropped.
                if method == "GET" and retries < self.MAX_RETRIES:
                    retries += 1
                    delay = float(min(2 ** retries, 30))
                    if waited + delay <= self.MAX_TOTAL_WAIT:
                        logger.warning(f"Power BI REST {method} network error "
                                       f"({type(e).__name__}); retry {retries}/{self.MAX_RETRIES}")
                        self._sleep(delay)
                        waited += delay
                        continue
                raise PowerBIRestError(
                    f"Power BI REST {method} {urlsplit(url).path} failed: "
                    f"{self._redact(type(e).__name__ + ': ' + str(e))}") from None

            status = int(response.status_code)
            if status == 401 and not refreshed:
                # Expired or revoked token: get a new one and repeat the call exactly once.
                refreshed = True
                self._token(force=True)
                continue
            if status in (429, 503):
                error = self._error(response, method, url)
                delay = self._retry_delay(response, retries + 1)
                error.retry_after = delay
                if (retries >= self.MAX_RETRIES or delay > self.MAX_RETRY_WAIT
                        or waited + delay > self.MAX_TOTAL_WAIT):
                    raise error
                retries += 1
                logger.warning(f"Power BI REST throttled (HTTP {status}); waiting {delay:.0f}s "
                               f"(retry {retries}/{self.MAX_RETRIES})")
                self._sleep(delay)
                waited += delay
                continue
            if status >= 400:
                raise self._error(response, method, url)
            return response

    def _json(self, response: Any, method: str, url: str) -> Any:
        try:
            return response.json()
        except ValueError:
            raise PowerBIRestError(
                f"Power BI REST {method} {urlsplit(url).path} returned a body that is not JSON",
                status=int(response.status_code)) from None

    def _get_json(self, url: str, timeout: Optional[float] = None) -> Any:
        return self._json(self._request("GET", url, timeout=timeout), "GET", url)

    def _collect(self, path: str, page_size: Optional[int] = None,
                 limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Read a whole collection.

        Follows ``@odata.nextLink`` when the service sends one. With ``page_size`` the endpoint
        is paged with ``$top`` / ``$skip`` until a short page. Items are de-duplicated by id, so
        an endpoint that ignores ``$skip`` ends the loop instead of repeating forever.
        """
        base = f"{self.BASE_URL}{path}"
        joiner = "&" if "?" in base else "?"
        url = f"{base}{joiner}$top={int(page_size)}&$skip=0" if page_size else base
        items: List[Dict[str, Any]] = []
        seen = set()
        skip = 0
        try:
            for _ in range(self.MAX_PAGES):
                data = self._get_json(url)
                page = data.get("value", []) if isinstance(data, dict) else (data or [])
                added = 0
                for item in page:
                    key = item.get("id") if isinstance(item, dict) else None
                    if key is not None:
                        if key in seen:
                            continue
                        seen.add(key)
                    items.append(item)
                    added += 1
                    if limit is not None and len(items) >= limit:
                        return items
                next_link = data.get("@odata.nextLink") if isinstance(data, dict) else None
                if next_link:
                    url = self._trusted_url(next_link)
                elif page_size and len(page) >= page_size and added:
                    skip += len(page)
                    url = f"{base}{joiner}$top={int(page_size)}&$skip={skip}"
                else:
                    return items
        except PowerBIRestError as e:
            e.partial = items
            raise
        error = PowerBIRestError(
            f"Power BI REST GET {path}: more than {self.MAX_PAGES} pages; giving up")
        error.partial = items
        raise error

    # ==================== WORKSPACES / DATASETS ====================

    def list_workspaces(self) -> List[Dict[str, Any]]:
        """
        List all workspaces accessible by the Service Principal

        Raises PowerBIRestError on any failure (an empty list means there are none).
        """
        workspaces = self._collect("/groups")
        logger.info(f"Found {len(workspaces)} workspace(s)")
        return [
            {
                "id": ws["id"],
                "name": ws["name"],
                "type": ws.get("type", "Workspace"),
                "state": ws.get("state", "Active"),
            }
            for ws in workspaces
        ]

    def list_datasets(self, workspace_id: str) -> List[Dict[str, Any]]:
        """
        List all datasets in a workspace

        Raises InvalidIdError for a workspace id that is not a GUID, PowerBIRestError on failure.
        """
        datasets = self._collect(f"/groups/{_guid(workspace_id, 'workspace_id')}/datasets")
        logger.info(f"Found {len(datasets)} dataset(s)")
        return [
            {
                "id": ds["id"],
                "name": ds["name"],
                "configuredBy": ds.get("configuredBy", "Unknown"),
                "isRefreshable": ds.get("isRefreshable", False),
            }
            for ds in datasets
        ]

    # ==================== REFRESH OPERATIONS ====================

    def resolve_dataset(self, workspace_name: str, dataset_name: str):
        """Resolve a workspace+dataset name to (workspace_id, dataset_id).

        Returns (workspace_id, dataset_id, None) or (None, None, error_message) when a name does
        not resolve to exactly one object. A failed REST call raises instead: "not found" must
        mean not found.
        """
        matches = [w for w in self.list_workspaces() if w["name"] == workspace_name]
        if not matches:
            return None, None, f"Workspace '{workspace_name}' not found (or no access)"
        if len(matches) > 1:
            return None, None, f"{len(matches)} workspaces are named '{workspace_name}'; use the id"
        workspace_id = matches[0]["id"]
        found = [d for d in self.list_datasets(workspace_id) if d["name"] == dataset_name]
        if not found:
            return None, None, f"Dataset '{dataset_name}' not found in workspace '{workspace_name}'"
        if len(found) > 1:
            return None, None, (f"{len(found)} datasets are named '{dataset_name}' in workspace "
                                f"'{workspace_name}'; use the dataset id")
        return workspace_id, found[0]["id"], None

    def _dataset_path(self, workspace_id: str, dataset_id: str) -> str:
        return (f"/groups/{_guid(workspace_id, 'workspace_id')}"
                f"/datasets/{_guid(dataset_id, 'dataset_id')}")

    def get_refresh_history(self, workspace_id: str, dataset_id: str,
                            top: int = 20) -> List[Dict[str, Any]]:
        """Get recent refresh history for a dataset (most recent first).

        Each entry: requestId, refreshType, startTime, endTime, status
        (Unknown|Completed|Failed|Disabled), serviceExceptionJson (a JSON string on failure).
        """
        path = self._dataset_path(workspace_id, dataset_id)
        data = self._get_json(f"{self.BASE_URL}{path}/refreshes?$top={int(top)}")
        return data.get("value", []) if isinstance(data, dict) else []

    def get_datasources(self, workspace_id: str, dataset_id: str) -> List[Dict[str, Any]]:
        """Get the data sources bound to a dataset (for gateway/source diagnostics)."""
        return self._collect(f"{self._dataset_path(workspace_id, dataset_id)}/datasources")

    def trigger_refresh(self, workspace_id: str, dataset_id: str,
                        body: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Trigger a dataset refresh. With no body, sends a standard refresh
        ({"notifyOption": "NoNotification"}) that works on Pro. A non-empty body uses the
        enhanced refresh API (Premium/PPU/Fabric) and must NOT include notifyOption.

        Returns {accepted, status_code, request_id, location, message}.
        """
        url = f"{self.BASE_URL}{self._dataset_path(workspace_id, dataset_id)}/refreshes"
        payload = body if body else {"notifyOption": "NoNotification"}
        try:
            response = self._request("POST", url, json_body=payload)
        except PowerBIRestError as e:
            return {"accepted": False, "status_code": e.status, "request_id": e.request_id,
                    "location": None, "message": str(e)}
        location = response.headers.get("Location")
        return {
            "accepted": True,
            "status_code": int(response.status_code),
            "request_id": response.headers.get("x-ms-request-id") or (
                location.rstrip("/").split("/")[-1] if location else None),
            "location": location,
            "message": "Refresh requested",
        }

    # ==================== ADMIN / SCANNER / ACTIVITY (Wave 3) ====================
    # These require Fabric/Power BI admin rights (or an SP in an allowed security group
    # with read-only admin APIs enabled). Reads only; rate-limited by the service.

    def admin_list_workspaces(self, top: Optional[int] = None,
                              page_size: int = 5000) -> List[Dict[str, Any]]:
        """List workspaces tenant-wide (admin). GET /admin/groups, paged with $top/$skip.

        ``top=None`` returns every workspace; a number returns at most that many.
        """
        if top is not None:
            top = int(top)
            if top <= 0:
                return []
        size = max(1, min(int(page_size), 5000))  # the API rejects $top above 5000
        if top is not None:
            size = min(size, top)
        return self._collect("/admin/groups", page_size=size, limit=top)

    def admin_list_modified_workspace_ids(self, modified_since: Optional[str] = None,
                                          exclude_personal: bool = True,
                                          exclude_inactive: bool = True) -> List[str]:
        """Ids of the workspaces the Scanner API can scan. GET /admin/workspaces/modified.

        ``modified_since`` (UTC ISO instant, at most 30 days back) limits the list to workspaces
        changed since then, which is what an incremental scan wants.
        """
        url = (f"{self.BASE_URL}/admin/workspaces/modified"
               f"?excludePersonalWorkspaces={'True' if exclude_personal else 'False'}"
               f"&excludeInActiveWorkspaces={'True' if exclude_inactive else 'False'}")
        if modified_since:
            if not _ISO_INSTANT.fullmatch(str(modified_since)):
                raise ValueError("modified_since must look like 2026-01-31T00:00:00.000Z")
            url += f"&modifiedSince={modified_since}"
        data = self._get_json(url)
        rows = data.get("value", []) if isinstance(data, dict) else (data or [])
        return [str(r["id"]) for r in rows if isinstance(r, dict) and r.get("id")]

    def admin_post_workspace_info(self, workspace_ids: List[str], lineage: bool = True,
                                  datasource_details: bool = False, dataset_schema: bool = False,
                                  dataset_expressions: bool = False,
                                  artifact_users: bool = False) -> Dict[str, Any]:
        """Start a metadata scan for up to 100 workspaces. POST /admin/workspaces/getInfo.
        Returns {id: scanId, status,...}. Use ``admin_start_scans`` for more than 100.

        RLS roles are part of a dataset's schema in the scan result: without
        ``dataset_schema=True`` the result cannot say whether a dataset has roles (see
        governance.summarize_scan, which then reports the RLS state as unknown).
        """
        ids = [_guid(i, "workspace id") for i in workspace_ids]
        if not ids:
            raise ValueError("at least one workspace id is required")
        if len(ids) > self.SCAN_BATCH:
            raise ValueError(f"getInfo accepts at most {self.SCAN_BATCH} workspaces per call; "
                             "use admin_start_scans to scan more")

        def flag(value: bool) -> str:
            return "true" if value else "false"

        url = (f"{self.BASE_URL}/admin/workspaces/getInfo"
               f"?lineage={flag(lineage)}&datasourceDetails={flag(datasource_details)}"
               f"&datasetSchema={flag(dataset_schema)}&datasetExpressions={flag(dataset_expressions)}"
               f"&getArtifactUsers={flag(artifact_users)}")
        return self._json(self._request("POST", url, json_body={"workspaces": ids}), "POST", url)

    def admin_start_scans(self, workspace_ids: Iterable[str], lineage: bool = True,
                          dataset_schema: bool = True, datasource_details: bool = False,
                          dataset_expressions: bool = False,
                          artifact_users: bool = False) -> List[str]:
        """Start scans for any number of workspaces, 100 per scan, and return every scan id.

        The ids are all a caller needs to wait for, fetch or later resume the scans
        (``admin_wait_for_scan`` / ``admin_get_scan_result``). If a later batch cannot be
        started, the error's ``partial`` carries the scan ids already started.
        """
        unique: List[str] = []
        seen = set()
        for raw in workspace_ids:
            wid = _guid(raw, "workspace id")
            if wid.lower() not in seen:
                seen.add(wid.lower())
                unique.append(wid)
        if not unique:
            raise ValueError("at least one workspace id is required")
        scan_ids: List[str] = []
        for start in range(0, len(unique), self.SCAN_BATCH):
            try:
                started = self.admin_post_workspace_info(
                    unique[start:start + self.SCAN_BATCH], lineage=lineage,
                    datasource_details=datasource_details, dataset_schema=dataset_schema,
                    dataset_expressions=dataset_expressions, artifact_users=artifact_users)
                scan_id = started.get("id") if isinstance(started, dict) else None
                if not scan_id:
                    raise PowerBIRestError(f"Scan did not start: {self._redact(started)[:300]}")
            except PowerBIRestError as e:
                e.partial = list(scan_ids)
                raise
            scan_ids.append(str(scan_id))
        return scan_ids

    def admin_get_scan_status(self, scan_id: str) -> Dict[str, Any]:
        """GET /admin/workspaces/scanStatus/{scanId}. status is a string (not a closed enum);
        known values include NotStarted, Running, Succeeded, Failed - match case-insensitively
        and treat anything else as still in progress. On Failed, inspect the .error object."""
        scan_id = _guid(scan_id, "scan_id")
        return self._get_json(f"{self.BASE_URL}/admin/workspaces/scanStatus/{scan_id}")

    def admin_scan_state(self, scan_id: str) -> str:
        """The scan status in lower case ('succeeded', 'failed', 'running', 'notstarted', ...)."""
        return str(self.admin_get_scan_status(scan_id).get("status", "")).lower()

    def admin_wait_for_scan(self, scan_id: str, timeout: float = 300,
                            poll_interval: float = 10) -> Dict[str, Any]:
        """Poll a scan until it succeeds and return its final status object.

        Raises ScanFailedError if it fails and ScanTimeoutError (carrying ``scan_id``) if it is
        still running after ``timeout`` seconds, so the caller can come back to the same scan.
        """
        scan_id = _guid(scan_id, "scan_id")
        interval = max(float(poll_interval), 0.1)
        deadline = self._clock() + float(timeout)
        state = ""
        # The poll count is bounded as well as the clock, so a stalled clock cannot loop forever.
        for _ in range(int(float(timeout) / interval) + 2):
            status = self.admin_get_scan_status(scan_id)
            state = str(status.get("status", "")).lower()
            if state == "succeeded":
                return status
            if state == "failed":
                reason = self._redact(status.get("error") or status)[:500]
                raise ScanFailedError(f"Scan {scan_id} failed: {reason}")
            if self._clock() + interval > deadline:
                break
            self._sleep(interval)
        raise ScanTimeoutError(
            f"Scan {scan_id} is still {state or 'in progress'} after {float(timeout):.0f}s",
            scan_id=scan_id, state=state)

    def admin_get_scan_result(self, scan_id: str) -> Dict[str, Any]:
        """GET /admin/workspaces/scanResult/{scanId}. Returns the full workspace metadata graph.
        Available for 24 hours after the scan succeeded."""
        scan_id = _guid(scan_id, "scan_id")
        return self._get_json(f"{self.BASE_URL}/admin/workspaces/scanResult/{scan_id}",
                              timeout=max(self.TIMEOUT, 120))

    @staticmethod
    def merge_scan_results(results: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
        """Combine the results of several scans into one scanResult-shaped dict."""
        merged: Dict[str, Any] = {"workspaces": [], "datasourceInstances": [],
                                  "misconfiguredDatasourceInstances": []}
        for result in results:
            if not isinstance(result, dict):
                continue
            for key in merged:
                merged[key].extend(result.get(key) or [])
        return merged

    def admin_get_activity_events(self, start_dt_iso: str, end_dt_iso: str,
                                  filter_expr: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get audit Activity Events for a window (same UTC day, <=28 days old) via
        GET /admin/activityevents. Datetimes are single-quoted UTC ISO. Pages by following
        continuationUri verbatim while continuationToken is non-null (a page may be empty but
        still have a token). Throttling is handled by ``_request``.

        Returns every event of the window, or raises: a partial pull is never returned as if it
        were complete (the events read so far are on the exception's ``partial``).
        """
        for label, value in (("start_dt_iso", start_dt_iso), ("end_dt_iso", end_dt_iso)):
            if not _ISO_INSTANT.fullmatch(str(value)):
                raise ValueError(
                    f"{label} must look like 2026-01-31T00:00:00.000Z, got {str(value)[:40]!r}")
        url = (f"{self.BASE_URL}/admin/activityevents"
               f"?startDateTime='{start_dt_iso}'&endDateTime='{end_dt_iso}'")
        if filter_expr:
            url += f"&$filter={quote(filter_expr)}"
        entities: List[Dict[str, Any]] = []
        try:
            for _ in range(self.MAX_PAGES):
                data = self._get_json(url)
                entities.extend(data.get("activityEventEntities", []) or [])
                if not data.get("continuationToken"):
                    return entities
                continuation = data.get("continuationUri")
                if not continuation:
                    raise PowerBIRestError(
                        "Activity events: a continuation token came without a continuationUri")
                # complete, correctly-encoded URL - follow verbatim, no re-encoding
                url = self._trusted_url(continuation)
        except PowerBIRestError as e:
            e.partial = entities
            raise
        error = PowerBIRestError(
            f"Activity events: more than {self.MAX_PAGES} pages in one window; giving up")
        error.partial = entities
        raise error

    def admin_get_activity_events_for_day(self, date: str,
                                          filter_expr: Optional[str] = None) -> List[Dict[str, Any]]:
        """Pull a full UTC day of activity events by looping 24 one-hour windows
        (the raw API allows at most ~1 hour per request). date = 'YYYY-MM-DD'.

        Raises on the first window that fails, with the events gathered so far on ``partial``.
        """
        if not _ISO_DATE.fullmatch(str(date)):
            raise ValueError(f"date must be YYYY-MM-DD, got {str(date)[:40]!r}")
        events: List[Dict[str, Any]] = []
        for h in range(24):
            start = f"{date}T{h:02d}:00:00.000Z"
            end = f"{date}T{h:02d}:59:59.999Z"
            try:
                events.extend(self.admin_get_activity_events(start, end, filter_expr))
            except PowerBIRestError as e:
                e.partial = events + list(e.partial)
                raise
        return events
