"""
Tests for the pure functions that interpret REST results: governance.summarize_scan (RLS coverage
from a Scanner result) and refresh_diagnostics.classify_refresh_error. No Power BI required.

Run: python tests/test_connectors_governance.py
"""
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))
import governance  # noqa: E402
import refresh_diagnostics as rd  # noqa: E402

_failures = []


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f": {detail}" if detail and not cond else ""))
    if not cond:
        _failures.append(name)


def eq(name, actual, expected):
    check(name, actual == expected, f"got {actual!r}, expected {expected!r}")


def test_rls_state():
    print("\n== a dataset's RLS state: has roles / has none / unknown ==")
    eq("roles listed", governance.dataset_rls_state({"roles": [{"name": "East"}]}), governance.RLS_PRESENT)
    eq("explicit empty roles list", governance.dataset_rls_state({"roles": []}), governance.RLS_ABSENT)
    eq("schema was scanned (tables present) and no roles", governance.dataset_rls_state({"tables": []}),
       governance.RLS_ABSENT)
    eq("nothing about roles or schema: unknown, not 'no RLS'",
       governance.dataset_rls_state({"id": "d", "name": "x"}), governance.RLS_UNKNOWN)
    eq("schema could not be retrieved: unknown",
       governance.dataset_rls_state({"roles": [], "schemaRetrievalError": "Dataset is not refreshed"}),
       governance.RLS_UNKNOWN)
    eq("roles present wins over a retrieval error",
       governance.dataset_rls_state({"roles": [{"name": "r"}], "schemaRetrievalError": "x"}),
       governance.RLS_PRESENT)


def test_summarize_scan():
    print("\n== summarize_scan: unknown is reported as unknown ==")
    scan = {"workspaces": [
        {"id": "w1", "name": "Sales WS", "datasets": [
            {"id": "d1", "name": "Protected", "roles": [{"name": "East"}], "tables": [],
             "sensitivityLabel": {"labelId": "L1"}},
            {"id": "d2", "name": "Open", "roles": [], "tables": []},
            {"id": "d3", "name": "NotScanned"},
            {"id": "d4", "name": "Broken", "schemaRetrievalError": "error"},
        ], "reports": [{"id": "r1", "name": "Rep", "datasetId": "d3"}]},
    ]}
    s = governance.summarize_scan(scan, dataset_name="NotScanned")
    eq("with RLS", s["datasets_with_rls"], ["Sales WS/Protected"])
    eq("known to have no RLS", s["datasets_without_rls"], ["Sales WS/Open"])
    eq("unknown", s["datasets_rls_unknown"], ["Sales WS/NotScanned", "Sales WS/Broken"])
    check("the three groups cover every dataset exactly once",
          len(s["datasets_with_rls"]) + len(s["datasets_without_rls"]) + len(s["datasets_rls_unknown"])
          == s["datasets"] == 4)
    check("a note explains how to resolve the unknowns", "datasetSchema=true" in s.get("rls_note", "")
          and "2 of 4" in s["rls_note"], s.get("rls_note"))
    eq("lineage still works", s["downstream_reports"], ["Sales WS/Rep"])
    eq("sensitivity labels", s["datasets_without_sensitivity_label"],
       ["Sales WS/Open", "Sales WS/NotScanned", "Sales WS/Broken"])

    # A scan made without the dataset schema (the previous request shape): nothing may be
    # reported as "without RLS", because nothing is known.
    bare = {"workspaces": [{"id": "w", "name": "WS", "datasets": [{"id": "d", "name": "HasRLSInReality"}],
                            "reports": []}]}
    s = governance.summarize_scan(bare)
    eq("schema-less scan: no dataset is accused of lacking RLS", s["datasets_without_rls"], [])
    eq("  it is listed as unknown instead", s["datasets_rls_unknown"], ["WS/HasRLSInReality"])

    full = {"workspaces": [{"id": "w", "name": "WS", "datasets": [{"id": "d", "name": "A", "roles": []}]}]}
    check("no note when everything is known", "rls_note" not in governance.summarize_scan(full))
    eq("empty scan", governance.summarize_scan({})["datasets"], 0)
    check("the summary is JSON-serialisable", json.dumps(governance.summarize_scan(scan)) is not None)


def cls(text):
    return rd.classify_refresh_error(text)["id"]


def test_refresh_classification():
    print("\n== classify_refresh_error: the most specific cause wins ==")
    # The three texts the previous first-match-wins ordering got wrong (all became credentials_expired).
    eq("timeout mentioning a cancellation token",
       cls('{"errorCode":"ModelRefresh_ShortMessage_ProcessingError","errorDescription":"The operation was '
           'canceled: the cancellation token was signaled after the 2h limit (timed out)."}'), "timeout")
    eq("capacity limit with '401' inside a request id",
       cls('{"errorCode":"CapacityLimitExceeded","requestId":"7c9e401a-11aa","errorDescription":"You have '
           'exceeded the capacity limit for semantic model refreshes"}'), "capacity_throttle")
    eq("out of memory that also mentions a column and credentials",
       cls('{"errorDescription":"Out of memory while processing column Sales[Amount]; memory limit '
           'reached","note":"credentials ok"}'), "model_eviction")

    cases = [
        ("capacity_throttle", "Refresh was throttled by the capacity"),
        ("capacity_throttle", "Too many requests; capacity limit reached"),
        ("model_eviction", "error 0xC11C0020 model evicted"),
        ("model_eviction", "Not enough memory to complete this operation"),
        ("model_eviction", "The operation hit the memory limit of the capacity and was evicted"),
        ("resource_governing", "Resource Governing: the command memory limit was exceeded"),
        ("timeout", "operation timed out"),
        ("timeout", "Timeout expired. The timeout period elapsed prior to completion"),
        ("timeout", "The refresh took too long and exceeded the maximum allowed time"),
        ("gateway_unreachable", "The gateway is offline"),
        ("gateway_unreachable", "DM_GWPipeline_Gateway_DataSourceAccessError"),
        ("credentials_expired", "AccessUnauthorized: credentials failed"),
        ("credentials_expired", "The credentials provided for the SQL source are invalid"),
        ("credentials_expired", "HTTP 401 returned by the source"),
        ("credentials_expired", "AADSTS700082: The refresh token has expired due to inactivity"),
        ("credentials_expired", "Login failed for user 'svc'"),
        ("source_query_error", "Expression.Error: The key didn't match any rows in the table"),
        ("source_query_error", "The column 'Region' of the table wasn't found"),
        ("source_query_error", "DataSource.Error: Microsoft.Data.Mashup reported a syntax error"),
        ("unknown", "something weird"),
        ("none", ""),
        ("none", None),
    ]
    for expected, text in cases:
        eq(f"{expected}: {str(text)[:44]!r}", cls(text), expected)

    result = rd.classify_refresh_error("exceeded the capacity limit for semantic model refreshes")
    eq("the matched phrase is reported", result["matched_on"],
       "exceeded the capacity limit for semantic model refreshes")
    check("result shape", result["matched"] is True and result["cause"] and result["remediation"])
    check("a request id containing 401 is not an auth error",
          cls("request 5d401a failed in some odd way") == "unknown")
    check("a lone 'token' is not an auth error", cls("the cancellation token fired") == "unknown")
    check("rules stay JSON-serialisable for the reference resource",
          json.dumps(rd.REFRESH_ERROR_RULES) is not None)
    check("every rule has the fields the resource exposes",
          all({"id", "match", "cause", "remediation"} <= set(rule) for rule in rd.REFRESH_ERROR_RULES))
    eq("auto-disable threshold", rd.CONSECUTIVE_FAILURE_DISABLE_THRESHOLD, 4)


if __name__ == "__main__":
    for fn in (test_rls_state, test_summarize_scan, test_refresh_classification):
        fn()
    print()
    if _failures:
        print(f"FAILED: {len(_failures)} check(s): {_failures}")
        sys.exit(1)
    print("ALL GOVERNANCE / REFRESH DIAGNOSTICS TESTS PASSED")
