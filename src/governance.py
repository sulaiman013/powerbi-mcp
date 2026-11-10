"""
Governance helpers: pure functions over the Power BI Admin Scanner API result, so
cross-workspace lineage / inventory / RLS-coverage answers can be unit tested without
a tenant. The server orchestrates the (admin-gated) Scanner calls and passes the
scanResult JSON in here.
"""
from collections import Counter
from typing import Any, Dict, List, Optional

RLS_PRESENT, RLS_ABSENT, RLS_UNKNOWN = "has_rls", "no_rls", "unknown"


def dataset_rls_state(dataset: Dict[str, Any]) -> str:
    """Whether a scanned dataset has RLS roles: 'has_rls', 'no_rls' or 'unknown'.

    Roles are part of the detailed dataset metadata a scan returns only when it was asked for
    (getInfo datasetSchema=true) and could be retrieved. So a dataset entry with no roles proves
    nothing by itself: it counts as 'no_rls' only when the entry shows that its metadata was
    read (an explicit ``roles`` list, or ``tables``), and as 'unknown' otherwise. Reporting an
    unscanned dataset as unprotected would send people chasing a problem that may not exist.
    """
    roles = dataset.get("roles")
    if roles:
        return RLS_PRESENT
    if dataset.get("schemaRetrievalError"):
        return RLS_UNKNOWN
    if isinstance(roles, list) or "tables" in dataset:
        return RLS_ABSENT
    return RLS_UNKNOWN


def summarize_scan(scan: Dict[str, Any], dataset_name: Optional[str] = None) -> Dict[str, Any]:
    """Summarize a Scanner API scanResult into a tenant inventory + lineage answer.

    Returns counts, RLS coverage (datasets with roles, without roles, and those whose state the
    scan cannot tell), sensitivity-label coverage, and - when dataset_name is given - the
    downstream reports that depend on that dataset.
    """
    workspaces = scan.get("workspaces", []) or []
    datasets: List[Dict[str, Any]] = []
    reports: List[Dict[str, Any]] = []
    for ws in workspaces:
        wn = ws.get("name") or ws.get("id")
        for ds in (ws.get("datasets") or []):
            label = ds.get("sensitivityLabel")
            label_id = label.get("labelId") if isinstance(label, dict) else ds.get("sensitivityLabelId")
            datasets.append({
                "workspace": wn,
                "id": ds.get("id"),
                "name": ds.get("name"),
                "roles": ds.get("roles") or [],
                "rls_state": dataset_rls_state(ds),
                "sensitivityLabel": label_id,
            })
        for rp in (ws.get("reports") or []):
            reports.append({
                "workspace": wn,
                "id": rp.get("id"),
                "name": rp.get("name"),
                "datasetId": rp.get("datasetId"),
            })

    def named(state: str) -> List[str]:
        return [f"{d['workspace']}/{d['name']}" for d in datasets if d["rls_state"] == state]

    no_rls = named(RLS_ABSENT)
    rls_unknown = named(RLS_UNKNOWN)
    unlabeled = [f"{d['workspace']}/{d['name']}" for d in datasets if not d.get("sensitivityLabel")]

    summary: Dict[str, Any] = {
        "workspaces": len(workspaces),
        "datasets": len(datasets),
        "reports": len(reports),
        "datasets_with_rls": named(RLS_PRESENT),
        "datasets_without_rls": no_rls,
        "datasets_rls_unknown": rls_unknown,
        "datasets_without_sensitivity_label": unlabeled,
    }
    if rls_unknown:
        summary["rls_note"] = (
            f"RLS state is unknown for {len(rls_unknown)} of {len(datasets)} dataset(s): the scan "
            "carried no schema for them. Scan with datasetSchema=true (and the tenant setting "
            "'Enhance admin APIs responses with detailed metadata' enabled) to resolve them.")

    if dataset_name:
        targets = [d for d in datasets if d.get("name") == dataset_name]
        downstream = []
        target_ids = {d["id"] for d in targets if d.get("id")}
        for r in reports:
            if r.get("datasetId") in target_ids:
                downstream.append(f"{r['workspace']}/{r['name']}")
        summary["focus_dataset"] = dataset_name
        summary["focus_found_in"] = [d["workspace"] for d in targets]
        summary["downstream_reports"] = downstream

    return summary


def aggregate_activity(events: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Aggregate Admin Activity Events (M365 Power BI schema, PascalCase fields) into a usage
    summary: counts by activity, top users, top viewed reports, distinct users."""
    by_activity: Counter = Counter()
    by_user: Counter = Counter()
    report_views: Counter = Counter()
    for e in events:
        act = e.get("Activity") or e.get("Operation")
        if act:
            by_activity[act] += 1
        user = e.get("UserId")
        if user:
            by_user[user] += 1
        if act and "viewreport" in str(act).lower():
            report = e.get("ReportName") or e.get("ReportId")
            if report:
                report_views[report] += 1
    return {
        "total_events": len(events),
        "distinct_users": len(by_user),
        "by_activity": by_activity.most_common(25),
        "top_users": by_user.most_common(25),
        "top_reports_by_views": report_views.most_common(25),
    }
