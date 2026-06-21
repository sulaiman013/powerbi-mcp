"""
Governance helpers: pure functions over the Power BI Admin Scanner API result, so
cross-workspace lineage / inventory / RLS-coverage answers can be unit tested without
a tenant. The server orchestrates the (admin-gated) Scanner calls and passes the
scanResult JSON in here.
"""
from collections import Counter
from typing import Any, Dict, List, Optional


def summarize_scan(scan: Dict[str, Any], dataset_name: Optional[str] = None) -> Dict[str, Any]:
    """Summarize a Scanner API scanResult into a tenant inventory + lineage answer.

    Returns counts, datasets missing RLS roles, sensitivity-label coverage, and - when
    dataset_name is given - the downstream reports that depend on that dataset.
    """
    workspaces = scan.get("workspaces", []) or []
    datasets: List[Dict[str, Any]] = []
    reports: List[Dict[str, Any]] = []
    for ws in workspaces:
        wn = ws.get("name") or ws.get("id")
        for ds in (ws.get("datasets") or []):
            datasets.append({
                "workspace": wn,
                "id": ds.get("id"),
                "name": ds.get("name"),
                "roles": ds.get("roles") or [],
                "sensitivityLabel": (ds.get("sensitivityLabel") or {}).get("labelId") if isinstance(ds.get("sensitivityLabel"), dict) else ds.get("sensitivityLabelId"),
            })
        for rp in (ws.get("reports") or []):
            reports.append({
                "workspace": wn,
                "id": rp.get("id"),
                "name": rp.get("name"),
                "datasetId": rp.get("datasetId"),
            })

    no_rls = [f"{d['workspace']}/{d['name']}" for d in datasets if not d["roles"]]
    unlabeled = [f"{d['workspace']}/{d['name']}" for d in datasets if not d.get("sensitivityLabel")]

    summary: Dict[str, Any] = {
        "workspaces": len(workspaces),
        "datasets": len(datasets),
        "reports": len(reports),
        "datasets_without_rls": no_rls,
        "datasets_without_sensitivity_label": unlabeled,
    }

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
