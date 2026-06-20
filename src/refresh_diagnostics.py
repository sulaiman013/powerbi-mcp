"""
Refresh failure classification + remediation knowledge.

Exposed now as an MCP reference resource (powerbi://reference/refresh-errors) and
reused by the Wave 2 refresh_doctor tool to turn a cryptic Power BI refresh error
into a named root cause and a concrete fix. Pure data + matching (no I/O).
"""
import re
from typing import Any, Dict, List, Optional

# Each rule: id, match (substrings/regex, case-insensitive), cause, remediation.
REFRESH_ERROR_RULES: List[Dict[str, Any]] = [
    {
        "id": "credentials_expired",
        "match": ["credentials", "AccessUnauthorized", "login failed", "token", "401", "oauth"],
        "cause": "Expired or invalid data source credentials / OAuth token.",
        "remediation": "Re-enter the data source credentials (semantic model settings > Data source credentials), "
                       "or refresh the OAuth token / service principal secret. For OAuth sources, re-consent.",
    },
    {
        "id": "capacity_throttle",
        "match": ["exceeded the capacity limit for semantic model refreshes", "capacity limit", "throttl"],
        "cause": "Capacity throttling: too many concurrent/queued refreshes for the capacity.",
        "remediation": "Stagger refresh schedules, reduce parallelism, or scale up the capacity. "
                       "Check the Fabric Capacity Metrics app for overload.",
    },
    {
        "id": "model_eviction",
        "match": ["0xC11C0020", "C11C0020", "evicted", "out of memory", "memory limit"],
        "cause": "Model eviction / out-of-memory: the model exceeded the memory limit (often during refresh of a large/bloated model).",
        "remediation": "Reduce model size (analyze_model_storage to find big/high-cardinality columns), enable incremental "
                       "refresh, remove unused columns (find_unused_objects), or move to a larger capacity.",
    },
    {
        "id": "gateway_unreachable",
        "match": ["gateway", "unable to connect to the data source", "DM_GWPipeline", "datasource is unreachable"],
        "cause": "On-premises data gateway is offline, unreachable, or misconfigured.",
        "remediation": "Verify the gateway service is running and online, the data source is mapped on the gateway, "
                       "and network/firewall to the source is open.",
    },
    {
        "id": "timeout",
        "match": ["timeout", "timed out", "operation was canceled", "took too long"],
        "cause": "Refresh exceeded the time limit (2h shared / 5h Premium) or a query ran too long.",
        "remediation": "Enable incremental refresh, optimize the source query / folding, reduce data volume, "
                       "or run via the XMLA endpoint to bypass the 5h limit on Premium.",
    },
    {
        "id": "source_query_error",
        "match": ["the key didn't match any rows", "column", "syntax error", "expression.error", "DataSource.Error"],
        "cause": "Source schema/query error: a column/table changed, or the Power Query step failed.",
        "remediation": "Open Power Query and fix the failing step (renamed/removed source column, type change, "
                       "or a bad transformation). Re-validate query folding.",
    },
    {
        "id": "resource_governing",
        "match": ["resource governing", "command memory limit", "mashup"],
        "cause": "Per-query resource governing limit hit (Pro/shared capacity memory cap during mashup).",
        "remediation": "Reduce the data pulled per query, simplify transformations, or move to Premium/PPU/Fabric capacity.",
    },
]

CONSECUTIVE_FAILURE_DISABLE_THRESHOLD = 4  # Power BI auto-disables a schedule after 4 consecutive failures.


def classify_refresh_error(error_text: Optional[str]) -> Dict[str, Any]:
    """Classify a refresh error message into a known cause + remediation.

    Returns {"id", "cause", "remediation", "matched"}; falls back to 'unknown'.
    """
    text = (error_text or "").lower()
    if not text.strip():
        return {"id": "none", "cause": "No error text provided.", "remediation": "", "matched": False}
    for rule in REFRESH_ERROR_RULES:
        for token in rule["match"]:
            t = token.lower()
            if t in text or re.search(re.escape(t), text):
                return {"id": rule["id"], "cause": rule["cause"], "remediation": rule["remediation"], "matched": True}
    return {
        "id": "unknown",
        "cause": "Unrecognized refresh error.",
        "remediation": "Inspect the full error JSON; check data source credentials, gateway status, and the Power Query steps.",
        "matched": False,
    }
