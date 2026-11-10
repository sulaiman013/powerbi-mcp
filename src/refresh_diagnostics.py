"""
Refresh failure classification + remediation knowledge.

Exposed now as an MCP reference resource (powerbi://reference/refresh-errors) and
reused by the Wave 2 refresh_doctor tool to turn a cryptic Power BI refresh error
into a named root cause and a concrete fix. Pure data + matching (no I/O).

How a text is classified: every rule lists phrases (``match``, plain substrings) and optionally
``patterns`` (regular expressions, for short tokens that need word boundaries). The rule whose
matched phrase is the LONGEST wins, because a long phrase is specific ("exceeded the capacity
limit for semantic model refreshes") while a short one is incidental ("credentials" mentioned in
passing, "401" inside a request id). Ties go to the rule listed first, and the list runs from
the most specific cause to the most general.
"""
import re
from typing import Any, Dict, List, Optional

# Each rule: id, match (substrings, case-insensitive), optional patterns (regex), cause, remediation.
REFRESH_ERROR_RULES: List[Dict[str, Any]] = [
    {
        "id": "capacity_throttle",
        "match": ["exceeded the capacity limit for semantic model refreshes", "capacity limit",
                  "capacitylimitexceeded", "throttl", "too many requests"],
        "cause": "Capacity throttling: too many concurrent/queued refreshes for the capacity.",
        "remediation": "Stagger refresh schedules, reduce parallelism, or scale up the capacity. "
                       "Check the Fabric Capacity Metrics app for overload.",
    },
    {
        "id": "model_eviction",
        "match": ["0xC11C0020", "C11C0020", "evicted", "out of memory", "memory limit",
                  "not enough memory", "insufficient memory", "outofmemory"],
        "cause": "Model eviction / out-of-memory: the model exceeded the memory limit (often "
                 "during refresh of a large/bloated model).",
        "remediation": "Reduce model size (analyze_model_storage to find big/high-cardinality "
                       "columns), enable incremental refresh, remove unused columns "
                       "(find_unused_objects), or move to a larger capacity.",
    },
    {
        "id": "resource_governing",
        "match": ["resource governing", "command memory limit", "mashup container", "mashup"],
        "cause": "Per-query resource governing limit hit (Pro/shared capacity memory cap during mashup).",
        "remediation": "Reduce the data pulled per query, simplify transformations, or move to "
                       "Premium/PPU/Fabric capacity.",
    },
    {
        "id": "timeout",
        "match": ["timeout", "timed out", "time-out", "operation was canceled",
                  "operation was cancelled", "took too long", "exceeded the maximum allowed time"],
        "cause": "Refresh exceeded the time limit (2h shared / 5h Premium) or a query ran too long.",
        "remediation": "Enable incremental refresh, optimize the source query / folding, reduce data "
                       "volume, or run via the XMLA endpoint to bypass the 5h limit on Premium.",
    },
    {
        "id": "gateway_unreachable",
        "match": ["gateway", "unable to connect to the data source", "DM_GWPipeline",
                  "datasource is unreachable"],
        "cause": "On-premises data gateway is offline, unreachable, or misconfigured.",
        "remediation": "Verify the gateway service is running and online, the data source is mapped "
                       "on the gateway, and network/firewall to the source is open.",
    },
    {
        "id": "credentials_expired",
        "match": ["credentials", "AccessUnauthorized", "login failed", "oauth", "invalid_grant",
                  "token expired", "token has expired", "expired token", "access token", "unauthorized"],
        # A bare "token" or "401" also appears in cancellation messages and request ids.
        "patterns": [r"\b401\b", r"\bAADSTS\d+"],
        "cause": "Expired or invalid data source credentials / OAuth token.",
        "remediation": "Re-enter the data source credentials (semantic model settings > Data source "
                       "credentials), or refresh the OAuth token / service principal secret. For "
                       "OAuth sources, re-consent.",
    },
    {
        "id": "source_query_error",
        "match": ["the key didn't match any rows", "syntax error", "expression.error",
                  "DataSource.Error", "wasn't found", "was not found", "column"],
        "cause": "Source schema/query error: a column/table changed, or the Power Query step failed.",
        "remediation": "Open Power Query and fix the failing step (renamed/removed source column, "
                       "type change, or a bad transformation). Re-validate query folding.",
    },
]

# Power BI auto-disables a schedule after 4 consecutive failures.
CONSECUTIVE_FAILURE_DISABLE_THRESHOLD = 4


def _longest_match(rule: Dict[str, Any], text: str) -> str:
    """The longest phrase of ``rule`` found in ``text`` (already lower-cased), or ''."""
    best = ""
    for token in rule["match"]:
        t = token.lower()
        if len(t) > len(best) and t in text:
            best = t
    for pattern in rule.get("patterns", ()):
        found = re.search(pattern, text, re.IGNORECASE)
        if found and len(found.group(0)) > len(best):
            best = found.group(0)
    return best


def classify_refresh_error(error_text: Optional[str]) -> Dict[str, Any]:
    """Classify a refresh error message into a known cause + remediation.

    Returns {"id", "cause", "remediation", "matched", "matched_on"}; falls back to 'unknown'.
    """
    text = (error_text or "").lower()
    if not text.strip():
        return {"id": "none", "cause": "No error text provided.", "remediation": "", "matched": False,
                "matched_on": None}
    winner: Optional[Dict[str, Any]] = None
    winning_phrase = ""
    for rule in REFRESH_ERROR_RULES:
        phrase = _longest_match(rule, text)
        if len(phrase) > len(winning_phrase):  # strictly longer: an earlier rule keeps a tie
            winner, winning_phrase = rule, phrase
    if winner is not None:
        return {"id": winner["id"], "cause": winner["cause"], "remediation": winner["remediation"],
                "matched": True, "matched_on": winning_phrase}
    return {
        "id": "unknown",
        "cause": "Unrecognized refresh error.",
        "remediation": "Inspect the full error JSON; check data source credentials, gateway status, "
                       "and the Power Query steps.",
        "matched": False,
        "matched_on": None,
    }
