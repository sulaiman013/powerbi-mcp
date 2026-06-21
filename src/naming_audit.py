"""
Naming-convention audit -> rename plan.

Pure functions that scan a gathered model's object names (tables, columns, measures) and propose
a normalized name for each, producing a rename PLAN that the server's existing rename engine
(batch_rename_* live, or pbip_rename_* for model+report) can apply. We bring the detection layer;
the safe-rename engine is already the strongest part of the server.

Conventions (all configurable): convert snake_case and camelCase to spaced Title Case, strip
warehouse DIM_/FACT_ table prefixes, trim whitespace, collapse double spaces, and (optionally)
expand common abbreviations. Acronyms up to 4 chars (ID, YTD, USD, SKU...) are preserved.

Entry point:
    audit(model, options) -> {"summary": {...}, "plan": [ {object_type, table, old, new, reasons} ]}
"""
import re
from typing import Any, Dict, List, Optional

# Common abbreviation -> expansion (word-wise, case-insensitive match on a whole word).
ABBREVIATIONS = {
    "qty": "Quantity", "amt": "Amount", "nbr": "Number", "no": "Number", "num": "Number",
    "cust": "Customer", "prod": "Product", "desc": "Description", "addr": "Address",
    "dt": "Date", "yr": "Year", "mth": "Month", "cat": "Category", "dept": "Department",
    "mgr": "Manager", "emp": "Employee", "org": "Organization", "txn": "Transaction",
    "avg": "Average", "tot": "Total", "pct": "Percent", "rev": "Revenue",
}
# Short tokens kept verbatim (legitimate acronyms / units).
_KEEP_ACRONYM = re.compile(r"^[A-Z0-9]{1,4}$")
_WAREHOUSE_PREFIX = re.compile(r"^(dim|dimension|fact|facts)[ _]+(.+)$", re.IGNORECASE)


def _titlecase_word(w: str) -> str:
    if not w:
        return w
    if _KEEP_ACRONYM.match(w):   # ID, YTD, USD, SKU, Q1 ...
        return w
    return w[0].upper() + w[1:].lower() if w.isupper() else (w[0].upper() + w[1:])


def suggest_name(name: str, kind: str, *, target_case: str = "title",
                 strip_warehouse_prefixes: bool = True,
                 expand_abbreviations: bool = False) -> (str, List[str]):
    """Return (suggested_name, reasons). reasons is empty if the name already conforms."""
    reasons: List[str] = []
    new = name

    if new != new.strip():
        reasons.append("leading/trailing whitespace")
        new = new.strip()

    if kind == "table" and strip_warehouse_prefixes:
        m = _WAREHOUSE_PREFIX.match(new)
        if m:
            reasons.append("warehouse DIM_/FACT_ prefix")
            new = m.group(2)

    if "_" in new:
        reasons.append("snake_case")
        new = new.replace("_", " ")

    camel = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", " ", new)
    if camel != new:
        reasons.append("camelCase")
        new = camel

    collapsed = re.sub(r"\s+", " ", new).strip()
    if collapsed != new:
        if "double space" not in reasons and "leading/trailing whitespace" not in reasons:
            reasons.append("inconsistent spacing")
        new = collapsed

    if expand_abbreviations:
        words = new.split(" ")
        expanded = [ABBREVIATIONS.get(w.lower(), w) for w in words]
        if expanded != words:
            reasons.append("abbreviation")
            new = " ".join(expanded)

    if target_case == "title":
        titled = " ".join(_titlecase_word(w) for w in new.split(" "))
        if titled != new:
            # Only record casing as a reason if nothing structural already changed it.
            if not reasons:
                reasons.append("casing")
            new = titled

    return new, reasons


def _classify(name: str) -> str:
    """A coarse style label for the convention-consistency summary."""
    if name != name.strip():
        return "untrimmed"
    if "_" in name:
        return "snake_case"
    if re.search(r"[a-z0-9][A-Z]", name):
        return "camelCase"
    if name.isupper() and len(name) > 4:
        return "UPPERCASE"
    return "spaced"


def audit(model: Dict[str, Any], options: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Audit a gathered model and return {summary, plan}."""
    options = options or {}
    scope = set(options.get("scope") or ["tables", "columns", "measures"])
    kw = {
        "target_case": options.get("target_case", "title"),
        "strip_warehouse_prefixes": options.get("strip_warehouse_prefixes", True),
        "expand_abbreviations": options.get("expand_abbreviations", False),
    }
    plan: List[Dict[str, Any]] = []
    styles: Dict[str, int] = {}

    def consider(object_type, table, old):
        styles[_classify(old)] = styles.get(_classify(old), 0) + 1
        new, reasons = suggest_name(old, "table" if object_type == "table" else object_type, **kw)
        if reasons and new and new != old:
            plan.append({"object_type": object_type, "table": table, "old": old,
                         "new": new, "reasons": reasons})

    for t in model.get("tables", []):
        tname = t.get("name", "")
        if "tables" in scope:
            consider("table", None, tname)
        if "columns" in scope:
            for c in t.get("columns", []):
                consider("column", tname, c.get("name", ""))
        if "measures" in scope:
            for m in t.get("measures", []):
                consider("measure", tname, m.get("name", ""))

    by_type: Dict[str, int] = {}
    by_reason: Dict[str, int] = {}
    for p in plan:
        by_type[p["object_type"]] = by_type.get(p["object_type"], 0) + 1
        for r in p["reasons"]:
            by_reason[r] = by_reason.get(r, 0) + 1
    dominant = max(styles, key=styles.get) if styles else None
    return {
        "summary": {
            "total_suggestions": len(plan),
            "by_type": by_type,
            "by_reason": by_reason,
            "observed_styles": styles,
            "dominant_style": dominant,
            "consistent": len([s for s, n in styles.items() if n]) <= 1,
        },
        "plan": plan,
    }
