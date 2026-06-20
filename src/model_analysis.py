"""
Model analysis: a lightweight Best Practice Analyzer (BPA) and an AI-readiness
auditor that operate on a normalized semantic-model metadata dict.

These are pure functions (no Power BI / .NET dependency) so they can be unit
tested without a live model. The server gathers metadata via INFO.VIEW.* DAX and
passes a normalized dict in here.

Normalized model dict shape:
{
  "tables": [
    {
      "name": str, "is_hidden": bool, "description": str,
      "columns": [ {"name","table","data_type","is_hidden","is_key",
                    "summarize_by","sort_by","description","display_folder",
                    "data_category","is_calculated","expression"} ],
      "measures": [ {"name","table","expression","format_string","description",
                     "display_folder","is_hidden","data_type"} ],
    }
  ],
  "relationships": [ {"from_table","from_column","to_table","to_column",
                      "is_active","cross_filter","from_cardinality","to_cardinality"} ],
}
Every field is optional; rules degrade gracefully when a field is missing.
"""
import re
from typing import Any, Dict, List

SEVERITY_ORDER = {"error": 3, "warning": 2, "info": 1}


def _all_measures(model: Dict[str, Any]) -> List[Dict[str, Any]]:
    out = []
    for t in model.get("tables", []):
        for m in t.get("measures", []):
            m = dict(m)
            m.setdefault("table", t.get("name"))
            out.append(m)
    return out


def _all_columns(model: Dict[str, Any]) -> List[Dict[str, Any]]:
    out = []
    for t in model.get("tables", []):
        for c in t.get("columns", []):
            c = dict(c)
            c.setdefault("table", t.get("name"))
            out.append(c)
    return out


def _truthy(v) -> bool:
    if isinstance(v, str):
        return v.strip().lower() in ("true", "1", "yes")
    return bool(v)


def _empty(v) -> bool:
    return v is None or (isinstance(v, str) and v.strip() == "")


# Division that is not already wrapped in DIVIDE(...). Crude but useful: a '/'
# that is not part of '//' (comment) and the expression lacks DIVIDE.
_DIV_RE = re.compile(r"(?<![/])/(?![/])")


def _rule_float_columns(model):
    hits = []
    for c in _all_columns(model):
        dt = str(c.get("data_type") or "").lower()
        if dt in ("double", "float"):
            hits.append({"object": f"{c.get('table')}[{c.get('name')}]",
                         "detail": "Floating-point column; prefer Fixed Decimal (Currency) for exact aggregation and smaller size."})
    return hits


def _rule_calculated_columns(model):
    hits = []
    for c in _all_columns(model):
        if _truthy(c.get("is_calculated")) or not _empty(c.get("expression")):
            hits.append({"object": f"{c.get('table')}[{c.get('name')}]",
                         "detail": "Calculated column; consider computing in Power Query / source for better compression and refresh."})
    return hits


def _rule_bidirectional(model):
    hits = []
    for r in model.get("relationships", []):
        cf = str(r.get("cross_filter") or "").lower()
        if cf in ("both", "bothdirections", "2"):
            hits.append({"object": f"{r.get('from_table')}[{r.get('from_column')}] -> {r.get('to_table')}[{r.get('to_column')}]",
                         "detail": "Bidirectional cross-filter; can cause ambiguity and slow queries. Use single direction + CROSSFILTER where needed."})
    return hits


def _rule_divide(model):
    hits = []
    for m in _all_measures(model):
        expr = m.get("expression") or ""
        if _DIV_RE.search(expr) and "divide(" not in expr.lower():
            hits.append({"object": f"{m.get('table')}[{m.get('name')}]",
                         "detail": "Uses '/' division; use DIVIDE(numerator, denominator) to handle divide-by-zero safely."})
    return hits


def _rule_iferror(model):
    hits = []
    for m in _all_measures(model):
        if "iferror(" in (m.get("expression") or "").lower():
            hits.append({"object": f"{m.get('table')}[{m.get('name')}]",
                         "detail": "Uses IFERROR; prefer DIVIDE / explicit error handling for performance and clarity."})
    return hits


def _rule_trailing_space(model):
    hits = []
    objs = ([(t.get("name"), "table") for t in model.get("tables", [])]
            + [(f"{m.get('table')}[{m.get('name')}]", m.get("name")) for m in _all_measures(model)]
            + [(f"{c.get('table')}[{c.get('name')}]", c.get("name")) for c in _all_columns(model)])
    for label, name in objs:
        if isinstance(name, str) and name != name.strip():
            hits.append({"object": label, "detail": "Name has leading/trailing whitespace; trim it."})
    return hits


def _rule_measure_no_format(model):
    hits = []
    for m in _all_measures(model):
        if _truthy(m.get("is_hidden")):
            continue
        if _empty(m.get("format_string")):
            hits.append({"object": f"{m.get('table')}[{m.get('name')}]",
                         "detail": "Visible measure has no format string; set one (e.g. '#,##0' or '0.0%')."})
    return hits


def _rule_measure_no_description(model):
    hits = []
    for m in _all_measures(model):
        if _truthy(m.get("is_hidden")):
            continue
        if _empty(m.get("description")):
            hits.append({"object": f"{m.get('table')}[{m.get('name')}]",
                         "detail": "Visible measure has no description; descriptions improve maintainability and Copilot/agent accuracy."})
    return hits


def _rule_column_no_description(model):
    hits = []
    for c in _all_columns(model):
        if _truthy(c.get("is_hidden")):
            continue
        if _empty(c.get("description")):
            hits.append({"object": f"{c.get('table')}[{c.get('name')}]",
                         "detail": "Visible column has no description (lowers AI-readiness)."})
    return hits


def _rule_table_no_relationship(model):
    rels = model.get("relationships", [])
    used = set()
    for r in rels:
        used.add(r.get("from_table"))
        used.add(r.get("to_table"))
    hits = []
    for t in model.get("tables", []):
        name = t.get("name")
        cols = t.get("columns", [])
        # ignore obvious disconnected parameter/measure tables (<=1 column, all measures)
        if len(cols) <= 1 and not t.get("measures"):
            continue
        if name not in used and not _truthy(t.get("is_hidden")):
            hits.append({"object": name,
                         "detail": "Table participates in no relationship; verify it is intentionally disconnected."})
    return hits


def _rule_rel_type_mismatch(model):
    # Build a column type index
    idx = {}
    for c in _all_columns(model):
        idx[(c.get("table"), c.get("name"))] = str(c.get("data_type") or "").lower()
    hits = []
    for r in model.get("relationships", []):
        ft = idx.get((r.get("from_table"), r.get("from_column")))
        tt = idx.get((r.get("to_table"), r.get("to_column")))
        if ft and tt and ft != tt:
            hits.append({"object": f"{r.get('from_table')}[{r.get('from_column')}] -> {r.get('to_table')}[{r.get('to_column')}]",
                         "detail": f"Relationship columns have different data types ({ft} vs {tt}); can cause errors or slow joins."})
    return hits


# Rule registry: id, category, severity, name, description, check
DEFAULT_BPA_RULES = [
    {"id": "PERF_FLOAT_COLUMN", "category": "Performance", "severity": "warning",
     "name": "Avoid floating-point data types", "check": _rule_float_columns},
    {"id": "PERF_CALC_COLUMN", "category": "Performance", "severity": "info",
     "name": "Reduce use of calculated columns", "check": _rule_calculated_columns},
    {"id": "PERF_BIDIRECTIONAL", "category": "Performance", "severity": "warning",
     "name": "Avoid bidirectional relationships", "check": _rule_bidirectional},
    {"id": "DAX_USE_DIVIDE", "category": "DAX", "severity": "warning",
     "name": "Use DIVIDE for division", "check": _rule_divide},
    {"id": "DAX_AVOID_IFERROR", "category": "DAX", "severity": "warning",
     "name": "Avoid IFERROR", "check": _rule_iferror},
    {"id": "NAMING_TRAILING_SPACE", "category": "Naming", "severity": "error",
     "name": "No leading/trailing spaces in names", "check": _rule_trailing_space},
    {"id": "FORMAT_MEASURE_NO_FORMAT", "category": "Formatting", "severity": "warning",
     "name": "Measures should have a format string", "check": _rule_measure_no_format},
    {"id": "MAINT_MEASURE_NO_DESC", "category": "Maintenance", "severity": "info",
     "name": "Measures should have descriptions", "check": _rule_measure_no_description},
    {"id": "MAINT_COLUMN_NO_DESC", "category": "Maintenance", "severity": "info",
     "name": "Visible columns should have descriptions", "check": _rule_column_no_description},
    {"id": "MAINT_TABLE_NO_REL", "category": "Maintenance", "severity": "warning",
     "name": "Tables should participate in a relationship", "check": _rule_table_no_relationship},
    {"id": "ERR_REL_TYPE_MISMATCH", "category": "Error Prevention", "severity": "warning",
     "name": "Relationship columns should share a data type", "check": _rule_rel_type_mismatch},
]


def run_bpa(model: Dict[str, Any], rules=None, categories=None, min_severity="info") -> Dict[str, Any]:
    """Run the Best Practice Analyzer over a normalized model dict.

    Returns {"summary": {...}, "findings": [ {rule_id, name, category, severity, object, detail} ]}.
    """
    rules = rules or DEFAULT_BPA_RULES
    min_rank = SEVERITY_ORDER.get(min_severity, 1)
    cat_filter = set(c.lower() for c in categories) if categories else None

    findings = []
    for rule in rules:
        if cat_filter and rule["category"].lower() not in cat_filter:
            continue
        if SEVERITY_ORDER.get(rule["severity"], 1) < min_rank:
            continue
        try:
            for hit in rule["check"](model):
                findings.append({
                    "rule_id": rule["id"],
                    "name": rule["name"],
                    "category": rule["category"],
                    "severity": rule["severity"],
                    "object": hit.get("object"),
                    "detail": hit.get("detail"),
                })
        except Exception as e:  # a malformed rule must not break the whole scan
            findings.append({"rule_id": rule["id"], "name": rule["name"],
                             "category": rule["category"], "severity": "info",
                             "object": "(rule error)", "detail": str(e)})

    findings.sort(key=lambda f: (-SEVERITY_ORDER.get(f["severity"], 1), f["category"], f["rule_id"]))
    by_sev = {"error": 0, "warning": 0, "info": 0}
    by_cat: Dict[str, int] = {}
    for f in findings:
        by_sev[f["severity"]] = by_sev.get(f["severity"], 0) + 1
        by_cat[f["category"]] = by_cat.get(f["category"], 0) + 1
    return {
        "summary": {"total": len(findings), "by_severity": by_sev, "by_category": by_cat,
                    "rules_run": len([r for r in rules if not cat_filter or r["category"].lower() in cat_filter])},
        "findings": findings,
    }


def audit_ai_readiness(model: Dict[str, Any]) -> Dict[str, Any]:
    """Score how 'AI-ready' a model is (descriptions, formats, hidden technical columns).

    Better-described, well-formatted models produce better Copilot / data-agent / LLM output.
    Returns a 0-100 score, component metrics, and concrete recommendations.
    """
    measures = _all_measures(model)
    columns = _all_columns(model)
    visible_measures = [m for m in measures if not _truthy(m.get("is_hidden"))]
    visible_columns = [c for c in columns if not _truthy(c.get("is_hidden"))]
    tables = model.get("tables", [])

    def pct(num, den):
        return round(100.0 * num / den, 1) if den else 100.0

    m_desc = sum(1 for m in visible_measures if not _empty(m.get("description")))
    m_fmt = sum(1 for m in visible_measures if not _empty(m.get("format_string")))
    c_desc = sum(1 for c in visible_columns if not _empty(c.get("description")))
    t_desc = sum(1 for t in tables if not _empty(t.get("description")))

    metrics = {
        "measures_total": len(visible_measures),
        "measures_with_description_pct": pct(m_desc, len(visible_measures)),
        "measures_with_format_pct": pct(m_fmt, len(visible_measures)),
        "visible_columns_total": len(visible_columns),
        "columns_with_description_pct": pct(c_desc, len(visible_columns)),
        "tables_total": len(tables),
        "tables_with_description_pct": pct(t_desc, len(tables)),
    }

    # Weighted score: measure descriptions/formats matter most for NL-to-DAX grounding.
    score = round(
        0.30 * metrics["measures_with_description_pct"]
        + 0.20 * metrics["measures_with_format_pct"]
        + 0.30 * metrics["columns_with_description_pct"]
        + 0.20 * metrics["tables_with_description_pct"],
        1,
    )

    recs = []
    if metrics["measures_with_description_pct"] < 90 and visible_measures:
        recs.append(f"Add descriptions to measures ({m_desc}/{len(visible_measures)} done) - the strongest lever for NL-to-DAX accuracy.")
    if metrics["measures_with_format_pct"] < 90 and visible_measures:
        recs.append(f"Set format strings on measures ({m_fmt}/{len(visible_measures)} done).")
    if metrics["columns_with_description_pct"] < 70 and visible_columns:
        recs.append(f"Describe visible columns ({c_desc}/{len(visible_columns)} done), especially keys and ambiguous names.")
    if metrics["tables_with_description_pct"] < 80 and tables:
        recs.append(f"Describe tables ({t_desc}/{len(tables)} done) so agents pick the right fact/dimension.")
    if not recs:
        recs.append("Model is well documented. Consider adding synonyms / verified answers for Copilot.")

    grade = "A" if score >= 90 else "B" if score >= 75 else "C" if score >= 60 else "D" if score >= 40 else "F"
    return {"score": score, "grade": grade, "metrics": metrics, "recommendations": recs}
