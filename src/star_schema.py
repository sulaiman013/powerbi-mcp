"""
Star-schema / data-warehouse audit for semantic models.

Pure functions over the normalized model dict (see model_analysis for the shape). Classifies
every table by its relationship topology (fact / dimension / date dimension / bridge /
disconnected), then checks the model against dimensional-modeling best practices:

  snowflake chains, bidirectional filters, many-to-many, fact-to-fact joins, a missing or
  unmarked date dimension, measure-less facts, descriptive text attributes stranded on facts,
  and role-playing (inactive) relationships.

Relationship convention (matching the gatherer): "from" is the MANY side, "to" is the ONE side.

Entry point:
    audit_star_schema(model) -> {"classification", "findings", "summary"}
"""
from typing import Any, Dict, List

SEVERITY_ORDER = {"error": 3, "warning": 2, "info": 1}

_DATE_NAME_HINTS = ("date", "calendar", "time", "dim_date", "dimdate")
_KEY_NAME_HINTS = ("key", "id", "code", "sk", "fk")


def _truthy(v) -> bool:
    return v is True or (isinstance(v, str) and v.strip().lower() in ("true", "1", "yes"))


def _is_many_side(rel: Dict[str, Any], table: str) -> bool:
    return rel.get("from_table") == table


def _is_one_side(rel: Dict[str, Any], table: str) -> bool:
    return rel.get("to_table") == table


def _looks_like_key(col_name: str) -> bool:
    n = (col_name or "").lower()
    return any(n.endswith(h) or n.startswith(h) for h in _KEY_NAME_HINTS)


def _has_date_column(table: Dict[str, Any]) -> bool:
    for c in table.get("columns", []):
        dt = (c.get("data_type") or "").lower()
        if "date" in dt:
            return True
    return False


def _is_marked_date_table(table: Dict[str, Any]) -> bool:
    """Marked as a date table: table dataCategory Time, or a date column flagged as key."""
    if (table.get("data_category") or "").lower() == "time":
        return True
    for c in table.get("columns", []):
        dt = (c.get("data_type") or "").lower()
        if "date" in dt and _truthy(c.get("is_key")):
            return True
    return False


def classify_tables(model: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """Classify each table: fact | dimension | date_dimension | bridge | disconnected.

    Topology first (facts sit on the MANY side, dimensions on the ONE side), then shape
    heuristics to separate bridges from facts and spot the date dimension.
    """
    rels = model.get("relationships", [])
    out: Dict[str, Dict[str, Any]] = {}
    for t in model.get("tables", []):
        name = t.get("name", "")
        many = [r for r in rels if _is_many_side(r, name)]
        one = [r for r in rels if _is_one_side(r, name)]
        n_cols = len(t.get("columns", []))
        n_measures = len(t.get("measures", []))
        lname = name.lower()

        if not many and not one:
            cls = "disconnected"
        elif one and not many:
            cls = "dimension"
        elif many and not one:
            # Many side only: a fact, unless it is a thin all-key table joining 2+ dims (bridge).
            all_keyish = n_cols > 0 and all(_looks_like_key(c.get("name", "")) for c in t.get("columns", []))
            if len(many) >= 2 and n_measures == 0 and n_cols <= 3 and all_keyish:
                cls = "bridge"
            else:
                cls = "fact"
        else:
            # Both sides: a snowflaked/mid-chain table. Lean on shape: measures or many
            # outgoing many-side links say fact; otherwise a (snowflaked) dimension.
            cls = "fact" if (n_measures > 0 or len(many) > len(one)) else "dimension"

        if cls == "dimension" and (any(h in lname for h in _DATE_NAME_HINTS) and _has_date_column(t)):
            cls = "date_dimension"

        out[name] = {
            "class": cls,
            "many_side_of": len(many),
            "one_side_of": len(one),
            "columns": n_cols,
            "measures": n_measures,
            "marked_date_table": _is_marked_date_table(t) if cls == "date_dimension" else False,
        }
    return out


def _finding(code, severity, table, message, recommendation) -> Dict[str, Any]:
    return {"code": code, "severity": severity, "table": table,
            "message": message, "recommendation": recommendation}


def audit_star_schema(model: Dict[str, Any]) -> Dict[str, Any]:
    """Audit the model against star-schema best practices. Returns
    {classification, findings, summary} with a 0-100 score and a letter grade."""
    rels = model.get("relationships", [])
    classification = classify_tables(model)
    findings: List[Dict[str, Any]] = []

    def cls(name):
        return classification.get(name, {}).get("class")

    # --- relationship-level checks
    for r in rels:
        ft, tt = r.get("from_table"), r.get("to_table")
        pair = f"{ft} -> {tt}"
        cross = (r.get("cross_filter") or "").lower()
        if cross in ("bothdirections", "both"):
            findings.append(_finding(
                "SS_BIDIRECTIONAL", "warning", pair,
                "Bidirectional cross-filter on a relationship.",
                "Prefer single-direction filtering from dimension to fact; use CROSSFILTER() in "
                "specific measures or a bridge table for the rare genuine need."))
        fc = (r.get("from_cardinality") or "").lower()
        tc = (r.get("to_cardinality") or "").lower()
        if fc == "many" and tc == "many":
            findings.append(_finding(
                "SS_MANY_TO_MANY", "warning", pair,
                "Many-to-many relationship.",
                "Model the shared grain explicitly with a bridge table joined 1:many to both sides."))
        if cls(ft) == "fact" and cls(tt) == "fact":
            findings.append(_finding(
                "SS_FACT_TO_FACT", "warning", pair,
                "Two fact tables are related directly.",
                "Facts should relate through conformed dimensions, not to each other; "
                "consider a shared dimension or drillthrough instead."))
        if cls(ft) == "dimension" and cls(tt) in ("dimension", "date_dimension"):
            findings.append(_finding(
                "SS_SNOWFLAKE", "info", pair,
                "Dimension chained to another dimension (snowflake).",
                "Flatten the outer dimension's attributes into the inner one; VertiPaq "
                "compresses a wide denormalized dimension better than a join chain."))

    inactive = [r for r in rels if not _truthy(r.get("is_active", True))]
    if inactive:
        pairs = ", ".join(f"{r.get('from_table')}->{r.get('to_table')}" for r in inactive[:5])
        findings.append(_finding(
            "SS_ROLE_PLAYING", "info", pairs,
            f"{len(inactive)} inactive relationship(s) (role-playing dimension pattern).",
            "Expose each role via dedicated measures with USERELATIONSHIP(), or duplicate the "
            "dimension per role for a self-describing model."))

    # --- table-level checks
    facts = [n for n, c in classification.items() if c["class"] == "fact"]
    date_dims = [n for n, c in classification.items() if c["class"] == "date_dimension"]
    tables_by_name = {t.get("name"): t for t in model.get("tables", [])}

    if facts and not date_dims:
        findings.append(_finding(
            "SS_NO_DATE_DIM", "warning", None,
            "No date dimension found, but the model has fact tables.",
            "Add a dedicated, marked date table and relate every fact date to it; time "
            "intelligence requires one."))
    for d in date_dims:
        if not classification[d].get("marked_date_table"):
            findings.append(_finding(
                "SS_DATE_NOT_MARKED", "warning", d,
                f"'{d}' looks like a date dimension but is not marked as a date table.",
                "Mark it as a date table (date column as key) so time-intelligence DAX is reliable."))

    for f in facts:
        t = tables_by_name.get(f, {})
        if classification[f]["measures"] == 0:
            findings.append(_finding(
                "SS_FACT_NO_MEASURES", "info", f,
                f"Fact table '{f}' has no explicit measures.",
                "Add explicit measures (or generate a suite) instead of relying on implicit "
                "aggregations; explicit measures carry formats, folders, and descriptions."))
        text_attrs = [c.get("name") for c in t.get("columns", [])
                      if (c.get("data_type") or "").lower() in ("string", "text")
                      and not _looks_like_key(c.get("name", ""))
                      and not _truthy(c.get("is_hidden"))]
        if len(text_attrs) >= 3:
            findings.append(_finding(
                "SS_TEXT_ON_FACT", "info", f,
                f"Fact table '{f}' carries {len(text_attrs)} visible text attributes "
                f"(e.g. {', '.join(text_attrs[:3])}).",
                "Move descriptive text to dimensions; high-cardinality text on a fact bloats "
                "the model and weakens compression."))

    disconnected = [n for n, c in classification.items()
                    if c["class"] == "disconnected" and c["columns"] > 1]
    for n in disconnected:
        findings.append(_finding(
            "SS_DISCONNECTED", "info", n,
            f"Table '{n}' participates in no relationship.",
            "Relate it, hide it, or confirm it is an intentional parameter/what-if table."))

    # --- score: start at 100, subtract by severity
    penalty = {"error": 15, "warning": 8, "info": 3}
    score = 100
    for f in findings:
        score -= penalty.get(f["severity"], 3)
    score = max(0, score)
    grade = ("A" if score >= 90 else "B" if score >= 75 else "C" if score >= 60
             else "D" if score >= 40 else "F")

    by_class: Dict[str, int] = {}
    for c in classification.values():
        by_class[c["class"]] = by_class.get(c["class"], 0) + 1
    findings.sort(key=lambda f: -SEVERITY_ORDER.get(f["severity"], 0))

    return {
        "classification": classification,
        "findings": findings,
        "summary": {
            "score": score,
            "grade": grade,
            "tables_by_class": by_class,
            "relationships": len(rels),
            "findings_by_severity": {
                s: sum(1 for f in findings if f["severity"] == s) for s in ("error", "warning", "info")
            },
        },
    }
