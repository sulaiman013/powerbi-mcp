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
import re
from typing import Any, Dict, List

SEVERITY_ORDER = {"error": 3, "warning": 2, "info": 1}

_DATE_NAME_TOKENS = {"date", "dates", "calendar", "time"}
_KEY_SUFFIX_RE = re.compile(r"(Key|KEY|Code|CODE|Id|ID|SK|FK)$")


def _truthy(v) -> bool:
    return v is True or (isinstance(v, str) and v.strip().lower() in ("true", "1", "yes"))


def _name_hints_date(lname: str) -> bool:
    """Token-based date-name detection: 'dim_date', 'DimDate', 'Calendar', 'Time' hit;
    'Sentiment', 'Downtime', 'Overtime' do not (no substring matching)."""
    for tok in re.split(r"[^a-z0-9]+", lname or ""):
        if tok in _DATE_NAME_TOKENS:
            return True
        for pre in ("dim", "fact"):
            if tok.startswith(pre) and tok[len(pre):] in _DATE_NAME_TOKENS:
                return True
    return False


def _is_many_side(rel: Dict[str, Any], table: str) -> bool:
    return rel.get("from_table") == table


def _is_one_side(rel: Dict[str, Any], table: str) -> bool:
    return rel.get("to_table") == table


def _looks_like_key(col_name: str) -> bool:
    """Key-ish column names by suffix with a case/underscore boundary, so 'StoreKey',
    'product_id', 'AccountSK' match but 'Idaho', 'Paid', 'Risk', 'Skill' do not."""
    n = (col_name or "").strip()
    low = n.lower()
    return bool(_KEY_SUFFIX_RE.search(n)) or low in ("id", "key", "sk", "fk", "code") \
        or low.endswith(("_id", "_key", "_sk", "_fk", "_code"))


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
            # A relationship-less table that holds measures is a dedicated measure table
            # (the common "_Measures" pattern), not an accidental orphan.
            cls = "measure_table" if n_measures > 0 else "disconnected"
        elif one and not many:
            cls = "dimension"
        elif many and not one:
            # Many side only: a fact, unless it is a bridge - either named as one, or a thin
            # all-key table joining 2+ dims.
            all_keyish = n_cols > 0 and all(_looks_like_key(c.get("name", "")) for c in t.get("columns", []))
            if n_measures == 0 and ("bridge" in lname
                                    or (len(many) >= 2 and n_cols <= 3 and all_keyish)):
                cls = "bridge"
            else:
                cls = "fact"
        else:
            # Both sides: a snowflaked/mid-chain table. Only call it a fact when BOTH signals
            # agree (topology dominance AND measures) - a dim with parked measures, or a dim
            # with two outriggers, must stay a dimension or every downstream fact check misfires.
            cls = "fact" if (len(many) > len(one) and n_measures > 0) else "dimension"

        if cls == "dimension" and ((_name_hints_date(lname) and _has_date_column(t))
                                   or _is_marked_date_table(t)):
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

    has_measure_table = any(c["class"] == "measure_table" for c in classification.values())
    for f in facts:
        t = tables_by_name.get(f, {})
        # A dedicated measure table centralizes the model's measures, so measure-less facts
        # are by design there - do not flag them.
        if classification[f]["measures"] == 0 and not has_measure_table:
            findings.append(_finding(
                "SS_FACT_NO_MEASURES", "info", f,
                f"Fact table '{f}' has no explicit measures.",
                "Add explicit measures (or generate a suite) instead of relying on implicit "
                "aggregations; explicit measures carry formats, folders, and descriptions."))
        rel_cols = {r.get("from_column") for r in rels if r.get("from_table") == f}
        text_attrs = [c.get("name") for c in t.get("columns", [])
                      if (c.get("data_type") or "").lower() in ("string", "text")
                      and not _looks_like_key(c.get("name", ""))
                      and c.get("name") not in rel_cols
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

    # --- score: start at 100, subtract by severity. Info findings often reflect intentional
    # design (role-playing dims, parameter tables), so their combined penalty is capped.
    penalty = {"error": 15, "warning": 8, "info": 2}
    score = 100
    info_penalty = 0
    for f in findings:
        if f["severity"] == "info":
            info_penalty += penalty["info"]
        else:
            score -= penalty.get(f["severity"], 2)
    score -= min(info_penalty, 10)
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
