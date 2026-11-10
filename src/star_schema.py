"""
Star-schema / data-warehouse audit for semantic models.

Pure functions over the normalized model dict (see model_analysis for the shape). Classifies
every table by its relationship topology (fact / dimension / date dimension / bridge /
measure table / disconnected), then checks the model against dimensional-modeling best practices:

  no relationships at all, snowflake chains, bidirectional filters, many-to-many, one-to-one,
  fact-to-fact joins, a missing, unrelated or unmarked date dimension, measure-less facts,
  descriptive text attributes stranded on facts, and role-playing (inactive) relationships.

Relationship convention (matching the gatherer): "from" is the MANY side, "to" is the ONE side.
The cardinality fields refine that: one/one is a one-to-one relationship (neither side is a
fact because of it) and one/many is read as the reverse direction.

The engine's internal RowNumber-<GUID> column, which the gatherer returns for every table, is
not counted as a column anywhere here.

Score (0-100):
    100 - 15 per error - 8 per warning - min(2 per info, 10)
        - 40 x (share of data tables that take part in no relationship)
    capped at 40 when the model has data tables but no relationship at all, and 0 when there is
    nothing to audit. A flat table or a pile of unrelated tables therefore cannot grade above D.

Entry point:
    audit_star_schema(model) -> {"classification", "findings", "summary"}
"""
from typing import Any, Dict, List, Optional

from model_analysis import _has_date_word, _is_auto_date_table, _is_internal_column, _name_words, _type_key

SEVERITY_ORDER = {"error": 3, "warning": 2, "info": 1}

# A column is key-like when its last word is one of these (StoreKey, product_id, AccountSK, ID).
_KEY_WORDS = {"key", "id", "sk", "fk", "pk", "code", "guid", "uuid"}
# Glued names (customerid, CUSTOMERKEY) are key-like by suffix, except ordinary words that merely
# end the same way.
_ID_LOOKALIKES = {
    "paid", "unpaid", "prepaid", "postpaid", "repaid", "overpaid", "underpaid", "valid", "invalid",
    "void", "avoid", "grid", "liquid", "solid", "rapid", "fluid", "covid", "android", "hybrid",
    "acid", "humid", "rigid", "vivid", "lucid", "arid", "avid", "tepid", "timid", "candid", "squid",
    "pyramid", "orchid", "medicaid", "amid", "said", "laid", "raid", "maid", "stupid", "splendid",
}
_KEY_LOOKALIKES = {"turkey", "monkey", "donkey", "hockey", "jockey", "whiskey", "hokey", "smokey", "lackey"}
_STRUCTURE_CAP = 40


def _truthy(v) -> bool:
    if isinstance(v, str):
        return v.strip().lower() in ("true", "1", "yes")
    return bool(v)          # True/False and the 1/0 some drivers return for booleans


def _is_active(rel: Dict[str, Any]) -> bool:
    """A relationship is active unless it says otherwise: a missing flag is not 'inactive'."""
    v = rel.get("is_active")
    return True if v is None else _truthy(v)


def _fold(v) -> str:
    return str(v or "").strip().casefold()


def _cardinality(rel: Dict[str, Any]) -> str:
    fc, tc = _fold(rel.get("from_cardinality")), _fold(rel.get("to_cardinality"))
    if fc == "one" and tc == "one":
        return "one_to_one"
    if fc == "many" and tc == "many":
        return "many_to_many"
    if fc == "one" and tc == "many":
        return "one_to_many"
    return "many_to_one"


def _many_table(rel: Dict[str, Any]) -> Optional[str]:
    """The table on the many (fact) side, or None for a one-to-one relationship. In a
    many-to-many relationship the 'from' table is the one being filtered, so it plays that role."""
    kind = _cardinality(rel)
    if kind == "one_to_one":
        return None
    return rel.get("to_table") if kind == "one_to_many" else rel.get("from_table")


def _one_table(rel: Dict[str, Any]) -> Optional[str]:
    kind = _cardinality(rel)
    if kind == "one_to_one":
        return None
    return rel.get("from_table") if kind == "one_to_many" else rel.get("to_table")


def _name_hints_date(name: str) -> bool:
    """Whole-word date-name detection in any naming style: 'dim_date', 'DimDate', 'DateDim',
    'dDate', 'Calendar', 'Time' hit; 'Sentiment', 'Downtime', 'Update Log' do not."""
    return _has_date_word(name, extra=("time",))


def _looks_like_key(col_name: str) -> bool:
    """Key-ish column names: the last word is key/id/sk/fk/pk/code ('StoreKey', 'product_id',
    'CUSTOMER_ID', 'AccountSK'), or a glued name ends in id/key ('customerid', 'CUSTOMERKEY').
    Ordinary words do not count in any letter case: 'Paid', 'PAID', 'Risk', 'TASK', 'Valid',
    'Turkey', 'Idaho', 'Skill'."""
    words = _name_words(col_name)
    if not words:
        return False
    last = words[-1]
    if last in _KEY_WORDS:
        return True
    for suffix, lookalikes in (("key", _KEY_LOOKALIKES), ("id", _ID_LOOKALIKES)):
        if last.endswith(suffix) and len(last) > len(suffix) + 2 and last not in lookalikes:
            return True
    return False


def _user_columns(table: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [c for c in (table.get("columns") or [])
            if isinstance(c, dict) and not _is_internal_column(c.get("name"))]


def _has_date_column(table: Dict[str, Any]) -> bool:
    return any("date" in _type_key(c.get("data_type")) for c in _user_columns(table))


def _is_marked_date_table(table: Dict[str, Any]) -> bool:
    """Marked as a date table: table dataCategory Time, or a date column flagged as key."""
    if _fold(table.get("data_category")) == "time":
        return True
    return any("date" in _type_key(c.get("data_type")) and _truthy(c.get("is_key"))
               for c in _user_columns(table))


def _marking_is_known(tables: List[Dict[str, Any]]) -> bool:
    """Did the gatherer supply any 'marked as date table' evidence at all? If every key flag is
    None the model may well be marked; the audit must not claim that it is not."""
    for t in tables:
        if t.get("data_category") is not None:
            return True
        if any(c.get("is_key") is not None for c in _user_columns(t)):
            return True
    return False


def _model_tables(model: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [t for t in (model.get("tables") or [])
            if isinstance(t, dict) and isinstance(t.get("name"), str) and t["name"] != ""]


def _model_relationships(model: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [r for r in (model.get("relationships") or []) if isinstance(r, dict)]


def classify_tables(model: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """Classify each table: fact | dimension | date_dimension | bridge | measure_table | disconnected.

    Topology first (facts sit on the MANY side, dimensions on the ONE side), then shape
    heuristics to separate bridges from facts and spot the date dimension.
    """
    rels = _model_relationships(model)
    out: Dict[str, Dict[str, Any]] = {}
    for t in _model_tables(model):
        name = t["name"]
        many = [r for r in rels if _many_table(r) == name]
        one = [r for r in rels if _one_table(r) == name]
        peers = [r for r in rels if _cardinality(r) == "one_to_one"
                 and name in (r.get("from_table"), r.get("to_table"))]
        cols = _user_columns(t)
        n_cols = len(cols)
        n_measures = len(t.get("measures") or [])

        if not many and not one and not peers:
            # A relationship-less table that holds measures is a dedicated measure table
            # (the common "_Measures" pattern), not an accidental orphan.
            cls = "measure_table" if n_measures > 0 else "disconnected"
        elif not many:
            # One side only, or only one-to-one partners: same grain as a dimension.
            cls = "dimension"
        elif not one:
            # Many side only: a fact, unless it is a bridge - either named as one, or a thin
            # all-key table joining 2+ dims.
            all_keyish = n_cols > 0 and all(_looks_like_key(c.get("name") or "") for c in cols)
            if n_measures == 0 and ("bridge" in _name_words(name)
                                    or (len(many) >= 2 and n_cols <= 3 and all_keyish)):
                cls = "bridge"
            else:
                cls = "fact"
        else:
            # Both sides: a snowflaked/mid-chain table. Only call it a fact when BOTH signals
            # agree (topology dominance AND measures) - a dim with parked measures, or a dim
            # with two outriggers, must stay a dimension or every downstream fact check misfires.
            cls = "fact" if (len(many) > len(one) and n_measures > 0) else "dimension"

        if cls == "dimension" and ((_name_hints_date(name) and _has_date_column(t))
                                   or _is_marked_date_table(t)):
            cls = "date_dimension"

        out[name] = {
            "class": cls,
            "many_side_of": len(many),
            "one_side_of": len(one),
            "one_to_one_with": len(peers),
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
    {classification, findings, summary} with a 0-100 score and a letter grade (see the module
    docstring for the formula)."""
    tables = _model_tables(model)
    rels = _model_relationships(model)
    classification = classify_tables(model)
    tables_by_name = {t["name"]: t for t in tables}
    findings: List[Dict[str, Any]] = []

    def cls(name):
        return classification.get(name, {}).get("class")

    # --- relationship-level checks
    for r in rels:
        ft, tt = r.get("from_table"), r.get("to_table")
        pair = f"{ft} -> {tt}"
        kind = _cardinality(r)
        if _fold(r.get("cross_filter")) in ("bothdirections", "both", "2") and kind != "one_to_one":
            findings.append(_finding(
                "SS_BIDIRECTIONAL", "warning", pair,
                "Bidirectional cross-filter on a relationship.",
                "Prefer single-direction filtering from dimension to fact; use CROSSFILTER() in "
                "specific measures or a bridge table for the rare genuine need."))
        if kind == "many_to_many":
            findings.append(_finding(
                "SS_MANY_TO_MANY", "warning", pair,
                "Many-to-many relationship.",
                "Model the shared grain explicitly with a bridge table joined 1:many to both sides."))
        if kind == "one_to_one":
            findings.append(_finding(
                "SS_ONE_TO_ONE", "info", pair,
                "One-to-one relationship: two tables at the same grain.",
                "Merge the two tables unless they are split on purpose (security, refresh, width)."))
            continue
        many_t, one_t = _many_table(r), _one_table(r)
        if cls(many_t) == "fact" and cls(one_t) == "fact":
            findings.append(_finding(
                "SS_FACT_TO_FACT", "warning", pair,
                "Two fact tables are related directly.",
                "Facts should relate through conformed dimensions, not to each other; "
                "consider a shared dimension or drillthrough instead."))
        if cls(many_t) == "dimension" and cls(one_t) in ("dimension", "date_dimension"):
            findings.append(_finding(
                "SS_SNOWFLAKE", "info", pair,
                "Dimension chained to another dimension (snowflake).",
                "Flatten the outer dimension's attributes into the inner one; VertiPaq "
                "compresses a wide denormalized dimension better than a join chain."))

    inactive = [r for r in rels if not _is_active(r)]
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
    marking_known = _marking_is_known(tables)

    if facts and not date_dims:
        # A date table that exists but is not related to any fact is a different problem from
        # having none: say which one it is.
        candidates = [t["name"] for t in tables
                      if cls(t["name"]) not in ("fact", "bridge") and not _is_auto_date_table(t["name"])
                      and ((_name_hints_date(t["name"]) and _has_date_column(t)) or _is_marked_date_table(t))]
        if candidates:
            findings.append(_finding(
                "SS_DATE_NOT_RELATED", "warning", ", ".join(candidates[:3]),
                "A date table exists but no fact table is related to it as its date dimension.",
                "Relate each fact's date column to the date table's date column (many-to-one); time "
                "intelligence needs that relationship."))
        else:
            findings.append(_finding(
                "SS_NO_DATE_DIM", "warning", None,
                "No date dimension found, but the model has fact tables.",
                "Add a dedicated, marked date table and relate every fact date to it; time "
                "intelligence requires one."))
    for d in date_dims:
        if classification[d].get("marked_date_table"):
            continue
        if marking_known:
            findings.append(_finding(
                "SS_DATE_NOT_MARKED", "warning", d,
                f"'{d}' looks like a date dimension but is not marked as a date table.",
                "Mark it as a date table (date column as key) so time-intelligence DAX is reliable."))
        else:
            findings.append(_finding(
                "SS_DATE_NOT_MARKED", "info", d,
                f"'{d}' looks like a date dimension; the metadata does not say whether it is marked "
                "as a date table.",
                "Confirm it is marked as a date table (date column as key) so time-intelligence DAX "
                "is reliable."))

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
        rel_cols |= {r.get("to_column") for r in rels if r.get("to_table") == f}
        text_attrs = [c.get("name") for c in _user_columns(t)
                      if _type_key(c.get("data_type")) in ("string", "text")
                      and not _looks_like_key(c.get("name") or "")
                      and c.get("name") not in rel_cols
                      and not _truthy(c.get("is_hidden"))]
        if len(text_attrs) >= 3:
            findings.append(_finding(
                "SS_TEXT_ON_FACT", "info", f,
                f"Fact table '{f}' carries {len(text_attrs)} visible text attributes "
                f"(e.g. {', '.join(str(a) for a in text_attrs[:3])}).",
                "Move descriptive text to dimensions; high-cardinality text on a fact bloats "
                "the model and weakens compression."))

    disconnected = [n for n, c in classification.items()
                    if c["class"] == "disconnected" and c["columns"] > 1 and not _is_auto_date_table(n)]
    for n in disconnected:
        findings.append(_finding(
            "SS_DISCONNECTED", "info", n,
            f"Table '{n}' participates in no relationship.",
            "Relate it, hide it, or confirm it is an intentional parameter/what-if table."))

    # --- structure: is there a star at all?
    # Data tables: every related table, plus stand-alone tables that look like data (more than
    # one real column, visible). Measure tables, single-column parameter tables, hidden helpers
    # and the engine's own date tables are not expected to be related. (Hidden tables that ARE
    # related count: hiding the fact table behind a measure table is a normal design.)
    def is_data_table(n: str, c: Dict[str, Any]) -> bool:
        if c["class"] == "measure_table" or _is_auto_date_table(n):
            return False
        if c["class"] != "disconnected":
            return True
        return c["columns"] > 1 and not _truthy(tables_by_name[n].get("is_hidden"))

    data_tables = [n for n, c in classification.items() if is_data_table(n, c)]
    connected = [n for n in data_tables if classification[n]["class"] != "disconnected"]
    if not data_tables:
        findings.append(_finding(
            "SS_EMPTY_MODEL", "warning", None,
            "The model has no data tables to audit." if tables else "The model has no tables.",
            "Load fact and dimension tables and relate them; there is no schema to assess yet."))
    elif not rels:
        what = ("a single flat table" if len(data_tables) == 1
                else f"{len(data_tables)} tables with no relationship between them")
        findings.append(_finding(
            "SS_NO_RELATIONSHIPS", "error", ", ".join(data_tables[:5]),
            f"The model is {what}: it is not a star schema.",
            "Split descriptive attributes into dimension tables, keep numeric events in a fact "
            "table, and relate them many-to-one on a key."))

    # --- score (formula in the module docstring). Info findings often reflect intentional design
    # (role-playing dims, parameter tables), so their combined penalty is capped.
    penalty = {"error": 15, "warning": 8, "info": 2}
    info_penalty = sum(penalty["info"] for f in findings if f["severity"] == "info")
    findings_penalty = (sum(penalty.get(f["severity"], 2) for f in findings if f["severity"] != "info")
                        + min(info_penalty, 10))
    coverage_penalty = round(40 * (1 - len(connected) / len(data_tables))) if data_tables else 0
    score = 100 - findings_penalty - coverage_penalty
    cap = None
    if not data_tables:
        cap = 0
    elif not rels:
        cap = _STRUCTURE_CAP
    if cap is not None:
        score = min(score, cap)
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
            "score_breakdown": {
                "findings_penalty": findings_penalty,
                "coverage_penalty": coverage_penalty,
                "data_tables": len(data_tables),
                "data_tables_related": len(connected),
                "cap": cap,
            },
        },
    }
