"""
Naming-convention audit -> rename plan.

Pure functions that scan a gathered model's object names (tables, columns, measures) and propose
a normalized name for each, producing a rename PLAN for the server's rename engine (pbip_rename_*
for model + report). We bring the detection layer; the safe-rename engine does the renaming.

Conventions (all configurable): convert snake_case, UPPER_SNAKE_CASE and camelCase identifiers to
spaced Title Case, strip warehouse DIM_/FACT_ table prefixes, trim whitespace, collapse runs of
spaces, and (optionally) expand common abbreviations.

What is deliberately left alone:
  - names that already have spaces are not re-split: 'iPhone Sales', 'Sales YoY %', 'Rolling 12M'
  - words written in mixed case (YoY, MoM, iPhone, McDonald) and words with digits (12M, H2O, Q1)
  - small connecting words in the middle of a name ('% of Total', 'Rank by Category')
  - acronyms: a configurable list (ID, YTD, USD, SKU, EBITDA...), plus any all-capitals word
    inside a mixed-case name, plus all-capitals words without a vowel (NPV, FX, CRM)

The plan is ordered so it can be applied top to bottom: a table's columns and measures come
before the table's own rename, so each entry's `table` is still the current name when it runs.
An entry whose new name is already taken, or is claimed by another entry, is marked
`safe: False` with the conflicting names; it must not be applied as it stands.

Entry point:
    audit(model, options) -> {"summary": {...}, "plan": [ {object_type, table, old, new, reasons,
                                                          safe, conflicts_with} ], "collisions": [...]}
"""
import re
from typing import Any, Dict, Iterable, List, Optional, Tuple

# Common abbreviation -> expansion (word-wise, case-insensitive match on a whole word).
# Deliberately excludes high-collision tokens that are also real words (no, cat, dt, tot, rev),
# so opt-in expansion does not mangle names like "No Show" or "Cat 5".
ABBREVIATIONS = {
    "qty": "Quantity", "amt": "Amount", "nbr": "Number", "num": "Number",
    "cust": "Customer", "prod": "Product", "desc": "Description", "addr": "Address",
    "yr": "Year", "mth": "Month", "dept": "Department",
    "mgr": "Manager", "emp": "Employee", "org": "Organization", "txn": "Transaction",
    "avg": "Average", "pct": "Percent",
}

# Acronyms and fixed spellings, in the form they should be written. Matched case-insensitively
# against whole words that are being re-cased, so customer_id -> Customer ID and
# SALES_YOY -> Sales YoY. Extend or replace through the `acronyms` option.
DEFAULT_ACRONYMS = (
    "ID", "YTD", "QTD", "MTD", "WTD", "FYTD", "FQTD", "PY", "PM", "PQ", "LY", "TY", "FY",
    "YoY", "MoM", "QoQ", "WoW", "DoD",
    "USD", "EUR", "GBP", "JPY", "CAD", "AUD", "CHF", "CNY", "INR",
    "SKU", "UPC", "EAN", "KPI", "ROI", "ROE", "ROA", "ROAS", "ROIC", "EBIT", "EBITDA", "COGS", "GAAP",
    "IFRS", "CAGR", "NPS", "CSAT", "ARR", "MRR", "ARPU", "LTV", "CLV", "CAC", "CTR", "CPC", "CPM", "CPA",
    "AOV", "GMV", "B2B", "B2C", "API", "URL", "URI", "HTTP", "HTTPS", "SQL", "DAX", "ETL", "CRM", "ERP",
    "HR", "IT", "UK", "US", "USA", "EU", "UAE", "EMEA", "APAC", "LATAM", "VAT", "GST", "PO", "GL", "AP",
    "AR", "FX", "PII", "RLS", "UTC", "GMT", "ISO", "GUID", "UUID", "DOB", "ZIP", "NAICS", "SIC", "ISBN",
    "VIN", "SSN", "EIN", "IBAN", "CEO", "CFO", "COO", "CTO", "VP", "OEM", "SLA", "KYC", "AML", "ESG", "OKR",
    "FTE", "NASDAQ", "NYSE", "UOM", "IP", "FAQ", "PDF", "CSV", "XML", "JSON", "HTML", "SMS", "GPS",
    "ATM", "RFP", "RFQ", "QA", "UI", "UX", "AI", "ML", "BI", "DB",
    "iPhone", "iPad", "iOS", "macOS", "eBay", "LinkedIn", "YouTube", "PayPal", "GitHub", "TikTok",
    "WhatsApp", "SharePoint", "OneDrive", "PowerPoint", "JavaScript", "kWh", "MWh", "pH",
)

# Lower-case inside a title-cased name unless they come first.
MINOR_WORDS = frozenset({"a", "an", "and", "as", "at", "by", "for", "from", "in", "of", "on", "or",
                         "per", "the", "to", "vs", "with"})

_WAREHOUSE_PREFIX = re.compile(r"^(dim|dimension|fact|facts)[ _]+(.+)$", re.IGNORECASE)
_PREFIX_WORDS = {"dim", "dimension", "fact", "facts"}
# Where a single identifier is split into words. Each alternative needs a real word to start
# after the boundary, so H2O, B2B, 12M and FY2024Q1 are not broken up.
_CAMEL_BOUNDARY = re.compile(
    r"(?<=[a-z])(?=[A-Z])"            # salesAmount -> sales Amount, customerID -> customer ID
    r"|(?<=[A-Z])(?=[A-Z][a-z])"      # HTTPStatus -> HTTP Status, YTDSales -> YTD Sales
    r"|(?<=[0-9])(?=[A-Z][a-z])")     # Q1Sales -> Q1 Sales
_NAME_PARTICLES = {"Mc", "Mac", "De", "La", "Le", "O"}
_VOWELS = set("AEIOU")
# Ordinary words that happen to have no vowel letter, so the 'no vowel means acronym' rule
# does not keep MY_VALUE as 'MY Value'.
_VOWELLESS_WORDS = {"my", "try", "dry", "sky", "fly", "why", "gym", "cry", "shy", "spy", "sync", "myth"}
_TARGET_CASES = ("title", "none")
# Starts the extra reason added to a plan entry whose new name is not free, so that a consumer
# that only prints the reasons still shows the conflict.
_COLLISION_PREFIX = "COLLISION"


def _acronym_map(acronyms: Optional[Iterable[str]] = None, replace: bool = False) -> Dict[str, str]:
    """casefolded word -> the spelling to use."""
    words = list(acronyms or []) if replace else list(DEFAULT_ACRONYMS) + list(acronyms or [])
    return {str(w).casefold(): str(w) for w in words if str(w).strip()}


def _split_identifier(token: str, fixed: Dict[str, str]) -> List[str]:
    """Words of one camelCase / PascalCase identifier. A token is kept whole when it is a known
    spelling, or when splitting would shave off a single letter or a name particle: that is a
    brand, a unit or a surname (iPhone, YoY, kWh, McDonald), not camelCase."""
    if token.casefold() in fixed:
        return [token]
    pieces = [p for p in _CAMEL_BOUNDARY.split(token) if p]
    # Re-join pieces that together are a known spelling: Linked|In -> LinkedIn, e|Bay -> eBay.
    parts: List[str] = []
    i = 0
    while i < len(pieces):
        for j in range(len(pieces), i + 1, -1):
            if "".join(pieces[i:j]).casefold() in fixed:
                parts.append("".join(pieces[i:j]))
                i = j
                break
        else:
            parts.append(pieces[i])
            i += 1
    if len(parts) < 2 or any(len(p) == 1 for p in parts) or parts[0] in _NAME_PARTICLES:
        return [token]
    return parts


def _case_word(word: str, first: bool, last: bool, all_upper_name: bool, fixed: Dict[str, str],
               from_identifier: bool) -> str:
    """Title-case one word, touching it only when it is written entirely in one case.

    from_identifier: the word is one of several taken out of an identifier (snake_case,
    camelCase). Only then is a Capitalized word that is a known acronym respelled
    (CustomerId -> Customer ID): in a name a person spaced, 'Customer Id' is left as written.
    """
    start = 0
    while start < len(word) and not word[start].isalnum():
        start += 1
    end = len(word)
    while end > start and not word[end - 1].isalnum():
        end -= 1
    lead, core, trail = word[:start], word[start:end], word[end:]
    if not any(ch.isalpha() for ch in core):
        return word
    is_lower = core == core.lower()
    is_upper = core == core.upper()
    key = core.casefold()
    if not (is_lower or is_upper):
        capitalized = core[0].isupper() and core[1:] == core[1:].lower()
        if from_identifier and capitalized and key in fixed:
            return lead + fixed[key] + trail
        return word                     # mixed case is somebody's deliberate spelling
    if is_upper and not all_upper_name:
        return word                     # an all-capitals word in a mixed-case name is an acronym
    if key in fixed:
        return lead + fixed[key] + trail
    if any(ch.isdigit() for ch in core):
        return word if is_upper else lead + core[0].upper() + core[1:] + trail
    if is_upper and len(core) == 1:
        return word                     # 'PLAN_A': a label, not the article
    if key in MINOR_WORDS and not first and not last:
        return lead + core.lower() + trail
    if (is_upper and key not in ABBREVIATIONS and key not in _VOWELLESS_WORDS
            and not (set(core) & _VOWELS)):
        return word                     # no vowel: an acronym we do not know, not a word (NPV, FX)
    return lead + core[0].upper() + core[1:].lower() + trail


def suggest_name(name: str, kind: str, *, target_case: str = "title",
                 strip_warehouse_prefixes: bool = True,
                 expand_abbreviations: bool = False,
                 acronyms: Optional[Iterable[str]] = None,
                 replace_acronyms: bool = False) -> Tuple[str, List[str]]:
    """Return (suggested_name, reasons). reasons is empty if the name already conforms.

    acronyms: extra spellings to keep (e.g. ["NPV", "SaaS"]); with replace_acronyms=True they
    replace DEFAULT_ACRONYMS instead of extending it.
    """
    if target_case not in _TARGET_CASES:
        raise ValueError(f"target_case must be one of {', '.join(_TARGET_CASES)} (got {target_case!r}).")
    fixed = _acronym_map(acronyms, replace_acronyms)
    reasons: List[str] = []
    new = name

    if new != new.strip():
        reasons.append("leading/trailing whitespace")
        new = new.strip()
    if re.search(r"\s{2,}|[^\S ]", new):
        reasons.append("inconsistent spacing")

    if kind == "table" and strip_warehouse_prefixes:
        m = _WAREHOUSE_PREFIX.match(new)
        if m:
            reasons.append("warehouse DIM_/FACT_ prefix")
            new = m.group(2)

    spaced_by_hand = bool(re.search(r"\s", new))
    if "_" in new:
        reasons.append("snake_case")
        new = new.replace("_", " ")

    words = new.split()
    # An identifier is one unbroken token; a name that already has spaces was spaced by a person
    # and its words are taken as written. A single word left over after a prefix was stripped is
    # an identifier too, so 'Fact HTTPStatus' is settled in one pass rather than two.
    is_identifier = not spaced_by_hand or len(words) == 1
    if is_identifier:
        split = [part for w in words for part in _split_identifier(w, fixed)]
        if split != words:
            reasons.append("camelCase")
            words = split
        if (kind == "table" and strip_warehouse_prefixes and len(words) > 1
                and words[0].casefold() in _PREFIX_WORDS and "warehouse DIM_/FACT_ prefix" not in reasons):
            reasons.append("warehouse DIM_/FACT_ prefix")   # DimCustomer, FactSales
            words = words[1:]

    if expand_abbreviations:
        expanded = []
        for i, w in enumerate(words):
            near = {x.casefold() for x in words[max(0, i - 1):i + 2]} - {w.casefold()}
            # 'Sort Desc' / 'Order Desc' is descending, not a description
            if w.casefold() == "desc" and near & {"sort", "order", "asc", "ascending"}:
                expanded.append(w)
            else:
                expanded.append(ABBREVIATIONS.get(w.casefold(), w))
        if expanded != words:
            reasons.append("abbreviation")
            words = expanded

    if target_case == "title":
        from_identifier = is_identifier and len(words) > 1
        # Repeat until stable: respelling 'id' as 'ID' can leave a name all in capitals, which is
        # then cased as a whole. Auditing a name that was just suggested must suggest nothing.
        for _ in range(3):
            joined = " ".join(words)
            all_upper_name = joined == joined.upper() and joined != joined.lower()
            titled = [_case_word(w, i == 0, i == len(words) - 1, all_upper_name, fixed, from_identifier)
                      for i, w in enumerate(words)]
            if titled == words:
                break
            # Only record casing as a reason if nothing structural already changed it.
            if not reasons:
                reasons.append("casing")
            words = titled

    return " ".join(words), reasons


def _classify(name: str) -> str:
    """A coarse style label for the convention-consistency summary."""
    if name != name.strip():
        return "untrimmed"
    if "_" in name:
        return "UPPER_SNAKE_CASE" if name == name.upper() and name != name.lower() else "snake_case"
    if " " in name:
        return "spaced"
    if re.search(r"[a-z0-9][A-Z]", name):
        # Internal caps: leading-lower is true camelCase; leading-upper is PascalCase
        # (e.g. a proper noun / table name), which should not be lumped in with camelCase.
        return "camelCase" if name[:1].islower() else "PascalCase"
    if name.isupper() and len(name) > 4:
        return "UPPERCASE"
    return "single word"


def _hidden(obj: Dict[str, Any]) -> bool:
    v = obj.get("is_hidden")
    if isinstance(v, str):
        return v.strip().lower() in ("true", "1", "yes")
    return bool(v)


def _usable(name) -> bool:
    return isinstance(name, str) and name.strip() != ""


def _internal_table(name: str) -> bool:
    return name.startswith(("LocalDateTable_", "DateTableTemplate_"))


def audit(model: Dict[str, Any], options: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Audit a gathered model and return {summary, plan, collisions}.

    options: scope (["tables", "columns", "measures"]), target_case ("title" | "none"),
    strip_warehouse_prefixes, expand_abbreviations, acronyms (extra spellings to keep),
    replace_acronyms.

    Not audited: hidden tables with everything in them, the engine's auto date/time tables,
    hidden columns and measures, RowNumber-<GUID> columns, and objects without a usable name.
    """
    options = options or {}
    scope = set(options.get("scope") or ["tables", "columns", "measures"])
    kw = {
        "target_case": options.get("target_case", "title"),
        "strip_warehouse_prefixes": options.get("strip_warehouse_prefixes", True),
        "expand_abbreviations": options.get("expand_abbreviations", False),
        "acronyms": options.get("acronyms"),
        "replace_acronyms": bool(options.get("replace_acronyms", False)),
    }
    if kw["target_case"] not in _TARGET_CASES:
        raise ValueError(f"target_case must be one of {', '.join(_TARGET_CASES)} "
                         f"(got {kw['target_case']!r}).")
    plan: List[Dict[str, Any]] = []
    styles: Dict[str, int] = {}

    def consider(object_type, table, old) -> None:
        styles[_classify(old)] = styles.get(_classify(old), 0) + 1
        new, reasons = suggest_name(old, object_type, **kw)
        if reasons and new and new != old:
            plan.append({"object_type": object_type, "table": table, "old": old, "new": new,
                         "reasons": reasons, "safe": True, "conflicts_with": []})

    tables = [t for t in (model.get("tables") or []) if isinstance(t, dict) and _usable(t.get("name"))]
    for t in tables:
        tname = t["name"]
        if _hidden(t) or _internal_table(tname):
            continue
        if "columns" in scope:
            for c in t.get("columns") or []:
                cname = c.get("name") if isinstance(c, dict) else None
                # Skip hidden columns and Power BI's internal RowNumber-<GUID> columns:
                # they are not user-facing and must never appear in a rename plan.
                if not _usable(cname) or _hidden(c) or cname.startswith("RowNumber-"):
                    continue
                consider("column", tname, cname)
        if "measures" in scope:
            for m in t.get("measures") or []:
                mname = m.get("name") if isinstance(m, dict) else None
                if not _usable(mname) or _hidden(m):
                    continue
                consider("measure", tname, mname)
        # The table itself goes last: its children above are addressed by its current name.
        if "tables" in scope:
            consider("table", None, tname)

    collisions = _mark_collisions(plan, tables)

    by_type: Dict[str, int] = {}
    by_reason: Dict[str, int] = {}
    for p in plan:
        by_type[p["object_type"]] = by_type.get(p["object_type"], 0) + 1
        for r in p["reasons"]:
            if not r.startswith(_COLLISION_PREFIX):
                by_reason[r] = by_reason.get(r, 0) + 1
    dominant = max(styles, key=styles.get) if styles else None
    unsafe = sum(1 for p in plan if not p["safe"])
    return {
        "summary": {
            "total_suggestions": len(plan),
            "safe_suggestions": len(plan) - unsafe,
            "collisions": unsafe,
            "by_type": by_type,
            "by_reason": by_reason,
            "observed_styles": styles,
            "dominant_style": dominant,
            # Whether the model uses a single naming style (not a conformance verdict).
            "single_style": len(styles) <= 1,
        },
        "plan": plan,
        "collisions": collisions,
    }


def _mark_collisions(plan: List[Dict[str, Any]], tables: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Flag plan entries whose new name is not free.

    Names are unique case-insensitively: tables across the model, columns within their table,
    measures across the model, and a measure may not share a name with a column of its own
    table. A target collides when an object that currently has that name exists in the same
    namespace, or when another plan entry claims it.
    """
    def names(objs) -> List[str]:
        return [o["name"] for o in (objs or []) if isinstance(o, dict) and _usable(o.get("name"))]

    table_names = [t["name"] for t in tables]
    columns_of = {t["name"]: names(t.get("columns")) for t in tables}
    measures_of = {t["name"]: names(t.get("measures")) for t in tables}
    all_measures = [(t, m) for t in table_names for m in measures_of[t]]

    def taken(entry) -> List[str]:
        """Existing objects (other than the entry itself) that already own the target name."""
        target, old, table = entry["new"].casefold(), entry["old"], entry["table"]
        if entry["object_type"] == "table":
            return [f"table '{n}'" for n in table_names if n.casefold() == target and n != old]
        if entry["object_type"] == "column":
            hits = [f"column {table}[{n}]" for n in columns_of.get(table, [])
                    if n.casefold() == target and n != old]
            return hits + [f"measure {table}[{n}]" for n in measures_of.get(table, [])
                           if n.casefold() == target]
        hits = [f"measure {t}[{n}]" for t, n in all_measures
                if n.casefold() == target and not (t == table and n == old)]
        return hits + [f"column {table}[{n}]" for n in columns_of.get(table, []) if n.casefold() == target]

    def namespaces(entry) -> List[Tuple[str, Any, str]]:
        """Namespaces the entry's new name will occupy."""
        target = entry["new"].casefold()
        if entry["object_type"] == "table":
            return [("table", None, target)]
        if entry["object_type"] == "column":
            return [("field", entry["table"], target)]
        return [("measure", None, target), ("field", entry["table"], target)]

    claims: Dict[Tuple[str, Any, str], List[Dict[str, Any]]] = {}
    for entry in plan:
        for ns in namespaces(entry):
            claims.setdefault(ns, []).append(entry)

    def describe(entry) -> str:
        where = f"{entry['table']}[{entry['old']}]" if entry["table"] else f"'{entry['old']}'"
        return f"{entry['object_type']} {where}"

    collisions: List[Dict[str, Any]] = []
    for entry in plan:
        conflicts = list(taken(entry))
        for ns in namespaces(entry):
            conflicts += [describe(other) + " (also renamed to this)"
                          for other in claims[ns] if other is not entry]
        conflicts = list(dict.fromkeys(conflicts))
        if conflicts:
            entry["safe"] = False
            entry["conflicts_with"] = conflicts
            entry["reasons"] = entry["reasons"] + [
                f"{_COLLISION_PREFIX}: '{entry['new']}' is not free ({'; '.join(conflicts)}) "
                "- do not apply as is"]
            collisions.append({"object_type": entry["object_type"], "table": entry["table"],
                               "old": entry["old"], "new": entry["new"], "conflicts_with": conflicts})
    return collisions
