"""
Custom Best Practice Analyzer (BPA) rule authoring + governance.

Our server already RUNS a built-in BPA (model_analysis.run_bpa). This module lets teams AUTHOR
and GOVERN their own BPA rule sets: validate a rules JSON against the public rule shape, and audit
where rules live (embedded in the model, ignored, or referenced as external files). Pure Python,
no external tool. The BPA rule JSON shape is a public, non-copyrightable fact; the validators here
are original.

Entry points:
    validate_rules(rules)            -> {valid, errors, warnings, rule_count, fixed?}
    audit_rule_sources(model_text)   -> {embedded_rule_count, external_rule_files, ignored_rule_ids, embedded_rule_ids}
"""
import json
import re
from typing import Any, Dict, List, Optional, Union

# TOM object types a rule Scope may target (the public BPA scope vocabulary).
VALID_SCOPES = {
    "Model", "Table", "Column", "DataColumn", "CalculatedColumn", "CalculatedTableColumn",
    "Measure", "Hierarchy", "Level", "Relationship", "Partition", "Perspective", "Culture",
    "ModelRole", "TablePermission", "KPI", "CalculationGroup", "CalculationItem",
    "CalculatedTable", "NamedExpression", "Variation", "ProviderDataSource",
    "StructuredDataSource", "Role",
}
VALID_SEVERITIES = {1, 2, 3}  # 1 info, 2 warning, 3 error
REQUIRED_FIELDS = ("ID", "Name", "Category", "Severity", "Scope", "Expression")
# Runtime-only fields that should not be committed to a rules file.
RUNTIME_FIELDS = ("ObjectCount", "ErrorMessage", "_comment")


def _as_rule_list(rules: Union[str, list, dict]) -> List[Dict[str, Any]]:
    """Accept a JSON string, a list of rules, or a {'Rules': [...]} wrapper."""
    if isinstance(rules, str):
        rules = json.loads(rules)
    if isinstance(rules, dict):
        rules = rules.get("Rules", rules.get("rules", []))
    if not isinstance(rules, list):
        raise ValueError("BPA rules must be a JSON array (or a {'Rules': [...]} object).")
    return rules


def validate_rules(rules: Union[str, list, dict], fix: bool = False) -> Dict[str, Any]:
    """Validate a BPA rules JSON. With fix=True, also return a cleaned copy (runtime fields
    stripped, null FixExpression dropped)."""
    rule_list = _as_rule_list(rules)
    errors: List[Dict[str, Any]] = []
    warnings: List[Dict[str, Any]] = []
    seen_ids: Dict[str, int] = {}

    def err(idx, rid, msg):
        errors.append({"index": idx, "rule_id": rid, "message": msg})

    def warn(idx, rid, msg):
        warnings.append({"index": idx, "rule_id": rid, "message": msg})

    cleaned: List[Dict[str, Any]] = []
    for i, rule in enumerate(rule_list):
        rid = rule.get("ID") if isinstance(rule, dict) else None
        if not isinstance(rule, dict):
            err(i, None, "Rule is not an object.")
            continue

        for field in REQUIRED_FIELDS:
            if field not in rule or rule.get(field) in (None, ""):
                err(i, rid, f"Missing required field '{field}'.")

        sev = rule.get("Severity")
        if sev is not None and sev not in VALID_SEVERITIES:
            err(i, rid, f"Severity {sev!r} is not one of {sorted(VALID_SEVERITIES)} (1=info, 2=warning, 3=error).")

        scope = rule.get("Scope")
        if isinstance(scope, str) and scope.strip():
            tokens = [s.strip() for s in scope.split(",") if s.strip()]
            bad = [t for t in tokens if t not in VALID_SCOPES]
            if bad:
                err(i, rid, f"Unknown Scope value(s): {', '.join(bad)}.")

        if rid is not None:
            seen_ids[rid] = seen_ids.get(rid, 0) + 1
            if " " in str(rid):
                warn(i, rid, "Rule ID contains spaces; prefer a hyphen/underscore identifier.")

        fix_expr = rule.get("FixExpression")
        if isinstance(fix_expr, str) and re.search(r"\.Delete\s*\(", fix_expr) and sev in (1, 2):
            warn(i, rid, "FixExpression deletes an object on a non-error rule; destructive auto-fix on a "
                         "low/medium-severity rule is risky.")

        present_runtime = [f for f in RUNTIME_FIELDS if f in rule]
        if present_runtime:
            warn(i, rid, f"Contains runtime-only field(s) {', '.join(present_runtime)}; strip before committing.")

        if fix:
            c = {k: v for k, v in rule.items() if k not in RUNTIME_FIELDS}
            if c.get("FixExpression") is None:
                c.pop("FixExpression", None)
            cleaned.append(c)

    for rid, count in seen_ids.items():
        if count > 1:
            errors.append({"index": None, "rule_id": rid, "message": f"Duplicate rule ID '{rid}' ({count} times)."})

    result: Dict[str, Any] = {
        "valid": len(errors) == 0,
        "rule_count": len(rule_list),
        "errors": errors,
        "warnings": warnings,
    }
    if fix:
        result["fixed"] = cleaned
    return result


def audit_rule_sources(model_text: Optional[str] = None,
                       local_rule_files: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    """Discover BPA rules associated with a model. Parses the model's TMDL/BIM text for embedded
    rules, external rule-file URLs, and ignored rule IDs. local_rule_files maps a label to JSON
    text of a user/machine BPARules.json so its rule IDs can be merged in."""
    embedded_ids: List[str] = []
    external_files: List[str] = []
    ignored_ids: List[str] = []

    text = model_text or ""

    def _extract_annotation(name: str) -> Optional[str]:
        # TMDL: annotation NAME = VALUE   (VALUE may be a quoted/brace/bracket blob on the line)
        m = re.search(rf"annotation\s+{re.escape(name)}\s*=\s*(.+)", text)
        if m:
            return m.group(1).strip()
        # BIM JSON: {"name": "NAME", "value": "..."}
        m = re.search(rf'"name"\s*:\s*"{re.escape(name)}"\s*,\s*"value"\s*:\s*("(?:[^"\\]|\\.)*")', text)
        if m:
            try:
                return json.loads(m.group(1))
            except Exception:
                return m.group(1)
        return None

    raw_rules = _extract_annotation("BestPracticeAnalyzer")
    if raw_rules:
        try:
            parsed = json.loads(raw_rules)
            rules = parsed.get("Rules", parsed) if isinstance(parsed, dict) else parsed
            for r in rules if isinstance(rules, list) else []:
                if isinstance(r, dict) and r.get("ID"):
                    embedded_ids.append(r["ID"])
        except Exception:
            pass

    raw_ext = _extract_annotation("BestPracticeAnalyzer_ExternalRuleFiles")
    if raw_ext:
        for u in re.findall(r"https?://[^\s\"'\]]+", raw_ext):
            external_files.append(u)

    raw_ignore = _extract_annotation("BestPracticeAnalyzer_IgnoreRules")
    if raw_ignore:
        try:
            parsed = json.loads(raw_ignore)
            ids = parsed.get("RuleIDs", []) if isinstance(parsed, dict) else parsed
            ignored_ids.extend([i for i in ids if isinstance(i, str)])
        except Exception:
            ignored_ids.extend(re.findall(r'"([^"]+)"', raw_ignore))

    local_summary = []
    for label, content in (local_rule_files or {}).items():
        try:
            res = validate_rules(content)
            local_summary.append({"source": label, "rule_count": res["rule_count"], "valid": res["valid"]})
        except Exception as e:
            local_summary.append({"source": label, "error": str(e)})

    return {
        "embedded_rule_count": len(embedded_ids),
        "embedded_rule_ids": embedded_ids,
        "external_rule_files": external_files,
        "ignored_rule_ids": ignored_ids,
        "local_rule_files": local_summary,
    }
