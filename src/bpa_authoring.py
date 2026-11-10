"""
Custom Best Practice Analyzer (BPA) rule authoring + governance.

Our server already RUNS a built-in BPA (model_analysis.run_bpa). This module lets teams AUTHOR
and GOVERN their own BPA rule sets: validate a rules JSON against the public rule shape, and audit
where rules live (embedded in the model, ignored, or referenced as external files). Pure Python,
no external tool. The BPA rule JSON shape is a public, non-copyrightable fact; the validators here
are original.

Entry points:
    validate_rules(rules)            -> {valid, errors, warnings, rule_count, fixed?}
    audit_rule_sources(model_text)   -> {embedded_rule_count, embedded_rule_ids, external_rule_files,
                                         ignored_rule_ids, local_rule_files, ...}
"""
import json
import re
from typing import Any, Dict, List, Optional, Union

# TOM object types a rule Scope may target (the public BPA scope vocabulary).
VALID_SCOPES = {
    "Model", "Table", "Measure", "Hierarchy", "Level", "Relationship", "Perspective", "Culture",
    "Partition", "ProviderDataSource", "DataColumn", "CalculatedColumn", "CalculatedTable",
    "CalculatedTableColumn", "KPI", "StructuredDataSource", "Variation", "NamedExpression",
    "ModelRole", "CalculationGroup", "CalculationItem", "TablePermission",
}
# Spellings found in older rule files. They are accepted, with a warning naming the current
# scope(s), because rejecting a rule file that the analyzer itself still reads would be wrong.
SCOPE_ALIASES = {
    "Column": "DataColumn, CalculatedColumn, CalculatedTableColumn",
    "DataSource": "ProviderDataSource",
    "Role": "ModelRole",
}
VALID_SEVERITIES = {1, 2, 3}  # 1 info, 2 warning, 3 error
REQUIRED_FIELDS = ("ID", "Name", "Category", "Severity", "Scope", "Expression")
_TEXT_FIELDS = ("ID", "Name", "Category", "Scope", "Expression")
# Runtime-only fields that should not be committed to a rules file.
RUNTIME_FIELDS = ("ObjectCount", "ErrorMessage", "_comment")

_RULES_ANNOTATION = "BestPracticeAnalyzer"
_EXTERNAL_ANNOTATION = "BestPracticeAnalyzer_ExternalRuleFiles"
_IGNORE_ANNOTATION = "BestPracticeAnalyzer_IgnoreRules"


def _as_rule_list(rules: Union[str, list, dict]) -> List[Any]:
    """Accept a JSON string, a list of rules, a {'Rules': [...]} wrapper, or one rule object."""
    if isinstance(rules, str):
        try:
            rules = json.loads(rules)
        except ValueError as e:
            raise ValueError(f"BPA rules are not valid JSON: {e}") from None
    if isinstance(rules, dict):
        wrapper_key = next((k for k in ("Rules", "rules") if k in rules), None)
        if wrapper_key:
            rules = rules[wrapper_key]
        elif any(field in rules for field in REQUIRED_FIELDS):
            rules = [rules]             # a single rule object: validate it, do not report "0 rules, valid"
        else:
            raise ValueError("BPA rules object is neither a rule (ID, Name, ...) nor a "
                             "{'Rules': [...]} wrapper.")
    if not isinstance(rules, list):
        raise ValueError("BPA rules must be a JSON array (or a {'Rules': [...]} object).")
    return rules


def _blank(value) -> bool:
    return value is None or (isinstance(value, str) and value.strip() == "")


def validate_rules(rules: Union[str, list, dict], fix: bool = False) -> Dict[str, Any]:
    """Validate a BPA rules JSON. With fix=True, also return a cleaned copy (runtime fields
    stripped, null FixExpression dropped).

    Raises ValueError only when the input as a whole is unusable (not JSON, not a list of
    rules). Every per-rule problem, whatever the value's type, is an entry in ``errors``.
    """
    rule_list = _as_rule_list(rules)
    errors: List[Dict[str, Any]] = []
    warnings: List[Dict[str, Any]] = []
    seen_ids: Dict[str, List[int]] = {}

    def err(idx, rid, msg):
        errors.append({"index": idx, "rule_id": rid, "message": msg})

    def warn(idx, rid, msg):
        warnings.append({"index": idx, "rule_id": rid, "message": msg})

    cleaned: List[Dict[str, Any]] = []
    for i, rule in enumerate(rule_list):
        if not isinstance(rule, dict):
            err(i, None, "Rule is not an object.")
            continue
        raw_id = rule.get("ID")
        rid = raw_id if isinstance(raw_id, str) else None

        for field in REQUIRED_FIELDS:
            if field not in rule or _blank(rule.get(field)):
                err(i, rid, f"Missing required field '{field}'.")
        for field in _TEXT_FIELDS:
            value = rule.get(field)
            if value is not None and not isinstance(value, str):
                err(i, rid, f"'{field}' must be text (got {type(value).__name__}).")

        sev = rule.get("Severity")
        if sev is not None:
            if isinstance(sev, bool) or not isinstance(sev, int):
                err(i, rid, f"Severity {sev!r} must be a whole number: 1 (info), 2 (warning) or 3 (error).")
            elif sev not in VALID_SEVERITIES:
                err(i, rid, f"Severity {sev!r} is not one of {sorted(VALID_SEVERITIES)} "
                            "(1=info, 2=warning, 3=error).")

        scope = rule.get("Scope")
        if isinstance(scope, str) and scope.strip():
            lowered = {s.lower(): s for s in VALID_SCOPES}
            bad = []
            for token in (s.strip() for s in scope.split(",")):
                if not token or token in VALID_SCOPES:
                    continue
                if token in SCOPE_ALIASES:
                    warn(i, rid, f"Scope '{token}' is an older spelling; write {SCOPE_ALIASES[token]}.")
                elif token.lower() in lowered:
                    bad.append(f"{token} (did you mean {lowered[token.lower()]}?)")
                else:
                    bad.append(token)
            if bad:
                err(i, rid, f"Unknown Scope value(s): {', '.join(bad)}.")

        level = rule.get("CompatibilityLevel")
        if level is not None and (isinstance(level, bool) or not isinstance(level, int) or level < 0):
            err(i, rid, f"CompatibilityLevel {level!r} must be a whole number such as 1200.")

        if rid is not None and rid.strip():
            seen_ids.setdefault(rid, []).append(i)
            if " " in rid:
                warn(i, rid, "Rule ID contains spaces; prefer a hyphen/underscore identifier.")

        fix_expr = rule.get("FixExpression")
        if fix_expr is not None and not isinstance(fix_expr, str):
            err(i, rid, f"'FixExpression' must be text (got {type(fix_expr).__name__}).")
        if isinstance(fix_expr, str) and re.search(r"\.?Delete\s*\(", fix_expr) and sev in (1, 2) \
                and not isinstance(sev, bool):
            warn(i, rid, "FixExpression deletes an object on a non-error rule; destructive auto-fix on a "
                         "low/medium-severity rule is risky.")

        present_runtime = [f for f in RUNTIME_FIELDS if f in rule]
        if present_runtime:
            warn(i, rid, f"Contains runtime-only field(s) {', '.join(present_runtime)}; "
                         "strip before committing.")

        if fix:
            c = {k: v for k, v in rule.items() if k not in RUNTIME_FIELDS}
            if c.get("FixExpression") is None:
                c.pop("FixExpression", None)
            cleaned.append(c)

    for rid, positions in seen_ids.items():
        if len(positions) > 1:
            errors.append({"index": None, "rule_id": rid,
                           "message": f"Duplicate rule ID '{rid}' ({len(positions)} times)."})
    by_folded: Dict[str, List[str]] = {}
    for rid in seen_ids:
        by_folded.setdefault(rid.casefold(), []).append(rid)
    for variants in by_folded.values():
        if len(variants) > 1:
            warnings.append({"index": None, "rule_id": variants[0],
                             "message": f"Rule IDs differ only in letter case: {', '.join(variants)}."})

    result: Dict[str, Any] = {
        "valid": len(errors) == 0,
        "rule_count": len(rule_list),
        "errors": errors,
        "warnings": warnings,
    }
    if fix:
        result["fixed"] = cleaned
    return result


# ---------------------------------------------------------------- annotation extraction

def _indented_block(text: str, pos: int, indent: int) -> str:
    """The lines after ``pos`` that are indented deeper than ``indent`` (a TMDL multi-line value)."""
    lines = []
    for line in text[pos:].split("\n")[1:]:
        body = line.rstrip("\r")
        if body.strip() and len(body) - len(body.lstrip(" \t")) <= indent:
            break
        lines.append(body.strip())
    return "\n".join(lines).strip()


def _strip_fence(value: str) -> str:
    """TMDL writes a verbatim value between ``` fences."""
    value = value.strip()
    if value.startswith("```"):
        value = value[3:]
        end = value.rfind("```")
        if end != -1:
            value = value[:end]
    return value.strip()


def _unquote(value: str) -> str:
    """A quoted TMDL / JSON string value -> its text."""
    if len(value) >= 2 and value.startswith('"') and value.endswith('"'):
        try:
            decoded = json.loads(value)
            if isinstance(decoded, str):
                return decoded
        except ValueError:
            pass
        return value[1:-1].replace('""', '"')
    return value


def _tmdl_values(text: str, name: str) -> List[str]:
    """Every value of ``annotation <name> = ...`` in TMDL text, in file order.

    Handles a value on the same line, a JSON value that starts on the line and runs over the
    following ones, a ``` fenced block, and a value indented on the lines below.
    """
    pattern = re.compile(rf"(?m)^(?P<indent>[ \t]*)annotation[ \t]+{re.escape(name)}[ \t]*="
                         r"[ \t]*(?P<rest>[^\r\n]*)")
    decoder = json.JSONDecoder()
    out: List[str] = []
    for m in pattern.finditer(text):
        rest = m.group("rest").strip()
        start = m.start("rest")
        if rest.startswith("```"):
            close = text.find("```", start + 3)
            out.append(text[start + 3:close if close != -1 else len(text)].strip())
        elif rest[:1] in ("[", "{"):
            try:
                value, _ = decoder.raw_decode(text, start)
                out.append(json.dumps(value))
            except ValueError:
                out.append(rest)
        elif rest:
            out.append(_unquote(rest))
        else:
            out.append(_strip_fence(_indented_block(text, m.end(), len(m.group("indent")))))
    return out


def _bim_values(text: str, name: str) -> Optional[List[str]]:
    """Every value of the named annotation in a BIM / TMSL JSON document, wherever it sits in
    the object tree. None when the text is not a JSON document."""
    stripped = text.lstrip()
    if stripped[:1] not in ("{", "["):
        return None
    try:
        doc = json.loads(stripped)
    except ValueError:
        return None
    found: List[str] = []

    def walk(node):
        if isinstance(node, dict):
            annotations = node.get("annotations")
            if isinstance(annotations, list):
                for a in annotations:
                    if isinstance(a, dict) and a.get("name") == name and "value" in a:
                        value = a["value"]
                        if isinstance(value, list) and all(isinstance(x, str) for x in value):
                            value = "\n".join(value)        # BIM stores multi-line text as an array of lines
                        found.append(value if isinstance(value, str) else json.dumps(value))
            for child in node.values():
                walk(child)
        elif isinstance(node, list):
            for child in node:
                walk(child)

    walk(doc)
    return found


def _fragment_values(text: str, name: str) -> List[str]:
    """Annotation values from JSON fragments (name before value, or value before name)."""
    string = r'("(?:[^"\\]|\\.)*")'
    n = re.escape(name)
    out = []
    for pattern in (rf'"name"\s*:\s*"{n}"\s*,\s*"value"\s*:\s*{string}',
                    rf'"value"\s*:\s*{string}\s*,\s*"name"\s*:\s*"{n}"'):
        for m in re.finditer(pattern, text):
            out.append(_unquote(m.group(1)))
    return out


def _annotation_values(text: str, name: str) -> List[str]:
    values = _bim_values(text, name)
    if values is not None:
        return values
    return _tmdl_values(text, name) + _fragment_values(text, name)


def _json_value(raw: str):
    """Decode an annotation value; a JSON string that itself holds JSON is decoded again."""
    value: Any = raw
    for _ in range(2):
        if not isinstance(value, str):
            break
        try:
            value = json.loads(value)
        except ValueError:
            return None
    return None if isinstance(value, str) else value


def _unique(items: List[str]) -> List[str]:
    return list(dict.fromkeys(items))


def audit_rule_sources(model_text: Optional[str] = None,
                       local_rule_files: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    """Discover BPA rules associated with a model. Parses the model's TMDL/BIM text for embedded
    rules, external rule files, and ignored rule IDs. local_rule_files maps a label to JSON
    text of a user/machine BPARules.json; each is validated and its rule IDs are returned.

    Every occurrence of an annotation is read, not just the first: ignore lists are set per
    object (model, table, column, measure), so one model can carry many of them.
    """
    text = model_text or ""

    embedded_ids: List[str] = []
    for raw in _annotation_values(text, _RULES_ANNOTATION):
        parsed = _json_value(raw)
        rules = parsed.get("Rules", parsed.get("rules")) if isinstance(parsed, dict) else parsed
        for r in rules if isinstance(rules, list) else []:
            if isinstance(r, dict) and isinstance(r.get("ID"), str) and r["ID"]:
                embedded_ids.append(r["ID"])

    external: List[str] = []
    for raw in _annotation_values(text, _EXTERNAL_ANNOTATION):
        parsed = _json_value(raw)
        if isinstance(parsed, list):
            external.extend(str(x).strip() for x in parsed if isinstance(x, str) and x.strip())
        else:
            # not JSON: take quoted entries, else anything that looks like a URL
            quoted = re.findall(r'"((?:[^"\\]|\\.)+)"', raw)
            external.extend(quoted or re.findall(r"https?://[^\s\"'\],]+", raw))
    external = _unique(external)
    urls = [x for x in external if re.match(r"(?i)https?://", x)]

    ignored_ids: List[str] = []
    ignore_values = _annotation_values(text, _IGNORE_ANNOTATION)
    for raw in ignore_values:
        parsed = _json_value(raw)
        ids = parsed.get("RuleIDs", parsed.get("ruleIDs", [])) if isinstance(parsed, dict) else parsed
        if isinstance(ids, list):
            ignored_ids.extend(i for i in ids if isinstance(i, str))
        else:
            ignored_ids.extend(re.findall(r'"([^"]+)"', raw))

    local_summary = []
    local_ids: List[str] = []
    for label, content in (local_rule_files or {}).items():
        try:
            res = validate_rules(content)
            ids = [r["ID"] for r in _as_rule_list(content)
                   if isinstance(r, dict) and isinstance(r.get("ID"), str)]
            local_ids.extend(ids)
            local_summary.append({"source": label, "rule_count": res["rule_count"], "valid": res["valid"],
                                  "rule_ids": ids})
        except Exception as e:
            local_summary.append({"source": label, "error": str(e)})

    return {
        "embedded_rule_count": len(embedded_ids),
        "embedded_rule_ids": embedded_ids,
        "external_rule_files": external,
        "external_rule_urls": urls,
        "external_rule_paths": [x for x in external if x not in urls],
        "ignored_rule_ids": _unique(ignored_ids),
        "ignore_annotation_count": len(ignore_values),
        "local_rule_files": local_summary,
        "local_rule_ids": _unique(local_ids),
    }
