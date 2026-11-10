"""
Reading and validating config/policies.yaml.

Validation is strict on purpose. A policy file that is silently half-understood protects
nothing: ``acton: block`` would fall back to the default action (allow), and ``table:`` in
place of ``tables:`` would drop every rule. So an unknown key, a wrong type or an unknown
value anywhere in the file is a PolicyConfigError, and the security layer answers a config
error by refusing queries until the file is fixed.
"""
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import yaml

from .pii_detector import DEFAULT_MAX_SCAN_CHARS, MaskingStrategy, PIIType

TOP_LEVEL_KEYS = ("global", "pii", "tables", "audit")
QUERY_TEXT_MODES = ("full", "redacted", "fingerprint")


class PolicyConfigError(ValueError):
    """The policy configuration cannot be used as written."""


def check_keys(mapping: Dict[str, Any], allowed: Iterable[str], where: str) -> None:
    allowed = tuple(allowed)
    unknown = [str(k) for k in mapping if k not in allowed]
    if unknown:
        raise PolicyConfigError(
            f"{where}: unknown key(s) {', '.join(sorted(unknown))} (allowed: {', '.join(allowed)})")


def expect_mapping(value: Any, where: str) -> Dict[str, Any]:
    if not isinstance(value, dict):
        raise PolicyConfigError(f"{where}: expected a mapping, found {type(value).__name__}")
    return value


def expect_bool(value: Any, where: str) -> bool:
    if not isinstance(value, bool):
        raise PolicyConfigError(f"{where}: expected true or false, found {value!r}")
    return value


def expect_int(value: Any, where: str, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise PolicyConfigError(f"{where}: expected a whole number >= {minimum}, found {value!r}")
    return value


def expect_str(value: Any, where: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise PolicyConfigError(f"{where}: expected non-empty text, found {value!r}")
    return value


def expect_choice(value: Any, choices: Iterable[str], where: str) -> str:
    choices = tuple(choices)
    text = value.strip().lower() if isinstance(value, str) else value
    if text not in choices:
        raise PolicyConfigError(f"{where}: unknown value {value!r} (use one of: {', '.join(choices)})")
    return text


def read_policy_file(path: Any) -> Dict[str, Any]:
    """Parse a policy file into a dict, or raise PolicyConfigError saying what is wrong.

    A missing file, unreadable file, invalid YAML, an empty document and unknown top-level keys
    are all errors. An empty document is refused because "no rules at all" is far more often a
    mistake than a decision; to run without policies on purpose, say so with
    ``global: {enabled: false}``.
    """
    p = Path(str(path))
    if not p.is_file():
        raise PolicyConfigError("the file does not exist")
    try:
        text = p.read_text(encoding="utf-8-sig")
    except (OSError, UnicodeDecodeError) as e:
        raise PolicyConfigError(f"the file cannot be read: {e}") from e
    try:
        config = yaml.safe_load(text)
    except yaml.YAMLError as e:
        raise PolicyConfigError("not valid YAML: " + " ".join(str(e).split())) from e
    if config is None:
        raise PolicyConfigError(
            "the file contains no settings (to disable policies on purpose use 'global: {enabled: false}')")
    expect_mapping(config, "top level")
    check_keys(config, TOP_LEVEL_KEYS, "top level")
    return config


# Old spelling in the global section ("default action when PII is detected") mapped onto the
# masking strategies the detector actually has.
_PII_ACTION_TO_STRATEGY = {
    "allow": MaskingStrategy.NONE,
    "mask": MaskingStrategy.PARTIAL,
    "hash": MaskingStrategy.HASH,
    "redact": MaskingStrategy.REDACT,
    "block": MaskingStrategy.FULL,
}


@dataclass
class PIISettings:
    enabled_types: Optional[List[PIIType]] = None          # None = every type
    default_strategy: MaskingStrategy = MaskingStrategy.PARTIAL
    type_strategies: Dict[PIIType, MaskingStrategy] = field(default_factory=dict)
    max_scan_chars: int = DEFAULT_MAX_SCAN_CHARS


def _strategy(value: Any, where: str) -> MaskingStrategy:
    return MaskingStrategy(expect_choice(value, [s.value for s in MaskingStrategy], where))


def _pii_type(value: Any, where: str) -> PIIType:
    return PIIType(expect_choice(value, [t.value for t in PIIType], where))


def parse_pii_settings(section: Any, pii_default_action: Optional[str] = None) -> PIISettings:
    """Settings for the PII detector from the ``pii`` section (None when the section is absent).

    ``pii_default_action`` is the legacy ``global.pii_default_action`` value; an explicit
    ``pii.default_strategy`` takes precedence over it.
    """
    settings = PIISettings()
    if pii_default_action is not None:
        action = expect_choice(pii_default_action, _PII_ACTION_TO_STRATEGY, "global.pii_default_action")
        settings.default_strategy = _PII_ACTION_TO_STRATEGY[action]
    if section is None:
        return settings
    expect_mapping(section, "pii")
    check_keys(section, ("enabled_types", "default_strategy", "strategies", "max_scan_chars"), "pii")
    if section.get("enabled_types") is not None:
        types = section["enabled_types"]
        if not isinstance(types, list):
            raise PolicyConfigError("pii.enabled_types: expected a list of PII type names")
        settings.enabled_types = [_pii_type(t, "pii.enabled_types") for t in types]
    if section.get("default_strategy") is not None:
        settings.default_strategy = _strategy(section["default_strategy"], "pii.default_strategy")
    if section.get("strategies") is not None:
        for name, strategy in expect_mapping(section["strategies"], "pii.strategies").items():
            pii_type = _pii_type(name, "pii.strategies")
            settings.type_strategies[pii_type] = _strategy(strategy, f"pii.strategies.{pii_type.value}")
    if section.get("max_scan_chars") is not None:
        settings.max_scan_chars = expect_int(section["max_scan_chars"], "pii.max_scan_chars", minimum=100)
    return settings


@dataclass
class AuditSettings:
    present: bool = False            # False when the file has no audit section
    log_dir: Optional[str] = None
    log_file: str = "audit.log"
    max_file_size_mb: float = 10
    backup_count: int = 5
    store_query_text: str = "redacted"
    required: bool = False


def parse_audit_settings(section: Any) -> AuditSettings:
    """Settings for the audit logger from the ``audit`` section.

    ``include_query_text`` and ``redact_sensitive`` are the older switches: false for the first
    means "fingerprint", false for the second means "full". ``store_query_text`` overrides both.
    """
    settings = AuditSettings()
    if section is None:
        return settings
    expect_mapping(section, "audit")
    check_keys(section, ("log_dir", "log_file", "max_file_size_mb", "backup_count", "store_query_text",
                         "include_query_text", "redact_sensitive", "required"), "audit")
    settings.present = True
    if section.get("log_dir") is not None:
        settings.log_dir = expect_str(section["log_dir"], "audit.log_dir")
    if section.get("log_file") is not None:
        name = expect_str(section["log_file"], "audit.log_file")
        if Path(name).name != name:
            raise PolicyConfigError(
                "audit.log_file: expected a file name without a folder (use audit.log_dir)")
        settings.log_file = name
    if section.get("max_file_size_mb") is not None:
        size = section["max_file_size_mb"]
        if isinstance(size, bool) or not isinstance(size, (int, float)) or size <= 0:
            raise PolicyConfigError(f"audit.max_file_size_mb: expected a positive number, found {size!r}")
        settings.max_file_size_mb = size
    if section.get("backup_count") is not None:
        settings.backup_count = expect_int(section["backup_count"], "audit.backup_count")
    if section.get("required") is not None:
        settings.required = expect_bool(section["required"], "audit.required")
    if section.get("redact_sensitive") is not None:
        if not expect_bool(section["redact_sensitive"], "audit.redact_sensitive"):
            settings.store_query_text = "full"
    if section.get("include_query_text") is not None:
        if not expect_bool(section["include_query_text"], "audit.include_query_text"):
            settings.store_query_text = "fingerprint"
    if section.get("store_query_text") is not None:
        settings.store_query_text = expect_choice(
            section["store_query_text"], QUERY_TEXT_MODES, "audit.store_query_text")
    return settings
