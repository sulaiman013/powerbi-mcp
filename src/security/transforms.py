"""
Value transformations applied by column policies and PII masking.

HASH is keyed (HMAC-SHA256). An unkeyed hash of a low-entropy value (a birth date, a phone
number, an SSN) is reversed by hashing every candidate, so the key is what makes the output
safe to show:
  * POWERBI_MCP_HASH_KEY set: hashes are stable across restarts and across servers that share
    the key, so they can be joined or compared over time. Anyone who learns the key can run the
    dictionary attack again; protect it like a credential.
  * not set: a random key is generated per process. Hashes are stable within one server
    session only, and a restart changes every hash.

NUMERIC_MASK multiplies by one secret factor per session. It keeps ratios and ordering and
hides magnitudes; a single known true value reveals the factor. It is an obfuscation for
exploratory use, not anonymisation.
"""
import hashlib
import hmac
import os
import re
import secrets
from decimal import Decimal
from typing import Any, Optional, Tuple

HASH_KEY_ENV = "POWERBI_MCP_HASH_KEY"
MIN_HASH_KEY_LENGTH = 16
REDACTED = "[REDACTED]"

_INTEGER_TEXT = re.compile(r"[+-]?\d+\Z")
_DECIMAL_TEXT = re.compile(r"[+-]?(?:\d+\.\d*|\.\d+)(?:[eE][+-]?\d+)?\Z")

_SESSION_HASH_KEY = secrets.token_bytes(32)


def resolve_hash_key() -> Tuple[bytes, bool, Optional[str]]:
    """(key, persistent, problem).

    ``persistent`` is True when the key came from POWERBI_MCP_HASH_KEY. ``problem`` is set when
    that variable holds a key too short to resist guessing; the session key is returned with it
    so nothing is ever hashed under the weak key.
    """
    raw = os.environ.get(HASH_KEY_ENV)
    if raw is None or raw == "":
        return _SESSION_HASH_KEY, False, None
    if len(raw) < MIN_HASH_KEY_LENGTH:
        return _SESSION_HASH_KEY, False, (
            f"{HASH_KEY_ENV} must be at least {MIN_HASH_KEY_LENGTH} characters long")
    return raw.encode("utf-8"), True, None


def keyed_hash(value: Any, key: bytes) -> str:
    """[HASH:<16 hex>] of str(value) under ``key``. Equal values give equal hashes."""
    digest = hmac.new(key, str(value).encode("utf-8"), hashlib.sha256).hexdigest()
    return f"[HASH:{digest[:16]}]"


def mask_value(value: Any, strategy: Optional[str] = None) -> Any:
    """Mask a value for a MASK column policy.

    The default ("partial") shows the trailing quarter of the text, never more than four
    characters, so a 16-digit card keeps its last four and a five-character code keeps one.
    "full" shows nothing. None stays None.
    """
    if value is None:
        return None
    s = str(value)
    if strategy == "full":
        return "*" * len(s)
    keep = min(4, len(s) // 4)
    return "*" * (len(s) - keep) + (s[len(s) - keep:] if keep else "")


def new_numeric_coefficient() -> float:
    """A random scale factor from [0.25, 0.75] or [1.5, 4.0].

    The gap around 1.0 matters: a factor of 0.9997 would leave the numbers effectively
    unmasked. The factor is not rounded, so it cannot be found by trying a short list.
    """
    rng = secrets.SystemRandom()
    return rng.uniform(0.25, 0.75) if rng.random() < 0.5 else rng.uniform(1.5, 4.0)


def scale_number(value: Any, coefficient: float) -> Any:
    """``value`` times ``coefficient`` for a NUMERIC_MASK column.

    Integers stay integers, and None stays None. Text that is a plain number is scaled and
    returned as text: the Desktop connector delivers fixed-decimal (currency) columns that way.
    Anything else (other text, a boolean, a date) is redacted instead of passed through,
    because a value derived from a masked number can carry the true figure.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return REDACTED
    if isinstance(value, int):
        return int(round(value * coefficient))
    if isinstance(value, float):
        return value * coefficient
    if isinstance(value, Decimal):
        return value * Decimal(repr(coefficient))
    if isinstance(value, str):
        text = value.strip()
        if len(text) <= 64 and _INTEGER_TEXT.match(text):
            return str(int(round(int(text) * coefficient)))
        if len(text) <= 64 and _DECIMAL_TEXT.match(text):
            return str(float(text) * coefficient)
    return REDACTED
