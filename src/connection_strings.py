"""
Analysis Services connection strings built from untrusted parts.

Workspace, dataset, role and user names arrive as tool arguments. Written into a connection
string unescaped, a value such as ``Sales;Data Source=other-host`` adds a second ``Data Source``
key, and ADOMD.NET keeps the last one: the connection (and, for the cloud path, the service
principal's password) goes to a host the caller chose. Every value therefore goes through
``quote_value``: wrapped in double quotes with embedded double quotes doubled, which the
ADOMD.NET parser reads back as one literal value whatever it contains.

Pure Python; importing this module loads no .NET.
"""
import re
from typing import Any, Iterable, List, Optional, Tuple

REDACTED = "***"


class ConnectionStringError(ValueError):
    """A value cannot be written into a connection string safely."""


def quote_value(value: Any, what: str = "value") -> str:
    """Return ``value`` as a double-quoted connection-string literal.

    Control characters are rejected rather than escaped: no legitimate name contains them and
    connection-string parsers disagree about what they mean.
    """
    if value is None:
        raise ConnectionStringError(f"{what} is required")
    text = str(value)
    if not text.strip():
        raise ConnectionStringError(f"{what} must not be empty")
    for ch in text:
        if ord(ch) < 32 or ord(ch) == 127:
            raise ConnectionStringError(f"{what} contains a control character (U+{ord(ch):04X})")
    return '"' + text.replace('"', '""') + '"'


def build(pairs: Iterable[Tuple[str, Any]]) -> str:
    """Join ``(key, value)`` pairs into ``Key="value";Key2="value2"``.

    Keys come from our own code, never from a caller; they are still checked so a typo cannot
    produce a malformed string.
    """
    parts: List[str] = []
    for key, value in pairs:
        if not re.fullmatch(r"[A-Za-z][A-Za-z0-9 ]*", key):
            raise ConnectionStringError(f"invalid connection-string key {key!r}")
        parts.append(f"{key}={quote_value(value, key)}")
    return ";".join(parts)


def local_data_source(port: Any) -> str:
    """``localhost:<port>`` for a local Analysis Services instance, with the port validated."""
    if isinstance(port, bool):
        raise ConnectionStringError("port must be an integer between 1 and 65535")
    try:
        number = int(port)
    except (TypeError, ValueError):
        raise ConnectionStringError("port must be an integer between 1 and 65535") from None
    if isinstance(port, float) and port != number:
        raise ConnectionStringError("port must be an integer between 1 and 65535")
    if not 1 <= number <= 65535:
        raise ConnectionStringError("port must be an integer between 1 and 65535")
    return f"localhost:{number}"


def role_list(role: Any) -> Optional[str]:
    """Normalise an RLS role argument into the value of the ``Roles`` property.

    ``None`` or blank means no role. A string is ONE role name. A list or tuple is several roles
    applied together. The property is a comma-separated list with no escape for a comma, so a
    role whose name contains one cannot be expressed and is refused instead of being silently
    split into two different roles.
    """
    if role is None:
        return None
    names = list(role) if isinstance(role, (list, tuple, set, frozenset)) else [role]
    cleaned: List[str] = []
    for name in names:
        if name is None or not str(name).strip():
            continue
        text = str(name).strip()
        if "," in text:
            raise ConnectionStringError(
                f"role name {text!r} contains a comma; the Roles connection property is "
                "comma-separated and cannot carry such a name")
        quote_value(text, "role name")  # control-character check
        cleaned.append(text)
    return ",".join(cleaned) if cleaned else None


# Password="quoted; value" | Password='quoted' | Password=bare-value
_SECRET_KEYS = r"(?:password|pwd|client[_ ]?secret|secret|access[_ ]?token)"
_SECRET_PAIR = re.compile(
    r"(?i)\b(" + _SECRET_KEYS + r"\s*=\s*)(\"(?:[^\"]|\"\")*\"|'(?:[^']|'')*'|[^;&\s]*)")
_BEARER = re.compile(r"(?i)\b(bearer\s+)[A-Za-z0-9._~+/=-]{8,}")


def redact(text: Any, secrets: Iterable[Optional[str]] = ()) -> str:
    """``text`` with connection-string passwords, bearer tokens and known secret values masked.

    Applied to every provider error before it is logged or raised: ADOMD.NET errors can quote
    the connection string that failed.
    """
    if text is None:
        return ""
    out = str(text)
    for secret in secrets:
        # Very short "secrets" would blank out ordinary text; real ones are far longer.
        if secret and len(secret) >= 4:
            out = out.replace(secret, REDACTED)
    out = _SECRET_PAIR.sub(lambda m: m.group(1) + REDACTED, out)
    out = _BEARER.sub(lambda m: m.group(1) + REDACTED, out)
    return out
