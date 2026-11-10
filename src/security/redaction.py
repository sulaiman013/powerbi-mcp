"""
Secret redaction for anything that leaves the process: tool responses, logs, the audit log.

The cloud connectors put the service-principal secret in the ADOMD/MSOLAP connection string
(Password=...), and provider errors can echo that string. REST errors can echo a token or a
request body. ``redact_secrets`` masks the usual carriers:

  Password=..., Pwd=...            connection strings, including quoted values holding a ';'
  SharedAccessSignature=...        whole value
  AccountKey=, SharedAccessKey=, client_secret=, access_token=, api_key=, secret= ...
                                   URL / key=value form and JSON / "key": "value" form
  Authorization: Bearer ...        and a bare "Bearer <token>" or JWT
  sig=<signature>                  SAS query-string signature
  literal values                   every string in ``extra_secrets`` (the configured secret)

Every pattern is bounded to the value it replaces: a match never runs past the end of its
line, the next ';', or the closing quote, so the rest of a response survives. The patterns are
written so the scan is linear in the length of the text.

This is a safety net, not a parser: a secret in a shape that is not listed here is not found.
Callers that know a secret value should always pass it in ``extra_secrets``.
"""
import re
from typing import Any, Iterable, List, Optional
from urllib.parse import quote

from .names import name_tokens

MASK = "***"
# Shorter literals are skipped: replacing every "abc" in a response would shred it, and a
# real credential is far longer.
MIN_LITERAL_LENGTH = 6

_KEY_START = r"(?<![A-Za-z0-9])"

# Connection-string value: a quoted value (a doubled quote is an escape) may hold ';'. An
# unquoted value cannot contain ';' or a quote by the connection-string grammar, so stopping
# there is exact. An opening quote with no partner on the line is redacted to the next ';'.
_CONN_VALUE = (
    r"""(?:"(?:[^"\r\n]|"")*"|'(?:[^'\r\n]|'')*'|["'][^;\r\n]*|[^;\r\n"']*)"""
)
# One token: URL query values, base64 keys.
_TOKEN_VALUE = r"""(?:"[^"\r\n]*"|'[^'\r\n]*'|[^\s;&"',}\])]+)"""

_CONN_KEYS = r"(?:password|passwd|pwd|shared[ _\-]?access[ _\-]?signature)"
_TOKEN_KEYS = (
    r"(?:client[ _\-]?secret|client[ _\-]?assertion|access[ _\-]?token|refresh[ _\-]?token|"
    r"id[ _\-]?token|account[ _\-]?key|shared[ _\-]?access[ _\-]?key|subscription[ _\-]?key|"
    r"x[ _\-]api[ _\-]key|api[ _\-]?key|secret)"
)
_COLON_KEYS = (
    r"(?:password|passwd|pwd|client[ _\-]?secret|client[ _\-]?assertion|access[ _\-]?token|"
    r"refresh[ _\-]?token|id[ _\-]?token|account[ _\-]?key|shared[ _\-]?access[ _\-]?key|"
    r"shared[ _\-]?access[ _\-]?signature|subscription[ _\-]?key|api[ _\-]?key|secret)"
)

_AUTH_HEADER = re.compile(
    r"(?i)" + _KEY_START + r"((?:proxy-)?authorization[\"']?[ \t]*[:=][ \t]*[\"']?)"
    r"(?:(bearer|basic|negotiate|digest|sharedaccesssignature)\s+)?[^\s,;\"']+")
_BEARER = re.compile(r"(?i)" + _KEY_START + r"(bearer)\s+[A-Za-z0-9._~+/\-]{16,}=*")
# Starts only at the beginning of a token run, so a long run is scanned once.
_JWT = re.compile(r"(?<![A-Za-z0-9_.\-])eyJ[A-Za-z0-9_\-]{8,}\.[A-Za-z0-9_\-]{8,}\.[A-Za-z0-9_\-]{8,}")
# Separators are spaces and tabs only: a key at the end of a line must not pull the next line
# in as its value.
_CONN = re.compile(r"(?i)" + _KEY_START + r"(" + _CONN_KEYS + r"[ \t]*=[ \t]*)" + _CONN_VALUE)
_TOKEN = re.compile(r"(?i)" + _KEY_START + r"(" + _TOKEN_KEYS + r"[ \t]*=[ \t]*)" + _TOKEN_VALUE)
_COLON = re.compile(
    r"(?i)" + _KEY_START + r"((?P<q>[\"']?)" + _COLON_KEYS + r"(?P=q)[ \t]*:[ \t]*)"
    r"""(?P<v>"(?:[^"\\\r\n]|\\.)*"|'(?:[^'\\\r\n]|\\.)*'|[^\s,;}\]"']+)""")
_SAS_SIG = re.compile(r"(?i)" + _KEY_START + r"(sig=)[A-Za-z0-9%+/_\-]{16,}={0,2}")

# Cheap test that decides whether the regex passes are worth running at all.
_TRIGGERS = ("password", "passwd", "pwd", "secret", "token", "key", "sig=", "bearer", "authorization",
             "assertion", "signature", "eyj")

# Field names whose value is a secret wherever it appears in structured content. Compared as
# the trailing words of the key, so "db_password" and "clientSecret" both match.
_SECRET_FIELDS = (
    ("password",), ("passwd",), ("pwd",), ("secret",), ("client", "secret"), ("client", "assertion"),
    ("access", "token"), ("refresh", "token"), ("id", "token"), ("account", "key"),
    ("shared", "access", "key"), ("shared", "access", "signature"), ("subscription", "key"),
    ("api", "key"), ("authorization",),
)


def _colon_replacement(match: "re.Match[str]") -> str:
    value = match.group("v")
    quote_char = value[0] if value[:1] in ("'", '"') else ""
    return f"{match.group(1)}{quote_char}{MASK}{quote_char}"


def _literals(extra_secrets: Optional[Iterable[Any]]) -> List[str]:
    out = set()
    for sec in extra_secrets or ():
        if isinstance(sec, str) and len(sec) >= MIN_LITERAL_LENGTH:
            out.add(sec)
            out.add(quote(sec, safe=""))  # the same secret inside a URL or form body
    # Longest first, so a secret that contains another is replaced whole.
    return sorted(out, key=len, reverse=True)


def redact_secrets(text: Any, extra_secrets: Optional[Iterable[Any]] = None) -> str:
    """``text`` (anything; it is passed through str) with credentials replaced by ***.

    ``extra_secrets`` lists known secret values to remove wherever they occur; entries shorter
    than MIN_LITERAL_LENGTH characters are ignored. None becomes "". Redacting twice gives the
    same result as redacting once.
    """
    if text is None:
        return ""
    s = text if isinstance(text, str) else str(text)
    for literal in _literals(extra_secrets):
        if literal in s:
            s = s.replace(literal, MASK)
    lowered = s.lower()
    if not any(trigger in lowered for trigger in _TRIGGERS):
        return s
    s = _AUTH_HEADER.sub(lambda m: f"{m.group(1)}{(m.group(2) + ' ') if m.group(2) else ''}{MASK}", s)
    s = _BEARER.sub(lambda m: f"{m.group(1)} {MASK}", s)
    s = _JWT.sub(MASK, s)
    s = _CONN.sub(lambda m: m.group(1) + MASK, s)
    s = _TOKEN.sub(lambda m: m.group(1) + MASK, s)
    s = _COLON.sub(_colon_replacement, s)
    s = _SAS_SIG.sub(lambda m: m.group(1) + MASK, s)
    return s


def is_secret_field(name: Any) -> bool:
    """True when a dict key names a credential ("password", "clientSecret", "db_password")."""
    if not isinstance(name, str):
        return False
    tokens = name_tokens(name)
    return any(tokens[-len(field):] == field for field in _SECRET_FIELDS if len(tokens) >= len(field))


def _has_content(value: Any) -> bool:
    """False for the values that cannot be a credential: None, an empty string, a boolean."""
    return not (value is None or isinstance(value, bool) or (isinstance(value, str) and not value))


def redact_structure(obj: Any, extra_secrets: Optional[Iterable[Any]] = None) -> Any:
    """A copy of ``obj`` with secrets redacted in every string of nested dicts, lists and tuples.

    A value stored under a credential-named key ("password", "client_secret", ...) is replaced
    whole, whatever its shape. Other types are returned unchanged; the input is never mutated.
    A container that contains itself is cut at the cycle.
    """
    literals = list(extra_secrets or ())
    active = set()

    def walk(value: Any) -> Any:
        if isinstance(value, str):
            return redact_secrets(value, literals)
        if isinstance(value, (dict, list, tuple)):
            if id(value) in active:
                return "<circular>"
            active.add(id(value))
            try:
                if isinstance(value, dict):
                    return {key: (MASK if is_secret_field(key) and _has_content(item) else walk(item))
                            for key, item in value.items()}
                walked = [walk(item) for item in value]
                return tuple(walked) if isinstance(value, tuple) else walked
            finally:
                active.discard(id(value))
        return value

    return walk(obj)
