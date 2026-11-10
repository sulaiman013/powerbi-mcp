"""
Position-preserving JSON editing for PBIR files.

A rename must change a handful of strings in a report file and nothing else. Parsing with the
standard library and dumping the result back rewrites the whole file (number formatting, escape
style, indentation), which shows up as noise in source control and can alter content the rename
never meant to touch. So this module parses JSON into ordinary dicts and lists whose strings
remember where their token sits in the source text, and applies edits by splicing replacement
tokens into the original text. Everything outside an edited token is returned byte for byte.

    doc = parse(text)                      # dict / list / JStr / numbers / bool / None
    edits = Edits(text)
    edits.set_string(doc["name"], "new")   # replace one string token
    edits.rename_key(doc, "old", "new")    # replace one object key
    new_text = edits.apply()

PBIR also nests documents inside strings: the legacy report.json keeps each visual's config as
JSON text in a string, and expression literals wrap text in single quotes ('...' with ''
escapes). ``literal_unwrap`` / ``literal_wrap`` handle the second layer; nested JSON is parsed
with ``parse`` again and written back through ``set_string`` on the outer token.
"""
import json
import re
from json.decoder import scanstring
from typing import Any, Dict, List, Optional, Tuple

_WS = re.compile(r"[ \t\r\n]*")
_NUMBER = re.compile(r"-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?")
_LITERALS = (("true", True), ("false", False), ("null", None))


class JsonSyntaxError(ValueError):
    """Text is not valid JSON."""


class JStr(str):
    """A decoded JSON string that knows the span of its token (quotes included) in the source."""

    start: int
    end: int

    def __new__(cls, value: str, start: int, end: int) -> "JStr":
        obj = super().__new__(cls, value)
        obj.start = start
        obj.end = end
        return obj


def parse(text: str) -> Any:
    """Parse JSON text. Strings (values and object keys) come back as JStr."""
    pos = _WS.match(text, 0).end()
    value, pos = _value(text, pos)
    pos = _WS.match(text, pos).end()
    if pos != len(text):
        raise JsonSyntaxError(f"unexpected content at offset {pos}")
    return value


def _string(text: str, pos: int) -> Tuple[JStr, int]:
    try:
        value, end = scanstring(text, pos + 1, True)
    except ValueError as e:
        raise JsonSyntaxError(str(e)) from None
    return JStr(value, pos, end), end


def _value(text: str, pos: int) -> Tuple[Any, int]:
    if pos >= len(text):
        raise JsonSyntaxError("unexpected end of JSON text")
    ch = text[pos]
    if ch == '"':
        return _string(text, pos)
    if ch == "{":
        obj: Dict[Any, Any] = {}
        pos = _WS.match(text, pos + 1).end()
        if text[pos:pos + 1] == "}":
            return obj, pos + 1
        while True:
            if text[pos:pos + 1] != '"':
                raise JsonSyntaxError(f"expected an object key at offset {pos}")
            key, pos = _string(text, pos)
            pos = _WS.match(text, pos).end()
            if text[pos:pos + 1] != ":":
                raise JsonSyntaxError(f"expected ':' at offset {pos}")
            pos = _WS.match(text, pos + 1).end()
            obj[key], pos = _value(text, pos)
            pos = _WS.match(text, pos).end()
            sep = text[pos:pos + 1]
            if sep == "}":
                return obj, pos + 1
            if sep != ",":
                raise JsonSyntaxError(f"expected ',' or '}}' at offset {pos}")
            pos = _WS.match(text, pos + 1).end()
    if ch == "[":
        arr: List[Any] = []
        pos = _WS.match(text, pos + 1).end()
        if text[pos:pos + 1] == "]":
            return arr, pos + 1
        while True:
            item, pos = _value(text, pos)
            arr.append(item)
            pos = _WS.match(text, pos).end()
            sep = text[pos:pos + 1]
            if sep == "]":
                return arr, pos + 1
            if sep != ",":
                raise JsonSyntaxError(f"expected ',' or ']' at offset {pos}")
            pos = _WS.match(text, pos + 1).end()
    for word, value in _LITERALS:
        if text.startswith(word, pos):
            return value, pos + len(word)
    m = _NUMBER.match(text, pos)
    if not m:
        raise JsonSyntaxError(f"unexpected character {ch!r} at offset {pos}")
    raw = m.group(0)
    return (float(raw) if any(c in raw for c in ".eE") else int(raw)), m.end()


def key_token(obj: Dict[Any, Any], key: str) -> Optional[JStr]:
    """The JStr key object of ``obj`` equal to ``key`` (dict lookups return the value only)."""
    for k in obj:
        if k == key:
            return k if isinstance(k, JStr) else None
    return None


def encode_string(value: str, like: str = "") -> str:
    """JSON string token for ``value``. Non-ASCII stays readable unless the token being
    replaced (``like``) was written with \\u escapes, in which case that style is kept."""
    ascii_only = False
    if like and "\\u" in like and like.isascii():
        try:
            ascii_only = any(ord(c) > 127 for c in json.loads(like))
        except ValueError:
            ascii_only = False
    return json.dumps(value, ensure_ascii=ascii_only)


class Edits:
    """Replacement tokens for one source text, applied in a single splice."""

    def __init__(self, text: str) -> None:
        self.text = text
        self._spans: Dict[Tuple[int, int], str] = {}

    def __len__(self) -> int:
        return len(self._spans)

    def replace_span(self, start: int, end: int, replacement: str) -> None:
        if self.text[start:end] != replacement:
            self._spans[(start, end)] = replacement
        else:
            self._spans.pop((start, end), None)

    def set_string(self, token: JStr, value: str) -> bool:
        """Replace a string token. Returns True when the value actually changes."""
        if str(token) == value:
            return False
        self.replace_span(token.start, token.end, encode_string(value, self.text[token.start:token.end]))
        return True

    def rename_key(self, obj: Dict[Any, Any], old: str, new: str) -> bool:
        token = key_token(obj, old)
        if token is None or old == new:
            return False
        return self.set_string(token, new)

    def apply(self) -> str:
        if not self._spans:
            return self.text
        out: List[str] = []
        cursor = 0
        for (start, end), replacement in sorted(self._spans.items()):
            if start < cursor:
                raise ValueError("overlapping JSON edits")
            out.append(self.text[cursor:start])
            out.append(replacement)
            cursor = end
        out.append(self.text[cursor:])
        return "".join(out)


# ---------------------------------------------------------------- PBIR expression literals

def literal_unwrap(value: str) -> Optional[str]:
    """Text inside a PBIR string literal ('it''s' -> it's); None when ``value`` is not one
    (numbers such as 12D, booleans, null, dates are written without the quotes)."""
    if len(value) >= 2 and value[0] == "'" and value[-1] == "'":
        return value[1:-1].replace("''", "'")
    return None


def literal_wrap(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"
