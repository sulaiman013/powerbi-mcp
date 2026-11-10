"""
Power Query (M) identifier rewriting for table renames.

A table's partition can read another table's query by name (``Source = Sales``,
``Table.NestedJoin(Sales, ...)``, ``Table.Combine({Sales, Returns})``). When that table is
renamed in the model, those identifiers must follow, and nothing else in the M text may: not
the string literals that name source objects (``Item="Sales"``, ``entity="Sales"``), not field
accesses (``row[Sales]``), not record field names, and not a step or parameter that merely
shares the name. M quotes names as ``#"..."`` (never with DAX's single quotes), and compares
them case-sensitively.
"""
import re
from dataclasses import dataclass
from typing import Dict, List, Tuple

WS, COMMENT, STRING, QIDENT, IDENT, NUMBER, OP = "ws", "comment", "string", "qident", "ident", "number", "op"

_KEYWORDS = frozenset({
    "and", "as", "each", "else", "error", "false", "if", "in", "is", "let", "meta", "not",
    "null", "or", "otherwise", "section", "shared", "then", "true", "try", "type",
})
_REGULAR = re.compile(r"[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*\Z")
_TWO_CHAR = frozenset({"=>", "<=", ">=", "<>", "..", "??"})
_OPEN = {"(": ")", "[": "]", "{": "}"}


@dataclass(frozen=True)
class Token:
    kind: str
    text: str
    start: int


def tokenize(src: str) -> List[Token]:
    """Lex M text into tokens whose texts concatenate back to ``src``. Never raises."""
    s = src or ""
    n = len(s)
    out: List[Token] = []
    i = 0
    while i < n:
        ch = s[i]
        nxt = s[i + 1] if i + 1 < n else ""
        start = i
        if ch.isspace():
            while i < n and s[i].isspace():
                i += 1
            out.append(Token(WS, s[start:i], start))
        elif ch == "/" and nxt == "/":
            while i < n and s[i] not in "\r\n":
                i += 1
            out.append(Token(COMMENT, s[start:i], start))
        elif ch == "/" and nxt == "*":
            close = s.find("*/", i + 2)
            i = n if close == -1 else close + 2
            out.append(Token(COMMENT, s[start:i], start))
        elif ch == '"':
            i = _scan_string(s, i + 1)
            out.append(Token(STRING, s[start:i], start))
        elif ch == "#" and nxt == '"':
            i = _scan_string(s, i + 2)
            out.append(Token(QIDENT, s[start:i], start))
        elif ch.isalpha() or ch == "_" or (ch == "#" and nxt.isalpha()):
            i += 1
            while i < n and (s[i].isalnum() or s[i] == "_" or
                             (s[i] == "." and i + 1 < n and (s[i + 1].isalpha() or s[i + 1] == "_"))):
                i += 1
            out.append(Token(IDENT, s[start:i], start))
        elif ch.isdigit():
            while i < n and (s[i].isalnum() or s[i] == "."):
                i += 1
            out.append(Token(NUMBER, s[start:i], start))
        else:
            i += 2 if s[i:i + 2] in _TWO_CHAR else 1
            out.append(Token(OP, s[start:i], start))
    return out


def _scan_string(s: str, i: int) -> int:
    n = len(s)
    while i < n:
        if s[i] == '"':
            if i + 1 < n and s[i + 1] == '"':
                i += 2
                continue
            return i + 1
        i += 1
    return n


def quote_identifier(name: str, force: bool = False) -> str:
    """M identifier text for ``name``: bare when it is a regular identifier, else #"..."."""
    if not force and _REGULAR.match(name) and name not in _KEYWORDS:
        return name
    return '#"' + name.replace('"', '""') + '"'


def identifier_name(token: Token) -> str:
    if token.kind == QIDENT:
        body = token.text[2:-1] if token.text.endswith('"') and len(token.text) >= 3 else token.text[2:]
        return body.replace('""', '"')
    return token.text


def rename_query_references(src: str, old: str, new: str) -> Tuple[str, int, bool]:
    """Rename references to the query ``old`` inside M text.

    Returns (new_text, replacements, shadowed). ``shadowed`` is True when the text itself
    defines something called ``old`` (a let step or a function parameter); every use is then
    left alone, because it most likely refers to that local definition and not to the query.
    """
    tokens = tokenize(src)
    sig = [i for i, t in enumerate(tokens) if t.kind not in (WS, COMMENT)]
    pos_of = {i: p for p, i in enumerate(sig)}

    # Match brackets, and for every '[' group note whether it assigns fields (a record or a
    # record type) or only names them (a field access or projection).
    partner: Dict[int, int] = {}
    assigns: Dict[int, bool] = {}
    stack: List[int] = []
    for i in sig:
        t = tokens[i]
        if t.kind != OP:
            continue
        if t.text in _OPEN:
            stack.append(i)
            if t.text == "[":
                assigns[i] = False
        elif t.text in ")]}":
            if stack and _OPEN[tokens[stack[-1]].text] == t.text:
                partner[stack.pop()] = i
        elif t.text == "=" and stack and tokens[stack[-1]].text == "[":
            assigns[stack[-1]] = True

    field_names = set()        # identifier tokens that name a field rather than refer to a value
    parameters = set()         # identifier tokens declared as function parameters
    stack = []
    in_name: Dict[int, bool] = {}
    for i in sig:
        t = tokens[i]
        if t.kind == OP:
            if t.text in _OPEN:
                stack.append(i)
                in_name[i] = True
                continue
            if t.text in ")]}":
                if stack and _OPEN[tokens[stack[-1]].text] == t.text:
                    opener = stack.pop()
                    after = sig[pos_of[i] + 1] if pos_of[i] + 1 < len(sig) else None
                    if t.text == ")" and after is not None and tokens[after].text == "=>":
                        for j in range(opener + 1, i):
                            if tokens[j].kind in (IDENT, QIDENT):
                                parameters.add(j)
                continue
            if stack and tokens[stack[-1]].text == "[":
                top = stack[-1]
                if t.text == "=":
                    in_name[top] = False
                elif t.text == ",":
                    in_name[top] = True
            continue
        if t.kind in (IDENT, QIDENT) and stack and tokens[stack[-1]].text == "[":
            top = stack[-1]
            if not assigns.get(top, False) or in_name.get(top, True):
                field_names.add(i)

    matches: List[int] = []
    shadowed = False
    for i in sig:
        t = tokens[i]
        if t.kind not in (IDENT, QIDENT) or identifier_name(t) != old:
            continue
        if i in parameters:
            shadowed = True
            continue
        if i in field_names:
            continue                 # row[Sales], [Sales = ...]: a field, never the query
        p = pos_of[i]
        nxt = tokens[sig[p + 1]] if p + 1 < len(sig) else None
        if nxt is not None and nxt.kind == OP and nxt.text == "=":
            shadowed = True          # a let step named like the query
            continue
        matches.append(i)
    if shadowed or not matches:
        return src, 0, shadowed
    out = []
    for i, t in enumerate(tokens):
        out.append(quote_identifier(new, force=False) if i in matches else t.text)
    return "".join(out), len(matches), False
