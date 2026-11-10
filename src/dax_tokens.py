"""
Shared DAX tokenizer and reference rewriting.

Every module that reads or rewrites DAX (the linter, the PBIP and TOM rename engines, the
security policy check) goes through this one lexer, because a regex over raw text cannot tell
a reference from the same characters inside a string literal, a comment, or another name.

Lexical rules that matter here:
  "..."   string literal; "" is an escaped quote
  '...'   quoted table name; '' is an escaped apostrophe
  [...]   column or measure name; ]] is an escaped ]
  // --   comment to end of line;  /* ... */ block comment
Names compare case-insensitively, as the engine does.

All rewrites are token substitutions: text outside the replaced token is returned byte for
byte, so formatting, comments and line endings survive a rename.
"""
from dataclasses import dataclass
from typing import Callable, Dict, Iterator, List, Optional, Set, Tuple

WS, COMMENT, STRING, QNAME, BRACKET, IDENT, NUMBER, OP = (
    "ws", "comment", "string", "qname", "bracket", "ident", "number", "op")

# Words that can sit directly before a bracketed name without being a table qualifier
# (RETURN [Total], NOT [Flag], ORDER BY [x]) and that are never a table reference themselves.
KEYWORDS = frozenset({
    "VAR", "RETURN", "IN", "NOT", "AND", "OR", "TRUE", "FALSE", "EVALUATE", "DEFINE",
    "MEASURE", "COLUMN", "TABLE", "FUNCTION", "ORDER", "BY", "ASC", "DESC", "START", "AT",
})

# Bare words DAX accepts as option arguments (DATEADD(..., MONTH), RANKX(..., DESC, DENSE),
# CROSSFILTER(..., BOTH), DATATABLE("c", INTEGER, ...)). A table may legally share one of these
# names, so a lone identifier spelled like one is only treated as a table when it is the first
# argument of a call, never when it trails a comma.
ENUM_WORDS = frozenset({
    "SECOND", "MINUTE", "HOUR", "DAY", "WEEK", "MONTH", "QUARTER", "YEAR",
    "DENSE", "SKIP", "BOTH", "NONE", "ONEWAY", "ONEWAY_LEFTFILTERSRIGHT", "ONEWAY_RIGHTFILTERSLEFT",
    "INTEGER", "DOUBLE", "STRING", "BOOLEAN", "CURRENCY", "DATETIME", "DECIMAL",
    "DEFAULT", "FIRST", "LAST", "BLANKS", "KEEP", "ABS", "REL", "ALPHABETICAL",
})

_TWO_CHAR_OPS = frozenset({"<=", ">=", "<>", "&&", "||", "==", ":="})


@dataclass(frozen=True)
class Token:
    kind: str
    text: str
    start: int

    @property
    def end(self) -> int:
        return self.start + len(self.text)

    @property
    def name(self) -> str:
        """The unescaped name carried by a qname/bracket token; the raw text otherwise."""
        if self.kind == QNAME:
            return unquote_table(self.text)
        if self.kind == BRACKET:
            return unbracket(self.text)
        return self.text


def tokenize(expr: str) -> List[Token]:
    """Lex ``expr`` into tokens whose texts concatenate back to ``expr`` exactly.

    Never raises: an unterminated string, quoted name, bracket or block comment runs to the
    end of the text as a single token of that kind.
    """
    s = expr or ""
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
        elif (ch == "/" and nxt == "/") or (ch == "-" and nxt == "-"):
            while i < n and s[i] not in "\r\n":
                i += 1
            out.append(Token(COMMENT, s[start:i], start))
        elif ch == "/" and nxt == "*":
            close = s.find("*/", i + 2)
            i = n if close == -1 else close + 2
            out.append(Token(COMMENT, s[start:i], start))
        elif ch == '"':
            i = _scan_delimited(s, i, '"')
            out.append(Token(STRING, s[start:i], start))
        elif ch == "'":
            i = _scan_delimited(s, i, "'")
            out.append(Token(QNAME, s[start:i], start))
        elif ch == "[":
            i = _scan_delimited(s, i, "]")
            out.append(Token(BRACKET, s[start:i], start))
        elif ch.isdigit() or (ch == "." and nxt.isdigit()):
            i = _scan_number(s, i)
            out.append(Token(NUMBER, s[start:i], start))
        elif ch.isalpha() or ch == "_":
            i += 1
            while i < n and (s[i].isalnum() or s[i] in "_."):
                i += 1
            out.append(Token(IDENT, s[start:i], start))
        else:
            i += 2 if s[i:i + 2] in _TWO_CHAR_OPS else 1
            out.append(Token(OP, s[start:i], start))
    return out


def _scan_delimited(s: str, i: int, close: str) -> int:
    """Index just past the closing delimiter of the token opening at ``i``; a doubled closing
    delimiter is an escape. Returns len(s) when the token is unterminated."""
    n = len(s)
    i += 1
    while i < n:
        if s[i] == close:
            if i + 1 < n and s[i + 1] == close:
                i += 2
                continue
            return i + 1
        i += 1
    return n


def _scan_number(s: str, i: int) -> int:
    n = len(s)
    while i < n and s[i].isdigit():
        i += 1
    if i < n and s[i] == "." and i + 1 < n and s[i + 1].isdigit():
        i += 1
        while i < n and s[i].isdigit():
            i += 1
    if i < n and s[i] in "eE":
        j = i + 1
        if j < n and s[j] in "+-":
            j += 1
        if j < n and s[j].isdigit():
            while j < n and s[j].isdigit():
                j += 1
            i = j
    return i


# ---------------------------------------------------------------- name helpers

def unquote_table(text: str) -> str:
    """'O''Brien' -> O'Brien. Text that is not quoted is returned unchanged."""
    if len(text) >= 2 and text[0] == "'" and text[-1] == "'":
        return text[1:-1].replace("''", "'")
    if text.startswith("'"):
        return text[1:].replace("''", "'")
    return text


def unbracket(text: str) -> str:
    """[Net]]Sales] -> Net]Sales. Text that is not bracketed is returned unchanged."""
    if len(text) >= 2 and text[0] == "[" and text[-1] == "]":
        return text[1:-1].replace("]]", "]")
    if text.startswith("["):
        return text[1:].replace("]]", "]")
    return text


def is_simple_identifier(name: str) -> bool:
    """True when a table name is legal in DAX without quotes."""
    if not name or not (name[0].isalpha() or name[0] == "_"):
        return False
    return all(c.isalnum() or c == "_" for c in name) and name.upper() not in KEYWORDS


def quote_table(name: str, force: bool = False) -> str:
    """Table reference text for ``name``: quoted only when required unless ``force``."""
    if not force and is_simple_identifier(name):
        return name
    return "'" + name.replace("'", "''") + "'"


def bracket(name: str) -> str:
    """[name] with any ] doubled."""
    return "[" + name.replace("]", "]]") + "]"


def column_ref(table: str, column: str, force_quote: bool = True) -> str:
    """'Table'[Column], correctly escaped. Quotes the table by default (always valid)."""
    return quote_table(table, force=force_quote) + bracket(column)


def names_equal(a: Optional[str], b: Optional[str]) -> bool:
    return a is not None and b is not None and a.casefold() == b.casefold()


# ---------------------------------------------------------------- structure helpers

def significant(tokens: List[Token]) -> List[int]:
    """Indices of tokens that are not whitespace or comments."""
    return [i for i, t in enumerate(tokens) if t.kind not in (WS, COMMENT)]


def strip_comments(expr: str) -> str:
    """``expr`` with every comment replaced by spaces, newlines kept, so that character
    offsets and line numbers still match the original text."""
    parts = []
    for t in tokenize(expr):
        if t.kind == COMMENT:
            parts.append("".join(c if c in "\r\n" else " " for c in t.text))
        else:
            parts.append(t.text)
    return "".join(parts)


def variable_names(tokens: List[Token]) -> Set[str]:
    """Case-folded names declared with VAR in the expression."""
    names: Set[str] = set()
    sig = significant(tokens)
    for pos, i in enumerate(sig[:-1]):
        t = tokens[i]
        if t.kind == IDENT and t.text.upper() == "VAR":
            nxt = tokens[sig[pos + 1]]
            if nxt.kind == IDENT:
                names.add(nxt.text.casefold())
    return names


@dataclass(frozen=True)
class Reference:
    """One name reference found in an expression.

    kind 'column'  : Table[Name]   (table set; may equally be a table-qualified measure)
    kind 'bare'    : [Name]        (table None; a measure, or a column in row context)
    kind 'table'   : Table used on its own, e.g. COUNTROWS(Sales)
    ``index`` is the token index of the name (the bracket, or the table token for 'table');
    ``table_index`` is the qualifier's token index for kind 'column'.
    """
    kind: str
    table: Optional[str]
    name: Optional[str]
    index: int
    table_index: Optional[int] = None


def iter_references(tokens: List[Token]) -> Iterator[Reference]:
    """Yield every table / column / measure reference in a token list.

    A bracketed name is qualified when a table token precedes it, with or without whitespace
    in between ('Sales' [Amount] is legal DAX), unless that token is a keyword such as RETURN.
    A lone identifier counts as a table reference when it is not a function call, a keyword
    or a VAR name; callers that rename tables should still match it against a known name.
    """
    sig = significant(tokens)
    variables = variable_names(tokens)
    qualifier_of: Dict[int, int] = {}
    for pos, i in enumerate(sig):
        if tokens[i].kind != BRACKET or pos == 0:
            continue
        prev_i = sig[pos - 1]
        prev = tokens[prev_i]
        if prev.kind == QNAME or (prev.kind == IDENT and prev.text.upper() not in KEYWORDS
                                  and prev.text.casefold() not in variables):
            qualifier_of[i] = prev_i
    qualifiers = set(qualifier_of.values())

    for pos, i in enumerate(sig):
        t = tokens[i]
        if t.kind == BRACKET:
            if i in qualifier_of:
                q = qualifier_of[i]
                yield Reference("column", tokens[q].name, t.name, i, q)
            elif pos > 0 and tokens[sig[pos - 1]].kind == OP and tokens[sig[pos - 1]].text == ".":
                # 'Date'[Date].[Year]: the bracket after a dot names a level of the column's
                # variation (auto date/time), not a measure or a column of the model.
                continue
            else:
                yield Reference("bare", None, t.name, i)
        elif t.kind in (QNAME, IDENT) and i not in qualifiers:
            if t.kind == IDENT:
                nxt = tokens[sig[pos + 1]] if pos + 1 < len(sig) else None
                if nxt is not None and nxt.kind == OP and nxt.text == "(":
                    continue  # function call
                if t.text.upper() in KEYWORDS or t.text.casefold() in variables:
                    continue
            yield Reference("table", t.name, None, i)


def references(expr: str) -> List[Reference]:
    return list(iter_references(tokenize(expr)))


def referenced_names(expr: str) -> Tuple[Set[str], Set[Tuple[Optional[str], str]]]:
    """(tables, columns) referenced by ``expr``.

    ``columns`` holds (table, name) pairs, with table None for an unqualified [Name].
    ``tables`` holds every table used as a qualifier, quoted, or written as a lone identifier
    (EVALUATE Customers). That last group over-includes on purpose: an option word such as
    MONTH in DATEADD(..., MONTH) lands in it too, so match the result against known table
    names rather than treating every entry as a table.
    """
    tables: Set[str] = set()
    columns: Set[Tuple[Optional[str], str]] = set()
    for r in references(expr):
        if r.kind == "column":
            tables.add(r.table)
            columns.add((r.table, r.name))
        elif r.kind == "bare":
            columns.add((None, r.name))
        else:
            tables.add(r.table)
    return tables, columns


# ---------------------------------------------------------------- rewrites

def _rebuild(tokens: List[Token], replacements: Dict[int, str]) -> str:
    return "".join(replacements.get(i, t.text) for i, t in enumerate(tokens))


def rename_table(expr: str, old: str, new: str) -> Tuple[str, int]:
    """Rename table ``old`` to ``new`` everywhere it is referenced in a DAX expression.

    A reference that was quoted stays quoted; an unquoted one is quoted only if the new name
    needs it. A lone unquoted identifier spelled like a DAX option word (a table called Month,
    against DATEADD(..., MONTH)) is renamed only where it cannot be the option: see ENUM_WORDS.
    Returns (new_expr, replacements).
    """
    tokens = tokenize(expr)
    repl: Dict[int, str] = {}
    for r in iter_references(tokens):
        idx = r.table_index if r.kind == "column" else (r.index if r.kind == "table" else None)
        if idx is None or not names_equal(r.table, old):
            continue
        if (r.kind == "table" and tokens[idx].kind == IDENT and tokens[idx].text.upper() in ENUM_WORDS
                and _follows_comma(tokens, idx)):
            continue
        repl[idx] = quote_table(new, force=tokens[idx].kind == QNAME)
    return (_rebuild(tokens, repl), len(repl)) if repl else (expr, 0)


def _follows_comma(tokens: List[Token], index: int) -> bool:
    for j in range(index - 1, -1, -1):
        if tokens[j].kind not in (WS, COMMENT):
            return tokens[j].kind == OP and tokens[j].text == ","
    return False


def rename_column(expr: str, table: str, old: str, new: str,
                  unqualified: bool = False) -> Tuple[str, int, int]:
    """Rename column ``table[old]`` to ``new``.

    Qualified references are always rewritten. An unqualified ``[old]`` is ambiguous (it may
    be a measure, or a same-named column of another table in row context), so it is only
    rewritten when the caller passes ``unqualified=True`` after checking the model.
    Returns (new_expr, replacements, unqualified_matches_left_alone).
    """
    tokens = tokenize(expr)
    repl: Dict[int, str] = {}
    skipped = 0
    for r in iter_references(tokens):
        if r.kind == "column" and names_equal(r.table, table) and names_equal(r.name, old):
            repl[r.index] = bracket(new)
        elif r.kind == "bare" and names_equal(r.name, old):
            if unqualified:
                repl[r.index] = bracket(new)
            else:
                skipped += 1
    return (_rebuild(tokens, repl) if repl else expr), len(repl), skipped


def rename_measure(expr: str, old: str, new: str,
                   is_column: Optional[Callable[[str, str], bool]] = None,
                   measure_table: Optional[str] = None) -> Tuple[str, int]:
    """Rename measure ``old`` to ``new``.

    Unqualified ``[old]`` is rewritten. ``T[old]`` is rewritten only when it can be the
    measure: ``is_column(T, old)`` is false (T has no such column) and, when given,
    ``measure_table`` is T. With neither hint supplied, qualified references are left alone,
    because ``T[old]`` is far more often a column than a table-qualified measure.
    Returns (new_expr, replacements).
    """
    tokens = tokenize(expr)
    repl: Dict[int, str] = {}
    for r in iter_references(tokens):
        if not names_equal(r.name, old):
            continue
        if r.kind == "bare":
            repl[r.index] = bracket(new)
        elif r.kind == "column":
            if is_column is None and measure_table is None:
                continue
            if is_column is not None and is_column(r.table, r.name):
                continue
            if measure_table is not None and not names_equal(r.table, measure_table):
                continue
            repl[r.index] = bracket(new)
    return (_rebuild(tokens, repl), len(repl)) if repl else (expr, 0)


# ---------------------------------------------------------------- statement classification

class NonQueryStatementError(ValueError):
    """Raised when text handed to a query tool is not a read-only DAX query."""


def statement_kind(text: str) -> str:
    """Classify command text sent to Analysis Services.

    'dax'     a DAX query (EVALUATE ..., or DEFINE ... EVALUATE ...): read-only by definition
    'dmv'     SELECT ... FROM $SYSTEM.<rowset>: a read-only metadata query
    'command' anything else: TMSL/JSON, XMLA, MDX, or other statements that can change state
    'empty'   nothing but whitespace and comments
    """
    tokens = tokenize(text or "")
    sig = [tokens[i] for i in significant(tokens)]
    if not sig:
        return "empty"
    first = sig[0]
    if first.kind == IDENT:
        word = first.text.upper()
        if word in ("EVALUATE", "DEFINE"):
            return "dax"
        if word == "SELECT":
            for pos, t in enumerate(sig):
                if t.kind == IDENT and t.text.upper() == "FROM":
                    after = sig[pos + 1:pos + 3]
                    if (len(after) == 2 and after[0].kind == OP and after[0].text == "$"
                            and after[1].kind == IDENT and after[1].text.upper().startswith("SYSTEM.")):
                        return "dmv"
                    break
    return "command"


def ensure_query(text: str, allow_dmv: bool = False) -> None:
    """Raise NonQueryStatementError unless ``text`` is a DAX query (or a DMV query when
    ``allow_dmv``). Query tools are annotated read-only and stay enabled in lockdown mode, so
    they must not forward TMSL, XMLA or other state-changing commands to the engine."""
    kind = statement_kind(text)
    if kind == "dax" or (allow_dmv and kind == "dmv"):
        return
    if kind == "empty":
        raise NonQueryStatementError("The query is empty.")
    raise NonQueryStatementError(
        "Only DAX queries are accepted here: the text must start with EVALUATE or DEFINE. "
        "TMSL, XMLA, MDX and other commands are refused because they can change the model.")
