"""
Column-name normalisation shared by the access-policy engine and the PII detector.

Power BI models spell the same concept many ways: "SSN Number", "ssn_number", "SsnNumber",
"SSNNumber". Rules written against one spelling must match the others, and must not match a
different word that merely contains the same letters ("ClassName" contains "ssn"). Both needs
are met by comparing names as sequences of words instead of as raw text.
"""
import fnmatch
import re
import unicodedata
from functools import lru_cache
from typing import Any, Optional, Pattern, Tuple

_WILDCARDS = "*?"
MATCH_MODES = ("glob", "word")
# A run-together spelling ("passwordhash") cannot be split into words. Word rules still match
# it when the phrase is this long, by prefix or suffix of a single token; shorter phrases
# ("ssn", "token") would hit unrelated words far too often.
_RUN_TOGETHER_MIN = 8


def parse_column_key(key: Any) -> Tuple[Optional[str], str]:
    """Split a result-set column key into (table, column).

    "Sales[Amount]"       -> ("Sales", "Amount")       a model column
    "'Sales Data'[Amt]"   -> ("Sales Data", "Amt")
    "[Total Sales]"       -> (None, "Total Sales")     a measure or a query-defined column
    "[Customers[City]]"   -> (None, "Customers[City]") a query-defined column named to look
                                                       like a model column: still not one
    "Amount"              -> (None, "Amount")
    """
    if not isinstance(key, str):
        return None, str(key)
    s = key.strip()
    if s.endswith("]"):
        if s.startswith("["):
            return None, s[1:-1].strip()
        open_at = s.find("[")
        if open_at > 0:
            table = s[:open_at].strip()
            if len(table) >= 2 and table[0] == "'" and table[-1] == "'":
                table = table[1:-1].replace("''", "'")
            if table:
                return table.strip(), s[open_at + 1:-1].strip()
    return None, s


def _split(name: str, keep_wildcards: bool) -> Tuple[str, ...]:
    """Words of ``name``, case-folded. Splits on anything that is not a letter or digit, on
    camelCase humps (PasswordHash), after an acronym (APIKey -> API, Key) and between letters
    and digits. With ``keep_wildcards``, * and ? stay inside the word they touch."""
    tokens = []
    cur = []

    def flush():
        if cur:
            tokens.append("".join(cur).casefold())
            cur.clear()

    # NFKC folds full-width and other compatibility forms onto the plain letters, as a
    # width-insensitive model collation does, so a rule cannot be dodged by spelling.
    for ch in unicodedata.normalize("NFKC", name):
        wild = keep_wildcards and ch in _WILDCARDS
        if not (wild or ch.isalnum()):
            flush()
            continue
        if cur:
            prev = cur[-1]
            if wild or prev in _WILDCARDS:
                pass  # a wildcard never starts or ends a word
            elif prev.isupper() and ch.islower() and len(cur) >= 2 and cur[-2].isupper():
                last = cur.pop()  # "APIKey": the K belongs to "Key", not to "API"
                flush()
                cur.append(last)
            elif (prev.islower() and ch.isupper()) or (prev.isdigit() != ch.isdigit()):
                flush()
        cur.append(ch)
    flush()
    return tuple(tokens)


@lru_cache(maxsize=8192)
def name_tokens(name: str) -> Tuple[str, ...]:
    """("ssn", "number") for "SSN Number", "ssn_number", "SsnNumber" and "SSNNumber" alike."""
    return _split(name or "", False)


def canonical_name(name: str) -> str:
    """The words of ``name`` joined with underscores: the form glob rules are matched against."""
    return "_".join(name_tokens(name))


@lru_cache(maxsize=2048)
def _glob_regex(pattern: str) -> Optional[Pattern[str]]:
    canonical = "_".join(_split(pattern or "", True))
    if not canonical:
        return None
    # fnmatch.translate escapes every literal character and anchors the pattern at both ends.
    return re.compile(fnmatch.translate(canonical))


@lru_cache(maxsize=2048)
def _raw_glob_regex(pattern: str) -> Pattern[str]:
    return re.compile(fnmatch.translate((pattern or "").strip().casefold()))


def glob_matches(pattern: str, name: str) -> bool:
    """fnmatch-style match of ``pattern`` against the whole column name, ignoring case.

    The pattern is tried two ways and either may match:
      * normalised: both sides are reduced to words joined by "_", so "card_number",
        "Card Number" and "CardNumber" are one pattern and one name;
      * as written: plain fnmatch on the text, so "Cost (USD)*" matches "Cost (USD)" exactly
        as it reads.
    Every character other than * and ? is literal in the normalised form, and the match is
    anchored at both ends in both forms.
    """
    regex = _glob_regex(pattern)
    if regex is not None and regex.match(canonical_name(name)):
        return True
    return bool(_raw_glob_regex(pattern).match((name or "").strip().casefold()))


def word_matches(phrase: str, name: str) -> bool:
    """True when the words of ``phrase`` appear in ``name`` as consecutive whole words.

    "ssn" matches "SSN", "SSN Number" and "CustomerSSN" but not "ClassName". A phrase of
    eight or more letters also matches a run-together spelling ("passwordhash") at the start
    or end of a word.
    """
    want = name_tokens(phrase)
    if not want:
        return False
    have = name_tokens(name)
    span = len(want)
    for i in range(len(have) - span + 1):
        if have[i:i + span] == want:
            return True
    joined = "".join(want)
    if len(joined) >= _RUN_TOGETHER_MIN:
        return any(tok.startswith(joined) or tok.endswith(joined) for tok in have)
    return False


@lru_cache(maxsize=65536)
def pattern_matches(pattern: str, name: str, mode: str = "glob") -> bool:
    """Whether a rule name matches a column name. Cached: a model has a fixed set of column
    names and a policy a fixed set of rules, and every query asks about the same pairs."""
    return word_matches(pattern, name) if mode == "word" else glob_matches(pattern, name)


def pattern_problem(pattern: Any, mode: Any = "glob") -> Optional[str]:
    """Why ``pattern`` cannot be used as a rule name, or None when it is fine. Checked when a
    policy file is loaded, so a bad pattern stops the load instead of failing on a query."""
    if not isinstance(pattern, str) or not pattern.strip():
        return "the name must be a non-empty string"
    if mode not in MATCH_MODES:
        return f"unknown match mode {mode!r} (use one of: {', '.join(MATCH_MODES)})"
    if any(ord(ch) < 32 for ch in pattern):
        return "the name contains a control character"
    if mode == "word":
        if any(ch in _WILDCARDS for ch in pattern):
            return "wildcards are not allowed with match: word (use match: glob)"
        if not name_tokens(pattern):
            return "the name has no letters or digits"
    elif _glob_regex(pattern) is None:
        return "the name has no letters, digits or wildcards"
    return None
