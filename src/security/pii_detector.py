"""
PII detection and masking for query results.

Two signals, in this order:

1. The column name. A column whose name says what it holds ("Email", "Home Phone",
   "SSN Number", "CustomerEmail") has every value masked, whatever its type: an SSN stored as
   an integer is still an SSN. Only the column part of a ``Table[Column]`` key is read, as whole
   words, and the PII word must be the head of the name: "Email Address" and "Work Email" are
   e-mail columns, "Email Opt In" and "Phone Type" are not, and neither is "Description"
   (which merely contains the letters "ip").

2. The value, for text in any other column. Only shapes that can be validated count: an
   e-mail address, a card number that passes the Luhn check with a known issuer prefix, an
   SSN written with separators and a plausible area/group/serial, a phone number written as
   one, an IP address, an IBAN with a valid checksum. A bare run of digits or an ISO date is
   not PII by shape alone, so order numbers and dates are left intact.

Detection is heuristic. It lowers the chance that personal data reaches the model; it does not
guarantee it. Columns that must never be exposed belong in a column policy (block/redact),
and ultimately behind object-level security in the model.

Every scan is linear in the length of the value. Text longer than ``max_scan_chars`` is
scanned up to that limit and the remainder is withheld, because unscanned text cannot be
called clean.
"""
import ipaddress
import logging
import re
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Any, Collection, Dict, Iterable, List, Optional, Tuple

from .names import name_tokens, parse_column_key
from .transforms import keyed_hash, resolve_hash_key

logger = logging.getLogger(__name__)

DEFAULT_MAX_SCAN_CHARS = 50_000


class MaskingStrategy(Enum):
    """How to mask detected PII"""
    FULL = "full"           # Replace entirely: "John" -> "****"
    PARTIAL = "partial"     # Partial mask: "john@email.com" -> "j***@e****.com"
    HASH = "hash"           # Keyed hash: "John" -> "[HASH:a8cfcd74...]"
    REDACT = "redact"       # Replace with label: "John" -> "[REDACTED-NAME]"
    NONE = "none"           # No masking (detection only)


class PIIType(Enum):
    """Types of PII that can be detected"""
    SSN = "ssn"
    CREDIT_CARD = "credit_card"
    EMAIL = "email"
    PHONE = "phone"
    IP_ADDRESS = "ip_address"
    DATE_OF_BIRTH = "date_of_birth"
    PASSPORT = "passport"
    DRIVERS_LICENSE = "drivers_license"
    BANK_ACCOUNT = "bank_account"
    NAME = "name"


# Words that may follow the PII word without changing what the column holds.
_NEUTRAL_SUFFIXES = frozenset({
    "number", "no", "num", "nbr", "nr", "address", "addr", "value", "text", "raw", "full",
    "primary", "secondary", "main", "alt", "alternate", "other", "work", "home", "personal",
    "business", "office", "mobile", "cell",
})

# Column-name indicators as word sequences. A name matches when it ENDS with the sequence,
# allowing only neutral suffix words or digits after it. Run-together spellings are listed
# separately because they cannot be split into words.
_COLUMN_INDICATORS: Dict[PIIType, Tuple[Tuple[str, ...], ...]] = {
    PIIType.SSN: (("ssn",), ("social", "security"), ("socialsecurity",), ("socialsecuritynumber",),
                  ("social", "insurance"), ("sin",), ("national", "insurance")),
    PIIType.CREDIT_CARD: (("credit", "card"), ("creditcard",), ("creditcardnumber",), ("debit", "card"),
                          ("card", "number"), ("cardnumber",), ("card", "no"), ("card", "num"),
                          ("cc", "number"), ("cc", "num"), ("ccn",)),
    PIIType.EMAIL: (("email",), ("e", "mail"), ("emailaddress",)),
    PIIType.PHONE: (("phone",), ("telephone",), ("phonenumber",), ("tel",), ("fax",), ("cellphone",),
                    ("mobile", "number"), ("mobile", "no"), ("cell", "number"), ("cell", "no")),
    PIIType.DATE_OF_BIRTH: (("dob",), ("birth", "date"), ("birthdate",), ("date", "of", "birth"),
                            ("dateofbirth",), ("birthday",), ("birth", "day")),
    PIIType.IP_ADDRESS: (("ip",), ("ipaddress",)),
    PIIType.BANK_ACCOUNT: (("bank", "account"), ("bankaccount",), ("iban",), ("routing", "number"),
                           ("sort", "code")),
    PIIType.PASSPORT: (("passport",), ("passportnumber",)),
    PIIType.DRIVERS_LICENSE: (("drivers", "license"), ("driver", "license"), ("driver", "s", "license"),
                              ("driving", "license"), ("drivers", "licence"), ("driver", "licence"),
                              ("driver", "s", "licence"), ("driving", "licence"), ("driverslicense",),
                              ("dl", "number")),
    PIIType.NAME: (("first", "name"), ("firstname",), ("last", "name"), ("lastname",), ("full", "name"),
                   ("fullname",), ("middle", "name"), ("given", "name"), ("family", "name"),
                   ("surname",), ("maiden", "name"), ("customer", "name"), ("employee", "name"),
                   ("contact", "name"), ("person", "name"), ("patient", "name"), ("member", "name")),
}
# Words that only mean PII when they are the whole column name ("Mobile", not "Mobile Sessions").
_WHOLE_NAME_INDICATORS: Dict[PIIType, Tuple[Tuple[str, ...], ...]] = {
    PIIType.PHONE: (("mobile",), ("cell",)),
}
# Checked in this order; the first enabled type whose indicator matches wins.
_TYPE_ORDER = (PIIType.SSN, PIIType.CREDIT_CARD, PIIType.EMAIL, PIIType.PHONE, PIIType.DATE_OF_BIRTH,
               PIIType.IP_ADDRESS, PIIType.BANK_ACCOUNT, PIIType.PASSPORT, PIIType.DRIVERS_LICENSE,
               PIIType.NAME)

_MISSING = object()
_DIGIT = re.compile(r"\d")
_DIGIT_RUN = re.compile(r"\d+")
_SSN =re.compile(r"(?<![\w-])(\d{3})([- ])(\d{2})\2(\d{4})(?![\w-])")
_CARD = re.compile(r"(?<![\w.])\d(?:[ -]?\d){11,18}(?!\w|\.\d)")
_PHONE_NANP = re.compile(
    r"(?<![\w.+-])(?:\+?1[ .-]?)?(?:\(\d{3}\) ?|\d{3}[ .-])\d{3}[ .-]\d{4}(?![\w-]|\.\d)")
_PHONE_INTL = re.compile(r"(?<![\w+])\+(?:[ .\-()]{0,2}\d){8,15}(?!\w)")
_OCTET = r"(?:25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)"
_IPV4 = re.compile(r"(?<![\w.])" + _OCTET + r"(?:\." + _OCTET + r"){3}(?!\w|\.\d)")
_IPV6 = re.compile(r"(?<![0-9A-Za-z:.])(?:[0-9A-Fa-f]{0,4}:){2,7}[0-9A-Fa-f]{0,4}(?![0-9A-Za-z:])")
_IBAN = re.compile(r"(?<![A-Za-z0-9])[A-Z]{2}\d{2}(?: ?[A-Z0-9]){11,30}(?![A-Za-z0-9])")

_EMAIL_LOCAL = frozenset("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._%+-")
_EMAIL_DOMAIN = frozenset("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-")
_EMAIL_MAX_LOCAL = 64
_EMAIL_MAX_DOMAIN = 255

# When two shapes overlap (a card number is also a run a phone pattern could read), the more
# specific, validated shape wins.
_VALUE_PRIORITY = (PIIType.EMAIL, PIIType.CREDIT_CARD, PIIType.SSN, PIIType.BANK_ACCOUNT,
                   PIIType.IP_ADDRESS, PIIType.PHONE)


def _luhn_ok(digits: str) -> bool:
    total = 0
    for i, ch in enumerate(reversed(digits)):
        d = ord(ch) - 48
        if i % 2:
            d *= 2
            if d > 9:
                d -= 9
        total += d
    return total % 10 == 0


def _issuer_ok(d: str) -> bool:
    """True when the leading digits and the length fit a card scheme. Luhn alone passes one
    random number in ten; the prefix keeps ordinary 16-digit identifiers out."""
    n = len(d)
    if d[0] == "4":                                        # Visa
        return n in (13, 16, 19)
    p2, p3, p4 = int(d[:2]), int(d[:3]), int(d[:4])
    if 51 <= p2 <= 55 or 2221 <= p4 <= 2720:              # Mastercard
        return n == 16
    if p2 in (34, 37):                                     # American Express
        return n == 15
    if p4 == 6011 or p2 == 65 or 644 <= p3 <= 649:        # Discover
        return 16 <= n <= 19
    if 3528 <= p4 <= 3589:                                 # JCB
        return 16 <= n <= 19
    if 300 <= p3 <= 305 or p2 in (36, 38, 39):            # Diners Club
        return 14 <= n <= 19
    if p2 == 62:                                           # UnionPay
        return 16 <= n <= 19
    if p2 == 50 or 56 <= p2 <= 69:                         # Maestro
        return 12 <= n <= 19
    return False


def _card_span(text: str, start: int) -> Optional[Tuple[int, int]]:
    """The (start, end) of a valid card number inside a digit-group candidate, or None.

    A candidate can carry extra digit groups on either side ("4111 1111 1111 1111 2024"), so
    every run of whole groups with a card-like digit count is tried, longest first.
    """
    groups = [(m.start(), m.end()) for m in _DIGIT_RUN.finditer(text)]
    for size in range(len(groups), 0, -1):
        for first in range(len(groups) - size + 1):
            a, b = groups[first][0], groups[first + size - 1][1]
            digits = "".join(ch for ch in text[a:b] if ch.isdigit())
            if 12 <= len(digits) <= 19 and _issuer_ok(digits) and _luhn_ok(digits):
                return start + a, start + b
    return None


def _ssn_ok(area: str, group: str, serial: str) -> bool:
    """Reject the combinations that are never issued: area 000 or 666, group 00, serial 0000.
    Areas 900-999 are kept: they are ITINs, which are just as sensitive."""
    return area not in ("000", "666") and group != "00" and serial != "0000"


def _iban_ok(text: str) -> bool:
    s = text.replace(" ", "")
    if not 15 <= len(s) <= 34:
        return False
    rearranged = s[4:] + s[:4]
    return int("".join(str(int(ch, 36)) for ch in rearranged)) % 97 == 1


def _ipv6_ok(text: str) -> bool:
    if len(text) < 7:  # "a::b" is valid IPv6 and also ordinary text
        return False
    try:
        ipaddress.IPv6Address(text)
    except ValueError:
        return False
    return True


def _find_emails(s: str) -> List[Tuple[int, int]]:
    """Spans of e-mail addresses. Works outward from each '@' over a bounded window, so the
    cost is linear however long the text is and whatever it contains."""
    spans = []
    floor = 0
    at = s.find("@")
    while at != -1:
        start = at
        lowest = max(floor, at - _EMAIL_MAX_LOCAL)
        while start > lowest and s[start - 1] in _EMAIL_LOCAL:
            start -= 1
        end = at + 1
        highest = min(len(s), at + 1 + _EMAIL_MAX_DOMAIN)
        while end < highest and s[end] in _EMAIL_DOMAIN:
            end += 1
        while end > at + 1 and s[end - 1] in ".-":
            end -= 1
        while start < at and s[start] == ".":
            start += 1
        domain = s[at + 1:end]
        tld = domain.rsplit(".", 1)[-1]
        if (start < at and "." in domain and ".." not in domain and domain[0] not in ".-"
                and len(tld) >= 2 and tld.isalpha()):
            spans.append((start, end))
            floor = end
        at = s.find("@", max(end, at + 1))
    return spans


def _as_text(value: Any) -> str:
    """Text of a non-string cell in a PII-named column, in the form a person would write."""
    if isinstance(value, float):
        return str(int(value)) if value.is_integer() else repr(value)
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return str(value)


class PIIDetector:
    """
    Detects and masks PII in query results

    Usage:
        detector = PIIDetector(default_strategy=MaskingStrategy.PARTIAL)
        masked_results, summary = detector.process_results(query_results)
    """

    def __init__(
        self,
        default_strategy: MaskingStrategy = MaskingStrategy.PARTIAL,
        enabled_types: Optional[List[PIIType]] = None,
        column_overrides: Optional[Dict[str, MaskingStrategy]] = None,
        type_strategies: Optional[Dict[PIIType, MaskingStrategy]] = None,
        max_scan_chars: int = DEFAULT_MAX_SCAN_CHARS,
    ):
        """
        Args:
            default_strategy: Masking strategy for a type with no entry in ``type_strategies``
            enabled_types: PII types to detect (None = all)
            column_overrides: Strategy per column, keyed by the lower-cased result key
                ("customers[email]") or the lower-cased column name ("email")
            type_strategies: Strategy per PII type
            max_scan_chars: Longest text value scanned in full (see the module docstring)
        """
        self.default_strategy = default_strategy
        self.enabled_types = list(enabled_types) if enabled_types is not None else list(PIIType)
        self.column_overrides = {str(k).lower(): v for k, v in (column_overrides or {}).items()}
        self.type_strategies = dict(type_strategies or {})
        self.max_scan_chars = max(1, int(max_scan_chars))
        self._hash_key = resolve_hash_key()[0]
        self._column_type_cache: Dict[str, Optional[PIIType]] = {}
        self._cache_types: List[PIIType] = list(self.enabled_types)

    # ------------------------------------------------------------- column names

    def detect_pii_type_from_column(self, column_name: str) -> Optional[PIIType]:
        """The PII type an (enabled) column name indicates, or None.

        Accepts a bare column name or a result key such as ``Customers[Email]``; the table part
        is ignored, so a table called "Email Campaigns" does not make its columns e-mail.
        """
        if not isinstance(column_name, str):
            return None
        if self._cache_types != self.enabled_types:  # enabled_types is a public, mutable list
            self._column_type_cache.clear()
            self._cache_types = list(self.enabled_types)
        cached = self._column_type_cache.get(column_name, _MISSING)
        if cached is not _MISSING:
            return cached
        tokens = name_tokens(parse_column_key(column_name)[1])
        found = None
        for pii_type in _TYPE_ORDER:
            if pii_type in self.enabled_types and self._name_indicates(tokens, pii_type):
                found = pii_type
                break
        if len(self._column_type_cache) < 4096:
            self._column_type_cache[column_name] = found
        return found

    @staticmethod
    def _name_indicates(tokens: Tuple[str, ...], pii_type: PIIType) -> bool:
        # Trailing neutral words and digits ("Phone Number", "Email 2") are not part of the head.
        end = len(tokens)
        while end > 0 and (tokens[end - 1] in _NEUTRAL_SUFFIXES or tokens[end - 1].isdigit()):
            end -= 1
        for stop in range(end, len(tokens) + 1):
            # The neutral words may themselves belong to the indicator ("card number").
            head = tokens[:stop]
            for seq in _COLUMN_INDICATORS.get(pii_type, ()):
                if len(head) >= len(seq) and head[-len(seq):] == seq:
                    return True
            for seq in _WHOLE_NAME_INDICATORS.get(pii_type, ()):
                if head == seq:
                    return True
        return False

    def _strategy_for(self, pii_type: PIIType, column_name: Optional[str]) -> MaskingStrategy:
        if column_name and self.column_overrides:
            key = column_name.lower()
            if key in self.column_overrides:
                return self.column_overrides[key]
            column = parse_column_key(column_name)[1].lower()
            if column in self.column_overrides:
                return self.column_overrides[column]
        return self.type_strategies.get(pii_type, self.default_strategy)

    # ------------------------------------------------------------------- values

    def detect_pii_in_value(self, value: str) -> List[Tuple[PIIType, str, int, int]]:
        """PII found in a text value as (type, matched_text, start, end), ordered by position.

        Matches never overlap. Only enabled types are reported, and only shapes that validate
        (see the module docstring).
        """
        if not isinstance(value, str) or not value:
            return []
        enabled = self.enabled_types
        found: Dict[PIIType, List[Tuple[int, int]]] = {}
        if PIIType.EMAIL in enabled and "@" in value:
            found[PIIType.EMAIL] = _find_emails(value)
        if _DIGIT.search(value):
            if PIIType.CREDIT_CARD in enabled:
                spans = []
                for m in _CARD.finditer(value):
                    span = _card_span(m.group(), m.start())
                    if span:
                        spans.append(span)
                found[PIIType.CREDIT_CARD] = spans
            if PIIType.SSN in enabled:
                found[PIIType.SSN] = [m.span() for m in _SSN.finditer(value)
                                      if _ssn_ok(m.group(1), m.group(3), m.group(4))]
            if PIIType.BANK_ACCOUNT in enabled:
                found[PIIType.BANK_ACCOUNT] = [m.span() for m in _IBAN.finditer(value) if _iban_ok(m.group())]
            if PIIType.IP_ADDRESS in enabled:
                spans = [m.span() for m in _IPV4.finditer(value)]
                if value.count(":") >= 2:
                    spans += [m.span() for m in _IPV6.finditer(value) if _ipv6_ok(m.group())]
                found[PIIType.IP_ADDRESS] = spans
            if PIIType.PHONE in enabled:
                spans = [m.span() for m in _PHONE_NANP.finditer(value)]
                if "+" in value:
                    spans += [m.span() for m in _PHONE_INTL.finditer(value)]
                found[PIIType.PHONE] = spans
        elif PIIType.IP_ADDRESS in enabled and value.count(":") >= 2:
            found[PIIType.IP_ADDRESS] = [m.span() for m in _IPV6.finditer(value) if _ipv6_ok(m.group())]

        accepted: List[Tuple[int, int, PIIType]] = []
        for pii_type in _VALUE_PRIORITY:
            for start, end in found.get(pii_type, ()):
                if all(end <= a or start >= b for a, b, _ in accepted):
                    accepted.append((start, end, pii_type))
        accepted.sort()
        return [(pii_type, value[start:end], start, end) for start, end, pii_type in accepted]

    def mask_value(
        self,
        value: str,
        pii_type: PIIType,
        strategy: Optional[MaskingStrategy] = None
    ) -> str:
        """Mask one PII value with ``strategy`` (the detector's default when None)."""
        strategy = strategy or self.default_strategy

        if strategy == MaskingStrategy.NONE:
            return value
        if strategy == MaskingStrategy.REDACT:
            return f"[REDACTED-{pii_type.value.upper()}]"
        if strategy == MaskingStrategy.HASH:
            return keyed_hash(value, self._hash_key)
        if strategy == MaskingStrategy.FULL:
            return '*' * min(len(value), 10)
        if strategy == MaskingStrategy.PARTIAL:
            return self._partial_mask(value, pii_type)
        return value

    def _partial_mask(self, value: str, pii_type: PIIType) -> str:
        """Type-aware partial mask: enough left to recognise the kind of value, not the value."""
        digits = re.sub(r"\D", "", value)

        if pii_type == PIIType.EMAIL:
            if '@' in value:
                local, domain = value.rsplit('@', 1)
                parts = domain.rsplit('.', 1)
                if local and len(parts) == 2 and parts[0]:
                    return f"{local[0]}***@{parts[0][0]}****.{parts[1]}"
            return "***@***.***"

        if pii_type == PIIType.PHONE:
            return f"(***) ***-{digits[-4:]}" if len(digits) >= 7 else "***-***-****"

        if pii_type == PIIType.SSN:
            return f"***-**-{digits[-4:]}" if len(digits) >= 9 else "***-**-****"

        if pii_type == PIIType.CREDIT_CARD:
            return f"****-****-****-{digits[-4:]}" if len(digits) >= 12 else "****-****-****-****"

        if pii_type == PIIType.NAME:
            return ' '.join(f"{word[0]}{'*' * (len(word) - 1)}" for word in value.split())

        if pii_type == PIIType.IP_ADDRESS:
            parts = value.split('.')
            if len(parts) == 4:
                return f"{parts[0]}.{parts[1]}.***.***"
            groups = value.split(':')
            if len(groups) >= 3:
                return f"{groups[0]}:{groups[1]}:****"
            return "***.***.***.***"

        if pii_type == PIIType.DATE_OF_BIRTH:
            return re.sub(r"\d", "*", value)

        if pii_type == PIIType.BANK_ACCOUNT:
            compact = value.replace(" ", "")
            return "*" * max(len(compact) - 4, 4) + (compact[-4:] if len(compact) >= 8 else "")

        # Passport, licence and anything else: first and last character only.
        if len(value) > 4:
            return f"{value[0]}{'*' * (len(value) - 2)}{value[-1]}"
        return '*' * len(value)

    def process_value(
        self,
        value: Any,
        column_name: Optional[str] = None
    ) -> Tuple[Any, List[Dict]]:
        """
        Detect and mask PII in one cell.

        Returns (processed_value, detections). A detection records the type, how it was found
        and the masked form; the original value is never stored in it.
        """
        if value is None or isinstance(value, bool):
            return value, []

        column_type = self.detect_pii_type_from_column(column_name) if column_name else None
        if column_type is not None:
            strategy = self._strategy_for(column_type, column_name)
            text = value if isinstance(value, str) else _as_text(value)
            if strategy == MaskingStrategy.NONE or not text:
                return value, []
            return self.mask_value(text, column_type, strategy), [{
                'type': column_type.value, 'source': 'column_name', 'column': column_name}]

        if not isinstance(value, str) or not value:
            return value, []

        text, withheld = self._within_scan_limit(value)
        detections: List[Dict] = []
        pieces: List[str] = []
        cursor = 0
        for pii_type, matched, start, end in self.detect_pii_in_value(text):
            strategy = self._strategy_for(pii_type, column_name)
            if strategy == MaskingStrategy.NONE:
                continue
            masked = self.mask_value(matched, pii_type, strategy)
            pieces.append(text[cursor:start])
            pieces.append(masked)
            cursor = end
            detections.append({'type': pii_type.value, 'source': 'pattern', 'masked': masked,
                               'column': column_name})
        if not detections and not withheld:
            return value, []
        pieces.append(text[cursor:])
        processed = "".join(pieces)
        if withheld:
            processed += f" [{withheld} more characters withheld: beyond the PII scan limit]"
            detections.append({'type': 'oversized_value', 'source': 'scan_limit', 'column': column_name})
        return processed, detections

    def _within_scan_limit(self, value: str) -> Tuple[str, int]:
        """(text to scan, characters withheld). The cut backs off to the last whitespace in the
        final 64 characters so a value straddling the limit is not left half visible."""
        limit = self.max_scan_chars
        if len(value) <= limit:
            return value, 0
        window = max(0, limit - 64)
        space = max(value.rfind(ch, window, limit) for ch in " \t\r\n")
        cut = space if space != -1 else window
        return value[:cut], len(value) - cut

    def process_row(self, row: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict]]:
        """Detect and mask PII in every column of one row."""
        processed_row = {}
        all_detections = []
        for column, value in row.items():
            processed_value, detections = self.process_value(value, column)
            processed_row[column] = processed_value
            all_detections.extend(detections)
        return processed_row, all_detections

    def process_results(
        self,
        results: List[Dict[str, Any]],
        skip_columns: Optional[Collection[str]] = None
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """
        Detect and mask PII in query results.

        Args:
            results: Row dictionaries
            skip_columns: Result keys to leave untouched. The security layer passes the columns
                a policy action already transformed, so a "[HASH:...]" is not masked again.

        Returns (processed_results, summary). The summary counts detections and keeps the
        first ten as examples; it never holds an original value.
        """
        skip = frozenset(skip_columns or ())
        processed_results = []
        total = 0
        rows_affected = 0
        withheld = 0
        types = set()
        columns = set()
        examples: List[Dict] = []

        for row in results:
            processed_row = {}
            row_hit = False
            for column, value in row.items():
                if column in skip:
                    processed_row[column] = value
                    continue
                processed_value, detections = self.process_value(value, column)
                processed_row[column] = processed_value
                for d in detections:
                    if d['source'] == 'scan_limit':
                        withheld += 1
                        continue
                    total += 1
                    row_hit = True
                    types.add(d['type'])
                    columns.add(column)
                    if len(examples) < 10:
                        examples.append(d)
            rows_affected += row_hit
            processed_results.append(processed_row)

        summary = {
            'total_detections': total,
            'rows_affected': rows_affected,
            'columns_affected': sorted(columns, key=str),
            'types_detected': sorted(types),
            'detections': examples,
            'values_withheld': withheld,
        }
        if total:
            logger.info("PII detection: %d instance(s) in %d column(s)", total, len(columns))
        return processed_results, summary


def enabled_types_from(names: Iterable[str]) -> List[PIIType]:
    """PIIType members for their string values; raises ValueError naming an unknown one."""
    return [PIIType(str(name).strip().lower()) for name in names]


# Convenience function
def mask_pii(
    results: List[Dict[str, Any]],
    strategy: MaskingStrategy = MaskingStrategy.PARTIAL
) -> List[Dict[str, Any]]:
    """Mask PII in query results with one strategy for every type."""
    detector = PIIDetector(default_strategy=strategy)
    processed, _ = detector.process_results(results)
    return processed
