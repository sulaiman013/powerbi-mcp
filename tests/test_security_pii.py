"""
PII detection and masking (src/security/pii_detector.py): column-name heuristics read whole
words of the column part only, values are validated before they count (Luhn, plausible SSN,
IBAN checksum), ordinary dates and digit runs are left alone, numbers in PII-named columns
are masked, matches never overlap, and every scan is linear with a per-value cap.
No Power BI required. Run: python tests/test_security_pii.py
"""
import datetime
import decimal
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))
from security.pii_detector import MaskingStrategy, PIIDetector, PIIType, mask_pii  # noqa: E402

_failures = []


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f": {detail}" if detail and not cond else ""))
    if not cond:
        _failures.append(name)


def eq(name, actual, expected):
    check(name, actual == expected, f"got {actual!r}, expected {expected!r}")


def test_column_names():
    print("\n== column-name heuristic: whole words, column part only ==")
    det = PIIDetector()
    expected = {
        PIIType.SSN: ["SSN", "ssn", "SSN Number", "Customer SSN", "CustomerSSN", "Social Security Number",
                      "social_security_no", "SocialSecurityNumber"],
        PIIType.CREDIT_CARD: ["Card Number", "card_number", "CardNumber", "Credit Card", "CreditCardNumber",
                              "Customer Credit Card No", "CC Number", "cardnumber"],
        PIIType.EMAIL: ["Email", "E-mail", "eMail", "Email Address", "Work Email", "CustomerEmail", "Email 2",
                        "emailaddress", "Contact E-Mail Address"],
        PIIType.PHONE: ["Phone", "Phone Number", "Home Phone", "Mobile Phone", "Cell Phone", "Telephone", "Tel",
                        "Mobile", "Mobile Number", "Cell", "Fax", "PhoneNumber", "Phone 2", "Work Phone No"],
        PIIType.DATE_OF_BIRTH: ["DOB", "Date of Birth", "BirthDate", "birth_date", "Birthday"],
        PIIType.IP_ADDRESS: ["IP", "IP Address", "Client IP", "IPAddress", "Source IP Address"],
        PIIType.BANK_ACCOUNT: ["IBAN", "Bank Account", "Bank Account Number", "Routing Number"],
        PIIType.PASSPORT: ["Passport", "Passport Number"],
        PIIType.DRIVERS_LICENSE: ["Drivers License", "Driver's License Number", "Driving Licence"],
        PIIType.NAME: ["First Name", "LastName", "Full Name", "Customer Name", "Surname", "Employee Name"],
    }
    for pii_type, names in expected.items():
        for name in names:
            eq(f"{pii_type.value}: {name}", det.detect_pii_type_from_column(f"T[{name}]"), pii_type)

    for name in ("Description", "Subscription", "Shipping Region", "ShipDate", "Zip", "Recipe", "Business Unit",
                 "Pricing", "Housing", "Hotel", "Category", "Telemetry", "Intel Score", "City", "Product Name",
                 "Name", "File Name", "Company Name", "Count", "Day", "Account Number", "Order Number",
                 "Email Opt In", "Email Verified", "Email Domain", "Emails Sent", "Phone Type", "Phone Calls Count",
                 "Mobile App Sessions", "Mobile Sessions", "Cell Count", "Battery Cell", "IP Country",
                 "Card Type", "Card", "Gift Card Balance", "Credit Card Fee", "Birth Year", "License Plate State",
                 "Mailing List", "Sin Tax Rate", "Passport Country", "Smartphone Model"):
        eq(f"not PII by name: {name}", det.detect_pii_type_from_column(f"T[{name}]"), None)

    eq("the table part is ignored", det.detect_pii_type_from_column("Email Campaigns[Subject]"), None)
    eq("quoted table, PII column", det.detect_pii_type_from_column("'Phone Book'[Email]"), PIIType.EMAIL)
    eq("a measure key", det.detect_pii_type_from_column("[Customer Email]"), PIIType.EMAIL)
    eq("a bare name", det.detect_pii_type_from_column("email"), PIIType.EMAIL)
    eq("non-string key", det.detect_pii_type_from_column(5), None)

    only = PIIDetector(enabled_types=[PIIType.EMAIL])
    eq("a disabled type is not reported", only.detect_pii_type_from_column("T[SSN]"), None)
    eq("... while the enabled one is", only.detect_pii_type_from_column("T[Email]"), PIIType.EMAIL)
    only.enabled_types.append(PIIType.SSN)
    eq("enabling a type later takes effect", only.detect_pii_type_from_column("T[SSN]"), PIIType.SSN)


def test_values_are_validated():
    print("\n== value detection: only shapes that validate ==")
    det = PIIDetector()

    def types(value):
        return [(t.value, text) for t, text, _, _ in det.detect_pii_in_value(value)]

    eq("e-mail", types("mail jane.doe+x@mail.example.co.uk today"), [("email", "jane.doe+x@mail.example.co.uk")])
    eq("e-mail followed by a full stop", types("Write to bob@example.org."), [("email", "bob@example.org")])
    eq("two e-mails", [t for t, _ in types("a@b.com,c@d.org")], ["email", "email"])
    for bad in ("a@b", "user@host.1", "@example.com", "a@.com", "x@y..com", "5 @ 6", "name@localhost"):
        eq(f"not an e-mail: {bad}", types(bad), [])

    eq("Visa, Luhn-valid", types("4111 1111 1111 1111"), [("credit_card", "4111 1111 1111 1111")])
    eq("Mastercard 2-series", types("2221000000000009"), [("credit_card", "2221000000000009")])
    eq("Amex with dashes", types("3782-822463-10005"), [("credit_card", "3782-822463-10005")])
    eq("JCB", types("3530111333300000"), [("credit_card", "3530111333300000")])
    eq("card followed by another number", types("card 4111 1111 1111 1111 2024"), [("credit_card", "4111 1111 1111 1111")])
    for bad in ("4111 1111 1111 1112", "4000-0000-0000-0001", "1234567812345678", "9999999999999995",
                "0000000000000000", "4111111111111111.50", "A4111111111111111"):
        eq(f"not a card (Luhn / issuer / context): {bad}", [t for t, _ in types(bad)], [])

    eq("SSN with dashes", types("ssn 123-45-6789."), [("ssn", "123-45-6789")])
    eq("SSN with spaces", types("123 45 6789"), [("ssn", "123 45 6789")])
    for bad in ("000-12-3456", "666-12-3456", "123-00-4567", "123-45-0000", "123456789", "123-45-67890",
                "1123-45-6789", "123-45 6789", "12-345-6789"):
        eq(f"not an SSN: {bad}", [t for t, _ in types(bad) if t == "ssn"], [])

    eq("US phone", types("call (555) 123-4567 now"), [("phone", "(555) 123-4567")])
    eq("US phone with dots", types("555.123.4567"), [("phone", "555.123.4567")])
    eq("US phone with country code", types("+1-555-123-4567"), [("phone", "+1-555-123-4567")])
    eq("international phone", types("+44 20 7946 0958"), [("phone", "+44 20 7946 0958")])
    for bad in ("5551234567", "4500012345", "12345", "+15", "2024-03-15", "03/15/2024", "2024-03-15T10:30:00+05:30",
                "12/25/2023 12:00:00 AM", "10:30:45", "1,234,567.89", "INV-2024-000123"):
        eq(f"not PII by shape: {bad}", types(bad), [])

    eq("IPv4", types("from 192.168.10.25,"), [("ip_address", "192.168.10.25")])
    eq("IPv6", types("addr 2001:db8:85a3::8a2e:370:7334 ok"), [("ip_address", "2001:db8:85a3::8a2e:370:7334")])
    for bad in ("999.1.1.1", "1.2.3", "1.2.3.4.5", "12:30:45", "00:1A:2B:3C:4D:5E", "a::b"):
        eq(f"not an IP: {bad}", types(bad), [])

    eq("IBAN with spaces", types("pay GB82 WEST 1234 5698 7654 32 today"), [("bank_account", "GB82 WEST 1234 5698 7654 32")])
    eq("IBAN compact", types("DE89370400440532013000"), [("bank_account", "DE89370400440532013000")])
    eq("IBAN with a wrong checksum is not one", types("GB82 WEST 1234 5698 7654 33"), [])

    eq("non-string input", det.detect_pii_in_value(12345), [])
    eq("empty string", det.detect_pii_in_value(""), [])
    limited = PIIDetector(enabled_types=[PIIType.EMAIL])
    eq("only enabled types are detected",
       [(t.value) for t, _, _, _ in limited.detect_pii_in_value("a@b.com 123-45-6789 192.168.1.1")], ["email"])


def test_masking_and_no_overlap():
    print("\n== masking: one mask per value, no garbling ==")
    det = PIIDetector()

    def masked(value, column="T[Note]"):
        return det.process_value(value, column)

    eq("US phone masked once", masked("555-123-4567"), ("(***) ***-4567", [
        {'type': 'phone', 'source': 'pattern', 'masked': '(***) ***-4567', 'column': 'T[Note]'}]))
    eq("phone with parentheses", masked("(555) 123-4567")[0], "(***) ***-4567")
    eq("+1 phone matched by two patterns is masked once", masked("call +1-555-123-4567 now")[0],
       "call (***) ***-4567 now")
    eq("several kinds in one text",
       masked("Jane <jane@example.com>, 555-123-4567, SSN 123-45-6789, card 4111-1111-1111-1111, ip 10.20.30.40")[0],
       "Jane <j***@e****.com>, (***) ***-4567, SSN ***-**-6789, card ****-****-****-1111, ip 10.20.***.***")
    value, found = masked("x@y.org and x@y.org")
    eq("repeats are each masked", (value, len(found)), ("x***@y****.org and x***@y****.org", 2))
    eq("IPv6 partial mask", masked("2001:db8:85a3::8a2e:370:7334")[0], "2001:db8:****")
    eq("IBAN partial mask", masked("DE89370400440532013000")[0], "******************3000")
    eq("text without PII is returned as is", masked("Blue cotton t-shirt, size 42"), ("Blue cotton t-shirt, size 42", []))
    eq("an order date and number are not touched", masked("2024-03-15 order 4500012345"), ("2024-03-15 order 4500012345", []))
    check("no detection carries the original value",
          all("jane@example.com" not in str(d) for d in masked("mail jane@example.com")[1]))

    eq("None passes through", masked(None), (None, []))
    eq("booleans pass through", det.process_value(True, "T[Email]"), (True, []))
    eq("numbers outside PII columns pass through", masked(5551234567), (5551234567, []))


def test_pii_named_columns_mask_any_type():
    print("\n== a PII-named column is masked whatever the value's type ==")
    det = PIIDetector()
    eq("integer SSN", det.process_value(123456789, "Customers[SSN Number]")[0], "***-**-6789")
    eq("integer card", det.process_value(4111111111111111, "Customers[Card Number]")[0], "****-****-****-1111")
    eq("integer phone", det.process_value(5551234567, "Customers[Phone]")[0], "(***) ***-4567")
    eq("float phone", det.process_value(5551234567.0, "Customers[Phone]")[0], "(***) ***-4567")
    eq("Decimal card", det.process_value(decimal.Decimal("4111111111111111"), "C[Credit Card]")[0], "****-****-****-1111")
    eq("date of birth as a date", det.process_value(datetime.date(1984, 7, 23), "C[DOB]")[0], "****-**-**")
    eq("date of birth as text", det.process_value("07/23/1984", "C[Date of Birth]")[0], "**/**/****")
    eq("an e-mail column masks whatever is in it", det.process_value("n/a", "C[Email]")[0], "***@***.***")
    eq("name column", det.process_value("Jane van Doe", "C[Customer Name]")[0], "J*** v** D**")
    eq("short SSN text does not crash", det.process_value("12", "C[SSN]")[0], "***-**-****")
    eq("empty text is left empty", det.process_value("", "C[Email]"), ("", []))
    _, found = det.process_value(123456789, "Customers[SSN Number]")
    eq("the detection says how it was found",
       found, [{'type': 'ssn', 'source': 'column_name', 'column': 'Customers[SSN Number]'}])


def test_strategies():
    print("\n== strategies: default, per type, per column ==")
    det = PIIDetector(default_strategy=MaskingStrategy.REDACT,
                      type_strategies={PIIType.EMAIL: MaskingStrategy.HASH, PIIType.PHONE: MaskingStrategy.NONE,
                                       PIIType.SSN: MaskingStrategy.FULL},
                      column_overrides={"c[backup email]": MaskingStrategy.PARTIAL, "Fax": MaskingStrategy.FULL})
    first = det.process_value("jane@example.com", "C[Email]")[0]
    check("hash strategy is keyed and stable", first.startswith("[HASH:") and first == det.process_value(
        "jane@example.com", "D[Email]")[0] and first != det.process_value("john@example.com", "C[Email]")[0], first)
    eq("none: detection off for that type", det.process_value("555-123-4567", "C[Phone]"), ("555-123-4567", []))
    eq("none also applies to values found by shape", det.process_value("call 555-123-4567", "C[Note]")[0],
       "call 555-123-4567")
    eq("full", det.process_value("123-45-6789", "C[Note]")[0], "**********")
    eq("default for the rest", det.process_value("10.0.0.1", "C[Note]")[0], "[REDACTED-IP_ADDRESS]")
    eq("override by full key", det.process_value("jane@example.com", "C[Backup Email]")[0], "j***@e****.com")
    eq("override by column name", det.process_value("555-123-4567", "C[Fax]")[0], "**********")
    eq("mask_pii convenience", mask_pii([{"C[Email]": "a@b.com"}], MaskingStrategy.REDACT), [{"C[Email]": "[REDACTED-EMAIL]"}])


def test_process_results_summary_and_skip():
    print("\n== process_results: summary, skip columns ==")
    det = PIIDetector()
    rows = [{"C[Email]": "a@b.com", "C[Note]": "ring 555-123-4567", "C[Id]": 1, "C[Hash]": "[HASH:0123456789abcdef]"},
            {"C[Email]": "c@d.org", "C[Note]": "nothing here", "C[Id]": 2, "C[Hash]": "x@y.org"},
            {"C[Email]": None, "C[Note]": None, "C[Id]": 3, "C[Hash]": None}]
    out, summary = det.process_results(rows, skip_columns=["C[Hash]"])
    eq("skip columns are left exactly as they are", [r["C[Hash]"] for r in out], ["[HASH:0123456789abcdef]", "x@y.org", None])
    eq("total detections", summary["total_detections"], 3)
    eq("rows affected counts rows", summary["rows_affected"], 2)
    eq("columns affected", summary["columns_affected"], ["C[Email]", "C[Note]"])
    eq("types detected", summary["types_detected"], ["email", "phone"])
    check("examples never hold a raw value", "a@b.com" not in str(summary) and "555-123-4567" not in str(summary), str(summary))
    eq("input rows are not mutated", rows[0]["C[Email]"], "a@b.com")
    many = [{"C[Email]": f"user{i}@example.com"} for i in range(50)]
    _, summary = det.process_results(many)
    eq("examples are capped at ten", (summary["total_detections"], len(summary["detections"])), (50, 10))
    eq("empty input", det.process_results([])[0], [])


def test_linear_and_capped():
    print("\n== linear-time scanning with a per-value cap ==")
    det = PIIDetector()
    hostile = [
        ("a." * 100000, "dotted text"),
        ("%20" * 70000, "percent-encoded text"),
        ("ab-" * 70000, "hyphenated ids"),
        ("QUJD+" * 40000, "base64 with plus signs"),
        ("x@" + "a." * 100000, "an @ followed by a very long dotted run"),
        ("@" * 200000, "nothing but @"),
        ("1 " * 100000, "digits separated by spaces"),
        ("1-" * 100000, "digits separated by dashes"),
        ("1." * 100000, "digits separated by dots"),
        (":" * 200000, "nothing but colons"),
        ("+1 " * 70000, "plus signs and digits"),
        ("AB12 " * 40000, "IBAN-like prefixes"),
        ("9" * 200000, "one long digit run"),
    ]
    big = PIIDetector(max_scan_chars=10_000_000)
    for text, label in hostile:
        start = time.time()
        big.process_value(text, "T[Blob]")
        elapsed = time.time() - start
        check(f"{label} ({len(text):,} chars) in {elapsed:.2f}s", elapsed < 3.0, f"{elapsed:.2f}s")

    small = PIIDetector(max_scan_chars=200)
    text = "contact a@b.com " + "lorem ipsum " * 40 + "secret tail c@d.org 123-45-6789"
    out, found = small.process_value(text, "T[Blob]")
    check("the scanned part is masked", "a***@b****.com" in out, out[:60])
    check("the part beyond the limit is withheld, not passed through",
          "c@d.org" not in out and "123-45-6789" not in out and "withheld" in out, out[-90:])
    check("the cut does not leave a half token", out.split(" [")[0].endswith(("ipsum", "lorem")), repr(out[150:230]))
    check("the detection list notes it", any(d["source"] == "scan_limit" for d in found), str(found))
    _, summary = small.process_results([{"T[Blob]": text}])
    eq("the summary counts withheld values", summary["values_withheld"], 1)
    eq("a short value is untouched by the cap", small.process_value("plain text", "T[Blob]"), ("plain text", []))
    solid = PIIDetector(max_scan_chars=100)
    out, _ = solid.process_value("x" * 80 + "4111111111111111" + "y" * 50, "T[Blob]")
    check("a value straddling the limit is not left half visible", "4111" not in out, out)

    rows = [{"T[a]": "x" * 50, "T[b]": i, "T[c]": "2024-03-15", "T[d]": "Order shipped to customer"} for i in range(20000)]
    start = time.time()
    det.process_results(rows)
    elapsed = time.time() - start
    check(f"80,000 ordinary cells in {elapsed:.2f}s", elapsed < 5.0, f"{elapsed:.2f}s")


if __name__ == "__main__":
    for fn in (test_column_names, test_values_are_validated, test_masking_and_no_overlap,
               test_pii_named_columns_mask_any_type, test_strategies, test_process_results_summary_and_skip,
               test_linear_and_capped):
        fn()
    print()
    if _failures:
        print(f"FAILED: {len(_failures)} check(s): {_failures}")
        sys.exit(1)
    print("ALL PII TESTS PASSED")
