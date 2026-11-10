"""
Tests for secret redaction (src/security/redaction.py): every credential carrier the server
can meet is masked, a match never swallows the text that follows it, the scan is linear, and
structured content is walked. No Power BI required.
Run: python tests/test_security_redaction.py
"""
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))
from security import redact_secrets, redact_structure  # noqa: E402
from security.redaction import is_secret_field  # noqa: E402

_failures = []


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f": {detail}" if detail and not cond else ""))
    if not cond:
        _failures.append(name)


def eq(name, actual, expected):
    check(name, actual == expected, f"got {actual!r}, expected {expected!r}")


def test_connection_strings():
    print("\n== connection strings ==")
    eq("Password= up to the next ;",
       redact_secrets("Provider=MSOLAP;Password=SuperSecret123;User ID=app:x"),
       "Provider=MSOLAP;Password=***;User ID=app:x")
    eq("quoted value containing ;",
       redact_secrets('Data Source=x;Password="ab;cd;ef";Initial Catalog=Sales'),
       "Data Source=x;Password=***;Initial Catalog=Sales")
    eq("single-quoted value with a doubled quote",
       redact_secrets("Password='it''s;here';Timeout=30"), "Password=***;Timeout=30")
    eq("double-quoted value with a doubled quote",
       redact_secrets('Password="a""b;c";Timeout=30'), "Password=***;Timeout=30")
    eq("unterminated quote is redacted to the next ;",
       redact_secrets('Password="abc def;User ID=x'), "Password=***;User ID=x")
    eq("Pwd=", redact_secrets("Server=s;Pwd=hunter2;Database=d"), "Server=s;Pwd=***;Database=d")
    eq("spaces around =", redact_secrets("PASSWORD = hunter2;x=1"), "PASSWORD = ***;x=1")
    eq("AccountKey=", redact_secrets("AccountName=acct;AccountKey=Zm9vYmFyYmF6==;EndpointSuffix=core"),
       "AccountName=acct;AccountKey=***;EndpointSuffix=core")
    eq("SharedAccessKey=", redact_secrets("Endpoint=sb://x;SharedAccessKey=abcd1234efgh=;EntityPath=q"),
       "Endpoint=sb://x;SharedAccessKey=***;EntityPath=q")
    eq("SharedAccessSignature= whole value",
       redact_secrets("BlobEndpoint=https://a.blob.core.windows.net;SharedAccessSignature=sv=2022&sig=abc&se=2030;x=1"),
       "BlobEndpoint=https://a.blob.core.windows.net;SharedAccessSignature=***;x=1")


def test_tokens_and_headers():
    print("\n== tokens, headers, URL and JSON forms ==")
    eq("client_secret in a URL body",
       redact_secrets("grant_type=client_credentials&client_secret=abcDEF123456&scope=x"),
       "grant_type=client_credentials&client_secret=***&scope=x")
    eq("client_secret in JSON", redact_secrets('{"client_id": "id", "client_secret": "abcDEF123456", "n": 1}'),
       '{"client_id": "id", "client_secret": "***", "n": 1}')
    eq("clientSecret in JSON", redact_secrets('{"clientSecret":"abcDEF123456"}'), '{"clientSecret":"***"}')
    eq("JSON value with an escaped quote", redact_secrets(r'{"password": "a\"b", "k": 2}'),
       '{"password": "***", "k": 2}')
    eq("YAML-style password", redact_secrets("user: bob\npassword: hunter2\nhost: h"),
       "user: bob\npassword: ***\nhost: h")
    eq("access_token in JSON", redact_secrets('{"token_type":"Bearer","access_token":"eyJabc.def.ghi"}'),
       '{"token_type":"Bearer","access_token":"***"}')
    eq("Authorization: Bearer", redact_secrets("GET /x\nAuthorization: Bearer abc.def-123\nAccept: */*"),
       "GET /x\nAuthorization: Bearer ***\nAccept: */*")
    eq("Authorization header in a dict repr",
       redact_secrets("{'Authorization': 'Bearer abcdefghijklmnop', 'Accept': 'json'}"),
       "{'Authorization': 'Bearer ***', 'Accept': 'json'}")
    eq("bare Bearer token", redact_secrets("sent Bearer abcdefghijklmnopqrstuvwxyz012345 to host"),
       "sent Bearer *** to host")
    jwt = "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.c2lnbmF0dXJlLWJ5dGVz"
    eq("bare JWT", redact_secrets(f"token was {jwt} at 10:00"), "token was *** at 10:00")
    eq("SAS sig= in a URL",
       redact_secrets("https://a.blob.core.windows.net/c/b?sv=2022-11-02&sig=AbCdEf0123456789%2Bxyz%3D&se=2030-01-01"),
       "https://a.blob.core.windows.net/c/b?sv=2022-11-02&sig=***&se=2030-01-01")
    eq("api_key=", redact_secrets("GET /v1?api_key=k_live_12345&page=2"), "GET /v1?api_key=***&page=2")
    eq("secret= (legacy pattern)", redact_secrets("secret=abc123 next"), "secret=*** next")


def test_literals():
    print("\n== known secret values ==")
    eq("literal removed everywhere", redact_secrets("id abcDEF123456 and again abcDEF123456.", ["abcDEF123456"]),
       "id *** and again ***.")
    eq("URL-encoded form of the literal", redact_secrets("x=p%40ss%2Fword1&y=2", ["p@ss/word1"]), "x=***&y=2")
    eq("short literal is ignored", redact_secrets("abc12 stays", ["abc12"]), "abc12 stays")
    eq("empty and None literals are a no-op", redact_secrets("hello world", ["", None]), "hello world")
    eq("longest literal wins", redact_secrets("key=SECRETVALUE-EXTRA", ["SECRETVALUE", "SECRETVALUE-EXTRA"]), "key=***")
    eq("None becomes an empty string", redact_secrets(None), "")
    eq("non-string input is stringified", redact_secrets(12345), "12345")


def test_does_not_eat_the_rest():
    print("\n== a match never swallows what follows it ==")
    text = ('Query returned 3 row(s)\n\n[\n  {"T[Note]": "reset: Password = see wiki"},\n'
            '  {"T[Note]": "row 2"},\n  {"T[Note]": "row 3"}\n]')
    out = redact_secrets(text)
    check("later rows survive a Password= inside a cell", '"row 2"' in out and '"row 3"' in out and out.endswith("]"), out)
    check("the value itself is masked", "see wiki" not in out, out)
    eq("a key at the end of a line does not pull in the next line",
       redact_secrets("Password=\nnext line stays\nsecret=\nthis too"), "Password=***\nnext line stays\nsecret=\nthis too")
    eq("text after an unquoted secret on other lines stays",
       redact_secrets("error: Pwd=abc\nline two\nline three"), "error: Pwd=***\nline two\nline three")
    eq("a closing quote ends the value", redact_secrets('connect("Server=s;Password=abc") failed, retry'),
       'connect("Server=s;Password=***") failed, retry')
    dax = 'IF ( SELECTEDVALUE ( Users[Password] ) = "x", 1 )\nMeasure B = SUM ( Sales[Amount] )'
    eq("a column called Password in DAX is not a key=value", redact_secrets(dax), dax)
    plain = "The token count per key is 42; no secrets here: just words like password and bearer."
    eq("ordinary prose is unchanged", redact_secrets(plain), plain)


def test_idempotent_and_linear():
    print("\n== idempotent, and linear in the input ==")
    sample = ('Provider=MSOLAP;Password="a;b";Pwd=x1;client_secret=abcDEF123456&z=1 {"secret": "s3cr3t!"} '
              'Authorization: Bearer abc.def.ghi sig=AbCdEf0123456789abcdef AccountKey=Zm9vYmFy==;')
    once = redact_secrets(sample, ["abcDEF123456"])
    eq("redacting twice changes nothing", redact_secrets(once, ["abcDEF123456"]), once)
    check("nothing sensitive is left", not any(s in once for s in ("a;b", "x1;", "abcDEF", "s3cr3t", "abc.def", "Zm9v")), once)
    hostile = [
        ("password " * 20000, "many keys without a value"),
        ("eyJ" + "a" * 200000, "a JWT prefix that never completes"),
        ("eyJ-" * 50000, "JWT prefixes separated by hyphens"),
        ('Password="' + "x" * 200000, "an unterminated quoted value"),
        ("bearer " * 30000, "bearer without a token"),
        ('"secret": ' * 20000, "JSON keys without values"),
        ("sig=" * 50000, "sig= without a signature"),
        ("a." * 100000, "dotted text with no secrets"),
    ]
    for text, label in hostile:
        start = time.time()
        redact_secrets(text)
        elapsed = time.time() - start
        check(f"linear on {label} ({len(text):,} chars)", elapsed < 2.0, f"{elapsed:.2f}s")


def test_structure():
    print("\n== redact_structure ==")
    original = {
        "error": "login failed: Password=abc123;",
        "rows": [{"note": "Pwd=x9;ok"}, ("client_secret=abcDEF123456", 5)],
        "password": "plain-value",
        "clientSecret": {"value": "nested"},
        "db_password": 12345678,
        "auth": {"access_token": "tok", "expires_in": 3600},
        "blanks": {"password": "", "api_key": None, "secret": False},
        "count": 3,
        "ok": True,
        "nextToken": "page-2",
    }
    out = redact_structure(original, ["abcDEF123456"])
    eq("strings in nested dicts", out["error"], "login failed: Password=***;")
    eq("strings in lists and tuples", out["rows"], [{"note": "Pwd=***;ok"}, ("client_secret=***", 5)])
    eq("a value under a credential key is replaced whole", out["password"], "***")
    eq("... even when it is a container", out["clientSecret"], "***")
    eq("... or a number", out["db_password"], "***")
    eq("nested credential key", out["auth"], {"access_token": "***", "expires_in": 3600})
    eq("empty, None and boolean values are left alone", out["blanks"],
       {"password": "", "api_key": None, "secret": False})
    eq("other scalars pass through", (out["count"], out["ok"]), (3, True))
    eq("a paging token is not a credential", out["nextToken"], "page-2")
    eq("the input is not mutated", original["password"], "plain-value")
    eq("a plain string works too", redact_structure("Pwd=abc;"), "Pwd=***;")
    loop = {"name": "x"}
    loop["self"] = loop
    eq("a cycle is cut, not followed forever", redact_structure(loop), {"name": "x", "self": "<circular>"})
    check("field-name detection", is_secret_field("Client Secret") and is_secret_field("apiKey")
          and not is_secret_field("secretary") and not is_secret_field("keyboard") and not is_secret_field(7))


if __name__ == "__main__":
    for fn in (test_connection_strings, test_tokens_and_headers, test_literals, test_does_not_eat_the_rest,
               test_idempotent_and_linear, test_structure):
        fn()
    print()
    if _failures:
        print(f"FAILED: {len(_failures)} check(s): {_failures}")
        sys.exit(1)
    print("ALL REDACTION TESTS PASSED")
