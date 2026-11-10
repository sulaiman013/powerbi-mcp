"""
Tests for src/safe_io.py: encoding and newline fidelity, atomic single writes, and the
all-or-nothing FileTransaction (including restore after a mid-commit failure).
No Power BI required. Run: python tests/test_safe_io.py
"""
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))
import safe_io  # noqa: E402

_failures = []


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f": {detail}" if detail and not cond else ""))
    if not cond:
        _failures.append(name)


def raw(path):
    with open(path, "rb") as f:
        return f.read()


def put(path, data: bytes):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        f.write(data)


def test_format_fidelity():
    print("\n== format fidelity ==")
    d = tempfile.mkdtemp()
    cases = {
        "lf.tmdl": b"table A\n\tcolumn x\n",
        "crlf.tmdl": b"table A\r\n\tcolumn x\r\n",
        "bom_crlf.json": b"\xef\xbb\xbf{\r\n  \"a\": 1\r\n}",
        "bom_lf.json": b"\xef\xbb\xbf{\n  \"a\": \"caf\xc3\xa9\"\n}",
    }
    for name, data in cases.items():
        p = os.path.join(d, name)
        put(p, data)
        text, fmt = safe_io.read_text(p)
        check(f"{name}: text has no BOM or CR", chr(0xFEFF) not in text and "\r" not in text)
        safe_io.write_text(p, text)
        check(f"{name}: unchanged text round-trips byte for byte", raw(p) == data)
        safe_io.write_text(p, text.replace("A", "B").replace("1", "2"))
        after = raw(p)
        check(f"{name}: BOM kept after an edit", after.startswith(b"\xef\xbb\xbf") == data.startswith(b"\xef\xbb\xbf"))
        check(f"{name}: newline style kept after an edit",
              (b"\r\n" in after) == (b"\r\n" in data) and after.count(b"\n") == data.count(b"\n"))
    new = os.path.join(d, "sub", "new.json")
    safe_io.write_json(new, {"name": "Größe", "n": 1})
    check("new file: UTF-8, no BOM, LF, non-ASCII kept", raw(new) == '{\n  "name": "Größe",\n  "n": 1\n}'.encode("utf-8"))
    check("no temp files left behind", not [f for _, _, fs in os.walk(d) for f in fs if f.endswith(".tmp")])
    check("mixed CRLF input is normalized once", safe_io.encode("a\r\nb\nc", safe_io.TextFormat(newline="\r\n")) == b"a\r\nb\r\nc")


def test_transaction():
    print("\n== transaction ==")
    d = tempfile.mkdtemp()
    a, b, c = (os.path.join(d, n) for n in ("a.tmdl", "b.json", "new", ))
    c = os.path.join(c, "c.json")
    put(a, b"one\r\n"), put(b, b"\xef\xbb\xbf{}")
    tx = safe_io.FileTransaction()
    tx.write(a, tx.read(a).replace("one", "two"))
    check("staged edit is visible to a later read", tx.read(a) == "two\n")
    check("disk is untouched before commit", raw(a) == b"one\r\n")
    tx.write_json(b, {"k": 1})
    tx.write(c, "x\n")
    check("exists() sees staged new files", tx.exists(c) and not os.path.exists(c))
    changed = tx.commit()
    check("commit reports three files", len(changed) == 3)
    check("CRLF kept", raw(a) == b"two\r\n")
    check("BOM kept", raw(b) == b'\xef\xbb\xbf{\n  "k": 1\n}')
    check("new file created in a new folder", raw(c) == b"x\n")

    tx2 = safe_io.FileTransaction()
    tx2.write(a, tx2.read(a))
    check("an unchanged staged file is not reported", tx2.paths == [])
    check("committing nothing is a no-op", tx2.commit() == [])

    tx3 = safe_io.FileTransaction()
    tx3.delete(c)
    tx3.commit()
    check("staged delete removes the file", not os.path.exists(c))


def test_transaction_restores_on_failure():
    print("\n== transaction restore ==")
    d = tempfile.mkdtemp()
    paths = [os.path.join(d, f"f{i}.txt") for i in range(4)]
    for i, p in enumerate(paths):
        put(p, f"orig{i}\n".encode())
    new_path = os.path.join(d, "created.txt")
    tx = safe_io.FileTransaction()
    for p in paths:
        tx.write(p, "changed\n")
    tx.write(new_path, "created\n")

    real_replace = os.replace
    calls = {"n": 0}

    def flaky(src, dst):
        calls["n"] += 1
        if calls["n"] == 3:
            raise PermissionError("simulated lock")
        return real_replace(src, dst)

    os.replace = flaky
    try:
        try:
            tx.commit()
            check("commit raises when a move fails", False)
        except PermissionError:
            check("commit raises when a move fails", True)
    finally:
        os.replace = real_replace
    check("every original file is restored", all(raw(p) == f"orig{i}\n".encode() for i, p in enumerate(paths)),
          str([raw(p) for p in paths]))
    check("a file the transaction would have created is gone", not os.path.exists(new_path))
    check("no temp files left behind", not [f for f in os.listdir(d) if f.endswith(".tmp")], str(os.listdir(d)))
    check("transaction is not marked committed", not tx.committed)

    # A failure while preparing (before anything is replaced) changes nothing at all.
    tx4 = safe_io.FileTransaction()
    tx4.write(paths[0], "changed\n")
    tx4.write(os.path.join(paths[1], "impossible", "x.txt"), "nope\n")   # parent is a file
    try:
        tx4.commit()
        check("prepare failure raises", False)
    except OSError:
        check("prepare failure raises", True)
    check("prepare failure leaves disk untouched", raw(paths[0]) == b"orig0\n")


def test_file_name_case_is_preserved():
    print("\n== file name case ==")
    d = tempfile.mkdtemp()
    mixed = os.path.join(d, "Cultures", "en-US.tmdl")
    layout = os.path.join(d, "DiagramLayout.JSON")
    put(mixed, b"a\n"), put(layout, b"{}")
    tx = safe_io.FileTransaction()
    tx.write(mixed, "b\n")
    tx.write_json(layout, {"a": 1})
    fresh = os.path.join(d, "NewFile.Json")
    tx.write(fresh, "x\n")
    expected = ["DiagramLayout.JSON", "NewFile.Json", "en-US.tmdl"]
    reported = tx.paths
    check("paths keep their spelling before commit", sorted(os.path.basename(p) for p in reported) == expected, str(reported))
    committed = tx.commit()
    check("commit returns paths in their real spelling",
          sorted(os.path.basename(p) for p in committed) == expected, str(committed))
    check("existing files keep their case on disk",
          sorted(os.listdir(d)) == ["Cultures", "DiagramLayout.JSON", "NewFile.Json"], str(os.listdir(d)))
    check("nested file keeps its case", os.listdir(os.path.join(d, "Cultures")) == ["en-US.tmdl"])

    tx2 = safe_io.FileTransaction()
    tx2.write(mixed, "c\n")
    tx2.write(os.path.join(d, "Cultures", "en-US.tmdl"), "d\n")
    check("two writes to one file are one staged entry", len(tx2.paths) == 1)

    if os.path.normcase("A") != "A":      # case-insensitive file system (Windows)
        other = os.path.join(d, "diagramlayout.json")
        tx3 = safe_io.FileTransaction()
        tx3.write(other, '{"b": 2}')
        check("a differently-cased path reaches the same file", tx3.exists(other))
        tx3.commit()
        check("writing through another spelling does not rename the file",
              "DiagramLayout.JSON" in os.listdir(d), str(os.listdir(d)))
        safe_io.write_text(os.path.join(d, "newfile.json"), "y\n")
        check("a single write does not rename the file either", "NewFile.Json" in os.listdir(d), str(os.listdir(d)))


def test_mixed_line_endings_are_kept():
    print("\n== mixed line endings ==")
    d = tempfile.mkdtemp()
    p = os.path.join(d, "mixed.tmdl")
    original = b"table A\n\tmeasure X = 1\r\n\tmeasure Y = 2\r\n\tcolumn c\n\tcolumn d\n"
    put(p, original)
    text, fmt = safe_io.read_text(p)
    check("a mixed file is detected", fmt.mixed and fmt.newline == "")
    check("its text keeps each line's own ending", text.count("\r\n") == 2 and text.count("\n") == 5)
    safe_io.write_text(p, text)
    check("unchanged text round-trips byte for byte", raw(p) == original)
    safe_io.write_text(p, text.replace("measure X = 1", "measure X = 100"))
    check("an edit to one line leaves every other ending alone",
          raw(p) == original.replace(b"X = 1", b"X = 100"))

    tx = safe_io.FileTransaction()
    tx.write(p, tx.read(p).replace("column d", "column e"))
    tx.commit()
    after = raw(p)
    check("a transaction keeps mixed endings too", after.count(b"\r\n") == 2 and after.count(b"\n") == 5, repr(after))
    tx2 = safe_io.FileTransaction()
    tx2.write(p, tx2.read(p).replace("column e", "column d").replace("X = 100", "X = 1"))
    tx2.commit()
    check("there and back again is byte-identical", raw(p) == original)

    for name, data, style in (("lf", b"a\nb\n", "\n"), ("crlf", b"a\r\nb\r\n", "\r\n"), ("none", b"single line", "\n")):
        check(f"single-style file ({name}) is not treated as mixed", safe_io.detect_format(data).newline == style)


if __name__ == "__main__":
    for fn in (test_format_fidelity, test_transaction, test_transaction_restores_on_failure,
               test_file_name_case_is_preserved, test_mixed_line_endings_are_kept):
        fn()
    print()
    if _failures:
        print(f"FAILED: {len(_failures)} check(s): {_failures}")
        sys.exit(1)
    print("ALL SAFE IO TESTS PASSED")
