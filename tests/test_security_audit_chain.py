"""
Audit log chain (src/security/audit_logger.py): sequence numbers, a checkpoint that makes a
missing tail or head visible, a chain that continues across rotation and is verified in every
rotated file, rotation that works on Windows, two writers on one log, what gets stored of a
query, and write failures that show up in the status. Every log goes to a temporary folder.
No Power BI required. Run: python tests/test_security_audit_chain.py
"""
import hashlib
import json
import logging
import os
import subprocess
import sys
import tempfile
import threading

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))
from security.audit_logger import (  # noqa: E402
    AuditEventType,
    AuditLogger,
    AuditWriteError,
    _FileLock,
    strip_literals,
)
from security.security_layer import SecurityLayer  # noqa: E402

logging.disable(logging.CRITICAL)  # integrity problems are logged as errors by design

KEY_ENV = "POWERBI_MCP_AUDIT_KEY"
_failures = []


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f": {detail}" if detail and not cond else ""))
    if not cond:
        _failures.append(name)


def eq(name, actual, expected):
    check(name, actual == expected, f"got {actual!r}, expected {expected!r}")


class audit_key:
    """Set (or clear, with None) the audit key for a block, restoring it afterwards."""

    def __init__(self, value):
        self.value = value

    def __enter__(self):
        self.saved = os.environ.pop(KEY_ENV, None)
        if self.value is not None:
            os.environ[KEY_ENV] = self.value

    def __exit__(self, *exc):
        os.environ.pop(KEY_ENV, None)
        if self.saved is not None:
            os.environ[KEY_ENV] = self.saved


def read_lines(path):
    with open(path, encoding="utf-8") as f:
        return f.read().splitlines()


def write_lines(path, lines):
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + ("\n" if lines else ""))


def fill(log, count, prefix="e"):
    for i in range(count):
        log.log_event(AuditEventType.QUERY_SUCCESS, message=f"{prefix}{i}")


def test_entry_format():
    print("\n== entry format: sequence, chain, checkpoint, tool ==")
    with audit_key(None), tempfile.TemporaryDirectory() as tmp:
        log = AuditLogger(log_dir=tmp)
        log.log_query(query="EVALUATE Sales", result_count=3, duration_ms=12.5, tool="desktop_execute_dax")
        log.log_policy_violation("query_policy", "blocked", query="EVALUATE X", tool="run_dax_tests")
        log.log_event(AuditEventType.CONNECTION, message="hello")
        entries = [json.loads(line) for line in read_lines(log.log_file)]
        eq("sequence numbers start at 1 and increase", [e["seq"] for e in entries], [1, 2, 3])
        eq("first entry links to GENESIS", entries[0]["prev_hash"], "GENESIS")
        check("each entry links to the one before", all(entries[i]["prev_hash"] == entries[i - 1]["entry_hash"]
                                                          for i in (1, 2)))
        eq("algorithm is recorded", {e["alg"] for e in entries}, {"sha256"})
        eq("the tool is recorded", [e["tool"] for e in entries], ["desktop_execute_dax", "run_dax_tests", None])
        body = {k: v for k, v in entries[0].items() if k != "entry_hash"}
        eq("entry_hash is the SHA-256 of the sorted entry",
           entries[0]["entry_hash"], hashlib.sha256(json.dumps(body, sort_keys=True, default=str).encode()).hexdigest())
        checkpoint = json.loads(open(os.path.join(tmp, "audit.checkpoint.json"), encoding="utf-8").read())
        eq("checkpoint records the last entry", (checkpoint["seq"], checkpoint["last_hash"], checkpoint["first_seq"]),
           (3, entries[2]["entry_hash"], 1))
        result = log.verify_chain()
        check("verifies", result["valid"] and result["checked"] == 3 and result["first_seq"] == 1
              and result["last_seq"] == 3, str(result))
        check("without a key the message says what that means", "No audit key" in result["message"], result["message"])
        summary = log.get_session_summary()
        check("session summary: healthy, with the essentials",
              summary["healthy"] and summary["query_count"] == 1 and summary["last_seq"] == 3
              and summary["keyed"] is False and summary["log_file"].endswith("audit.log"), str(summary))

    empty = AuditLogger(log_dir=tempfile.gettempdir(), log_file="powerbi-mcp-never-written.log")
    eq("a log that was never written verifies as empty", empty.verify_chain()["message"], "No audit log file yet.")


def test_truncation_and_deletion(key):
    label = "with a key" if key else "without a key"
    print(f"\n== truncation and deletion are detected ({label}) ==")
    with audit_key(key), tempfile.TemporaryDirectory() as tmp:
        writer = AuditLogger(log_dir=tmp)
        fill(writer, 6)
        lines = read_lines(writer.log_file)
        checkpoint_path = os.path.join(tmp, "audit.checkpoint.json")
        checkpoint = open(checkpoint_path, encoding="utf-8").read()
        # A separate logger stands for a later process: it has no memory of what was written.
        later = AuditLogger(log_dir=tmp)
        check("intact log verifies", later.verify_chain()["valid"])

        edited = json.loads(lines[1])
        edited["message"] = "TAMPERED"
        write_lines(writer.log_file, [lines[0], json.dumps(edited)] + lines[2:])
        result = later.verify_chain()
        check("an edited entry is detected, at its line", result["valid"] is False and result.get("broken_line") == 2
              and result.get("broken_file") == "audit.log", str(result))

        cases = {
            "last three entries removed": lines[:-3],
            "last entry removed": lines[:-1],
            "first two entries removed": lines[2:],
            "only the middle kept": lines[2:4],
            "an entry removed from the middle": lines[:2] + lines[3:],
            "two entries swapped": [lines[0], lines[2], lines[1]] + lines[3:],
            "an entry duplicated": lines[:3] + [lines[2]] + lines[3:],
            "file emptied": [],
        }
        for case, kept in cases.items():
            write_lines(writer.log_file, kept)
            result = later.verify_chain()
            check(f"{case}: detected", result["valid"] is False, result["message"])
        os.remove(writer.log_file)
        result = later.verify_chain()
        check("log file deleted: detected", result["valid"] is False and "missing" in result["message"], result["message"])
        write_lines(writer.log_file, lines)
        check("restored log verifies again", later.verify_chain()["valid"])

        os.remove(checkpoint_path)
        result = later.verify_chain()
        check("checkpoint deleted: detected", result["valid"] is False and "checkpoint" in result["message"].lower(),
              result["message"])
        tampered = json.loads(checkpoint)
        tampered["seq"] -= 2
        open(checkpoint_path, "w", encoding="utf-8").write(json.dumps(tampered))
        result = later.verify_chain()
        check("checkpoint edited: detected", result["valid"] is False, result["message"])
        open(checkpoint_path, "w", encoding="utf-8").write("not json")
        check("checkpoint corrupted: detected", later.verify_chain()["valid"] is False)
        open(checkpoint_path, "w", encoding="utf-8").write(checkpoint)
        check("restored checkpoint verifies again", later.verify_chain()["valid"])

        # The writer must not launder a truncation: numbering continues from the checkpoint.
        write_lines(writer.log_file, lines[:-2])
        later.log_event(AuditEventType.QUERY_SUCCESS, message="after truncation")
        entries = [json.loads(line) for line in read_lines(writer.log_file)]
        eq("the entry written after a truncation keeps the next number", entries[-1]["seq"], 7)
        result = later.verify_chain()
        check("... so the gap stays visible", result["valid"] is False and result.get("broken_line") == 5, str(result))
        check("... to any later process too", AuditLogger(log_dir=tmp).verify_chain()["valid"] is False)

    with audit_key(key), tempfile.TemporaryDirectory() as tmp:
        writer = AuditLogger(log_dir=tmp)
        fill(writer, 4)
        later = AuditLogger(log_dir=tmp)
        os.remove(os.path.join(tmp, "audit.checkpoint.json"))
        later.log_event(AuditEventType.QUERY_SUCCESS, message="written with the checkpoint gone")
        last = json.loads(read_lines(writer.log_file)[-1])
        check("a missing checkpoint is recorded inside the next entry", "checkpoint" in last.get("chain_anomaly", ""),
              str(last))
        result = AuditLogger(log_dir=tmp).verify_chain()
        check("... and verification reports it", result["valid"] is False and "integrity problem" in result["message"],
              result["message"])

    with audit_key(key), tempfile.TemporaryDirectory() as tmp:
        running = AuditLogger(log_dir=tmp)
        fill(running, 3)
        os.remove(running.log_file)
        os.remove(os.path.join(tmp, "audit.checkpoint.json"))
        result = running.verify_chain()
        check("log AND checkpoint deleted: the running server notices", result["valid"] is False, result["message"])
        check("... a fresh process cannot (documented limit)", AuditLogger(log_dir=tmp).verify_chain()["valid"] is True)
        running.log_event(AuditEventType.QUERY_SUCCESS, message="next")
        last = json.loads(read_lines(running.log_file)[-1])
        check("... and the running server records it in its next entry",
              last["seq"] == 4 and "disappeared" in last.get("chain_anomaly", ""), str(last))
        check("... which any later process then sees", AuditLogger(log_dir=tmp).verify_chain()["valid"] is False)


def test_keyed_chain():
    print("\n== HMAC key ==")
    with tempfile.TemporaryDirectory() as tmp:
        with audit_key("unit-test-key-0123456789"):
            log = AuditLogger(log_dir=tmp)
            fill(log, 4)
            lines = read_lines(log.log_file)
            entries = [json.loads(line) for line in lines]
            eq("entries are marked as keyed", {e["alg"] for e in entries}, {"hmac-sha256"})
            body = {k: v for k, v in entries[0].items() if k != "entry_hash"}
            plain = hashlib.sha256(json.dumps(body, sort_keys=True, default=str).encode()).hexdigest()
            check("entry_hash is not a plain SHA-256", entries[0]["entry_hash"] != plain)
            result = log.verify_chain()
            check("keyed chain verifies", result["valid"] and result["keyed"] and result["unkeyed_entries"] == 0, str(result))
            check("the key is not in the log, the checkpoint or the summary",
                  "unit-test-key" not in "".join(lines) + open(os.path.join(tmp, "audit.checkpoint.json")).read()
                  + json.dumps(log.get_session_summary()))

            # An attacker without the key rewrites an entry and recomputes plain hashes for the
            # rest of the chain and for the checkpoint.
            forged = [dict(e) for e in entries]
            forged[1]["message"] = "FORGED"
            prev = forged[0]["entry_hash"]
            for e in forged[1:]:
                e.pop("entry_hash")
                e["prev_hash"] = prev
                e["entry_hash"] = hashlib.sha256(json.dumps(e, sort_keys=True, default=str).encode()).hexdigest()
                prev = e["entry_hash"]
            write_lines(log.log_file, [json.dumps(e) for e in forged])
            later = AuditLogger(log_dir=tmp)
            result = later.verify_chain()
            check("re-chained forgery is detected", result["valid"] is False and result.get("broken_line") == 2, str(result))

            # Full replacement: every entry and the checkpoint rewritten as an unsigned chain.
            for e in forged:
                e["alg"] = "sha256"
            prev = "GENESIS"
            for e in forged:
                e.pop("entry_hash")
                e["prev_hash"] = prev
                e["entry_hash"] = hashlib.sha256(json.dumps(e, sort_keys=True, default=str).encode()).hexdigest()
                prev = e["entry_hash"]
            write_lines(log.log_file, [json.dumps(e) for e in forged])
            cp = {"version": 1, "seq": 4, "last_hash": prev, "first_seq": 1, "alg": "sha256", "updated": "x"}
            cp["mac"] = hashlib.sha256(json.dumps(cp, sort_keys=True, default=str).encode()).hexdigest()
            open(os.path.join(tmp, "audit.checkpoint.json"), "w", encoding="utf-8").write(json.dumps(cp))
            result = later.verify_chain()
            check("a log with no signed entry is not accepted under a key", result["valid"] is False, result["message"])
            result = log.verify_chain()
            check("the server that wrote the original notices the rewrite", result["valid"] is False, result["message"])
            write_lines(log.log_file, lines[:2] + [json.dumps(forged[2])] + lines[3:])
            check("an unsigned entry among signed ones is detected", later.verify_chain()["valid"] is False)
            write_lines(log.log_file, lines)

        with audit_key("a-different-key-0123456789"):
            result = AuditLogger(log_dir=tmp).verify_chain()
            check("a different key does not verify", result["valid"] is False, result["message"])
        with audit_key(None):
            result = AuditLogger(log_dir=tmp).verify_chain()
            check("no key: a keyed log cannot be verified, and the message says so",
                  result["valid"] is False and KEY_ENV in result["message"], result["message"])

    with tempfile.TemporaryDirectory() as tmp:
        with audit_key(None):
            fill(AuditLogger(log_dir=tmp), 3, "plain")
        with audit_key("unit-test-key-0123456789"):
            log = AuditLogger(log_dir=tmp)
            result = log.verify_chain()
            check("key set on an existing unsigned log: flagged until the next entry", result["valid"] is False
                  and "no entry is signed" in result["message"], result["message"])
            fill(log, 2, "keyed")
            result = log.verify_chain()
            check("after the next entry it verifies and counts the unsigned ones",
                  result["valid"] and result["unkeyed_entries"] == 3 and result["checked"] == 5, str(result))
            eq("numbering continues", [json.loads(line)["seq"] for line in read_lines(log.log_file)], [1, 2, 3, 4, 5])


def test_two_writers():
    print("\n== two server processes on one log ==")
    with audit_key("unit-test-key-0123456789"), tempfile.TemporaryDirectory() as tmp:
        a, b = AuditLogger(log_dir=tmp), AuditLogger(log_dir=tmp)
        for i in range(5):
            a.log_event(AuditEventType.QUERY_SUCCESS, message=f"a{i}")
            b.log_event(AuditEventType.QUERY_SUCCESS, message=f"b{i}")
        result = a.verify_chain()
        check("interleaved writers keep one valid chain", result["valid"] and result["checked"] == 10, str(result))
        eq("sequence is gapless", [json.loads(line)["seq"] for line in read_lines(a.log_file)], list(range(1, 11)))
        check("the other writer agrees", b.verify_chain()["valid"])

        def burst(log, tag):
            for i in range(25):
                log.log_query(query=f"EVALUATE T{tag}{i}", result_count=i, tool=tag)

        loggers = [AuditLogger(log_dir=tmp) for _ in range(4)]
        threads = [threading.Thread(target=burst, args=(log, f"w{n}")) for n, log in enumerate(loggers)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        result = a.verify_chain()
        check("four concurrent writers: still one valid chain", result["valid"] and result["checked"] == 110, str(result))
        eq("no write failed", sum(log.write_failures for log in loggers), 0)
        seqs = [json.loads(line)["seq"] for line in read_lines(a.log_file)]
        eq("every sequence number exactly once", seqs, list(range(1, 111)))

        # Real separate processes, the situation the lock exists for (two MCP clients, each
        # with its own server, sharing one install folder).
        src = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src")
        script = (
            "import sys, logging; sys.path.insert(0, sys.argv[1]); logging.disable(logging.CRITICAL)\n"
            "from security.audit_logger import AuditLogger\n"
            "log = AuditLogger(log_dir=sys.argv[2])\n"
            "for i in range(40): log.log_query(query='EVALUATE P' + sys.argv[3], result_count=i, tool=sys.argv[3])\n"
            "sys.exit(1 if log.write_failures else 0)\n")
        children = [subprocess.Popen([sys.executable, "-c", script, src, tmp, f"p{n}"]) for n in range(3)]
        codes = [child.wait(timeout=120) for child in children]
        eq("three separate processes wrote without a failure", codes, [0, 0, 0])
        result = a.verify_chain()
        check("... into one valid chain", result["valid"] and result["checked"] == 230 and result["last_seq"] == 230,
              str(result))
        tools = [json.loads(line).get("tool") for line in read_lines(a.log_file)[110:]]
        eq("... with every entry of every process present", sorted(set(tools)), ["p0", "p1", "p2"])

        with _FileLock(a._lock_path, 1.0):
            impatient = AuditLogger(log_dir=tmp, lock_timeout=0.2)
            impatient.log_event(AuditEventType.QUERY_SUCCESS, message="cannot get the lock")
            summary = impatient.get_session_summary()
            check("a write that cannot get the lock is a recorded failure, not a silent loss",
                  summary["write_failures"] == 1 and not summary["healthy"] and "lock" in summary["last_write_error"],
                  str(summary))
        check("the log is unharmed", a.verify_chain()["checked"] == 230)


def test_rotation(key):
    label = "with a key" if key else "without a key"
    print(f"\n== rotation ({label}) ==")
    with audit_key(key), tempfile.TemporaryDirectory() as tmp:
        log = AuditLogger(log_dir=tmp, backup_count=3)
        log.max_file_size = 1500  # a few entries per file
        fill(log, 60, "rot" + "x" * 80)
        files = sorted(f for f in os.listdir(tmp) if f.endswith(".log"))
        eq("backups are kept up to the limit", files, ["audit.1.log", "audit.2.log", "audit.3.log", "audit.log"])
        eq("no write or rotation failed (this used to break on Windows)",
           (log.write_failures, log.rotation_failures), (0, 0))
        result = log.verify_chain()
        retained = sum(len(read_lines(os.path.join(tmp, f))) for f in files)
        check("the chain verifies across every rotated file",
              result["valid"] and result["files"] == 4 and result["checked"] == retained and result["last_seq"] == 60,
              str(result))
        check("entries dropped by rotation are accounted for", result["first_seq"] > 1, str(result))
        check("a later process verifies it too", AuditLogger(log_dir=tmp, backup_count=3).verify_chain()["valid"])
        first_of_current = json.loads(read_lines(log.log_file)[0])
        last_of_previous = json.loads(read_lines(os.path.join(tmp, "audit.1.log"))[-1])
        check("the first entry of a new file links to the last entry of the old one",
              first_of_current["prev_hash"] == last_of_previous["entry_hash"]
              and first_of_current["seq"] == last_of_previous["seq"] + 1)

        recent = log.get_recent_events(count=12)
        eq("recent events reach back into rotated files", [e["seq"] for e in recent], list(range(49, 61)))

        rotated = os.path.join(tmp, "audit.2.log")
        saved = read_lines(rotated)
        edited = json.loads(saved[0])
        edited["message"] = "EDITED"
        write_lines(rotated, [json.dumps(edited)] + saved[1:])
        result = AuditLogger(log_dir=tmp, backup_count=3).verify_chain()
        check("an edit inside a rotated file is detected",
              result["valid"] is False and result.get("broken_file") == "audit.2.log" and result.get("broken_line") == 1,
              str(result))
        write_lines(rotated, saved)
        os.remove(os.path.join(tmp, "audit.3.log"))
        result = AuditLogger(log_dir=tmp, backup_count=3).verify_chain()
        check("removing the oldest retained file is detected", result["valid"] is False and "start" in result["message"],
              result["message"])
        write_lines(os.path.join(tmp, "audit.3.log"), [])
        os.remove(os.path.join(tmp, "audit.3.log"))
        os.remove(rotated)
        check("removing a file from the middle is detected", AuditLogger(log_dir=tmp).verify_chain()["valid"] is False)

    with audit_key(key), tempfile.TemporaryDirectory() as tmp:
        log = AuditLogger(log_dir=tmp, backup_count=0)
        log.max_file_size = 600
        fill(log, 20, "z" * 60)
        eq("backup_count 0 keeps only the current file", sorted(f for f in os.listdir(tmp) if f.endswith(".log")), ["audit.log"])
        result = log.verify_chain()
        check("... and still verifies", result["valid"] and result["last_seq"] == 20 and log.rotation_failures == 0, str(result))

    with audit_key(key), tempfile.TemporaryDirectory() as tmp:
        log = AuditLogger(log_dir=tmp, backup_count=4)
        log.max_file_size = 800
        fill(log, 40, "q" * 60)
        smaller = AuditLogger(log_dir=tmp, backup_count=1)
        smaller.max_file_size = 800
        fill(smaller, 12, "r" * 60)
        eq("lowering backup_count drops the extra files at the next rotation",
           sorted(f for f in os.listdir(tmp) if f.endswith(".log")), ["audit.1.log", "audit.log"])
        check("... without breaking the chain", smaller.verify_chain()["valid"], str(smaller.verify_chain()))


def test_stored_content():
    print("\n== what is stored of a query ==")
    query = ('EVALUATE FILTER(Customers, Customers[Email] = "jane.doe@example.com" && Customers[Phone] = "555-123-4567" '
             '&& Customers[Acct] = 1234567890 && Customers[Qty] > 5) -- asked by bob@example.com\n'
             '/* Password=hunter2; */')
    with audit_key(None), tempfile.TemporaryDirectory() as tmp:
        log = AuditLogger(log_dir=tmp)
        eq("default mode", log.store_query_text, "redacted")
        log.log_query(query=query, result_count=1)
        stored = json.loads(read_lines(log.log_file)[-1])["query"]
        check("string literals, long numbers and comments are gone",
              all(s not in stored["text"] for s in ("jane.doe", "555-123", "1234567890", "bob@", "hunter2")), stored["text"])
        check("the shape of the query is kept",
              "FILTER(Customers, Customers[Email] = \"?\"" in stored["text"] and "Customers[Qty] > 5" in stored["text"],
              stored["text"])
        eq("the mode is recorded with the entry", stored["stored_as"], "redacted")
        eq("length is the original length", stored["length"], len(query))
        other = query.replace("jane.doe@example.com", "someone.else@example.org").replace("1234567890", "9876543210")
        eq("the fingerprint ignores literal values", log._generate_query_fingerprint(other), stored["fingerprint"])
        check("... but not the shape", log._generate_query_fingerprint("EVALUATE Sales") != stored["fingerprint"])

        full = AuditLogger(log_dir=os.path.join(tmp, "full"), store_query_text="full")
        full.log_query(query=query)
        text = json.loads(read_lines(full.log_file)[-1])["query"]["text"]
        check("full: literals kept, secrets still removed", "jane.doe@example.com" in text and "hunter2" not in text, text)
        finger = AuditLogger(log_dir=os.path.join(tmp, "finger"), store_query_text="fingerprint")
        finger.log_query(query=query)
        stored = json.loads(read_lines(finger.log_file)[-1])["query"]
        check("fingerprint: no text at all", stored["text"] is None and len(stored["fingerprint"]) == 12, str(stored))
        eq("legacy include_query_text=False", AuditLogger(log_dir=os.path.join(tmp, "l1"),
                                                          include_query_text=False).store_query_text, "fingerprint")
        eq("legacy redact_sensitive=False", AuditLogger(log_dir=os.path.join(tmp, "l2"),
                                                        redact_sensitive=False).store_query_text, "full")
        try:
            AuditLogger(log_dir=os.path.join(tmp, "bad"), store_query_text="everything")
            check("an unknown mode is refused", False, "no exception")
        except ValueError:
            check("an unknown mode is refused", True)

        log.log_query(query="EVALUATE X", success=False, error_message=(
            'connect failed; Password=topsecret9; {"client_secret": "abcDEF123456"} Pwd=hunter2; '
            'Authorization: Bearer eyJhbGciOi.payload.signature'))
        error = json.loads(read_lines(log.log_file)[-1])["result"]["error"]
        check("errors are scrubbed in every shape",
              all(s not in error for s in ("topsecret9", "abcDEF123456", "hunter2", "eyJhbGciOi")), error)
        log.log_connection(source="cloud", success=False, error_message="Provider=MSOLAP;Password=Sup3rSecret!;User ID=x")
        entry = json.loads(read_lines(log.log_file)[-1])
        check("connection errors are scrubbed in the message and the details",
              "Sup3rSecret" not in json.dumps(entry) and "Password=***" in entry["message"], json.dumps(entry)[:200])
        log.log_event(AuditEventType.ACCESS_DENIED, message="token=x; secret=abc123",
                      details={"password": "p4ss", "note": "Pwd=zzz9;"}, extra={"client_secret": "s3cr3t"})
        entry = json.loads(read_lines(log.log_file)[-1])
        check("generic events are scrubbed too",
              all(s not in json.dumps(entry) for s in ("abc123", "p4ss", "zzz9", "s3cr3t")), json.dumps(entry)[:300])
        log.log_query(query="EVALUATE ROW(" + ", ".join(f'"c{i}", T[c{i}]' for i in range(3000)) + ")")
        text = json.loads(read_lines(log.log_file)[-1])["query"]["text"]
        check("a very long query is truncated in the log", len(text) < 10200 and "TRUNCATED" in text, str(len(text)))
        check("the log still verifies", log.verify_chain()["valid"])
    eq("strip_literals", strip_literals('EVALUATE ROW("x", "a ""b"" c", 12345678, 3.5) // note'),
       'EVALUATE ROW("?", "?", 0, 3.5)  ')


def test_failures_are_visible():
    print("\n== a write failure shows up instead of vanishing ==")
    with audit_key(None), tempfile.TemporaryDirectory() as tmp:
        os.mkdir(os.path.join(tmp, "audit.log"))  # the log path is a folder: every append fails
        log = AuditLogger(log_dir=tmp)
        event = log.log_query(query="EVALUATE Sales", result_count=1)
        check("the call still returns the event", event["event_type"] == "query_success")
        summary = log.get_session_summary()
        check("the failure is counted and described",
              summary["write_failures"] == 1 and summary["healthy"] is False and summary["last_write_error"], str(summary))
        log.log_query(query="EVALUATE Sales", result_count=1)
        eq("... every time", log.get_session_summary()["write_failures"], 2)

        strict = AuditLogger(log_dir=tmp, required=True)
        try:
            strict.log_query(query="EVALUATE Sales")
            check("required=True raises", False, "no exception")
        except AuditWriteError as e:
            check("required=True raises", "could not be written" in str(e), str(e))

        sec = SecurityLayer(config_path=None, enable_audit=True, audit_logger=log)
        rows, _ = sec.process_results([{"Sales[Amount]": 1}], query="EVALUATE Sales")
        eq("the layer still returns the rows when audit is not required", rows, [{"Sales[Amount]": 1}])
        status = sec.get_status()["audit"]
        check("and the status shows the audit log is failing", status["healthy"] is False and status["write_failures"] >= 3,
              str(status))
        strict_layer = SecurityLayer(config_path=None, enable_audit=True, audit_logger=strict)
        try:
            strict_layer.process_results([{"Sales[Amount]": 1}], query="EVALUATE Sales")
            check("with a required audit log, no audit entry means no rows", False, "rows were returned")
        except AuditWriteError:
            check("with a required audit log, no audit entry means no rows", True)


def test_legacy_logs():
    print("\n== logs written by earlier versions ==")

    def old_entry(i, prev, key=None):
        import hmac as _hmac
        event = {"timestamp": "2026-01-01T00:00:00+00:00", "session_id": "old", "event_type": "query_success",
                 "severity": "info", "message": f"old {i}", "details": {}, "prev_hash": prev}
        canonical = json.dumps(event, sort_keys=True, default=str).encode()
        event["entry_hash"] = (_hmac.new(key.encode(), canonical, hashlib.sha256).hexdigest() if key
                               else hashlib.sha256(canonical).hexdigest())
        return event

    for key in (None, "unit-test-key-0123456789"):
        label = "keyed" if key else "plain"
        with audit_key(key), tempfile.TemporaryDirectory() as tmp:
            prev, lines = "GENESIS", []
            for i in range(3):
                entry = old_entry(i, prev, key)
                prev = entry["entry_hash"]
                lines.append(json.dumps(entry))
            write_lines(os.path.join(tmp, "audit.log"), lines)
            log = AuditLogger(log_dir=tmp)
            result = log.verify_chain()
            check(f"{label}: an old log without sequence numbers still verifies", result["valid"] and result["checked"] == 3,
                  str(result))
            fill(log, 2, "new")
            entries = [json.loads(line) for line in read_lines(log.log_file)]
            eq(f"{label}: new entries continue the old chain", (entries[3]["prev_hash"], entries[3]["seq"], entries[4]["seq"]),
               (prev, 1, 2))
            result = log.verify_chain()
            check(f"{label}: the mixed log verifies", result["valid"] and result["checked"] == 5, str(result))
            write_lines(log.log_file, lines[:1] + lines[2:] + [json.dumps(e) for e in entries[3:]])
            check(f"{label}: tampering with the old part is detected", log.verify_chain()["valid"] is False)

    with audit_key(None), tempfile.TemporaryDirectory() as tmp:
        write_lines(os.path.join(tmp, "audit.log"), [json.dumps({"event_type": "x", "message": f"pre-chain {i}"})
                                                    for i in range(2)])
        log = AuditLogger(log_dir=tmp)
        check("a pre-chain log with no hashes at all is accepted as such", log.verify_chain()["valid"])
        first = old_entry(0, "GENESIS")
        second = old_entry(1, first["entry_hash"])
        stripped = {k: v for k, v in first.items() if k != "entry_hash"}
        write_lines(log.log_file, [json.dumps(stripped), json.dumps(second)])
        result = log.verify_chain()
        check("one stripped entry_hash in a chained log is tampering", result["valid"] is False
              and result.get("broken_line") == 1, str(result))


def test_recent_events_and_layer_audit():
    print("\n== reading back, and what the layer audits ==")
    with audit_key(None), tempfile.TemporaryDirectory() as tmp:
        log = AuditLogger(log_dir=tmp)
        eq("no file yet", log.get_recent_events(5), [])
        fill(log, 7)
        eq("the last N, oldest first", [e["message"] for e in log.get_recent_events(3)], ["e4", "e5", "e6"])
        eq("more than there are", len(log.get_recent_events(100)), 7)
        eq("zero means none (it used to mean all)", log.get_recent_events(0), [])
        eq("negative means none", log.get_recent_events(-1), [])
        eq("a non-number falls back to the default", len(log.get_recent_events("many")), 7)

    with audit_key(None), tempfile.TemporaryDirectory() as tmp:
        log = AuditLogger(log_dir=tmp, store_query_text="full")
        sec = SecurityLayer(config_path=None, enable_audit=True, audit_logger=log)
        sec.policy_engine.load_from_dict({"tables": [{"name": "*", "columns": [
            {"name": "ssn", "match": "word", "action": "block"}, {"name": "card number", "match": "word", "action": "mask"}]}]})
        sec.process_results([{"Customers[Card Number]": "4111111111111111", "Customers[Email]": "a@b.com"}],
                            query="EVALUATE TOPN(1, Customers)", source="desktop", model_name="Demo", port=5000,
                            duration_ms=4.0, tool="desktop_execute_dax")
        events = log.get_recent_events(10)
        query_event = next(e for e in events if e["event_type"] == "query_success")
        eq("the query entry names the tool", query_event["tool"], "desktop_execute_dax")
        eq("... the tables it touched", query_event["access"]["tables"], ["Customers"])
        eq("... what the policy did", query_event["access"]["policy"]["transformed_columns"], ["Customers[Card Number]"])
        eq("... and the PII it masked", (query_event["pii"]["detected"], query_event["pii"]["types"]), (True, ["email"]))
        check("a PII event is logged with the tool", any(e["event_type"] == "pii_detected" and e["tool"] == "desktop_execute_dax"
                                                         for e in events))
        check("neither value reached the log", "4111111111111111" not in json.dumps(events) and "a@b.com" not in json.dumps(events))

        sec.process_results([{"[x]": "123-45-6789"}], query='EVALUATE ROW("x", MAX(Customers[SSN]))', tool="run_dax_tests")
        events = log.get_recent_events(10)
        violation = [e for e in events if e["event_type"] == "policy_violation"]
        check("a query refused inside process_results is logged as a violation, with the tool",
              len(violation) == 1 and violation[0]["tool"] == "run_dax_tests", str(violation))
        denied = [e for e in events if e["event_type"] == "query_success" and (e["access"]["policy"] or {}).get("denied")]
        check("... and its query entry says it was denied", len(denied) == 1, str(denied))
        sec.log_policy_violation("statement_guard", "not a query", query="{}", tool="execute_dax")
        sec.log_connection("desktop", model_name="Demo", port=5000, tool="desktop_connect")
        tools = [e["tool"] for e in log.get_recent_events(2)]
        eq("the helper methods pass the tool through", tools, ["execute_dax", "desktop_connect"])
        check("the layer's verify sees a valid chain", sec.verify_audit_integrity()["valid"])

        quiet = AuditLogger(log_dir=os.path.join(tmp, "quiet"))
        sec = SecurityLayer(config_path=None, enable_audit=True, audit_logger=quiet)
        sec.audit_all_queries = False
        sec.process_results([{"Sales[Amount]": 1}], query="EVALUATE Sales")
        eq("audit_all_queries off: an uneventful query is not logged", quiet.get_recent_events(5), [])
        sec.process_results([], query="EVALUATE Sales", success=False, error_message="boom")
        eq("... a failure still is", [e["event_type"] for e in quiet.get_recent_events(5)], ["query_failure"])

        off = SecurityLayer(config_path=None, enable_audit=False)
        eq("audit disabled: verify says so", off.verify_audit_integrity()["message"], "Audit logging is disabled.")


if __name__ == "__main__":
    test_entry_format()
    test_truncation_and_deletion("unit-test-key-0123456789")
    test_truncation_and_deletion(None)
    test_keyed_chain()
    test_two_writers()
    test_rotation("unit-test-key-0123456789")
    test_rotation(None)
    test_stored_content()
    test_failures_are_visible()
    test_legacy_logs()
    test_recent_events_and_layer_audit()
    print()
    if _failures:
        print(f"FAILED: {len(_failures)} check(s): {_failures}")
        sys.exit(1)
    print("ALL AUDIT CHAIN TESTS PASSED")
