"""
Audit log: one JSON object per line, chained so that changes to the file can be detected.

FORMAT
------
Every entry carries
  seq         1, 2, 3, ... across rotations and restarts
  alg         "hmac-sha256" when POWERBI_MCP_AUDIT_KEY was set at write time, else "sha256"
  prev_hash   entry_hash of the entry before it ("GENESIS" for the first)
  entry_hash  hash of the entry itself (all fields but this one, keys sorted)
Next to the log sits <name>.checkpoint.json: the sequence number and hash of the last entry
and the lowest sequence number that should still be on disk, signed the same way. The
checkpoint is what makes a missing tail or a missing head visible: the chain inside a
truncated file is still perfectly consistent.

Rotation renames audit.log to audit.1.log (older files shift up, the oldest is dropped) and
the chain simply continues in the new file; verify_chain walks every file, oldest first.

Several server processes may write the same log. Each write takes an exclusive lock on
<name>.lock, re-reads the checkpoint and the last line, appends, and updates the checkpoint.

WHAT verify_chain DETECTS
-------------------------
With POWERBI_MCP_AUDIT_KEY set, someone who can write to the log folder but does not know
the key cannot do any of the following unnoticed:
  * edit, insert, reorder or delete entries anywhere in the retained files;
  * cut entries off the end, or off the start beyond what rotation dropped;
  * delete the log while leaving the checkpoint, or the checkpoint while leaving the log;
  * put unsigned entries after signed ones, or leave a log with no signed entry at all.
The writer never renumbers: if it finds the log shorter than the checkpoint says, the next
entry keeps the next sequence number, and the gap stays in the chain as evidence. If it
finds the checkpoint gone, it says so inside the (signed) entry it writes.

WHAT IT DOES NOT DETECT
-----------------------
  * Without a key, anything deliberate. A plain SHA-256 chain and checkpoint can be
    recomputed by anyone who can write the files; they catch accidental damage and careless
    edits only.
  * Even with a key, while no server is running: deleting BOTH the log files and the
    checkpoint, restoring an older copy of both, or replacing both with unsigned ones. Nothing
    on the machine remembers the newer entries (a running server does, and reports it). After
    the server writes again, a replaced log looks like one that predates the key, so
    verify_chain reports how many entries are unsigned (``unkeyed_entries``): if the key has
    been set since the log began, any number above zero means the log was replaced. To close
    the gap completely, ship the log or the checkpoint to a system the attacker cannot write.
  * Whoever holds the key can forge the log. The key lives in the server's environment, so it
    protects against other accounts and other processes, not against the server's own account.

CONTENT
-------
Query text is stored according to ``store_query_text``:
  "redacted" (default)  secrets removed and every string literal, long number and comment
                        replaced, so a filter value such as an e-mail address is not kept
  "full"                secrets removed, text otherwise as sent
  "fingerprint"         no text, only the fingerprint and length
Error messages have secrets removed and are capped in length; they can still quote data the
engine put in them.
"""
import hashlib
import hmac
import json
import logging
import os
import threading
import time
from collections import deque
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import dax_tokens as dt

from .redaction import redact_secrets, redact_structure

if os.name == "nt":
    import msvcrt
else:
    import fcntl

logger = logging.getLogger(__name__)

AUDIT_KEY_ENV = "POWERBI_MCP_AUDIT_KEY"
GENESIS = "GENESIS"
QUERY_TEXT_MODES = ("full", "redacted", "fingerprint")
_MAX_QUERY_CHARS = 10_000
_MAX_ERROR_CHARS = 2_000
_KEYED = "hmac-sha256"
_PLAIN = "sha256"


class AuditWriteError(RuntimeError):
    """An audit entry could not be written and the logger is configured as required."""


class AuditEventType(Enum):
    """Types of auditable events"""
    QUERY_EXECUTE = "query_execute"
    QUERY_SUCCESS = "query_success"
    QUERY_FAILURE = "query_failure"
    CONNECTION = "connection"
    DISCONNECTION = "disconnection"
    POLICY_VIOLATION = "policy_violation"
    PII_DETECTED = "pii_detected"
    ACCESS_DENIED = "access_denied"


class AuditSeverity(Enum):
    """Severity levels for audit events"""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class _FileLock:
    """Exclusive lock on a lock file, honoured by every process that writes the same log.
    msvcrt byte-range lock on Windows, flock elsewhere; both are released if the process dies."""

    def __init__(self, path: Path, timeout: float):
        self._path = path
        self._timeout = timeout
        self._fd: Optional[int] = None

    def __enter__(self) -> "_FileLock":
        fd = os.open(str(self._path), os.O_RDWR | os.O_CREAT)
        deadline = time.monotonic() + self._timeout
        while True:
            try:
                if os.name == "nt":
                    os.lseek(fd, 0, os.SEEK_SET)
                    msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
                else:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                self._fd = fd
                return self
            except OSError as e:
                if time.monotonic() >= deadline:
                    os.close(fd)
                    raise TimeoutError(f"could not lock {self._path} within {self._timeout:g}s "
                                       f"(held by another process)") from e
                time.sleep(0.01)

    def __exit__(self, *exc_info) -> None:
        fd, self._fd = self._fd, None
        if fd is None:
            return
        try:
            if os.name == "nt":
                os.lseek(fd, 0, os.SEEK_SET)
                msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)


def _replace(src: Path, dst: Path) -> None:
    """os.replace with a short retry: on Windows a scanner or indexer that has the target open
    for an instant makes the rename fail with PermissionError."""
    for attempt in range(5):
        try:
            os.replace(src, dst)
            return
        except PermissionError:
            if attempt == 4:
                raise
            time.sleep(0.02)


def _last_line(path: Path) -> Optional[str]:
    """The last non-empty line of a file, reading from the end."""
    with open(path, "rb") as f:
        f.seek(0, os.SEEK_END)
        pos = f.tell()
        block = 65536
        data = b""
        while pos > 0:
            step = min(block, pos)
            pos -= step
            f.seek(pos)
            data = f.read(step) + data
            stripped = data.rstrip(b"\r\n \t")
            newline = stripped.rfind(b"\n")
            if newline != -1:
                return stripped[newline + 1:].decode("utf-8", errors="replace")
            block *= 2
        stripped = data.strip()
        return stripped.decode("utf-8", errors="replace") if stripped else None


def strip_literals(query: str) -> str:
    """A DAX query with every string literal, comment and long number replaced.

    Table, column and measure names and the shape of the query survive; values typed into it
    (an e-mail address in a filter, an account number) do not.
    """
    parts = []
    for tok in dt.tokenize(query or ""):
        if tok.kind == dt.STRING:
            parts.append('"?"')
        elif tok.kind == dt.COMMENT:
            parts.append(" ")
        elif tok.kind == dt.NUMBER and sum(ch.isdigit() for ch in tok.text) >= 7:
            parts.append("0")
        else:
            parts.append(tok.text)
    return "".join(parts)


class AuditLogger:
    """
    Tamper-evident audit log for Power BI MCP queries. See the module docstring for the file
    format and for exactly what verification can and cannot detect.

    Usage:
        audit = AuditLogger(log_dir="./logs")
        audit.log_query(query="EVALUATE Sales", result_count=100, duration_ms=250, tool="execute_dax")
        audit.verify_chain()
    """

    def __init__(
        self,
        log_dir: Optional[str] = None,
        log_file: str = "audit.log",
        max_file_size_mb: float = 10,
        backup_count: int = 5,
        include_query_text: bool = True,
        redact_sensitive: bool = True,
        store_query_text: Optional[str] = None,
        required: bool = False,
        lock_timeout: float = 10.0,
    ):
        """
        Args:
            log_dir: Directory for log files (default: <install folder>/logs)
            log_file: Name of the log file
            max_file_size_mb: Size at which the file is rotated
            backup_count: Rotated files to keep (0 = drop the file when it is full)
            include_query_text: Older switch; False means store_query_text="fingerprint"
            redact_sensitive: Older switch; False means store_query_text="full"
            store_query_text: "full", "redacted" or "fingerprint" (overrides the two above)
            required: Raise AuditWriteError when an entry cannot be written, instead of
                recording the failure and carrying on
            lock_timeout: Seconds to wait for another process to release the log
        """
        self.log_dir = Path(log_dir) if log_dir else Path(__file__).parent.parent.parent / "logs"
        self.log_file = self.log_dir / log_file
        self.max_file_size = int(max_file_size_mb * 1024 * 1024)
        self.backup_count = max(0, int(backup_count))
        if store_query_text is None:
            store_query_text = "fingerprint" if not include_query_text else (
                "redacted" if redact_sensitive else "full")
        if store_query_text not in QUERY_TEXT_MODES:
            raise ValueError(f"store_query_text must be one of {', '.join(QUERY_TEXT_MODES)}")
        self.store_query_text = store_query_text
        self.required = bool(required)

        stem = self.log_file.stem
        self._checkpoint_path = self.log_dir / f"{stem}.checkpoint.json"
        self._lock_path = self.log_dir / f"{stem}.lock"
        self._lock_timeout = lock_timeout
        self._lock = threading.Lock()
        self._session_id = self._generate_session_id()
        self._query_count = 0

        key = os.getenv(AUDIT_KEY_ENV)
        self._hmac_key = key.encode() if key else None

        # What this process last wrote: lets a running server notice that the log AND the
        # checkpoint were removed underneath it, which nothing on disk can show.
        self._last_seq = 0
        self._last_hash: Optional[str] = None
        self._first_seq = 1
        self._known_signature: Optional[Tuple[Any, Any]] = None

        self.write_failures = 0
        self.last_write_error: Optional[str] = None
        self.rotation_failures = 0
        self.last_rotation_error: Optional[str] = None

        try:
            self.log_dir.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            self._record_write_failure(e)
            if self.required:
                raise AuditWriteError(f"audit log folder {self.log_dir} cannot be created: {e}") from e

        logger.info(f"Audit logger initialized: {self.log_file}")

    # ------------------------------------------------------------- legacy switches

    @property
    def include_query_text(self) -> bool:
        return self.store_query_text != "fingerprint"

    @include_query_text.setter
    def include_query_text(self, value: bool) -> None:
        if not value:
            self.store_query_text = "fingerprint"
        elif self.store_query_text == "fingerprint":
            self.store_query_text = "redacted"

    @property
    def redact_sensitive(self) -> bool:
        return self.store_query_text != "full"

    @redact_sensitive.setter
    def redact_sensitive(self, value: bool) -> None:
        if self.store_query_text != "fingerprint":
            self.store_query_text = "redacted" if value else "full"

    # ------------------------------------------------------------------- hashing

    @property
    def _alg(self) -> str:
        return _KEYED if self._hmac_key else _PLAIN

    @staticmethod
    def _canonical(event: Dict[str, Any]) -> bytes:
        return json.dumps(event, sort_keys=True, default=str).encode()

    def _hash_event(self, event: Dict[str, Any]) -> str:
        """Hash of an event dict (caller must exclude 'entry_hash'): HMAC-SHA256 when an audit
        key is configured, else plain SHA-256."""
        canonical = self._canonical(event)
        if self._hmac_key:
            return hmac.new(self._hmac_key, canonical, hashlib.sha256).hexdigest()
        return hashlib.sha256(canonical).hexdigest()

    def _entry_hash_status(self, entry: Dict[str, Any]) -> Tuple[str, bool]:
        """("ok" | "mismatch" | "nokey", keyed) for a stored entry.

        Entries written before the ``alg`` field existed are tried under the configured key
        first and as plain SHA-256 second, because either may have produced them.
        """
        stored = str(entry.get("entry_hash"))
        canonical = self._canonical({k: v for k, v in entry.items() if k != "entry_hash"})
        plain_ok = hmac.compare_digest(hashlib.sha256(canonical).hexdigest(), stored)
        alg = entry.get("alg")
        if alg == _KEYED or (alg is None and self._hmac_key):
            if not self._hmac_key:
                return "nokey", True
            if hmac.compare_digest(hmac.new(self._hmac_key, canonical, hashlib.sha256).hexdigest(), stored):
                return "ok", True
            if alg is None and plain_ok:
                return "ok", False
            return "mismatch", True
        return ("ok" if plain_ok else "mismatch"), False

    # ---------------------------------------------------------------- checkpoint

    def _checkpoint_mac(self, body: Dict[str, Any], alg: str) -> str:
        canonical = self._canonical(body)
        if alg == _KEYED:
            return hmac.new(self._hmac_key or b"", canonical, hashlib.sha256).hexdigest()
        return hashlib.sha256(canonical).hexdigest()

    def _read_checkpoint(self) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        """(checkpoint, problem). (None, None) when there is no checkpoint file."""
        try:
            raw = self._checkpoint_path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return None, None
        except OSError as e:
            return None, f"the checkpoint cannot be read: {e}"
        try:
            cp = json.loads(raw)
        except ValueError:
            return None, "the checkpoint is not valid JSON"
        if not (isinstance(cp, dict) and type(cp.get("seq")) is int and isinstance(cp.get("last_hash"), str)
                and type(cp.get("first_seq")) is int and isinstance(cp.get("mac"), str)
                and cp.get("alg") in (_KEYED, _PLAIN)):
            return None, "the checkpoint is malformed"
        if cp["alg"] == _KEYED and not self._hmac_key:
            return None, f"the checkpoint was written with an audit key, but {AUDIT_KEY_ENV} is not set"
        expected = self._checkpoint_mac({k: v for k, v in cp.items() if k != "mac"}, cp["alg"])
        if not hmac.compare_digest(expected, cp["mac"]):
            return None, ("the checkpoint does not match its signature (it was modified, or written "
                          "with a different audit key)")
        return cp, None

    def _write_checkpoint(self, seq: int, last_hash: str, first_seq: int) -> None:
        body = {"version": 1, "seq": seq, "last_hash": last_hash, "first_seq": first_seq,
                "alg": self._alg, "updated": datetime.now(timezone.utc).isoformat()}
        body["mac"] = self._checkpoint_mac(body, self._alg)
        tmp = self._checkpoint_path.with_name(self._checkpoint_path.name + ".tmp")
        tmp.write_text(json.dumps(body), encoding="utf-8")
        _replace(tmp, self._checkpoint_path)

    # --------------------------------------------------------------------- files

    def _rotated(self, index: int) -> Path:
        return self.log_dir / f"{self.log_file.stem}.{index}{self.log_file.suffix}"

    def _chain_files(self) -> List[Path]:
        """Every file of the chain that exists, oldest first."""
        files = [path for _, path in self._numbered_files()]
        if self.log_file.exists():
            files.append(self.log_file)
        return files

    def _read_tail(self) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        """(last entry on disk, problem). The newest non-empty file is used, so the tail is
        found in audit.1.log right after a rotation."""
        for path in reversed(self._chain_files()):
            line = _last_line(path)
            if line is None:
                continue
            try:
                entry = json.loads(line)
            except ValueError:
                return None, f"the last line of {path.name} is not valid JSON"
            if not isinstance(entry, dict):
                return None, f"the last line of {path.name} is not an audit entry"
            return entry, None
        return None, None

    @staticmethod
    def _first_seq_in(path: Path) -> Optional[int]:
        """Sequence number of the first entry in a file; None for a file without one (empty,
        written before numbering, or with a damaged first line, which verify_chain reports)."""
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                if line.strip():
                    try:
                        entry = json.loads(line)
                    except ValueError:
                        return None
                    seq = entry.get("seq") if isinstance(entry, dict) else None
                    return seq if type(seq) is int else None
        return None

    def _numbered_files(self) -> List[Tuple[int, Path]]:
        """Rotated files as (index, path), highest index (oldest) first."""
        prefix, suffix = self.log_file.stem + ".", self.log_file.suffix
        numbered = []
        if self.log_dir.is_dir():
            for path in self.log_dir.iterdir():
                name = path.name
                if name.startswith(prefix) and name.endswith(suffix):
                    middle = name[len(prefix):len(name) - len(suffix)]  # "" for the current file
                    if middle.isdigit():
                        numbered.append((int(middle), path))
        return sorted(numbered, reverse=True)

    def _rotate_if_needed(self, seq: int, last_hash: str, first_seq: int) -> int:
        """Rotate the current file when it is over the size limit. Returns the first sequence
        number that should remain on disk afterwards.

        The checkpoint is told about the entries that are about to be dropped BEFORE the oldest
        file goes, so a crash in between leaves extra history, never a false alarm. A rotation
        that fails (a file held open by a viewer) is recorded and the log keeps growing.
        """
        try:
            if self.log_file.stat().st_size <= self.max_file_size:
                return first_seq
        except FileNotFoundError:
            return first_seq
        try:
            if self.backup_count == 0:
                first_seq = seq + 1
                self._write_checkpoint(seq, last_hash, first_seq)
                os.remove(self.log_file)
                return first_seq
            # Files at or beyond the limit are dropped by this rotation (several, if the limit
            # was lowered since they were written).
            numbered = self._numbered_files()
            doomed = [path for index, path in numbered if index >= self.backup_count]
            if doomed:
                kept = [path for index, path in numbered if index < self.backup_count]
                survivor_first = self._first_seq_in(kept[0] if kept else self.log_file)
                if survivor_first is not None and survivor_first > first_seq:
                    first_seq = survivor_first
                    self._write_checkpoint(seq, last_hash, first_seq)
                for path in doomed:
                    os.remove(path)
            for i in range(self.backup_count - 1, 0, -1):
                if self._rotated(i).exists():
                    _replace(self._rotated(i), self._rotated(i + 1))
            _replace(self.log_file, self._rotated(1))
            logger.info(f"Rotated audit log: {self.log_file}")
        except OSError as e:
            self.rotation_failures += 1
            self.last_rotation_error = f"{type(e).__name__}: {e}"
            logger.error("Audit log rotation failed, the log keeps growing: %s", self.last_rotation_error)
        return first_seq

    # ------------------------------------------------------------------- writing

    def _record_write_failure(self, error: BaseException) -> None:
        self.write_failures += 1
        self.last_write_error = f"{datetime.now(timezone.utc).isoformat()} {type(error).__name__}: {error}"
        logger.error("Audit log write FAILED (%d so far): %s", self.write_failures, self.last_write_error)

    def _write_log(self, event: Dict[str, Any]) -> bool:
        """Append one event to the chain. Returns False when it could not be written; the
        failure is counted and reported by get_session_summary (or raised, when ``required``)."""
        with self._lock:
            try:
                with _FileLock(self._lock_path, self._lock_timeout):
                    self._append_locked(event)
                return True
            except (OSError, ValueError, TypeError) as e:
                self._record_write_failure(e)
                if self.required:
                    raise AuditWriteError(f"audit entry could not be written: {e}") from e
                return False

    @staticmethod
    def _file_signature(path: Path) -> Optional[Tuple[int, int, int]]:
        try:
            st = os.stat(path)
        except FileNotFoundError:
            return None
        return st.st_ino, st.st_size, st.st_mtime_ns

    def _disk_signature(self) -> Tuple[Optional[Tuple[int, int, int]], Optional[Tuple[int, int, int]]]:
        """Identity, size and modification time of the checkpoint and the current log. Every
        write by any process replaces the checkpoint file, so this changes whenever someone
        else has written (or removed) anything."""
        return self._file_signature(self._checkpoint_path), self._file_signature(self.log_file)

    def _append_locked(self, event: Dict[str, Any]) -> None:
        if self._known_signature is not None and self._disk_signature() == self._known_signature:
            # Nothing on disk changed since this process last wrote, so what it remembers IS
            # the tail. Skipping the re-read matters: opening a just-written file for reading
            # costs milliseconds on Windows while the virus scanner looks at it.
            self._append_entry(event, self._last_seq, str(self._last_hash), self._first_seq, None, True)
            return

        cp, cp_problem = self._read_checkpoint()
        tail, tail_problem = self._read_tail()
        tail_seq = tail.get("seq") if tail is not None and type(tail.get("seq")) is int else None
        tail_hash = tail.get("entry_hash") if tail is not None else None
        anomaly = cp_problem or tail_problem
        first_seq = 1

        if cp is not None:
            # The checkpoint is the authority on where the chain stands. If the log is shorter
            # than it says, the numbering still continues from the checkpoint, so the missing
            # entries show as a gap instead of being papered over.
            seq, prev, first_seq = cp["seq"], cp["last_hash"], cp["first_seq"]
            if (tail is not None and tail_seq == seq + 1 and tail.get("prev_hash") == prev
                    and self._entry_hash_status(tail)[0] == "ok"):
                seq, prev = tail_seq, str(tail_hash)  # a write interrupted before its checkpoint
        elif tail_seq is not None:
            # Numbered entries but no usable checkpoint. Every numbered write leaves one, so
            # it was removed; say so in the entry instead of quietly recreating it.
            anomaly = anomaly or "the checkpoint file was missing"
            seq, prev = tail_seq, str(tail_hash)
        elif tail is not None:
            # A log from before checkpoints existed: adopt it. Its entries carry no sequence
            # numbers, so numbering starts here.
            seq, prev = 0, (str(tail_hash) if tail_hash else GENESIS)
        elif self._last_seq:
            anomaly = anomaly or f"the log and its checkpoint disappeared after entry {self._last_seq}"
            seq, prev = self._last_seq, str(self._last_hash)
        else:
            seq, prev = 0, GENESIS

        self._append_entry(event, seq, prev, first_seq, anomaly, cp is not None)

    def _append_entry(self, event: Dict[str, Any], seq: int, prev: str, first_seq: int,
                      anomaly: Optional[str], has_checkpoint: bool) -> None:
        """Write the entry that follows (seq, prev) and move the checkpoint to it."""
        if anomaly is None:
            if not has_checkpoint:
                # A first write. The checkpoint goes down before the entry so that a crash
                # between the two reads as an interrupted write, not as a deleted checkpoint.
                self._write_checkpoint(seq, prev, first_seq)
            first_seq = self._rotate_if_needed(seq, prev, first_seq)

        entry = dict(event)
        entry["seq"] = seq + 1
        entry["alg"] = self._alg
        if anomaly:
            # Part of the signed entry, so the evidence cannot be dropped without the key.
            entry["chain_anomaly"] = anomaly
            logger.error("Audit log integrity problem recorded in entry %d: %s", seq + 1, anomaly)
        entry["prev_hash"] = prev
        entry["entry_hash"] = self._hash_event(entry)
        self._known_signature = None  # until this write has fully landed, trust only the disk
        with open(self.log_file, 'a', encoding='utf-8') as f:
            f.write(json.dumps(entry, default=str) + '\n')
        self._last_seq, self._last_hash = entry["seq"], entry["entry_hash"]
        self._write_checkpoint(entry["seq"], entry["entry_hash"], first_seq)
        self._first_seq = first_seq
        self._known_signature = self._disk_signature()

    # -------------------------------------------------------------- verification

    def verify_chain(self) -> Dict[str, Any]:
        """Verify every retained file of the audit log against its chain and checkpoint.

        Returns {valid, checked, message} plus, when something is wrong, broken_file and
        broken_line for the first problem. Also reported: files, first_seq, last_seq,
        keyed (an audit key is configured) and unkeyed_entries (entries only chain-checked).
        The module docstring lists what this can and cannot detect.
        """
        with self._lock:
            if not self.log_dir.is_dir():
                if self._last_seq:
                    return {"valid": False, "checked": 0, "message": (
                        f"The audit log folder is gone; this server wrote entry {self._last_seq} "
                        f"in this session.")}
                return {"valid": True, "checked": 0, "message": "No audit log file yet."}
            try:
                with _FileLock(self._lock_path, self._lock_timeout):
                    return self._verify_locked()
            except OSError as e:
                return {"valid": False, "checked": 0, "message": f"Verification error: {e}"}

    def _verify_locked(self) -> Dict[str, Any]:
        files = self._chain_files()
        cp, cp_problem = self._read_checkpoint()
        state = {"checked": 0, "files": len(files), "first_seq": None, "last_seq": None,
                 "keyed": bool(self._hmac_key), "unkeyed_entries": 0}

        def bad(message: str, path: Optional[Path] = None, line: Optional[int] = None) -> Dict[str, Any]:
            result = {"valid": False, **state, "message": message}
            if path is not None:
                result["broken_file"] = path.name
                result["broken_line"] = line
            return result

        prev_hash: Optional[str] = None
        prev_seq: Optional[int] = None
        keyed_seen = False
        hashed_seen = False
        first_unhashed: Optional[Tuple[Path, int]] = None

        for path in files:
            with open(path, "r", encoding="utf-8", errors="replace") as f:
                for lineno, raw in enumerate(f, 1):
                    raw = raw.strip()
                    if not raw:
                        continue
                    try:
                        entry = json.loads(raw)
                    except ValueError:
                        return bad(f"Line {lineno} is not valid JSON (corrupted).", path, lineno)
                    if not isinstance(entry, dict):
                        return bad(f"Line {lineno} is not an audit entry.", path, lineno)
                    stored = entry.get("entry_hash")
                    if stored is None:
                        # Acceptable only in a log written before chaining existed, where NO
                        # entry has a hash; one stripped hash must not pass as such an entry.
                        if hashed_seen:
                            return bad(f"Line {lineno} is missing entry_hash (tampering: stripped from a "
                                       f"chained log).", path, lineno)
                        if first_unhashed is None:
                            first_unhashed = (path, lineno)
                        state["checked"] += 1
                        continue
                    if first_unhashed is not None:
                        return bad(f"Line {first_unhashed[1]} is missing entry_hash (tampering: stripped "
                                   f"from a chained log).", *first_unhashed)
                    hashed_seen = True
                    status, keyed = self._entry_hash_status(entry)
                    if status == "nokey":
                        return bad(f"Line {lineno} was written with an audit key, but {AUDIT_KEY_ENV} is "
                                   f"not set: the log cannot be verified without it.", path, lineno)
                    if status == "mismatch":
                        hint = " (or the audit key differs from the one it was written with)" if keyed else ""
                        return bad(f"Line {lineno} entry_hash mismatch - the entry was modified{hint}.",
                                   path, lineno)
                    if keyed:
                        keyed_seen = True
                    else:
                        if keyed_seen:
                            return bad(f"Line {lineno} is not protected by the audit key although earlier "
                                       f"entries are (downgrade).", path, lineno)
                        state["unkeyed_entries"] += 1
                    if prev_hash is not None and entry.get("prev_hash") != prev_hash:
                        return bad(f"Line {lineno} chain linkage broken - an entry was inserted or deleted.",
                                   path, lineno)
                    seq = entry.get("seq")
                    if seq is None:
                        if prev_seq is not None:
                            return bad(f"Line {lineno} has no sequence number although earlier entries do.",
                                       path, lineno)
                    else:
                        if type(seq) is not int:
                            return bad(f"Line {lineno} has an invalid sequence number.", path, lineno)
                        if prev_seq is not None and seq != prev_seq + 1:
                            detail = (f"entries {prev_seq + 1}-{seq - 1} are missing" if seq > prev_seq + 1
                                      else "entries are duplicated or out of order")
                            return bad(f"Line {lineno} sequence jumps from {prev_seq} to {seq}: {detail}.",
                                       path, lineno)
                        if state["first_seq"] is None:
                            state["first_seq"] = seq
                        prev_seq = seq
                        state["last_seq"] = seq
                        if seq == self._last_seq and stored != self._last_hash:
                            return bad(f"Line {lineno} is not the entry {seq} this server wrote in this "
                                       f"session: the log was rewritten.", path, lineno)
                    if entry.get("chain_anomaly"):
                        return bad(f"Line {lineno}: the writer recorded an integrity problem when it wrote "
                                   f"this entry: {entry['chain_anomaly']}.", path, lineno)
                    prev_hash = stored
                    state["checked"] += 1

        if cp_problem:
            return bad(f"Checkpoint problem: {cp_problem}.")
        if cp is None:
            if prev_seq is not None:
                return bad("The checkpoint file is missing although the log has numbered entries (deleted?).")
        else:
            if cp["alg"] == _PLAIN and keyed_seen:
                return bad("The checkpoint is not protected by the audit key although log entries are.")
            if prev_seq is None:
                if cp["seq"] > 0:
                    return bad(f"The log is missing: the checkpoint records {cp['seq']} entr(ies) and none "
                               f"are on disk.")
                if prev_hash is not None and cp["last_hash"] != prev_hash:
                    return bad("The end of the log does not match the checkpoint (entries removed or "
                               "changed).")
            else:
                if prev_seq < cp["seq"]:
                    return bad(f"Entries {prev_seq + 1}-{cp['seq']} are missing from the end of the log "
                               f"(truncated or deleted).")
                if prev_seq == cp["seq"] and prev_hash != cp["last_hash"]:
                    return bad("The last entry does not match the checkpoint.")
                if state["first_seq"] > cp["first_seq"]:
                    return bad(f"Entries {cp['first_seq']}-{state['first_seq'] - 1} are missing from the "
                               f"start of the log (more was removed than rotation drops).")
        if self._last_seq and (prev_seq is None or prev_seq < self._last_seq):
            return bad(f"This server wrote entry {self._last_seq} in this session, but the log now ends at "
                       f"{prev_seq if prev_seq is not None else 'nothing'}.")

        if not files:
            return {"valid": True, **state, "message": "No audit log file yet."}
        if self._hmac_key and hashed_seen and not keyed_seen:
            # Indistinguishable on disk from a log replaced by someone without the key.
            return bad(f"An audit key is set but no entry is signed with it. This is expected only between "
                       f"setting {AUDIT_KEY_ENV} on an existing log and the next entry being written; "
                       f"otherwise the log was replaced.")
        message = f"Chain intact across {state['checked']} entr(ies)."
        if not self._hmac_key:
            message += (f" No audit key is set ({AUDIT_KEY_ENV}): this detects accidental damage, not "
                        f"deliberate rewriting.")
        elif state["unkeyed_entries"]:
            message += (f" The first {state['unkeyed_entries']} entr(ies) are not signed with the audit key "
                        f"(written before a key was set, or put there by someone without it): they are "
                        f"chain-checked only.")
        return {"valid": True, **state, "message": message}

    # ------------------------------------------------------------------- content

    def _generate_session_id(self) -> str:
        """Generate a unique session ID"""
        timestamp = datetime.now(timezone.utc).isoformat()
        return hashlib.sha256(f"{timestamp}{os.getpid()}{os.urandom(8).hex()}".encode()).hexdigest()[:16]

    def _generate_query_fingerprint(self, query: str) -> str:
        """Fingerprint of a query's shape: literals are replaced first, so the same query with
        different filter values has one fingerprint and the values cannot be guessed from it."""
        normalized = ' '.join(strip_literals(query or "").lower().split())
        return hashlib.sha256(normalized.encode()).hexdigest()[:12]

    def _stored_query_text(self, query: str) -> Optional[str]:
        if self.store_query_text == "fingerprint":
            return None
        text = redact_secrets(query or "")
        if self.store_query_text == "redacted":
            text = strip_literals(text)
        if len(text) > _MAX_QUERY_CHARS:
            text = f"{text[:_MAX_QUERY_CHARS]}... [TRUNCATED {len(text) - _MAX_QUERY_CHARS} chars]"
        return text

    @staticmethod
    def _safe_error(error_message: Optional[Any]) -> Optional[str]:
        if error_message is None:
            return None
        text = redact_secrets(error_message)
        if len(text) > _MAX_ERROR_CHARS:
            text = f"{text[:_MAX_ERROR_CHARS]}... [TRUNCATED {len(text) - _MAX_ERROR_CHARS} chars]"
        return text

    def log_event(
        self,
        event_type: AuditEventType,
        severity: AuditSeverity = AuditSeverity.INFO,
        message: str = "",
        details: Optional[Dict[str, Any]] = None,
        tool: Optional[str] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Log a generic audit event. Secrets are redacted from the message, the details and any
        extra fields before the entry is written.

        Returns:
            The event record (without the chain fields)
        """
        event = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'session_id': self._session_id,
            'event_type': event_type.value,
            'severity': severity.value,
            'tool': tool,
            'message': redact_secrets(message),
            'details': redact_structure(details or {}),
            **redact_structure(kwargs)
        }

        self._write_log(event)
        return event

    def log_query(
        self,
        query: str,
        source: str = "desktop",
        model_name: Optional[str] = None,
        port: Optional[int] = None,
        result_count: Optional[int] = None,
        duration_ms: Optional[float] = None,
        success: bool = True,
        error_message: Optional[str] = None,
        tables_accessed: Optional[List[str]] = None,
        columns_accessed: Optional[List[str]] = None,
        pii_detected: bool = False,
        pii_types: Optional[List[str]] = None,
        pii_count: int = 0,
        policy_applied: Optional[Any] = None,
        user_context: Optional[Dict[str, Any]] = None,
        tool: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Log a DAX query execution.

        Args:
            query: The DAX query text (stored according to ``store_query_text``)
            source: "desktop" or "cloud"
            model_name: Name of the Power BI model
            port: Port number (for desktop)
            result_count: Number of rows returned
            duration_ms: Query execution time in milliseconds
            success: Whether query succeeded
            error_message: Error message if failed (secrets are removed)
            tables_accessed: Tables the query referenced
            columns_accessed: Columns and measures the query referenced
            pii_detected: Whether PII was detected
            pii_types: Types of PII detected
            pii_count: Number of PII instances detected
            policy_applied: What the access policy did (blocked/masked columns, denial)
            user_context: Additional context
            tool: Name of the MCP tool that ran the query

        Returns:
            The event record (without the chain fields)
        """
        query = query or ""
        event_type = AuditEventType.QUERY_SUCCESS if success else AuditEventType.QUERY_FAILURE
        severity = AuditSeverity.INFO if success else AuditSeverity.ERROR
        if pii_detected:
            severity = AuditSeverity.WARNING

        query_fingerprint = self._generate_query_fingerprint(query)
        with self._lock:
            self._query_count += 1
            query_number = self._query_count

        event = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'session_id': self._session_id,
            'query_id': f"{self._session_id}_{query_number}",
            'query_number': query_number,
            'event_type': event_type.value,
            'severity': severity.value,
            'tool': tool,
            'source': source,
            'model': model_name,
            'port': port,
            'query': {
                'text': self._stored_query_text(query),
                'stored_as': self.store_query_text,
                'fingerprint': query_fingerprint,
                'length': len(query)
            },
            'result': {
                'success': success,
                'row_count': result_count,
                'duration_ms': duration_ms,
                'error': self._safe_error(error_message)
            },
            'access': {
                'tables': list(tables_accessed or []),
                'columns': list(columns_accessed or []),
                'policy': redact_structure(policy_applied)
            },
            'pii': {
                'detected': pii_detected,
                'types': list(pii_types or []),
                'count': pii_count
            },
            'context': redact_structure(user_context or {})
        }

        self._write_log(event)

        # The raw error is never interpolated here: it can carry a connection-string secret.
        status = "SUCCESS" if success else "FAILED"
        pii_info = f", PII: {pii_count} instances" if pii_detected else ""
        logger.info(f"Query [{query_fingerprint}]: {result_count or 0} rows, {duration_ms or 0:.0f}ms, "
                    f"{status}{pii_info}")

        return event

    def log_connection(
        self,
        source: str,
        model_name: Optional[str] = None,
        port: Optional[int] = None,
        workspace: Optional[str] = None,
        success: bool = True,
        error_message: Optional[str] = None,
        tool: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Log a connection event"""
        severity = AuditSeverity.INFO if success else AuditSeverity.ERROR
        safe_error = self._safe_error(error_message)

        message = f"Connected to {source}"
        if model_name:
            message += f": {model_name}"
        if not success:
            message = f"Connection failed to {source}: {safe_error}"

        return self.log_event(
            event_type=AuditEventType.CONNECTION,
            severity=severity,
            message=message,
            tool=tool,
            details={
                'source': source,
                'model': model_name,
                'port': port,
                'workspace': workspace,
                'success': success,
                'error': safe_error
            }
        )

    def log_policy_violation(
        self,
        policy_name: str,
        violation_type: str,
        table: Optional[str] = None,
        column: Optional[str] = None,
        action_taken: str = "blocked",
        query: Optional[str] = None,
        tool: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Log a policy violation"""
        query_fingerprint = self._generate_query_fingerprint(query) if query else None

        return self.log_event(
            event_type=AuditEventType.POLICY_VIOLATION,
            severity=AuditSeverity.WARNING,
            message=f"Policy violation: {policy_name} - {violation_type}",
            tool=tool,
            details={
                'policy': policy_name,
                'violation': violation_type,
                'table': table,
                'column': column,
                'action': action_taken,
                'query_fingerprint': query_fingerprint
            }
        )

    def log_pii_detection(
        self,
        pii_types: List[str],
        count: int,
        columns_affected: List[str],
        action_taken: str = "masked",
        tool: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Log PII detection event"""
        return self.log_event(
            event_type=AuditEventType.PII_DETECTED,
            severity=AuditSeverity.WARNING,
            message=f"PII detected: {count} instances of {', '.join(pii_types)}",
            tool=tool,
            details={
                'types': pii_types,
                'count': count,
                'columns': columns_affected,
                'action': action_taken
            }
        )

    def get_session_summary(self) -> Dict[str, Any]:
        """A summary of this logger's session, including any write or rotation failures: an
        audit log that silently stopped recording must be visible somewhere."""
        return {
            'session_id': self._session_id,
            'query_count': self._query_count,
            'log_file': str(self.log_file),
            'last_seq': self._last_seq,
            'keyed': bool(self._hmac_key),
            'store_query_text': self.store_query_text,
            'required': self.required,
            'healthy': self.write_failures == 0 and self.rotation_failures == 0,
            'write_failures': self.write_failures,
            'last_write_error': self.last_write_error,
            'rotation_failures': self.rotation_failures,
            'last_rotation_error': self.last_rotation_error,
        }

    def get_recent_events(self, count: int = 100) -> List[Dict[str, Any]]:
        """The most recent ``count`` events, oldest first, reading back into rotated files when
        the current one holds fewer. A count below 1 returns nothing. Lines that are not valid
        JSON are skipped (verify_chain reports them)."""
        try:
            count = int(count)
        except (TypeError, ValueError):
            count = 100
        if count < 1 or not self.log_dir.is_dir():
            return []
        events: List[Dict[str, Any]] = []
        with self._lock, _FileLock(self._lock_path, self._lock_timeout):
            for path in reversed(self._chain_files()):
                recent: deque = deque(maxlen=count - len(events))
                with open(path, "r", encoding="utf-8", errors="replace") as f:
                    for line in f:
                        if line.strip():
                            recent.append(line)
                chunk = []
                for line in recent:
                    try:
                        chunk.append(json.loads(line))
                    except ValueError:
                        continue
                events = chunk + events
                if len(events) >= count:
                    break
        return events


# Global audit logger instance
_audit_logger: Optional[AuditLogger] = None


def get_audit_logger() -> AuditLogger:
    """Get or create the global audit logger instance"""
    global _audit_logger
    if _audit_logger is None:
        _audit_logger = AuditLogger()
    return _audit_logger


def configure_audit_logger(**kwargs) -> AuditLogger:
    """Configure and return the global audit logger"""
    global _audit_logger
    _audit_logger = AuditLogger(**kwargs)
    return _audit_logger
