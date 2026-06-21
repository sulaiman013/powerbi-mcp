"""
Phase 3 tests: PBIP rename hardening.
 - _read_text/_write_text round-trip preserves BOM and CRLF, and writes atomically.
 - A mid-cascade failure rolls EVERY edited file back (no half-renamed model+report).

Run: python test_phase3_rename_safety.py   (pure Python, no Power BI)
"""
import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))
from powerbi_pbip_connector import PowerBIPBIPConnector, PBIPProject  # noqa: E402

_failures = []


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f": {detail}" if detail and not cond else ""))
    if not cond:
        _failures.append(name)


def test_atomic_encoding_roundtrip():
    print("\n== _read_text / _write_text (BOM + CRLF + atomic) ==")
    c = PowerBIPBIPConnector(auto_backup=False)
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "f.tmdl"
        # utf-8 BOM + CRLF line endings
        p.write_bytes("﻿table Sales\r\n\tcolumn Amount\r\n".encode("utf-8"))
        text = c._read_text(p)
        check("BOM stripped from text", not text.startswith("﻿"))
        check("encoding recorded as utf-8-sig", c._file_encodings[str(p)] == "utf-8-sig")
        check("CRLF preserved in text", "\r\n" in text)
        # write back unchanged
        c._write_text(p, text)
        raw = p.read_bytes()
        check("BOM re-added on write", raw.startswith(b"\xef\xbb\xbf"), repr(raw[:5]))
        check("CRLF preserved on write", b"\r\n" in raw)
        check("no leftover .tmp file", not (Path(d) / "f.tmdl.tmp").exists())


def test_transactional_rollback():
    print("\n== transactional rollback on mid-cascade failure ==")
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        tmdl = root / "Sales.tmdl"
        original = "table Sales\n\tcolumn Amount\n\nmeasure Total = SUM(Sales[Amount])\n"
        tmdl.write_text(original, encoding="utf-8")

        project = PBIPProject(
            root_path=root,
            pbip_file=root / "proj.pbip",
            semantic_model_folder=None,
            report_folder=None,
            report_json_path=None,
            tmdl_files=[tmdl],
        )
        c = PowerBIPBIPConnector(auto_backup=False)
        c.current_project = project

        # Force a failure AFTER the TMDL step has edited+cached the file (step 5: validate)
        def boom():
            raise RuntimeError("simulated mid-cascade failure")
        c.validate_tmdl_syntax = boom

        result = c.rename_table_in_files("Sales", "Sales Renamed")
        check("rename reports failure", result.success is False, result.message)
        check("flagged as rolled_back", (result.details or {}).get("rolled_back") is True, str(result.details))

        restored = tmdl.read_text(encoding="utf-8")
        check("file content restored to original", restored == original,
              f"got: {restored!r}")
        check("not left half-renamed", "Sales Renamed" not in restored)


if __name__ == "__main__":
    print("=" * 70)
    print("  PHASE 3 (PBIP RENAME SAFETY) TESTS")
    print("=" * 70)
    test_atomic_encoding_roundtrip()
    test_transactional_rollback()
    print("\n" + "=" * 70)
    if _failures:
        print(f"  {len(_failures)} CHECK(S) FAILED: {', '.join(_failures)}")
        sys.exit(1)
    print("  ALL PHASE 3 CHECKS PASSED")
    print("=" * 70)
