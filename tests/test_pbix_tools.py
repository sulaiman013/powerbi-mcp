"""
Tests for PBIX archive inspection/extraction on synthetic .pbix packages (no Power BI required).
Covers thin vs thick classification, legacy UTF-16-LE layout decoding, extraction, and Zip-Slip
protection. Run: python tests/test_pbix_tools.py
"""
import json
import os
import sys
import tempfile
import zipfile
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))
import pbix_tools  # noqa: E402

_failures = []


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f": {detail}" if detail and not cond else ""))
    if not cond:
        _failures.append(name)


LAYOUT = {"sections": [{"name": "p1", "displayName": "Page 1"}, {"name": "p2", "displayName": "Page 2"}],
          "config": "{}"}


def _make_pbix(path, entries):
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as z:
        for name, data in entries.items():
            z.writestr(name, data)


def _thin(path):
    _make_pbix(path, {
        "Version": "1.28".encode("utf-8"),
        "[Content_Types].xml": b"<Types/>",
        "Connections": json.dumps({"Version": 4, "Connections": []}).encode("utf-8"),
        "Report/Layout": json.dumps(LAYOUT).encode("utf-16-le"),
    })


def _thick(path):
    _make_pbix(path, {
        "Version": "1.28".encode("utf-8"),
        "DataModel": b"\x00\x01\x02opaque-vertipaq-bytes",
        "Report/Layout": json.dumps(LAYOUT).encode("utf-16-le"),
    })


def test_inspect_classification():
    print("\n== inspect classifies thin vs thick and the report format ==")
    with tempfile.TemporaryDirectory() as tmp:
        thin = os.path.join(tmp, "thin.pbix")
        thick = os.path.join(tmp, "thick.pbix")
        _thin(thin)
        _thick(thick)
        ti = pbix_tools.inspect(thin)
        check("thin classified", ti["type"].startswith("thin") and ti["has_connections"] and not ti["has_data_model"], str(ti["type"]))
        check("legacy report format", ti["report_format"].startswith("legacy"), ti["report_format"])
        check("page count from layout", ti["page_count"] == 2, str(ti["page_count"]))
        check("entries listed", ti["entry_count"] == 4)
        ki = pbix_tools.inspect(thick)
        check("thick classified", ki["type"].startswith("thick") and ki["has_data_model"], str(ki["type"]))


def test_read_layout():
    print("\n== read_layout decodes UTF-16-LE JSON ==")
    with tempfile.TemporaryDirectory() as tmp:
        p = os.path.join(tmp, "r.pbix")
        _thin(p)
        lj = pbix_tools.read_layout(p)
        check("layout decoded to dict", isinstance(lj, dict) and len(lj.get("sections", [])) == 2, str(type(lj)))


def test_extract():
    print("\n== extract writes members + a decoded Layout.json ==")
    with tempfile.TemporaryDirectory() as tmp:
        p = os.path.join(tmp, "r.pbix")
        _thin(p)
        dest = os.path.join(tmp, "out")
        res = pbix_tools.extract(p, dest)
        check("files extracted", res["file_count"] == 4 and "Report/Layout" in res["files"], str(res["files"]))
        check("layout decoded flag", res["layout_decoded"] is True)
        decoded = Path(dest) / "Report" / "Layout.json"
        check("decoded Layout.json written + valid", decoded.exists() and json.loads(decoded.read_text(encoding="utf-8"))["sections"][0]["name"] == "p1")


def test_zip_slip_protection():
    print("\n== extraction rejects path-traversal members ==")
    with tempfile.TemporaryDirectory() as tmp:
        p = os.path.join(tmp, "evil.pbix")
        _make_pbix(p, {"Version": b"1", "../escape.txt": b"pwned", "Report/Layout": json.dumps(LAYOUT).encode("utf-16-le")})
        raised = False
        try:
            pbix_tools.extract(p, os.path.join(tmp, "out"))
        except ValueError:
            raised = True
        check("path traversal rejected", raised)
        check("nothing escaped the dest", not os.path.exists(os.path.join(tmp, "escape.txt")))


if __name__ == "__main__":
    print("=" * 70)
    print("  PBIX TOOLS TESTS")
    print("=" * 70)
    test_inspect_classification()
    test_read_layout()
    test_extract()
    test_zip_slip_protection()
    print("\n" + "=" * 70)
    if _failures:
        print(f"  {len(_failures)} CHECK(S) FAILED: {', '.join(_failures)}")
        sys.exit(1)
    print("  ALL PBIX TOOLS CHECKS PASSED")
    print("=" * 70)
