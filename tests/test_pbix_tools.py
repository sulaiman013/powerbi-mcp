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
        check("nothing at all was extracted (members before the bad one included)",
              not os.path.exists(os.path.join(tmp, "out")) or os.listdir(os.path.join(tmp, "out")) == [])
        dest = os.path.join(tmp, "dest")
        for name in ("..\\x.txt", "a/../../x.txt", "/abs.txt", "\\\\server\\share\\x.txt", "C:\\Windows\\x.txt",
                     "D:rel.txt", "."):
            try:
                target = pbix_tools._safe_target(dest, name)
                # A drive-style name is an ordinary file name where drives do not exist.
                inside = os.path.commonpath([os.path.abspath(dest), target]) == os.path.abspath(dest)
                check(f"member {name!r} cannot leave the destination", inside and target != os.path.abspath(dest), target)
            except ValueError:
                check(f"member {name!r} cannot leave the destination", True)
        ok = pbix_tools._safe_target(dest, "Report/Layout")
        check("an ordinary member resolves inside the destination", ok == os.path.join(os.path.abspath(dest), "Report", "Layout"))
        root = os.path.abspath(os.sep)
        check("a drive or filesystem root is a valid destination",
              pbix_tools._safe_target(root, "Report/Layout") == os.path.join(root, "Report", "Layout"))


def test_streaming_and_size_cap():
    print("\n== members are streamed to disk and the unpacked size is capped ==")
    with tempfile.TemporaryDirectory() as tmp:
        p = os.path.join(tmp, "big.pbix")
        model = bytes(range(256)) * 12000                      # ~3 MB: several copy chunks
        _make_pbix(p, {"Version": b"1.28", "DataModel": model,
                       "Report/Layout": json.dumps(LAYOUT).encode("utf-16-le")})
        dest = os.path.join(tmp, "out")
        res = pbix_tools.extract(p, dest)
        check("large member extracted intact", Path(dest, "DataModel").read_bytes() == model)
        check("total bytes reported", res["total_bytes"] == len(model) + 4 + len(json.dumps(LAYOUT).encode("utf-16-le")),
              str(res["total_bytes"]))

        small = os.path.join(tmp, "small")
        raised = None
        try:
            pbix_tools.extract(p, small, max_bytes=1_000_000)
        except ValueError as e:
            raised = str(e)
        check("an archive over the cap is refused", raised is not None and "limit" in raised, str(raised))
        check("and nothing was written for it", not os.path.exists(small))
        check("no cap when max_bytes is None", pbix_tools.extract(p, os.path.join(tmp, "nocap"), max_bytes=None)["file_count"] == 3)
        check("the default cap allows real files but is finite", 1024 ** 3 <= pbix_tools.DEFAULT_MAX_EXTRACT_BYTES < 64 * 1024 ** 3)

        # A layout far larger than any real one is not loaded into memory to count pages.
        real_limit = pbix_tools.MAX_JSON_MEMBER_BYTES
        pbix_tools.MAX_JSON_MEMBER_BYTES = 10
        try:
            info = pbix_tools.inspect(p)
            check("inspect still lists an archive whose layout is over the JSON limit",
                  info["has_layout"] is True and info["page_count"] is None, str(info["page_count"]))
            res = pbix_tools.extract(p, os.path.join(tmp, "nolayout"))
            check("extract keeps the files and just skips decoding such a layout",
                  res["file_count"] == 3 and res["layout_decoded"] is False
                  and not os.path.exists(os.path.join(tmp, "nolayout", "Report", "Layout.json")), str(res))
        finally:
            pbix_tools.MAX_JSON_MEMBER_BYTES = real_limit


if __name__ == "__main__":
    print("=" * 70)
    print("  PBIX TOOLS TESTS")
    print("=" * 70)
    test_inspect_classification()
    test_read_layout()
    test_extract()
    test_zip_slip_protection()
    test_streaming_and_size_cap()
    print("\n" + "=" * 70)
    if _failures:
        print(f"  {len(_failures)} CHECK(S) FAILED: {', '.join(_failures)}")
        sys.exit(1)
    print("  ALL PBIX TOOLS CHECKS PASSED")
    print("=" * 70)
