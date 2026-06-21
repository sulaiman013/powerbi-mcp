"""
Tests for the shared ADOMD.NET discovery (issue #12: newer Power BI Desktop no longer
ships the client DLL). Verifies the ADOMD_DLL_PATH override and recursive discovery.
Run: python tests/test_adomd_loader.py
"""
import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))
from adomd_loader import DLL_NAME, find_adomd_dll, ensure_adomd_on_path  # noqa: E402

_failures = []


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f": {detail}" if detail and not cond else ""))
    if not cond:
        _failures.append(name)


def _clear_env():
    os.environ.pop("ADOMD_DLL_PATH", None)


def test_env_override_directory():
    print("\n== ADOMD_DLL_PATH points to a folder ==")
    with tempfile.TemporaryDirectory() as d:
        (Path(d) / DLL_NAME).write_bytes(b"stub")
        os.environ["ADOMD_DLL_PATH"] = d
        try:
            check("returns the folder", find_adomd_dll() == Path(d))
        finally:
            _clear_env()


def test_env_override_file():
    print("\n== ADOMD_DLL_PATH points to the DLL file ==")
    with tempfile.TemporaryDirectory() as d:
        f = Path(d) / DLL_NAME
        f.write_bytes(b"stub")
        os.environ["ADOMD_DLL_PATH"] = str(f)
        try:
            check("returns the parent folder", find_adomd_dll() == Path(d))
        finally:
            _clear_env()


def test_env_override_recursive():
    print("\n== ADOMD_DLL_PATH folder with DLL in a subfolder ==")
    with tempfile.TemporaryDirectory() as d:
        sub = Path(d) / "lib" / "net6.0"
        sub.mkdir(parents=True)
        (sub / DLL_NAME).write_bytes(b"stub")
        os.environ["ADOMD_DLL_PATH"] = d
        try:
            check("finds DLL in subfolder", find_adomd_dll() == sub)
        finally:
            _clear_env()


def test_env_missing_does_not_crash():
    print("\n== bad ADOMD_DLL_PATH falls through cleanly ==")
    with tempfile.TemporaryDirectory() as d:  # empty, no DLL
        os.environ["ADOMD_DLL_PATH"] = d
        try:
            r = find_adomd_dll()  # should fall through to system search, never crash
            check("returns Path or None", r is None or isinstance(r, Path))
        finally:
            _clear_env()


def test_ensure_on_path():
    print("\n== ensure_adomd_on_path adds the dir to sys.path ==")
    with tempfile.TemporaryDirectory() as d:
        (Path(d) / DLL_NAME).write_bytes(b"stub")
        os.environ["ADOMD_DLL_PATH"] = d
        try:
            got = ensure_adomd_on_path()
            check("returns the dir", got == Path(d))
            check("added to sys.path", d in sys.path)
        finally:
            _clear_env()
            if d in sys.path:
                sys.path.remove(d)


if __name__ == "__main__":
    print("=" * 70)
    print("  ADOMD LOADER TESTS (issue #12)")
    print("=" * 70)
    test_env_override_directory()
    test_env_override_file()
    test_env_override_recursive()
    test_env_missing_does_not_crash()
    test_ensure_on_path()
    print("\n" + "=" * 70)
    if _failures:
        print(f"  {len(_failures)} CHECK(S) FAILED: {', '.join(_failures)}")
        sys.exit(1)
    print("  ALL ADOMD LOADER CHECKS PASSED")
    print("=" * 70)
