#!/usr/bin/env python
"""
Run all Power BI MCP test suites.

The suites are assert-based scripts (each exits non-zero on failure) that run WITHOUT
Power BI / ADOMD - they cover the pure logic (security, PBIP rename, model analysis,
diff, refresh classification, governance) and mock the live connectors. Live cloud/
Desktop paths still need a Windows + Power BI environment to verify end to end.

Usage:  python run_tests.py
"""
import pathlib
import subprocess
import sys

ROOT = pathlib.Path(__file__).parent
TESTS = sorted((ROOT / "tests").glob("test_*.py"))


def main() -> int:
    failed = []
    for t in TESTS:
        result = subprocess.run([sys.executable, str(t)], capture_output=True, text=True)
        ok = result.returncode == 0
        print(f"  [{'PASS' if ok else 'FAIL'}] {t.name}")
        if not ok:
            failed.append(t.name)
            print(result.stdout[-2000:])
            print(result.stderr[-1000:])
    print(f"\n{len(TESTS) - len(failed)}/{len(TESTS)} suites passed")
    return 1 if failed else 0


if __name__ == "__main__":
    print("=" * 60)
    print("  Power BI MCP - test suites")
    print("=" * 60)
    sys.exit(main())
