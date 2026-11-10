#!/usr/bin/env python
"""
Run all Power BI MCP test suites.

The suites are assert-based scripts that run WITHOUT Power BI / ADOMD: they cover the pure
logic (DAX tokenizer, security, PBIP rename, report building, model analysis, diff, refresh
classification, governance) and exercise the server and live connectors through fakes. Live
cloud/Desktop paths still need a Windows + Power BI environment to verify end to end.

A suite fails when it exits non-zero OR prints a line starting with "[FAIL]": a suite that
reports a failed check but still exits 0 must not count as green.

Usage:  python run_tests.py              run everything
        python run_tests.py rename dax   run suites whose file name contains any given word
"""
import os
import pathlib
import re
import subprocess
import sys
import time

ROOT = pathlib.Path(__file__).parent
FAIL_LINE = re.compile(r"^\s*\[FAIL\]", re.MULTILINE)


def discover(filters):
    tests = sorted((ROOT / "tests").glob("test_*.py"))
    if filters:
        tests = [t for t in tests if any(f.lower() in t.name.lower() for f in filters)]
    return tests


def main(argv) -> int:
    tests = discover(argv)
    if not tests:
        print("No test suites matched.")
        return 1
    env = dict(os.environ, PYTHONDONTWRITEBYTECODE="1", PYTHONIOENCODING="utf-8")
    failed = []
    started = time.time()
    for t in tests:
        t0 = time.time()
        result = subprocess.run([sys.executable, str(t)], capture_output=True, text=True,
                                encoding="utf-8", errors="replace", env=env)
        printed_fail = bool(FAIL_LINE.search(result.stdout or ""))
        ok = result.returncode == 0 and not printed_fail
        print(f"  [{'PASS' if ok else 'FAIL'}] {t.name}  ({time.time() - t0:.1f}s)")
        if not ok:
            failed.append(t.name)
            if result.returncode == 0 and printed_fail:
                print("    exited 0 but printed failed checks:")
            lines = [ln for ln in (result.stdout or "").splitlines() if FAIL_LINE.match(ln)]
            for ln in lines[:20]:
                print("    " + ln.strip())
            if not lines:
                print((result.stdout or "")[-2000:])
            if result.stderr:
                print(result.stderr[-1500:])
    print(f"\n{len(tests) - len(failed)}/{len(tests)} suites passed in {time.time() - started:.0f}s")
    if failed:
        print("Failed: " + ", ".join(failed))
    return 1 if failed else 0


if __name__ == "__main__":
    print("=" * 60)
    print("  Power BI MCP - test suites")
    print("=" * 60)
    sys.exit(main(sys.argv[1:]))
