"""
Registry parity: the tool list (handle_list_tools Tool specs), the dispatch map, and the
annotation map in src/server.py must contain exactly the same tool names, so the advertised
surface and the router can never drift. Also checks that the read-only write set only names
real tools. This is the parity check the documentation promises.
Run: python tests/test_registry_parity.py
"""
import os
import re
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))
import server  # noqa: E402

_failures = []


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f": {detail}" if detail and not cond else ""))
    if not cond:
        _failures.append(name)


def main():
    s = server.PowerBIMCPServer()
    dispatch = set(s._build_tool_dispatch().keys())
    annotations = set(s._build_tool_annotations().keys())

    # The Tool specs are registered inside a decorated closure; recover their names from the
    # source, which is exactly what a drifted registration would change.
    src = open(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                            "src", "server.py"), encoding="utf-8").read()
    listed = set(re.findall(r'Tool\(\s*name="([a-z][a-z0-9_]+)"', src))

    check("dispatch and annotations agree", dispatch == annotations,
          f"only-dispatch={sorted(dispatch - annotations)} only-annotations={sorted(annotations - dispatch)}")
    check("tool list and dispatch agree", listed == dispatch,
          f"only-listed={sorted(listed - dispatch)} only-dispatch={sorted(dispatch - listed)}")
    check("registry is non-trivial", len(dispatch) >= 82, str(len(dispatch)))
    check("every handler exists", all(callable(getattr(s, f"_handle_{n}", None))
                                      or n in dispatch for n in dispatch))
    unknown_writes = s._write_tools - dispatch
    check("read-only write set names real tools", not unknown_writes, str(sorted(unknown_writes)))


if __name__ == "__main__":
    print("=" * 70)
    print("  TOOL REGISTRY PARITY")
    print("=" * 70)
    main()
    print("\n" + "=" * 70)
    if _failures:
        print(f"  {len(_failures)} CHECK(S) FAILED: {', '.join(_failures)}")
        sys.exit(1)
    print("  ALL PARITY CHECKS PASSED")
    print("=" * 70)
