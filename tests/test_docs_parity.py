"""
Keeps the documentation honest against the code: tool names and counts, the safety class shown
for each tool, suite counts, environment variables, and the generated spec reference.
No Power BI required. Run: python tests/test_docs_parity.py
"""
import ast
import glob
import io
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault("POWERBI_MCP_AUDIT_DIR", __import__("tempfile").mkdtemp(prefix="pbi-mcp-audit-"))  # keep logs/ clean
os.environ["ENABLE_AUDIT"] = "false"
sys.path.insert(0, os.path.join(ROOT, "src"))
import logging  # noqa: E402

logging.disable(logging.CRITICAL)
import server  # noqa: E402

_failures = []


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f": {detail}" if detail and not cond else ""))
    if not cond:
        _failures.append(name)


def read(*parts):
    with io.open(os.path.join(ROOT, *parts), encoding="utf-8") as f:
        return f.read()


def test_tool_reference():
    print("\n== docs/TOOLS.md lists every tool, once, with its real safety class ==")
    srv = server.PowerBIMCPServer()
    tools = {t.name: t for t in srv._tools}
    text = read("docs", "TOOLS.md")
    rows = re.findall(r"^\| `([a-z_0-9]+)` \| (\S+) \|", text, flags=re.MULTILINE)
    documented = [name for name, _ in rows]
    check("every registered tool is documented", set(tools) <= set(documented), str(sorted(set(tools) - set(documented))))
    check("no documented tool is missing from the server", set(documented) <= set(tools),
          str(sorted(set(documented) - set(tools))))
    check("no tool is documented twice", len(documented) == len(set(documented)),
          str(sorted(n for n in set(documented) if documented.count(n) > 1)))

    marks = {"read": chr(0x1F7E2), "write": chr(0x1F7E1), "destructive": chr(0x1F534)}
    wrong = []
    for name, mark in rows:
        a = tools[name].annotations if name in tools else None
        if a is None:
            continue
        expected = marks["read"] if a.readOnlyHint else (marks["destructive"] if a.destructiveHint else marks["write"])
        if mark != expected:
            wrong.append(name)
    check("each tool's read / write / destructive mark matches its annotation", not wrong, str(wrong))

    sections = re.findall(r"^## .+ — (\d+)\s*$", text, flags=re.MULTILINE)
    check("section counts add up to the number of tools", sum(int(n) for n in sections) == len(tools),
          f"{sum(int(n) for n in sections)} vs {len(tools)}")
    for heading, count in re.findall(r"^## (.+) — (\d+)\s*$", text, flags=re.MULTILINE):
        body = text.split(f"## {heading} — {count}", 1)[1].split("\n## ", 1)[0]
        listed = len(re.findall(r"^\| `[a-z_0-9]+` \|", body, flags=re.MULTILINE))
        check(f"section '{heading}' lists {count} tool(s)", listed == int(count), f"lists {listed}")

    structured = len(srv._structured_tools)
    check("the stated number of structured tools is right", f"{structured} of them also return typed" in text,
          f"server has {structured}")


def test_counts_everywhere():
    print("\n== the tool and suite counts quoted in the docs are the real ones ==")
    srv = server.PowerBIMCPServer()
    n_tools = len(srv._tools)
    n_suites = len(glob.glob(os.path.join(ROOT, "tests", "test_*.py")))
    for rel in (("README.md",), ("docs", "TOOLS.md"), ("docs", "ARCHITECTURE.md"), ("docs", "TESTING.md"), ("AGENTS.md",)):
        text = read(*rel)
        name = "/".join(rel)
        tool_counts = {int(n) for n in re.findall(r"\b(\d{2,3})\*{0,2} tools\b", text)}
        tool_counts |= {int(n) for n in re.findall(r"Tools-(\d+)-", text)}
        check(f"{name}: tool count is {n_tools} wherever it is quoted", tool_counts <= {n_tools}, str(sorted(tool_counts)))
        suite_counts = {int(n) for n in re.findall(r"\b(\d{2,3}) (?:assert-based test suites|assert suites|suites)\b", text)}
        check(f"{name}: suite count is {n_suites} wherever it is quoted", suite_counts <= {n_suites},
              str(sorted(suite_counts)))
    check("the README quotes the tool count", f"{n_tools} tools" in read("README.md"))


def test_suites_are_documented():
    print("\n== docs/TESTING.md lists every suite ==")
    text = read("docs", "TESTING.md")
    suites = sorted(os.path.basename(p) for p in glob.glob(os.path.join(ROOT, "tests", "test_*.py")))
    missing = [s for s in suites if f"`{s}`" not in text]
    check("every suite appears in the table", not missing, str(missing))
    listed = re.findall(r"`(test_[a-z0-9_]+\.py)`", text)
    stale = sorted(set(listed) - set(suites))
    check("no suite is listed that does not exist", not stale, str(stale))


def test_environment_variables():
    print("\n== every environment variable the code reads is documented ==")
    used = set()
    for path in glob.glob(os.path.join(ROOT, "src", "**", "*.py"), recursive=True):
        with io.open(path, encoding="utf-8") as f:
            source = f.read()
        used |= set(re.findall(r"""["']((?:POWERBI_MCP|ENABLE)_[A-Z_]+|ADOMD_DLL_PATH|TOM_DLL_PATH)["']""", source))
    tools_doc, env_example = read("docs", "TOOLS.md"), read(".env.example")
    not_for_env_file = {"POWERBI_MCP_SKIP_DOTENV"}   # it decides whether the .env file is read at all
    for var in sorted(used):
        check(f"{var} is in docs/TOOLS.md", f"`{var}`" in tools_doc)
        if var not in not_for_env_file:
            check(f"{var} is in .env.example", var in env_example)


def test_version_and_changelog():
    print("\n== version ==")
    changelog = read("CHANGELOG.md")
    newest = re.search(r"^## \[(\d+\.\d+\.\d+)\]", changelog, flags=re.MULTILINE)
    check("the newest changelog entry is the server version", bool(newest) and newest.group(1) == server.__version__,
          f"changelog {newest.group(1) if newest else None} vs server {server.__version__}")


def test_report_spec_reference_is_current():
    print("\n== docs/REPORT_SPEC.md matches the report_spec docstring ==")
    doc = ast.get_docstring(ast.parse(read("src", "report_spec.py"))).split("THE SPEC", 1)[1]
    reference = read("docs", "REPORT_SPEC.md")
    lines = [ln.strip() for ln in doc.splitlines() if len(ln.strip()) > 40]
    missing = [ln for ln in lines if ln not in reference]
    check("every substantial line of the docstring is in the reference (re-run docs/build_report_spec_doc.py)",
          not missing, f"{len(missing)} line(s), first: {missing[:1]}")


def test_architecture_file_map():
    print("\n== docs/ARCHITECTURE.md names every source module ==")
    text = read("docs", "ARCHITECTURE.md")
    modules = sorted(os.path.basename(p) for p in glob.glob(os.path.join(ROOT, "src", "*.py")))
    missing = [m for m in modules if m not in text]
    check("every module in src/ appears in the file map", not missing, str(missing))


if __name__ == "__main__":
    for fn in (test_tool_reference, test_counts_everywhere, test_suites_are_documented,
               test_environment_variables, test_version_and_changelog, test_report_spec_reference_is_current,
               test_architecture_file_map):
        fn()
    print()
    if _failures:
        print(f"FAILED: {len(_failures)} check(s): {_failures}")
        sys.exit(1)
    print("ALL DOCS PARITY TESTS PASSED")
