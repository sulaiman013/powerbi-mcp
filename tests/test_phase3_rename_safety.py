"""
PBIP write safety: every write keeps a file's encoding (BOM or not) and newline style and is
atomic, and a rename that fails part-way through its cascade leaves every file as it was.
The format checks go through the real write paths (authoring and rename), not only the helpers.
No Power BI required. Run: python tests/test_phase3_rename_safety.py
"""
import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import pbip_fixtures as fx  # noqa: E402
import pbip_rename  # noqa: E402
from powerbi_pbip_connector import PowerBIPBIPConnector  # noqa: E402

_failures = []


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f": {detail}" if detail and not cond else ""))
    if not cond:
        _failures.append(name)


def pure_crlf(data: bytes) -> bool:
    return data.count(b"\r\n") == data.count(b"\n") and b"\r\n" in data


def test_read_write_helpers():
    print("\n== _read_text / _write_text keep BOM and CRLF, and write atomically ==")
    c = PowerBIPBIPConnector(auto_backup=False)
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "f.tmdl"
        original = fx.BOM + "table Sales\r\n\tcolumn Amount\r\n".encode("utf-8")
        p.write_bytes(original)
        text = c._read_text(p)
        check("text comes back without the BOM and with plain newlines", text == "table Sales\n\tcolumn Amount\n",
              repr(text))
        c._write_text(p, text)
        check("writing it back is byte-identical", p.read_bytes() == original, repr(p.read_bytes()[:12]))
        c._write_text(p, text + "\tcolumn Qty\n")
        raw = p.read_bytes()
        check("BOM kept once on a changed file", raw.startswith(fx.BOM) and raw.count(fx.BOM) == 1)
        check("every newline is still CRLF, the new line included", pure_crlf(raw) and raw.endswith(b"column Qty\r\n"))
        check("no temp file left behind", [x.name for x in Path(d).iterdir()] == ["f.tmdl"])
        new = Path(d) / "new.tmdl"
        c._write_text(new, "table New\n")
        check("a new file is UTF-8 without BOM, LF", new.read_bytes() == b"table New\n")


def test_authoring_keeps_file_format():
    print("\n== authoring writes keep the format of the files they touch ==")
    with tempfile.TemporaryDirectory() as tmp:
        p = fx.build_project(tmp, crlf=True, bom=True)
        c = p.connector
        sales = p.definition / "tables" / "Sales.tmdl"
        res = c.add_measures("Sales", [{"name": "Units", "expression": "COUNTROWS(Sales)",
                                        "description": "Number of rows."},
                                       {"name": "Big", "expression": "VAR x = 1\nRETURN\n    x + 1"}])
        check("add_measures succeeds", res.get("success") is True, str(res))
        raw = sales.read_bytes()
        check("add_measures: BOM kept", raw.startswith(fx.BOM) and raw.count(fx.BOM) == 1)
        check("add_measures: pure CRLF, appended lines included", pure_crlf(raw)
              and b"\tmeasure Units = COUNTROWS(Sales)\r\n" in raw and b"\t\t\tRETURN\r\n" in raw)
        res = c.add_hierarchy("Sales", "Places", ["Region"])
        check("add_hierarchy: pure CRLF", res.get("success") is True and pure_crlf(sales.read_bytes()), str(res))
        res = c.add_calculation_group("Time Calc", [{"name": "Current", "expression": "SELECTEDMEASURE()"}])
        model = (p.definition / "model.tmdl").read_bytes()
        check("add_calculation_group succeeds and flags the model", res.get("discourage_implicit_measures_set") is True,
              str(res))
        check("model.tmdl: the inserted flag line is CRLF like the rest",
              pure_crlf(model) and b"model Model\r\n\tdiscourageImplicitMeasures\r\n" in model, repr(model[:80]))
        check("model.tmdl: BOM kept", model.startswith(fx.BOM) and model.count(fx.BOM) == 1)
        check("the new table file is written in the project's default form (no BOM, LF)",
              (p.definition / "tables" / "Time Calc.tmdl").read_bytes().startswith(b"table 'Time Calc'\n"))


def test_rename_keeps_file_format():
    print("\n== a rename writes each file back in its own format ==")
    with tempfile.TemporaryDirectory() as tmp:
        p = fx.build_project(tmp)                                   # LF, no BOM
        # one CRLF + BOM file among LF files, as happens when files come from different tools
        mixed = p.definition / "tables" / "_Measures.tmdl"
        fx.write(mixed, fx.MEASURES, bom=True, crlf=True)
        r = p.connector.rename_column_in_files("Sales", "Revenue", "Net Revenue")
        check("rename succeeds", r.success, r.message)
        raw = mixed.read_bytes()
        check("the CRLF + BOM file is still CRLF + BOM", pure_crlf(raw) and raw.startswith(fx.BOM)
              and b"SUM(Sales[Net Revenue])" in raw)
        lf = (p.definition / "tables" / "Sales.tmdl").read_bytes()
        check("an LF file is still LF without BOM", b"\r" not in lf and not lf.startswith(fx.BOM)
              and b"\tcolumn 'Net Revenue'\n" in lf)


def _mixed(text: str, crlf_lines) -> bytes:
    """``text`` with CRLF on the given 0-based line numbers and LF on every other line."""
    lines = text.split("\n")
    out = []
    for i, line in enumerate(lines[:-1]):
        out.append(line + ("\r\n" if i in crlf_lines else "\n"))
    return ("".join(out) + lines[-1]).encode("utf-8")


def _line_endings(data: bytes):
    return [ln.endswith(b"\r\n") for ln in data.splitlines(keepends=True)]


def test_mixed_line_endings_are_kept_line_by_line():
    print("\n== a file that mixes CRLF and LF keeps each line's own ending ==")
    with tempfile.TemporaryDirectory() as tmp:
        p = fx.build_project(tmp, visuals={"overview/card": fx.visual("card", "card", {"Values": [
            fx.projection(fx.measure("_Measures", "Revenue"), "_Measures.Revenue", "Revenue")]})})
        measures = p.definition / "tables" / "_Measures.tmdl"
        measures.write_bytes(_mixed(fx.MEASURES, crlf_lines={3, 4, 5, 12, 13}))      # a pasted CRLF block
        visual = p.visual_path("overview", "card")
        visual.write_bytes(_mixed(fx.read(visual), crlf_lines={0, 1, 2, 3}))
        p.connector.load_project(str(p.pbip))
        before = fx.snapshot(p.root)
        endings = {k: _line_endings(v) for k, v in before.items()}
        check("the fixture really is mixed", any(endings[k].count(True) not in (0, len(endings[k])) for k in endings))

        r = p.connector.rename_measure_in_files("Revenue", "Total Revenue")
        check("rename succeeds", r.success, r.message)
        after = fx.snapshot(p.root)
        changed = [k for k in before if before[k] != after[k]]
        check("the mixed model file and the mixed visual both changed",
              any(k.endswith("_Measures.tmdl") for k in changed) and any(k.endswith("card/visual.json") for k in changed),
              str(changed))
        check("every line of every changed file keeps the ending it had",
              all(_line_endings(after[k]) == endings[k] for k in changed),
              str([k for k in changed if _line_endings(after[k]) != endings[k]]))
        check("the edit landed on a CRLF line and on an LF line",
              b"\tmeasure 'Total Revenue' = SUM(Sales[Revenue])\r\n" in after[next(k for k in changed if k.endswith("_Measures.tmdl"))]
              and b"measure 'Rev x2' = [Total Revenue] * 2\n" in after[next(k for k in changed if k.endswith("_Measures.tmdl"))])
        check("renaming back restores every byte",
              p.connector.rename_measure_in_files("Total Revenue", "Revenue").success and fx.snapshot(p.root) == before)

        res = p.connector.add_measures("_Measures", [{"name": "Added", "expression": "VAR a = 1\nRETURN a"}])
        raw = measures.read_bytes()
        check("appending to a mixed file leaves its existing lines alone and follows the style it ends with",
              res.get("success") is True and raw.startswith(before[next(k for k in before if k.endswith("_Measures.tmdl"))].rstrip())
              and raw.endswith(b"\n\tmeasure Added =\n\t\t\tVAR a = 1\n\t\t\tRETURN a\n"), repr(raw[-60:]))


def test_mid_cascade_failure_changes_nothing():
    print("\n== a failure in the middle of the cascade leaves every file untouched ==")
    with tempfile.TemporaryDirectory() as tmp:
        p = fx.build_project(tmp, visuals={"overview/card": fx.visual("card", "card", {"Values": [
            fx.projection(fx.column("Sales", "Revenue"), "Sales.Revenue", "Revenue")]})})
        before = fx.snapshot(p.root)
        real = pbip_rename.RenameEngine._diagram

        def boom(self, op):          # runs after every TMDL file has been rewritten in memory
            raise OSError("simulated mid-cascade failure")
        pbip_rename.RenameEngine._diagram = boom
        try:
            result = p.connector.rename_table_in_files("Sales", "Sales Renamed")
        finally:
            pbip_rename.RenameEngine._diagram = real
        check("rename reports failure", result.success is False, result.message)
        check("the message carries the cause", "simulated mid-cascade failure" in result.message, result.message)
        check("flagged as nothing written", (result.details or {}).get("nothing_written") is True, str(result.details))
        check("no file is half-renamed: project is byte-identical", fx.snapshot(p.root) == before)
        check("no files reported as modified", result.files_modified == [] and result.references_updated == 0)
        ok = p.connector.rename_table_in_files("Sales", "Sales Renamed")
        check("the same rename succeeds once the failure is gone", ok.success, ok.message)


if __name__ == "__main__":
    print("=" * 70)
    print("  PBIP WRITE SAFETY TESTS")
    print("=" * 70)
    test_read_write_helpers()
    test_authoring_keeps_file_format()
    test_rename_keeps_file_format()
    test_mixed_line_endings_are_kept_line_by_line()
    test_mid_cascade_failure_changes_nothing()
    print("\n" + "=" * 70)
    if _failures:
        print(f"  {len(_failures)} CHECK(S) FAILED: {', '.join(_failures)}")
        sys.exit(1)
    print("  ALL PBIP WRITE SAFETY CHECKS PASSED")
    print("=" * 70)
