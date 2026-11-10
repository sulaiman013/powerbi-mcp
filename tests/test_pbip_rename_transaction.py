"""
PBIP rename, transaction and reporting behaviour: a rename either changes every file it must
or changes nothing, and the result says which. Covers the refusals (missing object, name
collision, bad input), batches as one transaction, failures while reading, validating and
writing, undo, backups, and which project a .pbip resolves to.
No Power BI required. Run: python tests/test_pbip_rename_transaction.py
"""
import os
import shutil
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import pbip_fixtures as fx  # noqa: E402
import pbip_report  # noqa: E402
import safe_io  # noqa: E402
from powerbi_pbip_connector import PowerBIPBIPConnector, RenameResult  # noqa: E402

_failures = []


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f": {detail}" if detail and not cond else ""))
    if not cond:
        _failures.append(name)


def visuals():
    return {"overview/card": fx.visual("card", "card", {"Values": [
        fx.projection(fx.measure("_Measures", "Revenue"), "_Measures.Revenue", "Revenue")]}),
        "overview/grid": fx.visual("grid", "tableEx", {"Values": [
            fx.projection(fx.column("Sales", "Region"), "Sales.Region", "Region"),
            fx.projection(fx.column("Customers", "Name"), "Customers.Name", "Name")]})}


def refused(name, project, result, *words):
    """A refusal: success False, a message that says why, and not one byte changed."""
    check(f"{name}: success is False", result.success is False, result.message)
    check(f"{name}: message says why", all(w in result.message for w in words), result.message)
    check(f"{name}: nothing reported as modified", result.files_modified == [] and result.references_updated == 0)
    check(f"{name}: project is byte-identical", fx.snapshot(project.root) == project.before)


def build(tmp, **kwargs):
    kwargs.setdefault("visuals", visuals())
    p = fx.build_project(tmp, **kwargs)
    p.before = fx.snapshot(p.root)
    return p


def test_refusals():
    print("\n== refusals are truthful and write nothing ==")
    with tempfile.TemporaryDirectory() as tmp:
        p = build(tmp)
        c = p.connector
        refused("unknown table", p, c.rename_table_in_files("DoesNotExist", "Whatever"), "does not exist")
        refused("unknown column", p, c.rename_column_in_files("Sales", "Nope", "X"), "does not exist")
        refused("column that is really a measure", p, c.rename_column_in_files("_Measures", "Revenue", "X"),
                "it is a measure")
        refused("unknown measure", p, c.rename_measure_in_files("Nope", "X"), "does not exist")
        refused("table name already taken", p, c.rename_table_in_files("Sales", "customers"), "already exists")
        refused("column name already taken by a column", p, c.rename_column_in_files("Sales", "ID", "Region"),
                "already has a column or measure")
        refused("measure name already taken", p, c.rename_measure_in_files("Revenue", "Rev x2"), "already exists")
        refused("empty new name", p, c.rename_table_in_files("Sales", "  "), "empty")
        refused("new name with a line break", p, c.rename_measure_in_files("Revenue", "A\nmeasure B = 1"),
                "line break")
        refused("same name", p, c.rename_table_in_files("Sales", "Sales"), "already has that name")
        refused("batch entry without a new name", p, c.batch_rename_tables([{"old_name": "Sales"}]), "needs")
        refused("column batch entry without a table", p,
                c.batch_rename_columns([{"old_name": "ID", "new_name": "X"}]), "table_name")
        refused("empty batch", p, c.batch_rename_measures([]), "no renames")
        check("a RenameResult always carries a warnings list",
              RenameResult(True, "", [], 0).warnings == [] and c.rename_table_in_files("Nope", "X").warnings == [])


def test_batch_is_one_transaction():
    print("\n== a batch is applied completely or not at all ==")
    with tempfile.TemporaryDirectory() as tmp:
        p = build(tmp)
        r = p.connector.batch_rename_columns([
            {"table_name": "Sales", "old_name": "Region", "new_name": "Zone"},
            {"table_name": "Sales", "old_name": "Missing", "new_name": "X"}])
        refused("second rename invalid", p, r, "'Sales'[Missing] does not exist")
    with tempfile.TemporaryDirectory() as tmp:
        p = build(tmp)
        r = p.connector.batch_rename_tables([
            {"old_name": "Sales", "new_name": "Orders"},
            {"old_name": "Orders", "new_name": "Sales Orders"},      # sees the first rename
            {"old_name": "Customers", "new_name": "Sales"}])          # the old name is free again
        check("chained batch succeeds", r.success, r.message)
        check("message lists every rename and the totals",
              "'Sales' -> 'Orders'" in r.message and "'Customers' -> 'Sales'" in r.message
              and f"{r.references_updated} reference(s)" in r.message, r.message)
        check("counts are real", r.references_updated > 20 and len(r.files_modified) >= 8,
              f"{r.references_updated} refs, {len(r.files_modified)} files")
        check("every reported file exists under its real name", all(os.path.exists(f) for f in r.files_modified)
              and any(f.endswith("diagramLayout.json") for f in r.files_modified), str(r.files_modified))
        check("operations are recorded in order", [o["new"] for o in r.details["operations"]]
              == ["Orders", "Sales Orders", "Sales"])
        m = p.tmdl("tables/_Measures.tmdl")
        check("final state reflects all three",
              "SUM('Sales Orders'[Revenue])" in m and "VAR _n = COUNTROWS(Sales)" in m
              and "'Sales Orders'[Region] = \"East\", Sales[Region] = \"East\"" in m, m)
        grid = p.visual("overview", "grid")["visual"]["query"]["queryState"]["Values"]["projections"]
        check("report follows all three", [x["queryRef"] for x in grid] == ["Sales Orders.Region", "Sales.Name"],
              str([x["queryRef"] for x in grid]))
        check("nothing left broken", p.connector.validate_tmdl_syntax() == []
              and p.connector.validate_report_bindings() == [])
        check("case-only rename is allowed", p.connector.rename_table_in_files("Sales", "SALES").success)


class _FailingReplace:
    """os.replace that fails like a locked file: for one destination, or (``after``) for
    every call once that many have gone through."""

    def __init__(self, locked=None, after=None):
        self.locked = locked
        self.after = after
        self.calls = 0
        self.real = os.replace

    def __call__(self, src, dst, *args, **kwargs):
        self.calls += 1
        if (self.locked and str(dst).endswith(self.locked)) or (self.after is not None and self.calls > self.after):
            raise PermissionError(13, "The process cannot access the file", str(dst))
        return self.real(src, dst, *args, **kwargs)


def test_failures_change_nothing():
    print("\n== a failure while reading, validating or writing leaves the project untouched ==")
    with tempfile.TemporaryDirectory() as tmp:
        p = build(tmp)
        patch = _FailingReplace(locked="_Measures.tmdl")     # one file cannot be replaced
        safe_io.os.replace = patch
        try:
            r = p.connector.rename_table_in_files("Sales", "Orders")
        finally:
            safe_io.os.replace = patch.real
        check("several files had already been replaced when the write failed", patch.calls > 3, str(patch.calls))
        refused("write fails part-way", p, r, "PermissionError")
        check("no temp files left behind", not list(p.root.rglob("*.tmp")), str(list(p.root.rglob("*.tmp"))))
        check("the same rename works once the file is writable", p.connector.rename_table_in_files("Sales", "Orders").success)
    with tempfile.TemporaryDirectory() as tmp:
        p = build(tmp)
        patch = _FailingReplace(after=3)                     # nothing can be written any more, restores included
        safe_io.os.replace = patch
        try:
            r = p.connector.rename_table_in_files("Sales", "Orders")
        finally:
            safe_io.os.replace = patch.real
        now = fx.snapshot(p.root)
        stuck = sorted(k for k in p.before if p.before[k] != now[k])
        check("when even the restore fails, the result does not claim nothing changed",
              r.success is False and "could not be put back" in r.message and "Nothing was changed" not in r.message,
              r.message)
        check("it names exactly the files left changed", len(stuck) == 3 and all(k.split("/", 1)[1] in r.message or k in r.message for k in stuck),
              f"{stuck} vs {r.message}")
        check("undo can finish the restore once writing works again",
              p.connector.rollback_changes() is True and fx.snapshot(p.root) == p.before)
    with tempfile.TemporaryDirectory() as tmp:
        p = build(tmp)
        bad = p.definition / "tables" / "Legacy.tmdl"
        bad.write_bytes(b"table Legacy\n\tcolumn Caf\xe9\n")       # not UTF-8
        p.before = fx.snapshot(p.root)
        refused("a model file that is not UTF-8", p, p.connector.rename_table_in_files("Sales", "Orders"), "Unicode")
    with tempfile.TemporaryDirectory() as tmp:
        p = build(tmp)
        fx.write(p.visual_path("overview", "card"), '{"name": "card", "visual": {"visualType": "card",}}')
        p.before = fx.snapshot(p.root)
        refused("a report file that is not valid JSON", p, p.connector.rename_measure_in_files("Revenue", "Total"),
                "card/visual.json", "not valid JSON")
    with tempfile.TemporaryDirectory() as tmp:
        p = build(tmp)
        real = pbip_report.ReportRewriter.rewrite
        # Simulate a rewriter that misses the report: the staged model no longer matches it,
        # and the pre-commit check must refuse rather than write a half rename.
        pbip_report.ReportRewriter.rewrite = lambda self, text, label: pbip_report.RewriteResult(text=text)
        try:
            r = p.connector.rename_measure_in_files("Revenue", "Total Revenue")
        finally:
            pbip_report.ReportRewriter.rewrite = real
        refused("result would leave a broken binding", p, r, "would not be consistent", "'_measures'[revenue]")
        check("the broken reference is listed as a validation error",
              [e.error_type for e in r.validation_errors] == ["BROKEN_REFERENCE"]
              and r.validation_errors[0].file_path.endswith("card/visual.json"), str(r.validation_errors))


def test_existing_problems_do_not_block():
    print("\n== problems that were already there neither block a rename nor are blamed on it ==")
    files = dict(fx.MODEL_FILES)
    files["tables/_Measures.tmdl"] = fx.MEASURES.replace(
        "\tpartition _Measures", "\tmeasure Broken = SUM(Sales[Gone]) + SUM(Ghost[X])\n\n\tpartition _Measures")
    vis = visuals()
    vis["overview/stale"] = fx.visual("stale", "card", {"Values": [
        fx.projection(fx.measure("_Measures", "Deleted Measure"), "_Measures.Deleted Measure"),
        fx.projection(fx.column("Old Table", "X"), "Old Table.X")]})
    with tempfile.TemporaryDirectory() as tmp:
        p = build(tmp, model=files, visuals=vis)
        check("the fixture really has broken references",
              len(p.connector.validate_report_bindings()) == 2 and len(p.connector.validate_tmdl_syntax()) == 2,
              str([e.message for e in p.connector.validate_tmdl_syntax()]))
        r = p.connector.rename_column_in_files("Sales", "Region", "Zone")
        check("unrelated rename succeeds", r.success, r.message)
        r = p.connector.rename_table_in_files("Sales", "Orders")
        check("renaming the table a broken DAX reference points at succeeds", r.success, r.message)
        check("the broken reference followed the table", "SUM(Orders[Gone]) + SUM(Ghost[X])" in p.tmdl("tables/_Measures.tmdl"))


def test_undo():
    print("\n== rollback_changes restores the bytes from before the last committed rename ==")
    with tempfile.TemporaryDirectory() as tmp:
        p = build(tmp, crlf=True)
        c = p.connector
        check("nothing to undo before any change", c.rollback_changes() is False)
        check("rename succeeds", c.rename_table_in_files("Sales", "Sales Data").success)
        check("project changed", fx.snapshot(p.root) != p.before)
        check("undo reports success", c.rollback_changes() is True)
        check("project is byte-identical to the start", fx.snapshot(p.root) == p.before)
        check("a second undo has nothing to do", c.rollback_changes() is False)
        check("a refused rename does not become the undo target",
              c.rename_table_in_files("Nope", "X").success is False and c.rollback_changes() is False)


def test_backups():
    print("\n== backups: opt-in, inside the project, project files only, unique, honest ==")
    with tempfile.TemporaryDirectory() as tmp:
        outer = Path(tmp) / "Documents"
        outer.mkdir()
        fx.write(outer / "unrelated" / "big.bin", "x" * 1000)
        fx.write(outer / "notes.txt", "not part of the project")
        p = build(str(outer), auto_backup=True)
        fx.write(p.model / ".pbi" / "cache.abf", "local data cache")
        c = p.connector
        r0 = c.rename_table_in_files("Nope", "X")
        check("a refused rename makes no backup", r0.backup_created is None and not (outer / ".pbip-backups").exists())
        r1 = c.rename_measure_in_files("Revenue", "Revenue A")
        r2 = c.rename_measure_in_files("Revenue A", "Revenue B")
        check("both renames succeed", r1.success and r2.success, r1.message + r2.message)
        b1, b2 = Path(r1.backup_created), Path(r2.backup_created)
        check("each call gets its own backup folder, even within one second", b1 != b2 and b1.is_dir() and b2.is_dir())
        check("backups live inside the project folder", b1.parent == outer / ".pbip-backups", str(b1))
        check("nothing was written beside the project folder", sorted(x.name for x in Path(tmp).iterdir()) == ["Documents"])
        names = sorted(x.name for x in b1.iterdir())
        check("a backup holds the project's own files only",
              names == ["Retail.Report", "Retail.SemanticModel", "Retail.pbip"], str(names))
        check("the local data cache is not copied", not (b1 / "Retail.SemanticModel" / ".pbi").exists())
        check("first backup holds the state before the first rename",
              "measure Revenue = " in fx.read(b1 / "Retail.SemanticModel/definition/tables/_Measures.tmdl"))
        check("second backup holds the state before the second",
              "measure 'Revenue A' = " in fx.read(b2 / "Retail.SemanticModel/definition/tables/_Measures.tmdl"))
        check("backups are kept out of source control", fx.read(outer / ".pbip-backups" / ".gitignore") == "*\n")
        check("a backup does not confuse project loading or renames",
              c.load_project(str(outer)) and c.rename_measure_in_files("Revenue B", "Revenue").success)

        real = shutil.copytree
        before = fx.snapshot(p.root)

        def no_space(*args, **kwargs):
            raise OSError(28, "No space left on device")
        shutil.copytree = no_space
        try:
            r = c.rename_measure_in_files("Revenue", "Revenue Z")
        finally:
            shutil.copytree = real
        check("a failed backup stops the rename and says so",
              r.success is False and "backup failed" in r.message and "No space left" in r.message, r.message)
        check("and the project is untouched", fx.snapshot(p.root) == before)
    with tempfile.TemporaryDirectory() as tmp:
        p = build(tmp)                                  # auto_backup=False
        r = p.connector.rename_measure_in_files("Revenue", "R2")
        check("no backup unless asked for", r.success and r.backup_created is None
              and not (p.root / ".pbip-backups").exists())


def test_project_discovery():
    print("\n== the .pbip decides which report and model are loaded ==")
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        for name in ("Alpha", "Beta"):
            fx.build_project(tmp, name=name, model={"tables/T.tmdl": f"table {name}Table\n\tcolumn A\n"},
                             visuals={"overview/v": fx.visual("v", "card", {})}, load=False)
        c = PowerBIPBIPConnector(auto_backup=False)
        check("loading Beta.pbip", c.load_project(str(root / "Beta.pbip")))
        info = c.get_project_info()
        check("gets Beta's report", Path(info["report_folder"]).name == "Beta.Report", info["report_folder"])
        check("and Beta's model", Path(info["semantic_model_folder"]).name == "Beta.SemanticModel"
              and "BetaTable" in c.model_field_catalog(), str(c.model_field_catalog()))
        check("a folder with two projects is refused, with the reason",
              c.load_project(tmp) is False and "2 .pbip files" in (c.last_error or ""), str(c.last_error))
        check("a path that is not a project is refused", c.load_project(str(root / "nope.pbip")) is False
              and "not a .pbip file" in (c.last_error or ""))

        # definition.pbir is what binds a report to its model, whatever the folders are called
        fx.write_json(root / "Beta.Report" / "definition.pbir",
                      {"version": "4.0", "datasetReference": {"byPath": {"path": "../Alpha.SemanticModel"}}})
        c.load_project(str(root / "Beta.pbip"))
        check("byPath is followed", Path(c.get_project_info()["semantic_model_folder"]).name == "Alpha.SemanticModel")
        fx.write_json(root / "Beta.Report" / "definition.pbir", {"version": "4.0", "datasetReference": {
            "byConnection": {"connectionString": "Data Source=powerbi://example", "pbiModelDatabaseName": "x"}}})
        c.load_project(str(root / "Beta.pbip"))
        info = c.get_project_info()
        check("a report bound to a remote model gets no local model",
              info["semantic_model_folder"] is None and info["remote_model"] is True and info["tmdl_file_count"] == 0,
              str(info))
        r = c.rename_table_in_files("AlphaTable", "X")
        check("renames on it are refused with the reason", r.success is False and "not part of this project" in r.message,
              r.message)
        check("its bindings are reported as uncheckable, not as hundreds of missing tables",
              [e.error_type for e in c.validate_report_bindings()] == ["NO_LOCAL_MODEL"])
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        fx.write(root / "Solo.pbip", "{}")                           # no artifacts list
        fx.write(root / "Solo.SemanticModel" / "definition" / "tables" / "T.tmdl", "table T\n\tcolumn A\n")
        fx.write_json(root / "Solo.Report" / "definition" / "pages" / "p" / "page.json", {"name": "p"})
        c = PowerBIPBIPConnector(auto_backup=False)
        check("with no artifacts or definition.pbir, the folders sharing the .pbip's name are used",
              c.load_project(tmp) and Path(c.get_project_info()["report_folder"]).name == "Solo.Report"
              and "T" in c.model_field_catalog())
        fx.write(root / "Solo.SemanticModel" / "definition" / "tables" / "Late.tmdl", "table Late\n\tcolumn B\n")
        check("a file added after loading is seen by the next operation",
              c.rename_table_in_files("Late", "Later").success and "Later" in c.model_field_catalog())


if __name__ == "__main__":
    print("=" * 70)
    print("  PBIP RENAME: TRANSACTION AND REPORTING TESTS")
    print("=" * 70)
    for fn in (test_refusals, test_batch_is_one_transaction, test_failures_change_nothing,
               test_existing_problems_do_not_block, test_undo, test_backups, test_project_discovery):
        fn()
    print("\n" + "=" * 70)
    if _failures:
        print(f"  {len(_failures)} CHECK(S) FAILED: {', '.join(_failures)}")
        sys.exit(1)
    print("  ALL PBIP TRANSACTION CHECKS PASSED")
    print("=" * 70)
