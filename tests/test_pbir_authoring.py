"""
PBIR report-authoring tests: emit/parse symmetry, end-to-end add_page/add_visual/bind_fields
on a temp PBIR-Enhanced project, and the field-existence validator. No Power BI required.
Run: python tests/test_pbir_authoring.py
"""
import json
import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))
import pbir_authoring  # noqa: E402
from powerbi_pbip_connector import PowerBIPBIPConnector  # noqa: E402

_failures = []


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f": {detail}" if detail and not cond else ""))
    if not cond:
        _failures.append(name)


def test_emit_parse_symmetry():
    print("\n== emit (build_visual) is the inverse of parse (_walk_report_refs) ==")
    vj = pbir_authoring.build_visual(
        "v1", "barChart", {"x": 0, "y": 0, "width": 400, "height": 300},
        {"Category": "Sales.Region", "Y": "Sales.Total Sales"})
    refs = set()
    PowerBIPBIPConnector._walk_report_refs(vj, refs)
    check("emitted column parses back", ("Sales", "Region") in refs, str(refs))
    check("emitted measure parses back", ("Sales", "Total Sales") in refs, str(refs))
    check("visualType set", vj["visual"]["visualType"] == "barChart")
    check("position has required keys", all(k in vj["position"] for k in ("x", "y", "width", "height")))
    check("first projection active", vj["visual"]["query"]["queryState"]["Category"]["projections"][0].get("active") is True)


SALES_TMDL = """table Sales
\tcolumn Region
\t\tdataType: string
\tcolumn Amount
\t\tdataType: double
\tmeasure 'Total Sales' = SUM(Sales[Amount])
"""


def _build_project(tmp):
    root = Path(tmp)
    (root / "proj.pbip").write_text("{}", encoding="utf-8")
    sm = root / "proj.SemanticModel" / "definition" / "tables"
    sm.mkdir(parents=True)
    (sm / "Sales.tmdl").write_text(SALES_TMDL, encoding="utf-8")
    pages = root / "proj.Report" / "definition" / "pages"
    (pages / "pageA").mkdir(parents=True)
    (root / "proj.Report" / "definition" / "report.json").write_text(json.dumps({"$schema": "x"}), encoding="utf-8")
    (pages / "pageA" / "page.json").write_text(json.dumps({
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/page/2.0.0/schema.json",
        "name": "pageA", "displayName": "Existing"}), encoding="utf-8")
    proj = PowerBIPBIPConnector._parse_pbip_project(root / "proj.pbip")
    c = PowerBIPBIPConnector(auto_backup=False)
    c.current_project = proj
    return c


def test_catalog_and_authoring():
    print("\n== field catalog + add_page + add_visual + bind_fields (temp project) ==")
    with tempfile.TemporaryDirectory() as tmp:
        c = _build_project(tmp)
        check("project is PBIR-Enhanced", c.current_project.is_pbir_enhanced is True)
        cat = c.model_field_catalog()
        check("catalog has Sales w/ fields", "Sales" in cat and {"Region", "Amount", "Total Sales"} <= cat["Sales"], str(cat))

        page = c.add_page("Overview", set_active=True)
        check("add_page ok", page.get("success") is True, str(page))
        page_dir = c._find_page_folder("Overview")
        check("page folder created", page_dir is not None and (page_dir / "page.json").exists())
        # schema-from-sibling: new page got the existing page's schema URL
        new_pj = json.loads((page_dir / "page.json").read_text(encoding="utf-8"))
        check("schema inherited from sibling", "page/2.0.0" in new_pj["$schema"], new_pj["$schema"])
        meta = json.loads((c._pages_folder() / "pages.json").read_text(encoding="utf-8"))
        check("registered in pages.json", page["page_name"] in meta["pageOrder"] and meta["activePageName"] == page["page_name"])

        vis = c.add_visual("Overview", "barChart",
                           position={"x": 10, "y": 10, "width": 500, "height": 320},
                           fields_by_role={"Category": "Sales.Region", "Y": "Sales.Total Sales"})
        check("add_visual ok", vis.get("success") is True, str(vis))
        vpath = Path(vis["path"])
        check("visual.json written", vpath.exists())
        vrefs = set()
        PowerBIPBIPConnector._walk_report_refs(json.loads(vpath.read_text(encoding="utf-8")), vrefs)
        check("visual bound to real fields", {("Sales", "Region"), ("Sales", "Total Sales")} <= vrefs, str(vrefs))

        bad = c.add_visual("Overview", "card", fields_by_role={"Values": "Sales.DoesNotExist"})
        check("missing field blocked", bad.get("success") is False and "Sales[DoesNotExist]" in bad.get("missing_fields", []), str(bad))

        bound = c.bind_fields("Overview", vis["visual_name"], {"Series": "Sales.Region"}, mode="add")
        check("bind_fields ok", bound.get("success") is True, str(bound))
        vrefs2 = set()
        PowerBIPBIPConnector._walk_report_refs(json.loads(vpath.read_text(encoding="utf-8")), vrefs2)
        check("series projection added", ("Sales", "Region") in vrefs2)


def test_report_validator():
    print("\n== validate_report_bindings catches missing fields ==")
    with tempfile.TemporaryDirectory() as tmp:
        c = _build_project(tmp)
        c.add_page("P", set_active=True)
        # valid visual
        c.add_visual("P", "card", fields_by_role={"Values": "Sales.Total Sales"})
        check("clean when all fields exist", c.validate_report_bindings() == [])
        # force a bad binding past validation
        c.add_visual("P", "card", fields_by_role={"Values": "Sales.Ghost"}, skip_validation=True)
        errs = c.validate_report_bindings()
        check("missing field detected", any(e.error_type == "MISSING_FIELD" and "Ghost" in e.message for e in errs), str([e.message for e in errs]))


def _projection(vj, role):
    return vj["visual"]["query"]["queryState"][role]["projections"][0]


def test_aggregation_and_native_ref():
    print("\n== Desktop-fidelity: nativeQueryRef + aggregated columns + no phantom refs ==")
    with tempfile.TemporaryDirectory() as tmp:
        c = _build_project(tmp)
        c.add_page("Agg", set_active=True)

        # Numeric COLUMN (Amount/double) on an aggregating well -> Sum(...) wrapper.
        vis = c.add_visual("Agg", "columnChart",
                           fields_by_role={"Category": "Sales.Region", "Y": "Sales.Amount"})
        vj = json.loads(Path(vis["path"]).read_text(encoding="utf-8"))
        y = _projection(vj, "Y")
        cat = _projection(vj, "Category")
        check("numeric column on Y is wrapped in Aggregation", "Aggregation" in y["field"], str(y["field"]))
        check("aggregation is Sum (Function 0)", y["field"].get("Aggregation", {}).get("Function") == 0)
        check("aggregated queryRef is Sum(Table.Field)", y["queryRef"] == "Sum(Sales.Amount)", y["queryRef"])
        check("aggregated nativeQueryRef is bare column", y["nativeQueryRef"] == "Amount", y.get("nativeQueryRef"))
        check("grouping column NOT aggregated", "Column" in cat["field"] and "Aggregation" not in cat["field"])
        check("grouping nativeQueryRef present", cat.get("nativeQueryRef") == "Region", cat.get("nativeQueryRef"))
        # The Sum(Sales.Amount) queryRef must NOT create a phantom ("Sum(Sales","Amount)") ref.
        errs = c.validate_report_bindings()
        check("aggregated queryRef makes no phantom missing refs", errs == [], str([e.message for e in errs]))

        # Non-numeric COLUMN (Region/string) on a value well -> CountNonNull (Function 5).
        v2 = c.add_visual("Agg", "card", fields_by_role={"Values": "Sales.Region"})
        vj2 = json.loads(Path(v2["path"]).read_text(encoding="utf-8"))
        val = _projection(vj2, "Values")
        check("text column on value well is CountNonNull", val["field"].get("Aggregation", {}).get("Function") == 5, str(val["field"]))
        check("CountNonNull queryRef form", val["queryRef"] == "CountNonNull(Sales.Region)", val["queryRef"])

        # Explicit MEASURE on a value well -> bare Measure, no aggregation.
        v3 = c.add_visual("Agg", "card", fields_by_role={"Values": "Sales.Total Sales"})
        vj3 = json.loads(Path(v3["path"]).read_text(encoding="utf-8"))
        m = _projection(vj3, "Values")
        check("measure stays a bare Measure", "Measure" in m["field"] and "Aggregation" not in m["field"], str(m["field"]))
        check("measure queryRef is Table.Field", m["queryRef"] == "Sales.Total Sales", m["queryRef"])


def test_aggregation_depends_on_visual_type():
    print("\n== aggregation is decided by visual type AND role (slicers, tables, grouping wells never) ==")
    check("card Values summarize", pbir_authoring.aggregates("card", "Values") is True)
    check("chart Y summarizes", pbir_authoring.aggregates("clusteredColumnChart", "Y") is True)
    check("slicer Values never summarize", pbir_authoring.aggregates("slicer", "Values") is False)
    check("table Values never summarize", pbir_authoring.aggregates("tableEx", "Values") is False)
    check("grouping wells never summarize on any visual",
          not any(pbir_authoring.aggregates(vt, role) for vt in ("barChart", "pivotTable", "card", None)
                  for role in ("Category", "Series", "Legend", "Rows", "Columns")))
    with tempfile.TemporaryDirectory() as tmp:
        c = _build_project(tmp)
        c.add_page("T", set_active=True)
        for vt in ("slicer", "tableEx"):
            vis = c.add_visual("T", vt, fields_by_role={"Values": ["Sales.Region", "Sales.Amount"]})
            check(f"{vt}: created", vis.get("success") is True, str(vis))
            projs = json.loads(Path(vis["path"]).read_text(encoding="utf-8"))["visual"]["query"]["queryState"]["Values"]["projections"]
            check(f"{vt}: text and numeric columns stay plain columns",
                  [list(p["field"])[0] for p in projs] == ["Column", "Column"]
                  and [p["queryRef"] for p in projs] == ["Sales.Region", "Sales.Amount"], str(projs))
        card = c.add_visual("T", "card", fields_by_role={"Values": "Sales.Amount"})
        cj = json.loads(Path(card["path"]).read_text(encoding="utf-8"))
        check("card: the same role name still aggregates", "Aggregation" in _projection(cj, "Values")["field"])
        # bind_fields takes the decision from the type of the visual it edits
        slicer = c.add_visual("T", "slicer")
        c.bind_fields("T", slicer["visual_name"], {"Values": "Sales.Region"})
        sj = json.loads(Path(slicer["path"]).read_text(encoding="utf-8"))
        check("bind_fields on a slicer keeps the column plain", "Column" in _projection(sj, "Values")["field"],
              str(_projection(sj, "Values")))
        c.bind_fields("T", card["visual_name"], {"Values": "Sales.Region"}, mode="replace")
        cj = json.loads(Path(card["path"]).read_text(encoding="utf-8"))
        check("bind_fields on a card aggregates", _projection(cj, "Values")["queryRef"] == "CountNonNull(Sales.Region)",
              str(_projection(cj, "Values")))
    a = pbir_authoring.emit_projection("Sales", "Id", aggregation=pbir_authoring.AGG_DISTINCT_COUNT)
    b = pbir_authoring.emit_projection("Sales", "Id", aggregation=pbir_authoring.AGG_COUNT_NONNULL)
    check("distinct count and count of one column get different queryRefs", a["queryRef"] != b["queryRef"],
          f"{a['queryRef']} / {b['queryRef']}")
    check("split_table_field understands an escaped apostrophe",
          pbir_authoring.split_table_field("'O''Brien Sales'.Net Amount") == ("O'Brien Sales", "Net Amount"),
          str(pbir_authoring.split_table_field("'O''Brien Sales'.Net Amount")))


def test_authoring_guards():
    print("\n== authoring never writes outside the project or over a file it cannot read ==")
    with tempfile.TemporaryDirectory() as tmp, tempfile.TemporaryDirectory() as elsewhere:
        c = _build_project(tmp)
        outside = Path(elsewhere) / "page"
        outside.mkdir()
        (outside / "page.json").write_text(json.dumps({"name": "x", "displayName": "X"}), encoding="utf-8")
        for ref in (str(outside), "..\\..\\..\\" + outside.name, "../pageA/../pageA"):
            res = c.add_visual(ref, "card", fields_by_role={"Values": "Sales.Total Sales"})
            check(f"page reference {ref[-18:]!r} is not followed as a path", res.get("success") is False, str(res))
        check("nothing was created outside the project", [x.name for x in outside.iterdir()] == ["page.json"])
        vis = c.add_visual("pageA", "card", fields_by_role={"Values": "Sales.Total Sales"})
        check("the page's folder name still works", vis.get("success") is True, str(vis))
        bad = c.bind_fields("pageA", "..\\" + vis["visual_name"], {"Values": "Sales.Total Sales"})
        check("visual name with a path separator refused", bad.get("success") is False, str(bad))
        check("unknown bind mode refused", c.bind_fields("pageA", vis["visual_name"], {"Values": "Sales.Amount"},
                                                         mode="merge").get("success") is False)

        vpath = Path(vis["path"])
        data = json.loads(vpath.read_text(encoding="utf-8"))
        data["visual"]["title"] = "Caf" + chr(0xE9)
        vpath.write_bytes(json.dumps(data, indent=2, ensure_ascii=False).replace("\n", "\r\n").encode("utf-8"))
        c.bind_fields("pageA", vis["visual_name"], {"Values": "Sales.Amount"})
        raw = vpath.read_bytes()
        check("bind_fields keeps CRLF line endings", raw.count(b"\r\n") == raw.count(b"\n") and b"\r\n" in raw)
        check("bind_fields keeps non-ASCII text readable", ("Caf" + chr(0xE9)).encode("utf-8") in raw)

        pages_json = c._pages_folder() / "pages.json"
        pages_json.write_text('{"pageOrder": ["pageA", "pageB"], "activePageName": "pageA",}', encoding="utf-8")
        before = sorted(p.name for p in c._pages_folder().iterdir())
        res = c.add_page("Broken")
        check("add_page refuses when pages.json cannot be parsed", res.get("success") is False
              and "pages.json" in res["message"], str(res))
        check("and neither rewrites it nor leaves a page folder behind",
              pages_json.read_text(encoding="utf-8").endswith('"pageA",}')
              and sorted(p.name for p in c._pages_folder().iterdir()) == before)


def test_validator_knows_hierarchies_and_other_report_files():
    print("\n== validate_report_bindings: hierarchies count, page filters and bookmarks are checked ==")
    with tempfile.TemporaryDirectory() as tmp:
        c = _build_project(tmp)
        c.add_hierarchy("Sales", "Geo", ["Region"])
        c.add_page("H", set_active=True)
        level = {"HierarchyLevel": {"Expression": {"Hierarchy": {"Expression": {"SourceRef": {"Entity": "Sales"}},
                                                                 "Hierarchy": "Geo"}}, "Level": "Region"}}
        c.add_visual("H", "slicer", fields_by_role={"Values": [
            {"table": "Sales", "field": "Region"}]}, skip_validation=True)
        page_dir = c._find_page_folder("H")
        vdir = page_dir / "visuals" / "hier"
        vdir.mkdir(parents=True)
        (vdir / "visual.json").write_text(json.dumps({"name": "hier", "visual": {"visualType": "slicer", "query": {
            "queryState": {"Values": {"projections": [{"field": level, "queryRef": "Sales.Geo.Region"}]}}}}}),
            encoding="utf-8")
        check("a hierarchy level reference is not reported missing", c.validate_report_bindings() == [],
              str([e.message for e in c.validate_report_bindings()]))
        page = json.loads((page_dir / "page.json").read_text(encoding="utf-8"))
        page["filterConfig"] = {"filters": [{"name": "f", "field": {"Column": {
            "Expression": {"SourceRef": {"Entity": "Sales"}}, "Property": "Deleted Column"}}}]}
        (page_dir / "page.json").write_text(json.dumps(page), encoding="utf-8")
        bm = c._pages_folder().parent / "bookmarks"
        bm.mkdir()
        (bm / "b.bookmark.json").write_text(json.dumps({"name": "b", "explorationState": {"filters": {"byExpr": [
            {"expression": {"Column": {"Expression": {"SourceRef": {"Entity": "Gone Table"}}, "Property": "X"}}}]}}}),
            encoding="utf-8")
        errs = c.validate_report_bindings()
        check("a page-level filter on a missing column is found",
              any(e.error_type == "MISSING_FIELD" and "Deleted Column" in e.message and e.file_path.endswith("page.json")
                  for e in errs), str([e.message for e in errs]))
        check("a bookmark that names a missing table is found",
              any(e.error_type == "MISSING_TABLE" and "Gone Table" in e.message for e in errs),
              str([e.message for e in errs]))


def test_emit_projection_aggregation_unit():
    print("\n== emit_projection aggregation shape + queryRef (pure module) ==")
    cases = {
        pbir_authoring.AGG_SUM: "Sum", pbir_authoring.AGG_AVERAGE: "Average",
        pbir_authoring.AGG_MIN: "Min", pbir_authoring.AGG_MAX: "Max",
        pbir_authoring.AGG_COUNT_NONNULL: "CountNonNull",
    }
    for fn, qname in cases.items():
        p = pbir_authoring.emit_projection("Sales", "Amount", aggregation=fn)
        agg = p["field"].get("Aggregation", {})
        check(f"agg {qname}: Aggregation node + Function", agg.get("Function") == fn, str(p["field"]))
        check(f"agg {qname}: inner Column ref", agg.get("Expression", {}).get("Column", {}).get("Property") == "Amount")
        check(f"agg {qname}: queryRef form", p["queryRef"] == f"{qname}(Sales.Amount)", p["queryRef"])
        check(f"agg {qname}: nativeQueryRef bare", p["nativeQueryRef"] == "Amount")
    # parser must still recover the underlying column from an aggregated projection
    refs = set()
    PowerBIPBIPConnector._walk_report_refs(
        pbir_authoring.emit_projection("Sales", "Amount", aggregation=pbir_authoring.AGG_SUM), refs)
    check("aggregated column parses back", ("Sales", "Amount") in refs, str(refs))


if __name__ == "__main__":
    print("=" * 70)
    print("  PBIR REPORT AUTHORING TESTS")
    print("=" * 70)
    test_emit_parse_symmetry()
    test_catalog_and_authoring()
    test_report_validator()
    test_aggregation_and_native_ref()
    test_aggregation_depends_on_visual_type()
    test_authoring_guards()
    test_validator_knows_hierarchies_and_other_report_files()
    test_emit_projection_aggregation_unit()
    print("\n" + "=" * 70)
    if _failures:
        print(f"  {len(_failures)} CHECK(S) FAILED: {', '.join(_failures)}")
        sys.exit(1)
    print("  ALL PBIR AUTHORING CHECKS PASSED")
    print("=" * 70)
