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


if __name__ == "__main__":
    print("=" * 70)
    print("  PBIR REPORT AUTHORING TESTS")
    print("=" * 70)
    test_emit_parse_symmetry()
    test_catalog_and_authoring()
    test_report_validator()
    test_aggregation_and_native_ref()
    print("\n" + "=" * 70)
    if _failures:
        print(f"  {len(_failures)} CHECK(S) FAILED: {', '.join(_failures)}")
        sys.exit(1)
    print("  ALL PBIR AUTHORING CHECKS PASSED")
    print("=" * 70)
