"""
Tests for src/publish_stage.py: staging a PBIP report bound byConnection (publish in place)
and staging a model, without touching the source project or the network.
No Power BI required. Run: python tests/test_publish_stage.py
"""
import json
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))
import publish_stage as ps  # noqa: E402

_failures = []
MODEL_ID = "0A1B2C3D-4e5f-6071-8293-a4b5c6d7e8f9"   # made up; mixed case on purpose


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f": {detail}" if detail and not cond else ""))
    if not cond:
        _failures.append(name)


def write(path, text):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        f.write(text)


def snapshot(root):
    out = {}
    for r, _, fs in os.walk(root):
        for f in fs:
            p = os.path.join(r, f)
            with open(p, "rb") as fh:
                out[os.path.relpath(p, root)] = fh.read()
    return out


def make_project(root):
    rep = os.path.join(root, "Sales.Report")
    write(os.path.join(rep, "definition.pbir"), json.dumps({
        "$schema": "https://example/definitionProperties/2.0.0/schema.json", "version": "4.0",
        "datasetReference": {"byPath": {"path": "../Sales.SemanticModel"}}}))
    write(os.path.join(rep, ".platform"), json.dumps({"metadata": {"type": "Report", "displayName": "Sales"},
                                                      "config": {"logicalId": "x"}}))
    write(os.path.join(rep, "definition", "report.json"), "{}")
    write(os.path.join(rep, "definition", "pages", "p1", "page.json"), '{"name": "p1"}')
    write(os.path.join(rep, ".pbi", "localSettings.json"), "{}")
    write(os.path.join(rep, "CustomVisuals", "abc", "package.json"), "{}")
    mod = os.path.join(root, "Sales.SemanticModel")
    write(os.path.join(mod, "definition.pbism"), "{}")
    write(os.path.join(mod, "definition", "tables", "Sales.tmdl"),
          'table Sales\n\tpartition Sales = m\n\t\tsource = Csv.Document(File.Contents("C:\\\\data\\\\sales.csv"))\n')
    write(os.path.join(mod, "definition", "tables", "Dim.tmdl"),
          'table Dim\n\tpartition Dim = m\n\t\tsource = #table({"a"}, {{1}})\n')
    write(os.path.join(mod, ".pbi", "cache.abf"), "binary")
    return rep, mod


def test_connection_string():
    print("\n== connection string ==")
    cs = ps.build_connection_string("My WS", "Sales Model", MODEL_ID)
    check("format", cs == ('Data Source="powerbi://api.powerbi.com/v1.0/myorg/My WS";initial catalog="Sales Model";'
                           f"integrated security=ClaimsToken;semanticmodelid={MODEL_ID.lower()}"), cs)
    tricky = ps.build_connection_string('A;B "x"', "M;Data Source=evil", MODEL_ID)
    check("quotes are doubled and ; stays inside the quoted value",
          'myorg/A;B ""x"""' in tricky and 'initial catalog="M;Data Source=evil"' in tricky, tricky)
    for bad in ("not-a-guid", "", None):
        try:
            ps.build_connection_string("W", "M", bad)
            check(f"rejects model id {bad!r}", False)
        except ps.StageError:
            check(f"rejects model id {bad!r}", True)
    try:
        ps.build_connection_string("W\nX", "M", MODEL_ID)
        check("rejects control characters", False)
    except ps.StageError:
        check("rejects control characters", True)


def test_stage_report():
    print("\n== stage report ==")
    with tempfile.TemporaryDirectory() as d:
        rep, _ = make_project(os.path.join(d, "proj"))
        before = snapshot(os.path.join(d, "proj"))
        stage = os.path.join(d, "stage")
        res = ps.stage_report(rep, stage, workspace="WS", model_name="Sales", model_id=MODEL_ID,
                              display_name="Sales Live")
        target = res["staged_path"]
        check("staged folder is named after the display name", target.endswith("Sales Live.Report"), target)
        pbir = json.load(open(os.path.join(target, "definition.pbir"), encoding="utf-8"))
        check("bound byConnection", "byConnection" in pbir["datasetReference"] and "byPath" not in pbir["datasetReference"])
        check("schema and version inherited", pbir["$schema"].startswith("https://example/") and pbir["version"] == "4.0")
        check("connection string returned matches the file",
              pbir["datasetReference"]["byConnection"]["connectionString"] == res["connection_string"])
        plat = json.load(open(os.path.join(target, ".platform"), encoding="utf-8"))
        check("display name updated, other platform fields kept",
              plat["metadata"]["displayName"] == "Sales Live" and plat["config"]["logicalId"] == "x")
        check("local .pbi folder is not staged", not os.path.exists(os.path.join(target, ".pbi")))
        check("pages are staged", os.path.isfile(os.path.join(target, "definition", "pages", "p1", "page.json")))
        check("private custom visual is flagged", any("custom visuals" in w for w in res["warnings"]))
        check("import command names the item and folder",
              'fab import "WS.Workspace/Sales Live.Report"' in res["import_command"] and target in res["import_command"])
        check("the source project is untouched", snapshot(os.path.join(d, "proj")) == before)
        check("staged files are LF without BOM",
              b"\r\n" not in open(os.path.join(target, "definition.pbir"), "rb").read()
              and not open(os.path.join(target, "definition.pbir"), "rb").read().startswith(b"\xef\xbb\xbf"))

        try:
            ps.stage_report(rep, stage, workspace="WS", model_name="Sales", model_id=MODEL_ID, display_name="Sales Live")
            check("refuses to overwrite by default", False)
        except ps.StageError:
            check("refuses to overwrite by default", True)
        res2 = ps.stage_report(rep, stage, workspace="WS", model_name="Sales", model_id=MODEL_ID,
                               display_name="Sales Live", overwrite=True)
        check("overwrite restages", os.path.isdir(res2["staged_path"]))
        for bad_stage in (rep, os.path.join(rep, "inner")):
            try:
                ps.stage_report(rep, bad_stage, workspace="WS", model_name="Sales", model_id=MODEL_ID)
                check("refuses a stage folder inside the source", False)
            except ps.StageError:
                check("refuses a stage folder inside the source", True)
        try:
            ps.stage_report(os.path.join(d, "missing"), stage, workspace="WS", model_name="Sales", model_id=MODEL_ID)
            check("rejects a folder that is not a report", False)
        except ps.StageError:
            check("rejects a folder that is not a report", True)
        check("a failed request left the source untouched", snapshot(os.path.join(d, "proj")) == before)


def test_stage_model():
    print("\n== stage model ==")
    with tempfile.TemporaryDirectory() as d:
        _, mod = make_project(os.path.join(d, "proj"))
        before = snapshot(os.path.join(d, "proj"))
        res = ps.stage_model(mod, os.path.join(d, "stage"))
        check("cache is not staged", not os.path.exists(os.path.join(res["staged_path"], ".pbi")))
        check("local file source detected", res["local_sources"] == ["definition/tables/Sales.tmdl"], str(res["local_sources"]))
        check("warns that the service cannot refresh it", any("gateway" in w for w in res["warnings"]))
        check("warns that no data is uploaded", any("no data" in w for w in res["warnings"]))
        check("summary mentions the command", "fab import" in ps.describe(res))
        check("the source project is untouched", snapshot(os.path.join(d, "proj")) == before)


if __name__ == "__main__":
    for fn in (test_connection_string, test_stage_report, test_stage_model):
        fn()
    print()
    if _failures:
        print(f"FAILED: {len(_failures)} check(s): {_failures}")
        sys.exit(1)
    print("ALL PUBLISH STAGE TESTS PASSED")
