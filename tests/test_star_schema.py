"""
Tests for the star-schema auditor: table classification by relationship topology and every
warehouse best-practice finding, on a synthetic model. No Power BI required.
Run: python tests/test_star_schema.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))
import star_schema  # noqa: E402

_failures = []


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f": {detail}" if detail and not cond else ""))
    if not cond:
        _failures.append(name)


def _cols(*specs):
    out = []
    for s in specs:
        c = {"name": s[0], "data_type": s[1] if len(s) > 1 else "string"}
        if len(s) > 2:
            c.update(s[2])
        out.append(c)
    return out


MODEL = {
    "tables": [
        {"name": "Sales",
         "columns": _cols(("CustomerKey", "int64"), ("ProductKey", "int64"),
                          ("OrderDate", "dateTime"), ("Amount", "double"),
                          ("Status", "string"), ("Channel", "string"), ("Notes", "string")),
         "measures": [{"name": "Total Sales", "expression": "SUM(Sales[Amount])"}]},
        {"name": "Returns",
         "columns": _cols(("ProductKey", "int64"), ("ReturnDate", "dateTime"), ("Qty", "int64")),
         "measures": []},
        {"name": "Customer", "columns": _cols(("CustomerKey", "int64"), ("Name",), ("RegionKey", "int64")), "measures": []},
        {"name": "Region", "columns": _cols(("RegionKey", "int64"), ("Region",)), "measures": []},
        {"name": "Product", "columns": _cols(("ProductKey", "int64"), ("Product",)), "measures": []},
        {"name": "Date", "columns": _cols(("Date", "dateTime", {"is_key": True}), ("Year", "int64")), "measures": []},
        {"name": "BridgeCustSeg", "columns": _cols(("CustomerKey", "int64"), ("SegmentKey", "int64")), "measures": []},
        {"name": "Segment", "columns": _cols(("SegmentKey", "int64"), ("Segment",)), "measures": []},
        {"name": "Params", "columns": _cols(("Value", "double"), ("Label",)), "measures": []},
    ],
    "relationships": [
        {"from_table": "Sales", "from_column": "CustomerKey", "to_table": "Customer", "to_column": "CustomerKey", "is_active": True},
        {"from_table": "Sales", "from_column": "ProductKey", "to_table": "Product", "to_column": "ProductKey", "is_active": True, "cross_filter": "BothDirections"},
        {"from_table": "Sales", "from_column": "OrderDate", "to_table": "Date", "to_column": "Date", "is_active": True},
        {"from_table": "Customer", "from_column": "RegionKey", "to_table": "Region", "to_column": "RegionKey", "is_active": True},
        {"from_table": "Returns", "from_column": "ProductKey", "to_table": "Product", "to_column": "ProductKey", "is_active": True},
        {"from_table": "Returns", "from_column": "ReturnDate", "to_table": "Date", "to_column": "Date", "is_active": False},
        {"from_table": "Returns", "from_column": "ProductKey", "to_table": "Sales", "to_column": "ProductKey", "is_active": True},
        {"from_table": "BridgeCustSeg", "from_column": "CustomerKey", "to_table": "Customer", "to_column": "CustomerKey", "is_active": True},
        {"from_table": "BridgeCustSeg", "from_column": "SegmentKey", "to_table": "Segment", "to_column": "SegmentKey", "is_active": True},
        {"from_table": "Sales", "from_column": "CustomerKey", "to_table": "Segment", "to_column": "SegmentKey",
         "is_active": True, "from_cardinality": "many", "to_cardinality": "many"},
    ],
}


def test_classification():
    print("\n== table classification ==")
    res = star_schema.audit_star_schema(MODEL)
    c = {n: v["class"] for n, v in res["classification"].items()}
    check("Sales is fact (both sides, has measures)", c["Sales"] == "fact", str(c))
    check("Returns is fact", c["Returns"] == "fact", c["Returns"])
    check("Customer is dimension", c["Customer"] == "dimension", c["Customer"])
    check("Region is dimension", c["Region"] == "dimension")
    check("Date is date_dimension", c["Date"] == "date_dimension", c["Date"])
    check("Date marked as date table", res["classification"]["Date"]["marked_date_table"] is True)
    check("BridgeCustSeg is bridge", c["BridgeCustSeg"] == "bridge", c["BridgeCustSeg"])
    check("Params is disconnected", c["Params"] == "disconnected")


def test_findings():
    print("\n== best-practice findings ==")
    res = star_schema.audit_star_schema(MODEL)
    codes = {f["code"] for f in res["findings"]}
    for expected in ("SS_BIDIRECTIONAL", "SS_MANY_TO_MANY", "SS_FACT_TO_FACT", "SS_SNOWFLAKE",
                     "SS_ROLE_PLAYING", "SS_FACT_NO_MEASURES", "SS_TEXT_ON_FACT", "SS_DISCONNECTED"):
        check(f"{expected} detected", expected in codes, str(codes))
    check("no missing-date-dim (model has one)", "SS_NO_DATE_DIM" not in codes)
    check("no unmarked-date (Date is marked)", "SS_DATE_NOT_MARKED" not in codes)
    ftf = next(f for f in res["findings"] if f["code"] == "SS_FACT_TO_FACT")
    check("fact-to-fact names the pair", "Returns" in ftf["table"] and "Sales" in ftf["table"], ftf["table"])
    check("every finding has a recommendation", all(f["recommendation"] for f in res["findings"]))


def test_negative_cases():
    print("\n== negative cases + summary ==")
    # Clean two-table star: no warnings at all
    clean = {
        "tables": [
            {"name": "Sales", "columns": _cols(("DateKey", "int64"), ("Amount", "double")),
             "measures": [{"name": "Total", "expression": "SUM(Sales[Amount])"}]},
            {"name": "Date", "columns": _cols(("Date", "dateTime", {"is_key": True})), "measures": []},
        ],
        "relationships": [
            {"from_table": "Sales", "from_column": "DateKey", "to_table": "Date", "to_column": "Date", "is_active": True},
        ],
    }
    res = star_schema.audit_star_schema(clean)
    check("clean star scores A", res["summary"]["grade"] == "A", str(res["summary"]))
    check("clean star has no warnings", res["summary"]["findings_by_severity"]["warning"] == 0,
          str(res["findings"]))
    # Fact-only model: missing date dimension flagged
    nodate = {
        "tables": [
            {"name": "Fact", "columns": _cols(("K", "int64")), "measures": [{"name": "M", "expression": "1"}]},
            {"name": "Dim", "columns": _cols(("K", "int64"), ("Attr",)), "measures": []},
        ],
        "relationships": [{"from_table": "Fact", "from_column": "K", "to_table": "Dim", "to_column": "K", "is_active": True}],
    }
    res2 = star_schema.audit_star_schema(nodate)
    check("missing date dim flagged", any(f["code"] == "SS_NO_DATE_DIM" for f in res2["findings"]))
    # Unmarked date dim
    unmarked = {
        "tables": [
            {"name": "Fact", "columns": _cols(("D", "dateTime")), "measures": [{"name": "M", "expression": "1"}]},
            {"name": "Calendar", "columns": _cols(("Date", "dateTime")), "measures": []},
        ],
        "relationships": [{"from_table": "Fact", "from_column": "D", "to_table": "Calendar", "to_column": "Date", "is_active": True}],
    }
    res3 = star_schema.audit_star_schema(unmarked)
    check("unmarked date dim flagged", any(f["code"] == "SS_DATE_NOT_MARKED" for f in res3["findings"]),
          str(res3["findings"]))
    check("summary counts classes", res3["summary"]["tables_by_class"].get("date_dimension") == 1)
    # Empty model degrades gracefully
    res4 = star_schema.audit_star_schema({})
    check("empty model ok", res4["summary"]["score"] == 100 and res4["findings"] == [])


if __name__ == "__main__":
    print("=" * 70)
    print("  STAR SCHEMA AUDIT TESTS")
    print("=" * 70)
    test_classification()
    test_findings()
    test_negative_cases()
    print("\n" + "=" * 70)
    if _failures:
        print(f"  {len(_failures)} CHECK(S) FAILED: {', '.join(_failures)}")
        sys.exit(1)
    print("  ALL STAR SCHEMA CHECKS PASSED")
    print("=" * 70)
