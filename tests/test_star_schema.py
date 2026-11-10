"""
Tests for the star-schema auditor: table classification by relationship topology, every
warehouse best-practice finding, and the score, on synthetic models. No Power BI required.
Run: python tests/test_star_schema.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))
import star_schema  # noqa: E402

_failures = []
ROW_NUMBER = "RowNumber-2662979B-1795-4F74-8F37-6A1BA8059B61"


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


def _rel(ft, fc, tt, tc, **kw):
    r = {"from_table": ft, "from_column": fc, "to_table": tt, "to_column": tc, "is_active": True}
    r.update(kw)
    return r


def _audit(model):
    res = star_schema.audit_star_schema(model)
    return ({n: v["class"] for n, v in res["classification"].items()},
            [f["code"] for f in res["findings"]], res["summary"])


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

FACT = {"name": "Sales", "columns": _cols(("DateKey", "int64"), ("Amount", "double")),
        "measures": [{"name": "Total", "expression": "SUM(Sales[Amount])"}]}
DATE = {"name": "Date", "columns": _cols(("Date", "dateTime", {"is_key": True})), "measures": []}


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
    check("classification keeps its keys", set(res["classification"]["Sales"]) >=
          {"class", "many_side_of", "one_side_of", "columns", "measures", "marked_date_table"})


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
    sev = [star_schema.SEVERITY_ORDER[f["severity"]] for f in res["findings"]]
    check("findings are ordered by severity", sev == sorted(sev, reverse=True))


def test_negative_cases():
    print("\n== negative cases + summary ==")
    # Clean two-table star: no warnings at all
    clean = {"tables": [FACT, DATE],
             "relationships": [_rel("Sales", "DateKey", "Date", "Date")]}
    res = star_schema.audit_star_schema(clean)
    check("clean star scores 100 / A", res["summary"]["grade"] == "A" and res["summary"]["score"] == 100, str(res["summary"]))
    check("clean star has no findings", res["findings"] == [], str(res["findings"]))
    # Fact-only model: missing date dimension flagged
    nodate = {
        "tables": [
            {"name": "Fact", "columns": _cols(("K", "int64")), "measures": [{"name": "M", "expression": "1"}]},
            {"name": "Dim", "columns": _cols(("K", "int64"), ("Attr",)), "measures": []},
        ],
        "relationships": [_rel("Fact", "K", "Dim", "K")],
    }
    res2 = star_schema.audit_star_schema(nodate)
    check("missing date dim flagged", any(f["code"] == "SS_NO_DATE_DIM" for f in res2["findings"]))
    # Unmarked date dim
    unmarked = {
        "tables": [
            {"name": "Fact", "columns": _cols(("D", "dateTime")), "measures": [{"name": "M", "expression": "1"}]},
            {"name": "Calendar", "columns": _cols(("Date", "dateTime")), "measures": []},
        ],
        "relationships": [_rel("Fact", "D", "Calendar", "Date")],
    }
    res3 = star_schema.audit_star_schema(unmarked)
    check("unmarked date dim flagged", any(f["code"] == "SS_DATE_NOT_MARKED" for f in res3["findings"]),
          str(res3["findings"]))
    check("summary counts classes", res3["summary"]["tables_by_class"].get("date_dimension") == 1)


def test_measure_table_and_bridge_hint():
    print("\n== live-UAT regressions: measure table + name-hinted bridge ==")
    model = {
        "tables": [
            {"name": "_Measures", "columns": _cols(("Col1",)),
             "measures": [{"name": "Total Revenue", "expression": "SUM(fact_sales[Revenue])"}]},
            {"name": "fact_sales", "columns": _cols(("DateKey", "int64"), ("Revenue", "double")), "measures": []},
            {"name": "dim_date", "columns": _cols(("Date", "dateTime", {"is_key": True})), "measures": []},
            {"name": "bridge_line_account",
             "columns": _cols(("LineKey", "int64"), ("AccountKey", "int64"), ("Weight", "double")), "measures": []},
            {"name": "dim_account", "columns": _cols(("AccountKey", "int64"), ("Account",)), "measures": []},
        ],
        "relationships": [
            _rel("fact_sales", "DateKey", "dim_date", "Date"),
            _rel("bridge_line_account", "AccountKey", "dim_account", "AccountKey"),
        ],
    }
    res = star_schema.audit_star_schema(model)
    c = {n: v["class"] for n, v in res["classification"].items()}
    codes = {f["code"] for f in res["findings"]}
    check("_Measures is measure_table", c["_Measures"] == "measure_table", str(c))
    check("measure table NOT flagged disconnected", "SS_DISCONNECTED" not in codes, str(codes))
    check("bridge by name hint (1 rel, extra attr)", c["bridge_line_account"] == "bridge", c["bridge_line_account"])
    check("facts not flagged measure-less when measure table exists",
          "SS_FACT_NO_MEASURES" not in codes, str(codes))
    check("a measure table does not lower the score", res["summary"]["score"] == 100, str(res["summary"]))


def test_score_is_not_generous():
    print("\n== defect 14: no star, no A ==")
    wide = _cols(("A",), ("B",), ("C",), ("D", "double"))
    classes, codes, summary = _audit({"tables": [{"name": "Flat", "columns": wide, "measures": []}]})
    check("single flat table: explicit error finding", "SS_NO_RELATIONSHIPS" in codes and summary["findings_by_severity"]["error"] == 1,
          str(codes))
    check("single flat table: capped at D", summary["score"] <= 40 and summary["grade"] in ("D", "F"), str(summary))
    flat = star_schema.audit_star_schema({"tables": [{"name": "Flat", "columns": wide, "measures": []}]})
    msg = next(f for f in flat["findings"] if f["code"] == "SS_NO_RELATIONSHIPS")["message"]
    check("the message says what the model is", "single flat table" in msg, msg)

    five = {"tables": [{"name": f"T{i}", "columns": _cols(("A",), ("B",)), "measures": []} for i in range(5)]}
    classes, codes, summary = _audit(five)
    check("five unrelated tables: not an A, not a B", summary["grade"] in ("D", "F") and summary["score"] <= 40, str(summary))
    check("five unrelated tables: message counts them",
          "5 tables with no relationship" in next(f for f in star_schema.audit_star_schema(five)["findings"]
                                                  if f["code"] == "SS_NO_RELATIONSHIPS")["message"])

    classes, codes, summary = _audit({})
    check("empty model is not 100", summary["score"] == 0 and summary["grade"] == "F" and codes == ["SS_EMPTY_MODEL"], str(summary))
    classes, codes, summary = _audit({"tables": [{"name": "P", "columns": _cols(("Value", "double")), "measures": []}]})
    check("only a parameter table: nothing to audit, not 100", summary["score"] == 0 and codes == ["SS_EMPTY_MODEL"], str(summary))

    partly = {"tables": [FACT, DATE,
                         {"name": "Loose1", "columns": _cols(("A",), ("B",)), "measures": []},
                         {"name": "Loose2", "columns": _cols(("A",), ("B",)), "measures": []}],
              "relationships": [_rel("Sales", "DateKey", "Date", "Date")]}
    classes, codes, summary = _audit(partly)
    check("unrelated data tables lower a real star's score in proportion",
          summary["score"] == 100 - 4 - 20 and summary["score_breakdown"]["coverage_penalty"] == 20, str(summary))
    hidden = {"tables": [FACT, DATE, {"name": "Helper", "is_hidden": True, "columns": _cols(("A",), ("B",)), "measures": []}],
              "relationships": [_rel("Sales", "DateKey", "Date", "Date")]}
    check("a hidden stand-alone helper costs no coverage", _audit(hidden)[2]["score_breakdown"]["coverage_penalty"] == 0)

    many_bad = {"tables": [FACT, DATE] + [{"name": f"D{i}", "columns": _cols(("K", "int64")), "measures": []} for i in range(20)],
                "relationships": [_rel("Sales", "DateKey", "Date", "Date")]
                + [_rel("Sales", "K", f"D{i}", "K", cross_filter="BothDirections", from_cardinality="Many",
                        to_cardinality="Many") for i in range(20)]}
    classes, codes, summary = _audit(many_bad)
    check("the score never goes below 0", summary["score"] == 0 and summary["grade"] == "F", str(summary))
    check("grades follow the documented thresholds", _audit({"tables": [FACT, DATE], "relationships": [
        _rel("Sales", "DateKey", "Date", "Date", cross_filter="both")]})[2]["score"] == 92)


def test_row_number_column_is_ignored():
    print("\n== defect 15: the engine's RowNumber column is not a column ==")
    rn = (ROW_NUMBER, "int64", {"is_hidden": True})

    def link(with_rn):
        cols = _cols(("CustomerKey", "int64"), ("SegmentKey", "int64"))
        return {"tables": [
            {"name": "CustSeg", "columns": (_cols(rn) if with_rn else []) + cols, "measures": []},
            {"name": "Customer", "columns": (_cols(rn) if with_rn else []) + _cols(("CustomerKey", "int64"), ("Name",)), "measures": []},
            {"name": "Segment", "columns": (_cols(rn) if with_rn else []) + _cols(("SegmentKey", "int64"), ("Segment",)), "measures": []}],
            "relationships": [_rel("CustSeg", "CustomerKey", "Customer", "CustomerKey"),
                              _rel("CustSeg", "SegmentKey", "Segment", "SegmentKey")]}
    plain, live = star_schema.audit_star_schema(link(False)), star_schema.audit_star_schema(link(True))
    check("thin all-key link table is a bridge", plain["classification"]["CustSeg"]["class"] == "bridge")
    check("...and still is with the RowNumber column present", live["classification"]["CustSeg"]["class"] == "bridge",
          live["classification"]["CustSeg"]["class"])
    check("column counts exclude RowNumber", live["classification"]["CustSeg"]["columns"] == 2)
    check("same findings with and without it", [f["code"] for f in plain["findings"]] == [f["code"] for f in live["findings"]])
    param = {"tables": [FACT, DATE, {"name": "Param", "columns": _cols(rn, ("Value", "double")), "measures": []}],
             "relationships": [_rel("Sales", "DateKey", "Date", "Date")]}
    check("a one-column parameter table stays unflagged", "SS_DISCONNECTED" not in _audit(param)[1], str(_audit(param)[1]))


def test_bridge_is_a_word():
    print("\n== defect 15: 'bridge' must be a word of the name ==")
    model = {"tables": [
        {"name": "_Measures", "columns": _cols(("c",)), "measures": [{"name": "m", "expression": "1"}]},
        {"name": "CambridgeOrders", "columns": _cols(("DateKey", "int64"), ("Amount", "double"), ("Qty", "int64")), "measures": []},
        DATE],
        "relationships": [_rel("CambridgeOrders", "DateKey", "Date", "Date")]}
    check("CambridgeOrders is a fact", _audit(model)[0]["CambridgeOrders"] == "fact", str(_audit(model)[0]))
    for name in ("Bridge Customer Segment", "customer_bridge", "BridgeCustSeg", "BRIDGE_ACCOUNT"):
        model["tables"][1]["name"] = name
        model["relationships"][0]["from_table"] = name
        check(f"{name} is a bridge", _audit(model)[0][name] == "bridge")


def test_is_active_coercion():
    print("\n== defect 16: is_active in every representation ==")
    for value, inactive in [(True, False), ("true", False), ("True", False), (1, False), ("1", False), (None, False),
                            ("Yes", False), (0, True), (False, True), ("false", True), ("0", True)]:
        m = {"tables": [FACT, DATE], "relationships": [_rel("Sales", "DateKey", "Date", "Date", is_active=value)]}
        check(f"is_active={value!r} -> {'inactive' if inactive else 'active'}",
              ("SS_ROLE_PLAYING" in _audit(m)[1]) is inactive, str(_audit(m)[1]))
    m = {"tables": [FACT, DATE],
         "relationships": [{"from_table": "Sales", "from_column": "DateKey", "to_table": "Date", "to_column": "Date"}]}
    check("is_active absent -> active", "SS_ROLE_PLAYING" not in _audit(m)[1])


def test_cardinality():
    print("\n== defect 17: one-to-one and reversed cardinality ==")
    one_to_one = {"tables": [
        {"name": "Customer", "columns": _cols(("CustomerKey", "int64"), ("Name",)), "measures": []},
        {"name": "CustomerDetail", "columns": _cols(("CustomerKey", "int64"), ("Notes",)), "measures": []}],
        "relationships": [_rel("Customer", "CustomerKey", "CustomerDetail", "CustomerKey",
                               from_cardinality="One", to_cardinality="One", cross_filter="BothDirections")]}
    classes, codes, summary = _audit(one_to_one)
    check("neither side of a 1:1 is a fact", classes == {"Customer": "dimension", "CustomerDetail": "dimension"}, str(classes))
    check("1:1 is reported as such, not as a missing date dimension or a measure-less fact",
          codes == ["SS_ONE_TO_ONE"], str(codes))
    with_fact = {"tables": [FACT, DATE] + one_to_one["tables"],
                 "relationships": [_rel("Sales", "DateKey", "Date", "Date"), _rel("Sales", "DateKey", "Customer", "CustomerKey")]
                 + one_to_one["relationships"]}
    classes, codes, _ = _audit(with_fact)
    check("a dimension with a 1:1 extension stays a dimension", classes["Customer"] == "dimension"
          and classes["CustomerDetail"] == "dimension" and classes["Sales"] == "fact", str(classes))
    reverse = {"tables": [FACT, DATE],
               "relationships": [_rel("Date", "Date", "Sales", "DateKey", from_cardinality="One", to_cardinality="Many")]}
    classes, codes, _ = _audit(reverse)
    check("one-to-many written from the one side is read correctly",
          classes == {"Sales": "fact", "Date": "date_dimension"} and codes == [], str((classes, codes)))


def test_key_detection():
    print("\n== defect 17: key-like column names ==")
    keys = ["StoreKey", "product_id", "AccountSK", "CUSTOMER_ID", "CUSTOMERKEY", "customerid", "customerkey", "Id", "ID",
            "Key", "ZipCode", "DISCOUNT_CODE", "Order Guid", "FK", "Customer Key", "DateSK", "ACCOUNT_FK"]
    words = ["Idaho", "Paid", "PAID", "Risk", "RISK", "Skill", "TASK", "DESK", "MASK", "ASK", "VALID", "Valid", "GRID",
             "COVID", "Turkey", "TURKEY", "Monkey", "Postcode", "Barcode", "Status", "Bid", "Liquid", "Amount", "", None]
    wrong = [k for k in keys if not star_schema._looks_like_key(k)]
    check("key-like names are recognized", not wrong, str(wrong))
    wrong = [w for w in words if star_schema._looks_like_key(w)]
    check("ordinary words are not keys, in any letter case", not wrong, str(wrong))

    def fact_with(names):
        return {"tables": [{"name": "F", "columns": _cols(*[(n,) for n in names]) + _cols(("K", "int64")),
                            "measures": [{"name": "m", "expression": "1"}]}, DATE],
                "relationships": [_rel("F", "K", "Date", "Date")]}
    check("upper-case text attributes on a fact are counted", "SS_TEXT_ON_FACT" in _audit(fact_with(["PAID", "RISK", "TASK", "STATUS"]))[1])
    check("Title-case text attributes likewise", "SS_TEXT_ON_FACT" in _audit(fact_with(["Paid", "Risk", "Task", "Status"]))[1])
    check("text key columns are not attributes", "SS_TEXT_ON_FACT" not in _audit(fact_with(["OrderCode", "customer_id", "SKU_KEY"]))[1])


def test_date_dimension_detection():
    print("\n== defect 17: date dimension names, marking, relation ==")
    hits = ["dim_date", "DimDate", "DateDim", "dDate", "DateDimension", "tblDate", "MasterCalendar", "Calendar", "Time",
            "Date Table", "Dates", "DIM_DATE", "dimdate", "datedim", "Fiscal Calendar"]
    misses = ["Sentiment", "Downtime", "Overtime", "Update Log", "Candidate", "Validated", "Product", "Mandates"]
    wrong = [n for n in hits if not star_schema._name_hints_date(n)]
    check("date-table names are recognized in any style", not wrong, str(wrong))
    wrong = [n for n in misses if star_schema._name_hints_date(n)]
    check("names that only contain the letters are not", not wrong, str(wrong))

    def model(name, cols, related=True, **table_props):
        t = dict({"name": name, "columns": cols, "measures": []}, **table_props)
        rels = [_rel("Sales", "DateKey", name, cols[0]["name"])] if related else \
            [_rel("Sales", "DateKey", "Product", "K")]
        tables = [FACT, t] + ([] if related else [{"name": "Product", "columns": _cols(("K", "int64"), ("P",)), "measures": []}])
        return {"tables": tables, "relationships": rels}

    classes, codes, _ = _audit(model("DateDim", _cols(("Date", "dateTime", {"is_key": False}), ("Year", "int64"))))
    check("unmarked 'DateDim' is the date dimension, flagged as unmarked",
          classes["DateDim"] == "date_dimension" and codes == ["SS_DATE_NOT_MARKED"], str((classes, codes)))
    res = star_schema.audit_star_schema(model("DateDim", _cols(("Date", "dateTime"), ("Year", "int64"))))
    f = next(x for x in res["findings"] if x["code"] == "SS_DATE_NOT_MARKED")
    check("without any key metadata the marking is 'unknown', not 'missing'",
          f["severity"] == "info" and "does not say" in f["message"], str(f))
    classes, codes, _ = _audit(model("DateDim", _cols(("Date", "dateTime")), data_category="Time"))
    check("table-level dataCategory Time counts as marked", codes == [], str(codes))
    classes, codes, _ = _audit(model("Date", _cols(("Date", "dateTime", {"is_key": True}), ("Year", "int64")), related=False))
    check("a date table nobody relates to is reported as unrelated, not as absent",
          "SS_DATE_NOT_RELATED" in codes and "SS_NO_DATE_DIM" not in codes, str(codes))


def test_robustness():
    print("\n== malformed input does not crash ==")
    cases = {
        "table name None": {"tables": [{"name": None, "columns": [], "measures": []}, FACT]},
        "data_type as a number": {"tables": [{"name": "T", "columns": [{"name": "c", "data_type": 6}], "measures": []}],
                                  "relationships": [_rel("T", "c", "U", "c")]},
        "column name None": {"tables": [{"name": "T", "columns": [{"name": None, "data_type": "string"}], "measures": []}],
                             "relationships": [_rel("T", "c", "U", "c"), _rel("T", "d", "V", "d")]},
        "relationship to an unknown table": {"tables": [FACT], "relationships": [_rel("Sales", "DateKey", "Ghost", "K")]},
        "None lists": {"tables": None, "relationships": None},
        "junk entries": {"tables": [None, "x", FACT], "relationships": [None, _rel("Sales", "DateKey", "Date", "Date")]},
    }
    for label, m in cases.items():
        try:
            res = star_schema.audit_star_schema(m)
            ok = 0 <= res["summary"]["score"] <= 100
        except Exception as e:  # noqa: BLE001 - the test reports the crash
            ok, res = False, repr(e)
        check(label, ok, str(res)[:120])


if __name__ == "__main__":
    print("=" * 70)
    print("  STAR SCHEMA AUDIT TESTS")
    print("=" * 70)
    test_classification()
    test_findings()
    test_negative_cases()
    test_measure_table_and_bridge_hint()
    test_score_is_not_generous()
    test_row_number_column_is_ignored()
    test_bridge_is_a_word()
    test_is_active_coercion()
    test_cardinality()
    test_key_detection()
    test_date_dimension_detection()
    test_robustness()
    print("\n" + "=" * 70)
    if _failures:
        print(f"  {len(_failures)} CHECK(S) FAILED: {', '.join(_failures)}")
        sys.exit(1)
    print("  ALL STAR SCHEMA CHECKS PASSED")
    print("=" * 70)
