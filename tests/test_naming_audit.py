"""
Tests for the naming-convention audit -> rename plan. No Power BI required.
Run: python tests/test_naming_audit.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))
import dax_generator  # noqa: E402
import naming_audit  # noqa: E402

_failures = []


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f": {detail}" if detail and not cond else ""))
    if not cond:
        _failures.append(name)


MODEL = {
    "tables": [
        {"name": "DIM_Customer",
         "columns": [{"name": "customer_id"}, {"name": "salesAmount"}, {"name": "  Region "},
                     {"name": "ID"}, {"name": "PRODUCT"}, {"name": "Qty Sold"}],
         "measures": [{"name": "Total_Sales"}]},
        {"name": "Sales",
         "columns": [{"name": "Order Date"}],
         "measures": [{"name": "Revenue"}]},
    ]
}


def _by_old(plan, old):
    for p in plan:
        if p["old"] == old:
            return p
    return None


def suggest(name, kind="column", **kw):
    return naming_audit.suggest_name(name, kind, **kw)[0]


def unchanged(name, kind="column", **kw):
    new, reasons = naming_audit.suggest_name(name, kind, **kw)
    return new == name and reasons == []


def test_detection():
    print("\n== detects and normalizes the common conventions ==")
    res = naming_audit.audit(MODEL)
    plan = res["plan"]
    p = _by_old(plan, "DIM_Customer")
    check("warehouse prefix stripped", p and p["new"] == "Customer" and "warehouse DIM_/FACT_ prefix" in p["reasons"], str(p))
    p = _by_old(plan, "salesAmount")
    check("camelCase split + titled", p and p["new"] == "Sales Amount" and "camelCase" in p["reasons"], str(p))
    p = _by_old(plan, "customer_id")
    check("snake_case converted, acronym spelled as one", p and p["new"] == "Customer ID" and "snake_case" in p["reasons"], str(p))
    p = _by_old(plan, "  Region ")
    check("whitespace trimmed", p and p["new"] == "Region", str(p))
    p = _by_old(plan, "PRODUCT")
    check("uppercase titled", p and p["new"] == "Product", str(p))


def test_acronyms_and_clean_preserved():
    print("\n== acronyms and already-clean names are left alone ==")
    res = naming_audit.audit(MODEL)
    plan = res["plan"]
    check("acronym ID preserved (no suggestion)", _by_old(plan, "ID") is None)
    check("clean 'Order Date' preserved", _by_old(plan, "Order Date") is None)
    check("clean 'Revenue' preserved", _by_old(plan, "Revenue") is None)


def test_abbreviations_opt_in():
    print("\n== abbreviation expansion is opt-in ==")
    off = naming_audit.audit(MODEL)
    on = naming_audit.audit(MODEL, {"expand_abbreviations": True})
    check("Qty Sold untouched by default", _by_old(off["plan"], "Qty Sold") is None)
    p = _by_old(on["plan"], "Qty Sold")
    check("Qty -> Quantity when enabled", p and p["new"] == "Quantity Sold" and "abbreviation" in p["reasons"], str(p))
    check("'Sort Desc' is descending, not a description", suggest("Sort Desc", expand_abbreviations=True) == "Sort Desc")
    check("'Product Desc' is a description", suggest("Product Desc", expand_abbreviations=True) == "Product Description")
    check("upper snake + abbreviations", suggest("AVG_AMT", expand_abbreviations=True) == "Average Amount")


def test_summary_and_scope():
    print("\n== summary + scope filter ==")
    res = naming_audit.audit(MODEL)
    s = res["summary"]
    check("summary counts suggestions", s["total_suggestions"] == len(res["plan"]) and s["total_suggestions"] >= 5)
    check("by_reason populated", s["by_reason"].get("snake_case", 0) >= 1)
    check("observed styles recorded", "snake_case" in s["observed_styles"])
    only_tables = naming_audit.audit(MODEL, {"scope": ["tables"]})
    check("scope=tables yields only tables", all(p["object_type"] == "table" for p in only_tables["plan"]))
    check("plan entries keep their keys", all({"object_type", "table", "old", "new", "reasons"} <= set(p) for p in res["plan"]))
    check("summary keeps its keys", {"total_suggestions", "by_type", "by_reason", "observed_styles", "dominant_style",
                                     "single_style"} <= set(s))


def test_hidden_and_internal_columns_skipped():
    print("\n== live-UAT regression: hidden + internal RowNumber columns skipped ==")
    model = {"tables": [{
        "name": "dim_date",
        "columns": [
            {"name": "RowNumber-2662979B-1795-4F74-8F37-6A1BA8059B61"},
            {"name": "hidden_helper", "is_hidden": True},
            {"name": "monthName"},
        ],
        "measures": [{"name": "secretCalc", "is_hidden": "true"}],
    }]}
    res = naming_audit.audit(model)
    olds = {p["old"] for p in res["plan"]}
    check("RowNumber-GUID never in plan", not any(o.startswith("RowNumber-") for o in olds), str(olds))
    check("hidden column never in plan", "hidden_helper" not in olds, str(olds))
    check("hidden measure never in plan", "secretCalc" not in olds, str(olds))
    check("visible column still audited", any(p["old"] == "monthName" for p in res["plan"]))


def test_upper_snake_case():
    print("\n== defect 18: UPPER_SNAKE_CASE becomes Title Case, acronyms stay ==")
    cases = {
        "ORDER_DATE": "Order Date", "CUSTOMER_NAME": "Customer Name", "UNIT_COST": "Unit Cost",
        "PRODUCT_TYPE": "Product Type", "IS_VALID": "Is Valid", "CUSTOMER_ID": "Customer ID",
        "NET_SALES_USD": "Net Sales USD", "SKU_COUNT": "SKU Count", "USD_AMT": "USD Amt", "ORDER_QTY": "Order Qty",
        "SALES_YOY": "Sales YoY", "COST_OF_GOODS": "Cost of Goods", "NPV_RATE": "NPV Rate", "PLAN_A": "Plan A",
        "SALE": "Sale", "DATE": "Date", "SALES": "Sales", "TOTAL SALES": "Total Sales", "FY2024_Q1": "FY2024 Q1",
    }
    for old, want in cases.items():
        check(f"{old} -> {want}", suggest(old) == want, suggest(old))
    for keep in ("ID", "EBITDA", "LATAM", "NASDAQ", "COGS", "KPI", "Q1", "FY2024Q1", "NPV", "A"):
        check(f"all-capitals acronym '{keep}' is kept", unchanged(keep), str(naming_audit.suggest_name(keep, "column")))
    check("lower snake_case uses the acronym spelling too", suggest("net_sales_usd") == "Net Sales USD"
          and suggest("sales_yoy") == "Sales YoY" and suggest("total sales ytd") == "Total Sales YTD")
    check("target_case none only restructures", naming_audit.suggest_name("ORDER_DATE", "column", target_case="none")
          == ("ORDER DATE", ["snake_case"]))


def test_tokens_left_alone():
    print("\n== defect 19: deliberate spellings are not re-split or re-cased ==")
    for name in ("Total Sales YoY", "Total Sales YoY %", "Sales MoM %", "Rolling 12M", "Rolling 3M", "H2O", "B2B Sales",
                 "iPhone Sales", "eBay Fees", "McDonald", "LinkedIn Clicks", "kWh", "pH", "% of Total",
                 "Cost of Goods Sold", "Year to Date", "Rank by Category", "EBITDA Margin", "Net EBITDA", "GAAP %",
                 "Sales (USD)", "YoY", "MoM", "iPhone", "LinkedIn", "YouTube", "12M", "FY2024Q1", "A1C"):
        check(f"'{name}' is left alone", unchanged(name, "measure"), str(naming_audit.suggest_name(name, "measure")))
    split = {"YTDSales": "YTD Sales", "SalesYTD": "Sales YTD", "HTTPStatusCode": "HTTP Status Code", "customerID": "Customer ID",
             "Top10Customers": "Top10 Customers", "Q1Sales": "Q1 Sales", "TotalSales": "Total Sales", "PowerBI": "Power BI",
             "salesAmountUSD": "Sales Amount USD", "CustomerId": "Customer ID", "SalesYtd": "Sales YTD",
             "LinkedInClicks": "LinkedIn Clicks", "eBayFees": "eBay Fees", "iPhoneSales": "iPhone Sales",
             "totalYoY": "Total YoY", "order_by": "Order By", "MY_VALUE": "My Value"}
    for old, want in split.items():
        check(f"identifier {old} -> {want}", suggest(old, "measure") == want, suggest(old, "measure"))
    for name in ("Id", "Customer Id", "Ytd", "xAxis"):
        check(f"'{name}' is not an identifier being restructured: left as written", unchanged(name))
    check("casing still fixed in a spaced name", suggest("total sales") == "Total Sales" and suggest("Sales amount") == "Sales Amount")
    check("a leading minor word is capitalized", suggest("of total") == "Of Total")
    check("double spaces and non-breaking spaces", naming_audit.suggest_name("Total  Sales", "measure")
          == ("Total Sales", ["inconsistent spacing"])
          and suggest("Sales" + chr(0xA0) + "Amt") == "Sales Amt")
    check("non-ASCII words are title-cased, scripts without case are untouched",
          suggest("montant_" + chr(0xE9) + "t" + chr(0xE9)) == "Montant " + chr(0xC9) + "t" + chr(0xE9)
          and suggest(chr(0x58F2) + chr(0x4E0A) + "_" + chr(0x91D1) + chr(0x984D)) == chr(0x58F2) + chr(0x4E0A) + " " + chr(0x91D1) + chr(0x984D))


def test_suggestions_are_stable():
    print("\n== a suggested name needs no further suggestion ==")
    cases = [("Fact AMOUNT", "table", "Amount"), ("Fact HTTPStatus", "table", "HTTP Status"), ("idDIM", "column", "ID Dim"),
             ("AMOUNTAMOUNT_id", "column", "Amountamount ID"), ("DIM_ORDER_DATE", "table", "Order Date"),
             ("  total_sales_YTD ", "measure", "Total Sales YTD"), ("customerId", "column", "Customer ID"),
             ("Dim customerName", "table", "Customer Name")]
    for old, kind, want in cases:
        new = suggest(old, kind)
        check(f"{old!r} -> {want!r} in one pass", new == want, new)
        check(f"...and {want!r} is then left alone", unchanged(new, kind), str(naming_audit.suggest_name(new, kind)))


def test_generator_names_are_clean():
    print("\n== defect 19: the measure generator's own names need no renaming ==")
    suite = (dax_generator.generate_time_intelligence("Total Sales", "Date.Date",
                                                      variants=list(dax_generator.TIME_INTELLIGENCE_VARIANTS))
             + dax_generator.generate_time_intelligence("Total Sales", "Date.Date", variants=["ytd", "qtd"], fiscal_year_end=6)
             + dax_generator.generate_ratios("Total Sales", ["Product.Category"])
             + dax_generator.generate_ranking("Total Sales", ["Product.Category"])
             + dax_generator.generate_column_stats("Sales.Amount"))
    model = {"tables": [{"name": "Sales", "columns": [], "measures": [{"name": m["name"]} for m in suite]}]}
    plan = naming_audit.audit(model)["plan"]
    check(f"zero suggestions for {len(suite)} generated measures", plan == [], str([(p["old"], p["new"]) for p in plan]))


def test_configurable_acronyms():
    print("\n== defect 18: the acronym list is configurable ==")
    check("unknown acronym with a vowel is title-cased by default", suggest("AOP_TARGET") == "Aop Target")
    check("...and kept once listed", suggest("AOP_TARGET", acronyms=["AOP"]) == "AOP Target")
    check("a mixed-case spelling can be listed", suggest("saas_revenue", acronyms=["SaaS"]) == "SaaS Revenue")
    check("a listed identifier is not split", suggest("SaaSRevenue", acronyms=["SaaSRevenue"]) == "SaaSRevenue")
    check("replace_acronyms drops the defaults", suggest("customer_id", acronyms=["KPI"], replace_acronyms=True) == "Customer Id")
    res = naming_audit.audit({"tables": [{"name": "T", "columns": [{"name": "AOP_TARGET"}]}]}, {"acronyms": ["AOP"]})
    check("audit() passes the option through", res["plan"][0]["new"] == "AOP Target", str(res["plan"]))
    try:
        naming_audit.audit(MODEL, {"target_case": "sentence"})
        check("unknown target_case is rejected", False)
    except ValueError as e:
        check("unknown target_case is rejected", "target_case" in str(e))


def test_collisions():
    print("\n== defect 20: colliding renames are flagged, never presented as safe ==")
    model = {"tables": [
        {"name": "DIM_Customer",
         "columns": [{"name": "customer_id"}, {"name": "CustomerId"}, {"name": "sales_amount"}, {"name": "Sales Amount"},
                     {"name": "order_count"}, {"name": "Region Name", "is_hidden": True}, {"name": "region_name"}],
         "measures": [{"name": "total_sales"}, {"name": "TotalSales"}, {"name": "Order Count"}, {"name": "net_margin"}]},
        {"name": "FACT_Customer", "columns": [{"name": "unit_price"}], "measures": [{"name": "avg_price"}]},
        {"name": "Customer", "columns": [], "measures": []},
        {"name": "Other", "columns": [{"name": "Net Margin"}, {"name": "customer_id"}], "measures": [{"name": "Avg Price"}]},
    ]}
    res = naming_audit.audit(model)
    plan = {(p["object_type"], p["table"], p["old"]): p for p in res["plan"]}

    def entry(kind, table, old):
        return plan[(kind, table, old)]

    def unsafe(kind, table, old, *needles):
        p = entry(kind, table, old)
        text = " | ".join(p["conflicts_with"])
        return p["safe"] is False and all(n in text for n in needles)

    check("two columns mapping to one name", unsafe("column", "DIM_Customer", "customer_id", "CustomerId")
          and unsafe("column", "DIM_Customer", "CustomerId", "customer_id"))
    check("column target already exists in the table", unsafe("column", "DIM_Customer", "sales_amount", "DIM_Customer[Sales Amount]"))
    check("column target exists as a hidden column", unsafe("column", "DIM_Customer", "region_name", "Region Name"))
    check("column target is a measure of the same table", unsafe("column", "DIM_Customer", "order_count", "measure DIM_Customer[Order Count]"))
    check("two measures mapping to one name", unsafe("measure", "DIM_Customer", "total_sales", "TotalSales"))
    check("measure target exists in another table (measures are model-wide)", unsafe("measure", "FACT_Customer", "avg_price", "Other[Avg Price]"))
    check("two tables mapping to an existing table name", unsafe("table", None, "DIM_Customer", "table 'Customer'", "FACT_Customer")
          and unsafe("table", None, "FACT_Customer", "table 'Customer'"))
    check("a same-named column in ANOTHER table is not a conflict",
          entry("measure", "DIM_Customer", "net_margin")["safe"] is True and entry("column", "Other", "customer_id")["safe"] is True
          and entry("column", "FACT_Customer", "unit_price")["safe"] is True)
    flagged = [p for p in res["plan"] if not p["safe"]]
    check("every unsafe entry says so in its reasons (the only thing a plain printer shows)",
          all(any(r.startswith("COLLISION") and "do not apply" in r for r in p["reasons"]) for p in flagged))
    check("safe entries carry no collision text", all(not any(r.startswith("COLLISION") for r in p["reasons"])
                                                      for p in res["plan"] if p["safe"]))
    s = res["summary"]
    check("summary counts safe and colliding entries", s["collisions"] == len(flagged) == len(res["collisions"])
          and s["safe_suggestions"] == s["total_suggestions"] - s["collisions"] and s["collisions"] >= 9, str(s))
    check("collision text is not counted as a naming reason", not any(k.startswith("COLLISION") for k in s["by_reason"]))
    case_only = naming_audit.audit({"tables": [{"name": "T", "columns": [{"name": "sales amount"}]}]})["plan"][0]
    check("a case-only rename does not collide with itself", case_only["new"] == "Sales Amount" and case_only["safe"] is True)
    check("a clean model reports no collisions", naming_audit.audit(MODEL)["summary"]["collisions"] == 0)


def test_plan_order():
    print("\n== defect 20: columns and measures come before their table's rename ==")
    model = {"tables": [
        {"name": "DIM_Customer", "columns": [{"name": "customer_id"}], "measures": [{"name": "total_sales"}]},
        {"name": "fact_orders", "columns": [{"name": "order_id"}], "measures": []}]}
    plan = naming_audit.audit(model)["plan"]
    order = [(p["object_type"], p["table"], p["old"]) for p in plan]
    check("order", order == [("column", "DIM_Customer", "customer_id"), ("measure", "DIM_Customer", "total_sales"),
                             ("table", None, "DIM_Customer"), ("column", "fact_orders", "order_id"),
                             ("table", None, "fact_orders")], str(order))
    names = {t["name"] for t in model["tables"]}
    ok = True
    for p in plan:                                   # replay: each entry must find its table under the name it uses
        if p["object_type"] == "table":
            names.discard(p["old"])
            names.add(p["new"])
        else:
            ok = ok and p["table"] in names
    check("applying top to bottom always addresses an existing table", ok)


def test_prefixes_hidden_tables_and_bad_names():
    print("\n== defect 21: prefixes, hidden tables, unusable names ==")
    for old, want in {"DimCustomer": "Customer", "FactSales": "Sales", "DIM_Customer": "Customer", "dim customer": "Customer",
                      "fact_order_lines": "Order Lines", "DIM_CUSTOMER": "Customer"}.items():
        check(f"table {old} -> {want}", suggest(old, "table") == want, suggest(old, "table"))
    check("a column keeps a Dim/Fact word", suggest("DimCustomer", "column") == "Dim Customer")
    check("prefix stripping can be turned off", suggest("DimCustomer", "table", strip_warehouse_prefixes=False) == "Dim Customer")
    check("'Dimension' / 'Fact' alone are names, not prefixes", unchanged("Dimension", "table") and unchanged("Fact", "table"))
    model = {"tables": [
        {"name": "hidden_stage", "is_hidden": True, "columns": [{"name": "raw_value"}], "measures": [{"name": "raw_total"}]},
        {"name": "LocalDateTable_0b1c2d3e", "columns": [{"name": "Date"}], "measures": []},
        {"name": "DateTableTemplate_9f", "columns": [], "measures": []},
        {"name": None, "columns": [{"name": "x_y"}]},
        {"name": "ok_table", "columns": [{"name": None}, {"name": ""}, {"name": 5}, None, {"name": "good_name"}],
         "measures": [{"name": None}, None]},
        None]}
    res = naming_audit.audit(model)
    olds = [p["old"] for p in res["plan"]]
    check("hidden tables are skipped with their contents; internal tables too; bad names do not crash",
          olds == ["good_name", "ok_table"], str(olds))
    check("empty and None models", naming_audit.audit({})["plan"] == [] and naming_audit.audit({"tables": None})["plan"] == [])
    styles = {n: naming_audit._classify(n) for n in ("Revenue", "TotalSales", "totalSales", "Total Sales", "ORDER_DATE", "order_date")}
    check("style labels", styles == {"Revenue": "single word", "TotalSales": "PascalCase", "totalSales": "camelCase",
                                     "Total Sales": "spaced", "ORDER_DATE": "UPPER_SNAKE_CASE", "order_date": "snake_case"}, str(styles))


if __name__ == "__main__":
    print("=" * 70)
    print("  NAMING AUDIT TESTS")
    print("=" * 70)
    test_detection()
    test_acronyms_and_clean_preserved()
    test_abbreviations_opt_in()
    test_summary_and_scope()
    test_hidden_and_internal_columns_skipped()
    test_upper_snake_case()
    test_tokens_left_alone()
    test_suggestions_are_stable()
    test_generator_names_are_clean()
    test_configurable_acronyms()
    test_collisions()
    test_plan_order()
    test_prefixes_hidden_tables_and_bad_names()
    print("\n" + "=" * 70)
    if _failures:
        print(f"  {len(_failures)} CHECK(S) FAILED: {', '.join(_failures)}")
        sys.exit(1)
    print("  ALL NAMING AUDIT CHECKS PASSED")
    print("=" * 70)
