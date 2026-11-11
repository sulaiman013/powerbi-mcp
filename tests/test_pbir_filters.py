"""
Tests for the PBIR filter builders (src/pbir_filters.py): typed literals, every compact filter
form, the filter container and its options, and merging a file's filters with the ones another
hand put there. The expected dicts follow the filterConfiguration and semanticQuery schemas;
the same shapes were loaded into Power BI Desktop and seen to filter. No Power BI required.
Run: python tests/test_pbir_filters.py
"""
import copy
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))
import pbir_filters as pf  # noqa: E402

_failures = []


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f": {detail}" if detail and not cond else ""))
    if not cond:
        _failures.append(name)


def eq(name, actual, expected):
    detail = ""
    if actual != expected:
        got, want = (json.dumps(v, sort_keys=True)[:700] for v in (actual, expected))
        detail = f"got {got} expected {want}"
    check(name, actual == expected, detail)


def refused(name, fragment, fn):
    """The call must raise ValueError, and the message must say what is wrong."""
    try:
        fn()
    except ValueError as e:
        check(name, fragment in str(e), f"the message was {str(e)!r}")
    else:
        check(name, False, "nothing was raised")


def V(value):
    return {"Literal": {"Value": value}}


CITY = {"table": "Customer", "field": "City", "kind": "Column"}
AMOUNT = {"table": "Sales", "field": "Amount", "kind": "Column"}
DATE = {"table": "Calendar", "field": "Date", "kind": "Column"}
TOTAL = {"table": "Sales", "field": "Total Sales", "kind": "Measure"}
SUMMED = {"table": "Sales", "field": "Amount", "kind": "Column", "aggregation": 0}
CITY_BY_TABLE = {"Column": {"Expression": {"SourceRef": {"Entity": "Customer"}}, "Property": "City"}}
CITY_BY_ALIAS = {"Column": {"Expression": {"SourceRef": {"Source": "c"}}, "Property": "City"}}
FROM_CUSTOMER = [{"Name": "c", "Entity": "Customer", "Type": 0}]


def where(container):
    return container["filter"]["Where"][0]["Condition"]


def test_expressions():
    print("\n== fields and literals ==")
    eq("a column by its table", pf.field_expr(CITY), CITY_BY_TABLE)
    eq("a column by the alias of a From clause", pf.field_expr(CITY, "c"), CITY_BY_ALIAS)
    eq("a measure", pf.field_expr(TOTAL, "s"),
       {"Measure": {"Expression": {"SourceRef": {"Source": "s"}}, "Property": "Total Sales"}})
    eq("an aggregated column", pf.field_expr(SUMMED),
       {"Aggregation": {"Expression": {"Column": {"Expression": {"SourceRef": {"Entity": "Sales"}},
                                                  "Property": "Amount"}}, "Function": 0}})

    eq("text is quoted", pf.literal("Lyon"), V("'Lyon'"))
    eq("an apostrophe is doubled", pf.literal("L'Aquila", "string"), V("'L''Aquila'"))
    eq("a whole number", pf.literal(12), V("12L"))
    eq("a fraction", pf.literal(1.5), V("1.5D"))
    eq("a float with no fraction is still a double", pf.literal(2.0), V("2D"))
    eq("a whole number for an integer column", pf.literal(2020, "int64"), V("2020L"))
    eq("a fraction against an integer column", pf.literal(2.5, "int64"), V("2.5D"))
    eq("a whole number for a double column", pf.literal(50000, "double"), V("50000D"))
    eq("a decimal column", [pf.literal(10, "decimal"), pf.literal(10.25, "decimal")], [V("10M"), V("10.25M")])
    eq("true and false", [pf.literal(True), pf.literal(False, "boolean")], [V("true"), V("false")])
    eq("null", pf.literal(None, "string"), V("null"))
    eq("a date for a date column", pf.literal("2024-03-01", "dateTime"), V("datetime'2024-03-01T00:00:00'"))
    eq("a date and time", pf.literal("2024-03-01T18:30", "dateTime"), V("datetime'2024-03-01T18:30:00'"))
    eq("the same text for a text column stays text", pf.literal("2024-03-01", "string"), V("'2024-03-01'"))
    eq("a literal written out is passed through", pf.literal({"literal": "datetime'2024-01-01T00:00:00'"}),
       V("datetime'2024-01-01T00:00:00'"))
    refused("text for a number column", "is text, but the field holds number values (int64)",
            lambda: pf.literal("many", "int64"))
    refused("a number for a text column", "is a number, but the field holds text values",
            lambda: pf.literal(5, "string"))
    refused("true for a number column", "is true/false", lambda: pf.literal(True, "double"))
    refused("a number for a date column", "is a number, but the field holds date values",
            lambda: pf.literal(2024, "dateTime"))
    refused("text that is not a date", "is not a date", lambda: pf.literal("March", "dateTime"))
    refused("an object that is not a literal", "is not a value", lambda: pf.literal({"value": 1}))
    refused("a list", "is not a value a filter can compare with", lambda: pf.literal([1]))

    # values with the right shape that still are not values: Desktop draws an error box for these
    for label, text in (("month 13", "2020-13-45"), ("30 February", "2014-02-30"),
                        ("hour 25", "2020-01-01T25:61:61"), ("year 0", "0000-01-01")):
        refused(f"{label} is not a date", "is not a date; write it as YYYY-MM-DD",
                lambda text=text: pf.literal(text, "dateTime"))
    eq("29 February in a leap year is one", pf.literal("2024-02-29", "dateTime"),
       V("datetime'2024-02-29T00:00:00'"))
    eq("a space between date and time is read too", pf.literal("2024-03-01 18:30:05", "dateTime"),
       V("datetime'2024-03-01T18:30:05'"))
    refused("a whole number beyond 64 bits", "outside the range of a whole number",
            lambda: pf.literal(10 ** 30, "int64"))
    refused("with no column type either", "outside the range of a whole number",
            lambda: pf.literal(-2 ** 63 - 1))
    eq("the ends of the range pass", [pf.literal(2 ** 63 - 1), pf.literal(-2 ** 63, "int64")],
       [V("9223372036854775807L"), V("-9223372036854775808L")])
    eq("a double column takes a number that large", pf.literal(10 ** 30, "double"), V("1" + "0" * 30 + "D"))
    for bad in (float("nan"), float("inf"), float("-inf")):
        refused(f"{bad!r} is not a number", "is not a number a filter can compare with",
                lambda bad=bad: pf.literal(bad, "double"))

    # a literal written out in full must be one of the forms the schema has
    for raw in ("'O''Brien'", "''", "12L", "-3L", "1.5D", "2D", "1e3D", "1.5M", "true", "false", "null",
                "datetime'2026-01-31T00:00:00'", "datetime'2026-01-31T23:59:59.0000000'"):
        eq(f"the literal {raw} passes as written", pf.literal({"literal": raw}), V(raw))
    for raw in ("DROP TABLE", "Lyon", "'unclosed", "'it's'", "12", "12l", "1.5", "1.5L", "TRUE", "None",
                "datetime'2026-02-30T00:00:00'", "datetime'2026-01-31'", "99999999999999999999L", ""):
        refused(f"the literal {raw!r} is not one", "is not a literal Power BI reads",
                lambda raw=raw: pf.literal({"literal": raw}))
    refused("a text literal for a number column", "\"'x'\" is text, but the field holds number values",
            lambda: pf.literal({"literal": "'x'"}, "int64"))
    refused("a number literal for a text column", "'5L' is a number, but the field holds text values",
            lambda: pf.literal({"literal": "5L"}, "string"))
    refused("a date literal for a number column", "is a date, but the field holds number values",
            lambda: pf.literal({"literal": "datetime'2026-01-31T00:00:00'"}, "double"))
    eq("null fits any column", pf.literal({"literal": "null"}, "int64"), V("null"))
    eq("a literal of the column's own kind passes", pf.literal({"literal": "5L"}, "double"), V("5L"))


def test_basic():
    print("\n== is one of, is none of, an empty card ==")
    f = pf.filter_container("f1", CITY, {"field": "Customer.City", "in": ["Lyon", "Turin"]},
                            data_type="string")
    eq("is one of", f, {
        "name": "f1", "field": CITY_BY_TABLE, "type": "Categorical",
        "filter": {"Version": 2, "From": FROM_CUSTOMER, "Where": [{"Condition": {"In": {
            "Expressions": [CITY_BY_ALIAS], "Values": [[V("'Lyon'")], [V("'Turin'")]]}}}]},
        "howCreated": "User"})
    plain = f
    f = pf.filter_container("f2", CITY, {"not_in": ["Lyon"]})
    eq("is none of wraps the list in Not", where(f),
       {"Not": {"Expression": {"In": {"Expressions": [CITY_BY_ALIAS], "Values": [[V("'Lyon'")]]}}}})
    eq("and switches the card to its inverted mode", f["objects"],
       {"general": [{"properties": {"isInvertedSelectionMode": {"expr": V("true")}}}]})
    eq("a blank among the values",
       where(pf.filter_container("f", CITY, {"in": [None, "Lyon"]}))["In"]["Values"],
       [[V("null")], [V("'Lyon'")]])
    eq("numbers take the column's type", where(pf.filter_container("f", AMOUNT, {"in": [1, 2]},
                                                                   data_type="double"))["In"]["Values"],
       [[V("1D")], [V("2D")]])
    empty = pf.filter_container("f3", CITY, {})
    eq("no condition leaves an empty card", empty,
       {"name": "f3", "field": CITY_BY_TABLE, "type": "Categorical", "howCreated": "User"})
    opts = pf.filter_container("f4", CITY, {"in": ["Lyon"], "display_name": "Home city", "hidden": True,
                                            "locked": True, "single_select": True}, how_created="Auto")
    eq("the options", [opts["displayName"], opts["isHiddenInViewMode"], opts["isLockedInViewMode"],
                       opts["howCreated"], opts["objects"]],
       ["Home city", True, True, "Auto",
        {"general": [{"properties": {"requireSingleSelect": {"expr": V("true")}}}]}])
    check("options left out leave no key", not {"displayName", "isHiddenInViewMode", "isLockedInViewMode",
                                                "objects"} & set(plain))
    check("hidden: false leaves no key either",
          "isHiddenInViewMode" not in pf.filter_container("f", CITY, {"hidden": False}))
    eq("the alias is the table's first letter", pf.filter_container("f", {"table": "_measures", "field": "X",
                                                                          "kind": "Measure"},
                                                                    {"op": ">", "value": 1})
       ["filter"]["From"], [{"Name": "t", "Entity": "_measures", "Type": 0}])

    refused("an empty list", 'needs a list of at least one value', lambda: pf.filter_container("f", CITY,
                                                                                               {"in": []}))
    refused("a value of the wrong kind", "is a number, but the field holds text values",
            lambda: pf.filter_container("f", CITY, {"in": ["Lyon", 7]}, data_type="string"))
    refused("two forms at once", "not several", lambda: pf.filter_container("f", CITY, {"in": ["a"],
                                                                                        "blank": True}))
    refused("an unknown key", "unknown filter key(s) ['inn']", lambda: pf.filter_container("f", CITY,
                                                                                           {"inn": ["a"]}))
    refused("single_select with two values", '"single_select" allows one value',
            lambda: pf.filter_container("f", CITY, {"in": ["a", "b"], "single_select": True}))
    refused("single_select on a comparison", '"single_select" goes with "in"',
            lambda: pf.filter_container("f", AMOUNT, {"op": ">", "value": 1, "single_select": True}))
    refused("an option that is not true or false", '"hidden" must be true or false',
            lambda: pf.filter_container("f", CITY, {"hidden": "yes"}))


def test_advanced():
    print("\n== comparisons ==")
    amount = {"Column": {"Expression": {"SourceRef": {"Source": "s"}}, "Property": "Amount"}}

    def compare(kind, value):
        return {"Comparison": {"ComparisonKind": kind, "Left": amount, "Right": V(value)}}

    f = pf.filter_container("f", AMOUNT, {"op": ">=", "value": 100}, data_type="int64")
    eq("one condition", [f["type"], where(f)], ["Advanced", compare(2, "100L")])
    eq("the comparison numbers are the schema's", [pf.COMPARISONS[o] for o in ("=", ">", ">=", "<", "<=")],
       [0, 1, 2, 3, 4])
    eq("is not", where(pf.filter_container("f", AMOUNT, {"op": "!=", "value": 0})),
       {"Not": {"Expression": compare(0, "0L")}})
    f = pf.filter_container("f", AMOUNT, {"op": ">=", "value": 100, "and": {"op": "<", "value": 500}})
    eq("two conditions joined by and", where(f), {"And": {"Left": compare(2, "100L"),
                                                          "Right": compare(3, "500L")}})
    f = pf.filter_container("f", AMOUNT, {"op": "<", "value": 10, "or": {"op": ">", "value": 90.5}})
    eq("or", where(f), {"Or": {"Left": compare(3, "10L"), "Right": compare(1, "90.5D")}})
    f = pf.filter_container("f", AMOUNT, {"between": [100, 500]}, data_type="double")
    eq("between includes both ends", where(f), {"And": {"Left": compare(2, "100D"),
                                                        "Right": compare(4, "500D")}})
    f = pf.filter_container("f", AMOUNT, {"blank": True})
    eq("is blank", where(f), compare(0, "null"))
    eq("is not blank", where(pf.filter_container("f", AMOUNT, {"blank": False})),
       {"Not": {"Expression": compare(0, "null")}})
    eq("a comparison or blank", where(pf.filter_container("f", AMOUNT, {"op": ">", "value": 5,
                                                                        "or": {"blank": True}})),
       {"Or": {"Left": compare(1, "5L"), "Right": compare(0, "null")}})
    f = pf.filter_container("f", CITY, {"op": "contains", "value": "on"}, data_type="string")
    eq("contains", where(f), {"Contains": {"Left": CITY_BY_ALIAS, "Right": V("'on'")}})
    eq("does not start with", where(pf.filter_container("f", CITY, {"op": "not_starts_with", "value": "L"})),
       {"Not": {"Expression": {"StartsWith": {"Left": CITY_BY_ALIAS, "Right": V("'L'")}}}})
    f = pf.filter_container("f", DATE, {"between": ["2024-01-01", "2024-12-31"]}, data_type="dateTime")
    eq("dates",
       [where(f)["And"]["Left"]["Comparison"]["Right"], where(f)["And"]["Right"]["Comparison"]["Right"]],
       [V("datetime'2024-01-01T00:00:00'"), V("datetime'2024-12-31T00:00:00'")])

    f = pf.filter_container("f", TOTAL, {"op": ">", "value": 1000})
    eq("a measure is compared the same way", [f["type"], f["field"], where(f)["Comparison"]["Left"],
                                              where(f)["Comparison"]["Right"]],
       ["Advanced",
        {"Measure": {"Expression": {"SourceRef": {"Entity": "Sales"}}, "Property": "Total Sales"}},
        {"Measure": {"Expression": {"SourceRef": {"Source": "s"}}, "Property": "Total Sales"}}, V("1000L")])
    f = pf.filter_container("f", SUMMED, {"op": ">", "value": 1.5}, data_type="string")
    eq("an aggregated column ignores the column's own type", where(f)["Comparison"]["Right"], V("1.5D"))
    eq("an empty card for a measure is an advanced one", pf.filter_container("f", TOTAL, {})["type"],
       "Advanced")

    refused("an unknown operator", "unknown operator '=>'", lambda: pf.filter_container(
        "f", AMOUNT, {"op": "=>", "value": 1}))
    refused("an operator with no value", 'needs "value"',
            lambda: pf.filter_container("f", AMOUNT, {"op": ">"}))
    refused("a value with no operator", '"value" goes with "op"', lambda: pf.filter_container(
        "f", AMOUNT, {"value": 1}))
    refused("and with nothing to join", 'joins a second condition', lambda: pf.filter_container(
        "f", AMOUNT, {"in": [1], "and": {"op": ">", "value": 1}}))
    refused("and together with or", '"and" or "or", not both', lambda: pf.filter_container(
        "f", AMOUNT, {"op": ">", "value": 1, "and": {"blank": True}, "or": {"blank": True}}))
    refused("a second condition that is not an object", "a condition is an object",
            lambda: pf.filter_container("f", AMOUNT, {"op": ">", "value": 1, "and": 5}))
    refused("a second condition with a stray key", "unknown key(s) ['valu']", lambda: pf.filter_container(
        "f", AMOUNT, {"op": ">", "value": 1, "and": {"op": "<", "valu": 5}}))
    refused("between with one end", "needs two values", lambda: pf.filter_container("f", AMOUNT,
                                                                                    {"between": [1]}))
    refused("between with the ends the wrong way round", "needs the lowest value first: 9 is above 1",
            lambda: pf.filter_container("f", AMOUNT, {"between": [9, 1]}))
    refused("dates the wrong way round", "needs the lowest value first",
            lambda: pf.filter_container("f", DATE, {"between": ["2024-12-31", "2024-01-01"]},
                                        data_type="dateTime"))
    refused("between with no value at an end", "needs a value at both ends",
            lambda: pf.filter_container("f", AMOUNT, {"between": [None, 5]}))
    refused("between two nulls", "needs a value at both ends",
            lambda: pf.filter_container("f", AMOUNT, {"between": [None, None]}))
    eq("between two equal ends is one value", where(pf.filter_container("f", AMOUNT, {"between": [5, 5]}))
       ["And"]["Right"]["Comparison"]["Right"], V("5L"))
    eq("text ends are not compared", pf.filter_container("f", CITY, {"between": ["b", "a"]})["type"],
       "Advanced")
    refused("a display name that is not text", '"display_name" must be text',
            lambda: pf.filter_container("f", AMOUNT, {"op": ">", "value": 1, "display_name": 5}))
    refused("a date that is not one, in a comparison", "'2014-02-30' is not a date",
            lambda: pf.filter_container("f", DATE, {"op": ">", "value": "2014-02-30"}, data_type="dateTime"))
    refused("contains on a number", "compares text",
            lambda: pf.filter_container("f", AMOUNT, {"op": "contains", "value": 1}))
    refused("blank that is not true or false", '"blank" must be true', lambda: pf.filter_container(
        "f", AMOUNT, {"blank": "yes"}))
    refused("a list of values for a measure", "can only be compared", lambda: pf.filter_container(
        "f", TOTAL, {"in": [1]}))
    refused("a comparison of the wrong kind", "is text, but the field holds number values",
            lambda: pf.filter_container("f", AMOUNT, {"op": ">", "value": "big"}, data_type="int64"))


def test_top_n():
    print("\n== top and bottom N ==")
    f = pf.filter_container("t", CITY, {"top": {"n": 5, "by": "Sales.Total Sales"}}, order_by=TOTAL)
    eq("top N ranks in a subquery and keeps the values it returns", f["filter"], {
        "Version": 2,
        "From": [{"Name": "subquery", "Type": 2, "Expression": {"Subquery": {"Query": {
            "Version": 2,
            "From": [{"Name": "c", "Entity": "Customer", "Type": 0},
                     {"Name": "s", "Entity": "Sales", "Type": 0}],
            "Select": [{**CITY_BY_ALIAS, "Name": "field"}],
            "OrderBy": [{"Direction": 2, "Expression": {"Measure": {
                "Expression": {"SourceRef": {"Source": "s"}}, "Property": "Total Sales"}}}],
            "Top": 5}}}},
            {"Name": "c", "Entity": "Customer", "Type": 0}],
        "Where": [{"Condition": {"In": {"Expressions": [CITY_BY_ALIAS],
                                        "Table": {"SourceRef": {"Source": "subquery"}}}}}]})
    eq("its type", f["type"], "TopN")
    f = pf.filter_container("t", CITY, {"top": {"n": 2, "by": "x", "direction": "bottom"}}, order_by=TOTAL)
    eq("bottom N sorts ascending", f["filter"]["From"][0]["Expression"]["Subquery"]["Query"]["OrderBy"][0]
       ["Direction"], 1)
    same = pf.filter_container("t", AMOUNT, {"top": {"n": 3, "by": "x"}}, order_by=SUMMED)
    query = same["filter"]["From"][0]["Expression"]["Subquery"]["Query"]
    eq("one table is listed once", query["From"], [{"Name": "s", "Entity": "Sales", "Type": 0}])
    eq("and an aggregated column can rank", list(query["OrderBy"][0]["Expression"]), ["Aggregation"])
    clash = pf.filter_container("t", CITY, {"top": {"n": 1, "by": "x"}},
                                order_by={"table": "Calendar", "field": "Days", "kind": "Measure"})
    eq("two tables with one initial get two aliases",
       [s["Name"] for s in clash["filter"]["From"][0]["Expression"]["Subquery"]["Query"]["From"]],
       ["c", "c1"])

    refused("no measure to rank by", 'needs "by"', lambda: pf.filter_container("t", CITY, {"top": {"n": 5}}))
    refused("n that is not a whole number", 'needs "n"', lambda: pf.filter_container(
        "t", CITY, {"top": {"n": 2.5, "by": "x"}}, order_by=TOTAL))
    refused("n of zero", 'needs "n"', lambda: pf.filter_container("t", CITY, {"top": {"n": 0, "by": "x"}},
                                                                order_by=TOTAL))
    refused("an unknown direction", '"direction" is "top" or "bottom"', lambda: pf.filter_container(
        "t", CITY, {"top": {"n": 1, "by": "x", "direction": "up"}}, order_by=TOTAL))
    refused("a stray key", "unknown key(s) ['count']", lambda: pf.filter_container(
        "t", CITY, {"top": {"count": 1, "by": "x"}}, order_by=TOTAL))
    refused("top that is not an object", '"top" is an object', lambda: pf.filter_container("t", CITY,
                                                                                         {"top": 5}))
    refused("top of a measure", "can only be compared", lambda: pf.filter_container(
        "t", TOTAL, {"top": {"n": 1, "by": "x"}}, order_by=TOTAL))


def test_relative_date():
    print("\n== relative dates ==")
    date = {"Column": {"Expression": {"SourceRef": {"Source": "c"}}, "Property": "Date"}}
    now = {"Now": {}}

    def add(expr, amount, unit):
        return {"DateAdd": {"Expression": expr, "Amount": amount, "TimeUnit": unit}}

    def span(expr, unit):
        return {"DateSpan": {"Expression": expr, "TimeUnit": unit}}

    def bounds(rel):
        f = pf.filter_container("r", DATE, {"relative_date": rel}, data_type="dateTime")
        between = where(f)["Between"]
        return [f["type"], between["Expression"], between["LowerBound"], between["UpperBound"]]

    eq("the last 3 months, today included", bounds({"last": 3, "unit": "months"}),
       ["RelativeDate", date, span(add(add(now, 1, 0), -3, 2), 0), span(now, 0)])
    eq("the last 7 days, today left out", bounds({"last": 7, "unit": "days", "include_today": False}),
       ["RelativeDate", date, span(add(now, -7, 0), 0), span(add(now, -1, 0), 0)])
    eq("the next 2 weeks", bounds({"next": 2, "unit": "weeks"}),
       ["RelativeDate", date, span(now, 0), span(add(add(now, -1, 0), 2, 1), 0)])
    eq("the next year, starting tomorrow", bounds({"next": 1, "unit": "year", "include_today": False}),
       ["RelativeDate", date, span(add(now, 1, 0), 0), span(add(now, 1, 3), 0)])
    eq("this month", bounds({"this": True, "unit": "months"}),
       ["RelativeDate", date, span(now, 2), span(now, 2)])
    eq("the time units are the schema's", pf.TIME_UNITS, {"days": 0, "weeks": 1, "months": 2, "years": 3})

    refused("a column that is not a date", "needs a date column, and this one is string",
            lambda: pf.filter_container("r", CITY, {"relative_date": {"last": 1, "unit": "days"}},
                                        data_type="string"))
    refused("two spans", 'exactly one of "last", "next" or "this"', lambda: pf.filter_container(
        "r", DATE, {"relative_date": {"last": 1, "next": 1, "unit": "days"}}))
    refused("an unknown unit", '"unit" must be one of', lambda: pf.filter_container(
        "r", DATE, {"relative_date": {"last": 1, "unit": "quarters"}}))
    refused("no unit", '"unit" must be one of', lambda: pf.filter_container(
        "r", DATE, {"relative_date": {"last": 1}}))
    refused("a count of zero", '"last" must be a whole number', lambda: pf.filter_container(
        "r", DATE, {"relative_date": {"last": 0, "unit": "days"}}))
    refused("this: false", '"this" must be true', lambda: pf.filter_container(
        "r", DATE, {"relative_date": {"this": False, "unit": "days"}}))
    refused("a stray key", "unknown key(s) ['units']", lambda: pf.filter_container(
        "r", DATE, {"relative_date": {"last": 1, "units": "days"}}))
    refused("include_today that is not true or false", '"include_today" must be', lambda: pf.filter_container(
        "r", DATE, {"relative_date": {"last": 1, "unit": "days", "include_today": 1}}))
    refused("relative_date of a measure", "can only be compared", lambda: pf.filter_container(
        "r", TOTAL, {"relative_date": {"last": 1, "unit": "days"}}))


def test_binding_and_merge():
    print("\n== a page binding's filter, and whose filter is whose ==")
    eq("the filter a drillthrough field is bound to", pf.binding_filter("b1", CITY),
       {"name": "b1", "field": CITY_BY_TABLE, "type": "Categorical", "howCreated": "Drillthrough"})

    def named(name, **more):
        return {"name": name, "type": "Categorical", **more}

    theirs = {"filters": [named("desk1"), named("old_a", v=1), named("desk2"), named("old_b")],
              "filterSortOrder": "Custom"}
    before = copy.deepcopy(theirs)
    ours = [named("new_c"), named("OLD_A", v=2)]
    merged = pf.merge_filters(theirs, ours, ["old_a", "old_b"])
    eq("ours replace what an earlier apply wrote; the rest stay in place",
       merged, {"filters": [named("desk1"), named("OLD_A", v=2), named("desk2"), named("new_c")],
                "filterSortOrder": "Custom"})
    eq("neither input is changed", [theirs, ours], [before, [named("new_c"), named("OLD_A", v=2)]])
    merged["filters"][1]["v"] = 99
    eq("and the result does not share the caller's filters", ours[1]["v"], 2)
    eq("names in order", pf.filter_names(theirs), ["desk1", "old_a", "desk2", "old_b"])
    eq("names of nothing", [pf.filter_names(None), pf.filter_names({"filters": "x"})], [[], []])
    eq("a file with no filters yet", pf.merge_filters(None, [named("a")]), {"filters": [named("a")]})
    eq("taking ours away leaves theirs",
       pf.merge_filters({"filters": [named("desk1"), named("a")]}, [], ["A"]),
       {"filters": [named("desk1")]})
    eq("and nothing at all when only ours were there", pf.merge_filters({"filters": [named("a")]}, [], ["a"]),
       None)
    eq("other settings of the file survive an empty list",
       pf.merge_filters({"filters": [named("a")], "filterSortOrder": "Ascending"}, [], ["a"]),
       {"filterSortOrder": "Ascending"})
    eq("with nothing of ours named before, every filter is theirs",
       pf.merge_filters({"filters": [named("x")]}, [named("y")]), {"filters": [named("x"), named("y")]})
    again = pf.merge_filters(merged, ours, ["new_c", "OLD_A"])
    eq("merging the same filters again changes only what changed", [f["name"] for f in again["filters"]],
       ["desk1", "OLD_A", "desk2", "new_c"])


def test_purity():
    print("\n== nothing is shared between two results ==")
    definition = {"in": ["Lyon"], "display_name": "City"}
    before = copy.deepcopy(definition)
    a = pf.filter_container("a", CITY, definition)
    b = pf.filter_container("b", CITY, definition)
    eq("the definition is not changed", definition, before)
    a["filter"]["Where"][0]["Condition"]["In"]["Values"].append("x")
    eq("two containers from one definition are independent", len(where(b)["In"]["Values"]), 1)
    for doc in (a, b, pf.filter_container("t", CITY, {"top": {"n": 1, "by": "x"}}, order_by=TOTAL),
                pf.filter_container("r", DATE, {"relative_date": {"this": True, "unit": "years"}})):
        json.dumps(doc)
    check("every container is JSON-serialisable", True)


if __name__ == "__main__":
    for fn in (test_expressions, test_basic, test_advanced, test_top_n, test_relative_date,
               test_binding_and_merge, test_purity):
        fn()
    print()
    if _failures:
        print(f"FAILED: {len(_failures)} check(s): {_failures}")
        sys.exit(1)
    print("ALL PBIR FILTER TESTS PASSED")
