"""
Tests for src/model_embed.py: the embedded-CSV Power Query expression (round trip through
raw deflate and base64 in pure Python), type inference, and embed_tables on a synthetic TMDL
model in a temp folder: only the partition source changes, files keep their bytes elsewhere,
every refusal leaves the model untouched, and a path parameter is removed only when nothing
uses it any more. Power Query is never run; no Power BI required.
Run: python tests/test_model_embed.py
"""
import base64
import os
import re
import sys
import tempfile
import zlib

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))
import model_embed as me  # noqa: E402

_failures = []


def check(name, cond, detail=""):
    line = f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f": {detail}" if detail and not cond else "")
    print(line.encode("ascii", "backslashreplace").decode("ascii"))
    if not cond:
        _failures.append(name)


def eq(name, actual, expected):
    check(name, actual == expected, f"got {actual!r}, expected {expected!r}")


def raises(name, fn, fragment=""):
    try:
        fn()
    except me.EmbedError as e:
        check(name, fragment.lower() in str(e).lower(), f"message was {e}")
    else:
        check(name, False, "no EmbedError raised")


def raw(path):
    with open(path, "rb") as f:
        return f.read()


def put(path, data: bytes):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        f.write(data)


CSV = ('id,name,amount,day,flag,code\n'
       '1,"Widget, large",12.50,2024-03-09,true,007\n'
       '2,"He said ""hi""",-3,2024-12-31,false,010\n'
       '3,"two\nlines",,2025-01-01,true,123\n'
       '4,' + "caf" + chr(0xE9) + ',1e3,2025-02-28,false,9\n')


def test_csv_partition():
    print("\n== embed_csv_partition ==")
    m = me.embed_csv_partition(CSV)
    lines = m.split("\n")
    eq("a let expression", (lines[0], lines[-2], lines[-1]), ("let", "in", '    #"Changed Type"'))
    check("reads the data with Binary.Decompress over base64 text",
          "Csv.Document(Binary.Decompress(Binary.FromText(\"" in m and "BinaryEncoding.Base64), Compression.Deflate)" in m)
    check("csv options pin the shape and the encoding", '[Delimiter=",", Columns=6, Encoding=65001, QuoteStyle=QuoteStyle.Csv]' in m)
    check("headers are promoted", 'Table.PromoteHeaders(Source, [PromoteAllScalars=true])' in m)
    check("types use a fixed culture", m.count('"en-US")') == 1)
    eq("the csv comes back exactly", me.decode_embedded_csv(m), CSV)
    b64 = re.search(r'Binary\.FromText\("([^"]*)"', m).group(1)
    data = base64.b64decode(b64)
    eq("it is a raw deflate stream", zlib.decompress(data, -15).decode("utf-8"), CSV)
    try:
        zlib.decompress(data)
        check("there is no zlib header", False, "plain zlib accepted it")
    except zlib.error:
        check("there is no zlib header", True)
    check("the expression has no line longer than its data line needs", len(lines) == 7, len(lines))

    types = me.infer_column_types(CSV)
    eq("inferred types", types, {"id": "int64", "name": "text", "amount": "double", "day": "date",
                                 "flag": "boolean", "code": "text"})
    check("types are written as M types",
          '{"id", Int64.Type}, {"name", type text}, {"amount", type number}, {"day", type date}, '
          '{"flag", type logical}, {"code", type text}' in m, m[-400:])
    check("empty fields become null in typed columns only",
          'Table.ReplaceValue(#"Promoted Headers", "", null, Replacer.ReplaceValue, {"id", "amount", "day", "flag"})' in m, m)

    explicit = me.embed_csv_partition(CSV, column_types={"amount": "decimal", "code": "Int64.Type", "day": "datetime"})
    check("explicit types win over inference",
          '{"amount", Currency.Type}' in explicit and '{"code", Int64.Type}' in explicit and '{"day", type datetime}' in explicit)
    plain = me.embed_csv_partition(CSV, infer_types=False)
    check("without inference every column is text and no null step is needed",
          plain.count("type text") == 6 and "Blank To Null" not in plain)
    semi = me.embed_csv_partition("a;b\n1;x,y\n", delimiter=";")
    check("another delimiter is carried into the query", '[Delimiter=";", Columns=2' in semi and me.decode_embedded_csv(semi) == 'a;b\n1;x,y\n')
    tab = me.embed_csv_partition("a\tb\n1\t2\n", delimiter="\t")
    check("a tab delimiter uses the M escape", 'Delimiter="#(tab)"' in tab)
    crlf = me.embed_csv_partition(chr(0xFEFF) + "a,b\r\n1,2\r\n")
    eq("a BOM and CRLF are normalised", me.decode_embedded_csv(crlf), "a,b\n1,2\n")
    quoted = me.embed_csv_partition('"say ""x""",b\n1,2\n')
    check("a quote in a column name is doubled in M", '{"say ""x""", Int64.Type}' in quoted, quoted[-200:])

    raises("an empty csv", lambda: me.embed_csv_partition(""), "empty")
    raises("a ragged row", lambda: me.embed_csv_partition("a,b\n1\n"), "line 2")
    raises("a repeated header", lambda: me.embed_csv_partition("a,A\n1,2\n"), "repeats")
    raises("an empty header name", lambda: me.embed_csv_partition("a,,c\n1,2,3\n"), "empty column name")
    raises("a type for a missing column", lambda: me.embed_csv_partition("a\n1\n", column_types={"z": "text"}), "does not have")
    raises("an unknown type", lambda: me.embed_csv_partition("a\n1\n", column_types={"a": "varchar"}), "unknown column type")
    raises("a bad delimiter", lambda: me.embed_csv_partition("a\n1\n", delimiter="||"), "single character")
    raises("decode needs an embedded expression", lambda: me.decode_embedded_csv("let x = 1 in x"), "no binary.fromtext")
    eq("leading zeros stay text; big integers stay whole",
       me.infer_column_types("zip,n,ts\n00123,9007199254740993,2024-01-01 10:30\n"),
       {"zip": "text", "n": "int64", "ts": "datetime"})


# ---------------------------------------------------------------- a synthetic model

T = "\t"
SALES = (
    "table Sales\r\n"
    "\tlineageTag: 11111111-aaaa\r\n"
    "\r\n"
    "\tmeasure 'Total Amount' = SUM ( Sales[Amount] )\r\n"
    "\t\tformatString: #,0\r\n"
    "\r\n"
    "\tcolumn OrderKey\r\n"
    "\t\tdataType: int64\r\n"
    "\t\tsummarizeBy: none\r\n"
    "\t\tsourceColumn: OrderKey\r\n"
    "\r\n"
    "\tcolumn 'Order Date'\r\n"
    "\t\tdataType: dateTime\r\n"
    "\t\tformatString: Short Date\r\n"
    "\t\tsourceColumn: Order Date\r\n"
    "\r\n"
    "\tcolumn Amount\r\n"
    "\t\tdataType: decimal\r\n"
    "\t\tsourceColumn: Amount\r\n"
    "\r\n"
    "\tcolumn Weight\r\n"
    "\t\tdataType: double\r\n"
    "\t\tsourceColumn: Weight\r\n"
    "\r\n"
    "\tcolumn Online\r\n"
    "\t\tdataType: boolean\r\n"
    "\t\tsourceColumn: Online\r\n"
    "\r\n"
    "\tcolumn Double = Sales[Amount] * 2\r\n"
    "\t\tdataType: decimal\r\n"
    "\r\n"
    "\tpartition Sales = m\r\n"
    "\t\tmode: import\r\n"
    "\t\tsource =\r\n"
    "\t\t\t\tlet\r\n"
    "\t\t\t\t    Source = Csv.Document(File.Contents(DataFolder & \"\\sales.csv\"), [Delimiter=\",\"]),\r\n"
    "\r\n"
    "\t\t\t\t    Cut = Table.SelectRows(Source, each [Date] <= AsOf)\r\n"
    "\t\t\t\tin\r\n"
    "\t\t\t\t    Helper(Cut)\r\n"
    "\r\n"
    "\tannotation PBI_ResultType = Table\r\n"
    "\r\n"
)
PRODUCT = (
    "table 'Dim Product'\n"
    "\n"
    "\tcolumn ProductKey\n"
    "\t\tdataType: int64\n"
    "\t\tsourceColumn: ProductKey\n"
    "\n"
    "\tcolumn 'Product Name'\n"
    "\t\tdataType: string\n"
    "\t\tsourceColumn: Product Name\n"
    "\n"
    "\tpartition 'Dim Product' = m\n"
    "\t\tmode: import\n"
    "\t\tsource = ```\n"
    "\t\t\t\tlet Source = Excel.Workbook(File.Contents(#\"Data Folder\" & \"\\products.xlsx\")) in Source\n"
    "\t\t\t\t```\n"
    "\n"
    "\tannotation PBI_ResultType = Table\n"
)
OTHER = (
    "table Budget\n"
    "\n"
    "\tcolumn Year\n"
    "\t\tdataType: int64\n"
    "\t\tsourceColumn: Year\n"
    "\n"
    "\tpartition Budget = m\n"
    "\t\tmode: import\n"
    "\t\tsource = let Source = Csv.Document(File.Contents(DataFolder & \"\\budget.csv\")) in Source\n"
)
CALC = (
    "table Calendar\n"
    "\n"
    "\tcolumn Date\n"
    "\t\tdataType: dateTime\n"
    "\t\tisNameInferred\n"
    "\t\tsourceColumn: [Date]\n"
    "\n"
    "\tpartition Calendar = calculated\n"
    "\t\tmode: import\n"
    "\t\tsource = CALENDAR ( DATE ( 2024, 1, 1 ), DATE ( 2024, 12, 31 ) )\n"
)
EXPRESSIONS = (
    "/// Folder that holds the source files.\n"
    "expression DataFolder = \"C:\\data\\files\" meta [IsParameterQuery=true, Type=\"Text\", IsParameterQueryRequired=true]\n"
    "\tlineageTag: 2222\n"
    "\n"
    "\tannotation PBI_ResultType = Text\n"
    "\n"
    "expression 'Data Folder' = \"C:\\data\\other\" meta [IsParameterQuery=true, Type=\"Text\"]\n"
    "\tlineageTag: 3333\n"
    "\n"
    "/// Cut-off date.\n"
    "expression AsOf = #date(2025, 12, 31) meta [IsParameterQuery=true, Type=\"Date\"]\n"
    "\tlineageTag: 4444\n"
    "\n"
    "expression Helper =\n"
    "\t\t(t as table) => Table.SelectRows(t, each [Date] <= AsOf)\n"
    "\tlineageTag: 5555\n"
    "\n"
)
MODEL = (
    "model Model\n"
    "\tculture: en-US\n"
    "\n"
    "annotation PBI_QueryOrder = [\"Sales\",\"Dim Product\",\"DataFolder\",\"Data Folder\",\"AsOf\",\"Helper\",\"Budget\"]\n"
    "\n"
    "ref table Sales\n"
    "ref table 'Dim Product'\n"
    "ref table Budget\n"
)
SALES_CSV = ("Order Date,OrderKey,Amount,Weight,Online,Note\n"
             "2024-03-09,1,12.50,0.5,true,x\n"
             "2024-12-31,2,-3,,false,\"y, z\"\n")
PRODUCT_CSV = "ProductKey,Product Name\n1,\"Bolt, M8\"\n2,Nut\n"
BUDGET_CSV = "Year\n2024\n2025\n"


def make_model():
    root = tempfile.mkdtemp()
    d = os.path.join(root, "Demo.SemanticModel", "definition")
    files = {
        os.path.join(d, "tables", "Sales.tmdl"): SALES.encode("utf-8"),
        os.path.join(d, "tables", "Dim Product.tmdl"): b"\xef\xbb\xbf" + PRODUCT.encode("utf-8"),
        os.path.join(d, "tables", "Budget.tmdl"): OTHER.encode("utf-8"),
        os.path.join(d, "tables", "Calendar.tmdl"): CALC.encode("utf-8"),
        os.path.join(d, "expressions.tmdl"): EXPRESSIONS.encode("utf-8"),
        os.path.join(d, "model.tmdl"): MODEL.encode("utf-8"),
        os.path.join(root, "sales.csv"): SALES_CSV.encode("utf-8"),
        os.path.join(root, "products.csv"): b"\xef\xbb\xbf" + PRODUCT_CSV.replace("\n", "\r\n").encode("utf-8"),
        os.path.join(root, "budget.csv"): BUDGET_CSV.encode("utf-8"),
    }
    for path, data in files.items():
        put(path, data)
    return root, d


def snapshot(d):
    out = {}
    for folder, _, names in os.walk(d):
        for n in names:
            out[os.path.join(folder, n)] = raw(os.path.join(folder, n))
    return out


def test_embed_tables():
    print("\n== embed_tables: two tables, one query still on disk ==")
    root, d = make_model()
    before = snapshot(d)
    targets = {"Sales": os.path.join(root, "sales.csv"), "dim product": os.path.join(root, "products.csv")}

    dry = me.embed_tables(d, targets, dry_run=True)
    eq("a dry run writes nothing", snapshot(d), before)
    eq("a dry run lists what would change",
       sorted(os.path.basename(p) for p in dry["changed_files"]), ["Dim Product.tmdl", "Sales.tmdl", "expressions.tmdl", "model.tmdl"])
    check("a dry run reports sizes", dry["dry_run"] and dry["totals"]["rows"] == 4 and dry["totals"]["embedded_chars"] > 0, dry["totals"])

    r = me.embed_tables(os.path.join(root, "Demo.SemanticModel"), targets)
    after = snapshot(d)
    check("the .SemanticModel folder is accepted", r["success"] and not r["dry_run"])
    eq("changed files are reported with their real names",
       sorted(os.path.basename(p) for p in r["changed_files"]), ["Dim Product.tmdl", "Sales.tmdl", "expressions.tmdl", "model.tmdl"])
    eq("untouched files keep their bytes",
       [n for n in ("Budget.tmdl", "Calendar.tmdl") if after[os.path.join(d, "tables", n)] != before[os.path.join(d, "tables", n)]], [])

    sales_path = os.path.join(d, "tables", "Sales.tmdl")
    new, old = after[sales_path], before[sales_path]
    head = old[:old.index(b"\t\tsource =\r\n")]
    tail = old[old.index(b"\r\n\tannotation PBI_ResultType"):]
    check("everything before the source block is byte for byte the same", new.startswith(head + b"\t\tsource =\r\n\t\t\t\tlet\r\n"))
    check("everything after the source block is byte for byte the same", new.endswith(tail))
    check("CRLF line endings are kept", b"\n" not in new.replace(b"\r\n", b""))
    check("the old query is gone", b"File.Contents" not in new and b"Helper(Cut)" not in new)
    text = new.decode("utf-8")
    eq("the embedded data is the csv, in model column order, without the extra column",
       me.decode_embedded_csv(text), "OrderKey,Order Date,Amount,Weight,Online\n1,2024-03-09,12.50,0.5,true\n2,2024-12-31,-3,,false\n")
    check("column types come from the model, not from the values",
          '{"OrderKey", Int64.Type}, {"Order Date", type date}, {"Amount", Currency.Type}, {"Weight", type number}, '
          '{"Online", type logical}' in text, text[-500:])
    check("the body keeps the file's own indentation", "\r\n\t\t\t\tin\r\n\t\t\t\t    #\"Changed Type\"\r\n\r\n\tannotation" in text)
    sales = next(t for t in r["tables"] if t["table"] == "Sales")
    eq("the calculated column is not asked of the csv", sorted(sales["column_types"]), ["Amount", "Online", "Order Date", "OrderKey", "Weight"])
    check("the dropped csv column is reported", any("Note" in w for w in sales["warnings"]), sales["warnings"])
    eq("row and column counts", (sales["rows"], sales["columns"]), (2, 5))

    product = after[os.path.join(d, "tables", "Dim Product.tmdl")]
    check("the byte order mark is kept", product.startswith(b"\xef\xbb\xbf"))
    ptext = product[3:].decode("utf-8")
    check("a fenced source is replaced whole", "```" not in ptext and "\t\tsource =\n\t\t\t\tlet\n" in ptext, ptext[-300:])
    check("the rest of that file is unchanged", ptext.startswith(PRODUCT[:PRODUCT.index("\t\tsource =")]) and ptext.endswith("\n\tannotation PBI_ResultType = Table\n"))
    eq("csv with BOM and CRLF is embedded normalised", me.decode_embedded_csv(ptext), PRODUCT_CSV)

    eq("only the parameter nothing uses any more is removed", r["removed_expressions"], ["Data Folder"])
    reasons = {k["name"]: k["reason"] for k in r["kept_expressions"]}
    check("a parameter another table still reads is kept", "Budget" in reasons.get("DataFolder", ""), reasons)
    check("a parameter a shared query still reads is kept", "expression Helper" in reasons.get("AsOf", ""), reasons)
    check("a shared function is never removed", "not a parameter" in reasons.get("Helper", ""), reasons)
    expr = after[os.path.join(d, "expressions.tmdl")].decode("utf-8")
    eq("the removed block is cut out cleanly",
       expr, EXPRESSIONS.replace("expression 'Data Folder' = \"C:\\data\\other\" meta [IsParameterQuery=true, Type=\"Text\"]\n\tlineageTag: 3333\n\n", ""))
    model = after[os.path.join(d, "model.tmdl")].decode("utf-8")
    eq("its query-order entry is removed and nothing else", model, MODEL.replace('"Data Folder",', ""))
    eq("the query that still reads a file is reported",
       r["external_sources"], [{"object": "table Budget", "functions": ["File.Contents"]}])
    check("so the model is not reported as self-contained", r["self_contained"] is False and any("external_sources" in n for n in r["notes"]))

    again = me.embed_tables(d, targets)
    eq("running it again changes nothing", again["changed_files"], [])

    print("\n== embed_tables: the last file query ==")
    last = me.embed_tables(d, {"Budget": os.path.join(root, "budget.csv")})
    eq("now the folder parameter goes too", last["removed_expressions"], ["DataFolder"])
    expr = raw(os.path.join(d, "expressions.tmdl")).decode("utf-8")
    check("with its description, and the others stay", "DataFolder" not in expr and "Folder that holds" not in expr
          and expr.startswith("/// Cut-off date.\nexpression AsOf") and "expression Helper =" in expr, expr)
    model = raw(os.path.join(d, "model.tmdl")).decode("utf-8")
    check("the query order is still a valid list", '["Sales","Dim Product","AsOf","Helper","Budget"]' in model, model)
    check("the model is self-contained", last["self_contained"] and last["external_sources"] == [])
    budget = raw(os.path.join(d, "tables", "Budget.tmdl")).decode("utf-8")
    check("a one-line source becomes a block two tabs deeper", "\t\tsource =\n\t\t\t\tlet\n\t\t\t\t    Source = Csv.Document(Binary.Decompress(" in budget, budget[:400])
    eq("and decodes to its csv", me.decode_embedded_csv(budget), BUDGET_CSV)


def test_parameter_file_removed():
    print("\n== embed_tables: the only expression is the parameter ==")
    root, d = make_model()
    put(os.path.join(d, "expressions.tmdl"),
        b"expression DataFolder = \"C:\\x\" meta [IsParameterQuery=true, Type=\"Text\"]\n\tlineageTag: 1\n")
    sales = SALES.replace("each [Date] <= AsOf", "each true").replace("Helper(Cut)", "Cut")
    put(os.path.join(d, "tables", "Sales.tmdl"), sales.encode("utf-8"))
    put(os.path.join(d, "model.tmdl"), b"model Model\n\nannotation PBI_QueryOrder = [\"DataFolder\"]\n\nref expression DataFolder\nref table Sales\n")
    os.remove(os.path.join(d, "tables", "Budget.tmdl"))
    os.remove(os.path.join(d, "tables", "Dim Product.tmdl"))
    r = me.embed_tables(d, {"Sales": os.path.join(root, "sales.csv")})
    eq("the parameter is removed", r["removed_expressions"], ["DataFolder"])
    check("an expressions file left empty is deleted", not os.path.exists(os.path.join(d, "expressions.tmdl")))
    eq("the query order and the ref line follow",
       raw(os.path.join(d, "model.tmdl")), b"model Model\n\nannotation PBI_QueryOrder = []\n\nref table Sales\n")
    check("the deleted file is among the changed files", any(p.endswith("expressions.tmdl") for p in r["changed_files"]), r["changed_files"])

    print("\n== embed_tables: only path parameters are removed ==")
    root, d = make_model()
    others = ("expression Region = \"EU\" meta [IsParameterQuery=true, Type=\"Text\"]\n\tlineageTag: 7\n\n"
              "expression Cutoff = #date(2025, 1, 1) meta [IsParameterQuery=true, Type=\"Date\"]\n\tlineageTag: 8\n\n")
    paths = ("expression DataFolder = \"C:\\x\" meta [IsParameterQuery=true, Type=\"Text\"]\n\tlineageTag: 1\n\n"
             "expression Share = \"\\\\server\\drop\" meta [IsParameterQuery=true, Type=\"Text\"]\n\tlineageTag: 2\n\n"
             "expression Feed = \"https://example.invalid/data\" meta [IsParameterQuery=true, Type=\"Text\"]\n\tlineageTag: 3\n\n")
    put(os.path.join(d, "expressions.tmdl"), (paths + others).encode("utf-8"))
    sales = (SALES.replace("each [Date] <= AsOf", "each [Region] = Region and [Date] <= Cutoff")
             .replace("Helper(Cut)", "Table.Combine({Cut, Csv.Document(File.Contents(Share)), Csv.Document(Web.Contents(Feed))})"))
    put(os.path.join(d, "tables", "Sales.tmdl"), sales.encode("utf-8"))
    os.remove(os.path.join(d, "tables", "Budget.tmdl"))
    os.remove(os.path.join(d, "tables", "Dim Product.tmdl"))
    r = me.embed_tables(d, {"Sales": os.path.join(root, "sales.csv")})
    eq("a drive path, a share and a URL are removed", r["removed_expressions"], ["DataFolder", "Share", "Feed"])
    eq("a plain text parameter and a date parameter are kept",
       [(k["name"], "not a file path" in k["reason"]) for k in r["kept_expressions"]], [("Region", True), ("Cutoff", True)])
    eq("the kept parameters are left exactly as they were", raw(os.path.join(d, "expressions.tmdl")).decode("utf-8"), others)


def test_refusals_leave_the_model_alone():
    print("\n== embed_tables: refusals ==")
    root, d = make_model()
    before = snapshot(d)
    sales_csv = os.path.join(root, "sales.csv")

    def refused(name, fragment, **kw):
        tables = kw.pop("tables")
        raises(name, lambda: me.embed_tables(d, tables, **kw), fragment)
        eq(f"{name}: nothing was written", snapshot(d), before)

    refused("an unknown table", "not in the model", tables={"Nope": sales_csv})
    refused("a missing csv", "does not exist", tables={"Sales": os.path.join(root, "none.csv")})
    refused("an oversized csv", "over the 40-byte limit", tables={"Sales": sales_csv}, max_bytes=40)
    refused("a calculated table", "calculated table", tables={"Calendar": sales_csv})
    put(os.path.join(root, "short.csv"), b"OrderKey,Amount\n1,2\n")
    refused("a csv without the source columns", "source column names", tables={"Sales": os.path.join(root, "short.csv")})
    refused("one bad table stops the whole batch", "not in the model",
            tables={"Sales": sales_csv, "Dim Product": os.path.join(root, "products.csv"), "Ghost": sales_csv})
    refused("no tables", "empty", tables={})
    raises("a folder that is not a model", lambda: me.embed_tables(root, {"Sales": sales_csv}), "not a tmdl model definition")

    two = SALES.replace("\tannotation PBI_ResultType = Table\r\n",
                        "\tpartition Sales2 = m\r\n\t\tmode: import\r\n\t\tsource = let a = 1 in a\r\n")
    put(os.path.join(d, "tables", "Sales.tmdl"), two.encode("utf-8"))
    raises("two partitions", lambda: me.embed_tables(d, {"Sales": sales_csv}), "2 partitions")
    put(os.path.join(d, "tables", "Sales.tmdl"), SALES.replace("partition Sales = m", "partition Sales = entity").encode("utf-8"))
    raises("a non-query partition", lambda: me.embed_tables(d, {"Sales": sales_csv}), "'entity' partition")
    put(os.path.join(root, "latin.csv"), "OrderKey\n1\n".encode("utf-8") + b"\xff\xfe")
    put(os.path.join(d, "tables", "Sales.tmdl"), SALES.encode("utf-8"))
    raises("a csv that is not utf-8", lambda: me.embed_tables(d, {"Sales": os.path.join(root, "latin.csv")}), "not utf-8")


if __name__ == "__main__":
    for fn in (test_csv_partition, test_embed_tables, test_parameter_file_removed, test_refusals_leave_the_model_alone):
        fn()
    print()
    if _failures:
        print(f"FAILED: {len(_failures)} check(s): {_failures}")
        sys.exit(1)
    print("ALL MODEL EMBED TESTS PASSED")
