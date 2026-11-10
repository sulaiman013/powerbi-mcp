"""
Synthetic PBIP projects for the offline-path tests (not a test suite itself).

Everything is built in a temp folder by the calling test. The shapes mirror what Power BI
Desktop writes: TMDL indented with tabs, relationships as Table.Column paths, and PBIR field
references as {"Column": {"Expression": {"SourceRef": {"Entity": T}}, "Property": C}} with
filters that reach the table through a From alias.
"""
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))
from powerbi_pbip_connector import PowerBIPBIPConnector  # noqa: E402

BOM = bytes([0xEF, 0xBB, 0xBF])


# ---------------------------------------------------------------- files

def write(path: Path, text: str, bom: bool = False, crlf: bool = False) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    body = text.replace("\r\n", "\n")
    if crlf:
        body = body.replace("\n", "\r\n")
    path.write_bytes((BOM if bom else b"") + body.encode("utf-8"))
    return path


def write_json(path: Path, obj: Any, crlf: bool = False) -> Path:
    return write(path, json.dumps(obj, indent=2, ensure_ascii=False), crlf=crlf)


def read(path: Path) -> str:
    return path.read_bytes().decode("utf-8-sig").replace("\r\n", "\n")


def load_json(path: Path) -> Any:
    return json.loads(read(path))


def snapshot(root: Path) -> Dict[str, bytes]:
    """{relative path: bytes} of every file under ``root`` (backups excluded)."""
    return {p.relative_to(root).as_posix(): p.read_bytes()
            for p in sorted(root.rglob("*")) if p.is_file() and ".pbip-backups" not in p.parts}


# ---------------------------------------------------------------- PBIR expression shapes

def source(entity: str) -> Dict[str, Any]:
    return {"SourceRef": {"Entity": entity}}


def column(entity: str, prop: str) -> Dict[str, Any]:
    return {"Column": {"Expression": source(entity), "Property": prop}}


def measure(entity: str, prop: str) -> Dict[str, Any]:
    return {"Measure": {"Expression": source(entity), "Property": prop}}


def aggregation(entity: str, prop: str, function: int = 0) -> Dict[str, Any]:
    return {"Aggregation": {"Expression": column(entity, prop), "Function": function}}


def hierarchy_level(entity: str, hierarchy: str, level: str) -> Dict[str, Any]:
    return {"HierarchyLevel": {
        "Expression": {"Hierarchy": {"Expression": source(entity), "Hierarchy": hierarchy}},
        "Level": level}}


def variation_level(entity: str, date_column: str, level: str) -> Dict[str, Any]:
    """A level of the automatic date hierarchy Desktop generates for a date column."""
    return {"HierarchyLevel": {
        "Expression": {"Hierarchy": {
            "Expression": {"PropertyVariationSource": {
                "Expression": source(entity), "Name": "Variation", "Property": date_column}},
            "Hierarchy": "Date Hierarchy"}},
        "Level": level}}


def projection(field: Dict[str, Any], query_ref: str, native: Optional[str] = None,
               display: Optional[str] = None, active: bool = False) -> Dict[str, Any]:
    out: Dict[str, Any] = {"field": field, "queryRef": query_ref}
    if native is not None:
        out["nativeQueryRef"] = native
    if display is not None:
        out["displayName"] = display
    if active:
        out["active"] = True
    return out


def literal(text: str) -> Dict[str, Any]:
    """A PBIR string literal property: the text in single quotes with '' escapes."""
    return {"expr": {"Literal": {"Value": "'" + text.replace("'", "''") + "'"}}}


def categorical_filter(name: str, entity: str, prop: str, alias: str, values: List[str],
                       kind: str = "Column") -> Dict[str, Any]:
    """A filter card as Desktop stores it: the field itself by Entity, the condition through a
    From alias (SourceRef.Source)."""
    field = column(entity, prop) if kind == "Column" else measure(entity, prop)
    aliased = {kind: {"Expression": {"SourceRef": {"Source": alias}}, "Property": prop}}
    return {
        "name": name, "field": field, "type": "Categorical",
        "filter": {
            "Version": 2,
            "From": [{"Name": alias, "Entity": entity, "Type": 0}],
            "Where": [{"Condition": {"In": {
                "Expressions": [aliased],
                "Values": [[{"Literal": {"Value": "'" + v + "'"}}] for v in values]}}}],
        },
    }


def visual(name: str, visual_type: str, roles: Dict[str, List[Dict[str, Any]]],
           objects: Optional[Dict[str, Any]] = None, filters: Optional[List[Dict[str, Any]]] = None,
           sort: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
    query: Dict[str, Any] = {"queryState": {role: {"projections": projs} for role, projs in roles.items()}}
    if sort is not None:
        query["sortDefinition"] = {"sort": sort, "isDefaultSort": True}
    body: Dict[str, Any] = {"visualType": visual_type, "query": query, "drillFilterOtherVisuals": True}
    if objects is not None:
        body["objects"] = objects
    out: Dict[str, Any] = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/"
                   "visualContainer/2.7.0/schema.json",
        "name": name,
        "position": {"x": 0, "y": 0, "z": 0, "width": 400, "height": 300, "tabOrder": 0},
        "visual": body,
    }
    if filters is not None:
        out["filterConfig"] = {"filters": filters}
    return out


def deneb_objects(spec: Dict[str, Any], config: Dict[str, Any], fields: List[str]) -> Dict[str, Any]:
    """The formatting objects of a Deneb visual: spec and config as compact JSON inside
    string literals, and the field-tracking map keyed by dataset field name."""
    compact = dict(separators=(",", ":"), ensure_ascii=False)
    tracking = {f: {"highlight": False, "format": True, "formatted": True} for f in fields}
    return {
        "vega": [{"properties": {
            "provider": literal("vegaLite"),
            "jsonSpec": literal(json.dumps(spec, **compact)),
            "jsonConfig": literal(json.dumps(config, **compact)),
        }}],
        "stateManagement": [{"properties": {
            "viewportHeight": {"expr": {"Literal": {"Value": "614D"}}},
            "supportFieldConfiguration": literal(json.dumps(tracking, **compact)),
        }}],
    }


def literal_json(prop: Dict[str, Any]) -> Any:
    """Decode a property written by ``literal`` whose text is JSON."""
    value = prop["expr"]["Literal"]["Value"]
    return json.loads(value[1:-1].replace("''", "'"))


# ---------------------------------------------------------------- a model

MODEL = """model Model
\tculture: en-US
\tdefaultPowerBIDataSourceVersion: powerBI_V3

annotation PBI_QueryOrder = ["Sales","Customers","Dim Date","_Measures"]

ref table Sales
ref table Customers
ref table 'Dim Date'
ref table _Measures

ref cultureInfo en-US
"""

DATABASE = "database\n\tcompatibilityLevel: 1567\n"

RELATIONSHIPS = """relationship 30950dc3-809a-47bb-bf13-3fe126f7a282
\tfromColumn: Sales.CustomerID
\ttoColumn: Customers.ID

relationship 'Sales to Dim Date'
\tfromColumn: Sales.'Order Date'
\ttoColumn: 'Dim Date'.Date
"""

SALES = """table Sales
\tlineageTag: 11111111-1111-1111-1111-111111111111

\tcolumn ID
\t\tdataType: int64
\t\tsummarizeBy: none
\t\tsourceColumn: ID

\tcolumn CustomerID
\t\tdataType: int64
\t\tsummarizeBy: none
\t\tsourceColumn: CustomerID

\tcolumn Revenue
\t\tdataType: double
\t\tsummarizeBy: sum
\t\tsourceColumn: Revenue

\tcolumn 'Order Date'
\t\tdataType: dateTime
\t\tsummarizeBy: none
\t\tsourceColumn: Order Date

\tcolumn Region
\t\tdataType: string
\t\tsummarizeBy: none
\t\tsourceColumn: Region

\tcolumn Margin = [Revenue] * 0.2
\t\tdataType: double
\t\tsummarizeBy: sum

\tcolumn 'ID Label' = "#" & [ID]
\t\tdataType: string
\t\tsummarizeBy: none

\tpartition Sales = m
\t\tmode: import
\t\tsource =
\t\t\t\tlet
\t\t\t\t    Source = Sql.Database("srv", "db"),
\t\t\t\t    Data = Source{[Schema="dbo",Item="Sales"]}[Data],
\t\t\t\t    Kept = Table.SelectRows(Data, each [Revenue] > 0)
\t\t\t\tin
\t\t\t\t    Kept

\tannotation PBI_ResultType = Table
"""

CUSTOMERS = """table Customers
\tlineageTag: 22222222-2222-2222-2222-222222222222

\tcolumn ID
\t\tdataType: int64
\t\tsummarizeBy: none
\t\tsourceColumn: ID

\tcolumn Name
\t\tdataType: string
\t\tsummarizeBy: none
\t\tsourceColumn: Name
\t\tsortByColumn: ID

\tcolumn Region
\t\tdataType: string
\t\tsummarizeBy: none
\t\tsourceColumn: Region

\thierarchy Geography

\t\tlevel Region
\t\t\tcolumn: Region

\t\tlevel Name
\t\t\tcolumn: Name

\tpartition Customers = m
\t\tmode: import
\t\tsource =
\t\t\t\tlet
\t\t\t\t    Source = Sql.Database("srv", "db"),
\t\t\t\t    Data = Source{[Schema="dbo",Item="Customers"]}[Data],
\t\t\t\t    Joined = Table.NestedJoin(Data, {"ID"}, Sales, {"CustomerID"}, "Sales", JoinKind.LeftOuter),
\t\t\t\t    Both = Table.Combine({Sales, Joined})
\t\t\t\tin
\t\t\t\t    Both

\tannotation PBI_ResultType = Table
"""

DIM_DATE = """table 'Dim Date'
\tlineageTag: 33333333-3333-3333-3333-333333333333
\tdataCategory: Time

\tcolumn Date
\t\tdataType: dateTime
\t\tisKey
\t\tsummarizeBy: none
\t\tisNameInferred
\t\tsourceColumn: [Date]

\tcolumn Month
\t\tdataType: string
\t\tsummarizeBy: none
\t\tsortByColumn: 'Month Number'
\t\tisNameInferred
\t\tsourceColumn: [Month]

\tcolumn 'Month Number'
\t\tdataType: int64
\t\tisHidden
\t\tsummarizeBy: none
\t\tisNameInferred
\t\tsourceColumn: [Month Number]

\thierarchy Calendar

\t\tlevel Month
\t\t\tcolumn: Month

\t\tlevel Date
\t\t\tcolumn: Date

\tpartition 'Dim Date' = calculated
\t\tmode: import
\t\tsource =
\t\t\t\tADDCOLUMNS(
\t\t\t\t    CALENDAR(MIN(Sales[Order Date]), MAX(Sales[Order Date])),
\t\t\t\t    "Month", FORMAT([Date], "MMM"),
\t\t\t\t    "Month Number", MONTH([Date])
\t\t\t\t)
"""

MEASURES = """table _Measures
\tlineageTag: 44444444-4444-4444-4444-444444444444

\t/// Total revenue across all sales.
\tmeasure Revenue = SUM(Sales[Revenue])
\t\tformatString: #,0
\t\tlineageTag: 55555555-5555-5555-5555-555555555555

\t\tannotation PBI_FormatHint = {"isGeneralNumber":true}

\tmeasure 'Rev x2' = [Revenue] * 2
\t\tlineageTag: 66666666-6666-6666-6666-666666666666

\tmeasure 'Revenue per Customer' =
\t\t\tVAR _rows = Sales
\t\t\tVAR _n = COUNTROWS(Customers)
\t\t\tRETURN
\t\t\t    DIVIDE(SUMX(_rows, Sales[Revenue]), _n) -- uses Sales[Revenue]
\t\tformatString: #,0.00
\t\tlineageTag: 77777777-7777-7777-7777-777777777777

\tmeasure 'East Revenue' = CALCULATE([Revenue], Sales[Region] = "East", Customers[Region] = "East")
\t\tlineageTag: 88888888-8888-8888-8888-888888888888

\tmeasure Label = "Sales[Revenue] is " & FORMAT([Revenue], "#,0")
\t\tlineageTag: 99999999-9999-9999-9999-999999999999

\tpartition _Measures = calculated
\t\tmode: import
\t\tsource = {1}
"""

ROLE = """role Reader
\tmodelPermission: read

\ttablePermission Sales = Sales[Region] = "East" && [Revenue] > 0

\ttablePermission Customers = Customers[Region] = "East"
"""

PERSPECTIVE = """perspective Core
\tperspectiveTable Sales
\t\tperspectiveColumn Revenue
\t\tperspectiveColumn Region

\tperspectiveTable _Measures
\t\tperspectiveMeasure Revenue
"""

CULTURE = """cultureInfo en-US

\tlinguisticMetadata =
\t\t\t{
\t\t\t  "Version": "1.0.0",
\t\t\t  "Language": "en-US",
\t\t\t  "Entities": {
\t\t\t    "sale": {
\t\t\t      "Definition": {"Binding": {"ConceptualEntity": "Sales"}},
\t\t\t      "State": "Generated",
\t\t\t      "Terms": [{"sale": {"State": "Generated"}}]
\t\t\t    },
\t\t\t    "sale.revenue": {
\t\t\t      "Definition": {"Binding": {"ConceptualEntity": "Sales", "ConceptualProperty": "Revenue"}},
\t\t\t      "State": "Generated",
\t\t\t      "Terms": [{"revenue": {"State": "Generated"}}]
\t\t\t    },
\t\t\t    "measure.revenue": {
\t\t\t      "Definition": {"Binding": {"ConceptualEntity": "_Measures", "ConceptualProperty": "Revenue"}},
\t\t\t      "State": "Generated"
\t\t\t    }
\t\t\t  }
\t\t\t}
\t\tcontentType: json

\ttranslations
\t\tmodel Model
\t\t\ttable Sales
\t\t\t\tcaption: Sales
\t\t\t\tcolumn Revenue
\t\t\t\t\tcaption: Revenue amount
\t\t\ttable _Measures
\t\t\t\tmeasure Revenue
\t\t\t\t\tcaption: Total revenue
"""

DIAGRAM = {"version": "1.1.0", "diagrams": [{"ordinal": 0, "nodes": [
    {"location": {"x": 0, "y": 0}, "nodeIndex": "Sales", "size": {"height": 300, "width": 234}},
    {"location": {"x": 300, "y": 0}, "nodeIndex": "Customers", "size": {"height": 300, "width": 234}},
    {"location": {"x": 600, "y": 0}, "nodeIndex": "Dim Date", "size": {"height": 300, "width": 234}}]}]}

MODEL_FILES: Dict[str, str] = {
    "model.tmdl": MODEL,
    "database.tmdl": DATABASE,
    "relationships.tmdl": RELATIONSHIPS,
    "tables/Sales.tmdl": SALES,
    "tables/Customers.tmdl": CUSTOMERS,
    "tables/Dim Date.tmdl": DIM_DATE,
    "tables/_Measures.tmdl": MEASURES,
    "roles/Reader.tmdl": ROLE,
    "perspectives/Core.tmdl": PERSPECTIVE,
    "cultures/en-US.tmdl": CULTURE,
}


# ---------------------------------------------------------------- a project

class Project:
    """Paths of a built project plus a loaded connector."""

    def __init__(self, root: Path, name: str, connector: PowerBIPBIPConnector) -> None:
        self.root = root
        self.name = name
        self.pbip = root / f"{name}.pbip"
        self.model = root / f"{name}.SemanticModel"
        self.definition = self.model / "definition"
        self.report = root / f"{name}.Report"
        self.report_definition = self.report / "definition"
        self.pages = self.report_definition / "pages"
        self.connector = connector

    def tmdl(self, rel: str) -> str:
        return read(self.definition / rel)

    def visual_path(self, page: str, name: str) -> Path:
        return self.pages / page / "visuals" / name / "visual.json"

    def visual(self, page: str, name: str) -> Any:
        return load_json(self.visual_path(page, name))


def build_project(tmp: str, name: str = "Retail", model: Optional[Dict[str, str]] = None,
                  visuals: Optional[Dict[str, Dict[str, Any]]] = None,
                  report_files: Optional[Dict[str, Any]] = None, auto_backup: bool = False,
                  crlf: bool = False, bom: bool = False, legacy_report: Optional[Dict[str, Any]] = None,
                  load: bool = True) -> Project:
    """Build <tmp>/<name>.pbip with its model and report folders.

    model         {path under definition/: TMDL text} (defaults to MODEL_FILES)
    visuals       {"<page>/<visual>": visual.json object}
    report_files  {path under <name>.Report/definition/: object or text} for page.json,
                  report.json, bookmarks, mobile.json, reportExtensions.json
    legacy_report a PBIR-Legacy report.json object instead of the definition/ folder
    """
    root = Path(tmp)
    write_json(root / f"{name}.pbip", {
        "version": "1.0", "artifacts": [{"report": {"path": f"{name}.Report"}}],
        "settings": {"enableAutoRecovery": True}}, crlf=crlf)
    model_dir = root / f"{name}.SemanticModel"
    for rel, text in (MODEL_FILES if model is None else model).items():
        write(model_dir / "definition" / rel, text, bom=bom, crlf=crlf)
    write_json(model_dir / "diagramLayout.json", DIAGRAM, crlf=crlf)
    report_dir = root / f"{name}.Report"
    write_json(report_dir / "definition.pbir", {
        "version": "4.0", "datasetReference": {"byPath": {"path": f"../{name}.SemanticModel"}}}, crlf=crlf)
    if legacy_report is not None:
        write_json(report_dir / "report.json", legacy_report, crlf=crlf)
    else:
        definition = report_dir / "definition"
        pages = sorted({key.split("/")[0] for key in (visuals or {})} | {"overview"})
        write_json(definition / "report.json", {"$schema": "report/3.0.0", "themeCollection": {}}, crlf=crlf)
        write_json(definition / "pages" / "pages.json", {"pageOrder": pages, "activePageName": pages[0]},
                   crlf=crlf)
        for page in pages:
            write_json(definition / "pages" / page / "page.json", {
                "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/"
                           "page/2.0.0/schema.json",
                "name": page, "displayName": page.title(), "displayOption": "FitToPage",
                "height": 720, "width": 1280}, crlf=crlf)
        for key, obj in (visuals or {}).items():
            page, vname = key.split("/")
            write_json(definition / "pages" / page / "visuals" / vname / "visual.json", obj, crlf=crlf)
        for rel, obj in (report_files or {}).items():
            if isinstance(obj, str):
                write(definition / rel, obj, crlf=crlf)
            else:
                write_json(definition / rel, obj, crlf=crlf)
    connector = PowerBIPBIPConnector(auto_backup=auto_backup)
    if load and not connector.load_project(str(root / f"{name}.pbip")):
        raise AssertionError(f"fixture project did not load: {connector.last_error}")
    return Project(root, name, connector)
