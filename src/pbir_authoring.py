"""
PBIR (Power BI Enhanced Report) authoring primitives.

Pure functions that EMIT PBIR JSON (pages and visuals) - the inverse of the parsing in
powerbi_pbip_connector._walk_report_refs. Used by the connector's add_page / add_visual /
bind_fields methods so an agent can author reports by writing schema-valid PBIR files.

PBIR is a publicly documented format (schemas: github.com/microsoft/json-schemas) and is in
preview as of mid-2026, so callers should prefer the $schema URL read from an existing file in
the same project over the defaults below (schemas bump roughly monthly).
"""
import uuid
from typing import Any, Dict, List, Optional

# Fallback $schema URLs, used only when a brand-new project has no sibling file to inherit
# from. The connector ALWAYS prefers the $schema read from an existing file in the same
# project (schemas bump ~monthly); these are the current published versions as of mid-2026.
# Our emitted shape is a minimal subset valid across all 2.x versions (verified against the
# published schemas: visualContainer 2.9.0 -> visualConfiguration 2.3.0 requires only visualType).
DEFAULT_SCHEMAS = {
    "visual": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.9.0/schema.json",
    "page": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/page/2.1.0/schema.json",
    "pagesMetadata": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/pagesMetadata/1.0.0/schema.json",
}

# visualType -> ordered list of accepted data roles. Used to map fields_by_role and to map a
# flat field list onto a visual's primary roles.
VISUAL_ROLES: Dict[str, List[str]] = {
    "card": ["Values"],
    "cardVisual": ["Values"],
    "kpi": ["Indicator", "TrendlineValues", "Goal"],
    "tableEx": ["Values"],
    "pivotTable": ["Rows", "Columns", "Values"],
    "slicer": ["Values"],
    "barChart": ["Category", "Y", "Series"],
    "columnChart": ["Category", "Y", "Series"],
    "lineChart": ["Category", "Y", "Series"],
    "areaChart": ["Category", "Y", "Series"],
    "pieChart": ["Category", "Y"],
    "donutChart": ["Category", "Y"],
    "gauge": ["Y", "MinValue", "MaxValue", "TargetValue"],
}

# Roles whose fields are aggregated (a plain column placed here is wrapped in an Aggregation,
# matching how Power BI Desktop itself materializes a column dropped onto a value well).
AGGREGATING_ROLES = {"y", "values", "value", "indicator", "goal", "trendlinevalues",
                     "targetvalue", "minvalue", "maxvalue", "size", "tooltips", "x"}

# Power BI QueryAggregateFunction integer enum (semanticQuery schema) and the matching
# queryRef function name Power BI writes, e.g. Sum(Sales.Amount) / CountNonNull(Sales.Id).
AGG_SUM, AGG_AVERAGE, AGG_DISTINCT_COUNT, AGG_MIN = 0, 1, 2, 3
AGG_MAX, AGG_COUNT_NONNULL, AGG_MEDIAN, AGG_STDDEV, AGG_VARIANCE = 4, 5, 6, 7, 8
AGG_QUERYREF_NAMES = {
    0: "Sum", 1: "Average", 2: "CountNonNull", 3: "Min", 4: "Max",
    5: "CountNonNull", 6: "Median", 7: "StandardDeviation", 8: "Var",
}


def new_name() -> str:
    """A 20-char hex object name, matching Power BI's default naming for pages/visuals."""
    return uuid.uuid4().hex[:20]


def emit_projection(table: str, prop: str, kind: str = "Column",
                    query_ref: Optional[str] = None, active: bool = False,
                    native_query_ref: Optional[str] = None,
                    aggregation: Optional[int] = None) -> Dict[str, Any]:
    """Build one PBIR field projection for a query role.

    kind is 'Column' or 'Measure'. Mirrors the shape parsed by _walk_report_refs:
    {field:{Column|Measure:{Expression:{SourceRef:{Entity}},Property}}, queryRef, nativeQueryRef, active?}.

    When ``aggregation`` (a QueryAggregateFunction int, e.g. AGG_SUM) is given, the field is a
    *column wrapped in an Aggregation* - what Power BI Desktop writes for a column dropped on a
    value well - and the queryRef takes the Sum(Table.Field) form. ``native_query_ref`` defaults
    to the bare property name, matching Power BI's own output and keeping diffs stable.
    """
    native = native_query_ref or prop
    if aggregation is not None:
        agg = int(aggregation)
        field: Dict[str, Any] = {
            "Aggregation": {
                "Expression": {
                    "Column": {
                        "Expression": {"SourceRef": {"Entity": table}},
                        "Property": prop,
                    }
                },
                "Function": agg,
            }
        }
        default_ref = f"{AGG_QUERYREF_NAMES.get(agg, 'Sum')}({table}.{prop})"
    else:
        kind = "Measure" if str(kind).lower().startswith("meas") else "Column"
        field = {
            kind: {
                "Expression": {"SourceRef": {"Entity": table}},
                "Property": prop,
            }
        }
        default_ref = f"{table}.{prop}"
    proj: Dict[str, Any] = {
        "field": field,
        "queryRef": query_ref or default_ref,
        "nativeQueryRef": native,
    }
    if active:
        proj["active"] = True
    return proj


def split_table_field(ref: str):
    """Split 'Table.Field' or \"'Table Name'.Field\" into (table, field). Field may itself
    contain dots only if the table is single-quoted; we split on the first unquoted dot."""
    s = ref.strip()
    if s.startswith("'"):
        end = s.find("'", 1)
        if end != -1:
            table = s[1:end]
            rest = s[end + 1:].lstrip(".")
            return table, rest
    table, _, field = s.partition(".")
    return table, field


def _projection_for(ref, kind: str, active: bool) -> Dict[str, Any]:
    """Accept either 'Table.Field' or a dict
    {table, field/property, kind?, queryRef?, nativeQueryRef?, aggregation?}."""
    if isinstance(ref, dict):
        table = ref.get("table") or ref.get("entity")
        prop = ref.get("field") or ref.get("property") or ref.get("column") or ref.get("measure")
        return emit_projection(table, prop, ref.get("kind", kind), ref.get("queryRef"), active,
                               ref.get("nativeQueryRef"), ref.get("aggregation"))
    table, prop = split_table_field(str(ref))
    return emit_projection(table, prop, kind, None, active)


def build_query_state(fields_by_role: Dict[str, Any], kinds: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    """Build query.queryState from {role: 'Table.Field' | [fields] | [dicts]}.

    The first projection in each role is marked active. kinds optionally overrides Column/Measure
    per role (default Column, but value-ish roles default to Measure).
    """
    kinds = kinds or {}
    measure_roles = {"y", "values", "indicator", "goal", "trendlinevalues", "targetvalue", "minvalue", "maxvalue"}
    state: Dict[str, Any] = {}
    for role, val in fields_by_role.items():
        items = val if isinstance(val, list) else [val]
        default_kind = kinds.get(role, "Measure" if role.lower() in measure_roles else "Column")
        projections = [_projection_for(it, default_kind, active=(i == 0)) for i, it in enumerate(items) if it]
        if projections:
            state[role] = {"projections": projections}
    return state


def build_visual(name: str, visual_type: str, position: Dict[str, Any],
                 fields_by_role: Optional[Dict[str, Any]] = None,
                 schema_url: Optional[str] = None) -> Dict[str, Any]:
    """Build a complete visual.json document."""
    pos = {
        "x": float(position.get("x", 0)),
        "y": float(position.get("y", 0)),
        "z": float(position.get("z", 0)),
        "width": float(position.get("width", 400)),
        "height": float(position.get("height", 300)),
        "tabOrder": int(position.get("tabOrder", 0)),
    }
    visual: Dict[str, Any] = {"visualType": visual_type, "drillFilterOtherVisuals": True}
    if fields_by_role:
        qs = build_query_state(fields_by_role)
        if qs:
            visual["query"] = {"queryState": qs}
    return {
        "$schema": schema_url or DEFAULT_SCHEMAS["visual"],
        "name": name,
        "position": pos,
        "visual": visual,
    }


def build_page(name: str, display_name: str, width: int = 1280, height: int = 720,
               schema_url: Optional[str] = None) -> Dict[str, Any]:
    """Build a page.json document."""
    return {
        "$schema": schema_url or DEFAULT_SCHEMAS["page"],
        "name": name,
        "displayName": display_name,
        "displayOption": "FitToPage",
        "height": height,
        "width": width,
    }


def build_pages_metadata(page_order: List[str], active_page: Optional[str] = None,
                         schema_url: Optional[str] = None) -> Dict[str, Any]:
    """Build pages.json (page order + active page)."""
    return {
        "$schema": schema_url or DEFAULT_SCHEMAS["pagesMetadata"],
        "pageOrder": list(page_order),
        "activePageName": active_page or (page_order[0] if page_order else ""),
    }
