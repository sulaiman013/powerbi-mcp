# Architecture

## Overview

A Python **stdio MCP server** (`src/server.py`) that routes 94 tools to lazily-created
connectors behind one request boundary, with pure-Python modules for everything that does not
need a live Power BI (parsers, emitters, generators, linters, auditors).

```
        MCP client (Claude / Copilot / VS Code)
                      |  stdio (JSON-RPC)
        +-------------v---------------------------------------------+
        |  server.py (PowerBIMCPServer)                             |
        |  - tool registry: tools + dispatch + annotations (parity) |
        |  - request boundary: read-only gate, statement guard,     |
        |    policy + PII enforcement, secret redaction             |
        |  - resources / prompts / completion                       |
        +--+---------+---------+---------+---------+---------+------+
           |         |         |         |         |         |
       Desktop   XMLA/REST    TOM      PBIP     Desktop   Security
      (ADOMD,   (cloud +   (live     (offline   Bridge    layer
       local)    admin)     write)    files)   (JSON-RPC  (policy/PII/
           |         |         |         |      pipe)      audit)
           |         |         |         |         |
     Power BI     Power BI  in-memory  .pbip    running
     Desktop's    Service   model      files    PBIDesktop.exe
     msmdsrv                           (TMDL+PBIR)
```

## The five integration paths

| Path | Transport | What it does |
|------|-----------|--------------|
| **Desktop (ADOMD)** | `pythonnet` + ADOMD.NET to the local `msmdsrv` port | DAX queries, metadata, RLS testing, VertiPaq DMVs |
| **Cloud (XMLA + REST)** | ADOMD.NET over XMLA; MSAL service-principal REST | Datasets, DAX, refresh history, admin Scanner/Activity APIs |
| **TOM (live writes)** | `Microsoft.AnalysisServices.Tabular` via pythonnet | Measures (single + validated batch), relationships, transactions |
| **PBIP (offline files)** | Pure filesystem | TMDL + PBIR: safe renames, model authoring, declarative report building, data embedding |
| **Desktop Bridge (preview)** | JSON-RPC 2.0 over named pipe `pbi-desktop-bridge-{pid}` | Open-file state, hot-reload from disk, page screenshots on the RUNNING Desktop |

The offline PBIP path and the Desktop Bridge together form the **edit-and-verify loop**:
author files offline, `bridge_reload` the running Desktop (no close/reopen), screenshot the
result. The bridge's discovered msmdsrv port also chains an instance straight into
`desktop_connect` for DAX/TOM work on the same window.

## Two shared foundations

Most of the defects this project has had came from treating structured text as plain text.
Two small modules now own that problem for everyone:

- **`dax_tokens.py`** - one DAX lexer (strings, comments, quoted table names, bracketed names
  with their escapes, dotted function names) plus reference extraction and token-level renames
  of tables, columns and measures. The linter, the PBIP and TOM rename engines, the security
  policy check and the server's validation probes all use it. It also classifies command text
  (`statement_kind`): a DAX query, a DMV query, or a command that could change state.
- **`safe_io.py`** - every file write. A file keeps its encoding (BOM or not), its newline
  style (a file with mixed endings is never normalized) and its name's case; each write is
  atomic; `FileTransaction` stages many files and commits them all or restores them all.

## Components

### `server.py`
- **Tool registry.** `build_tools` (the `Tool` specs), `_build_tool_dispatch` (name to handler)
  and `_build_tool_annotations` (name to `ToolAnnotations`). A parity test enforces that the
  three contain exactly the same names; a docs parity test enforces the documentation.
- **Read-only lockdown gate.** With `POWERBI_MCP_READONLY` on, every tool that is not annotated
  read-only is refused before dispatch, except a short list of session-scoped tools and three
  that refuse only their own write option. A new write tool is therefore gated by default.
- **Statement guard.** The tools that run caller-supplied text accept DAX queries only. TMSL,
  XMLA and MDX are refused in the server and again in the connectors, so a read-only-annotated
  tool cannot change a model.
- **One guarded query path.** Every tool that executes caller-supplied DAX (`execute_dax`,
  `desktop_execute_dax`, `validate_dax`, `run_dax_tests`, `rls_test_harness`,
  `analyze_query_performance`, the sample step of `scan_referential_integrity`, and the
  validation probes behind `create_measure` and the batch tools) goes through
  `_run_guarded_query`: statement guard, policy check, execution with a row cap, column policies
  and PII masking on the result, audit entry.
- **Response-boundary redaction.** Text and structured content returned by any handler, and
  every resource, pass through secret redaction before leaving the process.
- **Structured output.** Handlers return a string or a `(text, dict)` tuple; a tool with an
  `outputSchema` always returns structured content, including on refusal and on error.
- **Model metadata cache.** INFO.VIEW metadata is reused for a few seconds per connection and
  dropped whenever a non-read-only tool runs.
- **Resources / prompts / completion.** Model context (`powerbi://...`), reusable BI prompts,
  and argument completion grounded in the connected model.

### Connectors
- **`adomd_loader.py`** - ADOMD.NET and TOM discovery: environment overrides, Power BI Desktop,
  GAC, SSMS, SQL SDK, NuGet cache. Candidates are ranked for the active .NET runtime (a .NET
  Core build cannot load under .NET Framework), loaded and probed, with fall-through.
- **`connection_strings.py`** - builds connection strings with every value quoted and escaped,
  so a workspace, dataset or role name cannot inject a key.
- **`adomd_query.py`** - the one way a query runs over ADOMD: query-only check, command timeout,
  a row limit applied while reading, connections closed on every path, culture-invariant values.
- **`powerbi_desktop_connector.py`** - local Analysis Services: DAX, metadata, RLS role
  switching (a failed switch keeps the previous role), VertiPaq DMVs.
- **`powerbi_xmla_connector.py` / `powerbi_rest_connector.py`** - cloud datasets and the admin
  Scanner/Activity APIs. One request helper: timeout, token refresh on 401, bounded retries on
  429/503 honouring `Retry-After`, typed errors, GUID-validated ids, full paging. Secrets are
  redacted from every error.
- **`powerbi_tom_connector.py`** - live model writes. The connector owns transactions
  (`begin` / `commit` / `rollback`): nothing saves while one is open, a failed commit leaves it
  open, a failed save outside one undoes the local edit. `batch_create_measures` is
  all-or-nothing. The model is re-synced before a write. Rename fix-ups go through
  `dax_tokens`.
- **`desktop_bridge.py`** - Desktop Bridge client: pipe discovery, Content-Length JSON-RPC
  framing, response-id matching, a timeout that cancels the blocked read, page resolution
  across PBIR folders, embedded-PBIR `.pbix`, and legacy Layout. The argument shape of each
  method is taken from the running build's own manifest (Desktop 2.159 wraps arguments in an
  envelope, earlier builds do not), and a call waits while Desktop answers "not ready". It
  also opens a file through its Windows association and waits for its bridge (path lengths
  checked first, never a second window on one project), and works out the rectangle of one
  visual, parent groups included, for a focused screenshot.

### Offline PBIP engine
- **`tmdl_scan.py`** - a TMDL object tree and model catalog (tables, columns, measures,
  hierarchies, relationships, expressions).
- **`pbip_json.py`** - a JSON parser that keeps token positions, so an edit replaces one token
  and leaves the rest of the file byte for byte.
- **`pbip_mquery.py`** - a Power Query (M) tokenizer and query-reference rename.
- **`pbip_report.py`** - structural reading and rewriting of the report layer: visuals, mobile
  layouts, page and report filters, bookmarks, Deneb specs, legacy nested JSON.
- **`pbip_rename.py`** - the rename engine: plan the whole change in memory, validate the result
  (no new broken reference), commit through one `FileTransaction`.
- **`powerbi_pbip_connector.py`** - project discovery (from the `.pbip` artifacts and
  `definition.pbir`), the rename/repair/validate entry points, offline authoring (measures with
  idempotent upsert, date table, calculation group, hierarchy, pages and visuals), backups.

### Report as code
- **`pbir_builders.py`** - pure PBIR emitters for everything a designed report needs: text,
  shapes, images, action buttons, slicers with sync groups, visual groups, bookmarks, pages,
  themes, HTML Content and Deneb visuals, private custom visual registration, page bindings
  (drillthrough, tooltip pages), visual interactions, the phone layout, bookmark groups.
  `pbir_filters.py` builds report, page and visual filters and merges them with filters added
  in Desktop. `pbir_packages.py` reads `.pbiviz` packages.
- **`report_spec.py`** - the declarative spec behind `pbir_apply_spec`: validation, expansion of
  pop-ups and shared elements, idempotent apply, prune, dry run, and ownership by manifest so
  that what a person added in Desktop to a file the spec writes is kept. See
  [REPORT_SPEC.md](REPORT_SPEC.md).
- **`report_audit.py`** - read-only audit of the report layer: structure, design, governance,
  and interactivity (what each custom visual's package declares and its script calls).
- **`custom_visual.py`** - builds a real `.pbiviz` from a compact definition without the pbiviz
  toolchain: validation, a safety pass over the author's code, capabilities, a deterministic
  package, and an inspector for any `.pbiviz`. The JavaScript a built visual ships with lives in
  **`visual_kit/`**: selection and cross-filtering, highlights, the context menu, tooltips,
  keyboard, the format pane and number formatting. Author code is checked, never run, by the
  server.
- **`deneb_specs.py`** - generates Deneb (Vega-Lite) specs for ten chart kinds with Power BI
  interactivity wired in, and the `deneb` element that places them.
- **`payload_measures.py`** - "DAX as transport": text measures that pack rows for a JavaScript
  consumer, with the matching parser.
- **`js_app_measure.py`** - assembles CSS and JavaScript modules into measures for the HTML
  Content visual: IIFE and re-evaluation guard, a string/template/regex-aware minifier, build
  refusals, a Node syntax check, sizes against the ceiling.
- **`html_measures.py`** / **`svg_measures.py`** - DAX measures that return styled HTML and
  inline SVG micro-visuals.
- **`model_embed.py`** - embeds CSV data in M partitions so a model is self-contained.
- **`publish_stage.py`** - stages a report bound `byConnection` for an in-place publish.
- **`pbir_authoring.py`** / **`tmdl_authoring.py`** - the field-projection and TMDL emitters
  used by the single-visual tools and the model authoring tools.

### Analysis and audit
- **`model_analysis.py`** - 33 BPA rules, AI-readiness scorer, data dictionary, model diff,
  DAX test verdicts.
- **`dax_lint.py`** + **`dax_functions.py`** - DAX linter on the shared tokenizer, a function
  catalog, and mechanical rewrites.
- **`dax_generator.py`** - bulk measure-suite generation (time intelligence, ratios, ranks,
  column stats).
- **`star_schema.py`** - fact/dimension/date/bridge/measure-table classification from
  relationship topology, findings and a score.
- **`naming_audit.py`** - naming-convention audit producing a rename plan with collision checks.
- **`pbix_tools.py`** - `.pbix` (OPC ZIP) inspection/extraction, legacy Layout decoding.
- **`bpa_authoring.py`** - custom BPA rule validation and rule-source audit.
- **`refresh_diagnostics.py`** / **`governance.py`** - refresh error classification; Scanner
  summary and activity aggregation.

### Security layer (`src/security/`)
`security_layer.py` ties together:
- **`access_policy.py`** + **`query_analysis.py`** - column and table policies (BLOCK / MASK /
  HASH / REDACT / NUMERIC_MASK, `max_rows`, `require_filter`, `aggregate_only`). The check reads
  the query through the tokenizer, follows aliases and expands measures, refuses whole-table
  serialisers over protected tables, and applies the most restrictive referenced action to
  computed result columns.
- **`config.py`** - loads `config/policies.yaml` strictly. A missing, unparseable or invalid
  file **locks** the layer: queries are refused and `security_status` says why.
- **`pii_detector.py`** - linear-time detection with value validation (Luhn, SSN ranges),
  honouring the configured types and strategies.
- **`audit_logger.py`** - JSON-lines audit with sequence numbers and a hash chain that continues
  across rotation (HMAC-SHA256 when `POWERBI_MCP_AUDIT_KEY` is set), a checkpoint that exposes
  truncation, and a lock so several server processes can share one log.
- **`redaction.py`** - secret redaction for text and structures; **`transforms.py`** - keyed
  hashing and numeric masking; **`env.py`** - flag parsing.

**What the policy layer is and is not.** It works on query text and result names. It closes
the common ways a protected column reaches the model (direct reads, aliases, measures,
serialisers, error messages) and is tested for them, but it is defence in depth: a calculated
column that copies protected data is judged by its own name, and a filter on a masked column
can still confirm a guess. Row-level and object-level security in the semantic model is the
boundary. Metadata tools list column names regardless of policy.

## Platform constraints
- **Live connectivity is Windows-only** (ADOMD.NET / TOM / the Desktop Bridge named pipe).
  The server imports and runs without them; those tools report themselves unavailable.
- **The offline subset runs cross-platform** (PBIP renames and authoring, the report spec, the
  generators, BPA, linters, auditors, security) - see `Dockerfile` / `requirements-core.txt`.
- The Desktop Bridge needs Power BI Desktop June 2026+ with the preview option "Enable
  external tool access to Power BI Desktop through secure local APIs" (on by default).
- Cloud needs a service principal + (for XMLA) Premium/PPU/Fabric capacity. Admin Scanner and
  Activity tools need Fabric admin or a service principal allowed read-only admin APIs.

## Verification methodology
1. **Assert suites** (`tests/`, 53 suites, no Power BI needed) for all pure logic, with the
   server and the live connectors driven through fakes.
2. **Runs against real projects.** The rename engine, the report spec and the report audit
   were run on scratch copies of real, complex reports and checked for byte-level fidelity
   (see docs/TESTING.md).
3. **Doc-verification.** API contracts (REST shapes, INFO functions, PBIR and TMDL shapes, the
   Desktop Bridge protocol) are checked against Microsoft Learn and real exports.
4. **Engine and live checks (2026-07).** Generated TMDL parsed under Microsoft's
   `TmdlSerializer`; ADOMD queries, TOM batch writes and the Desktop Bridge were exercised
   against a running Power BI Desktop. These predate the 3.8.0 rewrite of the connectors and
   the rename engine and have not been repeated on it.

Generated DAX and M are not evaluated by an engine in the suites, generated reports are not
opened in Desktop by them, and the cloud paths have not been run against a tenant.

## File map
```
src/
  server.py                     MCP server: registry, request boundary, handlers
  dax_tokens.py                 Shared DAX tokenizer, reference rewriting, statement classifier
  safe_io.py                    Faithful atomic writes and the multi-file transaction
  adomd_loader.py               ADOMD.NET / TOM discovery, ranked for the active .NET runtime
  adomd_query.py                Guarded query execution over ADOMD
  connection_strings.py         Quoted, injection-proof connection strings
  powerbi_desktop_connector.py  Desktop (ADOMD) + RLS + VertiPaq DMVs
  powerbi_xmla_connector.py     Cloud XMLA
  powerbi_rest_connector.py     REST: discovery, refresh history, admin Scanner/Activity
  powerbi_tom_connector.py      TOM writes: measures, relationships, transactions
  desktop_bridge.py             Desktop Bridge client (JSON-RPC over named pipe)
  powerbi_pbip_connector.py     PBIP project: renames, repair, validation, authoring
  pbip_rename.py                Rename engine (plan, validate, commit)
  pbip_report.py                Report-layer reading and rewriting (PBIR, Deneb, legacy)
  pbip_json.py                  Position-preserving JSON parser
  pbip_mquery.py                Power Query tokenizer and reference rename
  tmdl_scan.py                  TMDL object tree and model catalog
  tmdl_authoring.py             TMDL emitters: measures, date table, calc groups, hierarchies
  pbir_authoring.py             PBIR field projections, single pages and visuals
  pbir_builders.py              PBIR emitters for designed reports
  pbir_packages.py              .pbiviz package reading
  pbir_filters.py               PBIR filters: report, page and visual level
  custom_visual.py              Builds and inspects .pbiviz custom visuals
  visual_kit/                   JavaScript runtime a built custom visual ships with
  deneb_specs.py                Deneb (Vega-Lite) spec generator with interactivity
  report_spec.py                Declarative report spec (pbir_apply_spec)
  report_audit.py               Report-layer audit
  payload_measures.py           Row-packing text measures + JavaScript parsers
  js_app_measure.py             JavaScript app assembled into measures
  html_measures.py              HTML Content measure generators
  svg_measures.py               SVG micro-visual measure generators
  model_embed.py                CSV data embedded in M partitions
  publish_stage.py              Publish staging (byConnection)
  dax_generator.py              Bulk measure-suite generation
  dax_lint.py                   DAX linter + rewrites
  dax_functions.py              DAX function catalog
  model_analysis.py             BPA, AI-readiness, data dictionary, diff, DAX tests
  star_schema.py                Star-schema classification + warehouse audit
  naming_audit.py               Naming-convention audit -> rename plan
  pbix_tools.py                 PBIX inspect/extract, Layout decode
  bpa_authoring.py              Custom BPA rule validation + rule-source audit
  refresh_diagnostics.py        Refresh error classification
  governance.py                 Scanner summary + activity aggregation
  security/                     security_layer, access_policy, query_analysis, config,
                                pii_detector, audit_logger, redaction, transforms, names, env
config/policies.yaml            Access policy definitions
tests/                          53 assert-based suites (run via run_tests.py)
docs/                           TOOLS, ARCHITECTURE, TESTING, REPORT_SPEC
```
