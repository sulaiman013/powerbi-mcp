# Architecture

## Overview

A Python **stdio MCP server** (`src/server.py`) that routes 82 tools to lazily-created
connectors, wrapped by a security layer, with pure-Python modules for everything that does
not need a live Power BI (emitters, linters, auditors, generators).

```
        MCP client (Claude / Copilot / VS Code)
                      |  stdio (JSON-RPC)
        +-------------v---------------------------------------------+
        |  server.py (PowerBIMCPServer)                             |
        |  - tool registry: dispatch + annotations + list (parity)  |
        |  - resources / prompts / completion                       |
        |  - read-only lockdown gate + response-boundary redaction  |
        +--+---------+---------+---------+---------+---------+------+
           |         |         |         |         |         |
       Desktop   XMLA/REST    TOM      PBIP     Desktop   Security
      (ADOMD,   (cloud +   (live     (offline   Bridge    layer
       local)    admin)     write)    files)   (JSON-RPC  (PII/audit/
           |         |         |         |      pipe)      policy)
           |         |         |         |         |
     Power BI     Power BI  in-memory  .pbip    running
     Desktop's    Service   model      files    PBIDesktop.exe
     msmdsrv                           (TMDL+PBIR)
```

## The five integration paths

| Path | Transport | What it does |
|------|-----------|--------------|
| **Desktop (ADOMD)** | `pythonnet` + ADOMD.NET to the local `msmdsrv` port | DAX queries, metadata, RLS testing, VertiPaq DMVs |
| **Cloud (XMLA + REST)** | `pyadomd` XMLA; MSAL service-principal REST | Datasets, DAX, refresh, admin Scanner/Activity APIs |
| **TOM (live writes)** | `Microsoft.AnalysisServices.Tabular` via pythonnet | Measures (single + validated batch), relationships, transactions |
| **PBIP (offline files)** | Pure filesystem | TMDL + PBIR editing: safe renames, report authoring, model authoring |
| **Desktop Bridge (preview)** | JSON-RPC 2.0 over named pipe `pbi-desktop-bridge-{pid}` | Open-file state, hot-reload from disk, page screenshots on the RUNNING Desktop |

The offline PBIP path and the Desktop Bridge together form the **edit-and-verify loop**:
author files offline, `bridge_reload` the running Desktop (no close/reopen), screenshot the
result. The bridge's discovered msmdsrv port also chains an instance straight into
`desktop_connect` for DAX/TOM work on the same window.

## Components

### `server.py`
- **Tool registry.** `_build_tool_dispatch()` (name to handler) and `_build_tool_annotations()`
  (name to `ToolAnnotations`) are the single source of truth; `handle_list_tools` and
  `handle_call_tool` derive from them. A parity test enforces that the tool list, the dispatch
  map, and the annotation map contain exactly the same names.
- **Read-only lockdown gate.** With `POWERBI_MCP_READONLY=true`, every destructive-annotated
  tool plus the non-destructive writers (`create_measure`, `create_relationship`,
  `batch_create_measures`, `tom_commit_transaction`) plus the file-writing tools
  (`export_data_dictionary`, `model_snapshot`, `pbix_extract`, `bridge_screenshot`) is refused
  before dispatch, with a structured refusal so outputSchema tools stay protocol-valid.
- **Response-boundary redaction.** Every text response passes through `redact_secrets` at the
  dispatch boundary, so a handler that swallows an exception cannot leak a connection-string
  secret to the model. Connectors additionally redact before re-raising and strip .NET stack
  traces from ADOMD errors.
- **Structured output.** Handlers return a string or a `(text, dict)` tuple; tools with an
  `outputSchema` return the tuple so clients get typed `structuredContent`.
- **Resources / prompts / completion.** Model context (`powerbi://...`), reusable BI prompts,
  and argument completion grounded in the connected model.

### Connectors (`src/powerbi_*_connector.py`, `src/adomd_loader.py`, `src/desktop_bridge.py`)
- **`adomd_loader.py`** - shared ADOMD.NET discovery: `ADOMD_DLL_PATH`, Power BI Desktop, GAC,
  SSMS, SQL SDK, Update Cache, NuGet cache. Used by the Desktop and XMLA connectors.
- **`powerbi_desktop_connector.py`** - local Analysis Services via ADOMD; DAX, metadata, RLS,
  VertiPaq DMVs. Errors are stripped of .NET stack traces.
- **`powerbi_xmla_connector.py` / `powerbi_rest_connector.py`** - cloud datasets and the admin
  Scanner/Activity APIs. The client secret is redacted from any provider error.
- **`powerbi_tom_connector.py`** - live model writes. TOM discovery honors `TOM_DLL_PATH` /
  `ADOMD_DLL_PATH` and the AMO NuGet cache. `batch_create_measures` pre-validates the whole
  batch (existence + duplicates) at the connector; the server layer adds per-expression
  validation, intra-batch sibling-reference deferral (validated post-create), and a
  compensating whole-batch rollback if a post-check fails.
- **`powerbi_pbip_connector.py`** - pure-filesystem TMDL + PBIR editing. Safe renames cascade
  across model files, report visuals, cultures, diagram, hierarchy levels, and sortByColumn
  wiring, transactionally (per-operation rollback cache) with atomic, encoding/BOM/CRLF
  preserving writes. Also hosts the offline authoring methods (measures, date table,
  calculation group, hierarchy, report pages/visuals) with collision checks and
  filename-safety guards.
- **`desktop_bridge.py`** - Desktop Bridge client: pipe discovery, Content-Length JSON-RPC
  framing, a timeout-guarded one-shot-per-call client (plain `open()` on the pipe, no
  pywin32), page resolution across PBIR folders, embedded-PBIR `.pbix`, and legacy Layout,
  and msmdsrv-port correlation.

### Emitters and authoring (pure Python, engine-verified)
- **`pbir_authoring.py`** - PBIR JSON emitters (pages, visuals, field projections) verified
  against Microsoft's published PBIR schemas; Desktop-faithful output (`nativeQueryRef`,
  `Aggregation` wrapping, sibling-inherited `$schema`).
- **`tmdl_authoring.py`** - TMDL text emitters (measures with `///` descriptions, date
  dimension, calculation groups, hierarchies) whose shapes were verified against the TMDL
  language reference, real Desktop exports, and Microsoft's own `TmdlSerializer`.
- **`dax_generator.py`** - bulk measure-suite generation (time intelligence, ratios, ranks,
  column stats); every generated expression is lint-clean by construction.
- **`svg_measures.py`** - SVG micro-visual measure generators (progress, bullet, pill,
  sparkline).

### Analysis and audit (pure Python, unit-tested)
- **`model_analysis.py`** - BPA rules, AI-readiness scorer, data dictionary, model diff, DAX
  test verdicts.
- **`dax_lint.py`** - DAX anti-pattern linter (comment/string-aware tokenizer) + rewrite hints.
- **`star_schema.py`** - fact/dimension/date/bridge/measure-table classification from
  relationship topology + warehouse best-practice findings with a score.
- **`naming_audit.py`** - naming-convention audit producing a rename plan (skips hidden and
  internal columns).
- **`pbix_tools.py`** - `.pbix` (OPC ZIP) inspection/extraction, legacy Layout decoding, and
  embedded-PBIR page listing. Zip-Slip protected.
- **`bpa_authoring.py`** - custom BPA rule validation and rule-source audit.
- **`refresh_diagnostics.py`** - refresh error to cause to remediation knowledge base.
- **`governance.py`** - Scanner-result summary and activity aggregation.

### Security layer (`src/security/`)
`security_layer.py` wraps `access_policy.py` (enforced BLOCK / MASK / HASH / REDACT /
NUMERIC_MASK column policies; linear-time reference extraction), `pii_detector.py` (masking
before results reach the model; detection summaries never retain raw values), and
`audit_logger.py` (JSON-lines audit with a tamper-evident hash chain; HMAC-SHA256 when
`POWERBI_MCP_AUDIT_KEY` is set; secrets scrubbed before persistence).

## Platform constraints
- **Live connectivity is Windows-only** (ADOMD.NET / TOM / the Desktop Bridge named pipe).
  The server imports and runs without them; those tools report themselves unavailable.
- **The offline subset runs cross-platform** (PBIP/TMDL/PBIR authoring and editing, BPA,
  AI-readiness, linters, auditors, security) - see `Dockerfile` / `requirements-core.txt`.
- The Desktop Bridge needs Power BI Desktop June 2026+ with the preview option "Enable
  external tool access to Power BI Desktop through secure local APIs" (on by default).
- Cloud needs a service principal + (for enhanced refresh) Premium/PPU. Admin Scanner and
  Activity tools need Fabric admin or a service principal allowed read-only admin APIs.

## Verification methodology
Four layers, strongest available per surface:
1. **Assert suites** (`tests/`, 25 suites, no Power BI needed) for all pure logic, with
   mock-tested orchestration for live paths.
2. **Adversarial doc-verification**: API contracts (REST shapes, INFO functions, PBIR
   schemas, TMDL shapes, the Desktop Bridge protocol) are fact-checked against Microsoft Learn
   and real exports before implementation, and audit findings are independently verified
   before they are acted on.
3. **Engine-level validation**: generated TMDL parses under Microsoft's own `TmdlSerializer`
   (the code path Desktop uses to open a PBIP) with every property round-tripping.
4. **Live testing** against a running Power BI Desktop: ADOMD queries, TOM batch writes with
   compensating rollback, star-schema audit on a real model, Desktop Bridge discovery,
   manifest, state, and hot-reload (both clean and unsaved-guard paths).

Cloud XMLA/REST/admin paths remain doc-verified + mock-tested; their end-to-end verification
needs a real tenant.

## File map
```
src/
  server.py                     MCP server: registry, dispatch, resources/prompts/completion
  adomd_loader.py               Shared ADOMD.NET discovery (env var, Desktop, GAC, SSMS, NuGet)
  powerbi_desktop_connector.py  Desktop (ADOMD) + RLS + VertiPaq DMVs
  powerbi_xmla_connector.py     Cloud XMLA
  powerbi_rest_connector.py     REST: discovery, refresh, admin Scanner/Activity
  powerbi_tom_connector.py      TOM writes: measures (incl. validated batches), relationships
  powerbi_pbip_connector.py     PBIP/TMDL/PBIR offline editing + authoring (transactional)
  desktop_bridge.py             Desktop Bridge client (JSON-RPC over named pipe)
  pbir_authoring.py             PBIR emitters: pages, visuals, field projections
  tmdl_authoring.py             TMDL emitters: measures, date table, calc groups, hierarchies
  dax_generator.py              Bulk measure-suite generation (time intel, ratios, ranks)
  svg_measures.py               SVG micro-visual DAX measure generators
  dax_lint.py                   DAX anti-pattern linter + rewrite hints
  star_schema.py                Star-schema classification + warehouse audit
  naming_audit.py               Naming-convention audit -> rename plan
  pbix_tools.py                 PBIX inspect/extract, Layout decode, embedded-PBIR pages
  bpa_authoring.py              Custom BPA rule validation + rule-source audit
  model_analysis.py             BPA, AI-readiness, data dictionary, diff, DAX tests
  refresh_diagnostics.py        Refresh error classification
  governance.py                 Scanner summary + activity aggregation
  security/                     security_layer, access_policy, pii_detector, audit_logger
config/policies.yaml            Access policy definitions
tests/                          25 assert-based suites (run via run_tests.py)
docs/                           This documentation
```
