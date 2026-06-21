# Architecture

## Overview

A Python **stdio MCP server** (`src/server.py`) that routes tool calls to lazily-created
connectors, wrapped by a security layer, with pure-Python analysis modules for the
logic that doesn't need Power BI.

```
        MCP client (Claude / Copilot / VS Code)
                      │  stdio (JSON-RPC)
        ┌─────────────▼──────────────┐
        │   server.py (PowerBIMCPServer)
        │   - tool registry: dispatch + annotations (single source of truth)
        │   - resources / prompts / completion handlers
        │   - read-only/lockdown gate
        └───┬───────────┬───────────┬───────────┬───────────┬────────┘
            │           │           │           │           │
      Desktop      XMLA/REST       TOM         PBIP      Security layer
   (ADOMD,local) (cloud/admin)  (live write) (offline)  (PII/audit/policy)
            │           │           │           │
   Power BI Desktop   Power BI    in-memory    .pbip files
                      Service     model        (TMDL + PBIR)

   Pure-Python analysis (no Power BI): model_analysis.py, refresh_diagnostics.py,
   governance.py, security/*
```

## Components

### `server.py`
- **Tool registry.** `_build_tool_dispatch()` (name → handler) and `_build_tool_annotations()`
  (name → `ToolAnnotations`) are the single source of truth; `handle_list_tools` and
  `handle_call_tool` derive from them, so the advertised list and the router cannot drift.
  A parity test enforces that `handle_list_tools`, the dispatch map, and the annotation map
  all contain exactly the same tool names.
- **Read-only/lockdown gate.** When `POWERBI_MCP_READONLY=true`, any tool in the computed
  write set (destructive ops + `create_measure`/`create_relationship`/`tom_commit_transaction`)
  is refused before dispatch.
- **Structured output.** Handlers return either a string or a `(text, dict)` tuple; tools
  with an `outputSchema` return the tuple so clients get typed `structuredContent`.
- **Resources / prompts / completion.** Model context (`powerbi://...`), reusable BI prompts,
  and argument completion grounded in the live model.

### Connectors (`src/powerbi_*_connector.py`)
- **Desktop** — connects to the local Analysis Services instance via ADOMD.NET (pythonnet);
  DAX, metadata, RLS testing, VertiPaq DMVs.
- **XMLA / REST** — cloud datasets (XMLA via pyadomd; REST via MSAL service principal for
  discovery, refresh, and admin Scanner/Activity APIs).
- **TOM** — live in-memory model writes (measures, relationships, transactions).
- **PBIP** — pure-filesystem editing of TMDL + PBIR; report-aware safe renames with a
  transactional, atomic (temp-file + `os.replace`), encoding/BOM-preserving write path.

### Security layer (`src/security/`)
`security_layer.py` wraps `access_policy.py` (enforced column/table policies incl. BLOCK /
MASK / HASH / REDACT / NUMERIC_MASK), `pii_detector.py`, and `audit_logger.py` (JSON-lines
with a tamper-evident hash chain). DAX result keys (`Table[Column]`) are parsed so column
policies actually fire.

### Pure-Python analysis (cross-platform, unit-tested)
- `model_analysis.py` — BPA rules, AI-readiness scorer, data-dictionary renderer, model diff,
  DAX test verdicts.
- `refresh_diagnostics.py` — refresh error → cause → remediation knowledge base.
- `governance.py` — Scanner-result summary (lineage/RLS/label coverage) and activity aggregation.

## Platform constraints
- **Live connectivity is Windows-only** (ADOMD.NET / TOM via .NET). The server imports and
  runs without them — those tools just report themselves unavailable.
- **The offline subset runs cross-platform** (PBIP editing, BPA, AI-readiness, model analysis,
  security, resources/prompts) — see the `Dockerfile` / `requirements-core.txt`.
- Cloud needs a service principal + (for enhanced refresh) Premium/PPU. Admin/Scanner/Activity
  tools need Fabric admin or an SP allowed to use read-only admin APIs.

## Verification methodology
Because live Power BI/Fabric isn't available in CI here, new live-path code is validated two
ways: (1) the orchestration/parsing logic is unit/mock tested in `tests/`, and (2) the exact
API contracts (REST shapes, INFO functions, PBIR schema, Scanner/Activity APIs) are
adversarially fact-checked against Microsoft Learn before merge. End-to-end verification of
the live paths still requires a Windows + Power BI / Fabric environment.

## File map
```
src/
  server.py                     MCP server: registry, dispatch, resources/prompts/completion
  powerbi_desktop_connector.py  Desktop (ADOMD) + RLS + VertiPaq DMVs
  powerbi_xmla_connector.py     Cloud XMLA
  powerbi_rest_connector.py     REST: discovery, refresh, admin Scanner/Activity
  powerbi_tom_connector.py      TOM writes: measures, relationships, transactions
  powerbi_pbip_connector.py     PBIP/TMDL/PBIR offline editing (transactional)
  model_analysis.py             BPA, AI-readiness, data dictionary, diff, DAX tests (pure)
  refresh_diagnostics.py        Refresh error classification (pure)
  governance.py                 Scanner summary + activity aggregation (pure)
  security/                     security_layer, access_policy, pii_detector, audit_logger
config/policies.yaml            Access policy definitions
tests/                          Assert-based suites (run via run_tests.py)
docs/                           This documentation
```
