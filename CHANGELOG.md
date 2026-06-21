# Changelog

All notable changes to the Power BI MCP Server. Format based on
[Keep a Changelog](https://keepachangelog.com/); this project uses date-stamped milestones.

## [3.2.0] - 2026-06-21 — DAX performance linter

Grew the server from **62 to 64 tools** with a pure-Python DAX static analyzer ("BPA for DAX").
No external tool, no DAX engine required: it tokenizes an expression and runs an original rule
set, so it works fully offline and on raw expressions, a single measure, or every measure in the
connected model.

### Added
- **`dax_lint`** — flags performance anti-patterns and correctness traps with a severity, line,
  and concrete rewrite hint each:
  - DL001 FILTER over a whole table inside CALCULATE
  - DL002 nested CALCULATE (extra context transition)
  - DL003 `/` division instead of DIVIDE (divide-by-zero risk)
  - DL004 IFERROR (optimizer fence)
  - DL005 `+ 0` blank-to-zero suppression
  - DL006 EARLIER (legacy; prefer VAR)
  - DL007 SUMMARIZE used to host an aggregation (wrong-result/perf trap)
  - DL008 unrecognized / likely-hallucinated function name
- **`dax_suggest_rewrite`** — before/after rewrite hints for the auto-fixable rules
  (`/` to DIVIDE, FILTER-whole-table to a boolean filter, SUMMARIZE to SUMMARIZECOLUMNS).
- New pure module **`dax_lint.py`** (comment/string-aware tokenizer + balanced-paren rule engine)
  and **`tests/test_dax_lint.py`** (each rule fires on its anti-pattern and stays silent on the
  clean equivalent; comments and string literals never create false positives).

---

## [3.1.0] - 2026-06-21 — PBIR report authoring (preview)

Grew the server from **58 to 62 tools** by adding offline **report authoring** on PBIR-Enhanced
PBIP projects, so an agent can build report pages and visuals, not just the semantic model.
The emitted PBIR is verified against Microsoft's published JSON schemas
(`github.com/microsoft/json-schemas`) and matches how Power BI Desktop itself writes files.

### Added — report authoring
- **`pbir_add_page`** — add a report page and register it in `pages.json` (active-page aware).
- **`pbir_add_visual`** — add a visual (bar/column/line/pie/card/table/slicer/gauge/KPI...) and
  bind fields by role in one call; field existence is validated against the model first.
- **`pbir_bind_fields`** — add or replace field projections on an existing visual.
- **`pbir_validate_report`** — report-wide check that every visual binding points at a real
  model field (the #1 cause of blank visuals after external edits).
- New pure module **`pbir_authoring.py`**: PBIR emitters (pages, visuals, field projections)
  that are the verified inverse of the report-reference parser.

### Desktop-fidelity (matches Power BI's own output, verified against published schemas)
- Every projection now carries **`nativeQueryRef`** (the bare field name), as Power BI writes it,
  keeping source-control diffs stable instead of letting Desktop rewrite the file on first save.
- A plain **column dropped on a value well** (Y / Values / ...) is wrapped in an **`Aggregation`**
  node with the matching **`Sum(...)` / `CountNonNull(...)` `queryRef`** (Sum for numeric columns,
  CountNonNull for text), while explicit **measures stay bare** — chosen automatically from the
  model field catalog (now data-type aware).
- `$schema` is **inherited from a sibling file** in the same project when present; the fallback
  defaults were refreshed to the current published versions (visualContainer 2.9.0, page 2.1.0).

### Fixed
- Report-reference scanning no longer mis-parses an aggregation-form `queryRef` such as
  `Sum(Sales.Amount)` into a phantom `("Sum(Sales", "Amount)")` reference (which would have made
  `pbir_validate_report` / `validate_report_bindings` report false missing tables).

---

## [3.0.1] - 2026-06-21 — ADOMD.NET discovery fix (issue #12)

### Fixed
- **ADOMD.NET not found on newer Power BI Desktop installs.** Recent Power BI Desktop (MSI)
  builds no longer ship `Microsoft.AnalysisServices.AdomdClient.dll`, so Desktop/XMLA
  connectivity failed even with Power BI Desktop installed. Discovery now also searches the
  **GAC** (`GAC_MSIL\Microsoft.AnalysisServices.AdomdClient`, where Power BI Desktop and SSMS
  actually register the standard assembly), **SSMS** (any version, x64 and x86), the **SQL
  Server SDK** and **Update Cache** (any version, not hard-coded numbers), the legacy
  ADOMD.NET MSI folder, and **ADOMD.NET NuGet** packages, and honors an explicit
  **`ADOMD_DLL_PATH`** environment variable (folder or full DLL path). (Power BI Desktop ships
  its own `Microsoft.PowerBI.AdomdClient.dll` in `bin`, not the standard assembly pyadomd needs.)

### Changed
- The Desktop and XMLA connectors now share one `adomd_loader.py` (removing the previous two
  divergent discovery copies).
- Clearer "ADOMD.NET not found" guidance in logs and the `desktop_discover_instances` error,
  and corrected the README prerequisite note (ADOMD.NET no longer ships with Power BI Desktop).

---

## [3.0.0] - 2026-06-21 — Agentic, Productivity & Governance overhaul

Grew the server from **34 to 58 tools** and made it a first-class MCP citizen, while
fixing several latent correctness and security bugs. New cloud/Desktop/admin code paths
are doc-verified against Microsoft Learn and unit/mock tested; they still need a live
Windows + Power BI / Fabric environment for end-to-end verification.

### Fixed (correctness)
- **Access policies were silently not enforcing.** `process_results` / `apply_to_results`
  received no table context and matched columns by exact dict key, so column-level
  BLOCK/MASK/HASH/REDACT policies never fired. Now DAX result keys (`Table[Column]`) are
  parsed, the `*` wildcard policy is consulted, and every action fires. Referenced
  tables/columns are extracted from the query for pre-query checks.
- **Two silently-shadowed duplicate methods** in the PBIP connector
  (`fix_all_dax_quoting`, `_deep_rename_column_in_json`); the richer implementations are
  now the only ones.
- **Cloud `execute_dax` was unbounded**; results are now capped (policy-aware + a hard ceiling).
- PBIR reference scanning now handles hierarchy/level references and From-clause `Source`
  aliases, and resolves each reference's table from its own expression (no mis-attribution).
- `refresh` history parsing handles the full status enum (`Unknown|Completed|Failed|Disabled`).
- `INFO.CALCDEPENDENCY` error messages now state the real constraint (needs write
  permission; not over a live Desktop connection), not "old engine".

### Added — agentic core
- **DAX safety loop:** `validate_dax` (validate a query/measure before committing);
  `create_measure` / `batch_update_measures` validate before they commit.
- **Transactions:** `tom_begin_transaction` / `tom_commit_transaction` /
  `tom_rollback_transaction` — atomic, reversible bulk model edits.
- **Dependency/impact:** `scan_measure_dependencies` (INFO.CALCDEPENDENCY).
- **Relationships:** `create_relationship`, `delete_relationship`.
- **Model quality & performance:** `run_bpa` (Best Practice Analyzer), `audit_ai_readiness`,
  `analyze_model_storage` (VertiPaq-style), `analyze_query_performance`.
- **Transactional, atomic, encoding-faithful PBIP renames** (auto-rollback on failure;
  temp-file + `os.replace` writes; BOM/CRLF preserved).

### Added — productivity, CI & documentation
- `export_data_dictionary` (Markdown/HTML + documentation-coverage score).
- `model_snapshot` + `model_diff` (semantic diff vs a baseline snapshot or the live model).
- `pre_deploy_gate` (machine PASS/FAIL on BPA + AI-readiness, for CI).
- `run_dax_tests` (DAX regression test runner).

### Added — diagnostics & ops
- `refresh_doctor` (classify refresh failures + remediation; works on Pro).
- `find_unused_objects` (model + report PBIR), `impact_analysis` (blast radius),
  `rls_test_harness` (role-by-role pass/fail matrix).

### Added — governance-ops fleet (admin/Premium-gated)
- `cross_workspace_lineage` (Scanner API: tenant inventory, RLS/label coverage, downstream
  reports per dataset), `fleet_refresh_monitor`, `usage_and_orphan_analytics` (Activity Events).

### Added — modern MCP surface
- **Tool annotations** (`readOnlyHint` / `destructiveHint` / `idempotentHint` / `openWorldHint`)
  on every tool, via a single dispatch+annotation registry (no more drift between the tool
  list and the call router).
- **Structured output** (`outputSchema` + `structuredContent`) on `validate_dax`, `run_bpa`,
  `audit_ai_readiness`, `pre_deploy_gate`, `run_dax_tests`, `refresh_doctor`.
- **Resources:** `powerbi://desktop/{schema|measures|bpa|ai-readiness}`,
  `powerbi://cloud/{workspace}/{dataset}/schema`, `powerbi://reference/{bpa-rules|refresh-errors}`.
- **Prompts:** `optimize_measure`, `explain_measure`, `audit_model`, `document_model`,
  `plan_safe_rename`, `pre_deploy_review`.
- **Completion** for prompt/resource arguments from the connected model.

### Security
- Connection-string secrets and PII are redacted from logs and error responses; verbose
  argument logging is gated behind DEBUG.
- **Tamper-evident audit log** (hash chain) + `verify_audit_integrity`.
- **Numeric masking** policy action (session-randomized, statistics-preserving).
- **Read-only / lockdown mode** (`POWERBI_MCP_READONLY=true`) refuses every write tool.

### Changed / Docs
- README rewritten for the 58-tool surface with positioning vs Microsoft's official MCP.
- Added `AGENTS.md` / `CLAUDE.md` (agent playbook), `Dockerfile` + `requirements-core.txt`
  (cross-platform offline image), and the `docs/` set.
- Tests moved to `tests/`; added `run_tests.py`.

---

## [2.0.0] — Desktop connectivity, TOM writes, PBIP safe editing, security layer
Initial public baseline: Desktop (ADOMD) + Cloud (XMLA/REST) connectivity, TOM write
operations, PBIP file-based safe renames, and the PII/audit/policy security layer.
