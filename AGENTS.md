# Power BI MCP Server - Agent Guide

This file orients AI agents (Claude, Copilot, etc.) working through this MCP server.
Read it before driving Power BI tasks.

## What this server does

A Model Context Protocol server that lets an agent inspect, query, validate, optimize,
govern, and safely refactor Power BI semantic models and reports. It connects to:

- **Power BI Desktop** (local Analysis Services) - read + write, RLS testing
- **Power BI Service** (XMLA + REST) - cloud datasets
- **PBIP / TMDL / PBIR files** (offline) - safe, report-aware refactoring

It also runs fully cross-platform for the offline + analysis subset (PBIP editing,
Best Practice Analyzer, AI-readiness, security) even without ADOMD/.NET.

## Golden rules

1. **Renaming: always use the PBIP tools, never the TOM `batch_rename_*` tools.**
   TOM renames only touch the model and break report visuals. `pbip_rename_tables`
   / `pbip_rename_columns` / `pbip_rename_measures` update the model AND the report
   layer (visual.json, cultures, diagram). Close Power BI Desktop before PBIP edits.
2. **Validate DAX before you commit it.** Call `validate_dax` on any new/edited
   measure expression. `create_measure` and `batch_update_measures` validate
   automatically (pass `skip_validation: true` only if you must).
3. **Batch model edits inside a transaction.** `tom_begin_transaction` ->
   edits -> `tom_commit_transaction` (or `tom_rollback_transaction`). Edits are
   atomic and reversible.
4. **Check impact before destructive changes.** `scan_measure_dependencies` (model
   dependents) and `pbip_scan_broken_refs` (report references) before delete/rename.
5. **Respect the safety hints.** Every tool is annotated (`readOnlyHint`,
   `destructiveHint`). Confirm destructive operations with the user.

## Recommended workflows

- **Author a measure:** `validate_dax` (draft) -> `create_measure` (auto-validates)
  -> `analyze_query_performance` to sanity-check.
- **Optimize a model:** `run_bpa` -> `audit_ai_readiness` -> `analyze_model_storage`
  -> remediate top issues -> re-run.
- **Improve DAX:** `dax_lint` (whole model or one measure) -> `dax_suggest_rewrite` ->
  apply the rewrite with `create_measure`/`batch_update_measures` (which `validate_dax` first).
- **Bulk-create measures:** `generate_measure_suite` (target='none' to preview) -> review ->
  re-run with target='pbip' (offline) or target='live' / `batch_create_measures` (validated batch).
- **Model a warehouse:** `audit_star_schema` -> `pbip_create_date_table` (if no date dim) ->
  `create_relationship` fact-to-date -> `pbip_add_calculation_group` (time_intelligence preset)
  or `generate_measure_suite` -> `pbip_add_hierarchy` -> `scan_referential_integrity` to verify.
- **Safe rename:** `scan_measure_dependencies` + `pbip_scan_broken_refs` ->
  `pbip_load_project` -> `pbip_rename_*` -> `pbip_validate`.
- **Author a report (PBIR, preview):** `pbip_load_project` -> `pbir_add_page` ->
  `pbir_add_visual` (bind fields by role) / `pbir_bind_fields` -> `pbir_validate_report`.
  Pass fields as `Table.Field`; the server picks measure vs aggregated-column from the model,
  so prefer naming explicit measures for value wells. Close Power BI Desktop before editing.
- **Ground yourself first:** read the `powerbi://desktop/schema` resource (or
  `get_model_info`) before generating DAX, so you use real table/column names.

## Prompts (guided workflows)

`optimize_measure`, `explain_measure`, `audit_model`, `document_model`,
`plan_safe_rename` - invoke these for ready-made, tool-orchestrated playbooks.

## Resources

`powerbi://desktop/schema`, `.../measures`, `.../bpa`, `.../ai-readiness`, and the
template `powerbi://cloud/{workspace}/{dataset}/schema` expose model context as
read-only resources (no tool call needed).

## DAX patterns the agent should prefer

- Use `DIVIDE(n, d)` instead of `n / d` (safe divide-by-zero).
- Use `SUMMARIZECOLUMNS(...)` instead of `SUMMARIZE` + `ADDCOLUMNS`.
- Use variables (`VAR`/`RETURN`) to avoid recomputing sub-expressions.
- Filter with boolean predicates inside `CALCULATE` rather than wrapping whole
  tables in `FILTER` when possible.
- Always set a `FormatString` and a `Description` on measures (helps Copilot too).

## Governance

A security layer can mask/block PII and sensitive columns and audit every query
(see `config/policies.yaml`, tools `security_status` / `security_audit_log`).
Column policies are enforced on `execute_dax` / `desktop_execute_dax` results.

## Positioning vs Microsoft's official Power BI MCP

Microsoft's official **remote** server is best for cloud chat-with-data, and the
official **local modeling** MCP for raw model authoring. This server is
complementary and differentiates on: report-layer-aware safe renames (which the
official local MCP explicitly cannot do), a governance/PII layer, RLS testing,
and an offline PBIP-first workflow that needs no Fabric capacity.
