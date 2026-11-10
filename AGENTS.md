# Power BI MCP Server - Agent Guide

This file orients AI agents (Claude, Copilot, etc.) working through this MCP server.
Read it before driving Power BI tasks.

## What this server does

A Model Context Protocol server (90 tools) that lets an agent inspect, query, validate,
optimize, govern, safely refactor and build Power BI semantic models and reports. It connects to:

- **Power BI Desktop** (local Analysis Services) - read + write, RLS testing
- **Power BI Service** (XMLA + REST) - cloud datasets
- **PBIP / TMDL / PBIR files** (offline) - safe, report-aware refactoring AND authoring:
  measures, date tables, calculation groups, hierarchies, and whole reports from a spec
- **The running Desktop app** (Desktop Bridge, preview) - open-file state, hot-reload
  from disk, page screenshots

It also runs fully cross-platform for the offline + analysis subset (PBIP editing, report
building, Best Practice Analyzer, linters, security) even without ADOMD/.NET.

## Golden rules

1. **Renaming: always use the PBIP tools, never the TOM `batch_rename_*` tools.**
   A live TOM rename only touches the model and breaks report visuals. `pbip_rename_tables`
   / `pbip_rename_columns` / `pbip_rename_measures` update the model AND the report (visuals,
   filters, bookmarks, Deneb specs, cultures, hierarchy levels, sortByColumn) in one
   all-or-nothing change. Read the result: it lists warnings for text that mentions the old
   name but is not a reference (titles, labels). `pbip_undo` restores the previous state.
2. **Validate DAX before you commit it.** Call `validate_dax` on any new/edited measure
   expression (pass `table_name` with `as_measure` to compile it as a real measure).
   `create_measure`, `batch_create_measures` and `batch_update_measures` validate
   automatically (pass `skip_validation: true` only if you must).
3. **Batch model edits inside a transaction.** `tom_begin_transaction` -> edits ->
   `tom_commit_transaction` (or `tom_rollback_transaction`). If a commit fails, nothing was
   saved and the transaction is still open: fix the problem and commit again, or roll back.
4. **Check impact before destructive changes.** `scan_measure_dependencies` (model
   dependents) and `pbip_scan_broken_refs` (report references) before delete/rename.
5. **Respect the safety hints.** Every tool is annotated (`readOnlyHint`,
   `destructiveHint`). Confirm destructive operations with the user.
6. **After any offline edit, reload and look.** `bridge_status` (never reload over unsaved
   changes) -> `bridge_reload` -> `bridge_screenshot`. Do not ask the user to close and reopen.
7. **Dry-run first** where a tool offers it: `pbir_apply_spec`, `pbip_embed_data`,
   `pbip_fix_dax_quoting`.
8. **Report results as they are.** A tool that says nothing was written means nothing was
   written. Generated DAX is not evaluated by the generators: validate it. A generated report
   has not been seen by anyone until you screenshot it.

## Recommended workflows

- **Ground yourself first:** read the `powerbi://desktop/schema` resource (or
  `get_model_info`) before generating DAX, so you use real table/column names.
- **Author a measure:** `validate_dax` (draft) -> `create_measure` (auto-validates)
  -> `analyze_query_performance` to sanity-check.
- **Optimize a model:** `run_bpa` -> `audit_ai_readiness` -> `analyze_model_storage`
  -> remediate top issues -> re-run.
- **Improve DAX:** `dax_lint` (whole model or one measure) -> `dax_suggest_rewrite` (a `/` is
  rewritten for you; other hints are templates) -> `validate_dax` -> apply.
- **Bulk-create measures:** `generate_measure_suite` (target='none' to preview) -> review ->
  re-run with target='pbip' (offline) or target='live' (validated batch).
- **Model a warehouse:** `audit_star_schema` -> `pbip_create_date_table` (if no date dim) ->
  `create_relationship` fact-to-date -> `pbip_add_calculation_group` (time_intelligence preset)
  or `generate_measure_suite` -> `pbip_add_hierarchy` -> `scan_referential_integrity` to verify.
- **Safe rename:** `scan_measure_dependencies` + `pbip_scan_broken_refs` ->
  `pbip_load_project` -> `pbip_rename_*` -> `pbir_validate_report` -> `bridge_reload`.
  A naming plan from `audit_naming` flags suggestions that would collide (`safe: false`):
  do not apply those as they are.

### Build a report as code

The server can build a designed report the way a build script would, from one spec.

1. `pbip_load_project` (the report must be in PBIR format, with `definition.pbir`).
2. Write the measures the report needs:
   - ordinary measures: `pbip_add_measures` with `mode: "upsert"` (re-runs replace in place);
   - KPI cards, headline sentences, bar lists as HTML: `generate_html_measure`;
   - micro-charts as SVG: `generate_svg_measure`;
   - a JavaScript app: `generate_payload_measure` for the data (rows packed into one text
     measure, with the parser to read them) and `build_js_app_measure` for the app itself.
3. Describe the report in a spec (see `docs/REPORT_SPEC.md`): theme, pages, elements with stable
   names, navigation buttons, synced slicers, pop-ups, elements shared across pages.
4. `pbir_apply_spec` with `dry_run: true`, fix what it reports, then apply. Re-applying the same
   spec changes nothing; `prune: true` removes what an earlier apply created and the spec
   dropped.
5. `audit_report_design` -> `bridge_reload` -> `bridge_screenshot` -> look, fix the spec, repeat.
6. To publish: `pbip_embed_data` if the model reads local files (dry run first), then
   `pbip_stage_publish`. It prepares the folder and prints the import command; it never
   contacts the service.

Things to know when you choose a technique:
- A native visual cross-filters other visuals. An HTML Content visual, a JavaScript app in a
  measure, and most bespoke custom visuals do not: a click inside them filters nothing outside.
  Report slicers still filter them, because slicers change what the measure returns.
- HTML Content, Deneb and private custom visuals are not certified. A tenant that only allows
  certified visuals will not render them. `audit_report_design` lists them.
- A payload measure returns one string. The practical ceiling is about 2.1 million characters
  (an empirical figure); `generate_payload_measure` estimates how many rows fit.
- Never put a secret or an API key in a measure: everything in a measure is readable.

## Prompts (guided workflows)

`optimize_measure`, `explain_measure`, `audit_model`, `document_model`,
`plan_safe_rename`, `pre_deploy_review` - invoke these for ready-made, tool-orchestrated
playbooks.

## Resources

`powerbi://desktop/schema`, `.../measures`, `.../bpa`, `.../ai-readiness`, the template
`powerbi://cloud/{workspace}/{dataset}/schema`, and the reference resources
`powerbi://reference/bpa-rules` and `powerbi://reference/refresh-errors` expose model
context as read-only resources (no tool call needed).

## DAX patterns the agent should prefer

- Use `DIVIDE(n, d)` instead of `n / d` (safe divide-by-zero).
- Use `SUMMARIZECOLUMNS(...)` instead of `SUMMARIZE` + `ADDCOLUMNS`.
- Use variables (`VAR`/`RETURN`) to avoid recomputing sub-expressions.
- Filter with boolean predicates inside `CALCULATE` rather than wrapping whole
  tables in `FILTER` when possible.
- Qualify columns with their table (`Sales[Amount]`) and leave measures unqualified
  (`[Total Sales]`). It keeps DAX readable and lets renames be exact.
- Always set a `FormatString` and a `Description` on measures (helps Copilot too).

## Governance

- The query tools accept DAX queries only. TMSL, XMLA and MDX are refused.
- Access policies (`config/policies.yaml`) block, mask, hash or redact columns in the results
  of every tool that runs DAX, follow aliases and measures, and refuse queries outright when the
  policy file cannot be loaded (`security_status` explains). They are defence in depth: the
  boundary is row-level and object-level security in the model.
- Queries routed through these tools are written to an audit log (`security_audit_log`,
  `verify_audit_integrity`).
- In read-only mode (`POWERBI_MCP_READONLY`) every tool that changes a model, a report or a
  file is refused.

## Positioning vs Microsoft's official Power BI MCP

Microsoft's official **remote** server is best for cloud chat-with-data, and the
official **local modeling** MCP for raw model authoring. This server is
complementary and differentiates on: report-layer-aware safe renames (which the
official local MCP explicitly cannot do), building designed reports from a spec, a
governance/PII layer, RLS testing, and an offline PBIP-first workflow that needs no
Fabric capacity.
