# Power BI MCP Server - Agent Guide

This file orients AI agents (Claude, Copilot, etc.) working through this MCP server.
Read it before driving Power BI tasks.

## What this server does

A Model Context Protocol server (94 tools) that lets an agent inspect, query, validate,
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
   changes) -> `bridge_reload` -> `bridge_screenshot`. Do not ask the user to close and reopen;
   if the report is not open, `bridge_open` opens it and waits for the bridge.
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
3. Get the bespoke visuals the report needs (see "Choosing a visual technique" below):
   - `generate_deneb_spec` returns a ready `deneb` element with the interactivity wired in;
   - `build_custom_visual` packages a real `.pbiviz` from a definition, or let the spec build it:
     `"custom_visuals": [{"definition": "visuals/bar_list/definition.json", "alias": "bars"}]`.
4. Describe the report in a spec (see `docs/REPORT_SPEC.md`): theme, pages, elements with stable
   names, navigation buttons, synced slicers, pop-ups, elements shared across pages, and the
   native report features: `filters` on the report, a page or a visual; `drillthrough` pages and
   drillthrough buttons; report-page tooltips (`tooltip` on the page, `tooltip_page` on the
   visual); `interactions` between visuals; `mobile` positions for the phone layout;
   `bookmark_groups`.
5. `pbir_apply_spec` with `dry_run: true`, fix what it reports, then apply. Re-applying the same
   spec changes nothing; `prune: true` removes what an earlier apply created and the spec
   dropped. Filters, interactions and drillthrough fields a person added in Desktop are kept.
6. `audit_report_design` -> `bridge_open` (if the report is not open yet; `minimized: true` keeps
   the window out of the way) -> `bridge_reload` -> `bridge_screenshot` -> look, fix the spec,
   repeat. `bridge_screenshot` with `visual: "<element name>"` captures one visual, which is far
   cheaper to read than a page.
7. To publish: `pbip_embed_data` if the model reads local files (dry run first), then
   `pbip_stage_publish`. It prepares the folder and prints the import command; it never
   contacts the service.

### Choosing a visual technique

What a reader can do with the visual decides the technique, not how it looks.

| Technique | Click cross-filters the page | Is cross-highlighted | Tooltips, report-page tooltips | Right-click menu, drillthrough | Certified |
|---|---|---|---|---|---|
| Native visual | yes | yes | yes | yes | yes |
| Deneb, spec from `generate_deneb_spec` | yes | yes | yes | yes | yes (AppSource edition) |
| Custom visual from `build_custom_visual` | yes, when it has a grouping role | yes | yes | yes | no (private `.pbiviz`) |
| HTML measure in HTML Content, Secure edition | no | no | one plain tooltip | no | yes |
| JavaScript app in a measure (HTML Content, Regular edition) | no | no | one plain tooltip | no | no |

- Need the visual to take part in the page (cross-filter, drillthrough, tooltip pages)? Use a
  native visual, Deneb, or `build_custom_visual`. A JavaScript app written for a measure
  (`function main(app)`) builds unchanged as a custom visual: its data then arrives from data
  roles and its rows become real data points.
- An HTML measure is still the lightest way to draw a designed card or sentence, and report
  slicers still filter it, because slicers change what the measure returns. A click inside it
  filters nothing. HTML Content can cross-filter only when a column sits in its Context role and
  the option is on, which a single-measure design does not have.
- The `html` element takes `"edition": "secure"` (certified; scripts and inline handlers are
  removed) or `"regular"` (the default; runs scripts, not certified). A static HTML measure works
  in both; a JavaScript app only in Regular.
- A tenant that allows only certified visuals renders Deneb and HTML Content Secure, and blocks
  HTML Content Regular and private custom visuals. `audit_report_design` says which a report
  uses, and what each private custom visual can and cannot do on a click
  (`inspect_custom_visual` for one package).
- In a custom visual's render code, mark each data point with `data-row="<row index>"`: the
  runtime then handles click, Ctrl-click, right-click, hover and keyboard for it.
- A payload measure returns one string. The practical ceiling is about 2.1 million characters
  (an empirical figure); `generate_payload_measure` estimates how many rows fit.
- Never put a secret or an API key in a measure or a visual: everything in them is readable.
- What was verified how: generated reports, Deneb specs and built visuals were loaded and
  rendered in Power BI Desktop, and a built visual's selection, highlight, tooltip and context
  menu calls were answered by the real host. Mouse clicks and hovering cannot be driven through
  the Desktop Bridge, so ask the user to click through a new interaction once.

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
