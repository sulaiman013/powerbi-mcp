# Tool Reference

The Power BI MCP server exposes **90 tools**, plus MCP **resources**, **prompts**, and
**completion**. Every tool carries MCP annotations (`readOnlyHint` / `destructiveHint` /
`idempotentHint` / `openWorldHint`) so clients can auto-approve reads and confirm writes.
29 of them also return typed `structuredContent`.

Legend: 🟢 read-only · 🟡 write (non-destructive) · 🔴 destructive

In read-only mode (`POWERBI_MCP_READONLY`) every 🟡 and 🔴 tool is refused, except the
session-scoped ones that change no model, report or file (`desktop_connect`,
`desktop_set_rls_role`, `tom_begin_transaction`, `tom_rollback_transaction`) and the three that
refuse only their write option (`generate_measure_suite`, `build_js_app_measure`,
`cross_workspace_lineage`).

## Desktop (local Analysis Services, ADOMD) — 7
| Tool | | Description |
|------|--|-------------|
| `desktop_discover_instances` | 🟢 | Discover running Power BI Desktop instances |
| `desktop_connect` | 🟡 | Connect to an instance (optional RLS role) |
| `desktop_list_tables` | 🟢 | List tables |
| `desktop_list_columns` | 🟢 | List columns for a table |
| `desktop_list_measures` | 🟢 | List measures |
| `desktop_execute_dax` | 🟢 | Execute a DAX query (query-only, policy-checked, masked, audited) |
| `desktop_get_model_info` | 🟢 | Tables, measures, relationships |

## Cloud (Power BI Service, XMLA + REST) — 6
| Tool | | Description |
|------|--|-------------|
| `list_workspaces` | 🟢 | Workspaces accessible to the service principal |
| `list_datasets` | 🟢 | Datasets in a workspace |
| `list_tables` | 🟢 | Tables in a dataset (XMLA) |
| `list_columns` | 🟢 | Columns for a table |
| `execute_dax` | 🟢 | Execute DAX against a cloud dataset (query-only, row-capped at the source, policy-checked) |
| `get_model_info` | 🟢 | Model info via INFO.VIEW functions |

> The two execute tools accept DAX queries only (`EVALUATE` / `DEFINE`). TMSL, XMLA, MDX and
> other commands are refused before anything reaches the engine, in the server and again in the
> connectors, so a tool annotated read-only cannot change a model.

## Security & audit — 3
| Tool | | Description |
|------|--|-------------|
| `security_status` | 🟢 | PII / audit / policy configuration, including whether the policy file failed to load (the layer is then locked) |
| `security_audit_log` | 🟢 | Recent audit log entries |
| `verify_audit_integrity` | 🟢 | Verify the audit hash chain across rotated files; says plainly what an unkeyed chain cannot detect |

## Row-Level Security — 3
| Tool | | Description |
|------|--|-------------|
| `desktop_list_rls_roles` | 🟢 | List RLS roles |
| `desktop_set_rls_role` | 🟡 | Activate/clear an RLS role for testing (a failed switch keeps the previous role) |
| `desktop_rls_status` | 🟢 | Current RLS status |

## Model writes via TOM — 7
| Tool | | Description |
|------|--|-------------|
| `create_measure` | 🟡 | Create a measure (compiled as a real measure on its table first; honors transactions) |
| `delete_measure` | 🔴 | Delete a measure |
| `batch_update_measures` | 🔴 | Bulk-update measure expressions (validates first) |
| `scan_table_dependencies` | 🟢 | Analyze a table's dependents before a rename |
| `batch_rename_tables` | 🔴 | ⚠️ DEPRECATED — use `pbip_rename_tables` (a live rename cannot update report visuals) |
| `batch_rename_columns` | 🔴 | ⚠️ DEPRECATED — use `pbip_rename_columns` |
| `batch_rename_measures` | 🔴 | ⚠️ DEPRECATED — use `pbip_rename_measures` |

## DAX safety loop & transactions — 5
| Tool | | Description |
|------|--|-------------|
| `validate_dax` | 🟢 | Validate a query or a measure expression against the model without committing; `table_name` validates it as a real query-scoped measure |
| `scan_measure_dependencies` | 🟢 | Upstream/downstream impact via INFO.CALCDEPENDENCY |
| `tom_begin_transaction` | 🟡 | Start a write transaction: live edits are held until commit |
| `tom_commit_transaction` | 🟡 | Save all pending edits at once; a failed commit leaves the transaction open |
| `tom_rollback_transaction` | 🟡 | Discard pending edits |

> The transaction is owned by the TOM connector. While it is open no tool saves (the deprecated
> renames included), and the connector refuses to switch to another Desktop instance. Measures
> created inside it are declared in later validation probes, so an edit may reference them
> before they are saved.

## Relationships — 2
| Tool | | Description |
|------|--|-------------|
| `create_relationship` | 🟡 | Create a relationship (cardinality + cross-filter) |
| `delete_relationship` | 🔴 | Delete a relationship |

## PBIP safe editing (offline) — 6
| Tool | | Description |
|------|--|-------------|
| `pbip_load_project` | 🟢 | Load a `.pbip` project (reads its artifacts and `definition.pbir` to find the right model and report) |
| `pbip_get_project_info` | 🟢 | Project structure |
| `pbip_rename_tables` | 🔴 | Rename tables across the model and the report, all or nothing |
| `pbip_rename_columns` | 🔴 | Rename columns across the model and the report, all or nothing |
| `pbip_rename_measures` | 🔴 | Rename measures across the model and the report, all or nothing |
| `pbip_undo` | 🔴 | Restore every file changed by the last rename or repair (one level, this session) |

> A rename is planned in memory over the whole project and written in one transaction. It covers
> TMDL declarations and structural properties (relationships, sort-by, hierarchy levels, `ref`
> lines, `PBI_QueryOrder`, cultures, diagram), DAX in measures, calculated columns and tables,
> RLS filters and calculation items (through a tokenizer, so strings, comments and other names
> are never touched), Power Query references to other queries, and the report: visuals, mobile
> layouts, page and report filters, bookmarks, legacy `report.json`, and the field names inside
> Deneb specs. Nothing is written when the old name does not exist, the new name collides, or
> the result would leave a broken reference. Text that mentions the old name but is not a
> reference (titles, DAX string literals) is left alone and listed as a warning.

## PBIP diagnostics & repair — 4
| Tool | | Description |
|------|--|-------------|
| `pbip_fix_broken_visuals` | 🔴 | Repair visual refs after an out-of-band rename |
| `pbip_fix_dax_quoting` | 🔴 | Quote unquoted table names in DAX (`dry_run` shows the changes first) |
| `pbip_scan_broken_refs` | 🟢 | Compare model vs report references |
| `pbip_validate` | 🟢 | Validate TMDL: quoting, unresolved references, duplicate names, malformed lines |

## Report authoring (PBIR, preview) — 4
| Tool | | Description |
|------|--|-------------|
| `pbir_add_page` | 🔴 | Add a report page to a PBIR-Enhanced project; register it in `pages.json` |
| `pbir_add_visual` | 🔴 | Add a visual (chart/card/table/slicer) and bind fields by role; validates fields exist |
| `pbir_bind_fields` | 🔴 | Add/replace field projections on an existing visual (`add` or `replace`) |
| `pbir_validate_report` | 🟢 | Report-wide check that every binding (visuals, filters, bookmarks) points at a real model field |

> Each projection carries `nativeQueryRef`. A plain column on a chart's value well is wrapped in
> an `Aggregation` with the matching `Sum(...)` / `CountNonNull(...)` `queryRef`; slicers and
> tables never aggregate; explicit measures stay bare. `$schema` is inherited from a sibling
> file in the project when present. For anything beyond one visual at a time, use the spec below.

## Report as code — 7
| Tool | | Description |
|------|--|-------------|
| `pbir_apply_spec` | 🔴 | Build or update the whole report from ONE declarative JSON spec: theme, pages, visuals, text, shapes, images, navigation buttons, synced slicers, HTML Content and Deneb visuals, private custom visuals, hidden pop-ups with open/close bookmarks, elements shared across pages. Idempotent, all-or-nothing, never touches what it did not create. See [REPORT_SPEC.md](REPORT_SPEC.md) |
| `audit_report_design` | 🟢 | Best Practice Analyzer for the report layer: structure (broken JSON, unregistered custom visuals, visuals off the page, mixed schemas), design (overlaps, crowding, missing alt text and titles, broken navigation, unsynced slicers) and governance notes (uncertified and script-capable visuals) |
| `generate_payload_measure` | 🟢 | DAX as transport: a text measure that packs rows at a chosen grain for a JavaScript consumer, plus the matching parser and a character-budget estimate |
| `build_js_app_measure` | 🟡 | A JavaScript app inside a measure: assemble CSS and ordered JS modules into three measures for the HTML Content visual (string/regex-aware minifier, safety checks, Node syntax check, size report); `target='pbip'` writes them idempotently |
| `generate_html_measure` | 🟢 | A DAX measure returning styled HTML (KPI card, KPI strip, headline sentence, bar list, table) for the HTML Content visual, with values escaped in DAX |
| `pbip_embed_data` | 🔴 | Make the model self-contained: embed CSV data in the partitions (deflate + base64 in M) so the service can refresh it with no gateway. Dry run by default |
| `pbip_stage_publish` | 🟡 | Stage the report (and optionally the model) for an in-place publish: copy it bound `byConnection` to a published model and return the import command. Offline; the source project is untouched |

> These tools bring a script-driven build pipeline into the server: describe the report, generate
> the measures that draw it, write everything in one pass, audit the result, reload the open
> Desktop and look. The JSON shapes were taken from report files that open in Power BI Desktop
> and the tests check the emitted structure against them; the generated DAX and M are not
> evaluated by the test suite, so validate measures with `validate_dax` and look at the result.
> HTML Content, Deneb and private custom visuals are not certified visuals: a tenant that only
> allows certified visuals will not render them.

## Model quality & performance — 4
| Tool | | Description |
|------|--|-------------|
| `run_bpa` | 🟢 | Best Practice Analyzer: 33 rules, 5 of them error severity (broken references, USERELATIONSHIP without a relationship, measures without an expression, invalid name characters, trailing spaces) |
| `audit_ai_readiness` | 🟢 | Copilot/agent-readiness score + recommendations |
| `analyze_model_storage` | 🟢 | VertiPaq-style table ranking (rows + sizes) |
| `analyze_query_performance` | 🟢 | Time a DAX query + optimization hints (linter findings included) |

## DAX quality — 2
| Tool | | Description |
|------|--|-------------|
| `dax_lint` | 🟢 | Static DAX anti-pattern + correctness linter (raw expression, one measure, or whole model); severity + line + hint per finding |
| `dax_suggest_rewrite` | 🟢 | `/` is rewritten mechanically to `DIVIDE` and the whole rewritten expression is returned; FILTER-over-a-table and SUMMARIZE-with-aggregation come back as templates |

> Rules: DL001 FILTER over a whole table as a CALCULATE filter, DL002 nested CALCULATE, DL003 `/`
> instead of DIVIDE, DL004 IFERROR, DL005 `+ 0` blank suppression, DL006 EARLIER, DL007 SUMMARIZE
> with an extension column, DL008 unrecognized function (checked against a catalog of 472
> functions; names from `DEFINE FUNCTION` are known). The linter and the BPA share one tokenizer,
> so a name containing `--`, `//` or a quote cannot confuse them.

## Authoring helpers — 2
| Tool | | Description |
|------|--|-------------|
| `generate_svg_measure` | 🟢 | A DAX measure returning an inline SVG micro-visual (progress / bullet / status_pill / sparkline); blank when there is no data; set the data category to "Image URL" |
| `audit_naming` | 🟢 | Audit table/column/measure names and return a rename plan (snake_case, camelCase and UPPER_SNAKE to Title Case, strip DIM_/FACT_ prefixes, trim; configurable acronyms). Suggestions that would collide are flagged `safe: false`. Apply with the `pbip_rename_*` tools |

## PBIX onboarding — 2
| Tool | | Description |
|------|--|-------------|
| `pbix_inspect` | 🟢 | Inspect a `.pbix` (ZIP/OPC) package: thick vs thin, report format, page count, entry list. No extraction |
| `pbix_extract` | 🟡 | Extract a `.pbix` to a folder (Zip-Slip protected, streamed, size-capped) and decode the legacy UTF-16-LE `Report/Layout` to readable `Report/Layout.json` |

## Custom BPA governance — 2
| Tool | | Description |
|------|--|-------------|
| `bpa_validate_rules` | 🟢 | Validate a custom BPA rules JSON (required fields, valid Severity/Scope, duplicate IDs, destructive low-severity fixes, stray runtime fields); optional `fix` returns a cleaned copy |
| `bpa_audit_rule_sources` | 🟢 | Audit where BPA rules live for the loaded project: embedded model rules, external rule-file URLs and paths, every ignored rule ID, plus any local user/machine BPARules.json |

## Bulk DAX creation — 3
| Tool | | Description |
|------|--|-------------|
| `generate_measure_suite` | 🟡 | Expand a base measure/column into a governed suite (time intelligence with optional fiscal year end / share-of-total / ranks incl. an ALLSELECTED variant / column stats), each with format, folder, description; `target` = none / pbip (offline TMDL) / live (validated TOM batch) |
| `batch_create_measures` | 🟡 | All-or-nothing live bulk measure creation: the whole batch, sibling references included, is compiled in one query before anything is created; honors transactions |
| `pbip_add_measures` | 🔴 | Bulk-write measures OFFLINE into a table's `.tmdl`; `mode: upsert` replaces same-named measures in place so a rebuild is byte-identical |

## Data modelling (offline TMDL) — 3
| Tool | | Description |
|------|--|-------------|
| `pbip_create_date_table` | 🔴 | Generate a complete calculated date-dimension table, marked as date table (sorted month/quarter labels, optional fiscal year) |
| `pbip_add_calculation_group` | 🔴 | Create a calculation group (custom items or the time-intelligence preset); sets `discourageImplicitMeasures`, warns on low compatibilityLevel |
| `pbip_add_hierarchy` | 🔴 | Add a drill-down hierarchy from existing columns (validated) |

> TMDL emission follows Power BI Desktop's own serialization, verified against the TMDL language
> reference and real PBIP exports: tab indentation, `///` doc-comment descriptions, multi-line
> expression placement, `isNameInferred` + bracketed `sourceColumn` on calculated tables,
> `dataCategory: Time` + `isKey` date marking, and calc-group Name/Ordinal companion columns.

## Warehouse audit — 2
| Tool | | Description |
|------|--|-------------|
| `audit_star_schema` | 🟢 | Classify tables (fact/dimension/date/bridge/measure table/disconnected) and flag snowflakes, bidirectional filters, M2M, 1:1, fact-to-fact joins, a missing/unmarked/unrelated date table, text-on-fact, and a model with no relationships; scored with recommendations |
| `scan_referential_integrity` | 🟢 | Orphan-key scan per active relationship (EXCEPT counts + sample keys; the samples go through the same column policies and masking as query results) |

## Power BI Desktop Bridge (preview) — 4
| Tool | | Description |
|------|--|-------------|
| `bridge_status` | 🟢 | Discover running Desktop Bridge instances: pid, open file, unsaved-changes flag, report pages (PBIR folder, embedded-PBIR `.pbix`, or legacy Layout), and the correlated Analysis Services port for `desktop_connect` |
| `bridge_manifest` | 🟢 | Method manifest of the running Desktop's bridge (the surface grows over time) |
| `bridge_reload` | 🔴 | Hot-reload the open PBIP/PBIR from disk (no close/reopen); refuses over unsaved Desktop changes unless `force` |
| `bridge_screenshot` | 🟡 | Capture PNG screenshots of report pages from the running Desktop (page id, display name, or `all`); saved to disk for visual verification |

> Talks JSON-RPC 2.0 to Microsoft's Power BI Desktop Bridge (preview, June 2026+ Desktop) over
> the local named pipe `pbi-desktop-bridge-{pid}`. Pure Python, no dependencies. Requires the
> Desktop preview option "Enable external tool access to Power BI Desktop through secure local
> APIs" (on by default). The edit-and-verify loop: author offline with `pbir_*`/`pbip_*` tools,
> `bridge_reload`, then `bridge_screenshot` to see the result. One operation at a time per
> Desktop window. Known Desktop-side preview defect: on some builds
> `report.snapshot.capture` returns an internal error (-32000) for any input, so
> `bridge_screenshot` may fail until a Desktop update ships; status, manifest, and reload are
> unaffected.

## Documentation, diff & CI — 5
| Tool | | Description |
|------|--|-------------|
| `export_data_dictionary` | 🟡 | Data dictionary (Markdown/HTML) + coverage score |
| `model_snapshot` | 🟡 | Capture model metadata to a JSON baseline |
| `model_diff` | 🟢 | Semantic diff vs a snapshot or the live model: every gathered property of tables, columns, measures and relationships, with rename detection |
| `pre_deploy_gate` | 🟢 | PASS/FAIL quality gate (BPA error rules + AI-readiness) for CI |
| `run_dax_tests` | 🟢 | DAX regression test runner (expected vs actual, absolute and relative tolerance); a test that cannot run fails the suite |

## Diagnostics & ops — 4
| Tool | | Description |
|------|--|-------------|
| `refresh_doctor` | 🟢 | Classify dataset refresh failures + remediation (Pro-OK) |
| `find_unused_objects` | 🟢 | Columns/measures unused by the model (calculations, relationships, sort-by) or the loaded report |
| `impact_analysis` | 🟢 | Blast radius: model dependents + report visuals |
| `rls_test_harness` | 🟢 | Evaluate under every RLS role → pass/fail matrix; restores the role that was active before |

## Governance-ops fleet (admin/Premium-gated) — 3
| Tool | | Description |
|------|--|-------------|
| `cross_workspace_lineage` | 🟡 | Scanner-API tenant inventory + lineage + RLS/label coverage. Scans in batches of 100 workspaces, says when it stopped at `max_workspaces`, and returns scan ids to resume when the wait runs out. Can cache the result to a file (not in read-only mode) |
| `fleet_refresh_monitor` | 🟢 | Refresh health + classified failures across workspaces; datasets it could not read are listed as unknown, never counted as healthy |
| `usage_and_orphan_analytics` | 🟢 | Per-day usage analytics from the Activity Events API |

---

## MCP Resources
- `powerbi://desktop/schema` · `.../measures` · `.../bpa` · `.../ai-readiness`
- `powerbi://cloud/{workspace}/{dataset}/schema` (template)
- `powerbi://reference/bpa-rules` · `powerbi://reference/refresh-errors`

## MCP Prompts
`optimize_measure` · `explain_measure` · `audit_model` · `document_model` ·
`plan_safe_rename` · `pre_deploy_review`

## Environment variables
| Variable | Purpose |
|----------|---------|
| `TENANT_ID`, `CLIENT_ID`, `CLIENT_SECRET` | Azure AD service principal for cloud/REST/admin |
| `POWERBI_MCP_READONLY` | Refuse every write tool (lockdown). Accepts 1/true/yes/on; a value that cannot be read turns lockdown ON |
| `POWERBI_MCP_QUERY_TIMEOUT` | Seconds before a DAX query is cancelled (default 120) |
| `POWERBI_MCP_POLICY_FILE` | Use this policy file instead of `config/policies.yaml`. Missing or invalid means queries are refused |
| `ENABLE_PII_DETECTION`, `ENABLE_AUDIT`, `ENABLE_POLICIES` | Toggle security subsystems (default on; an unreadable value leaves them on) |
| `POWERBI_MCP_AUDIT_DIR` | Folder for the audit log (default `logs/`) |
| `POWERBI_MCP_AUDIT_KEY` | Secret key that switches the audit hash chain to HMAC-SHA256 |
| `POWERBI_MCP_HASH_KEY` | Secret key for the `hash` column policy; without it a per-session key is used |
| `POWERBI_MCP_PBIP_BACKUPS` | `false` stops the automatic project backup before a rename or repair (default on) |
| `ADOMD_DLL_PATH` | Folder (or full path) of `Microsoft.AnalysisServices.AdomdClient.dll` if auto-discovery misses it |
| `TOM_DLL_PATH` | Folder (or full path) of `Microsoft.AnalysisServices.Tabular.dll` for live writes (`ADOMD_DLL_PATH` is also searched) |
| `PYTHONNET_RUNTIME` | `coreclr` when only the .NET (Core) builds of ADOMD/TOM are installed |
| `POWERBI_MCP_SKIP_DOTENV` | `1` stops the server reading a `.env` file. Set by the test runner so local settings cannot change a test result |
| `LOG_LEVEL` | `DEBUG` enables (redacted) argument logging |

## Verification status
- **Pure logic and file tools** (DAX tokenizer, PBIP/TMDL/PBIR renames and authoring, the report
  spec, measure generators, data embedding, BPA, linters, auditors, security, refresh
  classification, governance parsing): covered by the 50 assert suites. The rename engine, the
  report spec and the report audit were additionally run against copies of real, complex
  reports (HTML Content, Deneb, custom visuals, bookmarks, synced slicers).
- **Server and live connectors** (request boundary, transactions, query guard, REST paging and
  throttling, loader ranking): exercised through fakes in the suites.
- **Desktop live paths** verified against a running Power BI Desktop in 2026-07 (ADOMD queries,
  TOM batch writes, star-schema audit, Desktop Bridge status/manifest/hot-reload). The connector
  and rename code has changed since; it has not been re-run against a live Desktop.
- **Not verified by execution:** generated DAX and M (measure suites, payload, HTML and SVG
  measures, embedded partitions) are not evaluated by an engine in the test suite, and generated
  reports are not opened in Desktop by it. Validate measures with `validate_dax` and check the
  rendering with `bridge_reload` + `bridge_screenshot`.
- **Cloud paths** (XMLA / REST / Admin Scanner and Activity): doc-verified against Microsoft
  Learn and tested with fakes; end-to-end verification still needs a real tenant.
