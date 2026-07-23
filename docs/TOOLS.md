# Tool Reference

The Power BI MCP server exposes **82 tools**, plus MCP **resources**, **prompts**, and
**completion**. Every tool carries MCP annotations (`readOnlyHint` / `destructiveHint` /
`idempotentHint` / `openWorldHint`) so clients can auto-approve reads and confirm writes.

Legend: 🟢 read-only · 🟡 write (non-destructive) · 🔴 destructive

## Desktop (local Analysis Services, ADOMD) — 7
| Tool | | Description |
|------|--|-------------|
| `desktop_discover_instances` | 🟢 | Discover running Power BI Desktop instances |
| `desktop_connect` | 🟡 | Connect to an instance (optional RLS role) |
| `desktop_list_tables` | 🟢 | List tables |
| `desktop_list_columns` | 🟢 | List columns for a table |
| `desktop_list_measures` | 🟢 | List measures |
| `desktop_execute_dax` | 🟢 | Execute a DAX query (security-processed) |
| `desktop_get_model_info` | 🟢 | Tables, measures, relationships |

## Cloud (Power BI Service, XMLA + REST) — 6
| Tool | | Description |
|------|--|-------------|
| `list_workspaces` | 🟢 | Workspaces accessible to the service principal |
| `list_datasets` | 🟢 | Datasets in a workspace |
| `list_tables` | 🟢 | Tables in a dataset (XMLA) |
| `list_columns` | 🟢 | Columns for a table |
| `execute_dax` | 🟢 | Execute DAX against a cloud dataset (row-capped) |
| `get_model_info` | 🟢 | Model info via INFO.VIEW functions |

## Security & audit — 3
| Tool | | Description |
|------|--|-------------|
| `security_status` | 🟢 | Current PII/audit/policy configuration |
| `security_audit_log` | 🟢 | Recent audit log entries |
| `verify_audit_integrity` | 🟢 | Verify the tamper-evident audit hash chain |

## Row-Level Security — 3
| Tool | | Description |
|------|--|-------------|
| `desktop_list_rls_roles` | 🟢 | List RLS roles |
| `desktop_set_rls_role` | 🟡 | Activate/clear an RLS role for testing |
| `desktop_rls_status` | 🟢 | Current RLS status |

## Model writes via TOM — 7
| Tool | | Description |
|------|--|-------------|
| `create_measure` | 🟡 | Create a measure (validates DAX first; honors transactions) |
| `delete_measure` | 🔴 | Delete a measure |
| `batch_update_measures` | 🔴 | Bulk-update measure expressions (validates first) |
| `scan_table_dependencies` | 🟢 | Analyze a table's dependents before a rename |
| `batch_rename_tables` | 🔴 | ⚠️ DEPRECATED — use `pbip_rename_tables` (breaks visuals) |
| `batch_rename_columns` | 🔴 | ⚠️ DEPRECATED — use `pbip_rename_columns` |
| `batch_rename_measures` | 🔴 | ⚠️ DEPRECATED — use `pbip_rename_measures` |

## DAX safety loop & transactions — 5
| Tool | | Description |
|------|--|-------------|
| `validate_dax` | 🟢 | Validate a query/measure against the model without committing |
| `scan_measure_dependencies` | 🟢 | Upstream/downstream impact via INFO.CALCDEPENDENCY |
| `tom_begin_transaction` | 🟡 | Start an atomic write transaction |
| `tom_commit_transaction` | 🟡 | Commit pending edits |
| `tom_rollback_transaction` | 🟡 | Roll back pending edits |

## Relationships — 2
| Tool | | Description |
|------|--|-------------|
| `create_relationship` | 🟡 | Create a relationship (cardinality + cross-filter) |
| `delete_relationship` | 🔴 | Delete a relationship |

## PBIP safe editing (offline) — 5
| Tool | | Description |
|------|--|-------------|
| `pbip_load_project` | 🟢 | Load a `.pbip` project (detects PBIR format) |
| `pbip_get_project_info` | 🟢 | Project structure |
| `pbip_rename_tables` | 🔴 | Rename tables (model + report layer; transactional) |
| `pbip_rename_columns` | 🔴 | Rename columns (model + report layer) |
| `pbip_rename_measures` | 🔴 | Rename measures (model + report layer) |

## PBIP diagnostics & repair — 4
| Tool | | Description |
|------|--|-------------|
| `pbip_fix_broken_visuals` | 🔴 | Repair visual refs after an out-of-band rename |
| `pbip_fix_dax_quoting` | 🔴 | Quote unquoted table names in DAX |
| `pbip_scan_broken_refs` | 🟢 | Compare model vs report references |
| `pbip_validate` | 🟢 | Validate TMDL syntax / quoting |

## Report authoring (PBIR, preview) — 4
| Tool | | Description |
|------|--|-------------|
| `pbir_add_page` | 🔴 | Add a report page to a PBIR-Enhanced project; register it in `pages.json` |
| `pbir_add_visual` | 🔴 | Add a visual (chart/card/table/slicer) and bind fields by role; validates fields exist |
| `pbir_bind_fields` | 🔴 | Add/replace field projections on an existing visual (`add` or `replace`) |
| `pbir_validate_report` | 🟢 | Report-wide check that every visual binding points at a real model field |

> Emits Power-BI-faithful PBIR (each projection carries `nativeQueryRef`; a plain column on a
> value well is wrapped in an `Aggregation` with the matching `Sum(...)`/`CountNonNull(...)`
> `queryRef`, while explicit measures stay bare). `$schema` is inherited from a sibling file in
> the project when present, else falls back to the current published version. Requires a loaded
> PBIR-Enhanced PBIP project; writes are atomic and field existence is checked first.

## Model quality & performance — 4
| Tool | | Description |
|------|--|-------------|
| `run_bpa` | 🟢 | Best Practice Analyzer (severity + fix hints) |
| `audit_ai_readiness` | 🟢 | Copilot/agent-readiness score + recommendations |
| `analyze_model_storage` | 🟢 | VertiPaq-style table ranking (rows + sizes) |
| `analyze_query_performance` | 🟢 | Time a DAX query + optimization hints |

## DAX quality — 2
| Tool | | Description |
|------|--|-------------|
| `dax_lint` | 🟢 | Static DAX anti-pattern + correctness linter (raw expression, one measure, or whole model); severity + line + rewrite hint per finding |
| `dax_suggest_rewrite` | 🟢 | Concrete before/after rewrite hints for the auto-fixable anti-patterns |

> Rules: DL001 FILTER-whole-table in CALCULATE, DL002 nested CALCULATE, DL003 `/` instead of
> DIVIDE, DL004 IFERROR, DL005 `+ 0` blank suppression, DL006 EARLIER, DL007 SUMMARIZE used for
> aggregation, DL008 unrecognized/hallucinated function. Pure-Python tokenizer, no external tool.
> Pair `dax_suggest_rewrite` with `validate_dax` to confirm a rewrite still parses.

## Authoring helpers — 2
| Tool | | Description |
|------|--|-------------|
| `generate_svg_measure` | 🟢 | Generate a DAX measure returning an inline SVG micro-visual (progress / bullet / status_pill / sparkline); set the measure's data category to "Image URL" to render it |
| `audit_naming` | 🟢 | Audit table/column/measure names and return a rename plan (snake_case & camelCase to Title Case, strip DIM_/FACT_ prefixes, trim); apply with the rename tools |

## PBIX onboarding — 2
| Tool | | Description |
|------|--|-------------|
| `pbix_inspect` | 🟢 | Inspect a `.pbix` (ZIP/OPC) package: thick vs thin, report format, page count, entry list. No extraction |
| `pbix_extract` | 🟡 | Extract a `.pbix` to a folder (Zip-Slip protected) and decode the legacy UTF-16-LE `Report/Layout` to readable `Report/Layout.json` |

## Custom BPA governance — 2
| Tool | | Description |
|------|--|-------------|
| `bpa_validate_rules` | 🟢 | Validate a custom BPA rules JSON (required fields, valid Severity/Scope, duplicate IDs, destructive low-severity fixes, stray runtime fields); optional `fix` returns a cleaned copy |
| `bpa_audit_rule_sources` | 🟢 | Audit where BPA rules live for the loaded project: embedded model rules, external rule-file URLs, ignored rule IDs, plus any local user/machine BPARules.json |

## Bulk DAX creation — 3
| Tool | | Description |
|------|--|-------------|
| `generate_measure_suite` | 🟡 | Expand a base measure/column into a governed suite (time intelligence / ratios / ranking / column stats), each with format, folder, description; `target` = none / pbip (offline TMDL) / live (validated TOM batch) |
| `batch_create_measures` | 🟡 | All-or-nothing live bulk measure creation (each expression validated first; honors transactions) |
| `pbip_add_measures` | 🔴 | Bulk-append measures OFFLINE into a table's `.tmdl` (collision-checked batch; descriptions as `///` doc-comments) |

## Data modelling (offline TMDL) — 3
| Tool | | Description |
|------|--|-------------|
| `pbip_create_date_table` | 🔴 | Generate a complete calculated date-dimension table, marked as date table (sorted month/quarter labels, optional fiscal year) |
| `pbip_add_calculation_group` | 🔴 | Create a calculation group (custom items or the time-intelligence preset); sets `discourageImplicitMeasures`, warns on low compatibilityLevel |
| `pbip_add_hierarchy` | 🔴 | Add a drill-down hierarchy from existing columns (validated) |

> All TMDL emission matches Power BI Desktop's own serialization, verified against the TMDL
> language reference and real PBIP exports: tab indentation, `///` doc-comment descriptions,
> multi-line expression placement, `isNameInferred` + bracketed `sourceColumn` on calculated
> tables, `dataCategory: Time` + `isKey` date marking, and calc-group Name/Ordinal companion
> columns.

## Warehouse audit — 2
| Tool | | Description |
|------|--|-------------|
| `audit_star_schema` | 🟢 | Classify tables (fact/dimension/date/bridge/disconnected) and flag snowflakes, bidirectional filters, M2M, fact-to-fact joins, missing/unmarked date table, text-on-fact; scored with recommendations |
| `scan_referential_integrity` | 🟢 | Orphan-key scan per active relationship (EXCEPT counts + sample keys) against the connected model |

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
> `bridge_reload`, then `bridge_screenshot` to see the result. Snapshot/reload target PBIP/PBIR
> projects opened from disk; one operation at a time per Desktop window.

## Documentation, diff & CI — 5
| Tool | | Description |
|------|--|-------------|
| `export_data_dictionary` | 🟡 | Data dictionary (Markdown/HTML) + coverage score |
| `model_snapshot` | 🟡 | Capture model metadata to a JSON baseline |
| `model_diff` | 🟢 | Semantic diff vs a snapshot or the live model |
| `pre_deploy_gate` | 🟢 | PASS/FAIL quality gate (BPA + AI-readiness) for CI |
| `run_dax_tests` | 🟢 | DAX regression test runner (expected vs actual) |

## Diagnostics & ops — 4
| Tool | | Description |
|------|--|-------------|
| `refresh_doctor` | 🟢 | Classify dataset refresh failures + remediation (Pro-OK) |
| `find_unused_objects` | 🟢 | Columns/measures unused by model or report |
| `impact_analysis` | 🟢 | Blast radius: model dependents + report visuals |
| `rls_test_harness` | 🟢 | Evaluate under every RLS role → pass/fail matrix |

## Governance-ops fleet (admin/Premium-gated) — 3
| Tool | | Description |
|------|--|-------------|
| `cross_workspace_lineage` | 🟢 | Scanner-API tenant inventory + lineage + RLS/label coverage |
| `fleet_refresh_monitor` | 🟢 | Refresh health + classified failures across workspaces |
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
| `ADOMD_DLL_PATH` | Folder (or full path) of `Microsoft.AnalysisServices.AdomdClient.dll` if auto-discovery misses it |
| `POWERBI_MCP_READONLY` | `true` → refuse all write tools (lockdown mode) |
| `ENABLE_PII_DETECTION`, `ENABLE_AUDIT`, `ENABLE_POLICIES` | Toggle security subsystems (default true) |
| `LOG_LEVEL` | `DEBUG` enables (redacted) argument logging |

## Verification status
Tools that run purely on local files or pure logic (PBIP editing, BPA, AI-readiness,
diff, security, refresh classification, governance parsing) are covered by the test
suites. Tools that call a live Power BI Desktop / XMLA / REST / Admin endpoint are
doc-verified against Microsoft Learn and mock-tested, but need a real Windows + Power BI
/ Fabric environment for end-to-end verification.
