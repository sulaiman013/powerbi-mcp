# Power BI MCP Server

<p align="center">
  <strong>An enterprise-grade Model Context Protocol server for Power BI and Microsoft Fabric.</strong>
</p>

<p align="center">
  <a href="https://modelcontextprotocol.io"><img src="https://img.shields.io/badge/MCP-compatible-blue?style=flat-square" alt="MCP compatible"></a>
  <a href="https://www.python.org"><img src="https://img.shields.io/badge/Python-3.10%2B-green?style=flat-square" alt="Python 3.10+"></a>
  <a href="#"><img src="https://img.shields.io/badge/Tools-90-purple?style=flat-square" alt="90 tools"></a>
  <a href="#"><img src="https://img.shields.io/badge/Live-Windows-lightgrey?style=flat-square" alt="Windows for live connectivity"></a>
  <a href="#"><img src="https://img.shields.io/badge/Offline-cross--platform-success?style=flat-square" alt="Offline cross-platform"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-MIT-yellow?style=flat-square" alt="MIT license"></a>
</p>

<p align="center">
  <em>Let AI assistants inspect, query, validate, optimize, govern, and safely refactor Power BI
  semantic models and reports, through natural language.</em>
</p>

> **Disclaimer:** This is an independent, community project. It is not affiliated with, endorsed
> by, or connected to Microsoft Corporation or Anthropic.

---

## What it is

Power BI MCP Server connects an AI assistant (Claude, GitHub Copilot, any MCP client) to your
Power BI content through one consistent interface. It talks to a local Power BI Desktop model
(queries AND live writes), a published Power BI Service dataset, Power BI Project (PBIP) files
on disk, and the running Power BI Desktop app itself (through Microsoft's Desktop Bridge, for
hot-reload and screenshots). Every tool call passes one request boundary: a read-only gate, a
query-only guard, access policies and PII masking on query results, and secret redaction.

It exposes **90 tools** plus MCP **resources**, **prompts**, and **completion**, and ships with
50 assert-based test suites.

| Capability | What you get |
|------------|--------------|
| **Dual connectivity** | Power BI Desktop (local) and Power BI Service (cloud) |
| **Natural-language DAX** | Run, validate, and optimize DAX through conversation |
| **Safe refactoring** | PBIP-based renames that update the model **and** the report visuals |
| **Bulk DAX creation** | Measure-suite generator (time intelligence, ratios, ranks) written offline or live |
| **Data modelling** | Date-dimension generator, calculation groups, hierarchies, offline TMDL authoring |
| **Warehouse audit** | Star-schema classification + findings, referential-integrity orphan scan |
| **Report as code** | Build a whole designed report from one declarative spec: theme, pages, visuals, navigation, pop-ups, synced slicers |
| **Custom reporting techniques** | HTML and SVG measures, DAX-as-transport payload measures, a JavaScript app inside a measure, Deneb and custom visuals |
| **Self-contained publishing** | Embed data in the model, stage a report for an in-place publish that keeps its links |
| **Desktop Bridge (preview)** | Hot-reload the open report and screenshot pages in the RUNNING Desktop |
| **DAX safety loop** | Validate before committing; impact analysis; atomic transactions |
| **Model quality** | Best Practice Analyzer, AI-readiness scoring, VertiPaq-style storage analysis |
| **Diagnostics and ops** | Refresh-failure triage, unused-object detection, RLS test matrix |
| **Governance** | Column and table policies that fail closed, PII masking, a hash-chained audit log, read-only mode |
| **Fleet (admin)** | Cross-workspace lineage, fleet refresh monitor, usage analytics |
| **Modern MCP** | Tool annotations, structured output, resources, prompts, completion |

---

## What you can do with it (in plain words)

This server is a bridge. On its own an AI assistant can only talk. This gives it a set of
"hands" so it can reach into Power BI and actually do the work for you. You ask in plain
English, the assistant picks the right tool, and you get an answer or a change.

It plugs into four places:

1. **Power BI Desktop**: the app open on your PC. It talks to the live model inside it.
2. **Power BI Service**: the cloud, your published datasets and workspaces.
3. **PBIP files**: when you save a "Power BI Project", the model and report become text files
   on disk that it can edit directly, even with Desktop closed.
4. **The running Desktop app itself** (June 2026+): through Microsoft's Desktop Bridge it can
   hot-reload the open report after file edits and take page screenshots, no restart needed.

A few terms used below: a **semantic model** (dataset) is the data brain behind a report
(its tables, columns, relationships, and measures). A **measure** is a saved calculation
written in **DAX** (Power BI's formula language). **RLS** is row-level security (rules that
limit which rows a user can see).

### Things you can ask it to do

- **Understand a model you have never seen.** List tables, columns, measures (with formulas),
  and relationships, or get the whole picture at once.
  > "Connect to my Power BI Desktop and summarize the model."
- **Query your data in plain English.** It turns the question into DAX and runs it.
  > "What were the top 10 products by sales last quarter?"
- **Write, fix, and optimize measures safely.** It validates the DAX against your model
  *before* saving, so broken formulas are caught early. It can also explain or speed up a measure.
  > "Create a 'Margin %' measure as profit divided by sales, formatted as a percentage."
- **Rename tables, columns, or measures without breaking the report.** Normal tools fix only
  the model and leave visuals broken. This updates the model and the report visuals together,
  as one transaction that rolls back if anything fails.
  > "Rename the table 'Salesforce_Data' to 'Sales Force Data' everywhere."
- **See what will break before you change anything.** The full blast radius: every measure
  that depends on an object and every report visual that uses it.
  > "If I delete the 'Old Revenue' measure, what depends on it?"
- **Manage relationships** between tables (cardinality and filter direction).
- **Build a whole report from a spec.** Describe the report once (theme, pages, charts, text,
  navigation buttons, synced slicers, pop-ups that open and close with bookmarks, a masthead
  repeated on every page) and it writes the PBIR files. Applying the same spec again changes
  nothing; changing one element rewrites one file; anything wrong (a field that does not exist,
  a button pointing at a missing page) stops the whole apply before a single file is written.
  > "Build a three-page report from this spec, with a filters pop-up on every page."
- **Go beyond native visuals.** Generate measures that return styled HTML (KPI cards, headline
  sentences with live numbers, ranked bar lists) or inline SVG, pack fact rows into a text
  measure for a JavaScript app that filters in the browser with no query per click, and assemble
  that app from CSS and JavaScript modules into measures, minified and size-checked.
  > "Pack the incidents fact at row grain and build the app measures from this folder of modules."
- **Audit the report like a model.** Structure (broken JSON, unregistered custom visuals, visuals
  off the page), design (overlaps, crowded pages, missing alt text and titles, navigation that
  points nowhere) and governance notes (uncertified and script-capable visuals).
  > "Audit the report design and list what would not survive a tenant that blocks uncertified visuals."
- **Publish without a gateway.** Embed small static data in the model so the service can refresh
  it with no local files, and stage the report bound to the published model so importing it
  replaces the old version in place and every existing link keeps working.
  > "Make the model self-contained and stage the report for publishing over 'Sales Overview'."
- **Close the loop with the running Desktop (preview).** Through Microsoft's Power BI Desktop
  Bridge (June 2026+), the agent can see which file is open and whether it has unsaved changes,
  hot-reload the report from disk after offline edits (no close/reopen), and capture PNG
  screenshots of report pages so it can visually verify its own work. Edit, reload, look, fix.
  > "Add the visual, reload Desktop, and screenshot the page so you can check the layout."
- **Check model quality like a senior reviewer.** Best Practice Analyzer (performance, DAX,
  naming, formatting), an AI-readiness score, storage/size analysis, and query-performance hints.
  > "Audit this model and give me the top issues to fix before I ship."
- **Bulk-create governed measure suites.** Expand one base measure into a full time-intelligence
  set (YTD, QTD, MTD, PY, YoY, YoY %, MoM %, rolling windows), share-of-total ratios, ranks, or
  column statistics. Every measure arrives with a format string, display folder, and description,
  and can be written straight into the PBIP files offline or created live in one validated batch.
  > "Generate the full time-intelligence suite for 'Total Sales' over Date[Date] and add it to the model."
- **Build the data-warehouse backbone.** Generate a complete, marked date-dimension table
  (with sorted month/quarter labels and optional fiscal columns), create calculation groups
  (the professional alternative to measure explosion), and add drill-down hierarchies, all
  offline into the PBIP project.
  > "Create a date table from 2018 to 2030 with a July fiscal year, and a time-intelligence calculation group."
- **Audit the model as a star schema.** Classifies every table (fact, dimension, date dimension,
  bridge, disconnected) from relationship topology and flags snowflake chains, bidirectional
  filters, many-to-many, fact-to-fact joins, missing or unmarked date tables, and text attributes
  stranded on facts, with a score and a fix per finding.
  > "Audit my model as a star schema and tell me what a warehouse architect would flag."
- **Catch orphan keys before they distort totals.** Scan every relationship for fact keys with
  no matching dimension row (the cause of the hidden blank row).
  > "Scan referential integrity and show me sample orphan keys per relationship."
- **Lint your DAX for performance traps.** A static analyzer flags the classic anti-patterns
  (FILTER over a whole table inside CALCULATE, nested CALCULATE, `/` instead of DIVIDE, IFERROR,
  EARLIER, SUMMARIZE used for aggregation, blank-suppressing `+ 0`, and unrecognized or
  hallucinated function names) and hands back a concrete rewrite for each.
  > "Lint every measure in my model and suggest rewrites for the worst offenders."
- **Standardize naming across the model.** Audit table, column, and measure names and get a
  rename plan (snake_case and camelCase to spaced Title Case, strip DIM_/FACT_ prefixes, trim
  spaces), then apply it with the safe rename tools that also fix the report visuals.
  > "Audit naming and rename everything to Title Case without breaking the report."
- **Add micro-visuals with one DAX measure.** Generate a sparkline, bullet chart, progress bar,
  or status pill as an inline SVG measure that renders right inside a table, matrix, or card.
  > "Make me a progress-bar measure for 'Margin %' against a 100% target."
- **Open a real `.pbix` file.** Inspect a `.pbix` (it is a ZIP package): see whether it has an
  imported model or a live connection, which report format it uses, and how many pages, then
  extract it and get the report layout decoded to readable JSON.
  > "Inspect this .pbix and tell me if it has an imported model and how many pages."
- **Author and govern your own quality rules.** Validate a custom Best Practice Analyzer rule
  set (catch bad scopes, duplicate IDs, risky auto-deletes) and audit where rules actually live:
  embedded in the model, ignored, or pulled from external files.
  > "Validate our BPARules.json and tell me which rules this model is silently ignoring."
- **Clean up dead weight.** Find columns and measures that nothing uses (not in any formula
  and not in any visual) so you can remove clutter safely.
- **Test security roles properly.** Run a measure under every RLS role and get a pass/fail
  matrix that flags roles seeing too much or nothing.
- **Document the model automatically.** Generate a data dictionary (Markdown or HTML) with a
  documentation-coverage score, re-runnable any time.
- **Compare versions and gate deployments.** Snapshot the model, diff it later for a readable
  "what changed" list, run a pre-deploy PASS/FAIL quality gate, and run DAX regression tests.
- **Troubleshoot refreshes (cloud).** When a refresh fails it classifies the cause (expired
  credentials, gateway down, throttling, out of memory, timeout, bad source query) and tells
  you the fix.
- **Govern access and stay compliant.** Mask PII before the AI sees it, block/mask/hash/redact
  specific columns, keep a hash-chained audit log, and flip on read-only mode so an agent can
  look but not touch.
- **See across the whole tenant (admins).** Inventory every workspace, find datasets with no
  security or no sensitivity label, trace which reports use a dataset, monitor refresh health,
  and view usage analytics.

### A realistic end-to-end example

> 1. "Connect to my Power BI Desktop model."
> 2. "Audit it as a star schema and list what a warehouse architect would flag."
> 3. "Create a date table with a July fiscal year and a time-intelligence calculation group."
> 4. "Generate the full time-intelligence suite for 'Total Sales' and add it to the model."
> 5. "What would break if I rename the 'Customer ID' column? Rename it across model and report."
> 6. "Hot-reload Desktop and screenshot the overview page so you can check it."
> 7. "Export a data dictionary and run the pre-deploy quality gate before I publish."

Each step is one sentence; the server does the real Power BI work behind it.

---

## Why this server

Microsoft now ships official Power BI MCP servers (public preview): a remote one for
chat-with-data and a local modeling one for authoring semantic models. This project is
**complementary**. It leans into what those servers do not cover:

- **Report-aware safe renames.** The official local modeling MCP edits the model only and
  cannot touch the report layer. This server renames tables, columns, and measures across
  both the model (TMDL, DAX, Power Query references) and the report (visuals, filters,
  bookmarks, Deneb specs, cultures, diagram, hierarchies, sort wiring), so visuals do not break.
- **Report as code.** A declarative spec builds designed reports idempotently: the method behind
  script-built showcase reports, available as one tool, with the validation a script lacks.
- **Offline authoring of models.** Bulk measures (idempotent upsert), date dimensions,
  calculation groups and hierarchies are written straight into PBIP files, no Power BI required.
- **The full edit-and-verify loop.** Offline authoring + the Desktop Bridge (hot-reload the
  running Desktop, no close/reopen) + one call that chains a bridge instance into live DAX/TOM.
- **Warehouse-grade auditing.** Star-schema classification with findings and a score, a
  referential-integrity orphan scan, a DAX anti-pattern linter, and custom BPA rule governance.
- **A governance and security layer.** Column and table policies (block, mask, hash, redact,
  numeric-mask) that follow aliases and measures and fail closed on a broken policy file, PII
  masking, a hash-chained (HMAC-keyed if you set a key) audit log, and a read-only lockdown mode.
- **Diagnostics and fleet ops.** Refresh-failure classification, unused-object cleanup, impact
  analysis, an RLS test matrix, and tenant-wide lineage and usage analytics.
- **Offline, PBIP-first workflows.** The whole PBIP, analysis, and security subset runs
  cross-platform with no Fabric capacity required.

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for how it fits together.

---

## Quick start

### Prerequisites

**Live connectivity (Power BI Desktop / Service):** Windows 10/11, Power BI Desktop, Python 3.10+,
and the ADOMD.NET client library (plus AMO/TOM for live writes). Newer Power BI Desktop builds no
longer ship these, so install SQL Server Management Studio (SSMS), or grab both NuGet packages
once and point the server at them:

```bash
# one-time: fetch ADOMD + AMO/TOM from NuGet and extract the DLLs
curl -L -o adomd.nupkg "https://www.nuget.org/api/v2/package/Microsoft.AnalysisServices.AdomdClient.retail.amd64"
curl -L -o amo.nupkg   "https://www.nuget.org/api/v2/package/Microsoft.AnalysisServices.retail.amd64"
# unzip both (they are ZIP files) and copy lib/net45/*.dll into one folder, e.g. C:\pbi-dlls
# then set:  ADOMD_DLL_PATH=C:\pbi-dlls
```

The server also searches Power BI Desktop, the GAC, SSMS, the SQL Server SDK, and the NuGet
cache automatically (`ADOMD_DLL_PATH` / `TOM_DLL_PATH` override). Cloud additionally needs an
Azure AD service principal and, for some operations, a Premium / PPU / Fabric capacity.

**Desktop Bridge tools (hot-reload, screenshots):** Power BI Desktop June 2026 or later with
the preview option "Enable external tool access to Power BI Desktop through secure local APIs"
(File > Options > Preview features; on by default).

**Offline subset only (PBIP/TMDL/PBIR authoring and editing, BPA, linters, analysis, security):**
any OS, Python 3.10+, no .NET.

### Install

```bash
git clone https://github.com/sulaiman013/powerbi-mcp.git
cd powerbi-mcp

# Full install (Windows, for live connectivity)
pip install -r requirements.txt

# Or: offline / cross-platform subset only
pip install -r requirements-core.txt

# (Optional) cloud credentials, Windows
copy .env.example .env
# edit .env with your Azure AD service principal
```

### Configure Claude Desktop

Add to `%APPDATA%\Claude\claude_desktop_config.json`, then restart Claude Desktop:

```json
{
  "mcpServers": {
    "powerbi": {
      "command": "python",
      "args": ["C:/path/to/powerbi-mcp/src/server.py"],
      "env": {
        "PYTHONPATH": "C:/path/to/powerbi-mcp/src"
      }
    }
  }
}
```

### Run with Docker (offline, cross-platform)

The image runs the platform-independent tools (PBIP editing, BPA, AI-readiness, model analysis,
security, resources, prompts) on any OS with no .NET. Live Desktop / XMLA / TOM connectivity
still needs Windows + ADOMD.NET.

```bash
docker build -t powerbi-mcp .
docker run --rm -i -v /path/to/MyReport:/work powerbi-mcp
```

---

## Tools

90 tools across the categories below. The full reference, with read / write / destructive
markers, is in **[docs/TOOLS.md](docs/TOOLS.md)**.

| Category | Count | Highlights |
|----------|:-----:|------------|
| Desktop (local, ADOMD) | 7 | discover, connect, list tables/columns/measures, `desktop_execute_dax`, model info |
| Cloud (XMLA + REST) | 6 | workspaces, datasets, tables, columns, `execute_dax`, model info |
| Security and audit | 3 | `security_status`, `security_audit_log`, `verify_audit_integrity` |
| Row-Level Security | 3 | list roles, set role, status |
| Model writes (TOM) | 7 | `create_measure`, `delete_measure`, `batch_update_measures`, deprecated batch renames |
| DAX safety and transactions | 5 | `validate_dax`, `scan_measure_dependencies`, begin/commit/rollback transaction |
| Relationships | 2 | `create_relationship`, `delete_relationship` |
| PBIP safe editing | 6 | load project, get info, rename tables/columns/measures (model + report), `pbip_undo` |
| PBIP diagnostics | 4 | fix broken visuals, fix DAX quoting, scan broken refs, validate |
| Report authoring (PBIR, preview) | 4 | `pbir_add_page`, `pbir_add_visual`, `pbir_bind_fields`, `pbir_validate_report` |
| Report as code | 7 | `pbir_apply_spec`, `audit_report_design`, `generate_payload_measure`, `build_js_app_measure`, `generate_html_measure`, `pbip_embed_data`, `pbip_stage_publish` |
| Model quality and performance | 4 | `run_bpa`, `audit_ai_readiness`, `analyze_model_storage`, `analyze_query_performance` |
| DAX quality | 2 | `dax_lint` (performance anti-patterns), `dax_suggest_rewrite` |
| Authoring helpers | 2 | `generate_svg_measure` (sparkline/bullet/progress/pill), `audit_naming` |
| PBIX onboarding | 2 | `pbix_inspect`, `pbix_extract` (crack open a real `.pbix`) |
| Custom BPA governance | 2 | `bpa_validate_rules`, `bpa_audit_rule_sources` |
| Bulk DAX creation | 3 | `generate_measure_suite`, `batch_create_measures`, `pbip_add_measures` |
| Data modelling (offline TMDL) | 3 | `pbip_create_date_table`, `pbip_add_calculation_group`, `pbip_add_hierarchy` |
| Warehouse audit | 2 | `audit_star_schema`, `scan_referential_integrity` |
| Desktop Bridge (preview) | 4 | `bridge_status`, `bridge_manifest`, `bridge_reload` (hot-reload), `bridge_screenshot` |
| Documentation, diff, CI | 5 | `export_data_dictionary`, `model_snapshot`, `model_diff`, `pre_deploy_gate`, `run_dax_tests` |
| Diagnostics and ops | 4 | `refresh_doctor`, `find_unused_objects`, `impact_analysis`, `rls_test_harness` |
| Governance-ops fleet (admin) | 3 | `cross_workspace_lineage`, `fleet_refresh_monitor`, `usage_and_orphan_analytics` |

### Resources, prompts, and completion

- **Resources:** `powerbi://desktop/{schema,measures,bpa,ai-readiness}`,
  `powerbi://cloud/{workspace}/{dataset}/schema`,
  `powerbi://reference/{bpa-rules,refresh-errors}`. Attach model context without a tool call.
- **Prompts:** `optimize_measure`, `explain_measure`, `audit_model`, `document_model`,
  `plan_safe_rename`, `pre_deploy_review`. Ready-made, tool-orchestrated playbooks.
- **Completion:** grounds prompt and resource arguments in real table and measure names from
  the connected model.
- **Annotations and structured output:** every tool declares safety hints
  (`readOnlyHint`, `destructiveHint`); key tools return typed `structuredContent`.

---

## Safe renames: the two-layer problem

Power BI stores a model layer and a report layer separately. TOM (and the official modeling MCP)
can edit the model, but cannot update report visuals, so a TOM rename leaves visuals pointing at
the old name. This server solves it with **PBIP file editing**: it rewrites the TMDL model files
and the PBIR report files (visual bindings, cultures, diagram) together, so nothing breaks.

```
User: "Load PBIP project from C:/Projects/SalesReport"
User: "Rename table Salesforce_Data to Sales Force Data"
```

A rename is planned in memory over the whole project and written in one transaction: every file
changes or none does. Nothing is written when the old name does not exist, the new name collides
with another object, or the result would leave a broken reference. Files keep their encoding and
line endings, and files the rename does not touch stay byte-identical. DAX is rewritten through a
tokenizer, so a name inside a string, a comment or another name is never touched; text that only
mentions the old name (a title, a label) is listed for you to review. `pbip_undo` restores the
previous state.

> **Always** use the `pbip_rename_*` tools for renames, not the deprecated TOM `batch_rename_*`
> tools. Close Power BI Desktop before PBIP edits, or keep it open and hot-reload afterwards
> with `bridge_reload`.

---

## The edit-and-verify loop (Desktop Bridge)

With Power BI Desktop June 2026+ the agent can drive a complete authoring loop against the
RUNNING app, with the files on disk as the source of truth:

```
bridge_status          which file is open, unsaved-change state, pages, and the AS port
   |
pbip_* / pbir_* tools  author offline: measures, date table, calc groups, pages, visuals
   |
bridge_reload          hot-reload the open file from disk - no close/reopen
   |
bridge_screenshot      PNG of each page so the agent can SEE and fix its own work
```

`bridge_reload` refuses to run over unsaved Desktop changes (pass `force=true` to override),
and `bridge_status` reports the matching Analysis Services port so the same window is one
`desktop_connect` away from live DAX and TOM.

---

## Security and governance

- **Query-only execution.** The tools that run your DAX accept DAX queries and nothing else.
  TMSL, XMLA and MDX commands are refused before they reach the engine.
- **Column and table policies** from `config/policies.yaml`: `block`, `mask`, `hash`, `redact`,
  `numeric_mask`, plus `max_rows`, `require_filter` and `aggregate_only` per table. They apply to
  every tool that runs DAX, follow aliases (`SELECTCOLUMNS(T, "x", T[ssn])`) and measures that
  wrap a protected column, and refuse `TOCSV` / `TOJSON` over a protected table.
- **Fail closed.** If the policy file is missing, unparseable or contains an invalid rule,
  queries are refused and `security_status` says why. Nothing runs with silently empty policies.
- **PII masking** on query results (SSN, credit card, email, phone, IP), with value validation
  so dates, descriptions and order numbers are left alone.
- **Audit log** with sequence numbers and a hash chain that continues across rotated files;
  verify it with `verify_audit_integrity`. Set `POWERBI_MCP_AUDIT_KEY` for an HMAC-SHA256 chain.
  Without a key the chain only catches accidental edits: someone who can rewrite the log can
  recompute it.
- **Read-only / lockdown mode:** set `POWERBI_MCP_READONLY=true` (or `1`) to refuse every tool
  that changes a model, a report or a file, while reads and diagnostics keep working. A value
  that cannot be read as a boolean turns lockdown on.
- **Secret redaction** on every tool response (text and structured content), every resource and
  the logs: connection strings, client secrets, bearer tokens, SAS signatures.

**What this layer is not.** Policies work on query text and result names, so they are defence
in depth, not a security boundary. A calculated column that copies protected data is judged by
its own name, a filter on a masked column can still confirm a guess, and metadata tools list
column names regardless of policy. Row-level and object-level security in the semantic model is
the boundary; use this layer on top of it.

```yaml
# config/policies.yaml (excerpt)
tables:
  - name: "*"
    columns:
      - name: ssn
        action: block
      - name: card_number
        action: mask
```

### Environment variables

| Variable | Purpose |
|----------|---------|
| `TENANT_ID`, `CLIENT_ID`, `CLIENT_SECRET` | Azure AD service principal (cloud, REST, admin) |
| `ADOMD_DLL_PATH` | Folder (or full path) of `Microsoft.AnalysisServices.AdomdClient.dll`, if auto-discovery misses it |
| `TOM_DLL_PATH` | Folder (or full path) of `Microsoft.AnalysisServices.Tabular.dll` for live writes (`ADOMD_DLL_PATH` is also searched) |
| `POWERBI_MCP_READONLY` | Refuse all write tools (lockdown mode). Accepts 1/true/yes/on |
| `POWERBI_MCP_QUERY_TIMEOUT` | Seconds before a DAX query is cancelled (default 120) |
| `POWERBI_MCP_POLICY_FILE` | Your own policy file; missing or invalid means queries are refused |
| `POWERBI_MCP_AUDIT_KEY` | Secret key that switches the audit hash chain to HMAC-SHA256 |
| `POWERBI_MCP_HASH_KEY` | Secret key for the `hash` column policy (default: a per-session key) |
| `POWERBI_MCP_AUDIT_DIR` | Folder for the audit log (default `logs/`) |
| `POWERBI_MCP_PBIP_BACKUPS` | `false` stops the automatic project backup before a rename or repair |
| `ENABLE_PII_DETECTION`, `ENABLE_AUDIT`, `ENABLE_POLICIES` | Toggle security subsystems (default on) |
| `PYTHONNET_RUNTIME` | `coreclr` when only the .NET (Core) builds of ADOMD/TOM are installed |
| `LOG_LEVEL` | `DEBUG` enables redacted argument logging |

---

## Documentation

| Doc | Contents |
|-----|----------|
| [docs/TOOLS.md](docs/TOOLS.md) | Complete reference of all 90 tools, resources, prompts, env vars |
| [docs/REPORT_SPEC.md](docs/REPORT_SPEC.md) | The declarative report spec behind `pbir_apply_spec`, with an example |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | Components, request boundary, security layer, verification methodology, file map |
| [docs/TESTING.md](docs/TESTING.md) | How to run the suites and what each covers |
| [CHANGELOG.md](CHANGELOG.md) | Everything that changed, by milestone |
| [AGENTS.md](AGENTS.md) | Agent playbook: golden rules, workflows, DAX patterns |

---

## Testing and verification

The 50 suites in `tests/` are assert-based scripts that run without Power BI (pure logic is
tested directly; the server and the live connectors are driven through fakes). CI runs them on
Windows and Linux.

```bash
python run_tests.py
```

What backs each claim:

1. **Assert suites** for all pure logic (tokenizer, renames, the report spec, generators,
   linters, auditors, security, parsers), plus the request boundary through the real MCP handler.
2. **Runs against real projects**: the rename engine, the report spec and the report audit were
   run on copies of real, complex reports (HTML Content, Deneb, custom visuals, bookmarks) and
   checked byte for byte.
3. **Doc-verification**: API contracts (PBIR and TMDL shapes, REST/INFO surfaces, the Desktop
   Bridge protocol) checked against Microsoft Learn and real exports.
4. **Engine and live checks (2026-07)**: generated TMDL parsed under Microsoft's
   `TmdlSerializer`; ADOMD queries, TOM batch writes and the Desktop Bridge exercised against a
   running Power BI Desktop. These predate the 3.8.0 rewrite and have not been repeated on it.

Not verified by execution: generated DAX and M are not evaluated by an engine in the suites,
generated reports are not opened in Desktop by them, and the cloud paths have not been run
against a tenant. Details in [docs/TESTING.md](docs/TESTING.md).

---

## Project structure

```
powerbi-mcp/
├── src/
│   ├── server.py                    # MCP server: 90 tools, request boundary, handlers
│   ├── dax_tokens.py                # Shared DAX tokenizer + reference rewriting
│   ├── safe_io.py                   # Faithful atomic writes + multi-file transaction
│   ├── powerbi_*_connector.py       # Desktop (ADOMD), XMLA, REST, TOM, PBIP
│   ├── adomd_loader.py, adomd_query.py, connection_strings.py   # live connectivity plumbing
│   ├── desktop_bridge.py            # Power BI Desktop Bridge client (JSON-RPC over named pipe)
│   ├── pbip_rename.py, pbip_report.py, pbip_json.py, pbip_mquery.py, tmdl_scan.py
│   │                                # the rename engine and its parsers
│   ├── report_spec.py, pbir_builders.py, pbir_packages.py       # report as code
│   ├── report_audit.py              # report-layer audit
│   ├── payload_measures.py, js_app_measure.py, html_measures.py, svg_measures.py
│   │                                # measures that draw: payloads, JS apps, HTML, SVG
│   ├── model_embed.py, publish_stage.py                         # self-contained publishing
│   ├── pbir_authoring.py, tmdl_authoring.py                     # single-visual and TMDL emitters
│   ├── model_analysis.py, dax_lint.py, dax_functions.py, dax_generator.py
│   ├── star_schema.py, naming_audit.py, bpa_authoring.py, pbix_tools.py
│   ├── refresh_diagnostics.py, governance.py
│   └── security/                    # policies, query analysis, PII, audit, redaction
├── config/policies.yaml
├── tests/                           # 50 assert-based suites
├── docs/                            # TOOLS, REPORT_SPEC, ARCHITECTURE, TESTING
├── .github/workflows/tests.yml      # CI: the suites on Windows and Linux
├── run_tests.py
├── pbip_diagnostic_tool.py            # Standalone PBIP diagnostic utility
├── AGENTS.md, CLAUDE.md
├── Dockerfile, requirements-core.txt
├── pyproject.toml, .editorconfig
├── CHANGELOG.md, requirements.txt
└── README.md
```

---

## Limitations

| Limitation | Notes |
|------------|-------|
| Live connectivity is Windows only | ADOMD.NET, TOM, and the Desktop Bridge named pipe require Windows. The offline subset runs cross-platform via Docker. |
| TOM renames break visuals | Use the PBIP tools for safe renames (they update the report layer too). |
| `bridge_screenshot` depends on a Desktop preview fix | On some Desktop builds `report.snapshot.capture` returns an internal error for any input (a Desktop-side preview defect). Status, manifest, and hot-reload work. |
| Cloud paths are doc-verified, not live-verified | XMLA/REST/admin tools are tested with fakes and fact-checked against Microsoft Learn; they have not yet been exercised against a production tenant. |
| The 3.8.0 rewrite has not been re-run live | The connectors and the rename engine were rewritten and tested through fakes and on copies of real projects; the earlier live Desktop verification has not been repeated on the new code. |
| Generated DAX, M and reports are not engine-tested | Measure generators, embedded partitions and the report spec are tested for shape and fidelity, not evaluated by Power BI in the suites. Validate with `validate_dax` and look with `bridge_screenshot`. |
| HTML Content, Deneb and custom visuals are not certified | Reports that use them will not render in a tenant that only allows certified visuals; `audit_report_design` lists them. A script-driven visual also cannot cross-filter other visuals. |
| Access policies are defence in depth | They work on query text and result names. Use row-level and object-level security in the model as the boundary. |
| Cloud enhanced refresh needs Premium | XMLA and enhanced refresh need PPU / Premium / Fabric capacity. Basic refresh and history work on Pro. |
| Fleet governance is admin-gated | Scanner and Activity tools need Fabric admin, or a service principal allowed to use read-only admin APIs. |
| Deep server timings | `analyze_query_performance` gives duration and hints; use DAX Studio for storage-vs-formula-engine timings (a trace-based loop is on the roadmap). |

---

## Roadmap

### Done

- Power BI Desktop and Service connectivity, RLS testing, TOM writes, PBIP safe editing.
- DAX validate-before-commit loop, atomic transactions, dependency and impact analysis.
- Best Practice Analyzer, AI-readiness scoring, VertiPaq-style storage and query analysis.
- Transactional, atomic, encoding-faithful PBIP renames (model + report + hierarchies + sort wiring).
- Enforced column policies, PII masking, numeric masking, a hash-chained audit (HMAC with a key),
  read-only mode, response-boundary secret redaction.
- Documentation export, model snapshot and diff, pre-deploy gate, DAX regression runner.
- Refresh doctor, unused-object detection, RLS test matrix.
- Cross-workspace lineage, fleet refresh monitor, usage analytics.
- Modern MCP surface: annotations, structured output, resources, prompts, completion.
- Docker image for the cross-platform offline subset.
- PBIR report authoring (pages, visuals, field bindings) with schema-verified output.
- DAX anti-pattern linter with rewrite hints; SVG micro-visual measure generators.
- Naming audit with rename plans; PBIX inspection/extraction; custom BPA rule governance.
- Bulk DAX creation (time intelligence, ratios, ranks) written offline into TMDL or live via
  validated TOM batches with intra-batch references and rollback.
- Offline data modelling: generated date dimensions, calculation groups, hierarchies,
  engine-verified against Microsoft's TmdlSerializer and a live Desktop.
- Star-schema audit and referential-integrity orphan scanning.
- Power BI Desktop Bridge integration: status, manifest, hot-reload, screenshots.
- Report as code: a declarative report spec, report audit, payload / HTML / JavaScript-app
  measures, data embedding, publish staging.
- A rebuilt rename engine (tokenizer and parsers instead of regexes), connector-owned
  transactions, fail-closed policies, a CI pipeline.

### Planned

- Trace-based DAX optimization loop (formula-engine vs storage-engine timings) and an
  EVALUATEANDLOG debugger.
- Refresh trigger/monitor/cancel (Enhanced Refresh API + Desktop TMSL).
- PyPI packaging and tagged releases.
- Live validation of the cloud XMLA/REST/admin paths against a production tenant, and a live
  re-run of the Desktop paths on the rewritten connectors.
- Evaluate generated DAX and open generated reports in Desktop as part of the test run.
- Report spec: drillthrough, report and page filters, visual interactions, mobile layout.
- Best Practice Analyzer auto-fix; field parameters, object-level security, translations.
- Remote HTTP transport with Microsoft Entra OAuth (today, use the official remote Power BI MCP
  server for cloud auth).

---

## Contributing

1. Fork the repository.
2. Create a feature branch.
3. Keep the tool registry in sync (a tool lives in `build_tools`, `_build_tool_dispatch`,
   and `_build_tool_annotations` in `src/server.py`; a parity check enforces this) and add it to
   `docs/TOOLS.md` (a docs parity check enforces that too).
4. Read and rewrite DAX through `src/dax_tokens.py` and write files through `src/safe_io.py`.
5. Run `python run_tests.py` and keep all suites green.
6. Open a pull request.

Formatting conventions are in `pyproject.toml` and `.editorconfig`.

---

## Author

**Sulaiman Ahmed**, Data Analytics Engineer and Microsoft Certified Professional.

[![GitHub](https://img.shields.io/badge/GitHub-sulaiman013-181717?style=flat-square&logo=github)](https://github.com/sulaiman013)
[![Portfolio](https://img.shields.io/badge/Portfolio-sulaiman--ahmed-blue?style=flat-square&logo=google-chrome)](https://sulaiman-ahmed.lovable.app)

---

## License

MIT. See [LICENSE](LICENSE).

## Acknowledgments

- [Model Context Protocol](https://modelcontextprotocol.io) by Anthropic.
- Microsoft's TOM, TMDL, and PBIR documentation.
- The Power BI community for insights on the PBIP format and semantic-model best practices.
