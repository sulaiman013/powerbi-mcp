# Power BI MCP Server

<p align="center">
  <strong>An enterprise-grade Model Context Protocol server for Power BI and Microsoft Fabric.</strong>
</p>

<p align="center">
  <a href="https://modelcontextprotocol.io"><img src="https://img.shields.io/badge/MCP-compatible-blue?style=flat-square" alt="MCP compatible"></a>
  <a href="https://www.python.org"><img src="https://img.shields.io/badge/Python-3.10%2B-green?style=flat-square" alt="Python 3.10+"></a>
  <a href="#"><img src="https://img.shields.io/badge/Tools-82-purple?style=flat-square" alt="82 tools"></a>
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
hot-reload and screenshots), and wraps every operation in a security and governance layer.

It exposes **82 tools** plus MCP **resources**, **prompts**, and **completion**, and ships with
25 assert-based test suites.

| Capability | What you get |
|------------|--------------|
| **Dual connectivity** | Power BI Desktop (local) and Power BI Service (cloud) |
| **Natural-language DAX** | Run, validate, and optimize DAX through conversation |
| **Safe refactoring** | PBIP-based renames that update the model **and** the report visuals |
| **Bulk DAX creation** | Measure-suite generator (time intelligence, ratios, ranks) written offline or live |
| **Data modelling** | Date-dimension generator, calculation groups, hierarchies, offline TMDL authoring |
| **Warehouse audit** | Star-schema classification + findings, referential-integrity orphan scan |
| **Report authoring (preview)** | Add pages, visuals, and field bindings to PBIR reports from the agent |
| **Desktop Bridge (preview)** | Hot-reload the open report and screenshot pages in the RUNNING Desktop |
| **DAX safety loop** | Validate before committing; impact analysis; atomic transactions |
| **Model quality** | Best Practice Analyzer, AI-readiness scoring, VertiPaq-style storage analysis |
| **Diagnostics and ops** | Refresh-failure triage, unused-object detection, RLS test matrix |
| **Governance** | Enforced PII and column policies, tamper-evident audit, read-only mode |
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
- **Build report pages and visuals (preview).** On a saved PBIP project it can add a page, drop
  a chart, card, table, or slicer on it, and bind fields by role. It checks each field exists in
  the model first, picks measure vs aggregated-column automatically, and writes Power-BI-faithful
  PBIR files (right down to the `nativeQueryRef` and `Sum(...)` query refs Desktop itself writes).
  > "On the PBIP project, add an 'Overview' page with a bar chart of Sales by Region."
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
  specific columns, keep a tamper-evident audit log, and flip on read-only mode so an agent can
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
  both the model (TMDL) and the report (PBIR visuals, cultures, diagram, hierarchies, sort
  wiring), so visuals do not break.
- **Offline authoring of models AND reports.** Bulk measures, date dimensions, calculation
  groups, hierarchies, report pages and visuals are written straight into PBIP files, no
  Power BI required, in the exact shapes Desktop itself serializes (verified against
  Microsoft's own TMDL engine and PBIR schemas).
- **The full edit-and-verify loop.** Offline authoring + the Desktop Bridge (hot-reload the
  running Desktop, no close/reopen) + one call that chains a bridge instance into live DAX/TOM.
- **Warehouse-grade auditing.** Star-schema classification with findings and a score, a
  referential-integrity orphan scan, a DAX anti-pattern linter, and custom BPA rule governance.
- **A real governance and security layer.** Enforced column policies (block, mask, hash,
  redact, numeric-mask), PII detection, a tamper-evident (optionally HMAC-keyed) audit log,
  and a read-only lockdown mode.
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

82 tools across the categories below. The full reference, with parameters and read / write /
destructive markers, is in **[docs/TOOLS.md](docs/TOOLS.md)**.

| Category | Count | Highlights |
|----------|:-----:|------------|
| Desktop (local, ADOMD) | 7 | discover, connect, list tables/columns/measures, `desktop_execute_dax`, model info |
| Cloud (XMLA + REST) | 6 | workspaces, datasets, tables, columns, `execute_dax`, model info |
| Security and audit | 3 | `security_status`, `security_audit_log`, `verify_audit_integrity` |
| Row-Level Security | 3 | list roles, set role, status |
| Model writes (TOM) | 7 | `create_measure`, `delete_measure`, `batch_update_measures`, deprecated batch renames |
| DAX safety and transactions | 5 | `validate_dax`, `scan_measure_dependencies`, begin/commit/rollback transaction |
| Relationships | 2 | `create_relationship`, `delete_relationship` |
| PBIP safe editing | 5 | load project, get info, rename tables/columns/measures (model + report) |
| PBIP diagnostics | 4 | fix broken visuals, fix DAX quoting, scan broken refs, validate |
| Report authoring (PBIR, preview) | 4 | `pbir_add_page`, `pbir_add_visual`, `pbir_bind_fields`, `pbir_validate_report` |
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

The rename cascade is transactional (it rolls every file back on failure) and writes atomically
(temp file plus `os.replace`), preserving encoding and line endings.

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

- **PII detection and masking** before results reach the AI (SSN, credit card, email, phone, IP).
- **Enforced column and table policies** from `config/policies.yaml`: `block`, `mask`, `hash`,
  `redact`, and `numeric_mask` (session-randomized scaling that hides values but preserves ratios).
- **Audit logging** with a tamper-evident hash chain; verify it with `verify_audit_integrity`.
  Set `POWERBI_MCP_AUDIT_KEY` to switch the chain to HMAC-SHA256 (cryptographically strong against
  an attacker who edits the log); without a key it is a plain SHA-256 chain that still catches
  accidental edits and naive tampering.
- **Read-only / lockdown mode:** set `POWERBI_MCP_READONLY=true` to refuse every write tool
  (model/report mutations **and** file-writing tools like snapshots, dictionaries, and PBIX
  extraction) while reads and diagnostics keep working. Ideal for shared or autonomous agent use.
- Connection-string secrets and PII are redacted from logs, error messages, the audit log, and
  every tool response (redaction is applied at the response boundary, not just per-handler).

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
| `POWERBI_MCP_READONLY` | `true` refuses all write tools (lockdown mode) |
| `POWERBI_MCP_AUDIT_KEY` | Secret key that switches the audit hash chain to HMAC-SHA256 (stronger tamper-resistance) |
| `ENABLE_PII_DETECTION`, `ENABLE_AUDIT`, `ENABLE_POLICIES` | Toggle security subsystems (default true) |
| `LOG_LEVEL` | `DEBUG` enables redacted argument logging |

---

## Documentation

| Doc | Contents |
|-----|----------|
| [docs/TOOLS.md](docs/TOOLS.md) | Complete reference of all 82 tools, resources, prompts, env vars |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | Components, security layer, registry pattern, verification methodology, file map |
| [docs/TESTING.md](docs/TESTING.md) | How to run the suites and what each covers |
| [CHANGELOG.md](CHANGELOG.md) | Everything that changed, by milestone |
| [AGENTS.md](AGENTS.md) | Agent playbook: golden rules, workflows, DAX patterns |

---

## Testing and verification

The 25 suites in `tests/` are assert-based scripts that run without Power BI (pure logic is
tested directly; live connectors are mocked).

```bash
python run_tests.py
```

Verification goes four layers deep, using the strongest check available per surface:

1. **Assert suites** for all pure logic (emitters, linters, auditors, security, parsers).
2. **Adversarial doc-verification**: API contracts (PBIR schemas, TMDL shapes, REST/INFO
   surfaces, the Desktop Bridge protocol) fact-checked against Microsoft Learn and real
   exports before implementation.
3. **Engine-level validation**: generated TMDL parses under Microsoft's own `TmdlSerializer`,
   the code path Power BI Desktop runs when opening a PBIP.
4. **Live testing** against a running Power BI Desktop: ADOMD queries, validated TOM batch
   writes with rollback, the star-schema audit on a real model, and the Desktop Bridge
   (discovery, manifest, state, hot-reload).

Cloud XMLA/REST/admin paths are doc-verified and mock-tested; their end-to-end verification
needs a real tenant. Details in [docs/TESTING.md](docs/TESTING.md).

---

## Project structure

```
powerbi-mcp/
├── src/
│   ├── server.py                    # MCP server: 82 tools + resources/prompts/completion
│   ├── powerbi_desktop_connector.py # Desktop (ADOMD) + RLS + VertiPaq DMVs
│   ├── powerbi_xmla_connector.py    # Cloud XMLA
│   ├── powerbi_rest_connector.py    # REST: discovery, refresh, admin Scanner/Activity
│   ├── powerbi_tom_connector.py     # TOM writes: measures, relationships, transactions
│   ├── powerbi_pbip_connector.py    # PBIP/TMDL/PBIR offline editing (transactional)
│   ├── pbir_authoring.py            # PBIR emitters: pages, visuals, field projections
│   ├── adomd_loader.py             # Shared ADOMD.NET discovery (Desktop + XMLA)
│   ├── model_analysis.py            # BPA, AI-readiness, data dictionary, diff, DAX tests
│   ├── dax_lint.py                  # DAX anti-pattern linter + rewrite hints (tokenizer)
│   ├── svg_measures.py             # SVG micro-visual DAX measure generators
│   ├── naming_audit.py             # Naming-convention audit -> rename plan
│   ├── pbix_tools.py               # PBIX (.pbix ZIP) inspect/extract + layout decode
│   ├── bpa_authoring.py            # Custom BPA rule validation + rule-source audit
│   ├── dax_generator.py            # Bulk measure-suite generation (time intel, ratios, ranks)
│   ├── star_schema.py              # Star-schema classification + warehouse audit
│   ├── tmdl_authoring.py           # TMDL emitters: measures, date table, calc groups, hierarchies
│   ├── desktop_bridge.py           # Power BI Desktop Bridge client (JSON-RPC over named pipe)
│   ├── refresh_diagnostics.py       # Refresh error classification
│   ├── governance.py                # Scanner summary + activity aggregation
│   └── security/                    # security_layer, access_policy, pii_detector, audit_logger
├── config/policies.yaml
├── tests/                           # Assert-based suites
├── docs/                            # TOOLS, ARCHITECTURE, TESTING
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
| `bridge_screenshot` depends on a Desktop preview fix | On current Desktop builds `report.snapshot.capture` can return an internal error for any input (a Desktop-side preview defect; verified independent of this client). Status, manifest, and hot-reload work. |
| Cloud paths are doc-verified, not live-verified | XMLA/REST/admin tools are mock-tested and fact-checked against Microsoft Learn; they have not yet been exercised against a production tenant. |
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
- Enforced column policies, PII masking, numeric masking, HMAC-capable tamper-evident audit,
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

### Planned

- Trace-based DAX optimization loop (formula-engine vs storage-engine timings) and an
  EVALUATEANDLOG debugger.
- Refresh trigger/monitor/cancel (Enhanced Refresh API + Desktop TMSL).
- PyPI packaging, CI pipeline, and tagged releases.
- Live validation of the cloud XMLA/REST/admin paths against a production tenant.
- Best Practice Analyzer auto-fix; field parameters, object-level security, translations.
- Remote HTTP transport with Microsoft Entra OAuth (today, use the official remote Power BI MCP
  server for cloud auth).

---

## Contributing

1. Fork the repository.
2. Create a feature branch.
3. Keep the tool registry in sync (a tool lives in `handle_list_tools`, `_build_tool_dispatch`,
   and `_build_tool_annotations` in `src/server.py`; a parity check enforces this).
4. Run `python run_tests.py` and keep all suites green.
5. Open a pull request.

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
