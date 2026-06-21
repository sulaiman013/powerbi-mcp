# Power BI MCP Server

<p align="center">
  <strong>An enterprise-grade Model Context Protocol server for Power BI and Microsoft Fabric.</strong>
</p>

<p align="center">
  <a href="https://modelcontextprotocol.io"><img src="https://img.shields.io/badge/MCP-compatible-blue?style=flat-square" alt="MCP compatible"></a>
  <a href="https://www.python.org"><img src="https://img.shields.io/badge/Python-3.10%2B-green?style=flat-square" alt="Python 3.10+"></a>
  <a href="#"><img src="https://img.shields.io/badge/Tools-58-purple?style=flat-square" alt="58 tools"></a>
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
Power BI content through one consistent interface. It talks to a local Power BI Desktop model,
a published Power BI Service dataset, or Power BI Project (PBIP) files on disk, and wraps every
operation in a security and governance layer.

It exposes **58 tools** plus MCP **resources**, **prompts**, and **completion**, and ships with
12 assert-based test suites.

| Capability | What you get |
|------------|--------------|
| **Dual connectivity** | Power BI Desktop (local) and Power BI Service (cloud) |
| **Natural-language DAX** | Run, validate, and optimize DAX through conversation |
| **Safe refactoring** | PBIP-based renames that update the model **and** the report visuals |
| **DAX safety loop** | Validate before committing; impact analysis; atomic transactions |
| **Model quality** | Best Practice Analyzer, AI-readiness scoring, VertiPaq-style storage analysis |
| **Diagnostics and ops** | Refresh-failure triage, unused-object detection, RLS test matrix |
| **Governance** | Enforced PII and column policies, tamper-evident audit, read-only mode |
| **Fleet (admin)** | Cross-workspace lineage, fleet refresh monitor, usage analytics |
| **Modern MCP** | Tool annotations, structured output, resources, prompts, completion |

---

## Why this server

Microsoft now ships official Power BI MCP servers (public preview): a remote one for
chat-with-data and a local modeling one for authoring semantic models. This project is
**complementary**. It leans into what those servers do not cover:

- **Report-aware safe renames.** The official local modeling MCP edits the model only and
  cannot touch the report layer. This server renames tables, columns, and measures across
  both the model (TMDL) and the report (PBIR visuals, cultures, diagram), so visuals do not break.
- **A real governance and security layer.** Enforced column policies (block, mask, hash,
  redact, numeric-mask), PII detection, a tamper-evident audit log, and a read-only lockdown mode.
- **Diagnostics and fleet ops.** Refresh-failure classification, unused-object cleanup, impact
  analysis, an RLS test matrix, and tenant-wide lineage and usage analytics.
- **Offline, PBIP-first workflows.** The whole PBIP, analysis, and security subset runs
  cross-platform with no Fabric capacity required.

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for how it fits together.

---

## Quick start

### Prerequisites

**Live connectivity (Power BI Desktop / Service):** Windows 10/11, Power BI Desktop, Python 3.10+,
and ADOMD.NET (ships with Power BI Desktop or SSMS). Cloud also needs an Azure AD service
principal and, for some operations, a Premium / PPU / Fabric capacity.

**Offline subset only (PBIP editing, BPA, analysis, security):** any OS, Python 3.10+, no .NET.

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

58 tools across the categories below. The full reference, with parameters and read / write /
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
| Model quality and performance | 4 | `run_bpa`, `audit_ai_readiness`, `analyze_model_storage`, `analyze_query_performance` |
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
> tools. Close Power BI Desktop before PBIP edits, then reopen.

---

## Security and governance

- **PII detection and masking** before results reach the AI (SSN, credit card, email, phone, IP).
- **Enforced column and table policies** from `config/policies.yaml`: `block`, `mask`, `hash`,
  `redact`, and `numeric_mask` (session-randomized scaling that hides values but preserves ratios).
- **Audit logging** with a tamper-evident hash chain; verify it with `verify_audit_integrity`.
- **Read-only / lockdown mode:** set `POWERBI_MCP_READONLY=true` to refuse every write tool while
  reads and diagnostics keep working. Ideal for shared or autonomous agent use.
- Connection-string secrets and PII are redacted from logs and error messages.

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
| `POWERBI_MCP_READONLY` | `true` refuses all write tools (lockdown mode) |
| `ENABLE_PII_DETECTION`, `ENABLE_AUDIT`, `ENABLE_POLICIES` | Toggle security subsystems (default true) |
| `LOG_LEVEL` | `DEBUG` enables redacted argument logging |

---

## Documentation

| Doc | Contents |
|-----|----------|
| [docs/TOOLS.md](docs/TOOLS.md) | Complete reference of all 58 tools, resources, prompts, env vars |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | Components, security layer, registry pattern, verification methodology, file map |
| [docs/TESTING.md](docs/TESTING.md) | How to run the suites and what each covers |
| [CHANGELOG.md](CHANGELOG.md) | Everything that changed, by milestone |
| [AGENTS.md](AGENTS.md) | Agent playbook: golden rules, workflows, DAX patterns |

---

## Testing

The suites in `tests/` are assert-based scripts that run without Power BI (pure logic is tested
directly; live connectors are mocked).

```bash
python run_tests.py
```

See [docs/TESTING.md](docs/TESTING.md) for what each suite covers. Live Desktop, XMLA, REST, and
admin paths are doc-verified against Microsoft Learn and mock-tested; end-to-end verification of
those paths needs a Windows + Power BI / Fabric environment.

---

## Project structure

```
powerbi-mcp/
├── src/
│   ├── server.py                    # MCP server: 58 tools + resources/prompts/completion
│   ├── powerbi_desktop_connector.py # Desktop (ADOMD) + RLS + VertiPaq DMVs
│   ├── powerbi_xmla_connector.py    # Cloud XMLA
│   ├── powerbi_rest_connector.py    # REST: discovery, refresh, admin Scanner/Activity
│   ├── powerbi_tom_connector.py     # TOM writes: measures, relationships, transactions
│   ├── powerbi_pbip_connector.py    # PBIP/TMDL/PBIR offline editing (transactional)
│   ├── model_analysis.py            # BPA, AI-readiness, data dictionary, diff, DAX tests
│   ├── refresh_diagnostics.py       # Refresh error classification
│   ├── governance.py                # Scanner summary + activity aggregation
│   └── security/                    # security_layer, access_policy, pii_detector, audit_logger
├── config/policies.yaml
├── tests/                           # Assert-based suites
├── docs/                            # TOOLS, ARCHITECTURE, TESTING
├── run_tests.py
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
| Live connectivity is Windows only | ADOMD.NET and TOM require Windows. The offline subset runs cross-platform via Docker. |
| TOM renames break visuals | Use the PBIP tools for safe renames (they update the report layer too). |
| Cloud enhanced refresh needs Premium | XMLA and enhanced refresh need PPU / Premium / Fabric capacity. Basic refresh and history work on Pro. |
| Fleet governance is admin-gated | Scanner and Activity tools need Fabric admin, or a service principal allowed to use read-only admin APIs. |
| Deep server timings | `analyze_query_performance` gives duration and hints; use DAX Studio for storage-vs-formula-engine timings. |

---

## Roadmap

### Done

- Power BI Desktop and Service connectivity, RLS testing, TOM writes, PBIP safe editing.
- DAX validate-before-commit loop, atomic transactions, dependency and impact analysis.
- Best Practice Analyzer, AI-readiness scoring, VertiPaq-style storage and query analysis.
- Transactional, atomic, encoding-faithful PBIP renames.
- Enforced column policies, PII masking, numeric masking, tamper-evident audit, read-only mode.
- Documentation export, model snapshot and diff, pre-deploy gate, DAX regression runner.
- Refresh doctor, unused-object detection, RLS test matrix.
- Cross-workspace lineage, fleet refresh monitor, usage analytics.
- Modern MCP surface: annotations, structured output, resources, prompts, completion.
- Docker image for the cross-platform offline subset.

### Planned

- Remote HTTP transport with Microsoft Entra OAuth (today, use the official remote Power BI MCP
  server for cloud auth).
- Best Practice Analyzer auto-fix and custom team rule packs.
- Deeper VertiPaq (per-column cardinality) and server-timings capture.
- Optional local / open-source LLM support.

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
