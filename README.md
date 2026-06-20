# Power BI MCP Server

<p align="center">
  <strong>An enterprise-grade Model Context Protocol server for Power BI</strong>
</p>

<p align="center">
  <a href="https://modelcontextprotocol.io"><img src="https://img.shields.io/badge/MCP-Compatible-blue?style=flat-square" alt="MCP Compatible"></a>
  <a href="https://www.python.org"><img src="https://img.shields.io/badge/Python-3.10+-green?style=flat-square" alt="Python 3.10+"></a>
  <a href="#"><img src="https://img.shields.io/badge/Platform-Windows-lightgrey?style=flat-square" alt="Windows"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-MIT-yellow?style=flat-square" alt="MIT License"></a>
  <a href="#"><img src="https://img.shields.io/badge/Tools-45-purple?style=flat-square" alt="45 Tools"></a>
</p>

<p align="center">
  <em>Enable AI assistants to interact with Power BI Desktop and Power BI Service through natural language.</em>
</p>

---

> **Disclaimer**: This is an independent, community-developed tool and is not affiliated with, endorsed by, or connected to Microsoft Corporation or Anthropic.

---

## Overview

Power BI MCP Server bridges the gap between AI assistants and Microsoft Power BI, enabling seamless interaction with semantic models through the Model Context Protocol. Whether you're working with local `.pbix` files or cloud-hosted datasets, this server provides a unified interface for querying data, managing models, and performing bulk operations—all while maintaining enterprise-grade security.

### Key Capabilities

| Capability | Description |
|------------|-------------|
| **Dual Connectivity** | Connect to both Power BI Desktop (local) and Power BI Service (cloud) |
| **Natural Language Queries** | Execute DAX queries through conversational AI |
| **Bulk Operations** | Rename tables, columns, and measures across your model |
| **Security First** | PII detection, audit logging, and configurable access policies |
| **RLS Testing** | Test Row-Level Security roles during development |
| **Safe Refactoring** | PBIP-based editing preserves report visual integrity |
| **DAX Safety Loop** | Validate DAX before committing; impact analysis; atomic transactions |
| **Model Quality** | Best Practice Analyzer, AI-readiness scoring, VertiPaq-style storage analysis |
| **Modern MCP** | Tool annotations, structured output, resources, prompts, and completion |

---

## What's New: 2026 Agentic Enhancements

Microsoft now ships official Power BI MCP servers. This project leans into what they
**don't** do and modernizes the whole surface:

- **DAX safety loop** - `validate_dax` checks a query/measure against the live model
  before anything is written; `create_measure` / `batch_update_measures` validate
  automatically; `scan_measure_dependencies` (INFO.CALCDEPENDENCY) shows impact
  before a change.
- **Atomic transactions** - `tom_begin_transaction` / `tom_commit_transaction` /
  `tom_rollback_transaction` make bulk model edits all-or-nothing.
- **Model quality + performance** - `run_bpa` (Best Practice Analyzer),
  `audit_ai_readiness` (Copilot/agent-readiness score), `analyze_model_storage`
  (VertiPaq-style), and `analyze_query_performance`.
- **Relationship management** - `create_relationship` / `delete_relationship`.
- **Hardened safe rename** - the PBIP rename cascade is now transactional (auto
  rollback on failure) with atomic, BOM/CRLF-faithful writes.
- **Real governance** - column-level access policies now actually enforce at runtime
  (BLOCK / MASK / HASH / REDACT), and secrets are redacted from logs and errors.
- **First-class MCP** - every tool carries safety annotations
  (`readOnlyHint` / `destructiveHint`); key tools return typed structured output;
  the server exposes **resources** (`powerbi://...`), **prompts**, and **completion**.

**Positioning vs Microsoft's official Power BI MCP:** the official *remote* server is
best for cloud chat-with-data and the official *local modeling* MCP for raw model
authoring. This server is complementary: it adds **report-layer-aware safe renames**
(which the official local MCP explicitly cannot do), a **governance/PII layer**, **RLS
testing**, model-quality tooling, and an **offline PBIP-first** workflow that needs no
Fabric capacity. See [AGENTS.md](AGENTS.md) for the agent playbook.

---

## The V1 to V2 Journey: Challenges & Solutions

### The Challenge: Bulk Renames Breaking Reports

In V1, we successfully connected to Power BI Service via XMLA endpoints. V2 introduced Power BI Desktop connectivity and write operations using Microsoft's **Tabular Object Model (TOM)**. However, we encountered a critical limitation:

```
Problem: Renaming a table via TOM breaks all report visuals referencing that table.
```

**Root Cause Analysis:**

Power BI stores data in two separate layers:

```
+------------------------+     +------------------------+
|    Semantic Model      |     |    Report Definition   |
|    (Model Layer)       |     |    (Report Layer)      |
+------------------------+     +------------------------+
|  - Tables              |     |  - Visual bindings     |
|  - Columns             |     |  - Slicers & filters   |
|  - Measures            |     |  - Bookmarks           |
|  - Relationships       |     |  - Field references    |
|  - DAX expressions     |     |  - Formatting          |
+------------------------+     +------------------------+
         |                              |
         v                              v
    TOM can modify             TOM CANNOT modify
```

When you rename a table using TOM, it updates:
- The table name in the model
- DAX expressions in measures (with our auto-update logic)
- Relationship references

But it **cannot** update:
- Visual field bindings in `report.json`
- Slicer configurations
- Bookmark references

This causes visuals to show errors like *"Can't find column 'OldTableName'[Column]"*.

### The Solution: PBIP File-Based Editing

We researched Microsoft's own approach and discovered that even their tools face this limitation. The solution lies in the **Power BI Project (PBIP)** format—a text-based representation of both the model and report.

**Two Report Formats Supported:**

| Format | Structure | Default |
|--------|-----------|---------|
| **PBIR-Legacy** | Single `report.json` | Until Jan 2026 |
| **PBIR-Enhanced** | Individual `visual.json` files | From Jan 2026 |

```
MyReport.pbip
├── MyReport.SemanticModel/
│   ├── definition.tmdl          <- Model definitions (text)
│   └── definition/
│       ├── tables/*.tmdl        <- Table definitions
│       ├── cultures/*.tmdl      <- Linguistic schema (Q&A)
│       └── relationships.tmdl
└── MyReport.Report/
    ├── report.json              <- PBIR-Legacy: All visuals here
    ├── definition.pbir
    └── definition/              <- PBIR-Enhanced structure
        ├── report.json          <- Report-level settings only
        └── pages/
            └── [page_id]/
                └── visuals/
                    └── [visual_id]/
                        └── visual.json  <- Individual visual definition
```

**Reference:** [Data Goblins - Programmatically Modify Reports](https://data-goblins.com/power-bi/programmatically-modify-reports)

**Our Implementation:**

We built a dedicated **PBIP Connector** that:

1. **Parses PBIP Structure** - Locates all TMDL files and `report.json`
2. **Updates Model Layer** - Regex-based find-replace in TMDL files for:
   - Table definitions (`table OldName` → `table NewName`)
   - DAX references (`'OldTable'[Column]` → `'NewTable'[Column]`)
3. **Updates Report Layer** - JSON manipulation in `report.json` for:
   - Entity bindings (`"Entity": "OldTable"` → `"Entity": "NewTable"`)
   - Query references (`"OldTable.Column"` → `"NewTable.Column"`)
   - Native reference names

**Result:** Tables, columns, and measures can be renamed without breaking a single visual.

### Lessons Learned

| Challenge | Solution |
|-----------|----------|
| TOM can't modify report layer | PBIP file-based editing |
| DAX references not auto-updating | Regex-based expression rewriting |
| No visibility into rename impact | `scan_table_dependencies` tool |
| Complex PBIP structure | Dedicated connector with path discovery |

---

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                        AI Assistant                          │
│                  (Claude Desktop / Claude Code)              │
└─────────────────────────────┬────────────────────────────────┘
                              │ MCP Protocol
                              ▼
┌──────────────────────────────────────────────────────────────┐
│                    Power BI MCP Server                       │
│                  (45 Tools + Resources/Prompts)              │
├──────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │  Security   │  │   Audit     │  │   Access Policies   │  │
│  │  (PII Mask) │  │   Logger    │  │   (Block/Mask)      │  │
│  └─────────────┘  └─────────────┘  └─────────────────────┘  │
├──────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │   Desktop   │  │    XMLA     │  │       PBIP          │  │
│  │  Connector  │  │  Connector  │  │     Connector       │  │
│  │  (ADOMD)    │  │  (Cloud)    │  │  (File Editing)     │  │
│  └──────┬──────┘  └──────┬──────┘  └──────────┬──────────┘  │
│         │                │                     │             │
│  ┌──────┴──────┐  ┌──────┴──────┐  ┌──────────┴──────────┐  │
│  │     TOM     │  │   REST API  │  │    TMDL + JSON      │  │
│  │  Connector  │  │  Connector  │  │      Parser         │  │
│  └─────────────┘  └─────────────┘  └─────────────────────┘  │
└──────────────────────────────────────────────────────────────┘
         │                 │                     │
         ▼                 ▼                     ▼
┌─────────────┐    ┌─────────────┐       ┌─────────────┐
│  Power BI   │    │  Power BI   │       │    PBIP     │
│   Desktop   │    │   Service   │       │   Files     │
│ (localhost) │    │   (Cloud)   │       │   (.pbip)   │
└─────────────┘    └─────────────┘       └─────────────┘
```

---

## Available Tools

### Desktop Operations (7 tools)
| Tool | Description |
|------|-------------|
| `desktop_discover_instances` | Auto-discover running Power BI Desktop instances |
| `desktop_connect` | Connect to a specific instance with optional RLS role |
| `desktop_list_tables` | List all tables in the connected model |
| `desktop_list_columns` | List columns for a specific table |
| `desktop_list_measures` | List all measures with their expressions |
| `desktop_execute_dax` | Execute DAX queries with security processing |
| `desktop_get_model_info` | Get comprehensive model metadata |

### Cloud Operations (6 tools)
| Tool | Description |
|------|-------------|
| `list_workspaces` | List accessible Power BI Service workspaces |
| `list_datasets` | List datasets in a workspace |
| `list_tables` | List tables via XMLA endpoint |
| `list_columns` | List columns for a table |
| `execute_dax` | Execute DAX queries against cloud datasets |
| `get_model_info` | Get model info using INFO.VIEW functions |

### Security & Compliance (2 tools)
| Tool | Description |
|------|-------------|
| `security_status` | View current security configuration |
| `security_audit_log` | Query recent audit log entries |

### Row-Level Security (3 tools)
| Tool | Description |
|------|-------------|
| `desktop_list_rls_roles` | List RLS roles defined in the model |
| `desktop_set_rls_role` | Activate an RLS role for testing |
| `desktop_rls_status` | Get current RLS status |

### Model Modification via TOM (7 tools)

> ⚠️ **WARNING: TOM-based rename tools are DEPRECATED for renaming operations!**
>
> The `batch_rename_*` TOM tools only update the in-memory model and **DO NOT update report visuals**. This causes broken visuals after rename. **Use PBIP tools instead for all rename operations.**

| Tool | Description |
|------|-------------|
| `scan_table_dependencies` | Analyze impact before renaming |
| `batch_rename_tables` | ⚠️ DEPRECATED - Use `pbip_rename_tables` instead |
| `batch_rename_columns` | ⚠️ DEPRECATED - Use `pbip_rename_columns` instead |
| `batch_rename_measures` | ⚠️ DEPRECATED - Use `pbip_rename_measures` instead |
| `batch_update_measures` | Bulk update measure expressions |
| `create_measure` | Create a new DAX measure |
| `delete_measure` | Delete an existing measure |

### PBIP Safe Editing (5 tools) ✅ RECOMMENDED

> **This is the correct way to rename tables, columns, and measures!**
>
> PBIP tools update everything: TMDL files, DAX expressions (with proper quoting), report visuals, and Q&A schema. **Close Power BI Desktop before using, then reopen after.**

| Tool | Description |
|------|-------------|
| `pbip_load_project` | Load a PBIP project for editing |
| `pbip_get_project_info` | Get project structure information (detects PBIR format) |
| `pbip_rename_tables` | ✅ **Comprehensive rename**: updates TMDL + DAX quoting + visual.json + cultures (all automatic) |
| `pbip_rename_columns` | ✅ Rename columns (model + report layer, both PBIR formats) |
| `pbip_rename_measures` | ✅ Rename measures (model + report layer, both PBIR formats) |

### PBIP Diagnostics (4 tools)
| Tool | Description |
|------|-------------|
| `pbip_fix_broken_visuals` | Fix visual references after TOM/API rename (when rename was done outside PBIP tools) |
| `pbip_fix_dax_quoting` | Fix any remaining unquoted table names in DAX expressions |
| `pbip_scan_broken_refs` | Scan project for broken references - compare model vs visuals |
| `pbip_validate` | Validate TMDL syntax, find quoting issues, invalid references |

> **Note:** DAX quoting and visual updates are now **automatically handled** by `pbip_rename_tables`. The repair tools are only needed when renames were done outside of PBIP (e.g., via TOM API or Power BI Desktop).

### DAX Safety Loop & Transactions (5 tools) ✅ NEW

| Tool | Description |
|------|-------------|
| `validate_dax` | Validate a DAX query or scalar measure expression against the live model **without committing** |
| `scan_measure_dependencies` | Upstream/downstream impact analysis via `INFO.CALCDEPENDENCY` before renaming/deleting |
| `tom_begin_transaction` | Start an atomic write transaction (defers saves) |
| `tom_commit_transaction` | Commit all pending model edits |
| `tom_rollback_transaction` | Roll back all pending model edits |

> `create_measure` and `batch_update_measures` now **validate expressions before committing** and **honor open transactions**.

### Model Quality & Performance (4 tools) ✅ NEW

| Tool | Description |
|------|-------------|
| `run_bpa` | Best Practice Analyzer (Performance / DAX / Naming / Formatting / Maintenance / Error-Prevention) with severity and fix hints |
| `audit_ai_readiness` | Score Copilot/agent-readiness (description & format coverage) 0-100 with recommendations |
| `analyze_model_storage` | VertiPaq-style per-table row counts (exact) + sizes, ranked |
| `analyze_query_performance` | Time a DAX query and return optimization hints |

### Relationship Management (2 tools) ✅ NEW

| Tool | Description |
|------|-------------|
| `create_relationship` | Create a relationship (cardinality + cross-filter), transaction-aware |
| `delete_relationship` | Delete a relationship by name or by from/to table[/column] |

### MCP Resources, Prompts & Completion ✅ NEW

Beyond tools, the server is a first-class MCP citizen:

- **Resources:** `powerbi://desktop/schema`, `.../measures`, `.../bpa`, `.../ai-readiness`, and the template `powerbi://cloud/{workspace}/{dataset}/schema` - attach model context without spending a tool call.
- **Prompts:** `optimize_measure`, `explain_measure`, `audit_model`, `document_model`, `plan_safe_rename` - ready-made, tool-orchestrated playbooks.
- **Completion:** grounds prompt/template arguments in real table/measure names from the connected model.
- **Tool annotations + structured output:** every tool declares `readOnlyHint`/`destructiveHint`; `validate_dax`, `run_bpa`, and `audit_ai_readiness` return typed `structuredContent` for chainable workflows.

---

## Security Features

### PII Detection & Auto-Masking

Sensitive data is automatically detected and masked before being returned to the AI:

| Pattern | Example | Masked Output |
|---------|---------|---------------|
| SSN | `123-45-6789` | `***-**-6789` |
| Credit Card | `4111-1111-1111-1234` | `****-****-****-1234` |
| Email | `john@company.com` | `j***@c****.com` |
| Phone | `(555) 123-4567` | `(***) ***-4567` |

### Query Audit Logging

Every query is logged with:
- Timestamp and unique query fingerprint
- Row count and execution duration
- PII detection results
- Policy violation flags

Logs support compliance requirements (GDPR, HIPAA, SOC2).

### Configurable Access Policies

Define granular policies in `config/policies.yaml`:

```yaml
tables:
  - name: Customers
    columns:
      - name: SSN
        action: block      # Never return
      - name: Email
        action: mask       # Apply PII masking
      - name: Revenue
        action: allow      # Return as-is
```

---

## Installation

### Prerequisites

**For Desktop Connectivity:**
- Windows 10/11
- Power BI Desktop installed
- Python 3.10+
- ADOMD.NET (included with Power BI Desktop or SSMS)

**For Cloud Connectivity (Optional):**
- Azure AD App Registration with `Dataset.Read.All` and `Workspace.Read.All`
- Premium Per User (PPU) or Premium Capacity workspace
- XMLA endpoint enabled

### Quick Start

```bash
# Clone the repository
git clone https://github.com/sulaiman013/powerbi-mcp.git
cd powerbi-mcp

# Install dependencies
pip install -r requirements.txt

# (Optional) Configure cloud credentials  (Windows)
copy .env.example .env
# Edit .env with your Azure AD credentials
```

### Configure Claude Desktop

Add to `%APPDATA%\Claude\claude_desktop_config.json`:

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

Restart Claude Desktop to activate.

### Run with Docker (cross-platform, offline subset)

The Docker image runs the platform-independent tools (PBIP/TMDL/PBIR editing, Best
Practice Analyzer, AI-readiness, model analysis, security, resources/prompts) on any
OS with no .NET. Live Power BI Desktop / XMLA / TOM connectivity still requires
Windows + ADOMD.NET.

```bash
docker build -t powerbi-mcp .
# stdio MCP server; mount your PBIP project at /work
docker run --rm -i -v /path/to/MyReport:/work powerbi-mcp
```

---

## Usage Examples

### ⚠️ IMPORTANT: How to Rename Tables/Columns/Measures

**DO NOT** use TOM-based tools (`batch_rename_tables`, `batch_rename_columns`, `batch_rename_measures`) for renaming. They break report visuals!

**ALWAYS** use PBIP tools:
1. Close Power BI Desktop (if open)
2. `pbip_load_project` - Load the .pbip file
3. `pbip_rename_tables` / `pbip_rename_columns` / `pbip_rename_measures` - Do the rename
4. Reopen Power BI Desktop to see changes

### Basic Desktop Workflow

```
User: "Discover Power BI Desktop instances"
User: "Connect to Power BI Desktop"
User: "What tables are in my model?"
User: "Run DAX: EVALUATE TOPN(10, Sales)"
```

### Safe Bulk Rename with PBIP (✅ Correct Way)

```
User: "Load PBIP project from C:/Projects/SalesReport"
User: "Rename tables: Salesforce_Data to Leads Sales Data"
```

**What happens automatically:**
1. Table declaration updated: `table Salesforce_Data` → `table 'Leads Sales Data'`
2. DAX references quoted: `Salesforce_Data[Amount]` → `'Leads Sales Data'[Amount]`
3. Function calls fixed: `COUNTROWS(Salesforce_Data)` → `COUNTROWS('Leads Sales Data')`
4. Visual files updated: All `"Entity": "Salesforce_Data"` → `"Entity": "Leads Sales Data"`
5. Cultures/Q&A updated: All `"ConceptualEntity"` references fixed

### Diagnose Issues (When Needed)

```
User: "Load PBIP project from C:/Projects/SalesReport"
User: "Scan for broken references"
# Shows: Tables in model vs tables referenced in visuals

User: "Validate project"
# Shows: Any TMDL syntax errors or quoting issues
```

Use repair tools only if a rename was done **outside** the MCP server (e.g., via TOM API or Power BI Desktop UI).

### RLS Testing

```
User: "List RLS roles in my model"
User: "Set RLS role to 'Sales_East'"
User: "Run DAX: EVALUATE Sales"  # Results filtered by role
User: "Clear RLS role"
```

---

## Project Structure

```
powerbi-mcp/
├── src/
│   ├── server.py                    # MCP server (45 tools + resources/prompts/completion)
│   ├── powerbi_desktop_connector.py # Desktop + RLS
│   ├── powerbi_xmla_connector.py    # Cloud XMLA
│   ├── powerbi_rest_connector.py    # REST API
│   ├── powerbi_tom_connector.py     # TOM write operations + relationships
│   ├── powerbi_pbip_connector.py    # PBIP file editing (transactional)
│   ├── model_analysis.py            # BPA + AI-readiness engine (pure Python)
│   └── security/
│       ├── pii_detector.py          # PII detection
│       ├── audit_logger.py          # Audit logging
│       ├── access_policy.py         # Policy engine (enforced)
│       └── security_layer.py        # Unified security
├── config/
│   └── policies.yaml                # Access policies
├── test_*.py                        # Assert-based tests (run without Power BI)
├── AGENTS.md / CLAUDE.md            # Agent guidance
├── Dockerfile / requirements-core.txt  # Cross-platform offline image
├── .env.example
├── requirements.txt
└── README.md
```

---

## Limitations

| Limitation | Workaround |
|------------|------------|
| Live connectivity is Windows only | ADOMD.NET / TOM require Windows. The **offline subset** (PBIP editing, BPA, AI-readiness, analysis, security) runs cross-platform via Docker. |
| TOM renames break visuals | Use the PBIP tools for safe bulk renames (they update the report layer too) |
| Cloud requires Premium | XMLA endpoints need a PPU/Premium workspace |
| Deep server timings | `analyze_query_performance` gives duration + hints; use DAX Studio for storage-engine vs formula-engine breakdown |

---

## Roadmap

### Completed (V2)
- [x] Power BI Desktop connectivity
- [x] Cloud XMLA integration
- [x] Security layer (PII, audit, policies)
- [x] RLS testing support
- [x] TOM write operations
- [x] PBIP safe editing

### Completed (V3 - 2026 agentic enhancements)
- [x] DAX validate-before-commit loop + impact analysis
- [x] Atomic TOM transactions (begin/commit/rollback)
- [x] Best Practice Analyzer + AI-readiness scoring
- [x] VertiPaq-style storage + query performance analysis
- [x] Relationship management (create/delete)
- [x] Transactional, atomic, encoding-faithful PBIP renames
- [x] Enforced column-level access policies + secret redaction
- [x] Modern MCP surface (annotations, structured output, resources, prompts, completion)
- [x] Docker image for the cross-platform offline subset

### Planned (V4)
- [ ] Remote HTTP transport with Microsoft Entra OAuth (today: use the official remote Power BI MCP server for cloud auth)
- [ ] Open-source / local LLM integration (Ollama, LM Studio)
- [ ] BPA auto-fix and custom team rule packs (YAML)
- [ ] Deeper VertiPaq (per-column cardinality) and server-timings capture

---

## Author

<table>
<tr>
<td width="150">
<img src="https://github.com/sulaiman013.png" width="120" style="border-radius: 50%"/>
</td>
<td>

**Sulaiman Ahmed**
*Data Analytics Engineer & Microsoft Certified Professional*

Passionate about bridging the gap between AI and enterprise data platforms. This project combines expertise in Power BI semantic modeling, Python development, and the emerging Model Context Protocol ecosystem.

[![GitHub](https://img.shields.io/badge/GitHub-sulaiman013-181717?style=flat-square&logo=github)](https://github.com/sulaiman013)
[![Portfolio](https://img.shields.io/badge/Portfolio-sulaiman--ahmed.lovable.app-blue?style=flat-square&logo=google-chrome)](https://sulaiman-ahmed.lovable.app)

</td>
</tr>
</table>

---

## Contributing

Contributions are welcome! Please read our contributing guidelines before submitting PRs.

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

---

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## Acknowledgments

- [Model Context Protocol](https://modelcontextprotocol.io) by Anthropic
- Microsoft's TOM and TMDL documentation
- The Power BI community for insights on PBIP format

---

<p align="center">
  <sub>Built with passion for the Power BI and AI community</sub>
  <br>
  <a href="https://github.com/sulaiman013/powerbi-mcp/issues">Report Bug</a> | <a href="https://github.com/sulaiman013/powerbi-mcp/issues">Request Feature</a>
</p>
