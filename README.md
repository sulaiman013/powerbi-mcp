# Power BI MCP Server

[![MCP](https://img.shields.io/badge/MCP-Compatible-blue)](https://modelcontextprotocol.io)
[![Python](https://img.shields.io/badge/Python-3.10+-green)](https://www.python.org)
[![Platform](https://img.shields.io/badge/Platform-Windows-lightgrey)](https://github.com/sulaiman013/powerbi-mcp)
[![License](https://img.shields.io/badge/License-MIT-yellow)](LICENSE)

A Model Context Protocol (MCP) server that enables AI assistants like Claude Desktop and Claude Code to interact with Power BI semantic models in the cloud. Features **dynamic table discovery** via XMLA endpoints — no hardcoded schemas required.

**Version:** 1.0.0
**Developer:** [Sulaiman Ahmed](https://github.com/sulaiman013)

---

## Disclaimer

> **This is an independent, community-developed tool and is not affiliated with, endorsed by, or connected to Microsoft Corporation or Anthropic.**
>
> Power BI, Azure, and related trademarks are property of Microsoft Corporation. This project is provided "as-is" without warranty.

---

## Privacy

This MCP server:
- Runs **entirely locally** on your machine
- Collects **zero telemetry or usage data**
- Communicates only with Power BI Service APIs using your provided credentials

**Note:** AI assistants (Claude Desktop, etc.) have their own privacy policies independent of this MCP server. Review your AI platform's privacy policy for data handling practices.

---

## Features

| Feature | Description |
|---------|-------------|
| **Dynamic Discovery** | Automatically discovers tables, columns, and relationships via XMLA |
| **Cloud Connectivity** | Connects to Power BI Service semantic models (PPU/Premium workspaces) |
| **7 Integrated Tools** | List workspaces, datasets, tables, columns; execute DAX; get model info |
| **Service Principal Auth** | Secure authentication via Azure AD app registration |
| **No Hardcoding** | Schema discovered at runtime — works with any dataset |

---

## Prerequisites

### 1. Azure AD App Registration
1. Create an App Registration in [Azure Portal](https://portal.azure.com)
2. Generate a client secret
3. Grant API permissions:
   - `Power BI Service > Dataset.Read.All`
   - `Power BI Service > Workspace.Read.All`
4. Add the Service Principal to your Power BI workspace as **Member** or **Admin**

### 2. Power BI Workspace
- **Premium Per User (PPU)** or **Premium Capacity** workspace
- XMLA endpoint enabled: `Workspace Settings > Premium > XMLA Endpoint > Read/Write`

### 3. Windows Dependencies
- **Python 3.10+**
- **ADOMD.NET Client Library** — Install via one of:
  - [SQL Server Management Studio (SSMS)](https://docs.microsoft.com/sql/ssms/download-sql-server-management-studio-ssms)
  - Microsoft ADOMD.NET NuGet package
  - [Analysis Services Client Libraries](https://docs.microsoft.com/sql/analysis-services/client-libraries)

---

## Installation

### Step 1: Clone and Install Dependencies

```bash
git clone https://github.com/sulaiman013/powerbi-mcp.git
cd powerbi-mcp
pip install -r requirements.txt
```

### Step 2: Configure Environment

Copy `.env.example` to `.env` and add your Azure AD credentials:

```env
TENANT_ID=your-azure-tenant-id
CLIENT_ID=your-app-client-id
CLIENT_SECRET=your-app-client-secret
```

### Step 3: Configure Claude Desktop

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

### Step 4: Restart Claude Desktop

Restart Claude Desktop to load the MCP server. The Power BI tools will appear in Claude's tool list.

---

## Available Tools

| Tool | Description |
|------|-------------|
| `list_workspaces` | Lists all Power BI workspaces accessible to the Service Principal |
| `list_datasets` | Lists all datasets/semantic models in a workspace |
| `list_tables` | Dynamically discovers all tables in a dataset via XMLA |
| `list_columns` | Gets column metadata (name, type, description) for a table |
| `execute_dax` | Executes a custom DAX query against a dataset |
| `get_sample_data` | Retrieves sample rows from a table |
| `get_model_info` | Returns comprehensive model metadata using INFO.VIEW functions |

### Tool Parameters

| Tool | Required Parameters | Optional |
|------|---------------------|----------|
| `list_workspaces` | — | — |
| `list_datasets` | `workspace_id` | — |
| `list_tables` | `workspace_name`, `dataset_name` | — |
| `list_columns` | `workspace_name`, `dataset_name`, `table_name` | — |
| `execute_dax` | `workspace_name`, `dataset_name`, `dax_query` | — |
| `get_sample_data` | `workspace_name`, `dataset_name`, `table_name` | `num_rows` (default: 5) |
| `get_model_info` | `workspace_name`, `dataset_name` | — |

### get_model_info Details

Returns comprehensive semantic model metadata by executing:
- `INFO.VIEW.TABLES()` — All tables in the model
- `INFO.VIEW.COLUMNS()` — All columns grouped by table
- `INFO.VIEW.MEASURES()` — All measures with expressions
- `INFO.VIEW.RELATIONSHIPS()` — All relationships with cardinality

---

## Usage Examples

Once configured, ask Claude:

```
"List my Power BI workspaces"
"What datasets are in the Sales workspace?"
"Show me the tables in the Sales Analytics dataset"
"What columns does the Customers table have?"
"Get model info for the Sales Analytics dataset"
"Run this DAX: EVALUATE TOPN(10, Sales)"
```

---

## Architecture

```
powerbi-mcp/
├── src/
│   ├── server.py                  # MCP server with 7 tool handlers
│   ├── powerbi_rest_connector.py  # REST API for workspaces/datasets
│   └── powerbi_xmla_connector.py  # XMLA connector for tables/columns/DAX
├── .env.example                   # Environment template
├── requirements.txt               # Python dependencies
├── LICENSE                        # MIT License
└── README.md
```

### How It Works

1. **REST API** (`powerbi_rest_connector.py`): Lists workspaces and datasets via Power BI REST API
2. **XMLA Connector** (`powerbi_xmla_connector.py`): Connects via `powerbi://` protocol for schema discovery and DAX execution using pyadomd/ADOMD.NET
3. **MCP Server** (`server.py`): Exposes tools to Claude via Model Context Protocol

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| **ADOMD.NET not found** | Install SQL Server Management Studio (SSMS). The connector auto-searches common paths. |
| **Connection fails** | Verify XMLA endpoint is enabled in workspace settings. Check Service Principal has Member/Admin role. |
| **Authentication errors** | Verify tenant_id, client_id, client_secret. Ensure API permissions are admin-consented. |
| **Dataset not found** | Dataset names are case-sensitive. Use exact name from Power BI Service. |

---

## Limitations

- **Windows only** — Requires ADOMD.NET which is Windows-specific
- **Premium/PPU required** — XMLA endpoints only available on Premium workspaces
- **Read operations** — This version focuses on read/query operations

---

## License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.

---

## Support

- **Issues:** [GitHub Issues](https://github.com/sulaiman013/powerbi-mcp/issues)
- **Developer:** [Sulaiman Ahmed](https://github.com/sulaiman013)
