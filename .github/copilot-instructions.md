# PowerBI MCP Server

This project is a Model Context Protocol (MCP) server that provides AI assistants with access to Power BI datasets and metadata. It enables querying Power BI data models through ADOMD.NET and returns structured information about tables, relationships, and data.

## Repository Structure

- `/src`: Main server implementation (`server.py`)
- `/tests`: Test suite organized by layers (unit, local, integration)
- `/scripts`: Setup and utility scripts including `install_dotnet_adomd.sh`
- `/docs`: Documentation for setup, troubleshooting, and integration testing

## Technology Stack

- **Python 3.11+** for the MCP server implementation
- **ADOMD.NET** for Power BI connectivity and data model querying
- **pytest** for testing with layered test structure
- **Model Context Protocol (MCP)** for AI assistant integration

## Development Workflow

1. **Development-First Approach**: Always create separate `dev_*.py` files to test new functionality with live Power BI data before modifying main server code
2. **Test Integration**: Verify functionality works with real datasets before integrating into `server.py`
3. **Full Test Validation**: Run complete test suite after any changes

## Code Quality Standards

- **Mandatory code formatting before commits**: Run `black --check --diff src/ tests/ --line-length=120`
- **Import sorting**: Use `isort --check-only src/ tests/ --profile=black`
- **Linting**: Pass `flake8 src/ tests/ --config=.flake8`
- **GitHub Actions will fail if formatting checks don't pass**

## Testing Requirements

- **All tests must pass before commits**: `pytest -v`
- **Layered test structure**: unit tests (mocked), local tests (server startup), integration tests (external services)
- **Never skip tests due to implementation problems** - tests should pass or fail, not skip
- **Add regression tests when changing data structures or formats**

## Environment Setup

Supports three environments with consistent functionality:
- **Local Development** (Windows): Uses `.env` configuration and manual setup
- **Docker** (Linux): Uses `install_dotnet_adomd.sh --system` for systemwide .NET
- **Codespaces** (Linux): Uses `install_dotnet_adomd.sh --user` for user-local .NET

All environments use the same installation scripts and `requirements.txt` for consistency.
