# CLAUDE.md

Agent guidance for this repository lives in [AGENTS.md](AGENTS.md). Read it before
driving Power BI tasks through this MCP server.

Quick reminders:
- Rename with the `pbip_*` tools, never the deprecated TOM `batch_rename_*` tools.
- `validate_dax` before committing measures; batch model edits inside a
  `tom_begin_transaction` / `tom_commit_transaction` scope.
- `scan_measure_dependencies` + `pbip_scan_broken_refs` before destructive changes.
- Read the `powerbi://desktop/schema` resource to ground DAX in real object names.
- After offline PBIP/PBIR edits, hot-reload the open Desktop with `bridge_reload`
  (never over unsaved changes) instead of asking the user to close and reopen.

## Developer notes

- Source: `src/` (server.py + connectors + `security/` + `model_analysis.py`).
- Tests: `tests/test_*.py` are assert-based and run without Power BI (pure-Python
  paths). Run them after changes: `python run_tests.py` (or `python tests/test_<name>.py`).
- Every tool registers in one place (`_build_tool_dispatch` + `_build_tool_annotations`
  + `handle_list_tools` in `src/server.py`); keep the three in sync (a parity test
  guards this).
