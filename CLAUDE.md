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
- Build report pages with `pbir_apply_spec` (dry run first; see docs/REPORT_SPEC.md).

## Developer notes

- Source: `src/` (server.py + connectors + the PBIP engine + generators + `security/`).
  `docs/ARCHITECTURE.md` has the file map.
- Read and rewrite DAX only through `src/dax_tokens.py` (never a regex over raw text), and write
  files only through `src/safe_io.py` (atomic; keeps encoding, line endings and file-name case;
  `FileTransaction` for more than one file).
- Tests: `tests/test_*.py` are assert-based and run without Power BI (pure logic directly, the
  server and connectors through fakes). Run them after changes: `python run_tests.py`
  (or `python tests/test_<name>.py`). A check must fail the suite: no printed FAIL with exit 0.
- Every tool registers in three places in `src/server.py` (`build_tools` +
  `_build_tool_dispatch` + `_build_tool_annotations`) and in `docs/TOOLS.md`; parity tests
  guard both. A tool not annotated read-only is refused in read-only mode by default.
- A tool that runs caller-supplied DAX must go through `_run_guarded_query`.
- Do not write `\uXXXX` escapes for invisible characters in source (use `chr(0x...)`).
