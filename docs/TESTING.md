# Testing

The suites in `tests/` are **assert-based scripts** (no pytest required). Each prints
`[PASS]`/`[FAIL]` per check and exits non-zero on any failure. They run **without Power BI
or ADOMD** — pure logic is tested directly and live connectors are mocked.

## Run everything
```bash
python run_tests.py
```
Run one suite:
```bash
python tests/test_security_enforcement.py
```

## What each suite covers
| Suite | Covers |
|-------|--------|
| `test_dax_quoting_fixes.py` | TMDL/DAX quoting + table/column rename cascade (pre-existing) |
| `test_security_enforcement.py` | Access policies fire (BLOCK/MASK/HASH/REDACT/NUMERIC_MASK), `Table[Column]` parsing, query blocking |
| `test_bundle_a.py` | `validate_dax`, validate-before-commit, TOM transaction defer/commit/rollback |
| `test_model_analysis.py` | BPA rules, AI-readiness, data dictionary, model diff, DAX test verdicts |
| `test_bundle_b.py` | BPA / AI-readiness / storage / query-perf / data-dictionary handlers (mocked model) |
| `test_bundle_c.py` | MCP resources, prompts, completion, structured output |
| `test_phase3_rename_safety.py` | Atomic + BOM/CRLF-faithful writes; transactional rollback on mid-cascade failure |
| `test_bundle_d.py` | Relationship create/delete + transaction behavior |
| `test_wave1c.py` | Refresh-error classifier, governance reference resources, read-only mode |
| `test_wave2.py` | PBIR reference scanner, refresh_doctor, find_unused_objects, impact_analysis, rls_test_harness |
| `test_wave_extras.py` | Tamper-evident audit chain (tamper + deletion detection), DAX regression runner |
| `test_wave3.py` | Scanner summary, cross_workspace_lineage (cached), fleet refresh monitor, usage analytics |

## Coverage philosophy
- **Offline / pure logic** (security, PBIP rename, model analysis, diff, refresh
  classification, governance parsing) — fully unit/mock tested here.
- **Live paths** (Desktop/XMLA/TOM/REST/Admin) — orchestration is mock-tested and the API
  contracts are doc-verified against Microsoft Learn; **end-to-end verification needs a
  Windows + Power BI / Fabric environment** (see the live smoke-test checklist in the README).

## Adding a tool (keep the registry in sync)
A tool must be registered in three places in `src/server.py`: `handle_list_tools` (the `Tool`
spec), `_build_tool_dispatch` (the handler), and `_build_tool_annotations` (the safety hints).
A parity check (in the verification scripts) asserts all three contain the same names — keep
them aligned or `list_tools`/`call_tool` will drift.
