# Testing

The suites in `tests/` are **assert-based scripts** (no pytest required). Each prints
`[PASS]`/`[FAIL]` per check and exits non-zero on any failure. They run **without Power BI,
ADOMD or .NET**: pure logic is tested directly, and the server and live connectors are driven
through fakes.

## Run everything
```bash
python run_tests.py              # all 50 suites
python run_tests.py rename dax   # only suites whose file name contains a given word
```
Run one suite:
```bash
python tests/test_security_enforcement.py
```

The runner fails a suite that exits non-zero **or** prints a line starting with `[FAIL]`, so a
suite that reports a failed check but still exits 0 cannot count as green. CI
(`.github/workflows/tests.yml`) runs the same command on Windows and Linux for Python 3.10,
3.12 and 3.13 with only the cross-platform dependencies installed.

The runner starts each suite in a clean environment: this server's own settings
(`POWERBI_MCP_*`, `ENABLE_*`, the cloud credentials, the DLL paths) are removed and `.env`
loading is switched off, so a result does not depend on whose machine ran it. Suites that
build a server write their audit entries to a temporary folder, never to the repository's
`logs/`. A suite started directly (`python tests/test_x.py`) sees your shell's environment.

## What each suite covers

### Shared foundations
| Suite | Covers |
|-------|--------|
| `test_dax_tokens.py` | The DAX tokenizer every module shares: lossless lexing, escaped names, reference extraction, string/comment-safe table, column and measure renames, and the query-versus-command classifier |
| `test_safe_io.py` | Atomic writes that keep BOM, newline style (mixed endings included) and file-name case; the all-or-nothing file transaction and its restore on failure |

### Server
| Suite | Covers |
|-------|--------|
| `test_server_boundary.py` | The request boundary through the real MCP handler: flags, the statement guard, the read-only gate, structured results, secret redaction, one guarded path for every DAX-running tool, measure validation with sibling and pending references, the tenant scan (batches, resume, cache), transactions, the metadata cache |
| `test_registry_parity.py` | Tool list, dispatch map, and annotation map contain exactly the same names; write-gate names real tools |
| `test_docs_parity.py` | The docs against the code: every tool documented once with its real safety class, tool and suite counts, every environment variable, the version, the generated spec reference |
| `test_bundle_a.py` | `validate_dax`, validate-before-commit, transaction defer/commit/rollback |
| `test_bundle_b.py` | BPA / AI-readiness / storage / query-perf / data-dictionary handlers (fake model) |
| `test_bundle_c.py` | MCP resources, prompts, completion, structured output |
| `test_bundle_d.py` | Relationship create/delete + transaction behavior |
| `test_wave1c.py` | Refresh-error classifier, governance reference resources, read-only mode |
| `test_wave2.py` | PBIR reference scanner, refresh_doctor, find_unused_objects, impact_analysis, rls_test_harness |
| `test_wave3.py` | Scanner summary, cross_workspace_lineage (cached), fleet refresh monitor, usage analytics |
| `test_wave_extras.py` | Audit chain tamper and deletion detection, DAX regression runner |

### Offline PBIP: renames, parsing, authoring
| Suite | Covers |
|-------|--------|
| `test_pbip_rename_model.py` | Renames in TMDL: scoping to the owning table, DAX through the tokenizer, Power Query references, structural properties, names that used to break regex templates |
| `test_pbip_rename_report.py` | Renames in the report on real PBIR shapes: visuals, mobile layouts, page/report filters, bookmarks, legacy `report.json`, Deneb specs |
| `test_pbip_rename_transaction.py` | All-or-nothing behaviour: refusals, failures that change nothing, batches as one transaction, backups, encoding and line endings |
| `test_pbip_parsers.py` | TMDL scanner, position-preserving JSON parser, M tokenizer, project discovery, the read-only diagnostic tool |
| `test_phase3_rename_safety.py` | Byte-faithful writes through the rename path, including mixed line endings |
| `test_dax_quoting_fixes.py` | TMDL/DAX quoting helpers, the quoting validator and fixer on real TMDL shapes |
| `test_tmdl_authoring.py` | TMDL emitters, connector round-trips, filename safety, idempotent measure upsert |
| `test_pbir_authoring.py` | PBIR emit/parse symmetry, add page/visual/bind, aggregation by visual type, nativeQueryRef fidelity |
| `test_pbix_tools.py` | PBIX inspect/extract: thin/thick, UTF-16-LE layout decode, Zip-Slip guard, streaming and size cap |
| `test_desktop_bridge.py` | Bridge JSON-RPC framing, fake-pipe client, timeouts, id matching, discovery, page resolution, reload guard |

### Report as code
| Suite | Covers |
|-------|--------|
| `test_pbir_builders.py` | PBIR emitters (text, shapes, images, buttons, slicers, groups, bookmarks, themes, HTML Content, Deneb, custom visuals) against shapes taken from real files |
| `test_report_spec.py` | The declarative spec: validation, idempotence (twice applied gives identical bytes), prune, dry run, nothing written on failure, unmanaged pages untouched |
| `test_report_audit.py` | Report audit: structure, design and governance findings; robustness against wrong-typed JSON |
| `test_payload_measures.py` | Row-packing measures and their JavaScript parsers; escape chains decode correctly; budget estimates |
| `test_js_app_measure.py` | JavaScript-app assembly: the string/template/regex-aware minifier, build refusals, re-evaluation guard, sizes (Node-dependent checks skip cleanly without Node) |
| `test_html_measures.py` | HTML measure generators: escaping, blank handling, parameter validation |
| `test_model_embed.py` | CSV embedding: deflate/base64 round-trip, partition rewrite that leaves the rest of the file byte for byte, parameter removal, refusals |
| `test_publish_stage.py` | Publish staging: `byConnection` binding, quoting, display name, source project untouched |

### Security
| Suite | Covers |
|-------|--------|
| `test_security_enforcement.py` | Access policies fire (BLOCK/MASK/HASH/REDACT/NUMERIC_MASK), `Table[Column]` parsing, query blocking |
| `test_security_config.py` | Fail closed: a missing, unparseable or invalid policy file locks the layer; flag parsing; the shipped config's matching |
| `test_security_query_rules.py` | Aliases get the referenced column's action, TOCSV/TOJSON refusals, measure expansion, table rules (`max_rows`, `require_filter`, `aggregate_only`), wildcards |
| `test_security_pii.py` | PII detection: configured types and strategies, value validation (Luhn, SSN ranges), no false positives on dates and ordinary text, linear time |
| `test_security_audit_chain.py` | Sequence numbers, checkpoint, chain across rotation, Windows rotation, three real processes writing one valid chain, stored query text |
| `test_security_redaction.py` | Secret redaction for connection strings, JSON, bearer tokens and SAS, without eating the rest of the text |
| `test_security_audit_fixes.py` | Regression set: redaction, audit-chain tamper + HMAC, error scrubbing, bounded reference extraction, PII summary |

### Live connectors (through fakes)
| Suite | Covers |
|-------|--------|
| `test_connectors_adomd.py` | Connection-string quoting, the query-only gate, connections always closed, timeouts, row limits, value conversion, RLS role switching, redacted errors |
| `test_connectors_rest.py` | Token lifecycle and refresh, 401 retry, throttling with `Retry-After`, paging, GUID validation, Scanner helpers |
| `test_connectors_tom.py` | Connector-owned transactions, failed commit and failed save behaviour, all-or-nothing batch create, tokenizer-based rename fix-ups |
| `test_connectors_governance.py` | RLS state (has / none / unknown) in the scan summary, refresh error classification by specificity |
| `test_adomd_loader.py` | ADOMD.NET / TOM discovery: environment overrides, ranking by the active .NET runtime, fall-through when a load fails |

### Analysis, lint and generators
| Suite | Covers |
|-------|--------|
| `test_model_analysis.py` | 33 BPA rules, AI-readiness, data dictionary, model diff over every property with rename detection, DAX test verdicts |
| `test_dax_lint.py` | DAX rules fire and stay silent on clean DAX; names holding comment markers; line numbers; the function catalog; mechanical rewrites |
| `test_dax_generator.py` | Measure-suite templates (time intelligence incl. fiscal year, ratios, ranks, stats); blank-safe comparisons; every measure has a format; generated DAX is lint-clean |
| `test_star_schema.py` | Fact/dim/date/bridge/measure-table classification, cardinality, scoring that is not generous to flat models |
| `test_naming_audit.py` | Naming plan: casing rules, acronyms, collisions flagged, plan order |
| `test_svg_measures.py` | SVG micro-visual generators: blank handling, XML and URI escaping, parameter validation |
| `test_bpa_authoring.py` | Custom BPA rule validation + rule-source audit |

## Checks against real reports
Beyond the suites, three parts were run against scratch copies of real, complex report projects
(HTML Content, Deneb and custom visuals, bookmarks, pop-ups, synced slicers; 124 and 250
visuals):
- **Renames.** A column, a measure and a table renamed in one project left zero binding errors,
  zero stale references (the Deneb specs followed), every JSON parseable, and every untouched
  file byte-identical. Eight renames there and back on the second project left all 303 files
  byte-identical, including one with mixed line endings.
- **Report spec.** A dry run touched nothing; a spec with a bad field was refused before any
  write; applying wrote the planned files; re-applying was a no-op; pruning restored the project
  byte for byte; every generated visual used only JSON shapes already present in the real files.
- **Report audit.** Eleven real reports (1 to 457 visuals each) audited without an exception,
  with no error-level finding on these working reports.

These runs are not part of the repository's tests, which use synthetic fixtures only.

## What is not verified by execution
- **Generated DAX and M** (measure suites, payload, HTML and SVG measures, embedded partitions)
  are checked for shape, escaping and lint, not evaluated by an engine. Use `validate_dax`.
- **Generated reports** are not opened in Power BI Desktop by the suite. Use `bridge_reload` and
  `bridge_screenshot`.
- **Live Desktop paths** were verified against a running Power BI Desktop in 2026-07 (ADOMD
  queries, TOM batch writes, the star-schema audit, the Desktop Bridge). The connectors and the
  rename engine have been rewritten since and were not re-run live.
- **Cloud paths** (XMLA/REST/Admin) are tested with fakes and doc-verified against Microsoft
  Learn; end-to-end verification needs a real tenant.

## Engine-level verification of generated TMDL (2026-07)
A full project generated by `pbip_add_measures` + `pbip_create_date_table` (fiscal variant) +
`pbip_add_hierarchy` + `pbip_add_calculation_group` was deserialized with
`Microsoft.AnalysisServices.Tabular.TmdlSerializer` (the AMO code path Power BI Desktop uses to
open a PBIP), and every property round-tripped. The same structures were created on a live
Desktop model via TOM, calculate-refreshed and evaluated, then removed. This predates the
3.8.0 changes to those emitters (measure upsert, deterministic lineage tags).

## Adding a tool (keep the registries and the docs in sync)
A tool is registered in three places in `src/server.py`: the `Tool` spec in `build_tools`,
`_build_tool_dispatch` (the handler), and `_build_tool_annotations` (the safety hints).
`tests/test_registry_parity.py` asserts all three contain exactly the same names, and
`tests/test_docs_parity.py` asserts the tool is in `docs/TOOLS.md` with the right safety mark
and that the counts quoted in the docs are current. A tool that is not annotated read-only is
refused in read-only mode by default.
