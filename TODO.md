# blobrule4 TODO — Resolution Sieve

Status as of 2026-03-21.

## Done

- [x] Catalog expansion: 16 new YAML files (8 kinds × SS/PG/I_S)
- [x] Evidence layer: MetadataFact model (tier-tagged, append-only, provenance)
- [x] MetaData generator: snapshot → SA Table/Column/PK/FK/Index
- [x] SchemaCollection: evidence-driven federation with inferred cross-schema joins
- [x] Composable relation builders (Layers 0–3)
- [x] Structural analysis Phase 1 (9 analyses, set-based SQL)
- [x] Regex domain patterns (38 patterns, dual-mode probing)
- [x] Performance: vectorized per-pattern scans (233× faster than cross-join)

## Next up

- [ ] Connect to PG (rule4_test, Unix socket), run structural analysis on real catalog data
- [ ] Run regex probe against real column values from PG-scraped schemas
- [ ] End-to-end validation: structural facts + regex probing on non-mock data

## Planned (still valid)

- [ ] Structural Phase 2: column name clustering via blobembed (within + across table)
- [ ] Structural Phase 3: schema-level topic detection, view dependencies
- [ ] Schema evolution signals from TTST patch chain (pure SQL, no new deps)
- [ ] Value sampling: collect distinct values per column via blobodbc
- [ ] sieve.py orchestrator: sequence passes, resumability, rollup to column_resolution
- [ ] Confidence aggregation / weighted voting across passes
- [ ] MaterializedCTE SQLAlchemy compiler extension (when needed)

## Deferred (design captured, not blocking)

- [ ] Refactor structural.py to SA expression API (current SQL strings work fine)
- [ ] OpenAPI → virtual tables (adapters, reification pipeline)
- [ ] Tables-everywhere abstraction (canonical vocabulary, source adapters)
- [ ] Back-testing: regenerate known values from candidate queries
