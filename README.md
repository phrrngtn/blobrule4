# blobrule4

> **Note:** This code is almost entirely AI-authored (Claude, Anthropic), albeit under close human supervision, and is for research and experimentation purposes. Successful experiments may be re-implemented in a more coordinated and curated manner.

Schema survey and evolution tracking for SQL Server, PostgreSQL, and DuckDB databases.

Uses the blob\* extension family (blobodbc, blobtemplates) to scrape catalog metadata via ODBC, maintain transaction-time state tables (TTST) of schema snapshots, and produce RFC 6902 JSON Patch diffs for change detection.

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                        blobrule4                              │
│                                                              │
│  catalog/          Dialect-specific SQL (YAML)               │
│  sql/              Registry, TTST DDL, polling scripts       │
│  python/blobrule4/ Sampler, intern pipeline, SA models       │
│                                                              │
│  Depends on:                                                 │
│    blobodbc   (bo_*) — ODBC query execution, catalog fns     │
│    blobtemplates (bt_*) — json_nest, json_from_diff,         │
│                           text_diff, template rendering      │
└──────────────────────────────────────────────────────────────┘
```

## Pipeline

1. **Sample** — `python -m blobrule4.sampler` scrapes metadata from registered ODBC dataservers using dialect-specific catalog queries
2. **Intern** — `python -m blobrule4.intern` nests payloads, diffs against current snapshots, stores reverse patches
3. **Query** — `blobodbc.changelog` view shows human-readable change log derived from the patch chain
4. **Reconstruct** — apply reverse patches backward to reconstruct any historical schema state

## Sample Log Tables

One row per (dataserver, catalog, schema, sample\_time) per kind:

| Table | Content |
|---|---|
| `table_sample_log` | Tables with row counts, triggers, temporal type |
| `column_sample_log` | Columns with types, defaults, identity, computed |
| `primary_key_sample_log` | PK constraints with column ordinals |
| `foreign_key_sample_log` | FK constraints with actions and trust state |
| `index_sample_log` | Indexes with columns, includes, filter predicates |
| `trigger_sample_log` | Triggers with firing order and source text |
| `callable_sample_log` | Functions, procedures with signatures and source |

## TTST Tables

| Table | Purpose |
|---|---|
| `schema_snapshot` | Current nested JSON per (dataserver, catalog, schema, kind) |
| `schema_snapshot_patch` | Reverse RFC 6902 patches for history reconstruction |

## Catalog Queries

Dialect-specific SQL in `catalog/{sqlserver,postgresql,information_schema}/`:
- `tables.yml`, `columns.yml`, `primary_keys.yml`, `foreign_keys.yml`
- `indexes.yml`, `triggers.yml`, `callables.yml`

Each YAML has base SQL with optional WHERE fragments assembled dynamically per non-NULL parameters.

## Usage

```bash
cd python

# Seed dataserver registry
uv run --with duckdb --with sqlalchemy --with duckdb-engine \
    python -m blobrule4.seed /path/to/survey.duckdb

# Sample a dataserver
uv run --with duckdb --with sqlalchemy --with duckdb-engine \
    python -m blobrule4.sampler /path/to/survey.duckdb --dataserver sqlserver_docker --schema dbo

# Intern samples into TTST
uv run --with duckdb --with sqlalchemy --with duckdb-engine \
    python -m blobrule4.intern /path/to/survey.duckdb
```

## Dependencies

- **blobodbc** DuckDB extension (bo\_\* functions)
- **blobtemplates** DuckDB extension (bt\_\* functions)
- Python: duckdb, sqlalchemy, duckdb-engine, pyyaml
