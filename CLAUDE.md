# CLAUDE.md

## Project Context

blobrule4 maintains concise, expressive manifestations of database schema definitions using the blob\* extension family. It scrapes catalog metadata from ODBC-accessible databases, tracks schema evolution via TTST (transaction-time state tables) with RFC 6902 JSON Patch diffs, and produces human-readable changelogs.

The target databases are SQL Server, PostgreSQL, DuckDB, and SQLite. Each has its own catalog query dialect stored as YAML files in `catalog/`.

## Development Environment

- **Python**: Always use `uv run python`, never bare `python3` or `python`. Use `uv` for all dependency management.
- **Inline scripts**: Write scripts to a temp file with the Write tool, then execute with `uv run python /tmp/script.py`. Never use heredoc-in-bash or shell-escaped Python one-liners.
- **Shell**: Prefer HERE documents (`<<'EOF'`) for multi-line strings in bash commands except for trivial one-liners.

## SQL Style Guidelines

- **CTE naming**: Use UPPER_CASE_SNAKE_CASE for CTE names.
- **Table aliasing**: Always use explicit `AS` between a table/CTE reference and its alias.
- **Prefer CTEs over correlated subqueries**.
- **Prefer window functions over GROUP BY** when computing aggregates for filtering.
- Always use prepared statements / bind parameters. Never construct SQL via f-strings or string interpolation.

## Function Naming

SQL-level functions use short prefixes to avoid name collisions:
- `bo_*` — blobodbc (ODBC I/O)
- `bt_*` — blobtemplates (JSON/text transformations)
- `bb_*` — blobboxes (document extraction)
- `bf_*` — blobfilters (roaring bitmap fingerprints)

## Catalog Query YAML Convention

Each `catalog/{dialect}/{kind}.yml` has:
- `sql:` — base SQL without optional parameter WHERE clauses
- `parameters:` — each with an optional `where:` fragment
- The caller dynamically appends WHERE fragments for non-NULL parameters
- Do NOT use `::` cast syntax in PostgreSQL queries (conflicts with `:name` parameter rewriter). Use `CAST(x AS type)` instead.
- SQL Server `NVARCHAR(MAX)` / LOB columns must be LAST in the SELECT list (ODBC driver requirement).

## Architecture

- `catalog/` — dialect-specific SQL as YAML (source of truth for schema queries)
- `sql/` — DuckDB SQL scripts (registry DDL, polling, connection string builder)
- `python/blobrule4/` — models (SQLAlchemy), sampler, intern pipeline, seed data
- DuckDB is the local staging/compute layer; higher-ceremony results can be published to SQL Server or PostgreSQL via `bo_execute` / `bo_query_in_catalog`.

## Communication Style

- Terse. No trailing summaries. Lead with the action or result.
