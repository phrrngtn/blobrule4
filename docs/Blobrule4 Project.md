# Blobrule4 Project

Maintains concise, expressive manifestations of database schema definitions using the blob* extension family. Scrapes catalog metadata from ODBC-accessible databases, tracks schema evolution via TTST (transaction-time state tables) with RFC 6902 JSON Patch diffs, and produces human-readable changelogs.

## Pipeline

```
sampler.py → intern.py → structural.py → sieve.py (future)
(catalog)    (TTST)       (classify)      (orchestrate)
```

## Key Components

- **Catalog YAMLs** — dialect-specific SQL queries (sqlserver, postgresql, information_schema) for 15 kinds: tables, columns, PKs, FKs, indexes, triggers, callables, check_constraints, unique_constraints, views, column_comments, user_defined_types, partitioning, temporal_tables, synonyms
- **TTST** — snapshot + reverse patch chain for schema evolution tracking
- **[[MetaData Generator]]** — snapshot → SQLAlchemy Table/Column/PK/FK/Index objects
- **[[Schema Collection]]** — evidence-driven federation across schemas with inferred cross-schema joins
- **[[Composable Relation Builders]]** — Layers 0-3 operating on any SQLAlchemy FromClause
- **[[Resolution Sieve Architecture]]** — multi-tier column classification
- **[[Domain Registry]]** — 28 domains with blobfilters + embeddings in PG

## Related Projects
- [[Blobembed]] — in-database text embeddings via GGUF/llama.cpp
- [[Blobfilters]] — roaring bitmap membership testing
- [[Blobtemplates]] — JSON/text transformations
- [[Blobodbc]] — ODBC I/O
- [[Blobhttp]] — HTTP client for DuckDB
- [[Blobapi]] — web API integration, reified functions
