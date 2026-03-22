# Data As Control Plane

Design principle: classification rules, lookup tables, and algorithm parameters should be queryable data in the database, not Python dicts or CASE expressions.

## Four-Layer Execution Model

1. **Stable reference data → PG** — promoted from DuckDB when contents stop varying
2. **Data-driven control plane → SQL tables** — suffix→role maps, type classification rules, domain registry. Like reify's `llm_adapter` rows.
3. **Generated SQL for probing → Python+SQLAlchemy** — composable, dialect-independent query construction
4. **Process orchestration → Python** — sequencing, error handling, extension loading

## Promotion Path

registered DataFrame → DuckDB temp table → PG reference table

If you find yourself registering the same object repeatedly and contents don't vary, promote it.

## Links
- [[Facts As Evidence]]
- [[Resolution Sieve Architecture]]
- [[Blobrule4 Project]]
