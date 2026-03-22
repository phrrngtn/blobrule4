# Composable Relation Builders

SQLAlchemy-based query building blocks in `blobrule4/metadata.py`. Everything operates on any `FromClause` (Table, subquery, CTE) and returns a `Select` that can be `.cte()`'d or `.subquery()`'d.

## Layers

### Layer 0 — FROM/JOIN
- `equi_condition(left, right, pairs)` → ON clause
- `outer_join(left, right, pairs)` → LEFT OUTER JOIN
- `inner_join(left, right, pairs)` → INNER JOIN

### Layer 1 — Filtered Selectables
- `left_orphans(left, right, pairs)` → left rows with no right match
- `right_orphans(left, right, pairs)` → reverse
- `matched_rows(left, right, pairs)` → inner join, all columns

### Layer 2 — Profiling
- `top_n(selectable, col, n)` → window-based ranking (no LIMIT)
- `count_by(selectable, group_cols)` → GROUP BY + COUNT
- `coverage(left, right, pairs)` → single-row join coverage stats
- `profile_column(selectable, col)` → descriptive stats

### Layer 3 — Regex Domain Probing
- `unpivot_to_kv(table)` → UNPIVOT + DISTINCT to (column_name, val, freq)
- `regex_probe(kv, pattern, ...)` → dual-mode regexp_full_match + regexp_matches
- `regex_probe_all(duck, kv_table, patterns)` → vectorized per-pattern scan

## Key Performance Insight

DuckDB vectorizes `regexp_full_match(column, constant)` but NOT `regexp_full_match(column, varying)`. One scan per pattern enables vectorization; cross-join prevents it. Result: 233× speedup (0.17s vs 70s).

## SchemaCollection

Evidence-driven federation: `from_schemas()`, `from_evidence()`, `from_topic()`. Infers cross-schema joins by column name + type compatibility. JoinCandidate objects delegate to the composable functions.

## Links
- [[Blobrule4 Project]]
- [[Regex Domain Probing]]
