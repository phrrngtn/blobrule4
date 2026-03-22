# Table Topic Matching

Find related tables by comparing their semantic topic profiles. Extension of [[Topic Bounding Boxes]] with weighted column counts.

## Why Column Count Matters

Bare topic set intersection loses the shape:
- Table A: `{geolocation: 2, date: 5, status: 1}` — transactional, date-heavy
- Table B: `{geolocation: 47, date: 1}` — geographic dataset

Both share `{geolocation, date}` but are very different tables. The **column fraction per topic** captures this: A is 63% date, B is 98% geolocation.

## Proposed Schema

```
rule4_table_topic_profile:
  (ds_id, catalog, schema, table, category) PK
  column_count      INTEGER    -- columns assigned to this topic
  column_fraction   FLOAT      -- column_count / total_columns
  avg_similarity    FLOAT      -- mean cosine sim of columns to the topic
  topic_rank        INTEGER    -- 1 = dominant, 2 = second, etc.
```

Plus a per-table blobfilter of the topic set for fast screening.

## Matching Algorithm

**Coarse filter** (O(1) per pair): blobfilter containment on topic sets. Eliminates pairs with no topic overlap.

**Weighted Jaccard** on topic vectors:
```sql
-- For each shared topic, take min(weight_a, weight_b) / max(weight_a, weight_b)
-- Sum across shared topics → similarity score
SELECT SUM(LEAST(a.weight, b.weight)) / SUM(GREATEST(a.weight, b.weight))
FROM a_topics JOIN b_topics ON a.category = b.category
```

**Quality weighting**: avg_similarity per topic discounts weak assignments. A table whose columns match "date" at 0.72 is a stronger date table than one matching at 0.55.

## Pipeline

1. At catalog time: embed column names (10ms/col) → match to WordNet categories (20ms/col) → compute topic profile → store in PG
2. At query time: new table → topic profile (1-2s for 37 cols) → blobfilter screen (milliseconds) → weighted Jaccard against candidates → ranked matches

## Open Questions

- Should the topic vector include a "noise" dimension for columns that don't match any category well (sim < 0.6)?
- Is weighted Jaccard the right similarity, or should it be cosine on the weight vectors?
- Should topic matching consider the hypernym hierarchy? Two tables sharing `{city, suburb}` are more related than topic-set intersection suggests, because both are under `municipality`.

## Links
- [[Topic Bounding Boxes]]
- [[WordNet Taxonomy]]
- [[Domain Registry]]
- [[Resolution Sieve Architecture]]
