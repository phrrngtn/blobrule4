# Resolution Sieve Architecture

Multi-tier column/table classification system in [[Blobrule4 Project]]. Classifies columns as dimension/measure/key/content by layering cheap structural signals before expensive value probing.

## Three Cost Tiers

### Tier 1 — Metadata/Structure (free)
Already captured in TTST snapshots. Can run repeatedly.
- Names, paths, types, constraints, FKs, PKs, defaults, comments
- 10 structural analyses: FK topology, PK structure, type signatures, check constraint enums, default hints, column-name clustering, naming conventions, schema evolution, topic detection, view dependencies
- LLM is cheap here because input is just names and types

### Tier 2 — Profiles/Summaries (moderate)
One aggregate query per column, no row-level data crosses the wire.
- **Tier 2a**: [[Blobfilter Domain Probing]] — exact membership via roaring bitmaps (instant)
- **Tier 2b**: [[Regex Domain Probing]] — format matching (phone, zip, email, UUID)
- **Tier 2c**: Descriptive stats — cardinality ratio, nulls, min/max, histograms

### Tier 3 — Samples (expensive)
Actual data values cross the wire.
- Top-N by frequency
- TABLESAMPLE (preserves cross-column correlations)
- Constrained samples (WHERE filters informed by Tier 1/2)
- [[Back-testing]] — regenerate known values from candidate queries to validate mutual consistency

## Key Design Principles

- **[[Facts As Evidence]]** — discrete observations independent of classification schemes
- **Self-similarity** — the fact graph is composable across abstraction levels
- **[[Data As Control Plane]]** — classification rules are queryable rows, not code
- **Analysis executes as SQL** — Python generates queries via SQLAlchemy; DuckDB executes set-based

## Links
- [[Blobrule4 Project]]
- [[Domain Registry]]
- [[Composable Relation Builders]]
- [[Facts As Evidence]]
