# Facts As Evidence

Design principle in [[Blobrule4 Project]]: record discrete observations about metadata as facts, independent of any classification scheme.

## The Principle

Separate the **observation layer** from the **inference layer**:
1. Raw metadata → observed fact → evidence weighting → classification
2. Multiple classification schemes can weight the same facts differently
3. If the weighting model changes, re-derive conclusions without re-observing

## The `rule4_metadata_fact` Table

Each row is a single observation with provenance:
- `fact_type` — vocabulary: fk_member, pk_member, type_signature, check_enum, default_hint, naming_pattern, regex_probe, cardinality_ratio, blobfilter_match, etc.
- `fact_value` — JSON: the raw observation
- `tier` — 1=structural, 2=profile, 3=sample
- `source_kind` + `source_revision` — which snapshot produced this fact

## Self-Similarity

The fact graph is composable across abstraction levels. A filter that detects "is something changing a lot" should work on column-level facts AND on the fact graph itself ("are the domain classifications for this schema unstable?"). Dispatch on the *shape* of the result set (structural typing), not the abstraction level.

Related to type inference in PureScript / subtyping in type theory: analyses compose based on structural compatibility of inputs/outputs, not nominal type labels.

## Links
- [[Resolution Sieve Architecture]]
- [[Data As Control Plane]]
