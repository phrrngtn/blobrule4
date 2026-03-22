# Blobrule4 Index

## Architecture
- [[Blobrule4 Project]] — overview, pipeline, components
- [[Resolution Sieve Architecture]] — three-tier classification system

## Design Principles
- [[Facts As Evidence]] — observations independent of classification
- [[Data As Control Plane]] — rules as queryable rows, not code

## Components
- [[Composable Relation Builders]] — Layers 0-3, SQLAlchemy-based
- [[Domain Registry]] — 28 domains, blobfilters + embeddings in PG
- [[MetaData Generator]] — snapshot → SQLAlchemy MetaData
- [[Schema Collection]] — evidence-driven cross-schema federation

## Classification Tiers
- [[Blobfilter Domain Probing]] — Tier 2a, exact membership
- [[Regex Domain Probing]] — Tier 2b, format matching
- [[WordNet Taxonomy]] — category matching for topic detection
- [[Topic Bounding Boxes]] — R-tree-like coarse-to-fine table matching

## Related Projects
- [[Blobembed]] — in-database text embeddings
- [[Blobfilters]] — roaring bitmap membership
- [[Blobapi]] — web API integration, reified functions
