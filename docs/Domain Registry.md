# Domain Registry

Centralized collection of known value domains in PostgreSQL (`rule4_test`, schema `domain`). Each domain has primary members, Wikidata-sourced alternate labels, roaring bitmap blobfilters, and nomic embeddings.

## Current State (2026-03-21)

28 domains, 2,596 primary members, 13,217 total labels (with alt_labels), all indexed.

### Wikidata-Sourced (5 domains)
| Domain | QID | Primary | With Alts |
|---|---|---|---|
| countries | Q6256 | 222 | 3,398 |
| us_states | Q35657 | 50 | 1,122 |
| currencies | Q8142 | 175 | 1,632 |
| languages_major | Q34770 | 45 | 705 |
| chemical_elements | Q11344 | 174 | 856 |

### Geonames-Sourced
| Domain | Members |
|---|---|
| us_state_abbrev | 51 |
| country_iso2 | 252 |
| country_iso3 | 252 |
| country_names | 252 |
| us_cities_major (pop>50k) | 911 |

### Curated
boolean_labels, canadian_provinces, compass, continents, crime_categories, days_long/short, gender, http_methods, land_use, mime_types_common, months_long/short, quarters, school_types, status_values, us_census_race, utility_types

## Storage

- `domain.enumeration` — metadata + `filter_b64` (roaring bitmap as base64)
- `domain.member` — `(domain_name, label, alt_labels[])`
- `domain.member_embedding` — `(domain_name, label, model_name, embedding)` with model tracking

## Indexing

- **Blobfilters**: bf_containment_json(probe_json, bf_from_base64(filter)) = instant membership test
- **Embeddings**: nomic-ai/nomic-embed-text-v1.5-GGUF/Q8_0, 768-dim, ~20ms/embed
- **Alt_labels**: case variants + Wikidata alternates (Nepal has 101 alternatives)

## Links
- [[Resolution Sieve Architecture]]
- [[Blobfilter Domain Probing]]
- [[Blobembed]]
- [[WordNet Taxonomy]]
