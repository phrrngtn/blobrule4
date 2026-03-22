# Regex Domain Probing

Tier 2b in the [[Resolution Sieve Architecture]]. JOIN value samples against a regex pattern table via DuckDB's vectorized `regexp_full_match` and `regexp_matches`.

## Dual-Mode Probing

Run both `regexp_full_match` (whole value) and `regexp_matches` (substring) — the delta is the signal:

| full_match | substring | Interpretation |
|---|---|---|
| > 90% | ~100% | **IS this domain** (zipcode, phone) |
| ~0% | > 10% | **Free text with embedded instances** (descriptions containing phone numbers) |
| > 0% | >> full | **Mixed content** |

## Pattern Set

38 curated patterns in `catalog/reference/regex_domains.yml`, validated against DuckDB's RE2 engine. Categories: contact, identity_document, financial, datetime, network, geographic, code_identifier, cryptographic, file_system.

## Performance

UNPIVOT + DISTINCT + per-pattern vectorized scan: **0.17s** for 27 columns × 38 patterns × 50k rows. Length pre-filtering eliminates 85% of regex calls.

## Position in the Stack

- Tier 2a: [[Blobfilter Domain Probing]] (exact membership, instant)
- **Tier 2b: Regex probing** (format matching)
- Tier 2c: Descriptive stats (cardinality, nulls, min/max)

Regex detects *format* not *membership*. "Is this a phone number?" vs "Is this phone number in the US directory?"

## Links
- [[Resolution Sieve Architecture]]
- [[Blobfilter Domain Probing]]
- [[Composable Relation Builders]]
