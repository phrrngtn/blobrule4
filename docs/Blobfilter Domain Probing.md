# Blobfilter Domain Probing

Tier 2a in the [[Resolution Sieve Architecture]]. Instant exact membership testing via roaring bitmap filters.

## How It Works

```sql
-- Containment: what fraction of probe values are in the filter?
SELECT bf_containment_json(
    '["CHICAGO", "NAPERVILLE", "SKOKIE"]',
    bf_from_base64(filter_b64)
) AS containment
-- Returns 1.0 if all values are members, 0.5 if half, etc.
```

## Performance

- Filter build: 0.01s for all 28 domains
- Containment check: O(1) per probe — instant
- Filter size: ~168KB total for 13,217 labels

## Case Sensitivity

Filters include case variants via `alt_labels[]`. Each member's upper/lower forms plus Wikidata alternates are included. "Chicago", "CHICAGO", "chicago" all match.

## Results on Real Data

Chicago Business Licenses (10k rows):
- state × us_state_abbrev: **100%** match
- city × us_cities_major: **95.2%** match (remaining are small suburbs < 50k pop)
- QC/ON correctly NOT in us_state_abbrev but IN canadian_provinces

## Links
- [[Domain Registry]]
- [[Resolution Sieve Architecture]]
- [[Regex Domain Probing]]
