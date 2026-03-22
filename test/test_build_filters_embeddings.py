"""
Build blobfilters (roaring bitmaps) and nomic embeddings for all domain
members in PG. Store the filter as base64 in domain.enumeration.filter_b64
and embeddings in a new table.
"""
import json
import time
import glob
import sys
sys.path.insert(0, "python")

import duckdb

BLOBFILTERS_EXT = "/Users/paulharrington/checkouts/blobfilters/build/duckdb/blobfilters.duckdb_extension"
BLOBEMBED_EXT = "/Users/paulharrington/checkouts/blobembed/build/duckdb/blobembed.duckdb_extension"
MODEL_PATH = glob.glob(
    "/Users/paulharrington/.cache/huggingface/hub/models--nomic-ai--nomic-embed-text-v1.5-GGUF/"
    "snapshots/*/nomic-embed-text-v1.5.Q8_0.gguf"
)[0]
MODEL_ALIAS = "nomic"

duck = duckdb.connect(":memory:", config={"allow_unsigned_extensions": "true"})
duck.execute("INSTALL postgres; LOAD postgres;")
duck.execute("ATTACH 'host=/tmp dbname=rule4_test' AS pg (TYPE POSTGRES)")

# Load extensions
duck.execute(f"LOAD '{BLOBFILTERS_EXT}'")
duck.execute(f"LOAD '{BLOBEMBED_EXT}'")
duck.execute(f"SELECT be_load_model('{MODEL_ALIAS}', '{MODEL_PATH}')")
dim = duck.execute(f"SELECT be_embed_dim('{MODEL_ALIAS}')").fetchone()[0]
print(f"Model loaded: {MODEL_ALIAS}, dim={dim}")

# Stage domain data locally
duck.execute("""
    CREATE TABLE domains AS
    SELECT domain_name, domain_label, source, member_count
    FROM pg.domain.enumeration
""")
duck.execute("""
    CREATE TABLE members AS
    SELECT domain_name, label
    FROM pg.domain.member
""")

total_members = duck.execute("SELECT COUNT(*) FROM members").fetchone()[0]
n_domains = duck.execute("SELECT COUNT(*) FROM domains").fetchone()[0]
print(f"\n{n_domains} domains, {total_members} members")

# ── Build blobfilters ──
print(f"\n{'='*70}")
print("Building blobfilters (roaring bitmaps)")
print("=" * 70)

t0 = time.perf_counter()

# For each domain, build a filter from the JSON array of its members
filters = duck.execute("""
    WITH DOMAIN_MEMBERS AS (
        SELECT domain_name,
               json_group_array(label) AS members_json
        FROM members
        GROUP BY domain_name
    )
    SELECT domain_name,
           bf_to_base64(bf_build_json(members_json)) AS filter_b64,
           bf_cardinality(bf_build_json(members_json)) AS cardinality
    FROM DOMAIN_MEMBERS
""").fetchall()

t1 = time.perf_counter()
print(f"  Built {len(filters)} filters in {t1-t0:.2f}s\n")

print(f"  {'domain':<25} {'card':>5} {'filter_b64_len':>14}")
print("  " + "-" * 50)
for dn, fb64, card in filters:
    print(f"  {dn:<25} {card:>5} {len(fb64):>14}")

# Write filters back to PG
for dn, fb64, card in filters:
    duck.execute("""
        UPDATE pg.domain.enumeration
        SET filter_b64 = ?, updated_at = NOW()
        WHERE domain_name = ?
    """, [fb64, dn])
print(f"\n  Written to pg.domain.enumeration.filter_b64")

# ── Build embeddings ──
print(f"\n{'='*70}")
print("Building nomic embeddings (768-dim)")
print("=" * 70)

# Create local table for embeddings
duck.execute(f"""
    CREATE TABLE domain_embeddings (
        domain_name VARCHAR NOT NULL,
        label VARCHAR NOT NULL,
        embedding FLOAT[{dim}] NOT NULL,
        PRIMARY KEY (domain_name, label)
    )
""")

# Embed all members — batch by domain
t2 = time.perf_counter()
total_embedded = 0

domain_list = duck.execute("SELECT DISTINCT domain_name FROM members ORDER BY domain_name").fetchall()

for (dn,) in domain_list:
    members_list = duck.execute(
        "SELECT label FROM members WHERE domain_name = ?", [dn]
    ).fetchall()

    batch_start = time.perf_counter()
    for (label,) in members_list:
        duck.execute(f"""
            INSERT INTO domain_embeddings (domain_name, label, embedding)
            VALUES (?, ?, be_embed('{MODEL_ALIAS}', ?))
        """, [dn, label, label])
        total_embedded += 1

    batch_elapsed = time.perf_counter() - batch_start
    ms_per = batch_elapsed * 1000 / len(members_list) if members_list else 0
    print(f"  {dn:<25} {len(members_list):>5} members  {batch_elapsed:.1f}s ({ms_per:.1f}ms/embed)")

t3 = time.perf_counter()
print(f"\n  Total: {total_embedded} embeddings in {t3-t2:.1f}s ({(t3-t2)*1000/total_embedded:.1f}ms/embed)")

# ── Write embeddings to PG ──
print(f"\n{'='*70}")
print("Writing embeddings to PG")
print("=" * 70)

# Create the table in PG if needed
duck.execute("""
    CREATE TABLE IF NOT EXISTS pg.domain.member_embedding (
        domain_name TEXT NOT NULL,
        label TEXT NOT NULL,
        model_name TEXT NOT NULL,
        embedding FLOAT[] NOT NULL,
        PRIMARY KEY (domain_name, label, model_name)
    )
""")

# Write
t4 = time.perf_counter()
duck.execute(f"""
    INSERT INTO pg.domain.member_embedding (domain_name, label, model_name, embedding)
    SELECT domain_name, label, '{MODEL_ALIAS}', embedding::FLOAT[]
    FROM domain_embeddings
    ON CONFLICT (domain_name, label, model_name) DO UPDATE
        SET embedding = EXCLUDED.embedding
""")
t5 = time.perf_counter()
print(f"  Wrote {total_embedded} embeddings in {t5-t4:.1f}s")

# ── Verify: test containment on the Chicago data ──
print(f"\n{'='*70}")
print("VERIFICATION: blobfilter containment on Chicago Business Licenses")
print("=" * 70)

duck.execute("CREATE TABLE biz AS SELECT * FROM read_csv_auto('/tmp/chi_biz_licenses.csv')")

# Test state column against us_state_abbrev filter
state_filter = next(fb64 for dn, fb64, _ in filters if dn == "us_state_abbrev")
r = duck.execute("""
    WITH VALS AS (
        SELECT UPPER(TRIM(state)) AS val FROM biz WHERE state IS NOT NULL
    )
    SELECT
        COUNT(*) AS total,
        COUNT(*) FILTER (WHERE bf_containment_json(
            json_array(val), bf_from_base64(?)
        ) = 1.0) AS exact_match,
        COUNT(*) FILTER (WHERE bf_containment_json(
            json_array(val), bf_from_base64(?)
        ) > 0.0) AS partial_match
    FROM VALS
""", [state_filter, state_filter]).fetchone()
print(f"\n  state × us_state_abbrev: {r[1]}/{r[0]} exact match ({r[1]*100/r[0]:.1f}%)")

# Test city column against us_cities_major filter
city_filter = next(fb64 for dn, fb64, _ in filters if dn == "us_cities_major")
r = duck.execute("""
    WITH VALS AS (
        SELECT TRIM(city) AS val FROM biz WHERE city IS NOT NULL
    )
    SELECT
        COUNT(*) AS total,
        COUNT(*) FILTER (WHERE bf_containment_json(
            json_array(val), bf_from_base64(?)
        ) = 1.0) AS exact_match
    FROM VALS
""", [city_filter]).fetchone()
print(f"  city × us_cities_major: {r[1]}/{r[0]} exact match ({r[1]*100/r[0]:.1f}%)")

# Test zip_code against... well, ZIP codes aren't an enumeration. But let's test
# the state abbrev values that were anomalies
r = duck.execute("""
    SELECT bf_containment_json('["QC"]', bf_from_base64(?)),
           bf_containment_json('["ON"]', bf_from_base64(?))
""", [state_filter, state_filter]).fetchone()
print(f"\n  QC in us_state_abbrev: {r[0]}")
print(f"  ON in us_state_abbrev: {r[1]}")

ca_filter = next(fb64 for dn, fb64, _ in filters if dn == "canadian_provinces")
r = duck.execute("""
    SELECT bf_containment_json('["QC"]', bf_from_base64(?)),
           bf_containment_json('["ON"]', bf_from_base64(?))
""", [ca_filter, ca_filter]).fetchone()
print(f"  QC in canadian_provinces: {r[0]}")
print(f"  ON in canadian_provinces: {r[1]}")

# ── Embedding verification ──
print(f"\n{'='*70}")
print("VERIFICATION: embedding similarity")
print("=" * 70)

# Find closest domain for "Chicago"
r = duck.execute(f"""
    SELECT domain_name, label,
           be_cosine_sim(be_embed('{MODEL_ALIAS}', 'Chicago'), embedding) AS sim
    FROM domain_embeddings
    ORDER BY sim DESC
    LIMIT 10
""").fetchall()

print(f"\n  Top 10 matches for 'Chicago':")
for dn, label, sim in r:
    print(f"    {dn}.{label}: {sim:.4f}")

# Find closest domain for "Illinois"
r = duck.execute(f"""
    SELECT domain_name, label,
           be_cosine_sim(be_embed('{MODEL_ALIAS}', 'Illinois'), embedding) AS sim
    FROM domain_embeddings
    ORDER BY sim DESC
    LIMIT 5
""").fetchall()

print(f"\n  Top 5 matches for 'Illinois':")
for dn, label, sim in r:
    print(f"    {dn}.{label}: {sim:.4f}")

duck.close()
