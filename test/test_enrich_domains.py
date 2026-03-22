"""
Enrich PG domain.member alt_labels from the blobembed YAML (which has
Wikidata alt_labels), rebuild blobfilters, and re-embed everything.
Track model name on all embeddings.
"""
import yaml
import json
import time
import glob
import sys

PSQL = "/opt/homebrew/Cellar/postgresql@17/17.9/bin/psql"

import duckdb

duck = duckdb.connect(":memory:", config={"allow_unsigned_extensions": "true"})
duck.execute("INSTALL postgres; LOAD postgres;")
duck.execute("ATTACH 'host=/tmp dbname=rule4_test' AS pg (TYPE POSTGRES)")
duck.execute("LOAD '/Users/paulharrington/checkouts/blobfilters/build/duckdb/blobfilters.duckdb_extension'")
duck.execute("LOAD '/Users/paulharrington/checkouts/blobembed/build/duckdb/blobembed.duckdb_extension'")

MODEL_PATH = glob.glob(
    "/Users/paulharrington/.cache/huggingface/hub/models--nomic-ai--nomic-embed-text-v1.5-GGUF/"
    "snapshots/*/nomic-embed-text-v1.5.Q8_0.gguf"
)[0]
MODEL_ALIAS = "nomic"
MODEL_FULL = "nomic-ai/nomic-embed-text-v1.5-GGUF/Q8_0"

duck.execute(f"SELECT be_load_model('{MODEL_ALIAS}', '{MODEL_PATH}')")
dim = duck.execute(f"SELECT be_embed_dim('{MODEL_ALIAS}')").fetchone()[0]
print(f"Model: {MODEL_FULL} (alias={MODEL_ALIAS}, dim={dim})")

# ── Step 1: Merge alt_labels from blobembed YAML into PG ──
print(f"\n{'='*70}")
print("STEP 1: Enrich alt_labels from blobembed YAML")
print("=" * 70)

with open("/Users/paulharrington/checkouts/blobembed/data/domain_enumerations.yaml") as f:
    yaml_data = yaml.safe_load(f)

enriched = 0
for domain in yaml_data["domains"]:
    dn = domain["domain_name"]
    for member in domain.get("members", []):
        label = member["label"]
        alts = member.get("alt_labels", [])
        if not alts:
            continue

        # Build the combined alt_labels: existing case variants + YAML alts
        # Also add upper/lower of each alt
        all_alts = set()
        for a in alts:
            all_alts.add(a)
            all_alts.add(a.upper())
            all_alts.add(a.lower())
        # Add case variants of the label itself
        all_alts.add(label.upper())
        all_alts.add(label.lower())
        # Remove the label itself
        all_alts.discard(label)

        alt_array = sorted(all_alts)

        # Update PG — merge with existing alt_labels
        try:
            duck.execute("""
                UPDATE pg.domain.member
                SET alt_labels = ?::VARCHAR[]
                WHERE domain_name = ? AND label = ?
            """, [alt_array, dn, label])
            enriched += 1
        except Exception as e:
            # Member might not exist in PG (YAML has some extras)
            pass

print(f"  Enriched {enriched} members with Wikidata alt_labels")

# Verify
sample = duck.execute("""
    SELECT label, array_length(alt_labels) AS n_alts
    FROM pg.domain.member
    WHERE domain_name = 'countries'
    ORDER BY array_length(alt_labels) DESC
    LIMIT 5
""").fetchall()
print(f"\n  Most alt_labels (countries):")
for label, n in sample:
    print(f"    {label}: {n} alternatives")

# ── Step 2: Rebuild all blobfilters with enriched alt_labels ──
print(f"\n{'='*70}")
print("STEP 2: Rebuild blobfilters with enriched alt_labels")
print("=" * 70)

t0 = time.perf_counter()

# Stage all labels (primary + alt) locally
duck.execute("""
    CREATE TABLE all_labels AS
    SELECT domain_name, label FROM pg.domain.member
    UNION
    SELECT domain_name, unnest(alt_labels) AS label
    FROM pg.domain.member
    WHERE array_length(alt_labels) > 0
""")

total_labels = duck.execute("SELECT COUNT(*) FROM all_labels").fetchone()[0]
print(f"  Total labels (primary + alts): {total_labels:,}")

# Build filters
filters = duck.execute("""
    WITH GROUPED AS (
        SELECT domain_name,
               json_group_array(label) AS members_json
        FROM all_labels
        GROUP BY domain_name
    )
    SELECT domain_name,
           bf_to_base64(bf_build_json(members_json)) AS filter_b64,
           bf_cardinality(bf_build_json(members_json)) AS cardinality
    FROM GROUPED
""").fetchall()

# Write to PG
for dn, fb64, card in filters:
    duck.execute("""
        UPDATE pg.domain.enumeration
        SET filter_b64 = ?, member_count = ?, updated_at = NOW()
        WHERE domain_name = ?
    """, [fb64, card, dn])

t1 = time.perf_counter()
print(f"  Built and stored {len(filters)} filters in {t1-t0:.1f}s\n")

print(f"  {'domain':<25} {'members':>7} {'b64_len':>8}")
print("  " + "-" * 45)
for dn, fb64, card in sorted(filters, key=lambda x: -x[2]):
    print(f"  {dn:<25} {card:>7} {len(fb64):>8}")

# ── Step 3: Build embeddings for ALL labels (primary + alts) ──
print(f"\n{'='*70}")
print("STEP 3: Build embeddings for all labels")
print("=" * 70)

# Ensure the PG table tracks model name
duck.execute("""
    CREATE TABLE IF NOT EXISTS pg.domain.member_embedding (
        domain_name TEXT NOT NULL,
        label TEXT NOT NULL,
        model_name TEXT NOT NULL,
        embedding FLOAT[] NOT NULL,
        PRIMARY KEY (domain_name, label, model_name)
    )
""")

# Get labels that don't have embeddings yet
duck.execute("""
    CREATE TABLE labels_to_embed AS
    SELECT al.domain_name, al.label
    FROM all_labels AS al
    LEFT JOIN pg.domain.member_embedding AS me
        ON me.domain_name = al.domain_name
       AND me.label = al.label
       AND me.model_name = ?
    WHERE me.label IS NULL
""", [MODEL_ALIAS])

n_todo = duck.execute("SELECT COUNT(*) FROM labels_to_embed").fetchone()[0]
n_existing = duck.execute(
    "SELECT COUNT(*) FROM pg.domain.member_embedding WHERE model_name = ?",
    [MODEL_ALIAS]
).fetchone()[0]
print(f"  Existing embeddings: {n_existing:,}")
print(f"  New labels to embed: {n_todo:,}")

if n_todo > 0:
    t2 = time.perf_counter()

    # Batch by domain for progress reporting
    domains = duck.execute("""
        SELECT domain_name, COUNT(*) AS n
        FROM labels_to_embed
        GROUP BY domain_name ORDER BY n DESC
    """).fetchall()

    total_done = 0
    for dn, n in domains:
        labels = duck.execute(
            "SELECT label FROM labels_to_embed WHERE domain_name = ?", [dn]
        ).fetchall()

        batch_start = time.perf_counter()
        for (label,) in labels:
            try:
                duck.execute(f"""
                    INSERT INTO pg.domain.member_embedding
                        (domain_name, label, model_name, embedding)
                    VALUES (?, ?, ?, be_embed('{MODEL_ALIAS}', ?)::FLOAT[])
                    ON CONFLICT (domain_name, label, model_name) DO UPDATE
                        SET embedding = EXCLUDED.embedding
                """, [dn, label, MODEL_ALIAS, label])
                total_done += 1
            except Exception as e:
                print(f"    ERROR embedding {dn}.{label}: {e}", file=sys.stderr)

        elapsed = time.perf_counter() - batch_start
        ms_per = elapsed * 1000 / len(labels) if labels else 0
        print(f"  {dn:<25} {len(labels):>5} labels  {elapsed:.1f}s ({ms_per:.1f}ms/embed)")

    t3 = time.perf_counter()
    print(f"\n  Embedded {total_done:,} new labels in {t3-t2:.1f}s")

# ── Step 4: Summary ──
print(f"\n{'='*70}")
print("FINAL STATE")
print("=" * 70)

summary = duck.execute("""
    SELECT e.domain_name, e.domain_label, e.source,
           e.member_count AS filter_members,
           (SELECT COUNT(*) FROM pg.domain.member AS m WHERE m.domain_name = e.domain_name) AS primary_members,
           (SELECT COUNT(*) FROM pg.domain.member_embedding AS me
            WHERE me.domain_name = e.domain_name AND me.model_name = ?) AS embeddings,
           LENGTH(e.filter_b64) AS filter_b64_len
    FROM pg.domain.enumeration AS e
    ORDER BY e.domain_name
""", [MODEL_ALIAS]).fetchall()

print(f"\n  {'domain':<25} {'primary':>7} {'w/alts':>7} {'embeds':>7} {'filt_kb':>7}")
print("  " + "-" * 60)
total_primary = 0
total_filter = 0
total_embeds = 0
for dn, dl, src, filt_m, prim_m, embeds, filt_len in summary:
    total_primary += prim_m
    total_filter += filt_m
    total_embeds += embeds
    filt_kb = round(filt_len / 1024, 1) if filt_len else 0
    print(f"  {dn:<25} {prim_m:>7} {filt_m:>7} {embeds:>7} {filt_kb:>6.1f}k")

print(f"  {'':25} {'─'*7} {'─'*7} {'─'*7}")
print(f"  {'TOTAL':<25} {total_primary:>7} {total_filter:>7} {total_embeds:>7}")
print(f"\n  Model: {MODEL_FULL}")

duck.close()
