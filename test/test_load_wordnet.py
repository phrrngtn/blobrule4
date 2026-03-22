"""Insert WordNet categories + embeddings into PG. Tables already created."""
import yaml, json, time, glob, sys, os

# Suppress llama.cpp noise
os.environ["GGML_METAL_LOG_LEVEL"] = "0"

import duckdb

BLOBEMBED_EXT = "/Users/paulharrington/checkouts/blobembed/build/duckdb/blobembed.duckdb_extension"
MODEL_PATH = glob.glob(
    "/Users/paulharrington/.cache/huggingface/hub/models--nomic-ai--nomic-embed-text-v1.5-GGUF/"
    "snapshots/*/nomic-embed-text-v1.5.Q8_0.gguf"
)[0]

with open("/Users/paulharrington/checkouts/blobembed/data/wordnet_categories.yaml") as f:
    data = yaml.safe_load(f)
categories = data["categories"]

duck = duckdb.connect(":memory:", config={"allow_unsigned_extensions": "true"})
duck.execute(f"LOAD '{BLOBEMBED_EXT}'")
duck.execute(f"SELECT be_load_model('nomic', '{MODEL_PATH}')")
print(f"Loaded model, {len(categories)} categories")

# Build embeddings in DuckDB
duck.execute("""
    CREATE TABLE wn (synset_id VARCHAR, category VARCHAR, hypernym VARCHAR,
                     depth INTEGER, gloss VARCHAR)
""")
duck.executemany("INSERT INTO wn VALUES (?,?,?,?,?)",
    [(c.get("synset_id"), c["category"], c.get("hypernym"),
      c.get("depth", 0), c.get("gloss")) for c in categories])

t0 = time.perf_counter()
duck.execute("""
    CREATE TABLE wn_emb AS
    SELECT *, category || ': ' || COALESCE(gloss, category) AS embed_text,
           be_embed('nomic', category || ': ' || COALESCE(gloss, category))::FLOAT[] AS embedding
    FROM wn
""")
t1 = time.perf_counter()
print(f"Embedded {len(categories)} categories in {t1-t0:.0f}s")

# Insert into PG
import psycopg2
conn = psycopg2.connect(host="/tmp", dbname="rule4_test")
cur = conn.cursor()

# Categories
rows = duck.execute("SELECT synset_id, category, hypernym, depth, gloss, embed_text FROM wn_emb").fetchall()
cur.executemany("""
    INSERT INTO domain.wordnet_category (synset_id, category, hypernym, depth, gloss, embed_text)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT DO NOTHING
""", rows)
conn.commit()
print(f"Inserted {cur.rowcount} categories")

# Embeddings
emb_rows = duck.execute("SELECT category, synset_id, embedding FROM wn_emb").fetchall()
cur.executemany("""
    INSERT INTO domain.wordnet_category_embedding (category, synset_id, model_name, embedding)
    VALUES (%s, %s, 'nomic', %s)
    ON CONFLICT DO NOTHING
""", [(cat, sid, list(emb)) for cat, sid, emb in emb_rows])
conn.commit()
print(f"Inserted {cur.rowcount} embeddings")

cur.close()
conn.close()

# Verify via DuckDB → PG
duck.execute("INSTALL postgres; LOAD postgres;")
duck.execute("ATTACH 'host=/tmp dbname=rule4_test' AS pg (TYPE POSTGRES, READ_ONLY)")

counts = duck.execute("""
    SELECT (SELECT COUNT(*) FROM pg.domain.wordnet_category),
           (SELECT COUNT(*) FROM pg.domain.wordnet_category_embedding),
           (SELECT COUNT(*) FROM pg.domain.wordnet_category WHERE depth = 0)
""").fetchone()
print(f"\nPG: {counts[0]} categories ({counts[2]} manual), {counts[1]} embeddings")

# Semantic test
print("\nClosest WordNet categories for 'Chicago':")
for cat, hyp, d, sim in duck.execute("""
    SELECT e.category, c.hypernym, c.depth,
           be_cosine_sim(be_embed('nomic', 'Chicago'), e.embedding::FLOAT[]) AS sim
    FROM pg.domain.wordnet_category_embedding AS e
    JOIN pg.domain.wordnet_category AS c
        ON c.category = e.category AND COALESCE(c.synset_id,'') = COALESCE(e.synset_id,'')
    ORDER BY sim DESC LIMIT 5
""").fetchall():
    print(f"  {cat} ({hyp}, d={d}): {sim:.4f}")

print("\nClosest for 'revenue':")
for cat, hyp, d, sim in duck.execute("""
    SELECT e.category, c.hypernym, c.depth,
           be_cosine_sim(be_embed('nomic', 'revenue'), e.embedding::FLOAT[]) AS sim
    FROM pg.domain.wordnet_category_embedding AS e
    JOIN pg.domain.wordnet_category AS c
        ON c.category = e.category AND COALESCE(c.synset_id,'') = COALESCE(e.synset_id,'')
    ORDER BY sim DESC LIMIT 5
""").fetchall():
    print(f"  {cat} ({hyp}, d={d}): {sim:.4f}")

print("\nClosest for 'patient diagnosis':")
for cat, hyp, d, sim in duck.execute("""
    SELECT e.category, c.hypernym, c.depth,
           be_cosine_sim(be_embed('nomic', 'patient diagnosis'), e.embedding::FLOAT[]) AS sim
    FROM pg.domain.wordnet_category_embedding AS e
    JOIN pg.domain.wordnet_category AS c
        ON c.category = e.category AND COALESCE(c.synset_id,'') = COALESCE(e.synset_id,'')
    ORDER BY sim DESC LIMIT 5
""").fetchall():
    print(f"  {cat} ({hyp}, d={d}): {sim:.4f}")

duck.close()
