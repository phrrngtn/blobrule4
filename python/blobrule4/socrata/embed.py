"""
blobrule4.socrata.embed — Generate embeddings for Socrata catalog metadata.

Embeds resource names + descriptions and stores vectors in DuckDB for
semantic search against the Socrata catalog.

For resources (11K rows), embeds: "{name}. {description_truncated}"
For columns, embeds per-resource column summaries rather than individual
columns (322K would be too slow).

Usage:
    uv run python -m blobrule4.socrata.embed [--db PATH] [--domain DOMAIN]
"""

import os
import time

import duckdb
import psycopg2

PG_DSN = os.environ.get("PG_URL", "dbname=rule4_test host=/tmp")


def embed_resources(duck_conn, pg_dsn=PG_DSN, domain=None, batch_size=100):
    """Embed Socrata resource metadata into DuckDB vector table.

    Creates socrata_resource_embeddings table with (domain, resource_id, text, vec).

    Args:
        duck_conn: DuckDB connection with blobembed extension loaded.
        pg_dsn: PostgreSQL DSN for reading catalog metadata.
        domain: If set, only embed resources from this domain.
        batch_size: Rows to process per batch (for progress reporting).

    Returns:
        Number of resources embedded.
    """
    pg_conn = psycopg2.connect(pg_dsn)

    domain_filter = "AND domain = %(domain)s" if domain else ""
    with pg_conn.cursor() as cur:
        cur.execute(f"""
            SELECT domain, resource_id, name,
                   LEFT(description, 500) AS description
            FROM socrata.resource
            WHERE tt_end = '9999-12-31'
              {domain_filter}
            ORDER BY domain, resource_id
        """, {"domain": domain})
        rows = cur.fetchall()
    pg_conn.close()

    print(f"  {len(rows)} resources to embed")

    # Create target table
    duck_conn.execute("DROP TABLE IF EXISTS socrata_resource_embeddings")
    duck_conn.execute("""
        CREATE TABLE socrata_resource_embeddings (
            domain VARCHAR,
            resource_id VARCHAR,
            embed_text VARCHAR,
            vec FLOAT[768]
        )
    """)

    t0 = time.time()
    n = 0
    for domain_val, resource_id, name, description in rows:
        text = f"{name or ''}. {description or ''}"[:600].strip()
        if not text or text == ".":
            continue

        duck_conn.execute("""
            INSERT INTO socrata_resource_embeddings
            VALUES (?, ?, ?, be_embed('nomic', ?)::FLOAT[768])
        """, [domain_val, resource_id, text, text])

        n += 1
        if n % batch_size == 0:
            elapsed = time.time() - t0
            rate = n / elapsed
            print(f"  {n} / {len(rows)} ({rate:.1f}/s)")

    elapsed = time.time() - t0
    print(f"  Done: {n} embeddings in {elapsed:.1f}s ({n/elapsed:.1f}/s)")
    return n


def embed_resource_columns(duck_conn, pg_dsn=PG_DSN, domain=None):
    """Embed per-resource column summaries.

    For each resource, concatenates column names into a single string
    and embeds that. This gives a "schema fingerprint" embedding.

    Creates socrata_resource_column_summary_embeddings table.
    """
    pg_conn = psycopg2.connect(pg_dsn)

    domain_filter = "AND c.domain = %(domain)s" if domain else ""
    with pg_conn.cursor() as cur:
        cur.execute(f"""
            SELECT c.domain, c.resource_id,
                   r.name AS resource_name,
                   STRING_AGG(
                       COALESCE(c.display_name, c.field_name),
                       ', ' ORDER BY c.ordinal_position
                   ) AS column_list
            FROM socrata.resource_column AS c
            JOIN socrata.resource AS r
                ON c.domain = r.domain
                AND c.resource_id = r.resource_id
                AND r.tt_end = '9999-12-31'
            WHERE c.tt_end = '9999-12-31'
              {domain_filter}
            GROUP BY c.domain, c.resource_id, r.name
            ORDER BY c.domain, c.resource_id
        """, {"domain": domain})
        rows = cur.fetchall()
    pg_conn.close()

    print(f"  {len(rows)} resource column summaries to embed")

    duck_conn.execute("DROP TABLE IF EXISTS socrata_column_summary_embeddings")
    duck_conn.execute("""
        CREATE TABLE socrata_column_summary_embeddings (
            domain VARCHAR,
            resource_id VARCHAR,
            embed_text VARCHAR,
            vec FLOAT[768]
        )
    """)

    t0 = time.time()
    n = 0
    for domain_val, resource_id, resource_name, column_list in rows:
        text = f"{resource_name or ''}: {column_list or ''}"[:600].strip()
        if not text:
            continue

        duck_conn.execute("""
            INSERT INTO socrata_column_summary_embeddings
            VALUES (?, ?, ?, be_embed('nomic', ?)::FLOAT[768])
        """, [domain_val, resource_id, text, text])

        n += 1
        if n % 100 == 0:
            elapsed = time.time() - t0
            rate = n / elapsed
            print(f"  {n} / {len(rows)} ({rate:.1f}/s)")

    elapsed = time.time() - t0
    print(f"  Done: {n} embeddings in {elapsed:.1f}s ({n/elapsed:.1f}/s)")
    return n


def semantic_search(duck_conn, query, table="socrata_resource_embeddings", limit=10):
    """Search embedded resources by semantic similarity to a query string.

    Args:
        duck_conn: DuckDB connection with blobembed loaded and embeddings table.
        query: Natural language search query.
        table: Which embeddings table to search.
        limit: Max results.

    Returns:
        List of (domain, resource_id, embed_text, similarity).
    """
    return duck_conn.execute(f"""
        SELECT domain, resource_id, embed_text,
               list_cosine_similarity(vec, be_embed('nomic', ?)::FLOAT[768]) AS similarity
        FROM {table}
        ORDER BY similarity DESC
        LIMIT ?
    """, [query, limit]).fetchall()


def main():
    import argparse
    import blobembed_duckdb

    parser = argparse.ArgumentParser(description="Embed Socrata catalog metadata")
    parser.add_argument("--db", default="socrata_embeddings.duckdb",
                        help="DuckDB database for storing embeddings")
    parser.add_argument("--domain", default=None,
                        help="Only embed resources from this domain")
    parser.add_argument("--search", default=None,
                        help="Semantic search query (requires existing embeddings)")
    parser.add_argument("--columns", action="store_true",
                        help="Embed column summaries instead of resources")
    args = parser.parse_args()

    conn = duckdb.connect(args.db, config={"allow_unsigned_extensions": "true"})
    conn.execute(f"LOAD '{blobembed_duckdb.extension_path()}'")

    # Always load the model
    conn.execute("""
        SELECT be_load_hf_model('nomic',
            'nomic-ai/nomic-embed-text-v1.5-GGUF',
            'nomic-embed-text-v1.5.Q4_K_M.gguf')
    """)

    if args.search:
        table = ("socrata_column_summary_embeddings" if args.columns
                 else "socrata_resource_embeddings")
        print(f"Semantic search: \"{args.search}\"\n")
        results = semantic_search(conn, args.search, table)
        for domain, rid, text, sim in results:
            print(f"  {sim:.4f}  {domain:25s} {rid:12s} {text[:60]}")
    else:
        print("Loading nomic embedding model...")
        conn.execute("""
            SELECT be_load_hf_model('nomic',
                'nomic-ai/nomic-embed-text-v1.5-GGUF',
                'nomic-embed-text-v1.5.Q4_K_M.gguf')
        """)

        if args.columns:
            print("\nEmbedding resource column summaries...")
            n = embed_resource_columns(conn, domain=args.domain)
        else:
            print("\nEmbedding resources...")
            n = embed_resources(conn, domain=args.domain)

        print(f"\n{n} embeddings stored in {args.db}")

    conn.close()


if __name__ == "__main__":
    main()
