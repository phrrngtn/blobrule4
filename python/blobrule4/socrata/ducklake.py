"""
blobrule4.socrata.ducklake — Persist Socrata Discovery API catalog as DuckLake-managed tables.

Creates two tables in DuckLake:
  - socrata_resource: one row per dataset (relational columns + JSON blobs)
  - socrata_resource_column: one row per column per dataset (unnested arrays)

Both are managed by DuckLake for PIT time travel on the catalog itself.
Each sync run creates a new DuckLake snapshot so you can track how the
catalog evolves over time (new datasets, schema changes, description edits).

Usage:
    uv run python -m blobrule4.socrata.ducklake
    uv run python -m blobrule4.socrata.ducklake --domains data.cityofnewyork.us data.cityofchicago.org
    uv run python -m blobrule4.socrata.ducklake --all
"""

import json
import os
import time
import urllib.request

import duckdb

from blobrule4.socrata.scraper import fetch_domain_catalog

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "localhost:9000")
MINIO_KEY = os.environ.get("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")
S3_BUCKET = "ducklake-data"


def _s3_secret_sql():
    return (f"CREATE SECRET (TYPE S3, KEY_ID '{MINIO_KEY}', SECRET '{MINIO_SECRET}', "
            f"ENDPOINT '{MINIO_ENDPOINT}', USE_SSL false, URL_STYLE 'path')")


def catalog_to_parquet(resources, conn, resource_path, column_path):
    """Convert Discovery API results to two Parquet files.

    socrata_resource: one row per dataset
    socrata_resource_column: one row per column per dataset
    """

    # Build rows for socrata_resource
    res_rows = []
    col_rows = []

    for r in resources:
        res = r.get("resource", {})
        meta = r.get("metadata", {})
        cls = r.get("classification", {})

        resource_id = res.get("id", "")
        domain = meta.get("domain", "")

        res_rows.append({
            "domain": domain,
            "resource_id": resource_id,
            "name": res.get("name"),
            "description": res.get("description"),
            "resource_type": res.get("type"),
            "permalink": r.get("permalink"),
            "attribution": res.get("attribution"),
            "attribution_link": res.get("attribution_link"),
            "provenance": res.get("provenance"),
            "created_at": res.get("createdAt"),
            "updated_at": res.get("updatedAt"),
            "metadata_updated_at": res.get("metadata_updated_at"),
            "data_updated_at": res.get("data_updated_at"),
            "publication_date": res.get("publication_date"),
            "page_views_total": (res.get("page_views") or {}).get("page_views_total"),
            "download_count": res.get("download_count"),
            "domain_category": cls.get("domain_category"),
            "categories": json.dumps(cls.get("categories")),
            "domain_tags": json.dumps(cls.get("domain_tags")),
            "owner": json.dumps(r.get("owner")),
            "creator": json.dumps(r.get("creator")),
            "resource_json": json.dumps(res),
            "classification_json": json.dumps(cls),
        })

        # Unnest column arrays
        field_names = res.get("columns_field_name") or []
        datatypes = res.get("columns_datatype") or []
        descriptions = res.get("columns_description") or []
        display_names = res.get("columns_name") or []

        for i, fname in enumerate(field_names):
            col_rows.append({
                "resource_id": resource_id,
                "domain": domain,
                "ordinal_position": i + 1,
                "field_name": fname,
                "display_name": display_names[i] if i < len(display_names) else None,
                "data_type": datatypes[i] if i < len(datatypes) else None,
                "description": descriptions[i] if i < len(descriptions) else None,
            })

    # Convert to pyarrow for zero-copy registration with DuckDB.
    import pyarrow as pa

    res_table = pa.Table.from_pylist(res_rows)
    conn.register("_resources", res_table)
    conn.execute(f"COPY _resources TO '{resource_path}' (FORMAT PARQUET)")
    conn.unregister("_resources")

    if col_rows:
        col_table = pa.Table.from_pylist(col_rows)
        conn.register("_columns", col_table)
        conn.execute(f"COPY _columns TO '{column_path}' (FORMAT PARQUET)")
        conn.unregister("_columns")

    return len(res_rows), len(col_rows)


def main():
    import argparse
    from pathlib import Path

    parser = argparse.ArgumentParser(description="Socrata catalog → DuckLake")
    parser.add_argument("--domains", nargs="*", default=["data.cityofnewyork.us"],
                        help="Socrata domains to fetch")
    parser.add_argument("--all", action="store_true",
                        help="Fetch all domains from seed catalog")
    args = parser.parse_args()

    if args.all:
        # Use the existing 10K catalog to seed domain list
        seed_path = (Path(__file__).resolve().parent.parent.parent.parent
                     / "sql" / "socrata" / "all_socrata_catalog.json")
        if seed_path.exists():
            with open(seed_path) as f:
                seed = json.load(f)
            domains = sorted({
                r.get("metadata", {}).get("domain", "")
                for r in seed if r.get("metadata", {}).get("domain")
            })
            print(f"Found {len(domains)} domains from seed catalog")
        else:
            print(f"Seed catalog not found at {seed_path}")
            return
    else:
        domains = args.domains

    # Fetch catalogs
    all_resources = []
    t_start = time.time()
    for domain in domains:
        print(f"Fetching {domain}...", end=" ", flush=True)
        resources, _total = fetch_domain_catalog(domain)
        print(f"{len(resources)} datasets")
        all_resources.extend(resources)

    print(f"\nTotal: {len(all_resources)} resources from {len(domains)} domains "
          f"in {time.time() - t_start:.0f}s")

    if not all_resources:
        print("Nothing to write.")
        return

    # Write to MinIO as Parquet
    conn = duckdb.connect()
    conn.execute(_s3_secret_sql())

    from datetime import datetime, timezone
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    resource_path = f"s3://{S3_BUCKET}/catalog/socrata_resource/{ts}.parquet"
    column_path = f"s3://{S3_BUCKET}/catalog/socrata_resource_column/{ts}.parquet"

    print(f"\nWriting Parquet files...")
    n_res, n_cols = catalog_to_parquet(all_resources, conn, resource_path, column_path)
    print(f"  socrata_resource: {n_res:,} rows → {resource_path}")
    print(f"  socrata_resource_column: {n_cols:,} rows → {column_path}")

    # Verify
    res_cnt = conn.execute(f"SELECT count(*) FROM read_parquet('{resource_path}')").fetchone()[0]
    col_cnt = conn.execute(f"SELECT count(*) FROM read_parquet('{column_path}')").fetchone()[0]
    print(f"\nVerified: {res_cnt:,} resources, {col_cnt:,} columns")

    # Show sample
    print(f"\nSample resources:")
    rows = conn.execute(
        f"SELECT domain, resource_id, name, data_updated_at "
        f"FROM read_parquet('{resource_path}') "
        f"ORDER BY data_updated_at DESC LIMIT 5"
    ).fetchall()
    for row in rows:
        print(f"  {row[0]:>30}  {row[1]}  {row[3][:19] if row[3] else '?':>19}  {row[2][:50]}")

    # Domain summary
    print(f"\nResources by domain:")
    domain_counts = conn.execute(
        f"SELECT domain, count(*) AS n FROM read_parquet('{resource_path}') "
        f"GROUP BY domain ORDER BY n DESC LIMIT 10"
    ).fetchall()
    for d, n in domain_counts:
        print(f"  {n:>6}  {d}")

    conn.close()
    print(f"\nDone. These files can be registered in DuckLake as snapshots for PIT time travel.")


if __name__ == "__main__":
    main()
