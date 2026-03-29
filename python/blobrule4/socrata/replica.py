"""
blobrule4.socrata.replica — Create and maintain Socrata data replicas.

Reads resource metadata from the PG TTST catalog (socrata.resource_column),
builds SQLAlchemy table definitions, creates tables in each target database,
loads data from Socrata SODA2 API, and reports per-resource high-water marks.

The same SQLAlchemy MetaData drives all three dialects. The same hwm query
works against all three. Each database is a self-describing replica: its
sync state is inferred from the data it holds, not from an external ledger.

Usage:
    uv run python -m blobrule4.socrata.replica --create
    uv run python -m blobrule4.socrata.replica --load
    uv run python -m blobrule4.socrata.replica --hwm
    uv run python -m blobrule4.socrata.replica --create --load --hwm
"""

import argparse
import json
import os
import time
import urllib.parse
import urllib.request

import psycopg2
from sqlalchemy import (
    MetaData, Table, Column, String, func, select,
    create_engine, quoted_name,
)

from blobrule4.socrata.types import socrata_sa_type

PG_DSN = os.environ.get("PG_URL", "dbname=rule4_test host=localhost gssencmode=disable")
PG_REPLICA_URL = os.environ.get(
    "PG_REPLICA_URL", "postgresql://localhost/rule4_test?gssencmode=disable"
)
REPLICA_SCHEMA = "socrata_replica"
SQLITE_PATH = os.environ.get("SQLITE_REPLICA_PATH", "socrata_replica.sqlite")
DUCKDB_PATH = os.environ.get("DUCKDB_REPLICA_PATH", "socrata_replica.duckdb")

# Small, interesting demo resources across multiple domains
DEMO_RESOURCES = [
    ("data.cityofnewyork.us", "vfnx-vebw"),   # Squirrel Census (3K rows)
    ("data.cityofnewyork.us", "25th-nujf"),    # Popular Baby Names (22K rows)
    ("data.cityofnewyork.us", "hc8x-tcnd"),    # FDNY Firehouse Listing (219 rows)
    ("data.cityofnewyork.us", "ncbg-6agr"),    # DOF Parking Violation Codes (97 rows)
    ("data.cityofnewyork.us", "7zb8-7bpk"),    # Property Tax Rates (28 rows)
    ("data.cityofchicago.org", "z8bn-74gv"),   # Police Stations (23 rows)
    ("data.cityofchicago.org", "28km-gtjn"),    # Fire Stations (92 rows)
    ("opendata.utah.gov", "ierb-h3t5"),        # Hospital Characteristics (129 rows)
]

SODA_LIMIT = 50000


def metadata_from_pg(pg_conn, resources, schema=None):
    """Build SQLAlchemy MetaData from PG TTST catalog for given (domain, resource_id) pairs.

    Adds :id and :updated_at system columns to every table — these are Socrata
    system fields not listed in the catalog but present in every SODA2 response.
    """
    meta = MetaData(schema=schema)

    with pg_conn.cursor() as cur:
        for domain, resource_id in resources:
            cur.execute("""
                SELECT field_name, ordinal_position, data_type
                FROM socrata.resource_column
                WHERE domain = %(domain)s
                  AND resource_id = %(resource_id)s
                  AND tt_end = '9999-12-31'
                ORDER BY ordinal_position
            """, {"domain": domain, "resource_id": resource_id})

            rows = cur.fetchall()
            if not rows:
                print(f"  WARN: no columns for {domain}/{resource_id}")
                continue

            columns = [
                Column(quoted_name(":id", quote=True), String),
                Column(quoted_name(":updated_at", quote=True), String),
            ]
            for field_name, _ordinal, data_type in rows:
                columns.append(Column(
                    quoted_name(field_name, quote=True),
                    socrata_sa_type(data_type),
                ))

            Table(resource_id, meta, *columns)

    return meta


def _make_engines():
    """Create SQLAlchemy engines for all three target databases."""
    engines = {}

    engines["sqlite"] = create_engine(f"sqlite:///{SQLITE_PATH}")

    engines["postgresql"] = create_engine(PG_REPLICA_URL)

    engines["duckdb"] = create_engine(f"duckdb:///{DUCKDB_PATH}")

    return engines


def create_tables(engines, meta_by_dialect):
    """Create tables in all target databases."""
    for dialect_name, engine in engines.items():
        t0 = time.time()
        m = meta_by_dialect[dialect_name]
        m.create_all(engine, checkfirst=True)
        print(f"  {dialect_name}: {len(m.tables)} tables in {time.time()-t0:.1f}s")


def fetch_soda(domain, resource_id, limit=SODA_LIMIT):
    """Fetch data from Socrata SODA2 API as JSON list of dicts.

    Explicitly selects :id and :updated_at system fields (not returned by default)
    plus all user columns via *.
    """
    params = urllib.parse.urlencode({
        "$select": "*,:id,:updated_at",
        "$limit": str(limit),
        "$order": ":updated_at DESC",
    })
    url = f"https://{domain}/resource/{resource_id}.json?{params}"
    with urllib.request.urlopen(url, timeout=60) as resp:
        return json.loads(resp.read())


def _normalize_rows(rows, table):
    """Normalize SODA2 JSON rows for SQLAlchemy bulk insert.

    - Only include columns present in the table definition
    - Fill missing keys with None (sparse rows break SA bulk insert)
    - Serialize dicts/lists to JSON strings (geo columns come as objects)
    """
    col_names = {c.name for c in table.columns}
    clean = []
    for row in rows:
        normalized = {}
        for col_name in col_names:
            val = row.get(col_name)
            if isinstance(val, (dict, list)):
                val = json.dumps(val)
            normalized[col_name] = val
        clean.append(normalized)
    return clean


def load_data(engines, meta_by_dialect, resources):
    """Load data from Socrata into all three databases."""
    # Use any dialect's metadata to get table names (they're the same)
    any_meta = next(iter(meta_by_dialect.values()))

    for domain, resource_id in resources:
        if resource_id not in any_meta.tables:
            continue

        print(f"\n  {resource_id} ({domain})...", end=" ", flush=True)

        try:
            rows = fetch_soda(domain, resource_id, SODA_LIMIT)
        except Exception as e:
            print(f"FETCH ERROR: {e}")
            continue

        if not rows:
            print("0 rows")
            continue

        print(f"{len(rows)} rows", end="", flush=True)

        for dialect_name, engine in engines.items():
            table = meta_by_dialect[dialect_name].tables[
                f"{REPLICA_SCHEMA}.{resource_id}" if dialect_name == "postgresql"
                else resource_id
            ]
            clean_rows = _normalize_rows(rows, table)
            try:
                with engine.begin() as conn:
                    conn.execute(table.delete())
                    conn.execute(table.insert(), clean_rows)
                print(f" [{dialect_name}:ok]", end="", flush=True)
            except Exception as e:
                err = str(e).split('\n')[0][:80]
                print(f" [{dialect_name}:FAIL {err}]", end="", flush=True)

        print()


def get_hwm(engine, table):
    """Get max(:updated_at) for a table via SQLAlchemy expression API."""
    updated_at = table.c[":updated_at"]
    stmt = select(func.max(updated_at))
    with engine.connect() as conn:
        row = conn.execute(stmt).fetchone()
    return row[0] if row and row[0] else None


def report_hwm(engines, meta_by_dialect, resources):
    """Report per-resource high-water mark from each database."""
    any_meta = next(iter(meta_by_dialect.values()))
    dialect_names = list(engines.keys())

    print(f"\n{'resource_id':>12}", end="")
    for d in dialect_names:
        print(f"  {d:>26}", end="")
    print()
    print(f"{'':>12}", end="")
    for _ in dialect_names:
        print(f"  {'':->26}", end="")
    print()

    for _domain, resource_id in resources:
        tbl_key = resource_id
        if tbl_key not in any_meta.tables:
            continue

        print(f"{resource_id:>12}", end="")
        for dialect_name in dialect_names:
            table = meta_by_dialect[dialect_name].tables[
                f"{REPLICA_SCHEMA}.{resource_id}" if dialect_name == "postgresql"
                else resource_id
            ]
            try:
                hwm = get_hwm(engines[dialect_name], table)
                val = str(hwm)[:26] if hwm else "-"
            except Exception as e:
                val = f"ERR:{e}"[:26]
            print(f"  {val:>26}", end="")
        print()


def main():
    parser = argparse.ArgumentParser(description="Socrata replica sync across 3 dialects")
    parser.add_argument("--create", action="store_true", help="Create tables")
    parser.add_argument("--load", action="store_true", help="Load data from Socrata")
    parser.add_argument("--hwm", action="store_true", help="Report high-water marks")
    args = parser.parse_args()

    if not (args.create or args.load or args.hwm):
        parser.print_help()
        return

    # Build one MetaData per dialect — same tables, different schema settings
    pg_conn = psycopg2.connect(PG_DSN)
    meta_plain = metadata_from_pg(pg_conn, DEMO_RESOURCES)
    meta_pg = metadata_from_pg(pg_conn, DEMO_RESOURCES, schema=REPLICA_SCHEMA)
    pg_conn.close()
    print(f"Metadata: {len(meta_plain.tables)} tables from PG TTST catalog")

    meta_by_dialect = {
        "sqlite": meta_plain,
        "postgresql": meta_pg,
        "duckdb": metadata_from_pg(  # need a separate MetaData instance for DuckDB
            psycopg2.connect(PG_DSN), DEMO_RESOURCES
        ),
    }

    engines = _make_engines()

    # Ensure PG schema exists
    with engines["postgresql"].connect() as conn:
        from sqlalchemy import text
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{REPLICA_SCHEMA}"'))
        conn.commit()

    if args.create:
        print(f"\nCreating tables...")
        create_tables(engines, meta_by_dialect)

    if args.load:
        print(f"\nLoading data from Socrata...")
        load_data(engines, meta_by_dialect, DEMO_RESOURCES)

    if args.hwm:
        print(f"\nHigh-water marks (max :updated_at per resource per database):")
        report_hwm(engines, meta_by_dialect, DEMO_RESOURCES)

    for engine in engines.values():
        engine.dispose()


if __name__ == "__main__":
    main()
