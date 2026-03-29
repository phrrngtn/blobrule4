"""
blobrule4.socrata.scraper — Scrape Socrata Discovery API catalog into PostgreSQL TTST tables.

Thin client: paginates the Discovery API and feeds each page's JSON array
to the socrata.intern_catalog() stored procedure, which does all the
shredding and TTST interning in SQL.

Usage:
    uv run python -m blobrule4.socrata.scraper
    uv run python -m blobrule4.socrata.scraper --domains opendata.utah.gov data.texas.gov
    uv run python -m blobrule4.socrata.scraper --discover
    uv run python -m blobrule4.socrata.scraper --schema
"""

import json
import os
import time
import urllib.request
from pathlib import Path

import psycopg2

PG_DSN = os.environ.get("PG_URL", "dbname=rule4_test host=localhost gssencmode=disable")

CATALOG_API = "https://api.us.socrata.com/api/catalog/v1"
PAGE_SIZE = 100

# SQL files bundled with this package
# blobrule4/python/blobrule4/socrata/scraper.py → blobrule4/sql/socrata/
_SQL_DIR = Path(__file__).resolve().parents[3] / "sql" / "socrata"


def fetch_domain_catalog(domain, max_resources=50000):
    """Paginate through a domain's Discovery API catalog.

    Returns (results_list, total_count).
    """
    resources = []
    offset = 0
    total = 0
    while offset < max_resources:
        url = (f"{CATALOG_API}?domains={urllib.request.quote(domain)}"
               f"&only=datasets&limit={PAGE_SIZE}&offset={offset}")
        try:
            with urllib.request.urlopen(url, timeout=30) as resp:
                data = json.loads(resp.read())
        except Exception as e:
            print(f"  WARN: {domain} offset={offset}: {e}")
            break

        batch = data.get("results", [])
        if not batch:
            break
        resources.extend(batch)
        total = data.get("resultSetSize", 0)
        offset += len(batch)
        if offset >= total:
            break

    return resources, total


def discover_domains():
    """Discover Socrata domains from the global Discovery API.

    The /api/catalog/v1/domains endpoint is 404 (removed in SODA3).
    Instead, fetch the global catalog (capped at 10K) and extract unique domains.
    """
    print("Discovering domains from global catalog (limit 10000)...")
    url = f"{CATALOG_API}?only=datasets&limit=10000"
    with urllib.request.urlopen(url, timeout=60) as resp:
        data = json.loads(resp.read())

    domains = {}
    for r in data.get("results", []):
        d = r.get("metadata", {}).get("domain", "")
        if d:
            domains[d] = domains.get(d, 0) + 1

    return sorted(domains.items(), key=lambda x: -x[1])


def sql_dir():
    """Return the path to the bundled SQL directory."""
    return _SQL_DIR


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Scrape Socrata catalog → PostgreSQL TTST")
    parser.add_argument("--domains", nargs="*",
                        default=["data.cityofnewyork.us", "data.cityofchicago.org",
                                 "data.texas.gov", "data.colorado.gov", "opendata.utah.gov"],
                        help="Socrata domains to scrape")
    parser.add_argument("--discover", action="store_true",
                        help="Discover domains from global catalog and insert into socrata.domain")
    parser.add_argument("--schema", action="store_true",
                        help="Create schema + stored procedure and exit")
    args = parser.parse_args()

    conn = psycopg2.connect(PG_DSN)

    if args.schema:
        for sql_file in ["socrata_schema.sql", "socrata_intern.sql"]:
            path = _SQL_DIR / sql_file
            with open(path) as f:
                sql = f.read()
            with conn.cursor() as cur:
                cur.execute(sql)
            conn.commit()
            print(f"  Executed {sql_file}")
        print("Schema and stored procedure created.")
        conn.close()
        return

    if args.discover:
        domain_counts = discover_domains()
        print(f"Found {len(domain_counts)} domains:")
        with conn.cursor() as cur:
            for d, n in domain_counts[:50]:
                print(f"  {n:>5}  {d}")
                cur.execute("""
                    INSERT INTO socrata.domain (domain, resource_count)
                    VALUES (%(domain)s, %(resource_count)s)
                    ON CONFLICT (domain) DO UPDATE
                        SET resource_count = EXCLUDED.resource_count
                """, {"domain": d, "resource_count": n})
        conn.commit()
        print(f"\nInserted/updated {min(len(domain_counts), 50)} domains into socrata.domain")
        conn.close()
        return

    t_start = time.time()
    total_resources = 0

    for domain in args.domains:
        print(f"\n{'='*60}")
        print(f"Scraping {domain}...")
        t0 = time.time()

        results, resource_count = fetch_domain_catalog(domain)
        print(f"  Fetched {len(results)} resources ({resource_count} total on domain) "
              f"in {time.time() - t0:.0f}s")

        if not results:
            print(f"  No results, skipping.")
            continue

        # Feed the entire results array to the stored procedure.
        # The sproc does all shredding and TTST interning in SQL.
        with conn.cursor() as cur:
            cur.execute(
                "CALL socrata.intern_catalog(%(domain)s, %(payload)s::jsonb)",
                {"domain": domain, "payload": json.dumps(results)}
            )
        conn.commit()

        elapsed_domain = time.time() - t0
        print(f"  Interned in {elapsed_domain:.1f}s")
        total_resources += len(results)

    elapsed = time.time() - t_start
    print(f"\n{'='*60}")
    print(f"Done: {total_resources} resources from {len(args.domains)} domains in {elapsed:.0f}s")
    conn.close()


if __name__ == "__main__":
    main()
