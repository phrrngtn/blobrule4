"""
blobrule4.socrata.discover — Search Socrata catalog metadata via full-text search.

Queries the PostgreSQL socrata.resource and socrata.resource_column tables
using FTS indexes to find datasets and columns matching search terms.

Supports three search modes:
  - Resource search: find datasets by name/description
  - Column search: find columns by field_name/display_name/description
  - Combined: find datasets that have columns matching the terms

Usage:
    uv run python -m blobrule4.socrata.discover "heat pump water heater"
    uv run python -m blobrule4.socrata.discover --columns "model_number brand_name efficiency"
    uv run python -m blobrule4.socrata.discover --terms "QAHV" "Mitsubishi" "COP" "UEF"
"""

import os

import psycopg2

PG_DSN = os.environ.get("PG_URL", "dbname=rule4_test host=/tmp")


def _build_tsquery(terms, operator="or"):
    """Build a tsquery from a list of terms.

    Args:
        terms: List of search terms (words or phrases).
        operator: 'or' for any match, 'and' for all match.
    """
    op = " | " if operator == "or" else " & "
    # Split multi-word terms into individual words, join with operator
    words = []
    for t in terms:
        words.extend(t.strip().split())
    escaped = [w.replace("'", "''") for w in words if w]
    return op.join(escaped)


def search_resources(conn, terms, operator="or", limit=20, domain=None):
    """Search socrata.resource by name/description.

    Returns list of (domain, resource_id, name, description_excerpt, rank).
    """
    tsq = _build_tsquery(terms, operator)
    domain_filter = "AND r.domain = %(domain)s" if domain else ""

    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT r.domain, r.resource_id, r.name,
                   LEFT(r.description, 200) AS description_excerpt,
                   ts_rank(r.fts, query) AS rank
            FROM socrata.resource AS r,
                 to_tsquery('english', %(tsquery)s) AS query
            WHERE r.tt_end = '9999-12-31'
              AND r.fts @@ query
              {domain_filter}
            ORDER BY rank DESC
            LIMIT %(limit)s
        """, {"tsquery": tsq, "limit": limit, "domain": domain})
        return cur.fetchall()


def search_columns(conn, terms, operator="or", limit=30, domain=None):
    """Search socrata.resource_column by field_name/display_name/description.

    Returns list of (domain, resource_id, field_name, display_name, data_type,
    description_excerpt, rank).
    """
    tsq = _build_tsquery(terms, operator)
    domain_filter = "AND c.domain = %(domain)s" if domain else ""

    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT c.domain, c.resource_id, c.field_name, c.display_name,
                   c.data_type, LEFT(c.description, 200) AS description_excerpt,
                   ts_rank(c.fts, query) AS rank
            FROM socrata.resource_column AS c,
                 to_tsquery('english', %(tsquery)s) AS query
            WHERE c.tt_end = '9999-12-31'
              AND c.fts @@ query
              {domain_filter}
            ORDER BY rank DESC
            LIMIT %(limit)s
        """, {"tsquery": tsq, "limit": limit, "domain": domain})
        return cur.fetchall()


def search_resources_by_columns(conn, terms, operator="or", limit=20, domain=None):
    """Find resources that have columns matching the search terms.

    Groups by resource, ranks by sum of column match ranks.
    Returns list of (domain, resource_id, resource_name, matching_columns, total_rank).
    """
    tsq = _build_tsquery(terms, operator)
    domain_filter = "AND c.domain = %(domain)s" if domain else ""

    with conn.cursor() as cur:
        cur.execute(f"""
            WITH COLUMN_HITS AS (
                SELECT c.domain, c.resource_id, c.field_name, c.display_name,
                       ts_rank(c.fts, query) AS rank
                FROM socrata.resource_column AS c,
                     to_tsquery('english', %(tsquery)s) AS query
                WHERE c.tt_end = '9999-12-31'
                  AND c.fts @@ query
                  {domain_filter}
            ),
            RESOURCE_SCORES AS (
                SELECT h.domain, h.resource_id,
                       COUNT(*) AS matching_columns,
                       SUM(h.rank) AS total_rank,
                       STRING_AGG(h.field_name, ', ' ORDER BY h.rank DESC) AS column_names
                FROM COLUMN_HITS AS h
                GROUP BY h.domain, h.resource_id
            )
            SELECT s.domain, s.resource_id, r.name,
                   s.matching_columns, s.column_names, s.total_rank
            FROM RESOURCE_SCORES AS s
            JOIN socrata.resource AS r
                ON s.domain = r.domain
                AND s.resource_id = r.resource_id
                AND r.tt_end = '9999-12-31'
            ORDER BY s.total_rank DESC
            LIMIT %(limit)s
        """, {"tsquery": tsq, "limit": limit, "domain": domain})
        return cur.fetchall()


def main():
    import argparse
    parser = argparse.ArgumentParser(
        description="Search Socrata catalog metadata via full-text search")
    parser.add_argument("query", nargs="*",
                        help="Search terms (space-separated)")
    parser.add_argument("--terms", nargs="*",
                        help="Additional search terms")
    parser.add_argument("--columns", action="store_true",
                        help="Search column metadata instead of resources")
    parser.add_argument("--by-columns", action="store_true",
                        help="Find resources ranked by matching column count")
    parser.add_argument("--and", dest="use_and", action="store_true",
                        help="Require all terms to match (default: any)")
    parser.add_argument("--domain", default=None,
                        help="Restrict to a specific Socrata domain")
    parser.add_argument("--limit", type=int, default=20,
                        help="Max results")
    args = parser.parse_args()

    all_terms = (args.query or []) + (args.terms or [])
    if not all_terms:
        parser.print_help()
        return

    operator = "and" if args.use_and else "or"
    conn = psycopg2.connect(PG_DSN)

    if args.columns:
        results = search_columns(conn, all_terms, operator, args.limit, args.domain)
        print(f"Column matches for: {' '.join(all_terms)}\n")
        print(f"{'domain':25s} {'resource_id':12s} {'field_name':40s} {'type':10s} {'rank':>6s}")
        print("-" * 100)
        for domain, rid, fname, dname, dtype, desc, rank in results:
            print(f"{domain:25s} {rid:12s} {fname:40s} {dtype or '':10s} {rank:6.4f}")
            if dname and dname != fname:
                print(f"{'':25s} {'':12s}   display: {dname}")
            if desc:
                print(f"{'':25s} {'':12s}   {desc[:80]}")

    elif args.by_columns:
        results = search_resources_by_columns(
            conn, all_terms, operator, args.limit, args.domain)
        print(f"Resources with matching columns for: {' '.join(all_terms)}\n")
        print(f"{'domain':25s} {'resource_id':12s} {'#cols':>5s} {'rank':>7s} {'name':40s}")
        print("-" * 95)
        for domain, rid, name, ncols, col_names, rank in results:
            print(f"{domain:25s} {rid:12s} {ncols:>5d} {rank:7.4f} {(name or '')[:40]}")
            print(f"{'':25s} {'':12s}        columns: {col_names[:60]}")

    else:
        results = search_resources(conn, all_terms, operator, args.limit, args.domain)
        print(f"Resource matches for: {' '.join(all_terms)}\n")
        print(f"{'domain':25s} {'resource_id':12s} {'rank':>6s} {'name':50s}")
        print("-" * 100)
        for domain, rid, name, desc, rank in results:
            print(f"{domain:25s} {rid:12s} {rank:6.4f} {(name or '')[:50]}")
            if desc:
                print(f"{'':25s} {'':12s}        {desc[:70]}")

    conn.close()


if __name__ == "__main__":
    main()
