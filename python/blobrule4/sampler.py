"""
Schema sampler: scrape catalog metadata from ODBC dataservers into DuckDB.

Usage:
    uv run --with duckdb --with sqlalchemy --with duckdb-engine \
        python -m blobodbc.sampler /path/to/sample.duckdb [--schema dbo] [--dataserver sqlserver_docker]

The sampler:
  1. Opens (or creates) the DuckDB database
  2. Ensures the sample log tables exist (via SQLAlchemy create_all)
  3. Loads the blobodbc extension
  4. For each registered dataserver (or the one specified):
     a. Determines the dialect from SQL_DBMS_NAME
     b. Looks up catalog queries from blobrule4.catalog_queries
     c. Runs each query against each (catalog, schema) pair
     d. Stores the raw JSON payload into the appropriate *_sample_log table
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone

import duckdb
import sqlalchemy as sa

from blobrule4.models import Base, Dataserver, SAMPLE_LOG_CLASSES

EXTENSION_PATH = None  # Set at runtime


def find_extensions():
    """Locate blobodbc and blobtemplates DuckDB extensions."""
    import os
    checkouts = os.path.realpath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
    result = {}
    for name in ["blobodbc", "blobtemplates"]:
        path = os.path.join(checkouts, name, "build", "duckdb", f"{name}.duckdb_extension")
        if os.path.exists(path):
            result[name] = path
        else:
            raise FileNotFoundError(f"Cannot find {name}.duckdb_extension at {path}")
    return result


def build_conn_str(ds):
    """Build an ODBC connection string from a Dataserver row."""
    parts = [f"Driver={{{ds.driver}}}"]

    if ds.host is not None:
        server = ds.host
        if ds.port is not None:
            server += f",{ds.port}"
        parts.append(f"Server={server}")

    if ds.default_catalog is not None:
        parts.append(f"Database={ds.default_catalog}")

    if ds.auth_method == "sql_login":
        parts.append(f"UID={ds.username or ''}")
        # Resolve secret — for now, just support env: prefix
        pwd = resolve_secret(ds.secret_ref) if ds.secret_ref else ""
        parts.append(f"PWD={pwd}")
    elif ds.auth_method in ("trusted", "kerberos"):
        parts.append("Trusted_Connection=yes")

    if ds.extra_attrs:
        try:
            attrs = json.loads(ds.extra_attrs)
            for k, v in attrs.items():
                parts.append(f"{k}={v}")
        except (json.JSONDecodeError, TypeError):
            pass

    return ";".join(parts)


def resolve_secret(ref):
    """Resolve a secret_ref to its value.  Stub implementation."""
    import os
    if not ref:
        return ""
    if ref.startswith("env:"):
        env_name = ref[4:]
        val = os.environ.get(env_name, "")
        if val:
            return val
    # Development fallbacks — these should be replaced by Vault/OpenBao
    _dev_secrets = {
        "env:RULE4_SS_PASSWORD": "R4Developer!2024",
    }
    return _dev_secrets.get(ref, "")


def detect_dialect(duck, conn_str):
    """Detect the DBMS dialect via bo_driver_info."""
    raw = duck.execute(
        "SELECT bo_driver_info(?)", [conn_str]
    ).fetchone()[0]
    info = json.loads(raw)
    dbms = info.get("get_info", {}).get("SQL_DBMS_NAME", "")
    if "SQL Server" in dbms:
        return "sqlserver"
    elif "PostgreSQL" in dbms:
        return "postgresql"
    elif "DuckDB" in dbms:
        return "duckdb"
    else:
        return "information_schema"


def get_schemas(duck, conn_str):
    """Get the list of schemas from the driver."""
    raw = duck.execute(
        "SELECT bo_driver_info(?)", [conn_str]
    ).fetchone()[0]
    info = json.loads(raw)
    schemas = []
    for s in info.get("schemas", []):
        name = s.get("TABLE_SCHEM", "")
        if name:
            schemas.append(name)
    return schemas


def get_catalogs(duck, conn_str):
    """Get the list of catalogs from the driver."""
    raw = duck.execute(
        "SELECT bo_driver_info(?)", [conn_str]
    ).fetchone()[0]
    info = json.loads(raw)
    catalogs = []
    for c in info.get("catalogs", []):
        name = c.get("TABLE_CAT", "")
        if name:
            catalogs.append(name)
    return catalogs


def run_catalog_query(duck, conn_str, query_sql, where_fragments, params):
    """Execute a catalog query with dynamic WHERE assembly."""
    sql = query_sql.rstrip()
    active_params = {k: v for k, v in params.items() if v is not None}

    clauses = []
    for pname, fragment in where_fragments.items():
        if pname in active_params:
            clauses.append(fragment)

    if clauses:
        if "WHERE" in sql.upper():
            sql += "\n    AND " + "\n    AND ".join(clauses)
        else:
            sql += "\n  WHERE " + "\n    AND ".join(clauses)

    bind_json = json.dumps(active_params)
    raw = duck.execute(
        "SELECT bo_query_named(?, ?, ?)", [conn_str, sql, bind_json]
    ).fetchone()[0]
    return raw


def sample_schema(duck, ds_id, conn_str, catalog_name, schema_name,
                   dialect, catalog_queries, schema_filter=None):
    """Sample all catalog query kinds for one (dataserver, catalog, schema)."""
    if schema_filter and schema_name not in schema_filter:
        return

    sample_time = datetime.now(timezone.utc).isoformat()
    sampled = 0

    for query_name, log_class in SAMPLE_LOG_CLASSES.items():
        table_name = log_class.__tablename__

        # Find the catalog query — prefer dialect-specific, fall back to information_schema
        query_row = None
        for d in [dialect, "information_schema"]:
            key = (d, query_name)
            if key in catalog_queries:
                query_row = catalog_queries[key]
                break

        if query_row is None:
            continue

        query_sql = query_row["sql"]
        where_fragments = json.loads(query_row["where_fragments"]) if query_row["where_fragments"] else {}
        params = {"schema_name": schema_name, "table_name": None}

        t0 = time.monotonic()
        error = None
        payload = "[]"
        try:
            payload = run_catalog_query(duck, conn_str, query_sql, where_fragments, params)
        except Exception as e:
            error = str(e)

        duration_ms = (time.monotonic() - t0) * 1000

        duck.execute(
            f"INSERT INTO {table_name} "
            "(dataserver_id, catalog_name, schema_name, sample_time, duration_ms, payload, error) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            [ds_id, catalog_name, schema_name, sample_time, duration_ms, payload, error],
        )
        sampled += 1

    print(f"    {schema_name}: sampled {sampled} kinds", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(description="Sample ODBC catalog metadata into DuckDB")
    parser.add_argument("database", help="Path to DuckDB database file")
    parser.add_argument("--dataserver", help="Sample only this dataserver (by name)")
    parser.add_argument("--schema", action="append", dest="schemas",
                        help="Sample only these schemas (can repeat)")
    parser.add_argument("--catalog", help="Sample only this catalog")
    parser.add_argument("--extension", help="Path to blobodbc.duckdb_extension")
    args = parser.parse_args()

    # Open DuckDB and load extensions
    duck = duckdb.connect(args.database, config={"allow_unsigned_extensions": "true"})
    if args.extension:
        duck.execute(f"LOAD '{args.extension}'")
    else:
        for name, path in find_extensions().items():
            duck.execute(f"LOAD '{path}'")

    # Create tables — generate DDL from SA models using DuckDB dialect
    from duckdb_engine import Dialect as DuckDBDialect
    mock_dialect = DuckDBDialect()
    for table in Base.metadata.sorted_tables:
        ddl = str(sa.schema.CreateTable(table, if_not_exists=True).compile(
            dialect=mock_dialect))
        duck.execute(ddl)

    # Load catalog queries from YAML files
    import yaml
    catalog_dir = os.path.join(os.path.dirname(__file__), "..", "..", "catalog")
    catalog_dir = os.path.realpath(catalog_dir)
    catalog_queries = {}
    for dialect in os.listdir(catalog_dir):
        dialect_dir = os.path.join(catalog_dir, dialect)
        if not os.path.isdir(dialect_dir) or dialect.startswith("."):
            continue
        for fname in os.listdir(dialect_dir):
            if not fname.endswith(".yml"):
                continue
            with open(os.path.join(dialect_dir, fname)) as f:
                spec = yaml.safe_load(f)
            where_frags = {}
            for pname, pspec in spec.get("parameters", {}).items():
                if "where" in pspec:
                    where_frags[pname] = pspec["where"]
            catalog_queries[(dialect, fname.replace(".yml", ""))] = {
                "sql": spec.get("sql", "").rstrip(),
                "where_fragments": json.dumps(where_frags),
            }

    print(f"Loaded {len(catalog_queries)} catalog queries from {catalog_dir}", file=sys.stderr)

    # Get dataservers from DuckDB directly
    ds_query = "SELECT dataserver_id, name, driver, host, port, default_catalog, auth_method, username, secret_ref, extra_attrs FROM rule4_dataserver"
    if args.dataserver:
        ds_query += f" WHERE name = '{args.dataserver}'"

    dataservers = duck.execute(ds_query).fetchall()
    ds_columns = ["dataserver_id", "name", "driver", "host", "port",
                  "default_catalog", "auth_method", "username", "secret_ref", "extra_attrs"]

    if not dataservers:
        print("No dataservers found. Seed the dataserver table first.", file=sys.stderr)
        sys.exit(1)

    for ds_row in dataservers:
        ds = dict(zip(ds_columns, ds_row))

        # Build a simple namespace for build_conn_str
        class DS:
            pass
        ds_obj = DS()
        for k, v in ds.items():
            setattr(ds_obj, k, v)

        conn_str = build_conn_str(ds_obj)
        print(f"\n=== {ds['name']} ===", file=sys.stderr)

        try:
            dialect = detect_dialect(duck, conn_str)
        except Exception as e:
            print(f"  Cannot connect: {e}", file=sys.stderr)
            continue

        print(f"  Dialect: {dialect}", file=sys.stderr)

        catalogs = get_catalogs(duck, conn_str)
        if args.catalog:
            catalogs = [c for c in catalogs if c == args.catalog]

        for catalog_name in catalogs:
            print(f"  Catalog: {catalog_name}", file=sys.stderr)

            if len(catalogs) > 1 and catalog_name != ds["default_catalog"]:
                print(f"    (skipping non-default catalog for now)", file=sys.stderr)
                continue

            schemas = get_schemas(duck, conn_str)

            skip_schemas = {
                "pg_catalog", "pg_toast", "information_schema",
                "sys", "INFORMATION_SCHEMA", "guest",
            }
            schemas = [s for s in schemas
                       if s not in skip_schemas
                       and not s.startswith("pg_toast_temp_")
                       and not s.startswith("pg_temp_")]

            for schema_name in schemas:
                sample_schema(
                    duck, ds["dataserver_id"], conn_str,
                    catalog_name, schema_name,
                    dialect, catalog_queries,
                    schema_filter=args.schemas,
                )

    duck.close()
    print(f"\nDone. Results in {args.database}", file=sys.stderr)


if __name__ == "__main__":
    main()
