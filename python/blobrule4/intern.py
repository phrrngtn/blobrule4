"""
Intern sample logs into the TTST snapshot + reverse patch chain.

For each (dataserver, catalog, schema, kind):
  1. Find unprocessed samples (sample_time > last captured_at, or no snapshot exists)
  2. Nest the raw payload via json_nest with kind-appropriate keys
  3. Diff against the current snapshot
  4. If changed: store reverse patch, update snapshot, bump revision
  5. If unchanged: no-op

Usage:
    cd python && uv run --with duckdb --with sqlalchemy --with duckdb-engine \
        python -m blobodbc.intern /path/to/sample.duckdb
"""

import argparse
import json
import sys
import os

import duckdb

from blobrule4.models import Base, SchemaSnapshot, SchemaSnapshotPatch, SAMPLE_LOG_CLASSES

# Nesting keys per kind — determines the nested JSON structure
# and therefore the semantic quality of the diffs
NEST_KEYS = {
    "tables":       '["table_schema", "table_name"]',
    "columns":      '["table_schema", "table_name", "column_name"]',
    "primary_keys": '["table_schema", "table_name", "constraint_name", "column_name"]',
    "foreign_keys": '["fk_schema", "fk_table", "constraint_name", "fk_column"]',
    "indexes":      '["table_schema", "table_name", "index_name", "column_name"]',
    "triggers":     '["table_schema", "table_name", "trigger_name"]',
    "callables":    '["object_schema", "object_name"]',
}

# Fields to strip before nesting (noisy, non-semantic)
STRIP_FIELDS = {
    "ORDINAL_POSITION",  # PG: global across schema, not per-table
    "TABLE_CAT",         # redundant with the PK
    "TABLE_SCHEM",       # redundant with the PK (ODBC tier)
    "table_catalog",     # redundant (I_S tier)
}

# Derive LOG_TABLES from the model classes
LOG_TABLES = {kind: cls.__tablename__ for kind, cls in SAMPLE_LOG_CLASSES.items()}


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


def ensure_tables(duck):
    """Create TTST tables if they don't exist."""
    duck.execute("""
        CREATE TABLE IF NOT EXISTS rule4_schema_snapshot (
            dataserver_id INTEGER NOT NULL,
            catalog_name VARCHAR NOT NULL,
            schema_name VARCHAR NOT NULL,
            kind VARCHAR NOT NULL,
            revision_num INTEGER NOT NULL DEFAULT 1,
            snapshot TEXT NOT NULL,
            captured_at TIMESTAMP NOT NULL,
            PRIMARY KEY (dataserver_id, catalog_name, schema_name, kind)
        )
    """)
    duck.execute("""
        CREATE TABLE IF NOT EXISTS rule4_schema_snapshot_patch (
            dataserver_id INTEGER NOT NULL,
            catalog_name VARCHAR NOT NULL,
            schema_name VARCHAR NOT NULL,
            kind VARCHAR NOT NULL,
            revision_num INTEGER NOT NULL,
            patch TEXT NOT NULL,
            captured_at TIMESTAMP NOT NULL,
            PRIMARY KEY (dataserver_id, catalog_name, schema_name, kind, revision_num)
        )
    """)


def nest_payload(duck, payload_json, kind):
    """Nest a flat JSON payload using kind-appropriate keys."""
    keys = NEST_KEYS.get(kind)
    if not keys:
        return payload_json

    result = duck.execute(
        "SELECT bt_json_nest(?, ?)", [payload_json, keys]
    ).fetchone()[0]
    return result


def intern_sample(duck, dataserver_id, catalog_name, schema_name, kind,
                   sample_time, payload_json):
    """Intern one sample into the TTST."""
    # Nest the payload
    nested = nest_payload(duck, payload_json, kind)

    # Get current snapshot (if any)
    existing = duck.execute(
        "SELECT revision_num, snapshot FROM rule4_schema_snapshot "
        "WHERE dataserver_id = ? AND catalog_name = ? "
        "AND schema_name = ? AND kind = ?",
        [dataserver_id, catalog_name, schema_name, kind]
    ).fetchone()

    if existing is None:
        # First sample — create initial snapshot
        duck.execute(
            "INSERT INTO rule4_schema_snapshot "
            "(dataserver_id, catalog_name, schema_name, kind, revision_num, snapshot, captured_at) "
            "VALUES (?, ?, ?, ?, 1, ?, ?)",
            [dataserver_id, catalog_name, schema_name, kind, nested, sample_time]
        )
        return "new", 1, 0

    current_rev, current_snapshot = existing

    # Diff: current → new
    diff_raw = duck.execute(
        "SELECT bt_json_from_diff(?, ?)", [current_snapshot, nested]
    ).fetchone()[0]
    diff = json.loads(diff_raw)

    if not diff:
        return "unchanged", current_rev, 0

    # Compute reverse patch: new → current (so we can reconstruct current from new)
    reverse_patch_raw = duck.execute(
        "SELECT bt_json_from_diff(?, ?)", [nested, current_snapshot]
    ).fetchone()[0]

    new_rev = current_rev + 1

    # Store reverse patch (keyed by the revision we're transitioning FROM)
    duck.execute(
        "INSERT INTO rule4_schema_snapshot_patch "
        "(dataserver_id, catalog_name, schema_name, kind, revision_num, patch, captured_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        [dataserver_id, catalog_name, schema_name, kind,
         new_rev, reverse_patch_raw, sample_time]
    )

    # Update snapshot to new state
    duck.execute(
        "UPDATE rule4_schema_snapshot SET revision_num = ?, snapshot = ?, captured_at = ? "
        "WHERE dataserver_id = ? AND catalog_name = ? "
        "AND schema_name = ? AND kind = ?",
        [new_rev, nested, sample_time,
         dataserver_id, catalog_name, schema_name, kind]
    )

    return "changed", new_rev, len(diff)


def reconstruct_at_revision(duck, dataserver_id, catalog_name, schema_name,
                             kind, target_rev):
    """Reconstruct the snapshot at a specific revision by applying reverse patches."""
    current = duck.execute(
        "SELECT revision_num, snapshot FROM rule4_schema_snapshot "
        "WHERE dataserver_id = ? AND catalog_name = ? "
        "AND schema_name = ? AND kind = ?",
        [dataserver_id, catalog_name, schema_name, kind]
    ).fetchone()

    if current is None:
        return None

    current_rev, snapshot = current

    if target_rev == current_rev:
        return snapshot

    if target_rev > current_rev or target_rev < 1:
        return None

    # Apply reverse patches from current_rev down to target_rev + 1
    patches = duck.execute(
        "SELECT revision_num, patch FROM rule4_schema_snapshot_patch "
        "WHERE dataserver_id = ? AND catalog_name = ? "
        "AND schema_name = ? AND kind = ? "
        "AND revision_num <= ? AND revision_num > ? "
        "ORDER BY revision_num DESC",
        [dataserver_id, catalog_name, schema_name, kind,
         current_rev, target_rev]
    ).fetchall()

    doc = snapshot
    for rev_num, patch in patches:
        doc = duck.execute(
            "SELECT bt_json_apply_patch(?, ?)", [doc, patch]
        ).fetchone()[0]

    return doc


def intern_all(duck, verbose=True):
    """Intern all unprocessed samples."""
    for kind, log_table in LOG_TABLES.items():
        # Find all samples not yet interned
        # (sample_time > snapshot.captured_at, or no snapshot exists)
        rows = duck.execute(f"""
            SELECT l.dataserver_id, l.catalog_name, l.schema_name,
                   l.sample_time, l.payload
            FROM {log_table} AS l
            LEFT JOIN rule4_schema_snapshot AS s
                ON s.dataserver_id = l.dataserver_id
               AND s.catalog_name = l.catalog_name
               AND s.schema_name = l.schema_name
               AND s.kind = '{kind}'
            WHERE l.error IS NULL
              AND (s.captured_at IS NULL OR l.sample_time > s.captured_at)
            ORDER BY l.sample_time
        """).fetchall()

        for ds_id, cat, sch, sample_time, payload in rows:
            status, rev, n_ops = intern_sample(
                duck, ds_id, cat, sch, kind, sample_time, payload
            )
            if verbose:
                print(f"  {kind:15s} {cat}/{sch:15s} rev={rev} "
                      f"{status} ({n_ops} ops)", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(description="Intern sample logs into TTST")
    parser.add_argument("database", help="Path to DuckDB database file")
    parser.add_argument("--extension", help="Path to blobodbc.duckdb_extension")
    parser.add_argument("--reconstruct", nargs=2, metavar=("KIND", "REV"),
                        help="Reconstruct snapshot at revision (for testing)")
    args = parser.parse_args()

    duck = duckdb.connect(args.database, config={"allow_unsigned_extensions": "true"})
    if args.extension:
        duck.execute(f"LOAD '{args.extension}'")
    else:
        for name, path in find_extensions().items():
            duck.execute(f"LOAD '{path}'")

    ensure_tables(duck)

    if args.reconstruct:
        kind, rev = args.reconstruct[0], int(args.reconstruct[1])
        # Find the first matching snapshot
        row = duck.execute(
            "SELECT DISTINCT dataserver_id, catalog_name, schema_name "
            "FROM rule4_schema_snapshot WHERE kind = ? LIMIT 1", [kind]
        ).fetchone()
        if row:
            doc = reconstruct_at_revision(duck, row[0], row[1], row[2], kind, rev)
            if doc:
                print(json.dumps(json.loads(doc), indent=2))
            else:
                print(f"Cannot reconstruct revision {rev}", file=sys.stderr)
        return

    print("Interning samples...", file=sys.stderr)
    intern_all(duck)

    # Summary
    stats = duck.execute("""
        SELECT kind, revision_num, captured_at,
               length(snapshot) AS snapshot_bytes
        FROM rule4_schema_snapshot
        ORDER BY kind
    """).fetchall()
    print(f"\nSnapshot summary:", file=sys.stderr)
    for kind, rev, captured, size in stats:
        print(f"  {kind:15s} rev={rev} captured={str(captured)[:19]} "
              f"{size:,d} bytes", file=sys.stderr)

    patch_stats = duck.execute("""
        SELECT kind, COUNT(*) AS patches,
               SUM(length(patch)) AS total_patch_bytes
        FROM rule4_schema_snapshot_patch
        GROUP BY kind
        ORDER BY kind
    """).fetchall()
    if patch_stats:
        print(f"\nPatch chain:", file=sys.stderr)
        for kind, count, total_bytes in patch_stats:
            print(f"  {kind:15s} {count} patches, {total_bytes:,d} bytes total",
                  file=sys.stderr)

    duck.close()


if __name__ == "__main__":
    main()
