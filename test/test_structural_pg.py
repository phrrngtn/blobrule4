"""
Run structural analysis on the socrata catalog metadata itself.

The socrata schema tables (resource, resource_column, resource_view, etc.)
are themselves a relational schema with PKs, FKs, and typed columns.
Let's analyze them as if they were any other database.
"""
import json
import time
import sys
sys.path.insert(0, "python")

import duckdb
from blobrule4.structural import run_structural, ensure_fact_table

duck = duckdb.connect(":memory:")
duck.execute("INSTALL postgres; LOAD postgres;")
duck.execute("ATTACH 'host=/tmp dbname=rule4_test' AS pg (TYPE POSTGRES, READ_ONLY)")

# ── Build snapshots from PG catalog queries ──
print("=" * 70)
print("Building TTST snapshots from PG information_schema")
print("=" * 70)

# Create snapshot table
duck.execute("""
    CREATE TABLE rule4_schema_snapshot (
        dataserver_id INTEGER, catalog_name VARCHAR, schema_name VARCHAR,
        kind VARCHAR, revision_num INTEGER, snapshot TEXT,
        captured_at TIMESTAMP,
        PRIMARY KEY (dataserver_id, catalog_name, schema_name, kind)
    )
""")

# Query PG information_schema for the socrata schema
schemas_to_analyze = ['socrata', 'gazetteer', 'domain']

for schema in schemas_to_analyze:
    print(f"\n  Schema: {schema}")

    # Columns
    cols = duck.execute(f"""
        SELECT table_schema, table_name, column_name,
               ordinal_position, data_type,
               character_maximum_length, numeric_precision, numeric_scale,
               is_nullable, column_default
        FROM pg.information_schema.columns
        WHERE table_schema = '{schema}'
        ORDER BY table_name, ordinal_position
    """).fetchall()

    col_snap = {}
    for row in cols:
        ts, tn, cn = row[0], row[1], row[2]
        col_snap.setdefault(ts, {}).setdefault(tn, {})[cn] = {
            "ordinal_position": row[3],
            "data_type": row[4],
            "character_maximum_length": row[5],
            "numeric_precision": row[6],
            "numeric_scale": row[7],
            "is_nullable": row[8],
            "default_definition": row[9],
        }

    if col_snap:
        duck.execute(
            "INSERT INTO rule4_schema_snapshot VALUES (?, ?, ?, ?, 1, ?, CURRENT_TIMESTAMP)",
            [1, "rule4_test", schema, "columns", json.dumps(col_snap)]
        )
        n_tables = sum(len(v) for v in col_snap.values())
        n_cols = sum(len(c) for s in col_snap.values() for c in s.values())
        print(f"    columns: {n_cols} cols across {n_tables} tables")

    # Tables
    tables = duck.execute(f"""
        SELECT table_schema, table_name, table_type
        FROM pg.information_schema.tables
        WHERE table_schema = '{schema}'
    """).fetchall()

    tbl_snap = {}
    for ts, tn, tt in tables:
        tbl_snap.setdefault(ts, {})[tn] = {"table_type": tt}

    if tbl_snap:
        duck.execute(
            "INSERT INTO rule4_schema_snapshot VALUES (?, ?, ?, ?, 1, ?, CURRENT_TIMESTAMP)",
            [1, "rule4_test", schema, "tables", json.dumps(tbl_snap)]
        )
        print(f"    tables: {len(tables)}")

    # Primary keys
    pks = duck.execute(f"""
        SELECT kcu.table_schema, kcu.table_name,
               kcu.constraint_name, kcu.column_name,
               kcu.ordinal_position AS key_seq
        FROM pg.information_schema.key_column_usage AS kcu
        JOIN pg.information_schema.table_constraints AS tc
            ON tc.constraint_name = kcu.constraint_name
           AND tc.constraint_schema = kcu.constraint_schema
           AND tc.table_name = kcu.table_name
        WHERE tc.constraint_type = 'PRIMARY KEY'
          AND kcu.table_schema = '{schema}'
    """).fetchall()

    pk_snap = {}
    for ts, tn, cn_name, col, seq in pks:
        pk_snap.setdefault(ts, {}).setdefault(tn, {}).setdefault(cn_name, {})[col] = {
            "key_seq": seq
        }

    if pk_snap:
        duck.execute(
            "INSERT INTO rule4_schema_snapshot VALUES (?, ?, ?, ?, 1, ?, CURRENT_TIMESTAMP)",
            [1, "rule4_test", schema, "primary_keys", json.dumps(pk_snap)]
        )
        n_pks = sum(len(c) for s in pk_snap.values() for c in s.values())
        print(f"    primary_keys: {n_pks} constraints")

    # Foreign keys
    fks = duck.execute(f"""
        SELECT
            kcu.table_schema AS fk_schema,
            kcu.table_name AS fk_table,
            kcu.constraint_name,
            kcu.column_name AS fk_column,
            ccu.table_schema AS pk_schema,
            ccu.table_name AS pk_table,
            ccu.column_name AS pk_column,
            kcu.ordinal_position AS key_seq
        FROM pg.information_schema.key_column_usage AS kcu
        JOIN pg.information_schema.table_constraints AS tc
            ON tc.constraint_name = kcu.constraint_name
           AND tc.constraint_schema = kcu.constraint_schema
           AND tc.table_name = kcu.table_name
        JOIN pg.information_schema.constraint_column_usage AS ccu
            ON ccu.constraint_name = kcu.constraint_name
           AND ccu.constraint_schema = kcu.constraint_schema
        WHERE tc.constraint_type = 'FOREIGN KEY'
          AND kcu.table_schema = '{schema}'
    """).fetchall()

    fk_snap = {}
    for fk_s, fk_t, cname, fk_col, pk_s, pk_t, pk_col, seq in fks:
        fk_snap.setdefault(fk_s, {}).setdefault(fk_t, {}).setdefault(cname, {})[fk_col] = {
            "pk_schema": pk_s, "pk_table": pk_t, "pk_column": pk_col, "key_seq": seq
        }

    if fk_snap:
        duck.execute(
            "INSERT INTO rule4_schema_snapshot VALUES (?, ?, ?, ?, 1, ?, CURRENT_TIMESTAMP)",
            [1, "rule4_test", schema, "foreign_keys", json.dumps(fk_snap)]
        )
        n_fks = sum(len(c) for s in fk_snap.values()
                    for t in s.values() for c in t.values())
        print(f"    foreign_keys: {n_fks} constraints")

# ── Run structural analysis ──
print(f"\n{'='*70}")
print("Running structural analysis Phase 1")
print("=" * 70)

import pyarrow  # needed for reference table registration

for schema in schemas_to_analyze:
    print(f"\n  --- {schema} ---")
    total = run_structural(duck, 1, "rule4_test", schema)

# ── Show results ──
print(f"\n{'='*70}")
print("RESULTS: All facts by type")
print("=" * 70)

for row in duck.execute("""
    SELECT fact_type, schema_name, COUNT(*) AS n
    FROM rule4_metadata_fact
    GROUP BY fact_type, schema_name
    ORDER BY schema_name, fact_type
""").fetchall():
    print(f"  {row[1]:<15} {row[0]:<25} {row[2]:>5}")

# Show the interesting facts
print(f"\n{'='*70}")
print("FK TOPOLOGY")
print("=" * 70)
for row in duck.execute("""
    SELECT schema_name, table_name, fact_value
    FROM rule4_metadata_fact
    WHERE fact_type = 'fk_topology_role'
    ORDER BY schema_name, table_name
""").fetchall():
    v = json.loads(row[2])
    print(f"  {row[0]}.{row[1]:<30} role={v['role']:<12} out={v['outbound_fk_count']} in={v['inbound_fk_count']}")

print(f"\n{'='*70}")
print("PK STRUCTURE")
print("=" * 70)
for row in duck.execute("""
    SELECT schema_name, table_name, fact_value
    FROM rule4_metadata_fact
    WHERE fact_type = 'pk_structure'
    ORDER BY schema_name, table_name
""").fetchall():
    v = json.loads(row[2])
    print(f"  {row[0]}.{row[1]:<30} {v.get('columns', '?')}")

print(f"\n{'='*70}")
print("NAMING PATTERNS (candidate FKs)")
print("=" * 70)
for row in duck.execute("""
    SELECT schema_name, table_name, column_name, fact_value
    FROM rule4_metadata_fact
    WHERE fact_type = 'naming_pattern'
      AND fact_value LIKE '%candidate_fk%'
    ORDER BY schema_name, table_name
""").fetchall():
    v = json.loads(row[3])
    print(f"  {row[0]}.{row[1]}.{row[2]} → {v['pattern']}")

print(f"\n{'='*70}")
print("TYPE SIGNATURES (measures)")
print("=" * 70)
for row in duck.execute("""
    SELECT schema_name, table_name, column_name, fact_value
    FROM rule4_metadata_fact
    WHERE fact_type = 'type_signature'
      AND fact_value LIKE '%measure%'
    ORDER BY schema_name, table_name
""").fetchall():
    v = json.loads(row[3])
    print(f"  {row[0]}.{row[1]}.{row[2]:<25} {v['hint']:<25} ({v['type_name']})")

duck.close()
