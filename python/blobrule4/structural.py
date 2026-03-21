"""
Structural analysis: Pass 0 of the resolution sieve.

Extracts facts from TTST schema snapshots without touching any data
values.  Each analysis reads one or more snapshot kinds, computes
derived observations, and writes them to rule4_metadata_fact.

Phase 1 (no dependencies — pure pattern matching):
  - FK membership facts
  - PK structure facts
  - Type signature classification
  - Check constraint enumeration extraction
  - Default expression analysis
  - Unique constraint facts

Phase 2 (depends on Phase 1, needs blobembed):
  - Column name clustering (within table)
  - Column name clustering (across tables)
  - Schema evolution signals

Phase 3 (depends on Phases 1+2):
  - Schema-level topic detection
  - View dependency analysis

Usage:
    uv run python -m blobrule4.structural /path/to/survey.duckdb \\
        [--dataserver NAME] [--schema NAME] [--phases 1,2,3]
"""

import json
import re
import sys
from datetime import datetime, timezone


# ── Snapshot extraction ───────────────────────────────────────

def load_snapshot(duck, dataserver_id, catalog_name, schema_name, kind):
    """Load and parse a single snapshot kind.  Returns (revision, dict) or (None, None)."""
    row = duck.execute(
        "SELECT revision_num, snapshot FROM rule4_schema_snapshot "
        "WHERE dataserver_id = ? AND catalog_name = ? "
        "AND schema_name = ? AND kind = ?",
        [dataserver_id, catalog_name, schema_name, kind]
    ).fetchone()
    if row and row[1]:
        return row[0], json.loads(row[1])
    return None, None


def iter_leaf(snap, depth):
    """
    Iterate a nested dict, yielding (*keys, leaf_dict) tuples.

    depth=2: yields (k1, k2, leaf)
    depth=3: yields (k1, k2, k3, leaf)
    depth=4: yields (k1, k2, k3, k4, leaf)
    """
    if snap is None:
        return
    for k1, v1 in snap.items():
        if not isinstance(v1, dict):
            continue
        if depth == 1:
            yield (k1, v1)
        elif depth == 2:
            for k2, v2 in v1.items():
                if not isinstance(v2, dict):
                    continue
                yield (k1, k2, v2)
        elif depth == 3:
            for k2, v2 in v1.items():
                if not isinstance(v2, dict):
                    continue
                for k3, v3 in v2.items():
                    if not isinstance(v3, dict):
                        continue
                    yield (k1, k2, k3, v3)
        elif depth == 4:
            for k2, v2 in v1.items():
                if not isinstance(v2, dict):
                    continue
                for k3, v3 in v2.items():
                    if not isinstance(v3, dict):
                        continue
                    for k4, v4 in v3.items():
                        if not isinstance(v4, dict):
                            continue
                        yield (k1, k2, k3, k4, v4)


# ── Fact writing ──────────────────────────────────────────────

def ensure_fact_table(duck):
    """Create rule4_metadata_fact if it doesn't exist."""
    duck.execute("""
        CREATE TABLE IF NOT EXISTS rule4_metadata_fact (
            fact_id INTEGER PRIMARY KEY,
            dataserver_id INTEGER NOT NULL,
            catalog_name VARCHAR NOT NULL,
            schema_name VARCHAR NOT NULL,
            table_name VARCHAR NOT NULL,
            column_name VARCHAR,
            fact_type VARCHAR NOT NULL,
            fact_value TEXT NOT NULL,
            tier INTEGER NOT NULL,
            source_kind VARCHAR,
            source_revision INTEGER,
            observed_at TIMESTAMP NOT NULL
        )
    """)
    duck.execute("""
        CREATE SEQUENCE IF NOT EXISTS seq_metadata_fact START 1
    """)


def emit_fact(duck, *, dataserver_id, catalog_name, schema_name,
              table_name, column_name, fact_type, fact_value,
              tier, source_kind, source_revision):
    """Insert a single fact.  fact_value should be a JSON-serializable object."""
    duck.execute(
        "INSERT INTO rule4_metadata_fact "
        "(fact_id, dataserver_id, catalog_name, schema_name, table_name, "
        "column_name, fact_type, fact_value, tier, source_kind, "
        "source_revision, observed_at) "
        "VALUES (nextval('seq_metadata_fact'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [dataserver_id, catalog_name, schema_name, table_name,
         column_name, fact_type,
         json.dumps(fact_value) if not isinstance(fact_value, str) else fact_value,
         tier, source_kind, source_revision,
         datetime.now(timezone.utc)]
    )


def clear_facts(duck, dataserver_id, catalog_name, schema_name,
                tier, source_kind=None):
    """Remove existing facts for a scope (before re-analysis)."""
    sql = (
        "DELETE FROM rule4_metadata_fact "
        "WHERE dataserver_id = ? AND catalog_name = ? "
        "AND schema_name = ? AND tier = ?"
    )
    params = [dataserver_id, catalog_name, schema_name, tier]
    if source_kind:
        sql += " AND source_kind = ?"
        params.append(source_kind)
    duck.execute(sql, params)


# ── Phase 1 analyses ──────────────────────────────────────────

def analyze_fk_membership(duck, ds_id, cat, sch):
    """
    Extract FK membership facts: which columns participate in FKs,
    and what they reference.  Also computes table-level FK topology
    (fact/dimension/bridge/island).
    """
    rev, fk_snap = load_snapshot(duck, ds_id, cat, sch, "foreign_keys")
    if fk_snap is None:
        return 0

    clear_facts(duck, ds_id, cat, sch, 1, "foreign_keys")
    n = 0

    # Collect per-table outbound/inbound counts for topology
    outbound = {}  # table_key -> set of referenced tables
    inbound = {}   # table_key -> set of referencing tables

    # FK snap nesting: fk_schema -> fk_table -> constraint -> fk_col -> attrs
    for fk_schema, fk_table, constraint, fk_col, attrs in iter_leaf(fk_snap, 4):
        pk_schema = attrs.get("pk_schema", fk_schema)
        pk_table = attrs.get("pk_table")
        pk_column = attrs.get("pk_column")

        if not pk_table or not pk_column:
            continue

        # Column-level fact: this column is an FK member
        emit_fact(duck, dataserver_id=ds_id, catalog_name=cat,
                  schema_name=sch, table_name=fk_table,
                  column_name=fk_col, fact_type="fk_member",
                  fact_value={
                      "constraint": constraint,
                      "references": f"{pk_schema}.{pk_table}.{pk_column}",
                      "pk_schema": pk_schema,
                      "pk_table": pk_table,
                      "pk_column": pk_column,
                  },
                  tier=1, source_kind="foreign_keys", source_revision=rev)
        n += 1

        # Track topology
        fk_key = f"{fk_schema}.{fk_table}"
        pk_key = f"{pk_schema}.{pk_table}"
        outbound.setdefault(fk_key, set()).add(pk_key)
        inbound.setdefault(pk_key, set()).add(fk_key)

    # Table-level topology facts
    all_tables = set(outbound.keys()) | set(inbound.keys())
    for tbl_key in all_tables:
        tbl_schema, tbl_name = tbl_key.split(".", 1)
        out_count = len(outbound.get(tbl_key, set()))
        in_count = len(inbound.get(tbl_key, set()))

        if out_count >= 2 and in_count == 0:
            role = "fact"
        elif out_count == 0 and in_count >= 1:
            role = "dimension"
        elif out_count >= 1 and in_count >= 1:
            role = "bridge"
        elif out_count == 1 and in_count == 0:
            role = "detail"
        else:
            role = "mixed"

        emit_fact(duck, dataserver_id=ds_id, catalog_name=cat,
                  schema_name=sch, table_name=tbl_name,
                  column_name=None, fact_type="fk_topology_role",
                  fact_value={
                      "role": role,
                      "outbound_fk_count": out_count,
                      "inbound_fk_count": in_count,
                      "references": sorted(outbound.get(tbl_key, set())),
                      "referenced_by": sorted(inbound.get(tbl_key, set())),
                  },
                  tier=1, source_kind="foreign_keys", source_revision=rev)
        n += 1

    return n


def analyze_pk_structure(duck, ds_id, cat, sch):
    """
    Extract PK facts: single vs composite, identity, FK target status.
    """
    pk_rev, pk_snap = load_snapshot(duck, ds_id, cat, sch, "primary_keys")
    if pk_snap is None:
        return 0

    col_rev, col_snap = load_snapshot(duck, ds_id, cat, sch, "columns")
    fk_rev, fk_snap = load_snapshot(duck, ds_id, cat, sch, "foreign_keys")

    clear_facts(duck, ds_id, cat, sch, 1, "primary_keys")
    n = 0

    # Collect identity columns from columns snapshot
    identity_cols = set()
    if col_snap:
        for tbl_schema, tbl_name, col_name, attrs in iter_leaf(col_snap, 3):
            if attrs.get("is_identity"):
                identity_cols.add((tbl_schema, tbl_name, col_name))

    # Collect FK-referenced tables (the pk_table side)
    fk_targets = set()
    if fk_snap:
        for _, _, _, _, attrs in iter_leaf(fk_snap, 4):
            pk_schema = attrs.get("pk_schema", "")
            pk_table = attrs.get("pk_table", "")
            if pk_table:
                fk_targets.add((pk_schema, pk_table))

    # PK snap nesting: schema -> table -> constraint -> column -> attrs
    # Group by (schema, table, constraint)
    pk_groups = {}
    for tbl_schema, tbl_name, constraint, col_name, attrs in iter_leaf(pk_snap, 4):
        key = (tbl_schema, tbl_name, constraint)
        pk_groups.setdefault(key, []).append((col_name, attrs))

    for (tbl_schema, tbl_name, constraint), cols in pk_groups.items():
        col_count = len(cols)
        col_names = sorted([c[0] for c in cols])
        has_identity = any(
            (tbl_schema, tbl_name, c) in identity_cols for c, _ in cols
        )
        is_fk_target = (tbl_schema, tbl_name) in fk_targets

        # Classify
        if col_count == 1 and has_identity:
            classification = "surrogate_key"
        elif col_count == 1 and is_fk_target:
            classification = "natural_dimension_key"
        elif col_count == 1:
            classification = "natural_key"
        elif col_count > 1:
            classification = "composite_key"
        else:
            classification = "unknown"

        # Column-level facts
        for col_name, col_attrs in cols:
            emit_fact(duck, dataserver_id=ds_id, catalog_name=cat,
                      schema_name=sch, table_name=tbl_name,
                      column_name=col_name, fact_type="pk_member",
                      fact_value={
                          "constraint": constraint,
                          "pk_column_count": col_count,
                          "key_seq": col_attrs.get("key_seq", col_attrs.get("KEY_SEQ")),
                          "has_identity": has_identity,
                          "is_fk_target": is_fk_target,
                          "classification": classification,
                      },
                      tier=1, source_kind="primary_keys", source_revision=pk_rev)
            n += 1

        # Table-level PK structure fact
        emit_fact(duck, dataserver_id=ds_id, catalog_name=cat,
                  schema_name=sch, table_name=tbl_name,
                  column_name=None, fact_type="pk_structure",
                  fact_value={
                      "constraint": constraint,
                      "columns": col_names,
                      "column_count": col_count,
                      "has_identity": has_identity,
                      "is_fk_target": is_fk_target,
                      "classification": classification,
                  },
                  tier=1, source_kind="primary_keys", source_revision=pk_rev)
        n += 1

    return n


def analyze_type_signatures(duck, ds_id, cat, sch):
    """
    Classify each column by its data type into a semantic hint.
    """
    rev, col_snap = load_snapshot(duck, ds_id, cat, sch, "columns")
    if col_snap is None:
        return 0

    clear_facts(duck, ds_id, cat, sch, 1, "columns")
    n = 0

    for tbl_schema, tbl_name, col_name, attrs in iter_leaf(col_snap, 3):
        type_name = (attrs.get("type_name") or attrs.get("data_type") or "").lower()
        precision = attrs.get("precision") or attrs.get("numeric_precision")
        scale = attrs.get("scale") or attrs.get("numeric_scale")
        max_length = attrs.get("max_length") or attrs.get("character_maximum_length")

        # Classify
        if type_name in ("money", "smallmoney"):
            hint = "measure_currency"
        elif type_name in ("decimal", "numeric") and scale and int(scale) > 0:
            hint = "measure_decimal"
        elif type_name in ("float", "real", "double precision", "float4", "float8"):
            hint = "measure_float"
        elif type_name in ("int", "integer", "int4", "bigint", "int8",
                           "smallint", "tinyint"):
            # Ambiguous without more context — _id suffix helps
            if col_name.endswith("_id") or col_name.endswith("_key"):
                hint = "key_integer"
            else:
                hint = "measure_or_key"
        elif type_name in ("bit", "boolean", "bool"):
            hint = "flag_dimension"
        elif type_name in ("date",):
            hint = "date_dimension"
        elif type_name in ("datetime", "datetime2", "smalldatetime",
                           "timestamp", "timestamp without time zone",
                           "timestamp with time zone", "timestamptz",
                           "datetimeoffset"):
            hint = "datetime_dimension_or_audit"
        elif type_name in ("uniqueidentifier", "uuid"):
            hint = "surrogate_key"
        elif type_name in ("varchar", "nvarchar", "char", "nchar",
                           "character varying", "bpchar", "character"):
            ml = int(max_length) if max_length and int(max_length) > 0 else None
            # nvarchar max_length is bytes — halve
            if type_name.startswith("n") and ml:
                ml = ml // 2
            if ml and ml <= 5:
                hint = "code_dimension"
            elif ml and ml <= 255:
                hint = "name_dimension"
            else:
                hint = "content_text"
        elif type_name in ("text", "ntext", "citext"):
            hint = "content_text"
        elif type_name in ("varbinary", "binary", "image", "bytea"):
            hint = "binary_content"
        elif type_name in ("xml", "json", "jsonb"):
            hint = "structured_content"
        else:
            hint = "unknown"

        emit_fact(duck, dataserver_id=ds_id, catalog_name=cat,
                  schema_name=sch, table_name=tbl_name,
                  column_name=col_name, fact_type="type_signature",
                  fact_value={
                      "type_name": type_name,
                      "hint": hint,
                      "max_length": max_length,
                      "precision": precision,
                      "scale": scale,
                      "is_nullable": attrs.get("is_nullable"),
                      "is_identity": attrs.get("is_identity"),
                      "is_computed": attrs.get("is_computed"),
                  },
                  tier=1, source_kind="columns", source_revision=rev)
        n += 1

    return n


def analyze_check_enums(duck, ds_id, cat, sch):
    """
    Parse CHECK constraint definitions for IN-lists and extract
    enumeration values.
    """
    rev, snap = load_snapshot(duck, ds_id, cat, sch, "check_constraints")
    if snap is None:
        return 0

    clear_facts(duck, ds_id, cat, sch, 1, "check_constraints")
    n = 0

    # IN-list pattern: IN ('val1', 'val2', ...) or IN (1, 2, 3)
    in_list_re = re.compile(r"\bIN\s*\(\s*(.+?)\s*\)", re.IGNORECASE)
    # Extract individual values: 'quoted' or bare numbers
    val_re = re.compile(r"'([^']*)'|(\d+(?:\.\d+)?)")

    # Check constraint nesting: schema -> table -> constraint -> attrs
    for tbl_schema, tbl_name, constraint, attrs in iter_leaf(snap, 3):
        check_clause = attrs.get("check_clause", "")
        column_name = attrs.get("column_name")  # may be NULL for multi-column

        m = in_list_re.search(check_clause)
        if m:
            raw = m.group(1)
            values = []
            for vm in val_re.finditer(raw):
                values.append(vm.group(1) if vm.group(1) is not None else vm.group(2))

            if values:
                emit_fact(duck, dataserver_id=ds_id, catalog_name=cat,
                          schema_name=sch, table_name=tbl_name,
                          column_name=column_name, fact_type="check_enum",
                          fact_value={
                              "constraint": constraint,
                              "values": values,
                              "count": len(values),
                              "check_clause": check_clause,
                          },
                          tier=1, source_kind="check_constraints",
                          source_revision=rev)
                n += 1

        # Always record the constraint existence even without IN-list
        if not m:
            emit_fact(duck, dataserver_id=ds_id, catalog_name=cat,
                      schema_name=sch, table_name=tbl_name,
                      column_name=column_name, fact_type="check_constraint",
                      fact_value={
                          "constraint": constraint,
                          "check_clause": check_clause,
                      },
                      tier=1, source_kind="check_constraints",
                      source_revision=rev)
            n += 1

    return n


def analyze_defaults(duck, ds_id, cat, sch):
    """
    Classify default expressions into semantic hints.
    """
    rev, col_snap = load_snapshot(duck, ds_id, cat, sch, "columns")
    if col_snap is None:
        return 0

    clear_facts(duck, ds_id, cat, sch, 1, "default_hints")
    n = 0

    # Patterns for default classification
    timestamp_re = re.compile(
        r"getdate|sysdatetime|current_timestamp|now\(\)|sysutcdatetime",
        re.IGNORECASE)
    uuid_re = re.compile(
        r"newid|newsequentialid|gen_random_uuid|uuid_generate",
        re.IGNORECASE)
    user_re = re.compile(
        r"user_name|suser_sname|current_user|session_user|system_user",
        re.IGNORECASE)
    sequence_re = re.compile(
        r"next\s+value\s+for|nextval", re.IGNORECASE)
    quoted_string_re = re.compile(r"^['\(]*'([^']+)'['\)]*$")

    for tbl_schema, tbl_name, col_name, attrs in iter_leaf(col_snap, 3):
        default_def = attrs.get("default_definition")
        if not default_def:
            continue

        raw = default_def.strip()

        if timestamp_re.search(raw):
            hint = "audit_timestamp"
        elif uuid_re.search(raw):
            hint = "surrogate_uuid"
        elif user_re.search(raw):
            hint = "audit_user"
        elif sequence_re.search(raw):
            hint = "surrogate_sequence"
        elif raw.strip("() ") in ("0", "0.0", "0.00"):
            hint = "measure_baseline"
        elif raw.strip("() ") == "1":
            # Could be boolean default or measure
            type_name = (attrs.get("type_name") or "").lower()
            if type_name in ("bit", "boolean", "bool"):
                hint = "flag_default_true"
            else:
                hint = "measure_baseline"
        elif raw.strip("() ") == "''":
            hint = "optional_text"
        else:
            m = quoted_string_re.match(raw)
            if m and m.group(1).isupper():
                hint = "status_enum"
            else:
                hint = "other_default"

        emit_fact(duck, dataserver_id=ds_id, catalog_name=cat,
                  schema_name=sch, table_name=tbl_name,
                  column_name=col_name, fact_type="default_hint",
                  fact_value={
                      "default_definition": raw,
                      "hint": hint,
                  },
                  tier=1, source_kind="columns", source_revision=rev)
        n += 1

    return n


def analyze_unique_constraints(duck, ds_id, cat, sch):
    """
    Extract unique constraint facts — candidate keys beyond PK.
    """
    rev, snap = load_snapshot(duck, ds_id, cat, sch, "unique_constraints")
    if snap is None:
        return 0

    clear_facts(duck, ds_id, cat, sch, 1, "unique_constraints")
    n = 0

    # Nesting: schema -> table -> constraint -> column -> attrs
    # Group by constraint
    uc_groups = {}
    for tbl_schema, tbl_name, constraint, col_name, attrs in iter_leaf(snap, 4):
        key = (tbl_schema, tbl_name, constraint)
        uc_groups.setdefault(key, []).append((col_name, attrs))

    for (tbl_schema, tbl_name, constraint), cols in uc_groups.items():
        col_names = sorted([c[0] for c in cols])

        for col_name, col_attrs in cols:
            emit_fact(duck, dataserver_id=ds_id, catalog_name=cat,
                      schema_name=sch, table_name=tbl_name,
                      column_name=col_name, fact_type="unique_member",
                      fact_value={
                          "constraint": constraint,
                          "unique_column_count": len(cols),
                          "all_columns": col_names,
                          "key_seq": col_attrs.get("key_seq"),
                      },
                      tier=1, source_kind="unique_constraints",
                      source_revision=rev)
            n += 1

    return n


def analyze_naming_patterns(duck, ds_id, cat, sch):
    """
    Detect naming patterns: _id, _key, _date, _flag, _amt suffixes etc.
    Also detect columns whose name matches a table name (candidate FK
    even without a declared FK).
    """
    col_rev, col_snap = load_snapshot(duck, ds_id, cat, sch, "columns")
    if col_snap is None:
        return 0

    clear_facts(duck, ds_id, cat, sch, 1, "naming_patterns")
    n = 0

    # Collect all table names for cross-reference
    table_names = set()
    for tbl_schema, tbl_name, _, _ in iter_leaf(col_snap, 3):
        table_names.add(tbl_name.lower())

    suffix_map = {
        "_id": "key",
        "_key": "key",
        "_code": "code_dimension",
        "_type": "type_dimension",
        "_status": "status_dimension",
        "_name": "name",
        "_desc": "description",
        "_description": "description",
        "_date": "date",
        "_dt": "date",
        "_time": "time",
        "_ts": "timestamp",
        "_at": "timestamp",
        "_on": "timestamp",
        "_flag": "flag",
        "_ind": "flag",
        "_yn": "flag",
        "_amt": "amount",
        "_amount": "amount",
        "_qty": "quantity",
        "_count": "count",
        "_cnt": "count",
        "_pct": "percentage",
        "_percent": "percentage",
        "_rate": "rate",
        "_price": "price",
        "_cost": "cost",
        "_total": "total",
        "_num": "number",
        "_no": "number",
        "_number": "number",
    }

    for tbl_schema, tbl_name, col_name, attrs in iter_leaf(col_snap, 3):
        col_lower = col_name.lower()
        facts_for_col = []

        # Suffix matching
        for suffix, role in suffix_map.items():
            if col_lower.endswith(suffix):
                facts_for_col.append(("suffix", suffix, role))
                break

        # Table name reference: column "customer_id" when table "customer"
        # or "customers" exists
        if col_lower.endswith("_id"):
            ref_name = col_lower[:-3]  # strip _id
            candidates = [ref_name, ref_name + "s", ref_name + "es"]
            # Also try singularizing: "categories" -> "category"
            if ref_name.endswith("ie"):
                candidates.append(ref_name[:-2] + "y")
            for candidate in candidates:
                if candidate in table_names and candidate != tbl_name.lower():
                    facts_for_col.append(("table_ref", candidate, "candidate_fk"))
                    break

        for pattern_type, pattern_val, semantic_role in facts_for_col:
            emit_fact(duck, dataserver_id=ds_id, catalog_name=cat,
                      schema_name=sch, table_name=tbl_name,
                      column_name=col_name, fact_type="naming_pattern",
                      fact_value={
                          "pattern_type": pattern_type,
                          "pattern": pattern_val,
                          "semantic_role": semantic_role,
                      },
                      tier=1, source_kind="columns", source_revision=col_rev)
            n += 1

    return n


def analyze_column_comments(duck, ds_id, cat, sch):
    """Record column comment facts when present."""
    rev, snap = load_snapshot(duck, ds_id, cat, sch, "column_comments")
    if snap is None:
        return 0

    clear_facts(duck, ds_id, cat, sch, 1, "column_comments")
    n = 0

    for tbl_schema, tbl_name, col_name, attrs in iter_leaf(snap, 3):
        comment = attrs.get("column_comment") or attrs.get("property_value")
        if not comment:
            continue

        emit_fact(duck, dataserver_id=ds_id, catalog_name=cat,
                  schema_name=sch, table_name=tbl_name,
                  column_name=col_name, fact_type="column_comment",
                  fact_value={"comment": comment},
                  tier=1, source_kind="column_comments", source_revision=rev)
        n += 1

    return n


# ── Orchestration ─────────────────────────────────────────────

PHASE_1_ANALYSES = [
    ("fk_membership", analyze_fk_membership),
    ("pk_structure", analyze_pk_structure),
    ("type_signatures", analyze_type_signatures),
    ("check_enums", analyze_check_enums),
    ("defaults", analyze_defaults),
    ("unique_constraints", analyze_unique_constraints),
    ("naming_patterns", analyze_naming_patterns),
    ("column_comments", analyze_column_comments),
]


def run_phase_1(duck, ds_id, cat, sch, verbose=True):
    """Run all Phase 1 analyses (no dependencies)."""
    total = 0
    for name, fn in PHASE_1_ANALYSES:
        n = fn(duck, ds_id, cat, sch)
        total += n
        if verbose:
            print(f"  {name}: {n} facts", file=sys.stderr)
    return total


def run_structural(duck, ds_id, cat, sch, *, phases=None, verbose=True):
    """
    Run structural analysis for a single (dataserver, catalog, schema).

    Parameters
    ----------
    phases : set of int, optional
        Which phases to run (default: {1}).  Phases 2 and 3 require
        blobembed and are not yet implemented.
    """
    if phases is None:
        phases = {1}

    ensure_fact_table(duck)

    if verbose:
        print(f"Structural analysis: ds={ds_id} cat={cat} sch={sch}",
              file=sys.stderr)

    total = 0
    if 1 in phases:
        total += run_phase_1(duck, ds_id, cat, sch, verbose=verbose)

    if 2 in phases:
        if verbose:
            print("  Phase 2 not yet implemented (needs blobembed)",
                  file=sys.stderr)

    if 3 in phases:
        if verbose:
            print("  Phase 3 not yet implemented (needs Phase 2)",
                  file=sys.stderr)

    if verbose:
        print(f"  Total: {total} facts", file=sys.stderr)

    return total


# ── CLI entry point ───────────────────────────────────────────

def main():
    import argparse
    import duckdb

    parser = argparse.ArgumentParser(
        description="Run structural analysis (Pass 0) on schema snapshots")
    parser.add_argument("database", help="Path to DuckDB database")
    parser.add_argument("--dataserver", help="Filter to one dataserver name")
    parser.add_argument("--schema", help="Filter to one schema name")
    parser.add_argument("--phases", default="1",
                        help="Comma-separated phase numbers (default: 1)")
    args = parser.parse_args()

    phases = {int(p) for p in args.phases.split(",")}
    duck = duckdb.connect(args.database)

    # Find schemas to analyze
    sql = (
        "SELECT DISTINCT s.dataserver_id, s.catalog_name, s.schema_name "
        "FROM rule4_schema_snapshot AS s"
    )
    conditions = []
    params = []
    if args.dataserver:
        sql += " JOIN rule4_dataserver AS d ON d.dataserver_id = s.dataserver_id"
        conditions.append("d.name = ?")
        params.append(args.dataserver)
    if args.schema:
        conditions.append("s.schema_name = ?")
        params.append(args.schema)
    if conditions:
        sql += " WHERE " + " AND ".join(conditions)

    schemas = duck.execute(sql, params).fetchall()

    for ds_id, cat, sch in schemas:
        run_structural(duck, ds_id, cat, sch, phases=phases)

    duck.close()


if __name__ == "__main__":
    main()
