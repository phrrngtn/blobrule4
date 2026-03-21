"""
SQLAlchemy MetaData generator from rule4 schema snapshots.

Two levels of API:

  Low-level (single schema):
    build_metadata(duck, dataserver_id, catalog, schema) → sa.MetaData
    Reads TTST snapshots, builds Table/Column/PK/FK/Index objects.

  High-level (evidence-driven federation):
    SchemaCollection.from_evidence(duck, predicate) → SchemaCollection
    Queries the fact/evidence layer to discover relevant schemas,
    builds MetaData per schema, infers cross-source join paths from
    column name + type compatibility + evidence.

    Usage:
      coll = SchemaCollection.from_evidence(duck,
          "fact_type = 'fk_topology_role' AND fact_value LIKE '%finance%'")
      # or:
      coll = SchemaCollection.from_schemas(duck, [
          (1, 'erp_db', 'dbo'),
          (2, 'reporting_db', 'finance'),
      ])

      # coll.metadata has all tables from all matching schemas
      # coll.inferred_joins has cross-schema join candidates
      orders = coll['dbo.orders']
      gl = coll['finance.gl_entries']
      stmt = sa.select(orders, gl).select_from(
          orders.join(gl, coll.join_condition(orders, gl))
      )

This is NOT about ORM models for rule4's own tables (see models.py).
This generates SQLAlchemy representations of TARGET databases that
rule4 has scraped, enabling programmatic query construction via the
SQLAlchemy expression API.

Type mapping strategy:
  - ODBC DATA_TYPE codes are standardized (SQL_INTEGER=4, etc.)
  - bo_driver_info().type_info gives the driver's own type catalog at
    runtime, mapping TYPE_NAME → DATA_TYPE
  - For offline use (snapshot-only, no live connection), a static mapping
    from common type_name strings covers SQL Server, PostgreSQL, and
    information_schema dialects
"""

import json
import re

import sqlalchemy as sa
import sqlalchemy.types as sat


# ── ODBC DATA_TYPE code → SQLAlchemy type ─────────────────────
# These codes are from the ODBC spec and are driver-independent.

ODBC_TYPE_MAP = {
    -7: sat.Boolean,       # SQL_BIT
    -6: sat.SmallInteger,  # SQL_TINYINT
    5:  sat.SmallInteger,  # SQL_SMALLINT
    4:  sat.Integer,       # SQL_INTEGER
    -5: sat.BigInteger,    # SQL_BIGINT
    6:  sat.Float,         # SQL_FLOAT
    7:  sat.Float,         # SQL_REAL
    8:  sat.Float,         # SQL_DOUBLE
    2:  sat.Numeric,       # SQL_NUMERIC
    3:  sat.Numeric,       # SQL_DECIMAL
    1:  sat.String,        # SQL_CHAR
    12: sat.String,        # SQL_VARCHAR
    -1: sat.Text,          # SQL_LONGVARCHAR
    -8: sat.Unicode,       # SQL_WCHAR
    -9: sat.Unicode,       # SQL_WVARCHAR
    -10: sat.UnicodeText,  # SQL_WLONGVARCHAR
    -11: sat.Uuid,         # SQL_GUID
    91: sat.Date,          # SQL_TYPE_DATE
    92: sat.Time,          # SQL_TYPE_TIME
    93: sat.DateTime,      # SQL_TYPE_TIMESTAMP
    -2: sat.LargeBinary,   # SQL_BINARY
    -3: sat.LargeBinary,   # SQL_VARBINARY
    -4: sat.LargeBinary,   # SQL_LONGVARBINARY
}


# ── Static type_name → SQLAlchemy type (for offline / snapshot use) ───
# Covers SQL Server, PostgreSQL, and information_schema type strings.
# Returns a callable(length, precision, scale) → TypeEngine.

def _str(length=None, **_):
    return sat.String(length=length) if length and length > 0 else sat.String()

def _ustr(length=None, **_):
    return sat.Unicode(length=length) if length and length > 0 else sat.Unicode()

def _num(precision=None, scale=None, **_):
    return sat.Numeric(precision=precision, scale=scale)

def _float(precision=None, **_):
    return sat.Float(precision=precision)


STATIC_TYPE_MAP = {
    # Integers
    "bit":              lambda **_: sat.Boolean(),
    "boolean":          lambda **_: sat.Boolean(),
    "bool":             lambda **_: sat.Boolean(),
    "tinyint":          lambda **_: sat.SmallInteger(),
    "smallint":         lambda **_: sat.SmallInteger(),
    "int":              lambda **_: sat.Integer(),
    "integer":          lambda **_: sat.Integer(),
    "int4":             lambda **_: sat.Integer(),
    "bigint":           lambda **_: sat.BigInteger(),
    "int8":             lambda **_: sat.BigInteger(),
    "serial":           lambda **_: sat.Integer(),
    "bigserial":        lambda **_: sat.BigInteger(),
    "smallserial":      lambda **_: sat.SmallInteger(),
    # Fixed-point
    "decimal":          _num,
    "numeric":          _num,
    "money":            lambda **_: sat.Numeric(precision=19, scale=4),
    "smallmoney":       lambda **_: sat.Numeric(precision=10, scale=4),
    # Floating-point
    "real":             lambda **_: sat.Float(precision=24),
    "float":            _float,
    "float4":           lambda **_: sat.Float(precision=24),
    "float8":           lambda **_: sat.Float(precision=53),
    "double precision": lambda **_: sat.Float(precision=53),
    # Strings
    "char":             _str,
    "character":        _str,
    "bpchar":           _str,
    "varchar":          _str,
    "character varying": _str,
    "nchar":            _ustr,
    "nvarchar":         _ustr,
    "text":             lambda **_: sat.Text(),
    "ntext":            lambda **_: sat.UnicodeText(),
    "citext":           lambda **_: sat.Text(),
    "name":             lambda **_: sat.String(length=63),
    # Binary
    "binary":           lambda length=None, **_: sat.LargeBinary(length=length),
    "varbinary":        lambda length=None, **_: sat.LargeBinary(length=length),
    "image":            lambda **_: sat.LargeBinary(),
    "bytea":            lambda **_: sat.LargeBinary(),
    # Date/Time
    "date":             lambda **_: sat.Date(),
    "time":             lambda **_: sat.Time(),
    "time without time zone":      lambda **_: sat.Time(),
    "time with time zone":         lambda **_: sat.Time(timezone=True),
    "timetz":           lambda **_: sat.Time(timezone=True),
    "datetime":         lambda **_: sat.DateTime(),
    "datetime2":        lambda **_: sat.DateTime(),
    "smalldatetime":    lambda **_: sat.DateTime(),
    "datetimeoffset":   lambda **_: sat.DateTime(timezone=True),
    "timestamp":        lambda **_: sat.DateTime(),
    "timestamp without time zone": lambda **_: sat.DateTime(),
    "timestamp with time zone":    lambda **_: sat.DateTime(timezone=True),
    "timestamptz":      lambda **_: sat.DateTime(timezone=True),
    # UUID
    "uniqueidentifier": lambda **_: sat.Uuid(),
    "uuid":             lambda **_: sat.Uuid(),
    # Structured
    "xml":              lambda **_: sat.Text(),
    "json":             lambda **_: sat.JSON(),
    "jsonb":            lambda **_: sat.JSON(),
    # Arrays (PG) — represent as Text for now
    "array":            lambda **_: sat.Text(),
    "user-defined":     lambda **_: sat.Text(),
}

# PostgreSQL format_type() returns compound strings like "numeric(10,2)"
# or "character varying(255)". Parse these.
_PG_TYPE_RE = re.compile(
    r'^(?P<base>[a-z][a-z0-9_ ]*?)(?:\((?P<args>[^)]+)\))?(?:\[\])?$'
)


def normalize_type_name(raw_type_name):
    """
    Parse a type string into (base_name, {length, precision, scale}).

    Handles:
      - Simple names: "int", "varchar", "money"
      - PG format_type: "numeric(10,2)", "character varying(255)"
      - SQL Server: "nvarchar" (length comes from separate max_length column)
      - Arrays: "integer[]" → ("integer", {})
    """
    if not raw_type_name:
        return "unknown", {}

    raw = raw_type_name.strip().lower()
    m = _PG_TYPE_RE.match(raw)
    if not m:
        return raw, {}

    base = m.group("base").strip()
    args_str = m.group("args")
    params = {}

    if args_str:
        parts = [p.strip() for p in args_str.split(",")]
        if base in ("numeric", "decimal"):
            if len(parts) >= 1:
                params["precision"] = int(parts[0])
            if len(parts) >= 2:
                params["scale"] = int(parts[1])
        elif len(parts) == 1 and parts[0].isdigit():
            params["length"] = int(parts[0])

    return base, params


def resolve_type(type_name, col_attrs=None):
    """
    Map a captured type string + column attributes to a SQLAlchemy type.

    col_attrs may contain max_length, precision, scale,
    character_maximum_length, numeric_precision, numeric_scale.
    """
    col_attrs = col_attrs or {}
    base, parsed_params = normalize_type_name(type_name)

    # Merge: parsed params (from type string) < explicit column attrs
    length = (
        col_attrs.get("character_maximum_length")
        or col_attrs.get("max_length")
        or parsed_params.get("length")
    )
    precision = (
        col_attrs.get("numeric_precision")
        or col_attrs.get("precision")
        or parsed_params.get("precision")
    )
    scale = (
        col_attrs.get("numeric_scale")
        or col_attrs.get("scale")
        or parsed_params.get("scale")
    )

    # SQL Server nvarchar: max_length is in bytes, halve for Unicode
    if base in ("nchar", "nvarchar") and col_attrs.get("max_length"):
        ml = col_attrs["max_length"]
        if isinstance(ml, (int, float)) and ml > 0:
            length = int(ml) // 2

    # -1 means MAX in SQL Server
    if isinstance(length, (int, float)) and length < 0:
        length = None

    factory = STATIC_TYPE_MAP.get(base)
    if factory:
        return factory(length=length, precision=precision, scale=scale)

    return sat.NullType()


# ── Snapshot loading ──────────────────────────────────────────

def _load_snapshot(duck, dataserver_id, catalog_name, schema_name, kind):
    """Load and parse a single snapshot kind. Returns parsed JSON or None."""
    row = duck.execute(
        "SELECT snapshot FROM rule4_schema_snapshot "
        "WHERE dataserver_id = ? AND catalog_name = ? "
        "AND schema_name = ? AND kind = ?",
        [dataserver_id, catalog_name, schema_name, kind]
    ).fetchone()
    if row and row[0]:
        return json.loads(row[0])
    return None


def _iter_nested(snap, schema_name, depth=2):
    """
    Iterate a nested snapshot JSON, yielding tuples of keys + leaf dict.

    For depth=2: yields (key1, key2, leaf_dict)
    For depth=3: yields (key1, key2, key3, leaf_dict)

    Tries schema_name as the top-level key first; if absent, iterates
    all top-level keys.
    """
    if snap is None:
        return

    # Determine top-level scope
    if schema_name in snap:
        top_items = {schema_name: snap[schema_name]}
    else:
        top_items = snap

    for k1, v1 in top_items.items():
        if not isinstance(v1, dict):
            continue
        if depth == 1:
            yield (k1, v1)
        else:
            for k2, v2 in v1.items():
                if not isinstance(v2, dict):
                    continue
                if depth == 2:
                    yield (k1, k2, v2)
                else:
                    for k3, v3 in v2.items():
                        if not isinstance(v3, dict):
                            continue
                        yield (k1, k2, k3, v3)


# ── MetaData construction ─────────────────────────────────────

def build_metadata(duck, dataserver_id, catalog_name, schema_name,
                   *, metadata=None):
    """
    Build a SQLAlchemy MetaData from rule4_schema_snapshot data.

    Reads snapshots for kinds: columns, primary_keys, foreign_keys,
    indexes. Returns a MetaData populated with Table objects.

    Parameters
    ----------
    duck : duckdb.DuckDBPyConnection
        Connection to the survey DuckDB database.
    dataserver_id : int
    catalog_name : str
    schema_name : str
    metadata : sa.MetaData, optional
        If provided, tables are added to it (useful for cross-schema
        FK resolution).

    Returns
    -------
    sa.MetaData
    """
    if metadata is None:
        metadata = sa.MetaData()

    # Load snapshots
    columns_snap = _load_snapshot(duck, dataserver_id, catalog_name,
                                  schema_name, "columns")
    pk_snap = _load_snapshot(duck, dataserver_id, catalog_name,
                             schema_name, "primary_keys")
    fk_snap = _load_snapshot(duck, dataserver_id, catalog_name,
                             schema_name, "foreign_keys")
    idx_snap = _load_snapshot(duck, dataserver_id, catalog_name,
                              schema_name, "indexes")

    if columns_snap is None:
        return metadata

    # Phase 1: build tables with columns
    tables = _build_tables(metadata, columns_snap, schema_name)

    # Phase 2: apply primary keys
    _apply_primary_keys(tables, pk_snap, schema_name)

    # Phase 3: apply foreign keys
    _apply_foreign_keys(metadata, tables, fk_snap, schema_name)

    # Phase 4: apply indexes
    _apply_indexes(tables, idx_snap, schema_name)

    return metadata


def _build_tables(metadata, columns_snap, schema_name):
    """Create Table objects with Column objects from the columns snapshot."""
    tables = {}

    for tbl_schema, tbl_name, col_name, col_attrs in _iter_nested(
            columns_snap, schema_name, depth=3):

        table_key = f"{tbl_schema}.{tbl_name}"
        if table_key not in tables:
            tables[table_key] = sa.Table(
                tbl_name, metadata, schema=tbl_schema,
            )

        table = tables[table_key]

        # Skip if column already exists (defensive)
        if col_name in table.c:
            continue

        # Resolve type
        type_name = (col_attrs.get("type_name")
                     or col_attrs.get("data_type")
                     or "unknown")
        col_type = resolve_type(type_name, col_attrs)

        # Nullable
        nullable_raw = col_attrs.get("is_nullable", col_attrs.get("is_not_null"))
        if isinstance(nullable_raw, str):
            nullable = nullable_raw.upper() in ("YES", "TRUE", "1")
        elif isinstance(nullable_raw, bool):
            # is_not_null from PG is inverted
            if "is_not_null" in col_attrs:
                nullable = not nullable_raw
            else:
                nullable = nullable_raw
        elif isinstance(nullable_raw, (int, float)):
            if "is_not_null" in col_attrs:
                nullable = not bool(nullable_raw)
            else:
                nullable = bool(nullable_raw)
        else:
            nullable = True

        table.append_column(
            sa.Column(col_name, col_type, nullable=nullable)
        )

    return tables


def _apply_primary_keys(tables, pk_snap, schema_name):
    """Add PrimaryKeyConstraint to each table from the primary_keys snapshot."""
    if pk_snap is None:
        return

    # Collect PK columns per (table, constraint)
    # pk_snap nesting: schema -> table -> constraint -> column -> attrs
    pk_groups = {}  # (table_key, constraint_name) -> [(key_seq, col_name)]

    for item in _iter_nested(pk_snap, schema_name, depth=3):
        tbl_schema, tbl_name, rest = item[0], item[1], item[2:]
        # depth=3 gives (schema, table, constraint_or_column, attrs)
        # But PK nesting is 4 levels: schema/table/constraint/column
        # So we need depth=3 to get constraint-level, then iterate columns
        pass

    # Re-approach: PK snapshot has 4-level nesting
    # schema -> table -> constraint_name -> column_name -> {key_seq, ...}
    if schema_name in pk_snap:
        scope = {schema_name: pk_snap[schema_name]}
    else:
        scope = pk_snap

    for tbl_schema, tbl_dict in scope.items():
        if not isinstance(tbl_dict, dict):
            continue
        for tbl_name, constraint_dict in tbl_dict.items():
            if not isinstance(constraint_dict, dict):
                continue
            table_key = f"{tbl_schema}.{tbl_name}"
            if table_key not in tables:
                continue
            table = tables[table_key]

            for constraint_name, columns_dict in constraint_dict.items():
                if not isinstance(columns_dict, dict):
                    continue
                # Collect columns with their key_seq
                pk_cols = []
                for col_name, col_attrs in columns_dict.items():
                    seq = col_attrs.get("key_seq", col_attrs.get("KEY_SEQ", 0))
                    if col_name in table.c:
                        pk_cols.append((seq, col_name))

                if pk_cols:
                    pk_cols.sort(key=lambda x: x[0])
                    try:
                        table.append_constraint(
                            sa.PrimaryKeyConstraint(
                                *[table.c[name] for _, name in pk_cols],
                                name=constraint_name
                            )
                        )
                    except Exception:
                        pass  # PK already set, or column mismatch


def _apply_foreign_keys(metadata, tables, fk_snap, schema_name):
    """Add ForeignKeyConstraint objects from the foreign_keys snapshot."""
    if fk_snap is None:
        return

    # FK snapshot nesting: fk_schema -> fk_table -> constraint_name -> fk_column -> attrs
    # attrs includes: pk_schema, pk_table, pk_column, key_seq
    if schema_name in fk_snap:
        scope = {schema_name: fk_snap[schema_name]}
    else:
        scope = fk_snap

    for fk_schema, fk_tables in scope.items():
        if not isinstance(fk_tables, dict):
            continue
        for fk_table, constraints in fk_tables.items():
            if not isinstance(constraints, dict):
                continue
            table_key = f"{fk_schema}.{fk_table}"
            if table_key not in tables:
                continue
            table = tables[table_key]

            for constraint_name, fk_columns in constraints.items():
                if not isinstance(fk_columns, dict):
                    continue

                # Collect FK column pairs sorted by key_seq
                pairs = []
                for fk_col, attrs in fk_columns.items():
                    if not isinstance(attrs, dict):
                        continue
                    pk_schema = attrs.get("pk_schema", fk_schema)
                    pk_table = attrs.get("pk_table")
                    pk_column = attrs.get("pk_column")
                    seq = attrs.get("key_seq", attrs.get("KEY_SEQ", 0))
                    if pk_table and pk_column:
                        pairs.append((seq, fk_col, pk_schema, pk_table, pk_column))

                if not pairs:
                    continue

                pairs.sort(key=lambda x: x[0])

                # Ensure referenced table exists in metadata (create stub if needed)
                ref_schema = pairs[0][2]
                ref_table_name = pairs[0][3]
                ref_key = f"{ref_schema}.{ref_table_name}"

                if ref_key not in tables and ref_key not in {
                        t.key for t in metadata.tables.values()}:
                    # Create a stub table with just the referenced columns
                    stub = sa.Table(ref_table_name, metadata, schema=ref_schema)
                    for _, _, _, _, pk_col in pairs:
                        if pk_col not in stub.c:
                            stub.append_column(sa.Column(pk_col, sat.NullType()))

                local_cols = [fk_col for _, fk_col, _, _, _ in pairs]
                ref_cols = [
                    f"{pk_sch}.{pk_tbl}.{pk_col}"
                    for _, _, pk_sch, pk_tbl, pk_col in pairs
                ]

                # Only add if all local columns exist
                if all(c in table.c for c in local_cols):
                    try:
                        table.append_constraint(
                            sa.ForeignKeyConstraint(
                                local_cols, ref_cols,
                                name=constraint_name
                            )
                        )
                    except Exception:
                        pass


def _apply_indexes(tables, idx_snap, schema_name):
    """Create Index objects from the indexes snapshot (skip PK indexes)."""
    if idx_snap is None:
        return

    # Index snapshot nesting: schema -> table -> index_name -> column -> attrs
    if schema_name in idx_snap:
        scope = {schema_name: idx_snap[schema_name]}
    else:
        scope = idx_snap

    for tbl_schema, tbl_dict in scope.items():
        if not isinstance(tbl_dict, dict):
            continue
        for tbl_name, index_dict in tbl_dict.items():
            if not isinstance(index_dict, dict):
                continue
            table_key = f"{tbl_schema}.{tbl_name}"
            if table_key not in tables:
                continue
            table = tables[table_key]

            for index_name, columns_dict in index_dict.items():
                if not isinstance(columns_dict, dict):
                    continue

                # Collect index columns
                idx_cols = []
                is_unique = False
                is_primary = False

                for col_name, col_attrs in columns_dict.items():
                    if not isinstance(col_attrs, dict):
                        continue
                    is_primary = is_primary or bool(
                        col_attrs.get("is_primary_key")
                        or col_attrs.get("is_primary")
                    )
                    is_unique = is_unique or bool(col_attrs.get("is_unique"))
                    is_included = bool(col_attrs.get("is_included_column"))

                    if not is_included and col_name in table.c:
                        seq = col_attrs.get("key_ordinal", 0)
                        idx_cols.append((seq, col_name))

                # Skip PK indexes
                if is_primary:
                    continue

                if idx_cols:
                    idx_cols.sort(key=lambda x: x[0])
                    try:
                        sa.Index(
                            index_name,
                            *[table.c[name] for _, name in idx_cols],
                            unique=is_unique,
                        )
                    except Exception:
                        pass


# ── High-level: evidence-driven schema federation ─────────────

# Compatible type pairs for cross-source join inference.
# If (canonical_a, canonical_b) is in this set, columns can join.
_COMPATIBLE_TYPES = {
    frozenset({sat.Integer, sat.Integer}),
    frozenset({sat.Integer, sat.BigInteger}),
    frozenset({sat.Integer, sat.SmallInteger}),
    frozenset({sat.BigInteger, sat.BigInteger}),
    frozenset({sat.SmallInteger, sat.SmallInteger}),
    frozenset({sat.String, sat.String}),
    frozenset({sat.String, sat.Unicode}),
    frozenset({sat.Unicode, sat.Unicode}),
    frozenset({sat.Text, sat.Text}),
    frozenset({sat.Uuid, sat.Uuid}),
    frozenset({sat.Date, sat.Date}),
    frozenset({sat.DateTime, sat.DateTime}),
    frozenset({sat.Numeric, sat.Numeric}),
}


def _types_compatible(type_a, type_b):
    """Check if two SQLAlchemy column types are join-compatible."""
    cls_a = type(type_a)
    cls_b = type(type_b)
    if cls_a is cls_b:
        return True
    return frozenset({cls_a, cls_b}) in _COMPATIBLE_TYPES


# ── Composable relation builders ──────────────────────────────
#
# Everything below operates on Selectables (Table, subquery, CTE).
# The building blocks are:
#
#   Layer 0 — join_from: produces a FromClause (inner/outer join)
#   Layer 1 — filters: left_orphans / right_orphans / matched
#   Layer 2 — projections: profile, top_n, coverage summary
#
# Each layer returns a Select that can be .cte()'d, .subquery()'d,
# or further composed.  Nothing is table-specific — you can pass
# in a CTE from a previous step and it works.


def equi_condition(left, right, column_pairs):
    """
    Build an equi-join ON clause between two selectables.

    Parameters
    ----------
    left, right : sa.FromClause (Table, subquery, CTE, etc.)
    column_pairs : list of (left_col_name, right_col_name)

    Returns
    -------
    sa.BinaryExpression or sa.BooleanClauseList
    """
    clauses = [left.c[lc] == right.c[rc] for lc, rc in column_pairs]
    return clauses[0] if len(clauses) == 1 else sa.and_(*clauses)


def outer_join(left, right, column_pairs):
    """
    LEFT OUTER JOIN as a FromClause.  This is the base from which
    orphan detection and coverage are derived.

    Returns
    -------
    sa.Join
    """
    return left.outerjoin(right, equi_condition(left, right, column_pairs))


def inner_join(left, right, column_pairs):
    """INNER JOIN as a FromClause."""
    return left.join(right, equi_condition(left, right, column_pairs))


# ── Layer 1: filtered selectables ─────────────────────────────

def left_orphans(left, right, column_pairs):
    """
    Rows in `left` with no match in `right`.

    Returns a Select over the outer join, filtered to WHERE right-side
    key IS NULL.  The result is a full selectable — all left columns
    are available for further composition.
    """
    j = outer_join(left, right, column_pairs)
    null_check = right.c[column_pairs[0][1]]
    return sa.select(left).select_from(j).where(null_check.is_(None))


def right_orphans(left, right, column_pairs):
    """Rows in `right` with no match in `left` (flipped outer join)."""
    return left_orphans(right, left,
                        [(rc, lc) for lc, rc in column_pairs])


def matched_rows(left, right, column_pairs):
    """
    Rows in `left` that DO have a match in `right`.

    Returns a Select over the inner join with all columns from both
    sides available.
    """
    j = inner_join(left, right, column_pairs)
    return sa.select(left, right).select_from(j)


# ── Layer 2: profiling / summarization ────────────────────────

def top_n(selectable, order_col, n=10, *, desc=True):
    """
    Window-based TOP N over any selectable.

    Uses ROW_NUMBER() so this composes as a CTE without dialect-specific
    LIMIT/TOP syntax.
    """
    col = selectable.c[order_col] if isinstance(order_col, str) else order_col
    direction = col.desc() if desc else col.asc()
    rn = sa.func.row_number().over(order_by=direction).label("_rn")

    numbered = sa.select(selectable, rn).subquery("_ranked")
    return sa.select(numbered).where(numbered.c._rn <= n)


def count_by(selectable, group_cols, *, count_label="cnt"):
    """
    GROUP BY + COUNT(*) over any selectable.

    group_cols: list of column names (str) or column objects.
    """
    cols = [
        selectable.c[c] if isinstance(c, str) else c
        for c in group_cols
    ]
    return (
        sa.select(*cols, sa.func.count().label(count_label))
        .select_from(selectable)
        .group_by(*cols)
    )


def coverage(left, right, column_pairs):
    """
    Single-row coverage summary for a join candidate.

    Returns a Select with:
      total_left, matched, orphaned, coverage_ratio
    """
    j = outer_join(left, right, column_pairs)
    left_key = left.c[column_pairs[0][0]]
    right_check = right.c[column_pairs[0][1]]

    total = sa.func.count(sa.distinct(left_key)).label("total_left")
    matched = sa.func.count(sa.distinct(
        sa.case((right_check.isnot(None), left_key))
    )).label("matched")

    return sa.select(
        total,
        matched,
        (total - matched).label("orphaned"),
        (sa.cast(matched, sa.Float)
         / sa.func.nullif(total, 0)).label("coverage_ratio"),
    ).select_from(j)


def profile_column(selectable, col_name):
    """
    Descriptive stats for a single column in any selectable.

    Returns a single-row Select with:
      total, non_null, null_count, ndv (distinct), min_val, max_val
    """
    col = selectable.c[col_name]
    return sa.select(
        sa.func.count().label("total"),
        sa.func.count(col).label("non_null"),
        (sa.func.count() - sa.func.count(col)).label("null_count"),
        sa.func.count(sa.distinct(col)).label("ndv"),
        sa.func.min(col).label("min_val"),
        sa.func.max(col).label("max_val"),
    ).select_from(selectable)


# ── JoinCandidate: thin wrapper holding the pair + metadata ───

class JoinCandidate:
    """
    A potential join path between two selectables.

    This is a data holder — the actual query generation is in the
    composable functions above.  JoinCandidate provides convenience
    methods that delegate to them.
    """

    __slots__ = ("left", "right", "column_pairs",
                 "confidence", "source")

    def __init__(self, left, right, column_pairs,
                 confidence, source):
        self.left = left              # any sa.FromClause
        self.right = right            # any sa.FromClause
        self.column_pairs = column_pairs  # [(left_col, right_col), ...]
        self.confidence = confidence  # 0.0–1.0
        self.source = source          # 'declared_fk', 'name_match', 'evidence'

    # -- delegating convenience methods --

    def condition(self):
        """Equi-join ON clause."""
        return equi_condition(self.left, self.right, self.column_pairs)

    def outer(self):
        """LEFT OUTER JOIN FromClause."""
        return outer_join(self.left, self.right, self.column_pairs)

    def inner(self):
        """INNER JOIN FromClause."""
        return inner_join(self.left, self.right, self.column_pairs)

    def left_orphans(self):
        """Left rows with no right match."""
        return left_orphans(self.left, self.right, self.column_pairs)

    def right_orphans(self):
        """Right rows with no left match."""
        return right_orphans(self.left, self.right, self.column_pairs)

    def matched(self):
        """Rows that match on both sides."""
        return matched_rows(self.left, self.right, self.column_pairs)

    def coverage(self):
        """Single-row coverage summary."""
        return coverage(self.left, self.right, self.column_pairs)

    # For backward compat with SchemaCollection internals
    @property
    def left_table(self):
        return self.left

    @property
    def right_table(self):
        return self.right

    def __repr__(self):
        left_name = getattr(self.left, 'name', str(self.left))
        right_name = getattr(self.right, 'name', str(self.right))
        pairs = ", ".join(f"{l}={r}" for l, r in self.column_pairs)
        return (f"JoinCandidate({left_name} ↔ {right_name} "
                f"on [{pairs}] conf={self.confidence:.2f} "
                f"via {self.source})")


class SchemaCollection:
    """
    A federated collection of SQLAlchemy MetaData from multiple schemas,
    with declared and inferred join paths.

    The collection is built by querying the evidence/fact layer or by
    explicitly listing schemas. Once built, it provides:
      - Unified table lookup: coll['schema.table'] or coll.table('name')
      - Declared FK joins (from snapshots)
      - Inferred joins (from column name + type matching across schemas)
      - join_condition(table_a, table_b) → best ON clause
    """

    def __init__(self, duck, metadata, schemas, join_candidates):
        self._duck = duck
        self.metadata = metadata
        self.schemas = schemas  # [(dataserver_id, catalog, schema), ...]
        self.join_candidates = join_candidates  # [JoinCandidate, ...]

    def __getitem__(self, table_key):
        """Look up a table by 'schema.table' key."""
        if table_key in self.metadata.tables:
            return self.metadata.tables[table_key]
        # Try without schema qualification
        for key, table in self.metadata.tables.items():
            if key.endswith(f".{table_key}") or table.name == table_key:
                return table
        raise KeyError(f"Table {table_key!r} not found. "
                       f"Available: {list(self.metadata.tables.keys())}")

    def tables(self):
        """All tables in the collection."""
        return dict(self.metadata.tables)

    def joins_for(self, table):
        """Return all JoinCandidates involving this table."""
        return [
            jc for jc in self.join_candidates
            if jc.left_table is table or jc.right_table is table
        ]

    def join_condition(self, left, right):
        """
        Best join condition between two tables.

        Checks declared FKs first, then inferred candidates ranked by
        confidence.  Returns a SQLAlchemy BinaryExpression or None.
        """
        candidates = [
            jc for jc in self.join_candidates
            if (jc.left_table is left and jc.right_table is right)
            or (jc.left_table is right and jc.right_table is left)
        ]
        if not candidates:
            return None
        # Prefer declared FK, then highest confidence
        candidates.sort(
            key=lambda jc: (jc.source != "declared_fk", -jc.confidence)
        )
        return candidates[0].condition()

    @classmethod
    def from_schemas(cls, duck, schema_specs):
        """
        Build a collection from explicit schema specifications.

        Parameters
        ----------
        duck : duckdb.DuckDBPyConnection
        schema_specs : list of (dataserver_id, catalog_name, schema_name)

        Returns
        -------
        SchemaCollection
        """
        metadata = sa.MetaData()
        for ds_id, catalog, schema in schema_specs:
            build_metadata(duck, ds_id, catalog, schema, metadata=metadata)

        join_candidates = _find_declared_joins(metadata)
        join_candidates += _infer_cross_schema_joins(metadata)

        return cls(duck, metadata, list(schema_specs), join_candidates)

    @classmethod
    def from_evidence(cls, duck, where_clause, bind_params=None):
        """
        Build a collection by querying the evidence layer.

        Finds schemas that have facts matching the predicate, then builds
        MetaData for each and infers cross-schema joins.

        Parameters
        ----------
        duck : duckdb.DuckDBPyConnection
        where_clause : str
            SQL WHERE predicate over rule4_metadata_fact columns.
            Example: "fact_type = 'fk_topology_role'
                      AND fact_value LIKE '%finance%'"
        bind_params : list, optional
            Bind parameters for the where clause (use ? placeholders).

        Returns
        -------
        SchemaCollection
        """
        sql = (
            "SELECT DISTINCT dataserver_id, catalog_name, schema_name "
            "FROM rule4_metadata_fact "
            f"WHERE {where_clause}"
        )
        rows = duck.execute(sql, bind_params or []).fetchall()
        schema_specs = [(r[0], r[1], r[2]) for r in rows]

        if not schema_specs:
            return cls(duck, sa.MetaData(), [], [])

        return cls.from_schemas(duck, schema_specs)

    @classmethod
    def from_topic(cls, duck, topic_pattern):
        """
        Build a collection for schemas matching a topic/domain pattern.

        Convenience wrapper over from_evidence that queries for
        schema-level topic facts.

        Parameters
        ----------
        duck : duckdb.DuckDBPyConnection
        topic_pattern : str
            SQL LIKE pattern, e.g. '%finance%', '%healthcare%'

        Returns
        -------
        SchemaCollection
        """
        return cls.from_evidence(
            duck,
            "fact_type IN ('schema_topic', 'fk_topology_role', "
            "'column_comment', 'udt_label') "
            "AND LOWER(fact_value) LIKE LOWER(?)",
            [topic_pattern]
        )


def _find_declared_joins(metadata):
    """Extract JoinCandidates from declared ForeignKeyConstraints."""
    candidates = []
    for table in metadata.tables.values():
        for fk_constraint in table.foreign_key_constraints:
            # Resolve referenced table
            ref_cols = list(fk_constraint.referred_table.columns)
            if not ref_cols:
                continue
            ref_table = fk_constraint.referred_table

            pairs = []
            for fk_elem in fk_constraint.elements:
                local_col = fk_elem.parent.name
                remote_col = fk_elem.column.name
                pairs.append((local_col, remote_col))

            if pairs:
                candidates.append(JoinCandidate(
                    left=table,
                    right=ref_table,
                    column_pairs=pairs,
                    confidence=1.0,
                    source="declared_fk",
                ))

    return candidates


def _infer_cross_schema_joins(metadata, min_confidence=0.5):
    """
    Infer join candidates across schemas by column name + type matching.

    Rules:
      - Exact column name match + compatible type → confidence 0.7
      - Column name ends with _id and matches a PK column → confidence 0.85
      - Multiple columns match between two tables → boost confidence
      - Skip pairs already connected by declared FK
    """
    candidates = []
    tables = list(metadata.tables.values())

    # Index: column_name → [(table, column)]
    col_index = {}
    for table in tables:
        for col in table.columns:
            col_index.setdefault(col.name, []).append((table, col))

    # Find declared FK pairs to skip
    declared_pairs = set()
    for table in tables:
        for fk in table.foreign_keys:
            declared_pairs.add(
                (fk.parent.table.key, fk.column.table.key)
            )
            declared_pairs.add(
                (fk.column.table.key, fk.parent.table.key)
            )

    # For each column name appearing in 2+ tables
    seen_table_pairs = {}  # (left_key, right_key) → [(left_col, right_col)]

    for col_name, occurrences in col_index.items():
        if len(occurrences) < 2:
            continue

        for i, (table_a, col_a) in enumerate(occurrences):
            for table_b, col_b in occurrences[i + 1:]:
                if table_a is table_b:
                    continue
                # Skip same-schema if already FK-connected
                pair_key = (table_a.key, table_b.key)
                if pair_key in declared_pairs:
                    continue
                # Check type compatibility
                if not _types_compatible(col_a.type, col_b.type):
                    continue
                seen_table_pairs.setdefault(pair_key, []).append(
                    (col_a.name, col_b.name)
                )

    # Score each table pair
    for (left_key, right_key), col_pairs in seen_table_pairs.items():
        left_table = metadata.tables[left_key]
        right_table = metadata.tables[right_key]

        base_conf = 0.5

        # Boost for _id columns matching PK
        for lc, rc in col_pairs:
            if lc.endswith("_id"):
                # Check if it's a PK in either table
                left_pk_names = {c.name for c in left_table.primary_key.columns}
                right_pk_names = {c.name for c in right_table.primary_key.columns}
                if lc in left_pk_names or rc in right_pk_names:
                    base_conf = max(base_conf, 0.85)
                else:
                    base_conf = max(base_conf, 0.7)

        # Boost for multiple matching columns
        if len(col_pairs) >= 2:
            base_conf = min(base_conf + 0.1, 0.95)
        if len(col_pairs) >= 3:
            base_conf = min(base_conf + 0.1, 0.95)

        if base_conf >= min_confidence:
            candidates.append(JoinCandidate(
                left=left_table,
                right=right_table,
                column_pairs=col_pairs,
                confidence=base_conf,
                source="name_match",
            ))

    return candidates
