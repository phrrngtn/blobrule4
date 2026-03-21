"""
SQLAlchemy MetaData generator from rule4 schema snapshots.

Reads rule4_schema_snapshot JSON for a given (dataserver_id, catalog_name,
schema_name) and produces a sqlalchemy.MetaData populated with Table,
Column, PrimaryKeyConstraint, ForeignKeyConstraint, and Index objects.

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
