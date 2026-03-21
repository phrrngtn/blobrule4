"""
Structural analysis: Pass 0 of the resolution sieve.

Extracts facts from TTST schema snapshots without touching any data
values.  Each analysis reads one or more snapshot kinds, computes
derived observations, and writes them to rule4_metadata_fact.

Execution model:
  - Classification rules live in reference tables (registered DataFrames)
  - Snapshot JSON is flattened via DuckDB's json_keys + unnest (pure SQL)
  - Analysis is INSERT INTO ... SELECT ... JOIN reference tables
  - Python orchestrates and generates queries; DuckDB executes set-based
  - Complex parsing (regex on CHECK clauses) uses registered Python UDFs

Phase 1 (no dependencies — pure pattern matching):
  - FK membership + topology
  - PK structure
  - Type signature classification
  - Check constraint enumeration extraction
  - Default expression analysis
  - Unique constraint facts
  - Naming pattern detection
  - Column comments

Usage:
    uv run python -m blobrule4.structural /path/to/survey.duckdb \\
        [--dataserver NAME] [--schema NAME] [--phases 1,2,3]
"""

import json
import re
import sys

import duckdb


# ── Reference data ────────────────────────────────────────────
# These become registered tables in DuckDB.  When stable, promote to PG.

SUFFIX_RULES = [
    ("_id", "key"), ("_key", "key"), ("_code", "code_dimension"),
    ("_type", "type_dimension"), ("_status", "status_dimension"),
    ("_name", "name"), ("_desc", "description"),
    ("_description", "description"), ("_date", "date"),
    ("_dt", "date"), ("_time", "time"), ("_ts", "timestamp"),
    ("_at", "timestamp"), ("_on", "timestamp"),
    ("_flag", "flag"), ("_ind", "flag"), ("_yn", "flag"),
    ("_amt", "amount"), ("_amount", "amount"),
    ("_qty", "quantity"), ("_count", "count"), ("_cnt", "count"),
    ("_pct", "percentage"), ("_percent", "percentage"),
    ("_rate", "rate"), ("_price", "price"), ("_cost", "cost"),
    ("_total", "total"), ("_num", "number"), ("_no", "number"),
    ("_number", "number"),
]

TYPE_RULES = [
    # (type_name_pattern, hint)  — matched with = or LIKE
    ("money", "measure_currency"),
    ("smallmoney", "measure_currency"),
    ("float", "measure_float"),
    ("real", "measure_float"),
    ("float4", "measure_float"),
    ("float8", "measure_float"),
    ("double precision", "measure_float"),
    ("bit", "flag_dimension"),
    ("boolean", "flag_dimension"),
    ("bool", "flag_dimension"),
    ("date", "date_dimension"),
    ("datetime", "datetime_dimension_or_audit"),
    ("datetime2", "datetime_dimension_or_audit"),
    ("smalldatetime", "datetime_dimension_or_audit"),
    ("timestamp", "datetime_dimension_or_audit"),
    ("timestamp without time zone", "datetime_dimension_or_audit"),
    ("timestamp with time zone", "datetime_dimension_or_audit"),
    ("timestamptz", "datetime_dimension_or_audit"),
    ("datetimeoffset", "datetime_dimension_or_audit"),
    ("uniqueidentifier", "surrogate_key"),
    ("uuid", "surrogate_key"),
    ("text", "content_text"),
    ("ntext", "content_text"),
    ("citext", "content_text"),
    ("xml", "structured_content"),
    ("json", "structured_content"),
    ("jsonb", "structured_content"),
    ("varbinary", "binary_content"),
    ("binary", "binary_content"),
    ("image", "binary_content"),
    ("bytea", "binary_content"),
]

DEFAULT_RULES = [
    # (regex_pattern, hint)
    (r"getdate|sysdatetime|current_timestamp|now\(\)|sysutcdatetime", "audit_timestamp"),
    (r"newid|newsequentialid|gen_random_uuid|uuid_generate", "surrogate_uuid"),
    (r"user_name|suser_sname|current_user|session_user|system_user", "audit_user"),
    (r"next\s+value\s+for|nextval", "surrogate_sequence"),
]


_REFS_REGISTERED = set()

def _register_reference_tables(duck):
    """Register classification rule tables as DataFrames (idempotent)."""
    if id(duck) in _REFS_REGISTERED:
        return
    _REFS_REGISTERED.add(id(duck))

    import pyarrow as pa
    duck.register("_suffix_rules", pa.table({
        "suffix": [s for s, _ in SUFFIX_RULES],
        "semantic_role": [r for _, r in SUFFIX_RULES],
    }))

    duck.register("_type_rules", pa.table({
        "type_pattern": [t for t, _ in TYPE_RULES],
        "type_hint": [h for _, h in TYPE_RULES],
    }))


_UDFS_REGISTERED = set()

def _register_udfs(duck):
    """Register Python UDFs for logic that needs regex (idempotent)."""
    if id(duck) in _UDFS_REGISTERED:
        return
    _UDFS_REGISTERED.add(id(duck))

    def classify_default(default_def):
        """Classify a default expression into a semantic hint."""
        if default_def is None:
            return None
        raw = default_def.strip()
        for pattern, hint in DEFAULT_RULES:
            if re.search(pattern, raw, re.IGNORECASE):
                return hint
        stripped = raw.strip("() ")
        if stripped in ("0", "0.0", "0.00"):
            return "measure_baseline"
        if stripped == "1":
            return "measure_baseline"
        if stripped == "''":
            return "optional_text"
        # Single-quoted uppercase string → status enum
        m = re.match(r"^['\(]*'([^']+)'['\)]*$", raw)
        if m and m.group(1).isupper():
            return "status_enum"
        return "other_default"

    duck.create_function("classify_default", classify_default,
                         ["VARCHAR"], "VARCHAR")

    def extract_check_enum(check_clause):
        """Extract IN-list values from a CHECK constraint clause.
        Returns JSON array string, or NULL if no IN-list found."""
        if check_clause is None:
            return None
        m = re.search(r"\bIN\s*\(\s*(.+?)\s*\)", check_clause, re.IGNORECASE)
        if not m:
            return None
        raw = m.group(1)
        values = []
        for vm in re.finditer(r"'([^']*)'|(\d+(?:\.\d+)?)", raw):
            values.append(vm.group(1) if vm.group(1) is not None else vm.group(2))
        return json.dumps(values) if values else None

    duck.create_function("extract_check_enum", extract_check_enum,
                         ["VARCHAR"], "VARCHAR")


# ── Snapshot flattening SQL generators ────────────────────────
# These produce CTEs that unnest nested snapshot JSON into flat rows.

def _flatten_cte(kind, depth, key_names, *, suffix=""):
    """
    Generate a CTE that flattens a snapshot JSON of given nesting depth.

    Returns (cte_name, sql_fragment) where the fragment is a single CTE
    definition (no nested WITH — reads directly from rule4_schema_snapshot).

    suffix: optional string to disambiguate when multiple flatten CTEs
    are used in the same query (e.g., suffix="_REF").
    """
    cte_name = f"FLAT_{kind.upper()}{suffix}"

    select_cols = ["r.dataserver_id", "r.catalog_name", "r.schema_name",
                   "r.revision_num"]
    laterals = []
    doc_expr = "r.doc"

    for i, key_name in enumerate(key_names):
        ki = f"k{i+1}"
        select_cols.append(f"{ki}.key AS {key_name}")
        laterals.append(
            f"LATERAL (SELECT unnest(json_keys({doc_expr})) AS key) AS {ki}")
        doc_expr = f"({doc_expr} -> {ki}.key)"

    select_cols.append(f"{doc_expr} AS attrs")

    return cte_name, f"""
    {cte_name} AS (
        SELECT {', '.join(select_cols)}
        FROM (
            SELECT dataserver_id, catalog_name, schema_name,
                   revision_num, snapshot::JSON AS doc
            FROM rule4_schema_snapshot
            WHERE kind = '{kind}'
              AND dataserver_id = ?
              AND catalog_name = ?
              AND schema_name = ?
        ) AS r,
        {', '.join(laterals)}
    )"""


# ── Analysis SQL generators ───────────────────────────────────

def _type_signature_sql():
    """Generate INSERT that classifies columns by type."""
    cte_name, cte_sql = _flatten_cte(
        "columns", 3, ["table_schema", "table_name", "column_name"])

    return f"""
    WITH {cte_sql},
    CLASSIFIED AS (
        SELECT f.*,
               COALESCE(f.attrs ->> 'type_name', f.attrs ->> 'data_type', 'unknown') AS type_name,
               CAST(f.attrs ->> 'max_length' AS INTEGER) AS max_length,
               CAST(f.attrs ->> 'precision' AS INTEGER) AS col_precision,
               CAST(f.attrs ->> 'scale' AS INTEGER) AS col_scale,
               f.attrs ->> 'is_nullable' AS is_nullable,
               f.attrs ->> 'is_identity' AS is_identity,
               f.attrs ->> 'is_computed' AS is_computed,
               f.attrs ->> 'default_definition' AS default_definition
        FROM {cte_name} AS f
    ),
    WITH_HINT AS (
        SELECT c.*,
               COALESCE(
                   tr.type_hint,
                   -- Decimal with scale > 0
                   CASE WHEN LOWER(c.type_name) IN ('decimal', 'numeric')
                             AND c.col_scale > 0
                        THEN 'measure_decimal' END,
                   -- Integer types
                   CASE WHEN LOWER(c.type_name) IN ('int', 'integer', 'int4',
                             'bigint', 'int8', 'smallint', 'tinyint', 'serial',
                             'bigserial', 'smallserial')
                        THEN CASE WHEN suffix(LOWER(c.column_name), '_id')
                                       OR suffix(LOWER(c.column_name), '_key')
                                  THEN 'key_integer'
                                  ELSE 'measure_or_key' END
                   END,
                   -- String types by length
                   CASE WHEN LOWER(c.type_name) IN ('varchar', 'nvarchar', 'char',
                             'nchar', 'character varying', 'bpchar', 'character')
                        THEN CASE WHEN c.max_length > 0 AND c.max_length <= 10
                                  THEN 'code_dimension'
                                  WHEN c.max_length > 10 AND c.max_length <= 510
                                  THEN 'name_dimension'
                                  ELSE 'content_text' END
                   END,
                   'unknown'
               ) AS type_hint
        FROM CLASSIFIED AS c
        LEFT JOIN _type_rules AS tr
            ON LOWER(c.type_name) = tr.type_pattern
    )
    INSERT INTO rule4_metadata_fact
        (fact_id, dataserver_id, catalog_name, schema_name, table_name,
         column_name, fact_type, fact_value, tier, source_kind,
         source_revision, observed_at)
    SELECT nextval('seq_metadata_fact'),
           dataserver_id, catalog_name, schema_name, table_name,
           column_name, 'type_signature',
           json_object(
               'type_name', type_name,
               'hint', type_hint,
               'max_length', max_length,
               'precision', col_precision,
               'scale', col_scale,
               'is_nullable', is_nullable,
               'is_identity', is_identity,
               'is_computed', is_computed
           ),
           1, 'columns', revision_num, CURRENT_TIMESTAMP
    FROM WITH_HINT
    """


def _fk_membership_sql():
    """Generate INSERT for FK column-level membership facts."""
    cte_name, cte_sql = _flatten_cte(
        "foreign_keys", 4,
        ["fk_schema", "fk_table", "constraint_name", "fk_column"])

    return f"""
    WITH {cte_sql}
    INSERT INTO rule4_metadata_fact
        (fact_id, dataserver_id, catalog_name, schema_name, table_name,
         column_name, fact_type, fact_value, tier, source_kind,
         source_revision, observed_at)
    SELECT nextval('seq_metadata_fact'),
           dataserver_id, catalog_name, schema_name, fk_table,
           fk_column, 'fk_member',
           json_object(
               'constraint', constraint_name,
               'references', CONCAT(attrs ->> 'pk_schema', '.', attrs ->> 'pk_table', '.', attrs ->> 'pk_column'),
               'pk_schema', attrs ->> 'pk_schema',
               'pk_table', attrs ->> 'pk_table',
               'pk_column', attrs ->> 'pk_column'
           ),
           1, 'foreign_keys', revision_num, CURRENT_TIMESTAMP
    FROM {cte_name}
    WHERE attrs ->> 'pk_table' IS NOT NULL
    """


def _fk_topology_sql():
    """Generate INSERT for table-level FK topology facts."""
    cte_name, cte_sql = _flatten_cte(
        "foreign_keys", 4,
        ["fk_schema", "fk_table", "constraint_name", "fk_column"])

    return f"""
    WITH {cte_sql},
    FK_EDGES AS (
        SELECT DISTINCT
               dataserver_id, catalog_name, schema_name, revision_num,
               fk_schema, fk_table,
               attrs ->> 'pk_schema' AS pk_schema,
               attrs ->> 'pk_table' AS pk_table
        FROM {cte_name}
        WHERE attrs ->> 'pk_table' IS NOT NULL
    ),
    OUTBOUND AS (
        SELECT dataserver_id, catalog_name, schema_name, revision_num,
               fk_schema AS tbl_schema, fk_table AS tbl_name,
               COUNT(DISTINCT pk_table) AS outbound_count,
               LIST(DISTINCT CONCAT(pk_schema, '.', pk_table)) AS refs
        FROM FK_EDGES
        GROUP BY ALL
    ),
    INBOUND AS (
        SELECT dataserver_id, catalog_name, schema_name, revision_num,
               pk_schema AS tbl_schema, pk_table AS tbl_name,
               COUNT(DISTINCT fk_table) AS inbound_count,
               LIST(DISTINCT CONCAT(fk_schema, '.', fk_table)) AS refd_by
        FROM FK_EDGES
        GROUP BY ALL
    ),
    TOPOLOGY AS (
        SELECT COALESCE(o.dataserver_id, i.dataserver_id) AS dataserver_id,
               COALESCE(o.catalog_name, i.catalog_name) AS catalog_name,
               COALESCE(o.schema_name, i.schema_name) AS schema_name,
               COALESCE(o.revision_num, i.revision_num) AS revision_num,
               COALESCE(o.tbl_schema, i.tbl_schema) AS tbl_schema,
               COALESCE(o.tbl_name, i.tbl_name) AS tbl_name,
               COALESCE(o.outbound_count, 0) AS outbound_count,
               COALESCE(i.inbound_count, 0) AS inbound_count,
               o.refs,
               i.refd_by,
               CASE
                   WHEN COALESCE(o.outbound_count, 0) >= 2
                        AND COALESCE(i.inbound_count, 0) = 0
                   THEN 'fact'
                   WHEN COALESCE(o.outbound_count, 0) = 0
                        AND COALESCE(i.inbound_count, 0) >= 1
                   THEN 'dimension'
                   WHEN COALESCE(o.outbound_count, 0) >= 1
                        AND COALESCE(i.inbound_count, 0) >= 1
                   THEN 'bridge'
                   WHEN COALESCE(o.outbound_count, 0) = 1
                        AND COALESCE(i.inbound_count, 0) = 0
                   THEN 'detail'
                   ELSE 'mixed'
               END AS role
        FROM OUTBOUND AS o
        FULL OUTER JOIN INBOUND AS i
            ON o.dataserver_id = i.dataserver_id
           AND o.catalog_name = i.catalog_name
           AND o.schema_name = i.schema_name
           AND o.tbl_schema = i.tbl_schema
           AND o.tbl_name = i.tbl_name
    )
    INSERT INTO rule4_metadata_fact
        (fact_id, dataserver_id, catalog_name, schema_name, table_name,
         column_name, fact_type, fact_value, tier, source_kind,
         source_revision, observed_at)
    SELECT nextval('seq_metadata_fact'),
           dataserver_id, catalog_name, schema_name, tbl_name,
           NULL, 'fk_topology_role',
           json_object(
               'role', role,
               'outbound_fk_count', outbound_count,
               'inbound_fk_count', inbound_count
           ),
           1, 'foreign_keys', revision_num, CURRENT_TIMESTAMP
    FROM TOPOLOGY
    """


def _pk_structure_sql():
    """Generate INSERT for PK membership and structure facts."""
    cte_name, cte_sql = _flatten_cte(
        "primary_keys", 4,
        ["table_schema", "table_name", "constraint_name", "column_name"])

    return f"""
    WITH {cte_sql},
    PK_GROUPS AS (
        SELECT dataserver_id, catalog_name, schema_name, revision_num,
               table_schema, table_name, constraint_name,
               COUNT(*) AS pk_col_count,
               LIST(column_name ORDER BY CAST(attrs ->> 'key_seq' AS INTEGER)) AS pk_columns
        FROM {cte_name}
        GROUP BY ALL
    ),
    -- Column-level PK facts
    PK_COLS AS (
        SELECT f.dataserver_id, f.catalog_name, f.schema_name,
               f.revision_num, f.table_name, f.column_name,
               f.constraint_name,
               g.pk_col_count,
               f.attrs ->> 'key_seq' AS key_seq,
               CASE
                   WHEN g.pk_col_count = 1 THEN 'single'
                   ELSE 'composite'
               END AS pk_type
        FROM {cte_name} AS f
        JOIN PK_GROUPS AS g
            ON g.dataserver_id = f.dataserver_id
           AND g.catalog_name = f.catalog_name
           AND g.schema_name = f.schema_name
           AND g.table_schema = f.table_schema
           AND g.table_name = f.table_name
           AND g.constraint_name = f.constraint_name
    )
    INSERT INTO rule4_metadata_fact
        (fact_id, dataserver_id, catalog_name, schema_name, table_name,
         column_name, fact_type, fact_value, tier, source_kind,
         source_revision, observed_at)
    -- Column-level facts
    SELECT nextval('seq_metadata_fact'),
           dataserver_id, catalog_name, schema_name, table_name,
           column_name, 'pk_member',
           json_object(
               'constraint', constraint_name,
               'pk_column_count', pk_col_count,
               'key_seq', key_seq,
               'pk_type', pk_type
           ),
           1, 'primary_keys', revision_num, CURRENT_TIMESTAMP
    FROM PK_COLS
    UNION ALL
    -- Table-level facts
    SELECT nextval('seq_metadata_fact'),
           dataserver_id, catalog_name, schema_name, table_name,
           NULL, 'pk_structure',
           json_object(
               'constraint', constraint_name,
               'column_count', pk_col_count,
               'columns', pk_columns
           ),
           1, 'primary_keys', revision_num, CURRENT_TIMESTAMP
    FROM PK_GROUPS
    """


def _defaults_sql():
    """Generate INSERT for default expression classification (uses UDF)."""
    cte_name, cte_sql = _flatten_cte(
        "columns", 3, ["table_schema", "table_name", "column_name"])

    return f"""
    WITH {cte_sql},
    DEFAULTS AS (
        SELECT dataserver_id, catalog_name, schema_name, revision_num,
               table_name, column_name,
               attrs ->> 'default_definition' AS default_def,
               classify_default(attrs ->> 'default_definition') AS hint
        FROM {cte_name}
        WHERE attrs ->> 'default_definition' IS NOT NULL
    )
    INSERT INTO rule4_metadata_fact
        (fact_id, dataserver_id, catalog_name, schema_name, table_name,
         column_name, fact_type, fact_value, tier, source_kind,
         source_revision, observed_at)
    SELECT nextval('seq_metadata_fact'),
           dataserver_id, catalog_name, schema_name, table_name,
           column_name, 'default_hint',
           json_object('default_definition', default_def, 'hint', hint),
           1, 'columns', revision_num, CURRENT_TIMESTAMP
    FROM DEFAULTS
    """


def _naming_patterns_sql():
    """Generate INSERT for naming pattern detection via JOIN to suffix_rules."""
    cte_name, cte_sql = _flatten_cte(
        "columns", 3, ["table_schema", "table_name", "column_name"])

    return f"""
    WITH {cte_sql},
    -- Suffix matching via JOIN
    SUFFIX_MATCHES AS (
        SELECT f.dataserver_id, f.catalog_name, f.schema_name,
               f.revision_num, f.table_name, f.column_name,
               sr.suffix, sr.semantic_role,
               ROW_NUMBER() OVER (
                   PARTITION BY f.dataserver_id, f.catalog_name, f.schema_name,
                                f.table_name, f.column_name
                   ORDER BY LENGTH(sr.suffix) DESC
               ) AS rn
        FROM {cte_name} AS f
        JOIN _suffix_rules AS sr
            ON LOWER(f.column_name) LIKE '%' || sr.suffix
    )
    INSERT INTO rule4_metadata_fact
        (fact_id, dataserver_id, catalog_name, schema_name, table_name,
         column_name, fact_type, fact_value, tier, source_kind,
         source_revision, observed_at)
    SELECT nextval('seq_metadata_fact'),
           dataserver_id, catalog_name, schema_name, table_name,
           column_name, 'naming_pattern',
           json_object('pattern_type', 'suffix', 'pattern', suffix,
                       'semantic_role', semantic_role),
           1, 'columns', revision_num, CURRENT_TIMESTAMP
    FROM SUFFIX_MATCHES
    WHERE rn = 1
    """


def _candidate_fk_sql():
    """
    Detect columns named {table}_id where {table} or {table}s exists.
    Pure SQL: JOIN columns against tables snapshot on name matching.
    """
    col_cte, col_sql = _flatten_cte(
        "columns", 3, ["table_schema", "table_name", "column_name"])
    tbl_cte, tbl_sql = _flatten_cte(
        "tables", 2, ["tbl_schema", "tbl_name"], suffix="_REF")

    return f"""
    WITH {col_sql},
    {tbl_sql},
    ALL_TABLE_NAMES AS (
        SELECT DISTINCT LOWER(tbl_name) AS tbl_name_lower
        FROM {tbl_cte}
    ),
    CANDIDATE_FKS AS (
        SELECT c.dataserver_id, c.catalog_name, c.schema_name,
               c.revision_num, c.table_name, c.column_name,
               t.tbl_name_lower AS ref_table
        FROM {col_cte} AS c
        JOIN ALL_TABLE_NAMES AS t
            ON (
                -- customer_id → customers
                LOWER(c.column_name) = t.tbl_name_lower || '_id'
                -- customer_id → customer
                OR REPLACE(LOWER(c.column_name), '_id', '') = t.tbl_name_lower
                -- customer_id → customer (with trailing s on table)
                OR REPLACE(LOWER(c.column_name), '_id', '') || 's' = t.tbl_name_lower
            )
        WHERE suffix(LOWER(c.column_name), '_id')
          AND t.tbl_name_lower != LOWER(c.table_name)
    )
    INSERT INTO rule4_metadata_fact
        (fact_id, dataserver_id, catalog_name, schema_name, table_name,
         column_name, fact_type, fact_value, tier, source_kind,
         source_revision, observed_at)
    SELECT nextval('seq_metadata_fact'),
           dataserver_id, catalog_name, schema_name, table_name,
           column_name, 'naming_pattern',
           json_object('pattern_type', 'table_ref',
                       'pattern', ref_table,
                       'semantic_role', 'candidate_fk'),
           1, 'columns', revision_num, CURRENT_TIMESTAMP
    FROM CANDIDATE_FKS
    """


def _check_enums_sql():
    """Generate INSERT for check constraint enum extraction (uses UDF)."""
    cte_name, cte_sql = _flatten_cte(
        "check_constraints", 3,
        ["table_schema", "table_name", "constraint_name"])

    return f"""
    WITH {cte_sql}
    INSERT INTO rule4_metadata_fact
        (fact_id, dataserver_id, catalog_name, schema_name, table_name,
         column_name, fact_type, fact_value, tier, source_kind,
         source_revision, observed_at)
    SELECT nextval('seq_metadata_fact'),
           dataserver_id, catalog_name, schema_name, table_name,
           attrs ->> 'column_name', 'check_enum',
           json_object(
               'constraint', constraint_name,
               'values', extract_check_enum(attrs ->> 'check_clause')::JSON,
               'check_clause', attrs ->> 'check_clause'
           ),
           1, 'check_constraints', revision_num, CURRENT_TIMESTAMP
    FROM {cte_name}
    WHERE extract_check_enum(attrs ->> 'check_clause') IS NOT NULL
    """


def _unique_constraints_sql():
    """Generate INSERT for unique constraint membership facts."""
    cte_name, cte_sql = _flatten_cte(
        "unique_constraints", 4,
        ["table_schema", "table_name", "constraint_name", "column_name"])

    return f"""
    WITH {cte_sql}
    INSERT INTO rule4_metadata_fact
        (fact_id, dataserver_id, catalog_name, schema_name, table_name,
         column_name, fact_type, fact_value, tier, source_kind,
         source_revision, observed_at)
    SELECT nextval('seq_metadata_fact'),
           dataserver_id, catalog_name, schema_name, table_name,
           column_name, 'unique_member',
           json_object(
               'constraint', constraint_name,
               'key_seq', attrs ->> 'key_seq'
           ),
           1, 'unique_constraints', revision_num, CURRENT_TIMESTAMP
    FROM {cte_name}
    """


def _column_comments_sql():
    """Generate INSERT for column comment facts."""
    cte_name, cte_sql = _flatten_cte(
        "column_comments", 3,
        ["table_schema", "table_name", "column_name"])

    return f"""
    WITH {cte_sql}
    INSERT INTO rule4_metadata_fact
        (fact_id, dataserver_id, catalog_name, schema_name, table_name,
         column_name, fact_type, fact_value, tier, source_kind,
         source_revision, observed_at)
    SELECT nextval('seq_metadata_fact'),
           dataserver_id, catalog_name, schema_name, table_name,
           column_name, 'column_comment',
           json_object(
               'comment', COALESCE(attrs ->> 'column_comment',
                                   attrs ->> 'property_value')
           ),
           1, 'column_comments', revision_num, CURRENT_TIMESTAMP
    FROM {cte_name}
    WHERE COALESCE(attrs ->> 'column_comment', attrs ->> 'property_value') IS NOT NULL
    """


# ── Fact table setup ──────────────────────────────────────────

def ensure_fact_table(duck):
    """Create rule4_metadata_fact and sequence if they don't exist."""
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
    duck.execute("CREATE SEQUENCE IF NOT EXISTS seq_metadata_fact START 1")


def clear_tier1_facts(duck, ds_id, cat, sch):
    """Remove existing tier-1 facts for a scope before re-analysis."""
    duck.execute(
        "DELETE FROM rule4_metadata_fact "
        "WHERE dataserver_id = ? AND catalog_name = ? "
        "AND schema_name = ? AND tier = 1",
        [ds_id, cat, sch]
    )


# ── Orchestration ─────────────────────────────────────────────

# Each entry: (name, sql_generator, snapshot_kind_required)
PHASE_1_ANALYSES = [
    ("type_signatures", _type_signature_sql, "columns"),
    ("fk_membership", _fk_membership_sql, "foreign_keys"),
    ("fk_topology", _fk_topology_sql, "foreign_keys"),
    ("pk_structure", _pk_structure_sql, "primary_keys"),
    ("defaults", _defaults_sql, "columns"),
    ("naming_patterns", _naming_patterns_sql, "columns"),
    ("candidate_fks", _candidate_fk_sql, "tables"),
    ("check_enums", _check_enums_sql, "check_constraints"),
    ("unique_constraints", _unique_constraints_sql, "unique_constraints"),
    ("column_comments", _column_comments_sql, "column_comments"),
]


def _snapshot_exists(duck, ds_id, cat, sch, kind):
    """Check if a snapshot exists for the given scope and kind."""
    row = duck.execute(
        "SELECT 1 FROM rule4_schema_snapshot "
        "WHERE dataserver_id = ? AND catalog_name = ? "
        "AND schema_name = ? AND kind = ? LIMIT 1",
        [ds_id, cat, sch, kind]
    ).fetchone()
    return row is not None


def run_phase_1(duck, ds_id, cat, sch, verbose=True):
    """Run all Phase 1 analyses as set-based DuckDB SQL."""
    total = 0
    bind_params = [ds_id, cat, sch]

    for name, sql_gen, required_kind in PHASE_1_ANALYSES:
        if not _snapshot_exists(duck, ds_id, cat, sch, required_kind):
            if verbose:
                print(f"  {name}: skipped (no {required_kind} snapshot)",
                      file=sys.stderr)
            continue

        sql = sql_gen()
        # Count ? placeholders — each _flatten_cte needs 3 params
        n_params = sql.count("?")
        actual_params = bind_params * (n_params // 3) if n_params > 3 else bind_params
        try:
            duck.execute(sql, actual_params)
        except Exception as e:
            if verbose:
                print(f"  {name}: ERROR {e}", file=sys.stderr)
            continue

        if verbose:
            print(f"  {name}: done", file=sys.stderr)

    # Count total facts for this scope
    total = duck.execute(
        "SELECT COUNT(*) FROM rule4_metadata_fact "
        "WHERE dataserver_id = ? AND catalog_name = ? "
        "AND schema_name = ? AND tier = 1",
        bind_params
    ).fetchone()[0]

    return total


def run_structural(duck, ds_id, cat, sch, *, phases=None, verbose=True):
    """
    Run structural analysis for a single (dataserver, catalog, schema).
    """
    if phases is None:
        phases = {1}

    ensure_fact_table(duck)
    _register_reference_tables(duck)
    _register_udfs(duck)

    if verbose:
        print(f"Structural analysis: ds={ds_id} cat={cat} sch={sch}",
              file=sys.stderr)

    # Clear and re-analyze
    if 1 in phases:
        clear_tier1_facts(duck, ds_id, cat, sch)
        total = run_phase_1(duck, ds_id, cat, sch, verbose=verbose)
        if verbose:
            print(f"  Total tier-1 facts: {total}", file=sys.stderr)
    else:
        total = 0

    if 2 in phases and verbose:
        print("  Phase 2 not yet implemented (needs blobembed)",
              file=sys.stderr)
    if 3 in phases and verbose:
        print("  Phase 3 not yet implemented (needs Phase 2)",
              file=sys.stderr)

    return total


# ── CLI entry point ───────────────────────────────────────────

def main():
    import argparse

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
