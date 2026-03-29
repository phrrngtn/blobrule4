"""
blobrule4.socrata.catalog — SQLAlchemy models for the Socrata catalog TTST tables.

Defines the three-tier schema:
  Tier 1: domain_version       — domain emergence / resource count changes
  Tier 2: resource, resource_column — structure / metadata changes
  Tier 3: resource_data_hwm    — data freshness tracking

Also: resource_view, resource_view_column (from Views API, richer column stats).

All tables follow the TTST pattern: (business_key..., tt_start, tt_end, ...attributes).
tt_start is source-authoritative (Socrata's metadata_updated_at or data_updated_at).
tt_end uses '9999-12-31' sentinel for current rows.

Usage:
    from blobrule4.socrata.catalog import build_metadata, clone_ttst

    # Create tables in any dialect
    meta = build_metadata(schema="socrata")
    meta.create_all(engine, checkfirst=True)

    # Clone TTST history from PG to local SQLite/DuckDB
    clone_ttst(source_engine, target_engine, domain="data.cityofnewyork.us")
"""

from __future__ import annotations

from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    select,
)


def build_metadata(schema: str | None = "socrata") -> MetaData:
    """Build SQLAlchemy MetaData with all Socrata catalog TTST tables.

    Returns a new MetaData instance each call (safe for multi-dialect use).
    """
    meta = MetaData(schema=schema)

    # ── Tier 1: domain emergence ─────────────────────────────────────

    domain_version = Table(
        "domain_version", meta,
        Column("domain", String, primary_key=True),
        Column("tt_start", DateTime(timezone=True), primary_key=True, nullable=False),
        Column("tt_end", DateTime(timezone=True), nullable=False),
        Column("resource_count", Integer, nullable=False),
    )
    Index("idx_domain_version_current", domain_version.c.domain,
          postgresql_where=domain_version.c.tt_end == "9999-12-31")

    # ── Tier 2: resource structure ───────────────────────────────────

    resource = Table(
        "resource", meta,
        Column("domain", String, nullable=False),
        Column("resource_id", String, nullable=False),
        Column("tt_start", DateTime(timezone=True), nullable=False),
        Column("tt_end", DateTime(timezone=True), nullable=False),
        Column("name", Text),
        Column("description", Text),
        Column("resource_type", String),
        Column("permalink", String),
        Column("attribution", String),
        Column("attribution_link", String),
        Column("provenance", String),
        Column("created_at", String),
        Column("updated_at", String),
        Column("metadata_updated_at", String),
        Column("data_updated_at", String),
        Column("publication_date", String),
        Column("page_views_total", Integer),
        Column("download_count", Integer),
        Column("domain_category", String),
        Column("categories", Text),       # JSONB in PG, TEXT elsewhere
        Column("domain_tags", Text),      # JSONB in PG, TEXT elsewhere
        Column("owner", Text),            # JSONB in PG, TEXT elsewhere
        Column("creator", Text),          # JSONB in PG, TEXT elsewhere
        Column("resource_json", Text),    # JSONB in PG, TEXT elsewhere
        Column("classification_json", Text),  # JSONB in PG, TEXT elsewhere
    )
    # Composite PK
    resource.append_constraint(
        resource.primary_key.__class__(
            resource.c.domain, resource.c.resource_id, resource.c.tt_start
        )
    )
    Index("idx_resource_current", resource.c.domain, resource.c.resource_id,
          postgresql_where=resource.c.tt_end == "9999-12-31")

    resource_column = Table(
        "resource_column", meta,
        Column("domain", String, nullable=False),
        Column("resource_id", String, nullable=False),
        Column("field_name", String, nullable=False),
        Column("tt_start", DateTime(timezone=True), nullable=False),
        Column("tt_end", DateTime(timezone=True), nullable=False),
        Column("ordinal_position", Integer),
        Column("display_name", Text),
        Column("data_type", String),
        Column("description", Text),
    )
    resource_column.append_constraint(
        resource_column.primary_key.__class__(
            resource_column.c.domain, resource_column.c.resource_id,
            resource_column.c.field_name, resource_column.c.tt_start
        )
    )
    Index("idx_resource_column_current",
          resource_column.c.domain, resource_column.c.resource_id,
          resource_column.c.field_name,
          postgresql_where=resource_column.c.tt_end == "9999-12-31")

    # ── Views API tables (richer column stats) ───────────────────────

    resource_view = Table(
        "resource_view", meta,
        Column("domain", String, nullable=False),
        Column("resource_id", String, nullable=False),
        Column("tt_start", DateTime(timezone=True), nullable=False),
        Column("tt_end", DateTime(timezone=True), nullable=False),
        Column("name", Text),
        Column("description", Text),
        Column("asset_type", String),
        Column("category", String),
        Column("display_type", String),
        Column("provenance", String),
        Column("view_type", String),
        Column("created_at", DateTime(timezone=True)),
        Column("publication_date", DateTime(timezone=True)),
        Column("view_last_modified", DateTime(timezone=True)),
        Column("rows_updated_at", DateTime(timezone=True)),
        Column("view_count", Integer),
        Column("download_count", Integer),
        Column("view_json", Text),  # JSONB in PG, TEXT elsewhere
    )
    resource_view.append_constraint(
        resource_view.primary_key.__class__(
            resource_view.c.domain, resource_view.c.resource_id,
            resource_view.c.tt_start
        )
    )
    Index("idx_resource_view_current",
          resource_view.c.domain, resource_view.c.resource_id,
          postgresql_where=resource_view.c.tt_end == "9999-12-31")

    resource_view_column = Table(
        "resource_view_column", meta,
        Column("domain", String, nullable=False),
        Column("resource_id", String, nullable=False),
        Column("field_name", String, nullable=False),
        Column("tt_start", DateTime(timezone=True), nullable=False),
        Column("tt_end", DateTime(timezone=True), nullable=False),
        Column("ordinal_position", Integer),
        Column("display_name", Text),
        Column("data_type", String),
        Column("render_type", String),
        Column("description", Text),
        Column("cached_non_null", BigInteger),
        Column("cached_null", BigInteger),
        Column("cached_count", BigInteger),
        Column("cached_cardinality", BigInteger),
        Column("cached_smallest", Text),
        Column("cached_largest", Text),
        Column("column_json", Text),  # JSONB in PG, TEXT elsewhere
    )
    resource_view_column.append_constraint(
        resource_view_column.primary_key.__class__(
            resource_view_column.c.domain, resource_view_column.c.resource_id,
            resource_view_column.c.field_name, resource_view_column.c.tt_start
        )
    )
    Index("idx_resource_view_column_current",
          resource_view_column.c.domain, resource_view_column.c.resource_id,
          resource_view_column.c.field_name,
          postgresql_where=resource_view_column.c.tt_end == "9999-12-31")

    # ── Tier 3: data freshness ───────────────────────────────────────

    resource_data_hwm = Table(
        "resource_data_hwm", meta,
        Column("domain", String, nullable=False),
        Column("resource_id", String, nullable=False),
        Column("tt_start", DateTime(timezone=True), nullable=False),
        Column("tt_end", DateTime(timezone=True), nullable=False),
        Column("data_updated_at", DateTime(timezone=True), nullable=False),
        Column("rows_updated_at", DateTime(timezone=True)),
        Column("row_count", BigInteger),
    )
    resource_data_hwm.append_constraint(
        resource_data_hwm.primary_key.__class__(
            resource_data_hwm.c.domain, resource_data_hwm.c.resource_id,
            resource_data_hwm.c.tt_start
        )
    )
    Index("idx_resource_data_hwm_current",
          resource_data_hwm.c.domain, resource_data_hwm.c.resource_id,
          postgresql_where=resource_data_hwm.c.tt_end == "9999-12-31")

    # ── Non-temporal tracking tables ─────────────────────────────────

    Table(
        "domain", meta,
        Column("domain", String, primary_key=True),
        Column("resource_count", Integer),
        Column("last_scraped_at", DateTime(timezone=True)),
        Column("first_seen_at", DateTime(timezone=True)),
    )

    Table(
        "resource_view_detail_hwm", meta,
        Column("domain", String, nullable=False),
        Column("resource_id", String, nullable=False),
        Column("view_last_modified", BigInteger, nullable=False),
        Column("fetched_at", DateTime(timezone=True)),
    )

    return meta


# ── TTST Clone / Initialization ──────────────────────────────────────

# Tables to clone, in dependency order.
TTST_TABLES = [
    "domain_version",
    "resource",
    "resource_column",
    "resource_view",
    "resource_view_column",
    "resource_data_hwm",
]


def clone_ttst(source_engine, target_engine, domain=None,
               tables=None, current_only=False, batch_size=5000):
    """Clone TTST history from source to target database.

    Like ``git clone`` — copies temporal history so the target can independently
    tail from max(tt_start) going forward.

    Args:
        source_engine: SA engine for the source database (e.g. PG).
        target_engine: SA engine for the target database (e.g. SQLite, DuckDB).
        domain: If set, only clone rows for this domain. None = all domains.
        tables: List of table names to clone. None = all TTST tables.
        current_only: If True, only copy current rows (tt_end = '9999-12-31').
            Produces a snapshot, not a full history clone.
        batch_size: Rows per INSERT batch.

    The target schema is created if it doesn't exist. Existing rows in the
    target are NOT deleted — this is additive (merge-safe).
    """
    clone_tables = tables or TTST_TABLES

    # Build separate MetaData for source and target (may have different schemas)
    source_meta = build_metadata(schema="socrata")
    target_meta = build_metadata(schema=None)  # no schema for SQLite/DuckDB

    # Create target tables
    target_meta.create_all(target_engine, checkfirst=True)

    for table_name in clone_tables:
        source_table = source_meta.tables.get(f"socrata.{table_name}")
        target_table = target_meta.tables.get(table_name)
        if source_table is None or target_table is None:
            continue

        # Build SELECT from source
        stmt = select(source_table)
        if domain and "domain" in [c.name for c in source_table.columns]:
            stmt = stmt.where(source_table.c.domain == domain)
        if current_only and "tt_end" in [c.name for c in source_table.columns]:
            stmt = stmt.where(source_table.c.tt_end == "9999-12-31")

        # Stream from source, batch-insert into target
        col_names = [c.name for c in source_table.columns]
        n_rows = 0

        with source_engine.connect() as src_conn:
            result = src_conn.execute(stmt)
            batch = []

            for row in result:
                row_dict = dict(zip(col_names, row))
                # Serialize any non-scalar values (JSONB → TEXT for SQLite/DuckDB)
                for k, v in row_dict.items():
                    if isinstance(v, (dict, list)):
                        row_dict[k] = __import__("json").dumps(v)
                batch.append(row_dict)

                if len(batch) >= batch_size:
                    with target_engine.begin() as tgt_conn:
                        tgt_conn.execute(target_table.insert(), batch)
                    n_rows += len(batch)
                    batch = []

            if batch:
                with target_engine.begin() as tgt_conn:
                    tgt_conn.execute(target_table.insert(), batch)
                n_rows += len(batch)

        print(f"  {table_name}: {n_rows} rows")
