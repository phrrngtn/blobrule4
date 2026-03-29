-- TTST (transaction-time state table) schema for Socrata catalog metadata.
--
-- Business keys:
--   socrata_domain:          (domain)
--   socrata_resource:        (domain, resource_id)
--   socrata_resource_column: (domain, resource_id, field_name)
--
-- tt_start: Socrata's metadata_updated_at (source-authoritative timestamp).
-- tt_end:   '9999-12-31' sentinel for current rows; closed off on UPDATE/DELETE.
--
-- Deletions (resource removed from catalog) are inferred via set difference
-- between a full domain scrape and current rows. May never happen in practice.

CREATE SCHEMA IF NOT EXISTS socrata;

-- One row per Socrata domain. Not temporal — just tracks scrape state.
CREATE TABLE IF NOT EXISTS socrata.domain (
    domain              TEXT PRIMARY KEY,
    resource_count      INT,
    last_scraped_at     TIMESTAMPTZ,
    first_seen_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- TTST: one row per resource per version.
-- PK is (domain, resource_id, tt_start) — each version is unique.
CREATE TABLE IF NOT EXISTS socrata.resource (
    domain                  TEXT        NOT NULL,
    resource_id             TEXT        NOT NULL,
    tt_start                TIMESTAMPTZ NOT NULL,
    tt_end                  TIMESTAMPTZ NOT NULL DEFAULT '9999-12-31',

    name                    TEXT,
    description             TEXT,
    resource_type           TEXT,
    permalink               TEXT,
    attribution             TEXT,
    attribution_link        TEXT,
    provenance              TEXT,
    created_at              TEXT,
    updated_at              TEXT,
    metadata_updated_at     TEXT,
    data_updated_at         TEXT,
    publication_date        TEXT,
    page_views_total        INT,
    download_count          INT,
    domain_category         TEXT,
    categories              JSONB,
    domain_tags             JSONB,
    owner                   JSONB,
    creator                 JSONB,
    resource_json           JSONB,
    classification_json     JSONB,

    PRIMARY KEY (domain, resource_id, tt_start)
);

-- Index for efficient "current state" queries.
CREATE INDEX IF NOT EXISTS idx_resource_current
    ON socrata.resource (domain, resource_id)
    WHERE tt_end = '9999-12-31';

-- TTST: one row per column per resource per version.
CREATE TABLE IF NOT EXISTS socrata.resource_column (
    domain              TEXT        NOT NULL,
    resource_id         TEXT        NOT NULL,
    field_name          TEXT        NOT NULL,
    tt_start            TIMESTAMPTZ NOT NULL,
    tt_end              TIMESTAMPTZ NOT NULL DEFAULT '9999-12-31',

    ordinal_position    INT,
    display_name        TEXT,
    data_type           TEXT,
    description         TEXT,

    PRIMARY KEY (domain, resource_id, field_name, tt_start)
);

CREATE INDEX IF NOT EXISTS idx_resource_column_current
    ON socrata.resource_column (domain, resource_id, field_name)
    WHERE tt_end = '9999-12-31';
