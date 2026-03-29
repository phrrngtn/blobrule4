-- Full-text search indexes for Socrata catalog metadata.
--
-- Adds tsvector columns and GIN indexes on current rows for fast
-- keyword search across resource names/descriptions and column metadata.
--
-- Usage: run after socrata_schema.sql and after catalog data is loaded.

-- Resource-level FTS: search across name, description, domain_category
ALTER TABLE socrata.resource
    ADD COLUMN IF NOT EXISTS fts tsvector;

UPDATE socrata.resource
SET fts = to_tsvector('english',
    COALESCE(name, '') || ' ' ||
    COALESCE(description, '') || ' ' ||
    COALESCE(domain_category, '')
)
WHERE tt_end = '9999-12-31';

CREATE INDEX IF NOT EXISTS idx_resource_fts
    ON socrata.resource USING GIN (fts)
    WHERE tt_end = '9999-12-31';

-- Column-level FTS: search across field_name, display_name, description
ALTER TABLE socrata.resource_column
    ADD COLUMN IF NOT EXISTS fts tsvector;

UPDATE socrata.resource_column
SET fts = to_tsvector('english',
    COALESCE(field_name, '') || ' ' ||
    COALESCE(display_name, '') || ' ' ||
    COALESCE(description, '')
)
WHERE tt_end = '9999-12-31';

CREATE INDEX IF NOT EXISTS idx_resource_column_fts
    ON socrata.resource_column USING GIN (fts)
    WHERE tt_end = '9999-12-31';
