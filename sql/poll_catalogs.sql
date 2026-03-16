-- ============================================================
-- Poll catalogs for all registered dataservers.
--
-- Prerequisites:
--   - blobodbc extension loaded
--   - registry.sql executed (dataserver table populated)
--   - build_conn_str.sql executed (macro defined)
--
-- This script:
--   1. Builds connection strings for each dataserver
--   2. Calls bo_driver_info() to get the catalogs list
--   3. Compares against open intervals in catalog_interval
--   4. Opens new intervals for newly-visible catalogs
--   5. Closes intervals for catalogs that have disappeared
-- ============================================================

-- For this prototype, passwords are resolved from env vars
-- or hardcoded (in production, resolve from Vault/OpenBao).
-- The secret_ref 'env:RULE4_SS_PASSWORD' would be resolved
-- by a UDF or external process.  For now, we just use the
-- known dev password inline.

CREATE OR REPLACE TEMP TABLE poll_connections AS
SELECT
    d.dataserver_id,
    d.name AS dataserver_name,
    build_conn_str(
        d.driver, d.host, d.port, d.default_catalog,
        d.auth_method, d.username,
        -- Secret resolution stub: in production, resolve from secret_ref
        CASE
            WHEN d.secret_ref = 'env:RULE4_SS_PASSWORD' THEN 'R4Developer!2024'
            ELSE NULL
        END,
        d.extra_attrs
    ) AS conn_str
FROM dataserver AS d;

-- Fetch catalogs from each dataserver via bo_driver_info
CREATE OR REPLACE TEMP TABLE poll_results AS
WITH RAW_INFO AS (
    SELECT
        pc.dataserver_id,
        pc.dataserver_name,
        bo_driver_info(pc.conn_str)::JSON AS info
    FROM poll_connections AS pc
),
CATALOG_ROWS AS (
    SELECT
        ri.dataserver_id,
        ri.dataserver_name,
        unnest(from_json(ri.info->'catalogs', '["json"]')) AS cat_obj
    FROM RAW_INFO AS ri
)
SELECT
    cr.dataserver_id,
    cr.dataserver_name,
    COALESCE(
        cr.cat_obj->>'TABLE_CAT',
        cr.cat_obj->>'TABLE_SCHEM',
        'default'
    ) AS catalog_name,
    now() AS poll_time
FROM CATALOG_ROWS AS cr;

-- Show what we found
SELECT dataserver_name, catalog_name FROM poll_results ORDER BY ALL;

-- Open new intervals for catalogs we haven't seen before
INSERT INTO catalog_interval (dataserver_id, catalog_name, tt_start)
SELECT
    pr.dataserver_id,
    pr.catalog_name,
    pr.poll_time
FROM poll_results AS pr
WHERE NOT EXISTS (
    SELECT 1
    FROM catalog_interval AS ci
    WHERE ci.dataserver_id = pr.dataserver_id
      AND ci.catalog_name = pr.catalog_name
      AND ci.tt_end IS NULL
);

-- Close intervals for catalogs that have disappeared
UPDATE catalog_interval
SET tt_end = now()
WHERE tt_end IS NULL
  AND NOT EXISTS (
    SELECT 1
    FROM poll_results AS pr
    WHERE pr.dataserver_id = catalog_interval.dataserver_id
      AND pr.catalog_name = catalog_interval.catalog_name
);

-- Current state of all catalog intervals
SELECT
    d.name AS dataserver,
    ci.catalog_name,
    ci.tt_start,
    ci.tt_end,
    CASE WHEN ci.tt_end IS NULL THEN 'open' ELSE 'closed' END AS status
FROM catalog_interval AS ci
JOIN dataserver AS d ON d.dataserver_id = ci.dataserver_id
ORDER BY d.name, ci.catalog_name, ci.tt_start;
