-- ============================================================
-- Enriched changelog view
--
-- Uses LAG on the TTST patch table to derive [tt_start, tt_end)
-- intervals per revision, then joins the tables sample log on
-- the interval to resolve catalog-reported timestamps.
--
-- The tables sample payload is unnested once per matching sample
-- to extract per-table modify_date / create_date.
--
-- Filter on captured_at or schema_name to limit materialization.
-- ============================================================

CREATE OR REPLACE VIEW rule4_enriched_changelog AS
WITH PATCH_INTERVALS AS (
    -- Derive interval boundaries from the patch chain.
    -- tt_start = previous patch's captured_at (or NULL for first)
    -- tt_end   = this patch's captured_at (= the sample that detected the change)
    SELECT
        p.dataserver_id,
        p.catalog_name,
        p.schema_name,
        p.kind,
        p.revision_num,
        LAG(p.captured_at) OVER (
            PARTITION BY p.dataserver_id, p.catalog_name, p.schema_name, p.kind
            ORDER BY p.revision_num
        ) AS tt_start,
        p.captured_at AS tt_end,
        p.patch
    FROM rule4_schema_snapshot_patch AS p
),
PATCH_OPS AS (
    -- Unnest each patch into individual RFC 6902 operations
    SELECT
        pi.dataserver_id,
        pi.catalog_name,
        pi.schema_name,
        pi.kind,
        pi.revision_num,
        pi.tt_start,
        pi.tt_end,
        unnest(from_json(pi.patch::JSON, '["json"]')) AS op
    FROM PATCH_INTERVALS AS pi
),
PARSED_OPS AS (
    -- Parse each operation: flip reverse semantics, extract path components
    SELECT
        po.*,
        CASE op->>'op'
            WHEN 'remove' THEN 'ADD'
            WHEN 'add'    THEN 'DROP'
            WHEN 'replace' THEN 'ALTER'
        END AS change_type,
        op->>'path' AS path,
        split_part(op->>'path', '/', 2) AS object_schema,
        CASE WHEN length(op->>'path') - length(replace(op->>'path', '/', '')) >= 2
             THEN split_part(op->>'path', '/', 3)
             ELSE NULL
        END AS table_name,
        CASE WHEN length(op->>'path') - length(replace(op->>'path', '/', '')) >= 3
             THEN split_part(op->>'path', '/', 4)
             ELSE NULL
        END AS column_or_constraint,
        CASE WHEN length(op->>'path') - length(replace(op->>'path', '/', '')) >= 4
             THEN split_part(op->>'path', '/', 5)
             ELSE NULL
        END AS attribute,
        CASE op->>'op'
            WHEN 'replace' THEN op->>'value'
            ELSE NULL
        END AS old_value
    FROM PATCH_OPS AS po
),
TABLE_SAMPLE_ROWS AS (
    -- Unnest the tables sample payload to get per-table metadata.
    -- Each row in the sample log has a JSON array; unnest it to get
    -- one row per table with modify_date / create_date.
    SELECT
        ts.dataserver_id,
        ts.catalog_name,
        ts.schema_name,
        ts.sample_time,
        t.value->>'table_name' AS tbl_name,
        t.value->>'table_schema' AS tbl_schema,
        COALESCE(
            t.value->>'modify_date',
            t.value->>'create_date'
        ) AS catalog_timestamp
    FROM rule4_table_sample_log AS ts,
         LATERAL unnest(from_json(ts.payload::JSON, '["json"]')) AS t(value)
    WHERE ts.error IS NULL
)
SELECT
    po.dataserver_id,
    po.catalog_name,
    po.schema_name,
    po.tt_start,
    po.tt_end,
    po.kind,
    po.revision_num,
    po.change_type,
    po.path,
    po.object_schema,
    po.table_name,
    po.column_or_constraint,
    po.attribute,
    po.old_value,
    tsr.catalog_timestamp
FROM PARSED_OPS AS po
LEFT JOIN TABLE_SAMPLE_ROWS AS tsr
    ON tsr.dataserver_id = po.dataserver_id
   AND tsr.catalog_name = po.catalog_name
   AND tsr.schema_name = po.schema_name
   AND tsr.sample_time = po.tt_end
   AND tsr.tbl_name = po.table_name;
