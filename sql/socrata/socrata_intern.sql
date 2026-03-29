-- Stored procedure: intern a batch of Socrata Discovery API results into TTST tables.
--
-- Takes a JSONB array (the "results" array from the Discovery API response)
-- and the domain name. Shreds JSON into relational form, then does set-based
-- TTST close/insert.
--
-- Usage from Python:
--   cur.execute("CALL socrata.intern_catalog(%(domain)s, %(payload)s::jsonb, %(incremental)s)",
--               {"domain": "data.cityofnewyork.us", "payload": json.dumps(results),
--                "incremental": False})
--
-- Usage from psql for debugging:
--   CALL socrata.intern_catalog('data.cityofnewyork.us', '<jsonb array>'::jsonb, false);
--
-- p_incremental: when true, skip deletion detection (step 2). Use this when the
-- payload contains only recently-changed resources, not the full domain catalog.
-- Periodic full scrapes (p_incremental=false) are needed to catch deletions.

CREATE OR REPLACE PROCEDURE socrata.intern_catalog(
    p_domain        TEXT,
    p_results       JSONB,
    p_incremental   BOOLEAN DEFAULT false
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_scrape_ts     TIMESTAMPTZ := NOW();
    v_n_staged      INT;
    v_n_closed      INT;
    v_n_deleted     INT;
    v_n_inserted    INT;
BEGIN
    -- ================================================================
    -- Stage: shred the JSON array into a temp table
    -- ================================================================
    CREATE TEMP TABLE _staged_resource ON COMMIT DROP AS
    SELECT DISTINCT ON (p_domain, resource_id)
        p_domain                                            AS domain,
        resource_id,
        (res->>'metadata_updated_at')::timestamptz          AS tt_start,
        res->>'name'                                        AS name,
        res->>'description'                                 AS description,
        res->>'type'                                        AS resource_type,
        r->>'permalink'                                     AS permalink,
        res->>'attribution'                                 AS attribution,
        res->>'attribution_link'                             AS attribution_link,
        res->>'provenance'                                   AS provenance,
        res->>'createdAt'                                    AS created_at,
        res->>'updatedAt'                                    AS updated_at,
        res->>'metadata_updated_at'                          AS metadata_updated_at,
        res->>'data_updated_at'                              AS data_updated_at,
        res->>'publication_date'                             AS publication_date,
        (res->'page_views'->>'page_views_total')::int        AS page_views_total,
        (res->>'download_count')::int                        AS download_count,
        cls->>'domain_category'                              AS domain_category,
        cls->'categories'                                    AS categories,
        cls->'domain_tags'                                   AS domain_tags,
        r->'owner'                                           AS owner,
        r->'creator'                                         AS creator,
        r->'resource'                                        AS resource_json,
        r->'classification'                                  AS classification_json
    FROM jsonb_array_elements(p_results) AS r,
         LATERAL (SELECT r->'resource')   AS _res(res),
         LATERAL (SELECT r->'classification') AS _cls(cls),
         LATERAL (SELECT res->>'id')      AS _id(resource_id)
    ORDER BY p_domain, resource_id, (res->>'metadata_updated_at')::timestamptz DESC;

    CREATE INDEX ON _staged_resource (domain, resource_id);

    GET DIAGNOSTICS v_n_staged = ROW_COUNT;

    -- ================================================================
    -- 1. Close changed rows
    -- ================================================================
    UPDATE socrata.resource AS r
    SET tt_end = s.tt_start
    FROM _staged_resource AS s
    WHERE r.domain = s.domain
      AND r.resource_id = s.resource_id
      AND r.tt_end = '9999-12-31'
      AND (   r.name                IS DISTINCT FROM s.name
           OR r.description         IS DISTINCT FROM s.description
           OR r.resource_type       IS DISTINCT FROM s.resource_type
           OR r.permalink           IS DISTINCT FROM s.permalink
           OR r.attribution         IS DISTINCT FROM s.attribution
           OR r.attribution_link    IS DISTINCT FROM s.attribution_link
           OR r.provenance          IS DISTINCT FROM s.provenance
           OR r.created_at          IS DISTINCT FROM s.created_at
           OR r.updated_at          IS DISTINCT FROM s.updated_at
           OR r.metadata_updated_at IS DISTINCT FROM s.metadata_updated_at
           OR r.data_updated_at     IS DISTINCT FROM s.data_updated_at
           OR r.publication_date    IS DISTINCT FROM s.publication_date
           OR r.domain_category     IS DISTINCT FROM s.domain_category
          );

    GET DIAGNOSTICS v_n_closed = ROW_COUNT;

    -- ================================================================
    -- 2. Close deleted rows (set difference)
    --    Skipped in incremental mode — staged set is partial, not the
    --    full domain catalog, so set difference would be wrong.
    -- ================================================================
    v_n_deleted := 0;

    IF NOT p_incremental THEN
        UPDATE socrata.resource AS r
        SET tt_end = v_scrape_ts
        FROM (
            SELECT r2.domain, r2.resource_id, r2.tt_start
            FROM socrata.resource AS r2
            LEFT OUTER JOIN _staged_resource AS s
                ON r2.domain = s.domain AND r2.resource_id = s.resource_id
            WHERE r2.domain = p_domain
              AND r2.tt_end = '9999-12-31'
              AND s.resource_id IS NULL
        ) AS gone
        WHERE r.domain = gone.domain
          AND r.resource_id = gone.resource_id
          AND r.tt_start = gone.tt_start;

        GET DIAGNOSTICS v_n_deleted = ROW_COUNT;
    END IF;

    -- ================================================================
    -- 3. Insert new and changed rows
    -- ================================================================
    INSERT INTO socrata.resource (
        domain, resource_id, tt_start, tt_end,
        name, description, resource_type, permalink,
        attribution, attribution_link, provenance,
        created_at, updated_at, metadata_updated_at, data_updated_at,
        publication_date, page_views_total, download_count,
        domain_category, categories, domain_tags,
        owner, creator, resource_json, classification_json
    )
    SELECT
        s.domain, s.resource_id,
        -- If tt_start collides with the just-closed row, bump to scrape time.
        CASE WHEN closed.tt_start IS NOT NULL THEN v_scrape_ts
             ELSE s.tt_start END,
        '9999-12-31',
        s.name, s.description, s.resource_type, s.permalink,
        s.attribution, s.attribution_link, s.provenance,
        s.created_at, s.updated_at, s.metadata_updated_at, s.data_updated_at,
        s.publication_date, s.page_views_total, s.download_count,
        s.domain_category, s.categories, s.domain_tags,
        s.owner, s.creator, s.resource_json, s.classification_json
    FROM _staged_resource AS s
    LEFT OUTER JOIN socrata.resource AS r
        ON s.domain = r.domain
        AND s.resource_id = r.resource_id
        AND r.tt_end = '9999-12-31'
    LEFT OUTER JOIN socrata.resource AS closed
        ON s.domain = closed.domain
        AND s.resource_id = closed.resource_id
        AND s.tt_start = closed.tt_start
        AND closed.tt_end <> '9999-12-31'
    WHERE r.resource_id IS NULL;

    GET DIAGNOSTICS v_n_inserted = ROW_COUNT;

    -- ================================================================
    -- Resource columns: shred parallel arrays from the resource JSON
    -- ================================================================
    CREATE TEMP TABLE _staged_column ON COMMIT DROP AS
    SELECT DISTINCT ON (p_domain, resource_id, field_name)
        p_domain                                        AS domain,
        resource_id,
        field_name,
        (res->>'metadata_updated_at')::timestamptz      AS tt_start,
        ordinality::int                                 AS ordinal_position,
        display_name,
        data_type,
        col_description
    FROM jsonb_array_elements(p_results) AS r,
         LATERAL (SELECT r->'resource') AS _res(res),
         LATERAL (SELECT res->>'id')    AS _id(resource_id),
         LATERAL jsonb_array_elements_text(
             COALESCE(res->'columns_field_name', '[]'::jsonb)
         ) WITH ORDINALITY AS fnames(field_name, ordinality),
         LATERAL (SELECT
             res->'columns_name'        ->> (ordinality - 1)::int,
             res->'columns_datatype'    ->> (ordinality - 1)::int,
             res->'columns_description' ->> (ordinality - 1)::int
         ) AS _cols(display_name, data_type, col_description)
    ORDER BY p_domain, resource_id, field_name,
             (res->>'metadata_updated_at')::timestamptz DESC;

    CREATE INDEX ON _staged_column (domain, resource_id, field_name);

    -- Close changed columns
    UPDATE socrata.resource_column AS r
    SET tt_end = s.tt_start
    FROM _staged_column AS s
    WHERE r.domain = s.domain
      AND r.resource_id = s.resource_id
      AND r.field_name = s.field_name
      AND r.tt_end = '9999-12-31'
      AND (   r.ordinal_position IS DISTINCT FROM s.ordinal_position
           OR r.display_name     IS DISTINCT FROM s.display_name
           OR r.data_type        IS DISTINCT FROM s.data_type
           OR r.description      IS DISTINCT FROM s.col_description
          );

    -- Close deleted columns (skipped in incremental mode)
    IF NOT p_incremental THEN
        UPDATE socrata.resource_column AS r
        SET tt_end = v_scrape_ts
        FROM (
            SELECT r2.domain, r2.resource_id, r2.field_name, r2.tt_start
            FROM socrata.resource_column AS r2
            LEFT OUTER JOIN _staged_column AS s
                ON r2.domain = s.domain
                AND r2.resource_id = s.resource_id
                AND r2.field_name = s.field_name
            WHERE r2.domain = p_domain
              AND r2.tt_end = '9999-12-31'
              AND s.field_name IS NULL
        ) AS gone
        WHERE r.domain = gone.domain
          AND r.resource_id = gone.resource_id
          AND r.field_name = gone.field_name
          AND r.tt_start = gone.tt_start;
    END IF;

    -- Insert new/changed columns
    INSERT INTO socrata.resource_column (
        domain, resource_id, field_name, tt_start, tt_end,
        ordinal_position, display_name, data_type, description
    )
    SELECT
        s.domain, s.resource_id, s.field_name,
        CASE WHEN closed.tt_start IS NOT NULL THEN v_scrape_ts
             ELSE s.tt_start END,
        '9999-12-31',
        s.ordinal_position, s.display_name, s.data_type, s.col_description
    FROM _staged_column AS s
    LEFT OUTER JOIN socrata.resource_column AS r
        ON s.domain = r.domain
        AND s.resource_id = r.resource_id
        AND s.field_name = r.field_name
        AND r.tt_end = '9999-12-31'
    LEFT OUTER JOIN socrata.resource_column AS closed
        ON s.domain = closed.domain
        AND s.resource_id = closed.resource_id
        AND s.field_name = closed.field_name
        AND s.tt_start = closed.tt_start
        AND closed.tt_end <> '9999-12-31'
    WHERE r.field_name IS NULL;

    -- ================================================================
    -- Update domain tracking
    -- ================================================================
    IF p_incremental THEN
        -- Incremental: only update scrape timestamp, not resource count
        -- (the payload is partial, so its length is not the domain total)
        UPDATE socrata.domain
        SET last_scraped_at = v_scrape_ts
        WHERE domain = p_domain;
    ELSE
        INSERT INTO socrata.domain (domain, resource_count, last_scraped_at)
        VALUES (p_domain, jsonb_array_length(p_results), v_scrape_ts)
        ON CONFLICT (domain) DO UPDATE
            SET resource_count = EXCLUDED.resource_count,
                last_scraped_at = EXCLUDED.last_scraped_at;
    END IF;

    RAISE NOTICE 'domain=%, staged=%, closed=%, deleted=%, inserted=%',
        p_domain, v_n_staged, v_n_closed, v_n_deleted, v_n_inserted;
END;
$$;
