-- ============================================================
-- Dataserver registry and catalog TTST
-- ============================================================
--
-- Stores known ODBC-accessible dataservers and tracks which
-- catalogs (databases) are visible over time via polling.
--
-- Sensitive connection attributes (passwords, keytab paths)
-- are NOT stored here.  The `secret_ref` column holds a
-- pointer to an external secret store (Vault/OpenBao path,
-- macOS Keychain service name, env var name, etc.).
-- The connection string is assembled at query time from
-- the non-secret attributes + a resolved secret.
--
-- Schema evolution for remote catalogs is tracked as a
-- JSON snapshot (current state) plus a chain of reverse
-- JSON Patch (RFC 6902) documents.  To reconstruct revision
-- N, start from the current snapshot and apply patches
-- backward from the most recent to revision N+1.

-- ── Dataserver registry ─────────────────────────────────────

CREATE TABLE IF NOT EXISTS dataserver (
    dataserver_id   INTEGER PRIMARY KEY,
    name            TEXT NOT NULL UNIQUE,
    driver          TEXT NOT NULL,       -- ODBC driver name (as in odbcinst.ini)
    host            TEXT,                -- hostname or IP; NULL for embedded/local
    port            INTEGER,             -- NULL = driver default
    default_catalog TEXT,                -- default database to connect to
    auth_method     TEXT NOT NULL        -- 'sql_login', 'trusted', 'kerberos', 'none'
                    CHECK (auth_method IN ('sql_login', 'trusted', 'kerberos', 'none')),
    username        TEXT,                -- non-secret; NULL for trusted/kerberos
    secret_ref      TEXT,                -- pointer to secret store, e.g.:
                                         --   'vault:secret/data/odbc/rule4_sqlserver#password'
                                         --   'openbao:secret/data/odbc/pg_rule4#password'
                                         --   'env:RULE4_SS_PASSWORD'
                                         --   'keychain:blobodbc/rule4_sqlserver'
                                         -- NULL for auth methods that need no secret
    extra_attrs     TEXT,                -- JSON object of driver-specific attrs, e.g.:
                                         --   {"TrustServerCertificate": "yes"}
                                         --   {"GSSEncMode": "disable"}
    notes           TEXT
);

-- ── Catalog TTST (transaction-time state table) ─────────────
--
-- PK: (dataserver_id, catalog_name, tt_start)
-- An open interval has tt_end IS NULL.
-- Closing: UPDATE tt_end = poll_time WHERE tt_end IS NULL.

CREATE TABLE IF NOT EXISTS catalog_interval (
    dataserver_id   INTEGER NOT NULL REFERENCES dataserver(dataserver_id),
    catalog_name    TEXT NOT NULL,
    tt_start        TIMESTAMPTZ NOT NULL DEFAULT now(),
    tt_end          TIMESTAMPTZ,         -- NULL = currently visible
    PRIMARY KEY (dataserver_id, catalog_name, tt_start)
);

-- ── Schema snapshots + reverse patch chain ──────────────────
--
-- Each snapshot_kind represents a category of metadata:
--   'tables', 'columns', 'foreign_keys', 'type_info', etc.
--
-- The `snapshot` column holds the current (most recent) JSON.
-- The `patches` table holds reverse JSON Patch documents
-- ordered by revision descending.  To reconstruct state at
-- revision R, take `snapshot` and apply patches from
-- revision_num = current down to R+1.
--
-- This means:
--   - Reading current state is a single row fetch (no joins)
--   - Recent history is cheap (few patches to apply)
--   - Deep history is progressively more expensive
--   - Storage is compact: patches are small for incremental changes

CREATE TABLE IF NOT EXISTS catalog_snapshot (
    dataserver_id   INTEGER NOT NULL REFERENCES dataserver(dataserver_id),
    catalog_name    TEXT NOT NULL,
    snapshot_kind   TEXT NOT NULL,       -- 'tables', 'columns', 'foreign_keys', etc.
    revision_num    INTEGER NOT NULL DEFAULT 1,
    snapshot        TEXT NOT NULL,       -- current JSON blob (full state)
    captured_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (dataserver_id, catalog_name, snapshot_kind)
);

CREATE TABLE IF NOT EXISTS catalog_snapshot_patch (
    dataserver_id   INTEGER NOT NULL,
    catalog_name    TEXT NOT NULL,
    snapshot_kind   TEXT NOT NULL,
    revision_num    INTEGER NOT NULL,   -- the revision this patch reverts FROM
    patch           TEXT NOT NULL,       -- RFC 6902 JSON Patch (reverse direction)
    captured_at     TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (dataserver_id, catalog_name, snapshot_kind, revision_num),
    FOREIGN KEY (dataserver_id, catalog_name, snapshot_kind)
        REFERENCES catalog_snapshot(dataserver_id, catalog_name, snapshot_kind)
);

-- ============================================================
-- Seed: known dataservers
-- ============================================================

INSERT OR IGNORE INTO dataserver
    (dataserver_id, name, driver, host, port, default_catalog,
     auth_method, username, secret_ref, extra_attrs, notes)
VALUES
    (1,
     'sqlserver_docker',
     'ODBC Driver 18 for SQL Server',
     'localhost', 1433, 'rule4_test',
     'sql_login', 'rule4',
     'env:RULE4_SS_PASSWORD',
     '{"TrustServerCertificate": "yes"}',
     'SQL Server 2017 on Docker/Rosetta'),

    (2,
     'pg_local',
     'PostgreSQL Unicode',
     '/tmp', NULL, 'rule4_test',
     'trusted', NULL,
     NULL,
     '{"GSSEncMode": "disable"}',
     'PostgreSQL 17 via Unix socket'),

    (3,
     'duckdb_memory',
     'DuckDB Driver',
     NULL, NULL, ':memory:',
     'none', NULL,
     NULL,
     NULL,
     'DuckDB in-memory (ODBC driver test target)');
