-- ============================================================
-- Build an ODBC connection string from a dataserver row.
--
-- This is a DuckDB SQL macro.  Secrets are resolved externally
-- (the caller must supply the password or leave it NULL for
-- auth methods that don't need one).
-- ============================================================

CREATE OR REPLACE MACRO build_conn_str(
    driver, host, port, default_catalog,
    auth_method, username, password, extra_attrs
) AS
    'Driver={' || driver || '}'
    || CASE WHEN host IS NOT NULL
            THEN ';Server=' || host || COALESCE(',' || CAST(port AS TEXT), '')
            ELSE '' END
    || CASE WHEN default_catalog IS NOT NULL AND default_catalog != ':memory:'
            THEN ';Database=' || default_catalog
            ELSE CASE WHEN default_catalog = ':memory:'
                      THEN ';Database=:memory:'
                      ELSE '' END
       END
    || CASE auth_method
            WHEN 'sql_login' THEN ';UID=' || COALESCE(username, '')
                                  || ';PWD=' || COALESCE(password, '')
            WHEN 'trusted'   THEN ';Trusted_Connection=yes'
            WHEN 'kerberos'  THEN ';Trusted_Connection=yes'
            ELSE '' END
    -- Append extra_attrs JSON object as key=value pairs.
    -- DuckDB doesn't have json_each for objects natively in a macro,
    -- so we handle the common cases with a simple replacement approach.
    -- For production use, this should be done via blobtemplates.
    || COALESCE(
        ';' || replace(replace(replace(
            trim(BOTH '{}' FROM extra_attrs),
            '"', ''),
            ': ', '='),
            ', ', ';'),
        '');
