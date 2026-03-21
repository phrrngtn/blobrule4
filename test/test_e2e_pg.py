"""
End-to-end: connect to PG, build snapshots from socrata schema,
run structural analysis, and regex-probe actual column values.
"""
import json
import time
import sys
import yaml
sys.path.insert(0, "python")

import duckdb

# ── Step 1: Connect to DuckDB, attach PG via postgres scanner ──
print("=" * 70)
print("STEP 1: Connect to PG via DuckDB postgres scanner")
print("=" * 70)

duck = duckdb.connect(":memory:")

# Try postgres scanner extension
try:
    duck.execute("INSTALL postgres; LOAD postgres;")
    duck.execute("ATTACH 'host=/tmp dbname=rule4_test' AS pg (TYPE POSTGRES, READ_ONLY)")
    r = duck.execute("SELECT COUNT(*) FROM pg.socrata.resource").fetchone()[0]
    print(f"  Connected to PG. socrata.resource: {r} rows")
except Exception as e:
    print(f"  postgres scanner failed: {e}")
    print("  Falling back to psycopg2 via pandas")
    raise

# ── Step 2: Pull socrata metadata into local DuckDB tables ──
print(f"\n{'='*70}")
print("STEP 2: Stage socrata metadata locally")
print("=" * 70)

t0 = time.perf_counter()

# resource_column has the column metadata we want
duck.execute("""
    CREATE TABLE socrata_columns AS
    SELECT * FROM pg.socrata.resource_column
""")
col_count = duck.execute("SELECT COUNT(*) FROM socrata_columns").fetchone()[0]

duck.execute("""
    CREATE TABLE socrata_resources AS
    SELECT domain, resource_id, name, description, resource_type,
           domain_category, page_views_total, download_count
    FROM pg.socrata.resource
    WHERE tt_end = '9999-12-31 00:00:00-05'
""")
res_count = duck.execute("SELECT COUNT(*) FROM socrata_resources").fetchone()[0]

t1 = time.perf_counter()
print(f"  Staged {col_count:,} columns, {res_count:,} resources in {t1-t0:.1f}s")

# ── Step 3: Profile the socrata_columns table ──
print(f"\n{'='*70}")
print("STEP 3: Profile socrata_columns structure")
print("=" * 70)

cols = duck.execute("DESCRIBE socrata_columns").fetchall()
print(f"\n  {'Column':<30} {'Type':<15}")
print("  " + "-" * 45)
for name, dtype, *_ in cols:
    print(f"  {name:<30} {dtype:<15}")

# Quick stats
print()
stats = duck.execute("""
    SELECT COUNT(DISTINCT resource_id) AS resources,
           COUNT(DISTINCT field_name) AS distinct_field_names,
           COUNT(DISTINCT data_type) AS distinct_datatypes,
           COUNT(*) AS total_columns
    FROM socrata_columns
""").fetchone()
print(f"  {stats[3]:,} total columns across {stats[0]:,} resources")
print(f"  {stats[1]:,} distinct field names, {stats[2]} data types")

# Show datatype distribution
print(f"\n  Datatype distribution:")
for dtype, cnt in duck.execute("""
    SELECT data_type, COUNT(*) AS cnt
    FROM socrata_columns
    GROUP BY data_type ORDER BY cnt DESC
""").fetchall():
    print(f"    {dtype or 'NULL':<20} {cnt:>8,}")

# ── Step 4: UNPIVOT + DISTINCT on field_name column ──
print(f"\n{'='*70}")
print("STEP 4: Regex probe on socrata column NAMES (not values)")
print("=" * 70)

# The socrata column names themselves are interesting to probe
duck.execute("""
    CREATE TEMP TABLE name_kv AS
    SELECT 'field_name' AS column_name,
           field_name AS val,
           COUNT(*) AS freq
    FROM socrata_columns
    WHERE field_name IS NOT NULL
    GROUP BY field_name
""")
name_ndv = duck.execute("SELECT COUNT(*) FROM name_kv").fetchone()[0]
print(f"\n  {name_ndv:,} distinct field names to probe")

# Load regex patterns
with open("catalog/reference/regex_domains.yml") as f:
    data = yaml.safe_load(f)

CONSTRAINTS = {
    "EMAIL_ADDRESS": (5, 320, "@"), "US_PHONE": (10, 14, None),
    "INTL_PHONE_E164": (8, 16, "+"), "US_SSN": (11, 11, "-"),
    "US_SSN_UNFORMATTED": (9, 9, None), "CREDIT_CARD_NUMBER": (13, 19, None),
    "IBAN_CODE": (15, 34, None), "SWIFT_CODE": (8, 11, None),
    "ISO_DATE": (10, 10, "-"), "US_DATE_MDY": (8, 10, "/"),
    "ISO_DATETIME": (19, 35, None), "TIME_24H": (5, 8, ":"),
    "IPV4_ADDRESS": (7, 15, "."), "IPV6_ADDRESS": (15, 39, ":"),
    "MAC_ADDRESS": (17, 17, None), "URL": (10, 2048, "://"),
    "DOMAIN_NAME": (4, 253, "."), "US_ZIP_CODE": (5, 10, None),
    "UK_POSTCODE": (5, 8, None), "CANADA_POSTAL_CODE": (6, 7, None),
    "LAT_LONG_DECIMAL": (7, 40, ","), "UUID": (36, 36, "-"),
    "ISBN_13": (13, 17, None), "IMEI": (15, 15, None),
    "VIN": (17, 17, None), "MD5_HASH": (32, 32, None),
    "SHA1_HASH": (40, 40, None), "SHA256_HASH": (64, 64, None),
    "JWT_TOKEN": (30, 8192, "."), "BASE64": (20, 100000, None),
    "FILE_PATH_UNIX": (2, 4096, "/"), "FILE_PATH_WINDOWS": (4, 260, "\\"),
    "SEMVER": (5, 40, "."), "HEX_COLOR": (4, 7, "#"),
    "JSON_OBJECT": (2, 100000, "{"), "US_STATE_ABBREV": (2, 2, None),
    "US_CURRENCY": (2, 20, "$"), "PERCENTAGE": (2, 10, "%"),
}

patterns = []
for p in data["patterns"]:
    if not p.get("valid", True):
        continue
    c = CONSTRAINTS.get(p["label"], (1, 100000, None))
    patterns.append({
        "label": p["label"], "pattern": p["pattern"],
        "category": p["category"],
        "min_len": c[0], "max_len": c[1], "requires": c[2],
    })

from blobrule4.metadata import regex_probe_all

t2 = time.perf_counter()
name_results = regex_probe_all(duck, "name_kv", patterns)
t3 = time.perf_counter()
print(f"  Probed in {t3-t2:.2f}s")

if name_results:
    print(f"\n  {'pattern':<22} {'full':>6} {'sub':>6} signal")
    print("  " + "-" * 50)
    for col, label, cat, total, fr, fn, sr, sn in sorted(
            name_results, key=lambda r: -(r[4] or 0)):
        fpct = (fr or 0) * 100.0 / total
        spct = (sr or 0) * 100.0 / total
        sig = "IS" if fr and fr > total * 0.5 else "embedded" if sr and sr > total * 0.05 else "low"
        print(f"  {label:<22} {fpct:>5.1f}% {spct:>5.1f}%  {sig}")

# ── Step 5: Probe actual column descriptions ──
print(f"\n{'='*70}")
print("STEP 5: Regex probe on column descriptions")
print("=" * 70)

duck.execute("""
    CREATE TEMP TABLE desc_kv AS
    SELECT 'description' AS column_name,
           description AS val,
           COUNT(*) AS freq
    FROM socrata_columns
    WHERE description IS NOT NULL AND LENGTH(description) > 0
    GROUP BY description
""")
desc_ndv = duck.execute("SELECT COUNT(*) FROM desc_kv").fetchone()[0]
print(f"\n  {desc_ndv:,} distinct descriptions to probe")

t4 = time.perf_counter()
desc_results = regex_probe_all(duck, "desc_kv", patterns)
t5 = time.perf_counter()
print(f"  Probed in {t5-t4:.2f}s")

if desc_results:
    print(f"\n  {'pattern':<22} {'full':>6} {'sub':>6} signal")
    print("  " + "-" * 50)
    for col, label, cat, total, fr, fn, sr, sn in sorted(
            desc_results, key=lambda r: -(r[6] or 0)):
        fpct = (fr or 0) * 100.0 / total
        spct = (sr or 0) * 100.0 / total
        sig = "IS" if fr and fr > total * 0.5 else "embedded" if sr and sr > total * 0.05 else "low"
        print(f"  {label:<22} {fpct:>5.1f}% {spct:>5.1f}%  {sig}")

# ── Step 6: Pick a popular dataset and probe its actual values ──
print(f"\n{'='*70}")
print("STEP 6: Download a popular dataset and probe its values")
print("=" * 70)

# Find a dataset with many columns and high page views
popular = duck.execute("""
    SELECT r.domain, r.resource_id, r.name,
           COUNT(*) AS n_cols, r.page_views_total
    FROM socrata_resources AS r
    JOIN socrata_columns AS c ON c.domain = r.domain AND c.resource_id = r.resource_id
    WHERE r.resource_type = 'dataset'
      AND r.page_views_total > 1000
    GROUP BY r.domain, r.resource_id, r.name, r.page_views_total
    HAVING n_cols BETWEEN 10 AND 40
    ORDER BY r.page_views_total DESC
    LIMIT 5
""").fetchall()

print(f"\n  Top datasets by page views (10-40 columns):")
for domain, rid, name, ncols, views in popular:
    print(f"    {domain}/{rid} ({ncols} cols, {views:,} views): {name[:60]}")

if popular:
    domain, rid, name, ncols, views = popular[0]
    soda_url = f"https://{domain}/resource/{rid}.csv?$limit=10000"
    print(f"\n  Downloading: {soda_url}")

    import subprocess
    result = subprocess.run(
        ["curl", "-s", "-o", "/tmp/socrata_sample.csv", soda_url],
        capture_output=True, text=True, timeout=30
    )
    if result.returncode == 0:
        duck.execute("CREATE TABLE socrata_data AS SELECT * FROM read_csv_auto('/tmp/socrata_sample.csv')")
        row_count = duck.execute("SELECT COUNT(*) FROM socrata_data").fetchone()[0]
        data_cols = duck.execute("DESCRIBE socrata_data").fetchall()
        print(f"  Loaded {row_count:,} rows × {len(data_cols)} columns")

        # UNPIVOT and probe
        kv_sql = """
            WITH S AS (SELECT COLUMNS(*)::VARCHAR FROM socrata_data),
            KV AS (UNPIVOT S ON COLUMNS(*) INTO NAME column_name VALUE val)
            SELECT column_name, val, COUNT(*) AS freq
            FROM KV WHERE val IS NOT NULL
            GROUP BY column_name, val
        """
        duck.execute(f"CREATE TEMP TABLE data_kv AS {kv_sql}")
        data_ndv = duck.execute("SELECT COUNT(*) FROM data_kv").fetchone()[0]
        print(f"  {data_ndv:,} distinct values across {len(data_cols)} columns")

        t6 = time.perf_counter()
        data_results = regex_probe_all(duck, "data_kv", patterns)
        t7 = time.perf_counter()
        print(f"  Probed in {t7-t6:.2f}s")

        if data_results:
            print(f"\n  {'column':<25} {'pattern':<22} {'full%':>6} {'sub%':>6}  signal")
            print("  " + "-" * 75)
            for col, label, cat, total, fr, fn, sr, sn in sorted(
                    data_results, key=lambda r: (r[0], -(r[4] or 0))):
                fpct = (fr or 0) * 100.0 / total
                spct = (sr or 0) * 100.0 / total
                if fr and fr > total * 0.9:
                    sig = "IS domain"
                elif not fr and sr and sr > total * 0.05:
                    sig = "embedded"
                elif fr and sr and sr > fr * 2:
                    sig = "mixed"
                else:
                    sig = "low"
                print(f"  {col:<25} {label:<22} {fpct:>5.1f}% {spct:>5.1f}%  {sig}")

print(f"\n{'='*70}")
print("DONE")
print("=" * 70)
duck.close()
