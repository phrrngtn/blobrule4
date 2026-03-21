"""
Download Chicago Business Licenses sample, probe values,
focus on what fits and what DOESN'T fit expected domains.
"""
import json
import sys
import time
import subprocess
import yaml
sys.path.insert(0, "python")

import duckdb
from blobrule4.metadata import regex_probe_all

duck = duckdb.connect(":memory:")

# Download
print("Downloading Chicago Business Licenses (10k rows)...")
subprocess.run([
    "curl", "-s", "-o", "/tmp/chi_biz_licenses.csv",
    "https://data.cityofchicago.org/resource/r5kz-chrr.csv?$limit=10000"
], timeout=30)

duck.execute("CREATE TABLE biz AS SELECT * FROM read_csv_auto('/tmp/chi_biz_licenses.csv')")
row_count = duck.execute("SELECT COUNT(*) FROM biz").fetchone()[0]
cols = duck.execute("DESCRIBE biz").fetchall()
print(f"Loaded {row_count:,} rows × {len(cols)} columns\n")

# ── Column profiles ──
print("=" * 70)
print("COLUMN PROFILES")
print("=" * 70)

profiles = duck.execute("""
    WITH S AS (SELECT COLUMNS(*)::VARCHAR FROM biz),
    KV AS (UNPIVOT S ON COLUMNS(*) INTO NAME column_name VALUE val)
    SELECT column_name,
           COUNT(*) AS total,
           COUNT(val) AS non_null,
           COUNT(DISTINCT val) AS ndv,
           MIN(LENGTH(val)) AS min_len,
           MAX(LENGTH(val)) AS max_len
    FROM KV
    GROUP BY column_name
    ORDER BY column_name
""").fetchall()

print(f"\n{'column':<35} {'ndv':>6} {'null%':>6} {'len':>10}")
print("-" * 60)
for col, total, nn, ndv, minl, maxl in profiles:
    null_pct = (total - nn) * 100.0 / total
    print(f"{col:<35} {ndv:>6} {null_pct:>5.1f}% [{minl}-{maxl}]")

# ── UNPIVOT + DISTINCT + regex probe ──
print(f"\n{'='*70}")
print("REGEX PROBE (dual mode)")
print("=" * 70)

duck.execute("""
    CREATE TEMP TABLE kv AS
    WITH S AS (SELECT COLUMNS(*)::VARCHAR FROM biz),
    KVR AS (UNPIVOT S ON COLUMNS(*) INTO NAME column_name VALUE val)
    SELECT column_name, val, COUNT(*) AS freq
    FROM KVR WHERE val IS NOT NULL
    GROUP BY column_name, val
""")
ndv = duck.execute("SELECT COUNT(*) FROM kv").fetchone()[0]
print(f"\n{ndv:,} distinct values")

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

t0 = time.perf_counter()
results = regex_probe_all(duck, "kv", patterns)
t1 = time.perf_counter()
print(f"Probed in {t1-t0:.2f}s\n")

# Show results grouped by column
by_col = {}
for col, label, cat, total, fr, fn, sr, sn in results:
    by_col.setdefault(col, []).append((label, cat, total, fr or 0, fn or 0, sr or 0, sn or 0))

print(f"{'column':<30} {'pattern':<20} {'full%':>6} {'sub%':>6}  signal")
print("-" * 85)
for col in sorted(by_col.keys()):
    hits = sorted(by_col[col], key=lambda r: (-r[3], -r[5]))
    for label, cat, total, fr, fn, sr, sn in hits:
        fpct = fr * 100.0 / total
        spct = sr * 100.0 / total
        if fr > total * 0.9:
            sig = "** IS domain **"
        elif fr == 0 and sr > total * 0.05:
            sig = "embedded"
        elif fr > 0 and sr > fr * 2:
            sig = "mixed"
        else:
            sig = ""
        print(f"{col:<30} {label:<20} {fpct:>5.1f}% {spct:>5.1f}%  {sig}")

# ── Focus: what DOESN'T fit in the geo columns ──
print(f"\n{'='*70}")
print("WHAT DOESN'T FIT: anomalies in expected geographic columns")
print("=" * 70)

geo_cols = {
    "city": "name_dimension",
    "state": "US_STATE_ABBREV",
    "zip_code": "US_ZIP_CODE",
    "latitude": "LAT_LONG_DECIMAL",
    "longitude": "LAT_LONG_DECIMAL",
    "address": "name_dimension",
}

for col, expected in geo_cols.items():
    print(f"\n  --- {col} ---")

    # Top values
    top = duck.execute(f"""
        SELECT val, freq FROM kv
        WHERE column_name = '{col}'
        ORDER BY freq DESC LIMIT 5
    """).fetchall()
    print(f"  Top values: {[(v, f) for v, f in top]}")

    # Values that DON'T match expected pattern
    if expected == "US_STATE_ABBREV":
        pat = next(p["pattern"] for p in patterns if p["label"] == "US_STATE_ABBREV")
        outliers = duck.execute(f"""
            SELECT val, freq FROM kv
            WHERE column_name = '{col}'
              AND NOT regexp_full_match(val, ?)
            ORDER BY freq DESC LIMIT 10
        """, [pat]).fetchall()
        if outliers:
            print(f"  NON-MATCHING values ({len(outliers)}): {outliers}")
        else:
            print(f"  All values match {expected}")

    elif expected == "US_ZIP_CODE":
        pat = next(p["pattern"] for p in patterns if p["label"] == "US_ZIP_CODE")
        outliers = duck.execute(f"""
            SELECT val, freq FROM kv
            WHERE column_name = '{col}'
              AND NOT regexp_full_match(val, ?)
            ORDER BY freq DESC LIMIT 10
        """, [pat]).fetchall()
        if outliers:
            print(f"  NON-MATCHING values ({len(outliers)}): {outliers}")
        else:
            print(f"  All values match {expected}")

    elif expected == "LAT_LONG_DECIMAL":
        # Check for nulls, zeros, out-of-range
        stats = duck.execute(f"""
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE val = '0' OR val = '0.0') AS zeros,
                COUNT(*) FILTER (WHERE TRY_CAST(val AS DOUBLE) IS NULL) AS non_numeric,
                MIN(TRY_CAST(val AS DOUBLE)) AS min_val,
                MAX(TRY_CAST(val AS DOUBLE)) AS max_val
            FROM kv WHERE column_name = '{col}'
        """).fetchone()
        print(f"  total={stats[0]}, zeros={stats[1]}, non_numeric={stats[2]}, "
              f"range=[{stats[3]}, {stats[4]}]")

    # Null rate
    null_info = duck.execute(f"""
        SELECT SUM(freq) AS total,
               SUM(freq) FILTER (WHERE val IS NULL OR LENGTH(TRIM(val)) = 0) AS empty
        FROM kv WHERE column_name = '{col}'
    """).fetchone()
    if null_info[1] and null_info[1] > 0:
        print(f"  Empty/null: {null_info[1]}/{null_info[0]} ({null_info[1]*100/null_info[0]:.1f}%)")

# ── Show column-level summary: expected vs actual domain ──
print(f"\n{'='*70}")
print("COLUMN DOMAIN ASSESSMENT")
print("=" * 70)

# For each column, show what the regex probe thinks vs what the column name suggests
name_hints = {
    "city": "geographic", "state": "geographic", "zip_code": "geographic",
    "latitude": "geographic", "longitude": "geographic",
    "address": "geographic", "neighborhood": "geographic",
    "community_area": "geographic", "ward": "geographic",
    "police_district": "geographic", "precinct": "geographic",
    "license_code": "code_identifier", "license_number": "code_identifier",
    "account_number": "code_identifier", "id": "code_identifier",
    "license_id": "code_identifier", "site_number": "code_identifier",
    "date_issued": "datetime", "expiration_date": "datetime",
    "license_start_date": "datetime", "payment_date": "datetime",
    "application_created_date": "datetime",
    "license_status": "status_dimension", "license_description": "name_dimension",
    "doing_business_as_name": "name_dimension", "legal_name": "name_dimension",
}

print(f"\n{'column':<35} {'name_hint':<20} {'regex_says':<20} {'match?'}")
print("-" * 85)
for col in sorted(name_hints.keys()):
    hint = name_hints[col]
    regex_matches = by_col.get(col, [])
    if regex_matches:
        best = max(regex_matches, key=lambda r: r[3])  # highest full_rows
        fpct = best[3] * 100.0 / best[2] if best[2] else 0
        if fpct > 50:
            regex_says = f"{best[0]} ({fpct:.0f}%)"
        else:
            regex_says = "no strong match"
    else:
        regex_says = "no match"

    match = "yes" if (hint == "geographic" and "geo" in str(regex_matches).lower()) or \
                     (hint == "datetime" and "ISO" in str(regex_matches)) or \
                     (hint == "code_identifier" and regex_matches) else ""
    print(f"{col:<35} {hint:<20} {regex_says:<20} {match}")

duck.close()
