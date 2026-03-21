"""
Combined sieve run: structural facts (Tier 1) + regex probes (Tier 2b)
on a real dataset, all written to rule4_metadata_fact.

Then query the fact table to show how evidence layers combine for
column classification.
"""
import json
import sys
import time
import subprocess
import yaml
sys.path.insert(0, "python")

import duckdb
from blobrule4.structural import run_structural, ensure_fact_table, _register_reference_tables, _register_udfs
from blobrule4.metadata import regex_probe_all

duck = duckdb.connect(":memory:")
duck.execute("INSTALL postgres; LOAD postgres;")
duck.execute("ATTACH 'host=/tmp dbname=rule4_test' AS pg (TYPE POSTGRES, READ_ONLY)")

# ── Step 1: Build snapshots from a real Socrata dataset ──
# We'll treat the Chicago Business Licenses as if it were a database table
# we're analyzing for the first time.

print("=" * 70)
print("STEP 1: Build snapshots for Chicago Business Licenses")
print("=" * 70)

import os
if not os.path.exists("/tmp/chi_biz_licenses.csv"):
    subprocess.run([
        "curl", "-s", "-o", "/tmp/chi_biz_licenses.csv",
        "https://data.cityofchicago.org/resource/r5kz-chrr.csv?$limit=10000"
    ], timeout=60, capture_output=True)

duck.execute("CREATE TABLE biz AS SELECT * FROM read_csv_auto('/tmp/chi_biz_licenses.csv')")
row_count = duck.execute("SELECT COUNT(*) FROM biz").fetchone()[0]
cols = duck.execute("DESCRIBE biz").fetchall()
print(f"  Loaded {row_count:,} rows × {len(cols)} columns")

# Build a columns snapshot as if this were a cataloged table
duck.execute("""
    CREATE TABLE rule4_schema_snapshot (
        dataserver_id INTEGER, catalog_name VARCHAR, schema_name VARCHAR,
        kind VARCHAR, revision_num INTEGER, snapshot TEXT,
        captured_at TIMESTAMP,
        PRIMARY KEY (dataserver_id, catalog_name, schema_name, kind)
    )
""")

# Synthesize column metadata from DuckDB's own describe
col_snap = {"chicago": {"business_licenses": {}}}
for name, dtype, null, key, default, extra in cols:
    col_snap["chicago"]["business_licenses"][name] = {
        "ordinal_position": len(col_snap["chicago"]["business_licenses"]) + 1,
        "data_type": dtype.lower(),
        "is_nullable": "YES",
        "default_definition": None,
    }

duck.execute(
    "INSERT INTO rule4_schema_snapshot VALUES (?, ?, ?, ?, 1, ?, CURRENT_TIMESTAMP)",
    [1, "socrata", "chicago", "columns", json.dumps(col_snap)]
)

# Synthesize tables snapshot
tbl_snap = {"chicago": {"business_licenses": {"table_type": "BASE TABLE", "row_count": row_count}}}
duck.execute(
    "INSERT INTO rule4_schema_snapshot VALUES (?, ?, ?, ?, 1, ?, CURRENT_TIMESTAMP)",
    [1, "socrata", "chicago", "tables", json.dumps(tbl_snap)]
)
print("  Built columns + tables snapshots")

# ── Step 2: Run structural analysis (Tier 1) ──
print(f"\n{'='*70}")
print("STEP 2: Structural analysis (Tier 1 facts)")
print("=" * 70)

t0 = time.perf_counter()
n_structural = run_structural(duck, 1, "socrata", "chicago")
t1 = time.perf_counter()
print(f"  {n_structural} structural facts in {t1-t0:.2f}s")

# ── Step 3: Value profiling + regex probe (Tier 2b) ──
print(f"\n{'='*70}")
print("STEP 3: Value profiling + regex probe (Tier 2b facts)")
print("=" * 70)

# UNPIVOT + DISTINCT
duck.execute("""
    CREATE TEMP TABLE kv AS
    WITH S AS (SELECT COLUMNS(*)::VARCHAR FROM biz),
    KVR AS (UNPIVOT S ON COLUMNS(*) INTO NAME column_name VALUE val)
    SELECT column_name, val, COUNT(*) AS freq
    FROM KVR WHERE val IS NOT NULL
    GROUP BY column_name, val
""")
ndv = duck.execute("SELECT COUNT(*) FROM kv").fetchone()[0]
print(f"  {ndv:,} distinct values")

# Load patterns with constraints
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

t2 = time.perf_counter()
probe_results = regex_probe_all(duck, "kv", patterns)
t3 = time.perf_counter()
print(f"  {len(probe_results)} regex hits in {t3-t2:.2f}s")

# Write regex probe results as Tier 2b facts
n_regex = 0
duck.execute("CREATE SEQUENCE IF NOT EXISTS seq_metadata_fact START 10000")
for col, label, cat, total, fr, fn, sr, sn in probe_results:
    fr = fr or 0
    sr = sr or 0
    fpct = fr * 100.0 / total if total else 0
    spct = sr * 100.0 / total if total else 0

    # Determine signal type
    if fr > total * 0.9:
        signal = "is_domain"
    elif fr == 0 and sr > total * 0.05:
        signal = "embedded"
    elif fr > 0 and sr > fr * 2:
        signal = "mixed"
    else:
        signal = "low"

    duck.execute(
        "INSERT INTO rule4_metadata_fact VALUES "
        "(nextval('seq_metadata_fact'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [1, "socrata", "chicago", "business_licenses", col,
         "regex_probe",
         json.dumps({
             "pattern": label, "category": cat, "signal": signal,
             "full_match_pct": round(fpct, 1),
             "substring_match_pct": round(spct, 1),
             "full_ndv": fn, "sub_ndv": sn,
         }),
         2,  # tier 2
         "regex_domains", None,
         time.strftime("%Y-%m-%d %H:%M:%S")]
    )
    n_regex += 1

# Also write column profile facts (Tier 2)
profiles = duck.execute("""
    SELECT column_name,
           SUM(freq) AS total_rows,
           COUNT(*) AS ndv,
           MIN(LENGTH(val)) AS min_len,
           MAX(LENGTH(val)) AS max_len
    FROM kv
    GROUP BY column_name
""").fetchall()

n_profile = 0
for col, total, ndv, minl, maxl in profiles:
    cardinality_ratio = ndv / total if total else 0
    duck.execute(
        "INSERT INTO rule4_metadata_fact VALUES "
        "(nextval('seq_metadata_fact'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [1, "socrata", "chicago", "business_licenses", col,
         "cardinality_ratio",
         json.dumps({
             "ndv": ndv, "total": total,
             "ratio": round(cardinality_ratio, 4),
             "min_len": minl, "max_len": maxl,
         }),
         2, "value_profile", None,
         time.strftime("%Y-%m-%d %H:%M:%S")]
    )
    n_profile += 1

print(f"  Wrote {n_regex} regex facts + {n_profile} profile facts")

# ── Step 4: Combined evidence query ──
print(f"\n{'='*70}")
print("STEP 4: Combined evidence per column")
print("=" * 70)

total_facts = duck.execute("SELECT COUNT(*) FROM rule4_metadata_fact").fetchone()[0]
print(f"\n  Total facts in rule4_metadata_fact: {total_facts}\n")

# For each column, show all evidence from all tiers
evidence = duck.execute("""
    SELECT column_name, fact_type, tier, fact_value
    FROM rule4_metadata_fact
    WHERE table_name = 'business_licenses'
      AND column_name IS NOT NULL
    ORDER BY column_name, tier, fact_type
""").fetchall()

# Group by column
by_col = {}
for col, ftype, tier, fval in evidence:
    by_col.setdefault(col, []).append((ftype, tier, json.loads(fval)))

# Show selected interesting columns with all their evidence
interesting = ["zip_code", "state", "city", "latitude", "address",
               "date_issued", "license_id", "license_status",
               "doing_business_as_name", "account_number"]

for col in interesting:
    if col not in by_col:
        continue
    facts = by_col[col]
    print(f"\n  ┌─ {col}")

    # Tier 1 facts
    t1_facts = [(ft, v) for ft, t, v in facts if t == 1]
    if t1_facts:
        print(f"  │  Tier 1 (structural):")
        for ft, v in t1_facts:
            if ft == "type_signature":
                tname = v.get('type_name') or v.get('data_type') or '?'
                print(f"  │    type: {tname} → {v['hint']}")
            elif ft == "naming_pattern":
                print(f"  │    name: {v['pattern']} → {v['semantic_role']}")
            elif ft == "default_hint":
                print(f"  │    default: {v['default_definition']} → {v['hint']}")
            else:
                print(f"  │    {ft}: {v}")

    # Tier 2 profile
    t2_profile = [(ft, v) for ft, t, v in facts if t == 2 and ft == "cardinality_ratio"]
    if t2_profile:
        v = t2_profile[0][1]
        print(f"  │  Tier 2 (profile):")
        print(f"  │    cardinality: {v['ndv']} distinct / {v['total']} rows = {v['ratio']:.4f}")
        print(f"  │    length: [{v['min_len']}-{v['max_len']}]")

    # Tier 2b regex probes (only strong signals)
    t2b_facts = [(ft, v) for ft, t, v in facts if t == 2 and ft == "regex_probe"]
    strong = [v for _, v in t2b_facts if v["signal"] in ("is_domain", "embedded")]
    if strong:
        print(f"  │  Tier 2b (regex):")
        for v in strong:
            print(f"  │    {v['pattern']}: full={v['full_match_pct']}% sub={v['substring_match_pct']}% [{v['signal']}]")

    # Synthesize a classification
    type_hint = next((v["hint"] for ft, v in t1_facts if ft == "type_signature"), None)
    name_role = next((v["semantic_role"] for ft, v in t1_facts if ft == "naming_pattern"), None)
    is_domain = [v["pattern"] for v in strong if v["signal"] == "is_domain"] if strong else []
    card = t2_profile[0][1]["ratio"] if t2_profile else None

    if is_domain:
        classification = f"dimension ({is_domain[0]})"
    elif type_hint and "measure" in type_hint:
        classification = f"measure ({type_hint})"
    elif name_role in ("key", "code_dimension", "date", "timestamp"):
        classification = f"dimension ({name_role})"
    elif type_hint == "flag_dimension":
        classification = "dimension (flag)"
    elif card and card < 0.01:
        classification = "dimension (low cardinality)"
    elif card and card > 0.9:
        classification = "key/identifier (high cardinality)"
    elif type_hint == "content_text":
        classification = "descriptive (free text)"
    else:
        classification = "ambiguous — needs Tier 3"

    print(f"  └─ CLASSIFICATION: {classification}")

print(f"\n{'='*70}")
print("FACT TABLE SUMMARY")
print("=" * 70)

summary = duck.execute("""
    SELECT tier, fact_type, COUNT(*) AS n
    FROM rule4_metadata_fact
    GROUP BY tier, fact_type
    ORDER BY tier, fact_type
""").fetchall()

print(f"\n  {'tier':>4} {'fact_type':<25} {'count':>6}")
print("  " + "-" * 40)
for tier, ft, n in summary:
    print(f"  {tier:>4} {ft:<25} {n:>6}")

duck.close()
