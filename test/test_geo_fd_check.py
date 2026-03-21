"""
Synthesize geonames_id from geographic columns and verify
functional dependencies: city → state, zip → city, zip → state.
"""
import sys
import time
sys.path.insert(0, "python")

import duckdb

duck = duckdb.connect(":memory:")
duck.execute("INSTALL postgres; LOAD postgres;")
duck.execute("ATTACH 'host=/tmp dbname=rule4_test' AS pg (TYPE POSTGRES, READ_ONLY)")

duck.execute("CREATE TABLE biz AS SELECT * FROM read_csv_auto('/tmp/chi_biz_licenses.csv')")
print(f"Loaded {duck.execute('SELECT COUNT(*) FROM biz').fetchone()[0]:,} business licenses")

# Stage gazetteer
print("\nStaging gazetteer from PG...")
t0 = time.perf_counter()

duck.execute("""
    CREATE TABLE geonames_place AS
    SELECT geonameid, place_name, place_ascii, latitude, longitude,
           feature_code, country_code, admin1_name, population
    FROM pg.gazetteer.geonames_place
    WHERE country_code = 'US' AND feature_code LIKE 'PPL%'
""")

duck.execute("""
    CREATE TABLE geonames_admin1 AS
    SELECT code, name AS state_name, asciiname AS state_ascii, geonameid
    FROM pg.gazetteer.geonames_admin1
    WHERE code LIKE 'US.%'
""")

# Build state code ↔ state name mapping
duck.execute("""
    CREATE TABLE state_xref AS
    SELECT REPLACE(code, 'US.', '') AS state_code,
           state_name, state_ascii
    FROM geonames_admin1
""")

place_count = duck.execute("SELECT COUNT(*) FROM geonames_place").fetchone()[0]
admin1_count = duck.execute("SELECT COUNT(*) FROM geonames_admin1").fetchone()[0]
t1 = time.perf_counter()
print(f"  {place_count:,} US places, {admin1_count} states in {t1-t0:.1f}s")

# ── FD Check 1: state column → valid US state code ──
print(f"\n{'='*70}")
print("FD CHECK 1: state values vs geonames admin1")
print("=" * 70)

results = duck.execute("""
    WITH BIZ_STATES AS (
        SELECT UPPER(TRIM(state)) AS state_val, COUNT(*) AS freq
        FROM biz WHERE state IS NOT NULL
        GROUP BY state_val
    )
    SELECT bs.state_val, bs.freq, sx.state_name,
           CASE WHEN sx.state_code IS NOT NULL THEN 'valid'
                ELSE 'INVALID' END AS status
    FROM BIZ_STATES AS bs
    LEFT JOIN state_xref AS sx ON sx.state_code = bs.state_val
    ORDER BY bs.freq DESC
""").fetchall()

print(f"\n  {'code':<6} {'freq':>6} {'state_name':<25} {'status'}")
print("  " + "-" * 50)
for code, freq, name, status in results:
    flag = " ** ANOMALY **" if status == "INVALID" else ""
    print(f"  {code:<6} {freq:>6} {(name or ''):25s} {status}{flag}")

# ── FD Check 2: zip_code → city ──
print(f"\n{'='*70}")
print("FD CHECK 2: zip_code → city (functional dependency)")
print("=" * 70)

fd2 = duck.execute("""
    WITH ZC AS (
        SELECT zip_code, UPPER(TRIM(city)) AS city_val, COUNT(*) AS freq
        FROM biz WHERE zip_code IS NOT NULL AND city IS NOT NULL
        GROUP BY zip_code, city_val
    ),
    MULTI AS (
        SELECT zip_code, COUNT(DISTINCT city_val) AS n_cities,
               LIST(city_val ORDER BY freq DESC) AS cities,
               LIST(freq ORDER BY freq DESC) AS freqs
        FROM ZC GROUP BY zip_code HAVING n_cities > 1
    )
    SELECT * FROM MULTI ORDER BY n_cities DESC LIMIT 15
""").fetchall()

print(f"\n  {len(fd2)} zip codes violate zip→city FD:\n")
if fd2:
    for zc, nc, cities, freqs in fd2:
        cf = ", ".join(f"{c}({f})" for c, f in zip(cities, freqs))
        print(f"  {zc}: {cf}")

# ── FD Check 3: city → state ──
print(f"\n{'='*70}")
print("FD CHECK 3: city → state (functional dependency)")
print("=" * 70)

fd3 = duck.execute("""
    WITH CS AS (
        SELECT UPPER(TRIM(city)) AS city_val, UPPER(TRIM(state)) AS state_val,
               COUNT(*) AS freq
        FROM biz WHERE city IS NOT NULL AND state IS NOT NULL
        GROUP BY city_val, state_val
    ),
    MULTI AS (
        SELECT city_val, COUNT(DISTINCT state_val) AS n_states,
               LIST(state_val ORDER BY freq DESC) AS states,
               LIST(freq ORDER BY freq DESC) AS freqs
        FROM CS GROUP BY city_val HAVING n_states > 1
    )
    SELECT * FROM MULTI ORDER BY n_states DESC
""").fetchall()

if fd3:
    print(f"\n  {len(fd3)} cities appear in multiple states:\n")
    for city, ns, states, freqs in fd3:
        sf = ", ".join(f"{s}({f})" for s, f in zip(states, freqs))
        print(f"  {city}: {sf}")
else:
    print("  No violations")

# ── FD Check 4: Resolve city+state to geonames_id ──
print(f"\n{'='*70}")
print("FD CHECK 4: city+state → geonames place (resolution)")
print("=" * 70)

resolution = duck.execute("""
    WITH BIZ_LOCATIONS AS (
        SELECT UPPER(TRIM(city)) AS city_val,
               UPPER(TRIM(state)) AS state_code,
               COUNT(*) AS freq
        FROM biz
        WHERE city IS NOT NULL AND state IS NOT NULL
        GROUP BY city_val, state_code
    ),
    RESOLVED AS (
        SELECT bl.city_val, bl.state_code, bl.freq,
               sx.state_name,
               gp.geonameid,
               gp.place_ascii AS geonames_name,
               gp.population,
               gp.latitude AS geo_lat,
               gp.longitude AS geo_lon,
               ROW_NUMBER() OVER (
                   PARTITION BY bl.city_val, bl.state_code
                   ORDER BY gp.population DESC NULLS LAST
               ) AS rn
        FROM BIZ_LOCATIONS AS bl
        LEFT JOIN state_xref AS sx ON sx.state_code = bl.state_code
        LEFT JOIN geonames_place AS gp
            ON UPPER(gp.place_ascii) = bl.city_val
           AND gp.admin1_name = sx.state_name
    )
    SELECT city_val, state_code, freq, state_name,
           geonameid, geonames_name, population, geo_lat, geo_lon
    FROM RESOLVED WHERE rn = 1
    ORDER BY freq DESC
""").fetchall()

matched = sum(r[2] for r in resolution if r[4] is not None)
unmatched = sum(r[2] for r in resolution if r[4] is None)
total = matched + unmatched
print(f"\n  Resolution: {matched}/{total} rows ({matched*100/total:.1f}%) matched to geonames\n")

print(f"  {'city':<25} {'st':>3} {'freq':>6} {'geonames_id':>11} {'geo_name':<20} {'pop':>10}")
print("  " + "-" * 85)
for city, st, freq, sname, gid, gname, pop, glat, glon in resolution[:25]:
    flag = "" if gid else " ** UNRESOLVED **"
    print(f"  {city:<25} {st:>3} {freq:>6} {gid or '':>11} {(gname or ''):20s} {pop or '':>10}{flag}")

# Show unresolved
unresolved = [(r[0], r[1], r[2]) for r in resolution if r[4] is None]
if unresolved:
    print(f"\n  Unresolved cities ({len(unresolved)}):")
    for city, st, freq in sorted(unresolved, key=lambda x: -x[2])[:15]:
        print(f"    {city} ({st}): {freq} rows")

# ── FD Check 5: lat/lon consistency with geonames ──
print(f"\n{'='*70}")
print("FD CHECK 5: lat/lon consistency with geonames place coordinates")
print("=" * 70)

geo_consistency = duck.execute("""
    WITH BIZ_WITH_GEO AS (
        SELECT b.city, b.state, b.zip_code,
               TRY_CAST(b.latitude AS DOUBLE) AS biz_lat,
               TRY_CAST(b.longitude AS DOUBLE) AS biz_lon,
               gp.geonameid, gp.latitude AS geo_lat, gp.longitude AS geo_lon,
               gp.place_ascii,
               -- Haversine-ish distance in km (simplified)
               111.0 * SQRT(
                   POWER(TRY_CAST(b.latitude AS DOUBLE) - gp.latitude, 2) +
                   POWER((TRY_CAST(b.longitude AS DOUBLE) - gp.longitude) *
                         COS(RADIANS(gp.latitude)), 2)
               ) AS distance_km,
               ROW_NUMBER() OVER (
                   PARTITION BY b.rowid
                   ORDER BY gp.population DESC NULLS LAST
               ) AS rn
        FROM biz AS b
        LEFT JOIN state_xref AS sx ON sx.state_code = UPPER(TRIM(b.state))
        LEFT JOIN geonames_place AS gp
            ON UPPER(gp.place_ascii) = UPPER(TRIM(b.city))
           AND gp.admin1_name = sx.state_name
        WHERE b.latitude IS NOT NULL AND b.longitude IS NOT NULL
    )
    SELECT
        COUNT(*) AS total,
        COUNT(*) FILTER (WHERE rn = 1 AND geonameid IS NOT NULL) AS resolved,
        COUNT(*) FILTER (WHERE rn = 1 AND distance_km < 5) AS within_5km,
        COUNT(*) FILTER (WHERE rn = 1 AND distance_km < 20) AS within_20km,
        COUNT(*) FILTER (WHERE rn = 1 AND distance_km < 50) AS within_50km,
        COUNT(*) FILTER (WHERE rn = 1 AND distance_km >= 50) AS beyond_50km,
        ROUND(AVG(distance_km) FILTER (WHERE rn = 1 AND geonameid IS NOT NULL), 1) AS avg_dist_km,
        ROUND(MEDIAN(distance_km) FILTER (WHERE rn = 1 AND geonameid IS NOT NULL), 1) AS median_dist_km
    FROM BIZ_WITH_GEO
""").fetchone()

total, resolved, w5, w20, w50, b50, avg_d, med_d = geo_consistency
print(f"\n  Resolved rows: {resolved}/{total}")
print(f"  Distance from geonames city center:")
print(f"    < 5 km:   {w5:>6} ({w5*100/max(resolved,1):.1f}%)")
print(f"    < 20 km:  {w20:>6} ({w20*100/max(resolved,1):.1f}%)")
print(f"    < 50 km:  {w50:>6} ({w50*100/max(resolved,1):.1f}%)")
print(f"    > 50 km:  {b50:>6} ({b50*100/max(resolved,1):.1f}%) ** SUSPICIOUS **")
print(f"  Avg distance: {avg_d} km, Median: {med_d} km")

duck.close()
