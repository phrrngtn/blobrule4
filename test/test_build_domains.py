"""
Populate missing domain enumerations in PG via psql.
Uses the geonames gazetteer already in PG as the source for geographic domains.
"""
import subprocess
import sys

PSQL = "/opt/homebrew/Cellar/postgresql@17/17.9/bin/psql"

def psql(sql):
    """Execute SQL against rule4_test."""
    result = subprocess.run(
        [PSQL, "-h", "/tmp", "-d", "rule4_test", "-c", sql],
        capture_output=True, text=True, timeout=30
    )
    if result.returncode != 0:
        print(f"  ERROR: {result.stderr.strip()}", file=sys.stderr)
    return result.stdout.strip()


# ── 1. US State abbreviations from geonames ──
print("1. us_state_abbrev...")
psql("""
    INSERT INTO domain.enumeration (domain_name, domain_label, source, member_count)
    VALUES ('us_state_abbrev', 'us_state', 'geonames:admin1', 0)
    ON CONFLICT (domain_name) DO NOTHING
""")
psql("""
    INSERT INTO domain.member (domain_name, label)
    SELECT 'us_state_abbrev', REPLACE(code, 'US.', '')
    FROM gazetteer.geonames_admin1
    WHERE code LIKE 'US.%'
    ON CONFLICT DO NOTHING
""")
psql("""
    UPDATE domain.enumeration SET member_count = (
        SELECT COUNT(*) FROM domain.member WHERE domain_name = 'us_state_abbrev'
    ) WHERE domain_name = 'us_state_abbrev'
""")
print(f"   {psql('SELECT member_count FROM domain.enumeration WHERE domain_name = chr(39)||chr(39)||chr(39)').strip()}")

# Actually let me just get the count directly
count = psql("SELECT COUNT(*) FROM domain.member WHERE domain_name = 'us_state_abbrev'")
print(f"   {count.splitlines()[-1].strip()} state abbreviations")

# ── 2. Country ISO 2-letter codes ──
print("2. country_iso2...")
psql("""
    INSERT INTO domain.enumeration (domain_name, domain_label, source, member_count)
    VALUES ('country_iso2', 'country', 'geonames:country', 0)
    ON CONFLICT (domain_name) DO NOTHING
""")
psql("""
    INSERT INTO domain.member (domain_name, label)
    SELECT 'country_iso2', iso
    FROM gazetteer.geonames_country
    ON CONFLICT DO NOTHING
""")
psql("UPDATE domain.enumeration SET member_count = (SELECT COUNT(*) FROM domain.member WHERE domain_name = 'country_iso2') WHERE domain_name = 'country_iso2'")
count = psql("SELECT COUNT(*) FROM domain.member WHERE domain_name = 'country_iso2'")
print(f"   {count.splitlines()[-1].strip()} codes")

# ── 3. Country full names ──
print("3. country_names...")
psql("""
    INSERT INTO domain.enumeration (domain_name, domain_label, source, member_count)
    VALUES ('country_names', 'country', 'geonames:country', 0)
    ON CONFLICT (domain_name) DO NOTHING
""")
psql("""
    INSERT INTO domain.member (domain_name, label)
    SELECT 'country_names', country_name
    FROM gazetteer.geonames_country
    WHERE country_name IS NOT NULL
    ON CONFLICT DO NOTHING
""")
psql("UPDATE domain.enumeration SET member_count = (SELECT COUNT(*) FROM domain.member WHERE domain_name = 'country_names') WHERE domain_name = 'country_names'")
count = psql("SELECT COUNT(*) FROM domain.member WHERE domain_name = 'country_names'")
print(f"   {count.splitlines()[-1].strip()} names")

# ── 4. Canadian provinces ──
print("4. canadian_provinces...")
psql("""
    INSERT INTO domain.enumeration (domain_name, domain_label, source, member_count)
    VALUES ('canadian_provinces', 'province', 'curated', 0)
    ON CONFLICT (domain_name) DO NOTHING
""")
provinces = ["AB", "BC", "MB", "NB", "NL", "NS", "NT", "NU", "ON", "PE", "QC", "SK", "YT"]
for p in provinces:
    psql(f"INSERT INTO domain.member (domain_name, label) VALUES ('canadian_provinces', '{p}') ON CONFLICT DO NOTHING")
psql("UPDATE domain.enumeration SET member_count = (SELECT COUNT(*) FROM domain.member WHERE domain_name = 'canadian_provinces') WHERE domain_name = 'canadian_provinces'")
print(f"   {len(provinces)} provinces")

# ── 5. US major cities (pop > 50k from geonames) ──
print("5. us_cities_major...")
psql("""
    INSERT INTO domain.enumeration (domain_name, domain_label, source, member_count)
    VALUES ('us_cities_major', 'city', 'geonames:place_pop_50k', 0)
    ON CONFLICT (domain_name) DO NOTHING
""")
psql("""
    INSERT INTO domain.member (domain_name, label)
    SELECT DISTINCT 'us_cities_major', place_ascii
    FROM gazetteer.geonames_place
    WHERE country_code = 'US' AND feature_code LIKE 'PPL%' AND population > 50000
    ON CONFLICT DO NOTHING
""")
psql("UPDATE domain.enumeration SET member_count = (SELECT COUNT(*) FROM domain.member WHERE domain_name = 'us_cities_major') WHERE domain_name = 'us_cities_major'")
count = psql("SELECT COUNT(*) FROM domain.member WHERE domain_name = 'us_cities_major'")
print(f"   {count.splitlines()[-1].strip()} cities")

# ── 6. HTTP methods ──
print("6. http_methods...")
psql("""
    INSERT INTO domain.enumeration (domain_name, domain_label, source, member_count)
    VALUES ('http_methods', 'http_method', 'curated:rfc7231', 0)
    ON CONFLICT (domain_name) DO NOTHING
""")
for m in ["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS", "TRACE", "CONNECT"]:
    psql(f"INSERT INTO domain.member (domain_name, label) VALUES ('http_methods', '{m}') ON CONFLICT DO NOTHING")
psql("UPDATE domain.enumeration SET member_count = (SELECT COUNT(*) FROM domain.member WHERE domain_name = 'http_methods') WHERE domain_name = 'http_methods'")
print("   9 methods")

# ── 7. MIME types ──
print("7. mime_types_common...")
psql("""
    INSERT INTO domain.enumeration (domain_name, domain_label, source, member_count)
    VALUES ('mime_types_common', 'mime_type', 'curated:iana', 0)
    ON CONFLICT (domain_name) DO NOTHING
""")
mimes = [
    "application/json", "application/xml", "application/pdf",
    "application/zip", "application/gzip", "application/octet-stream",
    "application/x-www-form-urlencoded", "application/javascript",
    "text/plain", "text/html", "text/css", "text/csv", "text/xml",
    "text/javascript", "text/markdown",
    "image/png", "image/jpeg", "image/gif", "image/svg+xml", "image/webp",
    "audio/mpeg", "audio/wav", "video/mp4", "video/webm",
    "multipart/form-data", "multipart/mixed",
]
for m in mimes:
    psql(f"INSERT INTO domain.member (domain_name, label) VALUES ('mime_types_common', '{m}') ON CONFLICT DO NOTHING")
psql("UPDATE domain.enumeration SET member_count = (SELECT COUNT(*) FROM domain.member WHERE domain_name = 'mime_types_common') WHERE domain_name = 'mime_types_common'")
print(f"   {len(mimes)} types")

# ── 8. Common status/boolean values ──
print("8. status_values...")
psql("""
    INSERT INTO domain.enumeration (domain_name, domain_label, source, member_count)
    VALUES ('status_values', 'status', 'curated', 0)
    ON CONFLICT (domain_name) DO NOTHING
""")
statuses = [
    "Active", "Inactive", "Pending", "Approved", "Rejected", "Cancelled",
    "Expired", "Suspended", "Closed", "Open", "Draft", "Published",
    "Completed", "In Progress", "On Hold", "Archived", "Deleted",
    "Enabled", "Disabled", "Verified", "Unverified",
]
for s in statuses:
    psql(f"INSERT INTO domain.member (domain_name, label) VALUES ('status_values', '{s}') ON CONFLICT DO NOTHING")
psql("UPDATE domain.enumeration SET member_count = (SELECT COUNT(*) FROM domain.member WHERE domain_name = 'status_values') WHERE domain_name = 'status_values'")
print(f"   {len(statuses)} values")

# ── 9. Country ISO3 codes ──
print("9. country_iso3...")
psql("""
    INSERT INTO domain.enumeration (domain_name, domain_label, source, member_count)
    VALUES ('country_iso3', 'country', 'geonames:country', 0)
    ON CONFLICT (domain_name) DO NOTHING
""")
psql("""
    INSERT INTO domain.member (domain_name, label)
    SELECT 'country_iso3', country_iso3
    FROM gazetteer.geonames_country
    WHERE country_iso3 IS NOT NULL
    ON CONFLICT DO NOTHING
""")
psql("UPDATE domain.enumeration SET member_count = (SELECT COUNT(*) FROM domain.member WHERE domain_name = 'country_iso3') WHERE domain_name = 'country_iso3'")
count = psql("SELECT COUNT(*) FROM domain.member WHERE domain_name = 'country_iso3'")
print(f"   {count.splitlines()[-1].strip()} codes")

# ── 10. Continent codes ──
print("10. continents...")
psql("""
    INSERT INTO domain.enumeration (domain_name, domain_label, source, member_count)
    VALUES ('continents', 'continent', 'curated', 0)
    ON CONFLICT (domain_name) DO NOTHING
""")
for c in ["AF", "AN", "AS", "EU", "NA", "OC", "SA",
          "Africa", "Antarctica", "Asia", "Europe", "North America", "Oceania", "South America"]:
    psql(f"INSERT INTO domain.member (domain_name, label) VALUES ('continents', '{c}') ON CONFLICT DO NOTHING")
psql("UPDATE domain.enumeration SET member_count = (SELECT COUNT(*) FROM domain.member WHERE domain_name = 'continents') WHERE domain_name = 'continents'")
print("   14 values")

# ── Summary ──
print(f"\n{'='*70}")
print("FINAL DOMAIN REGISTRY")
print("=" * 70)
print(psql("""
    SELECT domain_name, domain_label, source, member_count
    FROM domain.enumeration ORDER BY domain_name
"""))
total = psql("SELECT COUNT(*) FROM domain.member")
print(f"\nTotal members: {total.splitlines()[-1].strip()}")
