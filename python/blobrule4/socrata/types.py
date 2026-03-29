"""
blobrule4.socrata.types — Socrata type mapping to SQLAlchemy types.

Maps Socrata Discovery API data_type values to SQLAlchemy column types.
Used by replica.py when dynamically building Table definitions from
socrata.resource_column metadata.
"""

from sqlalchemy import Boolean, Date, DateTime, Numeric, String


# Socrata data_type values as they appear in the Discovery API / Views API.
# Geo types are stored as String (JSON-serialized GeoJSON in SODA2 responses).
SOCRATA_TYPE_MAP = {
    "text":           String,
    "number":         Numeric,
    "calendar_date":  DateTime,
    "floating_timestamp": DateTime,
    "fixed_timestamp": DateTime,
    "date":           Date,
    "checkbox":       Boolean,
    "url":            String,
    "point":          String,
    "multipolygon":   String,
    "multipoint":     String,
    "multiline":      String,
    "polygon":        String,
    "line":           String,
    "location":       String,
    "photo":          String,
    "document":       String,
    "phone":          String,
    "email":          String,
    "html":           String,
    "money":          Numeric,
    "percent":        Numeric,
    "stars":          Numeric,
    "flag":           String,
    "drop_down_list": String,
}


def socrata_sa_type(data_type):
    """Map a Socrata data_type string to a SQLAlchemy type class.

    Falls back to String for unknown types.
    """
    if data_type is None:
        return String
    return SOCRATA_TYPE_MAP.get(data_type.lower(), String)
