"""
SQLAlchemy models for the blobodbc schema sampling database.

Tables:
  - dataserver: ODBC target registry
  - table_sample_log: raw table metadata per scrape
  - column_sample_log: raw column metadata per scrape
  - primary_key_sample_log: raw PK metadata per scrape
  - foreign_key_sample_log: raw FK metadata per scrape
  - index_sample_log: raw index metadata per scrape

Each *_sample_log table stores one row per (dataserver, catalog, schema, sample_time)
with the full query result as a JSON payload.  The relational expansion and
diff/TTST analysis happen downstream.
"""

from sqlalchemy import (
    Column,
    DateTime,
    Float,
    Integer,
    String,
    Text,
    func,
)
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


class Dataserver(Base):
    __tablename__ = "dataserver"

    dataserver_id = Column(Integer, primary_key=True, autoincrement=False)
    name = Column(String, nullable=False, unique=True)
    driver = Column(String, nullable=False)
    host = Column(String)
    port = Column(Integer)
    default_catalog = Column(String)
    auth_method = Column(String, nullable=False)
    username = Column(String)
    secret_ref = Column(String)
    extra_attrs = Column(Text)  # JSON object
    notes = Column(Text)


# ── Sample log tables ────────────────────────────────────────────
#
# PK is (dataserver_id, catalog_name, schema_name, sample_time).
# One row per sample per kind.  Payload is the raw JSON result
# of the dialect-specific catalog query.

class TableSampleLog(Base):
    __tablename__ = "table_sample_log"

    dataserver_id = Column(Integer, primary_key=True)
    catalog_name = Column(String, primary_key=True)
    schema_name = Column(String, primary_key=True)
    sample_time = Column(DateTime, primary_key=True)
    duration_ms = Column(Float)
    payload = Column(Text, nullable=False)
    error = Column(Text)


class ColumnSampleLog(Base):
    __tablename__ = "column_sample_log"

    dataserver_id = Column(Integer, primary_key=True)
    catalog_name = Column(String, primary_key=True)
    schema_name = Column(String, primary_key=True)
    sample_time = Column(DateTime, primary_key=True)
    duration_ms = Column(Float)
    payload = Column(Text, nullable=False)
    error = Column(Text)


class PrimaryKeySampleLog(Base):
    __tablename__ = "primary_key_sample_log"

    dataserver_id = Column(Integer, primary_key=True)
    catalog_name = Column(String, primary_key=True)
    schema_name = Column(String, primary_key=True)
    sample_time = Column(DateTime, primary_key=True)
    duration_ms = Column(Float)
    payload = Column(Text, nullable=False)
    error = Column(Text)


class ForeignKeySampleLog(Base):
    __tablename__ = "foreign_key_sample_log"

    dataserver_id = Column(Integer, primary_key=True)
    catalog_name = Column(String, primary_key=True)
    schema_name = Column(String, primary_key=True)
    sample_time = Column(DateTime, primary_key=True)
    duration_ms = Column(Float)
    payload = Column(Text, nullable=False)
    error = Column(Text)


class IndexSampleLog(Base):
    __tablename__ = "index_sample_log"

    dataserver_id = Column(Integer, primary_key=True)
    catalog_name = Column(String, primary_key=True)
    schema_name = Column(String, primary_key=True)
    sample_time = Column(DateTime, primary_key=True)
    duration_ms = Column(Float)
    payload = Column(Text, nullable=False)
    error = Column(Text)


class TriggerSampleLog(Base):
    __tablename__ = "trigger_sample_log"

    dataserver_id = Column(Integer, primary_key=True)
    catalog_name = Column(String, primary_key=True)
    schema_name = Column(String, primary_key=True)
    sample_time = Column(DateTime, primary_key=True)
    duration_ms = Column(Float)
    payload = Column(Text, nullable=False)
    error = Column(Text)


class CallableSampleLog(Base):
    __tablename__ = "callable_sample_log"

    dataserver_id = Column(Integer, primary_key=True)
    catalog_name = Column(String, primary_key=True)
    schema_name = Column(String, primary_key=True)
    sample_time = Column(DateTime, primary_key=True)
    duration_ms = Column(Float)
    payload = Column(Text, nullable=False)
    error = Column(Text)
