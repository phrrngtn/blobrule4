"""
TTST (Transaction-Time State Table) models for schema evolution tracking.

snapshot: current full state per (dataserver, catalog, schema, kind)
snapshot_patch: reverse patch chain for history reconstruction
"""

from sqlalchemy import (
    Column,
    DateTime,
    Integer,
    String,
    Text,
)
from blobrule4.models import Base


class SchemaSnapshot(Base):
    __tablename__ = "schema_snapshot"

    dataserver_id = Column(Integer, primary_key=True)
    catalog_name = Column(String, primary_key=True)
    schema_name = Column(String, primary_key=True)
    kind = Column(String, primary_key=True)  # 'tables', 'columns', etc.
    revision_num = Column(Integer, nullable=False, default=1)
    snapshot = Column(Text, nullable=False)   # current nested JSON
    captured_at = Column(DateTime, nullable=False)


class SchemaSnapshotPatch(Base):
    __tablename__ = "schema_snapshot_patch"

    dataserver_id = Column(Integer, primary_key=True)
    catalog_name = Column(String, primary_key=True)
    schema_name = Column(String, primary_key=True)
    kind = Column(String, primary_key=True)
    revision_num = Column(Integer, primary_key=True)  # revision this patch reverts FROM
    patch = Column(Text, nullable=False)       # RFC 6902 reverse patch
    captured_at = Column(DateTime, nullable=False)  # when the transition happened
