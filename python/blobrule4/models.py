"""
SQLAlchemy models for blobrule4 schema survey database.

Architecture:
  - Base entity models define the domain (Dataserver is the only
    hand-written one for now)
  - sample_log tables are mechanically derived via make_sample_log():
    same PK envelope (dataserver_id, catalog_name, schema_name,
    sample_time) + payload JSON + error
  - TTST tables are mechanically derived via make_snapshot_table()
    and make_patch_table(): snapshot + reverse patch chain

The _log and _ttst tables are "widened" versions — the widening
adds transactional columns and adjusts the PK constraints.
"""

from sqlalchemy import (
    Column,
    DateTime,
    Float,
    Integer,
    String,
    Text,
)
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


# ── Mechanical derivation: sample_log tables ─────────────────────

def make_sample_log(kind, base=Base):
    """
    Factory: create a sample_log table class for a given entity kind.

    Produces a class with:
      __tablename__ = "{kind}_sample_log"
      PK: (dataserver_id, catalog_name, schema_name, sample_time)
      Columns: duration_ms, payload (JSON text), error
    """
    return type(f"{kind.title().replace('_','')}SampleLog", (base,), {
        "__tablename__": f"rule4_{kind}_sample_log",
        "dataserver_id": Column(Integer, primary_key=True),
        "catalog_name": Column(String, primary_key=True),
        "schema_name": Column(String, primary_key=True),
        "sample_time": Column(DateTime, primary_key=True),
        "duration_ms": Column(Float),
        "payload": Column(Text, nullable=False),
        "error": Column(Text),
    })


# ── Mechanical derivation: TTST tables ───────────────────────────

def make_snapshot_table(name="rule4_schema_snapshot", base=Base):
    """Create the TTST current-state snapshot table."""
    return type("SchemaSnapshot", (base,), {
        "__tablename__": name,
        "dataserver_id": Column(Integer, primary_key=True),
        "catalog_name": Column(String, primary_key=True),
        "schema_name": Column(String, primary_key=True),
        "kind": Column(String, primary_key=True),
        "revision_num": Column(Integer, nullable=False, default=1),
        "snapshot": Column(Text, nullable=False),
        "captured_at": Column(DateTime, nullable=False),
    })


def make_patch_table(name="rule4_schema_snapshot_patch", base=Base):
    """Create the TTST reverse patch chain table."""
    return type("SchemaSnapshotPatch", (base,), {
        "__tablename__": name,
        "dataserver_id": Column(Integer, primary_key=True),
        "catalog_name": Column(String, primary_key=True),
        "schema_name": Column(String, primary_key=True),
        "kind": Column(String, primary_key=True),
        "revision_num": Column(Integer, primary_key=True),
        "patch": Column(Text, nullable=False),
        "captured_at": Column(DateTime, nullable=False),
    })


# ── Registry ────────────────────────────────────────────────────

class Dataserver(Base):
    __tablename__ = "rule4_dataserver"

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


# ── Instantiate all derived tables ──────────────────────────────

SAMPLE_KINDS = ["table", "column", "primary_key", "foreign_key",
                "index", "trigger", "callable",
                "check_constraint", "unique_constraint", "view",
                "column_comment", "user_defined_type", "partitioning",
                "temporal_table", "synonym"]

TableSampleLog = make_sample_log("table")
ColumnSampleLog = make_sample_log("column")
PrimaryKeySampleLog = make_sample_log("primary_key")
ForeignKeySampleLog = make_sample_log("foreign_key")
IndexSampleLog = make_sample_log("index")
TriggerSampleLog = make_sample_log("trigger")
CallableSampleLog = make_sample_log("callable")
CheckConstraintSampleLog = make_sample_log("check_constraint")
UniqueConstraintSampleLog = make_sample_log("unique_constraint")
ViewSampleLog = make_sample_log("view")
ColumnCommentSampleLog = make_sample_log("column_comment")
UserDefinedTypeSampleLog = make_sample_log("user_defined_type")
PartitioningSampleLog = make_sample_log("partitioning")
TemporalTableSampleLog = make_sample_log("temporal_table")
SynonymSampleLog = make_sample_log("synonym")

SchemaSnapshot = make_snapshot_table()
SchemaSnapshotPatch = make_patch_table()

# Map catalog query kind → sample log class (used by sampler + intern)
# Keys match the YAML filename (plural) for auto-discovery
SAMPLE_LOG_CLASSES = {
    "tables": TableSampleLog,
    "columns": ColumnSampleLog,
    "primary_keys": PrimaryKeySampleLog,
    "foreign_keys": ForeignKeySampleLog,
    "indexes": IndexSampleLog,
    "triggers": TriggerSampleLog,
    "callables": CallableSampleLog,
    "check_constraints": CheckConstraintSampleLog,
    "unique_constraints": UniqueConstraintSampleLog,
    "views": ViewSampleLog,
    "column_comments": ColumnCommentSampleLog,
    "user_defined_types": UserDefinedTypeSampleLog,
    "partitioning": PartitioningSampleLog,
    "temporal_tables": TemporalTableSampleLog,
    "synonyms": SynonymSampleLog,
}


# ── Fact / Evidence / Resolution tables ───────────────────────

class MetadataFact(Base):
    """
    Discrete observed facts about metadata — the evidence layer.

    Each row is a single observation: "column X participates in FK Y",
    "column X has type money", "CHECK constraint contains IN-list {A,B,C}".

    Facts are independent of any classification scheme. Multiple
    classification passes can reference the same facts with different
    weights. Facts are append-only; superseded facts get a newer
    observed_at, not deleted.

    fact_type vocabulary (extensible):
      Tier 1 (structural):
        fk_member, pk_member, unique_member, type_signature,
        check_enum, default_hint, column_comment, udt_label,
        partition_key, temporal_period, naming_pattern,
        fk_topology_role, view_dependency
      Tier 2 (profile):
        cardinality_ratio, null_fraction, min_max_range,
        histogram_shape, type_distribution
      Tier 3 (sample):
        blobfilter_match, embedding_similarity, llm_domain,
        frequency_distribution, value_pattern
    """
    __tablename__ = "rule4_metadata_fact"

    fact_id = Column(Integer, primary_key=True, autoincrement=True)
    dataserver_id = Column(Integer, nullable=False)
    catalog_name = Column(String, nullable=False)
    schema_name = Column(String, nullable=False)
    table_name = Column(String, nullable=False)
    column_name = Column(String)  # NULL for table-level facts
    fact_type = Column(String, nullable=False)  # vocabulary above
    fact_value = Column(Text, nullable=False)   # JSON: the raw observation
    tier = Column(Integer, nullable=False)      # 1=structural, 2=profile, 3=sample
    source_kind = Column(String)   # snapshot kind that produced this fact
    source_revision = Column(Integer)  # snapshot revision for provenance
    observed_at = Column(DateTime, nullable=False)
