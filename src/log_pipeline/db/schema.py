"""SQLAlchemy ORM declarative schema and table definitions for LogAnalyticsPipeline.

Defines tables:
- pipeline_runs: Tracking execution metadata, counters, and reconciliation status
- log_events: Persistent validated and normalized log records
- rejected_records: Quarantined malformed or invalid records with explicit reasons
"""

from __future__ import annotations

from datetime import datetime, timezone
from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    Float,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import declarative_base

Base = declarative_base()


def utc_now() -> datetime:
    """Return timezone-aware current UTC datetime."""
    return datetime.now(timezone.utc)


class PipelineRunModel(Base):
    """Execution metadata and accounting ledger for each pipeline run."""

    __tablename__ = "pipeline_runs"

    run_id = Column(String(64), primary_key=True, comment="Unique run identifier")
    start_time = Column(DateTime(timezone=True), nullable=False, comment="Run start time in UTC")
    end_time = Column(DateTime(timezone=True), nullable=True, comment="Run end time in UTC")
    status = Column(String(32), nullable=False, default="RUNNING", comment="RUNNING, SUCCESS, PARTIAL_SUCCESS, FAILED")
    input_files = Column(Text, nullable=False, comment="JSON serialized array of processed input file paths")
    input_count = Column(Integer, nullable=False, default=0, comment="Total raw lines/records read")
    valid_count = Column(Integer, nullable=False, default=0, comment="Valid records committed to log_events")
    rejected_count = Column(Integer, nullable=False, default=0, comment="Invalid records written to rejected_records")
    duplicate_count = Column(Integer, nullable=False, default=0, comment="Duplicate records skipped/idempotently handled")
    failure_details = Column(Text, nullable=True, comment="Error trace details in case of execution failure")

    __table_args__ = (
        Index("idx_pipeline_runs_start_time", "start_time"),
        Index("idx_pipeline_runs_status", "status"),
    )


class LogEventModel(Base):
    """Normalized, validated analytical log events table."""

    __tablename__ = "log_events"

    event_id = Column(String(64), primary_key=True, comment="Deterministic SHA-256 event ID or upstream UUID")
    run_id = Column(String(64), nullable=False, index=True, comment="Pipeline run that ingested this record")
    timestamp = Column(DateTime(timezone=True), nullable=False, index=True, comment="Event occurrence time in UTC")
    severity = Column(String(16), nullable=False, index=True, comment="Canonical severity: DEBUG, INFO, WARNING, ERROR, CRITICAL")
    service = Column(String(64), nullable=False, index=True, comment="Originating service identifier")
    endpoint = Column(String(255), nullable=True, comment="HTTP route or RPC method")
    http_status = Column(Integer, nullable=True, comment="HTTP status code (100-599)")
    latency_ms = Column(Float, nullable=True, comment="Processing latency in milliseconds")
    message = Column(Text, nullable=False, comment="Raw or extracted log message content")
    source_file = Column(String(255), nullable=False, comment="Input filename or URI")
    source_line = Column(Integer, nullable=False, comment="Line index within the source file")
    created_at = Column(DateTime(timezone=True), nullable=False, default=utc_now, comment="Record ingestion time in UTC")

    __table_args__ = (
        # Composite index for hourly service-level metric rollups
        Index("idx_log_events_svc_ts_sev", "service", "timestamp", "severity"),
        Index("idx_log_events_endpoint", "endpoint"),
    )


class RejectedRecordModel(Base):
    """Quarantine store for records failing syntax, schema, or semantic validation."""

    __tablename__ = "rejected_records"

    id = Column(BigInteger().with_variant(Integer, "sqlite"), primary_key=True, autoincrement=True)
    run_id = Column(String(64), nullable=False, index=True, comment="Pipeline execution ID during quarantine")
    source_file = Column(String(255), nullable=False, comment="Input filename where record originated")
    source_line = Column(Integer, nullable=False, comment="Line number in input file")
    raw_record = Column(Text, nullable=False, comment="Verbatim raw record payload")
    rejection_reasons = Column(Text, nullable=False, comment="JSON list or delimited string of failure reasons")
    created_at = Column(DateTime(timezone=True), nullable=False, default=utc_now, comment="Quarantine insertion timestamp")

    __table_args__ = (
        Index("idx_rejected_source", "source_file", "source_line"),
    )
