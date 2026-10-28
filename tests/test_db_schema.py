"""Unit and integration tests for database schema, models, and transaction handling."""

from datetime import datetime, timezone
import pytest
from sqlalchemy import inspect, select

from src.log_pipeline.db.schema import (
    Base,
    PipelineRunModel,
    LogEventModel,
    RejectedRecordModel,
)
from src.log_pipeline.db.connection import DatabaseManager
from src.log_pipeline.config import load_config


def test_schema_table_creation(sqlite_engine):
    """Verify all analytical tables and expected columns are created."""
    inspector = inspect(sqlite_engine)
    table_names = inspector.get_table_names()

    assert "pipeline_runs" in table_names
    assert "log_events" in table_names
    assert "rejected_records" in table_names

    log_columns = {col["name"] for col in inspector.get_columns("log_events")}
    required_cols = {
        "event_id",
        "run_id",
        "timestamp",
        "severity",
        "service",
        "endpoint",
        "http_status",
        "latency_ms",
        "message",
        "source_file",
        "source_line",
        "created_at",
    }
    assert required_cols.issubset(log_columns)


def test_pipeline_run_crud(db_session):
    """Verify inserting and querying pipeline run accounting rows."""
    run = PipelineRunModel(
        run_id="run-test-100",
        start_time=datetime.now(timezone.utc),
        status="RUNNING",
        input_files='["sample1.json", "sample2.log"]',
        input_count=200,
        valid_count=180,
        rejected_count=20,
        duplicate_count=0,
    )
    db_session.add(run)
    db_session.commit()

    queried = db_session.get(PipelineRunModel, "run-test-100")
    assert queried is not None
    assert queried.status == "RUNNING"
    assert queried.input_count == 200
    assert queried.valid_count == 180


def test_log_event_crud(db_session):
    """Verify inserting and querying LogEventModel rows."""
    event = LogEventModel(
        event_id="hash-123456",
        run_id="run-test-100",
        timestamp=datetime(2026, 10, 28, 14, 0, 0, tzinfo=timezone.utc),
        severity="ERROR",
        service="order-service",
        endpoint="/api/v1/orders",
        http_status=500,
        latency_ms=250.0,
        message="Order persistence failed",
        source_file="sample.jsonl",
        source_line=42,
    )
    db_session.add(event)
    db_session.commit()

    queried = db_session.get(LogEventModel, "hash-123456")
    assert queried is not None
    assert queried.service == "order-service"
    assert queried.severity == "ERROR"
    assert queried.http_status == 500


def test_rejected_record_crud(db_session):
    """Verify inserting and querying quarantine records."""
    rejected = RejectedRecordModel(
        run_id="run-test-100",
        source_file="bad.log",
        source_line=1,
        raw_record="corrupted log entry",
        rejection_reasons='["PARSE_ERROR: Unrecognized format"]',
    )
    db_session.add(rejected)
    db_session.commit()

    results = db_session.scalars(
        select(RejectedRecordModel).where(RejectedRecordModel.run_id == "run-test-100")
    ).all()
    assert len(results) == 1
    assert results[0].source_line == 1
    assert "PARSE_ERROR" in results[0].rejection_reasons


def test_database_manager_transaction_rollback(test_db_manager):
    """Verify that session_scope rolls back changes when an exception is raised."""
    try:
        with test_db_manager.session_scope() as session:
            run = PipelineRunModel(
                run_id="run-rollback-test",
                start_time=datetime.now(timezone.utc),
                status="RUNNING",
                input_files="[]",
            )
            session.add(run)
            # Deliberately raise exception inside the transaction
            raise RuntimeError("Forced simulation error")
    except RuntimeError:
        pass

    # Verify run was rolled back
    with test_db_manager.session_scope() as session:
        queried = session.get(PipelineRunModel, "run-rollback-test")
        assert queried is None


def test_database_manager_mysql_connection():
    """Verify live connectivity and table creation against local MariaDB/MySQL instance."""
    cfg = load_config()
    db_mgr = DatabaseManager(config=cfg.database)
    # Test connection
    assert db_mgr.test_connection() is True
    # Test table creation on real MySQL database
    db_mgr.create_tables()

    # Verify tables created
    inspector = inspect(db_mgr.engine)
    table_names = inspector.get_table_names()
    assert "pipeline_runs" in table_names
    assert "log_events" in table_names
    assert "rejected_records" in table_names
