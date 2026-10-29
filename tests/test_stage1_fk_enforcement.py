"""Stage 1B-3 regression tests: Database Foreign-Key Enforcement.

Verifies:
1. A valid pipeline run can be created.
2. A valid event referencing an existing run can be inserted.
3. A valid rejected record referencing an existing run can be inserted.
4. Inserting an event with a nonexistent run ID fails at the database level with IntegrityError.
5. Inserting a rejected record with a nonexistent run ID fails at the database level with IntegrityError.
6. SQLite foreign-key enforcement is enabled on a fresh connection (PRAGMA foreign_keys == 1).
7. DatabaseWriter operations enforce foreign keys and roll back cleanly on constraint failures.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import pytest
from sqlalchemy import text, select
from sqlalchemy.exc import IntegrityError

from src.log_pipeline.db.connection import DatabaseManager
from src.log_pipeline.db.schema import (
    Base,
    PipelineRunModel,
    LogEventModel,
    RejectedRecordModel,
)
from src.log_pipeline.db.writer import DatabaseWriter
from src.log_pipeline.models import CanonicalSeverity, LogEventRecord, RejectedRecord
from src.log_pipeline.config import load_config


def test_sqlite_foreign_key_pragma_enabled_on_fresh_connection(tmp_path: Path):
    """Verify PRAGMA foreign_keys=ON is applied to every fresh SQLite connection."""
    db_file = tmp_path / "pragma_test.db"
    db_mgr = DatabaseManager(url=f"sqlite:///{db_file}")

    with db_mgr.engine.connect() as conn:
        result = conn.execute(text("PRAGMA foreign_keys;"))
        pragma_val = result.scalar()
        assert pragma_val == 1, f"Expected PRAGMA foreign_keys=1, got {pragma_val}"


def test_create_valid_pipeline_run(test_db_manager):
    """Verify that a valid pipeline run record can be created and queried."""
    run_id = "run-fk-001"
    with test_db_manager.session_scope() as session:
        run = PipelineRunModel(
            run_id=run_id,
            start_time=datetime.now(timezone.utc),
            status="RUNNING",
            input_files='["test_input.log"]',
            input_count=10,
            valid_count=8,
            rejected_count=2,
            duplicate_count=0,
        )
        session.add(run)

    with test_db_manager.session_scope() as session:
        queried = session.get(PipelineRunModel, run_id)
        assert queried is not None
        assert queried.run_id == run_id
        assert queried.status == "RUNNING"
        assert queried.input_count == 10


def test_insert_valid_event_referencing_run(test_db_manager):
    """Verify inserting an event referencing an existing run succeeds."""
    run_id = "run-fk-002"
    with test_db_manager.session_scope() as session:
        run = PipelineRunModel(
            run_id=run_id,
            start_time=datetime.now(timezone.utc),
            status="RUNNING",
            input_files="[]",
        )
        session.add(run)

    with test_db_manager.session_scope() as session:
        event = LogEventModel(
            event_id="evt-fk-002",
            run_id=run_id,
            timestamp=datetime.now(timezone.utc),
            severity="INFO",
            service="order-service",
            endpoint="/api/v1/orders",
            http_status=200,
            latency_ms=15.0,
            message="Order placed",
            source_file="orders.log",
            source_line=1,
            created_at=datetime.now(timezone.utc),
        )
        session.add(event)

    with test_db_manager.session_scope() as session:
        queried = session.get(LogEventModel, "evt-fk-002")
        assert queried is not None
        assert queried.run_id == run_id
        assert queried.service == "order-service"


def test_insert_valid_rejected_record_referencing_run(test_db_manager):
    """Verify inserting a rejected record referencing an existing run succeeds."""
    run_id = "run-fk-003"
    with test_db_manager.session_scope() as session:
        run = PipelineRunModel(
            run_id=run_id,
            start_time=datetime.now(timezone.utc),
            status="RUNNING",
            input_files="[]",
        )
        session.add(run)

    with test_db_manager.session_scope() as session:
        rej = RejectedRecordModel(
            run_id=run_id,
            source_file="bad.log",
            source_line=10,
            raw_record="corrupted log payload",
            rejection_reasons='["INVALID_LOG_RECORD: MALFORMED_LATENCY"]',
            created_at=datetime.now(timezone.utc),
        )
        session.add(rej)

    with test_db_manager.session_scope() as session:
        results = session.scalars(
            select(RejectedRecordModel).where(RejectedRecordModel.run_id == run_id)
        ).all()
        assert len(results) == 1
        assert results[0].source_file == "bad.log"


def test_insert_event_nonexistent_run_fails(test_db_manager):
    """Verify inserting an event with a nonexistent run ID fails at DB level with IntegrityError."""
    with pytest.raises(IntegrityError):
        with test_db_manager.session_scope() as session:
            event = LogEventModel(
                event_id="evt-orphan-fail",
                run_id="nonexistent-run-id-999",
                timestamp=datetime.now(timezone.utc),
                severity="ERROR",
                service="orphan-service",
                endpoint="/fail",
                http_status=500,
                latency_ms=10.0,
                message="orphan event",
                source_file="orphan.log",
                source_line=1,
                created_at=datetime.now(timezone.utc),
            )
            session.add(event)

    # Verify no orphan row was committed
    with test_db_manager.session_scope() as session:
        queried = session.get(LogEventModel, "evt-orphan-fail")
        assert queried is None


def test_insert_rejected_record_nonexistent_run_fails(test_db_manager):
    """Verify inserting a rejected record with a nonexistent run ID fails at DB level with IntegrityError."""
    with pytest.raises(IntegrityError):
        with test_db_manager.session_scope() as session:
            rej = RejectedRecordModel(
                run_id="nonexistent-run-id-888",
                source_file="orphan_rej.log",
                source_line=1,
                raw_record="orphan rejected text",
                rejection_reasons='["TEST_REASON"]',
                created_at=datetime.now(timezone.utc),
            )
            session.add(rej)

    # Verify no orphan row was committed
    with test_db_manager.session_scope() as session:
        results = session.scalars(
            select(RejectedRecordModel).where(RejectedRecordModel.run_id == "nonexistent-run-id-888")
        ).all()
        assert len(results) == 0


def test_writer_write_batch_enforces_foreign_key(test_db_manager):
    """Verify DatabaseWriter methods enforce foreign keys and succeed when run exists."""
    writer = DatabaseWriter(test_db_manager)
    unregistered_run_id = "unregistered-run"

    ev = LogEventRecord(
        event_id="evt-unregistered",
        timestamp=datetime.now(timezone.utc),
        severity=CanonicalSeverity.INFO,
        service="test-service",
        endpoint="/test",
        http_status=200,
        latency_ms=10.0,
        message="test message",
        source_file="test.log",
        source_line=1,
        run_id=unregistered_run_id,
    )

    # Must raise IntegrityError when run record does not exist
    with pytest.raises(IntegrityError):
        writer.write_events_batch([ev])

    # Now create the run record
    registered_run_id = "registered-run"
    writer.start_run(registered_run_id, ["test.log"])

    ev_registered = LogEventRecord(
        event_id="evt-registered",
        timestamp=datetime.now(timezone.utc),
        severity=CanonicalSeverity.INFO,
        service="test-service",
        endpoint="/test",
        http_status=200,
        latency_ms=10.0,
        message="test message",
        source_file="test.log",
        source_line=1,
        run_id=registered_run_id,
    )

    # Succeeds once run record exists
    ins, dup = writer.write_events_batch([ev_registered])
    assert ins == 1
    assert dup == 0


def test_mysql_fk_enforcement_live():
    """Verify live MySQL foreign-key enforcement if MySQL is reachable."""
    cfg = load_config()
    db_mgr = DatabaseManager(config=cfg.database)
    connected, _ = db_mgr.verify_connection()
    if not connected:
        pytest.skip("MySQL not reachable in this test environment")

    db_mgr.create_tables()
    writer = DatabaseWriter(db_mgr)
    nonexistent_run = f"mysql-orphan-{int(datetime.now().timestamp())}"

    ev = LogEventRecord(
        event_id=f"mysql-orphan-evt-{nonexistent_run}",
        timestamp=datetime.now(timezone.utc),
        severity=CanonicalSeverity.INFO,
        service="mysql-service",
        endpoint="/test",
        http_status=200,
        latency_ms=10.0,
        message="orphan mysql event",
        source_file="mysql.log",
        source_line=1,
        run_id=nonexistent_run,
    )

    with pytest.raises(IntegrityError):
        writer.write_events_batch([ev])
