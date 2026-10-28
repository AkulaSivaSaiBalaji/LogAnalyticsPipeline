"""Unit and integration tests for DatabaseWriter, idempotency, and run ledger."""

from datetime import datetime, timezone
import pytest
from sqlalchemy import select

from src.log_pipeline.db.connection import DatabaseManager
from src.log_pipeline.db.schema import LogEventModel, PipelineRunModel, RejectedRecordModel
from src.log_pipeline.db.writer import DatabaseWriter
from src.log_pipeline.models import CanonicalSeverity, LogEventRecord, RejectedRecord
from src.log_pipeline.config import load_config


def test_writer_start_and_complete_run(test_db_manager):
    """Verify run ledger tracking from RUNNING to SUCCESS."""
    writer = DatabaseWriter(test_db_manager)
    run_id = "test-run-ledger-01"

    writer.start_run(run_id, ["file_a.json", "file_b.log"])

    with test_db_manager.session_scope() as session:
        run = session.get(PipelineRunModel, run_id)
        assert run is not None
        assert run.status == "RUNNING"
        assert "file_a.json" in run.input_files

    writer.complete_run(
        run_id=run_id,
        status="SUCCESS",
        input_count=100,
        valid_count=85,
        rejected_count=10,
        duplicate_count=5,
    )

    with test_db_manager.session_scope() as session:
        run = session.get(PipelineRunModel, run_id)
        assert run.status == "SUCCESS"
        assert run.input_count == 100
        assert run.valid_count == 85
        assert run.rejected_count == 10
        assert run.duplicate_count == 5
        # Verify reconciliation formula
        assert run.input_count == run.valid_count + run.rejected_count + run.duplicate_count


def test_writer_idempotency_and_duplicates(test_db_manager):
    """Verify that re-inserting identical records does not create duplicates."""
    writer = DatabaseWriter(test_db_manager, batch_size=100)
    run_id = "test-idempotency-run"

    events = [
        LogEventRecord(
            event_id="evt-fixed-1",
            timestamp=datetime(2026, 10, 28, 12, 0, 0, tzinfo=timezone.utc),
            severity=CanonicalSeverity.INFO,
            service="auth",
            endpoint="/login",
            http_status=200,
            latency_ms=10.0,
            message="msg 1",
            source_file="auth.log",
            source_line=1,
            run_id=run_id,
        ),
        LogEventRecord(
            event_id="evt-fixed-2",
            timestamp=datetime(2026, 10, 28, 12, 1, 0, tzinfo=timezone.utc),
            severity=CanonicalSeverity.ERROR,
            service="auth",
            endpoint="/login",
            http_status=500,
            latency_ms=100.0,
            message="msg 2",
            source_file="auth.log",
            source_line=2,
            run_id=run_id,
        ),
    ]

    # First run: 2 inserted, 0 duplicates
    inserted, dupes = writer.write_events_batch(events)
    assert inserted == 2
    assert dupes == 0

    # Verify rows in DB
    with test_db_manager.session_scope() as session:
        rows = session.scalars(select(LogEventModel)).all()
        assert len(rows) == 2

    # Second run (exact same records rerun): 0 inserted, 2 duplicates
    inserted2, dupes2 = writer.write_events_batch(events)
    assert inserted2 == 0
    assert dupes2 == 2

    # Database count must still be exactly 2 (idempotent guarantee)
    with test_db_manager.session_scope() as session:
        rows2 = session.scalars(select(LogEventModel)).all()
        assert len(rows2) == 2


def test_writer_in_batch_duplicates(test_db_manager):
    """Verify multiple occurrences of the same event_id within one batch are counted as duplicates."""
    writer = DatabaseWriter(test_db_manager)
    run_id = "test-inbatch-dupe"

    ev = LogEventRecord(
        event_id="evt-dupe-same",
        timestamp=datetime(2026, 10, 28, 12, 0, 0, tzinfo=timezone.utc),
        severity=CanonicalSeverity.INFO,
        service="order",
        message="Order placed",
        source_file="orders.log",
        source_line=1,
        run_id=run_id,
    )
    # Batch containing 3 identical event IDs
    batch = [ev, ev, ev]
    inserted, dupes = writer.write_events_batch(batch)
    assert inserted == 1
    assert dupes == 2


def test_writer_rejected_records(test_db_manager):
    """Verify rejected records batch insertion."""
    writer = DatabaseWriter(test_db_manager)
    rejections = [
        RejectedRecord(
            run_id="run-rej-1",
            source_file="bad1.jsonl",
            source_line=10,
            raw_record="bad record text 1",
            rejection_reasons=["MALFORMED_JSON_LINE"],
        ),
        RejectedRecord(
            run_id="run-rej-1",
            source_file="bad1.jsonl",
            source_line=15,
            raw_record="bad record text 2",
            rejection_reasons=["NEGATIVE_LATENCY", "INVALID_HTTP_STATUS"],
        ),
    ]

    count = writer.write_rejected_batch(rejections)
    assert count == 2

    with test_db_manager.session_scope() as session:
        records = session.scalars(select(RejectedRecordModel)).all()
        assert len(records) == 2
        assert records[0].source_file == "bad1.jsonl"
        assert "NEGATIVE_LATENCY" in records[1].rejection_reasons


def test_writer_mysql_idempotency_live():
    """Verify live MySQL idempotency and table writes if MySQL is available."""
    cfg = load_config()
    db_mgr = DatabaseManager(config=cfg.database)
    connected, _ = db_mgr.verify_connection()
    if not connected:
        pytest.skip("MySQL not reachable in this test environment")

    db_mgr.create_tables()
    writer = DatabaseWriter(db_mgr)
    test_run_id = f"mysql-test-{int(datetime.now().timestamp())}"

    ev = LogEventRecord(
        event_id=f"mysql-evt-{test_run_id}",
        timestamp=datetime.now(timezone.utc),
        severity=CanonicalSeverity.INFO,
        service="mysql-service-test",
        endpoint="/test",
        http_status=200,
        latency_ms=12.5,
        message="MySQL idempotency test",
        source_file="test.log",
        source_line=1,
        run_id=test_run_id,
    )

    ins1, dup1 = writer.write_events_batch([ev])
    assert ins1 == 1
    assert dup1 == 0

    # Rerun same event
    ins2, dup2 = writer.write_events_batch([ev])
    assert ins2 == 0
    assert dup2 == 1
