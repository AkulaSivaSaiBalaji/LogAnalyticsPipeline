"""Regression and safety tests for Stage 1D: SQLite Batch-Size Safety and Database Parameter Limits."""

from datetime import datetime, timezone
import json
from pathlib import Path
from unittest.mock import MagicMock
import pytest
from sqlalchemy import select, func

from src.log_pipeline.config import DatabaseConfig, PipelineConfig, load_config
from src.log_pipeline.db.connection import DatabaseManager
from src.log_pipeline.db.schema import LogEventModel, PipelineRunModel, RejectedRecordModel
from src.log_pipeline.db.writer import (
    DatabaseWriter,
    SQLITE_MAX_VARIABLES_DEFAULT,
    SQLITE_MAX_VARIABLES_LEGACY,
    SQLITE_EVENT_ROW_WIDTH,
    SQLITE_REJECTED_ROW_WIDTH,
)
from src.log_pipeline.models import CanonicalSeverity, LogEventRecord, RejectedRecord
from src.log_pipeline.pipeline import LogAnalyticsPipeline


def _make_event(run_id: str, idx: int, event_id: str = None) -> LogEventRecord:
    return LogEventRecord(
        event_id=event_id or f"evt-{idx}",
        run_id=run_id,
        timestamp=datetime(2026, 10, 28, 12, 0, 0, tzinfo=timezone.utc),
        severity=CanonicalSeverity.INFO,
        service="order-service",
        endpoint="/api/v1/orders",
        http_status=200,
        latency_ms=15.5,
        message=f"Order processed #{idx}",
        source_file="orders.log",
        source_line=idx,
    )


def _make_rejected(run_id: str, idx: int) -> RejectedRecord:
    return RejectedRecord(
        run_id=run_id,
        source_file="orders.log",
        source_line=idx,
        raw_record=f'{{"invalid_line": {idx}}}',
        rejection_reasons=["validation_error: field missing"],
        created_at=datetime.now(timezone.utc),
    )


def test_sqlite_batch_size_5000_safety(tmp_path):
    """Verify that a batch of 6,000 events with batch_size=5,000 inserts safely on SQLite without 'too many SQL variables'."""
    db_file = tmp_path / "safety_test.db"
    db_manager = DatabaseManager(DatabaseConfig(url=f"sqlite:///{db_file}"))
    db_manager.create_tables()

    writer = DatabaseWriter(db_manager, batch_size=5000)
    run_id = "run-safety-6000"
    writer.start_run(run_id, ["orders.log"])

    events = [_make_event(run_id, i + 1) for i in range(6000)]
    result = writer.write_events_batch(events)

    assert result.inserted_count == 6000
    assert result.duplicate_count == 0
    assert len(result.inserted_events) == 6000

    with db_manager.session_scope() as session:
        count = session.scalar(select(func.count()).select_from(LogEventModel).where(LogEventModel.run_id == run_id))
        assert count == 6000


def test_sqlite_exact_and_plus_one_chunk_boundary(tmp_path):
    """Verify exact SQLite parameter limit boundary (2,730 rows) and boundary + 1 (2,731 rows)."""
    db_file = tmp_path / "boundary_test.db"
    db_manager = DatabaseManager(DatabaseConfig(url=f"sqlite:///{db_file}"))
    db_manager.create_tables()

    writer = DatabaseWriter(db_manager, batch_size=5000)
    run_id = "run-boundary"
    writer.start_run(run_id, ["boundary.log"])

    # Exact boundary: 2,730 rows * 12 cols = 32,760 parameters (<= 32,766)
    boundary_events = [_make_event(run_id, i + 1, event_id=f"exact-{i}") for i in range(2730)]
    res1 = writer.write_events_batch(boundary_events)
    assert res1.inserted_count == 2730

    # Boundary + 1: 2,731 rows (requires 2 chunks: 2,730 and 1)
    plus_one_events = [_make_event(run_id, i + 1, event_id=f"plus-one-{i}") for i in range(2731)]
    res2 = writer.write_events_batch(plus_one_events)
    assert res2.inserted_count == 2731


def test_in_batch_dedup_across_sqlite_chunks(tmp_path):
    """Verify that in-batch deduplication works correctly when duplicate event IDs span across chunk boundaries."""
    db_file = tmp_path / "inbatch_chunk_test.db"
    db_manager = DatabaseManager(DatabaseConfig(url=f"sqlite:///{db_file}"))
    db_manager.create_tables()

    writer = DatabaseWriter(db_manager, batch_size=5000)
    run_id = "run-inbatch-chunks"
    writer.start_run(run_id, ["inbatch.log"])

    # Create 4,000 events. Put identical duplicate ID in chunk 1 (idx 10) and chunk 2 (idx 3500)
    events = [_make_event(run_id, i + 1, event_id=f"ev-{i}") for i in range(4000)]
    events[3500] = _make_event(run_id, 11, event_id="ev-10")  # duplicate of index 10 (idx=11 matches ev-10)

    res = writer.write_events_batch(events)
    assert res.inserted_count == 3999
    assert res.duplicate_count == 1
    assert len(res.inserted_events) == 3999


def test_db_level_dedup_across_sqlite_chunks(tmp_path):
    """Verify that database-level deduplication works correctly across SQLite chunk boundaries."""
    db_file = tmp_path / "dblevel_chunk_test.db"
    db_manager = DatabaseManager(DatabaseConfig(url=f"sqlite:///{db_file}"))
    db_manager.create_tables()

    writer = DatabaseWriter(db_manager, batch_size=5000)
    run_id_1 = "run-initial"
    writer.start_run(run_id_1, ["initial.log"])

    # Batch 1: 3,000 events
    batch1 = [_make_event(run_id_1, i + 1, event_id=f"existing-{i}") for i in range(3000)]
    res1 = writer.write_events_batch(batch1)
    assert res1.inserted_count == 3000

    # Batch 2: 3,000 events containing 1,000 duplicates from Batch 1 distributed across chunks
    run_id_2 = "run-second"
    writer.start_run(run_id_2, ["second.log"])
    batch2 = []
    for i in range(3000):
        if i < 1000:
            batch2.append(_make_event(run_id_2, i + 1, event_id=f"existing-{i}"))  # Duplicate
        else:
            batch2.append(_make_event(run_id_2, i + 1, event_id=f"new-{i}"))  # New

    res2 = writer.write_events_batch(batch2)
    assert res2.inserted_count == 2000
    assert res2.duplicate_count == 1000
    assert len(res2.inserted_events) == 2000
    assert all(ev.event_id.startswith("new-") for ev in res2.inserted_events)


def test_sqlite_rejected_batch_large(tmp_path):
    """Verify that write_rejected_batch safely chunks batches exceeding SQLite variable limits."""
    db_file = tmp_path / "rejected_safety.db"
    db_manager = DatabaseManager(DatabaseConfig(url=f"sqlite:///{db_file}"))
    db_manager.create_tables()

    writer = DatabaseWriter(db_manager, batch_size=7000)
    run_id = "run-rejected-large"
    writer.start_run(run_id, ["rejected.log"])

    # 7,000 rejected records with 6 cols = 42,000 variables without chunking
    records = [_make_rejected(run_id, i + 1) for i in range(7000)]
    inserted = writer.write_rejected_batch(records)
    assert inserted == 7000

    with db_manager.session_scope() as session:
        count = session.scalar(
            select(func.count()).select_from(RejectedRecordModel).where(RejectedRecordModel.run_id == run_id)
        )
        assert count == 7000


def test_transaction_rollback_across_chunks(tmp_path):
    """Verify that if a later chunk fails, the transaction rolls back earlier chunks cleanly."""
    db_file = tmp_path / "rollback_test.db"
    db_manager = DatabaseManager(DatabaseConfig(url=f"sqlite:///{db_file}"))
    db_manager.create_tables()

    writer = DatabaseWriter(db_manager, batch_size=5000)
    run_id = "run-rollback"
    writer.start_run(run_id, ["test.log"])

    # 4,000 events: Chunk 1 has 2,730, Chunk 2 has 1,270.
    events = [_make_event(run_id, i + 1, event_id=f"rb-{i}") for i in range(4000)]

    # Make Chunk 2 fail by monkeypatching session.scalars on the second call
    real_session_scope = db_manager.session_scope

    from contextlib import contextmanager
    @contextmanager
    def patched_session_scope():
        with real_session_scope() as session:
            orig_scalars = session.scalars
            call_count = 0
            def fake_scalars(stmt, *args, **kwargs):
                nonlocal call_count
                call_count += 1
                if call_count == 2:
                    raise RuntimeError("Simulated failure in chunk 2")
                return orig_scalars(stmt, *args, **kwargs)
            session.scalars = fake_scalars
            yield session

    db_manager.session_scope = patched_session_scope

    with pytest.raises(RuntimeError, match="Simulated failure in chunk 2"):
        writer.write_events_batch(events)

    # Restore session_scope
    db_manager.session_scope = real_session_scope

    # Verify that Chunk 1 was NOT committed (all 4,000 rows rolled back)
    with db_manager.session_scope() as session:
        count = session.scalar(select(func.count()).select_from(LogEventModel).where(LogEventModel.run_id == run_id))
        assert count == 0


def test_legacy_sqlite_variable_limit_adaptation(tmp_path):
    """Verify adaptation to legacy SQLite variable limit (999 variables -> max 83 event rows)."""
    db_file = tmp_path / "legacy_test.db"
    db_manager = DatabaseManager(DatabaseConfig(url=f"sqlite:///{db_file}"))
    db_manager.create_tables()

    writer = DatabaseWriter(db_manager, batch_size=5000)
    writer.sqlite_max_variables = SQLITE_MAX_VARIABLES_LEGACY  # 999
    assert writer.get_max_event_chunk_size() == 999 // 12  # 83
    assert writer.get_max_rejected_chunk_size() == 999 // 6  # 166

    run_id = "run-legacy"
    writer.start_run(run_id, ["legacy.log"])

    events = [_make_event(run_id, i + 1, event_id=f"leg-{i}") for i in range(250)]
    res = writer.write_events_batch(events)
    assert res.inserted_count == 250

    with db_manager.session_scope() as session:
        count = session.scalar(select(func.count()).select_from(LogEventModel).where(LogEventModel.run_id == run_id))
        assert count == 250


def test_fixed_parameters_calculation(tmp_path):
    """Verify that fixed parameter count correctly reduces the allowable rows per chunk."""
    db_file = tmp_path / "fixed_params.db"
    db_manager = DatabaseManager(DatabaseConfig(url=f"sqlite:///{db_file}"))
    writer = DatabaseWriter(db_manager, batch_size=5000)

    # Default: 32,766 // 12 = 2,730
    assert writer.get_max_event_chunk_size(fixed_params=0) == 2730

    # With 10 fixed parameters: (32,766 - 10) // 12 = 32,756 // 12 = 2,729
    assert writer.get_max_event_chunk_size(fixed_params=10) == 2729


def test_non_sqlite_chunk_size_unconstrained(tmp_path):
    """Verify that non-SQLite engines preserve caller-specified batch_size unconstrained."""
    db_file = tmp_path / "non_sqlite.db"
    db_manager = DatabaseManager(DatabaseConfig(url=f"sqlite:///{db_file}"))
    writer = DatabaseWriter(db_manager, batch_size=10000)

    # Simulate non-SQLite (e.g. MySQL)
    writer.is_sqlite = False
    assert writer.get_max_event_chunk_size() == 10000
    assert writer.get_max_rejected_chunk_size() == 10000


def test_end_to_end_pipeline_with_batch_size_5000_sqlite(tmp_path):
    """Run full LogAnalyticsPipeline with batch_size=5,000 on a 6,000-line dataset in SQLite."""
    db_path = tmp_path / "e2e_batch_5000.db"
    quarantine_dir = tmp_path / "quarantine"
    reports_dir = tmp_path / "reports"
    log_file = tmp_path / "large_input.jsonl"

    # Generate 6,000 JSONL records (5,800 valid, 200 invalid)
    with open(log_file, "w", encoding="utf-8") as f:
        for i in range(5800):
            record = {
                "event_id": f"e2e-evt-{i}",
                "timestamp": "2026-10-28T12:00:00Z",
                "severity": "INFO",
                "service": "checkout-svc",
                "endpoint": "/checkout",
                "http_status": 200,
                "latency_ms": 25.0,
                "message": "checkout completed",
            }
            f.write(json.dumps(record) + "\n")
        for i in range(200):
            # Missing service & endpoint -> invalid
            bad_record = {
                "timestamp": "2026-10-28T12:01:00Z",
                "severity": "ERROR",
                "http_status": 500,
                "latency_ms": -50.0,
                "message": "broken line",
            }
            f.write(json.dumps(bad_record) + "\n")

    cfg = load_config(
        database_url=f"sqlite:///{db_path}",
        quarantine_dir=quarantine_dir,
        reports_dir=reports_dir,
        batch_size=5000,
    )
    pipeline = LogAnalyticsPipeline(config=cfg)
    result = pipeline.run(files=[log_file])

    assert result["status"] == "PARTIAL_SUCCESS"
    assert result["input_count"] == 6000
    assert result["valid_count"] == 5800
    assert result["rejected_count"] == 200
    assert result["duplicate_count"] == 0
    assert result["reconciliation"]["is_balanced"] is True

    # Verify database counts
    with pipeline.db.session_scope() as session:
        valid_db_count = session.scalar(
            select(func.count()).select_from(LogEventModel).where(LogEventModel.run_id == result["run_id"])
        )
        assert valid_db_count == 5800
        rejected_db_count = session.scalar(
            select(func.count()).select_from(RejectedRecordModel).where(RejectedRecordModel.run_id == result["run_id"])
        )
        assert rejected_db_count == 200


def test_sqlite_runtime_getlimit_detection(tmp_path):
    """Verify that writer queries live connection getlimit when available on Python 3.11+."""
    import sqlite3
    db_file = tmp_path / "getlimit_test.db"
    db_manager = DatabaseManager(DatabaseConfig(url=f"sqlite:///{db_file}"))
    writer = DatabaseWriter(db_manager)

    # If python sqlite3 supports getlimit, verify it matches
    with db_manager.engine.connect() as conn:
        raw_conn = conn.connection.dbapi_connection
        if hasattr(raw_conn, "getlimit") and hasattr(sqlite3, "SQLITE_LIMIT_VARIABLE_NUMBER"):
            expected = raw_conn.getlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER)
            assert writer.sqlite_max_variables == expected
        else:
            assert writer.sqlite_max_variables in (SQLITE_MAX_VARIABLES_DEFAULT, SQLITE_MAX_VARIABLES_LEGACY)


def test_sqlite_fallback_path_batch_safety(tmp_path):
    """Verify that the SQLite fallback path (when RETURNING is not supported) safely chunks 5,000 events."""
    from unittest.mock import patch
    import sqlite3

    db_file = tmp_path / "fallback_batch.db"
    db_manager = DatabaseManager(DatabaseConfig(url=f"sqlite:///{db_file}"))
    db_manager.create_tables()

    writer = DatabaseWriter(db_manager, batch_size=5000)
    run_id = "run-fallback-5000"
    writer.start_run(run_id, ["fallback.log"])

    events = [_make_event(run_id, i + 1, event_id=f"fb-{i}") for i in range(5000)]

    # Mock sqlite_version to 3.34.0 (< 3.35) so supports_returning is False
    with patch.object(sqlite3, "sqlite_version", "3.34.0"):
        res = writer.write_events_batch(events)
        assert res.inserted_count == 5000
        assert res.duplicate_count == 0

    with db_manager.session_scope() as session:
        count = session.scalar(select(func.count()).select_from(LogEventModel).where(LogEventModel.run_id == run_id))
        assert count == 5000
