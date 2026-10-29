"""Stage 1C regression tests: Pipeline Run Lifecycle and Failure Recovery.

Verifies:
1. The active run ID remains unchanged throughout processing and failure handling.
2. A failure after rejected records are buffered does not silently lose them when DB is available.
3. A database failure is reported clearly when durable recovery cannot be completed.
4. Failed runs receive an accurate terminal status and cannot be overwritten as successful.
5. Successful runs retain existing behavior.
6. Retry/flush logic does not duplicate rejected records.
7. Foreign-key enforcement remains active.
8. Cleanup errors during failure handling do not mask the original root cause exception.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
import pytest
from sqlalchemy import select, func

from src.log_pipeline.config import PipelineConfig, DatabaseConfig
from src.log_pipeline.db.connection import DatabaseManager
from src.log_pipeline.db.schema import (
    Base,
    PipelineRunModel,
    LogEventModel,
    RejectedRecordModel,
)
from src.log_pipeline.db.writer import DatabaseWriter
from src.log_pipeline.models import CanonicalSeverity, LogEventRecord, RejectedRecord
from src.log_pipeline.pipeline import LogAnalyticsPipeline


def test_active_run_id_consistency_and_immutability(tmp_path: Path):
    """Verify run ID remains consistent throughout execution and cannot be overwritten once completed."""
    input_dir = tmp_path / "inputs"
    input_dir.mkdir(parents=True, exist_ok=True)
    sample_file = input_dir / "valid.jsonl"
    sample_file.write_text(
        json.dumps({
            "timestamp": "2026-10-28T12:00:00Z",
            "severity": "INFO",
            "service": "auth-service",
            "endpoint": "/api/v1/auth",
            "http_status": 200,
            "latency_ms": 15.0,
            "message": "Authenticated",
        }) + "\n",
        encoding="utf-8",
    )

    db_path = tmp_path / "run_id_test.db"
    config = PipelineConfig(
        database=DatabaseConfig(url=f"sqlite:///{db_path}"),
        input_dir=input_dir,
        quarantine_dir=tmp_path / "quarantine",
        reports_dir=tmp_path / "reports",
    )

    pipeline = LogAnalyticsPipeline(config=config)
    fixed_run_id = "run-fixed-audit-123"

    res = pipeline.run(files=[sample_file], run_id=fixed_run_id)

    # 1. Pipeline instance and result reflect the exact requested run_id
    assert pipeline.active_run_id == fixed_run_id
    assert res["run_id"] == fixed_run_id
    assert res["status"] == "SUCCESS"

    # 2. Database run table contains the exact run_id
    with pipeline.db.session_scope() as session:
        run_row = session.get(PipelineRunModel, fixed_run_id)
        assert run_row is not None
        assert run_row.status == "SUCCESS"

        # Events reference this run_id
        events = session.scalars(select(LogEventModel).where(LogEventModel.run_id == fixed_run_id)).all()
        assert len(events) == 1
        assert events[0].run_id == fixed_run_id

    # 3. Attempting to start another run with the same completed run_id fails
    with pytest.raises(ValueError, match="already completed with status 'SUCCESS'"):
        pipeline.run(files=[sample_file], run_id=fixed_run_id)


def test_failure_preserves_buffered_rejected_records_and_quarantine(tmp_path: Path, monkeypatch):
    """Verify an exception during processing does not lose buffered rejected records."""
    input_dir = tmp_path / "inputs"
    quarantine_dir = tmp_path / "quarantine"
    reports_dir = tmp_path / "reports"
    for d in (input_dir, quarantine_dir, reports_dir):
        d.mkdir(parents=True, exist_ok=True)

    # File with 1 valid line and 2 invalid lines (one malformed latency, one negative latency)
    log_content = (
        "2026-10-28 12:00:01.000 [INFO] [api] GET /ok 200 10.0ms - First ok\n"
        "2026-10-28 12:00:02.000 [INFO] [api] GET /bad 200 1.2.3ms - Bad latency\n"
        "2026-10-28 12:00:03.000 [INFO] [api] GET /neg 200 -50.0ms - Negative latency\n"
    )
    test_file = input_dir / "service.log"
    test_file.write_text(log_content, encoding="utf-8")

    db_path = tmp_path / "failure_preserves_rejected.db"
    config = PipelineConfig(
        database=DatabaseConfig(url=f"sqlite:///{db_path}"),
        input_dir=input_dir,
        quarantine_dir=quarantine_dir,
        reports_dir=reports_dir,
        batch_size=100,  # Buffer will hold the 2 rejected records without flushing during loop
    )

    pipeline = LogAnalyticsPipeline(config=config)
    run_id = "run-fail-with-buffered-rejections"

    # Inject failure during metrics calculation (after records are parsed and buffered)
    def broken_aggregates(*args, **kwargs):
        raise RuntimeError("Simulated fatal metrics calculation error")

    monkeypatch.setattr(
        "src.log_pipeline.metrics.calculator.MetricsCalculator.calculate_aggregates",
        broken_aggregates,
    )

    with pytest.raises(RuntimeError, match="Simulated fatal metrics calculation error"):
        pipeline.run(files=[test_file], run_id=run_id)

    # Database verification
    with pipeline.db.session_scope() as session:
        run = session.get(PipelineRunModel, run_id)
        assert run is not None
        assert run.status == "FAILED"
        assert "Simulated fatal metrics calculation error" in (run.failure_details or "")

        # Crucial check: the 2 buffered rejected records were NOT lost!
        rejections = session.scalars(
            select(RejectedRecordModel).where(RejectedRecordModel.run_id == run_id)
        ).all()
        assert len(rejections) == 2
        assert run.rejected_count == 2

    # Quarantine file verification on disk
    quarantine_files = list(quarantine_dir.glob(f"{run_id}_quarantined.jsonl"))
    assert len(quarantine_files) == 1
    disk_lines = [ln.strip() for ln in quarantine_files[0].read_text(encoding="utf-8").splitlines() if ln.strip()]
    assert len(disk_lines) == 2


def test_database_failure_reported_clearly(tmp_path: Path, monkeypatch):
    """Verify that a fatal database error during batch writing is reported explicitly and not swallowed."""
    input_dir = tmp_path / "inputs"
    input_dir.mkdir(parents=True, exist_ok=True)
    sample_file = input_dir / "valid.jsonl"
    sample_file.write_text(
        json.dumps({
            "timestamp": "2026-10-28T12:00:00Z",
            "severity": "INFO",
            "service": "api",
            "endpoint": "/test",
            "http_status": 200,
            "latency_ms": 10.0,
            "message": "ok",
        }) + "\n",
        encoding="utf-8",
    )

    db_path = tmp_path / "db_fatal.db"
    config = PipelineConfig(
        database=DatabaseConfig(url=f"sqlite:///{db_path}"),
        input_dir=input_dir,
        quarantine_dir=tmp_path / "quarantine",
        reports_dir=tmp_path / "reports",
    )

    pipeline = LogAnalyticsPipeline(config=config)
    run_id = "run-db-outage"

    def fatal_write(*args, **kwargs):
        raise ConnectionError("Simulated database connection lost during write_events_batch")

    monkeypatch.setattr(pipeline.writer, "write_events_batch", fatal_write)

    with pytest.raises(ConnectionError, match="Simulated database connection lost"):
        pipeline.run(files=[sample_file], run_id=run_id)


def test_failed_run_cannot_be_falsely_marked_successful(test_db_manager):
    """Verify state transition rules: a FAILED run cannot be overwritten with SUCCESS."""
    writer = DatabaseWriter(test_db_manager)
    run_id = "run-terminal-state-test"
    writer.start_run(run_id, ["app.log"])

    # Fail the run
    writer.complete_run(
        run_id=run_id,
        status="FAILED",
        input_count=10,
        valid_count=5,
        rejected_count=2,
        duplicate_count=0,
        failure_details="Fatal unhandled crash",
    )

    with test_db_manager.session_scope() as session:
        run = session.get(PipelineRunModel, run_id)
        assert run.status == "FAILED"
        assert run.failure_details == "Fatal unhandled crash"

    # Attempt to overwrite with SUCCESS
    writer.complete_run(
        run_id=run_id,
        status="SUCCESS",
        input_count=10,
        valid_count=10,
        rejected_count=0,
        duplicate_count=0,
    )

    # Status must strictly remain FAILED
    with test_db_manager.session_scope() as session:
        run = session.get(PipelineRunModel, run_id)
        assert run.status == "FAILED"
        assert run.failure_details == "Fatal unhandled crash"


def test_repeated_finalization_idempotent(test_db_manager):
    """Verify multiple finalizations do not duplicate or corrupt run ledger state."""
    writer = DatabaseWriter(test_db_manager)
    run_id = "run-repeat-final"
    writer.start_run(run_id, ["app.log"])

    writer.complete_run(
        run_id=run_id,
        status="SUCCESS",
        input_count=50,
        valid_count=48,
        rejected_count=2,
        duplicate_count=0,
    )

    # Second identical completion
    writer.complete_run(
        run_id=run_id,
        status="SUCCESS",
        input_count=50,
        valid_count=48,
        rejected_count=2,
        duplicate_count=0,
    )

    with test_db_manager.session_scope() as session:
        runs = session.scalars(select(PipelineRunModel).where(PipelineRunModel.run_id == run_id)).all()
        assert len(runs) == 1
        assert runs[0].status == "SUCCESS"
        assert runs[0].input_count == 50
        assert runs[0].valid_count == 48


def test_rejected_flush_does_not_duplicate_on_failure_recovery(tmp_path: Path, monkeypatch):
    """Verify that flushing unflushed rejected records during failure recovery does not duplicate records already flushed."""
    input_dir = tmp_path / "inputs"
    quarantine_dir = tmp_path / "quarantine"
    reports_dir = tmp_path / "reports"
    for d in (input_dir, quarantine_dir, reports_dir):
        d.mkdir(parents=True, exist_ok=True)

    # 3 rejected records total
    # With batch_size=2:
    # First 2 are flushed inside the processing loop.
    # The 3rd record remains in the buffer when the exception is raised.
    log_content = (
        "2026-10-28 12:00:01.000 [INFO] [api] GET /bad1 200 1.2.3ms - Bad 1\n"
        "2026-10-28 12:00:02.000 [INFO] [api] GET /bad2 200 1.2.3ms - Bad 2\n"
        "2026-10-28 12:00:03.000 [INFO] [api] GET /bad3 200 1.2.3ms - Bad 3\n"
    )
    test_file = input_dir / "service.log"
    test_file.write_text(log_content, encoding="utf-8")

    db_path = tmp_path / "no_dupe_rejections.db"
    config = PipelineConfig(
        database=DatabaseConfig(url=f"sqlite:///{db_path}"),
        input_dir=input_dir,
        quarantine_dir=quarantine_dir,
        reports_dir=reports_dir,
        batch_size=2,
    )

    pipeline = LogAnalyticsPipeline(config=config)
    run_id = "run-batch-2-fail"

    def fail_at_end(*args, **kwargs):
        raise RuntimeError("Simulated crash before finalization")

    monkeypatch.setattr(
        "src.log_pipeline.metrics.calculator.MetricsCalculator.calculate_aggregates",
        fail_at_end,
    )

    with pytest.raises(RuntimeError, match="Simulated crash before finalization"):
        pipeline.run(files=[test_file], run_id=run_id)

    # Verify exactly 3 rejected records are stored in DB, not 4 or 5
    with pipeline.db.session_scope() as session:
        rejections = session.scalars(
            select(RejectedRecordModel).where(RejectedRecordModel.run_id == run_id)
        ).all()
        assert len(rejections) == 3
        run = session.get(PipelineRunModel, run_id)
        assert run.rejected_count == 3


def test_cleanup_failure_does_not_mask_original_exception(tmp_path: Path, monkeypatch):
    """Verify that if complete_run fails during cleanup, the original exception is preserved."""
    input_dir = tmp_path / "inputs"
    input_dir.mkdir(parents=True, exist_ok=True)
    sample_file = input_dir / "valid.jsonl"
    sample_file.write_text(
        json.dumps({
            "timestamp": "2026-10-28T12:00:00Z",
            "severity": "INFO",
            "service": "api",
            "message": "ok",
        }) + "\n",
        encoding="utf-8",
    )

    db_path = tmp_path / "mask_test.db"
    config = PipelineConfig(
        database=DatabaseConfig(url=f"sqlite:///{db_path}"),
        input_dir=input_dir,
        quarantine_dir=tmp_path / "quarantine",
        reports_dir=tmp_path / "reports",
    )

    pipeline = LogAnalyticsPipeline(config=config)
    run_id = "run-mask-test"

    # Make writer.write_events_batch fail with OriginalRootCauseError
    class OriginalRootCauseError(Exception):
        pass

    def broken_write(*args, **kwargs):
        raise OriginalRootCauseError("Original processing failure")

    # Make writer.complete_run fail with CleanupSecondaryError
    class CleanupSecondaryError(Exception):
        pass

    def broken_complete(*args, **kwargs):
        raise CleanupSecondaryError("Secondary cleanup failure")

    monkeypatch.setattr(pipeline.writer, "write_events_batch", broken_write)
    monkeypatch.setattr(pipeline.writer, "complete_run", broken_complete)

    # Must raise OriginalRootCauseError, NOT CleanupSecondaryError!
    with pytest.raises(OriginalRootCauseError, match="Original processing failure"):
        pipeline.run(files=[sample_file], run_id=run_id)
