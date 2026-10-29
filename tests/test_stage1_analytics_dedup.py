"""Regression tests for Stage 1A: Analytics Correctness and Deduplication."""

import json
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd
import pytest
from sqlalchemy import select, func

from src.log_pipeline.config import PipelineConfig, DatabaseConfig
from src.log_pipeline.db.schema import LogEventModel
from src.log_pipeline.db.writer import DatabaseWriter, BatchWriteResult
from src.log_pipeline.models import CanonicalSeverity, LogEventRecord
from src.log_pipeline.pipeline import LogAnalyticsPipeline


def _create_jsonl(path: Path, rows: list) -> Path:
    """Helper to write records to a JSONL file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")
    return path


def test_stage1_aab_fixture_analytics(tmp_path):
    """Verify that input [a, a, b] deduplicates before computing metrics.

    Fixture:
    - Event a1: INFO, 200 OK, latency 10.0 ms
    - Event a2: INFO, 200 OK, latency 10.0 ms (duplicate of a1)
    - Event b:  ERROR, 500 Err, latency 100.0 ms

    Expected:
    - Input count: 3
    - Valid count (DB inserted): 2
    - Duplicate count: 1
    - Rejected count: 0
    - Total events in analytics: 2
    - Error count: 1
    - Error rate: 50.0%
    - Avg latency: 55.0 ms
    - p50 latency: 55.0 ms
    """
    db_path = tmp_path / "aab_test.db"
    log_file = _create_jsonl(
        tmp_path / "aab_input.jsonl",
        [
            {
                "event_id": "evt-a",
                "timestamp": "2026-10-28T12:00:00Z",
                "severity": "INFO",
                "service": "api",
                "endpoint": "/test",
                "http_status": 200,
                "latency_ms": 10.0,
                "message": "ok",
            },
            {
                "event_id": "evt-a",
                "timestamp": "2026-10-28T12:00:00Z",
                "severity": "INFO",
                "service": "api",
                "endpoint": "/test",
                "http_status": 200,
                "latency_ms": 10.0,
                "message": "ok",
            },
            {
                "event_id": "evt-b",
                "timestamp": "2026-10-28T12:00:00Z",
                "severity": "ERROR",
                "service": "api",
                "endpoint": "/test",
                "http_status": 500,
                "latency_ms": 100.0,
                "message": "fail",
            },
        ],
    )

    cfg = PipelineConfig(
        database=DatabaseConfig(url=f"sqlite:///{db_path}"),
        input_dir=tmp_path / "input",
        quarantine_dir=tmp_path / "quarantine",
        reports_dir=tmp_path / "reports",
        batch_size=100,
    )
    pipeline = LogAnalyticsPipeline(config=cfg)
    res = pipeline.run(files=[log_file], run_id="run-aab")

    # Reconciliation and count verification
    assert res["input_count"] == 3
    assert res["valid_count"] == 2
    assert res["duplicate_count"] == 1
    assert res["rejected_count"] == 0
    assert res["status"] == "SUCCESS"
    assert res["reconciliation"]["is_balanced"] is True
    assert res["reconciliation"]["discrepancy"] == 0

    # Target database rows verification
    with pipeline.db.session_scope() as session:
        db_rows = session.scalar(select(func.count()).select_from(LogEventModel))
        assert db_rows == 2

    # Analytical metrics verification
    assert len(res["service_metrics"]) == 1
    sm = res["service_metrics"][0]
    assert sm["service"] == "api"
    assert sm["total_events"] == 2
    assert sm["error_count"] == 1
    assert sm["error_rate_pct"] == 50.0
    assert sm["latency_count"] == 2
    assert sm["avg_latency_ms"] == 55.0
    assert sm["p50_latency_ms"] == 55.0

    # Verify CSV on disk
    service_csv = pd.read_csv(res["service_report"])
    assert len(service_csv) == 1
    assert service_csv.iloc[0]["total_events"] == 2
    assert service_csv.iloc[0]["error_rate_pct"] == 50.0
    assert service_csv.iloc[0]["avg_latency_ms"] == 55.0
    assert service_csv.iloc[0]["p50_latency_ms"] == 55.0


def test_stage1_in_batch_duplicates(tmp_path):
    """Verify multiple duplicates within a single batch are not double counted."""
    db_path = tmp_path / "inbatch_test.db"
    single_event = {
        "event_id": "evt-repeat",
        "timestamp": "2026-10-28T12:00:00Z",
        "severity": "INFO",
        "service": "billing",
        "endpoint": "/charge",
        "http_status": 200,
        "latency_ms": 25.0,
        "message": "charged",
    }
    log_file = _create_jsonl(tmp_path / "inbatch.jsonl", [single_event, single_event, single_event])

    cfg = PipelineConfig(
        database=DatabaseConfig(url=f"sqlite:///{db_path}"),
        input_dir=tmp_path / "input",
        quarantine_dir=tmp_path / "quarantine",
        reports_dir=tmp_path / "reports",
        batch_size=100,
    )
    pipeline = LogAnalyticsPipeline(config=cfg)
    res = pipeline.run(files=[log_file], run_id="run-inbatch")

    assert res["input_count"] == 3
    assert res["valid_count"] == 1
    assert res["duplicate_count"] == 2
    assert res["rejected_count"] == 0

    with pipeline.db.session_scope() as session:
        assert session.scalar(select(func.count()).select_from(LogEventModel)) == 1

    sm = res["service_metrics"][0]
    assert sm["total_events"] == 1
    assert sm["error_count"] == 0
    assert sm["error_rate_pct"] == 0.0
    assert sm["avg_latency_ms"] == 25.0


def test_stage1_cross_batch_duplicates(tmp_path):
    """Verify duplicates spanning across different batch chunk boundaries are excluded from metrics."""
    db_path = tmp_path / "cross_batch.db"
    events = [
        {
            "event_id": "cross-1",
            "timestamp": "2026-10-28T12:00:00Z",
            "severity": "INFO",
            "service": "auth",
            "endpoint": "/login",
            "http_status": 200,
            "latency_ms": 10.0,
            "message": "m1",
        },
        {
            "event_id": "cross-1",  # duplicate in next batch when batch_size=1
            "timestamp": "2026-10-28T12:00:00Z",
            "severity": "INFO",
            "service": "auth",
            "endpoint": "/login",
            "http_status": 200,
            "latency_ms": 10.0,
            "message": "m1",
        },
        {
            "event_id": "cross-2",
            "timestamp": "2026-10-28T12:00:00Z",
            "severity": "ERROR",
            "service": "auth",
            "endpoint": "/login",
            "http_status": 500,
            "latency_ms": 100.0,
            "message": "m2",
        },
    ]
    log_file = _create_jsonl(tmp_path / "cross.jsonl", events)

    # Set batch_size=1 to force cross-batch processing for each event
    cfg = PipelineConfig(
        database=DatabaseConfig(url=f"sqlite:///{db_path}"),
        input_dir=tmp_path / "input",
        quarantine_dir=tmp_path / "quarantine",
        reports_dir=tmp_path / "reports",
        batch_size=1,
    )
    pipeline = LogAnalyticsPipeline(config=cfg)
    res = pipeline.run(files=[log_file], run_id="run-cross")

    assert res["input_count"] == 3
    assert res["valid_count"] == 2
    assert res["duplicate_count"] == 1
    assert res["rejected_count"] == 0

    with pipeline.db.session_scope() as session:
        assert session.scalar(select(func.count()).select_from(LogEventModel)) == 2

    assert len(res["service_metrics"]) == 1
    sm = res["service_metrics"][0]
    assert sm["total_events"] == 2
    assert sm["error_count"] == 1
    assert sm["error_rate_pct"] == 50.0
    assert sm["avg_latency_ms"] == 55.0


def test_stage1_rerun_distinguishes_newly_inserted_events(tmp_path):
    """Verify that a rerun reports zero newly processed events rather than re-aggregating historical rows."""
    db_path = tmp_path / "rerun_test.db"
    events = [
        {
            "event_id": "r-1",
            "timestamp": "2026-10-28T12:00:00Z",
            "severity": "INFO",
            "service": "api",
            "endpoint": "/ping",
            "http_status": 200,
            "latency_ms": 20.0,
            "message": "ping 1",
        },
        {
            "event_id": "r-2",
            "timestamp": "2026-10-28T12:00:00Z",
            "severity": "INFO",
            "service": "api",
            "endpoint": "/ping",
            "http_status": 200,
            "latency_ms": 40.0,
            "message": "ping 2",
        },
    ]
    log_file = _create_jsonl(tmp_path / "rerun_data.jsonl", events)

    cfg = PipelineConfig(
        database=DatabaseConfig(url=f"sqlite:///{db_path}"),
        input_dir=tmp_path / "input",
        quarantine_dir=tmp_path / "quarantine",
        reports_dir=tmp_path / "reports",
        batch_size=100,
    )
    pipeline = LogAnalyticsPipeline(config=cfg)

    # First run
    res1 = pipeline.run(files=[log_file], run_id="run-first")
    assert res1["valid_count"] == 2
    assert res1["duplicate_count"] == 0
    assert len(res1["service_metrics"]) == 1
    assert res1["service_metrics"][0]["total_events"] == 2

    # Second run (pure rerun)
    res2 = pipeline.run(files=[log_file], run_id="run-second")
    assert res2["input_count"] == 2
    assert res2["valid_count"] == 0
    assert res2["duplicate_count"] == 2
    assert res2["rejected_count"] == 0
    assert res2["reconciliation"]["is_balanced"] is True

    # Run-level report semantics: 0 newly inserted events
    assert res2["service_metrics"] == []
    assert res2["hourly_metrics"] == []

    # Database still contains exactly the 2 rows
    with pipeline.db.session_scope() as session:
        assert session.scalar(select(func.count()).select_from(LogEventModel)) == 2


def test_writer_db_level_uniqueness_conflict(test_db_manager):
    """Verify that a database-level conflict only returns actually inserted events.

    Scenario:
    - Pre-insert 'evt-conflict-1' into database directly.
    - Submit a batch containing:
        [evt-conflict-1, evt-conflict-1, evt-new-2]
    - Total input: 3 items.
    - Within-batch duplicates: 1 ('evt-conflict-1' appears twice).
    - Database-level duplicate: 1 ('evt-conflict-1' already in DB).
    - Newly inserted: 1 ('evt-new-2').

    Verifications:
    - inserted_count == 1
    - duplicate_count == 2 (1 in-batch + 1 DB-level)
    - inserted_events contains strictly ['evt-new-2']
    - Unpacking (ins, dup) equals (1, 2)
    - Consistency: inserted_count + duplicate_count == 3 == len(batch)
    - Total DB rows == 2
    """
    writer = DatabaseWriter(test_db_manager)
    writer.start_run("run-pre", ["pre.log"])
    # Pre-populate database with an existing record
    pre_ev = LogEventRecord(
        event_id="evt-conflict-1",
        timestamp=datetime.now(timezone.utc),
        severity=CanonicalSeverity.INFO,
        service="api",
        endpoint="/test",
        http_status=200,
        latency_ms=10.0,
        message="pre-existing",
        source_file="pre.log",
        source_line=1,
        run_id="run-pre",
    )
    ins_pre, _ = writer.write_events_batch([pre_ev])
    assert ins_pre == 1

    # Now prepare incoming batch with an in-batch duplicate AND a DB conflict
    writer.start_run("run-new", ["new.log"])
    dup_ev = LogEventRecord(
        event_id="evt-conflict-1",
        timestamp=datetime.now(timezone.utc),
        severity=CanonicalSeverity.INFO,
        service="api",
        endpoint="/test",
        http_status=200,
        latency_ms=10.0,
        message="conflict",
        source_file="new.log",
        source_line=1,
        run_id="run-new",
    )
    new_ev = LogEventRecord(
        event_id="evt-new-2",
        timestamp=datetime.now(timezone.utc),
        severity=CanonicalSeverity.INFO,
        service="api",
        endpoint="/test",
        http_status=200,
        latency_ms=20.0,
        message="new",
        source_file="new.log",
        source_line=2,
        run_id="run-new",
    )

    batch = [dup_ev, dup_ev, new_ev]
    result = writer.write_events_batch(batch)

    # 1. Verify backwards-compatible tuple unpacking
    ins, dup = result
    assert ins == 1
    assert dup == 2

    # 2. Verify BatchWriteResult fields
    assert result.inserted_count == 1
    assert result.duplicate_count == 2
    assert len(result.inserted_events) == 1
    assert result.inserted_events[0].event_id == "evt-new-2"

    # 3. Verify exact accounting: inserted + duplicates == input
    assert result.inserted_count + result.duplicate_count == len(batch)

    # 4. Verify DB table state
    with test_db_manager.session_scope() as session:
        stored_ids = set(session.scalars(select(LogEventModel.event_id)).all())
        assert stored_ids == {"evt-conflict-1", "evt-new-2"}


def test_pipeline_reconciliation_with_db_level_conflict(tmp_path):
    """Verify pipeline end-to-end reconciliation with DB-level uniqueness conflicts."""
    db_path = tmp_path / "conflict_pipeline.db"
    cfg = PipelineConfig(
        database=DatabaseConfig(url=f"sqlite:///{db_path}"),
        input_dir=tmp_path / "input",
        quarantine_dir=tmp_path / "quarantine",
        reports_dir=tmp_path / "reports",
        batch_size=100,
    )
    pipeline = LogAnalyticsPipeline(config=cfg)

    # 1. Pre-seed DB with evt-existing
    seed_file = _create_jsonl(
        tmp_path / "seed.jsonl",
        [
            {
                "event_id": "evt-existing",
                "timestamp": "2026-10-28T12:00:00Z",
                "severity": "INFO",
                "service": "auth",
                "endpoint": "/login",
                "http_status": 200,
                "latency_ms": 10.0,
                "message": "seed",
            }
        ],
    )
    res_seed = pipeline.run(files=[seed_file], run_id="run-seed")
    assert res_seed["valid_count"] == 1

    # 2. New file has duplicate of evt-existing twice and 1 fresh record
    incoming_file = _create_jsonl(
        tmp_path / "incoming.jsonl",
        [
            {
                "event_id": "evt-existing",
                "timestamp": "2026-10-28T12:00:00Z",
                "severity": "INFO",
                "service": "auth",
                "endpoint": "/login",
                "http_status": 200,
                "latency_ms": 10.0,
                "message": "dupe1",
            },
            {
                "event_id": "evt-existing",
                "timestamp": "2026-10-28T12:00:00Z",
                "severity": "INFO",
                "service": "auth",
                "endpoint": "/login",
                "http_status": 200,
                "latency_ms": 10.0,
                "message": "dupe2",
            },
            {
                "event_id": "evt-fresh",
                "timestamp": "2026-10-28T12:00:00Z",
                "severity": "ERROR",
                "service": "auth",
                "endpoint": "/login",
                "http_status": 500,
                "latency_ms": 80.0,
                "message": "fresh",
            },
        ],
    )
    res = pipeline.run(files=[incoming_file], run_id="run-conflict-test")

    # Verify reconciliation equation and counts
    assert res["input_count"] == 3
    assert res["valid_count"] == 1
    assert res["duplicate_count"] == 2
    assert res["rejected_count"] == 0
    assert res["reconciliation"]["is_balanced"] is True
    assert res["reconciliation"]["discrepancy"] == 0

    # Published report must only include evt-fresh
    assert len(res["service_metrics"]) == 1
    sm = res["service_metrics"][0]
    assert sm["service"] == "auth"
    assert sm["total_events"] == 1
    assert sm["error_count"] == 1
    assert sm["error_rate_pct"] == 100.0
    assert sm["avg_latency_ms"] == 80.0
