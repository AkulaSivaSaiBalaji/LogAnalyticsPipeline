"""Tests for Correction C02: Conflicting Event ID Detection and Reconciliation Accounting.

Validates that:
1. An event_id with an identical payload is classified as a duplicate.
2. An event_id with a conflicting payload (different message, severity, status, latency)
   is classified as a conflict, never silently dropped as a duplicate.
3. In-batch identical duplicates vs in-batch conflicts are properly separated.
4. Concurrent database insert handling is race-safe and does not rely solely on pre-insert SELECT.
5. Reconciliation accounting strictly enforces:
   input_count = inserted_count + duplicate_count + conflict_count + rejected_count
"""

import json
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd
import pytest
from sqlalchemy import select

from src.log_pipeline.config import DatabaseConfig, PipelineConfig
from src.log_pipeline.db.connection import DatabaseManager
from src.log_pipeline.db.schema import LogEventModel, PipelineRunModel
from src.log_pipeline.db.writer import DatabaseWriter, BatchWriteResult
from src.log_pipeline.models import CanonicalSeverity, LogEventRecord
from src.log_pipeline.pipeline import LogAnalyticsPipeline


def _make_record(
    event_id: str,
    run_id: str = "run-c02",
    message: str = "success",
    severity: CanonicalSeverity = CanonicalSeverity.INFO,
    http_status: int = 200,
    latency_ms: float = 15.0,
    source_file: str = "app.log",
    source_line: int = 1,
    timestamp: datetime = None,
) -> LogEventRecord:
    ts = timestamp or datetime(2026, 10, 28, 12, 0, 0, tzinfo=timezone.utc)
    return LogEventRecord(
        event_id=event_id,
        run_id=run_id,
        timestamp=ts,
        severity=severity,
        service="order-service",
        endpoint="/orders",
        http_status=http_status,
        latency_ms=latency_ms,
        message=message,
        source_file=source_file,
        source_line=source_line,
    )


def test_identical_payload_classified_as_duplicate(test_db_manager):
    """Verify that an existing event_id with identical payload is classified as duplicate."""
    writer = DatabaseWriter(test_db_manager)
    writer.start_run("run-1", ["app.log"])

    ev1 = _make_record("evt-001", run_id="run-1", message="order placed")
    res1 = writer.write_events_batch([ev1])
    assert res1.inserted_count == 1
    assert res1.duplicate_count == 0
    assert res1.conflict_count == 0

    # Second batch in a new run with the exact same event payload
    writer.start_run("run-2", ["app.log"])
    ev2 = _make_record("evt-001", run_id="run-2", message="order placed")
    res2 = writer.write_events_batch([ev2])

    assert res2.inserted_count == 0
    assert res2.duplicate_count == 1
    assert res2.conflict_count == 0
    assert len(res2.conflicted_events) == 0

    # Backwards-compatible tuple unpacking: (inserted, dupes)
    ins, dup = res2
    assert ins == 0
    assert dup == 1


def test_conflicting_payload_message_classified_as_conflict(test_db_manager):
    """Verify that an existing event_id with different message is classified as conflict, not duplicate."""
    writer = DatabaseWriter(test_db_manager)
    writer.start_run("run-1", ["app.log"])

    ev_original = _make_record("evt-002", run_id="run-1", message="payment authorized")
    res1 = writer.write_events_batch([ev_original])
    assert res1.inserted_count == 1

    # Second event with same event_id but conflicting message
    writer.start_run("run-2", ["app.log"])
    ev_conflict = _make_record("evt-002", run_id="run-2", message="payment declined - fraud suspected")
    res2 = writer.write_events_batch([ev_conflict])

    assert res2.inserted_count == 0
    assert res2.duplicate_count == 0
    assert res2.conflict_count == 1
    assert len(res2.conflicted_events) == 1
    assert res2.conflicted_events[0][0].event_id == "evt-002"
    assert res2.conflicted_events[0][1] == "db_conflict"

    # Verify database was NOT overwritten with conflicting record
    with test_db_manager.session_scope() as session:
        stored = session.get(LogEventModel, "evt-002")
        assert stored.message == "payment authorized"


def test_conflicting_payload_severity_status_latency_timestamp(test_db_manager):
    """Verify differences in severity, status, latency, or timestamp trigger conflict classification."""
    writer = DatabaseWriter(test_db_manager)
    writer.start_run("run-base", ["base.log"])

    base_ev = _make_record("evt-multi-01", run_id="run-base", http_status=200, latency_ms=10.0)
    writer.write_events_batch([base_ev])

    # 1. Differing HTTP status
    writer.start_run("run-diff-status", ["status.log"])
    ev_status = _make_record("evt-multi-01", run_id="run-diff-status", http_status=500)
    res_status = writer.write_events_batch([ev_status])
    assert res_status.inserted_count == 0
    assert res_status.duplicate_count == 0
    assert res_status.conflict_count == 1

    # 2. Differing severity
    writer.start_run("run-diff-sev", ["sev.log"])
    ev_sev = _make_record("evt-multi-01", run_id="run-diff-sev", severity=CanonicalSeverity.ERROR)
    res_sev = writer.write_events_batch([ev_sev])
    assert res_sev.duplicate_count == 0
    assert res_sev.conflict_count == 1

    # 3. Differing latency
    writer.start_run("run-diff-lat", ["lat.log"])
    ev_lat = _make_record("evt-multi-01", run_id="run-diff-lat", latency_ms=999.0)
    res_lat = writer.write_events_batch([ev_lat])
    assert res_lat.duplicate_count == 0
    assert res_lat.conflict_count == 1

    # 4. Differing timestamp
    writer.start_run("run-diff-ts", ["ts.log"])
    ev_ts = _make_record(
        "evt-multi-01",
        run_id="run-diff-ts",
        timestamp=datetime(2026, 10, 29, 0, 0, 0, tzinfo=timezone.utc),
    )
    res_ts = writer.write_events_batch([ev_ts])
    assert res_ts.duplicate_count == 0
    assert res_ts.conflict_count == 1


def test_in_batch_identical_and_conflicting_events(test_db_manager):
    """Verify in-batch deduplication distinguishes identical duplicates from conflicting payloads."""
    writer = DatabaseWriter(test_db_manager)
    run_id = "run-batch-test"
    writer.start_run(run_id, ["batch.log"])

    ev_main = _make_record("evt-inbatch", run_id=run_id, message="original payload")
    ev_same = _make_record("evt-inbatch", run_id=run_id, message="original payload")
    ev_diff = _make_record("evt-inbatch", run_id=run_id, message="conflicting payload")

    # In-batch batch: [ev_main, ev_same, ev_diff]
    batch = [ev_main, ev_same, ev_diff]
    res = writer.write_events_batch(batch)

    assert res.inserted_count == 1
    assert res.duplicate_count == 1
    assert res.conflict_count == 1
    assert res.inserted_count + res.duplicate_count + res.conflict_count == len(batch)
    assert len(res.conflicted_events) == 1
    assert res.conflicted_events[0][1] == "in_batch_conflict"


def test_concurrent_insert_safety_simulation(test_db_manager):
    """Verify that writer safely classifies uninserted events post-insert without relying on pre-insert SELECT."""
    writer = DatabaseWriter(test_db_manager)
    writer.start_run("run-worker-a", ["worker_a.log"])
    writer.start_run("run-worker-b", ["worker_b.log"])

    # Worker A commits evt-race
    worker_a_ev = _make_record("evt-race", run_id="run-worker-a", message="worker A committed")
    res_a = writer.write_events_batch([worker_a_ev])
    assert res_a.inserted_count == 1

    # Worker B tries to insert evt-race with conflicting payload
    worker_b_ev = _make_record("evt-race", run_id="run-worker-b", message="worker B conflicting payload")
    # write_events_batch uses INSERT ... ON CONFLICT DO NOTHING (or IGNORE) and post-insert classification
    res_b = writer.write_events_batch([worker_b_ev])

    assert res_b.inserted_count == 0
    assert res_b.duplicate_count == 0
    assert res_b.conflict_count == 1
    assert res_b.conflicted_events[0][0].message == "worker B conflicting payload"


def test_pipeline_end_to_end_reconciliation_accounting_with_conflicts(tmp_path):
    """Verify end-to-end reconciliation accounting with new, duplicate, conflicting, and rejected events."""
    db_path = tmp_path / "c02_pipeline.db"
    cfg = PipelineConfig(
        database=DatabaseConfig(url=f"sqlite:///{db_path}"),
        input_dir=tmp_path / "input",
        quarantine_dir=tmp_path / "quarantine",
        reports_dir=tmp_path / "reports",
        batch_size=50,
    )
    pipeline = LogAnalyticsPipeline(config=cfg)

    # 1. Seed database with an existing record
    seed_file = tmp_path / "seed.jsonl"
    with open(seed_file, "w", encoding="utf-8") as f:
        f.write(
            json.dumps(
                {
                    "event_id": "seed-01",
                    "timestamp": "2026-10-28T12:00:00Z",
                    "severity": "INFO",
                    "service": "order-service",
                    "endpoint": "/orders",
                    "http_status": 200,
                    "latency_ms": 10.0,
                    "message": "seed message",
                }
            )
            + "\n"
        )
    res_seed = pipeline.run(files=[seed_file], run_id="run-seed")
    assert res_seed["status"] == "SUCCESS"
    assert res_seed["valid_count"] == 1

    # 2. Incoming run containing:
    # - 1 fresh valid event (fresh-01)
    # - 1 identical duplicate of seed-01
    # - 1 conflicting payload for seed-01 (different message)
    # - 1 malformed rejected record
    incoming_file = tmp_path / "incoming.jsonl"
    with open(incoming_file, "w", encoding="utf-8") as f:
        # Fresh event
        f.write(
            json.dumps(
                {
                    "event_id": "fresh-01",
                    "timestamp": "2026-10-28T12:00:00Z",
                    "severity": "INFO",
                    "service": "order-service",
                    "endpoint": "/orders",
                    "http_status": 200,
                    "latency_ms": 25.0,
                    "message": "fresh message",
                }
            )
            + "\n"
        )
        # Identical duplicate of seed-01
        f.write(
            json.dumps(
                {
                    "event_id": "seed-01",
                    "timestamp": "2026-10-28T12:00:00Z",
                    "severity": "INFO",
                    "service": "order-service",
                    "endpoint": "/orders",
                    "http_status": 200,
                    "latency_ms": 10.0,
                    "message": "seed message",
                }
            )
            + "\n"
        )
        # Conflicting payload for seed-01
        f.write(
            json.dumps(
                {
                    "event_id": "seed-01",
                    "timestamp": "2026-10-28T12:00:00Z",
                    "severity": "ERROR",
                    "service": "order-service",
                    "endpoint": "/orders",
                    "http_status": 500,
                    "latency_ms": 90.0,
                    "message": "conflicting seed payload",
                }
            )
            + "\n"
        )
        # Malformed invalid line
        f.write('{"invalid_json": true, missing_field\n')

    res = pipeline.run(files=[incoming_file], run_id="run-c02-verify")

    # Reconciliation verifications
    assert res["status"] == "PARTIAL_SUCCESS"  # rejected > 0 or conflict > 0
    assert res["input_count"] == 4
    assert res["valid_count"] == 1
    assert res["inserted_count"] == 1
    assert res["duplicate_count"] == 1
    assert res["conflict_count"] == 1
    assert res["rejected_count"] == 1

    # Exact reconciliation invariant
    recon = res["reconciliation"]
    assert recon["is_balanced"] is True
    assert recon["discrepancy"] == 0
    assert (
        recon["input_count"]
        == recon["valid_count"]
        + recon["duplicate_count"]
        + recon["conflict_count"]
        + recon["rejected_count"]
    )
    assert recon["accounted_total"] == 4

    # Verify reconciliation CSV report on disk
    recon_df = pd.read_csv(res["reconciliation_report"])
    assert "conflict_count" in recon_df.columns
    assert recon_df.iloc[0]["conflict_count"] == 1
    assert recon_df.iloc[0]["duplicate_count"] == 1
    assert recon_df.iloc[0]["valid_count"] == 1
    assert recon_df.iloc[0]["rejected_count"] == 1
    assert recon_df.iloc[0]["input_count"] == 4
