"""Unit and integration tests for metrics aggregation, reporting, and reconciliation."""

from datetime import datetime, timezone
from pathlib import Path
import pandas as pd
import pytest

from src.log_pipeline.metrics.calculator import MetricsCalculator, is_error_event
from src.log_pipeline.models import CanonicalSeverity, LogEventRecord
from src.log_pipeline.pipeline import LogAnalyticsPipeline
from src.log_pipeline.config import PipelineConfig, DatabaseConfig


def test_is_error_event_logic():
    """Verify error classification business rules."""
    # 200 OK INFO -> Not an error
    assert is_error_event("INFO", 200) is False
    # 404 Not Found WARN -> Not a server error
    assert is_error_event("WARNING", 404) is False
    # 500 Internal Error INFO -> Server Error
    assert is_error_event("INFO", 500) is True
    # 503 Service Unavailable -> Server Error
    assert is_error_event("INFO", 503) is True
    # ERROR severity with 200 -> Error
    assert is_error_event("ERROR", 200) is True
    # CRITICAL severity with None -> Error
    assert is_error_event("CRITICAL", None) is True


def test_metrics_on_known_fixture():
    """Verify metrics calculation against an independently verified known fixture.

    Fixture dataset:
    - Service: auth-service
      - Event 1: 10:00 UTC, 200 OK, latency 10.0 ms, INFO
      - Event 2: 10:15 UTC, 200 OK, latency 20.0 ms, INFO
      - Event 3: 10:30 UTC, 500 Err, latency 30.0 ms, ERROR
      - Event 4: 10:45 UTC, 200 OK, latency 40.0 ms, INFO
      - Event 5: 10:50 UTC, 200 OK, latency None,  INFO
    Independent Expected Values:
      Total Events: 5
      Error Count: 1 (Event 3)
      Error Rate: 1 / 5 = 20.0%
      Latency Count: 4 (10, 20, 30, 40)
      Avg Latency: (10 + 20 + 30 + 40) / 4 = 25.0 ms
      p50 Latency (median): 25.0 ms
      p95 Latency: 38.5 ms (linear quantile: 10 + 0.95*30)
      p99 Latency: 39.7 ms
    """
    ts_base = datetime(2026, 10, 28, 10, 0, 0, tzinfo=timezone.utc)
    events = [
        LogEventRecord(
            event_id="e1",
            timestamp=ts_base,
            severity=CanonicalSeverity.INFO,
            service="auth-service",
            endpoint="/login",
            http_status=200,
            latency_ms=10.0,
            message="m1",
            source_file="test.log",
            source_line=1,
            run_id="run-known",
        ),
        LogEventRecord(
            event_id="e2",
            timestamp=ts_base,
            severity=CanonicalSeverity.INFO,
            service="auth-service",
            endpoint="/login",
            http_status=200,
            latency_ms=20.0,
            message="m2",
            source_file="test.log",
            source_line=2,
            run_id="run-known",
        ),
        LogEventRecord(
            event_id="e3",
            timestamp=ts_base,
            severity=CanonicalSeverity.ERROR,
            service="auth-service",
            endpoint="/login",
            http_status=500,
            latency_ms=30.0,
            message="m3",
            source_file="test.log",
            source_line=3,
            run_id="run-known",
        ),
        LogEventRecord(
            event_id="e4",
            timestamp=ts_base,
            severity=CanonicalSeverity.INFO,
            service="auth-service",
            endpoint="/login",
            http_status=200,
            latency_ms=40.0,
            message="m4",
            source_file="test.log",
            source_line=4,
            run_id="run-known",
        ),
        LogEventRecord(
            event_id="e5",
            timestamp=ts_base,
            severity=CanonicalSeverity.INFO,
            service="auth-service",
            endpoint="/login",
            http_status=200,
            latency_ms=None,
            message="m5",
            source_file="test.log",
            source_line=5,
            run_id="run-known",
        ),
    ]

    df = MetricsCalculator.events_to_dataframe(events)
    hourly_df, service_df = MetricsCalculator.calculate_aggregates(df)

    assert len(service_df) == 1
    row = service_df.iloc[0]
    assert row["service"] == "auth-service"
    assert row["total_events"] == 5
    assert row["error_count"] == 1
    assert row["error_rate_pct"] == 20.0
    assert row["latency_count"] == 4
    assert row["avg_latency_ms"] == 25.0
    assert row["p50_latency_ms"] == 25.0
    assert 38.0 <= row["p95_latency_ms"] <= 40.0


def test_missing_latency_handling():
    """Verify groups with zero latency entries have None percentiles rather than 0.0."""
    events = [
        LogEventRecord(
            event_id="nolat1",
            timestamp=datetime(2026, 10, 28, 12, 0, 0, tzinfo=timezone.utc),
            severity=CanonicalSeverity.INFO,
            service="worker",
            latency_ms=None,
            message="background task",
            source_file="w.log",
            source_line=1,
            run_id="run-nolat",
        )
    ]
    df = MetricsCalculator.events_to_dataframe(events)
    _, service_df = MetricsCalculator.calculate_aggregates(df)
    row = service_df.iloc[0]
    assert row["total_events"] == 1
    assert row["latency_count"] == 0
    assert row["avg_latency_ms"] is None or pd.isna(row["avg_latency_ms"])
    assert row["p50_latency_ms"] is None or pd.isna(row["p50_latency_ms"])


def test_end_to_end_pipeline_execution(tmp_path):
    """Verify complete end-to-end pipeline run on sample files with reconciliation."""
    db_path = tmp_path / "test_pipeline.db"
    cfg = PipelineConfig(
        database=DatabaseConfig(url=f"sqlite:///{db_path}"),
        input_dir=Path("data/sample"),
        quarantine_dir=tmp_path / "quarantine",
        reports_dir=tmp_path / "reports",
        batch_size=100,
    )

    pipeline = LogAnalyticsPipeline(config=cfg)

    # 1. First execution
    res1 = pipeline.run()
    assert res1["status"] in ("SUCCESS", "PARTIAL_SUCCESS")
    assert res1["input_count"] > 0
    assert res1["valid_count"] > 0
    assert res1["rejected_count"] > 0  # sample_logs.jsonl and sample_logs.log contain intentional bad lines
    assert res1["duplicate_count"] == 0

    recon1 = res1["reconciliation"]
    assert recon1["is_balanced"] is True
    assert recon1["discrepancy"] == 0
    assert recon1["input_count"] == recon1["valid_count"] + recon1["rejected_count"] + recon1["duplicate_count"]

    # Verify generated files on disk
    assert Path(res1["hourly_report"]).exists()
    assert Path(res1["service_report"]).exists()
    assert Path(res1["reconciliation_report"]).exists()
    if res1["quarantine_file"]:
        assert Path(res1["quarantine_file"]).exists()

    # 2. Second execution (Idempotent Rerun on exact same inputs)
    res2 = pipeline.run()
    assert res2["input_count"] == res1["input_count"]
    # All previously inserted records are recognized as duplicates!
    assert res2["valid_count"] == 0
    assert res2["duplicate_count"] == res1["valid_count"]
    recon2 = res2["reconciliation"]
    assert recon2["is_balanced"] is True
    assert recon2["discrepancy"] == 0
