"""Unit tests for domain-specific logging edge cases and boundary conditions."""

from datetime import datetime, timezone
import pytest
import pandas as pd

from src.log_pipeline.models import (
    CanonicalSeverity,
    LogEventRecord,
    generate_deterministic_event_id,
    normalize_utc_timestamp,
    normalize_severity,
)
from src.log_pipeline.metrics.calculator import MetricsCalculator
from src.log_pipeline.parsers.base import RawLogRecord
from src.log_pipeline.validation.validator import RecordValidator


def test_edge_case_zero_latency():
    """Verify that zero latency (latency_ms = 0.0) is treated as valid non-null latency."""
    raw = {
        "timestamp": "2026-10-28T12:00:00Z",
        "severity": "INFO",
        "service": "cache-service",
        "endpoint": "/cache/get",
        "http_status": 200,
        "latency_ms": 0.0,
        "message": "Local memory cache hit in 0.0ms",
        "source_file": "cache.log",
        "source_line": 1,
        "run_id": "run-edge-01",
    }
    record = LogEventRecord(**raw)
    assert record.latency_ms == 0.0

    df = MetricsCalculator.events_to_dataframe([record])
    _, service_df = MetricsCalculator.calculate_aggregates(df)
    row = service_df.iloc[0]
    assert row["latency_count"] == 1
    assert row["avg_latency_ms"] == 0.0
    assert row["p50_latency_ms"] == 0.0


def test_edge_case_midnight_utc_rollover():
    """Verify timestamp hourly bucketing correctly splits across the midnight UTC boundary."""
    ts_before_midnight = datetime(2026, 10, 28, 23, 59, 59, 900000, tzinfo=timezone.utc)
    ts_after_midnight = datetime(2026, 10, 29, 0, 0, 0, 100000, tzinfo=timezone.utc)

    e1 = LogEventRecord(
        timestamp=ts_before_midnight,
        severity=CanonicalSeverity.INFO,
        service="gateway",
        message="Before midnight",
        source_file="gw.log",
        source_line=1,
        run_id="run-midnight",
    )
    e2 = LogEventRecord(
        timestamp=ts_after_midnight,
        severity=CanonicalSeverity.INFO,
        service="gateway",
        message="After midnight",
        source_file="gw.log",
        source_line=2,
        run_id="run-midnight",
    )

    df = MetricsCalculator.events_to_dataframe([e1, e2])
    hourly_df, _ = MetricsCalculator.calculate_aggregates(df)

    # Must produce two separate hour buckets: 2026-10-28 23:00:00 and 2026-10-29 00:00:00
    assert len(hourly_df) == 2
    hours = sorted(hourly_df["window_hour"].tolist())
    assert hours[0] == "2026-10-28 23:00:00"
    assert hours[1] == "2026-10-29 00:00:00"


def test_edge_case_retry_storm_distinct_lines():
    """Verify retry storms with identical messages and timestamps on distinct lines are preserved."""
    ts = datetime(2026, 10, 28, 14, 0, 0, tzinfo=timezone.utc)
    # Event 1: Initial failed attempt on line 100
    id1 = generate_deterministic_event_id(
        service="payment-service",
        endpoint="/api/v1/charge",
        timestamp=ts,
        source_file="payment.log",
        source_line=100,
        message="Downstream gateway timeout",
    )
    # Event 2: Immediate retry on line 101 with identical timestamp and message
    id2 = generate_deterministic_event_id(
        service="payment-service",
        endpoint="/api/v1/charge",
        timestamp=ts,
        source_file="payment.log",
        source_line=101,
        message="Downstream gateway timeout",
    )
    # Because they occur on separate source lines, both legitimate attempts are retained!
    assert id1 != id2


def test_edge_case_unexpected_extra_fields():
    """Verify records containing unexpected extra fields (e.g. trace_id, client_ip) pass validation."""
    validator = RecordValidator(run_id="run-extra")
    raw = RawLogRecord(
        source_file="api.jsonl",
        source_line=1,
        raw_text='{"timestamp": "2026-10-28T12:00:00Z", "severity": "INFO", "service": "api", "message": "ok", "trace_id": "tr-98124", "client_ip": "10.0.0.1"}',
        parsed_dict={
            "timestamp": "2026-10-28T12:00:00Z",
            "severity": "INFO",
            "service": "api",
            "message": "ok",
            "trace_id": "tr-98124",
            "client_ip": "10.0.0.1",
        },
    )
    result = validator.validate(raw)
    assert result.is_valid is True
    assert result.valid_record.service == "api"


def test_edge_case_whitespace_and_mixed_case_severity():
    """Verify leading/trailing whitespace in severity and mixed cases are normalized."""
    assert normalize_severity("  warning  ") == CanonicalSeverity.WARNING
    assert normalize_severity("\tcritical\n") == CanonicalSeverity.CRITICAL
    assert normalize_severity("DeBuG") == CanonicalSeverity.DEBUG
