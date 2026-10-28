"""Unit tests for models, canonical normalization, and validation rules."""

from datetime import datetime, timezone
import pytest
from pydantic import ValidationError

from src.log_pipeline.models import (
    CanonicalSeverity,
    LogEventRecord,
    RejectedRecord,
    PipelineRunRecord,
    normalize_severity,
    normalize_utc_timestamp,
    generate_deterministic_event_id,
)


def test_severity_normalization():
    """Verify alias mapping and canonical severity enforcement."""
    assert normalize_severity("INFO") == CanonicalSeverity.INFO
    assert normalize_severity("info") == CanonicalSeverity.INFO
    assert normalize_severity("WARN") == CanonicalSeverity.WARNING
    assert normalize_severity("warning") == CanonicalSeverity.WARNING
    assert normalize_severity("ERR") == CanonicalSeverity.ERROR
    assert normalize_severity("FATAL") == CanonicalSeverity.CRITICAL
    assert normalize_severity("trace") == CanonicalSeverity.DEBUG

    with pytest.raises(ValueError, match="Unrecognized severity"):
        normalize_severity("NOT_A_REAL_SEVERITY")

    with pytest.raises(ValueError, match="Severity is empty"):
        normalize_severity("   ")

    with pytest.raises(ValueError, match="Severity is missing or null"):
        normalize_severity(None)


def test_timestamp_normalization():
    """Verify UTC conversion, naive assumptions, and epoch conversion."""
    # Standard ISO string with Z
    dt1 = normalize_utc_timestamp("2026-10-28T12:00:00Z")
    assert dt1.tzinfo == timezone.utc
    assert dt1.year == 2026 and dt1.hour == 12

    # Timezone offset conversion (e.g. +02:00 -> UTC 10:00)
    dt2 = normalize_utc_timestamp("2026-10-28T12:00:00+02:00")
    assert dt2.tzinfo == timezone.utc
    assert dt2.hour == 10

    # Timezone-naive string assumed UTC
    dt3 = normalize_utc_timestamp("2026-10-28 15:30:00")
    assert dt3.tzinfo == timezone.utc
    assert dt3.hour == 15

    # Epoch seconds
    epoch_sec = 1700000000
    dt4 = normalize_utc_timestamp(epoch_sec)
    assert dt4.tzinfo == timezone.utc

    # Invalid timestamp
    with pytest.raises(ValueError, match="Unable to parse timestamp"):
        normalize_utc_timestamp("not-a-timestamp-string")


def test_deterministic_event_id():
    """Verify event ID hashing satisfies idempotency and distinctness."""
    ts = datetime(2026, 10, 28, 12, 0, 0, tzinfo=timezone.utc)
    id1 = generate_deterministic_event_id(
        service="auth",
        endpoint="/login",
        timestamp=ts,
        source_file="app.log",
        source_line=10,
        message="User logged in",
    )
    id2 = generate_deterministic_event_id(
        service="auth",
        endpoint="/login",
        timestamp=ts,
        source_file="app.log",
        source_line=10,
        message="User logged in",
    )
    # Identical input produces identical ID (idempotency)
    assert id1 == id2
    assert len(id1) == 32

    # Different line produces different ID (legitimate duplicate message preserved)
    id3 = generate_deterministic_event_id(
        service="auth",
        endpoint="/login",
        timestamp=ts,
        source_file="app.log",
        source_line=11,
        message="User logged in",
    )
    assert id1 != id3


def test_log_event_record_valid(sample_valid_record_dict):
    """Verify construction and validation of a valid LogEventRecord."""
    record = LogEventRecord(**sample_valid_record_dict)
    assert record.event_id == "test-evt-001"
    assert record.severity == CanonicalSeverity.INFO
    assert record.service == "checkout-service"
    assert record.latency_ms == 120.5
    assert record.http_status == 200


def test_log_event_record_auto_event_id():
    """Verify auto-generation of deterministic event_id when omitted."""
    raw = {
        "timestamp": "2026-10-28T12:00:00Z",
        "severity": "WARN",
        "service": "checkout-service",
        "endpoint": "/api/v1/checkout",
        "http_status": 400,
        "latency_ms": 45.0,
        "message": "Invalid coupon code",
        "source_file": "app.log",
        "source_line": 5,
        "run_id": "run-test-002",
    }
    record = LogEventRecord(**raw)
    assert record.event_id is not None
    assert len(record.event_id) == 32
    assert record.severity == CanonicalSeverity.WARNING


def test_log_event_negative_latency_rejected(sample_valid_record_dict):
    """Verify negative latency triggers validation failure."""
    invalid_data = sample_valid_record_dict.copy()
    invalid_data["latency_ms"] = -5.0
    with pytest.raises(ValidationError) as exc_info:
        LogEventRecord(**invalid_data)
    assert "Latency cannot be negative" in str(exc_info.value)


def test_log_event_invalid_http_status(sample_valid_record_dict):
    """Verify invalid HTTP status triggers validation failure."""
    invalid_data = sample_valid_record_dict.copy()
    invalid_data["http_status"] = 99
    with pytest.raises(ValidationError):
        LogEventRecord(**invalid_data)

    invalid_data["http_status"] = 600
    with pytest.raises(ValidationError):
        LogEventRecord(**invalid_data)


def test_log_event_empty_service(sample_valid_record_dict):
    """Verify empty service string triggers validation failure."""
    invalid_data = sample_valid_record_dict.copy()
    invalid_data["service"] = "   "
    with pytest.raises(ValidationError):
        LogEventRecord(**invalid_data)


def test_rejected_record_model():
    """Verify construction of RejectedRecord quarantine model."""
    rr = RejectedRecord(
        run_id="run-001",
        source_file="bad_logs.log",
        source_line=4,
        raw_record='{"bad": "json"',
        rejection_reasons=["JSON_PARSE_ERROR: Unterminated string"],
    )
    assert rr.run_id == "run-001"
    assert len(rr.rejection_reasons) == 1
    assert rr.created_at.tzinfo == timezone.utc


def test_pipeline_run_record():
    """Verify construction of PipelineRunRecord accounting model."""
    run = PipelineRunRecord(
        run_id="run-001",
        start_time=datetime.now(timezone.utc),
        input_files=["data/sample/sample_logs.json"],
        input_count=100,
        valid_count=95,
        rejected_count=5,
        duplicate_count=0,
    )
    assert run.input_count == run.valid_count + run.rejected_count + run.duplicate_count
