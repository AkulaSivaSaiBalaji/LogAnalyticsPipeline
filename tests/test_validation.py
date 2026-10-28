"""Unit tests for record validation and quarantine handling."""

from pathlib import Path
import pytest

from src.log_pipeline.parsers.base import RawLogRecord
from src.log_pipeline.validation.validator import RecordValidator
from src.log_pipeline.validation.quarantine import QuarantineHandler
from src.log_pipeline.models import CanonicalSeverity


def test_validator_happy_path():
    """Verify clean record passes validation and populates LogEventRecord."""
    validator = RecordValidator(run_id="run-101")
    raw = RawLogRecord(
        source_file="sample.jsonl",
        source_line=1,
        raw_text='{"timestamp": "2026-10-28T12:00:00Z", "severity": "info", "service": "api", "message": "all good"}',
        parsed_dict={
            "timestamp": "2026-10-28T12:00:00Z",
            "severity": "info",
            "service": "api",
            "message": "all good",
            "endpoint": "/v1/test",
            "http_status": 200,
            "latency_ms": 15.0,
        },
    )
    result = validator.validate(raw)
    assert result.is_valid is True
    assert result.valid_record is not None
    assert result.rejected_record is None
    assert result.valid_record.severity == CanonicalSeverity.INFO
    assert result.valid_record.source_file == "sample.jsonl"
    assert result.valid_record.source_line == 1
    assert result.valid_record.run_id == "run-101"


def test_validator_missing_required_fields():
    """Verify missing required fields route record to quarantine."""
    validator = RecordValidator(run_id="run-101")
    raw = RawLogRecord(
        source_file="sample.jsonl",
        source_line=2,
        raw_text='{"timestamp": "2026-10-28T12:00:00Z"}',
        parsed_dict={"timestamp": "2026-10-28T12:00:00Z"},
    )
    result = validator.validate(raw)
    assert result.is_valid is False
    assert result.valid_record is None
    assert result.rejected_record is not None
    assert any("MISSING_REQUIRED_FIELD" in r for r in result.rejected_record.rejection_reasons)


def test_validator_invalid_timestamp():
    """Verify invalid timestamp format produces explicit rejection reason."""
    validator = RecordValidator(run_id="run-101")
    raw = RawLogRecord(
        source_file="sample.jsonl",
        source_line=3,
        raw_text='{"timestamp": "invalid-date", "severity": "INFO", "service": "srv", "message": "test"}',
        parsed_dict={
            "timestamp": "invalid-date",
            "severity": "INFO",
            "service": "srv",
            "message": "test",
        },
    )
    result = validator.validate(raw)
    assert result.is_valid is False
    assert any("timestamp" in r.lower() for r in result.rejected_record.rejection_reasons)


def test_validator_negative_latency_and_bad_status():
    """Verify negative latency and out-of-range status codes are quarantined."""
    validator = RecordValidator(run_id="run-101")
    raw = RawLogRecord(
        source_file="sample.jsonl",
        source_line=4,
        raw_text="...",
        parsed_dict={
            "timestamp": "2026-10-28T12:00:00Z",
            "severity": "INFO",
            "service": "srv",
            "message": "test",
            "latency_ms": -50.0,
            "http_status": 700,
        },
    )
    result = validator.validate(raw)
    assert result.is_valid is False
    assert result.rejected_record is not None
    assert any("Latency cannot be negative" in r for r in result.rejected_record.rejection_reasons)


def test_validator_parser_error_forwarded():
    """Verify parser syntax errors are directly quarantined."""
    validator = RecordValidator(run_id="run-101")
    raw = RawLogRecord(
        source_file="sample.jsonl",
        source_line=5,
        raw_text="CORRUPT_SYNTAX",
        parse_error="MALFORMED_JSON_LINE: JSONDecodeError",
    )
    result = validator.validate(raw)
    assert result.is_valid is False
    assert "MALFORMED_JSON_LINE" in result.rejected_record.rejection_reasons[0]


def test_quarantine_handler_export(tmp_path):
    """Verify quarantine handler writes JSONL records and tracks counts."""
    handler = QuarantineHandler(quarantine_dir=tmp_path, run_id="run-test-quarantine")
    validator = RecordValidator(run_id="run-test-quarantine")

    bad_raw = RawLogRecord(
        source_file="bad.log",
        source_line=10,
        raw_text="unparseable text",
        parse_error="UNRECOGNIZED_TEXT_LOG_FORMAT",
    )
    res = validator.validate(bad_raw)
    handler.add(res.rejected_record)

    assert handler.count == 1
    assert "UNRECOGNIZED_TEXT_LOG_FORMAT" in handler.reason_counts

    export_file = handler.export_to_disk()
    assert export_file.exists()
    content = export_file.read_text(encoding="utf-8")
    assert "unparseable text" in content
    assert "UNRECOGNIZED_TEXT_LOG_FORMAT" in content
