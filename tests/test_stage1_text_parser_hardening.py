"""Stage 1B-2 regression tests: Text-parser hardening.

Verifies:
1. Malformed latency values (e.g., '1.2.3ms') do not raise uncaught ValueError and are quarantined.
2. Negative latency values (e.g., '-12.5ms') are rejected and not accepted as valid.
3. Non-finite latency values ('NaN', 'inf', '-inf') are rejected and not accepted as valid.
4. Valid existing log formats (standard HTTP, inline latency in general logs, zero latency) continue to parse correctly.
5. Malformed lines do not terminate processing of subsequent valid lines in the same file.
6. Pipeline end-to-end run reconciles counts (is_balanced == True) and exports rejected records.
7. LogEventRecord model rejects non-finite and negative latency.
"""

from __future__ import annotations

import math
from datetime import datetime, timezone
from pathlib import Path
import pytest
from sqlalchemy import select, func

from src.log_pipeline.config import PipelineConfig, DatabaseConfig
from src.log_pipeline.db.schema import LogEventModel, RejectedRecordModel
from src.log_pipeline.models import CanonicalSeverity, LogEventRecord
from src.log_pipeline.parsers.text_parser import TextLogParser, parse_latency
from src.log_pipeline.pipeline import LogAnalyticsPipeline


def test_parse_latency_unit():
    """Verify parse_latency helper behavior across valid, malformed, negative, and non-finite inputs."""
    # Valid nonnegative numbers
    assert parse_latency("12.5") == (12.5, None)
    assert parse_latency("0") == (0.0, None)
    assert parse_latency("0.0") == (0.0, None)
    assert parse_latency("-0.0") == (0.0, None)
    assert parse_latency(" 1500 ") == (1500.0, None)

    # Malformed formats
    val, err = parse_latency("1.2.3")
    assert val is None
    assert err is not None and "MALFORMED_LATENCY" in err

    val, err = parse_latency("abc")
    assert val is None
    assert err is not None and "MALFORMED_LATENCY" in err

    # Negative numbers
    val, err = parse_latency("-12.5")
    assert val is None
    assert err is not None and "NEGATIVE_LATENCY" in err

    val, err = parse_latency("-0.001")
    assert val is None
    assert err is not None and "NEGATIVE_LATENCY" in err

    # Non-finite values
    for nf in ["NaN", "nan", "inf", "+inf", "-inf", "Infinity", "-Infinity"]:
        val, err = parse_latency(nf)
        assert val is None, f"Expected None for {nf}"
        assert err is not None and "INVALID_LATENCY" in err, f"Expected INVALID_LATENCY for {nf}"


def test_model_latency_validation():
    """Verify LogEventRecord field validator enforces finite, nonnegative latency."""
    base_dict = {
        "event_id": "test-event-123",
        "timestamp": datetime.now(timezone.utc),
        "severity": CanonicalSeverity.INFO,
        "service": "api-service",
        "message": "request processed",
        "source_file": "app.log",
        "source_line": 1,
        "run_id": "run-test",
    }

    # None and nonnegative values are valid
    rec_none = LogEventRecord(**base_dict, latency_ms=None)
    assert rec_none.latency_ms is None

    rec_zero = LogEventRecord(**base_dict, latency_ms=0.0)
    assert rec_zero.latency_ms == 0.0

    rec_valid = LogEventRecord(**base_dict, latency_ms=42.5)
    assert rec_valid.latency_ms == 42.5

    # Negative latency must raise ValueError
    with pytest.raises(ValueError, match="negative"):
        LogEventRecord(**base_dict, latency_ms=-1.0)

    # Non-finite latencies must raise ValueError
    with pytest.raises(ValueError, match="finite"):
        LogEventRecord(**base_dict, latency_ms=float("nan"))

    with pytest.raises(ValueError, match="finite"):
        LogEventRecord(**base_dict, latency_ms=float("inf"))

    with pytest.raises(ValueError, match="finite"):
        LogEventRecord(**base_dict, latency_ms=float("-inf"))


def test_text_parser_various_latency_formats(tmp_path: Path):
    """Verify TextLogParser handles malformed, negative, NaN/inf, and valid HTTP log entries."""
    log_content = (
        # Line 1: valid HTTP with hyphen
        "2026-10-28 12:00:10.100 [INFO] [auth-service] GET /api/v1/session 200 12.5ms - Session ok\n"
        # Line 2: valid HTTP with space message
        "2026-10-28 12:00:11.200 [INFO] [auth-service] POST /api/v1/login 200 35.0ms Login successful\n"
        # Line 3: valid HTTP with no trailing message
        "2026-10-28 12:00:12.300 [INFO] [auth-service] GET /api/v1/health 200 5.0ms\n"
        # Line 4: valid HTTP with zero latency
        "2026-10-28 12:00:13.400 [INFO] [auth-service] GET /api/v1/cached 200 0ms\n"
        # Line 5: malformed latency with multiple dots (1.2.3ms)
        "2026-10-28 12:00:14.500 [INFO] [auth-service] GET /api/v1/bad 200 1.2.3ms - Malformed\n"
        # Line 6: negative latency (-15.5ms)
        "2026-10-28 12:00:15.600 [INFO] [auth-service] GET /api/v1/neg 200 -15.5ms - Negative\n"
        # Line 7: non-finite NaN latency (NaNms)
        "2026-10-28 12:00:16.700 [INFO] [auth-service] GET /api/v1/nan 200 NaNms - Nan latency\n"
        # Line 8: non-finite inf latency (infms)
        "2026-10-28 12:00:17.800 [INFO] [auth-service] GET /api/v1/inf 200 infms - Inf latency\n"
        # Line 9: valid general log with inline latency
        "2026-10-28 12:00:18.900 [INFO] [analytics-worker] Batch completed in 1500ms\n"
        # Line 10: general log with malformed inline latency
        "2026-10-28 12:00:19.000 [INFO] [analytics-worker] Batch completed in 1.2.3ms\n"
        # Line 11: general log with negative inline latency
        "2026-10-28 12:00:20.100 [INFO] [analytics-worker] Task failed with latency: -50ms\n"
        # Line 12: general log without latency
        "2026-10-28 12:00:21.200 [WARN] [db-worker] Pool connection idle\n"
    )
    log_file = tmp_path / "hardening.log"
    log_file.write_text(log_content, encoding="utf-8")

    parser = TextLogParser()
    records = list(parser.parse_file(log_file))
    assert len(records) == 12

    # Line 1: valid
    assert records[0].source_line == 1
    assert records[0].parse_error is None
    assert records[0].parsed_dict["latency_ms"] == 12.5

    # Line 2: valid
    assert records[1].source_line == 2
    assert records[1].parse_error is None
    assert records[1].parsed_dict["latency_ms"] == 35.0

    # Line 3: valid (no trailing message)
    assert records[2].source_line == 3
    assert records[2].parse_error is None
    assert records[2].parsed_dict["latency_ms"] == 5.0
    assert records[2].parsed_dict["message"] == "GET /api/v1/health"

    # Line 4: valid (zero latency)
    assert records[3].source_line == 4
    assert records[3].parse_error is None
    assert records[3].parsed_dict["latency_ms"] == 0.0

    # Line 5: malformed 1.2.3ms
    assert records[4].source_line == 5
    assert records[4].parse_error is not None
    assert "MALFORMED_LATENCY" in records[4].parse_error
    assert records[4].parsed_dict is None

    # Line 6: negative -15.5ms
    assert records[5].source_line == 6
    assert records[5].parse_error is not None
    assert "NEGATIVE_LATENCY" in records[5].parse_error
    assert records[5].parsed_dict is None

    # Line 7: NaNms
    assert records[6].source_line == 7
    assert records[6].parse_error is not None
    assert "INVALID_LATENCY" in records[6].parse_error
    assert records[6].parsed_dict is None

    # Line 8: infms
    assert records[7].source_line == 8
    assert records[7].parse_error is not None
    assert "INVALID_LATENCY" in records[7].parse_error
    assert records[7].parsed_dict is None

    # Line 9: general log with valid inline latency
    assert records[8].source_line == 9
    assert records[8].parse_error is None
    assert records[8].parsed_dict["latency_ms"] == 1500.0

    # Line 10: general log with malformed inline latency
    assert records[9].source_line == 10
    assert records[9].parse_error is not None
    assert "MALFORMED_LATENCY" in records[9].parse_error
    assert records[9].parsed_dict is None

    # Line 11: general log with negative inline latency
    assert records[10].source_line == 11
    assert records[10].parse_error is not None
    assert "NEGATIVE_LATENCY" in records[10].parse_error
    assert records[10].parsed_dict is None

    # Line 12: general log without latency
    assert records[11].source_line == 12
    assert records[11].parse_error is None
    assert records[11].parsed_dict["latency_ms"] is None


def test_pipeline_end_to_end_with_malformed_text_logs(tmp_path: Path):
    """Verify full pipeline execution reconciles counts and quarantines invalid text log lines."""
    input_dir = tmp_path / "inputs"
    quarantine_dir = tmp_path / "quarantine"
    reports_dir = tmp_path / "reports"
    for d in (input_dir, quarantine_dir, reports_dir):
        d.mkdir(parents=True, exist_ok=True)

    db_path = tmp_path / "pipeline_test.db"

    config = PipelineConfig(
        database=DatabaseConfig(url=f"sqlite:///{db_path}"),
        input_dir=input_dir,
        quarantine_dir=quarantine_dir,
        reports_dir=reports_dir,
        batch_size=100,
    )

    # 3 valid events (2 HTTP, 1 general) and 3 rejected events (1 malformed, 1 negative, 1 NaN)
    log_content = (
        "2026-10-28 12:00:01.000 [INFO] [api] GET /users 200 10.0ms - Ok\n"
        "2026-10-28 12:00:02.000 [INFO] [api] GET /users 200 1.2.3ms - Bad\n"
        "2026-10-28 12:00:03.000 [INFO] [api] GET /users 200 -50.0ms - Negative\n"
        "2026-10-28 12:00:04.000 [INFO] [api] GET /users 200 NaNms - NaN\n"
        "2026-10-28 12:00:05.000 [INFO] [api] GET /orders 200 20.0ms - Ok 2\n"
        "2026-10-28 12:00:06.000 [INFO] [worker] Process finished in 30.0ms\n"
    )
    test_file = input_dir / "service.log"
    test_file.write_text(log_content, encoding="utf-8")

    pipeline = LogAnalyticsPipeline(config=config)
    res = pipeline.run(files=[test_file], run_id="run-text-hardening")

    # Reconciliation asserts
    assert res["input_count"] == 6
    assert res["valid_count"] == 3
    assert res["rejected_count"] == 3
    assert res["duplicate_count"] == 0
    assert res["status"] == "PARTIAL_SUCCESS"
    assert res["reconciliation"]["is_balanced"] is True
    assert res["reconciliation"]["discrepancy"] == 0

    # Verify quarantine export exists and contains 3 records
    assert res["quarantine_file"] is not None
    quarantine_path = Path(res["quarantine_file"])
    assert quarantine_path.exists()
    quarantine_lines = [ln.strip() for ln in quarantine_path.read_text(encoding="utf-8").splitlines() if ln.strip()]
    assert len(quarantine_lines) == 3

    # Check database records
    with pipeline.db.session_scope() as session:
        db_events = session.scalars(select(LogEventModel)).all()
        assert len(db_events) == 3
        latencies = sorted([e.latency_ms for e in db_events])
        assert latencies == [10.0, 20.0, 30.0]

        db_rejected = session.scalars(select(RejectedRecordModel)).all()
        assert len(db_rejected) == 3
        reasons = [r.rejection_reasons for r in db_rejected]
        assert any("MALFORMED_LATENCY" in r for r in reasons)
        assert any("NEGATIVE_LATENCY" in r for r in reasons)
        assert any("INVALID_LATENCY" in r for r in reasons)
