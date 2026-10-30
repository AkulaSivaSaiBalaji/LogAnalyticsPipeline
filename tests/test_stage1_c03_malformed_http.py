"""Tests for Correction C03: Malformed HTTP Log Fallback Protection.

Verifies:
1. Valid HTTP access logs parse correctly with extracted endpoint, status, and latency.
2. Malformed HTTP access logs that fail strict HTTP format are detected and rejected/quarantined,
   rather than silently falling back to and being accepted by the general application log parser.
3. Valid general application logs continue to parse correctly with endpoint=None, http_status=None.
4. Pipeline end-to-end execution correctly routes malformed HTTP lines to quarantine
   and maintains exact reconciliation accounting.
"""

from pathlib import Path
import pytest
from sqlalchemy import select, func

from src.log_pipeline.config import DatabaseConfig, PipelineConfig
from src.log_pipeline.db.schema import LogEventModel, RejectedRecordModel
from src.log_pipeline.parsers.text_parser import TextLogParser
from src.log_pipeline.pipeline import LogAnalyticsPipeline


def test_valid_http_access_logs(tmp_path: Path):
    """Verify standard and variant valid HTTP access logs parse with full fields."""
    log_content = (
        # 1. Standard with method, endpoint, status, latency, message
        "2026-10-28 12:00:10.100 [INFO] [auth-service] GET /api/v1/session 200 12.5ms - Active session validated\n"
        # 2. POST with 201 Created and space separator message
        "2026-10-28 12:00:11.200 [INFO] [order-service] POST /api/v1/orders 201 45.0ms Order created successfully\n"
        # 3. No trailing message
        "2026-10-28 12:00:12.300 [INFO] [health-service] GET /health 200 2.0ms\n"
        # 4. Valid HTTP log without method (method is optional in HTTP pattern)
        "2026-10-28 12:00:13.400 [WARN] [gateway] /api/v1/cached 304 0ms - Cache not modified\n"
    )
    f = tmp_path / "valid_http.log"
    f.write_text(log_content, encoding="utf-8")

    parser = TextLogParser()
    records = list(parser.parse_file(f))
    assert len(records) == 4

    # Record 1
    assert records[0].parse_error is None
    d1 = records[0].parsed_dict
    assert d1["service"] == "auth-service"
    assert d1["endpoint"] == "/api/v1/session"
    assert d1["http_status"] == 200
    assert d1["latency_ms"] == 12.5
    assert d1["message"] == "Active session validated"

    # Record 2
    assert records[1].parse_error is None
    d2 = records[1].parsed_dict
    assert d2["service"] == "order-service"
    assert d2["endpoint"] == "/api/v1/orders"
    assert d2["http_status"] == 201
    assert d2["latency_ms"] == 45.0

    # Record 3
    assert records[2].parse_error is None
    d3 = records[2].parsed_dict
    assert d3["endpoint"] == "/health"
    assert d3["message"] == "GET /health"

    # Record 4
    assert records[3].parse_error is None
    d4 = records[3].parsed_dict
    assert d4["endpoint"] == "/api/v1/cached"
    assert d4["http_status"] == 304
    assert d4["latency_ms"] == 0.0


def test_malformed_http_access_logs_rejected_not_fallen_back(tmp_path: Path):
    """Verify malformed HTTP access logs are rejected and NOT silently accepted as general logs."""
    malformed_lines = [
        # Missing 'ms' unit on latency
        "2026-10-28 12:00:10.100 [INFO] [auth-service] GET /api/v1/session 200 12.5 - Active session validated\n",
        # 2-digit status code
        "2026-10-28 12:00:11.200 [INFO] [auth-service] GET /api/v1/session 20 12.5ms - Active session validated\n",
        # 4-digit status code
        "2026-10-28 12:00:12.300 [INFO] [auth-service] GET /api/v1/session 2000 12.5ms - Active session validated\n",
        # Out-of-range status code (999)
        "2026-10-28 12:00:13.400 [INFO] [auth-service] GET /api/v1/session 999 12.5ms - Invalid status\n",
        # Missing latency entirely
        "2026-10-28 12:00:14.500 [INFO] [auth-service] GET /api/v1/session 200\n",
        # Missing status and latency (just method and endpoint)
        "2026-10-28 12:00:15.600 [INFO] [auth-service] GET /api/v1/session - Route request\n",
        # Endpoint without method, missing 'ms' unit
        "2026-10-28 12:00:16.700 [INFO] [auth-service] /api/v1/session 200 12.5\n",
        # Endpoint without method, missing latency
        "2026-10-28 12:00:17.800 [INFO] [auth-service] /api/v1/session 200\n",
        # Method with endpoint lacking leading slash
        "2026-10-28 12:00:18.900 [INFO] [auth-service] GET api/v1/session 200 12.5ms\n",
        # Method followed directly by status, endpoint missing
        "2026-10-28 12:00:19.000 [INFO] [auth-service] GET 200 12.5ms\n",
        # Method with HTTP protocol version
        "2026-10-28 12:00:20.100 [INFO] [auth-service] GET /api/v1/session HTTP/1.1 200 12.5ms\n",
        # Typo in HTTP method
        "2026-10-28 12:00:21.200 [INFO] [auth-service] GTT /api/v1/session 200 12.5ms\n",
    ]

    f = tmp_path / "malformed_http.log"
    f.write_text("".join(malformed_lines), encoding="utf-8")

    parser = TextLogParser()
    records = list(parser.parse_file(f))
    assert len(records) == len(malformed_lines)

    for i, r in enumerate(records, start=1):
        # Must NOT be parsed as valid
        assert r.parsed_dict is None, f"Line {i} was incorrectly parsed as valid: {r.raw_text}"
        assert r.parse_error is not None, f"Line {i} did not report parse_error: {r.raw_text}"
        # Error must clearly identify malformed HTTP or invalid log record
        assert (
            "MALFORMED_HTTP_LOG" in r.parse_error or "INVALID_LOG_RECORD" in r.parse_error
        ), f"Line {i} had unexpected error message: {r.parse_error}"


def test_valid_general_application_logs_preserved(tmp_path: Path):
    """Verify general application logs (without HTTP access structure) parse correctly."""
    general_lines = [
        # Standard application log without latency
        "2026-10-28 12:01:00.000 [CRITICAL] [order-db] Database pool connection exhausted\n",
        # General log with inline latency
        "2026-10-28 12:00:25.000 [INFO] [analytics-worker] Batch metric rollup completed in 1500ms\n",
        # General log with idle state
        "2026-10-28 12:00:21.200 [WARN] [db-worker] Pool connection idle\n",
        # General log mentioning a filesystem path starting with /
        "2026-10-28 12:00:22.300 [INFO] [log-rotator] /var/log/app.log rotated successfully\n",
        # General log with natural text mentioning GET
        "2026-10-28 12:00:23.400 [INFO] [user-service] GET request received for user 123\n",
    ]

    f = tmp_path / "general_logs.log"
    f.write_text("".join(general_lines), encoding="utf-8")

    parser = TextLogParser()
    records = list(parser.parse_file(f))
    assert len(records) == len(general_lines)

    for i, r in enumerate(records, start=1):
        assert r.parse_error is None, f"General log line {i} failed parsing: {r.parse_error}"
        assert r.parsed_dict is not None
        # Must have endpoint=None and http_status=None
        assert r.parsed_dict["endpoint"] is None
        assert r.parsed_dict["http_status"] is None

    # Check inline latency extraction for record 2
    assert records[1].parsed_dict["latency_ms"] == 1500.0
    # Check no latency for record 1
    assert records[0].parsed_dict["latency_ms"] is None


def test_pipeline_end_to_end_malformed_http_logs(tmp_path: Path):
    """Verify pipeline rejects malformed HTTP logs and preserves valid records."""
    db_path = tmp_path / "c03_pipeline.db"
    cfg = PipelineConfig(
        database=DatabaseConfig(url=f"sqlite:///{db_path}"),
        input_dir=tmp_path / "input",
        quarantine_dir=tmp_path / "quarantine",
        reports_dir=tmp_path / "reports",
        batch_size=50,
    )
    pipeline = LogAnalyticsPipeline(config=cfg)

    log_file = tmp_path / "mixed.log"
    log_content = (
        # 1. Valid HTTP log
        "2026-10-28 12:00:10.100 [INFO] [auth-service] GET /api/v1/session 200 12.5ms - Session ok\n"
        # 2. Malformed HTTP log (missing ms)
        "2026-10-28 12:00:11.200 [INFO] [auth-service] GET /api/v1/session 200 12.5 - Malformed\n"
        # 3. Valid general log
        "2026-10-28 12:00:12.300 [INFO] [analytics-worker] Batch completed in 500ms\n"
        # 4. Malformed HTTP log (missing latency)
        "2026-10-28 12:00:13.400 [INFO] [auth-service] POST /api/v1/login 401\n"
    )
    log_file.write_text(log_content, encoding="utf-8")

    res = pipeline.run(files=[log_file], run_id="run-c03-e2e")

    # Invariants
    assert res["status"] == "PARTIAL_SUCCESS"
    assert res["input_count"] == 4
    assert res["valid_count"] == 2      # records 1 and 3
    assert res["rejected_count"] == 2   # records 2 and 4
    assert res["duplicate_count"] == 0
    assert res["conflict_count"] == 0

    recon = res["reconciliation"]
    assert recon["is_balanced"] is True
    assert recon["discrepancy"] == 0
    assert recon["accounted_total"] == 4

    # Verify database state
    with pipeline.db.session_scope() as session:
        events_count = session.scalar(select(func.count()).select_from(LogEventModel))
        rejected_count = session.scalar(select(func.count()).select_from(RejectedRecordModel))
        assert events_count == 2
        assert rejected_count == 2
