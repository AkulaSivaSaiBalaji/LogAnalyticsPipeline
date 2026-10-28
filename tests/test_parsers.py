"""Unit tests for JSON, JSONL, and text log parsers."""

from pathlib import Path
import pytest

from src.log_pipeline.parsers.json_parser import JsonLogParser
from src.log_pipeline.parsers.text_parser import TextLogParser
from src.log_pipeline.parsers.detector import get_parser_for_file


def test_json_array_parser(tmp_path):
    """Verify parsing standard JSON array of log records."""
    f = tmp_path / "events.json"
    f.write_text(
        """[
        {"timestamp": "2026-10-28T10:00:00Z", "severity": "INFO", "service": "api", "message": "msg 1"},
        {"timestamp": "2026-10-28T10:01:00Z", "severity": "WARN", "service": "api", "message": "msg 2"}
    ]""",
        encoding="utf-8",
    )

    parser = JsonLogParser()
    records = list(parser.parse_file(f))
    assert len(records) == 2
    assert records[0].source_line == 1
    assert records[0].parsed_dict["service"] == "api"
    assert records[0].parse_error is None
    assert records[1].source_line == 2


def test_jsonl_parser_malformed_recovery(tmp_path):
    """Verify JSONL parser recovers from corrupted lines and continues parsing."""
    f = tmp_path / "stream.jsonl"
    f.write_text(
        '{"timestamp": "2026-10-28T10:00:00Z", "severity": "INFO", "service": "auth", "message": "ok 1"}\n'
        'CORRUPTED_JSON_LINE_NOT_VALID_SYNTAX\n'
        '{"timestamp": "2026-10-28T10:02:00Z", "severity": "INFO", "service": "auth", "message": "ok 2"}\n',
        encoding="utf-8",
    )

    parser = JsonLogParser()
    records = list(parser.parse_file(f))
    assert len(records) == 3

    # First record is valid
    assert records[0].source_line == 1
    assert records[0].parse_error is None
    assert records[0].parsed_dict["message"] == "ok 1"

    # Second record failed with malformed line error
    assert records[1].source_line == 2
    assert records[1].parse_error is not None
    assert "MALFORMED_JSON_LINE" in records[1].parse_error
    assert records[1].raw_text == "CORRUPTED_JSON_LINE_NOT_VALID_SYNTAX"

    # Third record recovered and parsed successfully
    assert records[2].source_line == 3
    assert records[2].parse_error is None
    assert records[2].parsed_dict["message"] == "ok 2"


def test_text_log_parser_http_format(tmp_path):
    """Verify parsing semi-structured HTTP access logs."""
    f = tmp_path / "app.log"
    f.write_text(
        "2026-10-28 12:00:10.100 [INFO] [auth-service] GET /api/v1/session 200 12.5ms - Active session validated\n"
        "2026-10-28 12:00:15.300 [WARN] [auth-service] POST /api/v1/login 401 28.0ms - Invalid password\n"
        "MALFORMED UNPARSED LINE WITHOUT HEADER\n"
        "2026-10-28 12:01:00.000 [CRITICAL] [order-db] Database pool connection exhausted\n",
        encoding="utf-8",
    )

    parser = TextLogParser()
    records = list(parser.parse_file(f))
    assert len(records) == 4

    # Record 1: HTTP with endpoint, status, latency
    assert records[0].source_line == 1
    assert records[0].parse_error is None
    assert records[0].parsed_dict["service"] == "auth-service"
    assert records[0].parsed_dict["endpoint"] == "/api/v1/session"
    assert records[0].parsed_dict["http_status"] == 200
    assert records[0].parsed_dict["latency_ms"] == 12.5

    # Record 2: HTTP 401
    assert records[1].source_line == 2
    assert records[1].parsed_dict["http_status"] == 401

    # Record 3: Malformed line captured
    assert records[2].source_line == 3
    assert records[2].parse_error is not None
    assert "UNRECOGNIZED_TEXT_LOG_FORMAT" in records[2].parse_error

    # Record 4: General application log without endpoint
    assert records[3].source_line == 4
    assert records[3].parse_error is None
    assert records[3].parsed_dict["service"] == "order-db"
    assert records[3].parsed_dict["severity"] == "CRITICAL"


def test_empty_log_files(tmp_path):
    """Verify empty log files produce 0 records without errors."""
    empty_json = tmp_path / "empty.json"
    empty_json.write_text("", encoding="utf-8")
    assert list(JsonLogParser().parse_file(empty_json)) == []

    empty_log = tmp_path / "empty.log"
    empty_log.write_text("", encoding="utf-8")
    assert list(TextLogParser().parse_file(empty_log)) == []


def test_detector_router(tmp_path):
    """Verify auto-detection of parsers by file extension."""
    assert isinstance(get_parser_for_file(Path("test.json")), JsonLogParser)
    assert isinstance(get_parser_for_file(Path("test.jsonl")), JsonLogParser)
    assert isinstance(get_parser_for_file(Path("test.log")), TextLogParser)
