"""Tests for Stage 1E: Configuration, Input Validation, and CLI Consistency."""

import io
import json
import os
from pathlib import Path
from unittest.mock import patch
import pytest
from sqlalchemy import select

from src.log_pipeline.cli import build_parser, main
from src.log_pipeline.config import DatabaseConfig, PipelineConfig, load_config
from src.log_pipeline.db.connection import DatabaseManager
from src.log_pipeline.db.schema import PipelineRunModel
from src.log_pipeline.pipeline import LogAnalyticsPipeline


def test_missing_input_file_raises_file_not_found_and_records_failed_status(tmp_path):
    """Verify that a nonexistent file in files=[...] raises FileNotFoundError and is recorded as FAILED in ledger."""
    db_file = tmp_path / "missing_file.db"
    cfg = PipelineConfig(
        database=DatabaseConfig(url=f"sqlite:///{db_file}"),
        quarantine_dir=tmp_path / "q",
        reports_dir=tmp_path / "r",
    )
    pipeline = LogAnalyticsPipeline(cfg)
    missing_file = tmp_path / "nonexistent.jsonl"
    run_id = "run-missing-file"

    with pytest.raises(FileNotFoundError, match="Input path does not exist"):
        pipeline.run(files=[missing_file], run_id=run_id)

    # Verify that run ledger recorded FAILED status with failure details and preserved input manifest
    with pipeline.db.session_scope() as session:
        ledger = session.get(PipelineRunModel, run_id)
        assert ledger is not None
        assert ledger.status == "FAILED"
        assert "FileNotFoundError" in ledger.failure_details
        assert str(missing_file) in json.loads(ledger.input_files)


def test_missing_input_dir_raises_file_not_found(tmp_path):
    """Verify that a nonexistent config.input_dir raises FileNotFoundError when files is None."""
    db_file = tmp_path / "missing_dir.db"
    missing_dir = tmp_path / "nonexistent_input_dir"
    cfg = PipelineConfig(
        database=DatabaseConfig(url=f"sqlite:///{db_file}"),
        input_dir=missing_dir,
        quarantine_dir=tmp_path / "q",
        reports_dir=tmp_path / "r",
    )
    pipeline = LogAnalyticsPipeline(cfg)
    run_id = "run-missing-dir"

    with pytest.raises(FileNotFoundError, match="Configured input directory does not exist"):
        pipeline.run(files=None, run_id=run_id)

    with pipeline.db.session_scope() as session:
        ledger = session.get(PipelineRunModel, run_id)
        assert ledger is not None
        assert ledger.status == "FAILED"
        assert "FileNotFoundError" in ledger.failure_details


def test_empty_input_files_list_produces_valid_empty_batch(tmp_path):
    """Verify that an explicitly empty list files=[] succeeds with 0 records and balanced reconciliation."""
    db_file = tmp_path / "empty_batch.db"
    cfg = PipelineConfig(
        database=DatabaseConfig(url=f"sqlite:///{db_file}"),
        quarantine_dir=tmp_path / "q",
        reports_dir=tmp_path / "r",
    )
    pipeline = LogAnalyticsPipeline(cfg)
    run_id = "run-empty-list"

    res = pipeline.run(files=[], run_id=run_id)
    assert res["status"] == "SUCCESS"
    assert res["input_count"] == 0
    assert res["valid_count"] == 0
    assert res["rejected_count"] == 0
    assert res["duplicate_count"] == 0
    assert res["reconciliation"]["is_balanced"] is True


def test_empty_input_directory_produces_valid_empty_batch(tmp_path):
    """Verify that an existing but empty directory produces a valid empty batch run."""
    empty_dir = tmp_path / "empty_dir"
    empty_dir.mkdir()
    db_file = tmp_path / "empty_dir.db"
    cfg = PipelineConfig(
        database=DatabaseConfig(url=f"sqlite:///{db_file}"),
        input_dir=empty_dir,
        quarantine_dir=tmp_path / "q",
        reports_dir=tmp_path / "r",
    )
    pipeline = LogAnalyticsPipeline(cfg)
    run_id = "run-empty-dir"

    res = pipeline.run(files=[empty_dir], run_id=run_id)
    assert res["status"] == "SUCCESS"
    assert res["input_count"] == 0
    assert res["valid_count"] == 0
    assert res["reconciliation"]["is_balanced"] is True


def test_config_precedence_explicit_over_env_over_default(monkeypatch):
    """Verify explicit configuration precedence: argument > DATABASE_URL > discrete DB_* vars > defaults."""
    # 1. DATABASE_URL supersedes discrete DB_* vars (even if DB_PASSWORD is set in env)
    monkeypatch.setenv("DATABASE_URL", "sqlite:///from_env.db")
    monkeypatch.setenv("DB_PASSWORD", "secret_pass")
    monkeypatch.setenv("DB_HOST", "mysql.internal")
    monkeypatch.setenv("DB_NAME", "wrong_db")

    cfg1 = load_config()
    assert cfg1.database.url == "sqlite:///from_env.db"
    assert cfg1.database.name == "from_env.db"

    # 2. Explicit argument supersedes DATABASE_URL in env
    cfg2 = load_config(database_url="sqlite:///explicit_override.db")
    assert cfg2.database.url == "sqlite:///explicit_override.db"
    assert cfg2.database.name == "explicit_override.db"

    # 3. Discrete env vars used when DATABASE_URL is not set
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("DB_HOST", "cluster-db.net")
    monkeypatch.setenv("DB_NAME", "analytics_staging")
    cfg3 = load_config()
    assert cfg3.database.url is None
    assert cfg3.database.host == "cluster-db.net"
    assert cfg3.database.name == "analytics_staging"


def test_database_name_consistency_with_url():
    """Verify DatabaseConfig synchronizes name with URL and accurately renders __repr__."""
    # SQLite relative path
    cfg_sqlite = DatabaseConfig(url="sqlite:///data/custom.db")
    assert cfg_sqlite.name == "data/custom.db"
    assert "data/custom.db" in repr(cfg_sqlite)
    assert "localhost" not in repr(cfg_sqlite)

    # SQLite in-memory
    cfg_mem = DatabaseConfig(url="sqlite:///:memory:")
    assert cfg_mem.name == ":memory:"

    # MySQL URL
    cfg_mysql = DatabaseConfig(url="mysql+pymysql://root:pass@remote-host:3306/production_db")
    assert cfg_mysql.name == "production_db"
    assert cfg_mysql.host == "remote-host"
    assert "production_db" in repr(cfg_mysql)
    assert "pass" not in repr(cfg_mysql)

    # Discrete configuration without URL
    cfg_discrete = DatabaseConfig(host="localhost", port=3306, user="root", password="pwd", name="log_analytics_db")
    assert cfg_discrete.name == "log_analytics_db"
    assert "pwd" not in repr(cfg_discrete)


def test_batch_size_validation():
    """Verify load_config enforces positive batch_size values."""
    with pytest.raises(ValueError, match="batch_size must be a positive integer"):
        load_config(batch_size=0)

    with pytest.raises(ValueError, match="batch_size must be a positive integer"):
        load_config(batch_size=-100)

    cfg = load_config(batch_size=1000)
    assert cfg.batch_size == 1000


def test_cli_argument_validation_failures():
    """Verify CLI parser rejects invalid arguments cleanly."""
    parser = build_parser()

    # Invalid batch size
    with pytest.raises(SystemExit):
        with patch("sys.stderr", new=io.StringIO()):
            main(["--batch-size", "0"])

    # Nonexistent input file
    with pytest.raises(SystemExit):
        with patch("sys.stderr", new=io.StringIO()):
            main(["--files", "does_not_exist_file.log"])

    # Nonexistent input directory
    with pytest.raises(SystemExit):
        with patch("sys.stderr", new=io.StringIO()):
            main(["--input-dir", "does_not_exist_dir"])


def test_cli_successful_run_json_output(tmp_path):
    """Verify CLI executes successfully on a valid log file and emits structured JSON."""
    db_file = tmp_path / "cli_test.db"
    q_dir = tmp_path / "q"
    r_dir = tmp_path / "r"
    log_file = tmp_path / "valid.jsonl"

    record = {
        "event_id": "cli-1",
        "timestamp": "2026-10-28T12:00:00Z",
        "severity": "INFO",
        "service": "api-service",
        "endpoint": "/api/v1/ping",
        "http_status": 200,
        "latency_ms": 10.5,
        "message": "pong",
    }
    log_file.write_text(json.dumps(record) + "\n", encoding="utf-8")

    captured_out = io.StringIO()
    with patch("sys.stdout", new=captured_out):
        exit_code = main([
            "--files", str(log_file),
            "--database-url", f"sqlite:///{db_file}",
            "--quarantine-dir", str(q_dir),
            "--reports-dir", str(r_dir),
            "--json",
        ])

    assert exit_code == 0
    output_text = captured_out.getvalue()
    result = json.loads(output_text)
    assert result["status"] == "SUCCESS"
    assert result["valid_count"] == 1
    assert result["input_count"] == 1
    assert result["reconciliation"]["is_balanced"] is True


def test_cli_human_readable_output(tmp_path):
    """Verify CLI produces formatted text summary when --json is not set."""
    db_file = tmp_path / "cli_text.db"
    q_dir = tmp_path / "q"
    r_dir = tmp_path / "r"
    log_file = tmp_path / "valid.jsonl"

    record = {
        "event_id": "cli-text-1",
        "timestamp": "2026-10-28T12:00:00Z",
        "severity": "INFO",
        "service": "api-service",
        "endpoint": "/api/v1/ping",
        "http_status": 200,
        "latency_ms": 10.5,
        "message": "pong",
    }
    log_file.write_text(json.dumps(record) + "\n", encoding="utf-8")

    captured_out = io.StringIO()
    with patch("sys.stdout", new=captured_out):
        exit_code = main([
            "--files", str(log_file),
            "--database-url", f"sqlite:///{db_file}",
            "--quarantine-dir", str(q_dir),
            "--reports-dir", str(r_dir),
        ])

    assert exit_code == 0
    output_text = captured_out.getvalue()
    assert "Pipeline Run Completed:" in output_text
    assert "Status:          SUCCESS" in output_text
    assert "Valid Inserted:  1" in output_text


def test_benchmark_and_generator_cli_argument_validation():
    """Verify benchmark and generator scripts reject non-positive records."""
    import subprocess
    import sys

    # Test benchmark_pipeline.py with --records 0
    res = subprocess.run(
        [sys.executable, "scripts/benchmark_pipeline.py", "--records", "0"],
        capture_output=True,
        text=True,
    )
    assert res.returncode != 0
    assert "--records must be a positive integer" in res.stderr

    # Test generate_synthetic_logs.py with --records -5
    res2 = subprocess.run(
        [sys.executable, "scripts/generate_synthetic_logs.py", "--records", "-5"],
        capture_output=True,
        text=True,
    )
    assert res2.returncode != 0
    assert "--records must be a positive integer" in res2.stderr


def test_missing_input_cannot_overwrite_terminal_run(tmp_path):
    """Verify that a missing input file or error cannot overwrite an existing terminal run (SUCCESS or FAILED)."""
    db_file = tmp_path / "terminal_run_protection.db"
    log_file = tmp_path / "valid.jsonl"
    log_file.write_text(
        json.dumps({
            "timestamp": "2026-10-28T12:00:00Z",
            "severity": "INFO",
            "service": "auth-service",
            "endpoint": "/api/v1/login",
            "http_status": 200,
            "latency_ms": 42.0,
            "message": "success",
        }) + "\n",
        encoding="utf-8",
    )

    cfg = PipelineConfig(
        database=DatabaseConfig(url=f"sqlite:///{db_file}"),
        quarantine_dir=tmp_path / "q",
        reports_dir=tmp_path / "r",
    )
    pipeline = LogAnalyticsPipeline(cfg)
    run_id = "run-terminal-immutability"

    # 1. Run successfully
    res = pipeline.run(files=[log_file], run_id=run_id)
    assert res["status"] == "SUCCESS"
    assert res["valid_count"] == 1

    # Verify run ledger recorded SUCCESS
    with pipeline.db.session_scope() as session:
        run_record = session.get(PipelineRunModel, run_id)
        assert run_record is not None
        assert run_record.status == "SUCCESS"
        assert run_record.valid_count == 1
        orig_end_time = run_record.end_time

    # 2. Try to run with the same run_id and a missing input file
    missing_file = tmp_path / "does_not_exist.jsonl"
    with pytest.raises(ValueError, match="already completed with status 'SUCCESS'"):
        pipeline.run(files=[missing_file], run_id=run_id)

    # 3. Verify database record was NOT overwritten to FAILED
    with pipeline.db.session_scope() as session:
        run_record_after = session.get(PipelineRunModel, run_id)
        assert run_record_after.status == "SUCCESS"
        assert run_record_after.valid_count == 1
        assert run_record_after.end_time == orig_end_time
        assert run_record_after.failure_details is None
