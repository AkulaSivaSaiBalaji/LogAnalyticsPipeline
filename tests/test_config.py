"""Unit tests for configuration loading and environment handling."""

from pathlib import Path
import os
import pytest

from src.log_pipeline.config import load_config, DatabaseConfig, PipelineConfig


def test_default_config_loading():
    """Verify default values are loaded when no environment overrides exist."""
    cfg = load_config()
    assert isinstance(cfg, PipelineConfig)
    assert isinstance(cfg.database, DatabaseConfig)
    assert cfg.batch_size == 5000
    assert cfg.log_level in ["INFO", "DEBUG", "WARN", "WARNING"]
    assert isinstance(cfg.input_dir, Path)
    assert isinstance(cfg.quarantine_dir, Path)
    assert isinstance(cfg.reports_dir, Path)


def test_explicit_overrides():
    """Verify explicit parameters override environment and defaults."""
    custom_url = "sqlite:///custom_test.db"
    cfg = load_config(
        database_url=custom_url,
        input_dir="/tmp/inputs",
        quarantine_dir="/tmp/quarantine",
        reports_dir="/tmp/reports",
        batch_size=1000,
    )
    assert cfg.database.get_connection_url() == custom_url
    assert cfg.input_dir == Path("/tmp/inputs")
    assert cfg.quarantine_dir == Path("/tmp/quarantine")
    assert cfg.reports_dir == Path("/tmp/reports")
    assert cfg.batch_size == 1000


def test_database_url_generation():
    """Verify standard MySQL connection URL generation from components."""
    db_cfg = DatabaseConfig(
        host="db.internal",
        port=3307,
        user="test_admin",
        password="secure_password",
        name="analytics_prod",
    )
    expected = "mysql+pymysql://test_admin:secure_password@db.internal:3307/analytics_prod?charset=utf8mb4"
    assert db_cfg.get_connection_url() == expected


def test_ensure_directories(tmp_path):
    """Verify directory auto-creation helper."""
    quarantine_path = tmp_path / "custom_quarantine"
    reports_path = tmp_path / "custom_reports"
    cfg = PipelineConfig(
        quarantine_dir=quarantine_path,
        reports_dir=reports_path,
    )
    assert not quarantine_path.exists()
    assert not reports_path.exists()

    cfg.ensure_directories()
    assert quarantine_path.exists() and quarantine_path.is_dir()
    assert reports_path.exists() and reports_path.is_dir()
