"""Unit tests for configuration loading, secure URL handling, and environment settings."""

from pathlib import Path
import os
import pytest

from src.log_pipeline.config import load_config, DatabaseConfig, PipelineConfig


def test_default_config_loading():
    """Verify values are loaded correctly from defaults and environment."""
    cfg = load_config()
    assert isinstance(cfg, PipelineConfig)
    assert isinstance(cfg.database, DatabaseConfig)
    assert cfg.batch_size > 0
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
        batch_size=1234,
    )
    assert "sqlite" in cfg.database.get_connection_url()
    assert cfg.input_dir == Path("/tmp/inputs")
    assert cfg.quarantine_dir == Path("/tmp/quarantine")
    assert cfg.reports_dir == Path("/tmp/reports")
    assert cfg.batch_size == 1234


def test_database_url_generation_and_masking():
    """Verify standard MySQL connection URL generation via URL.create and credential masking."""
    db_cfg = DatabaseConfig(
        host="db.internal",
        port=3307,
        user="test_admin",
        password="secure_password",
        name="analytics_prod",
    )
    url_obj = db_cfg.get_url_object()
    assert url_obj.username == "test_admin"
    assert url_obj.password == "secure_password"
    assert url_obj.host == "db.internal"
    assert url_obj.port == 3307
    assert url_obj.database == "analytics_prod"

    # Masked URL must never expose the password
    masked = db_cfg.get_masked_url()
    assert "secure_password" not in masked
    assert "***" in masked
    assert "test_admin" in masked

    # Repr must never expose the password
    repr_str = repr(db_cfg)
    assert "secure_password" not in repr_str
    assert "***" in repr_str


def test_password_with_special_characters():
    """Verify URL.create properly handles passwords with '@', '#', '$' without breaking."""
    db_cfg = DatabaseConfig(
        host="localhost",
        port=3306,
        user="root",
        password="p@ss#word!$%",
        name="log_analytics",
    )
    url_obj = db_cfg.get_url_object()
    assert url_obj.password == "p@ss#word!$%"
    assert url_obj.host == "localhost"
    masked = db_cfg.get_masked_url()
    assert "p@ss#word!$%" not in masked
    assert "***" in masked


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
