"""Shared Pytest fixtures for LogAnalyticsPipeline tests."""

from __future__ import annotations

import os
from typing import Generator
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from src.log_pipeline.db.schema import Base
from src.log_pipeline.db.connection import DatabaseManager
from src.log_pipeline.config import DatabaseConfig, PipelineConfig


@pytest.fixture
def sqlite_engine():
    """Create an in-memory SQLite engine with all tables created."""
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=engine)
    yield engine
    Base.metadata.drop_all(bind=engine)
    engine.dispose()


@pytest.fixture
def db_session(sqlite_engine) -> Generator[Session, None, None]:
    """Provide a transactional database session against the test SQLite engine."""
    SessionLocal = sessionmaker(bind=sqlite_engine, autocommit=False, autoflush=False)
    session = SessionLocal()
    try:
        yield session
    finally:
        session.rollback()
        session.close()


@pytest.fixture
def test_db_manager() -> Generator[DatabaseManager, None, None]:
    """Provide an isolated SQLite-backed DatabaseManager."""
    db_mgr = DatabaseManager(url="sqlite:///:memory:")
    db_mgr.create_tables()
    yield db_mgr
    db_mgr.drop_tables()


@pytest.fixture
def sample_valid_record_dict():
    """A standard valid dictionary for LogEventRecord creation."""
    return {
        "event_id": "test-evt-001",
        "timestamp": "2026-10-28T12:00:00Z",
        "severity": "INFO",
        "service": "checkout-service",
        "endpoint": "/api/v1/checkout",
        "http_status": 200,
        "latency_ms": 120.5,
        "message": "Cart checked out successfully",
        "source_file": "sample_logs.jsonl",
        "source_line": 1,
        "run_id": "run-test-001",
    }
