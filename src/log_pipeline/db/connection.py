"""Database connection and session lifecycle management for LogAnalyticsPipeline.

Provides thread-safe connection pooling, transaction boundaries,
and automated schema bootstrapping.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Generator, Optional
from sqlalchemy import create_engine, Engine, text
from sqlalchemy.orm import sessionmaker, Session
from .schema import Base
from ..config import PipelineConfig, DatabaseConfig, load_config


class DatabaseManager:
    """Manages SQLAlchemy Engine, Session lifecycle, and transactions."""

    def __init__(self, config: Optional[DatabaseConfig] = None, url: Optional[str] = None):
        if url:
            self.connection_url = url
        elif config:
            self.connection_url = config.get_connection_url()
        else:
            cfg = load_config()
            self.connection_url = cfg.database.get_connection_url()

        # Engine configuration
        is_sqlite = self.connection_url.startswith("sqlite")
        engine_kwargs = {}
        if is_sqlite:
            engine_kwargs["connect_args"] = {"check_same_thread": False}
        else:
            engine_kwargs.update({
                "pool_size": 10,
                "max_overflow": 20,
                "pool_recycle": 3600,
                "pool_pre_ping": True,
            })

        self.engine: Engine = create_engine(self.connection_url, **engine_kwargs)
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)

    def create_tables(self) -> None:
        """Create all analytical tables if they do not exist."""
        Base.metadata.create_all(bind=self.engine)

    def drop_tables(self) -> None:
        """Drop all managed tables (primarily for test teardown)."""
        Base.metadata.drop_all(bind=self.engine)

    def test_connection(self) -> bool:
        """Execute a lightweight health check query."""
        with self.engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            return result.scalar() == 1

    @contextmanager
    def session_scope(self) -> Generator[Session, None, None]:
        """Provide a transactional scope around a series of operations.

        Ensures automatic commit on success and rollback on any exception.
        """
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()


def get_db_manager(config: Optional[PipelineConfig] = None) -> DatabaseManager:
    """Convenience factory function for DatabaseManager."""
    if config:
        return DatabaseManager(config=config.database)
    return DatabaseManager()
