"""Database connection and session lifecycle management for LogAnalyticsPipeline.

Provides thread-safe connection pooling, transaction boundaries,
automated schema bootstrapping, and sanitized credential protection.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Generator, Optional, Tuple, Union
from sqlalchemy import create_engine, Engine, text
from sqlalchemy.engine import URL, make_url
from sqlalchemy.orm import sessionmaker, Session
from .schema import Base
from ..config import PipelineConfig, DatabaseConfig, load_config

logger = logging.getLogger("log_pipeline.db")


class DatabaseManager:
    """Manages SQLAlchemy Engine, Session lifecycle, and transactions safely."""

    def __init__(
        self,
        config: Optional[DatabaseConfig] = None,
        url: Optional[Union[str, URL]] = None,
    ):
        if url:
            if isinstance(url, URL):
                self._url_obj = url
            else:
                self._url_obj = make_url(url)
        elif config:
            self._url_obj = config.get_url_object()
        else:
            cfg = load_config()
            self._url_obj = cfg.database.get_url_object()

        self.masked_url = self._url_obj.render_as_string(hide_password=True)
        is_sqlite = self._url_obj.drivername.startswith("sqlite")
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

        self.engine: Engine = create_engine(self._url_obj, **engine_kwargs)
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)

    def __repr__(self) -> str:
        return f"DatabaseManager(url='{self.masked_url}')"

    def create_tables(self) -> None:
        """Create all analytical tables if they do not exist."""
        Base.metadata.create_all(bind=self.engine)

    def drop_tables(self) -> None:
        """Drop all managed tables (primarily for test teardown)."""
        Base.metadata.drop_all(bind=self.engine)

    def verify_connection(self) -> Tuple[bool, str]:
        """Verify database connectivity safely without exposing credentials."""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                if result.scalar() == 1:
                    return True, f"Successfully connected to database at {self.masked_url}"
                return False, f"Unexpected response from database at {self.masked_url}"
        except Exception as e:
            # Sanitize error message to ensure no passwords or connection strings leak
            err_type = type(e).__name__
            err_msg = str(e)
            if "@" in err_msg and ":" in err_msg:
                # Remove any raw credentials if present in driver exception
                parts = err_msg.split("@")
                err_msg = f"{err_type}: Connection failed to @{parts[-1]}"
            else:
                err_msg = f"{err_type}: {err_msg[:120]}"
            return False, f"Database connectivity check failed for {self.masked_url}: {err_msg}"

    def test_connection(self) -> bool:
        """Execute a lightweight health check query returning boolean."""
        success, msg = self.verify_connection()
        if not success:
            logger.warning(msg)
        return success

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
