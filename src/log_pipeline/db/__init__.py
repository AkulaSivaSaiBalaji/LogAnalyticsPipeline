"""Database module for LogAnalyticsPipeline.

Provides connection handling, engine instantiation, session lifecycle,
idempotent batch writing, and SQLAlchemy ORM models.
"""

from .connection import DatabaseManager, get_db_manager
from .schema import Base, LogEventModel, RejectedRecordModel, PipelineRunModel
from .writer import DatabaseWriter

__all__ = [
    "DatabaseManager",
    "get_db_manager",
    "DatabaseWriter",
    "Base",
    "LogEventModel",
    "RejectedRecordModel",
    "PipelineRunModel",
]
