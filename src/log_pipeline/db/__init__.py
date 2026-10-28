"""Database module for LogAnalyticsPipeline.

Provides connection handling, engine instantiation, session lifecycle,
and SQLAlchemy ORM models.
"""

from .connection import DatabaseManager, get_db_manager
from .schema import Base, LogEventModel, RejectedRecordModel, PipelineRunModel

__all__ = [
    "DatabaseManager",
    "get_db_manager",
    "Base",
    "LogEventModel",
    "RejectedRecordModel",
    "PipelineRunModel",
]
