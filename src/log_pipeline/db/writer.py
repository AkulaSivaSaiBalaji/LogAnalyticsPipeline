"""Transactional database writer with batching, deduplication, and lineage tracking."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple, Optional
from sqlalchemy import select, func, text
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from .connection import DatabaseManager
from .schema import PipelineRunModel, LogEventModel, RejectedRecordModel
from ..models import LogEventRecord, RejectedRecord

logger = logging.getLogger("log_pipeline.db.writer")


class BatchWriteResult(tuple):
    """Result of write_events_batch maintaining backwards-compatible (inserted, dupes) tuple unpacking."""

    inserted_count: int
    duplicate_count: int
    inserted_events: List[LogEventRecord]

    def __new__(
        cls,
        inserted_count: int,
        duplicate_count: int,
        inserted_events: Optional[List[LogEventRecord]] = None,
    ):
        instance = super().__new__(cls, (inserted_count, duplicate_count))
        instance.inserted_count = inserted_count
        instance.duplicate_count = duplicate_count
        instance.inserted_events = inserted_events if inserted_events is not None else []
        return instance


# SQLite bind-variable safety limits
# SQLite default parameter limit is 32,766 (SQLITE_MAX_VARIABLE_NUMBER in SQLite 3.32.0+).
# Older SQLite versions had a limit of 999.
SQLITE_MAX_VARIABLES_DEFAULT = 32766
SQLITE_MAX_VARIABLES_LEGACY = 999
SQLITE_EVENT_ROW_WIDTH = 12
SQLITE_REJECTED_ROW_WIDTH = 6


class DatabaseWriter:
    """Handles high-throughput, idempotent batch writes to analytical tables."""

    def __init__(self, db_manager: DatabaseManager, batch_size: int = 5000):
        self.db = db_manager
        self.batch_size = max(1, batch_size)
        self.is_mysql = "mysql" in self.db._url_obj.drivername
        self.is_sqlite = "sqlite" in self.db._url_obj.drivername
        self._sqlite_max_variables: Optional[int] = None

    @property
    def sqlite_max_variables(self) -> int:
        """Effective SQLite parameter limit based on runtime connection getlimit or driver/sqlite3 version."""
        if self._sqlite_max_variables is not None:
            return self._sqlite_max_variables
        try:
            import sqlite3
            if self.is_sqlite and hasattr(self.db, "engine"):
                with self.db.engine.connect() as conn:
                    raw_conn = getattr(getattr(conn, "connection", None), "dbapi_connection", None)
                    if raw_conn and hasattr(raw_conn, "getlimit") and hasattr(sqlite3, "SQLITE_LIMIT_VARIABLE_NUMBER"):
                        self._sqlite_max_variables = raw_conn.getlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER)
                        return self._sqlite_max_variables
        except Exception:
            pass

        try:
            import sqlite3
            ver_parts = [int(p) for p in sqlite3.sqlite_version.split(".")[:2] if p.isdigit()]
            if tuple(ver_parts) >= (3, 32):
                return SQLITE_MAX_VARIABLES_DEFAULT
            return SQLITE_MAX_VARIABLES_LEGACY
        except Exception:
            return SQLITE_MAX_VARIABLES_DEFAULT

    @sqlite_max_variables.setter
    def sqlite_max_variables(self, value: Optional[int]) -> None:
        """Allow explicit override of SQLite max variables (useful for testing legacy/tight bounds)."""
        self._sqlite_max_variables = value

    def get_max_event_chunk_size(self, fixed_params: int = 0) -> int:
        """Calculate safe maximum rows per INSERT statement for LogEventModel.

        Accounts for row width (12 columns) and any fixed bound parameters in the statement.
        For SQLite, clamps to floor((max_variables - fixed_params) / 12).
        For MySQL or other engines, returns self.batch_size unconstrained by SQLite limits.
        """
        if self.is_sqlite:
            available_vars = max(1, self.sqlite_max_variables - fixed_params)
            safe_max = available_vars // SQLITE_EVENT_ROW_WIDTH
            return min(self.batch_size, max(1, safe_max))
        return self.batch_size

    def get_max_rejected_chunk_size(self, fixed_params: int = 0) -> int:
        """Calculate safe maximum rows per INSERT statement for RejectedRecordModel.

        Accounts for row width (6 columns) and any fixed bound parameters in the statement.
        For SQLite, clamps to floor((max_variables - fixed_params) / 6).
        For MySQL or other engines, returns self.batch_size unconstrained by SQLite limits.
        """
        if self.is_sqlite:
            available_vars = max(1, self.sqlite_max_variables - fixed_params)
            safe_max = available_vars // SQLITE_REJECTED_ROW_WIDTH
            return min(self.batch_size, max(1, safe_max))
        return self.batch_size

    @staticmethod
    def _to_row_dict(ev: LogEventRecord) -> Dict[str, Any]:
        """Convert LogEventRecord to dictionary for database column mapping."""
        return {
            "event_id": ev.event_id,
            "run_id": ev.run_id,
            "timestamp": ev.timestamp,
            "severity": ev.severity.value,
            "service": ev.service,
            "endpoint": ev.endpoint,
            "http_status": ev.http_status,
            "latency_ms": ev.latency_ms,
            "message": ev.message,
            "source_file": ev.source_file,
            "source_line": ev.source_line,
            "created_at": datetime.now(timezone.utc),
        }

    def start_run(self, run_id: str, input_files: List[str]) -> None:
        """Initialize pipeline run ledger row with RUNNING status."""
        with self.db.session_scope() as session:
            existing = session.get(PipelineRunModel, run_id)
            if existing is not None:
                if existing.status in ("SUCCESS", "PARTIAL_SUCCESS", "FAILED"):
                    raise ValueError(
                        f"Cannot start pipeline run '{run_id}': run already completed with status '{existing.status}'"
                    )
                # If already RUNNING, preserve existing state without resetting
                return

            run = PipelineRunModel(
                run_id=run_id,
                start_time=datetime.now(timezone.utc),
                status="RUNNING",
                input_files=json.dumps(input_files),
                input_count=0,
                valid_count=0,
                rejected_count=0,
                duplicate_count=0,
            )
            session.add(run)

    def write_events_batch(
        self,
        events: List[LogEventRecord],
    ) -> BatchWriteResult:
        """Write a batch of LogEventRecord items idempotently.

        Returns:
            BatchWriteResult: Tuple-compatible (inserted_count, duplicate_count)
            with an .inserted_events attribute containing the records that were
            newly persisted.
        """
        if not events:
            return BatchWriteResult(0, 0, [])

        # In-batch deduplication by event_id (retain first occurrence)
        unique_events: Dict[str, LogEventRecord] = {}
        in_batch_duplicates = 0
        for ev in events:
            if ev.event_id in unique_events:
                in_batch_duplicates += 1
            else:
                unique_events[ev.event_id] = ev

        candidate_ids = list(unique_events.keys())
        actually_inserted_ids: set[str] = set()

        with self.db.session_scope() as session:
            if self.is_sqlite:
                chunk_size = self.get_max_event_chunk_size()
                # Check SQLite capability for RETURNING (supported in SQLite 3.35+)
                supports_returning = True
                try:
                    import sqlite3
                    ver_parts = [int(p) for p in sqlite3.sqlite_version.split(".")[:2] if p.isdigit()]
                    supports_returning = tuple(ver_parts) >= (3, 35)
                except Exception:
                    supports_returning = True

                event_rows = [self._to_row_dict(ev) for ev in unique_events.values()]
                if supports_returning:
                    # Multi-chunk insert with RETURNING: skips existing rows and returns only newly inserted IDs
                    for i in range(0, len(event_rows), chunk_size):
                        chunk = event_rows[i : i + chunk_size]
                        stmt = (
                            sqlite_insert(LogEventModel)
                            .values(chunk)
                            .on_conflict_do_nothing(index_elements=["event_id"])
                            .returning(LogEventModel.event_id)
                        )
                        inserted_ids = session.scalars(stmt).all()
                        actually_inserted_ids.update(inserted_ids)
                else:
                    # Fallback for SQLite builds older than 3.35 lacking RETURNING
                    existing_ids: set[str] = set()
                    query_chunk_size = min(self.batch_size, min(500, self.sqlite_max_variables))
                    for i in range(0, len(candidate_ids), query_chunk_size):
                        chk_ids = candidate_ids[i : i + query_chunk_size]
                        stmt = select(LogEventModel.event_id).where(LogEventModel.event_id.in_(chk_ids))
                        existing_ids.update(session.scalars(stmt).all())

                    new_events = [ev for ev in unique_events.values() if ev.event_id not in existing_ids]
                    if new_events:
                        new_rows = [self._to_row_dict(ev) for ev in new_events]
                        for i in range(0, len(new_rows), chunk_size):
                            chunk = new_rows[i : i + chunk_size]
                            stmt = (
                                sqlite_insert(LogEventModel)
                                .values(chunk)
                                .on_conflict_do_nothing(index_elements=["event_id"])
                            )
                            session.execute(stmt)

                        for i in range(0, len(new_events), query_chunk_size):
                            chk_ids = [e.event_id for e in new_events[i : i + query_chunk_size]]
                            stmt = select(LogEventModel.event_id).where(LogEventModel.event_id.in_(chk_ids))
                            post_existing = set(session.scalars(stmt).all())
                            actually_inserted_ids.update(post_existing - existing_ids)

            elif self.is_mysql:
                # Pre-query existing IDs for MySQL (MySQL 8 does not support RETURNING on INSERT IGNORE)
                existing_ids: set[str] = set()
                query_chunk_size = min(self.batch_size, 500)
                for i in range(0, len(candidate_ids), query_chunk_size):
                    chk_ids = candidate_ids[i : i + query_chunk_size]
                    stmt = select(LogEventModel.event_id).where(LogEventModel.event_id.in_(chk_ids))
                    existing_ids.update(session.scalars(stmt).all())

                new_events = [ev for ev in unique_events.values() if ev.event_id not in existing_ids]
                if new_events:
                    new_rows = [self._to_row_dict(ev) for ev in new_events]
                    for i in range(0, len(new_rows), self.batch_size):
                        chunk = new_rows[i : i + self.batch_size]
                        stmt = mysql_insert(LogEventModel).values(chunk).prefix_with("IGNORE")
                        res = session.execute(stmt)
                        if res.rowcount == len(chunk):
                            actually_inserted_ids.update(r["event_id"] for r in chunk)
                        else:
                            # Handle rare concurrent conflict where fewer rows were inserted than candidate chunk
                            post_existing = set(
                                session.scalars(
                                    select(LogEventModel.event_id).where(
                                        LogEventModel.event_id.in_([r["event_id"] for r in chunk])
                                    )
                                ).all()
                            )
                            newly_added = post_existing - existing_ids
                            actually_inserted_ids.update(newly_added)
                            existing_ids.update(newly_added)

            else:
                # Generic fallback for any other SQL dialect
                for ev in unique_events.values():
                    existing = session.get(LogEventModel, ev.event_id)
                    if existing is None:
                        session.add(LogEventModel(**self._to_row_dict(ev)))
                        actually_inserted_ids.add(ev.event_id)

        # Build confirmed inserted_events preserving original candidate order
        inserted_events = [unique_events[eid] for eid in candidate_ids if eid in actually_inserted_ids]
        inserted_count = len(inserted_events)
        db_duplicates = len(unique_events) - inserted_count
        total_duplicates = in_batch_duplicates + db_duplicates

        return BatchWriteResult(inserted_count, total_duplicates, inserted_events)

    def write_rejected_batch(self, rejected_records: List[RejectedRecord]) -> int:
        """Write a batch of rejected records to quarantine table.

        Returns:
            Number of rejected records inserted.
        """
        if not rejected_records:
            return 0

        rows = [
            {
                "run_id": r.run_id,
                "source_file": r.source_file,
                "source_line": r.source_line,
                "raw_record": r.raw_record,
                "rejection_reasons": json.dumps(r.rejection_reasons),
                "created_at": r.created_at,
            }
            for r in rejected_records
        ]

        chunk_size = self.get_max_rejected_chunk_size()
        with self.db.session_scope() as session:
            for i in range(0, len(rows), chunk_size):
                chunk = rows[i : i + chunk_size]
                session.bulk_insert_mappings(RejectedRecordModel, chunk)

        return len(rows)

    def complete_run(
        self,
        run_id: str,
        status: str,
        input_count: int,
        valid_count: int,
        rejected_count: int,
        duplicate_count: int,
        failure_details: Optional[str] = None,
        input_files: Optional[List[str]] = None,
    ) -> None:
        """Update pipeline run ledger with terminal status and final reconciliation counts."""
        try:
            self.db.create_tables()
        except Exception:
            pass

        with self.db.session_scope() as session:
            run = session.get(PipelineRunModel, run_id)
            if run:
                # If run was already marked with a terminal status, refuse to overwrite
                if run.status in ("SUCCESS", "PARTIAL_SUCCESS", "FAILED"):
                    logger.warning(
                        f"Run '{run_id}' already has terminal status '{run.status}'; refusing to overwrite with '{status}'"
                    )
                    return
                run.end_time = datetime.now(timezone.utc)
                run.status = status
                run.input_count = input_count
                run.valid_count = valid_count
                run.rejected_count = rejected_count
                run.duplicate_count = duplicate_count
                if failure_details:
                    run.failure_details = failure_details
                if input_files is not None:
                    run.input_files = json.dumps(input_files)
            else:
                # If run record wasn't created yet (e.g. fatal abort on init)
                files_json = json.dumps(input_files or [])
                run = PipelineRunModel(
                    run_id=run_id,
                    start_time=datetime.now(timezone.utc),
                    end_time=datetime.now(timezone.utc),
                    status=status,
                    input_files=files_json,
                    input_count=input_count,
                    valid_count=valid_count,
                    rejected_count=rejected_count,
                    duplicate_count=duplicate_count,
                    failure_details=failure_details,
                )
                session.add(run)
