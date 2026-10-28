"""Transactional database writer with batching, deduplication, and lineage tracking."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple, Optional
from sqlalchemy import select, func, text
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from .connection import DatabaseManager
from .schema import PipelineRunModel, LogEventModel, RejectedRecordModel
from ..models import LogEventRecord, RejectedRecord


class DatabaseWriter:
    """Handles high-throughput, idempotent batch writes to analytical tables."""

    def __init__(self, db_manager: DatabaseManager, batch_size: int = 5000):
        self.db = db_manager
        self.batch_size = max(1, batch_size)
        self.is_mysql = "mysql" in self.db._url_obj.drivername
        self.is_sqlite = "sqlite" in self.db._url_obj.drivername

    def start_run(self, run_id: str, input_files: List[str]) -> None:
        """Initialize pipeline run ledger row with RUNNING status."""
        with self.db.session_scope() as session:
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
            session.merge(run)

    def write_events_batch(
        self,
        events: List[LogEventRecord],
    ) -> Tuple[int, int]:
        """Write a batch of LogEventRecord items idempotently.

        Returns:
            Tuple of (inserted_count, duplicate_count).
        """
        if not events:
            return 0, 0

        # In-batch deduplication by event_id
        unique_events: Dict[str, LogEventRecord] = {}
        in_batch_duplicates = 0
        for ev in events:
            if ev.event_id in unique_events:
                in_batch_duplicates += 1
            else:
                unique_events[ev.event_id] = ev

        event_rows: List[Dict[str, Any]] = [
            {
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
            for ev in unique_events.values()
        ]

        inserted_count = 0
        db_duplicates = 0

        with self.db.session_scope() as session:
            if self.is_mysql:
                # Chunk into batch_size
                for i in range(0, len(event_rows), self.batch_size):
                    chunk = event_rows[i : i + self.batch_size]
                    stmt = mysql_insert(LogEventModel).values(chunk)
                    # INSERT IGNORE bypasses duplicates based on PRIMARY KEY (event_id)
                    stmt = stmt.prefix_with("IGNORE")
                    res = session.execute(stmt)
                    rows_added = res.rowcount
                    inserted_count += rows_added
                    db_duplicates += len(chunk) - rows_added

            elif self.is_sqlite:
                for i in range(0, len(event_rows), self.batch_size):
                    chunk = event_rows[i : i + self.batch_size]
                    stmt = (
                        sqlite_insert(LogEventModel)
                        .values(chunk)
                        .on_conflict_do_nothing(index_elements=["event_id"])
                    )
                    res = session.execute(stmt)
                    rows_added = res.rowcount
                    inserted_count += rows_added
                    db_duplicates += len(chunk) - rows_added

            else:
                # Generic fallback for any other SQL dialect
                for row in event_rows:
                    existing = session.get(LogEventModel, row["event_id"])
                    if existing is None:
                        session.add(LogEventModel(**row))
                        inserted_count += 1
                    else:
                        db_duplicates += 1

        total_duplicates = in_batch_duplicates + db_duplicates
        return inserted_count, total_duplicates

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

        with self.db.session_scope() as session:
            for i in range(0, len(rows), self.batch_size):
                chunk = rows[i : i + self.batch_size]
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
    ) -> None:
        """Update pipeline run ledger with terminal status and final reconciliation counts."""
        with self.db.session_scope() as session:
            run = session.get(PipelineRunModel, run_id)
            if run:
                run.end_time = datetime.now(timezone.utc)
                run.status = status
                run.input_count = input_count
                run.valid_count = valid_count
                run.rejected_count = rejected_count
                run.duplicate_count = duplicate_count
                run.failure_details = failure_details
            else:
                # If run record wasn't created yet (e.g. fatal abort on init)
                run = PipelineRunModel(
                    run_id=run_id,
                    start_time=datetime.now(timezone.utc),
                    end_time=datetime.now(timezone.utc),
                    status=status,
                    input_files="[]",
                    input_count=input_count,
                    valid_count=valid_count,
                    rejected_count=rejected_count,
                    duplicate_count=duplicate_count,
                    failure_details=failure_details,
                )
                session.add(run)
