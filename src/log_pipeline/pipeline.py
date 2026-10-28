"""LogAnalyticsPipeline orchestrator.

Coordinates ingestion, parsing, schema validation, quarantine routing,
idempotent database storage, and analytical report generation.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from .config import PipelineConfig, load_config
from .db.connection import DatabaseManager
from .db.writer import DatabaseWriter
from .models import LogEventRecord, RejectedRecord
from .parsers.detector import get_parser_for_file
from .validation.validator import RecordValidator
from .validation.quarantine import QuarantineHandler
from .metrics.calculator import MetricsCalculator
from .metrics.reporter import ReportWriter

logger = logging.getLogger("log_pipeline")


class LogAnalyticsPipeline:
    """Production log analytics pipeline engine."""

    def __init__(
        self,
        config: Optional[PipelineConfig] = None,
        db_manager: Optional[DatabaseManager] = None,
    ):
        self.config = config or load_config()
        self.config.ensure_directories()
        self.db = db_manager or DatabaseManager(config=self.config.database)
        self.writer = DatabaseWriter(self.db, batch_size=self.config.batch_size)

    def run(
        self,
        files: Optional[List[str | Path]] = None,
        run_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Execute end-to-end log processing pipeline."""
        active_run_id = run_id or f"run_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
        start_time = datetime.now(timezone.utc)

        # 1. Resolve input files
        resolved_files: List[Path] = []
        if files:
            for f in files:
                p = Path(f)
                if p.is_dir():
                    resolved_files.extend(sorted([x for x in p.iterdir() if x.is_file()]))
                elif p.exists():
                    resolved_files.append(p)
        else:
            if self.config.input_dir.exists():
                resolved_files = sorted([x for x in self.config.input_dir.iterdir() if x.is_file()])

        input_file_names = [p.name for p in resolved_files]
        logger.info(f"Starting pipeline run {active_run_id} on {len(resolved_files)} files: {input_file_names}")

        # Ensure DB tables exist
        self.db.create_tables()
        self.writer.start_run(active_run_id, input_file_names)

        validator = RecordValidator(run_id=active_run_id)
        quarantine = QuarantineHandler(self.config.quarantine_dir, run_id=active_run_id)
        reporter = ReportWriter(self.config.reports_dir, run_id=active_run_id)

        input_count = 0
        valid_count = 0
        duplicate_count = 0
        rejected_count = 0

        valid_batch: List[LogEventRecord] = []
        rejected_batch: List[RejectedRecord] = []
        all_valid_events_for_metrics: List[LogEventRecord] = []

        try:
            for file_path in resolved_files:
                parser = get_parser_for_file(file_path)
                for raw_record in parser.parse_file(file_path):
                    input_count += 1
                    res = validator.validate(raw_record)

                    if res.is_valid and res.valid_record:
                        valid_batch.append(res.valid_record)
                        all_valid_events_for_metrics.append(res.valid_record)
                        if len(valid_batch) >= self.config.batch_size:
                            ins, dup = self.writer.write_events_batch(valid_batch)
                            valid_count += ins
                            duplicate_count += dup
                            valid_batch = []
                    else:
                        if res.rejected_record:
                            rejected_batch.append(res.rejected_record)
                            quarantine.add(res.rejected_record)
                        if len(rejected_batch) >= self.config.batch_size:
                            self.writer.write_rejected_batch(rejected_batch)
                            rejected_count += len(rejected_batch)
                            rejected_batch = []

            # Flush final partial batches
            if valid_batch:
                ins, dup = self.writer.write_events_batch(valid_batch)
                valid_count += ins
                duplicate_count += dup
                valid_batch = []

            if rejected_batch:
                self.writer.write_rejected_batch(rejected_batch)
                rejected_count += len(rejected_batch)
                rejected_batch = []

            # 2. Export quarantine file to disk if any records rejected
            quarantine_file: Optional[Path] = None
            if quarantine.count > 0:
                quarantine_file = quarantine.export_to_disk()

            # 3. Calculate summary metrics & percentiles
            events_df = MetricsCalculator.events_to_dataframe(all_valid_events_for_metrics)
            hourly_df, service_df = MetricsCalculator.calculate_aggregates(events_df)

            hourly_csv = reporter.write_hourly_metrics(hourly_df)
            service_csv = reporter.write_service_summary(service_df)

            # 4. Reconciliation accounting
            recon = MetricsCalculator.calculate_reconciliation(
                run_id=active_run_id,
                input_count=input_count,
                valid_count=valid_count,
                rejected_count=rejected_count,
                duplicate_count=duplicate_count,
            )
            recon_csv = reporter.write_reconciliation_report(recon)

            status = "SUCCESS" if rejected_count == 0 else "PARTIAL_SUCCESS"
            self.writer.complete_run(
                run_id=active_run_id,
                status=status,
                input_count=input_count,
                valid_count=valid_count,
                rejected_count=rejected_count,
                duplicate_count=duplicate_count,
            )

            end_time = datetime.now(timezone.utc)
            duration_sec = round((end_time - start_time).total_seconds(), 3)

            return {
                "run_id": active_run_id,
                "status": status,
                "duration_seconds": duration_sec,
                "input_count": input_count,
                "valid_count": valid_count,
                "rejected_count": rejected_count,
                "duplicate_count": duplicate_count,
                "reconciliation": recon,
                "quarantine_file": str(quarantine_file) if quarantine_file else None,
                "hourly_report": str(hourly_csv),
                "service_report": str(service_csv),
                "reconciliation_report": str(recon_csv),
                "hourly_metrics": hourly_df.to_dict(orient="records"),
                "service_metrics": service_df.to_dict(orient="records"),
            }

        except Exception as e:
            logger.exception(f"Fatal error during pipeline run {active_run_id}")
            # Sanitize error detail
            err_msg = f"{type(e).__name__}: {str(e)[:200]}"
            self.writer.complete_run(
                run_id=active_run_id,
                status="FAILED",
                input_count=input_count,
                valid_count=valid_count,
                rejected_count=rejected_count,
                duplicate_count=duplicate_count,
                failure_details=err_msg,
            )
            raise
