"""LogAnalyticsPipeline orchestrator.

Coordinates ingestion, parsing, schema validation, quarantine routing,
idempotent database storage, and analytical report generation.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .config import PipelineConfig, load_config
from .db.connection import DatabaseManager
from .db.schema import PipelineRunModel
from .db.writer import DatabaseWriter
from .models import LogEventRecord, RejectedRecord
from .parsers.base import normalize_source_path, resolve_source_identities
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

    def _flush_valid_batch(
        self,
        valid_batch: List[LogEventRecord],
        all_valid_events_for_metrics: List[LogEventRecord],
    ) -> Tuple[int, int]:
        """Write buffered valid events and clear the batch buffer."""
        if not valid_batch:
            return 0, 0
        write_res = self.writer.write_events_batch(valid_batch)
        ins, dup = write_res
        all_valid_events_for_metrics.extend(write_res.inserted_events)
        valid_batch.clear()
        return ins, dup

    def _flush_rejected_batch(
        self,
        rejected_batch: List[RejectedRecord],
    ) -> int:
        """Write buffered rejected records and clear the batch buffer."""
        if not rejected_batch:
            return 0
        flushed_count = self.writer.write_rejected_batch(rejected_batch)
        rejected_batch.clear()
        return flushed_count

    def run(
        self,
        files: Optional[List[str | Path]] = None,
        run_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Execute end-to-end log processing pipeline."""
        active_run_id = run_id or f"run_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
        self.active_run_id = active_run_id
        start_time = datetime.now(timezone.utc)

        # Ensure DB tables exist
        self.db.create_tables()

        # Ensure run_id cannot overwrite an existing terminal run
        with self.db.session_scope() as session:
            existing_run = session.get(PipelineRunModel, active_run_id)
            if existing_run is not None and existing_run.status in ("SUCCESS", "PARTIAL_SUCCESS", "FAILED"):
                raise ValueError(
                    f"Cannot execute pipeline with run_id '{active_run_id}': run already completed with status '{existing_run.status}'"
                )

        input_count = 0
        valid_count = 0
        duplicate_count = 0
        rejected_count = 0

        valid_batch: List[LogEventRecord] = []
        rejected_batch: List[RejectedRecord] = []
        all_valid_events_for_metrics: List[LogEventRecord] = []

        validator = RecordValidator(run_id=active_run_id)
        quarantine = QuarantineHandler(self.config.quarantine_dir, run_id=active_run_id)
        reporter = ReportWriter(self.config.reports_dir, run_id=active_run_id)

        manifest_requested_files: List[str] = []

        try:
            # 1. Resolve and validate input files and logical source names
            candidate_inputs: List[Path | Tuple[Path, str]] = []
            dir_roots: Dict[Path, Path] = {}

            if files is not None:
                for item in files:
                    if isinstance(item, tuple):
                        p_path, explicit_name = item
                        p = Path(p_path)
                        manifest_requested_files.append(str(p_path))
                        if not p.exists():
                            raise FileNotFoundError(f"Input path does not exist: {p}")
                        candidate_inputs.append((p, str(explicit_name)))
                        continue

                    p = Path(item)
                    manifest_requested_files.append(str(item))
                    if not p.exists():
                        raise FileNotFoundError(f"Input path does not exist: {p}")
                    if p.is_dir():
                        try:
                            is_inside_input = p.resolve().is_relative_to(self.config.input_dir.resolve())
                        except (ValueError, TypeError):
                            is_inside_input = False

                        root_dir = self.config.input_dir if is_inside_input else p
                        for sub in sorted(p.rglob("*")):
                            if sub.is_file():
                                candidate_inputs.append(sub)
                                dir_roots[sub] = root_dir
                    elif p.is_file():
                        candidate_inputs.append(p)
                        try:
                            is_inside_input = p.resolve().is_relative_to(self.config.input_dir.resolve())
                        except (ValueError, TypeError):
                            is_inside_input = False
                        if is_inside_input:
                            dir_roots[p] = self.config.input_dir
            else:
                manifest_requested_files.append(str(self.config.input_dir))
                if not self.config.input_dir.exists():
                    raise FileNotFoundError(f"Configured input directory does not exist: {self.config.input_dir}")
                for sub in sorted(self.config.input_dir.rglob("*")):
                    if sub.is_file():
                        candidate_inputs.append(sub)
                        dir_roots[sub] = self.config.input_dir

            # Deduplicate candidates while preserving order
            unique_inputs: List[Path | Tuple[Path, str]] = []
            seen_paths = set()
            for item in candidate_inputs:
                p = item[0] if isinstance(item, tuple) else item
                res_p = p.resolve()
                if res_p not in seen_paths:
                    seen_paths.add(res_p)
                    unique_inputs.append(item)

            resolved_dict = resolve_source_identities(
                unique_inputs,
                input_dir=self.config.input_dir,
                dir_roots=dir_roots,
            )
            resolved_files: List[Tuple[Path, str]] = [
                (item[0] if isinstance(item, tuple) else item, resolved_dict[item[0] if isinstance(item, tuple) else item])
                for item in unique_inputs
            ]

            input_file_names = [logical_name for _, logical_name in resolved_files]
            logger.info(f"Starting pipeline run {active_run_id} on {len(resolved_files)} files: {input_file_names}")

            # Initialize run ledger
            self.writer.start_run(active_run_id, input_file_names)

            for file_path, source_name in resolved_files:
                parser = get_parser_for_file(file_path)
                for raw_record in parser.parse_file(file_path, source_name=source_name):
                    input_count += 1
                    res = validator.validate(raw_record)

                    if res.is_valid and res.valid_record:
                        valid_batch.append(res.valid_record)
                        if len(valid_batch) >= self.config.batch_size:
                            ins, dup = self._flush_valid_batch(
                                valid_batch, all_valid_events_for_metrics
                            )
                            valid_count += ins
                            duplicate_count += dup
                    else:
                        if res.rejected_record:
                            rejected_batch.append(res.rejected_record)
                            quarantine.add(res.rejected_record)
                        if len(rejected_batch) >= self.config.batch_size:
                            flushed = self._flush_rejected_batch(rejected_batch)
                            rejected_count += flushed

            # Flush final partial batches
            if valid_batch:
                ins, dup = self._flush_valid_batch(
                    valid_batch, all_valid_events_for_metrics
                )
                valid_count += ins
                duplicate_count += dup

            if rejected_batch:
                flushed = self._flush_rejected_batch(rejected_batch)
                rejected_count += flushed

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
            err_msg = f"{type(e).__name__}: {str(e)[:200]}"

            # 1. Attempt to flush any buffered rejected records so they are not lost
            if rejected_batch:
                try:
                    flushed = self._flush_rejected_batch(rejected_batch)
                    rejected_count += flushed
                except Exception as flush_err:
                    logger.error(
                        f"Failed to flush buffered rejected records to DB during failure cleanup for run {active_run_id}: {flush_err}"
                    )

            # 2. Attempt to export quarantine file to disk
            try:
                if quarantine.count > 0:
                    quarantine.export_to_disk()
            except Exception as q_err:
                logger.error(
                    f"Failed to export quarantine to disk during failure cleanup for run {active_run_id}: {q_err}"
                )

            # 3. Attempt to record terminal FAILED status in ledger
            try:
                self.writer.complete_run(
                    run_id=active_run_id,
                    status="FAILED",
                    input_count=input_count,
                    valid_count=valid_count,
                    rejected_count=rejected_count,
                    duplicate_count=duplicate_count,
                    failure_details=err_msg,
                    input_files=manifest_requested_files,
                )
            except Exception as complete_err:
                logger.error(
                    f"Failed to record FAILED status in ledger for run {active_run_id}: {complete_err}"
                )

            # 4. Always re-raise original exception so caller is notified of failure
            raise


if __name__ == "__main__":
    from .cli import main
    import sys
    sys.exit(main())
