"""CSV report generator and reconciliation report writer."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict
import pandas as pd


class ReportWriter:
    """Writes machine-readable CSV reports and reconciliation audit files."""

    def __init__(self, reports_dir: Path, run_id: str):
        self.reports_dir = reports_dir
        self.run_id = run_id
        self.reports_dir.mkdir(parents=True, exist_ok=True)

    def write_hourly_metrics(self, df: pd.DataFrame) -> Path:
        """Export hourly service metrics to CSV."""
        path = self.reports_dir / f"{self.run_id}_hourly_metrics.csv"
        df.to_csv(path, index=False, na_rep="")
        return path

    def write_service_summary(self, df: pd.DataFrame) -> Path:
        """Export overall service summary metrics to CSV."""
        path = self.reports_dir / f"{self.run_id}_service_summary.csv"
        df.to_csv(path, index=False, na_rep="")
        return path

    def write_reconciliation_report(self, recon_data: Dict[str, Any]) -> Path:
        """Export reconciliation audit ledger to CSV."""
        path = self.reports_dir / f"{self.run_id}_reconciliation.csv"
        fieldnames = [
            "run_id",
            "input_count",
            "valid_count",
            "rejected_count",
            "duplicate_count",
            "accounted_total",
            "discrepancy",
            "is_balanced",
            "status",
        ]
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerow(recon_data)
        return path
