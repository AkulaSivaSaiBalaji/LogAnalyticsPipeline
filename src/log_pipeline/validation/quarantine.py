"""Quarantine persistence and bookkeeping."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List
from ..models import RejectedRecord


class QuarantineHandler:
    """Manages rejected records, reasons tallying, and disk exports."""

    def __init__(self, quarantine_dir: Path, run_id: str):
        self.quarantine_dir = quarantine_dir
        self.run_id = run_id
        self.rejected_records: List[RejectedRecord] = []
        self.reason_counts: Dict[str, int] = {}
        self.quarantine_dir.mkdir(parents=True, exist_ok=True)

    def add(self, record: RejectedRecord) -> None:
        """Add a rejected record and update failure metrics."""
        self.rejected_records.append(record)
        for reason in record.rejection_reasons:
            cat = reason.split(":")[0] if ":" in reason else reason
            self.reason_counts[cat] = self.reason_counts.get(cat, 0) + 1

    @property
    def count(self) -> int:
        return len(self.rejected_records)

    def export_to_disk(self) -> Path:
        """Export quarantined records to a JSONL file."""
        out_path = self.quarantine_dir / f"{self.run_id}_quarantined.jsonl"
        with open(out_path, "w", encoding="utf-8") as f:
            for rec in self.rejected_records:
                line_data = {
                    "run_id": rec.run_id,
                    "source_file": rec.source_file,
                    "source_line": rec.source_line,
                    "rejection_reasons": rec.rejection_reasons,
                    "raw_record": rec.raw_record,
                    "created_at": rec.created_at.isoformat(),
                }
                f.write(json.dumps(line_data) + "\n")
        return out_path
