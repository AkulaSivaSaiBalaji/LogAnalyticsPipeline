"""Schema validation and quarantine routing engine."""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional
from pydantic import ValidationError

from ..models import LogEventRecord, RejectedRecord
from ..parsers.base import RawLogRecord


@dataclass
class ValidationResult:
    """Outcome of validating a single RawLogRecord."""

    is_valid: bool
    valid_record: Optional[LogEventRecord] = None
    rejected_record: Optional[RejectedRecord] = None


class RecordValidator:
    """Validates raw parsed dictionary payloads against normalized schema requirements."""

    def __init__(self, run_id: str):
        self.run_id = run_id

    def validate(self, raw: RawLogRecord) -> ValidationResult:
        """Validate raw record, routing to valid LogEventRecord or RejectedRecord."""
        # 1. Check if parser already reported an error
        if raw.parse_error:
            reasons = [raw.parse_error]
            rej = RejectedRecord(
                run_id=self.run_id,
                source_file=raw.source_file,
                source_line=raw.source_line,
                raw_record=raw.raw_text,
                rejection_reasons=reasons,
            )
            return ValidationResult(is_valid=False, rejected_record=rej)

        if not isinstance(raw.parsed_dict, dict):
            rej = RejectedRecord(
                run_id=self.run_id,
                source_file=raw.source_file,
                source_line=raw.source_line,
                raw_record=raw.raw_text,
                rejection_reasons=["NO_DATA_EXTRACTED: Parsed payload is empty or not a dictionary"],
            )
            return ValidationResult(is_valid=False, rejected_record=rej)

        # 2. Check required fields presence
        rejection_reasons: List[str] = []
        data = dict(raw.parsed_dict)
        data["source_file"] = raw.source_file
        data["source_line"] = raw.source_line
        data["run_id"] = self.run_id

        # Missing required fields pre-check
        for req_field in ("timestamp", "severity", "service", "message"):
            val = data.get(req_field)
            if val is None or (isinstance(val, str) and not val.strip()):
                rejection_reasons.append(f"MISSING_REQUIRED_FIELD: '{req_field}' is absent or empty")

        if rejection_reasons:
            rej = RejectedRecord(
                run_id=self.run_id,
                source_file=raw.source_file,
                source_line=raw.source_line,
                raw_record=raw.raw_text,
                rejection_reasons=rejection_reasons,
            )
            return ValidationResult(is_valid=False, rejected_record=rej)

        # 3. Pydantic schema validation & normalization
        try:
            valid_event = LogEventRecord(**data)
            return ValidationResult(is_valid=True, valid_record=valid_event)
        except ValidationError as e:
            for err in e.errors():
                loc = ".".join(str(x) for x in err["loc"])
                msg = err["msg"]
                rejection_reasons.append(f"SCHEMA_VALIDATION_ERROR on '{loc}': {msg}")
        except Exception as e:
            rejection_reasons.append(f"NORMALIZATION_ERROR: {type(e).__name__}: {str(e)}")

        rej = RejectedRecord(
            run_id=self.run_id,
            source_file=raw.source_file,
            source_line=raw.source_line,
            raw_record=raw.raw_text,
            rejection_reasons=rejection_reasons,
        )
        return ValidationResult(is_valid=False, rejected_record=rej)
