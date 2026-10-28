"""Data models, canonical definitions, and schemas for LogAnalyticsPipeline.

Defines canonical log severity levels, normalized event schemas, quarantine records,
and pipeline run accounting structures.
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field, field_validator, model_validator


class CanonicalSeverity(str, Enum):
    """Canonical log severity levels supported by the analytical platform."""

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


# Mapping from common variants and alias terms to CanonicalSeverity
SEVERITY_ALIAS_MAP: Dict[str, CanonicalSeverity] = {
    "DEBUG": CanonicalSeverity.DEBUG,
    "DEBG": CanonicalSeverity.DEBUG,
    "TRACE": CanonicalSeverity.DEBUG,
    "VERBOSE": CanonicalSeverity.DEBUG,
    "INFO": CanonicalSeverity.INFO,
    "INFORMATION": CanonicalSeverity.INFO,
    "NOTICE": CanonicalSeverity.INFO,
    "WARN": CanonicalSeverity.WARNING,
    "WARNING": CanonicalSeverity.WARNING,
    "ERR": CanonicalSeverity.ERROR,
    "ERROR": CanonicalSeverity.ERROR,
    "CRIT": CanonicalSeverity.CRITICAL,
    "CRITICAL": CanonicalSeverity.CRITICAL,
    "FATAL": CanonicalSeverity.CRITICAL,
    "ALERT": CanonicalSeverity.CRITICAL,
    "EMERGENCY": CanonicalSeverity.CRITICAL,
}


def normalize_severity(raw_severity: Any) -> CanonicalSeverity:
    """Normalize raw severity string to canonical enum.

    Args:
        raw_severity: Raw severity value from input log.

    Returns:
        CanonicalSeverity enum member.

    Raises:
        ValueError: If severity is unrecognized or empty.
    """
    if raw_severity is None:
        raise ValueError("Severity is missing or null")
    if isinstance(raw_severity, CanonicalSeverity):
        return raw_severity
    if hasattr(raw_severity, "value"):
        raw_severity = raw_severity.value
    clean = str(raw_severity).strip().upper()
    if not clean:
        raise ValueError("Severity is empty")
    if clean in SEVERITY_ALIAS_MAP:
        return SEVERITY_ALIAS_MAP[clean]
    raise ValueError(f"Unrecognized severity: {raw_severity}")


def normalize_utc_timestamp(raw_ts: Any) -> datetime:
    """Parse and normalize any timestamp representation into timezone-aware UTC datetime.

    Policy:
    - Timezone-naive timestamps are assumed to be recorded in UTC (standard distributed practice).
    - Timezone-aware timestamps are converted to UTC via `.astimezone(timezone.utc)`.
    - Epoch numeric seconds or milliseconds are converted directly.

    Args:
        raw_ts: String, integer, float, or datetime representation.

    Returns:
        UTC datetime object.

    Raises:
        ValueError: If timestamp cannot be parsed into a valid datetime.
    """
    if raw_ts is None:
        raise ValueError("Timestamp is missing or null")

    if isinstance(raw_ts, datetime):
        if raw_ts.tzinfo is None:
            return raw_ts.replace(tzinfo=timezone.utc)
        return raw_ts.astimezone(timezone.utc)

    # Check numeric epoch (e.g., 1700000000 or 1700000000123)
    if isinstance(raw_ts, (int, float)):
        # If timestamp is > 1e11 it's likely milliseconds
        if raw_ts > 1e11:
            return datetime.fromtimestamp(raw_ts / 1000.0, tz=timezone.utc)
        return datetime.fromtimestamp(float(raw_ts), tz=timezone.utc)

    # String parsing
    ts_str = str(raw_ts).strip()
    if not ts_str:
        raise ValueError("Timestamp string is empty")

    # Fast-path ISO format parsing
    # Handle 'Z' suffix replacing with '+00:00'
    normalized_str = ts_str.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(normalized_str)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        pass

    # Fallback to dateutil/common formats
    common_formats = [
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%d/%b/%Y:%H:%M:%S %z",
        "%b %d %H:%M:%S",
    ]
    for fmt in common_formats:
        try:
            dt = datetime.strptime(ts_str, fmt)
            if dt.tzinfo is None:
                return dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except ValueError:
            continue

    raise ValueError(f"Unable to parse timestamp: {raw_ts}")


def generate_deterministic_event_id(
    service: str,
    endpoint: Optional[str],
    timestamp: datetime,
    source_file: str,
    source_line: int,
    message: str,
) -> str:
    """Generate a deterministic 32-character SHA-256 event ID.

    Identity Assumption:
    A log event is uniquely defined by its origin (source_file, source_line),
    service, endpoint, normalized UTC timestamp, and message.
    This guarantees:
    1. Idempotent reprocessing of identical log files without creating duplicate rows.
    2. Legitimate separate events with identical messages (e.g. health checks)
       occurring on distinct lines or timestamps are NOT collated or dropped.
    """
    ep = endpoint or ""
    ts_iso = timestamp.isoformat()
    raw_key = f"{source_file}:{source_line}:{service}:{ep}:{ts_iso}:{message}"
    return hashlib.sha256(raw_key.encode("utf-8")).hexdigest()[:32]


class LogEventRecord(BaseModel):
    """Normalized, validated application log event."""

    event_id: str = Field(description="Unique deterministic event identifier")
    timestamp: datetime = Field(description="Normalized UTC timestamp of event")
    severity: CanonicalSeverity = Field(description="Normalized canonical severity level")
    service: str = Field(min_length=1, description="Originating service or application name")
    endpoint: Optional[str] = Field(default=None, description="HTTP endpoint or route path if applicable")
    http_status: Optional[int] = Field(default=None, description="HTTP response status code (100-599)")
    latency_ms: Optional[float] = Field(default=None, description="Request execution latency in milliseconds")
    message: str = Field(min_length=1, description="Log message or event body")
    source_file: str = Field(min_length=1, description="Originating log filename or source path")
    source_line: int = Field(ge=1, description="1-indexed line number in source log file")
    run_id: str = Field(description="Pipeline execution identifier")

    @field_validator("http_status")
    @classmethod
    def validate_http_status(cls, v: Optional[int]) -> Optional[int]:
        if v is not None and not (100 <= v <= 599):
            raise ValueError(f"HTTP status code must be between 100 and 599, got {v}")
        return v

    @field_validator("latency_ms")
    @classmethod
    def validate_latency(cls, v: Optional[float]) -> Optional[float]:
        if v is not None and v < 0.0:
            raise ValueError(f"Latency cannot be negative, got {v}")
        return v

    @field_validator("service", "message", "source_file")
    @classmethod
    def validate_non_empty_strings(cls, v: str) -> str:
        s = v.strip()
        if not s:
            raise ValueError("Field cannot be empty or whitespace only")
        return s

    @model_validator(mode="before")
    @classmethod
    def normalize_fields(cls, data: Any) -> Any:
        """Pre-validation hook for normalization of raw fields."""
        if not isinstance(data, dict):
            return data

        # Normalize timestamp
        if "timestamp" in data:
            data["timestamp"] = normalize_utc_timestamp(data["timestamp"])

        # Normalize severity
        if "severity" in data:
            data["severity"] = normalize_severity(data["severity"])

        # Auto-generate event_id if not present or empty
        if not data.get("event_id") and "timestamp" in data and "service" in data:
            data["event_id"] = generate_deterministic_event_id(
                service=str(data.get("service", "")),
                endpoint=data.get("endpoint"),
                timestamp=data["timestamp"],
                source_file=str(data.get("source_file", "unknown")),
                source_line=int(data.get("source_line", 1)),
                message=str(data.get("message", "")),
            )
        return data


class RejectedRecord(BaseModel):
    """Schema for invalid or malformed records quarantined during ingestion."""

    run_id: str = Field(description="Pipeline run execution ID")
    source_file: str = Field(description="Path or name of source file")
    source_line: int = Field(description="Source file line number")
    raw_record: str = Field(description="Original unparsed log record content")
    rejection_reasons: List[str] = Field(description="Explicit list of validation or parser failure reasons")
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Quarantine insertion timestamp in UTC",
    )


class PipelineRunRecord(BaseModel):
    """Metadata and execution metrics for a pipeline batch run."""

    run_id: str = Field(description="Unique run identifier (UUID or timestamp-based)")
    start_time: datetime = Field(description="Run start time in UTC")
    end_time: Optional[datetime] = Field(default=None, description="Run completion time in UTC")
    status: str = Field(default="RUNNING", description="Status: RUNNING, SUCCESS, PARTIAL_SUCCESS, FAILED")
    input_files: List[str] = Field(default_factory=list, description="List of processed files")
    input_count: int = Field(default=0, ge=0, description="Total raw records scanned")
    valid_count: int = Field(default=0, ge=0, description="Valid records ingested into database")
    rejected_count: int = Field(default=0, ge=0, description="Invalid records sent to quarantine")
    duplicate_count: int = Field(default=0, ge=0, description="Duplicate records safely bypassed/handled")
    failure_details: Optional[str] = Field(default=None, description="Exception message or traceback on fatal abort")
