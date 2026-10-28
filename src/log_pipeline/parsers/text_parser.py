"""Semi-structured text application log parser with documented formats."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, Generator, Optional
from .base import BaseLogParser, RawLogRecord

# Primary HTTP access pattern:
# Example: 2026-10-28 12:00:10.100 [INFO] [auth-service] GET /api/v1/session 200 12.5ms - Active session validated
HTTP_LOG_PATTERN = re.compile(
    r"^(?P<timestamp>\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?)\s+"
    r"\[(?P<severity>[^\]]+)\]\s+"
    r"\[(?P<service>[^\]]+)\]\s+"
    r"(?:(?P<method>[A-Z]+)\s+)?(?P<endpoint>/\S*)\s+"
    r"(?P<http_status>\d{3})\s+"
    r"(?P<latency_ms>[\d.]+)ms\s*"
    r"(?:-\s*|\s+)(?P<message>.*)$",
    re.IGNORECASE,
)

# Secondary general application log pattern:
# Example: 2026-10-28 12:01:00.000 [CRITICAL] [order-db] Database pool connection exhausted
# Example: 2026-10-28 12:00:25.000 [INFO] [analytics-worker] Batch metric rollup completed in 1500ms
GENERAL_LOG_PATTERN = re.compile(
    r"^(?P<timestamp>\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?)\s+"
    r"\[(?P<severity>[^\]]+)\]\s+"
    r"\[(?P<service>[^\]]+)\]\s*"
    r"(?:-\s*|\s+)?(?P<message>.*)$",
    re.IGNORECASE,
)

# Optional helper pattern to extract trailing latency like 'in 1500ms' or 'latency: 42ms'
INLINE_LATENCY_PATTERN = re.compile(r"(?:in|latency[:=]?)\s+([\d.]+)\s*ms\b", re.IGNORECASE)


class TextLogParser(BaseLogParser):
    """Parser for semi-structured text application logs."""

    def parse_file(self, file_path: Path) -> Generator[RawLogRecord, None, None]:
        filename = file_path.name
        if not file_path.exists():
            yield RawLogRecord(
                source_file=filename,
                source_line=1,
                raw_text="",
                parse_error=f"FILE_NOT_FOUND: {file_path}",
            )
            return

        with open(file_path, "r", encoding="utf-8", errors="replace") as f:
            for line_no, line in enumerate(f, start=1):
                raw_text = line.rstrip("\r\n")
                if not raw_text.strip():
                    continue

                parsed_dict, err = self._parse_line(raw_text)
                yield RawLogRecord(
                    source_file=filename,
                    source_line=line_no,
                    raw_text=raw_text,
                    parsed_dict=parsed_dict,
                    parse_error=err,
                )

    def _parse_line(self, line: str) -> tuple[Optional[Dict[str, Any]], Optional[str]]:
        # 1. Try HTTP Access Pattern
        m_http = HTTP_LOG_PATTERN.match(line)
        if m_http:
            d = m_http.groupdict()
            res: Dict[str, Any] = {
                "timestamp": d["timestamp"],
                "severity": d["severity"].strip(),
                "service": d["service"].strip(),
                "endpoint": d["endpoint"],
                "http_status": int(d["http_status"]),
                "latency_ms": float(d["latency_ms"]),
                "message": d["message"].strip() if d["message"] else f"{d.get('method') or ''} {d['endpoint']}".strip(),
            }
            return res, None

        # 2. Try General Structured Application Pattern
        m_gen = GENERAL_LOG_PATTERN.match(line)
        if m_gen:
            d = m_gen.groupdict()
            msg = d["message"].strip()
            latency_ms: Optional[float] = None
            m_lat = INLINE_LATENCY_PATTERN.search(msg)
            if m_lat:
                try:
                    latency_ms = float(m_lat.group(1))
                except ValueError:
                    pass

            res = {
                "timestamp": d["timestamp"],
                "severity": d["severity"].strip(),
                "service": d["service"].strip(),
                "endpoint": None,
                "http_status": None,
                "latency_ms": latency_ms,
                "message": msg,
            }
            return res, None

        return None, "UNRECOGNIZED_TEXT_LOG_FORMAT: Line does not conform to documented [TIMESTAMP] [SEVERITY] [SERVICE] format"
