"""Semi-structured text application log parser with documented formats."""

from __future__ import annotations

import math
import re
from pathlib import Path
from typing import Any, Dict, Generator, Optional
from .base import BaseLogParser, RawLogRecord, normalize_source_path

# Primary HTTP access pattern:
# Example: 2026-10-28 12:00:10.100 [INFO] [auth-service] GET /api/v1/session 200 12.5ms - Active session validated
HTTP_LOG_PATTERN = re.compile(
    r"^(?P<timestamp>\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?)\s+"
    r"\[(?P<severity>[^\]]+)\]\s+"
    r"\[(?P<service>[^\]]+)\]\s+"
    r"(?:(?P<method>GET|POST|PUT|DELETE|PATCH|HEAD|OPTIONS|CONNECT|TRACE)\s+)?(?P<endpoint>/\S*)\s+"
    r"(?P<http_status>\d{3})\s+"
    r"(?P<latency_raw>\S+?)ms"
    r"(?:\s*(?:-\s*|\s+)(?P<message>.*))?$",
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

# Pattern to detect records that appear to be HTTP access logs but fail strict HTTP parsing,
# preventing them from silently falling back to the general application parser.
APPARENT_HTTP_LOG_PATTERN = re.compile(
    r"""^(?:
        # 1. Starts with HTTP method (with optional quotes) and an endpoint or path
        ["']?(?:GET|POST|PUT|DELETE|PATCH|HEAD|OPTIONS|CONNECT|TRACE)\b\s*(?:/\S*|https?://\S*)
        |
        # 2. Starts with HTTP method and a status code or latency or HTTP protocol
        ["']?(?:GET|POST|PUT|DELETE|PATCH|HEAD|OPTIONS|CONNECT|TRACE)\b\s+(?:\d{2,4}\b|\S+ms\b|HTTP/\d)
        |
        # 3. Starts with HTTP method and any token followed by status code, latency, or protocol
        ["']?(?:GET|POST|PUT|DELETE|PATCH|HEAD|OPTIONS|CONNECT|TRACE)\b\s+\S+\s+(?:\d{2,4}\b|\S+ms\b|HTTP/\d)
        |
        # 4. Any method-like token + path starting with / + status code or latency
        [A-Za-z]+\s+/\S+\s+(?:\d{2,4}\b|\S+ms\b|HTTP/\d)
        |
        # 5. Starts with an endpoint path followed by status code, latency, or HTTP protocol
        /\S+\s+(?:\d{2,4}\b|\S+ms\b|HTTP/\d)
        |
        # 6. Quoted HTTP request line (e.g. "GET /path HTTP/1.1" ...)
        ["'][A-Za-z]+\s+/\S*
    )""",
    re.IGNORECASE | re.VERBOSE,
)

# Optional helper pattern to extract trailing latency like 'in 1500ms' or 'latency: 42ms'
INLINE_LATENCY_PATTERN = re.compile(
    r"(?:\bin\s+|latency\s*[:=]?\s*)([+-]?[0-9A-Za-z.]+)\s*ms\b",
    re.IGNORECASE,
)


def parse_latency(raw_val: str) -> tuple[Optional[float], Optional[str]]:
    """Parse and validate raw latency value in milliseconds.

    Returns:
        (parsed_float, None) if valid nonnegative finite float.
        (None, error_message) if malformed, negative, or non-finite.
    """
    clean_val = raw_val.strip()
    try:
        val = float(clean_val)
    except (ValueError, TypeError):
        return None, f"MALFORMED_LATENCY: Invalid numeric format '{raw_val}'"
    if not math.isfinite(val):
        return None, f"INVALID_LATENCY: Latency must be a finite number, got '{raw_val}'"
    if val < 0.0:
        return None, f"NEGATIVE_LATENCY: Latency cannot be negative, got {val}"
    if val == 0.0:
        val = 0.0
    return val, None


class TextLogParser(BaseLogParser):
    """Parser for semi-structured text application logs."""

    def parse_file(
        self,
        file_path: Path,
        source_name: Optional[str] = None,
    ) -> Generator[RawLogRecord, None, None]:
        filename = source_name or normalize_source_path(file_path)
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
            lat_val, lat_err = parse_latency(d["latency_raw"])
            if lat_err is not None:
                return None, f"INVALID_LOG_RECORD: {lat_err}"

            status_val = int(d["http_status"])
            if not (100 <= status_val <= 599):
                return None, f"INVALID_LOG_RECORD: HTTP status code must be between 100 and 599, got {status_val}"

            res: Dict[str, Any] = {
                "timestamp": d["timestamp"],
                "severity": d["severity"].strip(),
                "service": d["service"].strip(),
                "endpoint": d["endpoint"],
                "http_status": status_val,
                "latency_ms": lat_val,
                "message": d["message"].strip() if d["message"] else f"{d.get('method') or ''} {d['endpoint']}".strip(),
            }
            return res, None

        # 2. Try General Structured Application Pattern
        m_gen = GENERAL_LOG_PATTERN.match(line)
        if m_gen:
            d = m_gen.groupdict()
            msg = d["message"].strip()

            # Guard against malformed HTTP logs falling back to general application logs
            if APPARENT_HTTP_LOG_PATTERN.match(msg):
                return None, f"MALFORMED_HTTP_LOG: Record appears to be an HTTP access log but does not conform to documented HTTP access format: '{msg}'"

            latency_ms: Optional[float] = None
            m_lat = INLINE_LATENCY_PATTERN.search(msg)
            if m_lat:
                lat_val, lat_err = parse_latency(m_lat.group(1))
                if lat_err is not None:
                    return None, f"INVALID_LOG_RECORD: {lat_err}"
                latency_ms = lat_val

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
