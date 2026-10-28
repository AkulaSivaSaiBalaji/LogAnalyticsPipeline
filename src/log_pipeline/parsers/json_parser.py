"""JSON and JSONL log parsers with malformed line recovery."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Generator
from .base import BaseLogParser, RawLogRecord


class JsonLogParser(BaseLogParser):
    """Parser for JSON array and line-delimited JSON (JSONL) application logs."""

    def __init__(self, mode: str = "auto"):
        """Initialize parser.

        Args:
            mode: 'auto' (detect based on extension/content), 'json' (array), or 'jsonl' (lines).
        """
        self.mode = mode

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

        file_mode = self._determine_mode(file_path)

        if file_mode == "json":
            yield from self._parse_json_array(file_path, filename)
        else:
            yield from self._parse_jsonl(file_path, filename)

    def _determine_mode(self, file_path: Path) -> str:
        if self.mode in ("json", "jsonl"):
            return self.mode

        if file_path.suffix.lower() == ".jsonl":
            return "jsonl"
        if file_path.suffix.lower() == ".json":
            # Sniff first non-whitespace character
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    for line in f:
                        stripped = line.strip()
                        if stripped:
                            if stripped.startswith("["):
                                return "json"
                            return "jsonl"
            except Exception:
                return "json"
            return "json"

        return "jsonl"

    def _parse_json_array(self, file_path: Path, filename: str) -> Generator[RawLogRecord, None, None]:
        try:
            content = file_path.read_text(encoding="utf-8").strip()
            if not content:
                # Empty file yields nothing
                return
            data = json.loads(content)
        except Exception as e:
            yield RawLogRecord(
                source_file=filename,
                source_line=1,
                raw_text=content[:500] if "content" in locals() else "",
                parse_error=f"MALFORMED_JSON_FILE: {type(e).__name__}: {str(e)}",
            )
            return

        if not isinstance(data, list):
            yield RawLogRecord(
                source_file=filename,
                source_line=1,
                raw_text=str(data)[:500],
                parse_error=f"UNEXPECTED_SCHEMA: Expected JSON array of objects, got {type(data).__name__}",
            )
            return

        for idx, item in enumerate(data, start=1):
            raw_str = json.dumps(item) if isinstance(item, (dict, list)) else str(item)
            if not isinstance(item, dict):
                yield RawLogRecord(
                    source_file=filename,
                    source_line=idx,
                    raw_text=raw_str,
                    parse_error=f"UNEXPECTED_SCHEMA: Array element is not a JSON object: {type(item).__name__}",
                )
                continue

            yield RawLogRecord(
                source_file=filename,
                source_line=idx,
                raw_text=raw_str,
                parsed_dict=item,
            )

    def _parse_jsonl(self, file_path: Path, filename: str) -> Generator[RawLogRecord, None, None]:
        with open(file_path, "r", encoding="utf-8", errors="replace") as f:
            for line_no, line in enumerate(f, start=1):
                raw_text = line.rstrip("\r\n")
                if not raw_text.strip():
                    # Empty or whitespace lines in JSONL are skipped
                    continue

                try:
                    parsed = json.loads(raw_text)
                    if not isinstance(parsed, dict):
                        yield RawLogRecord(
                            source_file=filename,
                            source_line=line_no,
                            raw_text=raw_text,
                            parse_error=f"UNEXPECTED_SCHEMA: Record is not a JSON object: {type(parsed).__name__}",
                        )
                        continue

                    yield RawLogRecord(
                        source_file=filename,
                        source_line=line_no,
                        raw_text=raw_text,
                        parsed_dict=parsed,
                    )
                except Exception as e:
                    # Malformed JSON recovery: capture error, do not abort
                    yield RawLogRecord(
                        source_file=filename,
                        source_line=line_no,
                        raw_text=raw_text,
                        parse_error=f"MALFORMED_JSON_LINE: {type(e).__name__}: {str(e)}",
                    )
