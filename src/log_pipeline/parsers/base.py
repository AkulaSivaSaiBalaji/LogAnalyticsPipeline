"""Base definitions and abstract contracts for log parsers."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Generator, Optional


@dataclass
class RawLogRecord:
    """Represents a scanned raw log record before schema validation."""

    source_file: str
    source_line: int
    raw_text: str
    parsed_dict: Optional[Dict[str, Any]] = None
    parse_error: Optional[str] = None


class BaseLogParser(ABC):
    """Abstract interface for format-specific log parsers."""

    @abstractmethod
    def parse_file(self, file_path: Path) -> Generator[RawLogRecord, None, None]:
        """Parse the given file and yield RawLogRecord items.

        Must never abort on malformed lines; malformed inputs must yield
        RawLogRecord with parse_error populated and continue parsing.
        """
        pass
