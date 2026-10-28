"""Factory and parser auto-detection router."""

from __future__ import annotations

from pathlib import Path
from .base import BaseLogParser
from .json_parser import JsonLogParser
from .text_parser import TextLogParser


def get_parser_for_file(file_path: Path) -> BaseLogParser:
    """Return the appropriate parser based on file extension and format sniffing."""
    suffix = file_path.suffix.lower()
    if suffix in (".json", ".jsonl", ".ndjson"):
        return JsonLogParser()
    if suffix in (".log", ".txt", ".out"):
        return TextLogParser()

    # Fallback: probe first line to detect JSON vs text
    try:
        with open(file_path, "r", encoding="utf-8", errors="replace") as f:
            first_line = f.readline().strip()
            if first_line.startswith("{") or first_line.startswith("["):
                return JsonLogParser()
    except Exception:
        pass

    return TextLogParser()
