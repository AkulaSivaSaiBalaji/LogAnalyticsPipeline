"""Base definitions and abstract contracts for log parsers."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Generator, Optional


def is_machine_part(part: str) -> bool:
    """Check if a path component is a machine-specific directory or root."""
    low = part.lower().strip()
    return (
        low in ("users", "home", "appdata", "local", "temp", "tmp")
        or low.startswith("pytest")
        or (len(part) in (2, 3) and part[1] == ":")
        or part in ("/", "\\")
    )


def extract_safe_logical_parts(p: Path) -> list[str]:
    """Extract path components excluding filesystem roots, drive letters, user home and system temp prefixes."""
    raw = [part for part in p.parts if part and part not in ("/", "\\") and not (len(part) in (2, 3) and part[1] == ":")]
    idx = 0
    while idx < len(raw):
        low = raw[idx].lower()
        if low in ("users", "home"):
            idx += 2
            continue
        if low in ("appdata", "local", "temp", "tmp"):
            idx += 1
            continue
        if low == "var" and idx + 1 < len(raw) and raw[idx + 1].lower() in ("log", "tmp", "run"):
            idx += 2
            continue
        if low.startswith(("temp_", "tmp_")) and idx < len(raw) - 2:
            idx += 1
            continue
        break
    return raw[idx:] if idx < len(raw) else [p.name]


def normalize_source_path(file_path: Path | str, base_dir: Optional[Path | str] = None) -> str:
    """Normalize a file path into a stable, portable, forward-slash logical source name.

    Preserves parent folder context (e.g. producer-a/app.jsonl) to distinguish
    identically named files across directories without embedding machine-specific absolute paths.
    """
    p = Path(file_path)

    # 1. If base_dir is supplied and p is located within it
    if base_dir is not None:
        try:
            resolved_p = p.resolve()
            resolved_base = Path(base_dir).resolve()
            if resolved_p.is_relative_to(resolved_base) and resolved_p != resolved_base:
                return resolved_p.relative_to(resolved_base).as_posix()
        except (ValueError, TypeError):
            pass

    # 2. If explicitly relative with directory structure (e.g. 'producer-a/app.jsonl')
    # Note: On Windows, paths starting with '/' or '\' have an anchor even if is_absolute() is False.
    # A path is truly relative only when it has no anchor and is not absolute.
    if not p.is_absolute() and not p.anchor:
        if len(p.parts) > 1:
            return p.as_posix()
        return p.name

    # 3. If located within current working directory (e.g. repo root)
    try:
        resolved_p = p.resolve()
        cwd = Path.cwd().resolve()
        if resolved_p.is_relative_to(cwd) and resolved_p != cwd:
            return resolved_p.relative_to(cwd).as_posix()
    except (ValueError, TypeError):
        pass

    # 4. If path is external/absolute, retain safe logical directory components
    safe = extract_safe_logical_parts(p)
    return "/".join(safe)


def resolve_source_identities(
    file_inputs: list[Path | tuple[Path, str]],
    input_dir: Optional[Path] = None,
    dir_roots: Optional[Dict[Path, Path]] = None,
) -> Dict[Path, str]:
    """Resolve collision-resistant, portable logical source names for a list of input files.

    Guarantees:
    1. Deterministic & Non-Batch-Dependent: The identity of a physical file is stable whether
       processed alone, alongside other files, or in reversed order.
    2. Input Dir Consistency: Files inside input_dir (or a specified directory argument) maintain
       their paths relative to that root.
    3. Safe & Machine-Independent: Strips filesystem roots, drive letters, and user home prefixes.
    4. Fast Fail on Ambiguity: If two distinct files produce the same logical identity, raises
       a descriptive ValueError rather than silently creating an ambiguous or batch-dependent identity.
    """
    dir_roots = dir_roots or {}
    results: Dict[Path, str] = {}
    resolved_input_dir = input_dir.resolve() if input_dir else None

    for item in file_inputs:
        if isinstance(item, tuple):
            p, explicit_name = item
            results[Path(p)] = explicit_name
            continue

        p = Path(item)
        root = dir_roots.get(p)
        if root is not None:
            try:
                res_p = p.resolve()
                res_root = root.resolve()
                if res_p.is_relative_to(res_root) and res_p != res_root:
                    if resolved_input_dir and res_root == resolved_input_dir:
                        results[p] = res_p.relative_to(res_root).as_posix()
                    else:
                        results[p] = f"{res_root.name}/{res_p.relative_to(res_root).as_posix()}"
                    continue
            except (ValueError, TypeError):
                pass

        if resolved_input_dir is not None:
            try:
                res_p = p.resolve()
                if res_p.is_relative_to(resolved_input_dir) and res_p != resolved_input_dir:
                    results[p] = res_p.relative_to(resolved_input_dir).as_posix()
                    continue
            except (ValueError, TypeError):
                pass

            try:
                res_p = p.resolve()
                res_parent = resolved_input_dir.parent
                if res_parent != res_parent.parent and res_p.is_relative_to(res_parent) and res_p != res_parent:
                    results[p] = res_p.relative_to(res_parent).as_posix()
                    continue
            except (ValueError, TypeError):
                pass

        # External fallback
        results[p] = normalize_source_path(p)

    # Collision validation: ensure no two distinct files produced the same identity
    names_to_paths: Dict[str, list[Path]] = {}
    for path, name in results.items():
        names_to_paths.setdefault(name, []).append(path)

    for name, paths in names_to_paths.items():
        unique_resolved = {p.resolve() for p in paths}
        if len(unique_resolved) > 1:
            raise ValueError(
                f"Conflicting logical source identity '{name}' derived for distinct files: {[str(p) for p in paths]}. "
                f"Please provide explicit (path, logical_name) pairs or place files in distinct directories."
            )

    return results


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
    def parse_file(
        self,
        file_path: Path,
        source_name: Optional[str] = None,
    ) -> Generator[RawLogRecord, None, None]:
        """Parse the given file and yield RawLogRecord items.

        Args:
            file_path: Filesystem path to the log file.
            source_name: Optional logical source name or relative path.
                         Defaults to normalize_source_path(file_path) if omitted.

        Must never abort on malformed lines; malformed inputs must yield
        RawLogRecord with parse_error populated and continue parsing.
        """
        pass
