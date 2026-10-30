"""Base definitions and abstract contracts for log parsers."""

from __future__ import annotations

import os
import re
import sys
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path, PurePath, PurePosixPath, PureWindowsPath
from typing import Any, Dict, Generator, List, Optional, Tuple, Union


def split_logical_path(file_path: Path | PurePath | str) -> Tuple[Optional[str], bool, List[str]]:
    """Parse any path representation (Windows, POSIX, mixed) into (drive, is_absolute, parts).

    Platform-independent: handles Windows backslashes and drive letters on Linux,
    and POSIX forward slashes on Windows.
    """
    raw = str(file_path).strip()
    if not raw:
        return (None, False, [])

    # Replace backslashes with forward slashes for cross-platform segmenting
    s = raw.replace("\\", "/")

    drive: Optional[str] = None
    is_abs: bool = False

    # 1. Check for Windows drive letter: e.g. "C:", "D:", "c:/", "D:\", "D:logs"
    drive_match = re.match(r"^([a-zA-Z]:)(.*)$", s)
    if drive_match:
        drive = drive_match.group(1).upper()
        rest = drive_match.group(2)
        if rest.startswith("/"):
            is_abs = True
            remainder = rest.lstrip("/")
        else:
            is_abs = False
            remainder = rest
    elif s.startswith("//"):
        # UNC path: e.g. //server/share/path
        unc_match = re.match(r"^//([^/]+)/([^/]+)(?:/(.*)|$)", s)
        if unc_match:
            drive = f"//{unc_match.group(1).lower()}/{unc_match.group(2).lower()}"
            is_abs = True
            remainder = unc_match.group(3) or ""
        else:
            is_abs = True
            remainder = s.lstrip("/")
    elif s.startswith("/"):
        is_abs = True
        remainder = s.lstrip("/")
    else:
        is_abs = False
        remainder = s

    # Split into components
    raw_segments = remainder.split("/") if remainder else []
    parts: List[str] = []
    for seg in raw_segments:
        if not seg or seg == ".":
            continue
        if seg == "..":
            if parts and parts[-1] != "..":
                parts.pop()
            else:
                parts.append("..")
        else:
            parts.append(seg)

    return (drive, is_abs, parts)


def is_machine_part(part: str) -> bool:
    """Check if a path component is a machine-specific directory or root."""
    low = part.lower().strip()
    return (
        low in ("users", "home", "appdata", "local", "roaming", "temp", "tmp")
        or low.startswith("pytest")
        or (len(part) in (2, 3) and part[1] == ":")
        or part in ("/", "\\")
    )


def extract_safe_logical_parts(p: Union[Path, PurePath, str, List[str]]) -> list[str]:
    """Extract path components excluding filesystem roots, drive letters, user home and system temp prefixes.

    Safe and deterministic: retains meaningful producer directory context (e.g. cluster-alpha,
    producer-a, test_service) while stripping transient runner directories.
    """
    if isinstance(p, (list, tuple)):
        raw = list(p)
    else:
        _, _, raw = split_logical_path(p)

    idx = 0
    in_temp_or_user = False
    while idx < len(raw):
        part = raw[idx]
        low = part.lower().strip()

        # System user directories
        if low in ("users", "home"):
            idx += 2
            in_temp_or_user = True
            continue

        # System temp / appdata / local / roaming directories
        if low in ("appdata", "local", "roaming", "temp", "tmp"):
            idx += 1
            in_temp_or_user = True
            continue

        # /var/log, /var/tmp, /var/run
        if low == "var" and idx + 1 < len(raw) and raw[idx + 1].lower() in ("log", "tmp", "run"):
            idx += 2
            in_temp_or_user = True
            continue

        # macOS /private/var or /private/tmp
        if low == "private" and idx + 1 < len(raw) and raw[idx + 1].lower() in ("var", "tmp"):
            idx += 2
            in_temp_or_user = True
            continue

        # Pytest directories (pytest-of-<user>, pytest-40, etc.)
        if low.startswith("pytest"):
            idx += 1
            in_temp_or_user = True
            continue

        # Temporary run prefixes (e.g. temp_run, tmp_run) when not the only parent
        if low.startswith(("temp_", "tmp_")) and idx < len(raw) - 2:
            idx += 1
            in_temp_or_user = True
            continue

        # Pytest test function directory (e.g. test_basename_collision0) ONLY if preceded by temp/user/pytest
        if in_temp_or_user and (low.startswith("test_") or low.startswith("run-") or low.startswith("run_")) and idx < len(raw) - 2:
            idx += 1
            continue

        break

    remaining = raw[idx:]
    return remaining if remaining else ([raw[-1]] if raw else [])


def normalize_source_path(file_path: Path | PurePath | str, base_dir: Optional[Path | PurePath | str] = None) -> str:
    """Normalize a file path into a stable, portable, forward-slash logical source name.

    Preserves parent folder context (e.g. producer-a/app.jsonl) to distinguish
    identically named files across directories without embedding machine-specific absolute paths.
    Platform-independent across Windows and POSIX.
    """
    # 1. If base_dir is supplied, try to compute relative path
    if base_dir is not None:
        # 1a. Physical check if both exist or can be resolved on disk
        try:
            p_obj = Path(file_path)
            base_obj = Path(base_dir)
            resolved_p = p_obj.resolve()
            resolved_base = base_obj.resolve()
            if resolved_p.is_relative_to(resolved_base) and resolved_p != resolved_base:
                return resolved_p.relative_to(resolved_base).as_posix()
        except (ValueError, TypeError, OSError):
            pass

        # 1b. Logical relative check (platform-independent)
        p_drive, p_abs, p_parts = split_logical_path(file_path)
        b_drive, b_abs, b_parts = split_logical_path(base_dir)
        if (p_drive == b_drive) and (p_abs == b_abs) and len(p_parts) > len(b_parts):
            matches = True
            for i, b_seg in enumerate(b_parts):
                p_seg = p_parts[i]
                is_mismatch = (p_seg.lower() != b_seg.lower()) if p_drive else (p_seg != b_seg)
                if is_mismatch:
                    matches = False
                    break
            if matches:
                return "/".join(p_parts[len(b_parts):])

    # 2. Parse logical components of file_path
    p_drive, p_abs, p_parts = split_logical_path(file_path)

    # 3. If explicitly relative with directory structure (e.g. 'producer-a/app.jsonl')
    if not p_drive and not p_abs:
        if len(p_parts) > 1:
            return "/".join(p_parts)
        return p_parts[0] if p_parts else str(file_path)

    # 4. If path is external/absolute, retain safe logical directory components
    safe = extract_safe_logical_parts(p_parts)
    return "/".join(safe) if safe else (p_parts[-1] if p_parts else str(file_path))


def is_path_case_sensitive(p_obj: Path) -> bool:
    """Determine whether the filesystem hosting an existing path is case-sensitive using a safe, non-destructive probe.

    Non-destructive probe mechanism:
    1. Traverses p_obj and its existing ancestor directories to find a component with alphabetic characters.
    2. Swaps the case of that component (e.g. 'App.log' -> 'aPP.LOG').
    3. Since the target component exists on disk:
       - On a case-insensitive filesystem (e.g. Windows NTFS, macOS default APFS):
         The case-swapped variant also exists and points to the same inode/file via os.path.samefile.
       - On a case-sensitive filesystem (e.g. Linux ext4, case-sensitive APFS):
         The case-swapped variant does not exist (or if it exists as a distinct file, has a different inode).
    4. If no component with alphabetic characters can be probed:
       Falls back to platform default (False for Windows/macOS, True for Linux/POSIX).
    """
    try:
        curr = p_obj.resolve()
        while curr and curr.name:
            if curr.exists():
                name = curr.name
                swapped = name.swapcase()
                if swapped != name:
                    probe = curr.with_name(swapped)
                    if probe.exists():
                        return not os.path.samefile(curr, probe)
                    # The component exists, but its case-swapped counterpart does not: case-sensitive!
                    return True
            if curr == curr.parent:
                break
            curr = curr.parent
    except Exception:
        pass
    # Platform default fallback when probing is impossible
    return sys.platform not in ("darwin", "win32") and os.name != "nt"


def canonical_path_key(p: Any) -> str:
    """Return a platform-independent canonical key for physical file deduplication and collision detection.

    For existing paths on disk:
      Resolves symlinks via Path.resolve() and uses a safe, non-destructive probe to detect whether the
      underlying filesystem volume is case-sensitive (e.g. Linux ext4 vs macOS APFS vs Windows NTFS).
      Case is preserved on case-sensitive volumes and folded to lowercase on case-insensitive volumes.

    For synthetic or non-existent paths:
      Since no physical file or inode exists on disk, runtime filesystem detection cannot be performed.
      A consistent syntactic convention is used:
      - Windows drive paths (e.g. 'D:/...') and UNC network paths (e.g. '//server/share/...') are normalized
        case-insensitively per Windows/SMB filesystem standards.
      - POSIX and relative paths preserve casing to prevent false collision assumptions on case-sensitive platforms.
    """
    try:
        p_obj = Path(p)
        if p_obj.exists():
            resolved = str(p_obj.resolve()).replace("\\", "/")
            if not is_path_case_sensitive(p_obj):
                return resolved.lower()
            return resolved
    except Exception:
        pass
    d, a, parts = split_logical_path(p)
    is_windows = bool(d and (re.match(r"^[a-zA-Z]:", d) or d.startswith("//")))
    joined_parts = "/".join(parts)
    if is_windows:
        return f"{d.upper()}|{a}|{joined_parts.lower()}"
    return f"{d if d else ''}|{a}|{joined_parts}"


def resolve_source_identities(
    file_inputs: list[Any],
    input_dir: Optional[Path | PurePath | str] = None,
    dir_roots: Optional[Dict[Any, Any]] = None,
) -> Dict[Any, str]:
    """Resolve collision-resistant, portable logical source names for a list of input files.

    Guarantees:
    1. Deterministic & Non-Batch-Dependent: The identity of a physical file is stable whether
       processed alone, alongside other files, or in reversed order.
    2. Input Dir Consistency: Files inside input_dir (or a specified directory argument) maintain
       their paths relative to that root.
    3. Safe & Machine-Independent: Strips filesystem roots, drive letters, and user home prefixes.
    4. Fast Fail on Ambiguity: If two distinct files produce the same logical identity, raises
       a descriptive ValueError rather than silently creating an ambiguous or batch-dependent identity.
    5. Platform Independence: Handles Windows backslashes and drive letters consistently on Linux,
       and POSIX paths on Windows.
    """
    dir_roots = dir_roots or {}
    results: Dict[Any, str] = {}
    resolved_input_dir = None
    if input_dir is not None:
        try:
            resolved_input_dir = Path(input_dir).resolve()
        except Exception:
            pass

    for item in file_inputs:
        if isinstance(item, tuple):
            p, explicit_name = item
            clean_name = str(explicit_name).replace("\\", "/")
            results[p] = clean_name
            results[item] = clean_name
            continue

        p = item
        # 1. Check dir_roots mapping
        root = dir_roots.get(p)
        if root is None and isinstance(p, (Path, PurePath, str)):
            root = dir_roots.get(Path(p)) or dir_roots.get(str(p))

        if root is not None:
            try:
                res_p = Path(p).resolve()
                res_root = Path(root).resolve()
                if res_p.is_relative_to(res_root) and res_p != res_root:
                    rel_path = res_p.relative_to(res_root).as_posix()
                    if resolved_input_dir and res_root == resolved_input_dir:
                        results[p] = rel_path
                    else:
                        results[p] = f"{res_root.name}/{rel_path}"
                    continue
            except (ValueError, TypeError, OSError):
                pass

        # 2. Check input_dir
        if input_dir is not None:
            try:
                res_p = Path(p).resolve()
                if resolved_input_dir and res_p.is_relative_to(resolved_input_dir) and res_p != resolved_input_dir:
                    results[p] = res_p.relative_to(resolved_input_dir).as_posix()
                    continue
            except (ValueError, TypeError, OSError):
                pass

            try:
                if resolved_input_dir is not None:
                    res_p = Path(p).resolve()
                    res_parent = resolved_input_dir.parent
                    if res_parent != res_parent.parent and res_p.is_relative_to(res_parent) and res_p != res_parent:
                        results[p] = res_p.relative_to(res_parent).as_posix()
                        continue
            except (ValueError, TypeError, OSError):
                pass

            # Logical containment check
            p_drive, p_abs, p_parts = split_logical_path(p)
            in_drive, in_abs, in_parts = split_logical_path(input_dir)
            if (p_drive == in_drive) and (p_abs == in_abs) and len(p_parts) > len(in_parts):
                matches = True
                for i, in_seg in enumerate(in_parts):
                    p_seg = p_parts[i]
                    is_mismatch = (p_seg.lower() != in_seg.lower()) if p_drive else (p_seg != in_seg)
                    if is_mismatch:
                        matches = False
                        break
                if matches:
                    results[p] = "/".join(p_parts[len(in_parts):])
                    continue

        # 3. External fallback: platform-independent normalization
        results[p] = normalize_source_path(p)

    # Collision validation: ensure no two distinct files produced the same identity
    names_to_inputs: Dict[str, list[Any]] = {}
    for item in file_inputs:
        if isinstance(item, tuple):
            p, explicit_name = item
            name = str(explicit_name).replace("\\", "/")
        else:
            p = item
            name = results.get(p) or normalize_source_path(p)
        names_to_inputs.setdefault(name, []).append(p)

    for name, inps in names_to_inputs.items():
        # Group inputs by canonical path key
        unique_canonical = set()
        unique_inputs_for_error = []
        for inp in inps:
            ck = canonical_path_key(inp)
            if ck not in unique_canonical:
                unique_canonical.add(ck)
                unique_inputs_for_error.append(inp)

        if len(unique_canonical) > 1:
            raise ValueError(
                f"Conflicting logical source identity '{name}' derived for distinct files: {[str(x) for x in unique_inputs_for_error]}. "
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
