"""Command Line Interface (CLI) for LogAnalyticsPipeline."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import List, Optional

from .config import load_config
from .pipeline import LogAnalyticsPipeline

logger = logging.getLogger("log_pipeline.cli")


def build_parser() -> argparse.ArgumentParser:
    """Construct CLI argument parser with input validation."""
    parser = argparse.ArgumentParser(
        prog="log_pipeline",
        description="Ingest, validate, normalize, store, and analyze structured and semi-structured application logs.",
    )
    parser.add_argument(
        "--files",
        "-f",
        nargs="+",
        default=None,
        help="One or more specific log files to process.",
    )
    parser.add_argument(
        "--input-dir",
        "-i",
        default=None,
        help="Directory containing log files to process (default: data/sample or LOG_INPUT_DIR).",
    )
    parser.add_argument(
        "--database-url",
        "-d",
        default=None,
        help="Database connection URL (e.g. sqlite:///data.db or mysql+pymysql://user:pass@host/db).",
    )
    parser.add_argument(
        "--batch-size",
        "-b",
        type=int,
        default=None,
        help="Batch size for database insertions (default: 5000 or BATCH_SIZE).",
    )
    parser.add_argument(
        "--quarantine-dir",
        default=None,
        help="Directory for quarantined invalid records (default: output/quarantine).",
    )
    parser.add_argument(
        "--reports-dir",
        default=None,
        help="Directory for summary CSV reports (default: output/reports).",
    )
    parser.add_argument(
        "--log-level",
        default=None,
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging verbosity level (default: INFO or LOG_LEVEL).",
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="Optional explicit run ID identifier for pipeline lineage tracking.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output execution summary as JSON to stdout.",
    )
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    """Execute pipeline CLI with argument validation and status exit codes."""
    parser = build_parser()
    args = parser.parse_args(argv)

    # 1. Validate batch size
    if args.batch_size is not None and args.batch_size <= 0:
        parser.error(f"--batch-size must be a positive integer (> 0), got: {args.batch_size}")

    # 2. Validate input directory if specified
    if args.input_dir is not None:
        p_dir = Path(args.input_dir)
        if not p_dir.exists():
            parser.error(f"Input directory does not exist: {args.input_dir}")
        if not p_dir.is_dir():
            parser.error(f"Input directory path is not a directory: {args.input_dir}")

    # 3. Validate files if specified
    if args.files is not None:
        for f in args.files:
            p_file = Path(f)
            if not p_file.exists():
                parser.error(f"Input file does not exist: {f}")

    # 4. Configure logging
    log_level_str = args.log_level or "INFO"
    log_numeric_level = getattr(logging, log_level_str.upper(), logging.INFO)
    logging.basicConfig(
        level=log_numeric_level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    try:
        cfg = load_config(
            database_url=args.database_url,
            input_dir=args.input_dir,
            quarantine_dir=args.quarantine_dir,
            reports_dir=args.reports_dir,
            batch_size=args.batch_size,
            log_level=args.log_level,
        )
        pipeline = LogAnalyticsPipeline(config=cfg)
        result = pipeline.run(files=args.files, run_id=args.run_id)

        if args.json:
            print(json.dumps(result, indent=2, default=str))
        else:
            print("=" * 65)
            print(f"Pipeline Run Completed: {result['run_id']}")
            print("=" * 65)
            print(f"Status:          {result['status']}")
            print(f"Input Records:   {result['input_count']:,}")
            print(f"Valid Inserted:  {result['valid_count']:,}")
            print(f"Rejected:        {result['rejected_count']:,}")
            print(f"Duplicates:      {result['duplicate_count']:,}")
            print(f"Balanced:        {result['reconciliation']['is_balanced']}")
            print("=" * 65)

        is_balanced = result.get("reconciliation", {}).get("is_balanced", False)
        if result.get("status") in ("SUCCESS", "PARTIAL_SUCCESS") and is_balanced:
            return 0
        return 1

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        if args.json:
            print(json.dumps({"error": str(e), "status": "FAILED"}, indent=2))
        else:
            print(f"Error: Pipeline execution failed: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
