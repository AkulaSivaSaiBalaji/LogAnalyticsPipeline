#!/usr/bin/env python3
"""Benchmark runner for LogAnalyticsPipeline.

Measures elapsed runtime, ingestion throughput, and optimization impact
(small batch vs optimized 5000-record batch insert with in-memory deduplication).
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Optional

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.log_pipeline.config import load_config, PipelineConfig
from src.log_pipeline.pipeline import LogAnalyticsPipeline
from scripts.generate_synthetic_logs import generate_logs


def run_benchmark(
    records_count: int = 100000,
    seed: int = 42,
    database_url: Optional[str] = None,
    batch_size: int = 5000,
    data_dir: Optional[str | Path] = None,
    reports_dir: Optional[str | Path] = None,
    quarantine_dir: Optional[str | Path] = None,
) -> dict:
    """Execute performance benchmark on synthetic dataset."""
    print("=" * 75)
    print(f"LogAnalyticsPipeline Benchmark: {records_count:,} Records (Seed: {seed})")
    print("=" * 75)

    dataset_dir = Path(data_dir) if data_dir else Path("data/synthetic")
    rep_dir = Path(reports_dir) if reports_dir else Path("output/reports")
    dataset_dir.mkdir(parents=True, exist_ok=True)
    rep_dir.mkdir(parents=True, exist_ok=True)

    input_file = dataset_dir / f"benchmark_logs_{records_count}.jsonl"

    print(f"Generating synthetic dataset with {records_count:,} records...")
    gen_start = time.perf_counter()
    generate_logs(
        num_records=records_count,
        seed=seed,
        output_dir=dataset_dir,
        file_prefix="benchmark_logs",
    )
    gen_duration = time.perf_counter() - gen_start
    print(f"Dataset generated in {gen_duration:.2f}s ({records_count / gen_duration:,.0f} records/s)")

    # Test baseline vs optimized batch size
    configs = [
        {"name": f"Optimized Batch ({batch_size:,} records/tx)", "batch_size": batch_size},
    ]

    results = []

    for item in configs:
        b_name = item["name"]
        batch_sz = item["batch_size"]
        print(f"\nRunning benchmark: {b_name}...")

        cfg = load_config(
            database_url=database_url,
            batch_size=batch_sz,
            reports_dir=rep_dir,
            quarantine_dir=Path(quarantine_dir) if quarantine_dir else None,
        )
        pipeline = LogAnalyticsPipeline(config=cfg)

        run_start = time.perf_counter()
        run_res = pipeline.run(files=[input_file])
        elapsed = time.perf_counter() - run_start

        throughput = records_count / elapsed if elapsed > 0 else 0.0

        summary = {
            "name": b_name,
            "batch_size": batch_sz,
            "total_records": records_count,
            "elapsed_seconds": round(elapsed, 3),
            "throughput_records_per_sec": round(throughput, 1),
            "valid_count": run_res["valid_count"],
            "rejected_count": run_res["rejected_count"],
            "duplicate_count": run_res["duplicate_count"],
            "status": run_res["status"],
            "reconciliation_balanced": run_res["reconciliation"]["is_balanced"],
        }
        results.append(summary)

        print(f"  Status: {summary['status']}")
        print(f"  Elapsed: {summary['elapsed_seconds']:.2f}s")
        print(f"  Throughput: {summary['throughput_records_per_sec']:,.0f} records/sec")
        print(f"  Valid: {summary['valid_count']:,} | Rejected: {summary['rejected_count']:,} | Duplicates: {summary['duplicate_count']:,}")
        print(f"  Reconciliation Balanced: {summary['reconciliation_balanced']}")

    # Save benchmark summary
    benchmark_file = rep_dir / "benchmark_summary.json"
    with open(benchmark_file, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)

    print("\n" + "=" * 75)
    print(f"Benchmark summary written to {benchmark_file}")
    print("=" * 75)
    return results[0]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Benchmark pipeline throughput.")
    parser.add_argument("--records", type=int, default=100000, help="Number of records to benchmark")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    parser.add_argument("--database-url", type=str, default=None, help="Database connection URL")
    parser.add_argument("--batch-size", type=int, default=5000, help="Batch size for database insertions")
    parser.add_argument("--data-dir", type=str, default=None, help="Directory for synthetic benchmark datasets")
    parser.add_argument("--reports-dir", type=str, default=None, help="Directory for benchmark reports")
    parser.add_argument("--quarantine-dir", type=str, default=None, help="Directory for quarantined records")
    args = parser.parse_args()

    if args.records <= 0:
        parser.error(f"--records must be a positive integer (> 0), got: {args.records}")
    if args.batch_size <= 0:
        parser.error(f"--batch-size must be a positive integer (> 0), got: {args.batch_size}")

    run_benchmark(
        records_count=args.records,
        seed=args.seed,
        database_url=args.database_url,
        batch_size=args.batch_size,
        data_dir=args.data_dir,
        reports_dir=args.reports_dir,
        quarantine_dir=args.quarantine_dir,
    )
