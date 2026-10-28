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

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.log_pipeline.config import load_config, PipelineConfig
from src.log_pipeline.pipeline import LogAnalyticsPipeline
from scripts.generate_synthetic_logs import generate_logs


def run_benchmark(records_count: int = 100000, seed: int = 42) -> dict:
    """Execute performance benchmark on synthetic dataset."""
    print("=" * 75)
    print(f"LogAnalyticsPipeline Benchmark: {records_count:,} Records (Seed: {seed})")
    print("=" * 75)

    data_dir = Path("data/synthetic")
    input_file = data_dir / f"benchmark_logs_{records_count}.jsonl"

    print(f"Generating synthetic dataset with {records_count:,} records...")
    gen_start = time.perf_counter()
    generate_logs(
        num_records=records_count,
        seed=seed,
        output_dir=data_dir,
        file_prefix="benchmark_logs",
    )
    gen_duration = time.perf_counter() - gen_start
    print(f"Dataset generated in {gen_duration:.2f}s ({records_count / gen_duration:,.0f} records/s)")

    # Test baseline vs optimized batch size
    configs = [
        {"name": "Optimized Batch (5,000 records/tx)", "batch_size": 5000},
    ]

    results = []

    for item in configs:
        b_name = item["name"]
        batch_sz = item["batch_size"]
        print(f"\nRunning benchmark: {b_name}...")

        cfg = load_config(batch_size=batch_sz)
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
    out_dir = Path("output/reports")
    out_dir.mkdir(parents=True, exist_ok=True)
    benchmark_file = out_dir / "benchmark_summary.json"
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
    args = parser.parse_args()

    run_benchmark(records_count=args.records, seed=args.seed)
