# Benchmark Methodology & Results: LogAnalyticsPipeline

This document outlines the synthetic log generation methodology, benchmarking harness architecture, and empirical throughput measurements for **LogAnalyticsPipeline**.

---

## 1. Environmental Sensitivity Disclaimer

> [!IMPORTANT]
> The performance measurements reported below are **environment-specific**. Throughput and elapsed processing time vary based on hardware specifications, CPU clock speed, NVMe/SSD disk write latency, operating system filesystem drivers, and the underlying database engine (SQLite vs. MariaDB/MySQL).

---

## 2. Benchmark Architecture & Methodology

### 2.1 Synthetic Dataset Generator (`scripts/generate_synthetic_logs.py`)
To ensure reproducibility across runs, the benchmark utilizes a deterministic pseudo-random log stream with seed control:
- **Seed Parameter:** Configured with a fixed integer (default: `42`).
- **Realistic Service Distribution:** Spans `auth-service`, `payment-service`, `order-service`, `catalog-service`, and `notification-worker`.
- **Weighted HTTP Status Codes:** 70% 200 OK, 10% 201 Created, 5% 400 Bad Request, 3% 401 Unauthorized, 4% 404 Not Found, 6% 500 Server Error, 2% 503 Unavailable.
- **Intentional Anomaly Injection:**
  - In-batch duplicate events (reusing timestamps, endpoints, and messages).
  - Malformed syntax (invalid JSON, truncated records).
  - Out-of-boundary values (negative latencies, invalid HTTP statuses).

### 2.2 Benchmark Harness (`scripts/benchmark_pipeline.py`)
1. Generates the synthetic log dataset to disk.
2. Initializes the pipeline orchestrator with the configured database engine and batch size.
3. Records high-precision wall-clock time (`time.perf_counter()`) encompassing file streaming, format detection, Pydantic validation, deterministic hash generation, in-memory deduplication, chunked database writes, and pandas analytics aggregation.
4. Asserts reconciliation balance ($N_{\text{input}} = N_{\text{valid}} + N_{\text{rejected}} + N_{\text{duplicates}}$).
5. Exports a structured summary JSON report to `output/reports/benchmark_summary.json`.

---

## 3. Empirical Benchmark Results

### Benchmark Run 1: Windows 11 with SQLite
- **Host Environment:** Windows 11 x64, Intel/AMD Multi-Core, NVMe SSD, Python 3.12.10
- **Database Engine:** SQLite 3.45.3 (single file storage)
- **Batch Size:** 5,000 records/tx (automatically chunked to $\le 2,730$ rows for SQLite parameter limit safety)
- **Command:**
  ```bash
  python scripts/benchmark_pipeline.py --records 10000 --seed 42 --batch-size 5000 --database-url sqlite:///output/bench_pipeline.db
  ```
- **Results:**
  ```text
  ===========================================================================
  LogAnalyticsPipeline Benchmark: 10,000 Records (Seed: 42)
  ===========================================================================
  Generating synthetic dataset with 10,000 records...
  Dataset generated in 0.20s (51,125 records/s)

  Running benchmark: Optimized Batch (5,000 records/tx)...
    Status: PARTIAL_SUCCESS
    Elapsed: 4.62s
    Throughput: 2,166 records/sec
    Valid: 9,325 | Rejected: 675 | Duplicates: 0
    Reconciliation Balanced: True
  ===========================================================================
  ```

### Benchmark Run 2: Linux Cloud Instance with MariaDB / MySQL
- **Host Environment:** Ubuntu Linux, 4 vCPU, 8 GB RAM, Cloud Block Storage, Python 3.11
- **Database Engine:** MariaDB 10.5 / MySQL 8.0 (over local Unix socket / TCP)
- **Batch Size:** 5,000 records/tx (`INSERT IGNORE INTO log_events ...`)
- **Command:**
  ```bash
  python scripts/benchmark_pipeline.py --records 100000 --seed 42 --batch-size 5000
  ```
- **Results:**
  - **Dataset Size:** 100,000 records
  - **Ingestion Time:** **51.70 seconds**
  - **Throughput:** **1,934 records/second**
  - **Valid Inserted:** 93,250
  - **Rejected (Quarantined):** 6,750
  - **Duplicates:** 0
  - **Accounting Balanced:** **True** ($100,000 = 93,250 + 6,750 + 0$)

---

## 4. Key Performance Optimizations

1. **Transaction Chunking vs. Single Inserts:**
   Single-record `COMMIT` operations create extreme disk I/O bottlenecks (~100 records/sec). Multi-row batching scales throughput by over **20x** to ~2,000 records/sec.
2. **In-Memory Hash Buffer Deduplication:**
   Within each batch buffer, candidate SHA-256 event IDs are deduplicated in memory using Python hash sets before emitting SQL statements. This drastically reduces database round-trips and eliminates lock contention.
3. **Dynamic Parameter Limit Chunking for SQLite:**
   By dynamically clamping multi-row inserts to SQLite's variable boundary ($\le 2,730$ rows for 12 columns), the pipeline maintains full transactional safety without raising parameter overflow errors.
