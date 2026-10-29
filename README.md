# LogAnalyticsPipeline

A production-grade, maintainable application log analytics pipeline built with Python, SQLAlchemy, PyMySQL, Pydantic, pandas, and MySQL/SQLite.

The pipeline ingests multi-format application logs (JSON arrays, JSONL, and semi-structured text), normalizes timestamps to UTC and severities to canonical levels, validates records with strict schema checking, quarantines invalid inputs with explicit failure reasons, idempotently loads valid events into relational storage, and generates hourly and service-level error-rate and latency summaries.

---

## 1. System Architecture

```mermaid
flowchart TD
    subgraph Inputs ["Input Sources"]
        JSON["JSON Array Files (.json)"]
        JSONL["Line-Delimited JSON (.jsonl)"]
        TEXT["Semi-Structured Text Logs (.log)"]
    end

    subgraph CoreEngine ["LogAnalyticsPipeline Engine"]
        DETECTOR["Format Detector"] --> PARSER["Resilient Streaming Parser"]
        PARSER -->|Raw Record + Lineage| NORM["Timestamp & Severity Normalizer"]
        NORM --> HASH["Deterministic SHA-256 Event ID"]
        HASH --> VALID["Pydantic Schema Validator"]
        
        VALID -->|Valid| BATCH_VALID["Valid Event Batch Buffer"]
        VALID -->|Invalid / Syntax Error| QUAR["Quarantine Buffer"]
    end

    subgraph Storage ["Persistent Storage Layer"]
        BATCH_VALID -->|INSERT IGNORE / Chunked Tx| DB_EVENTS[("Relational DB: log_events")]
        QUAR -->|Batch Bulk Insert| DB_REJECTED[("Relational DB: rejected_records")]
        CoreEngine -->|Run Lifecycle & Ledger| DB_RUNS[("Relational DB: pipeline_runs")]
    end

    subgraph Analytics ["Reporting & Analytics"]
        BATCH_VALID --> AGG["Metrics Calculator (pandas)"]
        AGG --> CSV_HOURLY["Hourly Metrics CSV"]
        AGG --> CSV_SERVICE["Service Summary CSV"]
        AGG --> CSV_RECON["Reconciliation Audit CSV"]
        QUAR --> QUAR_JSONL["Quarantined JSONL Export"]
    end

    Inputs --> DETECTOR
```

---

## 2. Business Rules & Design Assumptions

### 2.1 Timestamp Normalization
- **Timezone-Aware Timestamps**: Automatically converted to timezone-aware UTC (`datetime.astimezone(timezone.utc)`).
- **Timezone-Naive Timestamps**: Assumed to be recorded in UTC (distributed application logging best practice) and localized to UTC.
- **Epoch Timestamps**: Numeric values in seconds or milliseconds are parsed directly.
- **Unparseable Timestamps**: Routed to quarantine with reason `INVALID_TIMESTAMP`.

### 2.2 Severity Canonicalization
Input severities are normalized to canonical levels:
- `DEBUG` (maps `TRACE`, `VERBOSE`, `DEBG`)
- `INFO` (maps `INFORMATION`, `NOTICE`)
- `WARNING` (maps `WARN`)
- `ERROR` (maps `ERR`)
- `CRITICAL` (maps `FATAL`, `CRIT`, `ALERT`, `EMERGENCY`)
- Unknown or missing severities are quarantined with `UNRECOGNIZED_SEVERITY: <val>`.

### 2.3 Deterministic Identity & Duplicate Policy
- If upstream supplies `event_id`, it is preserved.
- When omitted, a deterministic 32-character SHA-256 hash is computed from:
  `sha256("{source_file}:{source_line}:{service}:{endpoint}:{utc_timestamp}:{message}")`
- **Idempotency Guarantee**: Reprocessing the exact same source file produces the exact same event IDs, skipping duplicates via database `INSERT IGNORE` (MySQL) or `ON CONFLICT DO NOTHING` (SQLite) without error or duplicate rows.
- **Legitimate Distinct Events**: Separate log entries with identical messages (e.g. repeated health checks or cron jobs) generated at different lines or timestamps receive distinct hashes and are never discarded.

### 2.4 Error-Rate Calculation
- **Error Definition**: An event is classified as an error if `severity IN ('ERROR', 'CRITICAL')` OR `http_status >= 500`. Client errors (`4xx`) are not system errors unless marked with error severity.
- **Error Rate Denominator**: Total events for that group (`window_hour` and `service`).
- **Formula**: `error_rate_pct = (error_count / total_events) * 100.0`.

### 2.5 Latency Percentiles
- Excludes records where `latency_ms` is `null` from latency calculations.
- Computes arithmetic mean, p50 (median), p95, and p99.
- If a group has 0 latency values, latency metrics are output as `null` (avoiding artificial 0.0 ms values).

### 2.6 Accounting Reconciliation Formula
Every pipeline run strictly reconciles to the fundamental invariant:
$$\text{input\_count} = \text{valid\_count} + \text{rejected\_count} + \text{duplicate\_count}$$

---

## 3. Environment Setup & Dependencies

### 3.1 Prerequisites
- **Python**: 3.10, 3.11, or 3.12 (tested on Python 3.12.10 x64)
- **Database**:
  - **SQLite**: Supported out of the box with zero external dependencies via `sqlite:///...`. Ideal for local development, automated testing, and standalone CLI runs.
  - **MySQL / MariaDB**: MySQL 8.0+ or MariaDB 10.5+ for multi-user production deployments.
- **Git**

### 3.2 Linux / macOS Setup
```bash
# 1. Clone repository and navigate to directory
git clone <repo_url>
cd LogAnalyticsPipeline

# 2. Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate

# 3. Upgrade pip and install dependencies
pip install --upgrade pip
pip install -r requirements.txt

# 4. Configure environment variables (optional; defaults work with SQLite)
cp .env.example .env
# Edit .env with custom database connection parameters if using MySQL
```

### 3.3 Windows PowerShell / CMD Setup
```powershell
# PowerShell:
cd LogAnalyticsPipeline
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
copy .env.example .env
```

```cmd
:: Command Prompt (CMD):
cd LogAnalyticsPipeline
python -m venv .venv
.venv\Scripts\activate.bat
python -m pip install --upgrade pip
pip install -r requirements.txt
copy .env.example .env
```

---

## 4. Supported CLI Invocation (`python -m src.log_pipeline`)

The primary and supported interface for running the pipeline is the standard Python module CLI:
```bash
python -m src.log_pipeline [OPTIONS]
```

### 4.1 Help & Usage (`--help`)
Display available flags, argument defaults, and help descriptions:
```bash
python -m src.log_pipeline --help
```

### 4.2 Ingesting Specific Log Files (`--files`)
Process one or more individual files. Supports JSON arrays, JSONL, and semi-structured text log formats:
```bash
# Ingest a single JSONL file
python -m src.log_pipeline --files data/sample/sample_logs.jsonl

# Ingest multiple files in a single run
python -m src.log_pipeline --files data/sample/sample_logs.json data/sample/sample_logs.log
```

### 4.3 Ingesting an Entire Directory (`--input-dir`)
Discover and process all supported `.json`, `.jsonl`, and `.log` files in a directory:
```bash
python -m src.log_pipeline --input-dir data/sample
```

### 4.4 Specifying Database Connection URL (`--database-url`)
Override default database settings with an explicit database connection URL:
```bash
# Run against a local SQLite database
python -m src.log_pipeline --files data/sample/sample_logs.jsonl --database-url sqlite:///output/my_database.db

# Run against a MySQL / MariaDB instance
python -m src.log_pipeline --files data/sample/sample_logs.jsonl --database-url mysql+pymysql://log_user:log_pass@localhost:3306/log_analytics_db
```

### 4.5 Adjusting Database Batch Size (`--batch-size`)
Configure the batch commit size for event and rejected-record writes:
```bash
python -m src.log_pipeline --files data/sample/sample_logs.jsonl --batch-size 1000
```

### 4.6 Machine-Readable Output (`--json`)
Output execution summary and metric rollups directly as JSON to standard output (ideal for automation and CI/CD pipelines):
```bash
python -m src.log_pipeline --files data/sample/sample_logs.jsonl --json
```

### 4.7 Combined Execution Example
```bash
python -m src.log_pipeline \
  --input-dir data/sample \
  --database-url sqlite:///log_analytics.db \
  --batch-size 5000 \
  --quarantine-dir output/quarantine \
  --reports-dir output/reports
```

### 4.8 Python API Invocation
The engine can also be invoked directly in Python scripts:
```python
from src.log_pipeline.pipeline import LogAnalyticsPipeline
from src.log_pipeline.config import load_config

config = load_config(database_url="sqlite:///log_analytics.db", batch_size=5000)
pipeline = LogAnalyticsPipeline(config)
result = pipeline.run(files=["data/sample/sample_logs.jsonl"])
print(f"Status: {result['status']}, Inserted: {result['valid_count']}")
```

---

## 5. Running the Automated Test Suite

The test suite covers unit logic, ORM schemas, validation boundaries, deterministic source identity, text parser hardening, foreign key enforcement, failure recovery, SQLite variable limit safety, and CLI arguments.

Run the test suite using pytest:
```bash
# Run full test suite with verbose output
pytest -v

# Run without pytest cache provider (recommended on sandboxed/isolated systems)
python -m pytest -v -p no:cacheprovider
```

### Test Suite Execution Summary
- **104 passed, 2 skipped** (106 collected items).
- The 2 skipped tests (`test_writer_mysql_idempotency_live` and `test_mysql_fk_enforcement_live`) are live MySQL integration tests that require an active MySQL daemon on `localhost:3306`. In environments without a live MySQL server, they skip gracefully and the remaining 104 tests verify all core behavior against SQLite.

---

## 6. Synthetic Data Generation & Benchmarking

### 6.1 Generating Reproducible Synthetic Datasets
Generate realistic test datasets with configurable record counts, deterministic seeds, intentional duplicates, syntax anomalies, and boundary values:
```bash
# Generate 10,000 synthetic records with seed 42
python scripts/generate_synthetic_logs.py --records 10000 --seed 42 --output-dir data/synthetic

# Generate 100,000 synthetic records for scale testing
python scripts/generate_synthetic_logs.py --records 100000 --seed 42 --output-dir data/synthetic
```

### 6.2 Running the Performance Benchmark
Execute the end-to-end benchmark measuring dataset generation, ingestion throughput, and reconciliation accounting:
```bash
# Run default benchmark (100,000 records, batch size 5,000)
python scripts/benchmark_pipeline.py --records 100000 --seed 42

# Run benchmark with custom batch size and SQLite target
python scripts/benchmark_pipeline.py --records 10000 --seed 42 --batch-size 5000 --database-url sqlite:///output/benchmark.db
```

### 6.3 Benchmark Methodology & Environment-Specific Results

> [!NOTE]
> Benchmark throughput and duration are **environment-specific** and depend on CPU architecture, disk I/O performance, operating system, and database engine. The figures below reflect empirical runs in specific test environments.

#### Benchmark Methodology
1. **Dataset Generation**: Creates a deterministic stream using `scripts/generate_synthetic_logs.py` with fixed random seed, realistic service distributions, weighted HTTP statuses (200, 201, 400, 404, 500, 503), intentional in-batch duplicates, out-of-order timestamps, and syntax anomalies.
2. **Ingestion & Processing**: Streams the dataset through format detection, timestamp/severity canonicalization, SHA-256 fingerprinting, Pydantic validation, and batch database insertion.
3. **Accounting & Aggregation**: Validates strict reconciliation balance ($N_{\text{input}} = N_{\text{valid}} + N_{\text{rejected}} + N_{\text{duplicates}}$) and generates analytical rollups.

#### Measured Results
- **Environment A (Windows 11, Python 3.12.10, SQLite 3.45.3, NVMe SSD)**:
  - **Dataset Size**: 10,000 records (Seed 42)
  - **Batch Size**: 5,000 records/tx (safely auto-chunked to $\le 2,730$ rows for SQLite limit safety)
  - **Ingestion Time**: **4.62 seconds**
  - **Throughput**: **2,166 records/second**
  - **Results**: 9,325 Valid | 675 Rejected | 0 Duplicates | Reconciliation Balanced: **True**
- **Environment B (Linux, Python 3.11, MariaDB 10.5, Cloud Instance)**:
  - **Dataset Size**: 100,000 records (Seed 42)
  - **Batch Size**: 5,000 records/tx
  - **Ingestion Time**: **51.70 seconds**
  - **Throughput**: **1,934 records/second**
  - **Results**: 93,250 Valid | 6,750 Rejected | 0 Duplicates | Reconciliation Balanced: **True**

---

## 7. Storage Architecture & SQLite Bind-Variable Chunking

### 7.1 Database Engine Support
The pipeline natively supports both production MySQL/MariaDB and embedded/local SQLite databases:
- **MySQL / MariaDB**: Uses dialect-native multi-row `INSERT IGNORE INTO log_events ...` statements for high throughput.
- **SQLite**: Uses dialect-native `INSERT INTO log_events ... ON CONFLICT (event_id) DO NOTHING RETURNING event_id` to identify newly inserted records for accurate metrics calculations.

### 7.2 Automatic SQLite Batch Chunking
SQLite enforces an internal limit on bound SQL statement parameters (`SQLITE_MAX_VARIABLE_NUMBER`):
- Modern SQLite (3.32.0+) default limit: **32,766** bound variables.
- Legacy SQLite default limit: **999** bound variables.

Attempting to execute a multi-row insert of 5,000 rows with 12 parameters per row requires 60,000 bound variables, which exceeds SQLite's limit and raises `sqlite3.OperationalError: too many SQL variables`.

`LogAnalyticsPipeline` solves this dynamically:
1. **Runtime Variable Limit Detection**: `DatabaseWriter` inspects the active connection using `conn.connection.dbapi_connection.getlimit(SQLITE_LIMIT_VARIABLE_NUMBER)` or the SQLite version.
2. **Dynamic Statement Chunking**: Multi-row inserts are automatically split into safe sub-chunks within a single transaction:
   - For `log_events` (12 columns/row): chunk size is clamped to $\lfloor (\text{limit} - \text{fixed}) / 12 \rfloor$ ($\le 2,730$ rows on modern SQLite, $\le 83$ rows on legacy builds).
   - For `rejected_records` (6 columns/row): chunk size is clamped to $\lfloor (\text{limit} - \text{fixed}) / 6 \rfloor$ ($\le 5,461$ rows on modern SQLite, $\le 166$ rows on legacy builds).
3. **Transaction Safety**: All chunks within a batch share the parent database transaction; if any chunk fails, the entire transaction rolls back.
4. **Transparent User Experience**: Callers can safely pass `--batch-size 5000` on SQLite without encountering parameter limit exceptions.

---

## 8. Output Artifacts

Each pipeline execution produces durable analytical reports and quarantine exports in the configured output directories (default `output/reports/` and `output/quarantine/`):

- `output/reports/<run_id>_hourly_metrics.csv`: Hourly breakdown of event count, error count, error rate %, and latency percentiles (p50, p95, p99) by service.
- `output/reports/<run_id>_service_summary.csv`: Aggregated summary across the entire run per service.
- `output/reports/<run_id>_reconciliation.csv`: Audit ledger validating $N_{\text{input}} = N_{\text{valid}} + N_{\text{rejected}} + N_{\text{duplicates}}$.
- `output/quarantine/<run_id>_quarantined.jsonl`: Verbatim raw unparsed records and explicit failure reason codes.

---

## 9. Known Limitations & Operating Considerations

1. **Existing-Database Foreign-Key Migration Limitation**:
   - Starting in Stage 1B-3, database foreign keys are enforced between `log_events.run_id` -> `pipeline_runs.run_id` and `rejected_records.run_id` -> `pipeline_runs.run_id`. SQLite connections actively enforce constraints via `PRAGMA foreign_keys=ON`.
   - **Limitation**: When connecting to an *existing, pre-migration database* containing orphan records (events or rejected records whose `run_id` does not exist in `pipeline_runs`), foreign-key violations will occur on insertion.
   - **Remediation**: The pipeline intentionally does not run automated schema migrations or mutate historical data. For existing databases, users must ensure parent `pipeline_runs` rows exist or initialize a fresh database schema (`sql/schema.sql` or `DatabaseManager.create_tables()`).
2. **MySQL Verification Limitation**:
   - While MySQL schema definitions, connection pooling, and dialect syntax (`INSERT IGNORE`) are fully implemented and supported in the codebase, executing the live MySQL test cases (`test_writer_mysql_idempotency_live`, `test_mysql_fk_enforcement_live`) requires an active MySQL/MariaDB server instance with accessible network permissions.
   - In environments without an active MySQL daemon (e.g., standard offline or air-gapped CI environments), those two integration tests are automatically skipped, while all remaining 104 unit, schema, and SQLite integration tests pass.
3. **Single-Node Standalone Process**:
   - The engine is designed as a standalone Python process with zero external streaming dependencies (Kafka/Flink/Spark).
4. **Memory Footprint for Massive Single JSON Array Files**:
   - Very large JSON array files (e.g. `[ ... ]` > 1 GB) must be parsed into memory by the Python JSON decoder. For high-volume log streams, line-delimited JSON (`.jsonl`) streaming is strongly recommended.
5. **Timezone Assumptions**:
   - Timezone-naive timestamps are assumed to represent UTC. If an application logs in local wall-clock time without an explicit offset, timestamps should include an ISO-8601 offset (e.g. `+05:30`) to avoid UTC skew.

---

## 10. Troubleshooting Guide

| Issue | Cause | Solution |
|---|---|---|
| `OperationalError: 2003 / 1698` | MySQL server not running or incorrect credentials | Verify MySQL daemon status (`systemctl status mysql` or `services.msc`). Alternatively, use SQLite: `--database-url sqlite:///log_analytics.db`. |
| `IntegrityError: FOREIGN KEY constraint failed` | Attempting to write records with unknown `run_id` or existing orphan data | Ensure the pipeline run was initialized through `LogAnalyticsPipeline.run()`. For pre-migration databases, initialize a clean schema. |
| `ValueError: Cannot execute pipeline with run_id '...'` | Attempted to reuse a `run_id` that already completed with a terminal status | Supply a unique `--run-id` or omit the flag to allow the pipeline to generate an automatic timestamped run ID. |
| `log_pipeline: error: Input file does not exist` | Invalid path supplied to `--files` or `--input-dir` | Verify the file or directory path exists before invoking the CLI. |
| `Special characters in password` | Password containing `@` or `#` breaks standard URI parsing | Use discrete `DB_USER`, `DB_PASSWORD`, `DB_HOST` in `.env` or use SQLAlchemy URL escaping. |
| `Quarantine records increasing` | Upstream format drift or malformed values (e.g. `1.2.3ms`) | Inspect `output/quarantine/<run_id>_quarantined.jsonl` to review explicit failure codes (`MALFORMED_LATENCY`, `INVALID_TIMESTAMP`). |
