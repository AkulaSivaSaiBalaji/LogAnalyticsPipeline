# Reliability Correction Stages (Stages 1A–1E): LogAnalyticsPipeline

This document provides a comprehensive technical overview of the implementation across all five iterative reliability correction stages executed in **LogAnalyticsPipeline**.

---

## Stage 1A: Analytics & Deduplication Accounting

### 1. Objective
Eliminate the discrepancy between database-inserted records and analytics rollups, ensuring that duplicates skipped by database idempotency logic are not counted in error rates or latency percentiles.

### 2. Implementation Details
- **`BatchWriteResult` Data Contract:**
  Created `BatchWriteResult` in `src/log_pipeline/db/writer.py` as a subclass of `tuple` (`tuple[int, int]`):
  - Preserves backwards compatibility for existing callers unpacking `ins, dup = writer.write_events_batch(...)`.
  - Adds a new attribute `.inserted_events: List[LogEventRecord]` containing only records confirmed to be newly inserted into the database.
- **Dialect-Specific RETURNING Logic:**
  - For SQLite, implemented `INSERT INTO log_events ... ON CONFLICT (event_id) DO NOTHING RETURNING event_id`. This allows SQLite to return the exact list of primary keys written by the engine in a single round-trip.
  - For MySQL and other dialects, implemented candidate ID chunking and pre-query filtering against `LogEventModel.event_id` to identify genuine new rows.
- **Pipeline Orchestrator Decoupling:**
  In `src/log_pipeline/pipeline.py`, modified batch flushing so that `all_valid_events_for_metrics` is populated exclusively from `res.inserted_events`.
- **Reconciliation Accounting:**
  Strict invariant maintained:
  $$\text{input\_count} = \text{valid\_count} + \text{rejected\_count} + \text{duplicate\_count}$$
  On re-running against the same data, `valid_count = 0`, `duplicate_count = N`, and metrics rollups correctly reflect zero new events.

### 3. Regression Suite
- Added `tests/test_stage1_analytics_dedup.py` covering:
  - In-batch duplicates
  - Cross-batch duplicates
  - DB-level uniqueness conflicts
  - Complete re-run idempotency

---

## Stage 1B-1: Deterministic Source Identity & Collision Resistance

### 1. Objective
Ensure external log files receive stable, collision-resistant logical source identities regardless of batch boundaries, directory input mode, or operating system path separators.

### 2. Implementation Details
- **Path Normalization:**
  Implemented `normalize_source_path(file_path)` in `src/log_pipeline/parsers/base.py`, converting Windows backslashes (`\`) to standard forward slashes (`/`) and removing redundant parent segments.
- **Deterministic Disambiguation:**
  Implemented `resolve_source_identities(files, input_dir)`:
  - Preserves relative paths when files are inside `config.input_dir`.
  - For external files, derives a stable logical identity using relative common root or path components without exposing machine-specific usernames or drive letters.
  - **Batch Independence:** Files receive the exact same logical identity whether processed individually, in a batch, or in reversed order.
- **Explicit Tuple Override Support:**
  Supported explicit `(Path, logical_name)` inputs across all pipeline entry points.
- **Ambiguous Collision Defense:**
  Detects when two distinct external files would resolve to the same logical name and raises an explicit `ValueError` rather than silently merging records.

### 3. Regression Suite
- Added `tests/test_stage1_source_identity.py` (11 tests) verifying:
  - Basename collision stability
  - Cross-platform path normalization
  - Batch-independent hashing
  - Explicit tuple overrides

---

## Stage 1B-2: Text Parser Hardening & Latency Validation

### 1. Objective
Harden semi-structured text log parsing and schema validation to prevent malformed or invalid numeric representations from crashing the pipeline or skewing metrics.

### 2. Implementation Details
- **Safe Unit-Aware Latency Parsing:**
  Implemented `parse_latency_string(val)` in `src/log_pipeline/parsers/text_parser.py`:
  - Parses floats with units: `ms`, `s`, `us`, `ns`.
  - Strictly rejects malformed strings (e.g., `1.2.3ms`, `1..2s`, `NaNms`) by returning `None` instead of raising uncaught `ValueError`.
- **Model-Level Numerical Boundary Checks:**
  In `src/log_pipeline/models.py`, added validators on `LogEventRecord`:
  - Enforces `latency_ms >= 0.0`.
  - Rejects `nan`, `inf`, and `-inf`.
  - Ensures HTTP status is within `[100, 599]`.
- **Quarantine Routing:**
  Malformed lines are captured with granular diagnostic reason codes (`MALFORMED_LATENCY`, `NEGATIVE_LATENCY`, `INVALID_HTTP_STATUS`) and flushed to quarantine without halting the pipeline.

### 3. Regression Suite
- Added `tests/test_stage1_text_parser_hardening.py` (4 tests) covering:
  - Unit parsing (`ms`, `s`, `us`)
  - Malformed strings (`1.2.3ms`)
  - Boundary rejection (`-10ms`, `nan`, `inf`)
  - End-to-end pipeline recovery

---

## Stage 1B-3: Database Foreign-Key Referential Integrity

### 1. Objective
Establish referential integrity between pipeline runs and their child records (`log_events` and `rejected_records`) at the database schema level.

### 2. Implementation Details
- **Schema ForeignKey Declarations:**
  In `src/log_pipeline/db/schema.py`:
  - `LogEventModel.run_id`: Declared with `ForeignKey("pipeline_runs.run_id")`.
  - `RejectedRecordModel.run_id`: Declared with `ForeignKey("pipeline_runs.run_id")`.
- **SQLite Foreign-Key Pragma Hook:**
  In `src/log_pipeline/db/connection.py`, attached a SQLAlchemy `connect` event listener to ensure `PRAGMA foreign_keys=ON` is executed on every newly established SQLite connection.
- **Orphan Prevention:**
  Direct inserts of events or rejected records referencing an uncommitted or invalid `run_id` fail with `sqlalchemy.exc.IntegrityError`.

### 3. Regression Suite
- Added `tests/test_stage1_fk_enforcement.py` (8 tests) verifying:
  - Pragma enabled on fresh SQLite connections
  - Valid foreign-key insertions
  - Constraint failure on non-existent `run_id`
  - Writer batch execution foreign-key enforcement

---

## Stage 1C: Run Lifecycle, Terminal Immutability & Failure Recovery

### 1. Objective
Ensure pipeline execution history is auditable and recoverable, preventing historical run overwrite and eliminating data loss of buffered records during failures.

### 2. Implementation Details
- **Terminal Status Immutability:**
  In `src/log_pipeline/db/writer.py` (`complete_run()`) and `src/log_pipeline/pipeline.py` (`run()`):
  - Once a run completes with a terminal status (`SUCCESS`, `PARTIAL_SUCCESS`, `FAILED`), its status and counts are frozen.
  - A subsequent call attempting to execute with an already finalized `run_id` raises a `ValueError` immediately.
  - `complete_run()` logs a warning and refuses to overwrite existing terminal runs.
- **Failure Recovery & Buffer Flushing:**
  In `src/log_pipeline/pipeline.py`, wrapped execution in comprehensive exception handling:
  1. Any buffered rejected records in `rejected_batch` are immediately flushed to the `rejected_records` database table.
  2. The quarantine disk file is exported.
  3. The run status is recorded as `FAILED` with sanitized error details and the requested input manifest.
  4. The original exception is re-raised to notify the caller without masking the root cause.

### 3. Regression Suite
- Added `tests/test_stage1_failure_recovery.py` (7 tests) verifying:
  - Run ID consistency throughout processing
  - Preservation of buffered rejections during failures
  - Clear database failure reporting
  - Rejection of overwriting terminal runs
  - Idempotent repeated finalization

---

## Stage 1D: SQLite Parameter Limit Safety & Transactional Chunking

### 1. Objective
Resolve `sqlite3.OperationalError: too many SQL variables` when large batch sizes (such as 5,000) are used with SQLite.

### 2. Implementation Details
- **Dynamic Limit Inspection:**
  In `src/log_pipeline/db/writer.py`:
  - Added `sqlite_max_variables` property.
  - Inspects the active connection using `conn.connection.dbapi_connection.getlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER)` when available.
  - Falls back to SQLite version detection: 32,766 for SQLite 3.32.0+, 999 for legacy SQLite.
- **Safe Statement Chunking:**
  Multi-row inserts are chunked to fit within the bound-variable limit:
  - `LogEventModel` (12 columns): $\lfloor (\text{limit} - \text{fixed}) / 12 \rfloor$ ($\le 2,730$ rows).
  - `RejectedRecordModel` (6 columns): $\lfloor (\text{limit} - \text{fixed}) / 6 \rfloor$ ($\le 5,461$ rows).
- **Single Transaction Atomicity:**
  All chunks within a batch share the parent database session transaction. If any sub-chunk fails, all previous sub-chunks are rolled back atomically.

### 3. Regression Suite
- Added `tests/test_stage1_batch_size_safety.py` (11 tests) verifying:
  - Batch size 5,000 execution on SQLite
  - Exact and boundary chunk limits ($2,730$ and $2,731$)
  - In-batch and DB-level deduplication across chunk boundaries
  - Transaction rollback across chunks
  - Legacy 999 variable limit adaptation

---

## Stage 1E: Configuration Precedence, Path Validation & Unified CLI

### 1. Objective
Enforce strict configuration resolution, validate input paths upfront, prevent credential leaks, and provide a standardized CLI interface.

### 2. Implementation Details
- **4-Tier Configuration Precedence:**
  In `src/log_pipeline/config.py` (`load_config()`):
  1. Explicit function arguments.
  2. Environment variables (`DATABASE_URL` supersedes discrete `DB_*`).
  3. `.env` file values.
  4. Application defaults.
- **Credential Masking:**
  In `DatabaseConfig.__repr__` and `__str__`, implemented URL sanitization via `render_as_string(hide_password=True)` to prevent credentials from appearing in logs or error traces.
- **Input Path Validation & Failure Tracking:**
  In `src/log_pipeline/pipeline.py`:
  - Tables are created prior to path validation.
  - Non-existent files or directories immediately raise `FileNotFoundError`, but record a `FAILED` status entry in `pipeline_runs` with the full requested manifest.
  - Explicitly empty inputs (`files=[]`) produce valid, balanced runs ($0 = 0 + 0 + 0$).
- **Standardized CLI Module:**
  Added `src/log_pipeline/cli.py` and `src/log_pipeline/__main__.py`:
  - Supports `python -m src.log_pipeline` with flags: `--files`, `--input-dir`, `--database-url`, `--batch-size`, `--quarantine-dir`, `--reports-dir`, `--log-level`, `--run-id`, `--json`.

### 3. Regression Suite
- Added `tests/test_stage1_config_input_cli.py` (12 tests) verifying:
  - Missing file and directory handling with failure ledger recording
  - Empty file list and directory handling
  - Precedence order enforcement
  - Credential masking
  - CLI argument validation, human-readable output, and JSON output
  - Protection against overwriting completed terminal runs
