# Data Dictionary: LogAnalyticsPipeline

## 1. Relational Database Tables (MySQL / MariaDB)

### 1.1 `pipeline_runs`
Tracks execution metadata, lineage, reconciliation accounting, and run health.

| Column Name | Data Type | Nullable | Key / Index | Description |
|---|---|---|---|---|
| `run_id` | `VARCHAR(64)` | No | PRIMARY KEY | Unique identifier for the pipeline run (e.g., `run_20261028_100000_a1b2c3`). |
| `start_time` | `DATETIME(6)` | No | INDEX | UTC timestamp when processing started. |
| `end_time` | `DATETIME(6)` | Yes | - | UTC timestamp when processing concluded. |
| `status` | `VARCHAR(32)` | No | INDEX | Terminal status: `RUNNING`, `SUCCESS`, `PARTIAL_SUCCESS`, `FAILED`. |
| `input_files` | `TEXT` | No | - | JSON serialized list of source file paths processed during the run. |
| `input_count` | `INT UNSIGNED` | No | - | Total raw records read from source files. |
| `valid_count` | `INT UNSIGNED` | No | - | Total new valid records committed to `log_events`. |
| `rejected_count`| `INT UNSIGNED` | No | - | Total invalid/malformed records committed to `rejected_records`. |
| `duplicate_count`| `INT UNSIGNED` | No | - | Total duplicate records safely skipped to ensure idempotency. |
| `failure_details`| `TEXT` | Yes | - | Sanitized exception type and traceback summary on fatal abort. |

---

### 1.2 `log_events`
Stores normalized, schema-validated analytical log records.

| Column Name | Data Type | Nullable | Key / Index | Description |
|---|---|---|---|---|
| `event_id` | `VARCHAR(64)` | No | PRIMARY KEY | Deterministic 32-char SHA-256 hash or provided UUID. |
| `run_id` | `VARCHAR(64)` | No | INDEX | Lineage pointer to `pipeline_runs.run_id`. |
| `timestamp` | `DATETIME(6)` | No | INDEX | Event occurrence timestamp normalized to UTC. |
| `severity` | `VARCHAR(16)` | No | INDEX | Canonical severity: `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`. |
| `service` | `VARCHAR(64)` | No | INDEX | Microservice or subsystem name emitting the log. |
| `endpoint` | `VARCHAR(255)` | Yes | INDEX | HTTP route, URL path, or RPC action (e.g., `/api/v1/checkout`). |
| `http_status` | `SMALLINT UNSIGNED`| Yes | - | HTTP response status code in range [100, 599]. |
| `latency_ms` | `DOUBLE` | Yes | - | Request processing latency in milliseconds ($\ge 0.0$). |
| `message` | `TEXT` | No | - | Human-readable log payload or error message. |
| `source_file` | `VARCHAR(255)` | No | - | Originating source file name. |
| `source_line` | `INT UNSIGNED` | No | - | 1-indexed line number in source file. |
| `created_at` | `DATETIME(6)` | No | - | Database ingestion UTC timestamp. |

*Composite Indexes*:
- `idx_log_events_svc_ts_sev (service, timestamp, severity)`: Optimized for hourly service rollups.

---

### 1.3 `rejected_records`
Quarantine store capturing unparsed or invalid records without data loss.

| Column Name | Data Type | Nullable | Key / Index | Description |
|---|---|---|---|---|
| `id` | `BIGINT UNSIGNED`| No | PRIMARY KEY (AUTO_INC) | Surrogate quarantine sequence ID. |
| `run_id` | `VARCHAR(64)` | No | INDEX | Lineage pointer to `pipeline_runs.run_id`. |
| `source_file` | `VARCHAR(255)` | No | INDEX (composite) | Originating source file name. |
| `source_line` | `INT UNSIGNED` | No | INDEX (composite) | Line number where failure occurred. |
| `raw_record` | `LONGTEXT` | No | - | Exact verbatim unparsed raw record content. |
| `rejection_reasons`| `TEXT` | No | - | JSON array of explicit validation/parser failure codes. |
| `created_at` | `DATETIME(6)` | No | - | Quarantine insertion timestamp in UTC. |

---

## 2. Exported Analytical CSV Reports

### 2.1 `<run_id>_hourly_metrics.csv`
Aggregated hourly operational metrics segmented by service.

| Field Name | Type | Description |
|---|---|---|
| `window_hour` | `string` | Hourly rollup timestamp in UTC (`YYYY-MM-DD HH:00:00`). |
| `service` | `string` | Service identifier. |
| `total_events` | `integer` | Count of valid log events in this window. |
| `error_count` | `integer` | Count of events with `severity IN ('ERROR', 'CRITICAL')` or `http_status >= 500`. |
| `error_rate_pct`| `float` | Percentage of events that were errors: `(error_count / total_events) * 100.0`. |
| `latency_count`| `integer` | Count of events with non-null latency measurements. |
| `avg_latency_ms`| `float` | Arithmetic mean latency (ms), excluding missing values. |
| `p50_latency_ms`| `float` | 50th percentile (median) latency (ms). `null` if `latency_count == 0`. |
| `p95_latency_ms`| `float` | 95th percentile latency (ms). `null` if `latency_count == 0`. |
| `p99_latency_ms`| `float` | 99th percentile latency (ms). `null` if `latency_count == 0`. |

---

### 2.2 `<run_id>_service_summary.csv`
Aggregated lifecycle metrics across the entire run window segmented by service.

Same schema as `hourly_metrics.csv` without the `window_hour` grouping.

---

### 2.3 `<run_id>_reconciliation.csv`
Auditing record proving mathematical completeness.

| Field Name | Type | Description |
|---|---|---|
| `run_id` | `string` | Pipeline execution ID. |
| `input_count` | `integer` | Total scanned raw records. |
| `valid_count` | `integer` | Ingested valid records. |
| `rejected_count`| `integer` | Quarantined invalid records. |
| `duplicate_count`| `integer` | Duplicate records safely ignored. |
| `accounted_total`| `integer` | `valid_count + rejected_count + duplicate_count`. |
| `discrepancy` | `integer` | `input_count - accounted_total` (must equal `0`). |
| `is_balanced` | `boolean` | `true` if `discrepancy == 0`. |
| `status` | `string` | `BALANCED` or `DISCREPANCY_DETECTED`. |
