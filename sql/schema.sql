-- =============================================================================
-- LogAnalyticsPipeline: Database Schema DDL (MySQL 8.0+ / MariaDB 10.5+)
-- Charset: utf8mb4 | Collation: utf8mb4_unicode_ci | Storage Engine: InnoDB
-- =============================================================================

CREATE DATABASE IF NOT EXISTS log_analytics_db
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE log_analytics_db;

-- -----------------------------------------------------------------------------
-- 1. pipeline_runs: Execution ledger for auditability, lineage, and reconciliation
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id VARCHAR(64) NOT NULL,
    start_time DATETIME(6) NOT NULL COMMENT 'Run start timestamp in UTC',
    end_time DATETIME(6) NULL COMMENT 'Run completion timestamp in UTC',
    status VARCHAR(32) NOT NULL DEFAULT 'RUNNING' COMMENT 'RUNNING, SUCCESS, PARTIAL_SUCCESS, FAILED',
    input_files TEXT NOT NULL COMMENT 'JSON array of processed file paths',
    input_count INT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'Total raw lines/records read',
    valid_count INT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'Valid records committed to log_events',
    rejected_count INT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'Records routed to rejected_records',
    duplicate_count INT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'Duplicate records skipped or idempotently updated',
    failure_details TEXT NULL COMMENT 'Exception traceback or failure root cause',
    PRIMARY KEY (run_id),
    INDEX idx_pipeline_runs_start_time (start_time),
    INDEX idx_pipeline_runs_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- -----------------------------------------------------------------------------
-- 2. log_events: Persistent validated and normalized event store
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS log_events (
    event_id VARCHAR(64) NOT NULL COMMENT 'Deterministic SHA-256 hash or provided unique ID',
    run_id VARCHAR(64) NOT NULL COMMENT 'Lineage link to pipeline_runs',
    timestamp DATETIME(6) NOT NULL COMMENT 'Normalized event timestamp in UTC',
    severity VARCHAR(16) NOT NULL COMMENT 'Canonical severity: DEBUG, INFO, WARNING, ERROR, CRITICAL',
    service VARCHAR(64) NOT NULL COMMENT 'Originating service or microservice name',
    endpoint VARCHAR(255) NULL COMMENT 'HTTP endpoint, URL path, or RPC action',
    http_status SMALLINT UNSIGNED NULL COMMENT 'HTTP status code (100 - 599)',
    latency_ms DOUBLE NULL COMMENT 'Request execution latency in milliseconds',
    message TEXT NOT NULL COMMENT 'Log event body or extracted message',
    source_file VARCHAR(255) NOT NULL COMMENT 'Original input filename',
    source_line INT UNSIGNED NOT NULL COMMENT '1-based line number in source file',
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT 'Ingestion time in UTC',
    PRIMARY KEY (event_id),
    INDEX idx_log_events_run_id (run_id),
    INDEX idx_log_events_timestamp (timestamp),
    INDEX idx_log_events_service (service),
    INDEX idx_log_events_severity (severity),
    INDEX idx_log_events_endpoint (endpoint),
    INDEX idx_log_events_svc_ts_sev (service, timestamp, severity)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- -----------------------------------------------------------------------------
-- 3. rejected_records: Quarantine table capturing all invalid or malformed data
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS rejected_records (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    run_id VARCHAR(64) NOT NULL COMMENT 'Pipeline run ID during ingestion',
    source_file VARCHAR(255) NOT NULL COMMENT 'Original input filename',
    source_line INT UNSIGNED NOT NULL COMMENT 'Line number in source file',
    raw_record LONGTEXT NOT NULL COMMENT 'Verbatim raw record string',
    rejection_reasons TEXT NOT NULL COMMENT 'JSON array or delimited list of reasons',
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT 'Quarantine timestamp in UTC',
    PRIMARY KEY (id),
    INDEX idx_rejected_run_id (run_id),
    INDEX idx_rejected_source (source_file, source_line)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
