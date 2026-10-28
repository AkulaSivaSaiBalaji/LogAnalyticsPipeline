-- =============================================================================
-- LogAnalyticsPipeline: Data Reconciliation & Quality Verification Queries
-- =============================================================================

USE log_analytics;

-- 1. Pipeline Run Accounting Reconciliation
-- Verifies the fundamental invariant: input_count = valid_count + rejected_count + duplicate_count
SELECT 
    run_id,
    status,
    input_count,
    valid_count,
    rejected_count,
    duplicate_count,
    (valid_count + rejected_count + duplicate_count) AS calculated_total,
    CASE 
        WHEN input_count = (valid_count + rejected_count + duplicate_count) THEN 'BALANCED'
        ELSE 'DISCREPANCY'
    END AS reconciliation_status
FROM pipeline_runs
ORDER BY start_time DESC;

-- 2. Quarantine Breakdown by Failure Reason
SELECT 
    rejection_reasons,
    COUNT(*) AS failure_count
FROM rejected_records
GROUP BY rejection_reasons
ORDER BY failure_count DESC;

-- 3. Ingested Events by Service and Severity
SELECT 
    service,
    severity,
    COUNT(*) AS event_count,
    MIN(timestamp) AS earliest_event_utc,
    MAX(timestamp) AS latest_event_utc
FROM log_events
GROUP BY service, severity
ORDER BY service, event_count DESC;

-- 4. Hourly Latency and Error Verification Query
SELECT 
    service,
    DATE_FORMAT(timestamp, '%Y-%m-%d %H:00:00') AS log_hour,
    COUNT(*) AS total_events,
    SUM(CASE WHEN severity IN ('ERROR', 'CRITICAL') OR (http_status >= 500) THEN 1 ELSE 0 END) AS error_events,
    ROUND(
        SUM(CASE WHEN severity IN ('ERROR', 'CRITICAL') OR (http_status >= 500) THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 
        2
    ) AS error_rate_pct,
    ROUND(AVG(latency_ms), 2) AS avg_latency_ms
FROM log_events
GROUP BY service, DATE_FORMAT(timestamp, '%Y-%m-%d %H:00:00')
ORDER BY log_hour DESC, service;
