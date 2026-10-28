"""LogAnalyticsPipeline package.

A modular, production-ready application log analytics pipeline:
- Ingestion of JSON, JSONL, and structured text logs
- Timestamp and severity canonical normalization
- Schema validation with comprehensive quarantine
- Idempotent relational storage (MySQL/SQLAlchemy)
- Metric aggregation: error rates, latency percentiles (p50/p95/p99)
- Machine-readable CSV summaries and reconciliation reports
"""

__version__ = "0.1.0"
