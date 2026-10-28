"""Analytical metric calculator for error rates, percentiles, and reconciliation."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
import numpy as np
import pandas as pd
from ..models import LogEventRecord


def is_error_event(severity: str, http_status: Optional[float | int]) -> bool:
    """Determine if a log record is classified as an error.

    Business Rule:
    A record is an error if severity is 'ERROR' or 'CRITICAL',
    or if http_status is a server error (500-599).
    HTTP 4xx client errors are not system errors unless severity is ERROR/CRITICAL.
    """
    sev_upper = str(severity).upper() if severity else ""
    if sev_upper in ("ERROR", "CRITICAL"):
        return True
    if http_status is not None and not pd.isna(http_status):
        try:
            status_int = int(http_status)
            if 500 <= status_int <= 599:
                return True
        except (ValueError, TypeError):
            pass
    return False


class MetricsCalculator:
    """Computes hourly and service-level aggregations and latency percentiles."""

    @staticmethod
    def events_to_dataframe(events: List[LogEventRecord]) -> pd.DataFrame:
        """Convert a list of LogEventRecord into a pandas DataFrame."""
        if not events:
            return pd.DataFrame(
                columns=[
                    "event_id",
                    "timestamp",
                    "severity",
                    "service",
                    "endpoint",
                    "http_status",
                    "latency_ms",
                    "message",
                    "source_file",
                    "source_line",
                    "run_id",
                ]
            )

        data = [
            {
                "event_id": ev.event_id,
                "timestamp": ev.timestamp,
                "severity": ev.severity.value,
                "service": ev.service,
                "endpoint": ev.endpoint,
                "http_status": ev.http_status,
                "latency_ms": ev.latency_ms,
                "message": ev.message,
                "source_file": ev.source_file,
                "source_line": ev.source_line,
                "run_id": ev.run_id,
            }
            for ev in events
        ]
        df = pd.DataFrame(data)
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        return df

    @classmethod
    def calculate_aggregates(
        cls, df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Compute hourly and service-level summary metrics.

        Returns:
            Tuple of (hourly_df, service_df).
        """
        if df.empty:
            empty_cols = [
                "service",
                "total_events",
                "error_count",
                "error_rate_pct",
                "latency_count",
                "avg_latency_ms",
                "p50_latency_ms",
                "p95_latency_ms",
                "p99_latency_ms",
            ]
            hourly_empty = pd.DataFrame(columns=["window_hour"] + empty_cols)
            service_empty = pd.DataFrame(columns=empty_cols)
            return hourly_empty, service_empty

        # Mark error flag
        df = df.copy()
        df["is_error"] = df.apply(
            lambda r: is_error_event(r["severity"], r.get("http_status")), axis=1
        )

        # Truncate timestamp to hour window
        df["window_hour"] = df["timestamp"].dt.strftime("%Y-%m-%d %H:00:00")

        # 1. Hourly Service Metrics
        hourly_df = cls._aggregate_group(df, group_keys=["window_hour", "service"])

        # 2. Overall Service Metrics
        service_df = cls._aggregate_group(df, group_keys=["service"])

        return hourly_df, service_df

    @staticmethod
    def _aggregate_group(df: pd.DataFrame, group_keys: List[str]) -> pd.DataFrame:
        rows = []
        for key, group in df.groupby(group_keys, as_index=False):
            if isinstance(key, tuple):
                group_dict = dict(zip(group_keys, key))
            else:
                group_dict = {group_keys[0]: key}

            total_events = len(group)
            error_count = int(group["is_error"].sum())
            error_rate_pct = round((error_count / total_events) * 100.0, 2) if total_events > 0 else 0.0

            # Latency calculations (filtering out NaNs)
            valid_latencies = group["latency_ms"].dropna()
            lat_count = len(valid_latencies)

            if lat_count > 0:
                avg_latency = round(float(valid_latencies.mean()), 2)
                p50 = round(float(valid_latencies.quantile(0.50)), 2)
                p95 = round(float(valid_latencies.quantile(0.95)), 2)
                p99 = round(float(valid_latencies.quantile(0.99)), 2)
            else:
                avg_latency = None
                p50 = None
                p95 = None
                p99 = None

            row_data = {
                **group_dict,
                "total_events": total_events,
                "error_count": error_count,
                "error_rate_pct": error_rate_pct,
                "latency_count": lat_count,
                "avg_latency_ms": avg_latency,
                "p50_latency_ms": p50,
                "p95_latency_ms": p95,
                "p99_latency_ms": p99,
            }
            rows.append(row_data)

        res_df = pd.DataFrame(rows)
        if not res_df.empty:
            res_df.sort_values(by=group_keys, inplace=True)
            res_df.reset_index(drop=True, inplace=True)
        return res_df

    @staticmethod
    def calculate_reconciliation(
        run_id: str,
        input_count: int,
        valid_count: int,
        rejected_count: int,
        duplicate_count: int,
    ) -> Dict[str, Any]:
        """Compute data reconciliation invariants and balance status."""
        accounted_total = valid_count + rejected_count + duplicate_count
        discrepancy = input_count - accounted_total
        is_balanced = discrepancy == 0

        return {
            "run_id": run_id,
            "input_count": input_count,
            "valid_count": valid_count,
            "rejected_count": rejected_count,
            "duplicate_count": duplicate_count,
            "accounted_total": accounted_total,
            "discrepancy": discrepancy,
            "is_balanced": is_balanced,
            "status": "BALANCED" if is_balanced else "DISCREPANCY_DETECTED",
        }
