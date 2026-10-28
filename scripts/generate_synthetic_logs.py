#!/usr/bin/env python3
"""Reproducible synthetic log generator for LogAnalyticsPipeline.

Generates realistic structured JSONL and semi-structured text logs
with a deterministic random seed. Includes valid events, intentional duplicates,
malformed syntax, out-of-order timestamps, and boundary validation errors.
"""

from __future__ import annotations

import argparse
import json
import random
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

SERVICES = [
    ("auth-service", ["/api/v1/login", "/api/v1/logout", "/api/v1/session", "/api/v1/token"]),
    ("payment-service", ["/api/v1/charge", "/api/v1/refund", "/api/v1/cards"]),
    ("order-service", ["/api/v1/orders", "/api/v1/checkout", "/api/v1/cart"]),
    ("catalog-service", ["/api/v1/products", "/api/v1/categories", "/api/v1/search"]),
    ("notification-worker", [None]),
]

HTTP_STATUS_WEIGHTS = [
    (200, 0.70),
    (201, 0.10),
    (400, 0.05),
    (401, 0.03),
    (404, 0.04),
    (500, 0.06),
    (503, 0.02),
]

SEVERITIES = ["INFO", "INFO", "INFO", "WARN", "WARNING", "DEBUG", "ERROR", "CRITICAL"]


def generate_logs(
    num_records: int = 10000,
    seed: int = 42,
    output_dir: Path = Path("data/synthetic"),
    file_prefix: str = "synthetic_logs",
) -> Path:
    """Generate reproducible synthetic log datasets."""
    random.seed(seed)
    output_dir.mkdir(parents=True, exist_ok=True)
    out_file = output_dir / f"{file_prefix}_{num_records}.jsonl"

    base_time = datetime(2026, 10, 28, 8, 0, 0, tzinfo=timezone.utc)
    recent_valid_events = []

    statuses, weights = zip(*HTTP_STATUS_WEIGHTS)

    with open(out_file, "w", encoding="utf-8") as f:
        for i in range(num_records):
            roll = random.random()

            # 1. 5% Intentional Duplicates (exact duplicate of a recent event)
            if roll < 0.05 and recent_valid_events:
                dupe = random.choice(recent_valid_events)
                f.write(json.dumps(dupe) + "\n")
                continue

            # 2. 3% Malformed JSON Syntax (broken quotes, truncated braces)
            if roll < 0.08:
                f.write(f'{{"timestamp": "2026-10-28T10:00:00Z", "service": "auth", "corrupt_line_{i}\n')
                continue

            # 3. 2% Negative Latency
            if roll < 0.10:
                service, endpoints = random.choice(SERVICES)
                record = {
                    "timestamp": (base_time + timedelta(seconds=i)).isoformat(),
                    "severity": "INFO",
                    "service": service,
                    "endpoint": random.choice(endpoints),
                    "http_status": 200,
                    "latency_ms": -1.0 * random.uniform(5.0, 50.0),
                    "message": "Transaction completed",
                }
                f.write(json.dumps(record) + "\n")
                continue

            # 4. 2% Invalid HTTP status or unparseable timestamp
            if roll < 0.12:
                service, endpoints = random.choice(SERVICES)
                record = {
                    "timestamp": "INVALID_TS_STRING" if random.random() < 0.5 else (base_time + timedelta(seconds=i)).isoformat(),
                    "severity": "INFO",
                    "service": service,
                    "endpoint": random.choice(endpoints),
                    "http_status": 999 if random.random() < 0.5 else 50,
                    "latency_ms": random.uniform(10.0, 100.0),
                    "message": "Malformed schema probe",
                }
                f.write(json.dumps(record) + "\n")
                continue

            # 5. 2% Out-of-order / Late-arriving timestamps
            if roll < 0.14:
                offset_seconds = random.randint(-7200, 7200)
                event_ts = base_time + timedelta(seconds=i + offset_seconds)
            else:
                event_ts = base_time + timedelta(seconds=i)

            # 6. Standard Valid Event
            service, endpoints = random.choice(SERVICES)
            endpoint = random.choice(endpoints)
            status = random.choices(statuses, weights=weights)[0]
            severity = "ERROR" if status >= 500 else random.choice(SEVERITIES)
            latency = round(random.uniform(2.0, 350.0), 2) if endpoint else None

            valid_record = {
                "timestamp": event_ts.isoformat(),
                "severity": severity,
                "service": service,
                "endpoint": endpoint,
                "http_status": status if endpoint else None,
                "latency_ms": latency,
                "message": f"Processed {service} action {i}",
            }
            f.write(json.dumps(valid_record) + "\n")

            if len(recent_valid_events) < 500:
                recent_valid_events.append(valid_record)
            else:
                recent_valid_events[i % 500] = valid_record

    return out_file


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate synthetic log data.")
    parser.add_argument("--records", type=int, default=10000, help="Number of records to generate")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility")
    parser.add_argument("--output-dir", type=str, default="data/synthetic", help="Output directory")
    args = parser.parse_args()

    generated_path = generate_logs(
        num_records=args.records,
        seed=args.seed,
        output_dir=Path(args.output_dir),
    )
    print(f"Generated {args.records} synthetic records at {generated_path}")
