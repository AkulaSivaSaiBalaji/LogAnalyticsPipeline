"""Regression tests for Stage 1B-1: Source Identity and Deterministic Event IDs."""

import json
from datetime import datetime, timezone
from pathlib import Path
import pytest
from sqlalchemy import select, func

from src.log_pipeline.config import PipelineConfig, DatabaseConfig
from src.log_pipeline.db.schema import LogEventModel
from src.log_pipeline.parsers.base import normalize_source_path
from src.log_pipeline.pipeline import LogAnalyticsPipeline


def _create_log_file(path: Path, content: str) -> Path:
    """Helper to create directory and write raw content to file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return path


def test_basename_collision_distinct_producers(tmp_path):
    """Verify that identically named files in different producer folders do not collide.

    Reproduction scenario:
    - producer-a/app.jsonl line 1: INFO 200 latency 10.0ms
    - producer-b/app.jsonl line 1: ERROR 500 latency 100.0ms
    Timestamp, service, endpoint, source line, and message are all identical.

    With basename-only identity ('app.jsonl'), these generated the exact same event ID,
    causing the second file's ERROR record to be dropped as a false duplicate.
    With directory-aware logical source identity, both events must persist and the error
    event must be retained.
    """
    db_path = tmp_path / "collision_test.db"
    f1 = _create_log_file(
        tmp_path / "producer-a" / "app.jsonl",
        json.dumps({
            "timestamp": "2026-10-28T12:00:00Z",
            "severity": "INFO",
            "service": "api",
            "endpoint": "/test",
            "http_status": 200,
            "latency_ms": 10.0,
            "message": "request processed",
        }) + "\n",
    )
    f2 = _create_log_file(
        tmp_path / "producer-b" / "app.jsonl",
        json.dumps({
            "timestamp": "2026-10-28T12:00:00Z",
            "severity": "ERROR",
            "service": "api",
            "endpoint": "/test",
            "http_status": 500,
            "latency_ms": 100.0,
            "message": "request processed",
        }) + "\n",
    )

    cfg = PipelineConfig(
        database=DatabaseConfig(url=f"sqlite:///{db_path}"),
        input_dir=tmp_path,
        quarantine_dir=tmp_path / "quarantine",
        reports_dir=tmp_path / "reports",
        batch_size=100,
    )
    pipeline = LogAnalyticsPipeline(config=cfg)
    res = pipeline.run(files=[f1, f2], run_id="run-collision")

    # 1. Pipeline counts: both valid, neither dropped
    assert res["input_count"] == 2
    assert res["valid_count"] == 2
    assert res["duplicate_count"] == 0
    assert res["rejected_count"] == 0
    assert res["reconciliation"]["is_balanced"] is True

    # 2. Database rows: 2 rows with distinct IDs and source files
    with pipeline.db.session_scope() as session:
        events = session.scalars(select(LogEventModel).order_by(LogEventModel.source_file)).all()
        assert len(events) == 2

        ev_a, ev_b = events[0], events[1]
        assert ev_a.event_id != ev_b.event_id
        assert "producer-a/app.jsonl" in ev_a.source_file
        assert "producer-b/app.jsonl" in ev_b.source_file
        assert ev_a.severity == "INFO"
        assert ev_b.severity == "ERROR"
        assert ev_b.http_status == 500

    # 3. Analytics: error event is preserved
    assert len(res["service_metrics"]) == 1
    sm = res["service_metrics"][0]
    assert sm["service"] == "api"
    assert sm["total_events"] == 2
    assert sm["error_count"] == 1
    assert sm["error_rate_pct"] == 50.0
    assert sm["avg_latency_ms"] == 55.0


def test_basename_collision_rerun_stability(tmp_path):
    """Verify that rerunning files across producer folders produces stable IDs without duplicate rows."""
    db_path = tmp_path / "rerun_stability.db"
    f1 = _create_log_file(
        tmp_path / "producer-a" / "app.jsonl",
        json.dumps({
            "timestamp": "2026-10-28T12:00:00Z",
            "severity": "INFO",
            "service": "api",
            "endpoint": "/test",
            "http_status": 200,
            "latency_ms": 10.0,
            "message": "request processed",
        }) + "\n",
    )
    f2 = _create_log_file(
        tmp_path / "producer-b" / "app.jsonl",
        json.dumps({
            "timestamp": "2026-10-28T12:00:00Z",
            "severity": "ERROR",
            "service": "api",
            "endpoint": "/test",
            "http_status": 500,
            "latency_ms": 100.0,
            "message": "request processed",
        }) + "\n",
    )

    cfg = PipelineConfig(
        database=DatabaseConfig(url=f"sqlite:///{db_path}"),
        input_dir=tmp_path,
        quarantine_dir=tmp_path / "quarantine",
        reports_dir=tmp_path / "reports",
        batch_size=100,
    )
    pipeline = LogAnalyticsPipeline(config=cfg)

    # First run
    res1 = pipeline.run(files=[f1, f2], run_id="run-1")
    assert res1["valid_count"] == 2
    assert res1["duplicate_count"] == 0

    with pipeline.db.session_scope() as session:
        ids_run1 = sorted(session.scalars(select(LogEventModel.event_id)).all())
        assert len(ids_run1) == 2

    # Second run on exact same files
    res2 = pipeline.run(files=[f1, f2], run_id="run-2")
    assert res2["input_count"] == 2
    assert res2["valid_count"] == 0
    assert res2["duplicate_count"] == 2
    assert res2["reconciliation"]["is_balanced"] is True

    # Confirm IDs and row counts in DB are stable
    with pipeline.db.session_scope() as session:
        ids_run2 = sorted(session.scalars(select(LogEventModel.event_id)).all())
        assert ids_run1 == ids_run2
        assert len(ids_run2) == 2


def test_basename_collision_text_logs(tmp_path):
    """Verify semi-structured text logs also retain producer folder identity."""
    db_path = tmp_path / "text_collision.db"
    f1 = _create_log_file(
        tmp_path / "service-east" / "app.log",
        "2026-10-28 12:00:00.000 [INFO] [api] GET /ping 200 10.0ms - healthy\n",
    )
    f2 = _create_log_file(
        tmp_path / "service-west" / "app.log",
        "2026-10-28 12:00:00.000 [ERROR] [api] GET /ping 500 90.0ms - failure\n",
    )

    cfg = PipelineConfig(
        database=DatabaseConfig(url=f"sqlite:///{db_path}"),
        input_dir=tmp_path,
        quarantine_dir=tmp_path / "quarantine",
        reports_dir=tmp_path / "reports",
        batch_size=100,
    )
    pipeline = LogAnalyticsPipeline(config=cfg)
    res = pipeline.run(files=[f1, f2], run_id="run-text")

    assert res["valid_count"] == 2
    assert res["duplicate_count"] == 0

    with pipeline.db.session_scope() as session:
        events = session.scalars(select(LogEventModel).order_by(LogEventModel.source_file)).all()
        assert len(events) == 2
        assert "service-east/app.log" in events[0].source_file
        assert "service-west/app.log" in events[1].source_file


def test_normalize_source_path_properties(tmp_path):
    """Verify normalize_source_path portability, forward slashes, and independence from machine paths."""
    # 1. Path relative to a base directory
    base = tmp_path / "data"
    sub_file = base / "producers" / "sensor" / "stream.jsonl"
    norm = normalize_source_path(sub_file, base_dir=base)
    assert norm == "producers/sensor/stream.jsonl"

    # 2. Windows-style backslashes normalized to forward slashes
    win_style = "producers\\sensor\\stream.jsonl"
    assert normalize_source_path(win_style) == "producers/sensor/stream.jsonl"

    # 3. Simple standalone filename preserved
    assert normalize_source_path("events.json") == "events.json"

    # 4. Immediate parent preserved for arbitrary external/temp paths
    ext_path = Path("/var/log/temp_run/producer-x/app.jsonl")
    assert normalize_source_path(ext_path) == "producer-x/app.jsonl"


def test_external_files_matching_final_components_receive_distinct_identities(tmp_path):
    """Verify external files outside input_dir with matching parent/file names are disambiguated."""
    db_path = tmp_path / "ext_disambiguation.db"
    configured_input = tmp_path / "configured_input"
    configured_input.mkdir()

    f1 = _create_log_file(
        tmp_path / "cluster-alpha" / "logs" / "app.log",
        "2026-10-28 12:00:00.000 [INFO] [auth] GET /verify 200 15.0ms - authenticated\n",
    )
    f2 = _create_log_file(
        tmp_path / "cluster-beta" / "logs" / "app.log",
        "2026-10-28 12:00:00.000 [INFO] [auth] GET /verify 200 15.0ms - authenticated\n",
    )

    cfg = PipelineConfig(
        database=DatabaseConfig(url=f"sqlite:///{db_path}"),
        input_dir=configured_input,
        quarantine_dir=tmp_path / "quarantine",
        reports_dir=tmp_path / "reports",
        batch_size=100,
    )
    pipeline = LogAnalyticsPipeline(config=cfg)
    res = pipeline.run(files=[f1, f2], run_id="run-ext-distinct")

    assert res["input_count"] == 2
    assert res["valid_count"] == 2
    assert res["duplicate_count"] == 0
    assert res["rejected_count"] == 0

    with pipeline.db.session_scope() as session:
        events = session.scalars(select(LogEventModel).order_by(LogEventModel.source_file)).all()
        assert len(events) == 2
        ev1, ev2 = events[0], events[1]
        assert ev1.event_id != ev2.event_id
        assert ev1.source_file == "cluster-alpha/logs/app.log"
        assert ev2.source_file == "cluster-beta/logs/app.log"
        # Ensure no drive letters, backslashes, or machine prefixes
        for sf in (ev1.source_file, ev2.source_file):
            assert "\\" not in sf
            assert ":" not in sf
            assert "pytest" not in sf


def test_input_dir_consistency_across_discovery_modes(tmp_path):
    """Verify that recursive scan, explicit file, and subdirectory argument yield identical source names and IDs."""
    input_dir = tmp_path / "data_input"
    sub_file = _create_log_file(
        input_dir / "service-payment" / "events.jsonl",
        json.dumps({
            "timestamp": "2026-10-28T14:30:00Z",
            "severity": "INFO",
            "service": "payment-api",
            "endpoint": "/charge",
            "http_status": 200,
            "latency_ms": 42.5,
            "message": "charge authorized",
        }) + "\n",
    )

    def run_with_inputs(files_arg, db_name, run_id):
        db_path = tmp_path / db_name
        cfg = PipelineConfig(
            database=DatabaseConfig(url=f"sqlite:///{db_path}"),
            input_dir=input_dir,
            quarantine_dir=tmp_path / f"quarantine_{run_id}",
            reports_dir=tmp_path / f"reports_{run_id}",
            batch_size=100,
        )
        pipe = LogAnalyticsPipeline(config=cfg)
        res = pipe.run(files=files_arg, run_id=run_id)
        assert res["valid_count"] == 1
        with pipe.db.session_scope() as session:
            ev = session.scalars(select(LogEventModel)).one()
            return ev.event_id, ev.source_file

    id_rec, sf_rec = run_with_inputs(None, "db_rec.db", "run-rec")
    id_file, sf_file = run_with_inputs([sub_file], "db_file.db", "run-file")
    id_sub, sf_sub = run_with_inputs([input_dir / "service-payment"], "db_sub.db", "run-sub")

    # All three discovery approaches must produce the exact same logical identity and event ID
    assert sf_rec == "service-payment/events.jsonl"
    assert sf_file == "service-payment/events.jsonl"
    assert sf_sub == "service-payment/events.jsonl"
    assert id_rec == id_file == id_sub


def test_rerun_preserves_ids_no_duplicate_rows_for_external_files(tmp_path):
    """Verify rerunning external files preserves deterministic IDs and does not inflate database."""
    db_path = tmp_path / "rerun_ext.db"
    configured_input = tmp_path / "configured_input"
    configured_input.mkdir()

    f1 = _create_log_file(
        tmp_path / "cluster-alpha" / "logs" / "app.log",
        "2026-10-28 12:00:00.000 [INFO] [auth] GET /verify 200 15.0ms - authenticated\n",
    )
    f2 = _create_log_file(
        tmp_path / "cluster-beta" / "logs" / "app.log",
        "2026-10-28 12:00:00.000 [INFO] [auth] GET /verify 200 15.0ms - authenticated\n",
    )

    cfg = PipelineConfig(
        database=DatabaseConfig(url=f"sqlite:///{db_path}"),
        input_dir=configured_input,
        quarantine_dir=tmp_path / "quarantine",
        reports_dir=tmp_path / "reports",
        batch_size=100,
    )
    pipeline = LogAnalyticsPipeline(config=cfg)

    # Initial run
    res1 = pipeline.run(files=[f1, f2], run_id="run-1")
    assert res1["valid_count"] == 2
    assert res1["duplicate_count"] == 0

    with pipeline.db.session_scope() as session:
        ids_1 = sorted(session.scalars(select(LogEventModel.event_id)).all())
        assert len(ids_1) == 2

    # Rerun with the exact same external inputs
    res2 = pipeline.run(files=[f1, f2], run_id="run-2")
    assert res2["input_count"] == 2
    assert res2["valid_count"] == 0
    assert res2["duplicate_count"] == 2
    assert res2["reconciliation"]["is_balanced"] is True

    with pipeline.db.session_scope() as session:
        ids_2 = sorted(session.scalars(select(LogEventModel.event_id)).all())
        assert ids_1 == ids_2
        assert len(ids_2) == 2


def test_batch_independent_external_file_identity(tmp_path):
    """Verify that an external file receives the exact same logical identity and event ID whether alone,

    alongside another file, or in reversed order (no batch-dependent identity expansion).
    """
    configured_input = tmp_path / "configured_input"
    configured_input.mkdir()

    f1 = _create_log_file(
        tmp_path / "cluster-alpha" / "logs" / "app.log",
        "2026-10-28 12:00:00.000 [INFO] [auth] GET /verify 200 15.0ms - authenticated\n",
    )
    f2 = _create_log_file(
        tmp_path / "cluster-beta" / "logs" / "app.log",
        "2026-10-28 12:00:00.000 [INFO] [auth] GET /verify 200 15.0ms - authenticated\n",
    )

    def run_pipeline(files_list, db_name, run_id):
        db_path = tmp_path / db_name
        cfg = PipelineConfig(
            database=DatabaseConfig(url=f"sqlite:///{db_path}"),
            input_dir=configured_input,
            quarantine_dir=tmp_path / f"quarantine_{run_id}",
            reports_dir=tmp_path / f"reports_{run_id}",
            batch_size=100,
        )
        pipe = LogAnalyticsPipeline(config=cfg)
        pipe.run(files=files_list, run_id=run_id)
        with pipe.db.session_scope() as session:
            evs = session.scalars(select(LogEventModel).order_by(LogEventModel.source_file)).all()
            return {ev.source_file: ev.event_id for ev in evs}

    # Run A: process only cluster-alpha
    res_a = run_pipeline([f1], "db_run_a.db", "run-a")
    assert "cluster-alpha/logs/app.log" in res_a
    id_a = res_a["cluster-alpha/logs/app.log"]

    # Run B: process cluster-alpha AND cluster-beta
    res_b = run_pipeline([f1, f2], "db_run_b.db", "run-b")
    assert "cluster-alpha/logs/app.log" in res_b
    assert "cluster-beta/logs/app.log" in res_b
    id_b_alpha = res_b["cluster-alpha/logs/app.log"]
    id_b_beta = res_b["cluster-beta/logs/app.log"]

    # Run C: process in reversed order
    res_c = run_pipeline([f2, f1], "db_run_c.db", "run-c")
    assert "cluster-alpha/logs/app.log" in res_c
    assert "cluster-beta/logs/app.log" in res_c
    id_c_alpha = res_c["cluster-alpha/logs/app.log"]
    id_c_beta = res_c["cluster-beta/logs/app.log"]

    # Identity and event ID for cluster-alpha must be 100% stable across Run A, B, and C
    assert id_a == id_b_alpha == id_c_alpha
    # Identity and event ID for cluster-beta must be 100% stable across Run B and C
    assert id_b_beta == id_c_beta
    # Alpha and Beta must never collide
    assert id_a != id_b_beta


def test_explicit_logical_source_tuple_support(tmp_path):
    """Verify that callers can supply explicit (path, logical_name) pairs to define source identity."""
    db_path = tmp_path / "explicit_tuple.db"
    f = _create_log_file(
        tmp_path / "anywhere" / "random.log",
        "2026-10-28 12:00:00.000 [INFO] [auth] GET /verify 200 15.0ms - authenticated\n",
    )

    cfg = PipelineConfig(
        database=DatabaseConfig(url=f"sqlite:///{db_path}"),
        input_dir=tmp_path / "empty_input",
        quarantine_dir=tmp_path / "quarantine",
        reports_dir=tmp_path / "reports",
        batch_size=100,
    )
    pipeline = LogAnalyticsPipeline(config=cfg)
    res = pipeline.run(files=[(f, "custom-producer/tenant-42/app.log")], run_id="run-tuple")

    assert res["valid_count"] == 1
    with pipeline.db.session_scope() as session:
        ev = session.scalars(select(LogEventModel)).one()
        assert ev.source_file == "custom-producer/tenant-42/app.log"


def test_ambiguous_identity_collision_raises(tmp_path):
    """Verify that distinct external files with truly colliding logical paths raise a descriptive error."""
    from src.log_pipeline.parsers.base import resolve_source_identities

    # Two distinct paths with identical safe components
    f_d = Path("D:/logs/app.log")
    f_e = Path("E:/logs/app.log")

    with pytest.raises(ValueError, match="Conflicting logical source identity"):
        resolve_source_identities([f_d, f_e])


def test_sanitization_does_not_collapse_unrelated_sources(tmp_path):
    """Verify that directory prefixes like test_service or prod_east are not sanitized away."""
    from src.log_pipeline.parsers.base import normalize_source_path

    p1 = Path("D:/test_service/logs/app.log")
    p2 = Path("D:/test_payment/logs/app.log")

    n1 = normalize_source_path(p1)
    n2 = normalize_source_path(p2)

    assert n1 == "test_service/logs/app.log"
    assert n2 == "test_payment/logs/app.log"
    assert n1 != n2
