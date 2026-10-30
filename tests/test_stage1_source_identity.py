"""Regression tests for Stage 1B-1: Source Identity and Deterministic Event IDs."""

import json
import os
from datetime import datetime, timezone
from pathlib import Path
import pytest
from sqlalchemy import select, func

from src.log_pipeline.config import PipelineConfig, DatabaseConfig
from src.log_pipeline.db.schema import LogEventModel
from pathlib import PurePosixPath, PureWindowsPath
from src.log_pipeline.parsers.base import (
    canonical_path_key,
    is_path_case_sensitive,
    normalize_source_path,
    resolve_source_identities,
)
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


def test_windows_separators_on_linux_and_posix_normalization():
    """Verify that Windows-style backslash separators normalize properly on both POSIX and Windows."""
    # Strings with backslashes
    assert normalize_source_path(r"producers\sensor\stream.jsonl") == "producers/sensor/stream.jsonl"
    assert normalize_source_path(r"cluster-alpha\logs\app.log") == "cluster-alpha/logs/app.log"
    assert normalize_source_path(r"producer-a\app.jsonl") == "producer-a/app.jsonl"

    # PurePosixPath simulating Linux parsing of Windows-style paths
    assert normalize_source_path(PurePosixPath(r"producers\sensor\stream.jsonl")) == "producers/sensor/stream.jsonl"
    assert normalize_source_path(PurePosixPath(r"cluster-alpha\logs\app.log")) == "cluster-alpha/logs/app.log"
    assert normalize_source_path(PureWindowsPath(r"producers\sensor\stream.jsonl")) == "producers/sensor/stream.jsonl"


def test_posix_paths_normalization():
    """Verify standard POSIX forward-slash paths normalize cleanly."""
    assert normalize_source_path("producers/sensor/stream.jsonl") == "producers/sensor/stream.jsonl"
    assert normalize_source_path(PurePosixPath("producers/sensor/stream.jsonl")) == "producers/sensor/stream.jsonl"
    assert normalize_source_path("/var/log/temp_run/producer-x/app.jsonl") == "producer-x/app.jsonl"
    assert normalize_source_path(PurePosixPath("/var/log/temp_run/producer-x/app.jsonl")) == "producer-x/app.jsonl"
    assert normalize_source_path("cluster-alpha/logs/app.log") == "cluster-alpha/logs/app.log"
    assert normalize_source_path(PurePosixPath("cluster-alpha/logs/app.log")) == "cluster-alpha/logs/app.log"


def test_windows_drive_letters_normalization():
    """Verify Windows drive letters are stripped from logical source identities on all platforms."""
    # Forward slash after drive
    assert normalize_source_path("D:/logs/producer-a/app.jsonl") == "logs/producer-a/app.jsonl"
    assert normalize_source_path(PurePosixPath("D:/logs/producer-a/app.jsonl")) == "logs/producer-a/app.jsonl"

    # Backslash after drive
    assert normalize_source_path(r"D:\logs\producer-a\app.jsonl") == "logs/producer-a/app.jsonl"
    assert normalize_source_path(PurePosixPath(r"D:\logs\producer-a\app.jsonl")) == "logs/producer-a/app.jsonl"

    # Other drives
    assert normalize_source_path("E:/logs/producer-b/app.jsonl") == "logs/producer-b/app.jsonl"
    assert normalize_source_path(PurePosixPath("E:/logs/producer-b/app.jsonl")) == "logs/producer-b/app.jsonl"
    assert normalize_source_path("C:/producers/sensor/stream.jsonl") == "producers/sensor/stream.jsonl"
    assert normalize_source_path(PurePosixPath("C:/producers/sensor/stream.jsonl")) == "producers/sensor/stream.jsonl"


def test_equivalent_path_representations():
    """Verify equivalent path representations produce identical logical source identities."""
    equivs = [
        "producers/sensor/stream.jsonl",
        r"producers\sensor\stream.jsonl",
        "./producers/sensor/stream.jsonl",
        r".\producers\sensor\stream.jsonl",
        "producers/./sensor//stream.jsonl",
        r"producers\.\sensor\\stream.jsonl",
    ]
    expected = "producers/sensor/stream.jsonl"
    for p in equivs:
        assert normalize_source_path(p) == expected
        assert normalize_source_path(PurePosixPath(p)) == expected
        assert normalize_source_path(PureWindowsPath(p)) == expected

    # Drive path equivalences
    assert (
        normalize_source_path("D:/logs/producer-a/app.jsonl")
        == normalize_source_path(r"D:\logs\producer-a\app.jsonl")
        == "logs/producer-a/app.jsonl"
    )


def test_different_producer_directories_with_identical_filenames(tmp_path):
    """Verify different producer directories with identical filenames resolve without collision."""
    f1 = "D:/logs/producer-a/app.jsonl"
    f2 = "E:/logs/producer-b/app.jsonl"

    res = resolve_source_identities([f1, f2])
    assert res[f1] == "logs/producer-a/app.jsonl"
    assert res[f2] == "logs/producer-b/app.jsonl"
    assert res[f1] != res[f2]

    # Also test with PurePosixPath (simulating Linux execution)
    p1 = PurePosixPath(f1)
    p2 = PurePosixPath(f2)
    res_posix = resolve_source_identities([p1, p2])
    assert res_posix[p1] == "logs/producer-a/app.jsonl"
    assert res_posix[p2] == "logs/producer-b/app.jsonl"
    assert res_posix[p1] != res_posix[p2]

    # End-to-end pipeline run with physical files in different producer dirs
    db_path = tmp_path / "distinct_producers.db"
    file_a = _create_log_file(
        tmp_path / "region-us" / "producer-1" / "stream.jsonl",
        json.dumps({
            "timestamp": "2026-10-28T12:00:00Z",
            "severity": "INFO",
            "service": "ingest",
            "endpoint": "/upload",
            "http_status": 200,
            "latency_ms": 12.0,
            "message": "batch uploaded",
        }) + "\n",
    )
    file_b = _create_log_file(
        tmp_path / "region-eu" / "producer-2" / "stream.jsonl",
        json.dumps({
            "timestamp": "2026-10-28T12:00:00Z",
            "severity": "INFO",
            "service": "ingest",
            "endpoint": "/upload",
            "http_status": 200,
            "latency_ms": 15.0,
            "message": "batch uploaded",
        }) + "\n",
    )

    cfg = PipelineConfig(
        database=DatabaseConfig(url=f"sqlite:///{db_path}"),
        input_dir=tmp_path / "inputs",
        quarantine_dir=tmp_path / "q",
        reports_dir=tmp_path / "r",
    )
    pipeline = LogAnalyticsPipeline(config=cfg)
    res = pipeline.run(files=[file_a, file_b], run_id="run-distinct-producers")
    assert res["valid_count"] == 2
    assert res["duplicate_count"] == 0

    with pipeline.db.session_scope() as session:
        events = session.scalars(select(LogEventModel).order_by(LogEventModel.source_file)).all()
        assert len(events) == 2
        assert events[0].event_id != events[1].event_id
        assert "region-eu/producer-2/stream.jsonl" in events[0].source_file
        assert "region-us/producer-1/stream.jsonl" in events[1].source_file


def test_ambiguous_logical_source_collisions_posix_and_windows():
    """Verify distinct files with different drive letters but identical remaining components raise collision on all platforms."""
    # String paths
    with pytest.raises(ValueError, match="Conflicting logical source identity"):
        resolve_source_identities(["D:/logs/app.log", "E:/logs/app.log"])

    # PurePosixPath (simulating Linux execution where D: and E: are distinct)
    with pytest.raises(ValueError, match="Conflicting logical source identity"):
        resolve_source_identities([PurePosixPath("D:/logs/app.log"), PurePosixPath("E:/logs/app.log")])

    # Windows backslash paths
    with pytest.raises(ValueError, match="Conflicting logical source identity"):
        resolve_source_identities([r"D:\logs\app.log", r"E:\logs\app.log"])


def test_stable_identities_across_different_working_directories(tmp_path, monkeypatch):
    """Verify changing current working directory does not alter logical source identities or event IDs."""
    input_dir = tmp_path / "inputs"
    sample_file = _create_log_file(
        input_dir / "service-auth" / "login.jsonl",
        json.dumps({
            "timestamp": "2026-10-28T12:00:00Z",
            "severity": "INFO",
            "service": "auth",
            "endpoint": "/login",
            "http_status": 200,
            "latency_ms": 25.0,
            "message": "user logged in",
        }) + "\n",
    )

    other_cwd1 = tmp_path / "workdir_alpha"
    other_cwd2 = tmp_path / "workdir_beta"
    other_cwd1.mkdir()
    other_cwd2.mkdir()

    def run_in_cwd(cwd_path, db_name):
        monkeypatch.chdir(cwd_path)
        cfg = PipelineConfig(
            database=DatabaseConfig(url=f"sqlite:///{tmp_path / db_name}"),
            input_dir=input_dir,
            quarantine_dir=tmp_path / f"q_{db_name}",
            reports_dir=tmp_path / f"r_{db_name}",
        )
        pipe = LogAnalyticsPipeline(config=cfg)
        res = pipe.run(files=[sample_file], run_id=f"run-{db_name}")
        assert res["valid_count"] == 1
        with pipe.db.session_scope() as session:
            ev = session.scalars(select(LogEventModel)).one()
            return ev.event_id, ev.source_file

    id1, sf1 = run_in_cwd(other_cwd1, "db1.db")
    id2, sf2 = run_in_cwd(other_cwd2, "db2.db")

    assert sf1 == "service-auth/login.jsonl"
    assert sf2 == "service-auth/login.jsonl"
    assert id1 == id2


def test_moving_input_root_preserves_event_identity(tmp_path):
    """Verify moving the entire input root directory produces identical source names and deterministic event IDs."""
    root1 = tmp_path / "initial_root"
    root2 = tmp_path / "moved_root"

    content = json.dumps({
        "timestamp": "2026-10-28T12:00:00Z",
        "severity": "INFO",
        "service": "payment",
        "endpoint": "/charge",
        "http_status": 200,
        "latency_ms": 50.0,
        "message": "charge authorized",
    }) + "\n"

    f1 = _create_log_file(root1 / "producer-payment" / "events.jsonl", content)
    f2 = _create_log_file(root2 / "producer-payment" / "events.jsonl", content)

    def run_root(root_dir, file_path, db_name):
        cfg = PipelineConfig(
            database=DatabaseConfig(url=f"sqlite:///{tmp_path / db_name}"),
            input_dir=root_dir,
            quarantine_dir=tmp_path / f"q_{db_name}",
            reports_dir=tmp_path / f"r_{db_name}",
        )
        pipe = LogAnalyticsPipeline(config=cfg)
        res = pipe.run(files=[file_path], run_id=f"run-{db_name}")
        assert res["valid_count"] == 1
        with pipe.db.session_scope() as session:
            ev = session.scalars(select(LogEventModel)).one()
            return ev.event_id, ev.source_file

    id1, sf1 = run_root(root1, f1, "db_root1.db")
    id2, sf2 = run_root(root2, f2, "db_root2.db")

    assert sf1 == "producer-payment/events.jsonl"
    assert sf2 == "producer-payment/events.jsonl"
    assert id1 == id2


def test_posix_mismatched_base_dir_not_stripped():
    """Verify that a POSIX path outside base_dir is not stripped as if it were inside."""
    norm = normalize_source_path("/data/producers/app.log", base_dir="/etc/nginx")
    assert norm == "data/producers/app.log"

    # resolve_source_identities with external posix path and input_dir
    res = resolve_source_identities(["/data/producers/app.log"], input_dir="/etc/nginx")
    assert res["/data/producers/app.log"] == "data/producers/app.log"


def test_unc_paths_case_insensitivity_and_resolution():
    """Verify UNC paths handle case insensitivity and slash variations properly."""
    u1 = "//SERVER/SHARE/logs/producer/app.log"
    u2 = r"\\server\share\logs\producer\app.log"

    assert normalize_source_path(u1) == normalize_source_path(u2) == "logs/producer/app.log"
    assert normalize_source_path(u1, base_dir="//server/share/logs") == "producer/app.log"
    assert normalize_source_path(u2, base_dir=r"\\SERVER\SHARE\logs") == "producer/app.log"


def test_case_sensitive_filesystem_existing_files(tmp_path):
    """Verify distinct existing files differing only by case have different canonical keys on case-sensitive filesystems."""
    probe_a = tmp_path / "probe_case"
    probe_a.write_text("a", encoding="utf-8")
    probe_b = tmp_path / "PROBE_CASE"
    is_case_sensitive = not probe_b.exists()

    if is_case_sensitive:
        f1 = tmp_path / "App.log"
        f2 = tmp_path / "app.log"
        f1.write_text("app upper", encoding="utf-8")
        f2.write_text("app lower", encoding="utf-8")

        ck1 = canonical_path_key(f1)
        ck2 = canonical_path_key(f2)
        assert ck1 != ck2, f"Canonical keys must differ on case-sensitive filesystem: {ck1} vs {ck2}"

        # Assigning those two files the same explicit logical name must raise ValueError
        with pytest.raises(ValueError, match="Conflicting logical source identity"):
            resolve_source_identities([(f1, "common_app.log"), (f2, "common_app.log")])


def test_simulated_existing_posix_files_differing_by_case(monkeypatch):
    """Verify that existing POSIX files differing only by case have different keys and raise on collision."""
    f1 = Path("/var/log/App.log")
    f2 = Path("/var/log/app.log")

    # Simulate existing POSIX files on a case-sensitive filesystem
    monkeypatch.setattr(Path, "resolve", lambda self: self)
    monkeypatch.setattr(Path, "exists", lambda self: self.as_posix() in ("/var/log/App.log", "/var/log/app.log"))

    ck1 = canonical_path_key(f1)
    ck2 = canonical_path_key(f2)
    assert ck1 != ck2
    assert ck1 == "/var/log/App.log"
    assert ck2 == "/var/log/app.log"

    # Assigning those two files the same explicit logical name must raise ValueError
    with pytest.raises(ValueError, match="Conflicting logical source identity"):
        resolve_source_identities([(f1, "common_log"), (f2, "common_log")])


def test_synthetic_posix_paths_case_preserved():
    """Verify synthetic/non-existent POSIX paths differing only by case do not produce identical keys."""
    p1 = "/var/log/service/App.log"
    p2 = "/var/log/service/app.log"
    ck1 = canonical_path_key(p1)
    ck2 = canonical_path_key(p2)
    assert ck1 != ck2
    assert ck1 == "|True|var/log/service/App.log"
    assert ck2 == "|True|var/log/service/app.log"

    # Assigning those two files the same explicit logical name must raise ValueError
    with pytest.raises(ValueError, match="Conflicting logical source identity"):
        resolve_source_identities([(p1, "shared_service.log"), (p2, "shared_service.log")])

    # Relative synthetic paths also preserve case
    r1 = "producer/Sensor/stream.jsonl"
    r2 = "producer/sensor/stream.jsonl"
    assert canonical_path_key(r1) != canonical_path_key(r2)


def test_windows_drive_and_unc_case_insensitive_canonical_keys():
    """Verify Windows drive and UNC paths continue to normalize case-insensitively."""
    # Windows drive paths
    w1 = "D:/Logs/Producer/App.log"
    w2 = "d:/logs/producer/app.log"
    assert canonical_path_key(w1) == canonical_path_key(w2)

    # UNC paths
    u1 = "//Server/Share/Logs/App.log"
    u2 = r"\\server\share\logs\app.log"
    assert canonical_path_key(u1) == canonical_path_key(u2)


def test_is_path_case_sensitive_mocked_macos_apfs(monkeypatch):
    """Verify macOS APFS volume: POSIX platform, but case-swapped probe refers to the same file."""
    import sys
    p = Path("/Volumes/Data/Logs/App.log")
    monkeypatch.setattr(Path, "resolve", lambda self: self)
    monkeypatch.setattr(Path, "exists", lambda self: True)
    monkeypatch.setattr(os.path, "samefile", lambda p1, p2: True)
    monkeypatch.setattr(sys, "platform", "darwin")
    monkeypatch.setattr(os, "name", "posix")

    assert is_path_case_sensitive(p) is False
    assert canonical_path_key(p) == "/volumes/data/logs/app.log"


def test_is_path_case_sensitive_mocked_linux_ext4(monkeypatch):
    """Verify Linux ext4 volume: POSIX platform, case-swapped probe does not exist."""
    import sys
    p = Path("/var/log/service/App.log")
    monkeypatch.setattr(Path, "resolve", lambda self: self)
    monkeypatch.setattr(Path, "exists", lambda self: "aPP.LOG" not in self.as_posix())
    monkeypatch.setattr(sys, "platform", "linux")
    monkeypatch.setattr(os, "name", "posix")

    assert is_path_case_sensitive(p) is True
    assert canonical_path_key(p) == "/var/log/service/App.log"


def test_is_path_case_sensitive_mocked_windows_cs_dir(monkeypatch):
    """Verify Windows directory with per-directory case sensitivity enabled."""
    import sys
    p = Path("C:/data/case_dir/App.log")
    monkeypatch.setattr(Path, "resolve", lambda self: self)
    monkeypatch.setattr(Path, "exists", lambda self: "aPP.LOG" not in self.as_posix())
    monkeypatch.setattr(sys, "platform", "win32")
    monkeypatch.setattr(os, "name", "nt")

    assert is_path_case_sensitive(p) is True
    assert canonical_path_key(p) == "C:/data/case_dir/App.log"


def test_is_path_case_sensitive_mocked_linux_distinct_files(monkeypatch):
    """Verify Linux scenario where both App.log and aPP.LOG exist as distinct files."""
    import sys
    p = Path("/var/log/service/App.log")
    monkeypatch.setattr(Path, "resolve", lambda self: self)
    monkeypatch.setattr(Path, "exists", lambda self: True)
    monkeypatch.setattr(os.path, "samefile", lambda p1, p2: False)
    monkeypatch.setattr(sys, "platform", "linux")
    monkeypatch.setattr(os, "name", "posix")

    assert is_path_case_sensitive(p) is True
    assert canonical_path_key(p) == "/var/log/service/App.log"


def test_is_path_case_sensitive_real_filesystem(tmp_path):
    """Verify non-destructive case-sensitivity probe on physical files and numeric names."""
    f = tmp_path / "12345.log"
    f.write_text("numeric data", encoding="utf-8")

    # The probe should traverse up to tmp_path or ancestors to determine volume sensitivity
    probe_a = tmp_path / "probe_case"
    probe_a.write_text("a", encoding="utf-8")
    probe_b = tmp_path / "PROBE_CASE"
    expected_cs = not probe_b.exists()

    assert is_path_case_sensitive(f) == expected_cs
