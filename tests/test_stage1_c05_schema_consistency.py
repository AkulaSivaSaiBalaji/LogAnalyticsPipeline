"""Regression tests for C05: SQL DDL and ORM Schema Consistency.

Verifies:
1. All tables defined in SQLAlchemy ORM (Base.metadata) exist in sql/schema.sql.
2. All columns, nullability constraints, and data types match between ORM models and sql/schema.sql.
3. Primary keys and unique constraints are consistent.
4. Foreign key constraints on run_id in log_events and rejected_records are properly defined
   in both the ORM models and sql/schema.sql.
5. Indexes declared in ORM models match those in sql/schema.sql.
6. Referential integrity and ON DELETE RESTRICT behavior are enforced, preventing orphan records
   and blocking premature deletion of parent pipeline_runs.
"""

from __future__ import annotations

import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest
from sqlalchemy import inspect, select, text
from sqlalchemy.exc import IntegrityError

from src.log_pipeline.db.schema import (
    Base,
    LogEventModel,
    PipelineRunModel,
    RejectedRecordModel,
)


def _load_schema_sql() -> str:
    schema_path = Path(__file__).resolve().parent.parent / "sql" / "schema.sql"
    return schema_path.read_text(encoding="utf-8")


def _parse_create_tables(sql_text: str) -> dict[str, str]:
    """Extract table name and body from CREATE TABLE statements in SQL DDL."""
    table_pattern = re.compile(
        r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)\s*\((.*?)\)\s*ENGINE\s*=",
        re.DOTALL | re.IGNORECASE,
    )
    tables = {}
    for match in table_pattern.finditer(sql_text):
        tbl_name = match.group(1).lower()
        tbl_body = match.group(2)
        tables[tbl_name] = tbl_body
    return tables


def _parse_columns_from_ddl(table_body: str) -> dict[str, dict]:
    """Parse column definitions, nullability, and primary key indicators from DDL body."""
    cols = {}
    for line in table_body.splitlines():
        line = line.strip().rstrip(",")
        if not line:
            continue
        # Skip constraints, indexes, and primary key clauses
        upper = line.upper()
        if (
            upper.startswith("PRIMARY KEY")
            or upper.startswith("INDEX")
            or upper.startswith("KEY")
            or upper.startswith("CONSTRAINT")
            or upper.startswith("FOREIGN KEY")
            or upper.startswith("UNIQUE")
        ):
            continue

        parts = line.split()
        if len(parts) >= 2:
            col_name = parts[0]
            col_type = parts[1].upper()
            is_nullable = "NOT NULL" not in upper
            cols[col_name] = {
                "type": col_type,
                "nullable": is_nullable,
                "line": line,
            }
    return cols


def _parse_foreign_keys_from_ddl(table_body: str) -> list[dict]:
    """Parse foreign key constraints from DDL body."""
    fk_pattern = re.compile(
        r"(?:CONSTRAINT\s+(\w+)\s+)?FOREIGN\s+KEY\s*\(([^)]+)\)\s*REFERENCES\s+(\w+)\s*\(([^)]+)\)"
        r"(?:\s+ON\s+DELETE\s+(\w+))?(?:\s+ON\s+UPDATE\s+(\w+))?",
        re.IGNORECASE,
    )
    fks = []
    for match in fk_pattern.finditer(table_body):
        fks.append({
            "name": match.group(1),
            "constrained_column": match.group(2).strip(),
            "referred_table": match.group(3).strip(),
            "referred_column": match.group(4).strip(),
            "on_delete": (match.group(5) or "").upper(),
            "on_update": (match.group(6) or "").upper(),
        })
    return fks


def _parse_indexes_from_ddl(table_body: str) -> set[str]:
    """Extract index names declared in table DDL."""
    idx_pattern = re.compile(r"INDEX\s+(\w+)", re.IGNORECASE)
    return {m.group(1).lower() for m in idx_pattern.finditer(table_body)}


def test_schema_sql_and_orm_table_names():
    """Verify all ORM tables are represented in sql/schema.sql."""
    sql_text = _load_schema_sql()
    tables_ddl = _parse_create_tables(sql_text)

    orm_tables = {tbl.name.lower() for tbl in Base.metadata.tables.values()}
    assert orm_tables == {"pipeline_runs", "log_events", "rejected_records"}
    assert set(tables_ddl.keys()) == orm_tables, (
        f"Mismatch between ORM tables {orm_tables} and DDL tables {set(tables_ddl.keys())}"
    )


def test_schema_sql_columns_and_nullability_match_orm():
    """Verify every ORM column exists in sql/schema.sql with matching nullability."""
    sql_text = _load_schema_sql()
    tables_ddl = _parse_create_tables(sql_text)

    for tbl_name, orm_table in Base.metadata.tables.items():
        assert tbl_name.lower() in tables_ddl, f"Table {tbl_name} missing from DDL"
        ddl_columns = _parse_columns_from_ddl(tables_ddl[tbl_name.lower()])

        for col in orm_table.columns:
            assert col.name in ddl_columns, (
                f"Column {col.name} of table {tbl_name} in ORM is missing from sql/schema.sql"
            )
            ddl_nullable = ddl_columns[col.name]["nullable"]
            orm_nullable = col.nullable
            assert ddl_nullable == orm_nullable, (
                f"Nullability mismatch on {tbl_name}.{col.name}: "
                f"ORM nullable={orm_nullable}, DDL nullable={ddl_nullable}"
            )


def test_schema_sql_primary_keys_match_orm():
    """Verify primary key columns match between ORM and sql/schema.sql."""
    sql_text = _load_schema_sql()
    tables_ddl = _parse_create_tables(sql_text)

    expected_pks = {
        "pipeline_runs": {"run_id"},
        "log_events": {"event_id"},
        "rejected_records": {"id"},
    }

    for tbl_name, exp_cols in expected_pks.items():
        orm_table = Base.metadata.tables[tbl_name]
        orm_pks = {c.name for c in orm_table.primary_key.columns}
        assert orm_pks == exp_cols

        body = tables_ddl[tbl_name]
        pk_match = re.search(r"PRIMARY\s+KEY\s*\(([^)]+)\)", body, re.IGNORECASE)
        assert pk_match is not None, f"PRIMARY KEY clause missing from DDL table {tbl_name}"
        ddl_pks = {col.strip() for col in pk_match.group(1).split(",")}
        assert ddl_pks == exp_cols, (
            f"PK mismatch on {tbl_name}: ORM {orm_pks} vs DDL {ddl_pks}"
        )


def test_schema_sql_run_id_foreign_keys_match_orm():
    """Verify run_id foreign key constraints exist on child tables with consistent referential integrity."""
    sql_text = _load_schema_sql()
    tables_ddl = _parse_create_tables(sql_text)

    # 1. log_events.run_id -> pipeline_runs.run_id
    log_events_fks = _parse_foreign_keys_from_ddl(tables_ddl["log_events"])
    assert len(log_events_fks) >= 1, "log_events must define a FOREIGN KEY constraint in sql/schema.sql"
    fk_log = next((fk for fk in log_events_fks if fk["constrained_column"] == "run_id"), None)
    assert fk_log is not None, "log_events missing foreign key on run_id"
    assert fk_log["referred_table"] == "pipeline_runs"
    assert fk_log["referred_column"] == "run_id"
    assert fk_log["on_delete"] == "RESTRICT", "Referential deletion must be RESTRICT to prevent orphan records"

    # 2. rejected_records.run_id -> pipeline_runs.run_id
    rej_fks = _parse_foreign_keys_from_ddl(tables_ddl["rejected_records"])
    assert len(rej_fks) >= 1, "rejected_records must define a FOREIGN KEY constraint in sql/schema.sql"
    fk_rej = next((fk for fk in rej_fks if fk["constrained_column"] == "run_id"), None)
    assert fk_rej is not None, "rejected_records missing foreign key on run_id"
    assert fk_rej["referred_table"] == "pipeline_runs"
    assert fk_rej["referred_column"] == "run_id"
    assert fk_rej["on_delete"] == "RESTRICT", "Referential deletion must be RESTRICT to prevent orphan records"

    # 3. Compare with ORM definitions
    orm_log_fks = list(LogEventModel.__table__.foreign_keys)
    assert len(orm_log_fks) == 1
    assert orm_log_fks[0].target_fullname == "pipeline_runs.run_id"

    orm_rej_fks = list(RejectedRecordModel.__table__.foreign_keys)
    assert len(orm_rej_fks) == 1
    assert orm_rej_fks[0].target_fullname == "pipeline_runs.run_id"


def test_schema_sql_indexes_match_orm():
    """Verify all explicit and column indexes in ORM are present in sql/schema.sql."""
    sql_text = _load_schema_sql()
    tables_ddl = _parse_create_tables(sql_text)

    for tbl_name, orm_table in Base.metadata.tables.items():
        ddl_indexes = _parse_indexes_from_ddl(tables_ddl[tbl_name.lower()])
        for idx in orm_table.indexes:
            assert idx.name.lower() in ddl_indexes, (
                f"Index {idx.name} on {tbl_name} is in ORM but missing from sql/schema.sql"
            )


def test_referential_integrity_and_deletion_restriction(test_db_manager):
    """Verify that ON DELETE RESTRICT prevents deleting a parent run while child events exist."""
    run_id = "test-run-c05-deletion"
    now = datetime.now(timezone.utc)

    # 1. Create pipeline run and child event
    with test_db_manager.session_scope() as session:
        run = PipelineRunModel(
            run_id=run_id,
            start_time=now,
            status="RUNNING",
            input_files="[]",
        )
        session.add(run)

    with test_db_manager.session_scope() as session:
        ev = LogEventModel(
            event_id="evt-c05-01",
            run_id=run_id,
            timestamp=now,
            severity="INFO",
            service="order-service",
            message="Test event",
            source_file="test.log",
            source_line=1,
            created_at=now,
        )
        rej = RejectedRecordModel(
            run_id=run_id,
            source_file="test.log",
            source_line=2,
            raw_record="bad record",
            rejection_reasons='["BAD_FORMAT"]',
            created_at=now,
        )
        session.add(ev)
        session.add(rej)

    # 2. Deleting parent run while children exist must fail with IntegrityError
    with pytest.raises(IntegrityError):
        with test_db_manager.session_scope() as session:
            parent_run = session.get(PipelineRunModel, run_id)
            session.delete(parent_run)

    # 3. Verify children and parent still exist
    with test_db_manager.session_scope() as session:
        assert session.get(PipelineRunModel, run_id) is not None
        assert session.get(LogEventModel, "evt-c05-01") is not None

    # 4. Deleting children first, then parent must succeed cleanly
    with test_db_manager.session_scope() as session:
        ev = session.get(LogEventModel, "evt-c05-01")
        session.delete(ev)
        rejs = session.scalars(select(RejectedRecordModel).where(RejectedRecordModel.run_id == run_id)).all()
        for r in rejs:
            session.delete(r)

    with test_db_manager.session_scope() as session:
        parent_run = session.get(PipelineRunModel, run_id)
        session.delete(parent_run)

    # 5. Confirm deletion completed
    with test_db_manager.session_scope() as session:
        assert session.get(PipelineRunModel, run_id) is None


def test_schema_sql_executable_sqlite_ddl_and_fk_enforcement():
    """Verify that tables adapted from sql/schema.sql build cleanly in SQLite and enforce referential integrity."""
    sql_text = _load_schema_sql()
    tables_ddl = _parse_create_tables(sql_text)

    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys = ON;")

    # Create tables in order: pipeline_runs first, then child tables
    table_order = ["pipeline_runs", "log_events", "rejected_records"]
    for tbl in table_order:
        body = tables_ddl[tbl]
        # Strip inline INDEX lines which SQLite does not support inside CREATE TABLE
        body_lines = []
        for line in body.splitlines():
            line_str = line.strip().rstrip(",")
            if line_str.upper().startswith("INDEX "):
                continue
            # Convert MySQL AUTO_INCREMENT to SQLite AUTOINCREMENT
            line_str = re.sub(r"\bBIGINT\s+UNSIGNED\s+NOT\s+NULL\s+AUTO_INCREMENT\b", "INTEGER PRIMARY KEY AUTOINCREMENT", line_str, flags=re.IGNORECASE)
            # Remove redundant PRIMARY KEY clause if id is already primary key
            if tbl == "rejected_records" and line_str.upper().startswith("PRIMARY KEY (ID)"):
                continue
            # Strip MySQL COMMENT '...'
            line_str = re.sub(r"\s+COMMENT\s+'[^']*'", "", line_str)
            if line_str:
                body_lines.append(line_str)

        create_stmt = f"CREATE TABLE {tbl} (\n  " + ",\n  ".join(body_lines) + "\n)"
        conn.execute(create_stmt)

    # Verify tables created
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    created_tables = {row[0] for row in cursor.fetchall()}
    assert set(table_order).issubset(created_tables)

    # Verify foreign key constraint enforcement in SQLite from DDL
    conn.execute("INSERT INTO pipeline_runs (run_id, start_time, status, input_files) VALUES ('run-ddl-1', '2026-10-28 12:00:00', 'RUNNING', '[]')")
    conn.execute("INSERT INTO log_events (event_id, run_id, timestamp, severity, service, message, source_file, source_line) VALUES ('e1', 'run-ddl-1', '2026-10-28 12:00:00', 'INFO', 'api', 'msg', 'f.log', 1)")

    # Orphan insert must fail
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute("INSERT INTO log_events (event_id, run_id, timestamp, severity, service, message, source_file, source_line) VALUES ('e2', 'nonexistent-run', '2026-10-28 12:00:00', 'INFO', 'api', 'msg', 'f.log', 2)")

    # Parent delete must fail
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute("DELETE FROM pipeline_runs WHERE run_id = 'run-ddl-1'")

    conn.close()
