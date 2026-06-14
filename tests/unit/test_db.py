"""Unit tests for db helpers using a fake psycopg2 (no real database)."""

from __future__ import annotations

import sys
import types
import uuid

import pytest

from src.models.employee import EmployeeRaw, JobApplicationRaw
from src.utils import db


class _FakeCursor:
    def __init__(self):
        self.executed: list[str] = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self.executed.append(sql)


class _FakeConn:
    def __init__(self):
        self.cursor_obj = _FakeCursor()
        self.committed = False

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.committed = True


@pytest.fixture
def fake_psycopg2(monkeypatch):
    """Inject a minimal fake psycopg2.extras.execute_values into sys.modules."""
    captured = {"rows": None}

    def execute_values(cur, sql, rows, template=None, page_size=100):
        captured["rows"] = list(rows)
        cur.execute(sql)

    extras = types.ModuleType("psycopg2.extras")
    extras.execute_values = execute_values
    pkg = types.ModuleType("psycopg2")
    pkg.extras = extras
    monkeypatch.setitem(sys.modules, "psycopg2", pkg)
    monkeypatch.setitem(sys.modules, "psycopg2.extras", extras)
    return captured


def _employee() -> EmployeeRaw:
    return EmployeeRaw(
        source_id="e1",
        first_name="Ada",
        last_name="Lovelace",
        email="ada@example.com",
        department="Engineering",
        job_title="Engineer",
        hire_date="2020-01-01",
        employment_type="full_time",
        level="IC4",
        location="Remote - US",
    )


def test_upsert_employees_returns_count(fake_psycopg2):
    conn = _FakeConn()
    n = db.upsert_employees(conn, [_employee()], uuid.uuid4())
    assert n == 1
    assert len(fake_psycopg2["rows"]) == 1


def test_upsert_employees_empty_short_circuits(fake_psycopg2):
    conn = _FakeConn()
    assert db.upsert_employees(conn, [], uuid.uuid4()) == 0
    assert fake_psycopg2["rows"] is None


def test_upsert_job_applications_returns_count(fake_psycopg2):
    conn = _FakeConn()
    app = JobApplicationRaw(
        source_id="a1",
        candidate_id="c1",
        job_id="JOB-1",
        job_title="Engineer",
        department="Data",
        stage="applied",
        applied_at="2024-01-01T00:00:00",
        stage_changed_at="2024-01-02T00:00:00",
    )
    assert db.upsert_job_applications(conn, [app], uuid.uuid4()) == 1


def test_bootstrap_roles_requires_passwords(monkeypatch):
    for key in (
        "INGESTION_WRITER_PASSWORD",
        "DBT_TRANSFORMER_PASSWORD",
        "ANALYST_READER_PASSWORD",
    ):
        monkeypatch.delenv(key, raising=False)
    with pytest.raises(RuntimeError, match="Missing required env var"):
        db.bootstrap_roles(_FakeConn())


def test_bootstrap_roles_creates_all(monkeypatch):
    monkeypatch.setenv("INGESTION_WRITER_PASSWORD", "p1")
    monkeypatch.setenv("DBT_TRANSFORMER_PASSWORD", "p2")
    monkeypatch.setenv("ANALYST_READER_PASSWORD", "p3")
    conn = _FakeConn()
    created = db.bootstrap_roles(conn)
    assert set(created) == {"ingestion_writer", "dbt_transformer", "analyst_reader"}
    assert conn.committed is True
