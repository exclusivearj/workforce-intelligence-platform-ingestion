"""Integration test: upsert idempotency against a real Postgres.

Spins up Postgres via testcontainers, runs init.sql, and verifies that
re-upserting the same records does not create duplicates.

Run with: ``pytest tests/integration/ -m integration``
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

INIT_SQL = Path(__file__).resolve().parents[2] / "docker" / "init.sql"


@pytest.fixture(scope="module")
def pg_conn():
    psycopg2 = pytest.importorskip("psycopg2")
    testcontainers = pytest.importorskip("testcontainers.postgres")

    with testcontainers.PostgresContainer("pgvector/pgvector:pg16") as pg:
        conn = psycopg2.connect(pg.get_connection_url().replace("+psycopg2", ""))
        with conn.cursor() as cur:
            cur.execute(INIT_SQL.read_text())
        conn.commit()
        # Point the db helper env at this container.
        os.environ["POSTGRES_HOST"] = pg.get_container_host_ip()
        os.environ["POSTGRES_PORT"] = pg.get_exposed_port(5432)
        os.environ["POSTGRES_DB"] = pg.dbname
        os.environ["POSTGRES_USER"] = pg.username
        os.environ["POSTGRES_PASSWORD"] = pg.password
        yield conn
        conn.close()


def test_upsert_is_idempotent(pg_conn):
    from src.models.employee import EmployeeRaw
    from src.utils.db import upsert_employees
    from src.utils.synthetic_data import generate_employees

    records = [EmployeeRaw(**e) for e in generate_employees(10, seed=1)]
    batch = uuid.uuid4()

    upsert_employees(pg_conn, records, batch, source="workday")
    pg_conn.commit()
    upsert_employees(pg_conn, records, batch, source="workday")
    pg_conn.commit()

    with pg_conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM raw.employees WHERE source = 'workday'")
        count = cur.fetchone()[0]
    assert count == 10
