"""Integration test: query analytics models through Trino.

Requires a running Trino (catalog `postgresql`) with dbt models already built.
Skipped automatically if Trino is unreachable.

Run with: ``pytest tests/integration/ -m integration``
"""

from __future__ import annotations

import os

import pytest

pytestmark = pytest.mark.integration


@pytest.fixture
def trino_cursor():
    trino = pytest.importorskip("trino")
    try:
        conn = trino.dbapi.connect(
            host=os.getenv("TRINO_HOST", "localhost"),
            port=int(os.getenv("TRINO_PORT", "8080")),
            user=os.getenv("TRINO_USER", "trino"),
            catalog="postgresql",
            schema="analytics",
        )
        cur = conn.cursor()
    except Exception as exc:  # pragma: no cover - environment dependent
        pytest.skip(f"Trino unavailable: {exc}")
    yield cur


def test_dim_employees_has_rows(trino_cursor):
    trino_cursor.execute("SELECT COUNT(*) FROM postgresql.analytics.dim_employees")
    assert trino_cursor.fetchone()[0] > 0
