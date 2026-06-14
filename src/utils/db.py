"""Postgres connection helpers, role bootstrapping, and bulk upserts.

Role creation lives here (not in init.sql) so that per-role passwords can be read
from the environment at runtime. Neither the Postgres docker entrypoint nor
``psql -f`` expands ``${VAR}`` placeholders inside .sql files, so doing it in
Python is the only portable approach that works in both local Docker and CI.
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterable
from typing import TYPE_CHECKING
from uuid import UUID

if TYPE_CHECKING:  # pragma: no cover - typing only
    from src.models.employee import EmployeeRaw, JobApplicationRaw


def get_connection():
    """Return a psycopg2 connection built from environment variables."""
    import psycopg2

    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "workforce"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "changeme"),
    )


# Roles and the env var that supplies each role's password.
_ROLE_PASSWORD_ENV = {
    "ingestion_writer": "INGESTION_WRITER_PASSWORD",
    "dbt_transformer": "DBT_TRANSFORMER_PASSWORD",
    "analyst_reader": "ANALYST_READER_PASSWORD",
}


def bootstrap_roles(conn) -> list[str]:
    """Create the platform roles + grants idempotently.

    Passwords are read from the environment; a missing password raises loudly
    rather than silently creating a passwordless login role.
    Returns the list of roles created or updated.
    """
    created: list[str] = []
    with conn.cursor() as cur:
        for role, env_key in _ROLE_PASSWORD_ENV.items():
            password = os.getenv(env_key)
            if not password:
                raise RuntimeError(
                    f"Missing required env var {env_key} for role '{role}'."
                )
            # CREATE ROLE is not idempotent; emulate IF NOT EXISTS via DO block.
            cur.execute(
                """
                DO $do$
                BEGIN
                    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = %(role)s) THEN
                        EXECUTE format('CREATE ROLE %%I LOGIN PASSWORD %%L', %(role)s, %(pw)s);
                    ELSE
                        EXECUTE format('ALTER ROLE %%I LOGIN PASSWORD %%L', %(role)s, %(pw)s);
                    END IF;
                END
                $do$;
                """,
                {"role": role, "pw": password},
            )
            created.append(role)

        cur.execute(
            """
            GRANT USAGE ON SCHEMA raw TO ingestion_writer;
            GRANT INSERT, UPDATE, SELECT ON ALL TABLES IN SCHEMA raw TO ingestion_writer;
            ALTER DEFAULT PRIVILEGES IN SCHEMA raw
                GRANT INSERT, UPDATE, SELECT ON TABLES TO ingestion_writer;

            GRANT USAGE ON SCHEMA raw, staging, analytics TO dbt_transformer;
            GRANT SELECT ON ALL TABLES IN SCHEMA raw TO dbt_transformer;
            GRANT CREATE ON SCHEMA staging, analytics TO dbt_transformer;

            GRANT USAGE ON SCHEMA analytics, dashboard TO analyst_reader;
            GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO analyst_reader;
            GRANT SELECT ON ALL TABLES IN SCHEMA dashboard TO analyst_reader;
            ALTER DEFAULT PRIVILEGES IN SCHEMA analytics
                GRANT SELECT ON TABLES TO analyst_reader;
            """
        )
    conn.commit()
    return created


def _employee_rows(records: Iterable[EmployeeRaw]):
    for rec in records:
        yield (rec.source_id, json.dumps(rec.model_dump(mode="json")))


def upsert_employees(
    conn, records: list[EmployeeRaw], batch_id: UUID, source: str = "workday"
) -> int:
    """Upsert employees into raw.employees keyed on (source, source_id).

    Idempotent: re-running with the same records updates payloads in place rather
    than inserting duplicates.
    """
    from psycopg2.extras import execute_values

    rows = [
        (source, source_id, payload, str(batch_id))
        for source_id, payload in _employee_rows(records)
    ]
    if not rows:
        return 0
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO raw.employees (source, source_id, payload, batch_id)
            VALUES %s
            ON CONFLICT (source, source_id) DO UPDATE
                SET payload = EXCLUDED.payload,
                    batch_id = EXCLUDED.batch_id,
                    ingested_at = NOW()
            """,
            rows,
            template="(%s, %s, %s::jsonb, %s::uuid)",
        )
    return len(rows)


def upsert_job_applications(
    conn, records: list[JobApplicationRaw], batch_id: UUID, source: str = "greenhouse"
) -> int:
    """Upsert job applications into raw.job_applications keyed on (source, source_id)."""
    from psycopg2.extras import execute_values

    rows = [
        (source, rec.source_id, json.dumps(rec.model_dump(mode="json")), str(batch_id))
        for rec in records
    ]
    if not rows:
        return 0
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO raw.job_applications (source, source_id, payload, batch_id)
            VALUES %s
            ON CONFLICT (source, source_id) DO UPDATE
                SET payload = EXCLUDED.payload,
                    batch_id = EXCLUDED.batch_id,
                    ingested_at = NOW()
            """,
            rows,
            template="(%s, %s, %s::jsonb, %s::uuid)",
        )
    return len(rows)
