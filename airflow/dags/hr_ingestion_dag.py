"""hr_ingestion DAG — extract from sources, land in raw, transform with dbt.

Schedule: daily 06:00. On success, triggers the llm_eval embedding refresh.
Uses the Airflow TaskFlow API (@task) throughout.
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

PII_FIELDS = {"email", "first_name", "last_name", "salary", "performance_rating", "ssn"}
EMPLOYEE_BASELINE = {
    "source_id": "str",
    "first_name": "str",
    "last_name": "str",
    "email": "str",
    "department": "str",
    "job_title": "str",
    "hire_date": "str",
    "employment_type": "str",
    "level": "str",
    "location": "str",
}


@dag(
    dag_id="hr_ingestion",
    schedule="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["ingestion", "people-analytics"],
)
def hr_ingestion_dag():
    @task
    def extract_workday() -> dict:
        from src.connectors.workday import WorkdayConnector
        from src.utils.db import get_connection, upsert_employees

        conn = get_connection()
        try:
            batch_id = uuid.uuid4()
            records = list(WorkdayConnector().fetch_employees())
            count = upsert_employees(conn, records, batch_id, source="workday")
            conn.commit()
        finally:
            conn.close()
        return {"source": "workday", "rows": count}

    @task
    def extract_greenhouse() -> dict:
        from src.connectors.greenhouse import GreenhouseConnector
        from src.utils.db import get_connection, upsert_job_applications

        conn = get_connection()
        try:
            batch_id = uuid.uuid4()
            records = list(GreenhouseConnector().fetch_job_applications())
            count = upsert_job_applications(conn, records, batch_id, source="greenhouse")
            conn.commit()
        finally:
            conn.close()
        return {"source": "greenhouse", "rows": count}

    @task
    def extract_airtable() -> dict:
        import logging

        from src.connectors.airtable import AirtableConnector
        from src.utils.db import (
            get_connection,
            upsert_employees,
            upsert_job_applications,
        )

        connector = AirtableConnector()
        if not connector.is_configured():
            return {"source": "airtable", "rows": 0, "skipped": True}

        conn = get_connection()
        try:
            batch_id = uuid.uuid4()
            emp = upsert_employees(
                conn, list(connector.fetch_employees()), batch_id, source="airtable"
            )
            apps = upsert_job_applications(
                conn, list(connector.fetch_job_applications()), batch_id, source="airtable"
            )
            conn.commit()
        except Exception as exc:
            # Airtable is an optional source (offline-first design): a misconfigured
            # base or transient API error must not fail the core ingestion pipeline.
            conn.rollback()
            logging.getLogger(__name__).warning("Airtable extract skipped: %s", exc)
            return {"source": "airtable", "rows": 0, "skipped": True, "error": str(exc)}
        finally:
            conn.close()
        return {"source": "airtable", "rows": emp + apps}

    @task
    def detect_schema_drift(workday_stats: dict, airtable_stats: dict) -> list:
        from src.connectors.workday import WorkdayConnector
        from src.utils.db import get_connection
        from src.utils.schema_drift import detect_drift, write_drift_events

        sample = [
            e.model_dump(mode="json")
            for _, e in zip(range(50), WorkdayConnector().fetch_employees())
        ]
        events = detect_drift("workday", sample, EMPLOYEE_BASELINE, PII_FIELDS)
        conn = get_connection()
        try:
            write_drift_events(conn, "workday", events)
        finally:
            conn.close()
        return [e.__dict__ for e in events]

    @task
    def run_dbt_models() -> None:
        from dbt.cli.main import dbtRunner

        # Default to the in-repo layout (dags/../../dbt); override with
        # INGESTION_DBT_DIR when the project is mounted elsewhere (e.g. in the
        # Airflow containers, where 1-ingestion lives at a fixed path).
        default_dir = os.path.join(os.path.dirname(__file__), "..", "..", "dbt")
        project_dir = os.getenv("INGESTION_DBT_DIR", default_dir)
        runner = dbtRunner()
        for cmd in (["deps"], ["build"]):
            result = runner.invoke(
                [*cmd, "--project-dir", project_dir, "--profiles-dir", project_dir]
            )
            if not result.success:
                raise RuntimeError(f"dbt {cmd[0]} failed: {result.exception}")

    @task
    def alert_on_pii_change(drift_events: list) -> None:
        pii_changes = [e for e in drift_events if e.get("is_pii")]
        if not pii_changes:
            return
        from src.utils.alerts import send_slack_alert

        fields = ", ".join(e["field_name"] for e in pii_changes)
        send_slack_alert(f"PII schema drift detected in fields: {fields}")

    workday = extract_workday()
    greenhouse = extract_greenhouse()
    airtable = extract_airtable()
    drift = detect_schema_drift(workday, airtable)
    dbt_run = run_dbt_models()
    alert = alert_on_pii_change(drift)

    trigger = TriggerDagRunOperator(
        task_id="trigger_llm_eval",
        trigger_dag_id="llm_eval_embedding_refresh",
        wait_for_completion=False,
        reset_dag_run=True,
    )

    [workday, greenhouse, airtable] >> drift >> dbt_run >> alert >> trigger


hr_ingestion_dag()
