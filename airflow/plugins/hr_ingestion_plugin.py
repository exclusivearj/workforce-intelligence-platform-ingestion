"""Airflow plugin placeholder for the hr_ingestion DAG group.

Kept intentionally minimal: the DAG uses the TaskFlow API and needs no custom
operators yet. This module exists so the plugins directory is importable and to
provide a home for shared macros/links as the pipeline grows.
"""

from __future__ import annotations

from airflow.plugins_manager import AirflowPlugin


class HRIngestionPlugin(AirflowPlugin):
    name = "hr_ingestion_plugin"
    macros = []
    operators = []
