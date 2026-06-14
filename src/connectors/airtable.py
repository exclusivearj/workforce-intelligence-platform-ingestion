"""Real Airtable REST connector (optional).

This is the one connector that talks to a real external service. It is fully
optional: if AIRTABLE_API_KEY / AIRTABLE_BASE_ID are unset, ``is_configured()``
returns False and the DAG skips it gracefully (offline-first).
"""

from __future__ import annotations

import os
import time
from collections.abc import Iterator

from src.connectors.base import BaseConnector
from src.models.employee import EmployeeRaw, JobApplicationRaw

_RATE_LIMIT_SLEEP = 0.2  # Airtable allows ~5 req/sec.


class AirtableConnector(BaseConnector):
    def __init__(
        self,
        api_key: str | None = None,
        base_id: str | None = None,
        employees_table: str = "Employees",
        applications_table: str = "Applications",
    ) -> None:
        self.api_key = api_key or os.getenv("AIRTABLE_API_KEY", "")
        self.base_id = base_id or os.getenv("AIRTABLE_BASE_ID", "")
        self.employees_table = employees_table
        self.applications_table = applications_table

    def is_configured(self) -> bool:
        return bool(self.api_key and self.base_id)

    def _api(self):
        from pyairtable import Api

        return Api(self.api_key)

    def _iter_records(self, table_name: str) -> Iterator[dict]:
        api = self._api()
        table = api.table(self.base_id, table_name)
        for page in table.iterate():
            for row in page:
                yield row.get("fields", {})
            time.sleep(_RATE_LIMIT_SLEEP)

    def fetch_employees(self) -> Iterator[EmployeeRaw]:
        if not self.is_configured():
            return
        for fields in self._iter_records(self.employees_table):
            fields.setdefault("employment_type", "full_time")
            yield EmployeeRaw(**fields)

    def fetch_job_applications(self) -> Iterator[JobApplicationRaw]:
        if not self.is_configured():
            return
        for fields in self._iter_records(self.applications_table):
            yield JobApplicationRaw(**fields)
