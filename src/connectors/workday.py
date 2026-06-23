"""Mock Workday REST connector.

Talks to the local Flask mock server (docker/mock_workday_server.py) by default.
To target a real Workday tenant, point WORKDAY_BASE_URL at the real API — the
connector logic (pagination, retry, field mapping) is unchanged.
"""

from __future__ import annotations

import os
from collections.abc import Iterator

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from src.connectors.base import BaseConnector
from src.models.employee import EmployeeRaw, JobApplicationRaw

# Workday API field names -> EmployeeRaw field names.
WORKDAY_FIELD_MAP = {
    "Worker_ID": "source_id",
    "Legal_First_Name": "first_name",
    "Legal_Last_Name": "last_name",
    "Work_Email": "email",
    "Cost_Center": "department",
    "Job_Title": "job_title",
    "Hire_Date": "hire_date",
    "Termination_Date": "termination_date",
    "Worker_Type": "employment_type",
    "Management_Level": "level",
    "Location": "location",
    "Manager_ID": "manager_id",
    "Base_Pay": "salary",
    "Performance_Rating": "performance_rating",
}


class _RetryableStatus(Exception):
    """Raised on 429/5xx so tenacity retries with backoff."""


class WorkdayConnector(BaseConnector):
    def __init__(
        self,
        base_url: str | None = None,
        api_token: str | None = None,
        client: httpx.Client | None = None,
    ) -> None:
        self.base_url = (base_url or os.getenv("WORKDAY_BASE_URL", "http://localhost:5001")).rstrip("/")
        self.api_token = api_token or os.getenv("WORKDAY_API_TOKEN", "")
        self._client = client or httpx.Client(timeout=30.0)

    @retry(
        retry=retry_if_exception_type(_RetryableStatus),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=0.5, max=10),
        reraise=True,
    )
    def _get(self, url: str, params: dict | None = None) -> dict:
        # Only send the bearer header when a token is configured: an empty token
        # would produce the malformed value "Bearer " which httpx rejects.
        headers = {"Authorization": f"Bearer {self.api_token}"} if self.api_token else {}
        resp = self._client.get(url, params=params, headers=headers)
        if resp.status_code == 429 or resp.status_code >= 500:
            raise _RetryableStatus(f"retryable status {resp.status_code}")
        resp.raise_for_status()
        return resp.json()

    def _paginate(self, path: str) -> Iterator[dict]:
        """Follow ``_links.next`` until it is absent, yielding each record."""
        url = f"{self.base_url}{path}"
        params: dict | None = {"page": 1, "page_size": 100}
        while url:
            body = self._get(url, params=params)
            yield from body.get("data", [])
            next_link = (body.get("_links") or {}).get("next")
            url = f"{self.base_url}{next_link}" if next_link else ""
            params = None  # next link already encodes paging state

    def _map_employee(self, record: dict) -> EmployeeRaw:
        mapped: dict = {}
        for src_key, dst_key in WORKDAY_FIELD_MAP.items():
            if src_key in record:
                mapped[dst_key] = record[src_key]
        return EmployeeRaw(**mapped)

    def fetch_employees(self) -> Iterator[EmployeeRaw]:
        for record in self._paginate("/api/v1/workers"):
            yield self._map_employee(record)

    def fetch_job_applications(self) -> Iterator[JobApplicationRaw]:
        # Workday is the system of record for workers, not candidates.
        raise NotImplementedError("Workday connector does not provide job applications.")
