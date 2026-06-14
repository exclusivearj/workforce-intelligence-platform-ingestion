"""Mock Greenhouse REST connector (applicant tracking system).

Greenhouse owns the candidate pipeline, so this connector implements
``fetch_job_applications`` only. Same pagination + retry pattern as Workday.
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

# Greenhouse application.status -> JobApplicationRaw.stage enum.
GREENHOUSE_STATUS_MAP = {
    "active": "applied",
    "submitted": "applied",
    "phone_screen": "phone_screen",
    "interviewing": "interview",
    "offer_extended": "offer",
    "hired": "hired",
    "rejected": "rejected",
}


class _RetryableStatus(Exception):
    """Raised on 429/5xx so tenacity retries with backoff."""


class GreenhouseConnector(BaseConnector):
    def __init__(
        self,
        base_url: str | None = None,
        api_key: str | None = None,
        client: httpx.Client | None = None,
    ) -> None:
        self.base_url = (base_url or os.getenv("GREENHOUSE_BASE_URL", "http://localhost:5002")).rstrip("/")
        self.api_key = api_key or os.getenv("GREENHOUSE_API_KEY", "")
        self._client = client or httpx.Client(timeout=30.0)

    @retry(
        retry=retry_if_exception_type(_RetryableStatus),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=0.5, max=10),
        reraise=True,
    )
    def _get(self, url: str, params: dict | None = None) -> dict:
        resp = self._client.get(url, params=params, auth=(self.api_key, ""))
        if resp.status_code == 429 or resp.status_code >= 500:
            raise _RetryableStatus(f"retryable status {resp.status_code}")
        resp.raise_for_status()
        return resp.json()

    def _paginate(self, path: str) -> Iterator[dict]:
        url = f"{self.base_url}{path}"
        params: dict | None = {"page": 1, "page_size": 100}
        while url:
            body = self._get(url, params=params)
            yield from body.get("data", [])
            next_link = (body.get("_links") or {}).get("next")
            url = f"{self.base_url}{next_link}" if next_link else ""
            params = None

    def _map_application(self, record: dict) -> JobApplicationRaw:
        return JobApplicationRaw(
            source_id=record["id"],
            candidate_id=record["candidate_id"],
            job_id=record["job_id"],
            job_title=record["job_title"],
            department=record["department"],
            stage=GREENHOUSE_STATUS_MAP.get(record.get("status", "active"), "applied"),
            applied_at=record["applied_at"],
            stage_changed_at=record.get("last_activity_at", record["applied_at"]),
            recruiter_id=record.get("recruiter_id"),
        )

    def fetch_employees(self) -> Iterator[EmployeeRaw]:
        raise NotImplementedError("Greenhouse connector does not provide employees.")

    def fetch_job_applications(self) -> Iterator[JobApplicationRaw]:
        for record in self._paginate("/api/v1/applications"):
            yield self._map_application(record)
