"""Unit tests for GreenhouseConnector."""

from __future__ import annotations

import httpx
import pytest

from src.connectors.greenhouse import GreenhouseConnector
from src.models.employee import JobApplicationRaw


def _client(handler) -> httpx.Client:
    return httpx.Client(transport=httpx.MockTransport(handler), base_url="http://mock")


def _gh_record(idx: int, status: str = "interviewing") -> dict:
    return {
        "id": f"app-{idx}",
        "candidate_id": f"cand-{idx}",
        "job_id": "JOB-1001",
        "job_title": "Data Engineer",
        "department": "Data",
        "status": status,
        "applied_at": "2024-01-01T12:00:00",
        "last_activity_at": "2024-02-01T12:00:00",
        "recruiter_id": "rec-1",
    }


def test_status_maps_to_stage_enum():
    page = {"data": [_gh_record(1, "interviewing")], "_links": {}}
    conn = GreenhouseConnector(
        base_url="http://mock", client=_client(lambda r: httpx.Response(200, json=page))
    )
    apps = list(conn.fetch_job_applications())
    assert len(apps) == 1
    assert isinstance(apps[0], JobApplicationRaw)
    assert apps[0].stage == "interview"


def test_unknown_status_defaults_to_applied():
    page = {"data": [_gh_record(1, "totally_new_status")], "_links": {}}
    conn = GreenhouseConnector(
        base_url="http://mock", client=_client(lambda r: httpx.Response(200, json=page))
    )
    assert list(conn.fetch_job_applications())[0].stage == "applied"


def test_fetch_employees_not_implemented():
    conn = GreenhouseConnector(base_url="http://mock")
    with pytest.raises(NotImplementedError):
        list(conn.fetch_employees())
