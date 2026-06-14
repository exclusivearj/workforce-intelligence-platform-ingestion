"""Unit tests for WorkdayConnector: mapping, pagination, retry."""

from __future__ import annotations

import httpx
import pytest

from src.connectors.workday import WorkdayConnector
from src.models.employee import EmployeeRaw


def _make_client(handler) -> httpx.Client:
    transport = httpx.MockTransport(handler)
    return httpx.Client(transport=transport, base_url="http://mock")


def test_fetch_employees_maps_fields(workday_records):
    page = {"data": workday_records, "_links": {}}

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=page)

    conn = WorkdayConnector(base_url="http://mock", client=_make_client(handler))
    employees = list(conn.fetch_employees())

    assert len(employees) == len(workday_records)
    assert all(isinstance(e, EmployeeRaw) for e in employees)
    assert employees[0].source_id == workday_records[0]["Worker_ID"]
    assert employees[0].email == workday_records[0]["Work_Email"]


def test_pagination_follows_next_link(workday_records):
    half = len(workday_records) // 2
    pages = {
        "1": {"data": workday_records[:half], "_links": {"next": "/api/v1/workers?page=2"}},
        "2": {"data": workday_records[half:], "_links": {}},
    }

    def handler(request: httpx.Request) -> httpx.Response:
        page = request.url.params.get("page", "1")
        return httpx.Response(200, json=pages[page])

    conn = WorkdayConnector(base_url="http://mock", client=_make_client(handler))
    employees = list(conn.fetch_employees())
    assert len(employees) == len(workday_records)


def test_retry_on_429(workday_records):
    calls = {"n": 0}
    page = {"data": workday_records[:1], "_links": {}}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(429, json={})
        return httpx.Response(200, json=page)

    conn = WorkdayConnector(base_url="http://mock", client=_make_client(handler))
    employees = list(conn.fetch_employees())
    assert calls["n"] == 2  # one 429, one success
    assert len(employees) == 1


def test_fetch_job_applications_not_implemented():
    conn = WorkdayConnector(base_url="http://mock")
    with pytest.raises(NotImplementedError):
        list(conn.fetch_job_applications())


def test_source_name():
    assert WorkdayConnector(base_url="http://mock").source_name() == "workday"
