"""Unit tests for AirtableConnector (offline-first / optional behaviour)."""

from __future__ import annotations

from src.connectors.airtable import AirtableConnector


def test_not_configured_when_creds_missing(monkeypatch):
    monkeypatch.delenv("AIRTABLE_API_KEY", raising=False)
    monkeypatch.delenv("AIRTABLE_BASE_ID", raising=False)
    conn = AirtableConnector(api_key="", base_id="")
    assert conn.is_configured() is False


def test_fetch_returns_empty_when_unconfigured():
    conn = AirtableConnector(api_key="", base_id="")
    assert list(conn.fetch_employees()) == []
    assert list(conn.fetch_job_applications()) == []


def test_is_configured_true_with_creds():
    conn = AirtableConnector(api_key="key", base_id="base")
    assert conn.is_configured() is True


def test_source_name():
    assert AirtableConnector(api_key="k", base_id="b").source_name() == "airtable"
