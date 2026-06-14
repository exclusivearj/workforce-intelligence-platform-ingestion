"""Unit tests for the schema drift detector."""

from __future__ import annotations

from src.utils.schema_drift import detect_drift

BASELINE = {"source_id": "str", "email": "str", "salary": "float"}
PII = {"email", "ssn"}


def test_no_drift_on_identical_schema():
    records = [{"source_id": "a", "email": "x@y.com", "salary": 1.0}]
    assert detect_drift("workday", records, BASELINE, PII) == []


def test_added_field_detected():
    records = [{"source_id": "a", "email": "x@y.com", "salary": 1.0, "team": "data"}]
    events = detect_drift("workday", records, BASELINE, PII)
    assert len(events) == 1
    assert events[0].field_name == "team"
    assert events[0].change_type == "added"
    assert events[0].is_pii is False


def test_removed_field_detected():
    records = [{"source_id": "a", "email": "x@y.com"}]
    events = detect_drift("workday", records, BASELINE, PII)
    removed = [e for e in events if e.change_type == "removed"]
    assert len(removed) == 1
    assert removed[0].field_name == "salary"


def test_type_change_detected():
    records = [{"source_id": "a", "email": "x@y.com", "salary": "100k"}]
    events = detect_drift("workday", records, BASELINE, PII)
    changed = [e for e in events if e.change_type == "type_changed"]
    assert len(changed) == 1
    assert changed[0].old_type == "float"
    assert changed[0].new_type == "str"


def test_pii_field_drift_flagged():
    records = [{"source_id": "a", "email": "x@y.com", "salary": 1.0, "ssn": "123"}]
    events = detect_drift("workday", records, BASELINE, PII)
    ssn_events = [e for e in events if e.field_name == "ssn"]
    assert ssn_events and ssn_events[0].is_pii is True
