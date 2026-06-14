"""Detect schema drift between incoming source records and a known baseline.

Drift here means: a field appeared, a field disappeared, or a field's inferred
Python type changed. Any drifted field that is also classified as PII is flagged
so the DAG can raise a louder alert.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class DriftEvent:
    field_name: str
    change_type: str  # 'added' | 'removed' | 'type_changed'
    old_type: str | None
    new_type: str | None
    is_pii: bool


def _python_type_name(value: object) -> str:
    return type(value).__name__


def _infer_schema(records: list[dict]) -> dict[str, str]:
    """Infer field -> type name using the first non-null value seen per field."""
    inferred: dict[str, str] = {}
    for record in records:
        for field, value in record.items():
            if value is None:
                inferred.setdefault(field, "NoneType")
                continue
            if field not in inferred or inferred[field] == "NoneType":
                inferred[field] = _python_type_name(value)
    return inferred


def detect_drift(
    source: str,
    new_records: list[dict],
    baseline_schema: dict[str, str],
    pii_fields: set[str],
) -> list[DriftEvent]:
    """Compare ``new_records`` against ``baseline_schema`` and return drift events."""
    observed = _infer_schema(new_records)
    events: list[DriftEvent] = []

    for field, new_type in observed.items():
        if field not in baseline_schema:
            events.append(
                DriftEvent(field, "added", None, new_type, field in pii_fields)
            )
        elif baseline_schema[field] != new_type and new_type != "NoneType":
            events.append(
                DriftEvent(
                    field,
                    "type_changed",
                    baseline_schema[field],
                    new_type,
                    field in pii_fields,
                )
            )

    for field, old_type in baseline_schema.items():
        if field not in observed:
            events.append(
                DriftEvent(field, "removed", old_type, None, field in pii_fields)
            )

    return events


def write_drift_events(conn, source: str, events: list[DriftEvent]) -> int:
    """Persist drift events to raw.schema_drift_log. Returns rows written."""
    if not events:
        return 0
    from psycopg2.extras import execute_values

    rows = [
        (source, e.field_name, e.change_type, e.old_type, e.new_type, e.is_pii)
        for e in events
    ]
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO raw.schema_drift_log
                (source, field_name, change_type, old_type, new_type, is_pii)
            VALUES %s
            """,
            rows,
        )
    conn.commit()
    return len(rows)
