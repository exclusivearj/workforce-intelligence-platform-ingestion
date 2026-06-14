"""Pydantic models for HR entities flowing through the ingestion pipeline."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict

EMPLOYMENT_TYPES = {"full_time", "part_time", "contractor"}
APPLICATION_STAGES = {
    "applied",
    "phone_screen",
    "interview",
    "offer",
    "hired",
    "rejected",
}


class EmployeeRaw(BaseModel):
    """A single employee record, normalised across all source systems."""

    model_config = ConfigDict(str_strip_whitespace=True)

    source_id: str
    first_name: str
    last_name: str
    email: str
    department: str
    job_title: str
    hire_date: date
    termination_date: date | None = None
    manager_id: str | None = None
    employment_type: str
    level: str
    location: str
    salary: Decimal | None = None             # sensitive — masked downstream
    performance_rating: str | None = None      # sensitive — masked downstream


class JobApplicationRaw(BaseModel):
    """A single job application / candidate pipeline record."""

    model_config = ConfigDict(str_strip_whitespace=True)

    source_id: str
    candidate_id: str
    job_id: str
    job_title: str
    department: str
    stage: str
    applied_at: datetime
    stage_changed_at: datetime
    recruiter_id: str | None = None
