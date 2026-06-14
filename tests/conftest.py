"""Shared pytest fixtures for the ingestion test suite."""

from __future__ import annotations

import pytest

from src.utils.synthetic_data import generate_employees, generate_job_applications


@pytest.fixture
def sample_employees() -> list[dict]:
    return generate_employees(25, seed=7)


@pytest.fixture
def sample_applications(sample_employees: list[dict]) -> list[dict]:
    return generate_job_applications(sample_employees, 50, seed=7)


def _workday_record(emp: dict) -> dict:
    """Synthetic employee dict -> Workday API field names (mirrors mock server)."""
    return {
        "Worker_ID": emp["source_id"],
        "Legal_First_Name": emp["first_name"],
        "Legal_Last_Name": emp["last_name"],
        "Work_Email": emp["email"],
        "Cost_Center": emp["department"],
        "Job_Title": emp["job_title"],
        "Hire_Date": emp["hire_date"],
        "Termination_Date": emp["termination_date"],
        "Worker_Type": emp["employment_type"],
        "Management_Level": emp["level"],
        "Location": emp["location"],
        "Manager_ID": emp["manager_id"],
        "Base_Pay": emp["salary"],
        "Performance_Rating": emp["performance_rating"],
    }


@pytest.fixture
def workday_records(sample_employees: list[dict]) -> list[dict]:
    return [_workday_record(e) for e in sample_employees]
