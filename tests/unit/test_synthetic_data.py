"""Unit tests for the synthetic data generator."""

from __future__ import annotations

from src.models.employee import EMPLOYMENT_TYPES, EmployeeRaw, JobApplicationRaw
from src.utils.synthetic_data import (
    DEPARTMENTS,
    generate_employees,
    generate_job_applications,
)


def test_generate_employees_count():
    assert len(generate_employees(50, seed=1)) == 50


def test_generate_employees_are_valid_models():
    for emp in generate_employees(20, seed=1):
        model = EmployeeRaw(**emp)
        assert model.department in DEPARTMENTS
        assert model.employment_type in EMPLOYMENT_TYPES


def test_generate_employees_is_deterministic_with_seed():
    a = generate_employees(10, seed=99)
    b = generate_employees(10, seed=99)
    assert [e["source_id"] for e in a] == [e["source_id"] for e in b]


def test_some_employees_terminated():
    emps = generate_employees(500, seed=3)
    terminated = [e for e in emps if e["termination_date"] is not None]
    # ~15% expected; assert a sane non-trivial fraction.
    assert 0 < len(terminated) < len(emps)


def test_generate_job_applications():
    emps = generate_employees(10, seed=2)
    apps = generate_job_applications(emps, 100, seed=2)
    assert len(apps) == 100
    for app in apps:
        model = JobApplicationRaw(**app)
        assert model.stage in {
            "applied",
            "phone_screen",
            "interview",
            "offer",
            "hired",
            "rejected",
        }
