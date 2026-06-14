"""Faker-based synthetic HR data generation.

Used in three places:
1. docker/mock_workday_server.py — serves fake data over HTTP
2. tests/conftest.py — fixtures
3. CLI: ``python -m src.utils.synthetic_data seed`` — seed Postgres directly
"""

from __future__ import annotations

import random
import sys
import uuid
from datetime import date, timedelta

from faker import Faker

DEPARTMENTS = [
    "Engineering",
    "Product",
    "Design",
    "Data",
    "Legal",
    "Finance",
    "Recruiting",
    "HR",
]
LEVELS = ["IC1", "IC2", "IC3", "IC4", "IC5", "M1", "M2", "M3", "M4"]
EMPLOYMENT_TYPES = ["full_time", "part_time", "contractor"]
LOCATIONS = [
    "San Francisco, CA",
    "New York, NY",
    "Austin, TX",
    "Seattle, WA",
    "Remote - US",
    "Dublin, IE",
    "London, UK",
]
PERFORMANCE_RATINGS = ["exceeds", "meets", "below", None]

# Stage distribution for the recruiting funnel.
STAGE_WEIGHTS = {
    "applied": 0.40,
    "phone_screen": 0.30,
    "interview": 0.20,
    "offer": 0.07,
    "hired": 0.03,
}

_TERMINATION_RATE = 0.15


def _seeded_faker(seed: int | None) -> Faker:
    fake = Faker()
    if seed is not None:
        Faker.seed(seed)
        random.seed(seed)
    return fake


def generate_employees(n: int = 500, seed: int | None = None) -> list[dict]:
    """Generate ``n`` synthetic employee records as plain dicts.

    ~15% receive a termination_date to simulate attrition. Hire dates fall within
    the last five years.
    """
    fake = _seeded_faker(seed)
    today = date.today()
    employees: list[dict] = []

    for _ in range(n):
        first = fake.first_name()
        last = fake.last_name()
        hire = fake.date_between(start_date="-5y", end_date="today")
        terminated = random.random() < _TERMINATION_RATE
        term_date = None
        if terminated:
            # Terminate somewhere between hire date and today.
            span = (today - hire).days
            if span > 1:
                term_date = hire + timedelta(days=random.randint(1, span))

        employees.append(
            {
                "source_id": fake.uuid4(),
                "first_name": first,
                "last_name": last,
                "email": f"{first}.{last}@example.com".lower(),
                "department": random.choice(DEPARTMENTS),
                "job_title": fake.job()[:80],
                "hire_date": hire.isoformat(),
                "termination_date": term_date.isoformat() if term_date else None,
                "manager_id": fake.uuid4() if random.random() > 0.1 else None,
                "employment_type": random.choices(
                    EMPLOYMENT_TYPES, weights=[0.8, 0.1, 0.1]
                )[0],
                "level": random.choice(LEVELS),
                "location": random.choice(LOCATIONS),
                "salary": round(random.uniform(80_000, 320_000), 2),
                "performance_rating": random.choice(PERFORMANCE_RATINGS),
            }
        )
    return employees


def generate_job_applications(
    employees: list[dict], n: int = 1000, seed: int | None = None
) -> list[dict]:
    """Generate ``n`` job application records distributed across funnel stages."""
    fake = _seeded_faker(seed)
    stages = list(STAGE_WEIGHTS.keys())
    weights = list(STAGE_WEIGHTS.values())
    applications: list[dict] = []

    for _ in range(n):
        dept = random.choice(DEPARTMENTS)
        applied = fake.date_time_between(start_date="-2y", end_date="now")
        stage = random.choices(stages, weights=weights)[0]
        stage_changed = applied + timedelta(days=random.randint(0, 60))

        applications.append(
            {
                "source_id": fake.uuid4(),
                "candidate_id": fake.uuid4(),
                "job_id": f"JOB-{random.randint(1000, 1099)}",
                "job_title": fake.job()[:80],
                "department": dept,
                "stage": stage,
                "applied_at": applied.isoformat(),
                "stage_changed_at": stage_changed.isoformat(),
                "recruiter_id": fake.uuid4() if random.random() > 0.2 else None,
            }
        )
    return applications


def seed_database(
    n_employees: int = 500, n_applications: int = 1000, seed: int | None = None
) -> dict[str, int]:
    """Generate synthetic data and upsert it into Postgres (``workday`` source).

    Imported lazily so this module stays importable without a DB driver present
    (e.g. in lightweight unit-test environments).
    """
    from src.models.employee import EmployeeRaw, JobApplicationRaw
    from src.utils.db import get_connection, upsert_employees, upsert_job_applications

    employees = [EmployeeRaw(**e) for e in generate_employees(n_employees, seed=seed)]
    raw_apps = generate_job_applications(
        generate_employees(10, seed=seed), n_applications, seed=seed
    )
    applications = [JobApplicationRaw(**a) for a in raw_apps]

    batch_id = uuid.uuid4()
    conn = get_connection()
    try:
        emp_count = upsert_employees(conn, employees, batch_id, source="workday")
        app_count = upsert_job_applications(
            conn, applications, batch_id, source="greenhouse"
        )
        conn.commit()
    finally:
        conn.close()
    return {"employees": emp_count, "job_applications": app_count}


def _main(argv: list[str]) -> int:
    if len(argv) >= 1 and argv[0] == "seed":
        stats = seed_database()
        print(f"Seeded {stats['employees']} employees, "
              f"{stats['job_applications']} job applications.")
        return 0
    print("usage: python -m src.utils.synthetic_data seed")
    return 1


if __name__ == "__main__":
    raise SystemExit(_main(sys.argv[1:]))


__all__ = [
    "generate_employees",
    "generate_job_applications",
    "seed_database",
    "DEPARTMENTS",
    "LEVELS",
    "EMPLOYMENT_TYPES",
]
