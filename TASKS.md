# TASKS.md — ingestion/

> Read `../TASKS.md` first for platform-wide rules.
> This file drives all implementation work for the HR ingestion pipeline.

---

## What this project builds

A production-grade HR data ingestion pipeline with three source connectors,
a layered Postgres data model, dbt transformations, and Trino OLAP access.

This is the foundational module. Every other project depends on the schemas,
roles, and data it creates.

---

## Directory structure

```
ingestion/
├── TASKS.md                     ← this file
├── README.md
├── Makefile
├── pyproject.toml
├── src/
│   ├── __init__.py
│   ├── connectors/
│   │   ├── __init__.py
│   │   ├── base.py              ← abstract BaseConnector class
│   │   ├── workday.py           ← mock Workday REST connector
│   │   ├── greenhouse.py        ← mock Greenhouse REST connector
│   │   └── airtable.py          ← real Airtable REST connector
│   ├── models/
│   │   ├── __init__.py
│   │   └── employee.py          ← Pydantic models for all HR entities
│   └── utils/
│       ├── __init__.py
│       ├── db.py                ← Postgres connection + helpers
│       ├── schema_drift.py      ← schema drift detector
│       └── synthetic_data.py    ← faker-based data generator
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── packages.yml
│   ├── macros/
│   │   └── generate_schema_name.sql
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_employees.sql
│   │   │   ├── stg_job_applications.sql
│   │   │   └── schema.yml
│   │   ├── marts/
│   │   │   ├── dim_employees.sql
│   │   │   ├── fct_headcount_daily.sql
│   │   │   ├── fct_attrition_monthly.sql
│   │   │   └── schema.yml
│   │   └── reports/
│   │       ├── rpt_recruiting_funnel.sql
│   │       └── schema.yml
│   └── tests/
│       ├── assert_no_null_employee_id.sql
│       └── assert_headcount_positive.sql
├── airflow/
│   ├── dags/
│   │   └── hr_ingestion_dag.py
│   └── plugins/
│       └── hr_ingestion_plugin.py
├── docker/
│   ├── init.sql                 ← Postgres schema + role DDL
│   ├── mock_workday_server.py   ← Flask mock API for local dev
│   └── trino/
│       └── catalog/
│           └── postgresql.properties
├── tests/
│   ├── conftest.py
│   ├── unit/
│   │   ├── test_workday_connector.py
│   │   ├── test_greenhouse_connector.py
│   │   ├── test_airtable_connector.py
│   │   ├── test_schema_drift.py
│   │   └── test_synthetic_data.py
│   └── integration/
│       ├── test_postgres_ingestion.py
│       └── test_trino_queries.py
└── .github/
    └── workflows/
        └── ingestion-ci.yml
```

---

## Implementation tasks

### Task 1.0 — Postgres initialisation DDL (`docker/init.sql`)

Create the following in order:

```sql
-- 1. Extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- 2. Schemas
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS governance;
CREATE SCHEMA IF NOT EXISTS dashboard;
CREATE SCHEMA IF NOT EXISTS llm;

-- 3. Roles
CREATE ROLE ingestion_writer LOGIN PASSWORD '${INGESTION_WRITER_PASSWORD}';
CREATE ROLE dbt_transformer  LOGIN PASSWORD '${DBT_TRANSFORMER_PASSWORD}';
CREATE ROLE analyst_reader   LOGIN PASSWORD '${ANALYST_READER_PASSWORD}';

-- 4. Grants
GRANT USAGE ON SCHEMA raw       TO ingestion_writer;
GRANT INSERT, UPDATE ON ALL TABLES IN SCHEMA raw TO ingestion_writer;

GRANT USAGE ON SCHEMA raw, staging, analytics TO dbt_transformer;
GRANT SELECT ON ALL TABLES IN SCHEMA raw      TO dbt_transformer;
GRANT CREATE ON SCHEMA staging, analytics     TO dbt_transformer;

GRANT USAGE ON SCHEMA analytics, dashboard    TO analyst_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO analyst_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA dashboard TO analyst_reader;

-- 5. Core raw tables (schema-on-write)
CREATE TABLE IF NOT EXISTS raw.employees (
    id              UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    source          VARCHAR(50) NOT NULL,             -- 'workday' | 'greenhouse' | 'airtable'
    source_id       VARCHAR(255) NOT NULL,
    payload         JSONB NOT NULL,
    ingested_at     TIMESTAMPTZ DEFAULT NOW(),
    batch_id        UUID NOT NULL,
    UNIQUE (source, source_id)
);

CREATE TABLE IF NOT EXISTS raw.job_applications (
    id              UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    source          VARCHAR(50) NOT NULL,
    source_id       VARCHAR(255) NOT NULL,
    payload         JSONB NOT NULL,
    ingested_at     TIMESTAMPTZ DEFAULT NOW(),
    batch_id        UUID NOT NULL
);

CREATE TABLE IF NOT EXISTS raw.schema_drift_log (
    id              SERIAL PRIMARY KEY,
    source          VARCHAR(50) NOT NULL,
    detected_at     TIMESTAMPTZ DEFAULT NOW(),
    field_name      VARCHAR(255) NOT NULL,
    change_type     VARCHAR(50) NOT NULL,   -- 'added' | 'removed' | 'type_changed'
    old_type        VARCHAR(100),
    new_type        VARCHAR(100),
    is_pii          BOOLEAN DEFAULT FALSE
);

-- 6. Indexes
CREATE INDEX idx_employees_source_id      ON raw.employees(source, source_id);
CREATE INDEX idx_employees_ingested_at    ON raw.employees(ingested_at);
CREATE INDEX idx_job_apps_source_id       ON raw.job_applications(source, source_id);
```

The `init.sql` file uses Docker's `POSTGRES_*` env substitution. Passwords must be
read from environment variables — not hardcoded.

---

### Task 1.1 — Trino catalog (`docker/trino/catalog/postgresql.properties`)

```properties
connector.name=postgresql
connection-url=jdbc:postgresql://postgres:5432/workforce
connection-user=analyst_reader
connection-password=<from env>
```

Trino must be able to query `analytics.*` tables via:
```sql
SELECT * FROM postgresql.analytics.dim_employees LIMIT 10;
```

---

### Task 1.2 — Pydantic models (`src/models/employee.py`)

Define these models:

```python
class EmployeeRaw(BaseModel):
    source_id: str
    first_name: str
    last_name: str
    email: str
    department: str
    job_title: str
    hire_date: date
    termination_date: Optional[date] = None
    manager_id: Optional[str] = None
    employment_type: str          # 'full_time' | 'part_time' | 'contractor'
    level: str                    # 'IC1' through 'IC6', 'M1' through 'M5'
    location: str
    salary: Optional[Decimal] = None   # sensitive — masked downstream
    performance_rating: Optional[str] = None  # sensitive

class JobApplicationRaw(BaseModel):
    source_id: str
    candidate_id: str
    job_id: str
    job_title: str
    department: str
    stage: str    # 'applied' | 'phone_screen' | 'interview' | 'offer' | 'hired' | 'rejected'
    applied_at: datetime
    stage_changed_at: datetime
    recruiter_id: Optional[str] = None
```

Add a `model_config = ConfigDict(str_strip_whitespace=True)` to both models.

---

### Task 1.3 — Base connector (`src/connectors/base.py`)

```python
from abc import ABC, abstractmethod
from typing import Iterator
from src.models.employee import EmployeeRaw, JobApplicationRaw

class BaseConnector(ABC):
    """Abstract base for all HR source connectors."""

    @abstractmethod
    def fetch_employees(self) -> Iterator[EmployeeRaw]:
        """Yield employee records from the source system."""
        ...

    @abstractmethod
    def fetch_job_applications(self) -> Iterator[JobApplicationRaw]:
        """Yield job application records from the source system."""
        ...

    def source_name(self) -> str:
        return self.__class__.__name__.replace("Connector", "").lower()
```

---

### Task 1.4 — Mock Workday connector (`src/connectors/workday.py`)

The mock Workday connector calls a local Flask server (`docker/mock_workday_server.py`)
that returns synthetic JSON. In production, replace the base URL env var to point at
the real Workday API.

Implementation requirements:
- Read `WORKDAY_BASE_URL` from env (defaults to `http://localhost:5001`)
- Read `WORKDAY_API_TOKEN` from env
- Use `httpx` for HTTP calls (not `requests`) — async-compatible
- Implement pagination: follow `_links.next` in response until absent
- Retry on 429 / 5xx with exponential backoff (use `tenacity`)
- Map Workday JSON keys to `EmployeeRaw` fields via a `_map_employee()` private method

```python
# Key mapping example
WORKDAY_FIELD_MAP = {
    "Worker_ID": "source_id",
    "Legal_First_Name": "first_name",
    "Legal_Last_Name": "last_name",
    "Work_Email": "email",
    "Cost_Center": "department",
    "Job_Title": "job_title",
    "Hire_Date": "hire_date",
    "Termination_Date": "termination_date",
}
```

---

### Task 1.5 — Mock Greenhouse connector (`src/connectors/greenhouse.py`)

Same pattern as Workday but:
- `GREENHOUSE_BASE_URL` / `GREENHOUSE_API_KEY` from env
- Greenhouse returns job applications, not employees
- Implement `fetch_job_applications()` only; `fetch_employees()` raises `NotImplementedError`
- Map `application.status` to the stage enum in `JobApplicationRaw`

---

### Task 1.6 — Airtable connector (`src/connectors/airtable.py`)

Uses the real Airtable API (free account). Implementation requirements:
- `AIRTABLE_API_KEY` and `AIRTABLE_BASE_ID` from env
- Use `pyairtable` library for the client
- Table name: `Employees` (user creates this in their Airtable base with synthetic data)
- Implement both `fetch_employees()` and `fetch_job_applications()` (two tables)
- Handle Airtable's rate limit (5 req/sec) with a `time.sleep(0.2)` between pages

In the README, add clear instructions for creating the Airtable base with the correct
field names. The user should be able to follow them in 10 minutes.

---

### Task 1.7 — Schema drift detector (`src/utils/schema_drift.py`)

```python
def detect_drift(
    source: str,
    new_records: list[dict],
    baseline_schema: dict[str, str],   # field_name -> type_hint
    pii_fields: set[str],
) -> list[DriftEvent]:
    """
    Compare field names and value types in new_records against baseline.
    Return list of DriftEvent(field_name, change_type, old_type, new_type, is_pii).
    """
```

Logic:
1. Infer schema from `new_records` (field names + Python type of first non-null value)
2. Compare against `baseline_schema`
3. Detect: added fields, removed fields, type changes
4. Flag any drifted field that appears in `pii_fields`
5. Write results to `raw.schema_drift_log`

---

### Task 1.8 — Synthetic data generator (`src/utils/synthetic_data.py`)

Use `faker` to generate realistic employee data for local development.

```python
def generate_employees(n: int = 500) -> list[dict]:
    """
    Generate n synthetic employee records.
    Departments: Engineering, Product, Design, Data, Legal, Finance, Recruiting, HR.
    Levels: IC1-IC5, M1-M4.
    Hire dates: random within last 5 years.
    ~15% have termination_date (attrition simulation).
    """

def generate_job_applications(employees: list[dict], n: int = 1000) -> list[dict]:
    """
    Generate n job application records.
    Stages distributed as: applied(40%) → phone_screen(30%) → interview(20%) → offer(7%) → hired(3%).
    """
```

This generator is used in:
1. `docker/mock_workday_server.py` (serves fake data via Flask)
2. `tests/conftest.py` (fixtures)
3. Standalone: `python -m ingestion.utils.synthetic_data` to seed the DB directly

---

### Task 1.9 — Database utilities (`src/utils/db.py`)

```python
def get_connection() -> psycopg2.connection:
    """Return a connection using env vars."""

def upsert_employees(conn, records: list[EmployeeRaw], batch_id: UUID) -> int:
    """
    Upsert into raw.employees using INSERT ... ON CONFLICT (source, source_id) DO UPDATE.
    Return count of rows written.
    """

def upsert_job_applications(conn, records: list[JobApplicationRaw], batch_id: UUID) -> int:
    """Upsert into raw.job_applications. Return count of rows written."""
```

Use `psycopg2` with `execute_values` for bulk inserts (not row-by-row).

---

### Task 1.10 — dbt models

**`dbt_project.yml`**:
```yaml
name: workforce_ingestion
version: '1.0.0'
config-version: 2
profile: workforce
model-paths: ["models"]
test-paths: ["tests"]
macro-paths: ["macros"]
models:
  workforce_ingestion:
    staging:
      +schema: staging
      +materialized: view
    marts:
      +schema: analytics
      +materialized: table
    reports:
      +schema: analytics
      +materialized: table
```

**`models/staging/stg_employees.sql`**:
```sql
-- Flatten JSONB payload from raw.employees into typed columns.
-- Source: raw.employees WHERE source IN ('workday', 'airtable')
-- Cast all fields to proper types.
-- Exclude terminated employees with termination_date > 90 days ago.
-- Add is_active flag: termination_date IS NULL OR termination_date > CURRENT_DATE
```

**`models/staging/stg_job_applications.sql`**:
```sql
-- Flatten raw.job_applications (source: greenhouse, airtable).
-- Parse stage timestamps.
-- Calculate time_in_stage_days for each stage transition.
```

**`models/marts/dim_employees.sql`**:
```sql
-- SCD Type 1 dimension from stg_employees.
-- Columns: employee_id, full_name, email, department, job_title, level,
--          hire_date, termination_date, is_active, employment_type,
--          manager_id, location, created_at, updated_at
-- NOTE: salary and performance_rating are intentionally EXCLUDED.
--       They exist in raw but are masked here. Governance module adds them
--       as restricted columns accessible only to the hr_partner role.
```

**`models/marts/fct_headcount_daily.sql`**:
```sql
-- Spine: generate_series over the past 2 years, daily.
-- For each date: count of active employees by department, level, employment_type.
-- Join against dim_employees using hire_date <= spine_date AND
-- (termination_date IS NULL OR termination_date > spine_date).
```

**`models/marts/fct_attrition_monthly.sql`**:
```sql
-- Monthly attrition rate = terminations in month / avg headcount in month.
-- Columns: year_month, department, voluntary_terminations,
--          involuntary_terminations, total_terminations, avg_headcount,
--          attrition_rate_pct, rolling_12m_attrition_rate_pct
```

**`models/reports/rpt_recruiting_funnel.sql`**:
```sql
-- From stg_job_applications.
-- Columns: job_id, job_title, department, applied_count, phone_screen_count,
--          interview_count, offer_count, hired_count,
--          application_to_hire_days_avg, offer_acceptance_rate_pct
-- Grain: one row per job_id per month.
```

**dbt tests to implement** (in `schema.yml`):
- `not_null` on all primary keys
- `unique` on `dim_employees.employee_id`
- `accepted_values` on `employment_type` and `level`
- Custom test `assert_headcount_positive`: headcount never negative

---

### Task 1.11 — Airflow DAG (`airflow/dags/hr_ingestion_dag.py`)

```python
from airflow.decorators import dag, task
from datetime import datetime, timedelta

@dag(
    dag_id="hr_ingestion",
    schedule="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["ingestion", "people-analytics"],
)
def hr_ingestion_dag():
    @task
    def extract_workday() -> dict:
        """Run WorkdayConnector, upsert to raw.employees. Return stats dict."""

    @task
    def extract_greenhouse() -> dict:
        """Run GreenhouseConnector, upsert to raw.job_applications. Return stats dict."""

    @task
    def extract_airtable() -> dict:
        """Run AirtableConnector, upsert to raw.employees + raw.job_applications."""

    @task
    def detect_schema_drift(workday_stats: dict, airtable_stats: dict) -> list:
        """Run schema drift detector against new records. Return list of drift events."""

    @task
    def run_dbt_models() -> None:
        """Run dbt using dbt-core Python API (not BashOperator)."""
        # from dbt.cli.main import dbtRunner
        # dbtRunner().invoke(["run", "--project-dir", ...])

    @task
    def alert_on_pii_change(drift_events: list) -> None:
        """If any drift_event.is_pii is True, send Slack alert via SLACK_WEBHOOK_URL."""

    @task
    def trigger_llm_eval(**context) -> None:
        """TriggerDagRunOperator pattern — kick off llm_eval_embedding_refresh."""

    # Wire tasks
    workday = extract_workday()
    greenhouse = extract_greenhouse()
    airtable = extract_airtable()
    drift = detect_schema_drift(workday, airtable)
    dbt = run_dbt_models()
    alert = alert_on_pii_change(drift)
    trigger = trigger_llm_eval()

    [workday, greenhouse, airtable] >> drift >> dbt >> alert >> trigger

hr_ingestion_dag()
```

---

### Task 1.12 — Mock Workday Flask server (`docker/mock_workday_server.py`)

```python
from flask import Flask, jsonify, request
from src.utils.synthetic_data import generate_employees, generate_job_applications

app = Flask(__name__)

# Generate data once at startup
EMPLOYEES = generate_employees(500)
APPLICATIONS = generate_job_applications(EMPLOYEES, 1000)

@app.route("/api/v1/workers")
def get_workers():
    """Paginated endpoint. ?page=1&page_size=100"""
    # Implement pagination with _links.next

@app.route("/api/v1/workers/<worker_id>")
def get_worker(worker_id: str):
    """Single worker by ID."""

if __name__ == "__main__":
    app.run(port=5001, debug=False)
```

---

### Task 1.13 — Tests

**`tests/unit/test_workday_connector.py`**:
- Mock `httpx.get` to return fixture JSON
- Assert `fetch_employees()` yields correct `EmployeeRaw` objects
- Assert retry logic fires on 429
- Assert pagination follows `_links.next`

**`tests/unit/test_schema_drift.py`**:
- Test: no drift on identical schema
- Test: new field detected as `added`
- Test: missing field detected as `removed`
- Test: PII field drift sets `is_pii=True`

**`tests/integration/test_postgres_ingestion.py`**:
- Spin up Postgres via `testcontainers`
- Run `init.sql`
- Upsert 10 synthetic employees
- Assert `raw.employees` count = 10
- Upsert same 10 again — assert count still = 10 (upsert idempotency)

**`tests/integration/test_trino_queries.py`**:
- Requires running Trino (mark with `@pytest.mark.integration`)
- Connect via `trino` Python client
- Assert `SELECT COUNT(*) FROM postgresql.analytics.dim_employees` returns > 0

---

### Task 1.14 — README.md

Include:
1. One-paragraph purpose statement
2. ASCII architecture diagram showing: sources → connectors → Postgres → dbt → Trino
3. Tech stack table: Language, Framework, DB, Orchestration, Testing
4. Setup instructions (step-by-step, numbered)
5. Airtable base setup instructions (field names, types — so recruiter can recreate)
6. `make` targets table
7. dbt model descriptions (one line each)
8. "Design decisions" section covering:
   - Why `upsert` over `truncate-load`
   - Why salary/performance_rating are excluded from `dim_employees`
   - Why Trino sits on top of Postgres (OLTP vs OLAP access patterns)

---

### Task 1.15 — pyproject.toml

```toml
[project]
name = "wip-ingestion"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = [
    "httpx>=0.27",
    "psycopg2-binary>=2.9",
    "pydantic>=2.7",
    "pyairtable>=2.3",
    "faker>=25.0",
    "python-dotenv>=1.0",
    "dbt-core>=1.8",
    "dbt-postgres>=1.8",
    "tenacity>=8.3",
    "apache-airflow>=2.9.1",
    "flask>=3.0",       # mock server only
]

[project.optional-dependencies]
dev = [
    "pytest>=8.0",
    "pytest-cov>=5.0",
    "pytest-mock>=3.14",
    "testcontainers[postgres]>=4.5",
    "ruff>=0.4",
    "sqlfluff>=3.0",
]
```

---

### Task 1.16 — Makefile

```makefile
.PHONY: setup seed dbt-run test test-unit test-integration lint

setup:
	pip install -e ".[dev]"
	python docker/mock_workday_server.py &
	python -m ingestion.utils.synthetic_data seed

seed:
	python -m ingestion.utils.synthetic_data seed

dbt-run:
	cd dbt && dbt deps && dbt run --profiles-dir .

test-unit:
	pytest tests/unit/ -v --cov=src --cov-report=term-missing

test-integration:
	pytest tests/integration/ -v -m integration

test: test-unit test-integration

lint:
	ruff check src/
	sqlfluff lint dbt/models --dialect postgres
```

---

## Acceptance criteria

- [ ] `make setup && make seed` completes without errors
- [ ] `raw.employees` contains ≥ 500 rows after seed
- [ ] `make dbt-run` completes with 0 model errors
- [ ] `analytics.dim_employees` is queryable via Trino
- [ ] `make test-unit` passes with ≥ 80% coverage
- [ ] Airflow DAG `hr_ingestion` visible in UI, runs end-to-end on manual trigger
- [ ] Schema drift log writes a row when a new field appears in source data
- [ ] PII field drift triggers a Slack alert (verify with a test field named `ssn`)
