# ingestion — HR data ingestion pipeline

Part 1 of 4 in the [workforce-intelligence-platform](../README.md).

Pulls employee and job application data from three HR source systems,
lands it in a layered Postgres data model, transforms it with dbt, and
exposes analytical SQL via Trino. This module establishes the shared data
layer that all other platform modules depend on.

---

## Architecture

```
 Workday API (mock)          Greenhouse API (mock)        Airtable API (real)
      │                             │                            │
      ▼                             ▼                            ▼
 WorkdayConnector          GreenhouseConnector          AirtableConnector
      │                             │                            │
      └──────────────────┬──────────┘                            │
                         ▼                                       │
                  raw.employees                         raw.job_applications
                  raw.job_applications ◄────────────────────────┘
                         │
                         ▼
                    dbt (staging)
                  stg_employees
                  stg_job_applications
                         │
                         ▼
                    dbt (marts)
             dim_employees  ──────────────────► Trino OLAP
             fct_headcount_daily                     │
             fct_attrition_monthly                   ▼
             rpt_recruiting_funnel          dashboard/ (Project 4)
                         │
                         ▼
                  Airflow DAG: hr_ingestion (daily 06:00)
```

---

## Tech stack

| Concern | Technology |
|---|---|
| Language | Python 3.11 |
| HTTP client | httpx + tenacity (retry) |
| Data validation | Pydantic v2 |
| Database | Postgres 16 (pgvector image) |
| Transformations | dbt-core 1.8 + dbt-postgres |
| Analytical SQL | Trino 438 |
| Orchestration | Apache Airflow 2.9 (Astronomer) |
| Testing | pytest + testcontainers |
| Synthetic data | faker 25+ |

---

## Setup

### Prerequisites
- Docker Desktop running
- `make infra-up` executed from the repo root (starts Postgres + Trino + Airflow)

### Install and seed

```bash
cd ingestion
pip install -e ".[dev]"
make seed          # generates 500 employees + 1000 job applications into Postgres
make dbt-run       # builds all dbt models
```

### Verify Trino access

```bash
# Trino CLI (or DBeaver / any JDBC client)
docker exec -it workforce-intelligence-platform-trino-1 trino
> SELECT COUNT(*) FROM postgresql.analytics.dim_employees;
```

---

## Airtable base setup

1. Create a free Airtable account at https://airtable.com
2. Create a new base named `workforce-hr`
3. Create a table named `Employees` with these fields:

| Field name | Type |
|---|---|
| `source_id` | Single line text |
| `first_name` | Single line text |
| `last_name` | Single line text |
| `email` | Email |
| `department` | Single select |
| `job_title` | Single line text |
| `hire_date` | Date |
| `termination_date` | Date (optional) |
| `employment_type` | Single select |
| `level` | Single line text |
| `location` | Single line text |

4. Copy your Personal Access Token from https://airtable.com/create/tokens
5. Set `AIRTABLE_API_KEY` and `AIRTABLE_BASE_ID` in your `.env` file

---

## Make targets

| Target | Description |
|---|---|
| `make setup` | Install dependencies + start mock Workday server |
| `make seed` | Generate and load 500 synthetic employees |
| `make dbt-run` | Run all dbt models |
| `make test-unit` | Run unit tests (no Docker needed) |
| `make test-integration` | Run integration tests (requires Postgres) |
| `make test` | Run all tests |
| `make lint` | ruff + sqlfluff |

---

## dbt models

| Model | Layer | Description |
|---|---|---|
| `stg_employees` | staging | Flattened JSONB from raw.employees |
| `stg_job_applications` | staging | Flattened JSONB from raw.job_applications |
| `dim_employees` | marts | Active employee dimension (SCD Type 1) |
| `fct_headcount_daily` | marts | Daily headcount by department/level |
| `fct_attrition_monthly` | marts | Monthly attrition rate + rolling 12m |
| `rpt_recruiting_funnel` | reports | Application → hire funnel by job/month |

---

## Design decisions

**Upsert over truncate-load.** Idempotent upserts on `(source, source_id)` mean the DAG can
be safely re-run without duplicating records. Truncate-load would require point-in-time recovery
if downstream transforms had already consumed the data.

**Salary and performance_rating excluded from dim_employees.** These fields exist in the raw layer
but are deliberately absent from the analytics-layer dimension. The governance module (Project 3)
adds them as restricted columns accessible only to roles with explicit grants. This models the
separation of concerns between data engineering and data access policy.

**Trino on top of Postgres.** The Postgres instance is the OLTP source of truth. Trino adds a
federated OLAP query layer without copying data — queries from the dashboard or ad-hoc analysts
hit Trino, which pushes predicates down to Postgres. This mirrors production patterns at companies
that run Trino against their operational stores.

**Mock servers over static fixtures.** The mock Workday and Greenhouse servers generate paginated
REST responses from the same synthetic data generator used in tests. This means the connector code
runs against a real HTTP client stack, not mocked responses — integration tests find bugs that
unit tests miss.
