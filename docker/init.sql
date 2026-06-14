-- ============================================================================
-- workforce-intelligence-platform :: shared Postgres bootstrap (schema-on-write)
-- ----------------------------------------------------------------------------
-- This file is mounted into docker-entrypoint-initdb.d and also run directly in
-- CI via `psql -f`. It MUST NOT contain ${ENV} placeholders: neither the Postgres
-- entrypoint nor psql expands shell-style variables in .sql files.
--
-- Role creation + per-role passwords live in src.utils.db.bootstrap_roles(),
-- which reads credentials from environment variables. See README "Design decisions".
-- ============================================================================

-- 1. Extensions ---------------------------------------------------------------
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- 2. Schemas ------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS governance;
CREATE SCHEMA IF NOT EXISTS dashboard;
CREATE SCHEMA IF NOT EXISTS llm;

-- 3. Core raw tables ----------------------------------------------------------
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
    batch_id        UUID NOT NULL,
    UNIQUE (source, source_id)
);

CREATE TABLE IF NOT EXISTS raw.schema_drift_log (
    id              SERIAL PRIMARY KEY,
    source          VARCHAR(50) NOT NULL,
    detected_at     TIMESTAMPTZ DEFAULT NOW(),
    field_name      VARCHAR(255) NOT NULL,
    change_type     VARCHAR(50) NOT NULL,             -- 'added' | 'removed' | 'type_changed'
    old_type        VARCHAR(100),
    new_type        VARCHAR(100),
    is_pii          BOOLEAN DEFAULT FALSE
);

-- 4. Indexes ------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_employees_source_id   ON raw.employees(source, source_id);
CREATE INDEX IF NOT EXISTS idx_employees_ingested_at ON raw.employees(ingested_at);
CREATE INDEX IF NOT EXISTS idx_job_apps_source_id    ON raw.job_applications(source, source_id);
