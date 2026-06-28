.PHONY: setup bootstrap mock-server seed dbt-run dbt-test test test-unit test-integration lint clean teardown

# Source the repo-root .env (Postgres creds + per-role passwords) into a recipe's
# shell so DB-touching targets work without manually exporting it. Applied only to
# the targets below — NOT to the test targets, which must stay hermetic (loading
# real credentials, e.g. AIRTABLE_API_KEY, would defeat the "unconfigured" tests).
# Harmless when the file is absent (e.g. CI, which supplies these as real env vars).
LOAD_ENV := set -a; [ -f ../.env ] && . ../.env; set +a

setup: bootstrap mock-server seed
	@echo "Ingestion setup complete."

bootstrap:
	pip install -e ".[dev]"
	@echo "Creating platform roles + grants (reads passwords from env)..."
	@$(LOAD_ENV); python -c "from src.utils.db import get_connection, bootstrap_roles; bootstrap_roles(get_connection())"

mock-server:
	@echo "Starting mock Workday server in background on :5001"
	python docker/mock_workday_server.py &

seed:
	@echo "Generating and loading synthetic HR data..."
	@$(LOAD_ENV); python -m src.utils.synthetic_data seed

dbt-run:
	@$(LOAD_ENV); cd dbt && dbt deps && dbt run --profiles-dir .

dbt-test:
	@$(LOAD_ENV); cd dbt && dbt test --profiles-dir .

test-unit:
	pytest tests/unit/ -v --cov=src --cov-report=term-missing --cov-fail-under=80

test-integration:
	pytest tests/integration/ -v -m integration

test: test-unit test-integration

lint:
	ruff check src/ tests/
	cd dbt && sqlfluff lint models --dialect postgres

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete
	rm -rf .coverage htmlcov

# Graceful teardown — the inverse of `infra-up` + `setup`. Stops the local
# mock-server, drops the dbt schemas + truncates the raw landing tables, clears
# local caches, then shuts down the shared Docker stack (repo-root `infra-down`).
# DB cleanup runs first, while Postgres is still up, and is skipped if it is
# already down. Volumes are preserved (sibling-module data + Airflow metadata
# survive) — use repo-root `make infra-reset` to also wipe them. Re-runnable;
# `make infra-up && make setup && make dbt-run` rebuilds from clean.
teardown:
	@echo "Tearing down ingestion (graceful)..."
	@echo "  - stopping any local mock-server (host process on :5001)..."
	-@pkill -f "docker/mock_workday_server.py" >/dev/null 2>&1 && echo "    stopped local mock-server" || echo "    none running"
	@echo "  - dropping dbt schemas + truncating raw tables (skipped if Postgres is down)..."
	@$(LOAD_ENV); python -c "from src.utils.db import get_connection, teardown_data; print('\n'.join('    ' + a for a in teardown_data(get_connection())))" || echo "    (Postgres not reachable — skipped DB cleanup)"
	@$(MAKE) clean
	@echo "  - shutting down the shared Docker stack (postgres/trino/airflow/mock-hr)..."
	@$(MAKE) -C .. infra-down
	@echo "Ingestion teardown complete. Volumes preserved — run repo-root 'make infra-reset' to also wipe them."
