.PHONY: setup bootstrap mock-server seed dbt-run dbt-test test test-unit test-integration lint clean

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
