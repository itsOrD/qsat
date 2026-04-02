.PHONY: setup dev mock test test-integration test-mock test-all build up up-demo up-secure down clean generate-data

setup:                              ## Install dependencies
	pip install -r requirements.txt

dev:                                ## Start service with hot reload
	uvicorn app.main:app --reload --port 8000

mock:                               ## Start the provided mock Slack server
	uvicorn mock_slack.server:app --host 0.0.0.0 --port 9000

test:                               ## Run unit tests
	python -m pytest tests/unit/ -v

test-integration:                   ## Run integration test (dry_run mode, no network needed)
	TEST_SLACK_MODE=dry_run python -m pytest tests/integration/ -v

test-mock:                          ## Run integration test against mock Slack (requires mock server)
	TEST_SLACK_MODE=mock python -m pytest tests/integration/ -v

test-all:                           ## Run all tests including integration (dry_run)
	python -m pytest tests/ -v

build:                              ## Build Docker image
	docker build -t quadsci-risk-alerts .

up:                                 ## Start all services with Docker Compose
	docker compose up --build

up-demo:                            ## Start Docker Compose in demo mode (RBAC disabled)
	APP_MODE=demo docker compose up --build

up-secure:                          ## Start Docker Compose in secure mode (RBAC enabled)
	@test -n "$(RBAC_RUNNER_TOKENS)" || (echo "RBAC_RUNNER_TOKENS is required"; exit 1)
	@test -n "$(RBAC_VIEWER_TOKENS)" || (echo "RBAC_VIEWER_TOKENS is required"; exit 1)
	APP_MODE=secure RBAC_RUNNER_TOKENS="$(RBAC_RUNNER_TOKENS)" RBAC_VIEWER_TOKENS="$(RBAC_VIEWER_TOKENS)" docker compose up --build

down:                               ## Stop Docker Compose services
	docker compose down

generate-data:                      ## Generate synthetic test Parquet file
	python scripts/generate_test_data.py

clean:                              ## Remove generated files
	rm -rf data/*.db __pycache__ .pytest_cache
