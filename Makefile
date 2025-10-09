# 254Carbon Ingestion Layer Makefile
# Build automation and common tasks

.PHONY: help setup lint test build validate-schemas connectors-index airflow-up infra-up observability-up observability-down observability-logs observability-status prometheus-up prometheus-down prometheus-logs grafana-up grafana-down grafana-logs jaeger-up jaeger-down jaeger-logs run-normalization simulate-miso clean wait-clickhouse pipeline-apply-ddl pipeline-services-up pipeline-services-down pipeline-bootstrap pipeline-run pipeline-status pipeline-down

CLUSTER_NAME ?= local-254carbon

# Default target
help:
	@echo "254Carbon Ingestion Layer - Available targets:"
	@echo "  setup              - Install development dependencies"
	@echo "  lint               - Run linting and formatting checks"
	@echo "  test               - Run unit and integration tests"
	@echo "  build              - Build multi-architecture containers"
	@echo "  validate-schemas   - Validate Avro schema compatibility"
	@echo "  connectors-index   - Generate connectors metadata index"
	@echo "  airflow-up         - Launch local Airflow instance"
	@echo "  infra-up           - Start Kafka/ClickHouse/Redis via compose"
	@echo "  observability-up   - Start observability stack (Prometheus, Grafana, Jaeger)"
	@echo "  observability-down - Stop observability stack"
	@echo "  observability-logs - View observability stack logs"
	@echo "  observability-status - Check observability stack status"
	@echo "  prometheus-up      - Start Prometheus only"
	@echo "  prometheus-down    - Stop Prometheus only"
	@echo "  prometheus-logs    - View Prometheus logs"
	@echo "  grafana-up         - Start Grafana only"
	@echo "  grafana-down       - Stop Grafana only"
	@echo "  grafana-logs       - View Grafana logs"
	@echo "  jaeger-up          - Start Jaeger only"
	@echo "  jaeger-down        - Stop Jaeger only"
	@echo "  jaeger-logs        - View Jaeger logs"
	@echo "  run-normalization  - Run normalization service locally"
	@echo "  simulate-miso      - Simulate MISO raw event publish"
	@echo "  clean              - Clean build artifacts and caches"

# Development setup
setup:
	pip install -r requirements-dev.txt
	pip install -e .

# Code quality
lint:
	ruff check .
	ruff format --check .
	mypy .

# Testing
test:
	pytest tests/ -v --cov=. --cov-report=xml

# Build containers
build:
	docker buildx bake --push

# Schema validation
validate-schemas:
	python scripts/validate_schemas.py

# Generate connector index
connectors-index:
	python scripts/generate_connectors_index.py

# Local development
airflow-up:
	docker-compose -f docker-compose.airflow.yml up -d

infra-up:
	docker-compose -f docker-compose.infra.yml up -d

# Observability
observability-up:
	docker-compose -f docker-compose.observability.yml up -d

observability-down:
	docker-compose -f docker-compose.observability.yml down

observability-logs:
	docker-compose -f docker-compose.observability.yml logs -f

observability-status:
	docker-compose -f docker-compose.observability.yml ps

# Prometheus
prometheus-up:
	docker-compose -f docker-compose.observability.yml up -d prometheus

prometheus-down:
	docker-compose -f docker-compose.observability.yml stop prometheus

prometheus-logs:
	docker-compose -f docker-compose.observability.yml logs -f prometheus

# Grafana
grafana-up:
	docker-compose -f docker-compose.observability.yml up -d grafana

grafana-down:
	docker-compose -f docker-compose.observability.yml stop grafana

grafana-logs:
	docker-compose -f docker-compose.observability.yml logs -f grafana

# Jaeger
jaeger-up:
	docker-compose -f docker-compose.observability.yml up -d jaeger

jaeger-down:
	docker-compose -f docker-compose.observability.yml stop jaeger

jaeger-logs:
	docker-compose -f docker-compose.observability.yml logs -f jaeger

run-normalization:
	python -m services.service-normalization.src.main

wait-clickhouse:
	@echo "Waiting for ClickHouse at http://localhost:8123/ping ..."
	@for _ in $$(seq 1 60); do \
		if curl -fsS http://localhost:8123/ping >/dev/null 2>&1; then \
			echo "ClickHouse is ready."; \
			exit 0; \
		fi; \
		sleep 1; \
	done; \
	echo "ClickHouse did not become ready within the allotted time." >&2; \
	exit 1

pipeline-apply-ddl: wait-clickhouse ## Apply ClickHouse Bronze/Silver/Gold schemas
	python scripts/apply_clickhouse_schemas.py

pipeline-services-up: ## Start normalization, enrichment, and aggregation services
	docker-compose -f docker-compose.services.yml up -d normalization enrichment aggregation

pipeline-services-down: ## Stop normalization, enrichment, and aggregation services
	docker-compose -f docker-compose.services.yml stop normalization enrichment aggregation || true

pipeline-status: ## Show status for normalization/enrichment/aggregation services
	docker-compose -f docker-compose.services.yml ps normalization enrichment aggregation

pipeline-bootstrap: infra-up pipeline-apply-ddl pipeline-services-up ## Bootstraps infra, loads DDLs, and launches normalization pipeline services
	@echo "Pipeline bootstrap complete."

pipeline-run: ## Publish exemplar raw events into Kafka (requires pipeline services running)
	python examples/pipeline/raw_pipeline.py --config examples/pipeline/raw_pipeline_config.yaml

pipeline-down: pipeline-services-down ## Stop pipeline services (infra left running)

pipeline-k8s-bootstrap: ## Bootstrap ingestion services onto local Kubernetes cluster via infra tooling (skips cluster-up if already running)
	@if ! kubectl config current-context >/dev/null 2>&1; then \
		$(MAKE) -C ../infra cluster-up; \
	else \
		context=$$(kubectl config current-context); \
		if [ "$$context" != "kind-$(CLUSTER_NAME)" ]; then \
			$(MAKE) -C ../infra cluster-up; \
		else \
			echo "Using existing cluster context $$context (skipping cluster-up)"; \
		fi; \
	fi
	$(MAKE) -C ../infra k8s-apply-base
	$(MAKE) -C ../infra k8s-apply-platform
	$(MAKE) -C ../infra verify

simulate-miso:
	python scripts/simulate_events.py --connector=miso --count=100

# Cleanup
clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type d -name ".mypy_cache" -exec rm -rf {} +
	rm -rf dist/ build/ *.egg-info/
