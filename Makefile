# 254Carbon Ingestion Layer Makefile
# Build automation and common tasks

.PHONY: help setup lint test build validate-schemas connectors-index airflow-up infra-up observability-up observability-down observability-logs observability-status prometheus-up prometheus-down prometheus-logs grafana-up grafana-down grafana-logs jaeger-up jaeger-down jaeger-logs run-normalization simulate-miso clean

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

simulate-miso:
	python scripts/simulate_events.py --connector=miso --count=100

# Cleanup
clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type d -name ".mypy_cache" -exec rm -rf {} +
	rm -rf dist/ build/ *.egg-info/
