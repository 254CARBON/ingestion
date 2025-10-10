# Ingestion Layer (`254carbon-ingestion`)

> Connector execution, Airflow orchestration, early normalization/enrichment, and event publishing into the Bronze→Silver stages of the 254Carbon platform.

Reference: [Platform Overview](../PLATFORM_OVERVIEW.md)

---

## Scope
- Manage connector lifecycles (Airflow, SeaTunnel, custom runners) for MISO, CAISO, ERCOT, PJM, and future markets.
- Publish governed events (`ingestion.*`, `normalized.*`, `enriched.*`) and maintain schema compatibility against `specs/`.
- Own ClickHouse Bronze/Silver bootstrap DDL, Redis cache warmers, and data quality assertions that gate downstream domains.
- Provide tooling for backfills, data repair, and connector metadata catalogues.

Out of scope: client-facing APIs, analytics/business logic, or cloud infrastructure bootstrap (see `../access`, `../data-processing`, `../infra`).

---

## Key Components
- `airflow/` – dynamic DAG generation and per-market orchestration. Local harness via `docker-compose.airflow.yml`.
- `connectors/` – connector definitions, schema contracts, and execution metadata.
- `services/` – Python services for normalization, enrichment, aggregation, and projection used during Bronze→Silver promotion.
- `configs/` – runtime policy bundles (normalization, enrichment taxonomy, aggregation rules, observability).
- `scripts/` – operational helpers (schema validation, connector index generation, data simulation).

---

## Environments

| Environment | Bootstrap | Entry Points | Notes |
|-------------|-----------|--------------|-------|
| `local` | `make infra-up` (Kafka/ClickHouse/Redis) + `make airflow-up` | Airflow UI `http://localhost:8084`, Prometheus `http://localhost:9090`, Grafana `http://localhost:3000` | Ideal for connector iteration and schema validation. |
| `dev` | `make pipeline-k8s-bootstrap` (delegates to `../infra`) | `kubectl -n ingestion` services; Airflow via `kubectl port-forward svc/airflow-web 8084:8080` | Shared integration cluster; keep DAG updates backwards compatible. |
| `staging` | GitOps pipeline (managed via `../meta`) | Same topology as dev with stricter RBAC | Smoke tests required before promoting to production. |
| `production` | GitOps apply through platform CD | Production Airflow + connectors via private endpoints | Enforced SLOs and change approvals; follow runbooks for hotfix/backfill. |

Environment templates live in `configs/environments/<env>/env.example`. Copy to `.env` for local use.

---

## Runbook

### Daily Checks (≤10 min)
- `make observability-status` – confirm Prometheus/Grafana/Jaeger containers stay healthy (local) or `kubectl get pods -n ingestion`.
- Review Airflow dashboard (`Airflow → Browse → DAG Runs`) to ensure overnight DAGs finished within SLA.
- Inspect Grafana dashboard `observability/dashboards/ingestion/connectors_health.json` for lag, error rate, and topic throughput anomalies.
- Validate schema drift: `make validate-schemas`; escalate if new payloads fail compatibility.

### Deployments / Releases
1. Update connector/service code and bump service manifest versions if needed.
2. Run `make test` and `make validate-schemas`.
3. Build images: `make build` (multi-arch) or delegate to CI.
4. Deploy: `kubectl apply -k k8s/overlays/<env>` (usually via `../infra` GitOps pipeline).
5. Verify: `kubectl get pods -n ingestion` and confirm dashboards show new build tags.

### Backfill / Replay
- Airflow backfill: `airflow dags backfill <dag_id> --start-date <ISO> --end-date <ISO>`.
- SeaTunnel batch replay: `python scripts/simulate_events.py --connector=<name> --count=<n>` for dry-run before scheduling.
- Topic offset reset: `kafka-consumer-groups --bootstrap-server <broker> --reset-offsets --to-datetime <ISO> --group <consumer> --topic <topic>`.
- Coordinate with `../data-processing` before replaying to avoid double counting.

### Hotfix / Rollback
- Pause DAG: `airflow dags pause <dag_id>` to halt connector ingestion.
- Roll back deployment: `kubectl rollout undo deployment/<svc> -n ingestion`.
- Resume when error rate stabilises and backlog cleared (`airflow dags unpause <dag_id>`).

---

## Configuration

| Variable | Description | Default | Location |
|----------|-------------|---------|----------|
| `INGEST_ENV` | Environment label for logging/tracing | `local` | `.env` or deployment manifests |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka brokers for raw + normalized topics | `localhost:9092` | `configs/environments/*/env.example` |
| `SCHEMA_REGISTRY_URL` | Schema registry endpoint | `http://localhost:8081` | same as above |
| `CLICKHOUSE_DSN` | Bronze/Silver ClickHouse DSN | `clickhouse://default@localhost:9000/carbon_ingestion` | same as above |
| `REDIS_URL` | Redis cache for dedupe/windowing | `redis://localhost:6379/0` | same as above |
| `NORMALIZATION_CONFIG` | Normalization policy file | `configs/normalization_rules.yaml` | repo |
| `ENRICHMENT_CONFIG` | Enrichment taxonomy | `configs/enrichment_taxonomy.yaml` | repo |
| `OTLP_ENDPOINT` | OpenTelemetry collector endpoint | `http://localhost:4317` | same as above |

Sensitive values (API keys, secrets) must be provided via Kubernetes secrets or local `.env` overrides.

---

## Observability
- Metrics: `/metrics` endpoints scraped by Prometheus configuration in `configs/prometheus/`.
- Dashboards: `observability/dashboards/ingestion/` (`connectors_health.json`, `bronze_to_silver_latency.json`).
- Alerts: RED/SLO rules defined in `observability/alerts/RED/ingestion_red.yaml` and `observability/alerts/SLO/`.
- Traces: OTLP exporters send to Tempo/Jaeger; filter by `service.name=254carbon-ingestion-*`.
- Logs: Structured JSON via stdout; inspect with `kubectl logs -n ingestion <pod>` or future Loki stack.

---

## Troubleshooting

### Airflow Scheduler Stalled
1. `docker-compose -f docker-compose.airflow.yml ps` (local) or `kubectl get pods -l component=scheduler -n ingestion`.
2. Restart scheduler container/deployment.
3. Clear tasks: `airflow tasks clear <dag_id> <task_id> --start-date <ISO> --end-date <ISO>`.

### Schema Validation Failures
- `make validate-schemas` to reproduce locally.
- Compare payload with `events/avro/*.avsc`; update connector transforms or evolve schema via `../specs`.
- Regenerate connector metadata: `make connectors-index`.

### Kafka Backlog / Lag
- Review Grafana metric `connector_consumer_lag`.
- Scale worker: `kubectl scale deployment/service-normalization --replicas=2 -n ingestion`.
- If lag persists, inspect consumer logs and consider DLQ replay workflow.

### Missing Bronze Records in ClickHouse
- Check service logs: `kubectl logs deployment/service-normalization -n ingestion`.
- Health probe: `python scripts/local_run_normalization.sh --check`.
- Reapply DDL: `make pipeline-apply-ddl`.

---

## Reference
- `Makefile` – automation for setup, validation, and local services (`make help`).
- `scripts/` – includes schema validation, connector index generation, and event simulators.
- `configs/` – normalization/enrichment policies, observability configs, environment templates.
- `service-manifest.yaml` – metadata consumed by `../meta`.

For cross-cutting guidance (environment promotion, shared SLOs) rely on the [Platform Overview](../PLATFORM_OVERVIEW.md).
