# Ingestion Layer (254carbon-ingestion)

> Unified ingestion, connector, and early data processing platform for multi‑market energy & commodities data powering the 254Carbon ecosystem.

***Carbon platform engineered by 254TRADING in Texas***

---

## Table of Contents
- [Purpose](#purpose)
- [Scope & Non-Goals](#scope--non-goals)
- [High-Level Flow](#high-level-flow)
- [Repository Layout](#repository-layout)
- [Data Lifecycle (Bronze → Silver → Gold → Served)](#data-lifecycle-bronze--silver--gold--served)
- [Connectors](#connectors)
  - [Connector Metadata Spec](#connector-metadata-spec)
  - [Directory Structure](#directory-structure)
  - [Execution Modes](#execution-modes)
  - [Versioning & Compatibility](#versioning--compatibility)
- [Airflow Layer](#airflow-layer)
  - [Dynamic DAG Generation](#dynamic-dag-generation)
  - [DAG Naming Convention](#dag-naming-convention)
  - [Local Airflow Development](#local-airflow-development)
- [SeaTunnel Pipelines](#seatunnel-pipelines)
  - [Config Conventions](#config-conventions)
  - [Runtime Variables](#runtime-variables)
- [Services Included (Deployables)](#services-included-deployables)
- [Events & Topics](#events--topics)
- [Schemas & Contracts](#schemas--contracts)
- [Environment & Configuration](#environment--configuration)
- [Multi-Architecture & Heterogeneous Cluster Notes](#multi-architecture--heterogeneous-cluster-notes)
- [Local Development Quick Start](#local-development-quick-start)
- [Testing Strategy](#testing-strategy)
- [CI/CD Overview](#cicd-overview)
- [Telemetry & Observability](#telemetry--observability)
- [Security & Access Control](#security--access-control)
- [Performance & Scaling Guidelines](#performance--scaling-guidelines)
- [Troubleshooting](#troubleshooting)
- [Roadmap](#roadmap)
- [Glossary](#glossary)
- [License](#license)

---

## Purpose

This repository centralizes the ingestion plane for 254Carbon:

- **Acquisition** of raw market data from multiple ISOs / exchanges via batch & streaming connectors.
- **Standardization** of incoming payloads into consistent normalized schemas.
- **Enrichment** and early **aggregation** before downstream analytical & serving layers.
- **Publishing** domain events (Avro/Protobuf) into Kafka with schema governance.
- **Quality Measurements** and metrics export for operations and data reliability.

---

## Scope & Non-Goals

| In Scope | Out of Scope |
|----------|--------------|
| Market connectors (MISO, CAISO, etc.) | Business REST APIs (handled by gateway repo) |
| Airflow DAG orchestration | User entitlements & auth |
| SeaTunnel job definitions | Long-running analytical simulations (analytics repo) |
| Normalization / enrichment / early aggregation | Complex ML feature engineering (ml repo) |
| Data quality validation & metrics | Final curve delivery or pricing model serving |
| Event publishing to Kafka | Front-end delivery of data |

---

## High-Level Flow

```
        +-------------+        +------------------+        +------------------+
        |  Connector  |  RAW   | Normalization Svc| SILVER | Enrichment Svc   |
        |  (Extract)  | -----> | (Schema / Types) | -----> | (Taxonomy / Meta)|
        +------+------+        +---------+--------+        +---------+--------+
               |                          |                           |
               | RAW EVENTS               | NORMALIZED EVENTS         | ENRICHED EVENTS
               v                          v                           v
          Kafka Topic               Kafka Topic                 Kafka Topic
        ingestion.*.raw.v1       normalized.market.*.v1      enriched.market.*.v1
               |                          |                           |
               |                          v                           v
               |                    Aggregation Svc  ----> Projection Service
               |                          |             (ClickHouse + Redis)
               |                          v                           v
               |                    AGGREGATED EVENTS       SERVING LAYER
               |                          |                    (Optimized Queries)
               v                          v                           v
          Kafka Topic               Kafka Topic                 ClickHouse
        aggregation.*.v1         projection.*.v1             Gold Tables
               |                          |                           |
               |                          +---------------------------+
               |                                    |
               +------------------------------------+
                              |
                              v
                       Data Quality Metrics → Metrics Service
```

Storage progression:
- **Bronze**: Raw ingest (schema lenient)
- **Silver**: Normalized typed canonical rows
- **Gold**: Enriched + aggregated (bars, curves pre-stage)
- **Served**: Projection tables for low-latency queries

---

## Repository Layout

```
.
├─ airflow/
│  ├─ dags/
│  │  ├─ dynamic_connectors_loader.py
│  │  ├─ market_miso_dag.py
│  │  └─ market_caiso_dag.py
│  ├─ plugins/
│  └─ airflow_settings.yaml
├─ seatunnel/
│  └─ jobs/
│     ├─ miso_realtime.conf
│     └─ caiso_daily.conf
├─ connectors/
│  ├─ base/
│  │  ├─ base_connector.py
│  │  └─ contract_validator.py
│  ├─ miso/
│  │  ├─ connector.yaml
│  │  ├─ extractor.py
│  │  ├─ transform.py
│  │  └─ schemas/
│  │     └─ raw_miso_trade.avsc
│  └─ caiso/
├─ services/
│  ├─ service-connector-registry/
│  ├─ service-normalization/
│  ├─ service-enrichment/
│  ├─ service-aggregation/
│  └─ service-projection/
├─ events/
│  ├─ avro/
│  │  └─ normalized.market.ticks.v1.avsc
│  └─ proto/
├─ scripts/
│  ├─ generate_connectors_index.py
│  ├─ validate_schemas.sh
│  └─ local_run_normalization.sh
├─ tests/
│  ├─ unit/
│  ├─ integration/
│  └─ data/
├─ configs/
│  ├─ logging.yaml
│  ├─ normalization_rules.yaml
│  ├─ enrichment_taxonomy.yaml
│  ├─ aggregation_policies.yaml
│  └─ projection_policies.yaml
├─ specs.lock.json
├─ service-manifest.yaml          (for each service subfolder too)
└─ README.md
```

---

## Data Lifecycle (Bronze → Silver → Gold → Served)

| Stage | Location | Characteristics | Mutation |
|-------|----------|-----------------|----------|
| Bronze | Raw topics (ingestion.*.raw.v1) + raw tables | Loose schema, possibly null-heavy | Append-only |
| Silver | Normalized topics / tables | Strong types, canonical keys | Append-only |
| Gold | Enriched + aggregated tables | Derived metrics, taxonomy applied | Recomputed; idempotent |
| Served | Projection / materialized views (outside this repo) | Query-optimized, low latency | Incremental refresh |

Reprocessing:
- Always rebuild downstream from earliest needed stage using *idempotent transformations*.
- Use `reprocess_window` parameter (timestamp range) in normalization & enrichment services.

---

## Connectors

### Connector Metadata Spec

`connector.yaml` example:

```yaml
name: miso
version: 1.2.0
market: MISO
mode: batch            # batch | streaming
schedule: "0 */1 * * *"
enabled: true
output_topic: ingestion.miso.raw.v1
schema: schemas/raw_miso_trade.avsc
retries: 3
backoff_seconds: 30
tenant_strategy: single
transforms:
  - name: sanitize_numeric
  - name: standardize_timezone
owner: platform
```

### Directory Structure

```
connectors/
  base/
  <market>/
    connector.yaml
    extractor.py
    transform.py
    schemas/
      raw_<market>_<entity>.avsc
    tests/
```

### Execution Modes
- `batch`: Airflow scheduled
- `streaming`: SeaTunnel continuous job or WebSocket client
- `hybrid`: batch snapshot + incremental polling

### Versioning & Compatibility
| Change Type | Action |
|-------------|--------|
| Backward-compatible field addition | Minor bump |
| Field removal / rename | Major bump + new schema version |
| Schedule change | Patch |
| New entity | Add new schema file |

---

## Airflow Layer

### Dynamic DAG Generation
- Script `generate_connectors_index.py` aggregates all `connector.yaml` into `connectors_index.json`.
- `dynamic_connectors_loader.py` reads index and auto-creates DAG per connector.
- Feature toggles: disabling a connector flips `enabled: false`.

### DAG Naming Convention
`ingest_<market>_<mode>` (e.g., `ingest_miso_batch`)

### Local Airflow Development

```bash
# From repo root
make airflow-up
# Open UI: http://localhost:8080 (default creds in docs/local-env.md)
```

---

## SeaTunnel Pipelines

### Config Conventions

A job file example snippet (`seatunnel/jobs/miso_realtime.conf`):

```
env {
  parallelism = 2
  job.mode = "STREAMING"
}

source {
  MISO_Connector {
    market = "MISO"
    endpoint = "${MISO_API_URL}"
    authToken = "${MISO_API_TOKEN}"
  }
}

transform {
  JsonFlattener {}
  TimezoneStandardizer { target_tz = "UTC" }
}

sink {
  Kafka {
    topic = "ingestion.miso.raw.v1"
    bootstrapServers = "${KAFKA_BOOTSTRAP}"
    format = "json"
  }
}
```

### Runtime Variables
Provided via environment or mounted config:
- `MISO_API_URL`, `MISO_API_TOKEN`, `KAFKA_BOOTSTRAP`
- Standard fallback defaults in `configs/seatunnel.env.example`

---

## Services Included (Deployables)

| Service | Port | Purpose |
|---------|------|---------|
| connector-registry | 8500 | Exposes connector metadata, health, version index |
| normalization | 8510 | Converts raw → normalized (Silver) |
| enrichment | 8511 | Adds taxonomy, semantic tags |
| aggregation | 8512 | OHLC bars, rolling metrics, curve pre-stage |
| projection | 8513 | Builds serving layer output tables (optional early subset) |

Each service has:
- `/health` (liveness/readiness)
- `/metrics` (Prometheus)
- Structured JSON logging
- Optional `/reprocess` endpoint (POST) with time window

---

## Events & Topics

| Topic | Producer | Consumer(s) | Schema | Notes |
|-------|----------|-------------|--------|-------|
| ingestion.miso.raw.v1 | Connector (MISO) | Normalization | raw_miso_trade.avsc | Bronze |
| ingestion.caiso.raw.v1 | Connector (CAISO) | Normalization | raw_caiso_trade.avsc | Bronze |
| normalized.market.ticks.v1 | Normalization | Enrichment, Analytics | normalized_tick.avsc | Silver |
| enriched.market.ticks.v1 | Enrichment | Aggregation | enriched_tick.avsc | Gold candidate |
| aggregation.ohlc.bars.v1 | Aggregation | Projection | ohlc_bar.avsc | OHLC aggregations |
| aggregation.rolling.metrics.v1 | Aggregation | Projection | rolling_metric.avsc | Rolling metrics |
| aggregation.curve.prestage.v1 | Aggregation | Projection | curve_prestage.avsc | Curve pre-stage |
| pricing.curve.updates.v1 | Aggregation | Gateway/Streaming (other repo) | curve_update.avsc | Derivative |
| data.quality.anomalies.v1 | Normalization/Enrichment | Metrics Service | dq_anomaly.avsc | Observability |

Envelope fields (common):
```
event_id (uuid), trace_id, occurred_at (timestamp micros),
tenant_id, schema_version, producer, payload{...}
```

---

## Schemas & Contracts

Schemas live in `events/avro` or `events/proto`.
Validation:
```bash
make validate-schemas
```
Compatibility policy: **BACKWARD** (additive allowed). Breaking changes → new versioned topic suffix (`*.v2`).

---

## Environment & Configuration

| Variable | Description | Default/Example |
|----------|-------------|-----------------|
| KAFKA_BOOTSTRAP | Kafka bootstrap servers | localhost:9092 |
| CLICKHOUSE_DSN | ClickHouse connect string | clickhouse://user:pass@host:9000 |
| REDIS_URL | Redis cache (optional) | redis://localhost:6379/0 |
| AIRFLOW__CORE__FERNET_KEY | Airflow encryption key | (generate) |
| NORMALIZATION_RULES_PATH | Path to normalization rules | configs/normalization_rules.yaml |
| ENRICHMENT_TAXONOMY_PATH | Taxonomy mapping file | configs/enrichment_taxonomy.yaml |
| AGGREGATION_POLICIES_PATH | Aggregation rules | configs/aggregation_policies.yaml |

See `configs/.env.example` (create `.env` for local runs).

---

## Multi-Architecture & Heterogeneous Cluster Notes

- Multi-arch build (amd64 + arm64) via `docker buildx bake`.
- GPU nodes (ARM Macs) reserved for future heavy transformations (embedding pre-stage) — currently optional.
- Affinity & tolerations recommended in Helm charts:
  - `nodeArch=arm64` vs `nodeArch=amd64`.
  
---

## Local Development Quick Start

### Prerequisites
- Docker / Colima / Rancher Desktop
- Python 3.12+
- `make`, `jq`

### Steps

```bash
# 1. Install Python deps for shared tooling
make setup

# 2. Spin up local infra (optional compose variant)
make infra-up

# 3. Generate connector index
make connectors-index

# 4. Run normalization service locally
make run-normalization

# 5. Trigger sample ingestion test (synthetic)
make simulate-miso
```

### Common Make Targets
| Command | Action |
|---------|--------|
| `make setup` | Install dev dependencies |
| `make lint` | Lint & format check |
| `make test` | Run unit + integration tests |
| `make build` | Multi-arch container build |
| `make validate-schemas` | Schema compatibility |
| `make connectors-index` | Aggregate connectors metadata |
| `make airflow-up` | Launch local Airflow |
| `make infra-up` | Start Kafka/ClickHouse/Redis via compose |

---

## Testing Strategy

| Layer | Tests |
|-------|-------|
| Connector | Unit (extract/transform), contract tests (validate sample payload vs schema) |
| Normalization | Transformation golden files, schema conformance, nullability edge cases |
| Enrichment | Taxonomy coverage, tag logic AB tests |
| Aggregation | Idempotency, window boundary correctness, reprocess accuracy |
| Events | Schema evolution tests (compatibility) |
| E2E | Synthetic “inject raw → consume aggregated result” harness |

Golden datasets placed under `tests/data/golden/<stage>/`.

---

## CI/CD Overview

Pipeline stages:
1. Lint (flake8/ruff + formatting)
2. Unit Tests
3. Schema Validation
4. Build Multi-Arch Image
5. SBOM Generation (CycloneDX)
6. Vulnerability Scan (Trivy/Grype)
7. Publish (if on main/tag)
8. Canary Deploy (optional local env automation)

Artifacts:
- `dist/` packages for internal libs
- `sbom.spdx.json`
- Coverage report `coverage.xml`

---

## Telemetry & Observability

Metrics (Prometheus):
- `connector_runs_total{connector="",status=""}`
- `normalization_records_processed_total`
- `normalization_latency_seconds_bucket`
- `enrichment_failures_total`
- `aggregation_recompute_duration_seconds`
- `data_quality_anomalies_total{type=""}`

Tracing (OpenTelemetry):
- Trace IDs propagate via event headers (`trace_id`)
- Span attributes: `stage=normalization|enrichment|aggregation`

Structured Logs:
```json
{
  "ts":"2025-10-05T19:12:33Z",
  "level":"INFO",
  "service":"normalization",
  "connector":"miso",
  "records":2500,
  "trace_id":"..."
}
```

---

## Security & Access Control

| Aspect | Approach |
|--------|---------|
| Network | Services internal only; no direct internet exposure (Ingress sits elsewhere) |
| Auth | Service-to-service (future mTLS / API tokens) |
| Secrets | `.env` for local → sealed/secret manager in future |
| Data Integrity | Checksums for raw file pulls (when applicable) |
| Multi-Tenancy (soft) | `tenant_id` required in all emitted events & table rows |

No PII expected; encryption at rest optional in current local mode.

---

## Performance & Scaling Guidelines

| Component | Guidance |
|-----------|----------|
| Connectors | Keep per-run < 5 min; chunk large pulls |
| Normalization | Aim P95 transform latency < 500ms per batch segment |
| Enrichment | Cache taxonomy in-memory w/ reload TTL |
| Aggregation | Prefer incremental updates vs full rebuild |
| Reprocessing | Use bounded window and back-pressure Kafka consumer if backlog builds |

Batch Parallelism:
- Configurable `PARALLEL_WORKERS` env for normalization/enrichment.

---

## Troubleshooting

| Symptom | Check | Command |
|---------|-------|---------|
| No normalized events | Connector output? Schema errors? | `kubectl logs svc/normalization` |
| High latency normalization | CPU saturation? | `kubectl top pods -l app=normalization` |
| Schema compatibility failures | Changed field type? | `make validate-schemas` |
| Airflow DAG missing | Index not regenerated | `make connectors-index` |
| Aggregation drift | Missing reprocess run | POST /aggregation/reprocess |

Local Kafka topic listing:
```bash
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092
```

---

## Roadmap

| Phase | Item | Status |
|-------|------|--------|
| 1 | Dynamic DAG loader baseline | ✅ |
| 1 | Normalization service + Avro schemas | ✅ |
| 2 | Enrichment taxonomy v1 | ✅ |
| 2 | Aggregation incremental bars | ✅ |
| 2 | Projection/serving layer | ✅ |
| 3 | Data quality anomaly event stream | ✅ |
| 3 | Reprocessing API (normalization/enrichment/aggregation) | ✅ |
| 3 | Operational tooling (CLI, dashboards, deployment scripts) | ✅ |
| 4 | Streaming connector support (websocket watch) | Planned |
| 4 | Backfill orchestration improvements | Planned |
| 5 | Full OTel tracing coverage | In Progress |
| 5 | mTLS between services | Planned |

---

## Glossary

| Term | Definition |
|------|------------|
| Bronze | Raw, minimally processed data store/event |
| Silver | Canonical normalized typed dataset |
| Gold | Enriched + aggregated dataset |
| Served | Specialized query-optimized derivative |
| Connector Registry | Service exposing connector metadata & health |
| Reprocessing | Replay transformation for a historical time window |
| Taxonomy | Controlled vocabulary describing instruments & categories |

---

## License

```text name=LICENSE
Copyright (c) 2025 254TRADING. All Rights Reserved.

This software and associated documentation (the “Software”) are the confidential and proprietary information of 254TRADING (“Company”). 
Unauthorized copying, distribution, modification, public display, reverse engineering, or reuse of any portion of the Software is strictly prohibited unless expressly permitted in a written agreement executed by Company.

No license or other rights (express, implied, by estoppel, or otherwise) are granted to any party unless a separate written agreement exists.

THE SOFTWARE IS PROVIDED “AS IS” WITHOUT WARRANTY OF ANY KIND, INCLUDING ANY IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, TITLE, OR NON-INFRINGEMENT. IN NO EVENT SHALL COMPANY BE LIABLE FOR ANY INDIRECT, SPECIAL, INCIDENTAL, CONSEQUENTIAL, OR EXEMPLARY DAMAGES.

For licensing inquiries contact: legal@254carbon.com
```

---

## Contact / Ownership

- Owner: Platform / Ingestion Domain
- Escalation: Open issue with label `ingestion` (if in multi-repo environment, reference service name)
- AI Agent Context: See `service-manifest.yaml` & `context.yaml` (if present)

---

## Quick Commands Recap

```bash
# Build & validate
make lint test validate-schemas build

# Generate connector index
make connectors-index

# Run normalization locally (dev mode)
make run-normalization

# Launch Airflow (local)
make airflow-up

# Simulate raw event publish
make simulate-miso
```

---

_This README is the canonical ingestion reference. Update alongside schema or pipeline changes. Keep transformation invariants and lifecycle behavior clearly documented to enable safe automated reasoning by AI agents._
