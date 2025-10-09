## Exemplar Raw Ingestion Pipeline

This directory contains a runnable example that exercises the ingestion connectors
and publishes bronze-stage (raw) events into Kafka.

### Files

- `raw_pipeline.py` – orchestrator script that loads connector configuration, performs extraction + transformation, and publishes raw events to Kafka topics.
- `raw_pipeline_config.yaml` – sample configuration wired to the CAISO example connector with local Kafka endpoints.

### Quick Start (recommended)

The easiest way to exercise the full Bronze → Silver → Gold path is to rely on the
Make targets introduced in the repository root:

```bash
# 1. Bootstrap infra, load ClickHouse schemas, and launch normalization/enrichment/aggregation
make pipeline-bootstrap

# 2. Publish exemplar CAISO data through the raw → normalized → enriched → aggregated pipeline
make pipeline-run

# 3. Check container health or tail logs as needed
make pipeline-status
```

### Manual execution

```bash
# Use the provided configuration and run all connectors (CAISO by default)
python examples/pipeline/raw_pipeline.py --config examples/pipeline/raw_pipeline_config.yaml

# Skip Kafka publishing (extract + transform only)
python examples/pipeline/raw_pipeline.py --dry-run

# Run a specific connector entry by name
python examples/pipeline/raw_pipeline.py --connector caiso_exemplar
```

The script prints a JSON summary for each connector run that includes record counts,
output topic, load success flags, and any error information.

> **Tip:** ClickHouse tables for the exemplar pipeline can be (re)applied at any time via
> `python scripts/apply_clickhouse_schemas.py`, which posts the Bronze/Silver/Gold DDLs to
> the local ClickHouse instance (defaults to `localhost:8123`).
