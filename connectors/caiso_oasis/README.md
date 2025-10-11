# CAISO OASIS Connector

CAISO OASIS connector for ingesting DAM/FMM/RTM LMP, Ancillary Services, and CRR data from CAISO's OASIS SingleZip API.

## Overview

This connector extracts real-time and historical market data from CAISO's OASIS (Open Access Same-Time Information System) using the SingleZip API endpoint. It publishes raw data to Bronze Kafka topics for downstream processing.

## Supported Data Types

- **LMP (Locational Marginal Prices)**
  - `lmp_dam` - Day-Ahead Market LMP
  - `lmp_fmm` - Fifteen-Minute Market LMP
  - `lmp_rtm` - Real-Time Market LMP

- **Ancillary Services**
  - `as_dam` - Day-Ahead Market Ancillary Services
  - `as_fmm` - Fifteen-Minute Market Ancillary Services
  - `as_rtm` - Real-Time Market Ancillary Services

- **Congestion Revenue Rights**
  - `crr` - CRR auction results and allocations

## Architecture

```
CAISO OASIS API → OASISClient → Extractor → Transform → Bronze Topics
```

### Components

1. **OASISClient** - HTTP client for CAISO OASIS SingleZip API
2. **Extractor** - Data extraction logic for different data types
3. **Connector** - Main connector coordinating extraction and publishing
4. **Transform** - Minimal transformation for Bronze publishing

## Configuration

### Environment Variables

```bash
# CAISO OASIS API
CAISO_OASIS_BASE_URL=https://oasis.caiso.com/oasisapi
CAISO_OASIS_TIMEOUT=60

# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
SCHEMA_REGISTRY_URL=http://localhost:8081
```

### Connector Configuration

```yaml
name: "caiso_oasis"
market: "CAISO"
mode: "batch"
schedule: "0 */1 * * *"  # Hourly runs
enabled: true

# Output topics
output_topics:
  lmp_dam: "ingestion.market.caiso.lmp.dam.raw.v1"
  lmp_fmm: "ingestion.market.caiso.lmp.fmm.raw.v1"
  lmp_rtm: "ingestion.market.caiso.lmp.rtm.raw.v1"
  as_dam: "ingestion.market.caiso.as.dam.raw.v1"
  as_fmm: "ingestion.market.caiso.as.fmm.raw.v1"
  as_rtm: "ingestion.market.caiso.as.rtm.raw.v1"
  crr: "ingestion.market.caiso.crr.raw.v1"

# Schedule overrides per dataset
dataset_schedules:
  lmp_dam: "0 * * * *"          # Hourly DAM LMP
  lmp_fmm: "*/15 * * * *"       # 15-minute FMM LMP
  lmp_rtm: "*/5 * * * *"        # 5-minute RTM LMP
  as_dam: "0 * * * *"           # Hourly DAM AS
  as_fmm: "*/15 * * * *"        # 15-minute FMM AS
  as_rtm: "*/5 * * * *"         # 5-minute RTM AS
  crr: "0 4 * * *"              # Daily CRR data
```

## Usage

### Basic Usage

```python
from ingestion.connectors.caiso_oasis import CAISOOASISConnector, CAISOASISConnectorConfig

config = CAISOASISConnectorConfig()
connector = CAISOOASISConnector(config)

# Extract LMP data for a specific time range
result = await connector.extract(
    data_type="lmp_dam",
    start="2024-01-01T00:00:00Z",
    end="2024-01-01T01:00:00Z",
    node="TH_NP15_GEN-APND"
)

# Transform for Bronze publishing
transformed = await connector.transform(result)

# Get output topic for publishing
topic = connector.get_output_topic("lmp_dam")
```

### Batch Processing

```python
# Run batch extraction for multiple data types
results = await connector.run_batch(
    start_date="2024-01-01",
    end_date="2024-01-02"
)
```

### Real-time Processing

```python
# Run real-time extraction (uses current time)
await connector.run_realtime()
```

## Data Flow

1. **Extraction**: Query CAISO OASIS SingleZip API for raw CSV data
2. **Parsing**: Parse ZIP-compressed CSV responses
3. **Transformation**: Add Bronze metadata and validate structure
4. **Publishing**: Send to Kafka Bronze topics for downstream processing

## Bronze Topics

The connector publishes to the following Kafka topics:

- `ingestion.market.caiso.lmp.dam.raw.v1` - DAM LMP data
- `ingestion.market.caiso.lmp.fmm.raw.v1` - FMM LMP data
- `ingestion.market.caiso.lmp.rtm.raw.v1` - RTM LMP data
- `ingestion.market.caiso.as.dam.raw.v1` - DAM AS data
- `ingestion.market.caiso.as.fmm.raw.v1` - FMM AS data
- `ingestion.market.caiso.as.rtm.raw.v1` - RTM AS data
- `ingestion.market.caiso.crr.raw.v1` - CRR data

## Error Handling

- **Rate Limiting**: Respects CAISO API rate limits (50 requests/minute)
- **Retry Logic**: Exponential backoff for transient failures
- **Circuit Breaker**: Protects against sustained API failures
- **Data Validation**: Schema validation before publishing

## Monitoring

The connector provides comprehensive metrics:

- Request success/failure rates
- Data extraction volumes
- Processing latencies
- Health status indicators

## Dependencies

- Python 3.11+
- httpx (async HTTP client)
- structlog (structured logging)
- Base connector framework
