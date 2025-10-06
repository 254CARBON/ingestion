# CAISO Connector Examples

This directory contains comprehensive examples for using the CAISO connector in the 254Carbon Ingestion Platform.

## Overview

The CAISO (California Independent System Operator) connector extracts market data from the OASIS (Open Access Same-Time Information System) platform. It handles:

- **PRC_LMP data** (Price, Quantity, and other market data)
- **Real-time and historical data** extraction
- **CSV-in-ZIP file processing** from OASIS endpoints
- **Rate limiting and respectful scraping** practices

## Files in this Directory

### Configuration Examples

- `caiso_config_basic.yaml` - Basic configuration for simple use cases
- `caiso_config_advanced.yaml` - Advanced configuration with all options
- `caiso_config_production.yaml` - Production-ready configuration

### Usage Examples

- `basic_usage.py` - Simple extraction example
- `batch_processing.py` - Batch processing with date ranges
- `real_time_monitoring.py` - Real-time data monitoring
- `error_handling.py` - Error handling and retry examples
- `integration_example.py` - Integration with normalization service

### Sample Data

- `sample_raw_data.json` - Example raw CAISO data
- `sample_normalized_data.json` - Example normalized output
- `sample_enriched_data.json` - Example enriched data

## Quick Start

### 1. Basic Configuration

```yaml
# caiso_config_basic.yaml
name: caiso_example
version: 1.0.0
market: CAISO
mode: batch
schedule: "0 */2 * * *"  # Every 2 hours
enabled: true
output_topic: ingestion.caiso.raw.v1
schema: schemas/raw_caiso_trade.avsc
retries: 3
backoff_seconds: 30
tenant_strategy: single
transforms:
  - sanitize_numeric
  - standardize_timezone
owner: platform
description: "Basic CAISO connector example"
tags:
  - market-data
  - energy
  - california

# CAISO specific settings
caiso_base_url: "https://oasis.caiso.com/oasisapi"
caiso_timeout: 30
caiso_rate_limit: 100
caiso_retry_attempts: 3
caiso_backoff_factor: 2.0
caiso_user_agent: "254Carbon/1.0"
oasis_version: "12"
oasis_result_format: "6"
default_market_run_id: "DAM"
default_node: "TH_SP15_GEN-APND"
```

### 2. Basic Usage

```python
# basic_usage.py
import asyncio
from datetime import datetime, timezone
from connectors.caiso.connector import CAISOConnector
from connectors.caiso.config import CAISOConnectorConfig

async def main():
    # Create configuration
    config = CAISOConnectorConfig(
        name="caiso_basic",
        version="1.0.0",
        market="CAISO",
        mode="batch",
        output_topic="ingestion.caiso.raw.v1",
        schema="schemas/raw_caiso_trade.avsc"
    )

    # Create and initialize connector
    connector = CAISOConnector(config)

    # Extract data for a specific time range and node
    result = await connector.extract(
        start="2024-01-01T00:00:00",
        end="2024-01-01T01:00:00",
        market_run_id="DAM",
        node="TH_SP15_GEN-APND"
    )

    print(f"Extracted {result.record_count} records")
    print(f"Sample record: {result.data[0] if result.data else 'No data'}")

if __name__ == "__main__":
    asyncio.run(main())
```

## Configuration Options

### Core Connector Settings

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | string | Required | Connector name |
| `version` | string | Required | Connector version |
| `market` | string | Required | Market identifier ("CAISO") |
| `mode` | string | "batch" | Execution mode: batch/realtime/hybrid |
| `schedule` | string | None | Cron schedule for batch mode |
| `enabled` | boolean | true | Whether connector is enabled |
| `output_topic` | string | Required | Kafka topic for output |
| `schema` | string | Required | Path to Avro schema file |

### CAISO-Specific Settings

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `caiso_base_url` | string | "https://oasis.caiso.com/oasisapi" | OASIS API base URL |
| `caiso_timeout` | int | 30 | Request timeout in seconds |
| `caiso_rate_limit` | int | 100 | Rate limit per minute |
| `caiso_retry_attempts` | int | 3 | Number of retry attempts |
| `caiso_backoff_factor` | float | 2.0 | Exponential backoff factor |
| `caiso_user_agent` | string | "254Carbon/1.0" | User agent for requests |
| `oasis_version` | string | "12" | OASIS report version |
| `oasis_result_format` | string | "6" | Result format (6=CSV in ZIP) |
| `default_market_run_id` | string | "DAM" | Default market run ID |
| `default_node` | string | None | Default node for queries |

## Usage Patterns

### Batch Processing

```python
# Extract historical data for a date range
result = await connector.extract(
    start="2024-01-01T00:00:00",
    end="2024-01-01T23:59:59",
    market_run_id="DAM",
    node="TH_SP15_GEN-APND"
)
```

### Real-Time Monitoring

```python
# Extract latest available data
result = await connector.extract(
    market_run_id="RTM",
    node="TH_SP15_GEN-APND"
    # Uses current time window automatically
)
```

### Custom Time Windows

```python
# Extract data for specific hours
result = await connector.extract(
    start="2024-01-01T08:00:00",  # 8 AM
    end="2024-01-01T18:00:00",    # 6 PM
    market_run_id="DAM",
    node="TH_SP15_GEN-APND"
)
```

## Error Handling

The connector includes comprehensive error handling:

```python
try:
    result = await connector.extract(...)
except ExtractionError as e:
    print(f"Extraction failed: {e}")
except NetworkError as e:
    print(f"Network error: {e}")
except ValidationError as e:
    print(f"Data validation error: {e}")
```

## Integration Examples

### With Normalization Service

```python
# Extract and normalize data in one pipeline
extraction_result = await connector.extract(...)
normalized_result = await normalization_service.normalize_batch(
    extraction_result.data
)
```

### With Kafka Publishing

```python
# Extract data and publish to Kafka
result = await connector.extract(...)
for record in result.data:
    await kafka_producer.send(
        topic=connector.config.output_topic,
        value=record,
        key=record["event_id"]
    )
```

## Troubleshooting

### Common Issues

1. **404 Errors**: Check node names and ensure they exist in CAISO system
2. **Rate Limiting**: Respect the configured rate limits
3. **Timeout Errors**: Increase `caiso_timeout` for large data requests
4. **Data Format Issues**: Ensure OASIS version and result format are correct

### Debugging

Enable debug logging:

```python
import logging
logging.getLogger('connectors.caiso').setLevel(logging.DEBUG)
```

## Sample Data Structure

### Raw CAISO Data Format

```json
{
  "INTERVALSTARTTIME_GMT": "2024-01-01T00:00:00",
  "NODE": "TH_SP15_GEN-APND",
  "LMP_TYPE": "LMP",
  "MW": "100.0",
  "VALUE": "50.25",
  "OPR_INTERVAL": "1",
  "MARKET_RUN_ID": "DAM",
  "XML_DATA_ITEM": "PRC_LMP",
  "PNODE_RESMRID": "TH_SP15_GEN-APND"
}
```

### Normalized Output Format

```json
{
  "event_id": "uuid-string",
  "occurred_at": 1704067200000000,
  "tenant_id": "default",
  "schema_version": "1.0.0",
  "producer": "caiso-connector",
  "market": "CAISO",
  "node": "TH_SP15_GEN-APND",
  "lmp_type": "LMP",
  "price": 50.25,
  "quantity": 100.0,
  "market_run_id": "DAM",
  "interval_start": "2024-01-01T00:00:00Z",
  "extraction_metadata": {
    "extraction_time": "2024-01-01T00:00:00Z",
    "source": "oasis.caiso.com",
    "version": "1.0.0"
  }
}
```

## Performance Considerations

- **Rate Limiting**: Default 100 requests/minute - adjust based on CAISO limits
- **Timeout Settings**: 30 seconds default - increase for large historical requests
- **Concurrent Requests**: Limited to 5 to avoid overwhelming the CAISO API
- **Data Volume**: Historical requests may return large ZIP files - consider chunking

## Best Practices

1. **Use Descriptive Node Names**: Include location and type (e.g., "TH_SP15_GEN-APND")
2. **Respect Rate Limits**: Don't exceed CAISO's acceptable usage policies
3. **Handle Time Zones**: CAISO data is in Pacific Time - ensure proper conversion
4. **Validate Data**: Always validate extracted data before processing
5. **Monitor Errors**: Set up alerts for extraction failures
6. **Test with Small Windows**: Start with small time windows for testing

## Support

For issues or questions:
- Check the troubleshooting section above
- Review CAISO OASIS API documentation
- Monitor connector logs for detailed error information
- Contact the platform team for configuration assistance
