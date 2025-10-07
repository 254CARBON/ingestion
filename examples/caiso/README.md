# CAISO Connector Examples

This directory contains examples and configurations for the CAISO connector, demonstrating how to extract and transform market data from CAISO's OASIS system.

## Files

- `basic_usage.py` - Basic CAISO connector usage example
- `batch_processing.py` - Batch processing example for historical data
- `caiso_config_basic.yaml` - Basic configuration for CAISO connector
- `caiso_config_advanced.yaml` - Advanced configuration with custom settings
- `caiso_config_production.yaml` - Production-ready configuration
- `error_handling.py` - Error handling and retry logic examples
- `integration_example.py` - End-to-end integration example
- `real_time_monitoring.py` - Real-time monitoring example
- `test_examples.py` - Test suite for all examples

## Quick Start

1. **Basic Usage**
   ```python
   from connectors.caiso import CAISOConnector, CAISOConnectorConfig
   
   config = CAISOConnectorConfig(
       name="caiso",
       market="CAISO",
       mode="batch",
       enabled=True,
       output_topic="ingestion.caiso.raw.v1",
       schema="schemas/raw_caiso_trade.avsc",
       default_node="TH_SP15_GEN-APND"
   )
   
   connector = CAISOConnector(config)
   
   # Extract data for a specific time window
   result = await connector.extract(
       start=datetime(2024, 1, 1),
       end=datetime(2024, 1, 2),
       market_run_id="DAM",
       node="TH_SP15_GEN-APND"
   )
   ```

2. **Configuration Files**
   - Use `caiso_config_basic.yaml` for development
   - Use `caiso_config_production.yaml` for production deployments
   - Customize settings in `caiso_config_advanced.yaml`

3. **Running Examples**
   ```bash
   # Run basic example
   python examples/caiso/basic_usage.py
   
   # Run batch processing
   python examples/caiso/batch_processing.py
   
   # Run integration test
   python examples/caiso/integration_example.py
   
   # Run all tests
   python examples/caiso/test_examples.py
   ```

## Configuration Options

### Basic Configuration
- `name`: Connector name
- `market`: Market identifier (CAISO)
- `mode`: Execution mode (batch, streaming, hybrid)
- `enabled`: Whether connector is enabled
- `output_topic`: Kafka topic for output
- `schema`: Path to Avro schema file

### CAISO-Specific Options
- `caiso_base_url`: OASIS API base URL
- `caiso_timeout`: Request timeout in seconds
- `caiso_rate_limit`: Rate limit per minute
- `default_market_run_id`: Default market run ID (DAM, RTM, etc.)
- `default_node`: Default node for queries

### Advanced Options
- `caiso_retry_attempts`: Number of retry attempts
- `caiso_backoff_factor`: Backoff factor for retries
- `oasis_version`: OASIS report version
- `oasis_result_format`: Result format (6=CSV in ZIP)

## Common CAISO Nodes

- `TH_SP15_GEN-APND` - SP15 Generation
- `TH_NP15_GEN-APND` - NP15 Generation
- `TH_ZP26_GEN-APND` - ZP26 Generation
- `TH_SP15_LOAD-APND` - SP15 Load
- `TH_NP15_LOAD-APND` - NP15 Load

## Market Run IDs

- `DAM` - Day Ahead Market
- `RTM` - Real Time Market
- `HASP` - Hour Ahead Scheduling Process
- `RUC` - Residual Unit Commitment

## Error Handling

The CAISO connector includes comprehensive error handling:

- **Network errors**: Automatic retry with exponential backoff
- **Rate limiting**: Respects CAISO rate limits
- **Data validation**: Validates extracted data against schema
- **Timeout handling**: Configurable timeouts for requests

## Monitoring

Monitor the CAISO connector using:

- **Health checks**: `/health/live` and `/health/ready` endpoints
- **Metrics**: Prometheus metrics at `/metrics` endpoint
- **Logs**: Structured logging with correlation IDs
- **Alerts**: Set up alerts for failed extractions

## Troubleshooting

### Common Issues

1. **Authentication Errors**
   - Verify CAISO API credentials
   - Check rate limiting settings

2. **Data Quality Issues**
   - Validate node names and market run IDs
   - Check time window parameters

3. **Performance Issues**
   - Adjust batch sizes
   - Monitor rate limits
   - Check network connectivity

### Debug Mode

Enable debug logging:
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

### Validation

Validate connector configuration:
```python
from connectors.caiso import CAISOConnectorConfig

config = CAISOConnectorConfig.from_yaml("caiso_config_basic.yaml")
print(config.validate())
```

## Support

For issues or questions:
- Check the troubleshooting guide
- Review example configurations
- Run test examples
- Contact the platform team