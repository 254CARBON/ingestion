# Cross-Cutting Data Loaders

Cross-cutting data loaders for EIA, EPA CEMS, CARB Cap-and-Trade, CEC demand forecasts, and NOAA hydro/climate data.

## Overview

These connectors provide comprehensive data coverage for California/WECC energy system modeling:

- **EIA 860/861/923**: Generator, utility, and fuel consumption data
- **EPA CEMS**: Hourly emissions data from power plants
- **CARB Cap-and-Trade**: Carbon allowance prices and compliance data
- **CEC Demand**: California electricity demand forecasts and electrification data
- **NOAA Hydro**: Precipitation, temperature, streamflow, and snowpack data

## Architecture

```
External APIs → SharedExtractor → Bronze Topics → Silver/Gold Processing
```

### Components

1. **SharedExtractor** - Common extraction logic for cross-cutting sources
2. **Individual Connectors** - Specialized connectors per data source
3. **Bronze Topics** - Raw data publishing for downstream processing

## Data Sources

### EIA 860/861/923
- **EIA 860**: Generator and utility data (nameplate capacity, fuel type, technology)
- **EIA 861**: Utility sales and revenue data
- **EIA 923**: Fuel consumption and generation data

**Bronze Topics:**
- `ingestion.reference.eia.860.raw.v1`
- `ingestion.reference.eia.861.raw.v1`
- `ingestion.reference.eia.923.raw.v1`

### EPA CEMS
- **Hourly Emissions**: CO2, NOx, SO2 mass emissions and heat input
- **Daily Aggregated**: Daily emissions summaries

**Bronze Topics:**
- `ingestion.emissions.epa.cems.hourly.raw.v1`
- `ingestion.emissions.epa.cems.daily.raw.v1`

### CARB Cap-and-Trade
- **Auction Results**: Cap-and-trade auction clearing prices and volumes
- **Allowance Prices**: Current market prices for carbon allowances
- **Compliance Data**: Entity compliance and surrender data

**Bronze Topics:**
- `ingestion.carbon.carb.auction_results.raw.v1`
- `ingestion.carbon.carb.allowance_prices.raw.v1`
- `ingestion.carbon.carb.compliance_data.raw.v1`

### CEC Demand
- **Demand Forecasts**: California electricity demand projections by scenario
- **Load Profiles**: Hourly load profiles by sector (residential, commercial, industrial)
- **Electrification**: Electrification adoption rates and impacts

**Bronze Topics:**
- `ingestion.demand.cec.forecast.raw.v1`
- `ingestion.demand.cec.load_profiles.raw.v1`
- `ingestion.demand.cec.electrification.raw.v1`

### NOAA Hydro
- **Precipitation**: Daily precipitation data by weather station
- **Temperature**: Daily temperature data (max, min, average)
- **Streamflow**: Daily streamflow data by gauge
- **Snowpack**: Snow water equivalent and snow depth data

**Bronze Topics:**
- `ingestion.weather.noaa.precipitation.raw.v1`
- `ingestion.weather.noaa.temperature.raw.v1`
- `ingestion.weather.noaa.streamflow.raw.v1`
- `ingestion.weather.noaa.snowpack.raw.v1`

## Configuration

### Environment Variables

```bash
# EIA API
EIA_API_KEY=your_eia_api_key
EIA_BASE_URL=https://api.eia.gov/v2

# EPA CEMS
EPA_CEMS_BASE_URL=https://ampd.epa.gov/ampd

# CARB Cap-and-Trade
CARB_CT_BASE_URL=https://www.arb.ca.gov/cc/capandtrade/auction/auction.htm

# CEC Demand
CEC_DEMAND_BASE_URL=https://www.energy.ca.gov/data-reports

# NOAA Hydro
NOAA_HYDRO_BASE_URL=https://water.weather.gov/ahps2

# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
SCHEMA_REGISTRY_URL=http://localhost:8081
```

### Scheduling

Each connector has optimized scheduling:

- **EIA**: Daily at 2-4 AM (staggered by dataset)
- **EPA CEMS**: Daily at 1-2 AM
- **CARB CT**: Daily at 5-7 AM
- **CEC Demand**: Daily at 8-10 AM
- **NOAA Hydro**: Daily at 12-3 PM

## Usage

### Basic Usage

```python
from ingestion.connectors.shared_extractors import SharedExtractor

# Initialize extractor
extractor = SharedExtractor(
    base_url="https://api.eia.gov/v2",
    timeout=60,
    rate_limit=100
)

# Extract EIA 860 data
result = await extractor.extract_eia_860_data(
    start_date="2024-01-01",
    end_date="2024-01-31"
)

# Extract EPA CEMS data
result = await extractor.extract_epa_cems_data(
    start_date="2024-01-01",
    end_date="2024-01-31",
    facility_ids=["12345", "67890"]
)

# Extract NOAA hydro data
result = await extractor.extract_noaa_hydro_data(
    start_date="2024-01-01",
    end_date="2024-01-31",
    data_type="precipitation",
    station_ids=["CA001", "CA002"]
)
```

### Batch Processing

```python
# Extract multiple data types in parallel
import asyncio

async def extract_all_sources():
    tasks = [
        extractor.extract_eia_860_data("2024-01-01", "2024-01-31"),
        extractor.extract_epa_cems_data("2024-01-01", "2024-01-31"),
        extractor.extract_carb_ct_data("2024-01-01", "2024-01-31"),
        extractor.extract_cec_demand_data("2024-01-01", "2024-01-31"),
        extractor.extract_noaa_hydro_data("2024-01-01", "2024-01-31"),
    ]
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    return results

# Run batch extraction
results = await extract_all_sources()
```

## Data Quality

### Validation

- **Schema Compliance**: All records validated against Avro schemas
- **Data Types**: Strict typing for numeric fields
- **Range Checks**: Reasonable value ranges for emissions, prices, etc.
- **Completeness**: Required fields validated

### Error Handling

- **Rate Limiting**: Respects API rate limits
- **Retry Logic**: Exponential backoff for transient failures
- **Circuit Breaker**: Protects against sustained API failures
- **Data Validation**: Schema validation before publishing

## Monitoring

### Metrics

- **Extraction Success Rate**: Percentage of successful extractions
- **Data Volume**: Records extracted per source
- **Processing Latency**: Time from extraction to Bronze publishing
- **API Response Times**: External API performance

### Alerts

- **Failed Extractions**: Immediate alerts for extraction failures
- **Data Freshness**: Alerts for stale data
- **Schema Violations**: Alerts for data quality issues
- **Rate Limit Breaches**: Alerts for API limit violations

## Dependencies

- Python 3.11+
- httpx (async HTTP client)
- structlog (structured logging)
- Base connector framework
- Kafka for Bronze topic publishing
- Schema Registry for Avro schemas

## Integration

### Downstream Processing

Bronze topics feed into:

1. **Silver Layer**: Normalization and enrichment
2. **Gold Layer**: Business logic and aggregation
3. **Served Tables**: ClickHouse for analytics
4. **IRP Inputs**: Production-cost model inputs

### Data Lineage

- **Extraction**: Source API → Raw records
- **Transformation**: Raw records → Bronze topics
- **Normalization**: Bronze → Silver → Gold
- **Serving**: Gold → ClickHouse → Analytics

## Troubleshooting

### Common Issues

1. **API Rate Limits**: Implement proper rate limiting and backoff
2. **Data Schema Changes**: Monitor for API schema changes
3. **Network Timeouts**: Increase timeout values for large datasets
4. **Authentication**: Ensure API keys are valid and not expired

### Debugging

```python
# Enable debug logging
import structlog
structlog.configure(
    processors=[
        structlog.dev.ConsoleRenderer()
    ],
    wrapper_class=structlog.make_filtering_bound_logger(20),  # DEBUG level
    logger_factory=structlog.PrintLoggerFactory(),
    cache_logger_on_first_use=True,
)
```

## Future Enhancements

- **Real-time Streaming**: WebSocket connections for real-time data
- **Data Compression**: Compress large datasets before publishing
- **Caching**: Redis caching for frequently accessed data
- **Data Quality Scoring**: Automated data quality assessment
- **Multi-region Support**: Expand beyond California/WECC
