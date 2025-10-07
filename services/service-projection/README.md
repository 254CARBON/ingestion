# Projection Service

The Projection Service is responsible for projecting aggregated market data from the Gold layer to the serving layer, using ClickHouse for data storage and Redis for caching.

## Overview

The Projection Service consumes aggregated data from Kafka topics and writes it to ClickHouse tables for efficient querying. It also maintains a Redis cache for frequently accessed data to improve query performance.

## Architecture

```
Aggregation Service → Kafka → Projection Service → ClickHouse (Gold) + Redis (Cache)
                              ↓
                         Query API
```

## Key Features

- **ClickHouse Integration**: Batch writes to optimized ClickHouse tables
- **Redis Caching**: Intelligent caching with TTL management
- **Kafka Consumers**: Three specialized consumers for different data types
- **Query API**: REST endpoints for data retrieval
- **Materialized Views**: Automatic refresh of ClickHouse materialized views
- **Observability**: Comprehensive metrics and tracing

## Components

### Core Components

- **ProjectionService**: Main service orchestrating data projection
- **ClickHouseWriter**: Handles ClickHouse connections and batch writes
- **CacheManager**: Manages Redis caching with invalidation strategies

### Kafka Consumers

- **KafkaOHLCConsumer**: Processes OHLC bar data
- **KafkaMetricsConsumer**: Processes rolling metrics data
- **KafkaCurveConsumer**: Processes curve data

### API Endpoints

- **Health Checks**: `/health`, `/readiness`, `/liveness`
- **Metrics**: `/metrics` (Prometheus format)
- **Queries**: `/query/ohlc`, `/query/metrics`, `/query/curves`
- **Cache Management**: `/cache/invalidate`, `/cache/refresh`

## Configuration

The service uses `configs/projection_policies.yaml` for configuration:

```yaml
clickhouse:
  dsn: "clickhouse://default@clickhouse:9000/carbon_ingestion"
  pool_size: 10
  batch_size: 1000

redis:
  url: "redis://redis:6379/0"
  default_ttl: 3600

cache:
  ohlc_bars:
    enabled: true
    ttl: 3600
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `HOST` | `0.0.0.0` | Service host |
| `PORT` | `8513` | Service port |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka bootstrap servers |
| `CLICKHOUSE_DSN` | `clickhouse://default@localhost:9000/carbon_ingestion` | ClickHouse DSN |
| `REDIS_URL` | `redis://localhost:6379/0` | Redis URL |
| `LOG_LEVEL` | `INFO` | Logging level |
| `PARALLELISM` | `4` | Processing parallelism |

## Database Tables

### Gold Layer Tables

- **`gold_ohlc_bars`**: OHLC bars with market data
- **`gold_rolling_metrics`**: Rolling metrics (averages, volatility, etc.)
- **`gold_curve_prestage`**: Curve data for forward pricing

### Materialized Views

- **`mv_daily_ohlc_bars`**: Daily OHLC aggregations
- **`mv_hourly_ohlc_bars`**: Hourly OHLC aggregations

## API Usage

### Query OHLC Bars

```bash
curl "http://localhost:8513/query/ohlc?market=CAISO&delivery_location=TH_SP15_GEN-APND&bar_type=daily"
```

Response:
```json
{
  "results": [
    {
      "market": "CAISO",
      "delivery_location": "TH_SP15_GEN-APND",
      "delivery_date": "2025-01-15",
      "bar_type": "daily",
      "open_price": 50.0,
      "high_price": 55.0,
      "low_price": 48.0,
      "close_price": 52.0,
      "volume": 1000.0,
      "vwap": 51.5,
      "trade_count": 100
    }
  ],
  "count": 1,
  "cached": true
}
```

### Query Rolling Metrics

```bash
curl "http://localhost:8513/query/metrics?market=CAISO&metric_type=rolling_average_price"
```

### Manage Cache

```bash
# Invalidate cache for a market
curl -X POST "http://localhost:8513/cache/invalidate/market" -d "market=CAISO"

# Refresh cache for specific data types
curl -X POST "http://localhost:8513/cache/refresh" \
  -H "Content-Type: application/json" \
  -d '{"market": "CAISO", "data_types": ["ohlc", "metrics"]}'
```

## Monitoring

### Metrics

The service exposes Prometheus metrics at `/metrics`:

- `projection_operations_total`: Total projection operations
- `projection_latency_seconds`: Projection latency histogram
- `clickhouse_writes_total`: ClickHouse write operations
- `cache_hit_ratio`: Cache hit ratio gauge

### Health Checks

- **Liveness**: `/health` - Basic service health
- **Readiness**: `/readiness` - Service readiness for traffic
- **Custom Health**: `/health` includes detailed component status

### Logging

Structured JSON logging with correlation IDs for request tracing.

## Development

### Running Locally

```bash
# Start dependencies
docker-compose -f docker-compose.infra.yml up -d

# Run projection service
cd services/service-projection
python -m src.main
```

### Testing

```bash
# Run unit tests
pytest tests/unit/services/test_projection_service.py

# Run integration tests
pytest tests/integration/test_projection_integration.py
```

## Deployment

### Docker

```bash
# Build image
docker build -t carbon-ingestion/projection-service:latest -f services/service-projection/Dockerfile .

# Run container
docker run -p 8513:8513 \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
  -e CLICKHOUSE_DSN=clickhouse://clickhouse:9000/carbon_ingestion \
  carbon-ingestion/projection-service:latest
```

### Kubernetes

```bash
# Apply deployment
kubectl apply -f k8s/projection-service.yaml

# Check status
kubectl get pods -l app=projection-service
```

## Performance

- **Throughput**: Processes 1000+ records per second
- **Latency**: Query responses <100ms for cached data
- **Scalability**: Horizontal scaling supported
- **Reliability**: Automatic retries and error handling

## Troubleshooting

### Common Issues

1. **ClickHouse Connection Failures**
   - Check ClickHouse service health
   - Verify DSN configuration
   - Check network connectivity

2. **Redis Cache Issues**
   - Verify Redis service availability
   - Check cache configuration
   - Monitor memory usage

3. **Kafka Consumer Lag**
   - Check Kafka broker health
   - Monitor consumer group status
   - Adjust parallelism settings

### Debug Logging

Enable debug logging:

```bash
export LOG_LEVEL=DEBUG
python -m src.main
```

### Metrics Monitoring

Monitor key metrics:
- High projection latency (>5s)
- Low cache hit rate (<70%)
- High error rate (>10%)
