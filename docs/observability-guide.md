# 254Carbon Ingestion Platform - Observability Guide

## Overview

This guide covers the observability stack for the 254Carbon Ingestion Platform, including metrics collection, distributed tracing, logging, and alerting.

## Table of Contents

1. [Architecture](#architecture)
2. [Metrics](#metrics)
3. [Tracing](#tracing)
4. [Logging](#logging)
5. [Alerting](#alerting)
6. [Dashboards](#dashboards)
7. [Setup](#setup)
8. [Usage](#usage)
9. [Troubleshooting](#troubleshooting)

## Architecture

The observability stack consists of:

- **Prometheus**: Metrics collection and storage
- **Grafana**: Visualization and dashboards
- **Jaeger**: Distributed tracing
- **OpenTelemetry Collector**: Telemetry data collection and processing
- **Alertmanager**: Alert routing and notification
- **Node Exporter**: System metrics
- **cAdvisor**: Container metrics

### Data Flow

```
Services → OpenTelemetry Collector → Prometheus/Jaeger
                ↓
        Grafana Dashboards
                ↓
        Alertmanager → Notifications
```

## Metrics

### Service Metrics

Each service exposes Prometheus metrics at `/metrics` endpoint:

#### HTTP Metrics
- `http_requests_total`: Total HTTP requests
- `http_request_duration_seconds`: Request duration histogram
- `http_request_size_bytes`: Request size histogram
- `http_response_size_bytes`: Response size histogram

#### Data Processing Metrics
- `carbon_ingestion_records_processed_total`: Records processed counter
- `carbon_ingestion_processing_duration_seconds`: Processing duration histogram
- `carbon_ingestion_batch_size`: Batch size histogram
- `carbon_ingestion_data_quality_score`: Data quality score gauge

#### Kafka Metrics
- `carbon_ingestion_kafka_messages_consumed_total`: Messages consumed counter
- `carbon_ingestion_kafka_messages_produced_total`: Messages produced counter
- `carbon_ingestion_kafka_consumer_lag`: Consumer lag gauge
- `carbon_ingestion_kafka_producer_errors_total`: Producer errors counter
- `carbon_ingestion_kafka_consumer_errors_total`: Consumer errors counter

#### ClickHouse Metrics
- `carbon_ingestion_clickhouse_queries_total`: Queries executed counter
- `carbon_ingestion_clickhouse_query_duration_seconds`: Query duration histogram
- `carbon_ingestion_clickhouse_connection_errors_total`: Connection errors counter

#### System Metrics
- `carbon_ingestion_active_connections`: Active connections gauge
- `carbon_ingestion_memory_usage_bytes`: Memory usage gauge
- `carbon_ingestion_cpu_usage_percent`: CPU usage gauge

#### Business Metrics
- `carbon_ingestion_market_data_volume`: Market data volume gauge
- `carbon_ingestion_price_range`: Price range summary
- `carbon_ingestion_throughput_records_per_second`: Throughput gauge

### Metric Labels

Common labels across metrics:
- `service`: Service name (e.g., normalization-service)
- `market`: Market identifier (e.g., MISO, CAISO)
- `data_type`: Data type (e.g., LMP, Trade, Generation)
- `status`: Operation status (e.g., success, error)
- `error_type`: Error type for error metrics

## Tracing

### OpenTelemetry Integration

The platform uses OpenTelemetry for distributed tracing:

#### Trace Attributes
- `carbon_ingestion.service`: Service name
- `carbon_ingestion.market`: Market identifier
- `carbon_ingestion.data_type`: Data type
- `carbon_ingestion.operation`: Operation type
- `carbon_ingestion.record_count`: Number of records processed

#### Span Types
- **Data Processing**: `carbon_ingestion_{service}_process`
- **Kafka Operations**: `kafka_{operation}`
- **ClickHouse Operations**: `clickhouse_{operation}`
- **Connector Operations**: `connector_{connector_name}_{operation}`
- **Validation**: `validation_{validation_type}`
- **Enrichment**: `enrichment_{enrichment_type}`
- **Aggregation**: `aggregation_{aggregation_type}`
- **Projection**: `projection_{projection_type}`

### Trace Context Propagation

Traces are propagated across services using:
- HTTP headers: `traceparent`, `tracestate`
- Kafka headers: `traceparent`, `tracestate`
- Internal service calls

## Logging

### Structured Logging

All services use structured JSON logging with the following fields:

```json
{
  "timestamp": "2024-01-01T00:00:00Z",
  "level": "INFO",
  "service": "normalization-service",
  "version": "1.0.0",
  "trace_id": "abc123...",
  "span_id": "def456...",
  "message": "Data normalized successfully",
  "market": "MISO",
  "data_type": "LMP",
  "record_count": 1000,
  "processing_time_ms": 150,
  "quality_score": 0.95
}
```

### Log Levels

- **DEBUG**: Detailed debugging information
- **INFO**: General information about service operation
- **WARNING**: Warning conditions that don't stop execution
- **ERROR**: Error conditions that may affect functionality
- **CRITICAL**: Critical errors that may cause service failure

### Log Aggregation

Logs are collected and aggregated using:
- **Fluentd**: Log collection and forwarding
- **Elasticsearch**: Log storage and indexing
- **Kibana**: Log visualization and analysis

## Alerting

### Alert Rules

Alert rules are defined in `configs/prometheus/rules/carbon-ingestion.yml`:

#### Service Health Alerts
- **ServiceDown**: Service is not responding
- **ServiceHighErrorRate**: Error rate > 5%
- **ServiceHighLatency**: P95 latency > 500ms

#### Data Pipeline Alerts
- **DataPipelineStalled**: Pipeline is not processing data
- **HighKafkaConsumerLag**: Consumer lag > 10,000 messages
- **DataQualityLow**: Data quality score < 95%
- **MissingData**: No data processed in 10 minutes

#### Infrastructure Alerts
- **HighCPUUsage**: CPU usage > 80%
- **HighMemoryUsage**: Memory usage > 85%
- **DiskSpaceLow**: Disk usage > 85%
- **ClickHouseDown**: ClickHouse service down
- **KafkaDown**: Kafka service down
- **RedisDown**: Redis service down

#### Connector Alerts
- **ConnectorFailure**: Connector has errors
- **ConnectorStalled**: Connector hasn't succeeded in 1 hour
- **ConnectorLowThroughput**: Throughput < 10 records/second

#### Airflow Alerts
- **AirflowDAGFailed**: DAG execution failed
- **AirflowTaskFailed**: Task execution failed
- **AirflowSchedulerDown**: Scheduler service down

### Alert Routing

Alerts are routed based on severity and component:

- **Critical**: On-call engineer, Slack #alerts-critical
- **Warning**: Team, Slack #alerts-warning
- **Data Pipeline**: Data team, Slack #data-pipeline-alerts
- **Infrastructure**: Platform team, Slack #infrastructure-alerts
- **Connector**: Connector team, Slack #connector-alerts

## Dashboards

### Overview Dashboard

The overview dashboard provides a high-level view of the platform:

- Service health status
- Request rate and response time
- Kafka consumer lag
- Data quality scores
- System resource usage

### Data Pipeline Dashboard

The data pipeline dashboard focuses on data flow:

- Throughput by service
- Processing latency
- Consumer lag by topic
- Data quality metrics
- Validation error rates

### Connector Dashboard

The connector dashboard monitors data ingestion:

- Connector health status
- Data volume by market
- Error rates and types
- Processing latency
- Success rates

### Infrastructure Dashboard

The infrastructure dashboard monitors system resources:

- CPU and memory usage
- Disk space utilization
- Network I/O
- Container metrics
- Node health

## Setup

### Prerequisites

- Docker and Docker Compose
- Network connectivity between services
- Sufficient disk space for metrics storage

### Quick Start

1. **Start observability stack**:
   ```bash
   make observability-up
   ```

2. **Verify services are running**:
   ```bash
   make observability-status
   ```

3. **Access dashboards**:
   - Grafana: http://localhost:3000 (admin/admin)
   - Prometheus: http://localhost:9090
   - Jaeger: http://localhost:16686

### Configuration

#### Prometheus Configuration

Edit `configs/prometheus/prometheus.yml` to:
- Add new scrape targets
- Configure alerting rules
- Set retention policies
- Adjust scrape intervals

#### Grafana Configuration

Edit `configs/grafana/datasources/prometheus.yml` to:
- Configure Prometheus data source
- Set query timeouts
- Configure authentication

#### Alertmanager Configuration

Edit `configs/alertmanager/alertmanager.yml` to:
- Configure notification channels
- Set up alert routing
- Define inhibition rules
- Configure grouping and timing

## Usage

### Viewing Metrics

1. **Prometheus UI**:
   - Navigate to http://localhost:9090
   - Use PromQL to query metrics
   - View targets and service discovery

2. **Grafana Dashboards**:
   - Navigate to http://localhost:3000
   - Import pre-configured dashboards
   - Create custom dashboards
   - Set up alerting rules

### Viewing Traces

1. **Jaeger UI**:
   - Navigate to http://localhost:16686
   - Search for traces by service, operation, or time range
   - View trace details and spans
   - Analyze performance bottlenecks

### Monitoring Alerts

1. **Alertmanager UI**:
   - Navigate to http://localhost:9093
   - View active alerts
   - Check alert routing
   - Test notification channels

### Custom Metrics

To add custom metrics to a service:

```python
from prometheus_client import Counter, Histogram, Gauge

# Define metric
custom_counter = Counter(
    'custom_metric_total',
    'Description of the metric',
    ['label1', 'label2']
)

# Use metric
custom_counter.labels(label1='value1', label2='value2').inc()
```

### Custom Traces

To add custom traces to a service:

```python
from .core.tracing import trace_function, create_span

@trace_function("custom_operation", {"custom.attribute": "value"})
async def custom_function():
    # Function implementation
    pass

# Or create spans manually
with create_span("custom_span", {"custom.attribute": "value"}) as span:
    # Span implementation
    pass
```

## Troubleshooting

### Common Issues

#### Prometheus Not Scraping Metrics

1. **Check service endpoints**:
   ```bash
   curl http://localhost:8510/metrics
   ```

2. **Verify Prometheus configuration**:
   ```bash
   docker exec prometheus cat /etc/prometheus/prometheus.yml
   ```

3. **Check service discovery**:
   - Navigate to http://localhost:9090/targets
   - Look for failed targets
   - Check error messages

#### Grafana Dashboards Not Loading

1. **Check data source connection**:
   - Navigate to Grafana → Configuration → Data Sources
   - Test Prometheus connection
   - Verify URL and credentials

2. **Check dashboard queries**:
   - Open dashboard in edit mode
   - Verify PromQL queries
   - Check time range and filters

#### Jaeger Traces Not Appearing

1. **Check trace propagation**:
   - Verify OpenTelemetry collector is running
   - Check trace headers in HTTP requests
   - Verify Kafka trace headers

2. **Check service configuration**:
   - Verify tracing is initialized
   - Check trace exporter configuration
   - Verify service name and version

#### Alerts Not Firing

1. **Check alert rules**:
   ```bash
   docker exec prometheus cat /etc/prometheus/rules/carbon-ingestion.yml
   ```

2. **Check Alertmanager configuration**:
   ```bash
   docker exec alertmanager cat /etc/alertmanager/alertmanager.yml
   ```

3. **Test alert rules**:
   - Navigate to http://localhost:9090/alerts
   - Check rule evaluation status
   - Verify alert conditions

### Debug Commands

```bash
# Check service health
curl http://localhost:8510/health

# View service metrics
curl http://localhost:8510/metrics

# Check Prometheus targets
curl http://localhost:9090/api/v1/targets

# Check Grafana data sources
curl http://localhost:3000/api/datasources

# Check Jaeger services
curl http://localhost:16686/api/services

# Check Alertmanager alerts
curl http://localhost:9093/api/v1/alerts
```

### Performance Tuning

#### Prometheus Performance

1. **Increase retention**:
   ```yaml
   --storage.tsdb.retention.time=30d
   ```

2. **Adjust scrape intervals**:
   ```yaml
   scrape_interval: 30s
   ```

3. **Optimize queries**:
   - Use recording rules for complex queries
   - Implement query caching
   - Optimize label selectors

#### Grafana Performance

1. **Enable query caching**:
   ```yaml
   jsonData:
     cacheTimeout: 300
   ```

2. **Optimize dashboards**:
   - Reduce query complexity
   - Use template variables
   - Implement dashboard refresh intervals

#### Jaeger Performance

1. **Adjust sampling rate**:
   ```yaml
   sampling:
     type: probabilistic
     param: 0.1
   ```

2. **Configure retention**:
   ```yaml
   storage:
     type: elasticsearch
     elasticsearch:
       max_span_age: 72h
   ```

## Best Practices

### Metrics Design

1. **Use consistent naming**: Follow Prometheus naming conventions
2. **Include relevant labels**: Add labels that help with filtering and grouping
3. **Avoid high cardinality**: Don't use labels with many unique values
4. **Document metrics**: Provide clear descriptions and examples

### Tracing Design

1. **Use meaningful span names**: Make span names descriptive and consistent
2. **Add relevant attributes**: Include attributes that help with debugging
3. **Keep traces focused**: Don't create overly detailed traces
4. **Handle errors properly**: Record exceptions and set appropriate status

### Alerting Design

1. **Set appropriate thresholds**: Avoid alert fatigue with reasonable thresholds
2. **Use alert grouping**: Group related alerts to reduce noise
3. **Provide runbooks**: Include troubleshooting steps in alert descriptions
4. **Test alerting**: Regularly test alert routing and notifications

### Dashboard Design

1. **Keep dashboards focused**: Each dashboard should have a clear purpose
2. **Use appropriate visualizations**: Choose charts that best represent the data
3. **Include context**: Add descriptions and help text
4. **Make dashboards actionable**: Include links to relevant tools and runbooks

## Support

For additional support:

- **Documentation**: [GitHub Wiki](https://github.com/254carbon/ingestion/wiki)
- **Issues**: [GitHub Issues](https://github.com/254carbon/ingestion/issues)
- **Discussions**: [GitHub Discussions](https://github.com/254carbon/ingestion/discussions)
- **Email**: team@254carbon.com
