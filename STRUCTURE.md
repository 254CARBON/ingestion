# 254Carbon Ingestion Layer - Detailed Architecture

## Overview

This document provides detailed architectural guidance for the 254Carbon ingestion platform, complementing the main README.md with implementation specifics and design patterns.

## Core Principles

### 1. Data Flow Architecture

The ingestion layer follows a staged data processing pipeline:

```
Raw Data → Bronze → Silver → Gold → Served
```

Each stage has specific characteristics:
- **Bronze**: Raw, schema-lenient, append-only
- **Silver**: Normalized, strongly-typed, canonical
- **Gold**: Enriched, aggregated, derived metrics
- **Served**: Query-optimized, low-latency projections

### 2. Service Architecture

Microservices follow a clear separation of concerns:

- **Connector Registry**: Metadata and discovery
- **Normalization**: Raw → Silver transformation
- **Enrichment**: Semantic tagging and taxonomy
- **Aggregation**: OHLC bars and rolling metrics
- **Projection**: Serving layer preparation

### 3. Event-Driven Design

All services communicate via Kafka topics with Avro schemas:
- Schema evolution with backward compatibility
- Dead letter queues for failed processing
- Circuit breakers for external dependencies
- Retry mechanisms with exponential backoff

## Implementation Patterns

### 1. Connector Framework

All connectors inherit from `BaseConnector` and implement:
- `extract()`: Data acquisition from source
- `transform()`: Data normalization and validation
- `load()`: Event publishing to Kafka

### 2. Service Patterns

Each microservice follows consistent patterns:
- FastAPI for HTTP APIs
- Async/await for I/O operations
- Pydantic for data validation
- Structured logging with correlation IDs
- Prometheus metrics for observability

### 3. Configuration Management

- YAML-based configuration with environment variable interpolation
- Validation on startup
- Hot-reload capability for certain configs
- Secrets management via environment variables

### 4. Error Handling

Comprehensive error handling strategy:
- Custom exception hierarchy
- Retry mechanisms with exponential backoff
- Circuit breakers for external services
- Dead letter queues for failed messages
- Graceful degradation

## Data Models

### 1. Event Envelope

All events share a common envelope structure:
```json
{
  "event_id": "uuid",
  "trace_id": "uuid",
  "occurred_at": "timestamp_micros",
  "tenant_id": "string",
  "schema_version": "string",
  "producer": "string",
  "payload": {...}
}
```

### 2. Connector Metadata

Connector specifications follow a standardized format:
```yaml
name: string
version: string
market: string
mode: batch|streaming|hybrid
schedule: cron_expression
enabled: boolean
output_topic: string
schema: path_to_avro_schema
retries: integer
backoff_seconds: integer
tenant_strategy: single|multi
transforms: list_of_transform_names
owner: string
```

## Deployment Considerations

### 1. Multi-Architecture Support

- Docker buildx for multi-arch builds (amd64 + arm64)
- Kubernetes affinity rules for architecture-specific workloads
- GPU nodes reserved for future ML workloads

### 2. Scaling Guidelines

- Horizontal scaling for stateless services
- Partitionable data stores for stateful components
- Configurable parallelism for batch operations
- Back-pressure mechanisms for streaming

### 3. Observability

- Prometheus metrics for all services
- OpenTelemetry tracing with correlation IDs
- Structured JSON logging
- Health checks for liveness/readiness

## Security Considerations

### 1. Network Security

- Services internal only (no direct internet exposure)
- Service-to-service authentication (future mTLS)
- Secrets management via environment variables
- Data integrity checksums for file transfers

### 2. Data Security

- No PII expected in ingestion layer
- Encryption at rest (optional in local mode)
- Tenant isolation via tenant_id
- Audit logging for all operations

## Performance Guidelines

### 1. Latency Targets

- Connector runs: < 5 minutes
- Normalization P95: < 500ms per batch
- Enrichment: Cache taxonomy in-memory
- Aggregation: Prefer incremental updates

### 2. Throughput Considerations

- Configurable parallelism for batch operations
- Kafka partitioning for horizontal scaling
- Connection pooling for external APIs
- Batch processing for efficiency

## Testing Strategy

### 1. Test Pyramid

- Unit tests for individual components
- Integration tests for service interactions
- End-to-end tests for complete pipelines
- Contract tests for schema compatibility

### 2. Test Data Management

- Golden datasets for transformation validation
- Synthetic data generation for load testing
- Test fixtures for consistent test environments
- Mock external dependencies

## Monitoring and Alerting

### 1. Key Metrics

- Connector run success/failure rates
- Processing latency percentiles
- Data quality anomaly counts
- Service health status

### 2. Alerting Thresholds

- Failed connector runs > 5%
- Processing latency P95 > 1 second
- Data quality anomalies > 1%
- Service downtime > 30 seconds

## Disaster Recovery

### 1. Data Recovery

- Idempotent transformations for reprocessing
- Bounded time windows for backfill operations
- Checkpoint mechanisms for streaming jobs
- Backup and restore procedures

### 2. Service Recovery

- Circuit breakers for external dependencies
- Graceful degradation for partial failures
- Automatic retry with exponential backoff
- Health check endpoints for monitoring

## Future Considerations

### 1. Planned Enhancements

- Full OpenTelemetry tracing coverage
- mTLS between services
- Advanced data quality anomaly detection
- Machine learning feature engineering

### 2. Scalability Improvements

- Horizontal scaling for all services
- Partitionable data stores
- Advanced caching strategies
- Performance optimization

## Conclusion

This architecture provides a solid foundation for the 254Carbon ingestion platform while maintaining flexibility for future enhancements. The modular design allows for independent development and deployment of components while ensuring consistency across the platform.