"""
Prometheus metrics for the Normalization Service.
"""

import time
from typing import Dict, Any, Optional
from functools import wraps

from prometheus_client import (
    Counter,
    Histogram,
    Gauge,
    Summary,
    Info,
    generate_latest,
    CONTENT_TYPE_LATEST
)
from fastapi import Request, Response
from fastapi.responses import PlainTextResponse

import structlog

logger = structlog.get_logger(__name__)

# Service info
SERVICE_INFO = Info(
    'carbon_ingestion_service_info',
    'Information about the carbon ingestion service',
    ['service', 'version', 'environment']
)

# Request metrics
REQUEST_COUNT = Counter(
    'carbon_ingestion_requests_total',
    'Total number of requests',
    ['method', 'endpoint', 'status_code', 'service']
)

REQUEST_DURATION = Histogram(
    'carbon_ingestion_request_duration_seconds',
    'Request duration in seconds',
    ['method', 'endpoint', 'service'],
    buckets=[0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
)

REQUEST_SIZE = Histogram(
    'carbon_ingestion_request_size_bytes',
    'Request size in bytes',
    ['method', 'endpoint', 'service'],
    buckets=[100, 1000, 10000, 100000, 1000000]
)

RESPONSE_SIZE = Histogram(
    'carbon_ingestion_response_size_bytes',
    'Response size in bytes',
    ['method', 'endpoint', 'service'],
    buckets=[100, 1000, 10000, 100000, 1000000]
)

# Data processing metrics
RECORDS_PROCESSED = Counter(
    'carbon_ingestion_records_processed_total',
    'Total number of records processed',
    ['service', 'market', 'data_type', 'status']
)

PROCESSING_DURATION = Histogram(
    'carbon_ingestion_processing_duration_seconds',
    'Record processing duration in seconds',
    ['service', 'market', 'data_type'],
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]
)

BATCH_SIZE = Histogram(
    'carbon_ingestion_batch_size',
    'Batch size for processing',
    ['service', 'market'],
    buckets=[1, 5, 10, 25, 50, 100, 250, 500, 1000]
)

# Data quality metrics
DATA_QUALITY_SCORE = Gauge(
    'carbon_ingestion_data_quality_score',
    'Data quality score (0-1)',
    ['service', 'market', 'data_type']
)

VALIDATION_ERRORS = Counter(
    'carbon_ingestion_validation_errors_total',
    'Total number of validation errors',
    ['service', 'market', 'error_type']
)

NORMALIZATION_ERRORS = Counter(
    'carbon_ingestion_normalization_errors_total',
    'Total number of normalization errors',
    ['service', 'market', 'error_type']
)

# Kafka metrics
KAFKA_MESSAGES_CONSUMED = Counter(
    'carbon_ingestion_kafka_messages_consumed_total',
    'Total number of Kafka messages consumed',
    ['service', 'topic', 'partition']
)

KAFKA_MESSAGES_PRODUCED = Counter(
    'carbon_ingestion_kafka_messages_produced_total',
    'Total number of Kafka messages produced',
    ['service', 'topic', 'partition']
)

KAFKA_CONSUMER_LAG = Gauge(
    'carbon_ingestion_kafka_consumer_lag',
    'Kafka consumer lag',
    ['service', 'topic', 'partition']
)

KAFKA_PRODUCER_ERRORS = Counter(
    'carbon_ingestion_kafka_producer_errors_total',
    'Total number of Kafka producer errors',
    ['service', 'topic', 'error_type']
)

KAFKA_CONSUMER_ERRORS = Counter(
    'carbon_ingestion_kafka_consumer_errors_total',
    'Total number of Kafka consumer errors',
    ['service', 'topic', 'error_type']
)

# ClickHouse metrics
CLICKHOUSE_QUERIES = Counter(
    'carbon_ingestion_clickhouse_queries_total',
    'Total number of ClickHouse queries',
    ['service', 'query_type', 'status']
)

CLICKHOUSE_QUERY_DURATION = Histogram(
    'carbon_ingestion_clickhouse_query_duration_seconds',
    'ClickHouse query duration in seconds',
    ['service', 'query_type'],
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0]
)

CLICKHOUSE_CONNECTION_ERRORS = Counter(
    'carbon_ingestion_clickhouse_connection_errors_total',
    'Total number of ClickHouse connection errors',
    ['service', 'error_type']
)

# System metrics
ACTIVE_CONNECTIONS = Gauge(
    'carbon_ingestion_active_connections',
    'Number of active connections',
    ['service', 'connection_type']
)

MEMORY_USAGE = Gauge(
    'carbon_ingestion_memory_usage_bytes',
    'Memory usage in bytes',
    ['service', 'type']
)

CPU_USAGE = Gauge(
    'carbon_ingestion_cpu_usage_percent',
    'CPU usage percentage',
    ['service']
)

# Business metrics
MARKET_DATA_VOLUME = Gauge(
    'carbon_ingestion_market_data_volume',
    'Market data volume processed',
    ['service', 'market', 'data_type', 'unit']
)

PRICE_RANGE = Summary(
    'carbon_ingestion_price_range',
    'Price range statistics',
    ['service', 'market', 'data_type']
)

THROUGHPUT = Gauge(
    'carbon_ingestion_throughput_records_per_second',
    'Throughput in records per second',
    ['service', 'market']
)


class MetricsCollector:
    """Metrics collector for the Normalization Service."""

    def __init__(self, service_name: str = "normalization-service", version: str = "1.0.0", environment: str = "production"):
        self.service_name = service_name
        self.version = version
        self.environment = environment
        
        # Set service info
        SERVICE_INFO.labels(
            service=service_name,
            version=version,
            environment=environment
        ).set(1)
        
        logger.info("MetricsCollector initialized", service=service_name, version=version, environment=environment)

    def record_request(self, method: str, endpoint: str, status_code: int, duration: float, request_size: int = 0, response_size: int = 0):
        """Record HTTP request metrics."""
        REQUEST_COUNT.labels(
            method=method,
            endpoint=endpoint,
            status_code=str(status_code),
            service=self.service_name
        ).inc()
        
        REQUEST_DURATION.labels(
            method=method,
            endpoint=endpoint,
            service=self.service_name
        ).observe(duration)
        
        if request_size > 0:
            REQUEST_SIZE.labels(
                method=method,
                endpoint=endpoint,
                service=self.service_name
            ).observe(request_size)
        
        if response_size > 0:
            RESPONSE_SIZE.labels(
                method=method,
                endpoint=endpoint,
                service=self.service_name
            ).observe(response_size)

    def record_record_processed(self, market: str, data_type: str, status: str, duration: float, batch_size: int = 1):
        """Record data processing metrics."""
        RECORDS_PROCESSED.labels(
            service=self.service_name,
            market=market,
            data_type=data_type,
            status=status
        ).inc(batch_size)
        
        PROCESSING_DURATION.labels(
            service=self.service_name,
            market=market,
            data_type=data_type
        ).observe(duration)
        
        BATCH_SIZE.labels(
            service=self.service_name,
            market=market
        ).observe(batch_size)

    def record_data_quality(self, market: str, data_type: str, score: float):
        """Record data quality metrics."""
        DATA_QUALITY_SCORE.labels(
            service=self.service_name,
            market=market,
            data_type=data_type
        ).set(score)

    def record_validation_error(self, market: str, error_type: str):
        """Record validation error metrics."""
        VALIDATION_ERRORS.labels(
            service=self.service_name,
            market=market,
            error_type=error_type
        ).inc()

    def record_normalization_error(self, market: str, error_type: str):
        """Record normalization error metrics."""
        NORMALIZATION_ERRORS.labels(
            service=self.service_name,
            market=market,
            error_type=error_type
        ).inc()

    def record_kafka_message_consumed(self, topic: str, partition: int):
        """Record Kafka message consumption metrics."""
        KAFKA_MESSAGES_CONSUMED.labels(
            service=self.service_name,
            topic=topic,
            partition=str(partition)
        ).inc()

    def record_kafka_message_produced(self, topic: str, partition: int):
        """Record Kafka message production metrics."""
        KAFKA_MESSAGES_PRODUCED.labels(
            service=self.service_name,
            topic=topic,
            partition=str(partition)
        ).inc()

    def record_kafka_consumer_lag(self, topic: str, partition: int, lag: int):
        """Record Kafka consumer lag metrics."""
        KAFKA_CONSUMER_LAG.labels(
            service=self.service_name,
            topic=topic,
            partition=str(partition)
        ).set(lag)

    def record_kafka_producer_error(self, topic: str, error_type: str):
        """Record Kafka producer error metrics."""
        KAFKA_PRODUCER_ERRORS.labels(
            service=self.service_name,
            topic=topic,
            error_type=error_type
        ).inc()

    def record_kafka_consumer_error(self, topic: str, error_type: str):
        """Record Kafka consumer error metrics."""
        KAFKA_CONSUMER_ERRORS.labels(
            service=self.service_name,
            topic=topic,
            error_type=error_type
        ).inc()

    def record_clickhouse_query(self, query_type: str, status: str, duration: float):
        """Record ClickHouse query metrics."""
        CLICKHOUSE_QUERIES.labels(
            service=self.service_name,
            query_type=query_type,
            status=status
        ).inc()
        
        CLICKHOUSE_QUERY_DURATION.labels(
            service=self.service_name,
            query_type=query_type
        ).observe(duration)

    def record_clickhouse_connection_error(self, error_type: str):
        """Record ClickHouse connection error metrics."""
        CLICKHOUSE_CONNECTION_ERRORS.labels(
            service=self.service_name,
            error_type=error_type
        ).inc()

    def record_active_connections(self, connection_type: str, count: int):
        """Record active connections metrics."""
        ACTIVE_CONNECTIONS.labels(
            service=self.service_name,
            connection_type=connection_type
        ).set(count)

    def record_memory_usage(self, memory_type: str, usage_bytes: int):
        """Record memory usage metrics."""
        MEMORY_USAGE.labels(
            service=self.service_name,
            type=memory_type
        ).set(usage_bytes)

    def record_cpu_usage(self, usage_percent: float):
        """Record CPU usage metrics."""
        CPU_USAGE.labels(service=self.service_name).set(usage_percent)

    def record_market_data_volume(self, market: str, data_type: str, unit: str, volume: float):
        """Record market data volume metrics."""
        MARKET_DATA_VOLUME.labels(
            service=self.service_name,
            market=market,
            data_type=data_type,
            unit=unit
        ).set(volume)

    def record_price_range(self, market: str, data_type: str, price: float):
        """Record price range metrics."""
        PRICE_RANGE.labels(
            service=self.service_name,
            market=market,
            data_type=data_type
        ).observe(price)

    def record_throughput(self, market: str, records_per_second: float):
        """Record throughput metrics."""
        THROUGHPUT.labels(
            service=self.service_name,
            market=market
        ).set(records_per_second)


def metrics_middleware(app):
    """FastAPI middleware for collecting request metrics."""
    
    @app.middleware("http")
    async def collect_metrics(request: Request, call_next):
        start_time = time.time()
        
        # Get request size
        request_size = 0
        if hasattr(request, '_body'):
            request_size = len(request._body)
        
        response = await call_next(request)
        
        # Calculate duration
        duration = time.time() - start_time
        
        # Get response size
        response_size = 0
        if hasattr(response, 'body'):
            response_size = len(response.body)
        
        # Record metrics
        metrics_collector.record_request(
            method=request.method,
            endpoint=request.url.path,
            status_code=response.status_code,
            duration=duration,
            request_size=request_size,
            response_size=response_size
        )
        
        return response


def timing_decorator(func):
    """Decorator to measure function execution time."""
    @wraps(func)
    async def wrapper(*args, **kwargs):
        start_time = time.time()
        try:
            result = await func(*args, **kwargs)
            duration = time.time() - start_time
            
            # Record timing metric
            PROCESSING_DURATION.labels(
                service=metrics_collector.service_name,
                market="unknown",
                data_type="unknown"
            ).observe(duration)
            
            return result
        except Exception as e:
            duration = time.time() - start_time
            
            # Record error metric
            NORMALIZATION_ERRORS.labels(
                service=metrics_collector.service_name,
                market="unknown",
                error_type=type(e).__name__
            ).inc()
            
            raise
    
    return wrapper


def get_metrics_response():
    """Get Prometheus metrics response."""
    return PlainTextResponse(
        generate_latest(),
        media_type=CONTENT_TYPE_LATEST
    )


# Global metrics collector instance
metrics_collector = MetricsCollector()
