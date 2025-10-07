"""
Observability module for projection service.

This module provides metrics collection and monitoring capabilities
for the projection service using Prometheus and OpenTelemetry.
"""

import time
from typing import Dict, Any, Optional

import structlog
from prometheus_client import Counter, Histogram, Gauge


class ProjectionMetrics:
    """Metrics collector for projection service."""

    def __init__(self):
        """Initialize metrics collectors."""
        self.logger = structlog.get_logger(__name__)

        # Projection operation metrics
        self.projection_operations_total = Counter(
            'projection_operations_total',
            'Total number of projection operations',
            ['operation_type', 'status']
        )

        self.projection_latency_seconds = Histogram(
            'projection_latency_seconds',
            'Projection operation latency in seconds',
            ['operation_type'],
            buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
        )

        # ClickHouse metrics
        self.clickhouse_writes_total = Counter(
            'clickhouse_writes_total',
            'Total ClickHouse write operations',
            ['table', 'status']
        )

        self.clickhouse_write_latency_seconds = Histogram(
            'clickhouse_write_latency_seconds',
            'ClickHouse write latency in seconds',
            ['table'],
            buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
        )

        # Redis cache metrics
        self.cache_operations_total = Counter(
            'cache_operations_total',
            'Total cache operations',
            ['operation_type', 'status']
        )

        self.cache_hit_ratio = Gauge(
            'cache_hit_ratio',
            'Cache hit ratio'
        )

        # Kafka consumer metrics
        self.kafka_messages_consumed_total = Counter(
            'kafka_messages_consumed_total',
            'Total Kafka messages consumed',
            ['consumer_type', 'status']
        )

        self.kafka_consumer_lag = Gauge(
            'kafka_consumer_lag',
            'Kafka consumer lag',
            ['consumer_type']
        )

        self.logger.info("Projection metrics initialized")

    def record_projection_operation(self, operation_type: str, status: str, duration: float) -> None:
        """Record projection operation metrics."""
        self.projection_operations_total.labels(
            operation_type=operation_type,
            status=status
        ).inc()

        if status == "success":
            self.projection_latency_seconds.labels(
                operation_type=operation_type
            ).observe(duration)

    def record_clickhouse_write(self, table: str, status: str, duration: float, rows: int) -> None:
        """Record ClickHouse write metrics."""
        self.clickhouse_writes_total.labels(
            table=table,
            status=status
        ).inc()

        if status == "success":
            self.clickhouse_write_latency_seconds.labels(
                table=table
            ).observe(duration)

    def record_cache_operation(self, operation_type: str, status: str) -> None:
        """Record cache operation metrics."""
        self.cache_operations_total.labels(
            operation_type=operation_type,
            status=status
        ).inc()

    def update_cache_hit_ratio(self, hit_rate: float) -> None:
        """Update cache hit ratio gauge."""
        self.cache_hit_ratio.set(hit_rate)

    def update_consumer_lag(self, consumer_type: str, lag: int) -> None:
        """Update Kafka consumer lag gauge."""
        self.kafka_consumer_lag.labels(
            consumer_type=consumer_type
        ).set(lag)

    def record_kafka_message(self, consumer_type: str, status: str) -> None:
        """Record Kafka message consumption metrics."""
        self.kafka_messages_consumed_total.labels(
            consumer_type=consumer_type,
            status=status
        ).inc()


# Global metrics instance
projection_metrics = ProjectionMetrics()
