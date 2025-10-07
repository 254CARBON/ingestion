"""
Tracing module for projection service.

This module provides OpenTelemetry tracing capabilities for
distributed tracing across the projection service.
"""

import logging
from contextlib import contextmanager
from typing import Dict, Any, Optional

import structlog
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.exporter.jaeger.thrift import JaegerExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.trace import Status, StatusCode


class ProjectionTracer:
    """Tracer for projection service operations."""

    def __init__(self, service_name: str = "projection-service"):
        """
        Initialize the tracer.

        Args:
            service_name: Name of the service for tracing
        """
        self.service_name = service_name
        self.logger = structlog.get_logger(__name__)

        # Initialize tracer provider
        resource = Resource.create({
            "service.name": service_name,
            "service.version": "1.0.0"
        })

        self.tracer_provider = TracerProvider(resource=resource)
        trace.set_tracer_provider(self.tracer_provider)

        # Add exporters based on environment
        self._setup_exporters()

        self.logger.info("Projection tracer initialized", service_name=service_name)

    def _setup_exporters(self) -> None:
        """Set up tracing exporters."""
        # Console exporter for development
        console_exporter = ConsoleSpanExporter()
        span_processor = BatchSpanProcessor(console_exporter)
        self.tracer_provider.add_span_processor(span_processor)

        # Try to add Jaeger exporter if available
        try:
            jaeger_exporter = JaegerExporter(
                agent_host_name="localhost",
                agent_port=6831,
            )
            jaeger_processor = BatchSpanProcessor(jaeger_exporter)
            self.tracer_provider.add_span_processor(jaeger_processor)
            self.logger.info("Jaeger exporter configured")
        except Exception as e:
            self.logger.debug("Jaeger exporter not available", error=str(e))

        # Try to add OTLP exporter if available
        try:
            otlp_exporter = OTLPSpanExporter(
                endpoint="http://localhost:4317",
                insecure=True
            )
            otlp_processor = BatchSpanProcessor(otlp_exporter)
            self.tracer_provider.add_span_processor(otlp_processor)
            self.logger.info("OTLP exporter configured")
        except Exception as e:
            self.logger.debug("OTLP exporter not available", error=str(e))

    def get_tracer(self, name: str = "projection") -> trace.Tracer:
        """Get a tracer instance."""
        return trace.get_tracer(name, tracer_provider=self.tracer_provider)

    @contextmanager
    def trace_projection_operation(self, operation_name: str, **attributes):
        """
        Context manager for tracing projection operations.

        Args:
            operation_name: Name of the operation
            **attributes: Additional span attributes
        """
        tracer = self.get_tracer()
        operation_type = attributes.pop('operation_type', 'unknown')

        with tracer.start_as_current_span(f"projection_{operation_name}") as span:
            # Set standard attributes
            span.set_attribute("projection.operation_type", operation_type)
            span.set_attribute("projection.service", self.service_name)

            # Set custom attributes
            for key, value in attributes.items():
                span.set_attribute(f"projection.{key}", value)

            try:
                yield span

                # Mark as successful
                span.set_status(Status(StatusCode.OK))

            except Exception as e:
                # Mark as failed
                span.set_attribute("error.type", type(e).__name__)
                span.set_attribute("error.message", str(e))
                span.set_status(Status(StatusCode.ERROR, str(e)))
                span.record_exception(e)
                raise

    @contextmanager
    def trace_clickhouse_operation(self, operation_name: str, table: str, **attributes):
        """
        Context manager for tracing ClickHouse operations.

        Args:
            operation_name: Name of the operation
            table: ClickHouse table name
            **attributes: Additional span attributes
        """
        tracer = self.get_tracer()

        with tracer.start_as_current_span(f"clickhouse_{operation_name}") as span:
            # Set standard attributes
            span.set_attribute("clickhouse.operation", operation_name)
            span.set_attribute("clickhouse.table", table)

            # Set custom attributes
            for key, value in attributes.items():
                span.set_attribute(f"clickhouse.{key}", value)

            try:
                yield span

                # Mark as successful
                span.set_status(Status(StatusCode.OK))

            except Exception as e:
                # Mark as failed
                span.set_attribute("error.type", type(e).__name__)
                span.set_attribute("error.message", str(e))
                span.set_status(Status(StatusCode.ERROR, str(e)))
                span.record_exception(e)
                raise

    @contextmanager
    def trace_cache_operation(self, operation_name: str, cache_key: str = None, **attributes):
        """
        Context manager for tracing cache operations.

        Args:
            operation_name: Name of the operation
            cache_key: Cache key being accessed
            **attributes: Additional span attributes
        """
        tracer = self.get_tracer()

        with tracer.start_as_current_span(f"cache_{operation_name}") as span:
            # Set standard attributes
            span.set_attribute("cache.operation", operation_name)
            if cache_key:
                span.set_attribute("cache.key", cache_key)

            # Set custom attributes
            for key, value in attributes.items():
                span.set_attribute(f"cache.{key}", value)

            try:
                yield span

                # Mark as successful
                span.set_status(Status(StatusCode.OK))

            except Exception as e:
                # Mark as failed
                span.set_attribute("error.type", type(e).__name__)
                span.set_attribute("error.message", str(e))
                span.set_status(Status(StatusCode.ERROR, str(e)))
                span.record_exception(e)
                raise

    @contextmanager
    def trace_kafka_consumer(self, consumer_type: str, message_count: int = 1, **attributes):
        """
        Context manager for tracing Kafka consumer operations.

        Args:
            consumer_type: Type of consumer (ohlc, metrics, curve)
            message_count: Number of messages being processed
            **attributes: Additional span attributes
        """
        tracer = self.get_tracer()

        with tracer.start_as_current_span(f"kafka_consume_{consumer_type}") as span:
            # Set standard attributes
            span.set_attribute("kafka.consumer_type", consumer_type)
            span.set_attribute("kafka.message_count", message_count)

            # Set custom attributes
            for key, value in attributes.items():
                span.set_attribute(f"kafka.{key}", value)

            try:
                yield span

                # Mark as successful
                span.set_status(Status(StatusCode.OK))

            except Exception as e:
                # Mark as failed
                span.set_attribute("error.type", type(e).__name__)
                span.set_attribute("error.message", str(e))
                span.set_status(Status(StatusCode.ERROR, str(e)))
                span.record_exception(e)
                raise

    def create_child_span(self, name: str, parent_context=None, **attributes) -> trace.Span:
        """
        Create a child span.

        Args:
            name: Span name
            parent_context: Parent context (optional)
            **attributes: Span attributes

        Returns:
            trace.Span: Created span
        """
        tracer = self.get_tracer()

        if parent_context:
            span = tracer.start_span(name, context=parent_context)
        else:
            span = tracer.start_span(name)

        # Set attributes
        for key, value in attributes.items():
            span.set_attribute(key, value)

        return span

    def inject_trace_context(self, carrier: Dict[str, Any]) -> None:
        """
        Inject trace context into a carrier (e.g., for Kafka headers).

        Args:
            carrier: Dictionary to inject context into
        """
        from opentelemetry.trace import get_current_span

        current_span = get_current_span()
        if current_span is not None:
            # Inject trace context
            from opentelemetry.trace.propagation.tracecontext import TraceContextPropagator
            from opentelemetry.context import attach, detach

            propagator = TraceContextPropagator()
            context = trace.set_span_in_context(current_span)

            with attach(context):
                propagator.inject(carrier)

    def extract_trace_context(self, carrier: Dict[str, Any]) -> Optional[trace.SpanContext]:
        """
        Extract trace context from a carrier.

        Args:
            carrier: Dictionary containing trace context

        Returns:
            Optional[trace.SpanContext]: Extracted span context
        """
        from opentelemetry.trace.propagation.tracecontext import TraceContextPropagator

        propagator = TraceContextPropagator()
        context = propagator.extract(carrier)

        if context:
            span = trace.get_current_span(context)
            return span.get_span_context()

        return None


# Global tracer instance
projection_tracer = ProjectionTracer()
