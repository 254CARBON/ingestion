"""
OpenTelemetry tracing for the Normalization Service.
"""

import time
import os
from typing import Dict, Any, Optional, Callable
from functools import wraps
import asyncio

from opentelemetry import trace
from opentelemetry.exporter.jaeger.thrift import JaegerExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.kafka import KafkaInstrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.resources import Resource
from opentelemetry.trace import Status, StatusCode
from opentelemetry.semconv.trace import SpanAttributes

import structlog

logger = structlog.get_logger(__name__)

# Global tracer
tracer = None


def _build_otlp_exporter_kwargs(endpoint_override: Optional[str] = None) -> Dict[str, Any]:
    endpoint = (
        endpoint_override
        or os.getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
        or os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
        or "http://otel-collector:4317"
    )
    headers_env = os.getenv("OTEL_EXPORTER_OTLP_HEADERS")
    headers: Dict[str, str] = {}
    if headers_env:
        for segment in headers_env.split(","):
            if not segment or "=" not in segment:
                continue
            key, value = segment.split("=", 1)
            key = key.strip()
            value = value.strip()
            if key:
                headers[key] = value

    exporter_kwargs: Dict[str, Any] = {"endpoint": endpoint}
    if headers:
        exporter_kwargs["headers"] = headers

    certificate = os.getenv("OTEL_EXPORTER_OTLP_CERTIFICATE")
    if certificate:
        exporter_kwargs["certificate_file"] = certificate

    if endpoint.startswith("http://"):
        exporter_kwargs["insecure"] = True

    return exporter_kwargs


def init_tracing(
    service_name: str = "normalization-service",
    service_version: str = "1.0.0",
    environment: str = "production",
    jaeger_endpoint: str = "http://jaeger:14268/api/traces",
    otlp_endpoint: Optional[str] = None
):
    """Initialize OpenTelemetry tracing."""
    global tracer
    
    # Create resource
    resource = Resource.create({
        "service.name": service_name,
        "service.version": service_version,
        "deployment.environment": environment,
        "service.namespace": "carbon-ingestion"
    })
    
    # Create tracer provider
    trace.set_tracer_provider(TracerProvider(resource=resource))
    tracer = trace.get_tracer(__name__)
    
    # Create exporters
    jaeger_exporter = JaegerExporter(
        agent_host_name="jaeger",
        agent_port=14268,
    )
    
    otlp_exporter = OTLPSpanExporter(**_build_otlp_exporter_kwargs(otlp_endpoint))
    
    # Create span processors
    span_processor = BatchSpanProcessor(jaeger_exporter)
    otlp_span_processor = BatchSpanProcessor(otlp_exporter)
    
    # Add processors to tracer provider
    trace.get_tracer_provider().add_span_processor(span_processor)
    trace.get_tracer_provider().add_span_processor(otlp_span_processor)
    
    # Instrument libraries
    FastAPIInstrumentor.instrument()
    KafkaInstrumentor.instrument()
    RequestsInstrumentor.instrument()
    HTTPXClientInstrumentor.instrument()
    
    logger.info("OpenTelemetry tracing initialized", service=service_name, version=service_version, environment=environment)


def get_tracer():
    """Get the global tracer."""
    global tracer
    if tracer is None:
        logger.warning("Tracer not initialized, initializing with defaults")
        init_tracing()
    return tracer


def trace_function(
    operation_name: Optional[str] = None,
    attributes: Optional[Dict[str, Any]] = None,
    record_exception: bool = True
):
    """Decorator to trace function execution."""
    def decorator(func: Callable):
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            tracer = get_tracer()
            operation = operation_name or f"{func.__module__}.{func.__name__}"
            
            with tracer.start_as_current_span(operation) as span:
                # Set attributes
                if attributes:
                    for key, value in attributes.items():
                        span.set_attribute(key, value)
                
                # Set function attributes
                span.set_attribute("function.name", func.__name__)
                span.set_attribute("function.module", func.__module__)
                
                try:
                    start_time = time.time()
                    result = await func(*args, **kwargs)
                    duration = time.time() - start_time
                    
                    # Set success attributes
                    span.set_attribute("function.duration", duration)
                    span.set_attribute("function.status", "success")
                    span.set_status(Status(StatusCode.OK))
                    
                    return result
                    
                except Exception as e:
                    duration = time.time() - start_time
                    
                    # Set error attributes
                    span.set_attribute("function.duration", duration)
                    span.set_attribute("function.status", "error")
                    span.set_attribute("error.type", type(e).__name__)
                    span.set_attribute("error.message", str(e))
                    span.set_status(Status(StatusCode.ERROR, str(e)))
                    
                    if record_exception:
                        span.record_exception(e)
                    
                    raise
        
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            tracer = get_tracer()
            operation = operation_name or f"{func.__module__}.{func.__name__}"
            
            with tracer.start_as_current_span(operation) as span:
                # Set attributes
                if attributes:
                    for key, value in attributes.items():
                        span.set_attribute(key, value)
                
                # Set function attributes
                span.set_attribute("function.name", func.__name__)
                span.set_attribute("function.module", func.__module__)
                
                try:
                    start_time = time.time()
                    result = func(*args, **kwargs)
                    duration = time.time() - start_time
                    
                    # Set success attributes
                    span.set_attribute("function.duration", duration)
                    span.set_attribute("function.status", "success")
                    span.set_status(Status(StatusCode.OK))
                    
                    return result
                    
                except Exception as e:
                    duration = time.time() - start_time
                    
                    # Set error attributes
                    span.set_attribute("function.duration", duration)
                    span.set_attribute("function.status", "error")
                    span.set_attribute("error.type", type(e).__name__)
                    span.set_attribute("error.message", str(e))
                    span.set_status(Status(StatusCode.ERROR, str(e)))
                    
                    if record_exception:
                        span.record_exception(e)
                    
                    raise
        
        # Return appropriate wrapper based on function type
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    
    return decorator


def trace_kafka_operation(operation: str, topic: str, partition: Optional[int] = None):
    """Trace Kafka operations."""
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            tracer = get_tracer()
            
            with tracer.start_as_current_span(f"kafka_{operation}") as span:
                # Set Kafka attributes
                span.set_attribute("messaging.system", "kafka")
                span.set_attribute("messaging.operation", operation)
                span.set_attribute("messaging.destination", topic)
                span.set_attribute("messaging.destination_kind", "topic")
                
                if partition is not None:
                    span.set_attribute("messaging.kafka.partition", partition)
                
                try:
                    result = await func(*args, **kwargs)
                    span.set_status(Status(StatusCode.OK))
                    return result
                    
                except Exception as e:
                    span.set_attribute("error.type", type(e).__name__)
                    span.set_attribute("error.message", str(e))
                    span.set_status(Status(StatusCode.ERROR, str(e)))
                    span.record_exception(e)
                    raise
        
        return wrapper
    return decorator


def trace_clickhouse_operation(operation: str, query_type: str = "unknown"):
    """Trace ClickHouse operations."""
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            tracer = get_tracer()
            
            with tracer.start_as_current_span(f"clickhouse_{operation}") as span:
                # Set ClickHouse attributes
                span.set_attribute("db.system", "clickhouse")
                span.set_attribute("db.operation", operation)
                span.set_attribute("db.query_type", query_type)
                
                try:
                    result = await func(*args, **kwargs)
                    span.set_status(Status(StatusCode.OK))
                    return result
                    
                except Exception as e:
                    span.set_attribute("error.type", type(e).__name__)
                    span.set_attribute("error.message", str(e))
                    span.set_status(Status(StatusCode.ERROR, str(e)))
                    span.record_exception(e)
                    raise
        
        return wrapper
    return decorator


def trace_data_processing(service: str, market: str, data_type: str, record_count: int = 1):
    """Trace data processing operations."""
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            tracer = get_tracer()
            
            with tracer.start_as_current_span(f"carbon_ingestion_{service}_process") as span:
                # Set data processing attributes
                span.set_attribute("carbon_ingestion.service", service)
                span.set_attribute("carbon_ingestion.market", market)
                span.set_attribute("carbon_ingestion.data_type", data_type)
                span.set_attribute("carbon_ingestion.record_count", record_count)
                
                try:
                    result = await func(*args, **kwargs)
                    span.set_status(Status(StatusCode.OK))
                    return result
                    
                except Exception as e:
                    span.set_attribute("error.type", type(e).__name__)
                    span.set_attribute("error.message", str(e))
                    span.set_status(Status(StatusCode.ERROR, str(e)))
                    span.record_exception(e)
                    raise
        
        return wrapper
    return decorator


def trace_connector_operation(connector_name: str, market: str, operation: str):
    """Trace connector operations."""
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            tracer = get_tracer()
            
            with tracer.start_as_current_span(f"connector_{connector_name}_{operation}") as span:
                # Set connector attributes
                span.set_attribute("carbon_ingestion.connector", connector_name)
                span.set_attribute("carbon_ingestion.market", market)
                span.set_attribute("carbon_ingestion.operation", operation)
                
                try:
                    result = await func(*args, **kwargs)
                    span.set_status(Status(StatusCode.OK))
                    return result
                    
                except Exception as e:
                    span.set_attribute("error.type", type(e).__name__)
                    span.set_attribute("error.message", str(e))
                    span.set_status(Status(StatusCode.ERROR, str(e)))
                    span.record_exception(e)
                    raise
        
        return wrapper
    return decorator


def trace_validation(validation_type: str, market: str, data_type: str):
    """Trace validation operations."""
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            tracer = get_tracer()
            
            with tracer.start_as_current_span(f"validation_{validation_type}") as span:
                # Set validation attributes
                span.set_attribute("carbon_ingestion.validation_type", validation_type)
                span.set_attribute("carbon_ingestion.market", market)
                span.set_attribute("carbon_ingestion.data_type", data_type)
                
                try:
                    result = await func(*args, **kwargs)
                    span.set_status(Status(StatusCode.OK))
                    return result
                    
                except Exception as e:
                    span.set_attribute("error.type", type(e).__name__)
                    span.set_attribute("error.message", str(e))
                    span.set_status(Status(StatusCode.ERROR, str(e)))
                    span.record_exception(e)
                    raise
        
        return wrapper
    return decorator


def trace_enrichment(enrichment_type: str, market: str, data_type: str):
    """Trace enrichment operations."""
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            tracer = get_tracer()
            
            with tracer.start_as_current_span(f"enrichment_{enrichment_type}") as span:
                # Set enrichment attributes
                span.set_attribute("carbon_ingestion.enrichment_type", enrichment_type)
                span.set_attribute("carbon_ingestion.market", market)
                span.set_attribute("carbon_ingestion.data_type", data_type)
                
                try:
                    result = await func(*args, **kwargs)
                    span.set_status(Status(StatusCode.OK))
                    return result
                    
                except Exception as e:
                    span.set_attribute("error.type", type(e).__name__)
                    span.set_attribute("error.message", str(e))
                    span.set_status(Status(StatusCode.ERROR, str(e)))
                    span.record_exception(e)
                    raise
        
        return wrapper
    return decorator


def trace_aggregation(aggregation_type: str, market: str, data_type: str, interval: str):
    """Trace aggregation operations."""
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            tracer = get_tracer()
            
            with tracer.start_as_current_span(f"aggregation_{aggregation_type}") as span:
                # Set aggregation attributes
                span.set_attribute("carbon_ingestion.aggregation_type", aggregation_type)
                span.set_attribute("carbon_ingestion.market", market)
                span.set_attribute("carbon_ingestion.data_type", data_type)
                span.set_attribute("carbon_ingestion.interval", interval)
                
                try:
                    result = await func(*args, **kwargs)
                    span.set_status(Status(StatusCode.OK))
                    return result
                    
                except Exception as e:
                    span.set_attribute("error.type", type(e).__name__)
                    span.set_attribute("error.message", str(e))
                    span.set_status(Status(StatusCode.ERROR, str(e)))
                    span.record_exception(e)
                    raise
        
        return wrapper
    return decorator


def trace_projection(projection_type: str, market: str, data_type: str):
    """Trace projection operations."""
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            tracer = get_tracer()
            
            with tracer.start_as_current_span(f"projection_{projection_type}") as span:
                # Set projection attributes
                span.set_attribute("carbon_ingestion.projection_type", projection_type)
                span.set_attribute("carbon_ingestion.market", market)
                span.set_attribute("carbon_ingestion.data_type", data_type)
                
                try:
                    result = await func(*args, **kwargs)
                    span.set_status(Status(StatusCode.OK))
                    return result
                    
                except Exception as e:
                    span.set_attribute("error.type", type(e).__name__)
                    span.set_attribute("error.message", str(e))
                    span.set_status(Status(StatusCode.ERROR, str(e)))
                    span.record_exception(e)
                    raise
        
        return wrapper
    return decorator


def create_span(name: str, attributes: Optional[Dict[str, Any]] = None):
    """Create a new span."""
    tracer = get_tracer()
    span = tracer.start_span(name)
    
    if attributes:
        for key, value in attributes.items():
            span.set_attribute(key, value)
    
    return span


def set_span_attribute(key: str, value: Any):
    """Set an attribute on the current span."""
    span = trace.get_current_span()
    if span:
        span.set_attribute(key, value)


def set_span_status(code: StatusCode, description: Optional[str] = None):
    """Set the status of the current span."""
    span = trace.get_current_span()
    if span:
        span.set_status(Status(code, description))


def record_span_exception(exception: Exception):
    """Record an exception on the current span."""
    span = trace.get_current_span()
    if span:
        span.record_exception(exception)
        span.set_status(Status(StatusCode.ERROR, str(exception)))
