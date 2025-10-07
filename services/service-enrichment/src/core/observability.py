"""
OpenTelemetry observability setup for enrichment service.

This module provides OpenTelemetry instrumentation for tracing,
metrics, and logging across the enrichment service.
"""

import logging
from typing import Any, Dict, Optional
from datetime import datetime, timezone

from opentelemetry import trace
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.aiokafka import AioKafkaInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.resources import Resource
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.instrumentation.logging import LoggingInstrumentor

from ..core.enricher import EnrichmentService


class ObservabilityConfig:
    """Configuration for observability."""
    
    def __init__(
        self,
        service_name: str = "enrichment-service",
        service_version: str = "1.0.0",
        environment: str = "production",
        otlp_endpoint: str = "http://localhost:4317",
        prometheus_endpoint: str = "http://localhost:8889",
        enable_tracing: bool = True,
        enable_metrics: bool = True,
        enable_logging: bool = True,
        trace_sampling_ratio: float = 1.0
    ):
        self.service_name = service_name
        self.service_version = service_version
        self.environment = environment
        self.otlp_endpoint = otlp_endpoint
        self.prometheus_endpoint = prometheus_endpoint
        self.enable_tracing = enable_tracing
        self.enable_metrics = enable_metrics
        self.enable_logging = enable_logging
        self.trace_sampling_ratio = trace_sampling_ratio


class ObservabilityManager:
    """Manages OpenTelemetry observability for the enrichment service."""
    
    def __init__(self, config: ObservabilityConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.tracer = None
        self.meter = None
        self._initialized = False
    
    def initialize(self) -> None:
        """Initialize OpenTelemetry observability."""
        if self._initialized:
            return
        
        try:
            # Create resource
            resource = Resource.create({
                "service.name": self.config.service_name,
                "service.version": self.config.service_version,
                "service.instance.id": f"{self.config.service_name}-{datetime.now().strftime('%Y%m%d-%H%M%S')}",
                "deployment.environment": self.config.environment,
                "telemetry.sdk.name": "opentelemetry",
                "telemetry.sdk.version": "1.0.0"
            })
            
            # Initialize tracing
            if self.config.enable_tracing:
                self._initialize_tracing(resource)
            
            # Initialize metrics
            if self.config.enable_metrics:
                self._initialize_metrics(resource)
            
            # Initialize logging
            if self.config.enable_logging:
                self._initialize_logging()
            
            # Instrument libraries
            self._instrument_libraries()
            
            self._initialized = True
            self.logger.info("OpenTelemetry observability initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize OpenTelemetry observability: {e}")
            raise
    
    def _initialize_tracing(self, resource: Resource) -> None:
        """Initialize tracing."""
        # Create tracer provider
        tracer_provider = TracerProvider(resource=resource)
        
        # Create OTLP exporter
        otlp_exporter = OTLPSpanExporter(endpoint=self.config.otlp_endpoint)
        
        # Create span processor
        span_processor = BatchSpanProcessor(otlp_exporter)
        tracer_provider.add_span_processor(span_processor)
        
        # Set global tracer provider
        trace.set_tracer_provider(tracer_provider)
        
        # Get tracer
        self.tracer = trace.get_tracer(
            instrumenting_module_name=self.config.service_name,
            instrumenting_library_version=self.config.service_version
        )
        
        self.logger.info("Tracing initialized")
    
    def _initialize_metrics(self, resource: Resource) -> None:
        """Initialize metrics."""
        # Create metric readers
        readers = []
        
        # Prometheus reader
        prometheus_reader = PrometheusMetricReader()
        readers.append(prometheus_reader)
        
        # OTLP reader
        otlp_exporter = OTLPMetricExporter(endpoint=self.config.otlp_endpoint)
        otlp_reader = PeriodicExportingMetricReader(
            exporter=otlp_exporter,
            export_interval_millis=30000  # 30 seconds
        )
        readers.append(otlp_reader)
        
        # Create meter provider
        meter_provider = MeterProvider(
            resource=resource,
            metric_readers=readers
        )
        
        # Get meter
        self.meter = meter_provider.get_meter(
            instrumenting_module_name=self.config.service_name,
            instrumenting_library_version=self.config.service_version
        )
        
        self.logger.info("Metrics initialized")
    
    def _initialize_logging(self) -> None:
        """Initialize logging instrumentation."""
        LoggingInstrumentor().instrument()
        self.logger.info("Logging instrumentation initialized")
    
    def _instrument_libraries(self) -> None:
        """Instrument third-party libraries."""
        # Instrument FastAPI
        FastAPIInstrumentor.instrument()
        
        # Instrument Kafka
        AioKafkaInstrumentor().instrument()
        
        # Instrument HTTP client
        HTTPXClientInstrumentor().instrument()
        
        self.logger.info("Library instrumentation completed")
    
    def create_span(self, name: str, **kwargs) -> Any:
        """Create a new span."""
        if not self.tracer:
            return trace.NoOpTracer().start_span(name)
        
        return self.tracer.start_span(name, **kwargs)
    
    def create_counter(self, name: str, description: str = "") -> Any:
        """Create a counter metric."""
        if not self.meter:
            return None
        
        return self.meter.create_counter(
            name=name,
            description=description
        )
    
    def create_histogram(self, name: str, description: str = "") -> Any:
        """Create a histogram metric."""
        if not self.meter:
            return None
        
        return self.meter.create_histogram(
            name=name,
            description=description
        )
    
    def create_gauge(self, name: str, description: str = "") -> Any:
        """Create a gauge metric."""
        if not self.meter:
            return None
        
        return self.meter.create_up_down_counter(
            name=name,
            description=description
        )
    
    def shutdown(self) -> None:
        """Shutdown observability."""
        if not self._initialized:
            return
        
        try:
            # Shutdown tracer provider
            if self.config.enable_tracing:
                tracer_provider = trace.get_tracer_provider()
                if hasattr(tracer_provider, 'shutdown'):
                    tracer_provider.shutdown()
            
            # Shutdown meter provider
            if self.config.enable_metrics:
                meter_provider = self.meter._meter_provider
                if hasattr(meter_provider, 'shutdown'):
                    meter_provider.shutdown()
            
            self._initialized = False
            self.logger.info("OpenTelemetry observability shutdown completed")
            
        except Exception as e:
            self.logger.error(f"Failed to shutdown OpenTelemetry observability: {e}")


# Global observability manager
_observability_manager: Optional[ObservabilityManager] = None


def init_observability(config: ObservabilityConfig) -> ObservabilityManager:
    """Initialize observability for the enrichment service."""
    global _observability_manager
    
    if _observability_manager is None:
        _observability_manager = ObservabilityManager(config)
        _observability_manager.initialize()
    
    return _observability_manager


def get_observability_manager() -> Optional[ObservabilityManager]:
    """Get the global observability manager."""
    return _observability_manager


def shutdown_observability() -> None:
    """Shutdown observability."""
    global _observability_manager
    
    if _observability_manager:
        _observability_manager.shutdown()
        _observability_manager = None
