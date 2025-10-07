"""
Distributed tracing utilities for enrichment service.

This module provides utilities for propagating trace context through
Kafka messages and maintaining distributed tracing across services.
"""

import json
import logging
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone

from opentelemetry import trace
from opentelemetry.propagate import extract, inject
from opentelemetry.trace import SpanKind, Status, StatusCode
from opentelemetry.instrumentation.kafka import KafkaInstrumentor


class TraceContext:
    """Trace context for distributed tracing."""
    
    def __init__(self, trace_id: str, span_id: str, baggage: Optional[Dict[str, str]] = None):
        self.trace_id = trace_id
        self.span_id = span_id
        self.baggage = baggage or {}
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert trace context to dictionary."""
        return {
            "trace_id": self.trace_id,
            "span_id": self.span_id,
            "baggage": self.baggage,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TraceContext":
        """Create trace context from dictionary."""
        return cls(
            trace_id=data.get("trace_id", ""),
            span_id=data.get("span_id", ""),
            baggage=data.get("baggage", {})
        )
    
    @classmethod
    def from_current_span(cls) -> "TraceContext":
        """Create trace context from current span."""
        current_span = trace.get_current_span()
        if current_span and current_span.is_recording():
            span_context = current_span.get_span_context()
            return cls(
                trace_id=format(span_context.trace_id, '032x'),
                span_id=format(span_context.span_id, '016x')
            )
        return cls("", "")


class KafkaTracePropagator:
    """Kafka trace context propagator."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def inject_trace_context(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """
        Inject trace context into Kafka message.
        
        Args:
            message: Kafka message to inject trace context into
            
        Returns:
            Dict[str, Any]: Message with injected trace context
        """
        try:
            # Get current trace context
            trace_context = TraceContext.from_current_span()
            
            # Add trace context to message
            message["_trace_context"] = trace_context.to_dict()
            
            # Add trace ID to message for easier correlation
            if trace_context.trace_id:
                message["trace_id"] = trace_context.trace_id
            
            self.logger.debug("Injected trace context into message", 
                            trace_id=trace_context.trace_id,
                            span_id=trace_context.span_id)
            
            return message
            
        except Exception as e:
            self.logger.error("Failed to inject trace context", error=str(e))
            return message
    
    def extract_trace_context(self, message: Dict[str, Any]) -> Optional[TraceContext]:
        """
        Extract trace context from Kafka message.
        
        Args:
            message: Kafka message to extract trace context from
            
        Returns:
            Optional[TraceContext]: Extracted trace context or None
        """
        try:
            if "_trace_context" in message:
                trace_data = message["_trace_context"]
                trace_context = TraceContext.from_dict(trace_data)
                
                self.logger.debug("Extracted trace context from message",
                                trace_id=trace_context.trace_id,
                                span_id=trace_context.span_id)
                
                return trace_context
            
            return None
            
        except Exception as e:
            self.logger.error("Failed to extract trace context", error=str(e))
            return None
    
    def create_child_span(self, message: Dict[str, Any], operation_name: str) -> Any:
        """
        Create a child span for processing a Kafka message.
        
        Args:
            message: Kafka message being processed
            operation_name: Name of the operation
            
        Returns:
            Any: OpenTelemetry span
        """
        try:
            # Extract trace context from message
            trace_context = self.extract_trace_context(message)
            
            # Get tracer
            tracer = trace.get_tracer("enrichment-service")
            
            # Create span
            with tracer.start_as_current_span(
                operation_name,
                kind=SpanKind.CONSUMER,
                attributes={
                    "messaging.system": "kafka",
                    "messaging.destination": message.get("topic", "unknown"),
                    "messaging.operation": "receive",
                    "messaging.message_id": message.get("event_id", "unknown"),
                    "tenant_id": message.get("tenant_id", "unknown"),
                    "market": message.get("market", "unknown"),
                    "data_type": message.get("data_type", "unknown")
                }
            ) as span:
                # Set trace context if available
                if trace_context and trace_context.trace_id:
                    span.set_attribute("trace.parent_trace_id", trace_context.trace_id)
                    span.set_attribute("trace.parent_span_id", trace_context.span_id)
                
                return span
                
        except Exception as e:
            self.logger.error("Failed to create child span", error=str(e))
            return trace.NoOpTracer().start_span(operation_name)


class EnrichmentTracer:
    """Tracer for enrichment operations."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.tracer = trace.get_tracer("enrichment-service")
        self.kafka_propagator = KafkaTracePropagator()
    
    def trace_enrichment(self, enriched_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Trace enrichment operation.
        
        Args:
            enriched_data: Enriched data to trace
            
        Returns:
            Dict[str, Any]: Enriched data with trace information
        """
        try:
            with self.tracer.start_as_current_span(
                "enrichment.enrich",
                kind=SpanKind.INTERNAL,
                attributes={
                    "enrichment.operation": "enrich",
                    "enrichment.market": enriched_data.get("market", "unknown"),
                    "enrichment.data_type": enriched_data.get("data_type", "unknown"),
                    "enrichment.taxonomy_tags_count": len(enriched_data.get("taxonomy_tags", [])),
                    "enrichment.semantic_tags_count": len(enriched_data.get("semantic_tags", [])),
                    "enrichment.score": enriched_data.get("enrichment_score", 0.0)
                }
            ) as span:
                # Add enrichment-specific attributes
                if "taxonomy_tags" in enriched_data:
                    span.set_attribute("enrichment.taxonomy_tags", 
                                     json.dumps(enriched_data["taxonomy_tags"]))
                
                if "semantic_tags" in enriched_data:
                    span.set_attribute("enrichment.semantic_tags", 
                                     json.dumps(enriched_data["semantic_tags"]))
                
                if "geospatial_data" in enriched_data:
                    span.set_attribute("enrichment.geospatial_enriched", True)
                
                # Inject trace context into enriched data
                enriched_data = self.kafka_propagator.inject_trace_context(enriched_data)
                
                return enriched_data
                
        except Exception as e:
            self.logger.error("Failed to trace enrichment operation", error=str(e))
            return enriched_data
    
    def trace_taxonomy_application(self, data: Dict[str, Any], taxonomy_tags: List[str]) -> None:
        """
        Trace taxonomy application.
        
        Args:
            data: Data being enriched
            taxonomy_tags: Applied taxonomy tags
        """
        try:
            with self.tracer.start_as_current_span(
                "enrichment.taxonomy",
                kind=SpanKind.INTERNAL,
                attributes={
                    "enrichment.taxonomy.market": data.get("market", "unknown"),
                    "enrichment.taxonomy.data_type": data.get("data_type", "unknown"),
                    "enrichment.taxonomy.tags_count": len(taxonomy_tags),
                    "enrichment.taxonomy.tags": json.dumps(taxonomy_tags)
                }
            ) as span:
                # Add taxonomy-specific attributes
                if "price" in data and data["price"] is not None:
                    span.set_attribute("enrichment.taxonomy.price", float(data["price"]))
                
                if "quantity" in data and data["quantity"] is not None:
                    span.set_attribute("enrichment.taxonomy.quantity", float(data["quantity"]))
                
                if "delivery_hour" in data and data["delivery_hour"] is not None:
                    span.set_attribute("enrichment.taxonomy.delivery_hour", int(data["delivery_hour"]))
                
        except Exception as e:
            self.logger.error("Failed to trace taxonomy application", error=str(e))
    
    def trace_semantic_tagging(self, data: Dict[str, Any], semantic_tags: List[str]) -> None:
        """
        Trace semantic tagging.
        
        Args:
            data: Data being enriched
            semantic_tags: Applied semantic tags
        """
        try:
            with self.tracer.start_as_current_span(
                "enrichment.semantic",
                kind=SpanKind.INTERNAL,
                attributes={
                    "enrichment.semantic.market": data.get("market", "unknown"),
                    "enrichment.semantic.data_type": data.get("data_type", "unknown"),
                    "enrichment.semantic.tags_count": len(semantic_tags),
                    "enrichment.semantic.tags": json.dumps(semantic_tags)
                }
            ) as span:
                # Add semantic-specific attributes
                if "price" in data and data["price"] is not None:
                    price = float(data["price"])
                    if price > 100:
                        span.set_attribute("enrichment.semantic.price_tier", "high")
                    elif price < 20:
                        span.set_attribute("enrichment.semantic.price_tier", "low")
                    else:
                        span.set_attribute("enrichment.semantic.price_tier", "medium")
                
                if "quantity" in data and data["quantity"] is not None:
                    quantity = float(data["quantity"])
                    if quantity > 1000:
                        span.set_attribute("enrichment.semantic.volume_tier", "large")
                    elif quantity < 100:
                        span.set_attribute("enrichment.semantic.volume_tier", "small")
                    else:
                        span.set_attribute("enrichment.semantic.volume_tier", "medium")
                
        except Exception as e:
            self.logger.error("Failed to trace semantic tagging", error=str(e))
    
    def trace_geospatial_enrichment(self, data: Dict[str, Any], geospatial_data: Optional[Dict[str, Any]]) -> None:
        """
        Trace geospatial enrichment.
        
        Args:
            data: Data being enriched
            geospatial_data: Geospatial enrichment data
        """
        try:
            with self.tracer.start_as_current_span(
                "enrichment.geospatial",
                kind=SpanKind.INTERNAL,
                attributes={
                    "enrichment.geospatial.market": data.get("market", "unknown"),
                    "enrichment.geospatial.location": data.get("delivery_location", "unknown"),
                    "enrichment.geospatial.enriched": geospatial_data is not None
                }
            ) as span:
                if geospatial_data:
                    span.set_attribute("enrichment.geospatial.region", 
                                     geospatial_data.get("region", "unknown"))
                    span.set_attribute("enrichment.geospatial.timezone", 
                                     geospatial_data.get("timezone", "unknown"))
                    
                    if "coordinates" in geospatial_data:
                        coords = geospatial_data["coordinates"]
                        span.set_attribute("enrichment.geospatial.latitude", coords[0])
                        span.set_attribute("enrichment.geospatial.longitude", coords[1])
                
        except Exception as e:
            self.logger.error("Failed to trace geospatial enrichment", error=str(e))


# Global tracer instance
_enrichment_tracer: Optional[EnrichmentTracer] = None


def get_enrichment_tracer() -> EnrichmentTracer:
    """Get the global enrichment tracer."""
    global _enrichment_tracer
    
    if _enrichment_tracer is None:
        _enrichment_tracer = EnrichmentTracer()
    
    return _enrichment_tracer


def init_kafka_tracing() -> None:
    """Initialize Kafka tracing instrumentation."""
    try:
        KafkaInstrumentor().instrument()
        logging.getLogger(__name__).info("Kafka tracing instrumentation initialized")
    except Exception as e:
        logging.getLogger(__name__).error(f"Failed to initialize Kafka tracing: {e}")
