"""
Distributed tracing utilities for aggregation service.

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
            tracer = trace.get_tracer("aggregation-service")
            
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


class AggregationTracer:
    """Tracer for aggregation operations."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.tracer = trace.get_tracer("aggregation-service")
        self.kafka_propagator = KafkaTracePropagator()
    
    def trace_aggregation(self, aggregation_result: Any) -> Any:
        """
        Trace aggregation operation.
        
        Args:
            aggregation_result: Aggregation result to trace
            
        Returns:
            Any: Aggregation result with trace information
        """
        try:
            with self.tracer.start_as_current_span(
                "aggregation.aggregate",
                kind=SpanKind.INTERNAL,
                attributes={
                    "aggregation.operation": "aggregate",
                    "aggregation.ohlc_bars_count": len(aggregation_result.ohlc_bars),
                    "aggregation.rolling_metrics_count": len(aggregation_result.rolling_metrics),
                    "aggregation.curve_prestage_count": len(aggregation_result.curve_prestage)
                }
            ) as span:
                # Add aggregation-specific attributes
                if aggregation_result.ohlc_bars:
                    span.set_attribute("aggregation.ohlc_bars_generated", True)
                    span.set_attribute("aggregation.ohlc_bars_sample", 
                                     json.dumps(aggregation_result.ohlc_bars[0].dict()))
                
                if aggregation_result.rolling_metrics:
                    span.set_attribute("aggregation.rolling_metrics_generated", True)
                    span.set_attribute("aggregation.rolling_metrics_sample", 
                                     json.dumps(aggregation_result.rolling_metrics[0].dict()))
                
                if aggregation_result.curve_prestage:
                    span.set_attribute("aggregation.curve_prestage_generated", True)
                    span.set_attribute("aggregation.curve_prestage_sample", 
                                     json.dumps(aggregation_result.curve_prestage[0].dict()))
                
                return aggregation_result
                
        except Exception as e:
            self.logger.error("Failed to trace aggregation operation", error=str(e))
            return aggregation_result
    
    def trace_ohlc_generation(self, data: Dict[str, Any], ohlc_bars: List[Any]) -> None:
        """
        Trace OHLC bar generation.
        
        Args:
            data: Data being aggregated
            ohlc_bars: Generated OHLC bars
        """
        try:
            with self.tracer.start_as_current_span(
                "aggregation.ohlc",
                kind=SpanKind.INTERNAL,
                attributes={
                    "aggregation.ohlc.market": data.get("market", "unknown"),
                    "aggregation.ohlc.location": data.get("delivery_location", "unknown"),
                    "aggregation.ohlc.bars_count": len(ohlc_bars),
                    "aggregation.ohlc.date": data.get("delivery_date", "unknown")
                }
            ) as span:
                # Add OHLC-specific attributes
                if "price" in data and data["price"] is not None:
                    span.set_attribute("aggregation.ohlc.input_price", float(data["price"]))
                
                if "quantity" in data and data["quantity"] is not None:
                    span.set_attribute("aggregation.ohlc.input_quantity", float(data["quantity"]))
                
                if ohlc_bars:
                    bar = ohlc_bars[0]
                    span.set_attribute("aggregation.ohlc.open_price", bar.open_price)
                    span.set_attribute("aggregation.ohlc.high_price", bar.high_price)
                    span.set_attribute("aggregation.ohlc.low_price", bar.low_price)
                    span.set_attribute("aggregation.ohlc.close_price", bar.close_price)
                    span.set_attribute("aggregation.ohlc.volume", bar.volume)
                    span.set_attribute("aggregation.ohlc.vwap", bar.vwap)
                
        except Exception as e:
            self.logger.error("Failed to trace OHLC generation", error=str(e))
    
    def trace_rolling_metrics(self, data: Dict[str, Any], rolling_metrics: List[Any]) -> None:
        """
        Trace rolling metrics generation.
        
        Args:
            data: Data being aggregated
            rolling_metrics: Generated rolling metrics
        """
        try:
            with self.tracer.start_as_current_span(
                "aggregation.rolling",
                kind=SpanKind.INTERNAL,
                attributes={
                    "aggregation.rolling.market": data.get("market", "unknown"),
                    "aggregation.rolling.location": data.get("delivery_location", "unknown"),
                    "aggregation.rolling.metrics_count": len(rolling_metrics),
                    "aggregation.rolling.date": data.get("delivery_date", "unknown")
                }
            ) as span:
                # Add rolling metrics-specific attributes
                if "price" in data and data["price"] is not None:
                    span.set_attribute("aggregation.rolling.input_price", float(data["price"]))
                
                if "quantity" in data and data["quantity"] is not None:
                    span.set_attribute("aggregation.rolling.input_quantity", float(data["quantity"]))
                
                if rolling_metrics:
                    for metric in rolling_metrics:
                        span.set_attribute(f"aggregation.rolling.{metric.metric_type}", metric.metric_value)
                
        except Exception as e:
            self.logger.error("Failed to trace rolling metrics", error=str(e))
    
    def trace_curve_prestage(self, data: Dict[str, Any], curve_prestage: List[Any]) -> None:
        """
        Trace curve pre-stage generation.
        
        Args:
            data: Data being aggregated
            curve_prestage: Generated curve pre-stage
        """
        try:
            with self.tracer.start_as_current_span(
                "aggregation.curve",
                kind=SpanKind.INTERNAL,
                attributes={
                    "aggregation.curve.market": data.get("market", "unknown"),
                    "aggregation.curve.date": data.get("delivery_date", "unknown"),
                    "aggregation.curve.hour": data.get("delivery_hour", "unknown"),
                    "aggregation.curve.records_count": len(curve_prestage)
                }
            ) as span:
                # Add curve pre-stage-specific attributes
                if "price" in data and data["price"] is not None:
                    span.set_attribute("aggregation.curve.input_price", float(data["price"]))
                
                if "quantity" in data and data["quantity"] is not None:
                    span.set_attribute("aggregation.curve.input_quantity", float(data["quantity"]))
                
                if curve_prestage:
                    curve = curve_prestage[0]
                    span.set_attribute("aggregation.curve.curve_type", curve.curve_type)
                    span.set_attribute("aggregation.curve.output_price", curve.price)
                    span.set_attribute("aggregation.curve.output_quantity", curve.quantity)
                
        except Exception as e:
            self.logger.error("Failed to trace curve pre-stage", error=str(e))


# Global tracer instance
_aggregation_tracer: Optional[AggregationTracer] = None


def get_aggregation_tracer() -> AggregationTracer:
    """Get the global aggregation tracer."""
    global _aggregation_tracer
    
    if _aggregation_tracer is None:
        _aggregation_tracer = AggregationTracer()
    
    return _aggregation_tracer


def init_kafka_tracing() -> None:
    """Initialize Kafka tracing instrumentation."""
    try:
        KafkaInstrumentor().instrument()
        logging.getLogger(__name__).info("Kafka tracing instrumentation initialized")
    except Exception as e:
        logging.getLogger(__name__).error(f"Failed to initialize Kafka tracing: {e}")

