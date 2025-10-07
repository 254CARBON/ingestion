"""
Metrics endpoints for aggregation service.
"""

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List

from fastapi import APIRouter, HTTPException
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field

router = APIRouter()
logger = logging.getLogger(__name__)


class MetricsResponse(BaseModel):
    """Metrics response model."""
    
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    metrics: Dict[str, Any] = Field(..., description="Service metrics")
    statistics: Dict[str, Any] = Field(..., description="Service statistics")


@router.get("/", response_model=MetricsResponse)
async def get_metrics():
    """Get service metrics."""
    try:
        # Get service instances (would be injected in real implementation)
        from ..core.aggregator import AggregationService
        from ..consumers.kafka_consumer import KafkaConsumerService
        from ..producers.kafka_producer import KafkaProducerService
        
        # Initialize services for metrics collection
        aggregation_service = AggregationService()
        consumer_config = KafkaConsumerService.ConsumerConfig()
        producer_config = KafkaProducerService.ProducerConfig()
        
        kafka_consumer = KafkaConsumerService(consumer_config, aggregation_service)
        kafka_producer = KafkaProducerService(producer_config)
        
        # Collect metrics from all services
        aggregation_stats = aggregation_service.get_stats()
        consumer_stats = kafka_consumer.get_stats()
        producer_stats = kafka_producer.get_stats()
        
        # Combine metrics
        combined_metrics = {
            "aggregation": aggregation_stats,
            "consumer": consumer_stats,
            "producer": producer_stats
        }
        
        # Calculate overall statistics
        overall_stats = {
            "total_aggregations": aggregation_stats.get("total_aggregations", 0),
            "successful_aggregations": aggregation_stats.get("successful_aggregations", 0),
            "failed_aggregations": aggregation_stats.get("failed_aggregations", 0),
            "ohlc_bars_generated": aggregation_stats.get("ohlc_bars_generated", 0),
            "rolling_metrics_generated": aggregation_stats.get("rolling_metrics_generated", 0),
            "curve_prestage_generated": aggregation_stats.get("curve_prestage_generated", 0),
            "total_messages_consumed": consumer_stats.get("total_messages", 0),
            "total_messages_published": producer_stats.get("total_messages", 0),
            "average_processing_latency_ms": consumer_stats.get("average_latency_ms", 0),
            "average_publish_latency_ms": producer_stats.get("average_latency_ms", 0),
            "success_rate": aggregation_stats.get("success_rate", 0)
        }
        
        return MetricsResponse(
            metrics=combined_metrics,
            statistics=overall_stats
        )
        
    except Exception as e:
        logger.error(f"Failed to get metrics: {e}")
        raise HTTPException(status_code=500, detail="Failed to get metrics")


@router.get("/prometheus", response_class=PlainTextResponse)
async def get_prometheus_metrics():
    """Get Prometheus-formatted metrics."""
    try:
        # Get service instances (would be injected in real implementation)
        from ..core.aggregator import AggregationService
        from ..consumers.kafka_consumer import KafkaConsumerService
        from ..producers.kafka_producer import KafkaProducerService
        
        # Initialize services for metrics collection
        aggregation_service = AggregationService()
        consumer_config = KafkaConsumerService.ConsumerConfig()
        producer_config = KafkaProducerService.ProducerConfig()
        
        kafka_consumer = KafkaConsumerService(consumer_config, aggregation_service)
        kafka_producer = KafkaProducerService(producer_config)
        
        # Collect metrics
        aggregation_stats = aggregation_service.get_stats()
        consumer_stats = kafka_consumer.get_stats()
        producer_stats = kafka_producer.get_stats()
        
        # Format as Prometheus metrics
        prometheus_metrics = []
        
        # Aggregation metrics
        prometheus_metrics.append(f"# HELP aggregation_total_aggregations Total number of aggregations")
        prometheus_metrics.append(f"# TYPE aggregation_total_aggregations counter")
        prometheus_metrics.append(f"aggregation_total_aggregations {aggregation_stats.get('total_aggregations', 0)}")
        
        prometheus_metrics.append(f"# HELP aggregation_successful_aggregations Total successful aggregations")
        prometheus_metrics.append(f"# TYPE aggregation_successful_aggregations counter")
        prometheus_metrics.append(f"aggregation_successful_aggregations {aggregation_stats.get('successful_aggregations', 0)}")
        
        prometheus_metrics.append(f"# HELP aggregation_failed_aggregations Total failed aggregations")
        prometheus_metrics.append(f"# TYPE aggregation_failed_aggregations counter")
        prometheus_metrics.append(f"aggregation_failed_aggregations {aggregation_stats.get('failed_aggregations', 0)}")
        
        prometheus_metrics.append(f"# HELP aggregation_ohlc_bars_generated Total OHLC bars generated")
        prometheus_metrics.append(f"# TYPE aggregation_ohlc_bars_generated counter")
        prometheus_metrics.append(f"aggregation_ohlc_bars_generated {aggregation_stats.get('ohlc_bars_generated', 0)}")
        
        prometheus_metrics.append(f"# HELP aggregation_rolling_metrics_generated Total rolling metrics generated")
        prometheus_metrics.append(f"# TYPE aggregation_rolling_metrics_generated counter")
        prometheus_metrics.append(f"aggregation_rolling_metrics_generated {aggregation_stats.get('rolling_metrics_generated', 0)}")
        
        prometheus_metrics.append(f"# HELP aggregation_curve_prestage_generated Total curve pre-stage generated")
        prometheus_metrics.append(f"# TYPE aggregation_curve_prestage_generated counter")
        prometheus_metrics.append(f"aggregation_curve_prestage_generated {aggregation_stats.get('curve_prestage_generated', 0)}")
        
        # Consumer metrics
        prometheus_metrics.append(f"# HELP aggregation_consumer_messages_total Total messages consumed")
        prometheus_metrics.append(f"# TYPE aggregation_consumer_messages_total counter")
        prometheus_metrics.append(f"aggregation_consumer_messages_total {consumer_stats.get('total_messages', 0)}")
        
        prometheus_metrics.append(f"# HELP aggregation_consumer_latency_seconds Average consumer latency")
        prometheus_metrics.append(f"# TYPE aggregation_consumer_latency_seconds gauge")
        prometheus_metrics.append(f"aggregation_consumer_latency_seconds {consumer_stats.get('average_latency_ms', 0) / 1000}")
        
        # Producer metrics
        prometheus_metrics.append(f"# HELP aggregation_producer_messages_total Total messages published")
        prometheus_metrics.append(f"# TYPE aggregation_producer_messages_total counter")
        prometheus_metrics.append(f"aggregation_producer_messages_total {producer_stats.get('total_messages', 0)}")
        
        prometheus_metrics.append(f"# HELP aggregation_producer_latency_seconds Average producer latency")
        prometheus_metrics.append(f"# TYPE aggregation_producer_latency_seconds gauge")
        prometheus_metrics.append(f"aggregation_producer_latency_seconds {producer_stats.get('average_latency_ms', 0) / 1000}")
        
        return "\n".join(prometheus_metrics)
        
    except Exception as e:
        logger.error(f"Failed to get Prometheus metrics: {e}")
        raise HTTPException(status_code=500, detail="Failed to get Prometheus metrics")


@router.get("/stats", response_model=Dict[str, Any])
async def get_stats():
    """Get service statistics."""
    try:
        # Get service instances (would be injected in real implementation)
        from ..core.aggregator import AggregationService
        from ..consumers.kafka_consumer import KafkaConsumerService
        from ..producers.kafka_producer import KafkaProducerService
        
        # Initialize services for statistics collection
        aggregation_service = AggregationService()
        consumer_config = KafkaConsumerService.ConsumerConfig()
        producer_config = KafkaProducerService.ProducerConfig()
        
        kafka_consumer = KafkaConsumerService(consumer_config, aggregation_service)
        kafka_producer = KafkaProducerService(producer_config)
        
        # Collect statistics
        aggregation_stats = aggregation_service.get_stats()
        consumer_stats = kafka_consumer.get_stats()
        producer_stats = kafka_producer.get_stats()
        
        return {
            "aggregation": aggregation_stats,
            "consumer": consumer_stats,
            "producer": producer_stats,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
    except Exception as e:
        logger.error(f"Failed to get statistics: {e}")
        raise HTTPException(status_code=500, detail="Failed to get statistics")

