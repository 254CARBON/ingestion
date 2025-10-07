"""
Health check endpoints for aggregation service.
"""

import logging
from datetime import datetime, timezone
from typing import Any, Dict

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

router = APIRouter()
logger = logging.getLogger(__name__)


class HealthResponse(BaseModel):
    """Health check response model."""
    
    status: str = Field(..., description="Service status")
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    version: str = Field("1.0.0", description="Service version")
    uptime: str = Field(..., description="Service uptime")


class LivenessResponse(BaseModel):
    """Liveness check response model."""
    
    status: str = Field(..., description="Liveness status")
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class ReadinessResponse(BaseModel):
    """Readiness check response model."""
    
    status: str = Field(..., description="Readiness status")
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    checks: Dict[str, bool] = Field(..., description="Component health checks")


@router.get("/", response_model=HealthResponse)
async def health_check():
    """Health check endpoint."""
    try:
        # Get service instances (would be injected in real implementation)
        from ..core.aggregator import AggregationService
        from ..consumers.kafka_consumer import KafkaConsumerService
        from ..producers.kafka_producer import KafkaProducerService
        
        # Initialize services for health check
        aggregation_service = AggregationService()
        consumer_config = KafkaConsumerService.ConsumerConfig()
        producer_config = KafkaProducerService.ProducerConfig()
        
        kafka_consumer = KafkaConsumerService(consumer_config, aggregation_service)
        kafka_producer = KafkaProducerService(producer_config)
        
        # Check service health
        aggregation_healthy = True  # Simplified check
        consumer_healthy = await kafka_consumer.is_ready()
        producer_healthy = await kafka_producer.is_ready()
        
        overall_healthy = aggregation_healthy and consumer_healthy and producer_healthy
        
        return HealthResponse(
            status="healthy" if overall_healthy else "unhealthy",
            uptime="unknown"  # Would calculate actual uptime in production
        )
        
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=503, detail="Service unhealthy")


@router.get("/live", response_model=LivenessResponse)
async def liveness_check():
    """Liveness check endpoint."""
    try:
        # Simple liveness check - service is alive if it can respond
        return LivenessResponse(status="alive")
        
    except Exception as e:
        logger.error(f"Liveness check failed: {e}")
        raise HTTPException(status_code=503, detail="Service not alive")


@router.get("/ready", response_model=ReadinessResponse)
async def readiness_check():
    """Readiness check endpoint."""
    try:
        # Get service instances (would be injected in real implementation)
        from ..core.aggregator import AggregationService
        from ..consumers.kafka_consumer import KafkaConsumerService
        from ..producers.kafka_producer import KafkaProducerService
        
        # Initialize services for readiness check
        aggregation_service = AggregationService()
        consumer_config = KafkaConsumerService.ConsumerConfig()
        producer_config = KafkaProducerService.ProducerConfig()
        
        kafka_consumer = KafkaConsumerService(consumer_config, aggregation_service)
        kafka_producer = KafkaProducerService(producer_config)
        
        # Check readiness
        aggregation_ready = True  # Simplified check
        consumer_ready = await kafka_consumer.is_ready()
        producer_ready = await kafka_producer.is_ready()
        
        checks = {
            "aggregation": aggregation_ready,
            "kafka_consumer": consumer_ready,
            "kafka_producer": producer_ready
        }
        
        # Overall readiness
        overall_ready = all(checks.values())
        
        return ReadinessResponse(
            status="ready" if overall_ready else "not_ready",
            checks=checks
        )
        
    except Exception as e:
        logger.error(f"Readiness check failed: {e}")
        raise HTTPException(status_code=503, detail="Service not ready")

