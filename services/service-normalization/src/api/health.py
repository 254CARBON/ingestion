"""
Health check endpoints for the Normalization Service.
"""

import logging
from datetime import datetime, timezone
from typing import Dict, Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from ..core.normalizer import NormalizationService
from ..core.rules_engine import RulesEngine
from ..core.validators import ValidationService
from ..consumers.kafka_consumer import KafkaConsumerService
from ..producers.kafka_producer import KafkaProducerService

router = APIRouter()
logger = logging.getLogger(__name__)


class HealthResponse(BaseModel):
    """Health check response model."""
    
    status: str
    timestamp: str
    version: str
    uptime_seconds: float
    services: Dict[str, Any]


class LivenessResponse(BaseModel):
    """Liveness check response model."""
    
    status: str
    timestamp: str


class ReadinessResponse(BaseModel):
    """Readiness check response model."""
    
    status: str
    timestamp: str
    checks: Dict[str, Any]


@router.get("/", response_model=HealthResponse)
async def health_check():
    """Comprehensive health check endpoint."""
    try:
        # Get service instances (would be injected in real implementation)
        normalization_service = NormalizationService()
        rules_engine = RulesEngine()
        validation_service = ValidationService()
        kafka_consumer = KafkaConsumerService()
        kafka_producer = KafkaProducerService()
        
        # Check service health
        normalization_health = await normalization_service.get_health_status()
        rules_health = await rules_engine.get_health_status()
        validation_health = await validation_service.get_health_status()
        consumer_health = await kafka_consumer.get_health_status()
        producer_health = await kafka_producer.get_health_status()
        
        # Calculate uptime (simplified)
        uptime_seconds = 0.0  # Would be calculated from start time
        
        return HealthResponse(
            status="healthy",
            timestamp=datetime.now(timezone.utc).isoformat(),
            version="1.0.0",
            uptime_seconds=uptime_seconds,
            services={
                "normalization": normalization_health,
                "rules_engine": rules_health,
                "validation": validation_health,
                "kafka_consumer": consumer_health,
                "kafka_producer": producer_health
            }
        )
        
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=503, detail="Service unhealthy")


@router.get("/live", response_model=LivenessResponse)
async def liveness_check():
    """Liveness check endpoint."""
    try:
        return LivenessResponse(
            status="alive",
            timestamp=datetime.now(timezone.utc).isoformat()
        )
    except Exception as e:
        logger.error(f"Liveness check failed: {e}")
        raise HTTPException(status_code=503, detail="Service not alive")


@router.get("/ready", response_model=ReadinessResponse)
async def readiness_check():
    """Readiness check endpoint."""
    try:
        # Get service instances (would be injected in real implementation)
        normalization_service = NormalizationService()
        rules_engine = RulesEngine()
        validation_service = ValidationService()
        kafka_consumer = KafkaConsumerService()
        kafka_producer = KafkaProducerService()
        
        # Check readiness
        normalization_ready = await normalization_service.is_ready()
        rules_ready = await rules_engine.is_ready()
        validation_ready = await validation_service.is_ready()
        consumer_ready = await kafka_consumer.is_ready()
        producer_ready = await kafka_producer.is_ready()
        
        checks = {
            "normalization": normalization_ready,
            "rules_engine": rules_ready,
            "validation": validation_ready,
            "kafka_consumer": consumer_ready,
            "kafka_producer": producer_ready
        }
        
        # Overall readiness
        overall_ready = all(checks.values())
        
        return ReadinessResponse(
            status="ready" if overall_ready else "not_ready",
            timestamp=datetime.now(timezone.utc).isoformat(),
            checks=checks
        )
        
    except Exception as e:
        logger.error(f"Readiness check failed: {e}")
        raise HTTPException(status_code=503, detail="Service not ready")
