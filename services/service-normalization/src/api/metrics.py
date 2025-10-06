"""
Metrics endpoints for the Normalization Service.
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


class MetricsResponse(BaseModel):
    """Metrics response model."""
    
    timestamp: str
    service_metrics: Dict[str, Any]
    processing_metrics: Dict[str, Any]
    kafka_metrics: Dict[str, Any]


class ProcessingStatsResponse(BaseModel):
    """Processing statistics response model."""
    
    timestamp: str
    stats: Dict[str, Any]


@router.get("/", response_model=MetricsResponse)
async def get_metrics():
    """Get comprehensive metrics for the service."""
    try:
        normalization_service = NormalizationService()
        rules_engine = RulesEngine()
        validation_service = ValidationService()
        kafka_consumer = KafkaConsumerService()
        kafka_producer = KafkaProducerService()
        
        # Get service metrics
        service_metrics = {
            "uptime_seconds": 0.0,  # Would be calculated from start time
            "requests_total": 0,  # Would be tracked
            "requests_successful": 0,
            "requests_failed": 0,
            "active_connections": 0
        }
        
        # Get processing metrics
        processing_metrics = await normalization_service.get_processing_metrics()
        
        # Get Kafka metrics
        consumer_metrics = await kafka_consumer.get_metrics()
        producer_metrics = await kafka_producer.get_metrics()
        
        kafka_metrics = {
            "consumer": consumer_metrics,
            "producer": producer_metrics
        }
        
        return MetricsResponse(
            timestamp=datetime.now(timezone.utc).isoformat(),
            service_metrics=service_metrics,
            processing_metrics=processing_metrics,
            kafka_metrics=kafka_metrics
        )
        
    except Exception as e:
        logger.error(f"Failed to get metrics: {e}")
        raise HTTPException(status_code=500, detail="Failed to get metrics")


@router.get("/processing", response_model=ProcessingStatsResponse)
async def get_processing_stats():
    """Get processing statistics."""
    try:
        normalization_service = NormalizationService()
        
        stats = await normalization_service.get_processing_stats()
        
        return ProcessingStatsResponse(
            timestamp=datetime.now(timezone.utc).isoformat(),
            stats=stats
        )
        
    except Exception as e:
        logger.error(f"Failed to get processing stats: {e}")
        raise HTTPException(status_code=500, detail="Failed to get processing stats")


@router.get("/kafka")
async def get_kafka_metrics():
    """Get Kafka-specific metrics."""
    try:
        kafka_consumer = KafkaConsumerService()
        kafka_producer = KafkaProducerService()
        
        consumer_metrics = await kafka_consumer.get_metrics()
        producer_metrics = await kafka_producer.get_metrics()
        
        return {
            "consumer": consumer_metrics,
            "producer": producer_metrics,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
    except Exception as e:
        logger.error(f"Failed to get Kafka metrics: {e}")
        raise HTTPException(status_code=500, detail="Failed to get Kafka metrics")


@router.get("/rules")
async def get_rules_metrics():
    """Get rules engine metrics."""
    try:
        rules_engine = RulesEngine()
        
        metrics = await rules_engine.get_metrics()
        
        return metrics
        
    except Exception as e:
        logger.error(f"Failed to get rules metrics: {e}")
        raise HTTPException(status_code=500, detail="Failed to get rules metrics")


@router.get("/validation")
async def get_validation_metrics():
    """Get validation service metrics."""
    try:
        validation_service = ValidationService()
        
        metrics = await validation_service.get_metrics()
        
        return metrics
        
    except Exception as e:
        logger.error(f"Failed to get validation metrics: {e}")
        raise HTTPException(status_code=500, detail="Failed to get validation metrics")


@router.get("/throughput")
async def get_throughput_metrics():
    """Get throughput metrics."""
    try:
        normalization_service = NormalizationService()
        
        # Get throughput metrics
        throughput = await normalization_service.get_throughput_metrics()
        
        return {
            "throughput": throughput,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
    except Exception as e:
        logger.error(f"Failed to get throughput metrics: {e}")
        raise HTTPException(status_code=500, detail="Failed to get throughput metrics")


@router.get("/latency")
async def get_latency_metrics():
    """Get latency metrics."""
    try:
        normalization_service = NormalizationService()
        
        # Get latency metrics
        latency = await normalization_service.get_latency_metrics()
        
        return {
            "latency": latency,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
    except Exception as e:
        logger.error(f"Failed to get latency metrics: {e}")
        raise HTTPException(status_code=500, detail="Failed to get latency metrics")


@router.get("/errors")
async def get_error_metrics():
    """Get error metrics."""
    try:
        normalization_service = NormalizationService()
        validation_service = ValidationService()
        
        # Get error metrics
        normalization_errors = await normalization_service.get_error_metrics()
        validation_errors = await validation_service.get_error_metrics()
        
        return {
            "normalization_errors": normalization_errors,
            "validation_errors": validation_errors,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
    except Exception as e:
        logger.error(f"Failed to get error metrics: {e}")
        raise HTTPException(status_code=500, detail="Failed to get error metrics")
