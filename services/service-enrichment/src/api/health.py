"""
Health check endpoints for the Enrichment Service.
"""

import logging
from datetime import datetime, timezone
from typing import Dict, Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from ..core.enricher import EnrichmentService
from ..core.taxonomy import TaxonomyService

router = APIRouter()
logger = logging.getLogger(__name__)


class HealthResponse(BaseModel):
    """Health check response model."""
    
    status: str
    timestamp: str
    version: str
    uptime_seconds: float
    services: Dict[str, Any]


@router.get("/", response_model=HealthResponse)
async def health_check():
    """Comprehensive health check endpoint."""
    try:
        # Get service instances (would be injected in real implementation)
        enrichment_service = EnrichmentService()
        taxonomy_service = TaxonomyService()
        
        # Check service health
        enrichment_health = await enrichment_service.get_health_status()
        taxonomy_health = await taxonomy_service.get_health_status()
        
        # Calculate uptime (simplified)
        uptime_seconds = 0.0  # Would be calculated from start time
        
        return HealthResponse(
            status="healthy",
            timestamp=datetime.now(timezone.utc).isoformat(),
            version="1.0.0",
            uptime_seconds=uptime_seconds,
            services={
                "enrichment": enrichment_health,
                "taxonomy": taxonomy_health
            }
        )
        
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=503, detail="Service unhealthy")


@router.get("/live")
async def liveness_check():
    """Liveness check endpoint."""
    try:
        return {
            "status": "alive",
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        logger.error(f"Liveness check failed: {e}")
        raise HTTPException(status_code=503, detail="Service not alive")


@router.get("/ready")
async def readiness_check():
    """Readiness check endpoint."""
    try:
        # Get service instances (would be injected in real implementation)
        enrichment_service = EnrichmentService()
        taxonomy_service = TaxonomyService()
        
        # Check readiness
        enrichment_ready = await enrichment_service.is_ready()
        taxonomy_ready = await taxonomy_service.is_ready()
        
        checks = {
            "enrichment": enrichment_ready,
            "taxonomy": taxonomy_ready
        }
        
        # Overall readiness
        overall_ready = all(checks.values())
        
        return {
            "status": "ready" if overall_ready else "not_ready",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "checks": checks
        }
        
    except Exception as e:
        logger.error(f"Readiness check failed: {e}")
        raise HTTPException(status_code=503, detail="Service not ready")
