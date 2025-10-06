"""
Health check endpoints for the Connector Registry Service.
"""

import logging
from datetime import datetime, timezone
from typing import Dict, Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from ..core.registry import ConnectorRegistryService
from ..core.discovery import ConnectorDiscoveryService

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
        registry_service = ConnectorRegistryService()
        discovery_service = ConnectorDiscoveryService()
        
        # Check service health
        registry_health = await registry_service.get_health_status()
        discovery_health = await discovery_service.get_health_status()
        
        # Calculate uptime (simplified)
        uptime_seconds = 0.0  # Would be calculated from start time
        
        return HealthResponse(
            status="healthy",
            timestamp=datetime.now(timezone.utc).isoformat(),
            version="1.0.0",
            uptime_seconds=uptime_seconds,
            services={
                "registry": registry_health,
                "discovery": discovery_health
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
        registry_service = ConnectorRegistryService()
        discovery_service = ConnectorDiscoveryService()
        
        # Check readiness
        registry_ready = await registry_service.is_ready()
        discovery_ready = await discovery_service.is_ready()
        
        checks = {
            "registry": registry_ready,
            "discovery": discovery_ready
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
