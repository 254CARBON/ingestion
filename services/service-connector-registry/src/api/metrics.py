"""
Metrics endpoints for the Connector Registry Service.
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


class MetricsResponse(BaseModel):
    """Metrics response model."""
    
    timestamp: str
    service_metrics: Dict[str, Any]
    connector_metrics: Dict[str, Any]
    discovery_metrics: Dict[str, Any]


class RegistryStatsResponse(BaseModel):
    """Registry statistics response model."""
    
    timestamp: str
    stats: Dict[str, Any]


@router.get("/", response_model=MetricsResponse)
async def get_metrics():
    """Get comprehensive metrics for the service."""
    try:
        registry_service = ConnectorRegistryService()
        discovery_service = ConnectorDiscoveryService()
        
        # Get service metrics
        service_metrics = {
            "uptime_seconds": 0.0,  # Would be calculated from start time
            "requests_total": 0,  # Would be tracked
            "requests_successful": 0,
            "requests_failed": 0,
            "active_connections": 0
        }
        
        # Get connector metrics
        connector_metrics = registry_service.get_registry_stats()
        
        # Get discovery metrics
        discovery_metrics = await discovery_service.get_metrics()
        
        return MetricsResponse(
            timestamp=datetime.now(timezone.utc).isoformat(),
            service_metrics=service_metrics,
            connector_metrics=connector_metrics,
            discovery_metrics=discovery_metrics
        )
        
    except Exception as e:
        logger.error(f"Failed to get metrics: {e}")
        raise HTTPException(status_code=500, detail="Failed to get metrics")


@router.get("/registry", response_model=RegistryStatsResponse)
async def get_registry_stats():
    """Get registry statistics."""
    try:
        registry_service = ConnectorRegistryService()
        
        stats = registry_service.get_registry_stats()
        
        return RegistryStatsResponse(
            timestamp=datetime.now(timezone.utc).isoformat(),
            stats=stats
        )
        
    except Exception as e:
        logger.error(f"Failed to get registry stats: {e}")
        raise HTTPException(status_code=500, detail="Failed to get registry stats")


@router.get("/connectors")
async def get_connector_metrics():
    """Get connector-specific metrics."""
    try:
        registry_service = ConnectorRegistryService()
        
        all_connectors = registry_service.load_all_connectors()
        
        metrics = {
            "total_connectors": len(all_connectors),
            "enabled_connectors": len([c for c in all_connectors.values() if c.enabled]),
            "disabled_connectors": len([c for c in all_connectors.values() if not c.enabled]),
            "markets": {},
            "modes": {},
            "owners": {}
        }
        
        # Count by market
        for metadata in all_connectors.values():
            market = metadata.market
            if market not in metrics["markets"]:
                metrics["markets"][market] = 0
            metrics["markets"][market] += 1
        
        # Count by mode
        for metadata in all_connectors.values():
            mode = metadata.mode
            if mode not in metrics["modes"]:
                metrics["modes"][mode] = 0
            metrics["modes"][mode] += 1
        
        # Count by owner
        for metadata in all_connectors.values():
            owner = metadata.owner
            if owner not in metrics["owners"]:
                metrics["owners"][owner] = 0
            metrics["owners"][owner] += 1
        
        return metrics
        
    except Exception as e:
        logger.error(f"Failed to get connector metrics: {e}")
        raise HTTPException(status_code=500, detail="Failed to get connector metrics")


@router.get("/discovery")
async def get_discovery_metrics():
    """Get discovery service metrics."""
    try:
        discovery_service = ConnectorDiscoveryService()
        
        metrics = await discovery_service.get_metrics()
        
        return metrics
        
    except Exception as e:
        logger.error(f"Failed to get discovery metrics: {e}")
        raise HTTPException(status_code=500, detail="Failed to get discovery metrics")


@router.get("/health/summary")
async def get_health_summary():
    """Get health summary across all connectors."""
    try:
        registry_service = ConnectorRegistryService()
        
        all_connectors = registry_service.load_all_connectors()
        
        summary = {
            "total": len(all_connectors),
            "healthy": 0,
            "unhealthy": 0,
            "unknown": 0,
            "by_market": {},
            "by_mode": {}
        }
        
        # Check health for each connector
        for name, metadata in all_connectors.items():
            try:
                health_status = registry_service.get_connector_health_status(name)
                if health_status and health_status.get("status") == "healthy":
                    summary["healthy"] += 1
                else:
                    summary["unhealthy"] += 1
            except Exception:
                summary["unknown"] += 1
            
            # Count by market
            market = metadata.market
            if market not in summary["by_market"]:
                summary["by_market"][market] = {"total": 0, "healthy": 0, "unhealthy": 0, "unknown": 0}
            summary["by_market"][market]["total"] += 1
            
            # Count by mode
            mode = metadata.mode
            if mode not in summary["by_mode"]:
                summary["by_mode"][mode] = {"total": 0, "healthy": 0, "unhealthy": 0, "unknown": 0}
            summary["by_mode"][mode]["total"] += 1
        
        return summary
        
    except Exception as e:
        logger.error(f"Failed to get health summary: {e}")
        raise HTTPException(status_code=500, detail="Failed to get health summary")
