"""
Metrics endpoints for the Enrichment Service.
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


class MetricsResponse(BaseModel):
    """Metrics response model."""
    
    timestamp: str
    service_metrics: Dict[str, Any]
    enrichment_metrics: Dict[str, Any]
    taxonomy_metrics: Dict[str, Any]


@router.get("/", response_model=MetricsResponse)
async def get_metrics():
    """Get comprehensive metrics for the service."""
    try:
        enrichment_service = EnrichmentService()
        taxonomy_service = TaxonomyService()
        
        # Get service metrics
        service_metrics = {
            "uptime_seconds": 0.0,  # Would be calculated from start time
            "requests_total": 0,  # Would be tracked
            "requests_successful": 0,
            "requests_failed": 0,
            "active_connections": 0
        }
        
        # Get enrichment metrics
        enrichment_metrics = await enrichment_service.get_metrics()
        
        # Get taxonomy metrics
        taxonomy_metrics = await taxonomy_service.get_metrics()
        
        return MetricsResponse(
            timestamp=datetime.now(timezone.utc).isoformat(),
            service_metrics=service_metrics,
            enrichment_metrics=enrichment_metrics,
            taxonomy_metrics=taxonomy_metrics
        )
        
    except Exception as e:
        logger.error(f"Failed to get metrics: {e}")
        raise HTTPException(status_code=500, detail="Failed to get metrics")
