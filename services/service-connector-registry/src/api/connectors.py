"""
Connector management endpoints for the Connector Registry Service.
"""

import logging
from typing import Dict, Any, List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from ..core.registry import ConnectorRegistryService
from ..core.discovery import ConnectorDiscoveryService

router = APIRouter()
logger = logging.getLogger(__name__)


class ConnectorInfo(BaseModel):
    """Connector information model."""
    
    name: str
    version: str
    market: str
    mode: str
    enabled: bool
    output_topic: str
    schema: str
    retries: int
    backoff_seconds: int
    tenant_strategy: str
    transforms: List[str]
    owner: str
    description: Optional[str] = None
    tags: List[str] = []


class ConnectorListResponse(BaseModel):
    """Connector list response model."""
    
    connectors: List[ConnectorInfo]
    total_count: int
    enabled_count: int
    disabled_count: int


class ConnectorDetailResponse(BaseModel):
    """Connector detail response model."""
    
    connector: ConnectorInfo
    health_status: Dict[str, Any]
    capabilities: Dict[str, Any]
    validation_result: Dict[str, Any]


@router.get("/", response_model=ConnectorListResponse)
async def list_connectors(
    market: Optional[str] = Query(None, description="Filter by market"),
    mode: Optional[str] = Query(None, description="Filter by mode"),
    enabled: Optional[bool] = Query(None, description="Filter by enabled status"),
    owner: Optional[str] = Query(None, description="Filter by owner")
):
    """List all connectors with optional filtering."""
    try:
        registry_service = ConnectorRegistryService()
        
        # Get all connectors
        all_connectors = registry_service.load_all_connectors()
        
        # Apply filters
        filtered_connectors = []
        for name, metadata in all_connectors.items():
            if market and metadata.market != market:
                continue
            if mode and metadata.mode != mode:
                continue
            if enabled is not None and metadata.enabled != enabled:
                continue
            if owner and metadata.owner != owner:
                continue
            
            filtered_connectors.append(ConnectorInfo(
                name=metadata.name,
                version=metadata.version,
                market=metadata.market,
                mode=metadata.mode,
                enabled=metadata.enabled,
                output_topic=metadata.output_topic,
                schema=metadata.schema,
                retries=metadata.retries,
                backoff_seconds=metadata.backoff_seconds,
                tenant_strategy=metadata.tenant_strategy,
                transforms=metadata.transforms,
                owner=metadata.owner,
                description=metadata.description,
                tags=metadata.tags
            ))
        
        # Calculate counts
        enabled_count = sum(1 for c in filtered_connectors if c.enabled)
        disabled_count = len(filtered_connectors) - enabled_count
        
        return ConnectorListResponse(
            connectors=filtered_connectors,
            total_count=len(filtered_connectors),
            enabled_count=enabled_count,
            disabled_count=disabled_count
        )
        
    except Exception as e:
        logger.error(f"Failed to list connectors: {e}")
        raise HTTPException(status_code=500, detail="Failed to list connectors")


@router.get("/{connector_name}", response_model=ConnectorDetailResponse)
async def get_connector(connector_name: str):
    """Get detailed information about a specific connector."""
    try:
        registry_service = ConnectorRegistryService()
        
        # Get connector metadata
        metadata = registry_service.get_connector(connector_name)
        if not metadata:
            raise HTTPException(status_code=404, detail="Connector not found")
        
        # Get health status
        health_status = registry_service.get_connector_health_status(connector_name)
        
        # Get capabilities
        capabilities = registry_service.get_connector_capabilities(connector_name)
        
        # Get validation result
        validation_result = registry_service.validate_connector(connector_name)
        
        return ConnectorDetailResponse(
            connector=ConnectorInfo(
                name=metadata.name,
                version=metadata.version,
                market=metadata.market,
                mode=metadata.mode,
                enabled=metadata.enabled,
                output_topic=metadata.output_topic,
                schema=metadata.schema,
                retries=metadata.retries,
                backoff_seconds=metadata.backoff_seconds,
                tenant_strategy=metadata.tenant_strategy,
                transforms=metadata.transforms,
                owner=metadata.owner,
                description=metadata.description,
                tags=metadata.tags
            ),
            health_status=health_status,
            capabilities=capabilities,
            validation_result=validation_result
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get connector {connector_name}: {e}")
        raise HTTPException(status_code=500, detail="Failed to get connector")


@router.get("/{connector_name}/health")
async def get_connector_health(connector_name: str):
    """Get health status for a specific connector."""
    try:
        registry_service = ConnectorRegistryService()
        
        health_status = registry_service.get_connector_health_status(connector_name)
        if not health_status:
            raise HTTPException(status_code=404, detail="Connector not found")
        
        return health_status
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get connector health {connector_name}: {e}")
        raise HTTPException(status_code=500, detail="Failed to get connector health")


@router.get("/{connector_name}/capabilities")
async def get_connector_capabilities(connector_name: str):
    """Get capabilities for a specific connector."""
    try:
        registry_service = ConnectorRegistryService()
        
        capabilities = registry_service.get_connector_capabilities(connector_name)
        if not capabilities:
            raise HTTPException(status_code=404, detail="Connector not found")
        
        return capabilities
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get connector capabilities {connector_name}: {e}")
        raise HTTPException(status_code=500, detail="Failed to get connector capabilities")


@router.get("/{connector_name}/validate")
async def validate_connector(connector_name: str):
    """Validate a specific connector."""
    try:
        registry_service = ConnectorRegistryService()
        
        validation_result = registry_service.validate_connector(connector_name)
        if not validation_result:
            raise HTTPException(status_code=404, detail="Connector not found")
        
        return validation_result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to validate connector {connector_name}: {e}")
        raise HTTPException(status_code=500, detail="Failed to validate connector")


@router.get("/markets/list")
async def list_markets():
    """List all available markets."""
    try:
        registry_service = ConnectorRegistryService()
        
        all_connectors = registry_service.load_all_connectors()
        markets = list(set(metadata.market for metadata in all_connectors.values()))
        
        return {"markets": sorted(markets)}
        
    except Exception as e:
        logger.error(f"Failed to list markets: {e}")
        raise HTTPException(status_code=500, detail="Failed to list markets")


@router.get("/modes/list")
async def list_modes():
    """List all available execution modes."""
    try:
        registry_service = ConnectorRegistryService()
        
        all_connectors = registry_service.load_all_connectors()
        modes = list(set(metadata.mode for metadata in all_connectors.values()))
        
        return {"modes": sorted(modes)}
        
    except Exception as e:
        logger.error(f"Failed to list modes: {e}")
        raise HTTPException(status_code=500, detail="Failed to list modes")


@router.get("/owners/list")
async def list_owners():
    """List all connector owners."""
    try:
        registry_service = ConnectorRegistryService()
        
        all_connectors = registry_service.load_all_connectors()
        owners = list(set(metadata.owner for metadata in all_connectors.values()))
        
        return {"owners": sorted(owners)}
        
    except Exception as e:
        logger.error(f"Failed to list owners: {e}")
        raise HTTPException(status_code=500, detail="Failed to list owners")
