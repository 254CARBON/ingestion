"""
Cache management endpoints for projection service.

This module provides API endpoints for managing the Redis cache,
including invalidation and refresh operations.
"""

from datetime import datetime, timezone
from typing import Dict, Any, List

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

router = APIRouter()


class InvalidateRequest(BaseModel):
    """Request model for cache invalidation."""

    pattern: str
    description: str = "Cache invalidation request"


class RefreshRequest(BaseModel):
    """Request model for cache refresh."""

    market: str
    data_types: List[str]
    description: str = "Cache refresh request"


@router.post("/invalidate")
async def invalidate_cache(request: InvalidateRequest) -> Dict[str, Any]:
    """
    Invalidate cache entries matching a pattern.

    Args:
        request: Invalidation request

    Returns:
        Dict[str, Any]: Invalidation result
    """
    from ..main import projection_service

    if not projection_service or not hasattr(projection_service, 'cache_manager'):
        raise HTTPException(status_code=503, detail="Cache manager not available")

    try:
        invalidated_count = await projection_service.cache_manager.invalidate_pattern(request.pattern)

        return {
            "success": True,
            "pattern": request.pattern,
            "invalidated_count": invalidated_count,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Invalidation failed: {str(e)}")


@router.post("/invalidate/market")
async def invalidate_market_cache(market: str = Query(..., description="Market to invalidate")) -> Dict[str, Any]:
    """
    Invalidate all cache entries for a specific market.

    Args:
        market: Market identifier

    Returns:
        Dict[str, Any]: Invalidation result
    """
    from ..main import projection_service

    if not projection_service or not hasattr(projection_service, 'cache_manager'):
        raise HTTPException(status_code=503, detail="Cache manager not available")

    try:
        invalidated_count = await projection_service.cache_manager.invalidate_by_market(market)

        return {
            "success": True,
            "market": market,
            "invalidated_count": invalidated_count,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Market invalidation failed: {str(e)}")


@router.post("/invalidate/location")
async def invalidate_location_cache(location: str = Query(..., description="Location to invalidate")) -> Dict[str, Any]:
    """
    Invalidate all cache entries for a specific location.

    Args:
        location: Location identifier

    Returns:
        Dict[str, Any]: Invalidation result
    """
    from ..main import projection_service

    if not projection_service or not hasattr(projection_service, 'cache_manager'):
        raise HTTPException(status_code=503, detail="Cache manager not available")

    try:
        invalidated_count = await projection_service.cache_manager.invalidate_by_location(location)

        return {
            "success": True,
            "location": location,
            "invalidated_count": invalidated_count,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Location invalidation failed: {str(e)}")


@router.post("/refresh")
async def refresh_cache(request: RefreshRequest) -> Dict[str, Any]:
    """
    Refresh cache for specific market and data types.

    Args:
        request: Refresh request

    Returns:
        Dict[str, Any]: Refresh result
    """
    from ..main import projection_service

    if not projection_service or not hasattr(projection_service, 'cache_manager'):
        raise HTTPException(status_code=503, detail="Cache manager not available")

    try:
        prefetched_count = await projection_service.cache_manager.prefetch_hot_data(
            request.market,
            request.data_types
        )

        return {
            "success": True,
            "market": request.market,
            "data_types": request.data_types,
            "prefetched_count": prefetched_count,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Refresh failed: {str(e)}")


@router.post("/clear")
async def clear_cache() -> Dict[str, Any]:
    """
    Clear all cache entries.

    Returns:
        Dict[str, Any]: Clear result
    """
    from ..main import projection_service

    if not projection_service or not hasattr(projection_service, 'cache_manager'):
        raise HTTPException(status_code=503, detail="Cache manager not available")

    try:
        success = await projection_service.cache_manager.clear_all()

        return {
            "success": success,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Clear failed: {str(e)}")


@router.get("/info")
async def get_cache_info() -> Dict[str, Any]:
    """
    Get Redis cache information.

    Returns:
        Dict[str, Any]: Cache information
    """
    from ..main import projection_service

    if not projection_service or not hasattr(projection_service, 'cache_manager'):
        raise HTTPException(status_code=503, detail="Cache manager not available")

    try:
        info = await projection_service.cache_manager.get_cache_info()

        return {
            "info": info,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Info query failed: {str(e)}")


@router.get("/stats")
async def get_cache_stats() -> Dict[str, Any]:
    """
    Get cache manager statistics.

    Returns:
        Dict[str, Any]: Cache statistics
    """
    from ..main import projection_service

    if not projection_service or not hasattr(projection_service, 'cache_manager'):
        raise HTTPException(status_code=503, detail="Cache manager not available")

    try:
        stats = projection_service.cache_manager.get_stats()

        return {
            "stats": stats,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Stats query failed: {str(e)}")
