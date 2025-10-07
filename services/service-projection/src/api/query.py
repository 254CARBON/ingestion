"""
Query endpoints for projection service.

This module provides API endpoints for querying OHLC bars, rolling metrics,
and curve data from the serving layer.
"""

from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

from fastapi import APIRouter, HTTPException, Query

router = APIRouter()


@router.get("/ohlc")
async def query_ohlc_bars(
    market: Optional[str] = Query(None, description="Market filter"),
    delivery_location: Optional[str] = Query(None, description="Delivery location filter"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    bar_type: str = Query("daily", description="Bar type (daily, hourly)"),
    limit: int = Query(1000, description="Maximum number of results"),
    use_cache: bool = Query(True, description="Use cache for query")
) -> Dict[str, Any]:
    """
    Query OHLC bars from the serving layer.

    Args:
        market: Market filter
        delivery_location: Delivery location filter
        start_date: Start date filter
        end_date: End date filter
        bar_type: Bar type (daily, hourly)
        limit: Maximum number of results
        use_cache: Whether to use cache

    Returns:
        Dict[str, Any]: Query results
    """
    from ..main import projection_service

    if not projection_service:
        raise HTTPException(status_code=503, detail="Projection service not available")

    try:
        results = await projection_service.query_ohlc_bars(
            market=market,
            delivery_location=delivery_location,
            start_date=start_date,
            end_date=end_date,
            bar_type=bar_type,
            use_cache=use_cache
        )

        return {
            "results": results,
            "count": len(results),
            "market": market,
            "delivery_location": delivery_location,
            "bar_type": bar_type,
            "cached": use_cache,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")


@router.get("/metrics")
async def query_rolling_metrics(
    market: Optional[str] = Query(None, description="Market filter"),
    delivery_location: Optional[str] = Query(None, description="Delivery location filter"),
    metric_type: Optional[str] = Query(None, description="Metric type filter"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    limit: int = Query(1000, description="Maximum number of results"),
    use_cache: bool = Query(True, description="Use cache for query")
) -> Dict[str, Any]:
    """
    Query rolling metrics from the serving layer.

    Args:
        market: Market filter
        delivery_location: Delivery location filter
        metric_type: Metric type filter
        start_date: Start date filter
        end_date: End date filter
        limit: Maximum number of results
        use_cache: Whether to use cache

    Returns:
        Dict[str, Any]: Query results
    """
    from ..main import projection_service

    if not projection_service:
        raise HTTPException(status_code=503, detail="Projection service not available")

    try:
        results = await projection_service.query_rolling_metrics(
            market=market,
            delivery_location=delivery_location,
            metric_type=metric_type,
            start_date=start_date,
            end_date=end_date,
            use_cache=use_cache
        )

        return {
            "results": results,
            "count": len(results),
            "market": market,
            "delivery_location": delivery_location,
            "metric_type": metric_type,
            "cached": use_cache,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")


@router.get("/curves")
async def query_curve_data(
    market: Optional[str] = Query(None, description="Market filter"),
    curve_type: Optional[str] = Query(None, description="Curve type filter"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    limit: int = Query(1000, description="Maximum number of results"),
    use_cache: bool = Query(True, description="Use cache for query")
) -> Dict[str, Any]:
    """
    Query curve data from the serving layer.

    Args:
        market: Market filter
        curve_type: Curve type filter
        start_date: Start date filter
        end_date: End date filter
        limit: Maximum number of results
        use_cache: Whether to use cache

    Returns:
        Dict[str, Any]: Query results
    """
    from ..main import projection_service

    if not projection_service:
        raise HTTPException(status_code=503, detail="Projection service not available")

    try:
        # For curve data, we need to query ClickHouse directly
        # since the projection service doesn't have a query method for curves yet
        if hasattr(projection_service, 'clickhouse_writer'):
            results = await projection_service.clickhouse_writer.query_curve_data(
                market=market,
                curve_type=curve_type,
                start_date=start_date,
                end_date=end_date,
                limit=limit
            )
        else:
            results = []

        return {
            "results": results,
            "count": len(results),
            "market": market,
            "curve_type": curve_type,
            "cached": use_cache,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")


@router.post("/refresh-views")
async def refresh_materialized_views() -> Dict[str, Any]:
    """
    Refresh materialized views in ClickHouse.

    Returns:
        Dict[str, Any]: Refresh result
    """
    from ..main import projection_service

    if not projection_service:
        raise HTTPException(status_code=503, detail="Projection service not available")

    try:
        result = await projection_service.manage_materialized_views()

        return {
            "success": True,
            "refreshed_views": result.get("refreshed_views", []),
            "errors": result.get("errors", []),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Refresh failed: {str(e)}")


@router.get("/stats")
async def get_table_stats(table: str = Query("gold_ohlc_bars", description="Table name")) -> Dict[str, Any]:
    """
    Get statistics for a ClickHouse table.

    Args:
        table: Table name

    Returns:
        Dict[str, Any]: Table statistics
    """
    from ..main import projection_service

    if not projection_service or not hasattr(projection_service, 'clickhouse_writer'):
        raise HTTPException(status_code=503, detail="ClickHouse writer not available")

    try:
        stats = await projection_service.clickhouse_writer.get_table_stats(table)

        return {
            "table": table,
            "stats": stats,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Stats query failed: {str(e)}")
