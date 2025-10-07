"""
Health check endpoints for projection service.
"""

import asyncio
from datetime import datetime, timezone
from typing import Dict, Any

from fastapi import APIRouter, HTTPException

router = APIRouter()


@router.get("/health")
async def health_check() -> Dict[str, Any]:
    """
    Health check endpoint.

    Returns:
        Dict[str, Any]: Health status information
    """
    from ..main import projection_service, kafka_consumers

    health_info = {
        "status": "healthy",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "service": "projection",
        "version": "1.0.0"
    }

    # Check projection service
    if projection_service:
        try:
            # Check if projection service is functional
            stats = projection_service.get_stats()
            health_info["projection_service"] = {
                "status": "healthy",
                "stats": stats
            }
        except Exception as e:
            health_info["projection_service"] = {
                "status": "unhealthy",
                "error": str(e)
            }
    else:
        health_info["projection_service"] = {
            "status": "not_initialized"
        }

    # Check Kafka consumers
    kafka_status = {}
    for name, consumer in kafka_consumers.items():
        if consumer:
            try:
                is_ready = await consumer.is_ready()
                stats = consumer.get_stats()
                kafka_status[name] = {
                    "status": "healthy" if is_ready else "unhealthy",
                    "stats": stats
                }
            except Exception as e:
                kafka_status[name] = {
                    "status": "error",
                    "error": str(e)
                }
        else:
            kafka_status[name] = {"status": "not_initialized"}

    health_info["kafka_consumers"] = kafka_status

    # Overall status
    all_healthy = True

    if health_info["projection_service"]["status"] != "healthy":
        all_healthy = False

    for consumer_status in kafka_status.values():
        if consumer_status["status"] not in ["healthy", "not_initialized"]:
            all_healthy = False

    if not all_healthy:
        health_info["status"] = "unhealthy"

    return health_info


@router.get("/readiness")
async def readiness_check() -> Dict[str, Any]:
    """
    Readiness check endpoint for Kubernetes.

    Returns:
        Dict[str, Any]: Readiness status
    """
    from ..main import projection_service, kafka_consumers

    readiness_info = {
        "ready": True,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

    # Check if all critical components are ready
    checks = []

    # Check projection service
    if projection_service:
        try:
            checks.append(True)  # Projection service is initialized
        except Exception:
            checks.append(False)
    else:
        checks.append(False)

    # Check Kafka consumers
    for consumer in kafka_consumers.values():
        if consumer:
            try:
                is_ready = await consumer.is_ready()
                checks.append(is_ready)
            except Exception:
                checks.append(False)
        else:
            checks.append(False)

    readiness_info["ready"] = all(checks)
    readiness_info["checks"] = len(checks)
    readiness_info["passed"] = sum(checks)

    return readiness_info


@router.get("/liveness")
async def liveness_check() -> Dict[str, Any]:
    """
    Liveness check endpoint for Kubernetes.

    Returns:
        Dict[str, Any]: Liveness status
    """
    return {
        "alive": True,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
