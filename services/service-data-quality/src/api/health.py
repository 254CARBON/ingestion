"""
Health check endpoints for data quality service.
"""

from datetime import datetime, timezone
from typing import Dict, Any

from fastapi import APIRouter

router = APIRouter()


@router.get("/health")
async def health_check() -> Dict[str, Any]:
    """
    Health check endpoint.

    Returns:
        Dict[str, Any]: Health status information
    """
    from ..main import anomaly_detector, kafka_consumers, kafka_producer

    health_info = {
        "status": "healthy",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "service": "data-quality",
        "version": "1.0.0"
    }

    # Check anomaly detector
    if anomaly_detector:
        try:
            stats = anomaly_detector.get_stats()
            health_info["anomaly_detector"] = {
                "status": "healthy",
                "stats": stats
            }
        except Exception as e:
            health_info["anomaly_detector"] = {
                "status": "unhealthy",
                "error": str(e)
            }
    else:
        health_info["anomaly_detector"] = {"status": "not_initialized"}

    # Check Kafka consumers
    kafka_status = {}
    for topic, consumer in kafka_consumers.items():
        if consumer:
            try:
                is_ready = await consumer.is_ready()
                stats = consumer.get_stats()
                kafka_status[topic] = {
                    "status": "healthy" if is_ready else "unhealthy",
                    "stats": stats
                }
            except Exception as e:
                kafka_status[topic] = {
                    "status": "error",
                    "error": str(e)
                }
        else:
            kafka_status[topic] = {"status": "not_initialized"}

    health_info["kafka_consumers"] = kafka_status

    # Check Kafka producer
    if kafka_producer:
        try:
            is_ready = await kafka_producer.is_ready()
            stats = kafka_producer.get_stats()
            health_info["kafka_producer"] = {
                "status": "healthy" if is_ready else "unhealthy",
                "stats": stats
            }
        except Exception as e:
            health_info["kafka_producer"] = {
                "status": "error",
                "error": str(e)
            }
    else:
        health_info["kafka_producer"] = {"status": "not_initialized"}

    # Overall status
    all_healthy = True

    if health_info["anomaly_detector"]["status"] != "healthy":
        all_healthy = False

    for consumer_status in kafka_status.values():
        if consumer_status["status"] not in ["healthy", "not_initialized"]:
            all_healthy = False

    if health_info["kafka_producer"]["status"] not in ["healthy", "not_initialized"]:
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
    from ..main import anomaly_detector, kafka_consumers, kafka_producer

    readiness_info = {
        "ready": True,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

    # Check if all critical components are ready
    checks = []

    # Check anomaly detector
    if anomaly_detector:
        checks.append(True)
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

    # Check Kafka producer
    if kafka_producer:
        try:
            is_ready = await kafka_producer.is_ready()
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
