"""
Quality score endpoints for data quality service.
"""

from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional

from fastapi import APIRouter, HTTPException, Query

router = APIRouter()


# In-memory quality score storage (for demonstration)
quality_scores: List[Dict[str, Any]] = []
MAX_QUALITY_SCORES = 10000


@router.get("/scores")
async def get_quality_scores(
    market: Optional[str] = Query(None, description="Market filter"),
    data_type: Optional[str] = Query(None, description="Data type filter"),
    start_time: Optional[str] = Query(None, description="Start time (ISO format)"),
    end_time: Optional[str] = Query(None, description="End time (ISO format)"),
    limit: int = Query(100, description="Maximum number of results")
) -> Dict[str, Any]:
    """
    Query quality scores over time.

    Args:
        market: Market filter
        data_type: Data type filter
        start_time: Start time filter
        end_time: End time filter
        limit: Maximum number of results

    Returns:
        Dict[str, Any]: Query results
    """
    try:
        # Filter quality scores
        filtered_scores = quality_scores.copy()

        if market:
            filtered_scores = [s for s in filtered_scores if s.get("market") == market]

        if data_type:
            filtered_scores = [s for s in filtered_scores if s.get("data_type") == data_type]

        # Apply limit
        filtered_scores = filtered_scores[:limit]

        # Calculate statistics
        if filtered_scores:
            scores = [s.get("score", 0) for s in filtered_scores]
            avg_score = sum(scores) / len(scores)
            min_score = min(scores)
            max_score = max(scores)
        else:
            avg_score = 0
            min_score = 0
            max_score = 0

        return {
            "scores": filtered_scores,
            "count": len(filtered_scores),
            "statistics": {
                "average": avg_score,
                "minimum": min_score,
                "maximum": max_score
            },
            "filters": {
                "market": market,
                "data_type": data_type
            },
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")


@router.get("/report")
async def generate_quality_report(
    market: Optional[str] = Query(None, description="Market filter"),
    hours: int = Query(24, description="Time window in hours")
) -> Dict[str, Any]:
    """
    Generate a quality report for the specified time window.

    Args:
        market: Market filter
        hours: Time window in hours

    Returns:
        Dict[str, Any]: Quality report
    """
    from ..main import anomaly_detector

    try:
        cutoff_time = datetime.now(timezone.utc) - timedelta(hours=hours)

        # Get anomaly detector stats
        detector_stats = anomaly_detector.get_stats() if anomaly_detector else {}

        # Filter quality scores for time window
        recent_scores = [
            s for s in quality_scores
            if datetime.fromisoformat(s.get("timestamp", "").replace("Z", "+00:00")) >= cutoff_time
        ]

        if market:
            recent_scores = [s for s in recent_scores if s.get("market") == market]

        # Calculate quality metrics
        if recent_scores:
            scores = [s.get("score", 0) for s in recent_scores]
            avg_quality = sum(scores) / len(scores)
            min_quality = min(scores)
            max_quality = max(scores)
            
            # Quality distribution
            high_quality = len([s for s in scores if s >= 0.9])
            medium_quality = len([s for s in scores if 0.7 <= s < 0.9])
            low_quality = len([s for s in scores if s < 0.7])
        else:
            avg_quality = 0
            min_quality = 0
            max_quality = 0
            high_quality = 0
            medium_quality = 0
            low_quality = 0

        report = {
            "report_period": {
                "start": cutoff_time.isoformat(),
                "end": datetime.now(timezone.utc).isoformat(),
                "hours": hours
            },
            "market": market or "all",
            "quality_metrics": {
                "average_quality_score": avg_quality,
                "minimum_quality_score": min_quality,
                "maximum_quality_score": max_quality,
                "total_records": len(recent_scores)
            },
            "quality_distribution": {
                "high_quality": high_quality,
                "medium_quality": medium_quality,
                "low_quality": low_quality
            },
            "anomaly_summary": {
                "total_anomalies": detector_stats.get("anomalies_detected", 0),
                "by_type": detector_stats.get("anomalies_by_type", {}),
                "by_severity": detector_stats.get("anomalies_by_severity", {}),
                "anomaly_rate": detector_stats.get("anomaly_rate", 0)
            },
            "recommendations": _generate_recommendations(avg_quality, detector_stats),
            "generated_at": datetime.now(timezone.utc).isoformat()
        }

        return report

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Report generation failed: {str(e)}")


def _generate_recommendations(avg_quality: float, detector_stats: Dict[str, Any]) -> List[str]:
    """Generate recommendations based on quality metrics."""
    recommendations = []

    if avg_quality < 0.7:
        recommendations.append("Average quality score is low. Review data sources and validation rules.")

    if avg_quality < 0.5:
        recommendations.append("CRITICAL: Quality score is critically low. Immediate investigation required.")

    anomaly_rate = detector_stats.get("anomaly_rate", 0)
    if anomaly_rate > 0.1:
        recommendations.append(f"High anomaly rate ({anomaly_rate:.2%}). Review anomaly patterns.")

    anomalies_by_type = detector_stats.get("anomalies_by_type", {})
    if "price_spike" in anomalies_by_type and anomalies_by_type["price_spike"] > 10:
        recommendations.append("Frequent price spikes detected. Check market conditions or data source.")

    if "missing_data" in anomalies_by_type and anomalies_by_type["missing_data"] > 5:
        recommendations.append("Missing data patterns detected. Verify connector health and scheduling.")

    if not recommendations:
        recommendations.append("Data quality is good. Continue monitoring.")

    return recommendations
