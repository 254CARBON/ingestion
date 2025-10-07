"""
Anomaly query endpoints for data quality service.
"""

from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional

from fastapi import APIRouter, HTTPException, Query

router = APIRouter()


# In-memory anomaly storage (for demonstration; in production, use database)
recent_anomalies: List[Dict[str, Any]] = []
MAX_RECENT_ANOMALIES = 1000


@router.get("")
async def get_anomalies(
    market: Optional[str] = Query(None, description="Market filter"),
    anomaly_type: Optional[str] = Query(None, description="Anomaly type filter"),
    severity: Optional[str] = Query(None, description="Severity filter"),
    start_time: Optional[str] = Query(None, description="Start time (ISO format)"),
    end_time: Optional[str] = Query(None, description="End time (ISO format)"),
    limit: int = Query(100, description="Maximum number of results")
) -> Dict[str, Any]:
    """
    Query detected anomalies.

    Args:
        market: Market filter
        anomaly_type: Anomaly type filter
        severity: Severity filter
        start_time: Start time filter
        end_time: End time filter
        limit: Maximum number of results

    Returns:
        Dict[str, Any]: Query results
    """
    try:
        # Filter anomalies
        filtered_anomalies = recent_anomalies.copy()

        if market:
            filtered_anomalies = [a for a in filtered_anomalies if a.get("market") == market]

        if anomaly_type:
            filtered_anomalies = [a for a in filtered_anomalies if a.get("anomaly_type") == anomaly_type]

        if severity:
            filtered_anomalies = [a for a in filtered_anomalies if a.get("severity") == severity]

        if start_time:
            start_dt = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
            filtered_anomalies = [
                a for a in filtered_anomalies
                if datetime.fromisoformat(a.get("detected_at", "").replace("Z", "+00:00")) >= start_dt
            ]

        if end_time:
            end_dt = datetime.fromisoformat(end_time.replace("Z", "+00:00"))
            filtered_anomalies = [
                a for a in filtered_anomalies
                if datetime.fromisoformat(a.get("detected_at", "").replace("Z", "+00:00")) <= end_dt
            ]

        # Apply limit
        filtered_anomalies = filtered_anomalies[:limit]

        return {
            "anomalies": filtered_anomalies,
            "count": len(filtered_anomalies),
            "filters": {
                "market": market,
                "anomaly_type": anomaly_type,
                "severity": severity,
                "start_time": start_time,
                "end_time": end_time
            },
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")


@router.get("/summary")
async def get_anomaly_summary(
    hours: int = Query(24, description="Time window in hours")
) -> Dict[str, Any]:
    """
    Get anomaly summary for the specified time window.

    Args:
        hours: Time window in hours

    Returns:
        Dict[str, Any]: Anomaly summary
    """
    try:
        cutoff_time = datetime.now(timezone.utc) - timedelta(hours=hours)

        # Filter recent anomalies
        recent = [
            a for a in recent_anomalies
            if datetime.fromisoformat(a.get("detected_at", "").replace("Z", "+00:00")) >= cutoff_time
        ]

        # Calculate summary statistics
        summary = {
            "total_anomalies": len(recent),
            "by_type": {},
            "by_market": {},
            "by_severity": {},
            "time_window_hours": hours,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

        for anomaly in recent:
            # Count by type
            anomaly_type = anomaly.get("anomaly_type", "unknown")
            summary["by_type"][anomaly_type] = summary["by_type"].get(anomaly_type, 0) + 1

            # Count by market
            market = anomaly.get("market", "UNKNOWN")
            summary["by_market"][market] = summary["by_market"].get(market, 0) + 1

            # Count by severity
            severity = anomaly.get("severity", "unknown")
            summary["by_severity"][severity] = summary["by_severity"].get(severity, 0) + 1

        return summary

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Summary query failed: {str(e)}")
