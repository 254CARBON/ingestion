"""
Metrics endpoints for data quality service.
"""

from datetime import datetime, timezone
from typing import Dict, Any

from fastapi import APIRouter

router = APIRouter()


@router.get("/metrics")
async def get_metrics() -> str:
    """
    Prometheus metrics endpoint.

    Returns:
        str: Prometheus metrics in text format
    """
    from ..main import anomaly_detector, kafka_consumers, kafka_producer

    metrics_data = []

    # Anomaly detector metrics
    if anomaly_detector:
        stats = anomaly_detector.get_stats()

        metrics_data.append(f"""# HELP dq_records_analyzed_total Total records analyzed
# TYPE dq_records_analyzed_total counter
dq_records_analyzed_total {stats['total_records_analyzed']}""")

        metrics_data.append(f"""# HELP dq_anomalies_detected_total Total anomalies detected
# TYPE dq_anomalies_detected_total counter
dq_anomalies_detected_total {stats['anomalies_detected']}""")

        metrics_data.append(f"""# HELP dq_anomaly_rate Anomaly detection rate
# TYPE dq_anomaly_rate gauge
dq_anomaly_rate {stats['anomaly_rate']}""")

        # Anomalies by type
        for anomaly_type, count in stats.get("anomalies_by_type", {}).items():
            metrics_data.append(f"""# HELP dq_anomalies_by_type_total Anomalies by type
# TYPE dq_anomalies_by_type_total counter
dq_anomalies_by_type_total{{type="{anomaly_type}"}} {count}""")

        # Anomalies by severity
        for severity, count in stats.get("anomalies_by_severity", {}).items():
            metrics_data.append(f"""# HELP dq_anomalies_by_severity_total Anomalies by severity
# TYPE dq_anomalies_by_severity_total counter
dq_anomalies_by_severity_total{{severity="{severity}"}} {count}""")

    # Kafka consumer metrics
    for topic, consumer in kafka_consumers.items():
        if consumer:
            stats = consumer.get_stats()

            metrics_data.append(f"""# HELP dq_consumer_messages_total Total messages consumed
# TYPE dq_consumer_messages_total counter
dq_consumer_messages_total{{topic="{topic}"}} {stats['total_messages']}""")

            metrics_data.append(f"""# HELP dq_consumer_success_rate Consumer success rate
# TYPE dq_consumer_success_rate gauge
dq_consumer_success_rate{{topic="{topic}"}} {stats['success_rate']}""")

    # Kafka producer metrics
    if kafka_producer:
        stats = kafka_producer.get_stats()

        metrics_data.append(f"""# HELP dq_producer_publishes_total Total anomalies published
# TYPE dq_producer_publishes_total counter
dq_producer_publishes_total {stats['successful_publishes']}""")

        metrics_data.append(f"""# HELP dq_producer_success_rate Producer success rate
# TYPE dq_producer_success_rate gauge
dq_producer_success_rate {stats['success_rate']}""")

    # Combine all metrics
    metrics_text = "\n".join(metrics_data) + "\n"

    return metrics_text


@router.get("/metrics/json")
async def get_metrics_json() -> Dict[str, Any]:
    """
    Get metrics in JSON format.

    Returns:
        Dict[str, Any]: Metrics in JSON format
    """
    from ..main import anomaly_detector, kafka_consumers, kafka_producer

    metrics = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "anomaly_detector": None,
        "kafka_consumers": {},
        "kafka_producer": None
    }

    # Anomaly detector metrics
    if anomaly_detector:
        metrics["anomaly_detector"] = anomaly_detector.get_stats()

    # Kafka consumer metrics
    for topic, consumer in kafka_consumers.items():
        if consumer:
            metrics["kafka_consumers"][topic] = consumer.get_stats()

    # Kafka producer metrics
    if kafka_producer:
        metrics["kafka_producer"] = kafka_producer.get_stats()

    return metrics
