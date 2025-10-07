"""
Metrics endpoints for projection service.
"""

from datetime import datetime, timezone
from typing import Dict, Any

from fastapi import APIRouter
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

router = APIRouter()


@router.get("/metrics")
async def get_metrics() -> str:
    """
    Prometheus metrics endpoint.

    Returns:
        str: Prometheus metrics in text format
    """
    from ..main import projection_service, kafka_consumers

    # Generate metrics from service statistics
    metrics_data = []

    # Projection service metrics
    if projection_service:
        stats = projection_service.get_stats()

        metrics_data.append(f"""# HELP projection_total_operations_total Total projection operations
# TYPE projection_total_operations_total counter
projection_total_operations_total {stats['total_projections']}""")

        metrics_data.append(f"""# HELP projection_successful_operations_total Successful projection operations
# TYPE projection_successful_operations_total counter
projection_successful_operations_total {stats['successful_projections']}""")

        metrics_data.append(f"""# HELP projection_failed_operations_total Failed projection operations
# TYPE projection_failed_operations_total counter
projection_failed_operations_total {stats['failed_projections']}""")

        metrics_data.append(f"""# HELP projection_records_projected_total Total records projected
# TYPE projection_records_projected_total counter
projection_records_projected_total {stats['records_projected']}""")

        metrics_data.append(f"""# HELP projection_cache_entries_created_total Cache entries created
# TYPE projection_cache_entries_created_total counter
projection_cache_entries_created_total {stats['cache_entries_created']}""")

        metrics_data.append(f"""# HELP projection_average_latency_seconds Average projection latency
# TYPE projection_average_latency_seconds gauge
projection_average_latency_seconds {stats['average_projection_time_ms'] / 1000}""")

        metrics_data.append(f"""# HELP projection_success_rate Success rate of projection operations
# TYPE projection_success_rate gauge
projection_success_rate {stats['success_rate']}""")

    # Kafka consumer metrics
    for name, consumer in kafka_consumers.items():
        if consumer:
            stats = consumer.get_stats()

            metrics_data.append(f"""# HELP kafka_consumer_{name}_messages_total Total messages consumed
# TYPE kafka_consumer_{name}_messages_total counter
kafka_consumer_{name}_messages_total {stats['total_messages']}""")

            metrics_data.append(f"""# HELP kafka_consumer_{name}_successful_projections_total Successful projections
# TYPE kafka_consumer_{name}_successful_projections_total counter
kafka_consumer_{name}_successful_projections_total {stats['successful_projections']}""")

            metrics_data.append(f"""# HELP kafka_consumer_{name}_failed_projections_total Failed projections
# TYPE kafka_consumer_{name}_failed_projections_total counter
kafka_consumer_{name}_failed_projections_total {stats['failed_projections']}""")

            metrics_data.append(f"""# HELP kafka_consumer_{name}_average_latency_seconds Average processing latency
# TYPE kafka_consumer_{name}_average_latency_seconds gauge
kafka_consumer_{name}_average_latency_seconds {stats['average_latency_ms'] / 1000}""")

            metrics_data.append(f"""# HELP kafka_consumer_{name}_success_rate Success rate
# TYPE kafka_consumer_{name}_success_rate gauge
kafka_consumer_{name}_success_rate {stats['success_rate']}""")

    # ClickHouse writer metrics
    if projection_service and hasattr(projection_service, 'clickhouse_writer'):
        ch_stats = projection_service.clickhouse_writer.get_stats()

        metrics_data.append(f"""# HELP clickhouse_writes_total Total ClickHouse writes
# TYPE clickhouse_writes_total counter
clickhouse_writes_total {ch_stats['total_writes']}""")

        metrics_data.append(f"""# HELP clickhouse_successful_writes_total Successful ClickHouse writes
# TYPE clickhouse_successful_writes_total counter
clickhouse_successful_writes_total {ch_stats['successful_writes']}""")

        metrics_data.append(f"""# HELP clickhouse_failed_writes_total Failed ClickHouse writes
# TYPE clickhouse_failed_writes_total counter
clickhouse_failed_writes_total {ch_stats['failed_writes']}""")

        metrics_data.append(f"""# HELP clickhouse_rows_written_total Total rows written to ClickHouse
# TYPE clickhouse_rows_written_total counter
clickhouse_rows_written_total {ch_stats['rows_written']}""")

        metrics_data.append(f"""# HELP clickhouse_average_write_latency_seconds Average write latency
# TYPE clickhouse_average_write_latency_seconds gauge
clickhouse_average_write_latency_seconds {ch_stats['average_write_latency_ms'] / 1000}""")

    # Cache manager metrics
    if projection_service and hasattr(projection_service, 'cache_manager'):
        cache_stats = projection_service.cache_manager.get_stats()

        metrics_data.append(f"""# HELP cache_total_operations_total Total cache operations
# TYPE cache_total_operations_total counter
cache_total_operations_total {cache_stats['total_operations']}""")

        metrics_data.append(f"""# HELP cache_successful_operations_total Successful cache operations
# TYPE cache_successful_operations_total counter
cache_successful_operations_total {cache_stats['successful_operations']}""")

        metrics_data.append(f"""# HELP cache_hits_total Cache hits
# TYPE cache_hits_total counter
cache_hits_total {cache_stats['cache_hits']}""")

        metrics_data.append(f"""# HELP cache_misses_total Cache misses
# TYPE cache_misses_total counter
cache_misses_total {cache_stats['cache_misses']}""")

        metrics_data.append(f"""# HELP cache_hit_rate Cache hit rate
# TYPE cache_hit_rate gauge
cache_hit_rate {cache_stats['cache_hit_rate']}""")

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
    from ..main import projection_service, kafka_consumers

    metrics = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "projection_service": None,
        "kafka_consumers": {},
        "clickhouse_writer": None,
        "cache_manager": None
    }

    # Projection service metrics
    if projection_service:
        metrics["projection_service"] = projection_service.get_stats()

    # Kafka consumer metrics
    for name, consumer in kafka_consumers.items():
        if consumer:
            metrics["kafka_consumers"][name] = consumer.get_stats()

    # ClickHouse writer metrics
    if projection_service and hasattr(projection_service, 'clickhouse_writer'):
        metrics["clickhouse_writer"] = projection_service.clickhouse_writer.get_stats()

    # Cache manager metrics
    if projection_service and hasattr(projection_service, 'cache_manager'):
        metrics["cache_manager"] = projection_service.cache_manager.get_stats()

    return metrics
