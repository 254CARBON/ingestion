"""
Projection Service - Main application entry point.

This service projects aggregated market data to the serving layer using
ClickHouse for storage and Redis for caching.
"""

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Dict, Any

import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from .api.health import router as health_router
from .api.metrics import router as metrics_router
from .api.query import router as query_router
from .api.cache import router as cache_router
from .core.projector import ProjectionService
from .consumers.ohlc_consumer import KafkaOHLCConsumer, OHLCConsumerConfig
from .consumers.metrics_consumer import KafkaMetricsConsumer, MetricsConsumerConfig
from .consumers.curve_consumer import KafkaCurveConsumer, CurveConsumerConfig


class ServiceConfig(BaseModel):
    """Service configuration."""

    host: str = "0.0.0.0"
    port: int = 8513
    kafka_bootstrap_servers: str = "localhost:9092"
    clickhouse_dsn: str = "clickhouse://default@localhost:9000/carbon_ingestion"
    redis_url: str = "redis://localhost:6379/0"
    input_topics: Dict[str, str] = {
        "ohlc": "aggregation.ohlc.bars.v1",
        "rolling": "aggregation.rolling.metrics.v1",
        "curve": "aggregation.curve.prestage.v1"
    }
    projection_config: str = "configs/projection_policies.yaml"
    enable_cors: bool = True
    log_level: str = "INFO"
    parallelism: int = 4


# Global service instances
projection_service: ProjectionService = None
kafka_consumers = {
    "ohlc": None,
    "metrics": None,
    "curve": None
}


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    global projection_service, kafka_consumers

    # Startup
    try:
        logging.info("Starting Projection Service")

        # Initialize projection service
        projection_service = ProjectionService()

        # Initialize Kafka consumers
        kafka_consumers["ohlc"] = KafkaOHLCConsumer(
            OHLCConsumerConfig(
                bootstrap_servers=app.state.config.kafka_bootstrap_servers,
                topic=app.state.config.input_topics["ohlc"],
                group_id="projection-ohlc-consumer"
            ),
            projection_service
        )

        kafka_consumers["metrics"] = KafkaMetricsConsumer(
            MetricsConsumerConfig(
                bootstrap_servers=app.state.config.kafka_bootstrap_servers,
                topic=app.state.config.input_topics["rolling"],
                group_id="projection-metrics-consumer"
            ),
            projection_service
        )

        kafka_consumers["curve"] = KafkaCurveConsumer(
            CurveConsumerConfig(
                bootstrap_servers=app.state.config.kafka_bootstrap_servers,
                topic=app.state.config.input_topics["curve"],
                group_id="projection-curve-consumer"
            ),
            projection_service
        )

        # Start Kafka consumers
        await kafka_consumers["ohlc"].start()
        await kafka_consumers["metrics"].start()
        await kafka_consumers["curve"].start()

        # Start consumer tasks
        ohlc_task = asyncio.create_task(kafka_consumers["ohlc"].consume_messages())
        metrics_task = asyncio.create_task(kafka_consumers["metrics"].consume_messages())
        curve_task = asyncio.create_task(kafka_consumers["curve"].consume_messages())

        logging.info("Projection Service started successfully")

        yield

    except Exception as e:
        logging.error("Failed to start Projection Service", error=str(e))
        raise

    finally:
        # Shutdown
        logging.info("Stopping Projection Service")

        # Stop consumer tasks
        if 'ohlc_task' in locals():
            ohlc_task.cancel()
        if 'metrics_task' in locals():
            metrics_task.cancel()
        if 'curve_task' in locals():
            curve_task.cancel()

        # Stop Kafka consumers
        for consumer_name, consumer in kafka_consumers.items():
            if consumer:
                try:
                    await consumer.stop()
                    logging.info(f"Stopped {consumer_name} consumer")
                except Exception as e:
                    logging.error(f"Error stopping {consumer_name} consumer", error=str(e))

        logging.info("Projection Service stopped")


def create_app(config: ServiceConfig) -> FastAPI:
    """Create FastAPI application."""
    app = FastAPI(
        title="Projection Service",
        description="Serving layer projection service for market data",
        version="1.0.0",
        lifespan=lifespan
    )

    # Store config in app state
    app.state.config = config

    # Configure CORS
    if config.enable_cors:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

    # Configure logging
    logging.basicConfig(
        level=getattr(logging, config.log_level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # Include routers
    app.include_router(health_router, prefix="", tags=["health"])
    app.include_router(metrics_router, prefix="", tags=["metrics"])
    app.include_router(query_router, prefix="/query", tags=["query"])
    app.include_router(cache_router, prefix="/cache", tags=["cache"])

    @app.get("/")
    async def root():
        """Root endpoint."""
        return {
            "service": "projection",
            "version": "1.0.0",
            "status": "running",
            "endpoints": [
                "/health",
                "/metrics",
                "/query/ohlc",
                "/query/metrics",
                "/query/curves",
                "/cache/invalidate",
                "/cache/refresh"
            ]
        }

    @app.get("/info")
    async def service_info():
        """Service information endpoint."""
        return {
            "service": "projection",
            "version": "1.0.0",
            "kafka_consumers": {
                name: consumer.get_stats() if consumer else None
                for name, consumer in kafka_consumers.items()
            },
            "projection_service": projection_service.get_stats() if projection_service else None
        }

    return app


def main():
    """Main entry point."""
    import os

    # Load configuration from environment
    config = ServiceConfig(
        host=os.getenv("HOST", "0.0.0.0"),
        port=int(os.getenv("PORT", "8513")),
        kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        clickhouse_dsn=os.getenv("CLICKHOUSE_DSN", "clickhouse://default@localhost:9000/carbon_ingestion"),
        redis_url=os.getenv("REDIS_URL", "redis://localhost:6379/0"),
        projection_config=os.getenv("PROJECTION_CONFIG", "configs/projection_policies.yaml"),
        enable_cors=os.getenv("ENABLE_CORS", "true").lower() == "true",
        log_level=os.getenv("LOG_LEVEL", "INFO"),
        parallelism=int(os.getenv("PARALLELISM", "4"))
    )

    # Create and run application
    app = create_app(config)

    uvicorn.run(
        app,
        host=config.host,
        port=config.port,
        log_level=config.log_level.lower(),
        access_log=True
    )


if __name__ == "__main__":
    main()
