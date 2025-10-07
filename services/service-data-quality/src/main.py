"""
Data Quality Service - Main application entry point.

This service monitors data quality across the ingestion pipeline and
detects anomalies in market data streams.
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
from .api.anomalies import router as anomalies_router
from .api.quality import router as quality_router
from .core.anomaly_detector import AnomalyDetector
from .consumers.kafka_consumer import KafkaConsumerService, ConsumerConfig
from .producers.kafka_producer import KafkaProducerService, ProducerConfig


class ServiceConfig(BaseModel):
    """Service configuration."""

    host: str = "0.0.0.0"
    port: int = 8514
    kafka_bootstrap_servers: str = "localhost:9092"
    input_topics: list = ["normalized.market.ticks.v1", "enriched.market.ticks.v1"]
    output_topic: str = "data.quality.anomalies.v1"
    detection_config: str = "configs/data_quality_rules.yaml"
    enable_cors: bool = True
    log_level: str = "INFO"
    parallelism: int = 2


# Global service instances
anomaly_detector: AnomalyDetector = None
kafka_consumers = {}
kafka_producer: KafkaProducerService = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    global anomaly_detector, kafka_consumers, kafka_producer

    # Startup
    try:
        logging.info("Starting Data Quality Service")

        # Initialize anomaly detector
        anomaly_detector = AnomalyDetector(app.state.config.detection_config)

        # Initialize Kafka producer
        kafka_producer = KafkaProducerService(
            ProducerConfig(
                bootstrap_servers=app.state.config.kafka_bootstrap_servers,
                output_topic=app.state.config.output_topic
            )
        )
        await kafka_producer.start()

        # Initialize Kafka consumers for each input topic
        for topic in app.state.config.input_topics:
            consumer = KafkaConsumerService(
                ConsumerConfig(
                    bootstrap_servers=app.state.config.kafka_bootstrap_servers,
                    topic=topic,
                    group_id=f"data-quality-{topic}"
                ),
                anomaly_detector,
                kafka_producer
            )
            await consumer.start()
            kafka_consumers[topic] = consumer

            # Start consumer task
            asyncio.create_task(consumer.consume_messages())

        logging.info("Data Quality Service started successfully")

        yield

    except Exception as e:
        logging.error("Failed to start Data Quality Service", error=str(e))
        raise

    finally:
        # Shutdown
        logging.info("Stopping Data Quality Service")

        # Stop Kafka consumers
        for topic, consumer in kafka_consumers.items():
            if consumer:
                try:
                    await consumer.stop()
                    logging.info(f"Stopped consumer for topic: {topic}")
                except Exception as e:
                    logging.error(f"Error stopping consumer for {topic}", error=str(e))

        # Stop Kafka producer
        if kafka_producer:
            try:
                await kafka_producer.stop()
                logging.info("Stopped Kafka producer")
            except Exception as e:
                logging.error("Error stopping Kafka producer", error=str(e))

        logging.info("Data Quality Service stopped")


def create_app(config: ServiceConfig) -> FastAPI:
    """Create FastAPI application."""
    app = FastAPI(
        title="Data Quality Service",
        description="Data quality monitoring and anomaly detection service",
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
    app.include_router(anomalies_router, prefix="/anomalies", tags=["anomalies"])
    app.include_router(quality_router, prefix="/quality", tags=["quality"])

    @app.get("/")
    async def root():
        """Root endpoint."""
        return {
            "service": "data-quality",
            "version": "1.0.0",
            "status": "running",
            "endpoints": [
                "/health",
                "/metrics",
                "/anomalies",
                "/quality/scores",
                "/quality/report"
            ]
        }

    @app.get("/info")
    async def service_info():
        """Service information endpoint."""
        return {
            "service": "data-quality",
            "version": "1.0.0",
            "kafka_consumers": {
                topic: consumer.get_stats() if consumer else None
                for topic, consumer in kafka_consumers.items()
            },
            "anomaly_detector": anomaly_detector.get_stats() if anomaly_detector else None
        }

    return app


def main():
    """Main entry point."""
    import os

    # Load configuration from environment
    config = ServiceConfig(
        host=os.getenv("HOST", "0.0.0.0"),
        port=int(os.getenv("PORT", "8514")),
        kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        input_topics=os.getenv("INPUT_TOPICS", "normalized.market.ticks.v1,enriched.market.ticks.v1").split(","),
        output_topic=os.getenv("OUTPUT_TOPIC", "data.quality.anomalies.v1"),
        detection_config=os.getenv("DETECTION_CONFIG", "configs/data_quality_rules.yaml"),
        enable_cors=os.getenv("ENABLE_CORS", "true").lower() == "true",
        log_level=os.getenv("LOG_LEVEL", "INFO"),
        parallelism=int(os.getenv("PARALLELISM", "2"))
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
