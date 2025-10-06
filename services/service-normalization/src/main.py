"""
Normalization Service - Main application entry point.

This service converts raw market data into normalized, standardized formats
with schema validation and data quality checks.
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
from .api.reprocess import router as reprocess_router
from .api.metrics import router as metrics_router
from .core.normalizer import NormalizationService
from .core.rules_engine import RulesEngine
from .core.validators import ValidationService
from .core.metrics import MetricsCollector, metrics_middleware, get_metrics_response
from .core.tracing import init_tracing, trace_function, trace_data_processing
from .consumers.kafka_consumer import KafkaConsumerService
from .producers.kafka_producer import KafkaProducerService


class ServiceConfig(BaseModel):
    """Service configuration."""
    
    host: str = "0.0.0.0"
    port: int = 8510
    kafka_bootstrap_servers: str = "localhost:9092"
    input_topic_pattern: str = "ingestion.*.raw.v1"
    output_topic: str = "normalized.market.ticks.v1"
    schema_registry_url: str = "http://localhost:8081"
    enable_cors: bool = True
    log_level: str = "INFO"
    parallelism: int = 4


# Global service instances
normalization_service: NormalizationService = None
rules_engine: RulesEngine = None
validation_service: ValidationService = None
kafka_consumer: KafkaConsumerService = None
kafka_producer: KafkaProducerService = None
metrics_collector: MetricsCollector = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    global normalization_service, rules_engine, validation_service, kafka_consumer, kafka_producer, metrics_collector
    
    # Startup
    logging.info("Starting Normalization Service")
    
    try:
        # Initialize observability
        init_tracing(
            service_name="normalization-service",
            service_version="1.0.0",
            environment="production"
        )
        metrics_collector = MetricsCollector(
            service_name="normalization-service",
            version="1.0.0",
            environment="production"
        )
        
        # Initialize services
        normalization_service = NormalizationService()
        rules_engine = RulesEngine()
        validation_service = ValidationService()
        kafka_consumer = KafkaConsumerService()
        kafka_producer = KafkaProducerService()
        
        # Start background tasks
        await kafka_consumer.start()
        await kafka_producer.start()
        
        logging.info("Normalization Service started successfully")
        yield
        
    except Exception as e:
        logging.error(f"Failed to start Normalization Service: {e}")
        raise
    
    finally:
        # Shutdown
        logging.info("Shutting down Normalization Service")
        
        if kafka_consumer:
            await kafka_consumer.stop()
        if kafka_producer:
            await kafka_producer.stop()
        
        logging.info("Normalization Service stopped")


def create_app(config: ServiceConfig) -> FastAPI:
    """Create and configure the FastAPI application."""
    
    app = FastAPI(
        title="254Carbon Normalization Service",
        description="Service for normalizing raw market data into standardized formats",
        version="1.0.0",
        lifespan=lifespan
    )
    
    # Add CORS middleware
    if config.enable_cors:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
    
    # Add metrics middleware
    app = metrics_middleware(app)
    
    # Include routers
    app.include_router(health_router, prefix="/health", tags=["health"])
    app.include_router(reprocess_router, prefix="/reprocess", tags=["reprocess"])
    app.include_router(metrics_router, prefix="/metrics", tags=["metrics"])
    
    # Add Prometheus metrics endpoint
    @app.get("/metrics", include_in_schema=False)
    async def prometheus_metrics():
        return get_metrics_response()
    
    # Global exception handler
    @app.exception_handler(Exception)
    async def global_exception_handler(request, exc):
        logging.error(f"Unhandled exception: {exc}")
        return JSONResponse(
            status_code=500,
            content={"error": "Internal server error", "detail": str(exc)}
        )
    
    return app


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Normalization Service")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind to")
    parser.add_argument("--port", type=int, default=8510, help="Port to bind to")
    parser.add_argument("--kafka-bootstrap", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--input-topic-pattern", default="ingestion.*.raw.v1", help="Input topic pattern")
    parser.add_argument("--output-topic", default="normalized.market.ticks.v1", help="Output topic")
    parser.add_argument("--schema-registry", default="http://localhost:8081", help="Schema registry URL")
    parser.add_argument("--log-level", default="INFO", help="Log level")
    parser.add_argument("--parallelism", type=int, default=4, help="Processing parallelism")
    parser.add_argument("--no-cors", action="store_true", help="Disable CORS")
    
    args = parser.parse_args()
    
    # Configure logging
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    # Create configuration
    config = ServiceConfig(
        host=args.host,
        port=args.port,
        kafka_bootstrap_servers=args.kafka_bootstrap,
        input_topic_pattern=args.input_topic_pattern,
        output_topic=args.output_topic,
        schema_registry_url=args.schema_registry,
        enable_cors=not args.no_cors,
        log_level=args.log_level,
        parallelism=args.parallelism
    )
    
    # Create and run application
    app = create_app(config)
    
    uvicorn.run(
        app,
        host=config.host,
        port=config.port,
        log_level=config.log_level.lower()
    )


if __name__ == "__main__":
    main()
