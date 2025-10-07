"""
Enrichment Service - Main application entry point.

This service adds semantic tags, taxonomy, and enrichment to normalized market data.
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
from .api.reprocess import router as reprocess_router
from .core.enricher import EnrichmentService
from .core.taxonomy import TaxonomyService
from .consumers.kafka_consumer import KafkaConsumerService, ConsumerConfig
from .producers.kafka_producer import KafkaProducerService, ProducerConfig


class ServiceConfig(BaseModel):
    """Service configuration."""
    
    host: str = "0.0.0.0"
    port: int = 8511
    kafka_bootstrap_servers: str = "localhost:9092"
    input_topic: str = "normalized.market.ticks.v1"
    output_topic: str = "enriched.market.ticks.v1"
    enable_cors: bool = True
    log_level: str = "INFO"
    parallelism: int = 4


# Global service instances
enrichment_service: EnrichmentService = None
taxonomy_service: TaxonomyService = None
kafka_consumer: KafkaConsumerService = None
kafka_producer: KafkaProducerService = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    global enrichment_service, taxonomy_service, kafka_consumer, kafka_producer
    
    # Startup
    logging.info("Starting Enrichment Service")
    
    try:
        # Initialize services
        enrichment_service = EnrichmentService()
        taxonomy_service = TaxonomyService()
        
        # Initialize Kafka services
        consumer_config = ConsumerConfig(
            bootstrap_servers=app.state.config.kafka_bootstrap_servers,
            topic=app.state.config.input_topic,
            group_id="enrichment-service"
        )
        producer_config = ProducerConfig(
            bootstrap_servers=app.state.config.kafka_bootstrap_servers,
            output_topic=app.state.config.output_topic
        )
        
        kafka_consumer = KafkaConsumerService(consumer_config, enrichment_service)
        kafka_producer = KafkaProducerService(producer_config)
        
        # Set up processing callback
        kafka_consumer.set_processing_callback(kafka_producer.publish_enriched_data)
        
        # Start Kafka services
        await kafka_consumer.start()
        await kafka_producer.start()
        
        # Start background consumption task
        consumption_task = asyncio.create_task(kafka_consumer.consume_messages())
        
        logging.info("Enrichment Service started successfully")
        yield
        
    except Exception as e:
        logging.error(f"Failed to start Enrichment Service: {e}")
        raise
    
    finally:
        # Shutdown
        logging.info("Shutting down Enrichment Service")
        
        # Cancel consumption task
        if 'consumption_task' in locals():
            consumption_task.cancel()
            try:
                await consumption_task
            except asyncio.CancelledError:
                pass
        
        if kafka_consumer:
            await kafka_consumer.stop()
        if kafka_producer:
            await kafka_producer.stop()
        
        logging.info("Enrichment Service stopped")


def create_app(config: ServiceConfig) -> FastAPI:
    """Create and configure the FastAPI application."""
    
    app = FastAPI(
        title="254Carbon Enrichment Service",
        description="Service for enriching normalized market data with semantic tags and taxonomy",
        version="1.0.0",
        lifespan=lifespan
    )
    
    # Store config in app state for lifespan access
    app.state.config = config
    
    # Add CORS middleware
    if config.enable_cors:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
    
    # Include routers
    app.include_router(health_router, prefix="/health", tags=["health"])
    app.include_router(metrics_router, prefix="/metrics", tags=["metrics"])
    app.include_router(reprocess_router, prefix="/reprocess", tags=["reprocess"])
    
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
    
    parser = argparse.ArgumentParser(description="Enrichment Service")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind to")
    parser.add_argument("--port", type=int, default=8511, help="Port to bind to")
    parser.add_argument("--kafka-bootstrap", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--input-topic", default="normalized.market.ticks.v1", help="Input topic")
    parser.add_argument("--output-topic", default="enriched.market.ticks.v1", help="Output topic")
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
        input_topic=args.input_topic,
        output_topic=args.output_topic,
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
