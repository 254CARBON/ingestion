"""
Connector Registry Service - Main application entry point.

This service provides a REST API for managing connector metadata,
health status, and discovery across the ingestion platform.
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
from .api.connectors import router as connectors_router
from .api.metrics import router as metrics_router
from .core.registry import ConnectorRegistryService
from .core.discovery import ConnectorDiscoveryService


class ServiceConfig(BaseModel):
    """Service configuration."""
    
    host: str = "0.0.0.0"
    port: int = 8500
    connectors_dir: str = "connectors"
    index_file: str = "connectors_index.json"
    enable_cors: bool = True
    log_level: str = "INFO"


# Global service instances
registry_service: ConnectorRegistryService = None
discovery_service: ConnectorDiscoveryService = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    global registry_service, discovery_service
    
    # Startup
    logging.info("Starting Connector Registry Service")
    
    try:
        # Initialize services
        registry_service = ConnectorRegistryService()
        discovery_service = ConnectorDiscoveryService()
        
        # Start background tasks
        await discovery_service.start()
        
        logging.info("Connector Registry Service started successfully")
        yield
        
    except Exception as e:
        logging.error(f"Failed to start Connector Registry Service: {e}")
        raise
    
    finally:
        # Shutdown
        logging.info("Shutting down Connector Registry Service")
        
        if discovery_service:
            await discovery_service.stop()
        
        logging.info("Connector Registry Service stopped")


def create_app(config: ServiceConfig) -> FastAPI:
    """Create and configure the FastAPI application."""
    
    app = FastAPI(
        title="254Carbon Connector Registry",
        description="REST API for managing connector metadata and discovery",
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
    
    # Include routers
    app.include_router(health_router, prefix="/health", tags=["health"])
    app.include_router(connectors_router, prefix="/connectors", tags=["connectors"])
    app.include_router(metrics_router, prefix="/metrics", tags=["metrics"])
    
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
    
    parser = argparse.ArgumentParser(description="Connector Registry Service")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind to")
    parser.add_argument("--port", type=int, default=8500, help="Port to bind to")
    parser.add_argument("--connectors-dir", default="connectors", help="Connectors directory")
    parser.add_argument("--index-file", default="connectors_index.json", help="Index file path")
    parser.add_argument("--log-level", default="INFO", help="Log level")
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
        connectors_dir=args.connectors_dir,
        index_file=args.index_file,
        enable_cors=not args.no_cors,
        log_level=args.log_level
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
