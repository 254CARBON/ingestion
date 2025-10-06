"""
Connector registry core service implementation.
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

from ...connectors.base import ConnectorRegistry as BaseConnectorRegistry
from ...connectors.base.exceptions import ConfigurationError

logger = logging.getLogger(__name__)


class ConnectorRegistryService:
    """Service for managing connector registry operations."""
    
    def __init__(self, connectors_dir: str = "connectors"):
        """
        Initialize the connector registry service.
        
        Args:
            connectors_dir: Directory containing connector definitions
        """
        self.connectors_dir = connectors_dir
        self.base_registry = BaseConnectorRegistry(connectors_dir)
        self.logger = logging.getLogger(self.__class__.__name__)
    
    async def get_health_status(self) -> Dict[str, Any]:
        """Get health status of the registry service."""
        try:
            stats = self.base_registry.get_registry_stats()
            
            return {
                "status": "healthy",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "connectors_loaded": stats.get("total_connectors", 0),
                "last_updated": stats.get("last_updated"),
                "errors": []
            }
        except Exception as e:
            self.logger.error(f"Health check failed: {e}")
            return {
                "status": "unhealthy",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "connectors_loaded": 0,
                "last_updated": None,
                "errors": [str(e)]
            }
    
    async def is_ready(self) -> bool:
        """Check if the registry service is ready."""
        try:
            # Check if we can load connectors
            connectors = self.base_registry.load_all_connectors()
            return len(connectors) >= 0  # Always ready if we can load (even if empty)
        except Exception as e:
            self.logger.error(f"Readiness check failed: {e}")
            return False
    
    def load_all_connectors(self) -> Dict[str, Any]:
        """Load all connector metadata."""
        return self.base_registry.load_all_connectors()
    
    def get_connector(self, connector_name: str) -> Optional[Any]:
        """Get metadata for a specific connector."""
        return self.base_registry.get_connector(connector_name)
    
    def get_enabled_connectors(self) -> Dict[str, Any]:
        """Get all enabled connectors."""
        return self.base_registry.get_enabled_connectors()
    
    def get_connectors_by_market(self, market: str) -> Dict[str, Any]:
        """Get connectors for a specific market."""
        return self.base_registry.get_connectors_by_market(market)
    
    def get_connectors_by_mode(self, mode: str) -> Dict[str, Any]:
        """Get connectors for a specific mode."""
        return self.base_registry.get_connectors_by_mode(mode)
    
    def validate_connector(self, connector_name: str) -> Dict[str, Any]:
        """Validate a connector's configuration and dependencies."""
        return self.base_registry.validate_connector(connector_name)
    
    def get_registry_stats(self) -> Dict[str, Any]:
        """Get registry statistics."""
        return self.base_registry.get_registry_stats()
    
    def get_connector_health_status(self, connector_name: str) -> Optional[Dict[str, Any]]:
        """Get health status for a specific connector."""
        try:
            metadata = self.get_connector(connector_name)
            if not metadata:
                return None
            
            # Basic health check
            validation_result = self.validate_connector(connector_name)
            
            return {
                "name": connector_name,
                "status": "healthy" if validation_result.get("valid", False) else "unhealthy",
                "enabled": metadata.enabled,
                "market": metadata.market,
                "mode": metadata.mode,
                "version": metadata.version,
                "validation_errors": validation_result.get("errors", []),
                "validation_warnings": validation_result.get("warnings", []),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
        except Exception as e:
            self.logger.error(f"Failed to get health status for {connector_name}: {e}")
            return {
                "name": connector_name,
                "status": "error",
                "error": str(e),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
    
    def get_connector_capabilities(self, connector_name: str) -> Optional[Dict[str, Any]]:
        """Get capabilities for a specific connector."""
        try:
            metadata = self.get_connector(connector_name)
            if not metadata:
                return None
            
            return {
                "name": connector_name,
                "version": metadata.version,
                "market": metadata.market,
                "mode": metadata.mode,
                "enabled": metadata.enabled,
                "output_topic": metadata.output_topic,
                "schema": metadata.schema,
                "retries": metadata.retries,
                "backoff_seconds": metadata.backoff_seconds,
                "tenant_strategy": metadata.tenant_strategy,
                "transforms": metadata.transforms,
                "owner": metadata.owner,
                "description": metadata.description,
                "tags": metadata.tags,
                "capabilities": {
                    "batch_mode": metadata.mode in ["batch", "hybrid"],
                    "streaming_mode": metadata.mode in ["streaming", "hybrid"],
                    "retry_support": metadata.retries > 0,
                    "transform_support": len(metadata.transforms) > 0,
                    "multi_tenant": metadata.tenant_strategy == "multi"
                }
            }
        except Exception as e:
            self.logger.error(f"Failed to get capabilities for {connector_name}: {e}")
            return None
    
    async def refresh_connectors(self) -> Dict[str, Any]:
        """Refresh the connector registry."""
        try:
            # Reload all connectors
            connectors = self.load_all_connectors()
            
            return {
                "status": "success",
                "connectors_loaded": len(connectors),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
        except Exception as e:
            self.logger.error(f"Failed to refresh connectors: {e}")
            return {
                "status": "error",
                "error": str(e),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
    
    async def generate_index(self) -> Dict[str, Any]:
        """Generate connector index."""
        try:
            index = self.base_registry.generate_index()
            return {
                "status": "success",
                "index": index,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
        except Exception as e:
            self.logger.error(f"Failed to generate index: {e}")
            return {
                "status": "error",
                "error": str(e),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
    
    async def save_index(self, output_file: str = "connectors_index.json") -> Dict[str, Any]:
        """Save connector index to file."""
        try:
            saved_file = self.base_registry.save_index(output_file)
            return {
                "status": "success",
                "file": saved_file,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
        except Exception as e:
            self.logger.error(f"Failed to save index: {e}")
            return {
                "status": "error",
                "error": str(e),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
