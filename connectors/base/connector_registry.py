"""
Connector registry for managing connector metadata and discovery.

This module provides functionality to discover, register, and manage
connector metadata across the ingestion platform.
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import yaml
from pydantic import BaseModel, Field, ValidationError

from .exceptions import ConfigurationError, ValidationError as ConnectorValidationError
from .utils import setup_logging


class ConnectorMetadata(BaseModel):
    """Metadata model for connectors."""
    
    name: str = Field(..., description="Connector name")
    version: str = Field(..., description="Connector version")
    market: str = Field(..., description="Market identifier")
    mode: str = Field(..., description="Execution mode")
    schedule: Optional[str] = Field(None, description="Cron schedule")
    enabled: bool = Field(True, description="Whether connector is enabled")
    output_topic: str = Field(..., description="Output Kafka topic")
    schema: str = Field(..., description="Schema file path")
    retries: int = Field(3, description="Number of retries")
    backoff_seconds: int = Field(30, description="Backoff delay")
    tenant_strategy: str = Field("single", description="Tenant strategy")
    transforms: List[str] = Field(default_factory=list, description="Transform names")
    owner: str = Field("platform", description="Connector owner")
    description: Optional[str] = Field(None, description="Connector description")
    tags: List[str] = Field(default_factory=list, description="Connector tags")
    
    class Config:
        """Pydantic configuration."""
        extra = "forbid"
        validate_assignment = True


class ConnectorRegistry:
    """
    Registry for managing connector metadata and discovery.
    
    This class provides functionality to discover connectors, validate
    their metadata, and maintain a registry of available connectors.
    """
    
    def __init__(self, connectors_dir: Optional[str] = None):
        """
        Initialize the connector registry.
        
        Args:
            connectors_dir: Directory containing connector definitions
        """
        self.connectors_dir = Path(connectors_dir) if connectors_dir else Path("connectors")
        self.logger = setup_logging(self.__class__.__name__)
        self._connectors: Dict[str, ConnectorMetadata] = {}
        self._index_file = Path("connectors_index.json")
    
    def discover_connectors(self) -> List[str]:
        """
        Discover all connectors in the connectors directory.
        
        Returns:
            List[str]: List of connector names
        """
        if not self.connectors_dir.exists():
            self.logger.warning(f"Connectors directory not found: {self.connectors_dir}")
            return []
        
        connectors = []
        
        for item in self.connectors_dir.iterdir():
            if item.is_dir() and not item.name.startswith('.'):
                # Check if it's a connector directory (has connector.yaml)
                connector_yaml = item / "connector.yaml"
                if connector_yaml.exists():
                    connectors.append(item.name)
        
        self.logger.info(f"Discovered {len(connectors)} connectors: {connectors}")
        return connectors
    
    def load_connector_metadata(self, connector_name: str) -> Optional[ConnectorMetadata]:
        """
        Load metadata for a specific connector.
        
        Args:
            connector_name: Name of the connector
            
        Returns:
            Optional[ConnectorMetadata]: Connector metadata or None if not found
        """
        try:
            connector_path = self.connectors_dir / connector_name
            connector_yaml = connector_path / "connector.yaml"
            
            if not connector_yaml.exists():
                self.logger.warning(f"Connector metadata not found: {connector_yaml}")
                return None
            
            with open(connector_yaml, 'r', encoding='utf-8') as f:
                metadata_dict = yaml.safe_load(f)
            
            # Add connector name if not present
            if 'name' not in metadata_dict:
                metadata_dict['name'] = connector_name
            
            metadata = ConnectorMetadata(**metadata_dict)
            self._connectors[connector_name] = metadata
            
            return metadata
            
        except ValidationError as e:
            self.logger.error(f"Invalid connector metadata for {connector_name}: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Failed to load connector metadata for {connector_name}: {e}")
            return None
    
    def load_all_connectors(self) -> Dict[str, ConnectorMetadata]:
        """
        Load metadata for all discovered connectors.
        
        Returns:
            Dict[str, ConnectorMetadata]: Dictionary of connector metadata
        """
        connectors = self.discover_connectors()
        
        for connector_name in connectors:
            if connector_name not in self._connectors:
                self.load_connector_metadata(connector_name)
        
        return self._connectors.copy()
    
    def get_connector(self, connector_name: str) -> Optional[ConnectorMetadata]:
        """
        Get metadata for a specific connector.
        
        Args:
            connector_name: Name of the connector
            
        Returns:
            Optional[ConnectorMetadata]: Connector metadata or None if not found
        """
        if connector_name in self._connectors:
            return self._connectors[connector_name]
        
        return self.load_connector_metadata(connector_name)
    
    def get_enabled_connectors(self) -> Dict[str, ConnectorMetadata]:
        """
        Get all enabled connectors.
        
        Returns:
            Dict[str, ConnectorMetadata]: Dictionary of enabled connector metadata
        """
        all_connectors = self.load_all_connectors()
        return {
            name: metadata for name, metadata in all_connectors.items()
            if metadata.enabled
        }
    
    def get_connectors_by_market(self, market: str) -> Dict[str, ConnectorMetadata]:
        """
        Get connectors for a specific market.
        
        Args:
            market: Market identifier
            
        Returns:
            Dict[str, ConnectorMetadata]: Dictionary of connector metadata
        """
        all_connectors = self.load_all_connectors()
        return {
            name: metadata for name, metadata in all_connectors.items()
            if metadata.market == market
        }
    
    def get_connectors_by_mode(self, mode: str) -> Dict[str, ConnectorMetadata]:
        """
        Get connectors for a specific execution mode.
        
        Args:
            mode: Execution mode (batch, streaming, hybrid)
            
        Returns:
            Dict[str, ConnectorMetadata]: Dictionary of connector metadata
        """
        all_connectors = self.load_all_connectors()
        return {
            name: metadata for name, metadata in all_connectors.items()
            if metadata.mode == mode
        }
    
    def validate_connector(self, connector_name: str) -> Dict[str, Any]:
        """
        Validate a connector's configuration and dependencies.
        
        Args:
            connector_name: Name of the connector
            
        Returns:
            Dict[str, Any]: Validation result
        """
        result = {
            "valid": True,
            "errors": [],
            "warnings": [],
            "connector": connector_name
        }
        
        metadata = self.get_connector(connector_name)
        if not metadata:
            result["valid"] = False
            result["errors"].append("Connector not found")
            return result
        
        # Validate schema file exists
        schema_path = self.connectors_dir / connector_name / metadata.schema
        if not schema_path.exists():
            result["valid"] = False
            result["errors"].append(f"Schema file not found: {schema_path}")
        
        # Validate required files exist
        connector_path = self.connectors_dir / connector_name
        required_files = ["extractor.py", "transform.py"]
        
        for file_name in required_files:
            file_path = connector_path / file_name
            if not file_path.exists():
                result["warnings"].append(f"Required file not found: {file_path}")
        
        # Validate configuration
        try:
            metadata.model_validate(metadata.dict())
        except ValidationError as e:
            result["valid"] = False
            result["errors"].append(f"Configuration validation failed: {e}")
        
        return result
    
    def generate_index(self) -> Dict[str, Any]:
        """
        Generate a connector index for external consumption.
        
        Returns:
            Dict[str, Any]: Connector index
        """
        all_connectors = self.load_all_connectors()
        
        index = {
            "version": "1.0.0",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "connectors": {}
        }
        
        for name, metadata in all_connectors.items():
            index["connectors"][name] = {
                "name": metadata.name,
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
                "tags": metadata.tags
            }
        
        return index
    
    def save_index(self, output_file: Optional[str] = None) -> str:
        """
        Save the connector index to a file.
        
        Args:
            output_file: Output file path (defaults to connectors_index.json)
            
        Returns:
            str: Path to the saved index file
        """
        if output_file is None:
            output_file = str(self._index_file)
        
        index = self.generate_index()
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(index, f, indent=2, ensure_ascii=False)
        
        self.logger.info(f"Connector index saved to: {output_file}")
        return output_file
    
    def load_index(self, index_file: Optional[str] = None) -> Dict[str, Any]:
        """
        Load connector index from a file.
        
        Args:
            index_file: Index file path (defaults to connectors_index.json)
            
        Returns:
            Dict[str, Any]: Loaded connector index
        """
        if index_file is None:
            index_file = str(self._index_file)
        
        try:
            with open(index_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            self.logger.warning(f"Index file not found: {index_file}")
            return {}
        except json.JSONDecodeError as e:
            self.logger.error(f"Invalid JSON in index file: {e}")
            return {}
    
    def get_registry_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the connector registry.
        
        Returns:
            Dict[str, Any]: Registry statistics
        """
        all_connectors = self.load_all_connectors()
        
        stats = {
            "total_connectors": len(all_connectors),
            "enabled_connectors": len([c for c in all_connectors.values() if c.enabled]),
            "disabled_connectors": len([c for c in all_connectors.values() if not c.enabled]),
            "markets": list(set(c.market for c in all_connectors.values())),
            "modes": list(set(c.mode for c in all_connectors.values())),
            "owners": list(set(c.owner for c in all_connectors.values())),
            "last_updated": datetime.now(timezone.utc).isoformat()
        }
        
        return stats
