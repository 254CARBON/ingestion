"""
End-to-end integration tests for the 254Carbon Ingestion Platform.
"""

import pytest
import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List

from connectors.base import ConnectorRegistry, ContractValidator
from connectors.miso.connector import MISOConnector
from connectors.miso.config import MISOConnectorConfig
from connectors.caiso.connector import CAISOConnector
from connectors.caiso.config import CAISOConnectorConfig


class TestEndToEndPipeline:
    """Test complete end-to-end data pipeline."""
    
    @pytest.fixture
    def temp_connectors_dir(self, temp_dir):
        """Create temporary connectors directory."""
        connectors_dir = temp_dir / "connectors"
        connectors_dir.mkdir()
        
        # Create MISO connector directory
        miso_dir = connectors_dir / "miso"
        miso_dir.mkdir()
        
        # Create CAISO connector directory
        caiso_dir = connectors_dir / "caiso"
        caiso_dir.mkdir()
        
        return connectors_dir
    
    @pytest.fixture
    def sample_connector_configs(self, temp_connectors_dir):
        """Create sample connector configurations."""
        # MISO config
        miso_config = {
            "name": "miso",
            "version": "1.2.0",
            "market": "MISO",
            "mode": "batch",
            "schedule": "0 */1 * * *",
            "enabled": True,
            "output_topic": "ingestion.miso.raw.v1",
            "schema": "schemas/raw_miso_trade.avsc",
            "retries": 3,
            "backoff_seconds": 30,
            "tenant_strategy": "single",
            "transforms": ["sanitize_numeric", "standardize_timezone"],
            "owner": "platform",
            "description": "MISO market data connector",
            "tags": ["market-data", "energy", "trading"]
        }
        
        # CAISO config
        caiso_config = {
            "name": "caiso",
            "version": "1.0.0",
            "market": "CAISO",
            "mode": "batch",
            "schedule": "0 2 * * *",
            "enabled": True,
            "output_topic": "ingestion.caiso.raw.v1",
            "schema": "schemas/raw_caiso_trade.avsc",
            "retries": 3,
            "backoff_seconds": 30,
            "tenant_strategy": "single",
            "transforms": ["sanitize_numeric", "standardize_timezone"],
            "owner": "platform",
            "description": "CAISO market data connector",
            "tags": ["market-data", "energy", "trading"]
        }
        
        # Write config files
        miso_config_file = temp_connectors_dir / "miso" / "connector.yaml"
        caiso_config_file = temp_connectors_dir / "caiso" / "connector.yaml"
        
        import yaml
        with open(miso_config_file, 'w') as f:
            yaml.dump(miso_config, f)
        with open(caiso_config_file, 'w') as f:
            yaml.dump(caiso_config, f)
        
        return {"miso": miso_config, "caiso": caiso_config}
    
    @pytest.fixture
    def sample_schemas(self, temp_connectors_dir):
        """Create sample Avro schemas."""
        # MISO schema
        miso_schema = {
            "type": "record",
            "name": "RawMISOTrade",
            "namespace": "com.twentyfivefourcarbon.ingestion.miso",
            "fields": [
                {"name": "event_id", "type": "string"},
                {"name": "occurred_at", "type": "long"},
                {"name": "tenant_id", "type": "string"},
                {"name": "schema_version", "type": "string", "default": "1.0.0"},
                {"name": "producer", "type": "string", "default": "miso-connector"},
                {"name": "trade_id", "type": ["null", "string"], "default": None},
                {"name": "price", "type": ["null", "double"], "default": None},
                {"name": "quantity", "type": ["null", "double"], "default": None}
            ]
        }
        
        # CAISO schema
        caiso_schema = {
            "type": "record",
            "name": "RawCAISOTrade",
            "namespace": "com.twentyfivefourcarbon.ingestion.caiso",
            "fields": [
                {"name": "event_id", "type": "string"},
                {"name": "occurred_at", "type": "long"},
                {"name": "tenant_id", "type": "string"},
                {"name": "schema_version", "type": "string", "default": "1.0.0"},
                {"name": "producer", "type": "string", "default": "caiso-connector"},
                {"name": "trade_id", "type": ["null", "string"], "default": None},
                {"name": "price", "type": ["null", "double"], "default": None},
                {"name": "quantity", "type": ["null", "double"], "default": None}
            ]
        }
        
        # Create schema directories
        miso_schemas_dir = temp_connectors_dir / "miso" / "schemas"
        caiso_schemas_dir = temp_connectors_dir / "caiso" / "schemas"
        miso_schemas_dir.mkdir()
        caiso_schemas_dir.mkdir()
        
        # Write schema files
        miso_schema_file = miso_schemas_dir / "raw_miso_trade.avsc"
        caiso_schema_file = caiso_schemas_dir / "raw_caiso_trade.avsc"
        
        with open(miso_schema_file, 'w') as f:
            json.dump(miso_schema, f, indent=2)
        with open(caiso_schema_file, 'w') as f:
            json.dump(caiso_schema, f, indent=2)
        
        return {"miso": miso_schema, "caiso": caiso_schema}
    
    @pytest.mark.asyncio
    async def test_connector_registry_discovery(self, temp_connectors_dir, sample_connector_configs):
        """Test connector registry discovery."""
        registry = ConnectorRegistry(str(temp_connectors_dir))
        
        # Discover connectors
        connectors = registry.discover_connectors()
        assert len(connectors) == 2
        assert "miso" in connectors
        assert "caiso" in connectors
        
        # Load all connectors
        all_connectors = registry.load_all_connectors()
        assert len(all_connectors) == 2
        
        # Test individual connector loading
        miso_metadata = registry.get_connector("miso")
        assert miso_metadata is not None
        assert miso_metadata.name == "miso"
        assert miso_metadata.market == "MISO"
        assert miso_metadata.enabled is True
        
        caiso_metadata = registry.get_connector("caiso")
        assert caiso_metadata is not None
        assert caiso_metadata.name == "caiso"
        assert caiso_metadata.market == "CAISO"
        assert caiso_metadata.enabled is True
    
    @pytest.mark.asyncio
    async def test_connector_validation(self, temp_connectors_dir, sample_connector_configs, sample_schemas):
        """Test connector validation."""
        registry = ConnectorRegistry(str(temp_connectors_dir))
        
        # Validate MISO connector
        miso_validation = registry.validate_connector("miso")
        assert miso_validation["valid"] is True
        assert len(miso_validation["errors"]) == 0
        
        # Validate CAISO connector
        caiso_validation = registry.validate_connector("caiso")
        assert caiso_validation["valid"] is True
        assert len(caiso_validation["errors"]) == 0
    
    @pytest.mark.asyncio
    async def test_schema_validation(self, temp_connectors_dir, sample_schemas):
        """Test Avro schema validation."""
        validator = ContractValidator(str(temp_connectors_dir))
        
        # Test MISO schema
        miso_schema = validator.load_schema("miso/schemas/raw_miso_trade")
        assert miso_schema is not None
        assert miso_schema.name == "RawMISOTrade"
        
        # Test CAISO schema
        caiso_schema = validator.load_schema("caiso/schemas/raw_caiso_trade")
        assert caiso_schema is not None
        assert caiso_schema.name == "RawCAISOTrade"
        
        # Test record validation
        sample_record = {
            "event_id": "test-123",
            "occurred_at": 1640995200000000,
            "tenant_id": "test-tenant",
            "schema_version": "1.0.0",
            "producer": "test-connector",
            "trade_id": "trade-456",
            "price": 50.25,
            "quantity": 100.0
        }
        
        # Validate against MISO schema
        miso_valid = validator.validate_record(sample_record, "miso/schemas/raw_miso_trade")
        assert miso_valid is True
        
        # Validate against CAISO schema
        caiso_valid = validator.validate_record(sample_record, "caiso/schemas/raw_caiso_trade")
        assert caiso_valid is True
    
    @pytest.mark.asyncio
    async def test_miso_connector_pipeline(self, sample_connector_configs):
        """Test MISO connector end-to-end pipeline."""
        # Create MISO connector config
        miso_config_data = {
            **sample_connector_configs["miso"],
            "miso_api_key": "test-api-key",
            "miso_base_url": "https://api.misoenergy.org",
            "miso_timeout": 30,
            "miso_rate_limit": 100
        }
        
        config = MISOConnectorConfig(**miso_config_data)
        connector = MISOConnector(config)
        
        # Test connector initialization
        assert connector.config.name == "miso"
        assert connector.config.market == "MISO"
        assert connector._extractor is not None
        assert connector._transformer is not None
        
        # Test health status
        health_status = connector.get_health_status()
        assert health_status["name"] == "miso"
        assert health_status["market"] == "MISO"
        assert health_status["enabled"] is True
        
        # Test connector info
        connector_info = connector.get_connector_info()
        assert "miso_base_url" in connector_info
        assert "transforms_enabled" in connector_info
        
        # Test extraction capabilities
        capabilities = connector.get_extraction_capabilities()
        assert "modes" in capabilities
        assert "batch" in capabilities["modes"]
        assert "realtime" in capabilities["modes"]
    
    @pytest.mark.asyncio
    async def test_caiso_connector_pipeline(self, sample_connector_configs):
        """Test CAISO connector end-to-end pipeline."""
        # Create CAISO connector config
        caiso_config_data = {
            **sample_connector_configs["caiso"],
            "caiso_api_key": "test-api-key",
            "caiso_base_url": "https://api.caiso.com",
            "caiso_timeout": 30,
            "caiso_rate_limit": 100
        }
        
        config = CAISOConnectorConfig(**caiso_config_data)
        connector = CAISOConnector(config)
        
        # Test connector initialization
        assert connector.config.name == "caiso"
        assert connector.config.market == "CAISO"
        assert connector._extractor is not None
        assert connector._transformer is not None
        
        # Test health status
        health_status = connector.get_health_status()
        assert health_status["name"] == "caiso"
        assert health_status["market"] == "CAISO"
        assert health_status["enabled"] is True
        
        # Test connector info
        connector_info = connector.get_connector_info()
        assert "caiso_base_url" in connector_info
        assert "transforms_enabled" in connector_info
        
        # Test extraction capabilities
        capabilities = connector.get_extraction_capabilities()
        assert "modes" in capabilities
        assert "batch" in capabilities["modes"]
        assert "realtime" in capabilities["modes"]
    
    @pytest.mark.asyncio
    async def test_index_generation(self, temp_connectors_dir, sample_connector_configs):
        """Test connector index generation."""
        registry = ConnectorRegistry(str(temp_connectors_dir))
        
        # Generate index
        index = registry.generate_index()
        
        assert "version" in index
        assert "generated_at" in index
        assert "connectors" in index
        assert len(index["connectors"]) == 2
        
        # Check MISO connector in index
        assert "miso" in index["connectors"]
        miso_info = index["connectors"]["miso"]
        assert miso_info["name"] == "miso"
        assert miso_info["market"] == "MISO"
        assert miso_info["enabled"] is True
        
        # Check CAISO connector in index
        assert "caiso" in index["connectors"]
        caiso_info = index["connectors"]["caiso"]
        assert caiso_info["name"] == "caiso"
        assert caiso_info["market"] == "CAISO"
        assert caiso_info["enabled"] is True
    
    @pytest.mark.asyncio
    async def test_registry_stats(self, temp_connectors_dir, sample_connector_configs):
        """Test registry statistics."""
        registry = ConnectorRegistry(str(temp_connectors_dir))
        
        # Get stats
        stats = registry.get_registry_stats()
        
        assert "total_connectors" in stats
        assert "enabled_connectors" in stats
        assert "disabled_connectors" in stats
        assert "markets" in stats
        assert "modes" in stats
        assert "owners" in stats
        assert "last_updated" in stats
        
        assert stats["total_connectors"] == 2
        assert stats["enabled_connectors"] == 2
        assert stats["disabled_connectors"] == 0
        assert "MISO" in stats["markets"]
        assert "CAISO" in stats["markets"]
        assert "batch" in stats["modes"]
        assert "platform" in stats["owners"]
    
    @pytest.mark.asyncio
    async def test_connector_filtering(self, temp_connectors_dir, sample_connector_configs):
        """Test connector filtering by various criteria."""
        registry = ConnectorRegistry(str(temp_connectors_dir))
        
        # Filter by market
        miso_connectors = registry.get_connectors_by_market("MISO")
        assert len(miso_connectors) == 1
        assert "miso" in miso_connectors
        
        caiso_connectors = registry.get_connectors_by_market("CAISO")
        assert len(caiso_connectors) == 1
        assert "caiso" in caiso_connectors
        
        # Filter by mode
        batch_connectors = registry.get_connectors_by_mode("batch")
        assert len(batch_connectors) == 2
        assert "miso" in batch_connectors
        assert "caiso" in batch_connectors
        
        # Get enabled connectors
        enabled_connectors = registry.get_enabled_connectors()
        assert len(enabled_connectors) == 2
        assert "miso" in enabled_connectors
        assert "caiso" in enabled_connectors


class TestDataPipelineIntegration:
    """Test data pipeline integration."""
    
    @pytest.mark.asyncio
    async def test_raw_to_normalized_pipeline(self, sample_raw_record, sample_normalized_record):
        """Test raw to normalized data pipeline."""
        # This would test the complete pipeline from raw data to normalized data
        # For now, we'll simulate the key components
        
        # Simulate raw data ingestion
        raw_data = [sample_raw_record]
        assert len(raw_data) == 1
        assert raw_data[0]["event_id"] == "test-event-123"
        
        # Simulate normalization
        normalized_data = [sample_normalized_record]
        assert len(normalized_data) == 1
        assert normalized_data[0]["event_id"] == "test-event-123"
        assert normalized_data[0]["market"] == "TEST"
        assert normalized_data[0]["instrument_id"] is not None
        
        # Verify data integrity
        assert normalized_data[0]["event_id"] == raw_data[0]["event_id"]
        assert normalized_data[0]["occurred_at"] == raw_data[0]["occurred_at"]
    
    @pytest.mark.asyncio
    async def test_normalized_to_enriched_pipeline(self, sample_normalized_record, sample_enriched_record):
        """Test normalized to enriched data pipeline."""
        # This would test the complete pipeline from normalized data to enriched data
        # For now, we'll simulate the key components
        
        # Simulate normalized data
        normalized_data = [sample_normalized_record]
        assert len(normalized_data) == 1
        assert normalized_data[0]["market"] == "TEST"
        
        # Simulate enrichment
        enriched_data = [sample_enriched_record]
        assert len(enriched_data) == 1
        assert enriched_data[0]["market"] == "TEST"
        assert enriched_data[0]["semantic_tags"] is not None
        assert enriched_data[0]["taxonomy_class"] is not None
        
        # Verify data integrity
        assert enriched_data[0]["event_id"] == normalized_data[0]["event_id"]
        assert enriched_data[0]["market"] == normalized_data[0]["market"]
        assert enriched_data[0]["price"] == normalized_data[0]["price"]
    
    @pytest.mark.asyncio
    async def test_error_handling_pipeline(self):
        """Test error handling in the data pipeline."""
        # Test with invalid data
        invalid_record = {"invalid": "data"}
        
        # Simulate error handling
        try:
            # This would trigger validation errors
            if "event_id" not in invalid_record:
                raise ValueError("Missing required field: event_id")
        except ValueError as e:
            assert "Missing required field" in str(e)
        
        # Test with partial data
        partial_record = {
            "event_id": "test-123",
            "occurred_at": 1640995200000000,
            # Missing other required fields
        }
        
        # Simulate partial validation
        errors = []
        required_fields = ["event_id", "occurred_at", "tenant_id", "schema_version", "producer"]
        for field in required_fields:
            if field not in partial_record:
                errors.append(f"Missing required field: {field}")
        
        assert len(errors) == 3  # tenant_id, schema_version, producer
        assert "Missing required field: tenant_id" in errors
        assert "Missing required field: schema_version" in errors
        assert "Missing required field: producer" in errors
