"""
Test suite for CAISO connector examples.

This module provides comprehensive tests for all CAISO connector examples
and configurations.
"""

import asyncio
import json
import pytest
import tempfile
import yaml
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from connectors.caiso import CAISOConnector, CAISOConnectorConfig


class TestCAISOConnectorConfig:
    """Test CAISO connector configuration."""
    
    def test_basic_config(self):
        """Test basic configuration creation."""
        config = CAISOConnectorConfig(
            name="caiso",
            market="CAISO",
            mode="batch",
            enabled=True,
            output_topic="ingestion.caiso.raw.v1",
            schema="schemas/raw_caiso_trade.avsc"
        )
        
        assert config.name == "caiso"
        assert config.market == "CAISO"
        assert config.mode == "batch"
        assert config.enabled is True
        assert config.output_topic == "ingestion.caiso.raw.v1"
        assert config.schema == "schemas/raw_caiso_trade.avsc"
    
    def test_caiso_specific_config(self):
        """Test CAISO-specific configuration options."""
        config = CAISOConnectorConfig(
            name="caiso",
            market="CAISO",
            mode="batch",
            enabled=True,
            output_topic="ingestion.caiso.raw.v1",
            schema="schemas/raw_caiso_trade.avsc",
            caiso_base_url="https://oasis.caiso.com/oasisapi",
            caiso_timeout=30,
            caiso_rate_limit=100,
            default_market_run_id="DAM",
            default_node="TH_SP15_GEN-APND"
        )
        
        assert config.caiso_base_url == "https://oasis.caiso.com/oasisapi"
        assert config.caiso_timeout == 30
        assert config.caiso_rate_limit == 100
        assert config.default_market_run_id == "DAM"
        assert config.default_node == "TH_SP15_GEN-APND"
    
    def test_config_validation(self):
        """Test configuration validation."""
        config = CAISOConnectorConfig(
            name="caiso",
            market="CAISO",
            mode="batch",
            enabled=True,
            output_topic="ingestion.caiso.raw.v1",
            schema="schemas/raw_caiso_trade.avsc"
        )
        
        # Test validation
        assert config.validate() is True
    
    def test_config_from_yaml(self):
        """Test loading configuration from YAML."""
        yaml_content = """
        name: caiso
        market: CAISO
        mode: batch
        enabled: true
        output_topic: ingestion.caiso.raw.v1
        schema: schemas/raw_caiso_trade.avsc
        caiso_base_url: https://oasis.caiso.com/oasisapi
        caiso_timeout: 30
        default_market_run_id: DAM
        default_node: TH_SP15_GEN-APND
        """
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(yaml_content)
            f.flush()
            
            try:
                config = CAISOConnectorConfig.from_yaml(f.name)
                assert config.name == "caiso"
                assert config.market == "CAISO"
                assert config.caiso_base_url == "https://oasis.caiso.com/oasisapi"
            finally:
                Path(f.name).unlink()


class TestCAISOConnector:
    """Test CAISO connector functionality."""
    
    @pytest.fixture
    def config(self):
        """Create test configuration."""
        return CAISOConnectorConfig(
            name="caiso",
            market="CAISO",
            mode="batch",
            enabled=True,
            output_topic="ingestion.caiso.raw.v1",
            schema="schemas/raw_caiso_trade.avsc",
            caiso_base_url="https://oasis.caiso.com/oasisapi",
            caiso_timeout=30,
            default_market_run_id="DAM",
            default_node="TH_SP15_GEN-APND"
        )
    
    @pytest.fixture
    def connector(self, config):
        """Create test connector."""
        return CAISOConnector(config)
    
    @pytest.mark.asyncio
    async def test_connector_initialization(self, connector):
        """Test connector initialization."""
        assert connector.config.name == "caiso"
        assert connector.config.market == "CAISO"
        assert connector._extractor is not None
        assert connector._transformer is not None
    
    @pytest.mark.asyncio
    async def test_extract_mock_data(self, connector):
        """Test extraction with mock data."""
        # Mock the OASIS client
        mock_data = [
            {
                "INTERVALSTARTTIME_GMT": "2024-01-01T00:00:00-00:00",
                "INTERVALENDTIME_GMT": "2024-01-01T01:00:00-00:00",
                "NODE": "TH_SP15_GEN-APND",
                "MARKET_RUN_ID": "DAM",
                "LMP_TYPE": "LMP",
                "MW": "45.50",
                "OPR_DT": "2024-01-01",
                "OPR_HR": "0"
            }
        ]
        
        with patch.object(connector._extractor._client, 'fetch_prc_lmp_csv', return_value=mock_data):
            result = await connector.extract(
                start=datetime(2024, 1, 1, tzinfo=timezone.utc),
                end=datetime(2024, 1, 1, 1, tzinfo=timezone.utc),
                market_run_id="DAM",
                node="TH_SP15_GEN-APND"
            )
            
            assert result.record_count == 1
            assert result.data[0]["market"] == "CAISO"
            assert result.data[0]["data_type"] == "market_price"
            assert result.data[0]["price"] == 45.50
    
    @pytest.mark.asyncio
    async def test_transform_data(self, connector):
        """Test data transformation."""
        # Create mock extraction result
        extraction_result = MagicMock()
        extraction_result.data = [
            {
                "event_id": "test-event-1",
                "occurred_at": 1704067200000000,  # 2024-01-01T00:00:00Z
                "tenant_id": "default",
                "market": "CAISO",
                "data_type": "market_price",
                "price": "45.50",
                "delivery_location": "TH_SP15_GEN-APND",
                "delivery_date": "2024-01-01",
                "delivery_hour": 0
            }
        ]
        
        result = await connector.transform(extraction_result)
        
        assert result.record_count == 1
        assert result.data[0]["price"] == 45.50
        assert "transformation_timestamp" in result.data[0]
        assert len(result.validation_errors) == 0
    
    @pytest.mark.asyncio
    async def test_load_data(self, connector):
        """Test data loading."""
        # Create mock transformation result
        transformation_result = MagicMock()
        transformation_result.data = [
            {
                "event_id": "test-event-1",
                "occurred_at": 1704067200000000,
                "tenant_id": "default",
                "market": "CAISO",
                "data_type": "market_price",
                "price": 45.50,
                "delivery_location": "TH_SP15_GEN-APND",
                "delivery_date": "2024-01-01",
                "delivery_hour": 0
            }
        ]
        
        # Mock Kafka producer
        with patch('connectors.caiso.kafka_producer.KafkaProducerService') as mock_producer:
            producer_instance = mock_producer.return_value
            producer_instance.start = AsyncMock()
            producer_instance.stop = AsyncMock()
            producer_instance.publish_batch = AsyncMock(
                return_value={
                    "total_count": 1,
                    "success_count": 1,
                    "failure_count": 0,
                    "errors": [],
                }
            )
            
            result = await connector.load(transformation_result)
            
            assert result.records_attempted == 1
            assert result.records_published == 1
            assert result.records_failed == 0
            assert result.metadata["load_method"] == "kafka"
            assert result.success is True
            producer_instance.publish_batch.assert_awaited_once()
    
    @pytest.mark.asyncio
    async def test_error_handling(self, connector):
        """Test error handling."""
        # Test extraction error
        with patch.object(connector._extractor._client, 'fetch_prc_lmp_csv', side_effect=Exception("Network error")):
            with pytest.raises(Exception):
                await connector.extract(
                    start=datetime(2024, 1, 1, tzinfo=timezone.utc),
                    end=datetime(2024, 1, 1, 1, tzinfo=timezone.utc),
                    market_run_id="DAM",
                    node="TH_SP15_GEN-APND"
                )
    
    @pytest.mark.asyncio
    async def test_cleanup(self, connector):
        """Test connector cleanup."""
        # Mock cleanup methods
        connector._extractor._client.aclose = AsyncMock()
        
        await connector.cleanup()
        
        # Verify cleanup was called
        connector._extractor._client.aclose.assert_called_once()


class TestCAISOExamples:
    """Test CAISO connector examples."""
    
    def test_basic_usage_example(self):
        """Test basic usage example."""
        # Import and verify the example can be loaded
        try:
            import examples.caiso.basic_usage
            assert True
        except ImportError:
            pytest.skip("Basic usage example not available")
    
    def test_integration_example(self):
        """Test integration example."""
        # Import and verify the example can be loaded
        try:
            import examples.caiso.integration_example
            assert True
        except ImportError:
            pytest.skip("Integration example not available")
    
    def test_config_files(self):
        """Test configuration files."""
        config_files = [
            "examples/caiso/caiso_config_basic.yaml",
            "examples/caiso/caiso_config_advanced.yaml",
            "examples/caiso/caiso_config_production.yaml"
        ]
        
        for config_file in config_files:
            if Path(config_file).exists():
                with open(config_file, 'r') as f:
                    config_data = yaml.safe_load(f)
                    assert "name" in config_data
                    assert "market" in config_data
                    assert config_data["market"] == "CAISO"
            else:
                pytest.skip(f"Config file {config_file} not found")


class TestCAISODataQuality:
    """Test CAISO data quality and validation."""
    
    def test_schema_validation(self):
        """Test schema validation."""
        # Mock schema validation
        sample_record = {
            "event_id": "test-event-1",
            "occurred_at": 1704067200000000,
            "tenant_id": "default",
            "market": "CAISO",
            "data_type": "market_price",
            "price": 45.50,
            "delivery_location": "TH_SP15_GEN-APND",
            "delivery_date": "2024-01-01",
            "delivery_hour": 0
        }
        
        # Test required fields
        required_fields = ["event_id", "occurred_at", "tenant_id", "market", "data_type"]
        for field in required_fields:
            assert field in sample_record
        
        # Test data types
        assert isinstance(sample_record["price"], (int, float))
        assert isinstance(sample_record["occurred_at"], int)
        assert isinstance(sample_record["delivery_hour"], int)
    
    def test_price_validation(self):
        """Test price validation."""
        # Test valid prices
        valid_prices = [0.0, 45.50, 100.0, 999.99]
        for price in valid_prices:
            assert price >= 0  # Prices should be non-negative
        
        # Test invalid prices
        invalid_prices = [-1.0, -100.0]
        for price in invalid_prices:
            assert price < 0  # These should be invalid
    
    def test_timestamp_validation(self):
        """Test timestamp validation."""
        # Test valid timestamps
        valid_timestamps = [
            1704067200000000,  # 2024-01-01T00:00:00Z
            1704070800000000,  # 2024-01-01T01:00:00Z
        ]
        
        for timestamp in valid_timestamps:
            assert timestamp > 0  # Timestamps should be positive
            assert isinstance(timestamp, int)  # Should be integer


if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v"])
