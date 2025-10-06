"""
Unit tests for MISO connector.
"""

import pytest
import asyncio
from unittest.mock import Mock, AsyncMock, patch
from datetime import datetime, timezone

from connectors.miso.connector import MISOConnector
from connectors.miso.config import MISOConnectorConfig
from connectors.miso.extractor import MISOExtractor, MISOAuthConfig
from connectors.miso.transform import MISOTransform, MISOTransformConfig
from connectors.base import ExtractionResult, TransformationResult
from connectors.base.exceptions import ConnectorError, ExtractionError, TransformationError


class TestMISOConnectorConfig:
    """Test MISO connector configuration."""
    
    def test_valid_config(self, sample_connector_config):
        """Test valid configuration creation."""
        config_data = {
            **sample_connector_config,
            "miso_api_key": "test-api-key",
            "miso_base_url": "https://api.misoenergy.org",
            "miso_timeout": 30,
            "miso_rate_limit": 100
        }
        
        config = MISOConnectorConfig(**config_data)
        
        assert config.name == "test_connector"
        assert config.market == "TEST"
        assert config.miso_api_key == "test-api-key"
        assert config.miso_base_url == "https://api.misoenergy.org"
        assert config.miso_timeout == 30
        assert config.miso_rate_limit == 100
    
    def test_invalid_config(self):
        """Test invalid configuration raises error."""
        with pytest.raises(ValueError):
            MISOConnectorConfig(
                name="test",
                version="1.0.0",
                market="TEST",
                mode="batch",
                output_topic="test",
                schema="test.avsc",
                # Missing required miso_api_key
            )


class TestMISOExtractor:
    """Test MISO extractor."""
    
    @pytest.fixture
    def auth_config(self):
        """Create MISO auth configuration."""
        return MISOAuthConfig(
            api_key="test-api-key",
            base_url="https://api.misoenergy.org",
            timeout=30,
            rate_limit=100
        )
    
    @pytest.fixture
    def extractor(self, auth_config):
        """Create MISO extractor."""
        return MISOExtractor(auth_config)
    
    @pytest.mark.asyncio
    async def test_extract_trades_success(self, extractor, sample_raw_record):
        """Test successful trade extraction."""
        with patch('connectors.miso.extractor.MISOApiClient') as mock_client_class:
            mock_client = AsyncMock()
            mock_client.get_trade_data.return_value = [sample_raw_record]
            mock_client_class.return_value.__aenter__.return_value = mock_client
            
            result = await extractor.extract_trades(
                start_date="2022-01-01",
                end_date="2022-01-01"
            )
            
            assert isinstance(result, ExtractionResult)
            assert result.record_count == 1
            assert len(result.data) == 1
            assert result.data[0]["event_id"] is not None
            assert result.data[0]["market"] == "MISO"
    
    @pytest.mark.asyncio
    async def test_extract_trades_failure(self, extractor):
        """Test trade extraction failure."""
        with patch('connectors.miso.extractor.MISOApiClient') as mock_client_class:
            mock_client = AsyncMock()
            mock_client.get_trade_data.side_effect = Exception("API Error")
            mock_client_class.return_value.__aenter__.return_value = mock_client
            
            with pytest.raises(ExtractionError):
                await extractor.extract_trades(
                    start_date="2022-01-01",
                    end_date="2022-01-01"
                )
    
    @pytest.mark.asyncio
    async def test_extract_realtime_success(self, extractor, sample_raw_record):
        """Test successful real-time extraction."""
        with patch('connectors.miso.extractor.MISOApiClient') as mock_client_class:
            mock_client = AsyncMock()
            mock_client.get_realtime_data.return_value = [sample_raw_record]
            mock_client_class.return_value.__aenter__.return_value = mock_client
            
            result = await extractor.extract_realtime()
            
            assert isinstance(result, ExtractionResult)
            assert result.record_count == 1
            assert len(result.data) == 1
            assert result.data[0]["event_id"] is not None


class TestMISOTransform:
    """Test MISO transformer."""
    
    @pytest.fixture
    def transform_config(self):
        """Create MISO transform configuration."""
        return MISOTransformConfig(
            timezone="UTC",
            decimal_precision=6,
            validate_required_fields=True,
            sanitize_numeric=True,
            standardize_timezone=True
        )
    
    @pytest.fixture
    def transformer(self, transform_config):
        """Create MISO transformer."""
        return MISOTransform(transform_config)
    
    def test_sanitize_numeric(self, transformer):
        """Test numeric sanitization."""
        # Test valid numeric values
        assert transformer.sanitize_numeric("50.25", "price") == 50.25
        assert transformer.sanitize_numeric(100.0, "quantity") == 100.0
        assert transformer.sanitize_numeric(42, "count") == 42.0
        
        # Test invalid values
        assert transformer.sanitize_numeric(None, "price") is None
        assert transformer.sanitize_numeric("", "price") is None
        assert transformer.sanitize_numeric("invalid", "price") is None
        assert transformer.sanitize_numeric("NULL", "price") is None
    
    def test_standardize_timezone(self, transformer):
        """Test timezone standardization."""
        # Test valid timestamp
        timestamp = 1640995200000000  # 2022-01-01 00:00:00 UTC
        result = transformer.standardize_timezone(timestamp)
        assert result == timestamp
        
        # Test invalid timestamp
        invalid_timestamp = -1
        result = transformer.standardize_timezone(invalid_timestamp)
        assert result == invalid_timestamp
    
    def test_validate_record(self, transformer, sample_normalized_record):
        """Test record validation."""
        errors = transformer.validate_record(sample_normalized_record)
        assert len(errors) == 0
        
        # Test missing required fields
        invalid_record = {"event_id": "test"}
        errors = transformer.validate_record(invalid_record)
        assert len(errors) > 0
        assert any("Missing required field" in error for error in errors)
    
    @pytest.mark.asyncio
    async def test_transform_success(self, transformer, sample_raw_record):
        """Test successful transformation."""
        extraction_result = ExtractionResult(
            data=[sample_raw_record],
            metadata={"test": "metadata"},
            record_count=1
        )
        
        result = await transformer.transform(extraction_result)
        
        assert isinstance(result, TransformationResult)
        assert result.record_count == 1
        assert len(result.data) == 1
        assert result.data[0]["event_id"] == sample_raw_record["event_id"]
    
    @pytest.mark.asyncio
    async def test_transform_failure(self, transformer):
        """Test transformation failure."""
        with pytest.raises(TransformationError):
            await transformer.transform(None)


class TestMISOConnector:
    """Test MISO connector."""
    
    @pytest.fixture
    def connector_config(self, sample_connector_config):
        """Create MISO connector configuration."""
        config_data = {
            **sample_connector_config,
            "miso_api_key": "test-api-key",
            "miso_base_url": "https://api.misoenergy.org",
            "miso_timeout": 30,
            "miso_rate_limit": 100
        }
        return MISOConnectorConfig(**config_data)
    
    @pytest.fixture
    def connector(self, connector_config):
        """Create MISO connector."""
        return MISOConnector(connector_config)
    
    def test_connector_initialization(self, connector):
        """Test connector initialization."""
        assert connector.config.name == "test_connector"
        assert connector.config.market == "TEST"
        assert connector._extractor is not None
        assert connector._transformer is not None
    
    def test_get_health_status(self, connector):
        """Test health status."""
        status = connector.get_health_status()
        
        assert status["name"] == "test_connector"
        assert status["version"] == "1.0.0"
        assert status["market"] == "TEST"
        assert status["enabled"] is True
    
    def test_get_connector_info(self, connector):
        """Test connector info."""
        info = connector.get_connector_info()
        
        assert "name" in info
        assert "version" in info
        assert "market" in info
        assert "miso_base_url" in info
        assert "transforms_enabled" in info
    
    def test_get_extraction_capabilities(self, connector):
        """Test extraction capabilities."""
        capabilities = connector.get_extraction_capabilities()
        
        assert "modes" in capabilities
        assert "batch_parameters" in capabilities
        assert "realtime_parameters" in capabilities
        assert "supported_formats" in capabilities
        assert "rate_limits" in capabilities
    
    def test_get_transformation_capabilities(self, connector):
        """Test transformation capabilities."""
        capabilities = connector.get_transformation_capabilities()
        
        assert "config" in capabilities
        assert "transforms_available" in capabilities
        assert "validation_enabled" in capabilities
    
    @pytest.mark.asyncio
    async def test_extract_batch_mode(self, connector, sample_raw_record):
        """Test batch mode extraction."""
        with patch.object(connector._extractor, 'extract_trades') as mock_extract:
            mock_extract.return_value = ExtractionResult(
                data=[sample_raw_record],
                metadata={"test": "metadata"},
                record_count=1
            )
            
            result = await connector.extract(
                mode="batch",
                start_date="2022-01-01",
                end_date="2022-01-01"
            )
            
            assert isinstance(result, ExtractionResult)
            assert result.record_count == 1
            mock_extract.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_extract_realtime_mode(self, connector, sample_raw_record):
        """Test real-time mode extraction."""
        with patch.object(connector._extractor, 'extract_realtime') as mock_extract:
            mock_extract.return_value = ExtractionResult(
                data=[sample_raw_record],
                metadata={"test": "metadata"},
                record_count=1
            )
            
            result = await connector.extract(mode="realtime")
            
            assert isinstance(result, ExtractionResult)
            assert result.record_count == 1
            mock_extract.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_extract_missing_parameters(self, connector):
        """Test extraction with missing parameters."""
        with pytest.raises(ConnectorError):
            await connector.extract(mode="batch")
    
    @pytest.mark.asyncio
    async def test_transform(self, connector, sample_raw_record):
        """Test transformation."""
        extraction_result = ExtractionResult(
            data=[sample_raw_record],
            metadata={"test": "metadata"},
            record_count=1
        )
        
        with patch.object(connector._transformer, 'transform') as mock_transform:
            mock_transform.return_value = TransformationResult(
                data=[sample_raw_record],
                metadata={"test": "metadata"},
                record_count=1,
                validation_errors=[]
            )
            
            result = await connector.transform(extraction_result)
            
            assert isinstance(result, TransformationResult)
            assert result.record_count == 1
            mock_transform.assert_called_once_with(extraction_result)
    
    @pytest.mark.asyncio
    async def test_run_batch(self, connector, sample_raw_record):
        """Test batch run."""
        with patch.object(connector, 'run') as mock_run:
            mock_run.return_value = True
            
            result = await connector.run_batch(
                start_date="2022-01-01",
                end_date="2022-01-01"
            )
            
            assert result is True
            mock_run.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_run_realtime(self, connector):
        """Test real-time run."""
        with patch.object(connector, 'run') as mock_run:
            mock_run.return_value = True
            
            result = await connector.run_realtime()
            
            assert result is True
            mock_run.assert_called_once()
