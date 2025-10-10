"""
Unit tests for CAISO connector.
"""

import pytest
import asyncio
from unittest.mock import Mock, AsyncMock, patch
from datetime import datetime, timezone

from connectors.caiso.connector import CAISOConnector
from connectors.caiso.config import CAISOConnectorConfig
from connectors.caiso.extractor import CAISOExtractor
from connectors.caiso.transform import CAISOTransform, CAISOTransformConfig
from connectors.caiso.oasis_client import OASISClientConfig
from connectors.base import ExtractionResult, TransformationResult
from connectors.base.exceptions import ConnectorError, ExtractionError, TransformationError


class TestCAISOConnectorConfig:
    """Test CAISO connector configuration."""

    def test_valid_config(self, sample_connector_config):
        """Test valid configuration creation."""
        config_data = {
            **sample_connector_config,
            "caiso_api_key": "test-api-key",
            "caiso_base_url": "https://oasis.caiso.com",
            "caiso_timeout": 30,
            "caiso_rate_limit": 100
        }

        config = CAISOConnectorConfig(**config_data)
        assert config.caiso_api_key == "test-api-key"
        assert config.caiso_base_url == "https://oasis.caiso.com"
        assert config.caiso_timeout == 30
        assert config.caiso_rate_limit == 100

    def test_config_fields(self):
        """Test configuration field access."""
        config = CAISOConnectorConfig(
            name="test",
            version="1.0.0",
            market="TEST",
            mode="batch",
            output_topic="test",
            schema="test.avsc"
        )

        # Test that default values are applied
        assert config.description == "CAISO market data connector"
        assert config.caiso_base_url == "https://oasis.caiso.com/oasisapi"
        assert config.caiso_timeout == 30


class TestCAISOExtractor:
    """Test CAISO extractor."""

    @pytest.fixture
    def extractor(self, sample_connector_config):
        """Create CAISO extractor."""
        config = CAISOConnectorConfig(
            **sample_connector_config,
            caiso_base_url="https://oasis.caiso.com",
            caiso_timeout=30
        )
        return CAISOExtractor(config)

    @pytest.mark.asyncio
    async def test_extract_prc_lmp_success(self, extractor, sample_raw_record):
        """Test successful PRC LMP extraction."""
        with patch.object(extractor._client, 'fetch_prc_lmp_csv') as mock_fetch:
            # Mock the CSV rows that would be returned
            mock_rows = [
                {
                    'INTERVALSTARTTIME_GMT': '2022-01-01T00:00:00',
                    'NODE': 'TEST_NODE',
                    'LMP_TYPE': 'LMP',
                    'MW': '100.0',
                    'VALUE': '50.25'
                }
            ]
            mock_fetch.return_value = mock_rows

            result = await extractor.extract_prc_lmp(
                start="2022-01-01T00:00:00",
                end="2022-01-01T01:00:00",
                market_run_id="DAM",
                node="TEST_NODE"
            )

            assert isinstance(result, ExtractionResult)
            assert result.record_count == 1
            assert len(result.data) == 1
            assert result.data[0]["event_id"] is not None
            assert result.data[0]["market"] == "CAISO"
        await extractor.close()

    @pytest.mark.asyncio
    async def test_extract_prc_lmp_failure(self, extractor):
        """Test PRC LMP extraction failure."""
        with patch.object(extractor._client, 'fetch_prc_lmp_csv') as mock_fetch:
            mock_fetch.side_effect = Exception("API Error")

            with pytest.raises(ExtractionError):
                await extractor.extract_prc_lmp(
                    start="2022-01-01T00:00:00",
                    end="2022-01-01T01:00:00",
                    market_run_id="DAM",
                    node="TEST_NODE"
                )



class TestCAISOTransform:
    """Test CAISO transform."""

    @pytest.fixture
    def transformer(self, sample_connector_config):
        """Create CAISO transformer."""
        config = CAISOConnectorConfig(
            **sample_connector_config,
            caiso_base_url="https://oasis.caiso.com"
        )
        return CAISOTransform(config)

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
            extraction_result = ExtractionResult(
                data=[{"invalid": "data"}],
                metadata={"test": "metadata"},
                record_count=1
            )
            await transformer.transform(extraction_result)


class TestCAISOConnector:
    """Test CAISO connector."""

    @pytest.fixture
    def connector(self, sample_connector_config):
        """Create CAISO connector."""
        config = CAISOConnectorConfig(
            **sample_connector_config,
            caiso_base_url="https://oasis.caiso.com",
            caiso_timeout=30
        )
        return CAISOConnector(config)

    def test_connector_initialization(self, connector):
        """Test connector initialization."""
        assert connector.config.name == "test_connector"
        assert connector.config.market == "TEST"
        assert connector._extractor is not None
        assert connector._transformer is not None

    def test_get_health_status(self, connector):
        """Test health status."""
        status = connector.get_health_status()
        assert "status" in status
        assert "name" in status
        assert "market" in status

    def test_get_metrics(self, connector):
        """Test metrics."""
        metrics = connector.get_metrics()
        assert "quality_metrics" in metrics
        assert "success_rate" in metrics

    def test_get_connector_info(self, connector):
        """Test connector info."""
        info = connector.get_connector_info()
        assert info["name"] == "test_connector"
        assert info["market"] == "TEST"
        assert "caiso_base_url" in info
        assert "transforms_enabled" in info

    def test_get_extraction_capabilities(self, connector):
        """Test extraction capabilities."""
        capabilities = connector.get_extraction_capabilities()
        assert "data_types" in capabilities
        assert "modes" in capabilities
        assert "batch_parameters" in capabilities
        assert "realtime_parameters" in capabilities

    def test_get_transformation_capabilities(self, connector):
        """Test transformation capabilities."""
        capabilities = connector.get_transformation_capabilities()
        assert "schema_versions" in capabilities
        assert "transforms" in capabilities
        assert "output_formats" in capabilities
        assert "config" in capabilities

    @pytest.mark.asyncio
    async def test_extract_lmp(self, connector, sample_raw_record):
        """Test LMP extraction flow."""
        with patch.object(connector._extractor, 'extract_prc_lmp') as mock_extract:
            mock_extract.return_value = ExtractionResult(
                data=[sample_raw_record],
                metadata={"test": "metadata"},
                record_count=1
            )

            result = await connector.extract(
                data_type="lmp",
                start="2022-01-01T00:00:00",
                end="2022-01-01T01:00:00",
                node="TEST_NODE"
            )

            assert isinstance(result, ExtractionResult)
            assert result.record_count == 1
            mock_extract.assert_called_once()

    @pytest.mark.asyncio
    async def test_extract_edam(self, connector, sample_raw_record):
        """Test EDAM extraction flow."""
        with patch.object(connector._extractor, 'extract_edam') as mock_extract:
            mock_extract.return_value = ExtractionResult(
                data=[sample_raw_record],
                metadata={"test": "metadata"},
                record_count=1
            )

            result = await connector.extract(
                data_type="edam",
                start="2022-01-01T00:00:00",
                end="2022-01-01T01:00:00"
            )

            assert isinstance(result, ExtractionResult)
            assert result.record_count == 1
            mock_extract.assert_called_once()

    @pytest.mark.asyncio
    async def test_extract_missing_parameters(self, connector):
        """Test extraction with missing parameters."""
        # The current implementation validates parameters
        with pytest.raises(ExtractionError):
            await connector.extract(data_type="lmp")

    @pytest.mark.asyncio
    async def test_extract_requires_node_for_lmp(self, connector):
        """Ensure node is required for LMP extractions."""
        with pytest.raises(ExtractionError):
            await connector.extract(
                data_type="lmp",
                start="2022-01-01T00:00:00",
                end="2022-01-01T01:00:00",
            )

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


class TestOASISClient:
    """Test OASIS client."""

    @pytest.fixture
    def client_config(self):
        """Create OASIS client config."""
        return OASISClientConfig(
            base_url="https://oasis.caiso.com",
            timeout=30
        )

    def test_client_config_initialization(self, client_config):
        """Test client config initialization."""
        assert client_config.base_url == "https://oasis.caiso.com"
        assert client_config.timeout == 30

    @pytest.mark.asyncio
    async def test_oasis_client_creation(self, client_config):
        """Test OASIS client creation."""
        # This would test the actual OASISClient creation
        # For now, just test that the config exists
        assert client_config is not None
