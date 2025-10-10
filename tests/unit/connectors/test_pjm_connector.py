"""
Unit tests for the PJM connector.
"""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest

from connectors.base import ExtractionResult, TransformationResult
from connectors.base.exceptions import ExtractionError, TransformationError
from connectors.pjm.config import PJMConnectorConfig
from connectors.pjm.connector import PJMConnector
from connectors.pjm.extractor import PJMExtractor


@pytest.fixture
def pjm_config(sample_connector_config) -> PJMConnectorConfig:
    return PJMConnectorConfig(
        **sample_connector_config,
        api_key="test-key",
        data_miner_base_url="https://example.com/dataminer",
        api_base_url="https://example.com/api",
    )


class TestPJMExtractor:
    @pytest.fixture
    def extractor(self, pjm_config):
        return PJMExtractor(pjm_config)

    @pytest.mark.asyncio
    async def test_extract_lmp_normalization(self, extractor):
        rows = [
            {
                "datetime_beginning_ept": "2024-01-01T00:00:00",
                "pnode_name": "PJM_HUB",
                "total_lmp_da": "42.5",
                "congestion_price_da": "1.2",
                "marginal_loss_price_da": "-0.4",
            }
        ]
        with patch.object(extractor, "_fetch_dataset", AsyncMock(return_value=rows)):
            result = await extractor.extract_data(
                data_type="lmp",
                start_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
                end_time=datetime(2024, 1, 1, 1, tzinfo=timezone.utc),
            )
            assert result == [
                {
                    "timestamp": "2024-01-01T00:00:00",
                    "node": "PJM_HUB",
                    "market_run": "DA",
                    "lmp": 42.5,
                    "congestion": 1.2,
                    "loss": -0.4,
                    "market": "pjm",
                    "data_type": "lmp",
                }
            ]

    @pytest.mark.asyncio
    async def test_extract_outage_normalization(self, extractor):
        rows = [
            {
                "outage_id": "12345",
                "unit_name": "GENERATOR_X",
                "outage_begin_time": "2024-01-01T02:00:00Z",
                "outage_end_time": "2024-01-02T02:00:00Z",
                "outage_mw": "150",
                "outage_reason": "Maintenance",
            }
        ]
        with patch.object(extractor, "_fetch_dataset", AsyncMock(return_value=rows)):
            result = await extractor.extract_data(
                data_type="outages",
                start_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
                end_time=datetime(2024, 1, 2, tzinfo=timezone.utc),
            )
            assert result == [
                {
                    "outage_id": "12345",
                    "resource_name": "GENERATOR_X",
                    "outage_class": None,
                    "status": None,
                    "start_time": "2024-01-01T02:00:00Z",
                    "end_time": "2024-01-02T02:00:00Z",
                    "capacity_mw": 150.0,
                    "reason": "Maintenance",
                    "market": "pjm",
                    "data_type": "outages",
                }
            ]


class TestPJMConnector:
    @pytest.fixture
    def connector(self, pjm_config):
        return PJMConnector(pjm_config)

    @pytest.mark.asyncio
    async def test_extract_updates_metrics(self, connector):
        sample_rows = [{"timestamp": "ts", "node": "PJM_HUB", "market": "pjm", "data_type": "lmp"}]
        with patch.object(connector._extractor, "extract_data", AsyncMock(return_value=sample_rows)) as mock_extract:
            result = await connector.extract(data_type="lmp")
            assert isinstance(result, ExtractionResult)
            assert result.record_count == 1
            assert connector.quality_metrics["total_requests"] == 1
            assert connector.quality_metrics["successful_requests"] == 1
            assert connector.quality_metrics["failed_requests"] == 0
            assert connector.quality_metrics["data_points_extracted"] == 1
            mock_extract.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_extract_invalid_type(self, connector):
        with pytest.raises(ExtractionError):
            await connector.extract(data_type="unknown")

    @pytest.mark.asyncio
    async def test_transform_lmp(self, connector):
        extraction = ExtractionResult(
            data=[
                {
                    "timestamp": "2024-01-01T00:00:00Z",
                    "node": "PJM_HUB",
                    "market_run": "DA",
                    "lmp": 42.5,
                    "congestion": 1.2,
                    "loss": -0.4,
                    "market": "pjm",
                    "data_type": "lmp",
                }
            ],
            metadata={"data_type": "lmp"},
            record_count=1,
        )
        result = await connector.transform(extraction)
        assert isinstance(result, TransformationResult)
        assert result.record_count == 1
        payload = result.data[0]["payload"]
        assert payload["market"] == "pjm"
        assert payload["data_type"] == "lmp"
        assert payload["node"] == "PJM_HUB"

    @pytest.mark.asyncio
    async def test_transform_outage(self, connector):
        extraction = ExtractionResult(
            data=[
                {
                    "outage_id": "12345",
                    "resource_name": "GENERATOR_X",
                    "status": "Planned",
                    "start_time": "2024-01-01T02:00:00Z",
                    "end_time": "2024-01-02T02:00:00Z",
                    "capacity_mw": 150.0,
                    "market": "pjm",
                    "data_type": "outages",
                }
            ],
            metadata={"data_type": "outages"},
            record_count=1,
        )
        result = await connector.transform(extraction, data_type="outages")
        assert result.record_count == 1
        payload = result.data[0]["payload"]
        assert payload["data_type"] == "outages"
        assert payload["outage_id"] == "12345"

    @pytest.mark.asyncio
    async def test_transform_invalid_type(self, connector):
        extraction = ExtractionResult(data=[], metadata={"data_type": "invalid"}, record_count=0)
        with pytest.raises(TransformationError):
            await connector.transform(extraction)

    @pytest.mark.asyncio
    async def test_cleanup(self, connector):
        with patch.object(connector._extractor, "close", AsyncMock()) as mock_close:
            await connector.cleanup()
            mock_close.assert_awaited_once()
