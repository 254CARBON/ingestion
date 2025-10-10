"""
Unit tests for the ISO-NE connector.
"""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest

from connectors.base import ExtractionResult, TransformationResult
from connectors.base.exceptions import ExtractionError, TransformationError
from connectors.isone.config import ISONEConnectorConfig
from connectors.isone.connector import ISONEConnector
from connectors.isone.extractor import ISONEExtractor


@pytest.fixture
def isone_config(sample_connector_config) -> ISONEConnectorConfig:
    return ISONEConnectorConfig(
        **sample_connector_config,
        api_key="test-key",
        base_url="https://example.com/isone",
    )


class TestISONEExtractor:
    @pytest.fixture
    def extractor(self, isone_config):
        return ISONEExtractor(isone_config)

    @pytest.mark.asyncio
    async def test_extract_lmp_normalization(self, extractor):
        rows = [
            {
                "timestamp": "2024-01-10T00:00:00Z",
                "node_id": "ISONE_HUB",
                "lmp": "55.2",
                "congestion_component": "2.1",
                "losses_component": "-0.5",
            }
        ]
        with patch.object(extractor, "_fetch_dataset", AsyncMock(return_value=rows)):
            result = await extractor.extract_data(
                data_type="lmp",
                start_time=datetime(2024, 1, 10, tzinfo=timezone.utc),
                end_time=datetime(2024, 1, 10, 1, tzinfo=timezone.utc),
            )
            assert result == [
                {
                    "timestamp": "2024-01-10T00:00:00Z",
                    "node": "ISONE_HUB",
                    "market_run": "DA",
                    "lmp": 55.2,
                    "congestion": 2.1,
                    "losses": -0.5,
                    "market": "isone",
                    "data_type": "lmp",
                }
            ]

    @pytest.mark.asyncio
    async def test_extract_outage_normalization(self, extractor):
        rows = [
            {
                "outage_id": "A1",
                "resource_name": "GEN_Z",
                "start_time": "2024-02-01T04:00:00Z",
                "end_time": "2024-02-02T04:00:00Z",
                "derate_mw": "200",
                "reason": "Inspection",
            }
        ]
        with patch.object(extractor, "_fetch_dataset", AsyncMock(return_value=rows)):
            result = await extractor.extract_data(
                data_type="outages",
                start_time=datetime(2024, 2, 1, tzinfo=timezone.utc),
                end_time=datetime(2024, 2, 3, tzinfo=timezone.utc),
            )
            assert result == [
                {
                    "outage_id": "A1",
                    "resource_name": "GEN_Z",
                    "status": None,
                    "start_time": "2024-02-01T04:00:00Z",
                    "end_time": "2024-02-02T04:00:00Z",
                    "derate_mw": 200.0,
                    "reason": "Inspection",
                    "market": "isone",
                    "data_type": "outages",
                }
            ]


class TestISONEConnector:
    @pytest.fixture
    def connector(self, isone_config):
        return ISONEConnector(isone_config)

    @pytest.mark.asyncio
    async def test_extract_updates_metrics(self, connector):
        sample_rows = [{"timestamp": "ts", "node": "ISONE_HUB", "market": "isone", "data_type": "lmp"}]
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
            await connector.extract(data_type="bad")

    @pytest.mark.asyncio
    async def test_transform_lmp(self, connector):
        extraction = ExtractionResult(
            data=[
                {
                    "timestamp": "2024-01-10T00:00:00Z",
                    "node": "ISONE_HUB",
                    "market_run": "DA",
                    "lmp": 55.2,
                    "congestion": 2.1,
                    "losses": -0.5,
                    "market": "isone",
                    "data_type": "lmp",
                }
            ],
            metadata={"data_type": "lmp"},
            record_count=1,
        )
        result = await connector.transform(extraction)
        assert isinstance(result, TransformationResult)
        payload = result.data[0]["payload"]
        assert payload["data_type"] == "lmp"
        assert payload["node"] == "ISONE_HUB"

    @pytest.mark.asyncio
    async def test_transform_outage(self, connector):
        extraction = ExtractionResult(
            data=[
                {
                    "outage_id": "A1",
                    "resource_name": "GEN_Z",
                    "status": "Planned",
                    "start_time": "2024-02-01T04:00:00Z",
                    "end_time": "2024-02-02T04:00:00Z",
                    "derate_mw": 200.0,
                    "market": "isone",
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
        assert payload["outage_id"] == "A1"

    @pytest.mark.asyncio
    async def test_transform_invalid_type(self, connector):
        extraction = ExtractionResult(data=[], metadata={"data_type": "unknown"}, record_count=0)
        with pytest.raises(TransformationError):
            await connector.transform(extraction)

    @pytest.mark.asyncio
    async def test_cleanup(self, connector):
        with patch.object(connector._extractor, "close", AsyncMock()) as mock_close:
            await connector.cleanup()
            mock_close.assert_awaited_once()
