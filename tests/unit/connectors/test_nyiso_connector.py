"""
Unit tests for the NYISO connector.
"""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest

from connectors.base import ExtractionResult, TransformationResult
from connectors.base.exceptions import ExtractionError, TransformationError
from connectors.nyiso.config import NYISOConnectorConfig
from connectors.nyiso.connector import NYISOConnector
from connectors.nyiso.extractor import NYISOExtractor


@pytest.fixture
def nyiso_config(sample_connector_config) -> NYISOConnectorConfig:
    return NYISOConnectorConfig(
        **sample_connector_config,
        api_key="test-key",
        oasis_base_url="https://example.com/oasis",
        rest_base_url="https://example.com/rest",
    )


class TestNYISOExtractor:
    @pytest.fixture
    def extractor(self, nyiso_config):
        return NYISOExtractor(nyiso_config)

    @pytest.mark.asyncio
    async def test_extract_lbmp_normalization(self, extractor):
        rows = [
            {
                "time_stamp": "2024-01-01T00:00:00Z",
                "node_id": "ZONE_A",
                "lbmp": "32.15",
                "congestion_component": "1.5",
                "losses_component": "-0.3",
            }
        ]
        with patch.object(extractor, "_fetch_dataset", AsyncMock(return_value=rows)):
            result = await extractor.extract_data(
                data_type="lbmp",
                start_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
                end_time=datetime(2024, 1, 1, 1, tzinfo=timezone.utc),
            )
            assert result == [
                {
                    "timestamp": "2024-01-01T00:00:00Z",
                    "node": "ZONE_A",
                    "market_run": "DA",
                    "lbmp": 32.15,
                    "congestion": 1.5,
                    "losses": -0.3,
                    "market": "nyiso",
                    "data_type": "lbmp",
                }
            ]

    @pytest.mark.asyncio
    async def test_extract_outage_normalization(self, extractor):
        rows = [
            {
                "outage_id": "5678",
                "unit_name": "UNIT_Y",
                "start_time": "2024-01-05T02:00:00Z",
                "end_time": "2024-01-05T10:00:00Z",
                "derate_mw": "75",
                "reason": "Forced outage",
            }
        ]
        with patch.object(extractor, "_fetch_dataset", AsyncMock(return_value=rows)):
            result = await extractor.extract_data(
                data_type="outages",
                start_time=datetime(2024, 1, 5, tzinfo=timezone.utc),
                end_time=datetime(2024, 1, 6, tzinfo=timezone.utc),
            )
            assert result == [
                {
                    "outage_id": "5678",
                    "resource_name": "UNIT_Y",
                    "status": None,
                    "start_time": "2024-01-05T02:00:00Z",
                    "end_time": "2024-01-05T10:00:00Z",
                    "derate_mw": 75.0,
                    "reason": "Forced outage",
                    "market": "nyiso",
                    "data_type": "outages",
                }
            ]


class TestNYISOConnector:
    @pytest.fixture
    def connector(self, nyiso_config):
        return NYISOConnector(nyiso_config)

    @pytest.mark.asyncio
    async def test_extract_updates_metrics(self, connector):
        sample_rows = [{"timestamp": "ts", "node": "ZONE_A", "market": "nyiso", "data_type": "lbmp"}]
        with patch.object(connector._extractor, "extract_data", AsyncMock(return_value=sample_rows)) as mock_extract:
            result = await connector.extract(data_type="lbmp")
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
            await connector.extract(data_type="invalid")

    @pytest.mark.asyncio
    async def test_transform_lbmp(self, connector):
        extraction = ExtractionResult(
            data=[
                {
                    "timestamp": "2024-01-01T00:00:00Z",
                    "node": "ZONE_A",
                    "market_run": "DA",
                    "lbmp": 32.15,
                    "congestion": 1.5,
                    "losses": -0.3,
                    "market": "nyiso",
                    "data_type": "lbmp",
                }
            ],
            metadata={"data_type": "lbmp"},
            record_count=1,
        )
        result = await connector.transform(extraction)
        assert isinstance(result, TransformationResult)
        assert result.record_count == 1
        payload = result.data[0]["payload"]
        assert payload["data_type"] == "lbmp"
        assert payload["node"] == "ZONE_A"

    @pytest.mark.asyncio
    async def test_transform_outage(self, connector):
        extraction = ExtractionResult(
            data=[
                {
                    "outage_id": "5678",
                    "resource_name": "UNIT_Y",
                    "status": "Forced",
                    "start_time": "2024-01-05T02:00:00Z",
                    "end_time": "2024-01-05T10:00:00Z",
                    "derate_mw": 75.0,
                    "market": "nyiso",
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
        assert payload["outage_id"] == "5678"

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
