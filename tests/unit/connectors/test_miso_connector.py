"""
Unit tests for the MISO connector.
"""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest

from connectors.base import ExtractionResult, TransformationResult
from connectors.base.exceptions import ExtractionError, TransformationError
from connectors.miso.config import MISOConnectorConfig
from connectors.miso.connector import MISOConnector
from connectors.miso.extractor import MISOExtractor


@pytest.fixture
def miso_config(sample_connector_config) -> MISOConnectorConfig:
    return MISOConnectorConfig(
        **sample_connector_config,
        miso_api_key="test-key",
        miso_base_url="https://example.com",
    )


class TestMISOExtractor:
    @pytest.fixture
    def extractor(self, miso_config):
        return MISOExtractor(miso_config)

    @pytest.mark.asyncio
    async def test_extract_lmp_normalization(self, extractor):
        rows = [
            {
                "timestamp": "2024-01-01T00:00:00Z",
                "settlementLocation": "HUB_A",
                "marketRunId": "RT",
                "lmp": "32.45",
                "congestion": "1.5",
                "loss": "-0.2",
                "energy": "31.15",
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
                    "timestamp": "2024-01-01T00:00:00Z",
                    "node": "HUB_A",
                    "market_run": "RT",
                    "lmp": 32.45,
                    "congestion": 1.5,
                    "loss": -0.2,
                    "energy": 31.15,
                    "market": "miso",
                    "data_type": "lmp",
                }
            ]

    @pytest.mark.asyncio
    async def test_extract_as_normalization(self, extractor):
        rows = [
            {
                "timestamp": "2024-01-01T00:00:00Z",
                "product": "REGUP",
                "zone": "NORTH",
                "clearedPrice": "5.5",
                "clearedMw": "120",
            }
        ]
        with patch.object(extractor, "_fetch_dataset", AsyncMock(return_value=rows)):
            result = await extractor.extract_data(
                data_type="as",
                start_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
                end_time=datetime(2024, 1, 1, 1, tzinfo=timezone.utc),
            )
            assert result == [
                {
                    "timestamp": "2024-01-01T00:00:00Z",
                    "product": "REGUP",
                    "zone": "NORTH",
                    "cleared_price": 5.5,
                    "cleared_mw": 120.0,
                    "market": "miso",
                    "data_type": "as",
                }
            ]

    @pytest.mark.asyncio
    async def test_extract_pra_normalization(self, extractor):
        rows = [
            {
                "auction": "2024 PRA R1",
                "planningZone": "ZONE_1",
                "auctionDate": "2024-03-01",
                "planningYear": "2024-2025",
                "clearingPrice": "150.25",
                "awardedMw": "500",
            }
        ]
        with patch.object(extractor, "_fetch_dataset", AsyncMock(return_value=rows)):
            result = await extractor.extract_data(
                data_type="pra",
                start_time=datetime(2024, 2, 1, tzinfo=timezone.utc),
                end_time=datetime(2024, 4, 1, tzinfo=timezone.utc),
            )
            assert result == [
                {
                    "auction": "2024 PRA R1",
                    "auction_date": "2024-03-01",
                    "planning_zone": "ZONE_1",
                    "planning_year": "2024-2025",
                    "clearing_price": 150.25,
                    "awarded_mw": 500.0,
                    "market": "miso",
                    "data_type": "pra",
                }
            ]

    @pytest.mark.asyncio
    async def test_extract_arr_normalization(self, extractor):
        rows = [
            {
                "auction": "ARR 2024",
                "pathName": "A-B",
                "auctionDate": "2024-05-01",
                "sourceNode": "A",
                "sinkNode": "B",
                "classType": "HDA",
                "clearingPrice": "2.75",
                "awardedMw": "100",
            }
        ]
        with patch.object(extractor, "_fetch_dataset", AsyncMock(return_value=rows)):
            result = await extractor.extract_data(
                data_type="arr",
                start_time=datetime(2024, 5, 1, tzinfo=timezone.utc),
                end_time=datetime(2024, 5, 2, tzinfo=timezone.utc),
            )
            assert result == [
                {
                    "auction": "ARR 2024",
                    "auction_date": "2024-05-01",
                    "path": "A-B",
                    "source": "A",
                    "sink": "B",
                    "class_type": "HDA",
                    "clearing_price": 2.75,
                    "awarded_mw": 100.0,
                    "market": "miso",
                    "data_type": "arr",
                }
            ]

    @pytest.mark.asyncio
    async def test_extract_tcr_normalization(self, extractor):
        rows = [
            {
                "auction": "TCR JUL-2024",
                "path": "B-C",
                "auctionDate": "2024-07-15",
                "source": "B",
                "sink": "C",
                "classType": "IDAC",
                "round": "R1",
                "clearingPrice": "3.5",
                "awardedMw": "80",
            }
        ]
        with patch.object(extractor, "_fetch_dataset", AsyncMock(return_value=rows)):
            result = await extractor.extract_data(
                data_type="tcr",
                start_time=datetime(2024, 7, 1, tzinfo=timezone.utc),
                end_time=datetime(2024, 7, 31, tzinfo=timezone.utc),
            )
            assert result == [
                {
                    "auction": "TCR JUL-2024",
                    "auction_date": "2024-07-15",
                    "path": "B-C",
                    "source": "B",
                    "sink": "C",
                    "class_type": "IDAC",
                    "round_id": "R1",
                    "clearing_price": 3.5,
                    "awarded_mw": 80.0,
                    "market": "miso",
                    "data_type": "tcr",
                }
            ]


class TestMISOConnector:
    @pytest.fixture
    def connector(self, miso_config):
        return MISOConnector(miso_config)

    @pytest.mark.asyncio
    async def test_extract_updates_metrics(self, connector):
        sample_rows = [
            {
                "timestamp": "2024-01-01T00:00:00Z",
                "node": "HUB_A",
                "market_run": "RT",
                "lmp": 32.45,
                "market": "miso",
                "data_type": "lmp",
            }
        ]
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
    async def test_extract_unsupported_type(self, connector):
        with pytest.raises(ExtractionError):
            await connector.extract(data_type="invalid")

    @pytest.mark.asyncio
    async def test_transform_lmp(self, connector):
        extraction = ExtractionResult(
            data=[
                {
                    "timestamp": "2024-01-01T00:00:00Z",
                    "node": "HUB_A",
                    "market_run": "RT",
                    "lmp": 32.45,
                    "congestion": 1.5,
                    "loss": -0.2,
                    "energy": 31.15,
                    "market": "miso",
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
        assert payload["market"] == "miso"
        assert payload["data_type"] == "lmp"
        assert payload["node"] == "HUB_A"

    @pytest.mark.asyncio
    async def test_transform_pra(self, connector):
        extraction = ExtractionResult(
            data=[
                {
                    "auction": "2024 PRA R1",
                    "auction_date": "2024-03-01",
                    "planning_zone": "ZONE_1",
                    "planning_year": "2024-2025",
                    "clearing_price": 150.25,
                    "awarded_mw": 500.0,
                    "market": "miso",
                    "data_type": "pra",
                }
            ],
            metadata={"data_type": "pra"},
            record_count=1,
        )
        result = await connector.transform(extraction, data_type="pra")
        assert result.record_count == 1
        payload = result.data[0]["payload"]
        assert payload["data_type"] == "pra"
        assert payload["planning_zone"] == "ZONE_1"

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
