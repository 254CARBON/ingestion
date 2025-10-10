"""
Unit tests for ERCOT connector.
"""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest

from connectors.base import ExtractionResult, TransformationResult
from connectors.base.exceptions import ExtractionError, TransformationError
from connectors.ercot.config import ERCOTConnectorConfig
from connectors.ercot.connector import ERCOTConnector
from connectors.ercot.extractor import ERCOTExtractor


@pytest.fixture
def ercot_config(sample_connector_config) -> ERCOTConnectorConfig:
    return ERCOTConnectorConfig(
        **sample_connector_config,
        base_url="https://example.com",
    )


class TestERCOTExtractor:
    @pytest.fixture
    def extractor(self, ercot_config):
        return ERCOTExtractor(ercot_config)

    @pytest.mark.asyncio
    async def test_extract_dam_normalization(self, extractor):
        rows = [
            {
                "timestamp": "2024-01-01T00:00:00Z",
                "node": "HB_NORTH",
                "lmp": "35.5",
                "mcc": "2.3",
                "mlc": "-1.0",
            }
        ]
        with patch.object(extractor, "_fetch_dataset", AsyncMock(return_value=rows)):
            result = await extractor.extract_data(
                data_type="dam",
                start_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
                end_time=datetime(2024, 1, 1, 1, tzinfo=timezone.utc),
            )
            assert result == [
                {
                    "timestamp": "2024-01-01T00:00:00Z",
                    "node": "HB_NORTH",
                    "lmp": 35.5,
                    "mcc": 2.3,
                    "mlc": -1.0,
                    "data_type": "dam",
                }
            ]
        await extractor.close()

    @pytest.mark.asyncio
    async def test_extract_ordc_normalization(self, extractor):
        rows = [
            {
                "timestamp": "2024-01-01T00:00:00Z",
                "reserve_zone": "R1",
                "ordc_price": "150.0",
                "demand_curve_mw": "250.5",
            }
        ]
        with patch.object(extractor, "_fetch_dataset", AsyncMock(return_value=rows)):
            result = await extractor.extract_data(
                data_type="ordc",
                start_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
                end_time=datetime(2024, 1, 1, 1, tzinfo=timezone.utc),
            )
            assert result == [
                {
                    "timestamp": "2024-01-01T00:00:00Z",
                    "reserve_zone": "R1",
                    "ordc_price": 150.0,
                    "demand_curve_mw": 250.5,
                }
            ]
        await extractor.close()

    @pytest.mark.asyncio
    async def test_extract_outages_normalization(self, extractor):
        rows = [
            {
                "outage_id": "OUT123",
                "unit_name": "PLANT_A",
                "outage_type": "Forced",
                "start_time": "2024-01-01T00:00:00Z",
                "end_time": "2024-01-02T00:00:00Z",
                "capacity_mw": "150",
            }
        ]
        with patch.object(extractor, "_fetch_dataset", AsyncMock(return_value=rows)):
            result = await extractor.extract_data(
                data_type="outages",
                start_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
                end_time=datetime(2024, 1, 1, 1, tzinfo=timezone.utc),
            )
            assert result == [
                {
                    "outage_id": "OUT123",
                    "unit_name": "PLANT_A",
                    "outage_type": "Forced",
                    "start_time": "2024-01-01T00:00:00Z",
                    "end_time": "2024-01-02T00:00:00Z",
                    "capacity_mw": 150.0,
                    "reason": None,
                }
            ]
        await extractor.close()


class TestERCOTConnector:
    @pytest.fixture
    def connector(self, ercot_config):
        return ERCOTConnector(ercot_config)

    @pytest.mark.asyncio
    async def test_extract_updates_metrics(self, connector):
        sample_rows = [
            {
                "timestamp": "2024-01-01T00:00:00Z",
                "node": "HB_NORTH",
                "lmp": 35.5,
                "mcc": 2.3,
                "mlc": -1.0,
                "data_type": "dam",
            }
        ]
        with patch.object(connector._extractor, "extract_data", AsyncMock(return_value=sample_rows)) as mock_extract:
            result = await connector.extract(data_type="dam")
            assert isinstance(result, ExtractionResult)
            assert result.record_count == 1
            assert connector.quality_metrics["total_requests"] == 1
            assert connector.quality_metrics["successful_requests"] == 1
            assert connector.quality_metrics["failed_requests"] == 0
            assert connector.quality_metrics["data_points_extracted"] == 1
            mock_extract.assert_called_once()

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
                    "node": "HB_NORTH",
                    "lmp": 35.5,
                    "mcc": 2.3,
                    "mlc": -1.0,
                    "data_type": "dam",
                }
            ],
            metadata={"data_type": "dam"},
            record_count=1,
        )
        result = await connector.transform(extraction)
        assert isinstance(result, TransformationResult)
        assert result.record_count == 1
        assert result.data[0]["payload"]["data_type"] == "dam"

    @pytest.mark.asyncio
    async def test_transform_ordc(self, connector):
        extraction = ExtractionResult(
            data=[
                {
                    "timestamp": "2024-01-01T00:00:00Z",
                    "reserve_zone": "R1",
                    "ordc_price": 150.0,
                    "demand_curve_mw": 250.5,
                }
            ],
            metadata={"data_type": "ordc"},
            record_count=1,
        )
        result = await connector.transform(extraction, data_type="ordc")
        assert result.record_count == 1
        assert result.data[0]["payload"]["reserve_zone"] == "R1"

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
