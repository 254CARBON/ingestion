"""
Unit tests for the AESO connector.
"""

from datetime import datetime, timezone
from typing import Any, Dict, List
from unittest.mock import AsyncMock, patch

import pytest

from connectors.aeso.config import AESOConnectorConfig
from connectors.aeso.connector import AESOConnector
from connectors.aeso.extractor import AESOExtractor
from connectors.aeso.transform import AESOTransform, AESOTransformConfig
from connectors.base import ExtractionResult


class DummyResponse:
    """Minimal HTTPX-like response used for extractor tests."""

    def __init__(self, payload: Dict[str, Any]):
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> Dict[str, Any]:
        return self._payload

    @property
    def text(self) -> str:
        return str(self._payload)


class DummyClient:
    """Minimal async client that records requests."""

    def __init__(self, response: DummyResponse):
        self.response = response
        self.calls: List[Dict[str, Any]] = []

    async def get(self, path: str, params: Dict[str, Any] | None = None):
        self.calls.append({"path": path, "params": params or {}})
        return self.response

    async def aclose(self) -> None:
        return None


@pytest.fixture
def aeso_config(sample_connector_config) -> AESOConnectorConfig:
    return AESOConnectorConfig(
        **sample_connector_config,
        aeso_api_key="test-key",
        aeso_base_url="https://api.example.com",
    )


class TestAESOExtractor:
    @pytest.mark.asyncio
    async def test_extract_pool_price_params(self, aeso_config):
        payload = {
            "return": {
                "Pool Price Report": [
                    {
                        "begin_datetime_utc": "2024-01-01 00:00",
                        "begin_datetime_mpt": "2023-12-31 17:00",
                        "pool_price": "75.12",
                        "forecast_pool_price": "80.0",
                        "rolling_30day_avg": "70.0",
                    }
                ]
            }
        }
        extractor = AESOExtractor(aeso_config)
        response = DummyResponse(payload)
        client = DummyClient(response)
        with patch.object(extractor, "_ensure_client", AsyncMock()):
            extractor._client = client
            start = datetime(2024, 1, 1, tzinfo=timezone.utc)
            end = datetime(2024, 1, 2, tzinfo=timezone.utc)
            rows = await extractor.extract_data(
                data_type="pool_price",
                start_time=start,
                end_time=end,
                time_granularity="hourly",
            )

        assert rows == payload["return"]["Pool Price Report"]
        assert client.calls, "Expected extractor to call HTTP client"
        call = client.calls[0]
        assert call["path"] == "/v1.1/price/poolPrice"
        assert call["params"]["startDate"] == "2024-01-01 00"
        assert call["params"]["endDate"] == "2024-01-02 00"

    @pytest.mark.asyncio
    async def test_extract_assets_array_params(self, aeso_config):
        payload = {"return": {"asset_list": []}}
        extractor = AESOExtractor(aeso_config)
        response = DummyResponse(payload)
        client = DummyClient(response)
        with patch.object(extractor, "_ensure_client", AsyncMock()):
            extractor._client = client
            rows = await extractor.extract_data(
                data_type="current_supply_demand_assets",
                asset_ids=["AB1", "AB2"],
            )

        assert rows == [payload["return"]]
        call = client.calls[0]
        assert call["path"] == "/v1/csd/generation/assets/current"
        assert call["params"]["assetIds"] == ["AB1", "AB2"]


class TestAESOTransform:
    def test_transform_pool_price_event(self):
        transform = AESOTransform(
            AESOTransformConfig(
                tenant_id="tenant-1",
                producer="ingestion-aeso@test",
                schema_version="1.2.3",
            )
        )
        rows = [
            {
                "begin_datetime_utc": "2024-01-01 00:00",
                "begin_datetime_mpt": "2023-12-31 17:00",
                "pool_price": "55.25",
                "forecast_pool_price": "60.10",
                "rolling_30day_avg": "50.00",
            }
        ]
        events = transform.transform(rows, "pool_price")
        assert len(events) == 1
        event = events[0]
        assert event["producer"] == "ingestion-aeso@test"
        assert event["tenant_id"] == "tenant-1"
        payload = event["payload"]
        assert payload["market"] == "aeso"
        assert payload["data_type"] == "pool_price"
        assert payload["pool_price_cad_per_mwh"] == 55.25
        assert payload["forecast_pool_price_cad_per_mwh"] == 60.10


class TestAESOConnector:
    @pytest.mark.asyncio
    async def test_extract_invokes_extractor(self, aeso_config):
        connector = AESOConnector(aeso_config)
        start = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end = datetime(2024, 1, 2, tzinfo=timezone.utc)
        with patch.object(connector._extractor, "extract_data", AsyncMock(return_value=[{"foo": "bar"}])) as mock_extract:
            result = await connector.extract(
                data_type="pool_price",
                start_time=start,
                end_time=end,
            )

        assert result.record_count == 1
        assert result.metadata["data_type"] == "pool_price"
        mock_extract.assert_awaited_once()
        kwargs = mock_extract.await_args.kwargs
        assert kwargs["data_type"] == "pool_price"
        assert kwargs["start_time"] == start
        assert kwargs["end_time"] == end

    @pytest.mark.asyncio
    async def test_transform_produces_events(self, aeso_config):
        connector = AESOConnector(aeso_config)
        extraction = ExtractionResult(
            data=[
                {
                    "begin_datetime_utc": "2024-01-01 00:00",
                    "pool_price": "40.5",
                    "forecast_pool_price": "44.1",
                    "rolling_30day_avg": "42.0",
                }
            ],
            metadata={"data_type": "pool_price"},
            record_count=1,
        )
        result = await connector.transform(extraction)
        assert result.record_count == 1
        payload = result.data[0]["payload"]
        assert payload["data_type"] == "pool_price"
        assert payload["pool_price_cad_per_mwh"] == 40.5
