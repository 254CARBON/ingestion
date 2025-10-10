"""
AESO data extractor implementation.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence

import httpx

from ..base.exceptions import ExtractionError
from .config import AESOConnectorConfig


class AESOExtractor:
    """Fetch datasets from the AESO public reporting APIs."""

    def __init__(self, config: AESOConnectorConfig):
        self.config = config
        self._client: Optional[httpx.AsyncClient] = None

    async def extract_data(
        self,
        *,
        data_type: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        time_granularity: Optional[str] = None,
        asset_ids: Optional[Sequence[str]] = None,
        extra_params: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """Execute the HTTP request for the configured dataset."""
        dataset_config = self.config.dataset_configs.get(data_type)
        if not dataset_config:
            raise ExtractionError(f"No dataset configuration for data_type={data_type}")

        await self._ensure_client()
        assert self._client is not None

        path = dataset_config.get("path")
        if not path:
            raise ExtractionError(f"No path configured for data_type={data_type}")

        fmt = str(dataset_config.get("format", "json")).lower()
        params: Dict[str, Any] = dict(dataset_config.get("static_params", {}))

        # Time window parameters
        time_params = dataset_config.get("time_params") or {}
        if time_params:
            time_format = dataset_config.get("time_format") or "%Y-%m-%d"
            if time_granularity:
                normalized = time_granularity.lower()
                if normalized in {"hour", "hourly"}:
                    time_format = dataset_config.get("hourly_time_format") or time_format
                elif normalized in {"minute", "minutely"}:
                    time_format = dataset_config.get("minutely_time_format") or time_format

            start_param = time_params.get("start")
            end_param = time_params.get("end")

            if start_param:
                if start_time:
                    params[start_param] = self._format_time(start_time, time_format)
                else:
                    raise ExtractionError(f"start_time is required for data_type={data_type}")
            if end_param:
                if end_time:
                    params[end_param] = self._format_time(end_time, time_format)
                else:
                    raise ExtractionError(f"end_time is required for data_type={data_type}")

        # Handle array-style parameters (e.g., multiple asset IDs)
        array_params = dataset_config.get("array_params") or {}
        if array_params:
            for key, param_name in array_params.items():
                values: Optional[Sequence[str]]
                if key == "asset_ids":
                    values = asset_ids
                else:
                    raw = (extra_params or {}).get(key) if extra_params else None
                    values = raw  # type: ignore[assignment]
                if not values:
                    continue
                filtered = [str(value) for value in values if value]
                if filtered:
                    params[param_name] = filtered

        if extra_params:
            for key, value in extra_params.items():
                if value is None:
                    continue
                if key in array_params:
                    # Already processed above
                    continue
                params[key] = value

        response = await self._client.get(path, params=params)
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            body = exc.response.text
            raise ExtractionError(
                f"AESO API request failed for {data_type}: "
                f"{exc.response.status_code} {body[:200]}"
            ) from exc

        if fmt != "json":
            raise ExtractionError(f"Unsupported dataset format '{fmt}' for AESO")

        payload = response.json()
        rows = self._resolve_json_payload(payload, dataset_config)
        return rows

    async def close(self) -> None:
        """Release HTTP resources."""
        if self._client:
            await self._client.aclose()
            self._client = None

    async def _ensure_client(self) -> None:
        if self._client is None:
            headers = {
                "User-Agent": "254Carbon-AESOConnector/1.0",
                "Accept": "application/json",
                "X-API-Key": self.config.aeso_api_key,
            }
            timeout = httpx.Timeout(self.config.aeso_timeout)
            base_url = self.config.aeso_base_url.rstrip("/")
            self._client = httpx.AsyncClient(
                base_url=base_url,
                timeout=timeout,
                headers=headers,
            )

    @staticmethod
    def _format_time(value: datetime, fmt: str) -> str:
        """Format datetimes using the API expectations."""
        aware = value.astimezone(timezone.utc)
        return aware.strftime(fmt)

    @staticmethod
    def _resolve_json_payload(payload: Any, dataset_config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Resolve the JSON payload into a list of dictionaries."""
        data_key = dataset_config.get("data_key")
        data = payload

        if data_key:
            for part in data_key.split("."):
                if isinstance(data, dict):
                    data = data.get(part, [])
                else:
                    data = []
                    break

        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)]
        if isinstance(data, dict):
            return [data]
        if isinstance(payload, dict):
            for key in ("items", "data", "results", "rows"):
                value = payload.get(key)
                if isinstance(value, list):
                    return [item for item in value if isinstance(item, dict)]
        raise ExtractionError("Unexpected JSON payload structure returned by AESO API")
