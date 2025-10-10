"""
ISO-NE web services extractor implementation.
"""

from __future__ import annotations

import csv
import io
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

import httpx

from ..base.exceptions import ExtractionError
from .config import ISONEConnectorConfig


class ISONEExtractor:
    """Fetch and normalize ISO-NE datasets."""

    def __init__(self, config: ISONEConnectorConfig):
        self.config = config
        self._client: Optional[httpx.AsyncClient] = None

    async def extract_data(
        self,
        *,
        data_type: str,
        start_time: datetime,
        end_time: datetime,
    ) -> List[Dict[str, Any]]:
        data_type = data_type.lower()
        if data_type not in self.config.dataset_configs:
            raise ExtractionError(f"No dataset configuration for data_type={data_type}")

        rows = await self._fetch_dataset(data_type, start_time, end_time)

        if data_type in {"lmp", "rt_lmp"}:
            return self._normalize_lmp_rows(rows, data_type)
        if data_type == "fcm":
            return self._normalize_fcm_rows(rows)
        if data_type == "ftr":
            return self._normalize_ftr_rows(rows)
        if data_type == "outages":
            return self._normalize_outage_rows(rows)

        raise ExtractionError(f"Unsupported ISO-NE data_type={data_type}")

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    async def _ensure_client(self) -> None:
        if self._client is None:
            headers = {
                "User-Agent": "254Carbon-Ingestion/1.0",
                "Accept": "application/json,text/csv",
            }
            auth = None
            if self.config.api_key:
                headers["X-API-Key"] = self.config.api_key
            elif self.config.username and self.config.password:
                auth = (self.config.username, self.config.password)

            self._client = httpx.AsyncClient(
                base_url=self.config.base_url.rstrip("/"),
                timeout=httpx.Timeout(self.config.api_timeout),
                headers=headers,
                auth=auth,
            )

    async def _fetch_dataset(
        self,
        data_type: str,
        start_time: datetime,
        end_time: datetime,
    ) -> List[Dict[str, Any]]:
        await self._ensure_client()
        assert self._client is not None

        dataset_config = self.config.dataset_configs[data_type]
        path = dataset_config.get("path")
        if not path:
            raise ExtractionError(f"ISO-NE dataset '{data_type}' missing path configuration")

        params: Dict[str, Any] = dict(dataset_config.get("static_params", {}))
        params.setdefault("format", dataset_config.get("format", "json"))

        time_params = dataset_config.get("time_params", {})
        time_format = dataset_config.get("time_format", "%Y%m%d")

        start_param = time_params.get("start")
        end_param = time_params.get("end")

        start_local = start_time.astimezone(timezone.utc)
        end_local = end_time.astimezone(timezone.utc)

        if start_param:
            params[start_param] = start_local.strftime(time_format)
        if end_param:
            params[end_param] = end_local.strftime(time_format)

        response = await self._client.get(path, params=params)
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise ExtractionError(
                f"ISO-NE API request failed for {data_type}: "
                f"{exc.response.status_code} {exc.response.text[:200]}"
            ) from exc

        fmt = str(dataset_config.get("format", "json")).lower()
        if fmt == "json":
            payload = response.json()
            return self._resolve_json_payload(payload, dataset_config)
        if fmt == "csv":
            reader = csv.DictReader(io.StringIO(response.text))
            return [row for row in reader]

        raise ExtractionError(f"Unsupported dataset format '{fmt}' for data_type={data_type}")

    @staticmethod
    def _resolve_json_payload(payload: Any, dataset_config: Dict[str, Any]) -> List[Dict[str, Any]]:
        data_key = dataset_config.get("data_key")
        data = payload

        if data_key:
            for part in str(data_key).split("."):
                if isinstance(data, dict):
                    data = data.get(part, [])
                else:
                    data = []
                    break

        if isinstance(data, list):
            return [row for row in data if isinstance(row, dict)]
        if isinstance(data, dict):
            return [data]

        if isinstance(payload, dict):
            for key in ("items", "data", "results", "rows"):
                value = payload.get(key)
                if isinstance(value, list):
                    return [row for row in value if isinstance(row, dict)]

        raise ExtractionError("Unexpected JSON payload structure returned by ISO-NE API")

    # ------------------------------------------------------------------
    # Normalisation helpers
    # ------------------------------------------------------------------

    def _normalize_lmp_rows(self, rows: Iterable[Dict[str, Any]], data_type: str) -> List[Dict[str, Any]]:
        normalized: List[Dict[str, Any]] = []
        for row in rows:
            timestamp = self._first_of(row, ["timestamp", "begin", "interval"])
            node = self._first_of(row, ["node_id", "location", "pnode"])
            if not timestamp or not node:
                continue

            normalized.append(
                {
                    "timestamp": timestamp,
                    "node": node,
                    "market_run": "DA" if data_type == "lmp" else "RT",
                    "lmp": self._to_float(self._first_of(row, ["lmp", "lmp_value"])),
                    "congestion": self._to_float(
                        self._first_of(row, ["congestion_component", "congestion", "mcc"])
                    ),
                    "losses": self._to_float(
                        self._first_of(row, ["losses_component", "loss", "mlc"])
                    ),
                    "market": "isone",
                    "data_type": data_type,
                }
            )
        return normalized

    def _normalize_fcm_rows(self, rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
        normalized: List[Dict[str, Any]] = []
        for row in rows:
            auction = self._first_of(row, ["auction", "auction_name"])
            zone = self._first_of(row, ["capacity_zone", "zone"])
            if not auction or not zone:
                continue

            normalized.append(
                {
                    "auction": auction,
                    "auction_date": self._first_of(row, ["auction_date", "delivery_date"]),
                    "capacity_zone": zone,
                    "clearing_price": self._to_float(
                        self._first_of(row, ["clearing_price", "clearing_price_usd_per_kw_month"])
                    ),
                    "capacity_obligations_mw": self._to_float(
                        self._first_of(row, ["capacity_obligations", "obligation_mw"])
                    ),
                    "auction_round": self._first_of(row, ["auction_round", "round"]),
                    "market": "isone",
                    "data_type": "fcm",
                }
            )
        return normalized

    def _normalize_ftr_rows(self, rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
        normalized: List[Dict[str, Any]] = []
        for row in rows:
            path = self._first_of(row, ["path", "path_name"])
            if not path:
                continue

            normalized.append(
                {
                    "auction": self._first_of(row, ["auction", "auction_name"]),
                    "auction_date": self._first_of(row, ["auction_date", "delivery_date"]),
                    "path": path,
                    "source": self._first_of(row, ["source", "source_name"]),
                    "sink": self._first_of(row, ["sink", "sink_name"]),
                    "clearing_price": self._to_float(
                        self._first_of(row, ["clearing_price", "clearing_price_usd_per_mw"])
                    ),
                    "awarded_mw": self._to_float(
                        self._first_of(row, ["awarded_quantity", "awarded_quantity_mw"])
                    ),
                    "market": "isone",
                    "data_type": "ftr",
                }
            )
        return normalized

    def _normalize_outage_rows(self, rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
        normalized: List[Dict[str, Any]] = []
        for row in rows:
            outage_id = self._first_of(row, ["outage_id", "event_id"])
            start_time = self._first_of(row, ["start_time", "start", "start_datetime"])
            if not outage_id or not start_time:
                continue

            normalized.append(
                {
                    "outage_id": outage_id,
                    "resource_name": self._first_of(row, ["resource_name", "unit", "equipment"]),
                    "status": self._first_of(row, ["status", "state"]),
                    "start_time": start_time,
                    "end_time": self._first_of(row, ["end_time", "end", "end_datetime"]),
                    "derate_mw": self._to_float(
                        self._first_of(row, ["derate_mw", "mw", "capacity_mw"])
                    ),
                    "reason": self._first_of(row, ["reason", "comments"]),
                    "market": "isone",
                    "data_type": "outages",
                }
            )
        return normalized

    @staticmethod
    def _first_of(row: Dict[str, Any], keys: Iterable[str]) -> Optional[Any]:
        for key in keys:
            if key in row and row[key] not in (None, ""):
                return row[key]
        return None

    @staticmethod
    def _to_float(value: Any) -> Optional[float]:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            cleaned = value.strip().replace(",", "")
            if not cleaned:
                return None
            try:
                return float(cleaned)
            except ValueError:
                return None
        return None
