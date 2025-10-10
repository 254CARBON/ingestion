"""
NYISO OASIS/REST extractor implementation.
"""

from __future__ import annotations

import csv
import io
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

import httpx

from ..base.exceptions import ExtractionError
from .config import NYISOConnectorConfig


class NYISOExtractor:
    """Fetch and normalize NYISO datasets."""

    def __init__(self, config: NYISOConnectorConfig):
        self.config = config
        self._clients: Dict[str, httpx.AsyncClient] = {}

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

        if data_type in {"lbmp", "rt_lbmp"}:
            return self._normalize_lbmp_rows(rows, data_type)
        if data_type == "icap":
            return self._normalize_icap_rows(rows)
        if data_type == "tcc":
            return self._normalize_tcc_rows(rows)
        if data_type == "outages":
            return self._normalize_outage_rows(rows)

        raise ExtractionError(f"Unsupported NYISO data_type={data_type}")

    async def close(self) -> None:
        for client in self._clients.values():
            await client.aclose()
        self._clients.clear()

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    async def _fetch_dataset(
        self,
        data_type: str,
        start_time: datetime,
        end_time: datetime,
    ) -> List[Dict[str, Any]]:
        dataset_config = self.config.dataset_configs[data_type]

        client = await self._get_client(dataset_config.get("base", "rest"))
        path = dataset_config.get("path")
        if not path:
            raise ExtractionError(f"NYISO dataset '{data_type}' missing path configuration")

        params: Dict[str, Any] = dict(dataset_config.get("static_params", {}))
        params.setdefault("format", dataset_config.get("format", "json"))

        time_params = dataset_config.get("time_params", {})
        time_format = dataset_config.get("time_format", "%Y%m%d")

        start_param = time_params.get("start")
        end_param = time_params.get("end")

        start_utc = start_time.astimezone(timezone.utc)
        end_utc = end_time.astimezone(timezone.utc)

        if start_param:
            params[start_param] = start_utc.strftime(time_format)
        if end_param:
            params[end_param] = end_utc.strftime(time_format)

        response = await client.get(path, params=params)
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise ExtractionError(
                f"NYISO API request failed for {data_type}: "
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

    async def _get_client(self, base_key: str) -> httpx.AsyncClient:
        if base_key not in self._clients:
            base_url = self._resolve_base_url(base_key)
            headers = {
                "User-Agent": "254Carbon-Ingestion/1.0",
                "Accept": "application/json,text/csv",
            }
            if self.config.api_key:
                headers["X-API-Key"] = self.config.api_key

            auth = None
            if self.config.username and self.config.password:
                auth = (self.config.username, self.config.password)

            self._clients[base_key] = httpx.AsyncClient(
                base_url=base_url.rstrip("/"),
                timeout=httpx.Timeout(self.config.api_timeout),
                headers=headers,
                auth=auth,
            )
        return self._clients[base_key]

    def _resolve_base_url(self, base_key: str) -> str:
        if base_key == "oasis":
            return self.config.oasis_base_url
        return self.config.rest_base_url

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

        raise ExtractionError("Unexpected JSON payload structure returned by NYISO API")

    # ------------------------------------------------------------------
    # Normalisation helpers
    # ------------------------------------------------------------------

    def _normalize_lbmp_rows(self, rows: Iterable[Dict[str, Any]], data_type: str) -> List[Dict[str, Any]]:
        normalized: List[Dict[str, Any]] = []
        for row in rows:
            timestamp = self._first_of(row, ["time_stamp", "timestamp", "interval_start"])
            node = self._first_of(row, ["ptid", "node_id", "name", "location"])
            if not timestamp or not node:
                continue

            normalized.append(
                {
                    "timestamp": timestamp,
                    "node": node,
                    "market_run": "DA" if data_type == "lbmp" else "RT",
                    "lbmp": self._to_float(self._first_of(row, ["lbmp", "price"])),
                    "congestion": self._to_float(
                        self._first_of(row, ["congestion_component", "congestion", "mcc"])
                    ),
                    "losses": self._to_float(
                        self._first_of(row, ["losses_component", "loss", "mlc"])
                    ),
                    "market": "nyiso",
                    "data_type": data_type,
                }
            )
        return normalized

    def _normalize_icap_rows(self, rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
        normalized: List[Dict[str, Any]] = []
        for row in rows:
            zone = self._first_of(row, ["capacity_zone", "zone"])
            month = self._first_of(row, ["month", "period"])
            if not zone or not month:
                continue

            normalized.append(
                {
                    "capacity_zone": zone,
                    "month": month,
                    "demand_curve_price": self._to_float(
                        self._first_of(row, ["icap_demand_curve_price", "price"])
                    ),
                    "capacity_obligations_mw": self._to_float(
                        self._first_of(row, ["capacity_obligations", "obligation_mw"])
                    ),
                    "market": "nyiso",
                    "data_type": "icap",
                }
            )
        return normalized

    def _normalize_tcc_rows(self, rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
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
                    "source": self._first_of(row, ["source_zone", "source"]),
                    "sink": self._first_of(row, ["sink_zone", "sink"]),
                    "clearing_price": self._to_float(
                        self._first_of(row, ["clearing_price", "clearing_price_usd_per_mw"])
                    ),
                    "awarded_mw": self._to_float(
                        self._first_of(row, ["awarded_quantity", "awarded_quantity_mw"])
                    ),
                    "market": "nyiso",
                    "data_type": "tcc",
                }
            )
        return normalized

    def _normalize_outage_rows(self, rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
        normalized: List[Dict[str, Any]] = []
        for row in rows:
            outage_id = self._first_of(row, ["outage_id", "event_id"])
            start_time = self._first_of(row, ["start_time", "startdate", "start_datetime"])
            if not outage_id or not start_time:
                continue

            normalized.append(
                {
                    "outage_id": outage_id,
                    "resource_name": self._first_of(row, ["unit_name", "resource_name", "plant_name"]),
                    "status": self._first_of(row, ["status", "state"]),
                    "start_time": start_time,
                    "end_time": self._first_of(row, ["end_time", "enddate", "end_datetime"]),
                    "derate_mw": self._to_float(
                        self._first_of(row, ["derate_mw", "mw_derated", "mw"])
                    ),
                    "reason": self._first_of(row, ["reason", "comments"]),
                    "market": "nyiso",
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
