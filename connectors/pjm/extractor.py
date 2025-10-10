"""
PJM Data Miner/API extractor implementation.
"""

from __future__ import annotations

import csv
import io
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

import httpx

from ..base.exceptions import ExtractionError
from .config import PJMConnectorConfig


class PJMExtractor:
    """Fetch and normalize datasets from the PJM Data Miner and public APIs."""

    def __init__(self, config: PJMConnectorConfig):
        self.config = config
        self._clients: Dict[str, httpx.AsyncClient] = {}

    async def extract_data(
        self,
        *,
        data_type: str,
        start_time: datetime,
        end_time: datetime,
    ) -> List[Dict[str, Any]]:
        """Extract and normalize records for the requested dataset."""
        data_type = data_type.lower()
        if data_type not in self.config.dataset_configs:
            raise ExtractionError(f"No dataset configuration for data_type={data_type}")

        rows = await self._fetch_dataset(data_type, start_time, end_time)

        if data_type in {"lmp", "rtm_lmp"}:
            return self._normalize_lmp_rows(rows, data_type)
        if data_type == "rpm":
            return self._normalize_rpm_rows(rows)
        if data_type == "tcr":
            return self._normalize_tcr_rows(rows)
        if data_type == "outages":
            return self._normalize_outage_rows(rows)

        raise ExtractionError(f"Unsupported PJM data_type={data_type}")

    async def close(self) -> None:
        """Release HTTP resources."""
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
        client = await self._get_client(dataset_config.get("base", "api"))

        path = dataset_config.get("path")
        if not path:
            raise ExtractionError(f"PJM dataset '{data_type}' missing path configuration")

        params: Dict[str, Any] = dict(dataset_config.get("static_params", {}))
        params.setdefault("format", dataset_config.get("format", "json"))

        time_params = dataset_config.get("time_params", {})
        time_format = dataset_config.get("time_format", "%Y-%m-%dT%H:%M:%S")

        start_param = time_params.get("start")
        end_param = time_params.get("end")

        start_ept = start_time.astimezone(timezone.utc)
        end_ept = end_time.astimezone(timezone.utc)

        if start_param:
            params[start_param] = start_ept.strftime(time_format)
        if end_param:
            params[end_param] = end_ept.strftime(time_format)

        response = await client.get(path, params=params)
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise ExtractionError(
                f"PJM API request failed for {data_type}: "
                f"{exc.response.status_code} {exc.response.text[:200]}"
            ) from exc

        fmt = str(dataset_config.get("format", "json")).lower()
        if fmt == "json":
            payload = response.json()
            return self._resolve_json_payload(payload, dataset_config)
        if fmt == "csv":
            text = response.text
            reader = csv.DictReader(io.StringIO(text))
            return [row for row in reader]

        raise ExtractionError(f"Unsupported dataset format '{fmt}' for data_type={data_type}")

    async def _get_client(self, base_key: str) -> httpx.AsyncClient:
        """Build or reuse an AsyncClient for the requested base."""
        if base_key not in self._clients:
            base_url = self._resolve_base_url(base_key)
            headers = {
                "User-Agent": "254Carbon-Ingestion/1.0",
                "Accept": "application/json",
            }
            if self.config.api_key:
                headers["Ocp-Apim-Subscription-Key"] = self.config.api_key

            timeout = httpx.Timeout(self.config.api_timeout)
            self._clients[base_key] = httpx.AsyncClient(
                base_url=base_url.rstrip("/"),
                timeout=timeout,
                headers=headers,
            )

        return self._clients[base_key]

    def _resolve_base_url(self, base_key: str) -> str:
        if base_key == "data_miner":
            return self.config.data_miner_base_url
        return self.config.api_base_url

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

        raise ExtractionError("Unexpected JSON payload structure returned by PJM API")

    # ------------------------------------------------------------------
    # Normalisation helpers
    # ------------------------------------------------------------------

    def _normalize_lmp_rows(self, rows: Iterable[Dict[str, Any]], data_type: str) -> List[Dict[str, Any]]:
        normalized: List[Dict[str, Any]] = []
        for row in rows:
            timestamp = self._first_of(
                row,
                [
                    "datetime_beginning_ept",
                    "datetime_beginning_utc",
                    "effective_datetime",
                    "timestamp",
                ],
            )
            node = self._first_of(row, ["pnode_name", "pnode", "node", "location"])
            if not timestamp or not node:
                continue

            normalized.append(
                {
                    "timestamp": timestamp,
                    "node": node,
                    "market_run": "DA" if data_type == "lmp" else "RT",
                    "lmp": self._to_float(
                        self._first_of(
                            row,
                            [
                                "total_lmp_da",
                                "total_lmp_rt",
                                "lmp",
                                "lmp_value",
                                "value",
                            ],
                        )
                    ),
                    "congestion": self._to_float(
                        self._first_of(
                            row,
                            [
                                "congestion_price_da",
                                "congestion_price_rt",
                                "congestion_component",
                                "congestion",
                            ],
                        )
                    ),
                    "loss": self._to_float(
                        self._first_of(
                            row,
                            [
                                "marginal_loss_price_da",
                                "marginal_loss_price_rt",
                                "loss_component",
                                "loss",
                            ],
                        )
                    ),
                    "market": "pjm",
                    "data_type": data_type,
                }
            )
        return normalized

    def _normalize_rpm_rows(self, rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
        normalized: List[Dict[str, Any]] = []
        for row in rows:
            auction = self._first_of(row, ["auction", "auction_name"])
            lda = self._first_of(row, ["lda", "locational_delivery_area"])
            if not auction or not lda:
                continue

            normalized.append(
                {
                    "auction": auction,
                    "auction_date": self._first_of(row, ["auction_date", "delivery_date"]),
                    "lda": lda,
                    "clearing_price": self._to_float(
                        self._first_of(row, ["clearing_price", "clearing_price_usd_per_mw_day"])
                    ),
                    "capacity_obligations_mw": self._to_float(
                        self._first_of(row, ["capacity_obligations", "awarded_mw"])
                    ),
                    "auction_type": self._first_of(row, ["auction_type", "product_type"]),
                    "market": "pjm",
                    "data_type": "rpm",
                }
            )
        return normalized

    def _normalize_tcr_rows(self, rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
        normalized: List[Dict[str, Any]] = []
        for row in rows:
            path = self._first_of(row, ["path_name", "path"])
            if not path:
                continue

            normalized.append(
                {
                    "auction": self._first_of(row, ["auction", "auction_name"]),
                    "auction_date": self._first_of(row, ["auction_date", "delivery_date"]),
                    "path": path,
                    "source": self._first_of(row, ["source", "source_name"]),
                    "sink": self._first_of(row, ["sink", "sink_name"]),
                    "round_id": self._first_of(row, ["round_id", "round"]),
                    "clearing_price": self._to_float(
                        self._first_of(row, ["clearing_price", "clearing_price_usd_per_mw"])
                    ),
                    "awarded_mw": self._to_float(
                        self._first_of(row, ["awarded_quantity", "awarded_quantity_mw"])
                    ),
                    "market": "pjm",
                    "data_type": "tcr",
                }
            )
        return normalized

    def _normalize_outage_rows(self, rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
        normalized: List[Dict[str, Any]] = []
        for row in rows:
            outage_id = self._first_of(row, ["outage_id", "event_id", "equipment_id"])
            start_time = self._first_of(
                row,
                [
                    "start_datetime",
                    "outage_begin_time",
                    "start_time",
                    "effective_start",
                ],
            )
            if not outage_id or not start_time:
                continue

            normalized.append(
                {
                    "outage_id": outage_id,
                    "resource_name": self._first_of(row, ["unit_name", "resource_name", "equipment_name"]),
                    "outage_class": self._first_of(row, ["outage_type", "outage_class"]),
                    "status": self._first_of(row, ["status", "state"]),
                    "start_time": start_time,
                    "end_time": self._first_of(
                        row,
                        [
                            "end_datetime",
                            "outage_end_time",
                            "end_time",
                            "effective_end",
                        ],
                    ),
                    "capacity_mw": self._to_float(
                        self._first_of(row, ["outage_mw", "mw", "capacity_mw"])
                    ),
                    "reason": self._first_of(row, ["outage_reason", "reason"]),
                    "market": "pjm",
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
