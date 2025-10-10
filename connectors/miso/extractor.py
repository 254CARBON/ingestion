"""
MISO data extractor implementation.
"""

from __future__ import annotations

import csv
import io
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

import httpx

from ..base.exceptions import ExtractionError
from .config import MISOConnectorConfig


class MISOExtractor:
    """Fetch and normalize datasets from the MISO Data Exchange APIs."""

    def __init__(self, config: MISOConnectorConfig):
        self.config = config
        self._client: Optional[httpx.AsyncClient] = None

    async def extract_data(
        self,
        *,
        data_type: str,
        start_time: datetime,
        end_time: datetime,
    ) -> List[Dict[str, Any]]:
        """Extract a dataset for the requested data_type."""
        if data_type not in self.config.dataset_configs:
            raise ExtractionError(f"No dataset configuration for data_type={data_type}")

        rows = await self._fetch_dataset(data_type, start_time, end_time)

        if data_type == "lmp":
            return self._normalize_lmp_rows(rows)
        if data_type == "as":
            return self._normalize_as_rows(rows)
        if data_type == "pra":
            return self._normalize_pra_rows(rows)
        if data_type == "arr":
            return self._normalize_arr_rows(rows)
        if data_type == "tcr":
            return self._normalize_tcr_rows(rows)

        raise ExtractionError(f"Unsupported data_type={data_type}")

    async def close(self) -> None:
        """Release HTTP resources."""
        if self._client:
            await self._client.aclose()
            self._client = None

    async def _ensure_client(self) -> None:
        if self._client is None:
            headers = {
                "User-Agent": "254Carbon-Ingestion/1.0",
                "Accept": "application/json",
            }
            if self.config.miso_api_key:
                headers["Ocp-Apim-Subscription-Key"] = self.config.miso_api_key

            timeout = httpx.Timeout(self.config.miso_timeout)
            self._client = httpx.AsyncClient(
                base_url=self.config.miso_base_url.rstrip("/"),
                timeout=timeout,
                headers=headers,
            )

    async def _fetch_dataset(
        self,
        data_type: str,
        start_time: datetime,
        end_time: datetime,
    ) -> List[Dict[str, Any]]:
        """Execute the HTTP request for the given dataset."""
        await self._ensure_client()
        assert self._client is not None

        dataset_config = self.config.dataset_configs[data_type]
        path = dataset_config.get("path")
        if not path:
            raise ExtractionError(f"No path configured for data_type={data_type}")

        fmt = str(dataset_config.get("format", "json")).lower()
        params: Dict[str, Any] = dict(dataset_config.get("static_params", {}))

        time_params = dataset_config.get("time_params", {})
        time_format = dataset_config.get("time_format", "%Y-%m-%dT%H:%M:%SZ")

        start_param = time_params.get("start")
        end_param = time_params.get("end")

        # Normalize supplied times to UTC before formatting.
        start_utc = start_time.astimezone(timezone.utc)
        end_utc = end_time.astimezone(timezone.utc)

        if start_param:
            params[start_param] = start_utc.strftime(time_format)
        if end_param:
            params[end_param] = end_utc.strftime(time_format)

        response = await self._client.get(path, params=params)

        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise ExtractionError(
                f"MISO API request failed for {data_type}: "
                f"{exc.response.status_code} {exc.response.text[:200]}"
            ) from exc

        if fmt == "json":
            payload = response.json()
            return self._resolve_json_payload(payload, dataset_config)

        if fmt == "csv":
            text = response.text
            reader = csv.DictReader(io.StringIO(text))
            return list(reader)

        raise ExtractionError(f"Unsupported dataset format '{fmt}' for {data_type}")

    @staticmethod
    def _resolve_json_payload(payload: Any, dataset_config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Resolve the JSON payload into a list of dictionaries."""
        data_key = dataset_config.get("data_key")
        data = payload

        if data_key:
            parts = data_key.split(".")
            for part in parts:
                if isinstance(data, dict):
                    data = data.get(part, [])
                else:
                    data = []
                    break

        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)]

        if isinstance(data, dict):
            # Some datasets return a single object.
            return [data]

        # Fallback heuristics for common wrapper keys
        if isinstance(payload, dict):
            for key in ("items", "data", "results", "rows"):
                value = payload.get(key)
                if isinstance(value, list):
                    return [item for item in value if isinstance(item, dict)]

        raise ExtractionError("Unexpected JSON payload structure returned by MISO API")

    # -------------------------------------------------------------------------
    # Normalization helpers
    # -------------------------------------------------------------------------

    def _normalize_lmp_rows(self, rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
        normalized: List[Dict[str, Any]] = []
        for row in rows:
            timestamp = self._first_of(row, ["timestamp", "startTime", "intervalStart", "mtu", "mtuStart"])
            location = self._first_of(
                row,
                ["settlementLocation", "pnodeName", "node", "location", "settlement_point"],
            )
            if not timestamp or not location:
                continue

            normalized.append(
                {
                    "timestamp": timestamp,
                    "node": location,
                    "market_run": self._first_of(row, ["marketRunId", "marketRun", "market_run"]),
                    "lmp": self._to_float(self._first_of(row, ["lmp", "lmpValue", "totalLmp", "price"])),
                    "congestion": self._to_float(
                        self._first_of(row, ["congestion", "congestionComponent", "congestionPrice", "mcc"])
                    ),
                    "loss": self._to_float(
                        self._first_of(row, ["loss", "lossComponent", "marginalLossPrice", "mlc"])
                    ),
                    "energy": self._to_float(
                        self._first_of(row, ["energy", "energyComponent", "energyPrice", "mep"])
                    ),
                    "market": "miso",
                    "data_type": "lmp",
                }
            )
        return normalized

    def _normalize_as_rows(self, rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
        normalized: List[Dict[str, Any]] = []
        for row in rows:
            timestamp = self._first_of(row, ["timestamp", "startTime", "intervalStart"])
            product = self._first_of(row, ["product", "productType", "asType", "ancillaryType"])
            if not timestamp or not product:
                continue

            normalized.append(
                {
                    "timestamp": timestamp,
                    "product": product,
                    "zone": self._first_of(row, ["zone", "reserveZone", "marketZone", "asRegion"]),
                    "cleared_price": self._to_float(
                        self._first_of(row, ["clearedPrice", "price", "clearingPrice"])
                    ),
                    "cleared_mw": self._to_float(
                        self._first_of(row, ["clearedMw", "quantity", "awardedMw", "mw"])
                    ),
                    "market": "miso",
                    "data_type": "as",
                }
            )
        return normalized

    def _normalize_pra_rows(self, rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
        normalized: List[Dict[str, Any]] = []
        for row in rows:
            auction = self._first_of(row, ["auction", "auctionName", "auctionId"])
            zone = self._first_of(row, ["planningZone", "zone", "praZone"])
            if not auction or not zone:
                continue

            normalized.append(
                {
                    "auction": auction,
                    "auction_date": self._first_of(row, ["auctionDate", "auction_date", "startDate"]),
                    "planning_zone": zone,
                    "planning_year": self._first_of(row, ["planningYear", "season", "planningYearLabel"]),
                    "clearing_price": self._to_float(
                        self._first_of(row, ["clearingPrice", "price", "clearedPrice"])
                    ),
                    "awarded_mw": self._to_float(
                        self._first_of(row, ["awardedMw", "mwAwarded", "awardedQuantity"])
                    ),
                    "market": "miso",
                    "data_type": "pra",
                }
            )
        return normalized

    def _normalize_arr_rows(self, rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
        normalized: List[Dict[str, Any]] = []
        for row in rows:
            auction = self._first_of(row, ["auction", "auctionName", "auctionId"])
            path = self._first_of(row, ["path", "pathName", "pathId"])
            if not auction or not path:
                continue

            normalized.append(
                {
                    "auction": auction,
                    "auction_date": self._first_of(row, ["auctionDate", "auction_date", "startDate"]),
                    "path": path,
                    "source": self._first_of(row, ["source", "sourceNode", "fromNode"]),
                    "sink": self._first_of(row, ["sink", "sinkNode", "toNode"]),
                    "class_type": self._first_of(row, ["class", "classType", "productClass"]),
                    "clearing_price": self._to_float(
                        self._first_of(row, ["clearingPrice", "price", "clearedPrice"])
                    ),
                    "awarded_mw": self._to_float(
                        self._first_of(row, ["awardedMw", "mwAwarded", "awardedQuantity"])
                    ),
                    "market": "miso",
                    "data_type": "arr",
                }
            )
        return normalized

    def _normalize_tcr_rows(self, rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
        normalized: List[Dict[str, Any]] = []
        for row in rows:
            auction = self._first_of(row, ["auction", "auctionName", "auctionId"])
            path = self._first_of(row, ["path", "pathName", "pathId"])
            if not auction or not path:
                continue

            normalized.append(
                {
                    "auction": auction,
                    "auction_date": self._first_of(row, ["auctionDate", "auction_date", "startDate"]),
                    "path": path,
                    "source": self._first_of(row, ["source", "sourceNode", "fromNode"]),
                    "sink": self._first_of(row, ["sink", "sinkNode", "toNode"]),
                    "class_type": self._first_of(row, ["class", "classType", "productClass"]),
                    "round_id": self._first_of(row, ["round", "roundId", "auctionRound"]),
                    "clearing_price": self._to_float(
                        self._first_of(row, ["clearingPrice", "price", "clearedPrice"])
                    ),
                    "awarded_mw": self._to_float(
                        self._first_of(row, ["awardedMw", "mwAwarded", "awardedQuantity"])
                    ),
                    "market": "miso",
                    "data_type": "tcr",
                }
            )
        return normalized

    @staticmethod
    def _first_of(row: Dict[str, Any], keys: Iterable[str]) -> Optional[Any]:
        """Return the first non-null value for any of the provided keys."""
        lowered_map = {str(k).lower(): k for k in row.keys()}
        for key in keys:
            # Direct match
            if key in row and row[key] not in (None, ""):
                return row[key]
            key_lower = key.lower()
            # Case-insensitive match
            matched_key = lowered_map.get(key_lower)
            if matched_key and row[matched_key] not in (None, ""):
                return row[matched_key]
        return None

    @staticmethod
    def _to_float(value: Any) -> Optional[float]:
        """Best-effort conversion to float."""
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            cleaned = value.strip()
            if not cleaned or cleaned.lower() in {"na", "n/a", "null"}:
                return None
            cleaned = cleaned.replace(",", "")
            try:
                return float(cleaned)
            except ValueError:
                return None
        return None
