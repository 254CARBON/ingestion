"""
ERCOT data extractor using MIS/public API endpoints.
"""

from __future__ import annotations

import csv
import io
import zipfile
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional

import httpx

from ..base.exceptions import ExtractionError
from .config import ERCOTConnectorConfig


class ERCOTExtractor:
    """Fetch and normalize ERCOT market datasets."""

    def __init__(self, config: ERCOTConnectorConfig):
        self.config = config
        self._client: Optional[httpx.AsyncClient] = None

    async def extract_data(
        self,
        *,
        data_type: str,
        start_time: datetime,
        end_time: datetime,
    ) -> List[Dict[str, Any]]:
        """Extract and normalize ERCOT data for the given data type."""
        if data_type not in self.config.dataset_configs:
            raise ExtractionError(f"No dataset configuration for data_type={data_type}")

        rows = await self._fetch_dataset(data_type, start_time, end_time)

        if data_type == "dam":
            return self._normalize_lmp_rows(rows, data_type)
        if data_type == "rtm":
            return self._normalize_lmp_rows(rows, data_type)
        if data_type == "ordc":
            return self._normalize_ordc_rows(rows)
        if data_type == "as":
            return self._normalize_as_rows(rows)
        if data_type == "crr":
            return self._normalize_crr_rows(rows)
        if data_type == "outages":
            return self._normalize_outage_rows(rows)

        raise ExtractionError(f"Unsupported data type: {data_type}")

    async def close(self) -> None:
        """Release underlying HTTP resources."""
        if self._client:
            await self._client.aclose()
            self._client = None

    async def _fetch_dataset(
        self,
        data_type: str,
        start_time: datetime,
        end_time: datetime,
    ) -> List[Dict[str, Any]]:
        dataset_config = self.config.dataset_configs[data_type]
        path = dataset_config.get("path")
        if not path:
            raise ExtractionError(f"No path configured for data_type={data_type}")

        fmt = dataset_config.get("format", "json").lower()
        params = {
            "startTime": start_time.strftime("%Y-%m-%dT%H:%M:%S"),
            "endTime": end_time.strftime("%Y-%m-%dT%H:%M:%S"),
            "docType": dataset_config.get("docType"),
            "format": "json" if fmt == "json" else "csv",
            "limit": self.config.max_records,
        }
        # Remove None params
        params = {k: v for k, v in params.items() if v is not None}

        await self._ensure_client()
        assert self._client is not None

        response = await self._client.get(path, params=params)
        response.raise_for_status()

        if fmt == "json":
            return self._parse_json_payload(response)
        if fmt in ("csv", "zip-csv"):
            content = await response.aread()
            if fmt == "zip-csv" or response.headers.get("Content-Type", "").startswith("application/zip"):
                csv_bytes = self._unzip_single_file(content)
                text = csv_bytes.decode("utf-8", errors="replace")
            else:
                text = content.decode("utf-8", errors="replace")
            return list(csv.DictReader(io.StringIO(text)))

        raise ExtractionError(f"Unsupported dataset format {fmt} for {data_type}")

    async def _ensure_client(self) -> None:
        if self._client is None:
            auth = None
            if self.config.api_key and self.config.api_secret:
                auth = (self.config.api_key, self.config.api_secret)

            self._client = httpx.AsyncClient(
                base_url=self.config.base_url.rstrip("/"),
                timeout=httpx.Timeout(30.0),
                auth=auth,
                headers={"User-Agent": "254Carbon-Ingestion/1.0"},
            )

    @staticmethod
    def _parse_json_payload(response: httpx.Response) -> List[Dict[str, Any]]:
        payload = response.json()
        if isinstance(payload, list):
            return payload
        if isinstance(payload, dict):
            for key in ("data", "rows", "results", "Dataset"):
                if key in payload and isinstance(payload[key], list):
                    return payload[key]
            return [payload]
        raise ExtractionError("Unexpected JSON payload structure from ERCOT")

    @staticmethod
    def _unzip_single_file(zip_bytes: bytes) -> bytes:
        with io.BytesIO(zip_bytes) as buffer:
            with zipfile.ZipFile(buffer) as archive:
                names = archive.namelist()
                if not names:
                    raise ExtractionError("Zip archive from ERCOT contained no files")
                with archive.open(names[0]) as zipped_file:
                    return zipped_file.read()

    # ---------------------------
    # Normalization helpers
    # ---------------------------

    def _normalize_lmp_rows(self, rows: Iterable[Dict[str, Any]], data_type: str) -> List[Dict[str, Any]]:
        normalized: List[Dict[str, Any]] = []
        for row in rows:
            timestamp = self._first_of(row, ["timestamp", "delivery_timestamp", "SettlementPointTime"])
            node = self._first_of(row, ["node", "SettlementPoint", "location"])
            if not timestamp or not node:
                continue

            normalized.append(
                {
                    "timestamp": timestamp,
                    "node": node,
                    "lmp": self._to_float(row, ["lmp", "LMP", "settlement_point_price"]),
                    "mcc": self._to_float(row, ["mcc", "MCC"]),
                    "mlc": self._to_float(row, ["mlc", "MLC"]),
                    "data_type": data_type,
                }
            )
        return normalized

    def _normalize_ordc_rows(self, rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
        normalized: List[Dict[str, Any]] = []
        for row in rows:
            timestamp = self._first_of(row, ["timestamp", "DeliveryInterval", "interval_datetime"])
            zone = self._first_of(row, ["reserve_zone", "ReserveZone", "Zone"])
            if not timestamp or not zone:
                continue
            normalized.append(
                {
                    "timestamp": timestamp,
                    "reserve_zone": zone,
                    "ordc_price": self._to_float(row, ["ordc_price", "Price", "price"]),
                    "demand_curve_mw": self._to_float(row, ["demand_curve_mw", "Quantity", "mw"]),
                }
            )
        return normalized

    def _normalize_as_rows(self, rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
        normalized: List[Dict[str, Any]] = []
        for row in rows:
            timestamp = self._first_of(row, ["timestamp", "DeliveryInterval", "interval_datetime"])
            product = self._first_of(row, ["as_type", "ProductType", "product"])
            zone = self._first_of(row, ["zone", "ASRegion", "region"])
            if not timestamp or not product:
                continue
            normalized.append(
                {
                    "timestamp": timestamp,
                    "as_type": product,
                    "zone": zone,
                    "cleared_price": self._to_float(row, ["cleared_price", "ClearedPrice", "price"]),
                    "cleared_quantity": self._to_float(row, ["cleared_quantity", "ClearedMW", "quantity"]),
                }
            )
        return normalized

    def _normalize_crr_rows(self, rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
        normalized: List[Dict[str, Any]] = []
        for row in rows:
            auction_date = self._first_of(row, ["auction_date", "AuctionDate", "trade_date"])
            path = self._first_of(row, ["path", "PathName"])
            if not auction_date or not path:
                continue
            normalized.append(
                {
                    "auction_date": auction_date,
                    "path": path,
                    "source": self._first_of(row, ["source", "SourcePoint"]),
                    "sink": self._first_of(row, ["sink", "SinkPoint"]),
                    "auction_type": self._first_of(row, ["auction_type", "AuctionType"]),
                    "clearing_price": self._to_float(row, ["clearing_price", "ClearedPrice", "price"]),
                    "awarded_quantity": self._to_float(row, ["awarded_quantity", "AwardMW", "quantity"]),
                }
            )
        return normalized

    def _normalize_outage_rows(self, rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
        normalized: List[Dict[str, Any]] = []
        for row in rows:
            outage_id = self._first_of(row, ["outage_id", "OutageId", "OutageID"])
            if not outage_id:
                continue
            normalized.append(
                {
                    "outage_id": outage_id,
                    "unit_name": self._first_of(row, ["unit_name", "UnitName", "ResourceName"]),
                    "outage_type": self._first_of(row, ["outage_type", "OutageType"]),
                    "start_time": self._first_of(row, ["start_time", "StartTime"]),
                    "end_time": self._first_of(row, ["end_time", "EndTime"]),
                    "capacity_mw": self._to_float(row, ["capacity_mw", "MaxCapacity", "mw"]),
                    "reason": self._first_of(row, ["reason", "ReasonCode"]),
                }
            )
        return normalized

    @staticmethod
    def _first_of(row: Dict[str, Any], keys: Iterable[str]) -> Optional[Any]:
        for key in keys:
            if key in row and row[key] not in (None, "", "null"):
                return row[key]
        return None

    @staticmethod
    def _to_float(row: Dict[str, Any], keys: Iterable[str]) -> Optional[float]:
        for key in keys:
            if key in row and row[key] not in (None, "", "null"):
                try:
                    return float(row[key])
                except (TypeError, ValueError):
                    continue
        return None
