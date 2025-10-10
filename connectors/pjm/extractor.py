"""
PJM data extractor using Data Miner API.
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import httpx

from ..base.exceptions import ExtractionError
from .config import PJMConnectorConfig


class PJMExtractor:
    """PJM Data Miner API data extractor."""

    def __init__(self, config: PJMConnectorConfig):
        self.config = config
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(60.0),
            limits=httpx.Limits(max_keepalive_connections=20, max_connections=100)
        )

        # Authentication setup
        self.auth_headers = {}
        if self.config.api_key:
            self.auth_headers["Ocp-Apim-Subscription-Key"] = self.config.api_key

    async def extract_lmp_data(
        self,
        data_type: str,
        start_time: datetime,
        end_time: datetime
    ) -> List[Dict[str, Any]]:
        """Extract LMP data from PJM Data Miner."""
        try:
            # PJM LMP data uses specific report types
            report_config = self.config.report_configs.get("lmp", {})

            # Build query parameters
            params = {
                "startRow": 1,
                "rowCount": self.config.batch_size,
                "format": "json"
            }

            # Add time filters
            if data_type == "lmp":
                params["datetime_beginning_ept"] = start_time.strftime("%Y-%m-%dT%H:%M:%S")
                params["datetime_ending_ept"] = end_time.strftime("%Y-%m-%dT%H:%M:%S")
                url = f"{self.config.data_miner_url}/da_hrl_lmps"
            else:  # rtm_lmp
                params["datetime_beginning_ept"] = start_time.strftime("%Y-%m-%dT%H:%M:%S")
                params["datetime_ending_ept"] = end_time.strftime("%Y-%m-%dT%H:%M:%S")
                url = f"{self.config.data_miner_url}/rt_hrl_lmps"

            response = await self.client.get(
                url,
                params=params,
                headers=self.auth_headers
            )

            if response.status_code != 200:
                raise ExtractionError(f"PJM API request failed: {response.status_code} - {response.text}")

            data = response.json()

            # Normalize PJM LMP format
            return self._normalize_lmp_data(data, data_type)

        except Exception as e:
            raise ExtractionError(f"LMP extraction failed: {str(e)}")

    async def extract_rpm_data(self, start_time: datetime, end_time: datetime) -> List[Dict[str, Any]]:
        """Extract RPM auction results."""
        try:
            # PJM RPM data from capacity market reports
            url = f"{self.config.base_url}/capacity_market_results"

            params = {
                "start_date": start_time.strftime("%Y-%m-%d"),
                "end_date": end_time.strftime("%Y-%m-%d"),
                "format": "json"
            }

            response = await self.client.get(
                url,
                params=params,
                headers=self.auth_headers
            )

            if response.status_code != 200:
                raise ExtractionError(f"PJM API request failed: {response.status_code} - {response.text}")

            data = response.json()
            return self._normalize_rpm_data(data)

        except Exception as e:
            raise ExtractionError(f"RPM extraction failed: {str(e)}")

    async def extract_tcr_data(self, start_time: datetime, end_time: datetime) -> List[Dict[str, Any]]:
        """Extract TCR auction results."""
        try:
            # PJM TCR data from transmission rights reports
            url = f"{self.config.base_url}/tcr_auction_results"

            params = {
                "start_date": start_time.strftime("%Y-%m-%d"),
                "end_date": end_time.strftime("%Y-%m-%d"),
                "format": "json"
            }

            response = await self.client.get(
                url,
                params=params,
                headers=self.auth_headers
            )

            if response.status_code != 200:
                raise ExtractionError(f"PJM API request failed: {response.status_code} - {response.text}")

            data = response.json()
            return self._normalize_tcr_data(data)

        except Exception as e:
            raise ExtractionError(f"TCR extraction failed: {str(e)}")

    async def extract_outages_data(self, start_time: datetime, end_time: datetime) -> List[Dict[str, Any]]:
        """Extract generation outages data."""
        try:
            # PJM outages from outage reports
            url = f"{self.config.base_url}/outages"

            params = {
                "start_datetime": start_time.strftime("%Y-%m-%dT%H:%M:%S"),
                "end_datetime": end_time.strftime("%Y-%m-%dT%H:%M:%S"),
                "format": "json"
            }

            response = await self.client.get(
                url,
                params=params,
                headers=self.auth_headers
            )

            if response.status_code != 200:
                raise ExtractionError(f"PJM API request failed: {response.status_code} - {response.text}")

            data = response.json()
            return self._normalize_outages_data(data)

        except Exception as e:
            raise ExtractionError(f"Outages extraction failed: {str(e)}")

    def _normalize_lmp_data(self, raw_data: Dict[str, Any], data_type: str) -> List[Dict[str, Any]]:
        """Normalize PJM LMP data format."""
        normalized = []

        # PJM Data Miner LMP format
        if "items" in raw_data:
            for item in raw_data["items"]:
                normalized.append({
                    "timestamp": item.get("datetime_beginning_ept"),
                    "node": item.get("pnode_name"),
                    "lmp": float(item.get("total_lmp_da", 0)) if data_type == "lmp" else float(item.get("total_lmp_rt", 0)),
                    "mcc": float(item.get("congestion_price_da", 0)) if data_type == "lmp" else float(item.get("congestion_price_rt", 0)),
                    "mlc": float(item.get("marginal_loss_price_da", 0)) if data_type == "lmp" else float(item.get("marginal_loss_price_rt", 0)),
                    "market": "pjm",
                    "data_type": data_type,
                    "ingested_at": datetime.now(timezone.utc).isoformat()
                })

        return normalized

    def _normalize_rpm_data(self, raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Normalize PJM RPM auction data."""
        normalized = []

        if "items" in raw_data:
            for item in raw_data["items"]:
                normalized.append({
                    "auction_date": item.get("auction_date"),
                    "lda": item.get("lda"),
                    "clearing_price_usd_per_mw_day": float(item.get("clearing_price", 0)),
                    "capacity_obligations_mw": float(item.get("capacity_obligations", 0)),
                    "auction_type": item.get("auction_type"),
                    "market": "pjm",
                    "data_type": "rpm",
                    "ingested_at": datetime.now(timezone.utc).isoformat()
                })

        return normalized

    def _normalize_tcr_data(self, raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Normalize PJM TCR auction data."""
        normalized = []

        if "items" in raw_data:
            for item in raw_data["items"]:
                normalized.append({
                    "auction_date": item.get("auction_date"),
                    "path": item.get("path_name"),
                    "source": item.get("source"),
                    "sink": item.get("sink"),
                    "auction_type": item.get("auction_type"),
                    "clearing_price_usd_per_mw": float(item.get("clearing_price", 0)),
                    "awarded_quantity_mw": float(item.get("awarded_quantity", 0)),
                    "market": "pjm",
                    "data_type": "tcr",
                    "ingested_at": datetime.now(timezone.utc).isoformat()
                })

        return normalized

    def _normalize_outages_data(self, raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Normalize PJM outages data."""
        normalized = []

        if "items" in raw_data:
            for item in raw_data["items"]:
                normalized.append({
                    "outage_id": item.get("outage_id"),
                    "unit_name": item.get("unit_name"),
                    "outage_type": item.get("outage_type"),
                    "start_time": item.get("outage_begin_time"),
                    "end_time": item.get("outage_end_time"),
                    "capacity_mw": float(item.get("outage_mw", 0)),
                    "reason": item.get("outage_reason"),
                    "market": "pjm",
                    "data_type": "outages",
                    "ingested_at": datetime.now(timezone.utc).isoformat()
                })

        return normalized

    async def close(self):
        """Close HTTP client."""
        await self.client.aclose()
