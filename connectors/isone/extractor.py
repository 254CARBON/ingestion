"""
ISO-NE data extractor using web services API.
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import httpx

from ..base.exceptions import ExtractionError
from .config import ISONEConnectorConfig


class ISONEExtractor:
    """ISO-NE web services API data extractor."""

    def __init__(self, config: ISONEConnectorConfig):
        self.config = config
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(60.0),
            limits=httpx.Limits(max_keepalive_connections=20, max_connections=100)
        )

        # Authentication setup
        self.auth_headers = {}
        if self.config.api_key:
            self.auth_headers["X-API-Key"] = self.config.api_key
        elif self.config.username and self.config.password:
            # Basic auth would be set up here
            pass

    async def extract_lmp_data(
        self,
        data_type: str,
        start_time: datetime,
        end_time: datetime
    ) -> List[Dict[str, Any]]:
        """Extract LMP data from ISO-NE web services."""
        try:
            # ISO-NE LMP data from API
            if data_type == "lmp":
                url = f"{self.config.base_url}/v1.1/lmp/da"
            else:  # rt_lmp
                url = f"{self.config.base_url}/v1.1/lmp/rt"

            # Format time parameters for ISO-NE API
            params = {
                "start": start_time.strftime("%Y%m%d%H%M"),
                "end": end_time.strftime("%Y%m%d%H%M"),
                "format": "json"
            }

            response = await self.client.get(
                url,
                params=params,
                headers=self.auth_headers
            )

            if response.status_code != 200:
                raise ExtractionError(f"ISO-NE API request failed: {response.status_code} - {response.text}")

            data = response.json()

            # Normalize ISO-NE LMP format
            return self._normalize_lmp_data(data, data_type)

        except Exception as e:
            raise ExtractionError(f"LMP extraction failed: {str(e)}")

    async def extract_fcm_data(self, start_time: datetime, end_time: datetime) -> List[Dict[str, Any]]:
        """Extract FCM auction results."""
        try:
            # ISO-NE FCM data from capacity market reports
            url = f"{self.config.base_url}/v1.1/capacity/fcm"

            params = {
                "startdate": start_time.strftime("%Y%m%d"),
                "enddate": end_time.strftime("%Y%m%d"),
                "format": "json"
            }

            response = await self.client.get(
                url,
                params=params,
                headers=self.auth_headers
            )

            if response.status_code != 200:
                raise ExtractionError(f"ISO-NE API request failed: {response.status_code} - {response.text}")

            data = response.json()
            return self._normalize_fcm_data(data)

        except Exception as e:
            raise ExtractionError(f"FCM extraction failed: {str(e)}")

    async def extract_ftr_data(self, start_time: datetime, end_time: datetime) -> List[Dict[str, Any]]:
        """Extract FTR auction results."""
        try:
            # ISO-NE FTR data from transmission rights reports
            url = f"{self.config.base_url}/v1.1/ftr/auction"

            params = {
                "startdate": start_time.strftime("%Y%m%d"),
                "enddate": end_time.strftime("%Y%m%d"),
                "format": "json"
            }

            response = await self.client.get(
                url,
                params=params,
                headers=self.auth_headers
            )

            if response.status_code != 200:
                raise ExtractionError(f"ISO-NE API request failed: {response.status_code} - {response.text}")

            data = response.json()
            return self._normalize_ftr_data(data)

        except Exception as e:
            raise ExtractionError(f"FTR extraction failed: {str(e)}")

    def _normalize_lmp_data(self, raw_data: Dict[str, Any], data_type: str) -> List[Dict[str, Any]]:
        """Normalize ISO-NE LMP data format."""
        normalized = []

        # ISO-NE API LMP format
        if "data" in raw_data:
            for item in raw_data["data"]:
                normalized.append({
                    "timestamp": item.get("timestamp"),
                    "node": item.get("node_id"),
                    "lmp": float(item.get("lmp", 0)),
                    "congestion": float(item.get("congestion_component", 0)),
                    "losses": float(item.get("losses_component", 0)),
                    "market": "isone",
                    "data_type": data_type,
                    "ingested_at": datetime.now(timezone.utc).isoformat()
                })

        return normalized

    def _normalize_fcm_data(self, raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Normalize ISO-NE FCM data."""
        normalized = []

        if "data" in raw_data:
            for item in raw_data["data"]:
                normalized.append({
                    "auction_date": item.get("auction_date"),
                    "capacity_zone": item.get("capacity_zone"),
                    "clearing_price_usd_per_kw_month": float(item.get("clearing_price", 0)),
                    "capacity_obligations_mw": float(item.get("capacity_obligations", 0)),
                    "auction_round": item.get("auction_round"),
                    "market": "isone",
                    "data_type": "fcm",
                    "ingested_at": datetime.now(timezone.utc).isoformat()
                })

        return normalized

    def _normalize_ftr_data(self, raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Normalize ISO-NE FTR data."""
        normalized = []

        if "data" in raw_data:
            for item in raw_data["data"]:
                normalized.append({
                    "auction_date": item.get("auction_date"),
                    "path": item.get("path"),
                    "source": item.get("source"),
                    "sink": item.get("sink"),
                    "auction_type": item.get("auction_type"),
                    "clearing_price_usd_per_mw": float(item.get("clearing_price", 0)),
                    "awarded_quantity_mw": float(item.get("awarded_quantity", 0)),
                    "market": "isone",
                    "data_type": "ftr",
                    "ingested_at": datetime.now(timezone.utc).isoformat()
                })

        return normalized

    async def close(self):
        """Close HTTP client."""
        await self.client.aclose()
