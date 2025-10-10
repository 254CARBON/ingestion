"""
NYISO data extractor using OASIS API.
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import httpx

from ..base.exceptions import ExtractionError
from .config import NYISOConnectorConfig


class NYISOExtractor:
    """NYISO OASIS API data extractor."""

    def __init__(self, config: NYISOConnectorConfig):
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

    async def extract_lbmp_data(
        self,
        data_type: str,
        start_time: datetime,
        end_time: datetime
    ) -> List[Dict[str, Any]]:
        """Extract LBMP data from NYISO OASIS."""
        try:
            # NYISO LBMP data from OASIS API
            if data_type == "lbmp":
                url = f"{self.config.data_url}/da_lbmp"
            else:  # rt_lbmp
                url = f"{self.config.data_url}/rt_lbmp"

            # Format time parameters for NYISO API
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
                raise ExtractionError(f"NYISO API request failed: {response.status_code} - {response.text}")

            data = response.json()

            # Normalize NYISO LBMP format
            return self._normalize_lbmp_data(data, data_type)

        except Exception as e:
            raise ExtractionError(f"LBMP extraction failed: {str(e)}")

    async def extract_icap_data(self, start_time: datetime, end_time: datetime) -> List[Dict[str, Any]]:
        """Extract ICAP demand curve data."""
        try:
            # NYISO ICAP data from capacity market reports
            url = f"{self.config.data_url}/icap_demand_curve"

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
                raise ExtractionError(f"NYISO API request failed: {response.status_code} - {response.text}")

            data = response.json()
            return self._normalize_icap_data(data)

        except Exception as e:
            raise ExtractionError(f"ICAP extraction failed: {str(e)}")

    async def extract_tcc_data(self, start_time: datetime, end_time: datetime) -> List[Dict[str, Any]]:
        """Extract TCC auction results."""
        try:
            # NYISO TCC data from transmission rights reports
            url = f"{self.config.data_url}/tcc_auction_results"

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
                raise ExtractionError(f"NYISO API request failed: {response.status_code} - {response.text}")

            data = response.json()
            return self._normalize_tcc_data(data)

        except Exception as e:
            raise ExtractionError(f"TCC extraction failed: {str(e)}")

    def _normalize_lbmp_data(self, raw_data: Dict[str, Any], data_type: str) -> List[Dict[str, Any]]:
        """Normalize NYISO LBMP data format."""
        normalized = []

        # NYISO OASIS LBMP format
        if "data" in raw_data:
            for item in raw_data["data"]:
                normalized.append({
                    "timestamp": item.get("time_stamp"),
                    "node": item.get("node_id"),
                    "lbmp": float(item.get("lbmp", 0)),
                    "congestion": float(item.get("congestion_component", 0)),
                    "losses": float(item.get("losses_component", 0)),
                    "market": "nyiso",
                    "data_type": data_type,
                    "ingested_at": datetime.now(timezone.utc).isoformat()
                })

        return normalized

    def _normalize_icap_data(self, raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Normalize NYISO ICAP data."""
        normalized = []

        if "data" in raw_data:
            for item in raw_data["data"]:
                normalized.append({
                    "capacity_zone": item.get("capacity_zone"),
                    "month": item.get("month"),
                    "icap_demand_curve_price": float(item.get("icap_demand_curve_price", 0)),
                    "capacity_obligations_mw": float(item.get("capacity_obligations", 0)),
                    "market": "nyiso",
                    "data_type": "icap",
                    "ingested_at": datetime.now(timezone.utc).isoformat()
                })

        return normalized

    def _normalize_tcc_data(self, raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Normalize NYISO TCC data."""
        normalized = []

        if "data" in raw_data:
            for item in raw_data["data"]:
                normalized.append({
                    "auction_date": item.get("auction_date"),
                    "path": item.get("path"),
                    "source": item.get("source_zone"),
                    "sink": item.get("sink_zone"),
                    "auction_type": item.get("auction_type"),
                    "clearing_price_usd_per_mw": float(item.get("clearing_price", 0)),
                    "awarded_quantity_mw": float(item.get("awarded_quantity", 0)),
                    "market": "nyiso",
                    "data_type": "tcc",
                    "ingested_at": datetime.now(timezone.utc).isoformat()
                })

        return normalized

    async def close(self):
        """Close HTTP client."""
        await self.client.aclose()
