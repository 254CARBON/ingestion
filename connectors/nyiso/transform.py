"""
NYISO data transformation utilities.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from ..base.exceptions import TransformationError


class NYISOTransformConfig:
    """NYISO transformation configuration."""

    # Field mappings for different data types
    lbmp_field_mapping: Dict[str, str] = {
        "timestamp": "time_stamp",
        "node": "node_id",
        "lbmp": "lbmp",
        "congestion": "congestion_component",
        "losses": "losses_component"
    }

    icap_field_mapping: Dict[str, str] = {
        "capacity_zone": "capacity_zone",
        "month": "month",
        "icap_demand_curve_price": "icap_demand_curve_price",
        "capacity_obligations": "capacity_obligations"
    }

    tcc_field_mapping: Dict[str, str] = {
        "auction_date": "auction_date",
        "path": "path",
        "source": "source_zone",
        "sink": "sink_zone",
        "auction_type": "auction_type",
        "clearing_price": "clearing_price",
        "awarded_quantity": "awarded_quantity"
    }


class NYISOTransform:
    """NYISO data transformation utilities."""

    def __init__(self, config: NYISOTransformConfig = None):
        self.config = config or NYISOTransformConfig()

    def transform_lbmp_data(self, raw_data: List[Dict[str, Any]], data_type: str) -> List[Dict[str, Any]]:
        """Transform LBMP data to normalized format."""
        normalized_data = []

        for item in raw_data:
            normalized_item = {
                "event_id": f"nyiso_{data_type}_{item.get('timestamp', '')}_{item.get('node', '')}",
                "trace_id": f"nyiso_{data_type}_transform",
                "schema_version": "1.0.0",
                "tenant_id": "default",
                "producer": "ingestion-nyiso@v1.0.0",
                "occurred_at": self._parse_nyiso_timestamp(item.get("timestamp")),
                "ingested_at": int(datetime.now(timezone.utc).timestamp() * 1_000_000),
                "payload": {
                    "market": "nyiso",
                    "data_type": data_type,
                    "node": item.get("node"),
                    "timestamp": item.get("timestamp"),
                    "lmp_usd_per_mwh": float(item.get("lbmp", 0)),
                    "mcc_usd_per_mwh": float(item.get("congestion", 0)),
                    "mlc_usd_per_mwh": float(item.get("losses", 0))
                }
            }
            normalized_data.append(normalized_item)

        return normalized_data

    def transform_icap_data(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform ICAP data to normalized format."""
        normalized_data = []

        for item in raw_data:
            normalized_item = {
                "event_id": f"nyiso_icap_{item.get('capacity_zone', '')}_{item.get('month', '')}",
                "trace_id": "nyiso_icap_transform",
                "schema_version": "1.0.0",
                "tenant_id": "default",
                "producer": "ingestion-nyiso@v1.0.0",
                "occurred_at": self._parse_nyiso_month(item.get("month")),
                "ingested_at": int(datetime.now(timezone.utc).timestamp() * 1_000_000),
                "payload": {
                    "market": "nyiso",
                    "data_type": "icap",
                    "capacity_zone": item.get("capacity_zone"),
                    "month": item.get("month"),
                    "icap_demand_curve_price_usd_per_kw_month": float(item.get("icap_demand_curve_price", 0)),
                    "capacity_obligations_mw": float(item.get("capacity_obligations_mw", 0))
                }
            }
            normalized_data.append(normalized_item)

        return normalized_data

    def transform_tcc_data(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform TCC data to normalized format."""
        normalized_data = []

        for item in raw_data:
            normalized_item = {
                "event_id": f"nyiso_tcc_{item.get('auction_date', '')}_{item.get('path', '')}",
                "trace_id": "nyiso_tcc_transform",
                "schema_version": "1.0.0",
                "tenant_id": "default",
                "producer": "ingestion-nyiso@v1.0.0",
                "occurred_at": self._parse_nyiso_date(item.get("auction_date")),
                "ingested_at": int(datetime.now(timezone.utc).timestamp() * 1_000_000),
                "payload": {
                    "market": "nyiso",
                    "data_type": "tcc",
                    "auction_date": item.get("auction_date"),
                    "path": item.get("path"),
                    "source": item.get("source"),
                    "sink": item.get("sink"),
                    "auction_type": item.get("auction_type"),
                    "clearing_price_usd_per_mw": float(item.get("clearing_price_usd_per_mw", 0)),
                    "awarded_quantity_mw": float(item.get("awarded_quantity_mw", 0))
                }
            }
            normalized_data.append(normalized_item)

        return normalized_data

    def _parse_nyiso_timestamp(self, timestamp_str: Optional[str]) -> int:
        """Parse NYISO timestamp string to microseconds since epoch."""
        if not timestamp_str:
            return int(datetime.now(timezone.utc).timestamp() * 1_000_000)

        try:
            # NYISO typically uses ISO format
            if "T" in timestamp_str:
                dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
            else:
                dt = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")

            return int(dt.timestamp() * 1_000_000)
        except Exception:
            return int(datetime.now(timezone.utc).timestamp() * 1_000_000)

    def _parse_nyiso_month(self, month_str: Optional[str]) -> int:
        """Parse NYISO month string to microseconds since epoch."""
        if not month_str:
            return int(datetime.now(timezone.utc).timestamp() * 1_000_000)

        try:
            # NYISO months are typically YYYY-MM format
            dt = datetime.strptime(month_str, "%Y-%m")
            return int(dt.timestamp() * 1_000_000)
        except Exception:
            return int(datetime.now(timezone.utc).timestamp() * 1_000_000)

    def _parse_nyiso_date(self, date_str: Optional[str]) -> int:
        """Parse NYISO date string to microseconds since epoch."""
        if not date_str:
            return int(datetime.now(timezone.utc).timestamp() * 1_000_000)

        try:
            # NYISO dates are typically YYYY-MM-DD format
            dt = datetime.strptime(date_str, "%Y-%m-%d")
            return int(dt.timestamp() * 1_000_000)
        except Exception:
            return int(datetime.now(timezone.utc).timestamp() * 1_000_000)
