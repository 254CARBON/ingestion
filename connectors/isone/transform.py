"""
ISO-NE data transformation utilities.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from ..base.exceptions import TransformationError


class ISONETransformConfig:
    """ISO-NE transformation configuration."""

    # Field mappings for different data types
    lmp_field_mapping: Dict[str, str] = {
        "timestamp": "timestamp",
        "node": "node_id",
        "lmp": "lmp",
        "congestion": "congestion_component",
        "losses": "losses_component"
    }

    fcm_field_mapping: Dict[str, str] = {
        "auction_date": "auction_date",
        "capacity_zone": "capacity_zone",
        "clearing_price": "clearing_price",
        "capacity_obligations": "capacity_obligations",
        "auction_round": "auction_round"
    }

    ftr_field_mapping: Dict[str, str] = {
        "auction_date": "auction_date",
        "path": "path",
        "source": "source",
        "sink": "sink",
        "auction_type": "auction_type",
        "clearing_price": "clearing_price",
        "awarded_quantity": "awarded_quantity"
    }


class ISONETransform:
    """ISO-NE data transformation utilities."""

    def __init__(self, config: ISONETransformConfig = None):
        self.config = config or ISONETransformConfig()

    def transform_lmp_data(self, raw_data: List[Dict[str, Any]], data_type: str) -> List[Dict[str, Any]]:
        """Transform LMP data to normalized format."""
        normalized_data = []

        for item in raw_data:
            normalized_item = {
                "event_id": f"isone_{data_type}_{item.get('timestamp', '')}_{item.get('node', '')}",
                "trace_id": f"isone_{data_type}_transform",
                "schema_version": "1.0.0",
                "tenant_id": "default",
                "producer": "ingestion-isone@v1.0.0",
                "occurred_at": self._parse_isone_timestamp(item.get("timestamp")),
                "ingested_at": int(datetime.now(timezone.utc).timestamp() * 1_000_000),
                "payload": {
                    "market": "isone",
                    "data_type": data_type,
                    "node": item.get("node"),
                    "timestamp": item.get("timestamp"),
                    "lmp_usd_per_mwh": float(item.get("lmp", 0)),
                    "mcc_usd_per_mwh": float(item.get("congestion", 0)),
                    "mlc_usd_per_mwh": float(item.get("losses", 0))
                }
            }
            normalized_data.append(normalized_item)

        return normalized_data

    def transform_fcm_data(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform FCM data to normalized format."""
        normalized_data = []

        for item in raw_data:
            normalized_item = {
                "event_id": f"isone_fcm_{item.get('auction_date', '')}_{item.get('capacity_zone', '')}",
                "trace_id": "isone_fcm_transform",
                "schema_version": "1.0.0",
                "tenant_id": "default",
                "producer": "ingestion-isone@v1.0.0",
                "occurred_at": self._parse_isone_date(item.get("auction_date")),
                "ingested_at": int(datetime.now(timezone.utc).timestamp() * 1_000_000),
                "payload": {
                    "market": "isone",
                    "data_type": "fcm",
                    "auction_date": item.get("auction_date"),
                    "capacity_zone": item.get("capacity_zone"),
                    "clearing_price_usd_per_kw_month": float(item.get("clearing_price_usd_per_kw_month", 0)),
                    "capacity_obligations_mw": float(item.get("capacity_obligations_mw", 0)),
                    "auction_round": item.get("auction_round")
                }
            }
            normalized_data.append(normalized_item)

        return normalized_data

    def transform_ftr_data(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform FTR data to normalized format."""
        normalized_data = []

        for item in raw_data:
            normalized_item = {
                "event_id": f"isone_ftr_{item.get('auction_date', '')}_{item.get('path', '')}",
                "trace_id": "isone_ftr_transform",
                "schema_version": "1.0.0",
                "tenant_id": "default",
                "producer": "ingestion-isone@v1.0.0",
                "occurred_at": self._parse_isone_date(item.get("auction_date")),
                "ingested_at": int(datetime.now(timezone.utc).timestamp() * 1_000_000),
                "payload": {
                    "market": "isone",
                    "data_type": "ftr",
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

    def _parse_isone_timestamp(self, timestamp_str: Optional[str]) -> int:
        """Parse ISO-NE timestamp string to microseconds since epoch."""
        if not timestamp_str:
            return int(datetime.now(timezone.utc).timestamp() * 1_000_000)

        try:
            # ISO-NE typically uses ISO format
            if "T" in timestamp_str:
                dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
            else:
                dt = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")

            return int(dt.timestamp() * 1_000_000)
        except Exception:
            return int(datetime.now(timezone.utc).timestamp() * 1_000_000)

    def _parse_isone_date(self, date_str: Optional[str]) -> int:
        """Parse ISO-NE date string to microseconds since epoch."""
        if not date_str:
            return int(datetime.now(timezone.utc).timestamp() * 1_000_000)

        try:
            # ISO-NE dates are typically YYYY-MM-DD format
            dt = datetime.strptime(date_str, "%Y-%m-%d")
            return int(dt.timestamp() * 1_000_000)
        except Exception:
            return int(datetime.now(timezone.utc).timestamp() * 1_000_000)
