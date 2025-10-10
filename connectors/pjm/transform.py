"""
PJM data transformation utilities.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from ..base.exceptions import TransformationError


class PJMTransformConfig:
    """PJM transformation configuration."""

    # Field mappings for different data types
    lmp_field_mapping: Dict[str, str] = {
        "timestamp": "datetime_beginning_ept",
        "node": "pnode_name",
        "lmp": "total_lmp_da",
        "mcc": "congestion_price_da",
        "mlc": "marginal_loss_price_da"
    }

    rtm_lmp_field_mapping: Dict[str, str] = {
        "timestamp": "datetime_beginning_ept",
        "node": "pnode_name",
        "lmp": "total_lmp_rt",
        "mcc": "congestion_price_rt",
        "mlc": "marginal_loss_price_rt"
    }

    rpm_field_mapping: Dict[str, str] = {
        "auction_date": "auction_date",
        "lda": "lda",
        "clearing_price": "clearing_price",
        "capacity_obligations": "capacity_obligations",
        "auction_type": "auction_type"
    }

    tcr_field_mapping: Dict[str, str] = {
        "auction_date": "auction_date",
        "path": "path_name",
        "source": "source",
        "sink": "sink",
        "auction_type": "auction_type",
        "clearing_price": "clearing_price",
        "awarded_quantity": "awarded_quantity"
    }

    outages_field_mapping: Dict[str, str] = {
        "outage_id": "outage_id",
        "unit_name": "unit_name",
        "outage_type": "outage_type",
        "start_time": "outage_begin_time",
        "end_time": "outage_end_time",
        "capacity_mw": "outage_mw",
        "reason": "outage_reason"
    }


class PJMTransform:
    """PJM data transformation utilities."""

    def __init__(self, config: PJMTransformConfig = None):
        self.config = config or PJMTransformConfig()

    def transform_lmp_data(self, raw_data: List[Dict[str, Any]], data_type: str) -> List[Dict[str, Any]]:
        """Transform LMP data to normalized format."""
        normalized_data = []

        for item in raw_data:
            normalized_item = {
                "event_id": f"pjm_{data_type}_{item.get('timestamp', '')}_{item.get('node', '')}",
                "trace_id": f"pjm_{data_type}_transform",
                "schema_version": "1.0.0",
                "tenant_id": "default",
                "producer": "ingestion-pjm@v1.0.0",
                "occurred_at": self._parse_pjm_timestamp(item.get("timestamp")),
                "ingested_at": int(datetime.now(timezone.utc).timestamp() * 1_000_000),
                "payload": {
                    "market": "pjm",
                    "data_type": data_type,
                    "node": item.get("node"),
                    "timestamp": item.get("timestamp"),
                    "lmp_usd_per_mwh": float(item.get("lmp", 0)),
                    "mcc_usd_per_mwh": float(item.get("mcc", 0)),
                    "mlc_usd_per_mwh": float(item.get("mlc", 0))
                }
            }
            normalized_data.append(normalized_item)

        return normalized_data

    def transform_rpm_data(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform RPM auction data to normalized format."""
        normalized_data = []

        for item in raw_data:
            normalized_item = {
                "event_id": f"pjm_rpm_{item.get('auction_date', '')}_{item.get('lda', '')}",
                "trace_id": "pjm_rpm_transform",
                "schema_version": "1.0.0",
                "tenant_id": "default",
                "producer": "ingestion-pjm@v1.0.0",
                "occurred_at": self._parse_pjm_date(item.get("auction_date")),
                "ingested_at": int(datetime.now(timezone.utc).timestamp() * 1_000_000),
                "payload": {
                    "market": "pjm",
                    "data_type": "rpm",
                    "auction_date": item.get("auction_date"),
                    "lda": item.get("lda"),
                    "clearing_price_usd_per_mw_day": float(item.get("clearing_price_usd_per_mw_day", 0)),
                    "capacity_obligations_mw": float(item.get("capacity_obligations_mw", 0)),
                    "auction_type": item.get("auction_type")
                }
            }
            normalized_data.append(normalized_item)

        return normalized_data

    def transform_tcr_data(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform TCR auction data to normalized format."""
        normalized_data = []

        for item in raw_data:
            normalized_item = {
                "event_id": f"pjm_tcr_{item.get('auction_date', '')}_{item.get('path', '')}",
                "trace_id": "pjm_tcr_transform",
                "schema_version": "1.0.0",
                "tenant_id": "default",
                "producer": "ingestion-pjm@v1.0.0",
                "occurred_at": self._parse_pjm_date(item.get("auction_date")),
                "ingested_at": int(datetime.now(timezone.utc).timestamp() * 1_000_000),
                "payload": {
                    "market": "pjm",
                    "data_type": "tcr",
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

    def transform_outages_data(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform outages data to normalized format."""
        normalized_data = []

        for item in raw_data:
            normalized_item = {
                "event_id": f"pjm_outages_{item.get('outage_id', '')}",
                "trace_id": "pjm_outages_transform",
                "schema_version": "1.0.0",
                "tenant_id": "default",
                "producer": "ingestion-pjm@v1.0.0",
                "occurred_at": self._parse_pjm_timestamp(item.get("start_time")),
                "ingested_at": int(datetime.now(timezone.utc).timestamp() * 1_000_000),
                "payload": {
                    "market": "pjm",
                    "data_type": "outages",
                    "outage_id": item.get("outage_id"),
                    "unit_name": item.get("unit_name"),
                    "outage_type": item.get("outage_type"),
                    "start_time": item.get("start_time"),
                    "end_time": item.get("end_time"),
                    "capacity_mw": float(item.get("capacity_mw", 0)),
                    "reason": item.get("reason")
                }
            }
            normalized_data.append(normalized_item)

        return normalized_data

    def _parse_pjm_timestamp(self, timestamp_str: Optional[str]) -> int:
        """Parse PJM timestamp string to microseconds since epoch."""
        if not timestamp_str:
            return int(datetime.now(timezone.utc).timestamp() * 1_000_000)

        try:
            # PJM typically uses ISO format with timezone
            if "T" in timestamp_str and ("Z" in timestamp_str or "+" in timestamp_str or "-" in timestamp_str[-6:]):
                dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
            else:
                # Fallback for other formats
                dt = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")

            return int(dt.timestamp() * 1_000_000)
        except Exception:
            return int(datetime.now(timezone.utc).timestamp() * 1_000_000)

    def _parse_pjm_date(self, date_str: Optional[str]) -> int:
        """Parse PJM date string to microseconds since epoch."""
        if not date_str:
            return int(datetime.now(timezone.utc).timestamp() * 1_000_000)

        try:
            # PJM dates are typically YYYY-MM-DD format
            dt = datetime.strptime(date_str, "%Y-%m-%d")
            return int(dt.timestamp() * 1_000_000)
        except Exception:
            return int(datetime.now(timezone.utc).timestamp() * 1_000_000)
