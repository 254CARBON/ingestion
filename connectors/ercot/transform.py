"""
ERCOT data transformation utilities.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from ..base.exceptions import TransformationError


class ERCOTTransformConfig:
    """ERCOT transformation configuration."""

    # Field mappings for different data types
    dam_field_mapping: Dict[str, str] = {
        "timestamp": "timestamp",
        "node": "node",
        "lmp": "lmp",
        "mcc": "mcc",
        "mlc": "mlc"
    }

    rtm_field_mapping: Dict[str, str] = {
        "timestamp": "timestamp",
        "node": "node",
        "lmp": "lmp",
        "mcc": "mcc",
        "mlc": "mlc"
    }

    ordc_field_mapping: Dict[str, str] = {
        "timestamp": "timestamp",
        "reserve_zone": "reserve_zone",
        "ordc_price": "ordc_price",
        "demand_curve_mw": "demand_curve_mw"
    }

    as_field_mapping: Dict[str, str] = {
        "timestamp": "timestamp",
        "as_type": "as_type",
        "zone": "zone",
        "cleared_price": "cleared_price",
        "cleared_quantity": "cleared_quantity"
    }

    crr_field_mapping: Dict[str, str] = {
        "auction_date": "auction_date",
        "path": "path",
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
        "start_time": "start_time",
        "end_time": "end_time",
        "capacity_mw": "capacity_mw",
        "reason": "reason"
    }


class ERCOTTransform:
    """ERCOT data transformation utilities."""

    def __init__(self, config: ERCOTTransformConfig = None):
        self.config = config or ERCOTTransformConfig()

    def transform_lmp_data(self, raw_data: List[Dict[str, Any]], data_type: str) -> List[Dict[str, Any]]:
        """Transform LMP data (DAM or RTM) to normalized format."""
        if data_type not in ["dam", "rtm"]:
            raise TransformationError(f"Unsupported LMP data type: {data_type}")

        normalized_data = []

        for item in raw_data:
            timestamp = item.get("timestamp")
            node = item.get("node")
            if not timestamp or not node:
                continue

            normalized_item = {
                "event_id": f"ercot_{data_type}_{timestamp}_{node}",
                "trace_id": f"ercot_{data_type}_transform",
                "schema_version": "1.0.0",
                "tenant_id": "default",
                "producer": "ingestion-ercot@v1.0.0",
                "occurred_at": self._parse_timestamp(timestamp),
                "ingested_at": int(datetime.now(timezone.utc).timestamp() * 1_000_000),
                "payload": {
                    "market": "ercot",
                    "data_type": data_type,
                    "node": node,
                    "timestamp": timestamp,
                    "lmp_usd_per_mwh": self._safe_float(item.get("lmp")),
                    "mcc_usd_per_mwh": self._safe_float(item.get("mcc")),
                    "mlc_usd_per_mwh": self._safe_float(item.get("mlc")),
                }
            }
            normalized_data.append(normalized_item)

        return normalized_data

    def transform_ordc_data(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform ORDC data to normalized format."""
        normalized_data = []

        for item in raw_data:
            timestamp = item.get("timestamp")
            zone = item.get("reserve_zone")
            if not timestamp or not zone:
                continue
            normalized_item = {
                "event_id": f"ercot_ordc_{timestamp}_{zone}",
                "trace_id": "ercot_ordc_transform",
                "schema_version": "1.0.0",
                "tenant_id": "default",
                "producer": "ingestion-ercot@v1.0.0",
                "occurred_at": self._parse_timestamp(timestamp),
                "ingested_at": int(datetime.now(timezone.utc).timestamp() * 1_000_000),
                "payload": {
                    "market": "ercot",
                    "data_type": "ordc",
                    "timestamp": timestamp,
                    "reserve_zone": zone,
                    "ordc_price_usd_per_mwh": self._safe_float(item.get("ordc_price")),
                    "demand_curve_mw": self._safe_float(item.get("demand_curve_mw")),
                }
            }
            normalized_data.append(normalized_item)

        return normalized_data

    def transform_as_data(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform Ancillary Services data to normalized format."""
        normalized_data = []

        for item in raw_data:
            timestamp = item.get("timestamp")
            as_type = item.get("as_type")
            if not timestamp or not as_type:
                continue
            normalized_item = {
                "event_id": f"ercot_as_{timestamp}_{as_type}",
                "trace_id": "ercot_as_transform",
                "schema_version": "1.0.0",
                "tenant_id": "default",
                "producer": "ingestion-ercot@v1.0.0",
                "occurred_at": self._parse_timestamp(timestamp),
                "ingested_at": int(datetime.now(timezone.utc).timestamp() * 1_000_000),
                "payload": {
                    "market": "ercot",
                    "data_type": "as",
                    "timestamp": timestamp,
                    "as_type": as_type,
                    "zone": item.get("zone"),
                    "cleared_price_usd_per_mwh": self._safe_float(item.get("cleared_price")),
                    "cleared_quantity_mw": self._safe_float(item.get("cleared_quantity")),
                }
            }
            normalized_data.append(normalized_item)

        return normalized_data

    def transform_crr_data(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform CRR auction data to normalized format."""
        normalized_data = []

        for item in raw_data:
            auction_date = item.get("auction_date")
            path = item.get("path")
            if not auction_date or not path:
                continue
            normalized_item = {
                "event_id": f"ercot_crr_{auction_date}_{path}",
                "trace_id": "ercot_crr_transform",
                "schema_version": "1.0.0",
                "tenant_id": "default",
                "producer": "ingestion-ercot@v1.0.0",
                "occurred_at": self._parse_timestamp(auction_date),
                "ingested_at": int(datetime.now(timezone.utc).timestamp() * 1_000_000),
                "payload": {
                    "market": "ercot",
                    "data_type": "crr",
                    "auction_date": auction_date,
                    "path": path,
                    "source": item.get("source"),
                    "sink": item.get("sink"),
                    "auction_type": item.get("auction_type"),
                    "clearing_price_usd_per_mw": self._safe_float(item.get("clearing_price")),
                    "awarded_quantity_mw": self._safe_float(item.get("awarded_quantity")),
                }
            }
            normalized_data.append(normalized_item)

        return normalized_data

    def transform_outages_data(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform outages data to normalized format."""
        normalized_data = []

        for item in raw_data:
            outage_id = item.get("outage_id")
            if not outage_id:
                continue
            normalized_item = {
                "event_id": f"ercot_outages_{outage_id}",
                "trace_id": "ercot_outages_transform",
                "schema_version": "1.0.0",
                "tenant_id": "default",
                "producer": "ingestion-ercot@v1.0.0",
                "occurred_at": self._parse_timestamp(item.get("start_time")),
                "ingested_at": int(datetime.now(timezone.utc).timestamp() * 1_000_000),
                "payload": {
                    "market": "ercot",
                    "data_type": "outages",
                    "outage_id": outage_id,
                    "unit_name": item.get("unit_name"),
                    "outage_type": item.get("outage_type"),
                    "start_time": item.get("start_time"),
                    "end_time": item.get("end_time"),
                    "capacity_mw": self._safe_float(item.get("capacity_mw")),
                    "reason": item.get("reason")
                }
            }
            normalized_data.append(normalized_item)

        return normalized_data

    def _parse_timestamp(self, timestamp_str: Optional[str]) -> int:
        """Parse timestamp string to microseconds since epoch."""
        if not timestamp_str:
            return int(datetime.now(timezone.utc).timestamp() * 1_000_000)

        try:
            # Handle various timestamp formats
            ts = timestamp_str.strip()
            if ts.endswith("Z"):
                ts = ts[:-1] + "+00:00"

            for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
                try:
                    dt = datetime.strptime(ts, fmt)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    break
                except ValueError:
                    continue
            else:
                dt = datetime.fromisoformat(ts)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)

            return int(dt.timestamp() * 1_000_000)
        except Exception:
            return int(datetime.now(timezone.utc).timestamp() * 1_000_000)

    @staticmethod
    def _safe_float(value: Optional[Any]) -> Optional[float]:
        if value in (None, "", "null"):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
