"""
NYISO data transformation utilities.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from ..base.exceptions import TransformationError


class NYISOTransformConfig(BaseModel):
    tenant_id: str = Field("default", description="Target tenant identifier")
    schema_version: str = Field("1.0.0", description="Schema version for emitted events")
    producer: str = Field("ingestion-nyiso@v1.0.0", description="Producer identifier included in envelopes")
    market: str = Field("nyiso", description="Normalized market identifier")


class NYISOTransform:
    """Convert NYISO datasets into ingestion events."""

    def __init__(self, config: Optional[NYISOTransformConfig] = None):
        self.config = config or NYISOTransformConfig()

    def transform(self, rows: List[Dict[str, Any]], data_type: str) -> List[Dict[str, Any]]:
        data_type = data_type.lower()
        if data_type in {"lbmp", "rt_lbmp"}:
            return self._transform_lbmp(rows, data_type)
        if data_type == "icap":
            return self._transform_icap(rows)
        if data_type == "tcc":
            return self._transform_tcc(rows)
        if data_type == "outages":
            return self._transform_outages(rows)
        raise TransformationError(f"Unsupported NYISO transformation for data_type={data_type}")

    def _transform_lbmp(self, rows: List[Dict[str, Any]], data_type: str) -> List[Dict[str, Any]]:
        events: List[Dict[str, Any]] = []
        for row in rows:
            timestamp = row.get("timestamp")
            node = row.get("node")
            if not timestamp or not node:
                continue

            payload = {
                "market": self.config.market,
                "data_type": data_type,
                "timestamp": timestamp,
                "node": node,
                "market_run": row.get("market_run"),
                "lbmp_usd_per_mwh": self._safe_float(row.get("lbmp")),
                "congestion_usd_per_mwh": self._safe_float(row.get("congestion")),
                "loss_usd_per_mwh": self._safe_float(row.get("losses")),
            }
            event_id = f"nyiso_{data_type}_{node}_{timestamp}"
            events.append(self._build_event(event_id, self._parse_timestamp(timestamp), payload))
        return events

    def _transform_icap(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        events: List[Dict[str, Any]] = []
        for row in rows:
            zone = row.get("capacity_zone")
            month = row.get("month")
            if not zone or not month:
                continue

            payload = {
                "market": self.config.market,
                "data_type": "icap",
                "capacity_zone": zone,
                "month": month,
                "icap_demand_curve_price_usd_per_kw_month": self._safe_float(row.get("demand_curve_price")),
                "capacity_obligations_mw": self._safe_float(row.get("capacity_obligations_mw")),
            }
            event_id = f"nyiso_icap_{zone}_{month}"
            events.append(self._build_event(event_id, self._parse_month(month), payload))
        return events

    def _transform_tcc(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        events: List[Dict[str, Any]] = []
        for row in rows:
            auction = row.get("auction")
            path = row.get("path")
            if not path:
                continue

            payload = {
                "market": self.config.market,
                "data_type": "tcc",
                "auction": auction,
                "auction_date": row.get("auction_date"),
                "path": path,
                "source": row.get("source"),
                "sink": row.get("sink"),
                "clearing_price_usd_per_mw": self._safe_float(row.get("clearing_price")),
                "awarded_quantity_mw": self._safe_float(row.get("awarded_mw")),
            }
            event_id = f"nyiso_tcc_{auction}_{path}"
            events.append(self._build_event(event_id, self._parse_date(row.get("auction_date")), payload))
        return events

    def _transform_outages(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        events: List[Dict[str, Any]] = []
        for row in rows:
            outage_id = row.get("outage_id")
            start = row.get("start_time")
            if not outage_id or not start:
                continue

            payload = {
                "market": self.config.market,
                "data_type": "outages",
                "outage_id": outage_id,
                "resource_name": row.get("resource_name"),
                "status": row.get("status"),
                "start_time": start,
                "end_time": row.get("end_time"),
                "derate_mw": self._safe_float(row.get("derate_mw")),
                "reason": row.get("reason"),
            }
            event_id = f"nyiso_outage_{outage_id}"
            events.append(self._build_event(event_id, self._parse_timestamp(start), payload))
        return events

    def _build_event(self, event_id: str, occurred_at: int, payload: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "event_id": event_id,
            "trace_id": f"{self.config.producer}:{payload.get('data_type', 'unknown')}",
            "schema_version": self.config.schema_version,
            "tenant_id": self.config.tenant_id,
            "producer": self.config.producer,
            "occurred_at": occurred_at,
            "ingested_at": int(datetime.now(timezone.utc).timestamp() * 1_000_000),
            "payload": payload,
        }

    @staticmethod
    def _safe_float(value: Any) -> Optional[float]:
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

    @staticmethod
    def _parse_timestamp(value: Optional[str]) -> int:
        if not value:
            return int(datetime.now(timezone.utc).timestamp() * 1_000_000)
        cleaned = value.strip()
        if cleaned.endswith("Z"):
            cleaned = cleaned[:-1] + "+00:00"
        if "+" not in cleaned[-6:] and "-" not in cleaned[-6:]:
            cleaned = f"{cleaned}+00:00"
        try:
            dt = datetime.fromisoformat(cleaned)
        except ValueError:
            try:
                dt = datetime.strptime(cleaned, "%Y-%m-%d %H:%M:%S")
                dt = dt.replace(tzinfo=timezone.utc)
            except ValueError:
                return int(datetime.now(timezone.utc).timestamp() * 1_000_000)
        return int(dt.astimezone(timezone.utc).timestamp() * 1_000_000)

    @staticmethod
    def _parse_month(value: Optional[str]) -> int:
        if not value:
            return int(datetime.now(timezone.utc).timestamp() * 1_000_000)
        try:
            dt = datetime.strptime(value, "%Y-%m")
            return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1_000_000)
        except ValueError:
            return int(datetime.now(timezone.utc).timestamp() * 1_000_000)

    @staticmethod
    def _parse_date(value: Optional[str]) -> int:
        if not value:
            return int(datetime.now(timezone.utc).timestamp() * 1_000_000)
        try:
            dt = datetime.strptime(value, "%Y-%m-%d")
            return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1_000_000)
        except ValueError:
            return int(datetime.now(timezone.utc).timestamp() * 1_000_000)
