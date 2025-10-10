"""
PJM data transformation implementation.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from ..base.exceptions import TransformationError


class PJMTransformConfig(BaseModel):
    """Configuration for PJM normalization and event envelopes."""

    tenant_id: str = Field("default", description="Target tenant identifier")
    schema_version: str = Field("1.0.0", description="Schema version for emitted events")
    producer: str = Field("ingestion-pjm@v1.0.0", description="Producer identifier included in envelopes")
    market: str = Field("pjm", description="Normalized market identifier")


class PJMTransform:
    """Convert PJM datasets into ingestion event envelopes."""

    def __init__(self, config: Optional[PJMTransformConfig] = None):
        self.config = config or PJMTransformConfig()

    def transform(self, rows: List[Dict[str, Any]], data_type: str) -> List[Dict[str, Any]]:
        data_type = data_type.lower()
        if data_type in {"lmp", "rtm_lmp"}:
            return self._transform_lmp(rows, data_type)
        if data_type == "rpm":
            return self._transform_rpm(rows)
        if data_type == "tcr":
            return self._transform_tcr(rows)
        if data_type == "outages":
            return self._transform_outages(rows)
        raise TransformationError(f"Unsupported PJM transformation for data_type={data_type}")

    # ------------------------------------------------------------------
    # Dataset transformations
    # ------------------------------------------------------------------

    def _transform_lmp(self, rows: List[Dict[str, Any]], data_type: str) -> List[Dict[str, Any]]:
        events: List[Dict[str, Any]] = []
        for row in rows:
            timestamp = row.get("timestamp")
            node = row.get("node")
            if not timestamp or not node:
                continue

            event_id = f"pjm_{data_type}_{node}_{timestamp}"
            payload = {
                "market": self.config.market,
                "data_type": data_type,
                "timestamp": timestamp,
                "node": node,
                "market_run": row.get("market_run"),
                "lmp_usd_per_mwh": self._safe_float(row.get("lmp")),
                "congestion_usd_per_mwh": self._safe_float(row.get("congestion")),
                "loss_usd_per_mwh": self._safe_float(row.get("loss")),
            }
            events.append(self._build_event(event_id, self._parse_timestamp(timestamp), payload))
        return events

    def _transform_rpm(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        events: List[Dict[str, Any]] = []
        for row in rows:
            auction = row.get("auction")
            lda = row.get("lda")
            if not auction or not lda:
                continue

            event_id = f"pjm_rpm_{auction}_{lda}"
            payload = {
                "market": self.config.market,
                "data_type": "rpm",
                "auction": auction,
                "auction_date": row.get("auction_date"),
                "lda": lda,
                "clearing_price_usd_per_mw_day": self._safe_float(row.get("clearing_price")),
                "capacity_obligations_mw": self._safe_float(row.get("capacity_obligations_mw")),
                "auction_type": row.get("auction_type"),
            }
            occurred_at = self._parse_date_or_now(row.get("auction_date"))
            events.append(self._build_event(event_id, occurred_at, payload))
        return events

    def _transform_tcr(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        events: List[Dict[str, Any]] = []
        for row in rows:
            auction = row.get("auction")
            path = row.get("path")
            if not auction or not path:
                continue

            event_id = f"pjm_tcr_{auction}_{path}"
            payload = {
                "market": self.config.market,
                "data_type": "tcr",
                "auction": auction,
                "auction_date": row.get("auction_date"),
                "path": path,
                "source": row.get("source"),
                "sink": row.get("sink"),
                "round_id": row.get("round_id"),
                "clearing_price_usd_per_mw": self._safe_float(row.get("clearing_price")),
                "awarded_quantity_mw": self._safe_float(row.get("awarded_mw")),
            }
            occurred_at = self._parse_date_or_now(row.get("auction_date"))
            events.append(self._build_event(event_id, occurred_at, payload))
        return events

    def _transform_outages(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        events: List[Dict[str, Any]] = []
        for row in rows:
            outage_id = row.get("outage_id")
            start = row.get("start_time")
            if not outage_id or not start:
                continue

            event_id = f"pjm_outage_{outage_id}"
            payload = {
                "market": self.config.market,
                "data_type": "outages",
                "outage_id": outage_id,
                "resource_name": row.get("resource_name"),
                "status": row.get("status"),
                "outage_class": row.get("outage_class"),
                "start_time": start,
                "end_time": row.get("end_time"),
                "capacity_mw": self._safe_float(row.get("capacity_mw")),
                "reason": row.get("reason"),
            }
            events.append(self._build_event(event_id, self._parse_timestamp(start), payload))
        return events

    # ------------------------------------------------------------------
    # Helper utilities
    # ------------------------------------------------------------------

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
    def _parse_date_or_now(value: Optional[str]) -> int:
        if not value:
            return int(datetime.now(timezone.utc).timestamp() * 1_000_000)
        try:
            dt = datetime.strptime(value, "%Y-%m-%d")
            return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1_000_000)
        except ValueError:
            return int(datetime.now(timezone.utc).timestamp() * 1_000_000)
