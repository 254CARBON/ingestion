"""
MISO data transformation implementation.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from ..base.exceptions import TransformationError


class MISOTransformConfig(BaseModel):
    """Transformation configuration for MISO datasets."""

    tenant_id: str = Field("default", description="Target tenant identifier")
    schema_version: str = Field("1.0.0", description="Envelope schema version")
    producer: str = Field("ingestion-miso@v1.0.0", description="Producer identifier for emitted events")
    market: str = Field("miso", description="Normalized market identifier")


class MISOTransform:
    """Transform normalized MISO rows into ingestion events."""

    def __init__(self, config: Optional[MISOTransformConfig] = None):
        self.config = config or MISOTransformConfig()

    def transform_lmp(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        events: List[Dict[str, Any]] = []
        for row in rows:
            timestamp = row.get("timestamp")
            node = row.get("node")
            if not timestamp or not node:
                continue

            occurred_at = self._parse_timestamp(timestamp)
            event_id = f"miso_lmp_{node}_{timestamp}"
            payload = {
                "market": self.config.market,
                "data_type": "lmp",
                "timestamp": timestamp,
                "node": node,
                "market_run": row.get("market_run"),
                "lmp_usd_per_mwh": self._safe_float(row.get("lmp")),
                "congestion_usd_per_mwh": self._safe_float(row.get("congestion")),
                "loss_usd_per_mwh": self._safe_float(row.get("loss")),
                "energy_usd_per_mwh": self._safe_float(row.get("energy")),
            }
            events.append(self._build_event(event_id, occurred_at, payload))
        return events

    def transform_as(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        events: List[Dict[str, Any]] = []
        for row in rows:
            timestamp = row.get("timestamp")
            product = row.get("product")
            if not timestamp or not product:
                continue

            occurred_at = self._parse_timestamp(timestamp)
            event_id = f"miso_as_{product}_{timestamp}"
            payload = {
                "market": self.config.market,
                "data_type": "as",
                "timestamp": timestamp,
                "product": product,
                "zone": row.get("zone"),
                "cleared_price_usd_per_mwh": self._safe_float(row.get("cleared_price")),
                "cleared_quantity_mw": self._safe_float(row.get("cleared_mw")),
            }
            events.append(self._build_event(event_id, occurred_at, payload))
        return events

    def transform_pra(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        events: List[Dict[str, Any]] = []
        for row in rows:
            auction = row.get("auction")
            zone = row.get("planning_zone")
            if not auction or not zone:
                continue

            auction_date = row.get("auction_date")
            occurred_at = self._parse_date_or_now(auction_date)
            event_id = f"miso_pra_{auction}_{zone}"
            payload = {
                "market": self.config.market,
                "data_type": "pra",
                "auction": auction,
                "auction_date": auction_date,
                "planning_zone": zone,
                "planning_year": row.get("planning_year"),
                "clearing_price_usd_per_mw_day": self._safe_float(row.get("clearing_price")),
                "awarded_quantity_mw": self._safe_float(row.get("awarded_mw")),
            }
            events.append(self._build_event(event_id, occurred_at, payload))
        return events

    def transform_arr(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        events: List[Dict[str, Any]] = []
        for row in rows:
            auction = row.get("auction")
            path = row.get("path")
            if not auction or not path:
                continue

            auction_date = row.get("auction_date")
            occurred_at = self._parse_date_or_now(auction_date)
            event_id = f"miso_arr_{auction}_{path}"
            payload = {
                "market": self.config.market,
                "data_type": "arr",
                "auction": auction,
                "auction_date": auction_date,
                "path": path,
                "source": row.get("source"),
                "sink": row.get("sink"),
                "class_type": row.get("class_type"),
                "clearing_price_usd_per_mw": self._safe_float(row.get("clearing_price")),
                "awarded_quantity_mw": self._safe_float(row.get("awarded_mw")),
            }
            events.append(self._build_event(event_id, occurred_at, payload))
        return events

    def transform_tcr(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        events: List[Dict[str, Any]] = []
        for row in rows:
            auction = row.get("auction")
            path = row.get("path")
            if not auction or not path:
                continue

            auction_date = row.get("auction_date")
            occurred_at = self._parse_date_or_now(auction_date)
            event_id = f"miso_tcr_{auction}_{path}"
            payload = {
                "market": self.config.market,
                "data_type": "tcr",
                "auction": auction,
                "auction_date": auction_date,
                "path": path,
                "source": row.get("source"),
                "sink": row.get("sink"),
                "class_type": row.get("class_type"),
                "round_id": row.get("round_id"),
                "clearing_price_usd_per_mw": self._safe_float(row.get("clearing_price")),
                "awarded_quantity_mw": self._safe_float(row.get("awarded_mw")),
            }
            events.append(self._build_event(event_id, occurred_at, payload))
        return events

    def transform(self, rows: List[Dict[str, Any]], data_type: str) -> List[Dict[str, Any]]:
        """Dispatch transformation based on data_type."""
        if data_type == "lmp":
            return self.transform_lmp(rows)
        if data_type == "as":
            return self.transform_as(rows)
        if data_type == "pra":
            return self.transform_pra(rows)
        if data_type == "arr":
            return self.transform_arr(rows)
        if data_type == "tcr":
            return self.transform_tcr(rows)
        raise TransformationError(f"Unsupported transformation for data_type={data_type}")

    # ------------------------------------------------------------------
    # Helper utilities
    # ------------------------------------------------------------------

    def _build_event(self, event_id: str, occurred_at: int, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Construct the ingestion event envelope."""
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
    def _parse_timestamp(timestamp_str: Optional[str]) -> int:
        """Parse ISO timestamps to microseconds since epoch."""
        if not timestamp_str:
            return int(datetime.now(timezone.utc).timestamp() * 1_000_000)

        try:
            cleaned = timestamp_str.strip()
            if cleaned.endswith("Z"):
                cleaned = cleaned.replace("Z", "+00:00")
            if "+" not in cleaned[-6:] and "-" not in cleaned[-6:]:
                cleaned = f"{cleaned}+00:00"
            dt = datetime.fromisoformat(cleaned)
            return int(dt.astimezone(timezone.utc).timestamp() * 1_000_000)
        except ValueError:
            return int(datetime.now(timezone.utc).timestamp() * 1_000_000)

    @staticmethod
    def _parse_date_or_now(date_str: Optional[str]) -> int:
        """Parse date strings (YYYY-MM-DD) or fallback to current time."""
        if not date_str:
            return int(datetime.now(timezone.utc).timestamp() * 1_000_000)

        patterns = ["%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y"]
        for pattern in patterns:
            try:
                dt = datetime.strptime(date_str, pattern)
                dt = dt.replace(tzinfo=timezone.utc)
                return int(dt.timestamp() * 1_000_000)
            except ValueError:
                continue

        # Attempt ISO parsing as a last resort
        try:
            cleaned = date_str.strip()
            if cleaned.endswith("Z"):
                cleaned = cleaned.replace("Z", "+00:00")
            if "+" not in cleaned[-6:] and "-" not in cleaned[-6:]:
                cleaned = f"{cleaned}+00:00"
            dt = datetime.fromisoformat(cleaned)
            return int(dt.astimezone(timezone.utc).timestamp() * 1_000_000)
        except ValueError:
            return int(datetime.now(timezone.utc).timestamp() * 1_000_000)
