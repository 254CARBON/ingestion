"""
AESO data transformation implementation.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import uuid4

from pydantic import BaseModel, Field

from ..base.exceptions import TransformationError


class AESOTransformConfig(BaseModel):
    """Transformation configuration for AESO datasets."""

    tenant_id: str = Field("default", description="Target tenant identifier")
    schema_version: str = Field("1.0.0", description="Envelope schema version")
    producer: str = Field("ingestion-aeso@v1.0.0", description="Producer identifier for emitted events")
    market: str = Field("aeso", description="Normalized market identifier")


class AESOTransform:
    """Transform normalized AESO rows into ingestion-ready events."""

    def __init__(self, config: Optional[AESOTransformConfig] = None):
        self.config = config or AESOTransformConfig()
        self._transformers = {
            "pool_price": self.transform_pool_price,
            "system_marginal_price": self.transform_system_marginal_price,
            "alberta_internal_load": self.transform_alberta_internal_load,
            "current_supply_demand_summary": self.transform_supply_demand_summary,
            "current_supply_demand_assets": self.transform_supply_demand_assets,
        }

    def transform(self, rows: List[Dict[str, Any]], data_type: str) -> List[Dict[str, Any]]:
        """Dispatch transformation based on data_type."""
        handler = self._transformers.get(data_type)
        if handler is None:
            raise TransformationError(f"Unsupported AESO data_type={data_type}")
        return handler(rows)

    def transform_pool_price(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        events: List[Dict[str, Any]] = []
        for row in rows:
            begin_utc = row.get("begin_datetime_utc")
            if not begin_utc:
                continue
            occurred_at = self._parse_timestamp(begin_utc)
            event_id = f"aeso_pool_price_{begin_utc.replace(' ', '_')}"
            payload = {
                "market": self.config.market,
                "data_type": "pool_price",
                "begin_datetime_utc": begin_utc,
                "begin_datetime_mpt": row.get("begin_datetime_mpt"),
                "pool_price_cad_per_mwh": self._safe_float(row.get("pool_price")),
                "forecast_pool_price_cad_per_mwh": self._safe_float(row.get("forecast_pool_price")),
                "rolling_30day_avg_cad_per_mwh": self._safe_float(row.get("rolling_30day_avg")),
            }
            events.append(self._build_event(event_id, occurred_at, payload))
        return events

    def transform_system_marginal_price(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        events: List[Dict[str, Any]] = []
        for row in rows:
            begin_utc = row.get("begin_datetime_utc")
            if not begin_utc:
                continue
            occurred_at = self._parse_timestamp(begin_utc)
            end_utc = row.get("end_datetime_utc")
            event_id = f"aeso_system_marginal_price_{begin_utc.replace(' ', '_')}"
            payload = {
                "market": self.config.market,
                "data_type": "system_marginal_price",
                "begin_datetime_utc": begin_utc,
                "end_datetime_utc": end_utc,
                "system_marginal_price_cad_per_mwh": self._safe_float(row.get("system_marginal_price")),
                "volume_mwh": self._safe_float(row.get("volume")),
            }
            events.append(self._build_event(event_id, occurred_at, payload))
        return events

    def transform_alberta_internal_load(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        events: List[Dict[str, Any]] = []
        for row in rows:
            begin_utc = row.get("begin_datetime_utc") or row.get("begin_date_time_utc")
            if not begin_utc:
                continue
            occurred_at = self._parse_timestamp(begin_utc)
            event_id = f"aeso_internal_load_{begin_utc.replace(' ', '_')}"
            payload = {
                "market": self.config.market,
                "data_type": "alberta_internal_load",
                "begin_datetime_utc": begin_utc,
                "begin_datetime_mpt": row.get("begin_datetime_mpt"),
                "alberta_internal_load_mw": self._safe_float(
                    row.get("alberta_internal_load") or row.get("alberta_internal_load_mw")
                ),
                "forecast_alberta_internal_load_mw": self._safe_float(
                    row.get("forecast_alberta_internal_load")
                ),
            }
            events.append(self._build_event(event_id, occurred_at, payload))
        return events

    def transform_supply_demand_summary(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        events: List[Dict[str, Any]] = []
        for row in rows:
            updated = row.get("last_updated_datetime_utc") or row.get("last_updated_datetime_mpt")
            occurred_at = self._parse_timestamp(updated) if updated else datetime.now(timezone.utc)
            event_id = f"aeso_csd_summary_{occurred_at.strftime('%Y%m%d%H%M')}"

            payload = {
                "market": self.config.market,
                "data_type": "current_supply_demand_summary",
                "last_updated_datetime_utc": row.get("last_updated_datetime_utc"),
                "last_updated_datetime_mpt": row.get("last_updated_datetime_mpt"),
                "total_max_generation_capability_mw": self._safe_int(row.get("total_max_generation_capability")),
                "total_net_generation_mw": self._safe_int(row.get("total_net_generation")),
                "net_to_grid_generation_mw": self._safe_int(row.get("net_to_grid_generation")),
                "net_actual_interchange_mw": self._safe_int(row.get("net_actual_interchange")),
                "alberta_internal_load_mw": self._safe_int(row.get("alberta_internal_load")),
                "contingency_reserve_required_mw": self._safe_int(row.get("contingency_reserve_required")),
                "dispatched_contingency_reserve_total_mw": self._safe_int(
                    row.get("dispatched_contigency_reserve_total")
                ),
                "dispatched_contingency_reserve_gen_mw": self._safe_int(
                    row.get("dispatched_contingency_reserve_gen")
                ),
                "dispatched_contingency_reserve_other_mw": self._safe_int(
                    row.get("dispatched_contingency_reserve_other")
                ),
                "lssi_armed_dispatched_mw": self._safe_int(row.get("lssi_armed_dispatched")),
                "lssi_offered_volume_mw": self._safe_int(row.get("lssi_offered_volume")),
                "generation_data": [
                    self._normalize_generation_entry(entry) for entry in row.get("generation_data_list", [])
                ],
                "interchange": [
                    self._normalize_interchange_entry(entry) for entry in row.get("interchange_list", [])
                ],
            }
            events.append(self._build_event(event_id, occurred_at, payload))
        return events

    def transform_supply_demand_assets(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        events: List[Dict[str, Any]] = []
        for snapshot in rows:
            updated = snapshot.get("last_updated_datetime_utc") or snapshot.get("last_updated_datetime_mpt")
            occurred_at = self._parse_timestamp(updated) if updated else datetime.now(timezone.utc)
            timestamp_component = occurred_at.strftime("%Y%m%d%H%M")
            for entry in snapshot.get("asset_list", []):
                asset_id = entry.get("asset") or entry.get("asset_id")
                if not asset_id:
                    continue
                event_id = f"aeso_csd_asset_{asset_id}_{timestamp_component}"
                payload = {
                    "market": self.config.market,
                    "data_type": "current_supply_demand_assets",
                    "snapshot_last_updated_utc": snapshot.get("last_updated_datetime_utc"),
                    "snapshot_last_updated_mpt": snapshot.get("last_updated_datetime_mpt"),
                    "asset_id": asset_id,
                    "fuel_type": entry.get("fuel_type"),
                    "sub_fuel_type": entry.get("sub_fuel_type"),
                    "maximum_capability_mw": self._safe_int(entry.get("maximum_capability")),
                    "net_generation_mw": self._safe_int(entry.get("net_generation")),
                    "dispatched_contingency_reserve_mw": self._safe_int(
                        entry.get("dispatched_contingency_reserve")
                    ),
                }
                events.append(self._build_event(event_id, occurred_at, payload))
        return events

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _build_event(self, event_id: str, occurred_at: datetime, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Construct the ingestion event envelope."""
        return {
            "event_id": event_id,
            "trace_id": str(uuid4()),
            "schema_version": self.config.schema_version,
            "tenant_id": self.config.tenant_id,
            "producer": self.config.producer,
            "occurred_at": occurred_at.astimezone(timezone.utc).isoformat(),
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            "payload": payload,
        }

    @staticmethod
    def _parse_timestamp(value: Optional[str]) -> datetime:
        """Parse AESO timestamp strings into timezone-aware datetimes."""
        if not value:
            raise TransformationError("Missing timestamp value in AESO payload")
        variants = [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%Y-%m-%d %H",
        ]
        for fmt in variants:
            try:
                return datetime.strptime(value, fmt).replace(tzinfo=timezone.utc)
            except ValueError:
                continue
        raise TransformationError(f"Unsupported timestamp format: {value}")

    @staticmethod
    def _safe_float(value: Any) -> Optional[float]:
        """Best-effort conversion to float."""
        if value in (None, "", "-", "null"):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _safe_int(value: Any) -> Optional[int]:
        """Best-effort conversion to int."""
        if value in (None, "", "-", "null"):
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            try:
                return int(float(value))
            except (TypeError, ValueError):
                return None

    def _normalize_generation_entry(self, entry: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize generation summary entries."""
        return {
            "fuel_type": entry.get("fuel_type"),
            "aggregated_maximum_capability_mw": self._safe_int(entry.get("aggregated_maximum_capability")),
            "aggregated_net_generation_mw": self._safe_int(entry.get("aggregated_net_generation")),
            "aggregated_dispatch_contingency_reserve_mw": self._safe_int(
                entry.get("aggregated_dispatch_contingency_reserve")
            ),
        }

    def _normalize_interchange_entry(self, entry: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize interchange flow entries."""
        return {
            "path": entry.get("path"),
            "actual_flow_mw": self._safe_int(entry.get("actual_flow")),
        }
