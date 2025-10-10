"""
AESO connector implementation.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Sequence

from ..base.base_connector import (
    BaseConnector,
    ConnectorConfig,
    ExtractionResult,
    TransformationResult,
)
from ..base.exceptions import ExtractionError, TransformationError
from .config import AESOConnectorConfig
from .extractor import AESOExtractor
from .transform import AESOTransform, AESOTransformConfig


class AESOConnector(BaseConnector):
    """Connector for AESO market reporting APIs."""

    def __init__(self, config: ConnectorConfig):
        super().__init__(config)
        if not isinstance(config, AESOConnectorConfig):
            self.config = AESOConnectorConfig(**config.dict())  # type: ignore[arg-type]

        self._extractor = AESOExtractor(self.config)  # type: ignore[arg-type]
        self._transformer = AESOTransform(AESOTransformConfig())

        self.supported_data_types = [
            "pool_price",
            "system_marginal_price",
            "alberta_internal_load",
            "current_supply_demand_summary",
            "current_supply_demand_assets",
        ]

        self.metrics_snapshot = {
            "requests_total": 0,
            "requests_succeeded": 0,
            "requests_failed": 0,
            "records_extracted": 0,
        }

    async def extract(self, **kwargs) -> ExtractionResult:
        """Extract raw data for the requested data type."""
        data_type = (kwargs.get("data_type") or "pool_price").lower()
        if data_type not in self.supported_data_types:
            raise ExtractionError(f"Unsupported data type: {data_type}")

        self.metrics_snapshot["requests_total"] += 1

        requires_time_window = data_type in {
            "pool_price",
            "system_marginal_price",
            "alberta_internal_load",
        }

        lookback_hours = int(kwargs.get("lookback_hours") or self.config.default_start_hours_back)
        end_default = datetime.now(timezone.utc)
        start_default = end_default - timedelta(hours=lookback_hours)

        start_time = None
        end_time = None
        if requires_time_window:
            start_time = self._coerce_datetime(kwargs.get("start_time") or kwargs.get("start"), start_default)
            end_time = self._coerce_datetime(kwargs.get("end_time") or kwargs.get("end"), end_default)
            if start_time > end_time:
                raise ExtractionError("start_time cannot be later than end_time")

        time_granularity = kwargs.get("time_granularity") or kwargs.get("granularity")
        asset_ids: Optional[Sequence[str]] = kwargs.get("asset_ids")

        try:
            rows = await self._extractor.extract_data(
                data_type=data_type,
                start_time=start_time,
                end_time=end_time,
                time_granularity=time_granularity,
                asset_ids=asset_ids,
            )
        except Exception as exc:
            self.metrics_snapshot["requests_failed"] += 1
            if isinstance(exc, ExtractionError):
                raise
            raise ExtractionError(f"AESO extraction failed: {exc}") from exc

        self.metrics_snapshot["requests_succeeded"] += 1
        self.metrics_snapshot["records_extracted"] += len(rows)

        metadata = {
            "data_type": data_type,
            "start_time": start_time.isoformat() if start_time else None,
            "end_time": end_time.isoformat() if end_time else None,
            "source": "aeso-reporting-api",
            "record_count": len(rows),
        }

        return ExtractionResult(
            data=rows,
            metadata=metadata,
            record_count=len(rows),
        )

    async def transform(self, extraction_result: ExtractionResult, **kwargs) -> TransformationResult:
        """Transform extracted rows into ingestion events."""
        data_type = (kwargs.get("data_type") or extraction_result.metadata.get("data_type") or "pool_price").lower()
        try:
            transformed = self._transformer.transform(extraction_result.data, data_type)
        except Exception as exc:
            if isinstance(exc, TransformationError):
                raise
            raise TransformationError(f"AESO transformation failed: {exc}") from exc

        metadata = {
            "data_type": data_type,
            "transformed_count": len(transformed),
            "original_count": extraction_result.record_count,
        }

        return TransformationResult(
            data=transformed,
            metadata=metadata,
            record_count=len(transformed),
        )

    async def extract_and_transform(self, **kwargs) -> TransformationResult:
        """Convenience helper to run extraction and transformation in sequence."""
        extraction_result = await self.extract(**kwargs)
        return await self.transform(extraction_result, **kwargs)

    async def cleanup(self) -> None:
        """Release extractor resources."""
        await self._extractor.close()

    def get_connector_info(self) -> Dict[str, Any]:
        """Connector metadata for orchestration tooling."""
        return {
            "name": self.config.name,
            "version": self.config.version,
            "market": self.config.market,
            "mode": self.config.mode,
            "supported_data_types": self.supported_data_types,
            "base_url": self.config.aeso_base_url,
            "dataset_count": len(self.config.dataset_configs),
        }

    def get_extraction_capabilities(self) -> Dict[str, Any]:
        """Describe extraction capabilities."""
        return {
            "data_types": self.supported_data_types,
            "modes": ["batch"],
            "formats": ["json"],
            "incremental": True,
            "rate_limits": {
                "requests_per_minute": self.config.aeso_rate_limit,
            },
            "batch_parameters": ["data_type", "start_time", "end_time", "asset_ids"],
        }

    def get_metrics(self) -> Dict[str, Any]:
        """Expose extraction metrics."""
        metrics = super().get_metrics()
        metrics.update(
            {
                "requests_total": self.metrics_snapshot["requests_total"],
                "requests_succeeded": self.metrics_snapshot["requests_succeeded"],
                "requests_failed": self.metrics_snapshot["requests_failed"],
                "success_rate": (
                    self.metrics_snapshot["requests_succeeded"]
                    / max(1, self.metrics_snapshot["requests_total"])
                ),
                "records_extracted": self.metrics_snapshot["records_extracted"],
            }
        )
        return metrics

    @staticmethod
    def _coerce_datetime(value: Optional[Any], default: datetime) -> datetime:
        """Parse user-supplied start/end arguments into timezone-aware datetimes."""
        if value is None:
            return default
        if isinstance(value, datetime):
            return value.astimezone(timezone.utc)
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(value, tz=timezone.utc)
        if isinstance(value, str):
            cleaned = value.strip()
            if not cleaned:
                return default
            if cleaned.endswith("Z"):
                cleaned = cleaned[:-1] + "+00:00"
            if "+" not in cleaned[-6:] and "-" not in cleaned[-6:]:
                cleaned = f"{cleaned}+00:00"
            return datetime.fromisoformat(cleaned).astimezone(timezone.utc)
        raise ValueError(f"Unsupported datetime value: {value!r}")
