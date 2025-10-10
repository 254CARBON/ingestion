"""
MISO connector implementation.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from ..base.base_connector import (
    BaseConnector,
    ConnectorConfig,
    ExtractionResult,
    TransformationResult,
)
from ..base.exceptions import ExtractionError, TransformationError
from .config import MISOConnectorConfig
from .extractor import MISOExtractor
from .transform import MISOTransform, MISOTransformConfig


class MISOConnector(BaseConnector):
    """Connector for MISO Data Exchange datasets."""

    def __init__(self, config: ConnectorConfig):
        super().__init__(config)
        if not isinstance(config, MISOConnectorConfig):
            self.config = MISOConnectorConfig(**config.dict())  # type: ignore[arg-type]

        self._extractor = MISOExtractor(self.config)  # type: ignore[arg-type]
        self._transformer = MISOTransform(MISOTransformConfig())

        self.supported_data_types = ["lmp", "as", "pra", "arr", "tcr"]
        self.quality_metrics = {
            "total_requests": 0,
            "successful_requests": 0,
            "failed_requests": 0,
            "data_points_extracted": 0,
        }

    async def extract(self, **kwargs) -> ExtractionResult:
        """Extract raw data for the requested data type."""
        data_type = (kwargs.get("data_type") or "lmp").lower()
        if data_type not in self.supported_data_types:
            raise ExtractionError(f"Unsupported data type: {data_type}")

        self.quality_metrics["total_requests"] += 1

        lookback_hours = int(kwargs.get("lookback_hours") or self.config.default_start_hours_back)
        end_default = datetime.now(timezone.utc)
        start_default = end_default - timedelta(hours=lookback_hours)

        start_time = self._coerce_datetime(kwargs.get("start_time") or kwargs.get("start"), start_default)
        end_time = self._coerce_datetime(kwargs.get("end_time") or kwargs.get("end"), end_default)

        try:
            rows = await self._extractor.extract_data(
                data_type=data_type,
                start_time=start_time,
                end_time=end_time,
            )
        except Exception as exc:
            self.quality_metrics["failed_requests"] += 1
            if isinstance(exc, ExtractionError):
                raise
            raise ExtractionError(f"MISO extraction failed: {exc}") from exc

        self.quality_metrics["successful_requests"] += 1
        self.quality_metrics["data_points_extracted"] += len(rows)

        metadata = {
            "data_type": data_type,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "source": "miso-data-exchange",
        }

        return ExtractionResult(
            data=rows,
            metadata=metadata,
            record_count=len(rows),
        )

    async def transform(self, extraction_result: ExtractionResult, **kwargs) -> TransformationResult:
        """Transform extracted rows into ingestion events."""
        data_type = (kwargs.get("data_type") or extraction_result.metadata.get("data_type") or "lmp").lower()
        try:
            transformed = self._transformer.transform(extraction_result.data, data_type)
        except Exception as exc:
            if isinstance(exc, TransformationError):
                raise
            raise TransformationError(f"MISO transformation failed: {exc}") from exc

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
            "base_url": self.config.miso_base_url,
            "dataset_count": len(self.config.dataset_configs),
            "transforms_enabled": len(self.config.transforms) if self.config.transforms else 0,
        }

    def get_extraction_capabilities(self) -> Dict[str, Any]:
        """Describe extraction capabilities."""
        return {
            "data_types": self.supported_data_types,
            "modes": ["batch"],
            "formats": ["json", "csv"],
            "incremental": True,
            "rate_limits": {
                "requests_per_minute": self.config.miso_rate_limit,
            },
            "batch_parameters": ["data_type", "start_time", "end_time"],
        }

    def get_metrics(self) -> Dict[str, Any]:
        """Expose extraction metrics."""
        metrics = super().get_metrics()
        metrics.update(
            {
                "quality_metrics": self.quality_metrics,
                "success_rate": (
                    self.quality_metrics["successful_requests"]
                    / max(1, self.quality_metrics["total_requests"])
                ),
                "data_extraction_rate": self.quality_metrics["data_points_extracted"],
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
