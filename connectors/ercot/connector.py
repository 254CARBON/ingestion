"""
ERCOT connector implementation for DAM/RTM, ORDC, AS, CRR, and outages data.
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
from .config import ERCOTConnectorConfig
from .extractor import ERCOTExtractor
from .transform import ERCOTTransform, ERCOTTransformConfig


class ERCOTConnector(BaseConnector):
    """ERCOT market data connector using MIS/public datasets."""

    def __init__(self, config: ConnectorConfig):
        super().__init__(config)

        if not isinstance(config, ERCOTConnectorConfig):
            self.config = ERCOTConnectorConfig(**config.dict())  # type: ignore[arg-type]

        self._extractor = ERCOTExtractor(self.config)  # type: ignore[arg-type]
        self._transformer = ERCOTTransform(ERCOTTransformConfig())

        self.supported_data_types = ["dam", "rtm", "ordc", "as", "crr", "outages"]
        self.quality_metrics = {
            "total_requests": 0,
            "successful_requests": 0,
            "failed_requests": 0,
            "data_points_extracted": 0,
        }

    async def extract(self, **kwargs) -> ExtractionResult:
        """Extract ERCOT data for a requested data type."""
        try:
            data_type = (kwargs.get("data_type") or "dam").lower()
            if data_type not in self.supported_data_types:
                raise ExtractionError(f"Unsupported data type: {data_type}")

            self.quality_metrics["total_requests"] += 1

            end_default = datetime.now(timezone.utc)
            start_default = end_default - timedelta(hours=self.config.default_start_hours_back)
            start_time = self._coerce_datetime(kwargs.get("start_time") or kwargs.get("start"), start_default)
            end_time = self._coerce_datetime(kwargs.get("end_time") or kwargs.get("end"), end_default)

            raw_records = await self._extractor.extract_data(
                data_type=data_type,
                start_time=start_time,
                end_time=end_time,
            )

            extraction_metadata = {
                "data_type": data_type,
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "source": "ercot-mis",
            }

            result = ExtractionResult(
                data=raw_records,
                metadata=extraction_metadata,
                record_count=len(raw_records),
            )

            self.quality_metrics["successful_requests"] += 1
            self.quality_metrics["data_points_extracted"] += result.record_count

            return result
        except Exception as exc:
            self.quality_metrics["failed_requests"] += 1
            self.logger.error("ERCOT extraction failed", error=str(exc))
            if isinstance(exc, ExtractionError):
                raise
            raise ExtractionError(f"Extraction failed: {exc}") from exc

    async def transform(self, extraction_result: ExtractionResult, **kwargs) -> TransformationResult:
        """Transform ERCOT raw data into normalized ingestion events."""
        data_type = (kwargs.get("data_type") or extraction_result.metadata.get("data_type") or "dam").lower()

        try:
            if data_type == "dam":
                normalized = self._transformer.transform_lmp_data(extraction_result.data, "dam")
            elif data_type == "rtm":
                normalized = self._transformer.transform_lmp_data(extraction_result.data, "rtm")
            elif data_type == "ordc":
                normalized = self._transformer.transform_ordc_data(extraction_result.data)
            elif data_type == "as":
                normalized = self._transformer.transform_as_data(extraction_result.data)
            elif data_type == "crr":
                normalized = self._transformer.transform_crr_data(extraction_result.data)
            elif data_type == "outages":
                normalized = self._transformer.transform_outages_data(extraction_result.data)
            else:
                raise TransformationError(f"Unsupported transformation for data type: {data_type}")

            metadata = {
                "data_type": data_type,
                "transformed_count": len(normalized),
                "original_count": extraction_result.record_count,
            }

            return TransformationResult(
                data=normalized,
                metadata=metadata,
                record_count=len(normalized),
            )
        except Exception as exc:
            self.logger.error("ERCOT transformation failed", error=str(exc))
            if isinstance(exc, TransformationError):
                raise
            raise TransformationError(f"Transformation failed: {exc}") from exc

    async def extract_and_transform(self, **kwargs) -> TransformationResult:
        """Convenience helper that runs extraction then transformation."""
        extraction_result = await self.extract(**kwargs)
        return await self.transform(extraction_result, **kwargs)

    def get_connector_info(self) -> Dict[str, Any]:
        """Return connector metadata for diagnostics."""
        return {
            "name": self.config.name,
            "version": self.config.version,
            "market": self.config.market,
            "mode": self.config.mode,
            "supported_data_types": self.supported_data_types,
            "base_url": self.config.base_url,
            "transforms_enabled": len(self.config.transforms) if self.config.transforms else 0,
            "dataset_configs": list(self.config.dataset_configs.keys()),
            "kafka_bootstrap_servers": self.config.kafka_bootstrap_servers,
            "schema_registry_url": self.config.schema_registry_url,
        }

    def get_extraction_capabilities(self) -> Dict[str, Any]:
        """Describe extraction capabilities for orchestration tooling."""
        return {
            "data_types": self.supported_data_types,
            "modes": ["batch"],
            "formats": ["json", "csv"],
            "incremental": True,
            "rate_limits": {
                "requests_per_minute": self.config.requests_per_minute,
            },
            "batch_parameters": ["data_type", "start_time", "end_time"],
        }

    def get_metrics(self) -> Dict[str, Any]:
        """Expose extraction quality metrics."""
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

    async def cleanup(self) -> None:
        """Release resources held by extractor."""
        await self._extractor.close()

    @staticmethod
    def _coerce_datetime(value: Optional[Any], default: datetime) -> datetime:
        """Parse user-supplied start/end arguments."""
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
