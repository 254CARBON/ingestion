"""
NYISO connector implementation using OASIS and REST APIs.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict

from ..base.base_connector import (
    BaseConnector,
    ConnectorConfig,
    ExtractionResult,
    TransformationResult,
)
from ..base.exceptions import ExtractionError, TransformationError
from .config import NYISOConnectorConfig
from .extractor import NYISOExtractor
from .transform import NYISOTransform, NYISOTransformConfig


class NYISOConnector(BaseConnector):
    """Connector for NYISO market datasets including generator outages."""

    def __init__(self, config: ConnectorConfig):
        super().__init__(config)
        if not isinstance(config, NYISOConnectorConfig):
            self.config = NYISOConnectorConfig(**config.dict())  # type: ignore[arg-type]

        self._extractor = NYISOExtractor(self.config)  # type: ignore[arg-type]
        self._transformer = NYISOTransform(NYISOTransformConfig())

        self.supported_data_types = ["lbmp", "rt_lbmp", "icap", "tcc", "outages"]
        self.quality_metrics = {
            "total_requests": 0,
            "successful_requests": 0,
            "failed_requests": 0,
            "data_points_extracted": 0,
        }

    async def extract(self, **kwargs: Any) -> ExtractionResult:
        data_type = (kwargs.get("data_type") or "lbmp").lower()
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
            raise ExtractionError(f"NYISO extraction failed: {exc}") from exc

        self.quality_metrics["successful_requests"] += 1
        self.quality_metrics["data_points_extracted"] += len(rows)

        metadata = {
            "data_type": data_type,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "source": "nyiso-oasis",
        }

        return ExtractionResult(
            data=rows,
            metadata=metadata,
            record_count=len(rows),
        )

    async def transform(self, extraction_result: ExtractionResult, **kwargs: Any) -> TransformationResult:
        data_type = (
            kwargs.get("data_type")
            or extraction_result.metadata.get("data_type")
            or "lbmp"
        ).lower()
        try:
            transformed = self._transformer.transform(extraction_result.data, data_type)
        except Exception as exc:
            if isinstance(exc, TransformationError):
                raise
            raise TransformationError(f"NYISO transformation failed: {exc}") from exc

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

    async def extract_and_transform(self, **kwargs: Any) -> TransformationResult:
        extraction = await self.extract(**kwargs)
        return await self.transform(extraction, **kwargs)

    async def cleanup(self) -> None:
        await self._extractor.close()

    def get_connector_info(self) -> Dict[str, Any]:
        return {
            "name": self.config.name,
            "version": self.config.version,
            "market": self.config.market,
            "mode": self.config.mode,
            "supported_data_types": self.supported_data_types,
            "base_urls": {
                "oasis": self.config.oasis_base_url,
                "rest": self.config.rest_base_url,
            },
        }

    def get_metrics(self) -> Dict[str, Any]:
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
    def _coerce_datetime(value: Any, default: datetime) -> datetime:
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
