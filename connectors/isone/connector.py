"""
ISO-NE connector implementation using web services API.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from ..base.base_connector import (
    BaseConnector,
    ConnectorConfig,
    ExtractionResult,
    TransformationResult,
)
from ..base.exceptions import ExtractionError, TransformationError
from .config import ISONEConnectorConfig
from .extractor import ISONEExtractor
from .transform import ISONETransform, ISONETransformConfig


class ISONEConnector(BaseConnector):
    """ISO-NE market data connector using web services API."""

    def __init__(self, config: ConnectorConfig):
        super().__init__(config)
        # Promote to specialized config if needed
        if not isinstance(config, ISONEConnectorConfig):
            self.config = ISONEConnectorConfig(**config.dict())  # type: ignore[arg-type]
        # Components
        self._extractor = ISONEExtractor(self.config)  # type: ignore[arg-type]
        self._transformer = ISONETransform(ISONETransformConfig())

        # Supported data types
        self.supported_data_types = ["lmp", "rt_lmp", "fcm", "ftr"]

    async def extract(self, **kwargs) -> ExtractionResult:
        """Extract data from ISO-NE web services."""
        try:
            self.logger.info("Starting ISO-NE data extraction")

            # Resolve extraction parameters
            data_type = kwargs.get("data_type", "lmp")
            if data_type not in self.supported_data_types:
                raise ExtractionError(f"Unsupported data type: {data_type}")

            start_time = kwargs.get("start_time")
            end_time = kwargs.get("end_time")

            if not start_time:
                start_time = datetime.now(timezone.utc) - timedelta(hours=self.config.default_start_hours_back)
            if not end_time:
                end_time = datetime.now(timezone.utc)

            # Extract data based on type
            if data_type in ["lmp", "rt_lmp"]:
                raw_data = await self._extract_lmp_data(data_type, start_time, end_time)
            elif data_type == "fcm":
                raw_data = await self._extract_fcm_data(start_time, end_time)
            elif data_type == "ftr":
                raw_data = await self._extract_ftr_data(start_time, end_time)
            else:
                raise ExtractionError(f"Unsupported data type: {data_type}")

            return ExtractionResult(
                success=True,
                data=raw_data,
                metadata={
                    "data_type": data_type,
                    "start_time": start_time.isoformat(),
                    "end_time": end_time.isoformat(),
                    "record_count": len(raw_data) if isinstance(raw_data, list) else 1
                }
            )

        except Exception as e:
            self.logger.error("ISO-NE extraction failed", error=str(e))
            raise ExtractionError(f"Extraction failed: {str(e)}")

    async def _extract_lmp_data(self, data_type: str, start_time: datetime, end_time: datetime) -> List[Dict[str, Any]]:
        """Extract LMP data (DAM or RT)."""
        try:
            return await self._extractor.extract_lmp_data(data_type, start_time, end_time)
        except Exception as e:
            raise ExtractionError(f"LMP extraction failed: {str(e)}")

    async def _extract_fcm_data(self, start_time: datetime, end_time: datetime) -> List[Dict[str, Any]]:
        """Extract FCM auction results."""
        try:
            return await self._extractor.extract_fcm_data(start_time, end_time)
        except Exception as e:
            raise ExtractionError(f"FCM extraction failed: {str(e)}")

    async def _extract_ftr_data(self, start_time: datetime, end_time: datetime) -> List[Dict[str, Any]]:
        """Extract FTR auction results."""
        try:
            return await self._extractor.extract_ftr_data(start_time, end_time)
        except Exception as e:
            raise ExtractionError(f"FTR extraction failed: {str(e)}")

    async def transform(self, extraction_result: ExtractionResult, **kwargs) -> TransformationResult:
        """Transform raw ISO-NE data to normalized format."""
        try:
            self.logger.info("Starting ISO-NE data transformation")

            data_type = extraction_result.metadata.get("data_type", "lmp")

            # Transform based on data type
            if data_type in ["lmp", "rt_lmp"]:
                normalized_data = self._transformer.transform_lmp_data(
                    extraction_result.data,
                    data_type
                )
            elif data_type == "fcm":
                normalized_data = self._transformer.transform_fcm_data(
                    extraction_result.data
                )
            elif data_type == "ftr":
                normalized_data = self._transformer.transform_ftr_data(
                    extraction_result.data
                )
            else:
                raise TransformationError(f"Unsupported transformation for data type: {data_type}")

            return TransformationResult(
                success=True,
                data=normalized_data,
                metadata={
                    "data_type": data_type,
                    "transformed_count": len(normalized_data),
                    "original_count": extraction_result.metadata.get("record_count", 0)
                }
            )

        except Exception as e:
            self.logger.error("ISO-NE transformation failed", error=str(e))
            raise TransformationError(f"Transformation failed: {str(e)}")

    async def extract_and_transform(self, **kwargs) -> TransformationResult:
        """Extract and transform ISO-NE data in one operation."""
        extraction_result = await self.extract(**kwargs)
        return await self.transform(extraction_result, **kwargs)
