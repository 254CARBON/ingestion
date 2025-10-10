"""
PJM connector implementation using Data Miner API.
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
from .config import PJMConnectorConfig
from .extractor import PJMExtractor
from .transform import PJMTransform, PJMTransformConfig


class PJMConnector(BaseConnector):
    """PJM market data connector using Data Miner API."""

    def __init__(self, config: ConnectorConfig):
        super().__init__(config)
        # Promote to specialized config if needed
        if not isinstance(config, PJMConnectorConfig):
            self.config = PJMConnectorConfig(**config.dict())  # type: ignore[arg-type]
        # Components
        self._extractor = PJMExtractor(self.config)  # type: ignore[arg-type]
        self._transformer = PJMTransform(PJMTransformConfig())

        # Supported data types
        self.supported_data_types = ["lmp", "rtm_lmp", "rpm", "tcr", "outages"]

    async def extract(self, **kwargs) -> ExtractionResult:
        """Extract data from PJM Data Miner."""
        try:
            self.logger.info("Starting PJM data extraction")

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
            if data_type in ["lmp", "rtm_lmp"]:
                raw_data = await self._extract_lmp_data(data_type, start_time, end_time)
            elif data_type == "rpm":
                raw_data = await self._extract_rpm_data(start_time, end_time)
            elif data_type == "tcr":
                raw_data = await self._extract_tcr_data(start_time, end_time)
            elif data_type == "outages":
                raw_data = await self._extract_outages_data(start_time, end_time)
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
            self.logger.error("PJM extraction failed", error=str(e))
            raise ExtractionError(f"Extraction failed: {str(e)}")

    async def _extract_lmp_data(self, data_type: str, start_time: datetime, end_time: datetime) -> List[Dict[str, Any]]:
        """Extract LMP data (DAM or RTM)."""
        try:
            return await self._extractor.extract_lmp_data(data_type, start_time, end_time)
        except Exception as e:
            raise ExtractionError(f"LMP extraction failed: {str(e)}")

    async def _extract_rpm_data(self, start_time: datetime, end_time: datetime) -> List[Dict[str, Any]]:
        """Extract RPM auction results."""
        try:
            return await self._extractor.extract_rpm_data(start_time, end_time)
        except Exception as e:
            raise ExtractionError(f"RPM extraction failed: {str(e)}")

    async def _extract_tcr_data(self, start_time: datetime, end_time: datetime) -> List[Dict[str, Any]]:
        """Extract TCR auction results."""
        try:
            return await self._extractor.extract_tcr_data(start_time, end_time)
        except Exception as e:
            raise ExtractionError(f"TCR extraction failed: {str(e)}")

    async def _extract_outages_data(self, start_time: datetime, end_time: datetime) -> List[Dict[str, Any]]:
        """Extract generation outages data."""
        try:
            return await self._extractor.extract_outages_data(start_time, end_time)
        except Exception as e:
            raise ExtractionError(f"Outages extraction failed: {str(e)}")

    async def transform(self, extraction_result: ExtractionResult, **kwargs) -> TransformationResult:
        """Transform raw PJM data to normalized format."""
        try:
            self.logger.info("Starting PJM data transformation")

            data_type = extraction_result.metadata.get("data_type", "lmp")

            # Transform based on data type
            if data_type in ["lmp", "rtm_lmp"]:
                normalized_data = self._transformer.transform_lmp_data(
                    extraction_result.data,
                    data_type
                )
            elif data_type == "rpm":
                normalized_data = self._transformer.transform_rpm_data(
                    extraction_result.data
                )
            elif data_type == "tcr":
                normalized_data = self._transformer.transform_tcr_data(
                    extraction_result.data
                )
            elif data_type == "outages":
                normalized_data = self._transformer.transform_outages_data(
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
            self.logger.error("PJM transformation failed", error=str(e))
            raise TransformationError(f"Transformation failed: {str(e)}")

    async def extract_and_transform(self, **kwargs) -> TransformationResult:
        """Extract and transform PJM data in one operation."""
        extraction_result = await self.extract(**kwargs)
        return await self.transform(extraction_result, **kwargs)
