"""
NYISO connector implementation using OASIS API.
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
from .config import NYISOConnectorConfig
from .extractor import NYISOExtractor
from .transform import NYISOTransform, NYISOTransformConfig


class NYISOConnector(BaseConnector):
    """NYISO market data connector using OASIS API."""

    def __init__(self, config: ConnectorConfig):
        super().__init__(config)
        # Promote to specialized config if needed
        if not isinstance(config, NYISOConnectorConfig):
            self.config = NYISOConnectorConfig(**config.dict())  # type: ignore[arg-type]
        # Components
        self._extractor = NYISOExtractor(self.config)  # type: ignore[arg-type]
        self._transformer = NYISOTransform(NYISOTransformConfig())

        # Supported data types
        self.supported_data_types = ["lbmp", "rt_lbmp", "icap", "tcc"]

    async def extract(self, **kwargs) -> ExtractionResult:
        """Extract data from NYISO OASIS."""
        try:
            self.logger.info("Starting NYISO data extraction")

            # Resolve extraction parameters
            data_type = kwargs.get("data_type", "lbmp")
            if data_type not in self.supported_data_types:
                raise ExtractionError(f"Unsupported data type: {data_type}")

            start_time = kwargs.get("start_time")
            end_time = kwargs.get("end_time")

            if not start_time:
                start_time = datetime.now(timezone.utc) - timedelta(hours=self.config.default_start_hours_back)
            if not end_time:
                end_time = datetime.now(timezone.utc)

            # Extract data based on type
            if data_type in ["lbmp", "rt_lbmp"]:
                raw_data = await self._extract_lbmp_data(data_type, start_time, end_time)
            elif data_type == "icap":
                raw_data = await self._extract_icap_data(start_time, end_time)
            elif data_type == "tcc":
                raw_data = await self._extract_tcc_data(start_time, end_time)
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
            self.logger.error("NYISO extraction failed", error=str(e))
            raise ExtractionError(f"Extraction failed: {str(e)}")

    async def _extract_lbmp_data(self, data_type: str, start_time: datetime, end_time: datetime) -> List[Dict[str, Any]]:
        """Extract LBMP data (DAM or RT)."""
        try:
            return await self._extractor.extract_lbmp_data(data_type, start_time, end_time)
        except Exception as e:
            raise ExtractionError(f"LBMP extraction failed: {str(e)}")

    async def _extract_icap_data(self, start_time: datetime, end_time: datetime) -> List[Dict[str, Any]]:
        """Extract ICAP demand curve data."""
        try:
            return await self._extractor.extract_icap_data(start_time, end_time)
        except Exception as e:
            raise ExtractionError(f"ICAP extraction failed: {str(e)}")

    async def _extract_tcc_data(self, start_time: datetime, end_time: datetime) -> List[Dict[str, Any]]:
        """Extract TCC auction results."""
        try:
            return await self._extractor.extract_tcc_data(start_time, end_time)
        except Exception as e:
            raise ExtractionError(f"TCC extraction failed: {str(e)}")

    async def transform(self, extraction_result: ExtractionResult, **kwargs) -> TransformationResult:
        """Transform raw NYISO data to normalized format."""
        try:
            self.logger.info("Starting NYISO data transformation")

            data_type = extraction_result.metadata.get("data_type", "lbmp")

            # Transform based on data type
            if data_type in ["lbmp", "rt_lbmp"]:
                normalized_data = self._transformer.transform_lbmp_data(
                    extraction_result.data,
                    data_type
                )
            elif data_type == "icap":
                normalized_data = self._transformer.transform_icap_data(
                    extraction_result.data
                )
            elif data_type == "tcc":
                normalized_data = self._transformer.transform_tcc_data(
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
            self.logger.error("NYISO transformation failed", error=str(e))
            raise TransformationError(f"Transformation failed: {str(e)}")

    async def extract_and_transform(self, **kwargs) -> TransformationResult:
        """Extract and transform NYISO data in one operation."""
        extraction_result = await self.extract(**kwargs)
        return await self.transform(extraction_result, **kwargs)
