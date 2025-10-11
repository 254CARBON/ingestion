"""
CAISO OASIS connector implementation for Bronze topic publishing.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

import structlog

from ..base.base_connector import (
    BaseConnector,
    ConnectorConfig,
    ExtractionResult,
    TransformationResult,
)
from ..base.exceptions import ExtractionError, TransformationError
from .config import CAISOASISConnectorConfig
from .extractor import CAISOOASISExtractor


logger = structlog.get_logger("caiso_oasis_connector")


class CAISOOASISConnector(BaseConnector):
    """CAISO OASIS connector for Bronze topic publishing."""

    def __init__(self, config: ConnectorConfig):
        super().__init__(config)

        # Promote to specialized config
        if not isinstance(config, CAISOASISConnectorConfig):
            self.config = CAISOASISConnectorConfig(**config.dict())

        # Components
        self._extractor = CAISOOASISExtractor(self.config)
        self.endpoints = {
            "oasis_singlezip": "PRC_LMP",
            "oasis_as": "PRC_AS",
            "oasis_crr": "PRC_CRR",
        }

        # Supported data types
        self.supported_data_types = [
            "lmp_dam", "lmp_fmm", "lmp_rtm",
            "as_dam", "as_fmm", "as_rtm",
            "crr"
        ]

        # Quality metrics
        self.quality_metrics = {
            "total_requests": 0,
            "successful_requests": 0,
            "failed_requests": 0,
            "data_points_extracted": 0,
        }

    async def extract(self, **kwargs) -> ExtractionResult:
        """Extract CAISO OASIS data."""
        try:
            self.logger.info("Starting CAISO OASIS extraction")
            self.quality_metrics["total_requests"] += 1

            # Resolve extraction parameters
            data_type = (kwargs.get("data_type") or "lmp_dam").lower()
            if data_type not in self.supported_data_types:
                raise ExtractionError(f"Unsupported data type: {data_type}")

            start = kwargs.get("start") or kwargs.get("startdatetime")
            end = kwargs.get("end") or kwargs.get("enddatetime")

            # Default to previous hour if not provided
            if not start or not end:
                now = datetime.now(timezone.utc)
                end_dt = now.replace(minute=0, second=0, microsecond=0)
                start_dt = end_dt - timedelta(hours=1)
                start = start or start_dt.isoformat()
                end = end or end_dt.isoformat()

            # Extract data
            result = await self._extractor.extract_by_data_type(
                data_type=data_type,
                start_datetime=start,
                end_datetime=end,
                **kwargs
            )

            self.logger.info(
                "CAISO OASIS extraction complete",
                record_count=result.record_count,
                data_type=data_type,
            )
            self.quality_metrics["successful_requests"] += 1
            self.quality_metrics["data_points_extracted"] += result.record_count

            return result

        except Exception as e:
            self.logger.error("Failed to extract CAISO OASIS data", error=str(e))
            self.quality_metrics["failed_requests"] += 1
            raise ExtractionError(f"CAISO OASIS extraction failed: {e}") from e

    async def transform(self, extraction_result: ExtractionResult) -> TransformationResult:
        """Transform extracted CAISO OASIS data for Bronze publishing."""
        try:
            self.logger.info(
                "Starting CAISO OASIS data transformation",
                input_records=extraction_result.record_count,
            )

            # For Bronze topics, we keep the data in raw format
            # with minimal transformation for immediate publishing
            transformed_records = []

            for record in extraction_result.records:
                # Add Bronze-specific metadata
                bronze_record = {
                    **record,
                    "bronze_metadata": {
                        "published_at": datetime.now(timezone.utc).isoformat(),
                        "data_quality_score": 1.0,  # Placeholder for quality scoring
                        "processing_stage": "raw",
                        "validation_status": "pending",
                    }
                }
                transformed_records.append(bronze_record)

            # Validate schema compliance
            validation_errors = []
            if len(transformed_records) > 0:
                # Schema validation would go here
                # For now, assume all records are valid
                pass

            result = TransformationResult(
                records=transformed_records,
                record_count=len(transformed_records),
                validation_errors=validation_errors,
                metadata={
                    "transformation_type": "bronze_raw",
                    "original_records": extraction_result.record_count,
                    "transformed_records": len(transformed_records),
                    "validation_errors": len(validation_errors),
                }
            )

            self.logger.info(
                "CAISO OASIS data transformation completed",
                output_records=result.record_count,
                validation_errors=len(validation_errors),
            )

            return result

        except Exception as e:
            self.logger.error("Failed to transform CAISO OASIS data", error=str(e))
            raise TransformationError(f"CAISO OASIS transformation failed: {e}") from e

    def get_output_topic(self, data_type: str) -> str:
        """Get the Bronze output topic for a data type."""
        return self.config.output_topics.get(data_type, f"ingestion.market.caiso.{data_type}.raw.v1")

    def get_connector_info(self) -> Dict[str, str]:
        """Get connector information."""
        return {
            "name": self.config.name,
            "version": self.config.version,
            "market": self.config.market,
            "mode": self.config.mode,
            "supported_data_types": ", ".join(self.supported_data_types),
            "endpoints": ", ".join(self.endpoints.keys()),
            "output_topics": ", ".join(self.config.output_topics.values()),
        }

    def get_health_status(self) -> Dict[str, str]:
        """Get health status."""
        base_status = super().get_health_status()
        base_status.update({
            "endpoints": list(self.endpoints.keys()),
            "data_types": len(self.supported_data_types),
        })
        return base_status

    def get_metrics(self) -> Dict[str, str]:
        """Get connector metrics."""
        base_metrics = super().get_metrics()
        base_metrics.update({
            "quality_metrics": str(self.quality_metrics),
            "success_rate": str(
                self.quality_metrics["successful_requests"] /
                max(1, self.quality_metrics["total_requests"])
            ),
            "data_extraction_rate": str(self.quality_metrics["data_points_extracted"]),
        })
        return base_metrics

    async def cleanup(self) -> None:
        """Cleanup connector resources."""
        try:
            await self._extractor.close()
        except Exception as exc:
            self.logger.warning("Error during CAISO OASIS connector cleanup", error=str(exc))
