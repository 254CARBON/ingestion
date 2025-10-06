"""
CAISO connector implementation built on OASIS SingleZip endpoints.

Queries `/oasisapi/SingleZip` for PRC_LMP data (CSV-in-ZIP) and wires
extraction + transformation to produce the RawCAISOTrade records.
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
from .config import CAISOConnectorConfig
from .extractor import CAISOExtractor
from .transform import CAISOTransform, CAISOTransformConfig


class CAISOConnector(BaseConnector):
    """CAISO market data connector using OASIS SingleZip (no browser)."""

    def __init__(self, config: ConnectorConfig):
        super().__init__(config)
        # Promote to specialized config if needed
        if not isinstance(config, CAISOConnectorConfig):
            self.config = CAISOConnectorConfig(**config.dict())  # type: ignore[arg-type]
        # Components
        self._extractor = CAISOExtractor(self.config)  # type: ignore[arg-type]
        self._transformer = CAISOTransform(CAISOTransformConfig())
        self.endpoints = {"singlezip": "PRC_LMP"}

    async def extract(self, **kwargs) -> ExtractionResult:
        """Extract PRC_LMP data from CAISO OASIS."""
        try:
            self.logger.info("Starting CAISO OASIS extraction")
            # Resolve time window and query params
            start = kwargs.get("start") or kwargs.get("startdatetime")
            end = kwargs.get("end") or kwargs.get("enddatetime")
            market_run_id = kwargs.get("market_run_id") or getattr(self.config, "default_market_run_id", "DAM")
            node = kwargs.get("node") or getattr(self.config, "default_node", None)
            version = getattr(self.config, "oasis_version", "12")

            if node is None:
                raise ExtractionError("CAISO connector requires a 'node' argument or 'default_node' in config")

            # Default to previous clock hour UTC if not provided
            if not start or not end:
                now = datetime.now(timezone.utc)
                end_dt = now.replace(minute=0, second=0, microsecond=0)
                start_dt = end_dt - timedelta(hours=1)
                start = start or start_dt
                end = end or end_dt

            result = await self._extractor.extract_prc_lmp(
                start=start,
                end=end,
                market_run_id=market_run_id,
                node=node,
                version=version,
            )

            self.logger.info(
                "CAISO OASIS extraction complete",
                record_count=result.record_count,
            )
            return result
        except Exception as e:
            self.logger.error("Failed to extract CAISO data", error=str(e))
            raise ExtractionError(f"CAISO extraction failed: {e}") from e

    async def transform(self, extraction_result: ExtractionResult) -> TransformationResult:
        """Transform extracted CAISO data."""
        try:
            self.logger.info(
                "Starting CAISO data transformation",
                input_records=extraction_result.record_count,
            )
            result = await self._transformer.transform(extraction_result)
            self.logger.info(
                "CAISO data transformation completed",
                output_records=result.record_count,
                validation_errors=len(result.validation_errors),
            )
            return result
        except Exception as e:
            self.logger.error("Failed to transform CAISO data", error=str(e))
            raise TransformationError(f"CAISO transformation failed: {e}") from e

    def get_health_status(self) -> Dict[str, Any]:
        base_status = super().get_health_status()
        base_status.update({"endpoints": list(self.endpoints.keys())})
        return base_status

    def get_metrics(self) -> Dict[str, Any]:
        base_metrics = super().get_metrics()
        base_metrics.update({
            "quality_metrics": self.quality_metrics,
            "success_rate": (
                self.quality_metrics["successful_requests"] /
                max(1, self.quality_metrics["total_requests"])
            ),
            "data_extraction_rate": self.quality_metrics["data_points_extracted"]
        })
        return base_metrics

    def get_transformation_capabilities(self) -> Dict[str, Any]:
        """Get transformation capabilities."""
        return {
            "schema_versions": ["1.0.0", "1.1.0"],
            "transforms": ["normalize_fields", "validate_schema", "enrich_metadata"],
            "output_formats": ["avro", "json", "parquet"],
            "config": self.config.dict()
        }

    def get_connector_info(self) -> Dict[str, Any]:
        return {
            "name": self.config.name,
            "version": self.config.version,
            "market": self.config.market,
            "mode": self.config.mode,
            "endpoints": list(self.endpoints.keys()),
            "extractor_type": "CAISOExtractor",
            "transformer_type": "CAISOTransform",
            "caiso_base_url": getattr(self.config, "caiso_base_url", ""),
            "transforms_enabled": len(self.config.transforms) if self.config.transforms else 0,
            "kafka_bootstrap_servers": self.config.kafka_bootstrap_servers,
            "schema_registry_url": self.config.schema_registry_url,
        }

    def get_extraction_capabilities(self) -> Dict[str, Any]:
        return {
            "data_types": ["trade_data", "curve_data", "market_prices", "system_status"],
            "modes": ["batch", "realtime"],
            "formats": ["xml", "csv"],
            "incremental": True,
            "rate_limits": {
                "requests_per_minute": 50,
                "concurrent_requests": 5
            },
            "batch_parameters": ["start_date", "end_date", "node"],
            "realtime_parameters": ["interval_minutes"]
        }

    async def run_batch(self, start_date: str, end_date: str) -> bool:
        try:
            await self.run(start=start_date, end=end_date)
            return True
        except Exception as e:
            self.logger.error(f"Batch run failed: {e}")
            return False

    async def run_realtime(self) -> bool:
        try:
            await self.run()
            return True
        except Exception as e:
            self.logger.error(f"Real-time run failed: {e}")
            return False

