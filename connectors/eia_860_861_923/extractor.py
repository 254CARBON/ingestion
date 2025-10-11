"""
EIA 860/861/923 extractor for generator, utility, and fuel consumption data.
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

import httpx
import structlog

from ..base.base_connector import ExtractionResult
from ..base.exceptions import ExtractionError
from .config import EIAConnectorConfig


logger = structlog.get_logger("eia_extractor")


class EIAExtractor:
    """Extractor for EIA 860/861/923 data."""

    def __init__(self, config: EIAConnectorConfig):
        self.config = config
        self.client = httpx.AsyncClient(
            base_url=config.eia_base_url,
            timeout=config.eia_timeout,
            headers={"User-Agent": "254Carbon-EIA/1.0"}
        )

    async def close(self) -> None:
        """Close the HTTP client."""
        await self.client.aclose()

    async def extract_eia_860_data(
        self,
        start_date: str,
        end_date: str,
        limit: int = 5000
    ) -> ExtractionResult:
        """Extract EIA 860 generator and utility data."""
        logger.info(
            "Extracting EIA 860 data",
            start=start_date,
            end=end_date
        )

        try:
            params = {
                "api_key": self.config.eia_api_key,
                "frequency": "annual",
                "data[0]": "value",
                "facets[state][]": "*",
                "sort[0][column]": "period",
                "sort[0][direction]": "desc",
                "offset": 0,
                "length": limit,
            }

            response = await self.client.get("/electricity/data/eia860", params=params)
            response.raise_for_status()
            data = response.json()

            # Transform to extraction result format
            records = []
            for item in data.get("response", {}).get("data", []):
                record = {
                    "period": item.get("period", ""),
                    "state": item.get("state", ""),
                    "plant_code": item.get("plantCode", ""),
                    "generator_id": item.get("generatorId", ""),
                    "nameplate_capacity_mw": item.get("nameplateCapacity", ""),
                    "fuel_type": item.get("fuelType", ""),
                    "technology": item.get("technology", ""),
                    "data_type": "eia_860",
                    "extracted_at": datetime.now(timezone.utc).isoformat(),
                    "source": "EIA_API",
                }
                records.append(record)

            return ExtractionResult(
                records=records,
                record_count=len(records),
                metadata={
                    "extraction_method": "EIA_API",
                    "dataset": "eia860",
                    "start_date": start_date,
                    "end_date": end_date,
                    "api_response_total": data.get("response", {}).get("total", 0),
                }
            )

        except Exception as e:
            logger.error("Failed to extract EIA 860 data", error=str(e))
            raise ExtractionError(f"EIA 860 extraction failed: {e}") from e

    async def extract_eia_861_data(
        self,
        start_date: str,
        end_date: str,
        limit: int = 5000
    ) -> ExtractionResult:
        """Extract EIA 861 utility sales and revenue data."""
        logger.info(
            "Extracting EIA 861 data",
            start=start_date,
            end=end_date
        )

        try:
            params = {
                "api_key": self.config.eia_api_key,
                "frequency": "annual",
                "data[0]": "value",
                "facets[state][]": "*",
                "sort[0][column]": "period",
                "sort[0][direction]": "desc",
                "offset": 0,
                "length": limit,
            }

            response = await self.client.get("/electricity/data/eia861", params=params)
            response.raise_for_status()
            data = response.json()

            # Transform to extraction result format
            records = []
            for item in data.get("response", {}).get("data", []):
                record = {
                    "period": item.get("period", ""),
                    "state": item.get("state", ""),
                    "utility_id": item.get("utilityId", ""),
                    "sales_revenue_thousand_dollars": item.get("salesRevenue", ""),
                    "customers": item.get("customers", ""),
                    "sales_mwh": item.get("salesMwh", ""),
                    "data_type": "eia_861",
                    "extracted_at": datetime.now(timezone.utc).isoformat(),
                    "source": "EIA_API",
                }
                records.append(record)

            return ExtractionResult(
                records=records,
                record_count=len(records),
                metadata={
                    "extraction_method": "EIA_API",
                    "dataset": "eia861",
                    "start_date": start_date,
                    "end_date": end_date,
                    "api_response_total": data.get("response", {}).get("total", 0),
                }
            )

        except Exception as e:
            logger.error("Failed to extract EIA 861 data", error=str(e))
            raise ExtractionError(f"EIA 861 extraction failed: {e}") from e

    async def extract_eia_923_data(
        self,
        start_date: str,
        end_date: str,
        limit: int = 5000
    ) -> ExtractionResult:
        """Extract EIA 923 fuel consumption and generation data."""
        logger.info(
            "Extracting EIA 923 data",
            start=start_date,
            end=end_date
        )

        try:
            params = {
                "api_key": self.config.eia_api_key,
                "frequency": "annual",
                "data[0]": "value",
                "facets[state][]": "*",
                "sort[0][column]": "period",
                "sort[0][direction]": "desc",
                "offset": 0,
                "length": limit,
            }

            response = await self.client.get("/electricity/data/eia923", params=params)
            response.raise_for_status()
            data = response.json()

            # Transform to extraction result format
            records = []
            for item in data.get("response", {}).get("data", []):
                record = {
                    "period": item.get("period", ""),
                    "state": item.get("state", ""),
                    "plant_code": item.get("plantCode", ""),
                    "generator_id": item.get("generatorId", ""),
                    "fuel_consumed_mmbtu": item.get("fuelConsumed", ""),
                    "net_generation_mwh": item.get("netGeneration", ""),
                    "fuel_type": item.get("fuelType", ""),
                    "data_type": "eia_923",
                    "extracted_at": datetime.now(timezone.utc).isoformat(),
                    "source": "EIA_API",
                }
                records.append(record)

            return ExtractionResult(
                records=records,
                record_count=len(records),
                metadata={
                    "extraction_method": "EIA_API",
                    "dataset": "eia923",
                    "start_date": start_date,
                    "end_date": end_date,
                    "api_response_total": data.get("response", {}).get("total", 0),
                }
            )

        except Exception as e:
            logger.error("Failed to extract EIA 923 data", error=str(e))
            raise ExtractionError(f"EIA 923 extraction failed: {e}") from e

    async def extract_by_data_type(
        self,
        data_type: str,
        start_date: str,
        end_date: str,
        **kwargs
    ) -> ExtractionResult:
        """Extract data based on data type."""
        limit = kwargs.get("limit", self.config.max_records)

        if data_type == "eia_860":
            return await self.extract_eia_860_data(
                start_date=start_date,
                end_date=end_date,
                limit=limit
            )

        elif data_type == "eia_861":
            return await self.extract_eia_861_data(
                start_date=start_date,
                end_date=end_date,
                limit=limit
            )

        elif data_type == "eia_923":
            return await self.extract_eia_923_data(
                start_date=start_date,
                end_date=end_date,
                limit=limit
            )

        else:
            raise ExtractionError(f"Unsupported data type: {data_type}")

    async def extract_batch(
        self,
        data_types: List[str],
        start_date: str,
        end_date: str,
        **kwargs
    ) -> Dict[str, ExtractionResult]:
        """Extract multiple data types in batch."""
        results = {}

        for data_type in data_types:
            try:
                result = await self.extract_by_data_type(
                    data_type=data_type,
                    start_date=start_date,
                    end_date=end_date,
                    **kwargs
                )
                results[data_type] = result

            except Exception as e:
                logger.error(
                    "Failed to extract data type",
                    data_type=data_type,
                    error=str(e)
                )
                results[data_type] = ExtractionResult(
                    records=[],
                    record_count=0,
                    metadata={"error": str(e)}
                )

        return results
