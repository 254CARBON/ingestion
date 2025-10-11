"""
CAISO OASIS extractor for DAM/FMM/RTM LMP, AS, and CRR data.
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

import structlog

from ..base.base_connector import ExtractionResult
from ..base.exceptions import ExtractionError
from .config import CAISOASISConnectorConfig
from .oasis_client import OASISClient, OASISClientConfig


logger = structlog.get_logger("caiso_oasis_extractor")


class CAISOOASISExtractor:
    """Extractor for CAISO OASIS data."""

    def __init__(self, config: CAISOASISConnectorConfig):
        self.config = config
        self.oasis_client = OASISClient(OASISClientConfig(
            base_url=config.caiso_base_url,
            timeout=config.caiso_timeout,
            user_agent="254Carbon-CAISO-OASIS/1.0"
        ))

    async def close(self) -> None:
        """Close the OASIS client."""
        await self.oasis_client.aclose()

    async def extract_lmp_data(
        self,
        market_run_id: str,
        start_datetime: str,
        end_datetime: str,
        node: str,
        version: str = "12"
    ) -> ExtractionResult:
        """Extract LMP data for a specific market run."""
        logger.info(
            "Extracting CAISO LMP data",
            market_run_id=market_run_id,
            start=start_datetime,
            end=end_datetime,
            node=node
        )

        try:
            rows = await self.oasis_client.fetch_prc_lmp_csv(
                startdatetime=start_datetime,
                enddatetime=end_datetime,
                market_run_id=market_run_id,
                node=node,
                version=version
            )

            # Transform to extraction result format
            records = []
            for row in rows:
                record = {
                    "timestamp": row.get("INTERVALSTARTTIME_GMT", ""),
                    "market": "CAISO",
                    "ba": "CAISO",
                    "node": node,
                    "lmp_usd_per_mwh": row.get("LMP_CONG_PRC", ""),
                    "lmp_marginal_loss_usd_per_mwh": row.get("LMP_MLC_PRC", ""),
                    "lmp_energy_usd_per_mwh": row.get("LMP_ENE_PRC", ""),
                    "market_run_id": market_run_id,
                    "data_type": f"lmp_{market_run_id.lower()}",
                    "extracted_at": datetime.now(timezone.utc).isoformat(),
                    "source": "OASIS_SingleZip",
                }
                records.append(record)

            return ExtractionResult(
                records=records,
                record_count=len(records),
                metadata={
                    "extraction_method": "OASIS_SingleZip",
                    "queryname": "PRC_LMP",
                    "market_run_id": market_run_id,
                    "node": node,
                    "version": version,
                    "start_datetime": start_datetime,
                    "end_datetime": end_datetime,
                }
            )

        except Exception as e:
            logger.error("Failed to extract LMP data", error=str(e))
            raise ExtractionError(f"LMP extraction failed: {e}") from e

    async def extract_as_data(
        self,
        market_run_id: str,
        start_datetime: str,
        end_datetime: str,
        version: str = "12"
    ) -> ExtractionResult:
        """Extract Ancillary Services data for a specific market run."""
        logger.info(
            "Extracting CAISO AS data",
            market_run_id=market_run_id,
            start=start_datetime,
            end=end_datetime
        )

        try:
            # AS data requires different query parameters
            params = {
                "version": version,
                "startdatetime": start_datetime,
                "enddatetime": end_datetime,
                "market_run_id": market_run_id,
            }

            rows = await self.oasis_client.fetch_csv_rows("PRC_AS", params, resultformat="6")

            # Transform to extraction result format
            records = []
            for row in rows:
                record = {
                    "timestamp": row.get("INTERVALSTARTTIME_GMT", ""),
                    "market": "CAISO",
                    "ba": "CAISO",
                    "as_type": row.get("AS_TYPE", ""),
                    "as_region": row.get("AS_REGION", ""),
                    "clearing_price_usd_per_mw": row.get("AS_CLEARING_PRC", ""),
                    "cleared_mw": row.get("AS_CLEAR_MW", ""),
                    "market_run_id": market_run_id,
                    "data_type": f"as_{market_run_id.lower()}",
                    "extracted_at": datetime.now(timezone.utc).isoformat(),
                    "source": "OASIS_SingleZip",
                }
                records.append(record)

            return ExtractionResult(
                records=records,
                record_count=len(records),
                metadata={
                    "extraction_method": "OASIS_SingleZip",
                    "queryname": "PRC_AS",
                    "market_run_id": market_run_id,
                    "version": version,
                    "start_datetime": start_datetime,
                    "end_datetime": end_datetime,
                }
            )

        except Exception as e:
            logger.error("Failed to extract AS data", error=str(e))
            raise ExtractionError(f"AS extraction failed: {e}") from e

    async def extract_crr_data(
        self,
        start_datetime: str,
        end_datetime: str,
        version: str = "12"
    ) -> ExtractionResult:
        """Extract Congestion Revenue Rights data."""
        logger.info(
            "Extracting CAISO CRR data",
            start=start_datetime,
            end=end_datetime
        )

        try:
            params = {
                "version": version,
                "startdatetime": start_datetime,
                "enddatetime": end_datetime,
            }

            rows = await self.oasis_client.fetch_csv_rows("PRC_CRR", params, resultformat="6")

            # Transform to extraction result format
            records = []
            for row in rows:
                record = {
                    "timestamp": row.get("TRADING_DATE", ""),
                    "market": "CAISO",
                    "ba": "CAISO",
                    "crr_type": row.get("CRR_TYPE", ""),
                    "source": row.get("SOURCE", ""),
                    "sink": row.get("SINK", ""),
                    "mw": row.get("MW", ""),
                    "price_usd_per_mw": row.get("PRICE", ""),
                    "data_type": "crr",
                    "extracted_at": datetime.now(timezone.utc).isoformat(),
                    "source": "OASIS_SingleZip",
                }
                records.append(record)

            return ExtractionResult(
                records=records,
                record_count=len(records),
                metadata={
                    "extraction_method": "OASIS_SingleZip",
                    "queryname": "PRC_CRR",
                    "version": version,
                    "start_datetime": start_datetime,
                    "end_datetime": end_datetime,
                }
            )

        except Exception as e:
            logger.error("Failed to extract CRR data", error=str(e))
            raise ExtractionError(f"CRR extraction failed: {e}") from e

    async def extract_by_data_type(
        self,
        data_type: str,
        start_datetime: str,
        end_datetime: str,
        **kwargs
    ) -> ExtractionResult:
        """Extract data based on data type."""
        if data_type.startswith("lmp_"):
            market_run_id = data_type.split("_")[1].upper()
            node = kwargs.get("node", self.config.default_node)
            return await self.extract_lmp_data(
                market_run_id=market_run_id,
                start_datetime=start_datetime,
                end_datetime=end_datetime,
                node=node,
                version=self.config.oasis_version
            )

        elif data_type.startswith("as_"):
            market_run_id = data_type.split("_")[1].upper()
            return await self.extract_as_data(
                market_run_id=market_run_id,
                start_datetime=start_datetime,
                end_datetime=end_datetime,
                version=self.config.oasis_version
            )

        elif data_type == "crr":
            return await self.extract_crr_data(
                start_datetime=start_datetime,
                end_datetime=end_datetime,
                version=self.config.oasis_version
            )

        else:
            raise ExtractionError(f"Unsupported data type: {data_type}")

    async def extract_batch(
        self,
        data_types: List[str],
        start_datetime: str,
        end_datetime: str,
        **kwargs
    ) -> Dict[str, ExtractionResult]:
        """Extract multiple data types in batch."""
        results = {}

        for data_type in data_types:
            try:
                result = await self.extract_by_data_type(
                    data_type=data_type,
                    start_datetime=start_datetime,
                    end_datetime=end_datetime,
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
