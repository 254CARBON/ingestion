"""
Shared extractors for cross-cutting data sources (EPA CEMS, CARB CT, CEC, NOAA).
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Any

import httpx
import structlog

from .base.base_connector import ExtractionResult
from .base.exceptions import ExtractionError


logger = structlog.get_logger("shared_extractors")


class SharedExtractor:
    """Shared extractor for cross-cutting data sources."""

    def __init__(self, base_url: str, timeout: int = 60, rate_limit: int = 30):
        self.base_url = base_url
        self.timeout = timeout
        self.rate_limit = rate_limit
        self.client = httpx.AsyncClient(
            base_url=base_url,
            timeout=timeout,
            headers={"User-Agent": "254Carbon-Shared/1.0"}
        )

    async def close(self) -> None:
        """Close the HTTP client."""
        await self.client.aclose()

    async def extract_epa_cems_data(
        self,
        start_date: str,
        end_date: str,
        facility_ids: Optional[List[str]] = None,
        limit: int = 50000
    ) -> ExtractionResult:
        """Extract EPA CEMS hourly emissions data."""
        logger.info(
            "Extracting EPA CEMS data",
            start=start_date,
            end=end_date,
            facility_count=len(facility_ids) if facility_ids else "all"
        )

        try:
            params = {
                "start_date": start_date,
                "end_date": end_date,
                "limit": limit,
            }
            
            if facility_ids:
                params["facility_ids"] = ",".join(facility_ids)

            response = await self.client.get("/api/v1/hourly", params=params)
            response.raise_for_status()
            data = response.json()

            # Transform to extraction result format
            records = []
            for item in data.get("data", []):
                record = {
                    "facility_id": item.get("facility_id", ""),
                    "unit_id": item.get("unit_id", ""),
                    "date": item.get("date", ""),
                    "hour": item.get("hour", ""),
                    "co2_mass_tons": item.get("co2_mass", ""),
                    "nox_mass_tons": item.get("nox_mass", ""),
                    "so2_mass_tons": item.get("so2_mass", ""),
                    "heat_input_mmbtu": item.get("heat_input", ""),
                    "data_type": "epa_cems_hourly",
                    "extracted_at": datetime.now(timezone.utc).isoformat(),
                    "source": "EPA_CEMS_API",
                }
                records.append(record)

            return ExtractionResult(
                records=records,
                record_count=len(records),
                metadata={
                    "extraction_method": "EPA_CEMS_API",
                    "start_date": start_date,
                    "end_date": end_date,
                    "facility_count": len(facility_ids) if facility_ids else "all",
                }
            )

        except Exception as e:
            logger.error("Failed to extract EPA CEMS data", error=str(e))
            raise ExtractionError(f"EPA CEMS extraction failed: {e}") from e

    async def extract_carb_ct_data(
        self,
        start_date: str,
        end_date: str,
        data_type: str = "auction_results",
        limit: int = 1000
    ) -> ExtractionResult:
        """Extract CARB Cap-and-Trade data."""
        logger.info(
            "Extracting CARB CT data",
            data_type=data_type,
            start=start_date,
            end=end_date
        )

        try:
            endpoint_map = {
                "auction_results": "/auction/results",
                "allowance_prices": "/market/prices",
                "compliance_data": "/compliance/data",
            }
            
            endpoint = endpoint_map.get(data_type, "/auction/results")
            
            params = {
                "start_date": start_date,
                "end_date": end_date,
                "limit": limit,
            }

            response = await self.client.get(endpoint, params=params)
            response.raise_for_status()
            data = response.json()

            # Transform to extraction result format
            records = []
            for item in data.get("data", []):
                if data_type == "auction_results":
                    record = {
                        "auction_date": item.get("auction_date", ""),
                        "allowance_type": item.get("allowance_type", ""),
                        "clearing_price_usd": item.get("clearing_price", ""),
                        "volume_sold": item.get("volume_sold", ""),
                        "total_volume": item.get("total_volume", ""),
                        "data_type": "carb_auction_results",
                        "extracted_at": datetime.now(timezone.utc).isoformat(),
                        "source": "CARB_CT_API",
                    }
                elif data_type == "allowance_prices":
                    record = {
                        "date": item.get("date", ""),
                        "allowance_type": item.get("allowance_type", ""),
                        "bid_price_usd": item.get("bid_price", ""),
                        "ask_price_usd": item.get("ask_price", ""),
                        "volume": item.get("volume", ""),
                        "data_type": "carb_allowance_prices",
                        "extracted_at": datetime.now(timezone.utc).isoformat(),
                        "source": "CARB_CT_API",
                    }
                else:  # compliance_data
                    record = {
                        "entity_id": item.get("entity_id", ""),
                        "compliance_period": item.get("compliance_period", ""),
                        "allowances_surrendered": item.get("allowances_surrendered", ""),
                        "compliance_status": item.get("compliance_status", ""),
                        "data_type": "carb_compliance_data",
                        "extracted_at": datetime.now(timezone.utc).isoformat(),
                        "source": "CARB_CT_API",
                    }
                records.append(record)

            return ExtractionResult(
                records=records,
                record_count=len(records),
                metadata={
                    "extraction_method": "CARB_CT_API",
                    "data_type": data_type,
                    "start_date": start_date,
                    "end_date": end_date,
                }
            )

        except Exception as e:
            logger.error("Failed to extract CARB CT data", error=str(e))
            raise ExtractionError(f"CARB CT extraction failed: {e}") from e

    async def extract_cec_demand_data(
        self,
        start_date: str,
        end_date: str,
        data_type: str = "demand_forecast",
        limit: int = 5000
    ) -> ExtractionResult:
        """Extract CEC demand forecast data."""
        logger.info(
            "Extracting CEC demand data",
            data_type=data_type,
            start=start_date,
            end=end_date
        )

        try:
            endpoint_map = {
                "demand_forecast": "/demand/forecast",
                "load_profiles": "/demand/profiles",
                "electrification": "/electrification/data",
            }
            
            endpoint = endpoint_map.get(data_type, "/demand/forecast")
            
            params = {
                "start_date": start_date,
                "end_date": end_date,
                "limit": limit,
            }

            response = await self.client.get(endpoint, params=params)
            response.raise_for_status()
            data = response.json()

            # Transform to extraction result format
            records = []
            for item in data.get("data", []):
                if data_type == "demand_forecast":
                    record = {
                        "forecast_year": item.get("forecast_year", ""),
                        "scenario": item.get("scenario", ""),
                        "peak_demand_mw": item.get("peak_demand_mw", ""),
                        "annual_load_gwh": item.get("annual_load_gwh", ""),
                        "growth_rate": item.get("growth_rate", ""),
                        "data_type": "cec_demand_forecast",
                        "extracted_at": datetime.now(timezone.utc).isoformat(),
                        "source": "CEC_DEMAND_API",
                    }
                elif data_type == "load_profiles":
                    record = {
                        "date": item.get("date", ""),
                        "hour": item.get("hour", ""),
                        "residential_mw": item.get("residential_mw", ""),
                        "commercial_mw": item.get("commercial_mw", ""),
                        "industrial_mw": item.get("industrial_mw", ""),
                        "total_mw": item.get("total_mw", ""),
                        "data_type": "cec_load_profiles",
                        "extracted_at": datetime.now(timezone.utc).isoformat(),
                        "source": "CEC_DEMAND_API",
                    }
                else:  # electrification
                    record = {
                        "year": item.get("year", ""),
                        "sector": item.get("sector", ""),
                        "adoption_rate": item.get("adoption_rate", ""),
                        "additional_load_mw": item.get("additional_load_mw", ""),
                        "co2_reduction_tons": item.get("co2_reduction_tons", ""),
                        "data_type": "cec_electrification",
                        "extracted_at": datetime.now(timezone.utc).isoformat(),
                        "source": "CEC_DEMAND_API",
                    }
                records.append(record)

            return ExtractionResult(
                records=records,
                record_count=len(records),
                metadata={
                    "extraction_method": "CEC_DEMAND_API",
                    "data_type": data_type,
                    "start_date": start_date,
                    "end_date": end_date,
                }
            )

        except Exception as e:
            logger.error("Failed to extract CEC demand data", error=str(e))
            raise ExtractionError(f"CEC demand extraction failed: {e}") from e

    async def extract_noaa_hydro_data(
        self,
        start_date: str,
        end_date: str,
        data_type: str = "precipitation",
        station_ids: Optional[List[str]] = None,
        limit: int = 10000
    ) -> ExtractionResult:
        """Extract NOAA hydro and climate data."""
        logger.info(
            "Extracting NOAA hydro data",
            data_type=data_type,
            start=start_date,
            end=end_date,
            station_count=len(station_ids) if station_ids else "all"
        )

        try:
            endpoint_map = {
                "precipitation": "/precipitation/data",
                "temperature": "/temperature/data",
                "streamflow": "/streamflow/data",
                "snowpack": "/snowpack/data",
            }
            
            endpoint = endpoint_map.get(data_type, "/precipitation/data")
            
            params = {
                "start_date": start_date,
                "end_date": end_date,
                "limit": limit,
            }
            
            if station_ids:
                params["station_ids"] = ",".join(station_ids)

            response = await self.client.get(endpoint, params=params)
            response.raise_for_status()
            data = response.json()

            # Transform to extraction result format
            records = []
            for item in data.get("data", []):
                if data_type == "precipitation":
                    record = {
                        "station_id": item.get("station_id", ""),
                        "date": item.get("date", ""),
                        "precipitation_inches": item.get("precipitation_inches", ""),
                        "latitude": item.get("latitude", ""),
                        "longitude": item.get("longitude", ""),
                        "data_type": "noaa_precipitation",
                        "extracted_at": datetime.now(timezone.utc).isoformat(),
                        "source": "NOAA_HYDRO_API",
                    }
                elif data_type == "temperature":
                    record = {
                        "station_id": item.get("station_id", ""),
                        "date": item.get("date", ""),
                        "max_temp_f": item.get("max_temp_f", ""),
                        "min_temp_f": item.get("min_temp_f", ""),
                        "avg_temp_f": item.get("avg_temp_f", ""),
                        "data_type": "noaa_temperature",
                        "extracted_at": datetime.now(timezone.utc).isoformat(),
                        "source": "NOAA_HYDRO_API",
                    }
                elif data_type == "streamflow":
                    record = {
                        "gauge_id": item.get("gauge_id", ""),
                        "date": item.get("date", ""),
                        "flow_cfs": item.get("flow_cfs", ""),
                        "stage_feet": item.get("stage_feet", ""),
                        "river_name": item.get("river_name", ""),
                        "data_type": "noaa_streamflow",
                        "extracted_at": datetime.now(timezone.utc).isoformat(),
                        "source": "NOAA_HYDRO_API",
                    }
                else:  # snowpack
                    record = {
                        "station_id": item.get("station_id", ""),
                        "date": item.get("date", ""),
                        "swe_inches": item.get("swe_inches", ""),
                        "snow_depth_inches": item.get("snow_depth_inches", ""),
                        "elevation_feet": item.get("elevation_feet", ""),
                        "data_type": "noaa_snowpack",
                        "extracted_at": datetime.now(timezone.utc).isoformat(),
                        "source": "NOAA_HYDRO_API",
                    }
                records.append(record)

            return ExtractionResult(
                records=records,
                record_count=len(records),
                metadata={
                    "extraction_method": "NOAA_HYDRO_API",
                    "data_type": data_type,
                    "start_date": start_date,
                    "end_date": end_date,
                    "station_count": len(station_ids) if station_ids else "all",
                }
            )

        except Exception as e:
            logger.error("Failed to extract NOAA hydro data", error=str(e))
            raise ExtractionError(f"NOAA hydro extraction failed: {e}") from e
