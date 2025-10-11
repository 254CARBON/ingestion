"""
ERCOT snapshot exporter for program MVP metrics.

Exports hourly load and generation data to CSV contracts for the program metrics loader.
"""

from __future__ import annotations

import csv
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import structlog

from .extractor import ERCOTExtractor
from .config import ERCOTConnectorConfig

logger = structlog.get_logger(__name__)


class ERCOTSnapshotExporter:
    """Export ERCOT data to program MVP CSV contracts."""

    def __init__(self, config: ERCOTConnectorConfig):
        self.config = config
        self.extractor = ERCOTExtractor(config)

    async def export_snapshot(
        self,
        start_time: datetime,
        end_time: datetime,
        output_dir: Path,
    ) -> None:
        """Export ERCOT snapshot to CSV contracts."""
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Extract hourly load and generation data
        load_data = await self._extract_load_data(start_time, end_time)
        gen_data = await self._extract_generation_data(start_time, end_time)
        
        # Export to CSV contracts
        await self._export_load_demand_csv(load_data, output_dir)
        await self._export_generation_actual_csv(gen_data, output_dir)
        await self._export_generation_capacity_csv(output_dir)
        await self._export_rec_ledger_csv(output_dir)
        await self._export_emission_factors_csv(output_dir)
        
        logger.info("ERCOT snapshot export completed", output_dir=str(output_dir))

    async def _extract_load_data(
        self, start_time: datetime, end_time: datetime
    ) -> List[Dict[str, Any]]:
        """Extract hourly load data from ERCOT MIS."""
        try:
            result = await self.extractor.extract_data(
                data_type="dam",
                start_time=start_time,
                end_time=end_time,
            )
            
            # Aggregate load by zone
            load_by_zone = {}
            for record in result.records:
                timestamp = record.get("timestamp")
                zone = record.get("zone", "ERCOT")
                demand_mw = record.get("demand_mw", 0.0)
                
                if timestamp and demand_mw > 0:
                    key = (timestamp, zone)
                    if key not in load_by_zone:
                        load_by_zone[key] = {"timestamp": timestamp, "zone": zone, "demand_mw": 0.0}
                    load_by_zone[key]["demand_mw"] += demand_mw
            
            return list(load_by_zone.values())
            
        except Exception as e:
            logger.error("Failed to extract ERCOT load data", error=str(e))
            return []

    async def _extract_generation_data(
        self, start_time: datetime, end_time: datetime
    ) -> List[Dict[str, Any]]:
        """Extract hourly generation data from ERCOT MIS."""
        try:
            result = await self.extractor.extract_data(
                data_type="dam",
                start_time=start_time,
                end_time=end_time,
            )
            
            # Extract generation by fuel type
            gen_records = []
            for record in result.records:
                timestamp = record.get("timestamp")
                fuel = record.get("fuel")
                output_mw = record.get("output_mw", 0.0)
                
                if timestamp and fuel and output_mw > 0:
                    # Map ERCOT fuel codes to standard types
                    fuel_mapping = {
                        "NG": "natural_gas",
                        "COAL": "coal",
                        "WIND": "wind",
                        "SOLAR": "solar",
                        "NUC": "nuclear",
                        "HYDRO": "hydro",
                    }
                    
                    resource_type_mapping = {
                        "natural_gas": "gas_ccgt",
                        "coal": "coal",
                        "wind": "wind",
                        "solar": "solar_pv",
                        "nuclear": "nuclear",
                        "hydro": "hydro",
                    }
                    
                    mapped_fuel = fuel_mapping.get(fuel, fuel.lower())
                    resource_type = resource_type_mapping.get(mapped_fuel, "unknown")
                    
                    gen_records.append({
                        "timestamp": timestamp,
                        "resource_id": f"{mapped_fuel.upper()}_PLANT_001",
                        "resource_type": resource_type,
                        "fuel": mapped_fuel,
                        "output_mw": output_mw,
                        "output_mwh": output_mw,  # 1-hour intervals
                    })
            
            return gen_records
            
        except Exception as e:
            logger.error("Failed to extract ERCOT generation data", error=str(e))
            return []

    async def _export_load_demand_csv(
        self, load_data: List[Dict[str, Any]], output_dir: Path
    ) -> None:
        """Export load demand CSV."""
        file_path = output_dir / "load_demand.csv"
        
        with open(file_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=[
                "timestamp", "market", "ba", "zone", "demand_mw", "data_source"
            ])
            writer.writeheader()
            
            for record in load_data:
                writer.writerow({
                    "timestamp": record["timestamp"].isoformat() + "Z",
                    "market": "ercot",
                    "ba": "ERCOT",
                    "zone": record["zone"],
                    "demand_mw": record["demand_mw"],
                    "data_source": "ercot_mis",
                })

    async def _export_generation_actual_csv(
        self, gen_data: List[Dict[str, Any]], output_dir: Path
    ) -> None:
        """Export generation actual CSV."""
        file_path = output_dir / "generation_actual.csv"
        
        with open(file_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=[
                "timestamp", "market", "ba", "zone", "resource_id", "resource_type",
                "fuel", "output_mw", "output_mwh", "data_source"
            ])
            writer.writeheader()
            
            for record in gen_data:
                writer.writerow({
                    "timestamp": record["timestamp"].isoformat() + "Z",
                    "market": "ercot",
                    "ba": "ERCOT",
                    "zone": "ERCOT",
                    "resource_id": record["resource_id"],
                    "resource_type": record["resource_type"],
                    "fuel": record["fuel"],
                    "output_mw": record["output_mw"],
                    "output_mwh": record["output_mwh"],
                    "data_source": "ercot_mis",
                })

    async def _export_generation_capacity_csv(self, output_dir: Path) -> None:
        """Export generation capacity CSV with seasonal UCAP factors."""
        file_path = output_dir / "generation_capacity.csv"
        
        # Static capacity data for MVP (based on ERCOT seasonal accreditation)
        capacity_data = [
            {
                "resource_id": "WIND_FARM_001",
                "resource_type": "wind",
                "fuel": "wind",
                "nameplate_mw": 3000.0,
                "ucap_factor": 0.15,  # ERCOT wind UCAP (summer)
                "cost_curve": "linear:500,3000",
            },
            {
                "resource_id": "SOLAR_FARM_001",
                "resource_type": "solar_pv",
                "fuel": "solar",
                "nameplate_mw": 2000.0,
                "ucap_factor": 0.25,  # ERCOT solar UCAP (summer)
                "cost_curve": "linear:700,2000",
            },
            {
                "resource_id": "GAS_PLANT_001",
                "resource_type": "gas_ccgt",
                "fuel": "natural_gas",
                "nameplate_mw": 1500.0,
                "ucap_factor": 1.0,
                "cost_curve": "linear:1100,1500",
            },
        ]
        
        with open(file_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=[
                "effective_date", "market", "ba", "zone", "resource_id", "resource_type",
                "fuel", "nameplate_mw", "ucap_factor", "ucap_mw", "availability_factor",
                "cost_curve", "data_source"
            ])
            writer.writeheader()
            
            for record in capacity_data:
                writer.writerow({
                    "effective_date": "2024-01-01",
                    "market": "ercot",
                    "ba": "ERCOT",
                    "zone": "ERCOT",
                    "resource_id": record["resource_id"],
                    "resource_type": record["resource_type"],
                    "fuel": record["fuel"],
                    "nameplate_mw": record["nameplate_mw"],
                    "ucap_factor": record["ucap_factor"],
                    "ucap_mw": record["nameplate_mw"] * record["ucap_factor"],
                    "availability_factor": 0.95,
                    "cost_curve": record["cost_curve"],
                    "data_source": "seasonal_accreditation",
                })

    async def _export_rec_ledger_csv(self, output_dir: Path) -> None:
        """Export synthetic REC ledger CSV."""
        file_path = output_dir / "rec_ledger.csv"
        
        # Synthetic REC data for MVP
        rec_data = [
            {
                "lse": "ONCOR_ELECTRIC",
                "certificate_id": "REC_2024_001",
                "resource_id": "WIND_FARM_001",
                "mwh": 1500.0,
                "status": "available",
            },
            {
                "lse": "CENTERPOINT_ENERGY",
                "certificate_id": "REC_2024_002",
                "resource_id": "SOLAR_FARM_001",
                "mwh": 1000.0,
                "status": "available",
            },
        ]
        
        with open(file_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=[
                "vintage_year", "market", "lse", "certificate_id", "resource_id",
                "mwh", "status", "retired_year", "data_source"
            ])
            writer.writeheader()
            
            for record in rec_data:
                writer.writerow({
                    "vintage_year": 2024,
                    "market": "ercot",
                    "lse": record["lse"],
                    "certificate_id": record["certificate_id"],
                    "resource_id": record["resource_id"],
                    "mwh": record["mwh"],
                    "status": record["status"],
                    "retired_year": "",
                    "data_source": "synthetic_mvp",
                })

    async def _export_emission_factors_csv(self, output_dir: Path) -> None:
        """Export emission factors CSV."""
        file_path = output_dir / "emission_factors.csv"
        
        # EPA eGRID factors for scope1
        emission_factors = [
            {"fuel": "natural_gas", "kg_co2e_per_mwh": 400.0},
            {"fuel": "coal", "kg_co2e_per_mwh": 900.0},
            {"fuel": "solar", "kg_co2e_per_mwh": 0.0},
            {"fuel": "wind", "kg_co2e_per_mwh": 0.0},
            {"fuel": "hydro", "kg_co2e_per_mwh": 0.0},
            {"fuel": "nuclear", "kg_co2e_per_mwh": 0.0},
        ]
        
        with open(file_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=[
                "fuel", "scope", "kg_co2e_per_mwh", "source", "effective_date", "expires_at"
            ])
            writer.writeheader()
            
            for record in emission_factors:
                writer.writerow({
                    "fuel": record["fuel"],
                    "scope": "scope1",
                    "kg_co2e_per_mwh": record["kg_co2e_per_mwh"],
                    "source": "epa_egrid",
                    "effective_date": "2024-01-01",
                    "expires_at": "",
                })

    async def close(self) -> None:
        """Close extractor resources."""
        if self.extractor:
            await self.extractor.close()
