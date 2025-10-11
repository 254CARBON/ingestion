"""
CAISO OASIS connector configuration.
"""

from __future__ import annotations

from typing import Dict, List, Optional

from ..base.base_connector import ConnectorConfig


class CAISOASISConnectorConfig(ConnectorConfig):
    """Configuration for CAISO OASIS connector."""

    # CAISO OASIS API settings
    caiso_base_url: str = "https://oasis.caiso.com/oasisapi"
    caiso_timeout: int = 60
    caiso_rate_limit: int = 50
    caiso_retry_attempts: int = 3
    caiso_backoff_factor: float = 2.0

    # Default query parameters
    default_market_run_id: str = "DAM"
    default_node: str = "TH_NP15_GEN-APND"
    oasis_version: str = "12"

    # Batch processing settings
    batch_size: int = 1000
    max_records: int = 5000

    # Dataset-specific configurations
    dataset_configs: Dict[str, Dict[str, str]] = {
        "lmp_dam": {
            "queryname": "PRC_LMP",
            "market_run_id": "DAM",
            "resultformat": "6",
        },
        "lmp_fmm": {
            "queryname": "PRC_LMP",
            "market_run_id": "FMM",
            "resultformat": "6",
        },
        "lmp_rtm": {
            "queryname": "PRC_LMP",
            "market_run_id": "RTM",
            "resultformat": "6",
        },
        "as_dam": {
            "queryname": "PRC_AS",
            "market_run_id": "DAM",
            "resultformat": "6",
        },
        "as_fmm": {
            "queryname": "PRC_AS",
            "market_run_id": "FMM",
            "resultformat": "6",
        },
        "as_rtm": {
            "queryname": "PRC_AS",
            "market_run_id": "RTM",
            "resultformat": "6",
        },
        "crr": {
            "queryname": "PRC_CRR",
            "resultformat": "6",
        },
    }

    # Output topic mapping
    output_topics: Dict[str, str] = {
        "lmp_dam": "ingestion.market.caiso.lmp.dam.raw.v1",
        "lmp_fmm": "ingestion.market.caiso.lmp.fmm.raw.v1",
        "lmp_rtm": "ingestion.market.caiso.lmp.rtm.raw.v1",
        "as_dam": "ingestion.market.caiso.as.dam.raw.v1",
        "as_fmm": "ingestion.market.caiso.as.fmm.raw.v1",
        "as_rtm": "ingestion.market.caiso.as.rtm.raw.v1",
        "crr": "ingestion.market.caiso.crr.raw.v1",
    }

    # Schedule overrides for different data types
    dataset_schedules: Dict[str, str] = {
        "lmp_dam": "0 * * * *",      # Hourly DAM LMP
        "lmp_fmm": "*/15 * * * *",   # 15-minute FMM LMP
        "lmp_rtm": "*/5 * * * *",    # 5-minute RTM LMP
        "as_dam": "0 * * * *",       # Hourly DAM AS
        "as_fmm": "*/15 * * * *",    # 15-minute FMM AS
        "as_rtm": "*/5 * * * *",     # 5-minute RTM AS
        "crr": "0 4 * * *",          # Daily CRR data
    }
