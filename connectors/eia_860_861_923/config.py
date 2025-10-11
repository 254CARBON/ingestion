"""
EIA 860/861/923 connector configuration.
"""

from __future__ import annotations

from typing import Dict, List, Optional

from ..base.base_connector import ConnectorConfig


class EIAConnectorConfig(ConnectorConfig):
    """Configuration for EIA 860/861/923 connector."""

    # EIA API settings
    eia_api_key: str = ""
    eia_base_url: str = "https://api.eia.gov/v2"
    eia_timeout: int = 60
    eia_rate_limit: int = 100
    eia_retry_attempts: int = 3
    eia_backoff_factor: float = 2.0

    # Batch processing settings
    batch_size: int = 1000
    max_records: int = 10000

    # Dataset-specific configurations
    dataset_configs: Dict[str, Dict[str, str]] = {
        "eia_860": {
            "dataset": "electricity/data/eia860",
            "description": "Generator and utility data",
            "fields": "plant_code,generator_id,nameplate_capacity,fuel_type,technology",
        },
        "eia_861": {
            "dataset": "electricity/data/eia861",
            "description": "Utility sales and revenue data",
            "fields": "utility_id,state,sales_revenue,customers",
        },
        "eia_923": {
            "dataset": "electricity/data/eia923",
            "description": "Fuel consumption and generation data",
            "fields": "plant_code,generator_id,fuel_consumed,net_generation,fuel_type",
        },
    }

    # Output topic mapping
    output_topics: Dict[str, str] = {
        "eia_860": "ingestion.reference.eia.860.raw.v1",
        "eia_861": "ingestion.reference.eia.861.raw.v1",
        "eia_923": "ingestion.reference.eia.923.raw.v1",
    }

    # Schedule overrides for different datasets
    dataset_schedules: Dict[str, str] = {
        "eia_860": "0 2 * * *",    # Daily generator data
        "eia_861": "0 3 * * *",    # Daily utility data
        "eia_923": "0 4 * * *",    # Daily fuel consumption
    }
