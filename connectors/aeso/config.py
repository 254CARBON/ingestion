"""
AESO connector configuration.
"""

from __future__ import annotations

from typing import Any, Dict, List

from pydantic import Field

from ..base.base_connector import ConnectorConfig


def _default_dataset_configs() -> Dict[str, Dict[str, Any]]:
    """
    Default dataset configuration for AESO public market APIs.

    Paths and parameters are based on the documented AESO API gateway.
    """
    return {
        "pool_price": {
            "path": "/v1.1/price/poolPrice",
            "format": "json",
            "data_key": "return.Pool Price Report",
            "time_params": {"start": "startDate", "end": "endDate"},
            "time_format": "%Y-%m-%d",
            "hourly_time_format": "%Y-%m-%d %H",
            "description": "Pool price report with optional hourly granularity.",
        },
        "system_marginal_price": {
            "path": "/v1.1/price/systemMarginalPrice",
            "format": "json",
            "data_key": "return.System Marginal Price Report",
            "time_params": {"start": "startDate", "end": "endDate"},
            "time_format": "%Y-%m-%d",
            "hourly_time_format": "%Y-%m-%d %H:%M",
            "description": "System marginal price report including volume data.",
        },
        "alberta_internal_load": {
            "path": "/v1/load/albertaInternalLoad",
            "format": "json",
            "data_key": "return.Actual Forecast Report",
            "time_params": {"start": "startDate", "end": "endDate"},
            "time_format": "%Y-%m-%d",
            "description": "Actual and forecast Alberta internal load.",
        },
        "current_supply_demand_summary": {
            "path": "/v1/csd/summary/current",
            "format": "json",
            "data_key": "return",
            "description": "Current supply demand summary snapshot.",
        },
        "current_supply_demand_assets": {
            "path": "/v1/csd/generation/assets/current",
            "format": "json",
            "data_key": "return",
            "array_params": {"asset_ids": "assetIds"},
            "description": "Current supply demand by generation asset.",
        },
    }


class AESOConnectorConfig(ConnectorConfig):
    """Configuration model for the AESO connector."""

    description: str = Field("AESO market data connector", description="Connector description")
    tags: List[str] = Field(default_factory=lambda: ["market-data", "energy"], description="Connector tags")

    aeso_api_key: str = Field(..., description="API key for the AESO API gateway")
    aeso_base_url: str = Field(
        "https://api.aeso.ca/report",
        description="Base URL for AESO reporting APIs (without trailing slash).",
    )

    aeso_timeout: int = Field(30, description="Request timeout in seconds")
    aeso_rate_limit: int = Field(60, description="Request-per-minute throttle to respect API limits")
    retry_attempts: int = Field(3, description="Retry attempts for transient failures")
    retry_backoff_factor: float = Field(2.0, description="Exponential backoff factor for retries")
    default_start_hours_back: int = Field(24, description="Default lookback window when start_time is omitted")

    dataset_configs: Dict[str, Dict[str, Any]] = Field(
        default_factory=_default_dataset_configs,
        description="Dataset configuration keyed by data_type.",
    )

    class Config:
        """Pydantic configuration."""

        extra = "forbid"
        validate_assignment = True
