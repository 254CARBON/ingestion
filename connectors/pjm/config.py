"""
PJM connector configuration.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import Field

from ..base.base_connector import ConnectorConfig


def _default_dataset_configs() -> Dict[str, Dict[str, Any]]:
    """Default dataset metadata for PJM Data Miner and API endpoints."""
    return {
        "lmp": {
            "base": "data_miner",
            "path": "/rest/api/v1/reporting/da_hrl_lmps",
            "format": "json",
            "data_key": "items",
            "time_params": {"start": "datetime_beginning_ept", "end": "datetime_ending_ept"},
            "time_format": "%Y-%m-%dT%H:%M:%S",
            "description": "Day-Ahead hourly LMPs",
        },
        "rtm_lmp": {
            "base": "data_miner",
            "path": "/rest/api/v1/reporting/rt_hrl_lmps",
            "format": "json",
            "data_key": "items",
            "time_params": {"start": "datetime_beginning_ept", "end": "datetime_ending_ept"},
            "time_format": "%Y-%m-%dT%H:%M:%S",
            "description": "Real-Time hourly LMPs",
        },
        "rpm": {
            "base": "api",
            "path": "/capacity_market_results",
            "format": "json",
            "data_key": "items",
            "time_params": {"start": "start_date", "end": "end_date"},
            "time_format": "%Y-%m-%d",
            "description": "Reliability Pricing Model auction results",
        },
        "tcr": {
            "base": "api",
            "path": "/tcr_auction_results",
            "format": "json",
            "data_key": "items",
            "time_params": {"start": "start_date", "end": "end_date"},
            "time_format": "%Y-%m-%d",
            "description": "Transmission Congestion Rights auction results",
        },
        "outages": {
            "base": "api",
            "path": "/generation_outages",
            "format": "json",
            "data_key": "items",
            "time_params": {"start": "start_datetime", "end": "end_datetime"},
            "time_format": "%Y-%m-%dT%H:%M:%S",
            "description": "Generation outage status records",
        },
    }


class PJMConnectorConfig(ConnectorConfig):
    """PJM connector configuration."""

    description: str = Field("PJM market data connector", description="Connector description")
    tags: List[str] = Field(default_factory=lambda: ["pjm", "market"], description="Connector tags")

    data_miner_base_url: str = Field(
        "https://dataminer2.pjm.com",
        description="Base URL for PJM Data Miner endpoints",
    )
    api_base_url: str = Field(
        "https://api.pjm.com/api/v1",
        description="Base URL for PJM API endpoints",
    )
    api_timeout: float = Field(60.0, description="Request timeout in seconds")
    api_key: Optional[str] = Field(
        None,
        description="Subscription key for PJM Data Miner and API access",
    )
    client_id: Optional[str] = Field(None, description="OAuth client ID when API key not provided")
    client_secret: Optional[str] = Field(None, description="OAuth client secret when API key not provided")

    retry_attempts: int = Field(3, description="Retry attempts for transient failures")
    retry_delay_seconds: int = Field(10, description="Base delay between retries in seconds")
    max_concurrent_requests: int = Field(3, description="Maximum concurrent API requests")
    default_start_hours_back: int = Field(24, description="Default lookback window in hours")

    dataset_configs: Dict[str, Dict[str, Any]] = Field(
        default_factory=_default_dataset_configs,
        description="Dataset metadata keyed by data_type",
    )

    class Config:
        """Pydantic configuration."""

        extra = "forbid"
        validate_assignment = True

    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)
        if not self.api_key and not (self.client_id and self.client_secret):
            raise ValueError("PJM connector requires either api_key or client credentials")
