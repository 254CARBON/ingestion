"""
MISO connector configuration.
"""

from __future__ import annotations

from typing import Any, Dict, List

from pydantic import Field

from ..base.base_connector import ConnectorConfig


def _default_dataset_configs() -> Dict[str, Dict[str, Any]]:
    """
    Default dataset configuration for common MISO Data Exchange endpoints.

    These defaults mirror the datasets documented at https://data-exchange.misoenergy.org/apis.
    Users can override paths or query parameters via connector configuration if the
    portal evolves or custom access tiers are required.
    """
    return {
        "lmp": {
            "path": "/services/api/miso-market/v1/lmp",
            "format": "json",
            "data_key": "items",
            "time_params": {"start": "startTime", "end": "endTime"},
            "time_format": "%Y-%m-%dT%H:%M:%SZ",
            "description": "Locational Marginal Prices (5-minute/hourly)",
        },
        "as": {
            "path": "/services/api/miso-market/v1/ancillary-services",
            "format": "json",
            "data_key": "items",
            "time_params": {"start": "startTime", "end": "endTime"},
            "time_format": "%Y-%m-%dT%H:%M:%SZ",
            "description": "Ancillary services clearing results",
        },
        "pra": {
            "path": "/services/api/miso-market/v1/planning-reserve-auction",
            "format": "json",
            "data_key": "items",
            "time_params": {"start": "auctionStartDate", "end": "auctionEndDate"},
            "time_format": "%Y-%m-%d",
            "description": "Planning Reserve Auction awards",
        },
        "arr": {
            "path": "/services/api/miso-market/v1/auction-revenue-rights",
            "format": "json",
            "data_key": "items",
            "time_params": {"start": "auctionStartDate", "end": "auctionEndDate"},
            "time_format": "%Y-%m-%d",
            "description": "Auction Revenue Rights results",
        },
        "tcr": {
            "path": "/services/api/miso-market/v1/transmission-congestion-rights",
            "format": "json",
            "data_key": "items",
            "time_params": {"start": "auctionStartDate", "end": "auctionEndDate"},
            "time_format": "%Y-%m-%d",
            "description": "Transmission Congestion Rights auction results",
        },
    }


class MISOConnectorConfig(ConnectorConfig):
    """Configuration for the MISO connector."""

    description: str = Field("MISO market data connector", description="Connector description")
    tags: List[str] = Field(default_factory=list, description="Connector tags")

    miso_api_key: str = Field(..., description="Subscription key for the MISO Data Exchange APIs")
    miso_base_url: str = Field(
        "https://data-exchange.misoenergy.org",
        description="Base URL for MISO Data Exchange APIs",
    )

    miso_timeout: int = Field(60, description="Request timeout in seconds")
    miso_rate_limit: int = Field(60, description="Requests per minute throttle")
    miso_retry_attempts: int = Field(3, description="Retry attempts for transient failures")
    miso_backoff_factor: float = Field(2.0, description="Backoff factor for retries")
    retry_delay_seconds: int = Field(5, description="Initial retry delay for 429 handling")
    max_concurrent_requests: int = Field(4, description="Maximum in-flight HTTP requests")

    default_start_hours_back: int = Field(6, description="Default lookback window when time range not provided")
    batch_size: int = Field(1000, description="Maximum rows requested per page")
    max_records: int = Field(5000, description="Safety cap on records per extraction")

    dataset_configs: Dict[str, Dict[str, Any]] = Field(
        default_factory=_default_dataset_configs,
        description="Dataset configuration keyed by data_type",
    )

    class Config:
        """Pydantic configuration."""

        extra = "forbid"
        validate_assignment = True
