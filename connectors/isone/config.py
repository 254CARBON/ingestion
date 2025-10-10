"""
ISO-NE connector configuration.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import Field

from ..base.base_connector import ConnectorConfig


def _default_dataset_configs() -> Dict[str, Dict[str, Any]]:
    """Default dataset metadata for ISO-NE web services."""
    return {
        "lmp": {
            "path": "/v1.1/lmp/da",
            "format": "json",
            "data_key": "data",
            "time_params": {"start": "start", "end": "end"},
            "time_format": "%Y%m%d%H%M",
            "description": "Day-Ahead locational marginal prices",
        },
        "rt_lmp": {
            "path": "/v1.1/lmp/rt",
            "format": "json",
            "data_key": "data",
            "time_params": {"start": "start", "end": "end"},
            "time_format": "%Y%m%d%H%M",
            "description": "Real-Time locational marginal prices",
        },
        "fcm": {
            "path": "/v1.1/capacity/fcm",
            "format": "json",
            "data_key": "data",
            "time_params": {"start": "startdate", "end": "enddate"},
            "time_format": "%Y%m%d",
            "description": "Forward Capacity Market auction results",
        },
        "ftr": {
            "path": "/v1.1/ftr/auction",
            "format": "json",
            "data_key": "data",
            "time_params": {"start": "startdate", "end": "enddate"},
            "time_format": "%Y%m%d",
            "description": "Financial Transmission Rights auction results",
        },
        "outages": {
            "path": "/v1.1/outage/generation",
            "format": "json",
            "data_key": "data",
            "time_params": {"start": "start", "end": "end"},
            "time_format": "%Y-%m-%dT%H:%M:%S",
            "description": "Generation outage events",
        },
    }


class ISONEConnectorConfig(ConnectorConfig):
    """ISO-NE connector configuration."""

    description: str = Field("ISO-NE market data connector", description="Connector description")
    tags: List[str] = Field(default_factory=lambda: ["isone", "market"], description="Connector tags")

    base_url: str = Field(
        "https://webservices.iso-ne.com/api",
        description="Base URL for ISO-NE web services",
    )
    api_timeout: float = Field(60.0, description="Request timeout in seconds")

    api_key: Optional[str] = Field(None, description="API key for ISO-NE web services")
    username: Optional[str] = Field(None, description="Username for ISO-NE basic authentication")
    password: Optional[str] = Field(None, description="Password for ISO-NE basic authentication")

    retry_attempts: int = Field(3, description="Retry attempts for transient failures")
    retry_delay_seconds: int = Field(15, description="Base delay between retry attempts")
    max_concurrent_requests: int = Field(2, description="Maximum concurrent API requests")
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
        if not self.api_key and not (self.username and self.password):
            raise ValueError("ISO-NE connector requires either api_key or username/password credentials")
