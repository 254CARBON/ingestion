"""
NYISO connector configuration.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import Field

from ..base.base_connector import ConnectorConfig


def _default_dataset_configs() -> Dict[str, Dict[str, Any]]:
    """Default dataset metadata for NYISO OASIS and public REST endpoints."""
    return {
        "lbmp": {
            "base": "oasis",
            "path": "/lbmp/da",
            "format": "json",
            "data_key": "data",
            "time_params": {"start": "startdate", "end": "enddate"},
            "time_format": "%Y%m%d%H%M",
            "description": "Day-Ahead LBMP prices",
        },
        "rt_lbmp": {
            "base": "oasis",
            "path": "/lbmp/rt",
            "format": "json",
            "data_key": "data",
            "time_params": {"start": "startdate", "end": "enddate"},
            "time_format": "%Y%m%d%H%M",
            "description": "Real-Time LBMP prices",
        },
        "icap": {
            "base": "rest",
            "path": "/icap/demand_curve",
            "format": "json",
            "data_key": "data",
            "time_params": {"start": "startdate", "end": "enddate"},
            "time_format": "%Y%m%d",
            "description": "Installed Capacity demand curve",
        },
        "tcc": {
            "base": "rest",
            "path": "/tcc/auction_results",
            "format": "json",
            "data_key": "data",
            "time_params": {"start": "startdate", "end": "enddate"},
            "time_format": "%Y%m%d",
            "description": "Transmission Congestion Contracts auction results",
        },
        "outages": {
            "base": "rest",
            "path": "/outages/generator",
            "format": "json",
            "data_key": "data",
            "time_params": {"start": "startdate", "end": "enddate"},
            "time_format": "%Y-%m-%dT%H:%M:%SZ",
            "description": "Generator outage status records",
        },
    }


class NYISOConnectorConfig(ConnectorConfig):
    """NYISO connector configuration."""

    description: str = Field("NYISO market data connector", description="Connector description")
    tags: List[str] = Field(default_factory=lambda: ["nyiso", "market"], description="Connector tags")

    oasis_base_url: str = Field(
        "https://www.nyiso.com/oasis/api",
        description="Base URL for NYISO OASIS datasets",
    )
    rest_base_url: str = Field(
        "https://www.nyiso.com/api",
        description="Base URL for NYISO REST datasets",
    )
    api_timeout: float = Field(60.0, description="Request timeout in seconds")

    api_key: Optional[str] = Field(None, description="API key for NYISO services")
    username: Optional[str] = Field(None, description="Username for NYISO basic authentication")
    password: Optional[str] = Field(None, description="Password for NYISO basic authentication")

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
            raise ValueError("NYISO connector requires either api_key or username/password credentials")
