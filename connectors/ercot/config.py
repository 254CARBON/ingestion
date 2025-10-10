"""
ERCOT connector configuration.
"""

from __future__ import annotations

from typing import Dict, List, Optional

from pydantic import Field

from ..base.base_connector import ConnectorConfig


class ERCOTConnectorConfig(ConnectorConfig):
    """ERCOT connector configuration."""

    # API endpoints and authentication
    base_url: str = "https://www.ercot.com"
    api_key: Optional[str] = None
    api_secret: Optional[str] = None

    description: str = Field("ERCOT market data connector", description="Connector description")
    tags: List[str] = Field(default_factory=list, description="Connector tags")

    # Dataset endpoint configuration
    dataset_configs: Dict[str, Dict[str, str]] = {
        "dam": {"path": "/api/1/services/read/damnp3", "format": "json"},
        "rtm": {"path": "/api/1/services/read/rtmnp3", "format": "json"},
        "ordc": {"path": "/api/1/services/read/np3ordc", "format": "json"},
        "as": {"path": "/api/1/services/read/ancillary", "format": "json"},
        "crr": {"path": "/api/1/services/read/crr", "format": "json"},
        "outages": {"path": "/api/1/services/read/outages", "format": "json"},
    }

    # Default extraction parameters
    default_start_hours_back: int = 24
    max_records: int = 5000

    # Rate limiting
    requests_per_minute: int = 30
    retry_attempts: int = 3
    retry_delay_seconds: int = 5

    # Data processing
    batch_size: int = 1000
    max_concurrent_requests: int = 5
