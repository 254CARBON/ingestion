"""
ISO-NE connector configuration.
"""

from __future__ import annotations

from typing import Dict, List, Optional

from ..base.base_connector import ConnectorConfig


class ISONEConnectorConfig(ConnectorConfig):
    """ISO-NE connector configuration."""

    # API endpoints
    base_url: str = "https://webservices.iso-ne.com/api"
    data_url: str = "https://www.iso-ne.com/ws/wsclient"

    # Authentication (API key or OAuth)
    api_key: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None

    # Data endpoints
    lmp_endpoint: str = "/v1.1/lmp/da"
    rtm_lmp_endpoint: str = "/v1.1/lmp/rt"
    fcm_endpoint: str = "/v1.1/capacity/fcm"
    ftr_endpoint: str = "/v1.1/ftr/auction"

    # Report configurations
    report_configs: Dict[str, Dict[str, Any]] = {
        "lmp": {
            "report_type": "LMP",
            "frequency": "hourly",
            "data_items": ["lmp", "congestion", "losses"],
            "download_format": "csv"
        },
        "rt_lmp": {
            "report_type": "RT_LMP",
            "frequency": "hourly",
            "data_items": ["lmp", "congestion", "losses"],
            "download_format": "csv"
        },
        "fcm": {
            "report_type": "FCM",
            "frequency": "auction",
            "data_items": ["clearing_price", "capacity_obligations"],
            "download_format": "csv"
        },
        "ftr": {
            "report_type": "FTR",
            "frequency": "auction",
            "data_items": ["auction_price", "awarded_quantity"],
            "download_format": "csv"
        }
    }

    # Rate limiting
    requests_per_minute: int = 20
    retry_attempts: int = 3
    retry_delay_seconds: int = 15

    # Data processing
    batch_size: int = 1000
    max_concurrent_requests: int = 2

    # Default parameters
    default_start_hours_back: int = 24
    default_market_run_id: str = "DAM"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if not self.api_key and not (self.username and self.password):
            raise ValueError("ISO-NE connector requires either api_key or username + password")
