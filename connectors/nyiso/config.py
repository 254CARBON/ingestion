"""
NYISO connector configuration.
"""

from __future__ import annotations

from typing import Dict, List, Optional

from ..base.base_connector import ConnectorConfig


class NYISOConnectorConfig(ConnectorConfig):
    """NYISO connector configuration."""

    # API endpoints
    base_url: str = "https://mis.nyiso.com/public/api"
    data_url: str = "https://www.nyiso.com/OASIS/REST"

    # Authentication (API key or OAuth)
    api_key: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None

    # Data endpoints
    lbmp_endpoint: str = "/da_lbmp"
    rtm_lbmp_endpoint: str = "/rt_lbmp"
    icap_endpoint: str = "/icap_demand_curve"
    tcc_endpoint: str = "/tcc_auction_results"

    # Report configurations
    report_configs: Dict[str, Dict[str, Any]] = {
        "lbmp": {
            "report_type": "LBMP",
            "frequency": "hourly",
            "data_items": ["lbmp", "congestion", "losses"],
            "download_format": "csv"
        },
        "rt_lbmp": {
            "report_type": "RT_LBMP",
            "frequency": "hourly",
            "data_items": ["lbmp", "congestion", "losses"],
            "download_format": "csv"
        },
        "icap": {
            "report_type": "ICAP",
            "frequency": "monthly",
            "data_items": ["icap_demand_curve", "capacity_obligations"],
            "download_format": "csv"
        },
        "tcc": {
            "report_type": "TCC",
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
            raise ValueError("NYISO connector requires either api_key or username + password")
