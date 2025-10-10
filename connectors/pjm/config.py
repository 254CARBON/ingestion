"""
PJM connector configuration.
"""

from __future__ import annotations

from typing import Dict, List, Optional

from ..base.base_connector import ConnectorConfig


class PJMConnectorConfig(ConnectorConfig):
    """PJM connector configuration."""

    # API endpoints
    base_url: str = "https://api.pjm.com/api/v1"
    data_miner_url: str = "https://dataminer2.pjm.com"

    # Authentication (API key or OAuth)
    api_key: Optional[str] = None
    client_id: Optional[str] = None
    client_secret: Optional[str] = None

    # Data endpoints
    lmp_endpoint: str = "/da_hrl_lmps"
    rtm_lmp_endpoint: str = "/rt_hrl_lmps"
    rpm_endpoint: str = "/capacity_market_results"
    tcr_endpoint: str = "/tcr_auction_results"
    outages_endpoint: str = "/outages"

    # Report configurations
    report_configs: Dict[str, Dict[str, Any]] = {
        "lmp": {
            "report_type": "LMP",
            "frequency": "hourly",
            "data_items": ["lmp", "mcc", "mlc"],
            "download_format": "csv"
        },
        "rpm": {
            "report_type": "RPM",
            "frequency": "auction",
            "data_items": ["clearing_price", "capacity_obligations"],
            "download_format": "csv"
        },
        "tcr": {
            "report_type": "TCR",
            "frequency": "auction",
            "data_items": ["auction_price", "awarded_quantity"],
            "download_format": "csv"
        },
        "outages": {
            "report_type": "OUTAGES",
            "frequency": "real_time",
            "data_items": ["outage_start", "outage_end", "capacity_mw"],
            "download_format": "csv"
        }
    }

    # Rate limiting
    requests_per_minute: int = 30
    retry_attempts: int = 3
    retry_delay_seconds: int = 10

    # Data processing
    batch_size: int = 1000
    max_concurrent_requests: int = 3

    # Default parameters
    default_start_hours_back: int = 24
    default_market_run_id: str = "DAM"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if not self.api_key and not (self.client_id and self.client_secret):
            raise ValueError("PJM connector requires either api_key or client_id + client_secret")
