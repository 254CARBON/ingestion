"""
CAISO connector configuration.
"""

from typing import Optional
from pydantic import Field
from ..base.base_connector import ConnectorConfig


class CAISOConnectorConfig(ConnectorConfig):
    """Configuration for CAISO connector."""
    
    description: str = Field("CAISO market data connector", description="Connector description")
    tags: list = Field(default_factory=list, description="Connector tags")
    caiso_api_key: Optional[str] = Field(None, description="CAISO API key (optional)")
    caiso_base_url: str = Field("https://oasis.caiso.com/oasisapi", description="CAISO OASIS base URL")
    caiso_timeout: int = Field(30, description="Request timeout in seconds")
    caiso_rate_limit: int = Field(100, description="Rate limit per minute")
    caiso_retry_attempts: int = Field(3, description="Number of retry attempts")
    caiso_backoff_factor: float = Field(2.0, description="Backoff factor for retries")
    caiso_user_agent: str = Field("254Carbon/1.0", description="User agent for requests")
    # OASIS-specific parameters
    oasis_version: str = Field("12", description="OASIS report version parameter")
    oasis_result_format: str = Field("6", description="OASIS resultformat (6=CSV in ZIP)")
    default_market_run_id: str = Field("DAM", description="Default MARKET_RUN_ID for LMP queries")
    default_node: Optional[str] = Field(None, description="Default node for queries (e.g., TH_SP15_GEN-APND)")
    
    class Config:
        """Pydantic configuration."""
        extra = "forbid"
        validate_assignment = True
