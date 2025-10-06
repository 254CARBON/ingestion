"""
MISO connector configuration.
"""

from typing import Optional
from pydantic import Field
from ..base.base_connector import ConnectorConfig


class MISOConnectorConfig(ConnectorConfig):
    """Configuration for MISO connector."""
    
    description: str = Field("MISO market data connector", description="Connector description")
    tags: list = Field(default_factory=list, description="Connector tags")
    miso_api_key: str = Field(..., description="MISO API key")
    miso_base_url: str = Field("https://api.misoenergy.org", description="MISO API base URL")
    miso_timeout: int = Field(30, description="Request timeout in seconds")
    miso_rate_limit: int = Field(100, description="Rate limit per minute")
    miso_retry_attempts: int = Field(3, description="Number of retry attempts")
    miso_backoff_factor: float = Field(2.0, description="Backoff factor for retries")
    
    class Config:
        """Pydantic configuration."""
        extra = "forbid"
        validate_assignment = True
