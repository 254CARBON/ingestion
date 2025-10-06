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
    caiso_base_url: str = Field("https://oasis.caiso.com", description="CAISO base URL")
    caiso_timeout: int = Field(30, description="Request timeout in seconds")
    caiso_rate_limit: int = Field(100, description="Rate limit per minute")
    caiso_retry_attempts: int = Field(3, description="Number of retry attempts")
    caiso_backoff_factor: float = Field(2.0, description="Backoff factor for retries")
    caiso_user_agent: str = Field("254Carbon/1.0", description="User agent for requests")
    
    class Config:
        """Pydantic configuration."""
        extra = "forbid"
        validate_assignment = True
