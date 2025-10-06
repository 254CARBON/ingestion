"""
MISO data extractor implementation.

This module provides the MISO extractor for pulling data from MISO APIs
and external data sources.
"""

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import uuid4

import httpx
import structlog
from pydantic import BaseModel, Field

from ..base import BaseConnector, ExtractionResult
from ..base.exceptions import ExtractionError, NetworkError, AuthenticationError
from ..base.utils import setup_logging, retry_with_backoff


class MISOAuthConfig(BaseModel):
    """MISO authentication configuration."""
    
    api_key: str = Field(..., description="MISO API key")
    base_url: str = Field(..., description="MISO API base URL")
    timeout: int = Field(30, description="Request timeout in seconds")
    rate_limit: int = Field(100, description="Requests per minute")


class MISOApiClient:
    """MISO API client for data extraction."""
    
    def __init__(self, auth_config: MISOAuthConfig):
        """
        Initialize the MISO API client.
        
        Args:
            auth_config: MISO authentication configuration
        """
        self.auth_config = auth_config
        self.logger = setup_logging(self.__class__.__name__)
        self._client: Optional[httpx.AsyncClient] = None
    
    async def __aenter__(self):
        """Async context manager entry."""
        self._client = httpx.AsyncClient(
            base_url=self.auth_config.base_url,
            timeout=self.auth_config.timeout,
            headers={
                "Authorization": f"Bearer {self.auth_config.api_key}",
                "Content-Type": "application/json",
                "User-Agent": "254Carbon-Ingestion/1.0"
            }
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self._client:
            await self._client.aclose()
    
    async def get_trade_data(
        self,
        start_date: str,
        end_date: str,
        settlement_point: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get MISO trade data for a date range.
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            settlement_point: Optional settlement point filter
            
        Returns:
            List[Dict[str, Any]]: Trade data records
            
        Raises:
            ExtractionError: If extraction fails
        """
        if not self._client:
            raise ExtractionError("API client not initialized")
        
        try:
            params = {
                "startDate": start_date,
                "endDate": end_date,
                "format": "json"
            }
            
            if settlement_point:
                params["settlementPoint"] = settlement_point
            
            self.logger.info(f"Fetching MISO trade data: {params}")
            
            response = await self._client.get("/api/trade-data", params=params)
            response.raise_for_status()
            
            data = response.json()
            
            if not isinstance(data, list):
                raise ExtractionError(f"Unexpected response format: {type(data)}")
            
            self.logger.info(f"Retrieved {len(data)} trade records")
            return data
            
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 401:
                raise AuthenticationError(f"MISO API authentication failed: {e}")
            elif e.response.status_code == 429:
                raise ExtractionError(f"MISO API rate limit exceeded: {e}")
            else:
                raise ExtractionError(f"MISO API error: {e}")
        except httpx.RequestError as e:
            raise NetworkError(f"Network error accessing MISO API: {e}")
        except Exception as e:
            raise ExtractionError(f"Unexpected error extracting MISO data: {e}")
    
    async def get_realtime_data(self) -> List[Dict[str, Any]]:
        """
        Get MISO real-time market data.
        
        Returns:
            List[Dict[str, Any]]: Real-time data records
            
        Raises:
            ExtractionError: If extraction fails
        """
        if not self._client:
            raise ExtractionError("API client not initialized")
        
        try:
            self.logger.info("Fetching MISO real-time data")
            
            response = await self._client.get("/api/realtime-data")
            response.raise_for_status()
            
            data = response.json()
            
            if not isinstance(data, list):
                raise ExtractionError(f"Unexpected response format: {type(data)}")
            
            self.logger.info(f"Retrieved {len(data)} real-time records")
            return data
            
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 401:
                raise AuthenticationError(f"MISO API authentication failed: {e}")
            elif e.response.status_code == 429:
                raise ExtractionError(f"MISO API rate limit exceeded: {e}")
            else:
                raise ExtractionError(f"MISO API error: {e}")
        except httpx.RequestError as e:
            raise NetworkError(f"Network error accessing MISO API: {e}")
        except Exception as e:
            raise ExtractionError(f"Unexpected error extracting MISO real-time data: {e}")


class MISOExtractor:
    """MISO data extractor."""
    
    def __init__(self, auth_config: MISOAuthConfig):
        """
        Initialize the MISO extractor.
        
        Args:
            auth_config: MISO authentication configuration
        """
        self.auth_config = auth_config
        self.logger = setup_logging(self.__class__.__name__)
    
    async def extract_trades(
        self,
        start_date: str,
        end_date: str,
        settlement_point: Optional[str] = None
    ) -> ExtractionResult:
        """
        Extract MISO trade data.
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            settlement_point: Optional settlement point filter
            
        Returns:
            ExtractionResult: Extracted data and metadata
        """
        try:
            async with MISOApiClient(self.auth_config) as client:
                raw_data = await client.get_trade_data(
                    start_date, end_date, settlement_point
                )
            
            # Transform raw data into standardized format
            processed_data = []
            for record in raw_data:
                processed_record = {
                    "event_id": str(uuid4()),
                    "trace_id": None,
                    "occurred_at": int(datetime.now(timezone.utc).timestamp() * 1_000_000),
                    "tenant_id": "default",
                    "schema_version": "1.0.0",
                    "producer": "miso-connector",
                    "trade_id": record.get("tradeId"),
                    "settlement_point": record.get("settlementPoint"),
                    "hub": record.get("hub"),
                    "price": record.get("price"),
                    "quantity": record.get("quantity"),
                    "trade_date": record.get("tradeDate"),
                    "trade_hour": record.get("tradeHour"),
                    "trade_type": record.get("tradeType"),
                    "product_type": record.get("productType"),
                    "delivery_date": record.get("deliveryDate"),
                    "delivery_hour": record.get("deliveryHour"),
                    "bid_price": record.get("bidPrice"),
                    "offer_price": record.get("offerPrice"),
                    "clearing_price": record.get("clearingPrice"),
                    "congestion_price": record.get("congestionPrice"),
                    "loss_price": record.get("lossPrice"),
                    "raw_data": json.dumps(record),
                    "extraction_metadata": {
                        "extraction_time": datetime.now(timezone.utc).isoformat(),
                        "source": "miso-api",
                        "version": "1.2.0"
                    }
                }
                processed_data.append(processed_record)
            
            return ExtractionResult(
                data=processed_data,
                metadata={
                    "start_date": start_date,
                    "end_date": end_date,
                    "settlement_point": settlement_point,
                    "extraction_time": datetime.now(timezone.utc).isoformat(),
                    "source": "miso-api",
                    "version": "1.2.0"
                },
                record_count=len(processed_data)
            )
            
        except Exception as e:
            self.logger.error(f"Failed to extract MISO trade data: {e}")
            raise ExtractionError(f"MISO trade data extraction failed: {e}") from e
    
    async def extract_realtime(self) -> ExtractionResult:
        """
        Extract MISO real-time data.
        
        Returns:
            ExtractionResult: Extracted data and metadata
        """
        try:
            async with MISOApiClient(self.auth_config) as client:
                raw_data = await client.get_realtime_data()
            
            # Transform raw data into standardized format
            processed_data = []
            for record in raw_data:
                processed_record = {
                    "event_id": str(uuid4()),
                    "trace_id": None,
                    "occurred_at": int(datetime.now(timezone.utc).timestamp() * 1_000_000),
                    "tenant_id": "default",
                    "schema_version": "1.0.0",
                    "producer": "miso-connector",
                    "trade_id": record.get("tradeId"),
                    "settlement_point": record.get("settlementPoint"),
                    "hub": record.get("hub"),
                    "price": record.get("price"),
                    "quantity": record.get("quantity"),
                    "trade_date": record.get("tradeDate"),
                    "trade_hour": record.get("tradeHour"),
                    "trade_type": record.get("tradeType"),
                    "product_type": record.get("productType"),
                    "delivery_date": record.get("deliveryDate"),
                    "delivery_hour": record.get("deliveryHour"),
                    "bid_price": record.get("bidPrice"),
                    "offer_price": record.get("offerPrice"),
                    "clearing_price": record.get("clearingPrice"),
                    "congestion_price": record.get("congestionPrice"),
                    "loss_price": record.get("lossPrice"),
                    "raw_data": json.dumps(record),
                    "extraction_metadata": {
                        "extraction_time": datetime.now(timezone.utc).isoformat(),
                        "source": "miso-api",
                        "version": "1.2.0",
                        "mode": "realtime"
                    }
                }
                processed_data.append(processed_record)
            
            return ExtractionResult(
                data=processed_data,
                metadata={
                    "extraction_time": datetime.now(timezone.utc).isoformat(),
                    "source": "miso-api",
                    "version": "1.2.0",
                    "mode": "realtime"
                },
                record_count=len(processed_data)
            )
            
        except Exception as e:
            self.logger.error(f"Failed to extract MISO real-time data: {e}")
            raise ExtractionError(f"MISO real-time data extraction failed: {e}") from e
