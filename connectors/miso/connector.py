"""
MISO connector implementation.

This connector integrates with MISO's API to extract market data including
trade data, curve data, and market prices.
"""

import asyncio
import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional
from uuid import uuid4

import structlog
import aiohttp
from aiohttp import ClientSession, ClientTimeout

from ..base.base_connector import BaseConnector, ConnectorConfig, ExtractionResult, TransformationResult
from ..base.exceptions import ExtractionError, TransformationError
from .extractor import MISOExtractor
from .transform import MISOTransform


class MISOConnector(BaseConnector):
    """
    MISO market data connector.
    
    This connector integrates with MISO's API to extract market data
    including trade data, curve data, and market prices.
    """
    
    def __init__(self, config: ConnectorConfig):
        """
        Initialize the MISO connector.

        Args:
            config: Connector configuration
        """
        super().__init__(config)
        self.base_url = "https://api.misoenergy.org"
        self.session: Optional[ClientSession] = None

        # Initialize extractor and transformer
        self._extractor = MISOExtractor(config)
        self._transformer = MISOTransform(config)

        # API configuration
        self.api_config = {
            "timeout": 30,
            "retry_attempts": 3,
            "retry_delay": 5,
            "rate_limit_delay": 1,
            "max_concurrent_requests": 10
        }

        # Data endpoints
        self.endpoints = {
            "trade_data": "/api/v1/trades",
            "curve_data": "/api/v1/curves",
            "market_prices": "/api/v1/prices",
            "system_status": "/api/v1/status"
        }

        # Data quality metrics
        self.quality_metrics = {
            "total_requests": 0,
            "successful_requests": 0,
            "failed_requests": 0,
            "rate_limit_hits": 0,
            "data_points_extracted": 0
        }
    
    async def extract(self, **kwargs) -> ExtractionResult:
        """
        Extract data from MISO API.

        Args:
            **kwargs: Additional extraction parameters

        Returns:
            ExtractionResult: Extracted data and metadata
        """
        try:
            mode = kwargs.get("mode", "batch")
            self.logger.info(f"Starting MISO data extraction in {mode} mode")

            # For batch mode, use the extractor with parameters
            if mode == "batch" and "start_date" in kwargs and "end_date" in kwargs:
                return await self._extractor.extract_trades(
                    start_date=kwargs["start_date"],
                    end_date=kwargs["end_date"],
                    settlement_point=kwargs.get("settlement_point")
                )

            # Initialize HTTP session for real-time or full extraction
            await self._initialize_session()

            # Extract different data types
            extracted_data = []
            metadata = {
                "extraction_timestamp": datetime.now(timezone.utc).isoformat(),
                "source": "MISO API",
                "extraction_method": "api_client",
                "data_types": []
            }

            # Extract trade data
            trade_data = await self._extract_trade_data()
            if trade_data:
                extracted_data.extend(trade_data)
                metadata["data_types"].append("trade_data")
            
            # Extract curve data
            curve_data = await self._extract_curve_data()
            if curve_data:
                extracted_data.extend(curve_data)
                metadata["data_types"].append("curve_data")
            
            # Extract market prices
            price_data = await self._extract_market_prices()
            if price_data:
                extracted_data.extend(price_data)
                metadata["data_types"].append("market_prices")
            
            # Extract system status
            status_data = await self._extract_system_status()
            if status_data:
                extracted_data.extend(status_data)
                metadata["data_types"].append("system_status")
            
            # Update quality metrics
            self.quality_metrics["data_points_extracted"] += len(extracted_data)
            
            self.logger.info("MISO data extraction completed", 
                           record_count=len(extracted_data),
                           data_types=metadata["data_types"])
            
            return ExtractionResult(
                data=extracted_data,
                metadata=metadata,
                record_count=len(extracted_data)
            )
            
        except Exception as e:
            self.logger.error("Failed to extract MISO data", error=str(e))
            raise ExtractionError(f"MISO extraction failed: {e}") from e
        
        finally:
            await self._cleanup()
    
    async def transform(self, extraction_result: ExtractionResult) -> TransformationResult:
        """
        Transform extracted MISO data.

        Args:
            extraction_result: Result from the extract operation

        Returns:
            TransformationResult: Transformed data and metadata
        """
        # Use the transformer for transformation
        return await self._transformer.transform(extraction_result)
    
    async def _initialize_session(self) -> None:
        """Initialize HTTP session for API requests."""
        try:
            timeout = ClientTimeout(total=self.api_config["timeout"])
            
            self.session = ClientSession(
                timeout=timeout,
                headers={
                    "User-Agent": "254Carbon-Ingestion/1.0",
                    "Accept": "application/json",
                    "Content-Type": "application/json"
                }
            )
            
            self.logger.info("HTTP session initialized successfully")
            
        except Exception as e:
            self.logger.error("Failed to initialize HTTP session", error=str(e))
            raise
    
    async def _extract_trade_data(self) -> List[Dict[str, Any]]:
        """Extract trade data from MISO API."""
        try:
            self.logger.info("Extracting MISO trade data")
            
            # Set up query parameters for trade data
            params = {
                "startDate": (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"),
                "endDate": datetime.now().strftime("%Y-%m-%d"),
                "format": "json"
            }
            
            # Make API request
            trade_data = await self._make_api_request(
                self.endpoints["trade_data"],
                params=params
            )
            
            if trade_data:
                # Add data type identifier
                for record in trade_data:
                    record["data_type"] = "trade"
                    record["source"] = "MISO"
            
            self.logger.info("Trade data extracted successfully", record_count=len(trade_data))
            return trade_data
            
        except Exception as e:
            self.logger.error("Failed to extract trade data", error=str(e))
            return []
    
    async def _extract_curve_data(self) -> List[Dict[str, Any]]:
        """Extract curve data from MISO API."""
        try:
            self.logger.info("Extracting MISO curve data")
            
            # Set up query parameters for curve data
            params = {
                "startDate": (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"),
                "endDate": datetime.now().strftime("%Y-%m-%d"),
                "format": "json"
            }
            
            # Make API request
            curve_data = await self._make_api_request(
                self.endpoints["curve_data"],
                params=params
            )
            
            if curve_data:
                # Add data type identifier
                for record in curve_data:
                    record["data_type"] = "curve"
                    record["source"] = "MISO"
            
            self.logger.info("Curve data extracted successfully", record_count=len(curve_data))
            return curve_data
            
        except Exception as e:
            self.logger.error("Failed to extract curve data", error=str(e))
            return []
    
    async def _extract_market_prices(self) -> List[Dict[str, Any]]:
        """Extract market prices from MISO API."""
        try:
            self.logger.info("Extracting MISO market prices")
            
            # Set up query parameters for market prices
            params = {
                "startDate": (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"),
                "endDate": datetime.now().strftime("%Y-%m-%d"),
                "format": "json"
            }
            
            # Make API request
            price_data = await self._make_api_request(
                self.endpoints["market_prices"],
                params=params
            )
            
            if price_data:
                # Add data type identifier
                for record in price_data:
                    record["data_type"] = "market_price"
                    record["source"] = "MISO"
            
            self.logger.info("Market prices extracted successfully", record_count=len(price_data))
            return price_data
            
        except Exception as e:
            self.logger.error("Failed to extract market prices", error=str(e))
            return []
    
    async def _extract_system_status(self) -> List[Dict[str, Any]]:
        """Extract system status from MISO API."""
        try:
            self.logger.info("Extracting MISO system status")
            
            # Set up query parameters for system status
            params = {
                "startDate": (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"),
                "endDate": datetime.now().strftime("%Y-%m-%d"),
                "format": "json"
            }
            
            # Make API request
            status_data = await self._make_api_request(
                self.endpoints["system_status"],
                params=params
            )
            
            if status_data:
                # Add data type identifier
                for record in status_data:
                    record["data_type"] = "system_status"
                    record["source"] = "MISO"
            
            self.logger.info("System status extracted successfully", record_count=len(status_data))
            return status_data
            
        except Exception as e:
            self.logger.error("Failed to extract system status", error=str(e))
            return []
    
    async def _make_api_request(
        self, 
        endpoint: str, 
        params: Optional[Dict[str, Any]] = None,
        retry_count: int = 0
    ) -> List[Dict[str, Any]]:
        """
        Make API request with retry logic.
        
        Args:
            endpoint: API endpoint
            params: Query parameters
            retry_count: Current retry count
            
        Returns:
            List[Dict[str, Any]]: API response data
        """
        if not self.session:
            raise Exception("HTTP session not initialized")
        
        try:
            self.quality_metrics["total_requests"] += 1
            
            # Apply rate limiting
            await self._rate_limit()
            
            # Make request
            url = f"{self.base_url}{endpoint}"
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    self.quality_metrics["successful_requests"] += 1
                    
                    # Handle different response formats
                    if isinstance(data, list):
                        return data
                    elif isinstance(data, dict):
                        if "data" in data:
                            return data["data"]
                        elif "results" in data:
                            return data["results"]
                        else:
                            return [data]
                    else:
                        return []
                
                elif response.status == 429:  # Rate limited
                    self.quality_metrics["rate_limit_hits"] += 1
                    if retry_count < self.api_config["retry_attempts"]:
                        await asyncio.sleep(self.api_config["retry_delay"] * (2 ** retry_count))
                        return await self._make_api_request(endpoint, params, retry_count + 1)
                    else:
                        raise Exception(f"Rate limited after {self.api_config['retry_attempts']} retries")
                
                else:
                    self.quality_metrics["failed_requests"] += 1
                    raise Exception(f"API request failed with status {response.status}")
        
        except Exception as e:
            self.quality_metrics["failed_requests"] += 1
            self.logger.error("API request failed", 
                            endpoint=endpoint, 
                            error=str(e), 
                            retry_count=retry_count)
            
            if retry_count < self.api_config["retry_attempts"]:
                await asyncio.sleep(self.api_config["retry_delay"] * (2 ** retry_count))
                return await self._make_api_request(endpoint, params, retry_count + 1)
            else:
                raise
    
    async def _transform_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform a single MISO record.
        
        Args:
            record: Raw record to transform
            
        Returns:
            Dict[str, Any]: Transformed record
        """
        try:
            # Create base transformed record
            transformed = {
                "event_id": str(uuid4()),
                "occurred_at": int(datetime.now(timezone.utc).timestamp() * 1000000),
                "tenant_id": "default",
                "schema_version": "1.0.0",
                "producer": "miso-connector",
                "market": "MISO",
                "market_id": "MISO",
                "timezone": "America/Chicago",
                "currency": "USD",
                "unit": "MWh",
                "price_unit": "$/MWh",
                "data_type": record.get("data_type", "unknown"),
                "source": record.get("source", "MISO"),
                "raw_data": record,
                "transformation_timestamp": datetime.now(timezone.utc).isoformat()
            }
            
            # Apply data type specific transformations
            data_type = record.get("data_type", "unknown")
            
            if data_type == "trade":
                transformed.update(self._transform_trade_record(record))
            elif data_type == "curve":
                transformed.update(self._transform_curve_record(record))
            elif data_type == "market_price":
                transformed.update(self._transform_price_record(record))
            elif data_type == "system_status":
                transformed.update(self._transform_status_record(record))
            
            return transformed
            
        except Exception as e:
            self.logger.error("Failed to transform record", error=str(e), record=record)
            raise
    
    def _transform_trade_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Transform trade record."""
        return {
            "trade_id": record.get("trade_id", str(uuid4())),
            "delivery_location": record.get("delivery_location", "unknown"),
            "delivery_date": self._parse_date(record.get("delivery_date")),
            "delivery_hour": self._parse_hour(record.get("delivery_hour")),
            "price": self._parse_float(record.get("price")),
            "quantity": self._parse_float(record.get("quantity")),
            "bid_price": self._parse_float(record.get("bid_price")),
            "offer_price": self._parse_float(record.get("offer_price")),
            "clearing_price": self._parse_float(record.get("clearing_price")),
            "congestion_price": self._parse_float(record.get("congestion_price")),
            "loss_price": self._parse_float(record.get("loss_price"))
        }
    
    def _transform_curve_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Transform curve record."""
        return {
            "curve_type": record.get("curve_type", "unknown"),
            "delivery_date": self._parse_date(record.get("delivery_date")),
            "delivery_hour": self._parse_hour(record.get("delivery_hour")),
            "price": self._parse_float(record.get("price")),
            "quantity": self._parse_float(record.get("quantity"))
        }
    
    def _transform_price_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Transform market price record."""
        return {
            "price_type": record.get("price_type", "unknown"),
            "delivery_date": self._parse_date(record.get("delivery_date")),
            "delivery_hour": self._parse_hour(record.get("delivery_hour")),
            "price": self._parse_float(record.get("price")),
            "quantity": self._parse_float(record.get("quantity"))
        }
    
    def _transform_status_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Transform system status record."""
        return {
            "status_type": record.get("status_type", "unknown"),
            "status_value": record.get("status_value", "unknown"),
            "timestamp": self._parse_timestamp(record.get("timestamp"))
        }
    
    def _parse_date(self, date_str: Any) -> Optional[str]:
        """Parse date string."""
        if not date_str:
            return None
        
        try:
            if isinstance(date_str, str):
                # Try various date formats
                for fmt in ["%Y-%m-%d", "%Y%m%d", "%m/%d/%Y", "%d/%m/%Y"]:
                    try:
                        dt = datetime.strptime(date_str, fmt)
                        return dt.strftime("%Y-%m-%d")
                    except ValueError:
                        continue
            
            return str(date_str)
        except Exception:
            return None
    
    def _parse_hour(self, hour_str: Any) -> Optional[int]:
        """Parse hour string."""
        if not hour_str:
            return None
        
        try:
            if isinstance(hour_str, str):
                return int(hour_str)
            elif isinstance(hour_str, (int, float)):
                return int(hour_str)
        except (ValueError, TypeError):
            pass
        
        return None
    
    def _parse_float(self, value: Any) -> Optional[float]:
        """Parse float value."""
        if value is None:
            return None
        
        try:
            if isinstance(value, str):
                # Remove non-numeric characters except decimal point and minus
                cleaned = ''.join(c for c in value if c.isdigit() or c in '.-')
                if cleaned:
                    return float(cleaned)
            elif isinstance(value, (int, float)):
                return float(value)
        except (ValueError, TypeError):
            pass
        
        return None
    
    def _parse_timestamp(self, timestamp_str: Any) -> Optional[str]:
        """Parse timestamp string."""
        if not timestamp_str:
            return None
        
        try:
            if isinstance(timestamp_str, str):
                # Try various timestamp formats
                for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y%m%dT%H%M%S"]:
                    try:
                        dt = datetime.strptime(timestamp_str, fmt)
                        return dt.isoformat()
                    except ValueError:
                        continue
            
            return str(timestamp_str)
        except Exception:
            return None
    
    async def _rate_limit(self) -> None:
        """Apply rate limiting to avoid being blocked."""
        await asyncio.sleep(self.api_config["rate_limit_delay"])
    
    async def _cleanup(self) -> None:
        """Clean up HTTP session and resources."""
        try:
            if self.session:
                await self.session.close()
                self.session = None
            
            self.logger.info("MISO connector cleanup completed")
            
        except Exception as e:
            self.logger.error("Error during MISO connector cleanup", error=str(e))
    
    def get_health_status(self) -> Dict[str, Any]:
        """Get the health status of the MISO connector."""
        base_status = super().get_health_status()
        base_status.update({
            "quality_metrics": self.quality_metrics,
            "api_config": self.api_config,
            "endpoints": list(self.endpoints.keys())
        })
        return base_status
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get MISO connector metrics."""
        base_metrics = super().get_metrics()
        base_metrics.update({
            "quality_metrics": self.quality_metrics,
            "success_rate": (
                self.quality_metrics["successful_requests"] /
                max(1, self.quality_metrics["total_requests"])
            ),
            "data_extraction_rate": self.quality_metrics["data_points_extracted"]
        })
        return base_metrics

    def get_connector_info(self) -> Dict[str, Any]:
        """Get connector information."""
        return {
            "name": self.config.name,
            "version": self.config.version,
            "market": self.config.market,
            "mode": self.config.mode,
            "endpoints": list(self.endpoints.keys()),
            "extractor_type": "MISOExtractor",
            "transformer_type": "MISOTransform",
            "miso_base_url": self.base_url,
            "transforms_enabled": len(self.config.transforms) if self.config.transforms else 0,
            "kafka_bootstrap_servers": self.config.kafka_bootstrap_servers,
            "schema_registry_url": self.config.schema_registry_url
        }

    def get_extraction_capabilities(self) -> Dict[str, Any]:
        """Get extraction capabilities."""
        return {
            "data_types": ["trade_data", "curve_data", "market_prices", "system_status"],
            "modes": ["batch", "realtime"],
            "formats": ["json", "csv"],
            "incremental": True,
            "rate_limits": {
                "requests_per_minute": 100,
                "concurrent_requests": 10
            },
            "batch_parameters": ["start_date", "end_date", "settlement_point"],
            "realtime_parameters": ["interval_minutes"]
        }

    def get_transformation_capabilities(self) -> Dict[str, Any]:
        """Get transformation capabilities."""
        return {
            "schema_versions": ["1.0.0", "1.1.0"],
            "transforms": ["normalize_fields", "validate_schema", "enrich_metadata"],
            "output_formats": ["avro", "json", "parquet"],
            "config": self.config.dict()
        }

    async def run_batch(self, start_date: str, end_date: str) -> bool:
        """Run batch extraction."""
        try:
            await self.run()
            return True
        except Exception as e:
            self.logger.error(f"Batch run failed: {e}")
            return False

    async def run_realtime(self) -> bool:
        """Run real-time extraction."""
        try:
            await self.run()
            return True
        except Exception as e:
            self.logger.error(f"Real-time run failed: {e}")
            return False