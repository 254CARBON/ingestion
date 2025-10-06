"""
CAISO connector implementation.

This connector scrapes market data from oasis.caiso.com without requiring an API key.
It handles session management, anti-scraping measures, and data extraction.
"""

import asyncio
import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional
from uuid import uuid4

import structlog
from bs4 import BeautifulSoup
import aiohttp
from aiohttp import ClientSession, ClientTimeout
from playwright.async_api import async_playwright, Browser, Page

from ..base.base_connector import BaseConnector, ConnectorConfig, ExtractionResult, TransformationResult
from ..base.exceptions import ExtractionError, TransformationError


class CAISOConnector(BaseConnector):
    """
    CAISO market data connector.
    
    This connector scrapes data from oasis.caiso.com without requiring an API key.
    It handles various data types including trade data, curve data, and market prices.
    """
    
    def __init__(self, config: ConnectorConfig):
        """
        Initialize the CAISO connector.
        
        Args:
            config: Connector configuration
        """
        super().__init__(config)
        self.base_url = "https://oasis.caiso.com"
        self.session: Optional[ClientSession] = None
        self.browser: Optional[Browser] = None
        self.page: Optional[Page] = None
        
        # Scraping configuration
        self.scraping_config = {
            "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "timeout": 30,
            "retry_attempts": 3,
            "retry_delay": 5,
            "rate_limit_delay": 2,
            "max_concurrent_requests": 5
        }
        
        # Data endpoints
        self.endpoints = {
            "trade_data": "/oasisapi/GroupQuery",
            "curve_data": "/oasisapi/GroupQuery",
            "market_prices": "/oasisapi/GroupQuery",
            "system_status": "/oasisapi/GroupQuery"
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
        Extract data from CAISO OASIS.
        
        Args:
            **kwargs: Additional extraction parameters
            
        Returns:
            ExtractionResult: Extracted data and metadata
        """
        try:
            self.logger.info("Starting CAISO data extraction")
            
            # Initialize browser session
            await self._initialize_browser()
            
            # Extract different data types
            extracted_data = []
            metadata = {
                "extraction_timestamp": datetime.now(timezone.utc).isoformat(),
                "source": "CAISO OASIS",
                "extraction_method": "web_scraping",
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
            
            self.logger.info("CAISO data extraction completed", 
                           record_count=len(extracted_data),
                           data_types=metadata["data_types"])
            
            return ExtractionResult(
                data=extracted_data,
                metadata=metadata,
                record_count=len(extracted_data)
            )
            
        except Exception as e:
            self.logger.error("Failed to extract CAISO data", error=str(e))
            raise ExtractionError(f"CAISO extraction failed: {e}") from e
        
        finally:
            await self._cleanup()
    
    async def transform(self, extraction_result: ExtractionResult) -> TransformationResult:
        """
        Transform extracted CAISO data.
        
        Args:
            extraction_result: Result from the extract operation
            
        Returns:
            TransformationResult: Transformed data and metadata
        """
        try:
            self.logger.info("Starting CAISO data transformation", 
                           input_records=extraction_result.record_count)
            
            transformed_data = []
            validation_errors = []
            
            for record in extraction_result.data:
                try:
                    # Apply CAISO-specific transformations
                    transformed_record = await self._transform_record(record)
                    transformed_data.append(transformed_record)
                    
                except Exception as e:
                    error_msg = f"Failed to transform record: {str(e)}"
                    validation_errors.append(error_msg)
                    self.logger.warning("Record transformation failed", 
                                      error=error_msg, 
                                      record_id=record.get("id"))
            
            # Create transformation metadata
            metadata = {
                **extraction_result.metadata,
                "transformation_timestamp": datetime.now(timezone.utc).isoformat(),
                "transformation_method": "caiso_specific",
                "validation_errors": validation_errors,
                "success_rate": len(transformed_data) / max(1, extraction_result.record_count)
            }
            
            self.logger.info("CAISO data transformation completed", 
                           output_records=len(transformed_data),
                           validation_errors=len(validation_errors))
            
            return TransformationResult(
                data=transformed_data,
                metadata=metadata,
                record_count=len(transformed_data),
                validation_errors=validation_errors
            )
            
        except Exception as e:
            self.logger.error("Failed to transform CAISO data", error=str(e))
            raise TransformationError(f"CAISO transformation failed: {e}") from e
    
    async def _initialize_browser(self) -> None:
        """Initialize browser session for web scraping."""
        try:
            playwright = await async_playwright().start()
            self.browser = await playwright.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-gpu",
                    "--disable-web-security",
                    "--disable-features=VizDisplayCompositor"
                ]
            )
            
            self.page = await self.browser.new_page()
            
            # Set user agent and viewport
            await self.page.set_user_agent(self.scraping_config["user_agent"])
            await self.page.set_viewport_size({"width": 1920, "height": 1080})
            
            # Set up request interception for rate limiting
            await self.page.route("**/*", self._handle_request)
            
            self.logger.info("Browser session initialized successfully")
            
        except Exception as e:
            self.logger.error("Failed to initialize browser session", error=str(e))
            raise
    
    async def _handle_request(self, route) -> None:
        """Handle request routing for rate limiting and anti-detection."""
        try:
            # Add random delay to avoid detection
            await asyncio.sleep(0.1)
            
            # Continue with the request
            await route.continue_()
            
        except Exception as e:
            self.logger.warning("Request handling failed", error=str(e))
            await route.abort()
    
    async def _extract_trade_data(self) -> List[Dict[str, Any]]:
        """Extract trade data from CAISO OASIS."""
        try:
            self.logger.info("Extracting CAISO trade data")
            
            # Navigate to trade data page
            trade_url = f"{self.base_url}/oasisapi/GroupQuery"
            
            # Set up query parameters for trade data
            query_params = {
                "groupType": "TRD",
                "resultFormat": "JSON",
                "queryname": "TRD_LMP",
                "startdatetime": (datetime.now() - timedelta(days=1)).strftime("%Y%m%dT00:00-0000"),
                "enddatetime": datetime.now().strftime("%Y%m%dT23:59-0000")
            }
            
            # Make request with rate limiting
            await self._rate_limit()
            
            response = await self.page.goto(
                f"{trade_url}?{'&'.join([f'{k}={v}' for k, v in query_params.items()])}"
            )
            
            if not response or response.status != 200:
                raise Exception(f"Failed to fetch trade data: {response.status if response else 'No response'}")
            
            # Wait for data to load
            await self.page.wait_for_timeout(2000)
            
            # Extract data from page
            content = await self.page.content()
            soup = BeautifulSoup(content, 'html.parser')
            
            # Parse trade data (simplified - actual implementation would be more complex)
            trade_data = []
            
            # Look for data tables or JSON responses
            data_scripts = soup.find_all('script', type='application/json')
            for script in data_scripts:
                try:
                    data = json.loads(script.string)
                    if isinstance(data, list):
                        trade_data.extend(data)
                    elif isinstance(data, dict) and 'data' in data:
                        trade_data.extend(data['data'])
                except (json.JSONDecodeError, TypeError):
                    continue
            
            # If no JSON data found, try to parse HTML tables
            if not trade_data:
                tables = soup.find_all('table')
                for table in tables:
                    rows = table.find_all('tr')
                    headers = [th.get_text().strip() for th in rows[0].find_all(['th', 'td'])]
                    
                    for row in rows[1:]:
                        cells = [td.get_text().strip() for td in row.find_all(['td', 'th'])]
                        if len(cells) == len(headers):
                            trade_record = dict(zip(headers, cells))
                            trade_record['data_type'] = 'trade'
                            trade_record['source'] = 'CAISO'
                            trade_data.append(trade_record)
            
            self.logger.info("Trade data extracted successfully", record_count=len(trade_data))
            return trade_data
            
        except Exception as e:
            self.logger.error("Failed to extract trade data", error=str(e))
            return []
    
    async def _extract_curve_data(self) -> List[Dict[str, Any]]:
        """Extract curve data from CAISO OASIS."""
        try:
            self.logger.info("Extracting CAISO curve data")
            
            # Similar implementation to trade data but for curves
            curve_data = []
            
            # Navigate to curve data page
            curve_url = f"{self.base_url}/oasisapi/GroupQuery"
            
            # Set up query parameters for curve data
            query_params = {
                "groupType": "CURVE",
                "resultFormat": "JSON",
                "queryname": "CURVE_LMP",
                "startdatetime": (datetime.now() - timedelta(days=1)).strftime("%Y%m%dT00:00-0000"),
                "enddatetime": datetime.now().strftime("%Y%m%dT23:59-0000")
            }
            
            # Make request with rate limiting
            await self._rate_limit()
            
            response = await self.page.goto(
                f"{curve_url}?{'&'.join([f'{k}={v}' for k, v in query_params.items()])}"
            )
            
            if not response or response.status != 200:
                raise Exception(f"Failed to fetch curve data: {response.status if response else 'No response'}")
            
            # Wait for data to load
            await self.page.wait_for_timeout(2000)
            
            # Extract data from page
            content = await self.page.content()
            soup = BeautifulSoup(content, 'html.parser')
            
            # Parse curve data (simplified)
            data_scripts = soup.find_all('script', type='application/json')
            for script in data_scripts:
                try:
                    data = json.loads(script.string)
                    if isinstance(data, list):
                        for item in data:
                            item['data_type'] = 'curve'
                            item['source'] = 'CAISO'
                        curve_data.extend(data)
                    elif isinstance(data, dict) and 'data' in data:
                        for item in data['data']:
                            item['data_type'] = 'curve'
                            item['source'] = 'CAISO'
                        curve_data.extend(data['data'])
                except (json.JSONDecodeError, TypeError):
                    continue
            
            self.logger.info("Curve data extracted successfully", record_count=len(curve_data))
            return curve_data
            
        except Exception as e:
            self.logger.error("Failed to extract curve data", error=str(e))
            return []
    
    async def _extract_market_prices(self) -> List[Dict[str, Any]]:
        """Extract market prices from CAISO OASIS."""
        try:
            self.logger.info("Extracting CAISO market prices")
            
            # Similar implementation for market prices
            price_data = []
            
            # Navigate to market prices page
            price_url = f"{self.base_url}/oasisapi/GroupQuery"
            
            # Set up query parameters for market prices
            query_params = {
                "groupType": "PRICE",
                "resultFormat": "JSON",
                "queryname": "PRICE_LMP",
                "startdatetime": (datetime.now() - timedelta(days=1)).strftime("%Y%m%dT00:00-0000"),
                "enddatetime": datetime.now().strftime("%Y%m%dT23:59-0000")
            }
            
            # Make request with rate limiting
            await self._rate_limit()
            
            response = await self.page.goto(
                f"{price_url}?{'&'.join([f'{k}={v}' for k, v in query_params.items()])}"
            )
            
            if not response or response.status != 200:
                raise Exception(f"Failed to fetch market prices: {response.status if response else 'No response'}")
            
            # Wait for data to load
            await self.page.wait_for_timeout(2000)
            
            # Extract data from page
            content = await self.page.content()
            soup = BeautifulSoup(content, 'html.parser')
            
            # Parse market price data (simplified)
            data_scripts = soup.find_all('script', type='application/json')
            for script in data_scripts:
                try:
                    data = json.loads(script.string)
                    if isinstance(data, list):
                        for item in data:
                            item['data_type'] = 'market_price'
                            item['source'] = 'CAISO'
                        price_data.extend(data)
                    elif isinstance(data, dict) and 'data' in data:
                        for item in data['data']:
                            item['data_type'] = 'market_price'
                            item['source'] = 'CAISO'
                        price_data.extend(data['data'])
                except (json.JSONDecodeError, TypeError):
                    continue
            
            self.logger.info("Market prices extracted successfully", record_count=len(price_data))
            return price_data
            
        except Exception as e:
            self.logger.error("Failed to extract market prices", error=str(e))
            return []
    
    async def _extract_system_status(self) -> List[Dict[str, Any]]:
        """Extract system status from CAISO OASIS."""
        try:
            self.logger.info("Extracting CAISO system status")
            
            # Similar implementation for system status
            status_data = []
            
            # Navigate to system status page
            status_url = f"{self.base_url}/oasisapi/GroupQuery"
            
            # Set up query parameters for system status
            query_params = {
                "groupType": "STATUS",
                "resultFormat": "JSON",
                "queryname": "STATUS_SYSTEM",
                "startdatetime": (datetime.now() - timedelta(days=1)).strftime("%Y%m%dT00:00-0000"),
                "enddatetime": datetime.now().strftime("%Y%m%dT23:59-0000")
            }
            
            # Make request with rate limiting
            await self._rate_limit()
            
            response = await self.page.goto(
                f"{status_url}?{'&'.join([f'{k}={v}' for k, v in query_params.items()])}"
            )
            
            if not response or response.status != 200:
                raise Exception(f"Failed to fetch system status: {response.status if response else 'No response'}")
            
            # Wait for data to load
            await self.page.wait_for_timeout(2000)
            
            # Extract data from page
            content = await self.page.content()
            soup = BeautifulSoup(content, 'html.parser')
            
            # Parse system status data (simplified)
            data_scripts = soup.find_all('script', type='application/json')
            for script in data_scripts:
                try:
                    data = json.loads(script.string)
                    if isinstance(data, list):
                        for item in data:
                            item['data_type'] = 'system_status'
                            item['source'] = 'CAISO'
                        status_data.extend(data)
                    elif isinstance(data, dict) and 'data' in data:
                        for item in data['data']:
                            item['data_type'] = 'system_status'
                            item['source'] = 'CAISO'
                        status_data.extend(data['data'])
                except (json.JSONDecodeError, TypeError):
                    continue
            
            self.logger.info("System status extracted successfully", record_count=len(status_data))
            return status_data
            
        except Exception as e:
            self.logger.error("Failed to extract system status", error=str(e))
            return []
    
    async def _transform_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform a single CAISO record.
        
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
                "producer": "caiso-connector",
                "market": "CAISO",
                "market_id": "CAISO",
                "timezone": "America/Los_Angeles",
                "currency": "USD",
                "unit": "MWh",
                "price_unit": "$/MWh",
                "data_type": record.get("data_type", "unknown"),
                "source": record.get("source", "CAISO"),
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
        await asyncio.sleep(self.scraping_config["rate_limit_delay"])
    
    async def _cleanup(self) -> None:
        """Clean up browser session and resources."""
        try:
            if self.page:
                await self.page.close()
                self.page = None
            
            if self.browser:
                await self.browser.close()
                self.browser = None
            
            if self.session:
                await self.session.close()
                self.session = None
            
            self.logger.info("CAISO connector cleanup completed")
            
        except Exception as e:
            self.logger.error("Error during CAISO connector cleanup", error=str(e))
    
    def get_health_status(self) -> Dict[str, Any]:
        """Get the health status of the CAISO connector."""
        base_status = super().get_health_status()
        base_status.update({
            "quality_metrics": self.quality_metrics,
            "scraping_config": self.scraping_config,
            "endpoints": list(self.endpoints.keys())
        })
        return base_status
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get CAISO connector metrics."""
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