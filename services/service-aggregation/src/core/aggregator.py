"""
Core aggregation service implementation.

This module provides the main aggregation logic for generating OHLC bars,
rolling metrics, and curve pre-staging from enriched market data.
"""

import asyncio
import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4
from collections import defaultdict, deque
import statistics

import structlog
import yaml
from pydantic import BaseModel, Field


class OHLCBar(BaseModel):
    """OHLC bar data structure."""
    
    market: str = Field(..., description="Market identifier")
    delivery_location: str = Field(..., description="Delivery location")
    delivery_date: str = Field(..., description="Delivery date")
    bar_type: str = Field(..., description="Bar type (daily, hourly, etc.)")
    bar_size: str = Field(..., description="Bar size (1D, 1H, etc.)")
    open_price: float = Field(..., description="Opening price")
    high_price: float = Field(..., description="Highest price")
    low_price: float = Field(..., description="Lowest price")
    close_price: float = Field(..., description="Closing price")
    volume: float = Field(..., description="Total volume")
    vwap: float = Field(..., description="Volume-weighted average price")
    trade_count: int = Field(..., description="Number of trades")
    price_currency: str = Field(..., description="Price currency")
    quantity_unit: str = Field(..., description="Quantity unit")
    aggregation_timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    aggregation_date: str = Field(default_factory=lambda: datetime.now(timezone.utc).strftime("%Y-%m-%d"))


class RollingMetric(BaseModel):
    """Rolling metric data structure."""
    
    market: str = Field(..., description="Market identifier")
    delivery_location: str = Field(..., description="Delivery location")
    delivery_date: str = Field(..., description="Delivery date")
    metric_type: str = Field(..., description="Type of metric")
    window_size: str = Field(..., description="Window size")
    metric_value: float = Field(..., description="Metric value")
    metric_unit: str = Field(..., description="Metric unit")
    sample_count: int = Field(..., description="Number of samples")
    calculation_timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    calculation_date: str = Field(default_factory=lambda: datetime.now(timezone.utc).strftime("%Y-%m-%d"))


class CurvePreStage(BaseModel):
    """Curve pre-stage data structure."""
    
    market: str = Field(..., description="Market identifier")
    curve_type: str = Field(..., description="Type of curve")
    delivery_date: str = Field(..., description="Delivery date")
    delivery_hour: int = Field(..., description="Delivery hour")
    price: float = Field(..., description="Price value")
    quantity: float = Field(..., description="Quantity value")
    price_currency: str = Field(..., description="Price currency")
    quantity_unit: str = Field(..., description="Quantity unit")
    curve_metadata: Dict[str, Any] = Field(default_factory=dict, description="Curve metadata")
    aggregation_timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    aggregation_date: str = Field(default_factory=lambda: datetime.now(timezone.utc).strftime("%Y-%m-%d"))


class AggregationResult(BaseModel):
    """Result of aggregation operation."""
    
    ohlc_bars: List[OHLCBar] = Field(default_factory=list, description="Generated OHLC bars")
    rolling_metrics: List[RollingMetric] = Field(default_factory=list, description="Generated rolling metrics")
    curve_prestage: List[CurvePreStage] = Field(default_factory=list, description="Generated curve pre-stage")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Aggregation metadata")
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class AggregationService:
    """
    Core aggregation service for market data.
    
    This service handles the aggregation of enriched market data into
    OHLC bars, rolling metrics, and curve pre-staging.
    """
    
    def __init__(self, config_path: str = "configs/aggregation_policies.yaml"):
        """
        Initialize the aggregation service.
        
        Args:
            config_path: Path to aggregation policies configuration
        """
        self.config_path = config_path
        self.logger = structlog.get_logger(__name__)
        
        # Load configuration
        self.config = self._load_config()
        
        # Aggregation windows and buffers
        self.ohlc_buffers: Dict[str, Dict[str, List[Dict[str, Any]]]] = defaultdict(lambda: defaultdict(list))
        self.rolling_windows: Dict[str, Dict[str, deque]] = defaultdict(lambda: defaultdict(lambda: deque(maxlen=1000)))
        self.curve_buffers: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        
        # Aggregation statistics
        self.stats = {
            "total_aggregations": 0,
            "successful_aggregations": 0,
            "failed_aggregations": 0,
            "ohlc_bars_generated": 0,
            "rolling_metrics_generated": 0,
            "curve_prestage_generated": 0,
            "buffer_sizes": {}
        }
    
    def _load_config(self) -> Dict[str, Any]:
        """
        Load aggregation configuration.
        
        Returns:
            Dict[str, Any]: Configuration data
        """
        try:
            with open(self.config_path, 'r') as f:
                config = yaml.safe_load(f)
            self.logger.info("Aggregation configuration loaded", config_path=self.config_path)
            return config
        except Exception as e:
            self.logger.error("Failed to load aggregation configuration", 
                            error=str(e), config_path=self.config_path)
            return {}
    
    async def aggregate(self, enriched_data: Dict[str, Any]) -> AggregationResult:
        """
        Aggregate enriched market data.
        
        Args:
            enriched_data: Enriched data to aggregate
            
        Returns:
            AggregationResult: Aggregation result
        """
        try:
            self.stats["total_aggregations"] += 1
            
            # Initialize result
            result = AggregationResult()
            
            # Generate OHLC bars
            ohlc_bars = await self._generate_ohlc_bars(enriched_data)
            result.ohlc_bars = ohlc_bars
            self.stats["ohlc_bars_generated"] += len(ohlc_bars)
            
            # Generate rolling metrics
            rolling_metrics = await self._generate_rolling_metrics(enriched_data)
            result.rolling_metrics = rolling_metrics
            self.stats["rolling_metrics_generated"] += len(rolling_metrics)
            
            # Generate curve pre-stage
            curve_prestage = await self._generate_curve_prestage(enriched_data)
            result.curve_prestage = curve_prestage
            self.stats["curve_prestage_generated"] += len(curve_prestage)
            
            # Update metadata
            result.metadata = {
                "aggregation_method": "incremental",
                "ohlc_bars_count": len(ohlc_bars),
                "rolling_metrics_count": len(rolling_metrics),
                "curve_prestage_count": len(curve_prestage),
                "aggregation_timestamp": datetime.now(timezone.utc).isoformat(),
                "buffer_sizes": dict(self.stats["buffer_sizes"])
            }
            
            # Update statistics
            self.stats["successful_aggregations"] += 1
            
            self.logger.info("Data aggregated successfully", 
                           ohlc_bars=len(ohlc_bars),
                           rolling_metrics=len(rolling_metrics),
                           curve_prestage=len(curve_prestage))
            
            return result
            
        except Exception as e:
            self.stats["failed_aggregations"] += 1
            self.logger.error("Failed to aggregate data", error=str(e), enriched_data=enriched_data)
            
            # Return error result
            return AggregationResult(
                metadata={"error": str(e)}
            )
    
    async def _generate_ohlc_bars(self, data: Dict[str, Any]) -> List[OHLCBar]:
        """
        Generate OHLC bars from enriched data.
        
        Args:
            data: Enriched data
            
        Returns:
            List[OHLCBar]: Generated OHLC bars
        """
        try:
            ohlc_bars = []
            
            # Extract key fields
            market = data.get("market", "UNKNOWN")
            delivery_location = data.get("delivery_location", "unknown")
            delivery_date = data.get("delivery_date")
            delivery_hour = data.get("delivery_hour")
            price = data.get("price")
            quantity = data.get("quantity")
            
            if not all([market, delivery_location, delivery_date, price is not None, quantity is not None]):
                return ohlc_bars
            
            # Add to buffer
            buffer_key = f"{market}_{delivery_location}_{delivery_date}"
            self.ohlc_buffers[market][buffer_key].append({
                "delivery_hour": delivery_hour,
                "price": float(price),
                "quantity": float(quantity),
                "timestamp": datetime.now(timezone.utc)
            })
            
            # Update buffer size stats
            self.stats["buffer_sizes"][buffer_key] = len(self.ohlc_buffers[market][buffer_key])
            
            # Generate daily OHLC bar
            daily_bar = await self._create_daily_ohlc_bar(market, delivery_location, delivery_date)
            if daily_bar:
                ohlc_bars.append(daily_bar)
            
            # Generate hourly OHLC bar if hour is specified
            if delivery_hour is not None:
                hourly_bar = await self._create_hourly_ohlc_bar(market, delivery_location, delivery_date, delivery_hour)
                if hourly_bar:
                    ohlc_bars.append(hourly_bar)
            
            return ohlc_bars
            
        except Exception as e:
            self.logger.error("Failed to generate OHLC bars", error=str(e))
            return []
    
    async def _create_daily_ohlc_bar(self, market: str, delivery_location: str, delivery_date: str) -> Optional[OHLCBar]:
        """Create daily OHLC bar."""
        try:
            buffer_key = f"{market}_{delivery_location}_{delivery_date}"
            trades = self.ohlc_buffers[market][buffer_key]
            
            if not trades:
                return None
            
            # Calculate OHLC values
            prices = [trade["price"] for trade in trades]
            quantities = [trade["quantity"] for trade in trades]
            
            open_price = prices[0] if prices else 0.0
            high_price = max(prices) if prices else 0.0
            low_price = min(prices) if prices else 0.0
            close_price = prices[-1] if prices else 0.0
            volume = sum(quantities) if quantities else 0.0
            
            # Calculate VWAP
            if volume > 0:
                vwap = sum(p * q for p, q in zip(prices, quantities)) / volume
            else:
                vwap = 0.0
            
            return OHLCBar(
                market=market,
                delivery_location=delivery_location,
                delivery_date=delivery_date,
                bar_type="daily",
                bar_size="1D",
                open_price=open_price,
                high_price=high_price,
                low_price=low_price,
                close_price=close_price,
                volume=volume,
                vwap=vwap,
                trade_count=len(trades),
                price_currency="USD",
                quantity_unit="MWh"
            )
            
        except Exception as e:
            self.logger.error("Failed to create daily OHLC bar", error=str(e))
            return None
    
    async def _create_hourly_ohlc_bar(self, market: str, delivery_location: str, delivery_date: str, delivery_hour: int) -> Optional[OHLCBar]:
        """Create hourly OHLC bar."""
        try:
            buffer_key = f"{market}_{delivery_location}_{delivery_date}"
            trades = self.ohlc_buffers[market][buffer_key]
            
            # Filter trades for the specific hour
            hour_trades = [trade for trade in trades if trade["delivery_hour"] == delivery_hour]
            
            if not hour_trades:
                return None
            
            # Calculate OHLC values
            prices = [trade["price"] for trade in hour_trades]
            quantities = [trade["quantity"] for trade in hour_trades]
            
            open_price = prices[0] if prices else 0.0
            high_price = max(prices) if prices else 0.0
            low_price = min(prices) if prices else 0.0
            close_price = prices[-1] if prices else 0.0
            volume = sum(quantities) if quantities else 0.0
            
            # Calculate VWAP
            if volume > 0:
                vwap = sum(p * q for p, q in zip(prices, quantities)) / volume
            else:
                vwap = 0.0
            
            return OHLCBar(
                market=market,
                delivery_location=delivery_location,
                delivery_date=delivery_date,
                bar_type="hourly",
                bar_size="1H",
                open_price=open_price,
                high_price=high_price,
                low_price=low_price,
                close_price=close_price,
                volume=volume,
                vwap=vwap,
                trade_count=len(hour_trades),
                price_currency="USD",
                quantity_unit="MWh"
            )
            
        except Exception as e:
            self.logger.error("Failed to create hourly OHLC bar", error=str(e))
            return None
    
    async def _generate_rolling_metrics(self, data: Dict[str, Any]) -> List[RollingMetric]:
        """
        Generate rolling metrics from enriched data.
        
        Args:
            data: Enriched data
            
        Returns:
            List[RollingMetric]: Generated rolling metrics
        """
        try:
            rolling_metrics = []
            
            # Extract key fields
            market = data.get("market", "UNKNOWN")
            delivery_location = data.get("delivery_location", "unknown")
            delivery_date = data.get("delivery_date")
            price = data.get("price")
            quantity = data.get("quantity")
            
            if not all([market, delivery_location, delivery_date, price is not None, quantity is not None]):
                return rolling_metrics
            
            # Add to rolling windows
            window_key = f"{market}_{delivery_location}_{delivery_date}"
            self.rolling_windows[market][window_key].append({
                "price": float(price),
                "quantity": float(quantity),
                "timestamp": datetime.now(timezone.utc)
            })
            
            # Generate rolling metrics
            window_data = list(self.rolling_windows[market][window_key])
            
            if len(window_data) >= 5:  # Minimum window size
                # Rolling average price
                avg_price = statistics.mean([d["price"] for d in window_data[-5:]])
                rolling_metrics.append(RollingMetric(
                    market=market,
                    delivery_location=delivery_location,
                    delivery_date=delivery_date,
                    metric_type="rolling_average_price",
                    window_size="5",
                    metric_value=avg_price,
                    metric_unit="USD/MWh",
                    sample_count=5
                ))
                
                # Rolling total volume
                total_volume = sum([d["quantity"] for d in window_data[-5:]])
                rolling_metrics.append(RollingMetric(
                    market=market,
                    delivery_location=delivery_location,
                    delivery_date=delivery_date,
                    metric_type="rolling_total_volume",
                    window_size="5",
                    metric_value=total_volume,
                    metric_unit="MWh",
                    sample_count=5
                ))
                
                # Rolling price volatility
                prices = [d["price"] for d in window_data[-5:]]
                if len(prices) > 1:
                    volatility = statistics.stdev(prices)
                    rolling_metrics.append(RollingMetric(
                        market=market,
                        delivery_location=delivery_location,
                        delivery_date=delivery_date,
                        metric_type="rolling_price_volatility",
                        window_size="5",
                        metric_value=volatility,
                        metric_unit="USD/MWh",
                        sample_count=5
                    ))
            
            return rolling_metrics
            
        except Exception as e:
            self.logger.error("Failed to generate rolling metrics", error=str(e))
            return []
    
    async def _generate_curve_prestage(self, data: Dict[str, Any]) -> List[CurvePreStage]:
        """
        Generate curve pre-stage from enriched data.
        
        Args:
            data: Enriched data
            
        Returns:
            List[CurvePreStage]: Generated curve pre-stage
        """
        try:
            curve_prestage = []
            
            # Extract key fields
            market = data.get("market", "UNKNOWN")
            delivery_date = data.get("delivery_date")
            delivery_hour = data.get("delivery_hour")
            price = data.get("price")
            quantity = data.get("quantity")
            data_type = data.get("data_type", "unknown")
            
            if not all([market, delivery_date, delivery_hour is not None, price is not None, quantity is not None]):
                return curve_prestage
            
            # Determine curve type based on data type
            curve_type = self._get_curve_type(data_type)
            
            # Add to curve buffer
            buffer_key = f"{market}_{curve_type}_{delivery_date}"
            self.curve_buffers[buffer_key].append({
                "delivery_hour": delivery_hour,
                "price": float(price),
                "quantity": float(quantity),
                "timestamp": datetime.now(timezone.utc)
            })
            
            # Generate curve pre-stage entry
            curve_metadata = {
                "data_type": data_type,
                "source": data.get("source", "unknown"),
                "enrichment_score": data.get("enrichment_score", 0.0),
                "taxonomy_tags": data.get("taxonomy_tags", []),
                "semantic_tags": data.get("semantic_tags", [])
            }
            
            curve_prestage.append(CurvePreStage(
                market=market,
                curve_type=curve_type,
                delivery_date=delivery_date,
                delivery_hour=delivery_hour,
                price=float(price),
                quantity=float(quantity),
                price_currency="USD",
                quantity_unit="MWh",
                curve_metadata=curve_metadata
            ))
            
            return curve_prestage
            
        except Exception as e:
            self.logger.error("Failed to generate curve pre-stage", error=str(e))
            return []
    
    def _get_curve_type(self, data_type: str) -> str:
        """Get curve type from data type."""
        curve_type_mapping = {
            "trade": "trade_curve",
            "curve": "price_curve",
            "market_price": "spot_curve",
            "system_status": "status_curve"
        }
        
        return curve_type_mapping.get(data_type, "unknown_curve")
    
    async def flush_buffers(self) -> AggregationResult:
        """
        Flush all aggregation buffers.
        
        Returns:
            AggregationResult: Aggregation result with flushed data
        """
        try:
            result = AggregationResult()
            
            # Flush OHLC buffers
            for market, market_buffers in self.ohlc_buffers.items():
                for buffer_key, trades in market_buffers.items():
                    if trades:
                        # Extract components from buffer key
                        parts = buffer_key.split("_")
                        if len(parts) >= 3:
                            delivery_location = parts[1]
                            delivery_date = parts[2]
                            
                            # Create final OHLC bar
                            ohlc_bar = await self._create_daily_ohlc_bar(market, delivery_location, delivery_date)
                            if ohlc_bar:
                                result.ohlc_bars.append(ohlc_bar)
            
            # Flush rolling windows
            for market, market_windows in self.rolling_windows.items():
                for window_key, window_data in market_windows.items():
                    if window_data:
                        # Extract components from window key
                        parts = window_key.split("_")
                        if len(parts) >= 3:
                            delivery_location = parts[1]
                            delivery_date = parts[2]
                            
                            # Create final rolling metrics
                            if len(window_data) >= 5:
                                prices = [d["price"] for d in window_data]
                                quantities = [d["quantity"] for d in window_data]
                                
                                # Final rolling average
                                avg_price = statistics.mean(prices)
                                result.rolling_metrics.append(RollingMetric(
                                    market=market,
                                    delivery_location=delivery_location,
                                    delivery_date=delivery_date,
                                    metric_type="final_rolling_average_price",
                                    window_size="all",
                                    metric_value=avg_price,
                                    metric_unit="USD/MWh",
                                    sample_count=len(window_data)
                                ))
                                
                                # Final rolling total volume
                                total_volume = sum(quantities)
                                result.rolling_metrics.append(RollingMetric(
                                    market=market,
                                    delivery_location=delivery_location,
                                    delivery_date=delivery_date,
                                    metric_type="final_rolling_total_volume",
                                    window_size="all",
                                    metric_value=total_volume,
                                    metric_unit="MWh",
                                    sample_count=len(window_data)
                                ))
            
            # Flush curve buffers
            for buffer_key, curve_data in self.curve_buffers.items():
                if curve_data:
                    # Extract components from buffer key
                    parts = buffer_key.split("_")
                    if len(parts) >= 3:
                        market = parts[0]
                        curve_type = parts[1]
                        delivery_date = parts[2]
                        
                        # Create final curve pre-stage entries
                        for data_point in curve_data:
                            result.curve_prestage.append(CurvePreStage(
                                market=market,
                                curve_type=curve_type,
                                delivery_date=delivery_date,
                                delivery_hour=data_point["delivery_hour"],
                                price=data_point["price"],
                                quantity=data_point["quantity"],
                                price_currency="USD",
                                quantity_unit="MWh",
                                curve_metadata={"flushed": True}
                            ))
            
            # Clear buffers
            self.ohlc_buffers.clear()
            self.rolling_windows.clear()
            self.curve_buffers.clear()
            
            # Update metadata
            result.metadata = {
                "flush_operation": True,
                "ohlc_bars_count": len(result.ohlc_bars),
                "rolling_metrics_count": len(result.rolling_metrics),
                "curve_prestage_count": len(result.curve_prestage),
                "flush_timestamp": datetime.now(timezone.utc).isoformat()
            }
            
            self.logger.info("Buffers flushed successfully", 
                           ohlc_bars=len(result.ohlc_bars),
                           rolling_metrics=len(result.rolling_metrics),
                           curve_prestage=len(result.curve_prestage))
            
            return result
            
        except Exception as e:
            self.logger.error("Failed to flush buffers", error=str(e))
            return AggregationResult(metadata={"error": str(e)})
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get aggregation statistics.
        
        Returns:
            Dict[str, Any]: Aggregation statistics
        """
        return {
            **self.stats,
            "success_rate": (
                self.stats["successful_aggregations"] / max(1, self.stats["total_aggregations"])
            ),
            "buffer_count": len(self.ohlc_buffers) + len(self.rolling_windows) + len(self.curve_buffers)
        }
    
    def reset_stats(self) -> None:
        """Reset aggregation statistics."""
        self.stats = {
            "total_aggregations": 0,
            "successful_aggregations": 0,
            "failed_aggregations": 0,
            "ohlc_bars_generated": 0,
            "rolling_metrics_generated": 0,
            "curve_prestage_generated": 0,
            "buffer_sizes": {}
        }
