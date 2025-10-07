"""
Unit tests for aggregation service.
"""

import pytest
import asyncio
from unittest.mock import Mock, AsyncMock, patch
from datetime import datetime, timezone
import statistics

from services.service-aggregation.src.core.aggregator import (
    AggregationService, 
    AggregationResult,
    OHLCBar,
    RollingMetric,
    CurvePreStage
)


class TestAggregationService:
    """Test aggregation service."""

    @pytest.fixture
    def aggregation_service(self):
        """Create aggregation service."""
        with patch('services.service-aggregation.src.core.aggregator.open') as mock_open:
            mock_open.return_value.__enter__.return_value.read.return_value = """
ohlc_policies:
  daily:
    enabled: true
    aggregation_window: "1D"
    buffer_size: 1000
  hourly:
    enabled: true
    aggregation_window: "1H"
    buffer_size: 100
rolling_metrics:
  price_metrics:
    rolling_average_price:
      enabled: true
      window_sizes: [5, 10, 20]
curve_prestage:
  trade_curves:
    enabled: true
    buffer_size: 1000
"""
            return AggregationService()

    @pytest.fixture
    def sample_enriched_data(self):
        """Sample enriched data."""
        return {
            "event_id": "test-event-123",
            "occurred_at": int(datetime.now(timezone.utc).timestamp() * 1_000_000),
            "tenant_id": "default",
            "schema_version": "1.0.0",
            "producer": "enrichment-service",
            "market": "CAISO",
            "data_type": "market_price",
            "delivery_location": "hub",
            "delivery_date": "2025-01-15",
            "delivery_hour": 14,
            "price": 75.50,
            "quantity": 250.0,
            "source": "caiso-oasis",
            "taxonomy_tags": ["energy_trading", "spot_pricing"],
            "semantic_tags": ["california_energy", "normal_price"],
            "enrichment_score": 0.95
        }

    @pytest.mark.asyncio
    async def test_aggregate_success(self, aggregation_service, sample_enriched_data):
        """Test successful aggregation."""
        result = await aggregation_service.aggregate(sample_enriched_data)
        
        assert isinstance(result, AggregationResult)
        assert isinstance(result.ohlc_bars, list)
        assert isinstance(result.rolling_metrics, list)
        assert isinstance(result.curve_prestage, list)
        assert "aggregation_method" in result.metadata
        assert "ohlc_bars_count" in result.metadata
        assert "rolling_metrics_count" in result.metadata
        assert "curve_prestage_count" in result.metadata

    @pytest.mark.asyncio
    async def test_aggregate_with_missing_fields(self, aggregation_service):
        """Test aggregation with missing fields."""
        minimal_data = {
            "event_id": "test-event-456",
            "market": "MISO",
            "delivery_location": "node",
            "delivery_date": "2025-01-15",
            "price": 50.0,
            "quantity": 100.0
        }
        
        result = await aggregation_service.aggregate(minimal_data)
        
        assert isinstance(result, AggregationResult)
        assert len(result.ohlc_bars) == 0  # No hour specified
        assert len(result.rolling_metrics) == 0  # Insufficient data
        assert len(result.curve_prestage) == 0  # Missing hour

    @pytest.mark.asyncio
    async def test_generate_ohlc_bars(self, aggregation_service, sample_enriched_data):
        """Test OHLC bar generation."""
        ohlc_bars = await aggregation_service._generate_ohlc_bars(sample_enriched_data)
        
        assert isinstance(ohlc_bars, list)
        assert len(ohlc_bars) >= 1  # At least daily bar
        
        # Check daily bar
        daily_bar = next((bar for bar in ohlc_bars if bar.bar_type == "daily"), None)
        assert daily_bar is not None
        assert daily_bar.market == "CAISO"
        assert daily_bar.delivery_location == "hub"
        assert daily_bar.delivery_date == "2025-01-15"
        assert daily_bar.bar_size == "1D"
        assert daily_bar.open_price == 75.50
        assert daily_bar.high_price == 75.50
        assert daily_bar.low_price == 75.50
        assert daily_bar.close_price == 75.50
        assert daily_bar.volume == 250.0
        assert daily_bar.vwap == 75.50
        assert daily_bar.trade_count == 1
        
        # Check hourly bar
        hourly_bar = next((bar for bar in ohlc_bars if bar.bar_type == "hourly"), None)
        assert hourly_bar is not None
        assert hourly_bar.bar_size == "1H"
        assert hourly_bar.open_price == 75.50
        assert hourly_bar.high_price == 75.50
        assert hourly_bar.low_price == 75.50
        assert hourly_bar.close_price == 75.50
        assert hourly_bar.volume == 250.0
        assert hourly_bar.vwap == 75.50
        assert hourly_bar.trade_count == 1

    @pytest.mark.asyncio
    async def test_create_daily_ohlc_bar(self, aggregation_service):
        """Test daily OHLC bar creation."""
        # Add some trades to buffer
        market = "CAISO"
        delivery_location = "hub"
        delivery_date = "2025-01-15"
        
        trades = [
            {"delivery_hour": 10, "price": 50.0, "quantity": 100.0, "timestamp": datetime.now(timezone.utc)},
            {"delivery_hour": 11, "price": 60.0, "quantity": 150.0, "timestamp": datetime.now(timezone.utc)},
            {"delivery_hour": 12, "price": 55.0, "quantity": 200.0, "timestamp": datetime.now(timezone.utc)}
        ]
        
        buffer_key = f"{market}_{delivery_location}_{delivery_date}"
        aggregation_service.ohlc_buffers[market][buffer_key] = trades
        
        ohlc_bar = await aggregation_service._create_daily_ohlc_bar(market, delivery_location, delivery_date)
        
        assert ohlc_bar is not None
        assert ohlc_bar.market == market
        assert ohlc_bar.delivery_location == delivery_location
        assert ohlc_bar.delivery_date == delivery_date
        assert ohlc_bar.bar_type == "daily"
        assert ohlc_bar.bar_size == "1D"
        assert ohlc_bar.open_price == 50.0
        assert ohlc_bar.high_price == 60.0
        assert ohlc_bar.low_price == 50.0
        assert ohlc_bar.close_price == 55.0
        assert ohlc_bar.volume == 450.0
        assert ohlc_bar.trade_count == 3

    @pytest.mark.asyncio
    async def test_create_hourly_ohlc_bar(self, aggregation_service):
        """Test hourly OHLC bar creation."""
        # Add some trades to buffer
        market = "CAISO"
        delivery_location = "hub"
        delivery_date = "2025-01-15"
        delivery_hour = 14
        
        trades = [
            {"delivery_hour": 10, "price": 50.0, "quantity": 100.0, "timestamp": datetime.now(timezone.utc)},
            {"delivery_hour": 14, "price": 60.0, "quantity": 150.0, "timestamp": datetime.now(timezone.utc)},
            {"delivery_hour": 14, "price": 55.0, "quantity": 200.0, "timestamp": datetime.now(timezone.utc)},
            {"delivery_hour": 15, "price": 70.0, "quantity": 300.0, "timestamp": datetime.now(timezone.utc)}
        ]
        
        buffer_key = f"{market}_{delivery_location}_{delivery_date}"
        aggregation_service.ohlc_buffers[market][buffer_key] = trades
        
        ohlc_bar = await aggregation_service._create_hourly_ohlc_bar(market, delivery_location, delivery_date, delivery_hour)
        
        assert ohlc_bar is not None
        assert ohlc_bar.market == market
        assert ohlc_bar.delivery_location == delivery_location
        assert ohlc_bar.delivery_date == delivery_date
        assert ohlc_bar.bar_type == "hourly"
        assert ohlc_bar.bar_size == "1H"
        assert ohlc_bar.open_price == 60.0
        assert ohlc_bar.high_price == 60.0
        assert ohlc_bar.low_price == 55.0
        assert ohlc_bar.close_price == 55.0
        assert ohlc_bar.volume == 350.0
        assert ohlc_bar.trade_count == 2

    @pytest.mark.asyncio
    async def test_generate_rolling_metrics(self, aggregation_service):
        """Test rolling metrics generation."""
        # Add data to rolling windows
        market = "CAISO"
        delivery_location = "hub"
        delivery_date = "2025-01-15"
        
        window_data = [
            {"price": 50.0, "quantity": 100.0, "timestamp": datetime.now(timezone.utc)},
            {"price": 55.0, "quantity": 150.0, "timestamp": datetime.now(timezone.utc)},
            {"price": 60.0, "quantity": 200.0, "timestamp": datetime.now(timezone.utc)},
            {"price": 45.0, "quantity": 120.0, "timestamp": datetime.now(timezone.utc)},
            {"price": 65.0, "quantity": 180.0, "timestamp": datetime.now(timezone.utc)}
        ]
        
        window_key = f"{market}_{delivery_location}_{delivery_date}"
        aggregation_service.rolling_windows[market][window_key] = window_data
        
        enriched_data = {
            "market": market,
            "delivery_location": delivery_location,
            "delivery_date": delivery_date,
            "price": 70.0,
            "quantity": 250.0
        }
        
        rolling_metrics = await aggregation_service._generate_rolling_metrics(enriched_data)
        
        assert isinstance(rolling_metrics, list)
        assert len(rolling_metrics) >= 3  # At least 3 metrics
        
        # Check rolling average price
        avg_metric = next((m for m in rolling_metrics if m.metric_type == "rolling_average_price"), None)
        assert avg_metric is not None
        assert avg_metric.market == market
        assert avg_metric.delivery_location == delivery_location
        assert avg_metric.delivery_date == delivery_date
        assert avg_metric.window_size == "5"
        assert avg_metric.metric_unit == "USD/MWh"
        assert avg_metric.sample_count == 5
        
        # Check rolling total volume
        volume_metric = next((m for m in rolling_metrics if m.metric_type == "rolling_total_volume"), None)
        assert volume_metric is not None
        assert volume_metric.metric_unit == "MWh"
        
        # Check rolling price volatility
        volatility_metric = next((m for m in rolling_metrics if m.metric_type == "rolling_price_volatility"), None)
        assert volatility_metric is not None
        assert volatility_metric.metric_unit == "USD/MWh"

    @pytest.mark.asyncio
    async def test_generate_curve_prestage(self, aggregation_service, sample_enriched_data):
        """Test curve pre-stage generation."""
        curve_prestage = await aggregation_service._generate_curve_prestage(sample_enriched_data)
        
        assert isinstance(curve_prestage, list)
        assert len(curve_prestage) == 1
        
        curve = curve_prestage[0]
        assert isinstance(curve, CurvePreStage)
        assert curve.market == "CAISO"
        assert curve.curve_type == "spot_curve"  # Based on data_type "market_price"
        assert curve.delivery_date == "2025-01-15"
        assert curve.delivery_hour == 14
        assert curve.price == 75.50
        assert curve.quantity == 250.0
        assert curve.price_currency == "USD"
        assert curve.quantity_unit == "MWh"
        assert "curve_metadata" in curve.dict()

    def test_get_curve_type(self, aggregation_service):
        """Test curve type mapping."""
        # Test trade data type
        curve_type = aggregation_service._get_curve_type("trade")
        assert curve_type == "trade_curve"
        
        # Test curve data type
        curve_type = aggregation_service._get_curve_type("curve")
        assert curve_type == "price_curve"
        
        # Test market price data type
        curve_type = aggregation_service._get_curve_type("market_price")
        assert curve_type == "spot_curve"
        
        # Test system status data type
        curve_type = aggregation_service._get_curve_type("system_status")
        assert curve_type == "status_curve"
        
        # Test unknown data type
        curve_type = aggregation_service._get_curve_type("unknown")
        assert curve_type == "unknown_curve"

    @pytest.mark.asyncio
    async def test_flush_buffers(self, aggregation_service):
        """Test buffer flushing."""
        # Add data to buffers
        market = "CAISO"
        delivery_location = "hub"
        delivery_date = "2025-01-15"
        
        # Add OHLC data
        trades = [
            {"delivery_hour": 10, "price": 50.0, "quantity": 100.0, "timestamp": datetime.now(timezone.utc)},
            {"delivery_hour": 11, "price": 60.0, "quantity": 150.0, "timestamp": datetime.now(timezone.utc)}
        ]
        buffer_key = f"{market}_{delivery_location}_{delivery_date}"
        aggregation_service.ohlc_buffers[market][buffer_key] = trades
        
        # Add rolling metrics data
        window_data = [
            {"price": 50.0, "quantity": 100.0, "timestamp": datetime.now(timezone.utc)},
            {"price": 60.0, "quantity": 150.0, "timestamp": datetime.now(timezone.utc)}
        ]
        window_key = f"{market}_{delivery_location}_{delivery_date}"
        aggregation_service.rolling_windows[market][window_key] = window_data
        
        # Add curve data
        curve_data = [
            {"delivery_hour": 10, "price": 50.0, "quantity": 100.0, "timestamp": datetime.now(timezone.utc)},
            {"delivery_hour": 11, "price": 60.0, "quantity": 150.0, "timestamp": datetime.now(timezone.utc)}
        ]
        curve_buffer_key = f"{market}_spot_curve_{delivery_date}"
        aggregation_service.curve_buffers[curve_buffer_key] = curve_data
        
        # Flush buffers
        result = await aggregation_service.flush_buffers()
        
        assert isinstance(result, AggregationResult)
        assert len(result.ohlc_bars) >= 1
        assert len(result.rolling_metrics) >= 1
        assert len(result.curve_prestage) >= 1
        assert result.metadata["flush_operation"] is True
        
        # Verify buffers are cleared
        assert len(aggregation_service.ohlc_buffers) == 0
        assert len(aggregation_service.rolling_windows) == 0
        assert len(aggregation_service.curve_buffers) == 0

    def test_get_stats(self, aggregation_service):
        """Test statistics retrieval."""
        stats = aggregation_service.get_stats()
        
        assert "total_aggregations" in stats
        assert "successful_aggregations" in stats
        assert "failed_aggregations" in stats
        assert "ohlc_bars_generated" in stats
        assert "rolling_metrics_generated" in stats
        assert "curve_prestage_generated" in stats
        assert "success_rate" in stats

    def test_reset_stats(self, aggregation_service):
        """Test statistics reset."""
        # Add some stats
        aggregation_service.stats["total_aggregations"] = 10
        aggregation_service.stats["successful_aggregations"] = 8
        aggregation_service.stats["ohlc_bars_generated"] = 25
        
        # Reset stats
        aggregation_service.reset_stats()
        
        # Verify reset
        assert aggregation_service.stats["total_aggregations"] == 0
        assert aggregation_service.stats["successful_aggregations"] == 0
        assert aggregation_service.stats["failed_aggregations"] == 0
        assert aggregation_service.stats["ohlc_bars_generated"] == 0
        assert aggregation_service.stats["rolling_metrics_generated"] == 0
        assert aggregation_service.stats["curve_prestage_generated"] == 0

    @pytest.mark.asyncio
    async def test_aggregate_error_handling(self, aggregation_service):
        """Test aggregation error handling."""
        # Test with invalid data
        invalid_data = {
            "market": "CAISO",
            "delivery_location": "hub",
            "delivery_date": "2025-01-15",
            "price": "invalid_price",  # Invalid price
            "quantity": "invalid_quantity"  # Invalid quantity
        }
        
        result = await aggregation_service.aggregate(invalid_data)
        
        assert isinstance(result, AggregationResult)
        assert len(result.ohlc_bars) == 0
        assert len(result.rolling_metrics) == 0
        assert len(result.curve_prestage) == 0
        assert "error" in result.metadata

    @pytest.mark.asyncio
    async def test_ohlc_bar_vwap_calculation(self, aggregation_service):
        """Test VWAP calculation in OHLC bars."""
        market = "CAISO"
        delivery_location = "hub"
        delivery_date = "2025-01-15"
        
        # Add trades with different prices and quantities
        trades = [
            {"delivery_hour": 10, "price": 50.0, "quantity": 100.0, "timestamp": datetime.now(timezone.utc)},
            {"delivery_hour": 11, "price": 60.0, "quantity": 200.0, "timestamp": datetime.now(timezone.utc)},
            {"delivery_hour": 12, "price": 70.0, "quantity": 300.0, "timestamp": datetime.now(timezone.utc)}
        ]
        
        buffer_key = f"{market}_{delivery_location}_{delivery_date}"
        aggregation_service.ohlc_buffers[market][buffer_key] = trades
        
        ohlc_bar = await aggregation_service._create_daily_ohlc_bar(market, delivery_location, delivery_date)
        
        # Calculate expected VWAP
        total_value = (50.0 * 100.0) + (60.0 * 200.0) + (70.0 * 300.0)
        total_volume = 100.0 + 200.0 + 300.0
        expected_vwap = total_value / total_volume
        
        assert ohlc_bar.vwap == expected_vwap
        assert ohlc_bar.volume == total_volume

    @pytest.mark.asyncio
    async def test_rolling_metrics_statistics(self, aggregation_service):
        """Test rolling metrics statistical calculations."""
        market = "CAISO"
        delivery_location = "hub"
        delivery_date = "2025-01-15"
        
        # Add data with known statistics
        prices = [50.0, 55.0, 60.0, 45.0, 65.0]
        quantities = [100.0, 150.0, 200.0, 120.0, 180.0]
        
        window_data = []
        for i, (price, quantity) in enumerate(zip(prices, quantities)):
            window_data.append({
                "price": price,
                "quantity": quantity,
                "timestamp": datetime.now(timezone.utc)
            })
        
        window_key = f"{market}_{delivery_location}_{delivery_date}"
        aggregation_service.rolling_windows[market][window_key] = window_data
        
        enriched_data = {
            "market": market,
            "delivery_location": delivery_location,
            "delivery_date": delivery_date,
            "price": 70.0,
            "quantity": 250.0
        }
        
        rolling_metrics = await aggregation_service._generate_rolling_metrics(enriched_data)
        
        # Check rolling average price
        avg_metric = next((m for m in rolling_metrics if m.metric_type == "rolling_average_price"), None)
        expected_avg = statistics.mean(prices)
        assert avg_metric.metric_value == expected_avg
        
        # Check rolling total volume
        volume_metric = next((m for m in rolling_metrics if m.metric_type == "rolling_total_volume"), None)
        expected_volume = sum(quantities)
        assert volume_metric.metric_value == expected_volume
        
        # Check rolling price volatility
        volatility_metric = next((m for m in rolling_metrics if m.metric_type == "rolling_price_volatility"), None)
        expected_volatility = statistics.stdev(prices)
        assert abs(volatility_metric.metric_value - expected_volatility) < 0.001
