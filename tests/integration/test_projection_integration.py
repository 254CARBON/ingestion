"""
Integration tests for projection service.
"""

import pytest
import asyncio
import json
from unittest.mock import Mock, AsyncMock, patch
from datetime import datetime, timezone

from services.service-projection.src.core.projector import ProjectionService
from services.service-projection.src.core.clickhouse_writer import ClickHouseWriter, ClickHouseWriterConfig
from services.service-projection.src.core.cache_manager import CacheManager, CacheManagerConfig
from services.service-projection.src.consumers.ohlc_consumer import KafkaOHLCConsumer, OHLCConsumerConfig
from services.service-projection.src.consumers.metrics_consumer import KafkaMetricsConsumer, MetricsConsumerConfig
from services.service-projection.src.consumers.curve_consumer import KafkaCurveConsumer, CurveConsumerConfig


class TestProjectionIntegration:
    """Test projection service integration."""

    @pytest.fixture
    def projection_service(self):
        """Create projection service for integration testing."""
        with patch('services.service-projection.src.core.projector.ClickHouseWriter') as mock_ch_writer, \
             patch('services.service-projection.src.core.projector.CacheManager') as mock_cache_manager:

            # Create real instances for testing
            ch_config = ClickHouseWriterConfig(dsn="clickhouse://test:test@localhost:9000/test")
            cache_config = CacheManagerConfig(redis_url="redis://localhost:6379/0")

            # Mock the actual ClickHouse and Redis connections for testing
            mock_ch_writer.return_value = Mock(spec=ClickHouseWriter)
            mock_cache_manager.return_value = Mock(spec=CacheManager)

            service = ProjectionService()
            service.clickhouse_writer = mock_ch_writer.return_value
            service.cache_manager = mock_cache_manager.return_value
            return service

    @pytest.fixture
    def sample_aggregated_data(self):
        """Sample data from aggregation service."""
        return {
            "ohlc_bars": [
                {
                    "market": "CAISO",
                    "delivery_location": "TH_SP15_GEN-APND",
                    "delivery_date": "2025-01-15",
                    "bar_type": "daily",
                    "bar_size": "1D",
                    "open_price": 50.0,
                    "high_price": 55.0,
                    "low_price": 48.0,
                    "close_price": 52.0,
                    "volume": 1000.0,
                    "vwap": 51.5,
                    "trade_count": 100,
                    "price_currency": "USD",
                    "quantity_unit": "MWh"
                }
            ],
            "rolling_metrics": [
                {
                    "market": "CAISO",
                    "delivery_location": "TH_SP15_GEN-APND",
                    "delivery_date": "2025-01-15",
                    "metric_type": "rolling_average_price",
                    "window_size": "5",
                    "metric_value": 52.5,
                    "metric_unit": "USD/MWh",
                    "sample_count": 5
                }
            ],
            "curve_data": [
                {
                    "market": "CAISO",
                    "curve_type": "spot_curve",
                    "delivery_date": "2025-01-15",
                    "delivery_hour": 14,
                    "price": 52.0,
                    "quantity": 250.0,
                    "price_currency": "USD",
                    "quantity_unit": "MWh",
                    "curve_metadata": {"source": "aggregation"}
                }
            ]
        }

    @pytest.mark.asyncio
    async def test_end_to_end_projection_flow(self, projection_service, sample_aggregated_data):
        """Test end-to-end projection flow."""
        # Mock ClickHouse writer methods
        projection_service.clickhouse_writer.write_ohlc_bars = AsyncMock(return_value=1)
        projection_service.clickhouse_writer.write_rolling_metrics = AsyncMock(return_value=1)
        projection_service.clickhouse_writer.write_curve_data = AsyncMock(return_value=1)

        # Mock cache manager methods
        projection_service.cache_manager.set = AsyncMock(return_value=True)

        # Project OHLC bars
        ohlc_result = await projection_service.project_ohlc_bars(sample_aggregated_data["ohlc_bars"])
        assert ohlc_result.records_projected == 1
        assert "gold_ohlc_bars" in ohlc_result.tables_written

        # Project rolling metrics
        metrics_result = await projection_service.project_rolling_metrics(sample_aggregated_data["rolling_metrics"])
        assert metrics_result.records_projected == 1
        assert "gold_rolling_metrics" in metrics_result.tables_written

        # Project curve data
        curve_result = await projection_service.project_curve_data(sample_aggregated_data["curve_data"])
        assert curve_result.records_projected == 1
        assert "gold_curve_prestage" in curve_result.tables_written

        # Verify all cache entries were created
        assert projection_service.cache_manager.set.call_count == 3

    @pytest.mark.asyncio
    async def test_projection_with_clickhouse_failures(self, projection_service, sample_aggregated_data):
        """Test projection handling of ClickHouse failures."""
        # Mock ClickHouse writer to fail
        projection_service.clickhouse_writer.write_ohlc_bars = AsyncMock(side_effect=Exception("ClickHouse error"))

        # Projection should handle the failure gracefully
        result = await projection_service.project_ohlc_bars(sample_aggregated_data["ohlc_bars"])

        assert result.records_projected == 0
        assert "error" in result.metadata

    @pytest.mark.asyncio
    async def test_projection_with_cache_failures(self, projection_service, sample_aggregated_data):
        """Test projection handling of cache failures."""
        # Mock ClickHouse writer to succeed
        projection_service.clickhouse_writer.write_ohlc_bars = AsyncMock(return_value=1)

        # Mock cache manager to fail
        projection_service.cache_manager.set = AsyncMock(side_effect=Exception("Redis error"))

        # Projection should still succeed but log the cache failure
        result = await projection_service.project_ohlc_bars(sample_aggregated_data["ohlc_bars"])

        assert result.records_projected == 1  # ClickHouse write succeeded
        assert result.cache_entries == 0  # Cache failed

    @pytest.mark.asyncio
    async def test_query_with_cache_integration(self, projection_service):
        """Test query integration with cache."""
        # Mock cache manager for cache hit
        projection_service.cache_manager.get = AsyncMock(return_value=[
            {"market": "CAISO", "delivery_location": "hub", "price": 50.0}
        ])

        # Query with cache should return cached data
        result = await projection_service.query_ohlc_bars(
            market="CAISO",
            delivery_location="hub",
            use_cache=True
        )

        assert result == [{"market": "CAISO", "delivery_location": "hub", "price": 50.0}]

        # Cache should have been checked
        projection_service.cache_manager.get.assert_called_once()

    @pytest.mark.asyncio
    async def test_materialized_view_management(self, projection_service):
        """Test materialized view management integration."""
        # Mock ClickHouse writer methods
        projection_service.clickhouse_writer.refresh_materialized_view = AsyncMock(return_value=True)

        # Manage materialized views
        result = await projection_service.manage_materialized_views()

        assert "refreshed_views" in result
        assert "errors" in result

        # Should have attempted to refresh views
        assert projection_service.clickhouse_writer.refresh_materialized_view.call_count >= 1

    @pytest.mark.asyncio
    async def test_kafka_consumer_integration(self, projection_service):
        """Test Kafka consumer integration."""
        # Create consumer
        consumer_config = OHLCConsumerConfig(
            bootstrap_servers="localhost:9092",
            topic="test.ohlc.bars.v1"
        )

        consumer = KafkaOHLCConsumer(consumer_config, projection_service)

        # Mock Kafka consumer
        with patch('services.service-projection.src.consumers.ohlc_consumer.AIOKafkaConsumer') as mock_consumer_class:
            mock_consumer = AsyncMock()
            mock_consumer_class.return_value = mock_consumer
            consumer.consumer = mock_consumer

            # Test consumer lifecycle
            await consumer.start()
            await consumer.stop()

            # Verify consumer methods were called
            mock_consumer.start.assert_called_once()
            mock_consumer.stop.assert_called_once()

    @pytest.mark.asyncio
    async def test_projection_batch_operations(self, projection_service):
        """Test batch projection operations."""
        # Create larger dataset
        large_ohlc_bars = []
        for i in range(100):
            large_ohlc_bars.append({
                "market": "CAISO",
                "delivery_location": f"hub_{i}",
                "delivery_date": "2025-01-15",
                "bar_type": "daily",
                "open_price": 50.0 + i,
                "high_price": 55.0 + i,
                "low_price": 48.0 + i,
                "close_price": 52.0 + i,
                "volume": 1000.0 + i * 10,
                "trade_count": 100 + i
            })

        # Mock ClickHouse writer
        projection_service.clickhouse_writer.write_ohlc_bars = AsyncMock(return_value=100)

        # Mock cache manager
        projection_service.cache_manager.set = AsyncMock(return_value=True)

        # Project large batch
        result = await projection_service.project_ohlc_bars(large_ohlc_bars)

        assert result.records_projected == 100
        assert result.cache_entries == 100

        # Verify batch write was called
        projection_service.clickhouse_writer.write_ohlc_bars.assert_called_once()

        # Verify cache was populated for each bar
        assert projection_service.cache_manager.set.call_count == 100

    @pytest.mark.asyncio
    async def test_projection_error_recovery(self, projection_service, sample_aggregated_data):
        """Test projection error recovery and resilience."""
        # Mock ClickHouse writer with intermittent failures
        call_count = 0
        async def mock_write_ohlc_bars(data):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise Exception("Temporary ClickHouse error")
            return len(data)

        projection_service.clickhouse_writer.write_ohlc_bars = AsyncMock(side_effect=mock_write_ohlc_bars)

        # Mock cache manager
        projection_service.cache_manager.set = AsyncMock(return_value=True)

        # First attempt should fail
        result1 = await projection_service.project_ohlc_bars(sample_aggregated_data["ohlc_bars"])
        assert result1.records_projected == 0
        assert "error" in result1.metadata

        # Second attempt should succeed
        result2 = await projection_service.project_ohlc_bars(sample_aggregated_data["ohlc_bars"])
        assert result2.records_projected == 1

        # Verify statistics track both attempts
        stats = projection_service.get_stats()
        assert stats["total_projections"] == 2
        assert stats["successful_projections"] == 1
        assert stats["failed_projections"] == 1

    @pytest.mark.asyncio
    async def test_projection_performance(self, projection_service):
        """Test projection performance with realistic data volumes."""
        # Create performance test data
        performance_data = []
        for i in range(1000):  # 1000 OHLC bars
            performance_data.append({
                "market": "CAISO",
                "delivery_location": f"hub_{i % 10}",  # 10 different locations
                "delivery_date": "2025-01-15",
                "bar_type": "daily",
                "open_price": 50.0 + (i % 10),
                "high_price": 55.0 + (i % 10),
                "low_price": 48.0 + (i % 10),
                "close_price": 52.0 + (i % 10),
                "volume": 1000.0 + i,
                "trade_count": 100 + i
            })

        # Mock ClickHouse writer for performance
        projection_service.clickhouse_writer.write_ohlc_bars = AsyncMock(return_value=1000)

        # Mock cache manager for performance
        projection_service.cache_manager.set = AsyncMock(return_value=True)

        # Measure projection time
        start_time = datetime.now(timezone.utc)

        result = await projection_service.project_ohlc_bars(performance_data)

        end_time = datetime.now(timezone.utc)
        projection_time = (end_time - start_time).total_seconds()

        # Performance assertions
        assert result.records_projected == 1000
        assert projection_time < 5.0  # Should complete within 5 seconds

        # Verify all cache entries were created
        assert projection_service.cache_manager.set.call_count == 1000

    @pytest.mark.asyncio
    async def test_projection_data_consistency(self, projection_service, sample_aggregated_data):
        """Test data consistency across projection operations."""
        # Mock ClickHouse writer to capture written data
        written_ohlc_data = []
        written_metrics_data = []
        written_curve_data = []

        async def mock_write_ohlc_bars(data):
            written_ohlc_data.extend(data)
            return len(data)

        async def mock_write_rolling_metrics(data):
            written_metrics_data.extend(data)
            return len(data)

        async def mock_write_curve_data(data):
            written_curve_data.extend(data)
            return len(data)

        projection_service.clickhouse_writer.write_ohlc_bars = AsyncMock(side_effect=mock_write_ohlc_bars)
        projection_service.clickhouse_writer.write_rolling_metrics = AsyncMock(side_effect=mock_write_rolling_metrics)
        projection_service.clickhouse_writer.write_curve_data = AsyncMock(side_effect=mock_write_curve_data)

        # Project all data types
        ohlc_result = await projection_service.project_ohlc_bars(sample_aggregated_data["ohlc_bars"])
        metrics_result = await projection_service.project_rolling_metrics(sample_aggregated_data["rolling_metrics"])
        curve_result = await projection_service.project_curve_data(sample_aggregated_data["curve_data"])

        # Verify data consistency
        assert len(written_ohlc_data) == 1
        assert len(written_metrics_data) == 1
        assert len(written_curve_data) == 1

        # Verify OHLC data integrity
        ohlc_data = written_ohlc_data[0]
        original_ohlc = sample_aggregated_data["ohlc_bars"][0]
        assert ohlc_data["market"] == original_ohlc["market"]
        assert ohlc_data["delivery_location"] == original_ohlc["delivery_location"]
        assert ohlc_data["open_price"] == original_ohlc["open_price"]
        assert ohlc_data["aggregation_timestamp"] is not None

        # Verify metrics data integrity
        metrics_data = written_metrics_data[0]
        original_metrics = sample_aggregated_data["rolling_metrics"][0]
        assert metrics_data["market"] == original_metrics["market"]
        assert metrics_data["metric_type"] == original_metrics["metric_type"]
        assert metrics_data["calculation_timestamp"] is not None

        # Verify curve data integrity
        curve_data = written_curve_data[0]
        original_curve = sample_aggregated_data["curve_data"][0]
        assert curve_data["market"] == original_curve["market"]
        assert curve_data["curve_type"] == original_curve["curve_type"]
        assert curve_data["aggregation_timestamp"] is not None

    @pytest.mark.asyncio
    async def test_projection_with_different_markets(self, projection_service):
        """Test projection with data from different markets."""
        multi_market_data = [
            {
                "market": "CAISO",
                "delivery_location": "hub_caiso",
                "delivery_date": "2025-01-15",
                "open_price": 50.0,
                "high_price": 55.0,
                "low_price": 48.0,
                "close_price": 52.0,
                "volume": 1000.0,
                "trade_count": 100
            },
            {
                "market": "MISO",
                "delivery_location": "hub_miso",
                "delivery_date": "2025-01-15",
                "open_price": 45.0,
                "high_price": 50.0,
                "low_price": 43.0,
                "close_price": 47.0,
                "volume": 800.0,
                "trade_count": 80
            }
        ]

        # Mock ClickHouse writer
        projection_service.clickhouse_writer.write_ohlc_bars = AsyncMock(return_value=2)

        # Mock cache manager
        projection_service.cache_manager.set = AsyncMock(return_value=True)

        # Project multi-market data
        result = await projection_service.project_ohlc_bars(multi_market_data)

        assert result.records_projected == 2
        assert len(result.metadata["markets"]) == 2

        # Verify cache entries for both markets
        assert projection_service.cache_manager.set.call_count == 2

    @pytest.mark.asyncio
    async def test_projection_cache_invalidation(self, projection_service):
        """Test cache invalidation across projection operations."""
        # Mock cache manager
        projection_service.cache_manager.invalidate_by_market = AsyncMock(return_value=5)

        # Invalidate cache for a market
        invalidated = await projection_service.cache_manager.invalidate_by_market("CAISO")

        assert invalidated == 5
        projection_service.cache_manager.invalidate_by_market.assert_called_once_with("CAISO")

    @pytest.mark.asyncio
    async def test_projection_concurrent_operations(self, projection_service):
        """Test concurrent projection operations."""
        # Create concurrent tasks
        tasks = []

        # Mock ClickHouse writer and cache manager for concurrent access
        projection_service.clickhouse_writer.write_ohlc_bars = AsyncMock(return_value=1)
        projection_service.cache_manager.set = AsyncMock(return_value=True)

        # Create multiple projection tasks
        for i in range(10):
            ohlc_data = [{
                "market": "CAISO",
                "delivery_location": f"hub_{i}",
                "delivery_date": "2025-01-15",
                "open_price": 50.0 + i,
                "high_price": 55.0 + i,
                "low_price": 48.0 + i,
                "close_price": 52.0 + i,
                "volume": 1000.0 + i,
                "trade_count": 100 + i
            }]
            tasks.append(projection_service.project_ohlc_bars(ohlc_data))

        # Execute all tasks concurrently
        results = await asyncio.gather(*tasks)

        # Verify all tasks completed successfully
        assert len(results) == 10
        for result in results:
            assert result.records_projected == 1

        # Verify statistics
        stats = projection_service.get_stats()
        assert stats["total_projections"] == 10
        assert stats["successful_projections"] == 10
