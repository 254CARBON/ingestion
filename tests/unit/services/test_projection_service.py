"""
Unit tests for projection service.
"""

import pytest
import asyncio
from unittest.mock import Mock, AsyncMock, patch
from datetime import datetime, timezone

from services.service-projection.src.core.projector import ProjectionService, ProjectionResult
from services.service-projection.src.core.clickhouse_writer import ClickHouseWriter, ClickHouseWriterConfig
from services.service-projection.src.core.cache_manager import CacheManager, CacheManagerConfig
from services.service-projection.src.consumers.ohlc_consumer import KafkaOHLCConsumer, OHLCConsumerConfig
from services.service-projection.src.consumers.metrics_consumer import KafkaMetricsConsumer, MetricsConsumerConfig
from services.service-projection.src.consumers.curve_consumer import KafkaCurveConsumer, CurveConsumerConfig


class TestProjectionService:
    """Test projection service."""

    @pytest.fixture
    def projection_service(self):
        """Create projection service."""
        with patch('services.service-projection.src.core.projector.ClickHouseWriter') as mock_ch_writer, \
             patch('services.service-projection.src.core.projector.CacheManager') as mock_cache_manager:

            mock_ch_writer.return_value = Mock()
            mock_cache_manager.return_value = Mock()

            service = ProjectionService()
            service.clickhouse_writer = mock_ch_writer.return_value
            service.cache_manager = mock_cache_manager.return_value
            return service

    @pytest.fixture
    def sample_ohlc_bars(self):
        """Sample OHLC bars data."""
        return [
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
        ]

    @pytest.fixture
    def sample_rolling_metrics(self):
        """Sample rolling metrics data."""
        return [
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
        ]

    @pytest.fixture
    def sample_curve_data(self):
        """Sample curve data."""
        return [
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

    @pytest.mark.asyncio
    async def test_project_ohlc_bars_success(self, projection_service, sample_ohlc_bars):
        """Test successful OHLC bars projection."""
        # Mock ClickHouse writer
        projection_service.clickhouse_writer.write_ohlc_bars = AsyncMock(return_value=1)

        # Mock cache manager
        projection_service.cache_manager.set = AsyncMock(return_value=True)

        result = await projection_service.project_ohlc_bars(sample_ohlc_bars)

        assert isinstance(result, ProjectionResult)
        assert result.records_projected == 1
        assert result.tables_written == ["gold_ohlc_bars"]
        assert result.cache_entries == 1

        # Verify ClickHouse writer was called
        projection_service.clickhouse_writer.write_ohlc_bars.assert_called_once()

        # Verify cache manager was called
        projection_service.cache_manager.set.assert_called_once()

    @pytest.mark.asyncio
    async def test_project_rolling_metrics_success(self, projection_service, sample_rolling_metrics):
        """Test successful rolling metrics projection."""
        # Mock ClickHouse writer
        projection_service.clickhouse_writer.write_rolling_metrics = AsyncMock(return_value=1)

        # Mock cache manager
        projection_service.cache_manager.set = AsyncMock(return_value=True)

        result = await projection_service.project_rolling_metrics(sample_rolling_metrics)

        assert isinstance(result, ProjectionResult)
        assert result.records_projected == 1
        assert result.tables_written == ["gold_rolling_metrics"]
        assert result.cache_entries == 1

        # Verify ClickHouse writer was called
        projection_service.clickhouse_writer.write_rolling_metrics.assert_called_once()

        # Verify cache manager was called
        projection_service.cache_manager.set.assert_called_once()

    @pytest.mark.asyncio
    async def test_project_curve_data_success(self, projection_service, sample_curve_data):
        """Test successful curve data projection."""
        # Mock ClickHouse writer
        projection_service.clickhouse_writer.write_curve_data = AsyncMock(return_value=1)

        # Mock cache manager
        projection_service.cache_manager.set = AsyncMock(return_value=True)

        result = await projection_service.project_curve_data(sample_curve_data)

        assert isinstance(result, ProjectionResult)
        assert result.records_projected == 1
        assert result.tables_written == ["gold_curve_prestage"]
        assert result.cache_entries == 1

        # Verify ClickHouse writer was called
        projection_service.clickhouse_writer.write_curve_data.assert_called_once()

        # Verify cache manager was called
        projection_service.cache_manager.set.assert_called_once()

    @pytest.mark.asyncio
    async def test_project_empty_data(self, projection_service):
        """Test projection with empty data."""
        result = await projection_service.project_ohlc_bars([])

        assert isinstance(result, ProjectionResult)
        assert result.records_projected == 0
        assert result.metadata["reason"] == "empty_data"

    @pytest.mark.asyncio
    async def test_project_clickhouse_failure(self, projection_service, sample_ohlc_bars):
        """Test projection with ClickHouse failure."""
        # Mock ClickHouse writer to raise exception
        projection_service.clickhouse_writer.write_ohlc_bars = AsyncMock(side_effect=Exception("ClickHouse error"))

        result = await projection_service.project_ohlc_bars(sample_ohlc_bars)

        assert isinstance(result, ProjectionResult)
        assert result.records_projected == 0
        assert "error" in result.metadata

    @pytest.mark.asyncio
    async def test_query_ohlc_bars_with_cache(self, projection_service, sample_ohlc_bars):
        """Test OHLC bars query with cache hit."""
        # Mock cache manager to return cached data
        projection_service.cache_manager.get = AsyncMock(return_value=sample_ohlc_bars)

        result = await projection_service.query_ohlc_bars(
            market="CAISO",
            delivery_location="TH_SP15_GEN-APND",
            use_cache=True
        )

        assert result == sample_ohlc_bars

        # Verify cache was checked but ClickHouse was not
        projection_service.cache_manager.get.assert_called_once()
        projection_service.clickhouse_writer.query_ohlc_bars.assert_not_called()

    @pytest.mark.asyncio
    async def test_query_ohlc_bars_without_cache(self, projection_service, sample_ohlc_bars):
        """Test OHLC bars query without cache."""
        # Mock cache manager to return None (cache miss)
        projection_service.cache_manager.get = AsyncMock(return_value=None)

        # Mock ClickHouse writer to return data
        projection_service.clickhouse_writer.query_ohlc_bars = AsyncMock(return_value=sample_ohlc_bars)

        # Mock cache manager set for caching result
        projection_service.cache_manager.set = AsyncMock(return_value=True)

        result = await projection_service.query_ohlc_bars(
            market="CAISO",
            delivery_location="TH_SP15_GEN-APND",
            use_cache=True
        )

        assert result == sample_ohlc_bars

        # Verify ClickHouse was called and result was cached
        projection_service.clickhouse_writer.query_ohlc_bars.assert_called_once()
        projection_service.cache_manager.set.assert_called_once()

    @pytest.mark.asyncio
    async def test_manage_materialized_views(self, projection_service):
        """Test materialized view management."""
        # Mock ClickHouse writer methods
        projection_service.clickhouse_writer.refresh_materialized_view = AsyncMock(return_value=True)

        result = await projection_service.manage_materialized_views()

        assert "refreshed_views" in result
        assert "errors" in result
        assert "timestamp" in result

        # Verify materialized views were refreshed
        assert projection_service.clickhouse_writer.refresh_materialized_view.call_count >= 1

    def test_get_stats(self, projection_service):
        """Test statistics retrieval."""
        stats = projection_service.get_stats()

        assert "total_projections" in stats
        assert "successful_projections" in stats
        assert "failed_projections" in stats
        assert "records_projected" in stats
        assert "success_rate" in stats
        assert "clickhouse_writer_stats" in stats
        assert "cache_manager_stats" in stats

    def test_reset_stats(self, projection_service):
        """Test statistics reset."""
        # Add some stats
        projection_service.stats["total_projections"] = 10
        projection_service.stats["successful_projections"] = 8

        # Reset stats
        projection_service.reset_stats()

        # Verify reset
        assert projection_service.stats["total_projections"] == 0
        assert projection_service.stats["successful_projections"] == 0
        assert projection_service.stats["failed_projections"] == 0


class TestClickHouseWriter:
    """Test ClickHouse writer."""

    @pytest.fixture
    def clickhouse_writer(self):
        """Create ClickHouse writer."""
        config = ClickHouseWriterConfig(
            dsn="clickhouse://test:test@localhost:9000/test",
            pool_size=2,
            batch_size=100
        )

        with patch('services.service-projection.src.core.clickhouse_writer.Client') as mock_client:
            writer = ClickHouseWriter(config)
            writer._clients = [mock_client]
            return writer

    @pytest.mark.asyncio
    async def test_write_ohlc_bars_success(self, clickhouse_writer):
        """Test successful OHLC bars write."""
        ohlc_bars = [
            {
                "market": "CAISO",
                "delivery_location": "hub",
                "delivery_date": "2025-01-15",
                "open_price": 50.0,
                "high_price": 55.0,
                "low_price": 48.0,
                "close_price": 52.0,
                "volume": 1000.0,
                "trade_count": 100
            }
        ]

        # Mock successful execution
        clickhouse_writer._clients[0].execute = Mock()

        result = await clickhouse_writer.write_ohlc_bars(ohlc_bars)

        assert result == 1

        # Verify execute was called
        clickhouse_writer._clients[0].execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_write_rolling_metrics_success(self, clickhouse_writer):
        """Test successful rolling metrics write."""
        rolling_metrics = [
            {
                "market": "CAISO",
                "delivery_location": "hub",
                "delivery_date": "2025-01-15",
                "metric_type": "rolling_average_price",
                "metric_value": 52.5,
                "sample_count": 5
            }
        ]

        # Mock successful execution
        clickhouse_writer._clients[0].execute = Mock()

        result = await clickhouse_writer.write_rolling_metrics(rolling_metrics)

        assert result == 1

        # Verify execute was called
        clickhouse_writer._clients[0].execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_query_ohlc_bars(self, clickhouse_writer):
        """Test OHLC bars query."""
        # Mock query result
        mock_columns = [("market", "String"), ("delivery_location", "String")]
        mock_rows = [("CAISO", "hub")]
        clickhouse_writer._clients[0].execute = Mock(return_value=(mock_columns, mock_rows))

        result = await clickhouse_writer.query_ohlc_bars(
            market="CAISO",
            delivery_location="hub"
        )

        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0]["market"] == "CAISO"
        assert result[0]["delivery_location"] == "hub"

    def test_get_stats(self, clickhouse_writer):
        """Test ClickHouse writer statistics."""
        stats = clickhouse_writer.get_stats()

        assert "total_writes" in stats
        assert "successful_writes" in stats
        assert "failed_writes" in stats
        assert "rows_written" in stats
        assert "success_rate" in stats


class TestCacheManager:
    """Test Redis cache manager."""

    @pytest.fixture
    def cache_manager(self):
        """Create cache manager."""
        config = CacheManagerConfig(
            redis_url="redis://localhost:6379/0",
            default_ttl=3600
        )

        with patch('services.service-projection.src.core.cache_manager.redis.Redis') as mock_redis:
            manager = CacheManager(config)
            manager._redis = mock_redis
            return manager

    @pytest.mark.asyncio
    async def test_set_and_get_cache(self, cache_manager):
        """Test cache set and get operations."""
        # Mock Redis operations
        cache_manager._redis.setex = AsyncMock()
        cache_manager._redis.get = AsyncMock(return_value='{"test": "data"}')

        # Test set
        success = await cache_manager.set("test_key", {"test": "data"}, ttl=1800)
        assert success is True
        cache_manager._redis.setex.assert_called_once()

        # Test get
        value = await cache_manager.get("test_key")
        assert value == {"test": "data"}
        cache_manager._redis.get.assert_called_once()

    @pytest.mark.asyncio
    async def test_cache_miss(self, cache_manager):
        """Test cache miss."""
        # Mock Redis to return None
        cache_manager._redis.get = AsyncMock(return_value=None)

        value = await cache_manager.get("missing_key")
        assert value is None

    @pytest.mark.asyncio
    async def test_cache_invalidation(self, cache_manager):
        """Test cache invalidation."""
        # Mock Redis operations
        cache_manager._redis.keys = AsyncMock(return_value=["key1", "key2"])
        cache_manager._redis.delete = AsyncMock(return_value=2)

        invalidated = await cache_manager.invalidate_pattern("test:*")

        assert invalidated == 2
        cache_manager._redis.keys.assert_called_once()
        cache_manager._redis.delete.assert_called_once()

    def test_get_stats(self, cache_manager):
        """Test cache manager statistics."""
        stats = cache_manager.get_stats()

        assert "total_operations" in stats
        assert "successful_operations" in stats
        assert "cache_hits" in stats
        assert "cache_misses" in stats
        assert "cache_hit_rate" in stats


class TestKafkaOHLCConsumer:
    """Test Kafka OHLC consumer."""

    @pytest.fixture
    def ohlc_consumer(self):
        """Create OHLC consumer."""
        config = OHLCConsumerConfig(
            bootstrap_servers="localhost:9092",
            topic="test.ohlc.bars.v1",
            group_id="test-ohlc-consumer"
        )

        with patch('services.service-projection.src.consumers.ohlc_consumer.AIOKafkaConsumer') as mock_consumer_class:
            consumer = KafkaOHLCConsumer(config, Mock())
            consumer.consumer = mock_consumer_class.return_value
            return consumer

    @pytest.mark.asyncio
    async def test_consumer_start_stop(self, ohlc_consumer):
        """Test consumer start and stop."""
        # Mock consumer methods
        ohlc_consumer.consumer.start = AsyncMock()
        ohlc_consumer.consumer.stop = AsyncMock()

        # Test start
        await ohlc_consumer.start()
        ohlc_consumer.consumer.start.assert_called_once()

        # Test stop
        await ohlc_consumer.stop()
        ohlc_consumer.consumer.stop.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_message_success(self, ohlc_consumer):
        """Test successful message processing."""
        # Mock projection service
        ohlc_consumer.projection_service.project_ohlc_bars = AsyncMock(return_value=Mock(records_projected=1))

        # Mock Kafka message
        mock_message = Mock()
        mock_message.value = {
            "payload": {
                "market": "CAISO",
                "delivery_location": "hub",
                "delivery_date": "2025-01-15",
                "open_price": 50.0
            }
        }
        mock_message.topic = "test.ohlc.bars.v1"
        mock_message.partition = 0
        mock_message.offset = 123

        # Process message
        await ohlc_consumer._process_message(mock_message)

        # Verify projection was called
        ohlc_consumer.projection_service.project_ohlc_bars.assert_called_once()

        # Verify stats were updated
        assert ohlc_consumer.stats["total_messages"] == 1
        assert ohlc_consumer.stats["successful_projections"] == 1

    def test_get_stats(self, ohlc_consumer):
        """Test consumer statistics."""
        # Add some stats
        ohlc_consumer.stats["total_messages"] = 10
        ohlc_consumer.stats["successful_projections"] = 8
        ohlc_consumer.stats["processing_latency_ms"] = [100, 150, 200]

        stats = ohlc_consumer.get_stats()

        assert stats["total_messages"] == 10
        assert stats["successful_projections"] == 8
        assert stats["average_latency_ms"] == 150.0
        assert stats["success_rate"] == 0.8


class TestKafkaMetricsConsumer:
    """Test Kafka metrics consumer."""

    @pytest.fixture
    def metrics_consumer(self):
        """Create metrics consumer."""
        config = MetricsConsumerConfig(
            bootstrap_servers="localhost:9092",
            topic="test.rolling.metrics.v1",
            group_id="test-metrics-consumer"
        )

        with patch('services.service-projection.src.consumers.metrics_consumer.AIOKafkaConsumer') as mock_consumer_class:
            consumer = KafkaMetricsConsumer(config, Mock())
            consumer.consumer = mock_consumer_class.return_value
            return consumer

    @pytest.mark.asyncio
    async def test_process_message_success(self, metrics_consumer):
        """Test successful metrics message processing."""
        # Mock projection service
        metrics_consumer.projection_service.project_rolling_metrics = AsyncMock(return_value=Mock(records_projected=1))

        # Mock Kafka message
        mock_message = Mock()
        mock_message.value = {
            "payload": {
                "market": "CAISO",
                "delivery_location": "hub",
                "metric_type": "rolling_average_price",
                "metric_value": 52.5
            }
        }
        mock_message.topic = "test.rolling.metrics.v1"
        mock_message.partition = 0
        mock_message.offset = 456

        # Process message
        await metrics_consumer._process_message(mock_message)

        # Verify projection was called
        metrics_consumer.projection_service.project_rolling_metrics.assert_called_once()

        # Verify stats were updated
        assert metrics_consumer.stats["total_messages"] == 1
        assert metrics_consumer.stats["successful_projections"] == 1


class TestKafkaCurveConsumer:
    """Test Kafka curve consumer."""

    @pytest.fixture
    def curve_consumer(self):
        """Create curve consumer."""
        config = CurveConsumerConfig(
            bootstrap_servers="localhost:9092",
            topic="test.curve.prestage.v1",
            group_id="test-curve-consumer"
        )

        with patch('services.service-projection.src.consumers.curve_consumer.AIOKafkaConsumer') as mock_consumer_class:
            consumer = KafkaCurveConsumer(config, Mock())
            consumer.consumer = mock_consumer_class.return_value
            return consumer

    @pytest.mark.asyncio
    async def test_process_message_success(self, curve_consumer):
        """Test successful curve message processing."""
        # Mock projection service
        curve_consumer.projection_service.project_curve_data = AsyncMock(return_value=Mock(records_projected=1))

        # Mock Kafka message
        mock_message = Mock()
        mock_message.value = {
            "payload": {
                "market": "CAISO",
                "curve_type": "spot_curve",
                "delivery_date": "2025-01-15",
                "price": 52.0
            }
        }
        mock_message.topic = "test.curve.prestage.v1"
        mock_message.partition = 0
        mock_message.offset = 789

        # Process message
        await curve_consumer._process_message(mock_message)

        # Verify projection was called
        curve_consumer.projection_service.project_curve_data.assert_called_once()

        # Verify stats were updated
        assert curve_consumer.stats["total_messages"] == 1
        assert curve_consumer.stats["successful_projections"] == 1
