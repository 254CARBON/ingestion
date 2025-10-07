"""
Integration tests for Kafka integration.
"""

import pytest
import asyncio
import json
from unittest.mock import Mock, AsyncMock, patch
from datetime import datetime, timezone
from uuid import uuid4

from services.service-enrichment.src.consumers.kafka_consumer import KafkaConsumerService, ConsumerConfig
from services.service-enrichment.src.producers.kafka_producer import KafkaProducerService, ProducerConfig
from services.service-enrichment.src.core.enricher import EnrichmentService, EnrichmentResult
from services.service-aggregation.src.consumers.kafka_consumer import KafkaConsumerService as AggKafkaConsumerService
from services.service-aggregation.src.producers.kafka_producer import KafkaProducerService as AggKafkaProducerService
from services.service-aggregation.src.core.aggregator import AggregationService


class TestKafkaIntegration:
    """Test Kafka integration for services."""

    @pytest.fixture
    def consumer_config(self):
        """Create consumer configuration."""
        return ConsumerConfig(
            bootstrap_servers="localhost:9092",
            topic="test.normalized.ticks.v1",
            group_id="test-enrichment-group"
        )

    @pytest.fixture
    def producer_config(self):
        """Create producer configuration."""
        return ProducerConfig(
            bootstrap_servers="localhost:9092",
            output_topic="test.enriched.ticks.v1"
        )

    @pytest.fixture
    def enrichment_service(self):
        """Create enrichment service."""
        with patch('services.service-enrichment.src.core.enricher.TaxonomyService') as mock_taxonomy:
            mock_taxonomy.return_value.get_market_taxonomy.return_value = {
                "instruments": {
                    "market_price": ["spot_pricing", "real_time_market"]
                },
                "locations": {
                    "hub": ["trading_hub", "high_liquidity"]
                },
                "price_ranges": {
                    "medium": {"min": 50, "max": 100, "tags": ["medium_price"]}
                },
                "quantity_ranges": {
                    "small": {"min": 0, "max": 500, "tags": ["small_quantity"]}
                },
                "time_periods": {
                    "afternoon": {"start": 12, "end": 18, "tags": ["afternoon"]}
                }
            }
            return EnrichmentService()

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
    def sample_normalized_data(self):
        """Sample normalized data."""
        return {
            "event_id": str(uuid4()),
            "occurred_at": int(datetime.now(timezone.utc).timestamp() * 1_000_000),
            "tenant_id": "default",
            "schema_version": "1.0.0",
            "producer": "normalization-service",
            "market": "CAISO",
            "data_type": "market_price",
            "delivery_location": "hub",
            "delivery_date": "2025-01-15",
            "delivery_hour": 14,
            "price": 75.50,
            "quantity": 250.0,
            "source": "caiso-oasis"
        }

    @pytest.fixture
    def sample_enriched_data(self):
        """Sample enriched data."""
        return {
            "event_id": str(uuid4()),
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
            "taxonomy_tags": ["spot_pricing", "real_time_market", "trading_hub"],
            "semantic_tags": ["california_energy", "normal_price", "daytime"],
            "geospatial_data": {
                "location": "hub",
                "coordinates": [39.8283, -98.5795],
                "region": "hub_region",
                "timezone": "America/Chicago",
                "market_region": "California",
                "market_coordinates": [36.7783, -119.4179]
            },
            "metadata_tags": {
                "data_quality": {"completeness": 1.0, "accuracy": 1.0, "consistency": 1.0},
                "business_context": {"market_segment": "california_energy_market", "trading_session": "off_peak_session", "price_tier": "mid_tier"},
                "technical_context": {"data_source": "caiso-oasis", "processing_stage": "enriched", "enrichment_version": "1.0.0"},
                "temporal_context": {"delivery_date": "2025-01-15", "delivery_day_of_week": "Wednesday", "delivery_month": "January", "delivery_quarter": "Q1"}
            },
            "enrichment_score": 0.95,
            "enrichment_timestamp": datetime.now(timezone.utc).isoformat()
        }

    @pytest.mark.asyncio
    async def test_enrichment_kafka_consumer_start_stop(self, consumer_config, enrichment_service):
        """Test enrichment Kafka consumer start/stop."""
        consumer = KafkaConsumerService(consumer_config, enrichment_service)
        
        # Mock the AIOKafkaConsumer
        with patch('services.service-enrichment.src.consumers.kafka_consumer.AIOKafkaConsumer') as mock_consumer_class:
            mock_consumer = AsyncMock()
            mock_consumer_class.return_value = mock_consumer
            
            # Test start
            await consumer.start()
            mock_consumer.start.assert_called_once()
            
            # Test stop
            await consumer.stop()
            mock_consumer.stop.assert_called_once()

    @pytest.mark.asyncio
    async def test_enrichment_kafka_producer_start_stop(self, producer_config):
        """Test enrichment Kafka producer start/stop."""
        producer = KafkaProducerService(producer_config)
        
        # Mock the AIOKafkaProducer
        with patch('services.service-enrichment.src.producers.kafka_producer.AIOKafkaProducer') as mock_producer_class:
            mock_producer = AsyncMock()
            mock_producer_class.return_value = mock_producer
            
            # Test start
            await producer.start()
            mock_producer.start.assert_called_once()
            
            # Test stop
            await producer.stop()
            mock_producer.stop.assert_called_once()

    @pytest.mark.asyncio
    async def test_enrichment_message_processing(self, consumer_config, enrichment_service, sample_normalized_data):
        """Test enrichment message processing."""
        consumer = KafkaConsumerService(consumer_config, enrichment_service)
        
        # Mock the AIOKafkaConsumer
        with patch('services.service-enrichment.src.consumers.kafka_consumer.AIOKafkaConsumer') as mock_consumer_class:
            mock_consumer = AsyncMock()
            mock_consumer_class.return_value = mock_consumer
            
            # Mock message
            mock_message = Mock()
            mock_message.value = sample_normalized_data
            mock_message.topic = "test.normalized.ticks.v1"
            mock_message.partition = 0
            mock_message.offset = 123
            mock_message.timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)
            mock_message.key = None
            
            # Mock consumer iteration
            mock_consumer.__aiter__.return_value = [mock_message]
            
            # Set up processing callback
            processing_callback = AsyncMock()
            consumer.set_processing_callback(processing_callback)
            
            # Start consumer
            await consumer.start()
            
            # Process message
            await consumer._process_message(mock_message)
            
            # Verify callback was called
            processing_callback.assert_called_once()
            
            # Verify enrichment was performed
            callback_args = processing_callback.call_args[0][0]
            assert isinstance(callback_args, EnrichmentResult)
            assert callback_args.enriched_data["event_id"] == sample_normalized_data["event_id"]
            assert "taxonomy_tags" in callback_args.enriched_data
            assert "semantic_tags" in callback_args.enriched_data

    @pytest.mark.asyncio
    async def test_enrichment_data_publishing(self, producer_config, enrichment_service, sample_normalized_data):
        """Test enriched data publishing."""
        producer = KafkaProducerService(producer_config)
        
        # Mock the AIOKafkaProducer
        with patch('services.service-enrichment.src.producers.kafka_producer.AIOKafkaProducer') as mock_producer_class:
            mock_producer = AsyncMock()
            mock_producer_class.return_value = mock_producer
            
            # Start producer
            await producer.start()
            
            # Create enrichment result
            enrichment_result = await enrichment_service.enrich(sample_normalized_data)
            
            # Publish enriched data
            success = await producer.publish_enriched_data(enrichment_result)
            
            # Verify success
            assert success is True
            mock_producer.send_and_wait.assert_called_once()
            
            # Verify message structure
            call_args = mock_producer.send_and_wait.call_args
            assert call_args[0][0] == producer_config.output_topic  # topic
            message_payload = call_args[1]["value"]
            assert "event_id" in message_payload
            assert "payload" in message_payload
            assert "taxonomy_tags" in message_payload
            assert "semantic_tags" in message_payload
            assert "enrichment_score" in message_payload

    @pytest.mark.asyncio
    async def test_aggregation_kafka_consumer_start_stop(self, aggregation_service):
        """Test aggregation Kafka consumer start/stop."""
        consumer_config = ConsumerConfig(
            bootstrap_servers="localhost:9092",
            topic="test.enriched.ticks.v1",
            group_id="test-aggregation-group"
        )
        
        consumer = AggKafkaConsumerService(consumer_config, aggregation_service)
        
        # Mock the AIOKafkaConsumer
        with patch('services.service-aggregation.src.consumers.kafka_consumer.AIOKafkaConsumer') as mock_consumer_class:
            mock_consumer = AsyncMock()
            mock_consumer_class.return_value = mock_consumer
            
            # Test start
            await consumer.start()
            mock_consumer.start.assert_called_once()
            
            # Test stop
            await consumer.stop()
            mock_consumer.stop.assert_called_once()

    @pytest.mark.asyncio
    async def test_aggregation_kafka_producer_start_stop(self):
        """Test aggregation Kafka producer start/stop."""
        producer_config = ProducerConfig(
            bootstrap_servers="localhost:9092",
            output_topics={
                "ohlc": "test.ohlc.bars.v1",
                "rolling": "test.rolling.metrics.v1",
                "curve": "test.curve.prestage.v1"
            }
        )
        
        producer = AggKafkaProducerService(producer_config)
        
        # Mock the AIOKafkaProducer
        with patch('services.service-aggregation.src.producers.kafka_producer.AIOKafkaProducer') as mock_producer_class:
            mock_producer = AsyncMock()
            mock_producer_class.return_value = mock_producer
            
            # Test start
            await producer.start()
            mock_producer.start.assert_called_once()
            
            # Test stop
            await producer.stop()
            mock_producer.stop.assert_called_once()

    @pytest.mark.asyncio
    async def test_aggregation_message_processing(self, aggregation_service, sample_enriched_data):
        """Test aggregation message processing."""
        consumer_config = ConsumerConfig(
            bootstrap_servers="localhost:9092",
            topic="test.enriched.ticks.v1",
            group_id="test-aggregation-group"
        )
        
        consumer = AggKafkaConsumerService(consumer_config, aggregation_service)
        
        # Mock the AIOKafkaConsumer
        with patch('services.service-aggregation.src.consumers.kafka_consumer.AIOKafkaConsumer') as mock_consumer_class:
            mock_consumer = AsyncMock()
            mock_consumer_class.return_value = mock_consumer
            
            # Mock message with envelope
            message_payload = {
                "event_id": str(uuid4()),
                "trace_id": str(uuid4()),
                "occurred_at": datetime.now(timezone.utc).isoformat(),
                "tenant_id": "default",
                "schema_version": "1.0.0",
                "producer": "enrichment-service",
                "payload": sample_enriched_data
            }
            
            mock_message = Mock()
            mock_message.value = message_payload
            mock_message.topic = "test.enriched.ticks.v1"
            mock_message.partition = 0
            mock_message.offset = 456
            mock_message.timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)
            mock_message.key = None
            
            # Mock consumer iteration
            mock_consumer.__aiter__.return_value = [mock_message]
            
            # Set up processing callback
            processing_callback = AsyncMock()
            consumer.set_processing_callback(processing_callback)
            
            # Start consumer
            await consumer.start()
            
            # Process message
            await consumer._process_message(mock_message)
            
            # Verify callback was called
            processing_callback.assert_called_once()
            
            # Verify aggregation was performed
            callback_args = processing_callback.call_args[0][0]
            assert hasattr(callback_args, 'ohlc_bars')
            assert hasattr(callback_args, 'rolling_metrics')
            assert hasattr(callback_args, 'curve_prestage')

    @pytest.mark.asyncio
    async def test_aggregation_data_publishing(self, aggregation_service, sample_enriched_data):
        """Test aggregated data publishing."""
        producer_config = ProducerConfig(
            bootstrap_servers="localhost:9092",
            output_topics={
                "ohlc": "test.ohlc.bars.v1",
                "rolling": "test.rolling.metrics.v1",
                "curve": "test.curve.prestage.v1"
            }
        )
        
        producer = AggKafkaProducerService(producer_config)
        
        # Mock the AIOKafkaProducer
        with patch('services.service-aggregation.src.producers.kafka_producer.AIOKafkaProducer') as mock_producer_class:
            mock_producer = AsyncMock()
            mock_producer_class.return_value = mock_producer
            
            # Start producer
            await producer.start()
            
            # Create aggregation result
            aggregation_result = await aggregation_service.aggregate(sample_enriched_data)
            
            # Publish aggregated data
            success = await producer.publish_aggregated_data(aggregation_result)
            
            # Verify success
            assert success is True
            
            # Verify messages were sent to appropriate topics
            assert mock_producer.send_and_wait.call_count >= 1
            
            # Check that messages were sent to correct topics
            calls = mock_producer.send_and_wait.call_args_list
            topics_sent = [call[0][0] for call in calls]
            
            # Should have sent to at least one topic
            assert any(topic in topics_sent for topic in producer_config.output_topics.values())

    @pytest.mark.asyncio
    async def test_end_to_end_data_flow(self, enrichment_service, aggregation_service, sample_normalized_data):
        """Test end-to-end data flow through enrichment and aggregation."""
        # Mock Kafka components
        with patch('services.service-enrichment.src.consumers.kafka_consumer.AIOKafkaConsumer') as mock_enrich_consumer_class, \
             patch('services.service-enrichment.src.producers.kafka_producer.AIOKafkaProducer') as mock_enrich_producer_class, \
             patch('services.service-aggregation.src.consumers.kafka_consumer.AIOKafkaConsumer') as mock_agg_consumer_class, \
             patch('services.service-aggregation.src.producers.kafka_producer.AIOKafkaProducer') as mock_agg_producer_class:
            
            # Set up mocks
            mock_enrich_consumer = AsyncMock()
            mock_enrich_producer = AsyncMock()
            mock_agg_consumer = AsyncMock()
            mock_agg_producer = AsyncMock()
            
            mock_enrich_consumer_class.return_value = mock_enrich_consumer
            mock_enrich_producer_class.return_value = mock_enrich_producer
            mock_agg_consumer_class.return_value = mock_agg_consumer
            mock_agg_producer_class.return_value = mock_agg_producer
            
            # Create services
            enrichment_consumer = KafkaConsumerService(
                ConsumerConfig(bootstrap_servers="localhost:9092", topic="test.normalized.ticks.v1"),
                enrichment_service
            )
            
            enrichment_producer = KafkaProducerService(
                ProducerConfig(bootstrap_servers="localhost:9092", output_topic="test.enriched.ticks.v1")
            )
            
            aggregation_consumer = AggKafkaConsumerService(
                ConsumerConfig(bootstrap_servers="localhost:9092", topic="test.enriched.ticks.v1"),
                aggregation_service
            )
            
            aggregation_producer = AggKafkaProducerService(
                ProducerConfig(
                    bootstrap_servers="localhost:9092",
                    output_topics={
                        "ohlc": "test.ohlc.bars.v1",
                        "rolling": "test.rolling.metrics.v1",
                        "curve": "test.curve.prestage.v1"
                    }
                )
            )
            
            # Start services
            await enrichment_consumer.start()
            await enrichment_producer.start()
            await aggregation_consumer.start()
            await aggregation_producer.start()
            
            # Set up processing callbacks
            enrichment_consumer.set_processing_callback(enrichment_producer.publish_enriched_data)
            aggregation_consumer.set_processing_callback(aggregation_producer.publish_aggregated_data)
            
            # Simulate message flow
            # 1. Normalized data -> Enrichment
            enrichment_result = await enrichment_service.enrich(sample_normalized_data)
            assert isinstance(enrichment_result, EnrichmentResult)
            assert "taxonomy_tags" in enrichment_result.enriched_data
            
            # 2. Enriched data -> Aggregation
            aggregation_result = await aggregation_service.aggregate(enrichment_result.enriched_data)
            assert hasattr(aggregation_result, 'ohlc_bars')
            assert hasattr(aggregation_result, 'rolling_metrics')
            assert hasattr(aggregation_result, 'curve_prestage')
            
            # 3. Publish enriched data
            enrich_success = await enrichment_producer.publish_enriched_data(enrichment_result)
            assert enrich_success is True
            
            # 4. Publish aggregated data
            agg_success = await aggregation_producer.publish_aggregated_data(aggregation_result)
            assert agg_success is True
            
            # Verify all services were started
            mock_enrich_consumer.start.assert_called_once()
            mock_enrich_producer.start.assert_called_once()
            mock_agg_consumer.start.assert_called_once()
            mock_agg_producer.start.assert_called_once()

    @pytest.mark.asyncio
    async def test_kafka_error_handling(self, consumer_config, enrichment_service):
        """Test Kafka error handling."""
        consumer = KafkaConsumerService(consumer_config, enrichment_service)
        
        # Mock the AIOKafkaConsumer to raise an error
        with patch('services.service-enrichment.src.consumers.kafka_consumer.AIOKafkaConsumer') as mock_consumer_class:
            mock_consumer = AsyncMock()
            mock_consumer.start.side_effect = Exception("Kafka connection failed")
            mock_consumer_class.return_value = mock_consumer
            
            # Test that start raises the error
            with pytest.raises(Exception, match="Kafka connection failed"):
                await consumer.start()

    @pytest.mark.asyncio
    async def test_message_serialization_deserialization(self, producer_config, enrichment_service, sample_normalized_data):
        """Test message serialization and deserialization."""
        producer = KafkaProducerService(producer_config)
        
        # Mock the AIOKafkaProducer
        with patch('services.service-enrichment.src.producers.kafka_producer.AIOKafkaProducer') as mock_producer_class:
            mock_producer = AsyncMock()
            mock_producer_class.return_value = mock_producer
            
            # Start producer
            await producer.start()
            
            # Create enrichment result
            enrichment_result = await enrichment_service.enrich(sample_normalized_data)
            
            # Publish enriched data
            await producer.publish_enriched_data(enrichment_result)
            
            # Verify serialization
            call_args = mock_producer.send_and_wait.call_args
            message_payload = call_args[1]["value"]
            
            # Should be JSON serializable
            json_str = json.dumps(message_payload)
            deserialized = json.loads(json_str)
            
            assert deserialized["event_id"] == message_payload["event_id"]
            assert deserialized["payload"]["event_id"] == sample_normalized_data["event_id"]
            assert "taxonomy_tags" in deserialized
            assert "semantic_tags" in deserialized
            assert "enrichment_score" in deserialized

    def test_consumer_stats(self, consumer_config, enrichment_service):
        """Test consumer statistics."""
        consumer = KafkaConsumerService(consumer_config, enrichment_service)
        
        # Add some stats
        consumer.stats["total_messages"] = 10
        consumer.stats["successful_enrichments"] = 8
        consumer.stats["processing_latency_ms"] = [100, 150, 200]
        
        stats = consumer.get_stats()
        
        assert stats["total_messages"] == 10
        assert stats["successful_enrichments"] == 8
        assert stats["average_latency_ms"] == 150.0
        assert stats["success_rate"] == 0.8

    def test_producer_stats(self, producer_config):
        """Test producer statistics."""
        producer = KafkaProducerService(producer_config)
        
        # Add some stats
        producer.stats["total_messages"] = 15
        producer.stats["successful_publishes"] = 12
        producer.stats["publish_latency_ms"] = [50, 75, 100]
        
        stats = producer.get_stats()
        
        assert stats["total_messages"] == 15
        assert stats["successful_publishes"] == 12
        assert stats["average_latency_ms"] == 75.0
        assert stats["success_rate"] == 0.8
