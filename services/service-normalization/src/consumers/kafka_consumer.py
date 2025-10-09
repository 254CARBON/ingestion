"""
Kafka consumer for the normalization service.

This module handles consuming raw market data from Kafka topics
and passing it to the normalization service for processing.
"""

import asyncio
import json
import logging
import re
import time
from typing import Any, Dict, List, Optional
from uuid import uuid4

import structlog
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

from ..core.normalizer import NormalizationService
from ..core.rules_engine import RulesEngine
from ..core.validators import ValidationService
from ..core.metrics import MetricsCollector
from ..producers.kafka_producer import KafkaProducerService


class KafkaConsumerService:
    """
    Kafka consumer service for the normalization service.
    
    This service consumes raw market data from Kafka topics and processes
    it through the normalization pipeline.
    """
    
    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        schema_registry_url: str = "http://localhost:8081",
        input_topic_pattern: str = "ingestion.*.raw.v1",
        group_id: str = None,
        normalization_service: Optional[NormalizationService] = None,
        rules_engine: Optional[RulesEngine] = None,
        validation_service: Optional[ValidationService] = None,
        kafka_producer: Optional[KafkaProducerService] = None,
        metrics_collector: Optional[MetricsCollector] = None,
        output_topic: Optional[str] = None,
        output_schema_str: Optional[str] = None,
        **kwargs
    ):
        """
        Initialize the Kafka consumer service.
        
        Args:
            bootstrap_servers: Kafka bootstrap servers
            schema_registry_url: Schema Registry URL
            input_topic_pattern: Pattern for input topics
            group_id: Consumer group ID
            **kwargs: Additional consumer configuration
        """
        self.bootstrap_servers = bootstrap_servers
        self.schema_registry_url = schema_registry_url
        self.input_topic_pattern = input_topic_pattern
        self.group_id = group_id or f"normalization-consumer-{uuid4().hex[:8]}"
        self.logger = structlog.get_logger(__name__)
        
        # Consumer configuration
        self.consumer_config = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': self.group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True,
            'auto.commit.interval.ms': 1000,
            'session.timeout.ms': 30000,
            'heartbeat.interval.ms': 10000,
            'max.poll.interval.ms': 300000,
            'fetch.wait.max.ms': 500,
            'fetch.min.bytes': 1,
            'fetch.max.bytes': 52428800,  # 50MB
            'topic.metadata.refresh.interval.ms': 10000,
        }
        self.consumer_config.update(kwargs)
        
        self.consumer: Optional[Consumer] = None
        self.schema_registry_client: Optional[SchemaRegistryClient] = None
        self.avro_deserializers: Dict[str, AvroDeserializer] = {}
        self._is_started = False
        self._running = False
        self._subscription_regex: Optional[str] = None
        
        # Initialize normalization components
        self.normalization_service = normalization_service or NormalizationService()
        self.rules_engine = rules_engine or RulesEngine()
        self.validation_service = validation_service or ValidationService()

        # Downstream publishing & metrics
        self.kafka_producer = kafka_producer
        self.metrics_collector = metrics_collector
        self.output_topic = output_topic or getattr(kafka_producer, "output_topic", None)
        self._output_schema = output_schema_str
        self._last_consume_timestamp: Optional[float] = None
    
    async def start(self) -> None:
        """Start the Kafka consumer service."""
        try:
            self.logger.info("Starting normalization Kafka consumer service", 
                           bootstrap_servers=self.bootstrap_servers,
                           input_topic_pattern=self.input_topic_pattern,
                           group_id=self.group_id)
            
            # Initialize consumer
            self.consumer = Consumer(self.consumer_config)
            
            # Subscribe using regex pattern to capture existing and future topics
            regex_pattern = self._pattern_to_regex(self.input_topic_pattern)
            self.consumer.subscribe([regex_pattern])
            self._subscription_regex = regex_pattern
            
            # Log existing topics (if any)
            topics = await self._get_matching_topics()
            if topics:
                self.logger.info("Matched Kafka topics for normalization", topics=topics)
            else:
                self.logger.warning("No topics currently match pattern; waiting for producers", 
                                    pattern=self.input_topic_pattern,
                                    regex_pattern=regex_pattern)
            
            # Initialize Schema Registry client
            self.schema_registry_client = SchemaRegistryClient({
                'url': self.schema_registry_url
            })
            
            self._is_started = True
            self.logger.info("Normalization Kafka consumer service started successfully", 
                             pattern=self.input_topic_pattern,
                             regex_pattern=regex_pattern)
            
        except Exception as e:
            self.logger.error("Failed to start normalization Kafka consumer service", error=str(e))
            raise
    
    async def stop(self) -> None:
        """Stop the Kafka consumer service."""
        if not self._is_started:
            return
        
        try:
            self.logger.info("Stopping normalization Kafka consumer service")
            self._running = False
            
            if self.consumer:
                self.consumer.close()
                self.consumer = None
            
            self.schema_registry_client = None
            self.avro_deserializers = {}
            self._is_started = False
            
            self.logger.info("Normalization Kafka consumer service stopped")
            
        except Exception as e:
            self.logger.error("Error stopping normalization Kafka consumer service", error=str(e))
    
    async def _get_matching_topics(self) -> List[str]:
        """
        Get list of topics matching the input pattern.
        
        Returns:
            List[str]: List of matching topic names
        """
        try:
            if not self.consumer:
                return []
            
            # Get topic metadata
            metadata = self.consumer.list_topics(timeout=5)
            
            # Filter topics matching the pattern
            matching_topics = []
            pattern_parts = self.input_topic_pattern.split('*')
            
            for topic_name in metadata.topics.keys():
                if self._matches_pattern(topic_name, pattern_parts):
                    matching_topics.append(topic_name)
            
            return matching_topics
            
        except Exception as e:
            self.logger.error("Failed to get matching topics", error=str(e))
            return []

    def _pattern_to_regex(self, pattern: str) -> str:
        """
        Convert a glob-like pattern (with *) into a regex understood by Kafka.
        
        Args:
            pattern: Input pattern string
            
        Returns:
            str: Regex pattern string
        """
        if pattern.startswith('^'):
            return pattern
        
        escaped = re.escape(pattern)
        regex = escaped.replace(r'\*', '.*')
        return f"^{regex}$"
    
    def _matches_pattern(self, topic_name: str, pattern_parts: List[str]) -> bool:
        """
        Check if topic name matches the pattern.
        
        Args:
            topic_name: Topic name to check
            pattern_parts: Pattern parts split by '*'
            
        Returns:
            bool: True if matches, False otherwise
        """
        if len(pattern_parts) == 1:
            return topic_name == pattern_parts[0]
        
        if not topic_name.startswith(pattern_parts[0]):
            return False
        
        if not topic_name.endswith(pattern_parts[-1]):
            return False
        
        # Check middle parts
        remaining = topic_name[len(pattern_parts[0]):-len(pattern_parts[-1])]
        for part in pattern_parts[1:-1]:
            if part not in remaining:
                return False
        
        return True
    
    def _setup_avro_deserializer(self, topic: str) -> None:
        """
        Set up Avro deserializer for the given topic.
        
        Args:
            topic: Topic name
        """
        try:
            if not self.schema_registry_client:
                return
            
            # Get schema for topic
            try:
                schema = self.schema_registry_client.get_latest_version(f"{topic}-value")
                self.logger.info("Using existing schema", topic=topic, schema_id=schema.schema_id)
            except Exception as e:
                self.logger.warning("Schema not found for topic", topic=topic, error=str(e))
                return
            
            # Create Avro deserializer
            self.avro_deserializers[topic] = AvroDeserializer(
                self.schema_registry_client,
                schema.schema.schema_str,
                conf={'use.latest.version': True}
            )
            
        except Exception as e:
            self.logger.error("Failed to setup Avro deserializer", topic=topic, error=str(e))
    
    def _deserialize_message(self, topic: str, value: bytes) -> Dict[str, Any]:
        """
        Deserialize a Kafka message.
        
        Args:
            topic: Topic name
            value: Message value bytes
            
        Returns:
            Dict[str, Any]: Deserialized message
        """
        try:
            # Try Avro deserialization first
            if topic in self.avro_deserializers:
                deserializer = self.avro_deserializers[topic]
                return deserializer(
                    value,
                    SerializationContext(topic, MessageField.VALUE)
                )
            else:
                # Fallback to JSON deserialization
                return json.loads(value.decode('utf-8'))
                
        except Exception as e:
            self.logger.error("Failed to deserialize message", topic=topic, error=str(e))
            # Return raw message as fallback
            try:
                return json.loads(value.decode('utf-8'))
            except Exception:
                return {"raw_value": value.decode('utf-8', errors='ignore')}
    
    async def consume_messages(self, batch_size: int = 100, timeout_ms: int = 1000) -> None:
        """
        Consume messages from Kafka topics and process them.
        
        Args:
            batch_size: Maximum number of messages to process in one batch
            timeout_ms: Timeout for polling messages
        """
        if not self._is_started or not self.consumer:
            raise Exception("Consumer not started")
        
        self._running = True
        self.logger.info("Starting message consumption", 
                       pattern=self.input_topic_pattern,
                       batch_size=batch_size)
        
        try:
            while self._running:
                messages = self.consumer.consume(
                    num_messages=batch_size,
                    timeout=timeout_ms / 1000.0
                )
                
                if not messages:
                    continue
                
                for msg in messages:
                    if msg is None:
                        continue
                    
                    if msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            self.logger.debug("Reached end of partition", 
                                            topic=msg.topic(), 
                                            partition=msg.partition())
                        else:
                            self.logger.error("Consumer error", error=str(msg.error()))
                            if self.metrics_collector:
                                self.metrics_collector.record_kafka_consumer_error(
                                    msg.topic() or self.input_topic_pattern,
                                    str(msg.error().code())
                                )
                        continue
                    
                    topic = msg.topic()
                    if topic and topic not in self.avro_deserializers:
                        self._setup_avro_deserializer(topic)
                    
                    try:
                        message_data = self._deserialize_message(topic, msg.value())
                        await self._process_message(msg, message_data)
                    
                    except Exception as e:
                        self.logger.error("Error processing message", 
                                        error=str(e),
                                        topic=topic,
                                        partition=msg.partition(),
                                        offset=msg.offset())
                        if self.metrics_collector:
                            self.metrics_collector.record_kafka_consumer_error(
                                topic or "unknown",
                                "processing_error"
                            )
                        continue
                
                if self.consumer:
                    self.consumer.commit(asynchronous=False)
                
        except Exception as e:
            self.logger.error("Error in message consumption loop", error=str(e))
            raise
        
        finally:
            self._running = False
            self.logger.info("Message consumption stopped")
    
    async def _process_message(self, message, message_data: Dict[str, Any]) -> None:
        """
        Process a single message through the normalization pipeline.
        
        Args:
            message: Original Kafka message
            message_data: Message data to process
        """
        start_time = time.perf_counter()
        topic = message.topic() or self.input_topic_pattern
        partition = message.partition() if message.partition() is not None else -1
        offset = message.offset()
        previous_timestamp = self._last_consume_timestamp
        
        try:
            # Extract payload (support envelope and direct payload)
            payload: Dict[str, Any] = {}
            if isinstance(message_data, dict):
                payload = message_data.get("payload") or {}
                if not payload and "payload" not in message_data:
                    payload = message_data
            
            if not payload:
                self.logger.warning("Empty payload in message", message_id=message_data.get("event_id"))
                if self.metrics_collector:
                    self.metrics_collector.record_normalization_error("UNKNOWN", "empty_payload")
                return
            
            if self.metrics_collector:
                self.metrics_collector.record_kafka_message_consumed(topic, partition)
            
            # Apply normalization rules
            normalization_result = await self.normalization_service.normalize(payload)
            normalized_payload = getattr(normalization_result, "normalized_data", {})
            
            if not normalized_payload:
                self.logger.warning("Normalization produced empty payload", message_id=message_data.get("event_id"))
                if self.metrics_collector:
                    market = payload.get("market", "UNKNOWN")
                    self.metrics_collector.record_normalization_error(market, "empty_output")
                return
            
            # Validate normalized data
            validation_result = await self.validation_service.validate(normalized_payload)
            
            prepared_payload = self._prepare_normalized_payload(
                normalized_payload,
                normalization_result,
                validation_result
            )
            market = prepared_payload.get("market", "UNKNOWN")
            
            publish_success = False
            if self.kafka_producer:
                publish_success = await self.kafka_producer.publish_normalized_record(
                    prepared_payload,
                    schema_str=self._output_schema
                )
            else:
                self.logger.warning("Kafka producer not configured; skipping publish", topic=self.output_topic)
            
            processing_duration = time.perf_counter() - start_time
            timestamp = self._record_processing_metrics(
                market=market,
                normalized_payload=prepared_payload,
                normalization_result=normalization_result,
                validation_result=validation_result,
                publish_success=publish_success,
                duration=processing_duration,
                topic=topic,
                partition=partition,
                previous_timestamp=previous_timestamp
            )
            self._last_consume_timestamp = timestamp
            
            self.logger.debug(
                "Message processed successfully",
                topic=topic,
                partition=partition,
                offset=offset,
                publish_success=publish_success,
                validation_status=getattr(validation_result, "status", "unknown")
            )
            
        except Exception as e:
            self.logger.error("Failed to process message", 
                            error=str(e),
                            message_id=message_data.get("event_id"))
            raise

    def _prepare_normalized_payload(
        self,
        normalized_payload: Dict[str, Any],
        normalization_result,
        validation_result,
    ) -> Dict[str, Any]:
        """
        Merge normalization and validation metadata into the payload.
        
        Args:
            normalized_payload: Base normalized payload
            normalization_result: Result object from normalization
            validation_result: Result object from validation
        
        Returns:
            Dict[str, Any]: Prepared payload ready for publishing
        """
        prepared = dict(normalized_payload)
        
        metadata_map = self._stringify_metadata(getattr(normalization_result, "metadata", {}))
        metadata_map["validation_status"] = getattr(validation_result, "status", "unknown")
        metadata_map["quality_score"] = f"{getattr(normalization_result, 'quality_score', 0.0):.4f}"
        metadata_map["validation_score"] = f"{getattr(validation_result, 'score', 0.0):.4f}"
        prepared["normalization_metadata"] = metadata_map
        
        # Merge validation errors
        validation_errors = list(prepared.get("validation_errors", []))
        for err in getattr(validation_result, "errors", []):
            if err not in validation_errors:
                validation_errors.append(err)
        for err in getattr(normalization_result, "validation_errors", []):
            if err not in validation_errors:
                validation_errors.append(err)
        prepared["validation_errors"] = validation_errors
        
        # Merge quality flags with validation warnings
        quality_flags = list(prepared.get("quality_flags") or [])
        for warning in getattr(validation_result, "warnings", []):
            warning_str = str(warning)
            if warning_str not in quality_flags:
                quality_flags.append(warning_str)
        prepared["quality_flags"] = quality_flags
        
        return prepared

    @staticmethod
    def _stringify_metadata(metadata: Dict[str, Any]) -> Dict[str, str]:
        """
        Convert metadata values to strings to satisfy Avro schema constraints.
        """
        stringified: Dict[str, str] = {}
        for key, value in metadata.items():
            if isinstance(value, str):
                stringified[key] = value
            else:
                try:
                    stringified[key] = json.dumps(value, default=str)
                except (TypeError, ValueError):
                    stringified[key] = str(value)
        return stringified

    def _record_processing_metrics(
        self,
        *,
        market: str,
        normalized_payload: Dict[str, Any],
        normalization_result,
        validation_result,
        publish_success: bool,
        duration: float,
        topic: str,
        partition: int,
        previous_timestamp: Optional[float],
    ) -> float:
        """
        Record metrics related to normalization processing.
        
        Returns:
            float: Current timestamp for throughput calculations
        """
        current_time = time.time()
        
        if not self.metrics_collector:
            return current_time
        
        data_type = normalized_payload.get("instrument_type") or normalized_payload.get("data_type", "unknown")
        status = getattr(validation_result, "status", "unknown")
        
        self.metrics_collector.record_record_processed(
            market=market,
            data_type=data_type,
            status=status,
            duration=duration,
            batch_size=1
        )
        
        self.metrics_collector.record_data_quality(
            market=market,
            data_type=data_type,
            score=getattr(normalization_result, "quality_score", 0.0)
        )
        
        # Record validation errors and warnings
        for err in getattr(validation_result, "errors", []):
            self.metrics_collector.record_validation_error(market, str(err))
        for warning in getattr(validation_result, "warnings", []):
            self.metrics_collector.record_validation_error(market, f"warning:{warning}")
        for err in getattr(normalization_result, "validation_errors", []):
            self.metrics_collector.record_validation_error(market, f"normalization:{err}")
        
        # Kafka metrics
        if publish_success:
            self.metrics_collector.record_kafka_message_produced(
                self.output_topic or getattr(self.kafka_producer, "output_topic", "normalized.market.ticks.v1"),
                partition if isinstance(partition, int) else -1
            )
        elif self.kafka_producer or self.output_topic:
            self.metrics_collector.record_kafka_producer_error(
                self.output_topic or getattr(self.kafka_producer, "output_topic", "normalized.market.ticks.v1"),
                "publish_failed"
            )
        
        # Volume and price metrics
        quantity = normalized_payload.get("quantity")
        if isinstance(quantity, (int, float)):
            unit = normalized_payload.get("unit", "MWh")
            self.metrics_collector.record_market_data_volume(market, data_type, unit, float(quantity))
        
        price = normalized_payload.get("price")
        if isinstance(price, (int, float)):
            self.metrics_collector.record_price_range(market, data_type, float(price))
        
        # Throughput
        if previous_timestamp:
            elapsed = current_time - previous_timestamp
            if elapsed > 0:
                records_per_second = 1.0 / elapsed
                self.metrics_collector.record_throughput(market, records_per_second)
        
        return current_time
    
    def get_metrics(self) -> Dict[str, Any]:
        """
        Get consumer metrics.
        
        Returns:
            Dict[str, Any]: Consumer metrics
        """
        if not self.consumer:
            return {}
        
        try:
            # Get topic metadata
            metadata = self.consumer.list_topics(timeout=1)
            
            return {
                "topics": list(metadata.topics.keys()),
                "is_started": self._is_started,
                "is_running": self._running,
                "bootstrap_servers": self.bootstrap_servers,
                "input_topic_pattern": self.input_topic_pattern,
                "group_id": self.group_id
            }
        except Exception as e:
            self.logger.error("Failed to get consumer metrics", error=str(e))
            return {"error": str(e)}
