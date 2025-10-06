"""
Kafka consumer for the normalization service.

This module handles consuming raw market data from Kafka topics
and passing it to the normalization service for processing.
"""

import asyncio
import json
import logging
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
            **kwargs
        }
        
        self.consumer: Optional[Consumer] = None
        self.schema_registry_client: Optional[SchemaRegistryClient] = None
        self.avro_deserializers: Dict[str, AvroDeserializer] = {}
        self._is_started = False
        self._running = False
        
        # Initialize normalization components
        self.normalization_service = NormalizationService()
        self.rules_engine = RulesEngine()
        self.validation_service = ValidationService()
    
    async def start(self) -> None:
        """Start the Kafka consumer service."""
        try:
            self.logger.info("Starting normalization Kafka consumer service", 
                           bootstrap_servers=self.bootstrap_servers,
                           input_topic_pattern=self.input_topic_pattern,
                           group_id=self.group_id)
            
            # Initialize consumer
            self.consumer = Consumer(self.consumer_config)
            
            # Get list of topics matching the pattern
            topics = await self._get_matching_topics()
            if not topics:
                self.logger.warning("No topics found matching pattern", pattern=self.input_topic_pattern)
                return
            
            # Subscribe to topics
            self.consumer.subscribe(topics)
            
            # Initialize Schema Registry client
            self.schema_registry_client = SchemaRegistryClient({
                'url': self.schema_registry_url
            })
            
            self._is_started = True
            self.logger.info("Normalization Kafka consumer service started successfully", topics=topics)
            
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
                # Poll for messages
                messages = self.consumer.consume(
                    num_messages=batch_size,
                    timeout=timeout_ms / 1000.0
                )
                
                if not messages:
                    continue
                
                # Process messages
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
                        continue
                    
                    try:
                        # Deserialize message
                        message_data = self._deserialize_message(msg.topic(), msg.value())
                        
                        # Process message through normalization pipeline
                        await self._process_message(message_data)
                        
                        self.logger.debug("Message processed successfully", 
                                        topic=msg.topic(),
                                        partition=msg.partition(),
                                        offset=msg.offset())
                    
                    except Exception as e:
                        self.logger.error("Error processing message", 
                                        error=str(e),
                                        topic=msg.topic(),
                                        partition=msg.partition(),
                                        offset=msg.offset())
                        continue
                
                # Commit offsets
                self.consumer.commit(asynchronous=False)
                
        except Exception as e:
            self.logger.error("Error in message consumption loop", error=str(e))
            raise
        
        finally:
            self._running = False
            self.logger.info("Message consumption stopped")
    
    async def _process_message(self, message_data: Dict[str, Any]) -> None:
        """
        Process a single message through the normalization pipeline.
        
        Args:
            message_data: Message data to process
        """
        try:
            # Extract payload
            payload = message_data.get("payload", {})
            if not payload:
                self.logger.warning("Empty payload in message", message_id=message_data.get("event_id"))
                return
            
            # Apply normalization rules
            normalized_data = await self.normalization_service.normalize(payload)
            
            # Validate normalized data
            validation_result = await self.validation_service.validate(normalized_data)
            
            # TODO: Publish to output topic via producer
            self.logger.info("Message normalized successfully", 
                           message_id=message_data.get("event_id"),
                           validation_status=validation_result.get("status"))
            
        except Exception as e:
            self.logger.error("Failed to process message", 
                            error=str(e),
                            message_id=message_data.get("event_id"))
            raise
    
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