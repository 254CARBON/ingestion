"""
Kafka producer service for connectors.

This module provides a Kafka producer implementation with Schema Registry integration
for publishing connector data to Kafka topics.
"""

import asyncio
import json
import logging
from typing import Any, Dict, Optional
from uuid import uuid4

import structlog
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

from .exceptions import ConnectorError


class KafkaProducerService:
    """
    Kafka producer service with Schema Registry integration.
    
    This service handles publishing connector data to Kafka topics with
    proper schema validation and serialization.
    """
    
    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        schema_registry_url: str = "http://localhost:8081",
        output_topic: str = "ingestion.raw.v1",
        **kwargs
    ):
        """
        Initialize the Kafka producer service.
        
        Args:
            bootstrap_servers: Kafka bootstrap servers
            schema_registry_url: Schema Registry URL
            output_topic: Output topic name
            **kwargs: Additional producer configuration
        """
        self.bootstrap_servers = bootstrap_servers
        self.schema_registry_url = schema_registry_url
        self.output_topic = output_topic
        self.logger = structlog.get_logger(__name__)
        
        # Producer configuration
        self.producer_config = {
            'bootstrap.servers': bootstrap_servers,
            'client.id': f"connector-producer-{uuid4().hex[:8]}",
            'acks': 'all',
            'retries': 3,
            'retry.backoff.ms': 1000,
            'batch.size': 16384,
            'linger.ms': 10,
            'compression.type': 'snappy',
            **kwargs
        }
        
        self.producer: Optional[Producer] = None
        self.schema_registry_client: Optional[SchemaRegistryClient] = None
        self.avro_serializer: Optional[AvroSerializer] = None
        self._is_started = False
    
    async def start(self) -> None:
        """Start the Kafka producer service."""
        try:
            self.logger.info("Starting Kafka producer service", 
                           bootstrap_servers=self.bootstrap_servers,
                           output_topic=self.output_topic)
            
            # Initialize producer
            self.producer = Producer(self.producer_config)
            
            # Initialize Schema Registry client
            self.schema_registry_client = SchemaRegistryClient({
                'url': self.schema_registry_url
            })
            
            # Create Avro serializer (will be set up when schema is available)
            self.avro_serializer = None
            
            self._is_started = True
            self.logger.info("Kafka producer service started successfully")
            
        except Exception as e:
            self.logger.error("Failed to start Kafka producer service", error=str(e))
            raise ConnectorError(f"Failed to start Kafka producer: {e}") from e
    
    async def stop(self) -> None:
        """Stop the Kafka producer service."""
        if not self._is_started:
            return
        
        try:
            self.logger.info("Stopping Kafka producer service")
            
            if self.producer:
                # Flush any remaining messages
                self.producer.flush(timeout=10)
                self.producer = None
            
            self.schema_registry_client = None
            self.avro_serializer = None
            self._is_started = False
            
            self.logger.info("Kafka producer service stopped")
            
        except Exception as e:
            self.logger.error("Error stopping Kafka producer service", error=str(e))
    
    def _setup_avro_serializer(self, schema_str: str) -> None:
        """
        Set up Avro serializer with the given schema.
        
        Args:
            schema_str: Avro schema as string
        """
        try:
            if not self.schema_registry_client:
                raise ConnectorError("Schema Registry client not initialized")
            
            # Register schema if not exists
            try:
                schema = self.schema_registry_client.get_latest_version(
                    f"{self.output_topic}-value"
                )
                self.logger.info("Using existing schema", schema_id=schema.schema_id)
            except Exception:
                # Schema doesn't exist, register it
                schema = self.schema_registry_client.register_schema(
                    f"{self.output_topic}-value",
                    schema_str
                )
                self.logger.info("Registered new schema", schema_id=schema.schema_id)
            
            # Create Avro serializer
            self.avro_serializer = AvroSerializer(
                self.schema_registry_client,
                schema_str,
                conf={'auto.register.schemas': False}
            )
            
        except Exception as e:
            self.logger.error("Failed to setup Avro serializer", error=str(e))
            raise ConnectorError(f"Failed to setup Avro serializer: {e}") from e
    
    async def publish_record(self, record: Dict[str, Any], schema_str: Optional[str] = None) -> bool:
        """
        Publish a single record to Kafka.
        
        Args:
            record: Record data to publish
            schema_str: Avro schema string (optional)
            
        Returns:
            bool: True if published successfully, False otherwise
        """
        if not self._is_started or not self.producer:
            raise ConnectorError("Producer not started")
        
        try:
            # Add envelope fields
            envelope = {
                "event_id": str(uuid4()),
                "trace_id": str(uuid4()),
                "occurred_at": int(asyncio.get_event_loop().time() * 1000000),  # microseconds
                "tenant_id": record.get("tenant_id", "default"),
                "schema_version": "1.0.0",
                "producer": "connector",
                "payload": record
            }
            
            # Serialize message
            if schema_str and self.avro_serializer is None:
                self._setup_avro_serializer(schema_str)
            
            if self.avro_serializer:
                # Use Avro serialization
                serialized_value = self.avro_serializer(
                    envelope,
                    SerializationContext(self.output_topic, MessageField.VALUE)
                )
                message_value = serialized_value
            else:
                # Fallback to JSON serialization
                message_value = json.dumps(envelope).encode('utf-8')
            
            # Publish message
            future = self.producer.produce(
                topic=self.output_topic,
                value=message_value,
                key=envelope["event_id"].encode('utf-8'),
                callback=self._delivery_callback
            )
            
            # Wait for delivery confirmation
            self.producer.poll(0)
            
            self.logger.debug("Record published successfully", 
                            event_id=envelope["event_id"],
                            topic=self.output_topic)
            return True
            
        except Exception as e:
            self.logger.error("Failed to publish record", error=str(e), record=record)
            return False
    
    def _delivery_callback(self, err, msg) -> None:
        """
        Callback for message delivery confirmation.
        
        Args:
            err: Error if delivery failed
            msg: Message object
        """
        if err is not None:
            self.logger.error("Message delivery failed", error=str(err), topic=msg.topic())
        else:
            self.logger.debug("Message delivered successfully", 
                            topic=msg.topic(), 
                            partition=msg.partition(),
                            offset=msg.offset())
    
    async def publish_batch(self, records: list[Dict[str, Any]], schema_str: Optional[str] = None) -> int:
        """
        Publish a batch of records to Kafka.
        
        Args:
            records: List of records to publish
            schema_str: Avro schema string (optional)
            
        Returns:
            int: Number of successfully published records
        """
        if not self._is_started or not self.producer:
            raise ConnectorError("Producer not started")
        
        success_count = 0
        
        try:
            self.logger.info("Publishing batch of records", 
                           count=len(records), 
                           topic=self.output_topic)
            
            for record in records:
                if await self.publish_record(record, schema_str):
                    success_count += 1
            
            # Flush to ensure all messages are sent
            self.producer.flush(timeout=10)
            
            self.logger.info("Batch publishing completed", 
                           success_count=success_count,
                           total_count=len(records))
            
        except Exception as e:
            self.logger.error("Failed to publish batch", error=str(e))
        
        return success_count
    
    def get_metrics(self) -> Dict[str, Any]:
        """
        Get producer metrics.
        
        Returns:
            Dict[str, Any]: Producer metrics
        """
        if not self.producer:
            return {}
        
        try:
            metrics = self.producer.list_topics(timeout=1)
            return {
                "topics": list(metrics.topics.keys()),
                "is_started": self._is_started,
                "bootstrap_servers": self.bootstrap_servers,
                "output_topic": self.output_topic
            }
        except Exception as e:
            self.logger.error("Failed to get producer metrics", error=str(e))
            return {"error": str(e)}
