"""
Kafka producer service for connectors.

This module provides a Kafka producer implementation with Schema Registry integration
for publishing connector data to Kafka topics.
"""

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4

import structlog
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

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
        *,
        use_event_envelope: bool = True,
        default_producer: str = "connector",
        **kwargs: Any,
    ):
        """
        Initialize the Kafka producer service.
        
        Args:
            bootstrap_servers: Kafka bootstrap servers
            schema_registry_url: Schema Registry URL
            output_topic: Output topic name
            use_event_envelope: Whether to wrap payloads in the standard event envelope
            default_producer: Default producer identifier applied to events
            **kwargs: Additional producer configuration
        """
        self.bootstrap_servers = bootstrap_servers
        self.schema_registry_url = schema_registry_url
        self.output_topic = output_topic
        self.use_event_envelope = use_event_envelope
        self.default_producer = default_producer
        self.logger = structlog.get_logger(__name__)
        
        # Producer configuration
        self.producer_config: Dict[str, Any] = {
            "bootstrap.servers": bootstrap_servers,
            "client.id": f"connector-producer-{uuid4().hex[:8]}",
            "acks": "all",
            "retries": 3,
            "retry.backoff.ms": 1000,
            "batch.size": 16384,
            "linger.ms": 10,
            "compression.type": "snappy",
            **kwargs,
        }
        self.client_id = self.producer_config["client.id"]
        
        self.producer: Optional[Producer] = None
        self.schema_registry_client: Optional[SchemaRegistryClient] = None
        self.avro_serializer: Optional[AvroSerializer] = None
        self._is_started = False
    
    async def start(self) -> None:
        """Start the Kafka producer service."""
        try:
            self.logger.info(
                "Starting Kafka producer service",
                bootstrap_servers=self.bootstrap_servers,
                output_topic=self.output_topic,
                client_id=self.client_id,
            )
            
            # Initialize producer
            self.producer = Producer(self.producer_config)
            
            # Initialize Schema Registry client
            self.schema_registry_client = SchemaRegistryClient({"url": self.schema_registry_url})
            
            # Create Avro serializer (will be set up when schema is available)
            self.avro_serializer = None
            
            self._is_started = True
            self.logger.info("Kafka producer service started successfully")
            
        except Exception as exc:  # pragma: no cover - defensive logging
            self.logger.error("Failed to start Kafka producer service", error=str(exc))
            raise ConnectorError(f"Failed to start Kafka producer: {exc}") from exc
    
    async def stop(self) -> None:
        """Stop the Kafka producer service."""
        if not self._is_started:
            return
        
        try:
            self.logger.info("Stopping Kafka producer service", client_id=self.client_id)
            
            if self.producer:
                # Flush any remaining messages
                self.producer.flush(timeout=10)
                self.producer = None
            
            self.schema_registry_client = None
            self.avro_serializer = None
            self._is_started = False
            
            self.logger.info("Kafka producer service stopped")
            
        except Exception as exc:  # pragma: no cover - defensive logging
            self.logger.error("Error stopping Kafka producer service", error=str(exc))
    
    def _setup_avro_serializer(self, schema_str: str) -> None:
        """
        Set up Avro serializer with the given schema.
        
        Args:
            schema_str: Avro schema as string
        """
        try:
            if not self.schema_registry_client:
                raise ConnectorError("Schema Registry client not initialized")
            
            subject = f"{self.output_topic}-value"
            
            # Register schema if not exists
            try:
                schema = self.schema_registry_client.get_latest_version(subject)
                self.logger.info("Using existing schema", schema_id=schema.schema_id, subject=subject)
            except Exception:
                schema = self.schema_registry_client.register_schema(subject, schema_str)
                self.logger.info("Registered new schema", schema_id=schema.schema_id, subject=subject)
            
            # Create Avro serializer
            self.avro_serializer = AvroSerializer(
                self.schema_registry_client,
                schema_str,
                conf={"auto.register.schemas": False},
            )
            
        except Exception as exc:
            self.logger.error("Failed to setup Avro serializer", error=str(exc))
            raise ConnectorError(f"Failed to setup Avro serializer: {exc}") from exc
    
    def _ensure_defaults(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Ensure required event fields are populated without mutating the input."""
        prepared = dict(record)
        
        if not prepared.get("event_id"):
            prepared["event_id"] = str(uuid4())
        
        if not prepared.get("trace_id"):
            prepared["trace_id"] = str(uuid4())
        
        occurred_at = prepared.get("occurred_at")
        if not isinstance(occurred_at, int):
            prepared["occurred_at"] = int(datetime.now(timezone.utc).timestamp() * 1_000_000)
        
        prepared.setdefault("tenant_id", "default")
        prepared.setdefault("schema_version", "1.0.0")
        prepared.setdefault("producer", self.default_producer)
        
        return prepared
    
    def _prepare_payload(self, record: Dict[str, Any]) -> Tuple[Dict[str, Any], str]:
        """
        Prepare payload for publishing, applying envelope if required.
        
        Returns:
            Tuple of (payload, event_id)
        """
        prepared = self._ensure_defaults(record)
        
        if self.use_event_envelope:
            envelope = {
                "event_id": prepared["event_id"],
                "trace_id": prepared["trace_id"],
                "occurred_at": prepared["occurred_at"],
                "tenant_id": prepared["tenant_id"],
                "schema_version": prepared["schema_version"],
                "producer": prepared["producer"],
                "payload": prepared,
            }
            return envelope, envelope["event_id"]
        
        return prepared, prepared["event_id"]
    
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
        
        payload, event_id = self._prepare_payload(record)
        
        try:
            if schema_str and self.avro_serializer is None:
                self._setup_avro_serializer(schema_str)
            
            if self.avro_serializer:
                message_value = self.avro_serializer(
                    payload,
                    SerializationContext(self.output_topic, MessageField.VALUE),
                )
            else:
                message_value = json.dumps(payload, default=str).encode("utf-8")
            
            self.producer.produce(
                topic=self.output_topic,
                value=message_value,
                key=event_id.encode("utf-8"),
                callback=self._delivery_callback,
            )
            
            # Poll to trigger delivery callbacks
            self.producer.poll(0)
            
            self.logger.debug(
                "Record published successfully",
                topic=self.output_topic,
                event_id=event_id,
                use_envelope=self.use_event_envelope,
            )
            return True
        
        except ConnectorError:
            raise
        except Exception as exc:
            self.logger.error("Failed to publish record", error=str(exc), topic=self.output_topic)
            raise ConnectorError(f"Failed to publish record: {exc}") from exc
    
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
            self.logger.debug(
                "Message delivered successfully",
                topic=msg.topic(),
                partition=msg.partition(),
                offset=msg.offset(),
            )
    
    async def publish_batch(
        self,
        records: List[Dict[str, Any]],
        schema_str: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Publish a batch of records to Kafka.
        
        Args:
            records: List of records to publish
            schema_str: Avro schema string (optional)
            
        Returns:
            Dict[str, Any]: Summary containing success/failure counts and errors.
        """
        if not self._is_started or not self.producer:
            raise ConnectorError("Producer not started")
        
        summary = {
            "total_count": len(records),
            "success_count": 0,
            "failure_count": 0,
            "errors": [],
        }
        
        if len(records) == 0:
            return summary
        
        try:
            if schema_str and self.avro_serializer is None:
                self._setup_avro_serializer(schema_str)
            
            self.logger.info(
                "Publishing batch of records",
                topic=self.output_topic,
                count=len(records),
                client_id=self.client_id,
            )
            
            for idx, record in enumerate(records):
                try:
                    await self.publish_record(record, schema_str=None)
                    summary["success_count"] += 1
                except ConnectorError as exc:
                    summary["failure_count"] += 1
                    summary["errors"].append(f"Record {idx}: {exc}")
                except Exception as exc:  # pragma: no cover - defensive
                    summary["failure_count"] += 1
                    summary["errors"].append(f"Record {idx}: {exc}")
            
            self.producer.flush(timeout=10)
            
            self.logger.info(
                "Batch publishing completed",
                topic=self.output_topic,
                total=summary["total_count"],
                success=summary["success_count"],
                failure=summary["failure_count"],
            )
            
        except ConnectorError:
            raise
        except Exception as exc:  # pragma: no cover - defensive
            self.logger.error("Failed to publish batch", error=str(exc))
            summary["errors"].append(str(exc))
            summary["failure_count"] = summary["total_count"]
        
        return summary
    
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
                "output_topic": self.output_topic,
                "client_id": self.client_id,
            }
        except Exception as exc:  # pragma: no cover - defensive
            self.logger.error("Failed to get producer metrics", error=str(exc))
            return {"error": str(exc)}
