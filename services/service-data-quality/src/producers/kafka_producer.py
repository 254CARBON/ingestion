"""
Kafka producer for data quality service.

This module provides Kafka producer functionality for publishing detected
anomalies to downstream topics.
"""

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import uuid4

import structlog
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError
from pydantic import BaseModel, Field

from ..core.anomaly_detector import Anomaly


class ProducerConfig(BaseModel):
    """Configuration for Kafka producer."""

    bootstrap_servers: str = Field("localhost:9092", description="Kafka bootstrap servers")
    output_topic: str = Field("data.quality.anomalies.v1", description="Output topic")
    compression_type: str = Field("gzip", description="Compression type")
    batch_size: int = Field(16384, description="Batch size")
    linger_ms: int = Field(10, description="Linger time in ms")
    retries: int = Field(3, description="Number of retries")
    acks: str = Field("all", description="Acknowledgment level")
    request_timeout_ms: int = Field(30000, description="Request timeout")


class KafkaProducerService:
    """
    Kafka producer service for data quality anomalies.

    This service publishes detected anomalies to Kafka topics for
    downstream consumption by monitoring and alerting systems.
    """

    def __init__(self, config: ProducerConfig):
        """
        Initialize the Kafka producer service.

        Args:
            config: Producer configuration
        """
        self.config = config
        self.logger = structlog.get_logger(__name__)

        # Producer instance
        self.producer: Optional[AIOKafkaProducer] = None

        # Publishing statistics
        self.stats = {
            "total_messages": 0,
            "successful_publishes": 0,
            "failed_publishes": 0,
            "producer_errors": 0,
            "last_publish_time": None,
            "publish_latency_ms": []
        }

    async def start(self) -> None:
        """Start the Kafka producer."""
        try:
            self.logger.info("Starting data quality Kafka producer",
                           bootstrap_servers=self.config.bootstrap_servers,
                           output_topic=self.config.output_topic)

            # Create producer
            self.producer = AIOKafkaProducer(
                bootstrap_servers=self.config.bootstrap_servers,
                compression_type=self.config.compression_type,
                batch_size=self.config.batch_size,
                linger_ms=self.config.linger_ms,
                retries=self.config.retries,
                acks=self.config.acks,
                request_timeout_ms=self.config.request_timeout_ms,
                value_serializer=lambda x: json.dumps(x).encode('utf-8') if x else None
            )

            # Start producer
            await self.producer.start()

            self.logger.info("Data quality Kafka producer started successfully")

        except Exception as e:
            self.logger.error("Failed to start data quality Kafka producer", error=str(e))
            raise

    async def stop(self) -> None:
        """Stop the Kafka producer."""
        try:
            if self.producer:
                await self.producer.stop()
                self.logger.info("Data quality Kafka producer stopped")
        except Exception as e:
            self.logger.error("Failed to stop data quality Kafka producer", error=str(e))

    async def publish_anomaly(self, anomaly: Anomaly) -> bool:
        """
        Publish an anomaly to Kafka.

        Args:
            anomaly: Anomaly to publish

        Returns:
            bool: True if successful, False otherwise
        """
        if not self.producer:
            raise RuntimeError("Producer not started")

        start_time = datetime.now(timezone.utc)

        try:
            self.stats["total_messages"] += 1

            # Prepare message payload
            message_payload = {
                "event_id": str(uuid4()),
                "trace_id": None,
                "occurred_at": int(datetime.now(timezone.utc).timestamp() * 1_000_000),
                "tenant_id": "default",
                "schema_version": "1.0.0",
                "producer": "data-quality-service",
                "payload": anomaly.dict()
            }

            # Publish message
            await self.producer.send_and_wait(
                self.config.output_topic,
                value=message_payload,
                key=anomaly.anomaly_id.encode('utf-8')
            )

            # Update statistics
            self.stats["successful_publishes"] += 1
            self.stats["last_publish_time"] = datetime.now(timezone.utc).isoformat()

            # Calculate publish latency
            end_time = datetime.now(timezone.utc)
            latency_ms = (end_time - start_time).total_seconds() * 1000
            self.stats["publish_latency_ms"].append(latency_ms)

            # Keep only last 1000 latency measurements
            if len(self.stats["publish_latency_ms"]) > 1000:
                self.stats["publish_latency_ms"] = self.stats["publish_latency_ms"][-1000:]

            self.logger.debug("Anomaly published successfully",
                            anomaly_id=anomaly.anomaly_id,
                            anomaly_type=anomaly.anomaly_type,
                            severity=anomaly.severity)

            return True

        except Exception as e:
            self.stats["failed_publishes"] += 1
            self.logger.error("Failed to publish anomaly",
                            error=str(e),
                            anomaly_id=anomaly.anomaly_id)
            return False

    async def publish_batch(self, anomalies: List[Anomaly]) -> Dict[str, int]:
        """
        Publish a batch of anomalies.

        Args:
            anomalies: List of anomalies to publish

        Returns:
            Dict[str, int]: Publishing statistics
        """
        if not self.producer:
            raise RuntimeError("Producer not started")

        batch_stats = {
            "total": len(anomalies),
            "successful": 0,
            "failed": 0
        }

        try:
            self.logger.info("Publishing batch of anomalies", batch_size=len(anomalies))

            # Publish each anomaly
            for anomaly in anomalies:
                success = await self.publish_anomaly(anomaly)
                if success:
                    batch_stats["successful"] += 1
                else:
                    batch_stats["failed"] += 1

            self.logger.info("Batch publishing completed", **batch_stats)
            return batch_stats

        except Exception as e:
            self.logger.error("Failed to publish batch", error=str(e))
            batch_stats["failed"] = len(anomalies)
            return batch_stats

    async def is_ready(self) -> bool:
        """
        Check if producer is ready.

        Returns:
            bool: True if ready, False otherwise
        """
        try:
            return self.producer is not None and not self.producer._closed
        except Exception:
            return False

    def get_stats(self) -> Dict[str, Any]:
        """
        Get producer statistics.

        Returns:
            Dict[str, Any]: Producer statistics
        """
        # Calculate average latency
        if self.stats["publish_latency_ms"]:
            avg_latency = sum(self.stats["publish_latency_ms"]) / len(self.stats["publish_latency_ms"])
        else:
            avg_latency = 0.0

        return {
            **self.stats,
            "average_latency_ms": avg_latency,
            "success_rate": (
                self.stats["successful_publishes"] / max(1, self.stats["total_messages"])
            ),
            "producer_ready": self.producer is not None and not self.producer._closed if self.producer else False
        }

    def reset_stats(self) -> None:
        """Reset producer statistics."""
        self.stats = {
            "total_messages": 0,
            "successful_publishes": 0,
            "failed_publishes": 0,
            "producer_errors": 0,
            "last_publish_time": None,
            "publish_latency_ms": []
        }
