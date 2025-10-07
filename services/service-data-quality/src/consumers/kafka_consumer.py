"""
Kafka consumer for data quality service.

This module provides Kafka consumer functionality for consuming market data
and analyzing it for quality issues and anomalies.
"""

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Callable

import structlog
from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError
from pydantic import BaseModel, Field

from ..core.anomaly_detector import AnomalyDetector


class ConsumerConfig(BaseModel):
    """Configuration for Kafka consumer."""

    bootstrap_servers: str = Field("localhost:9092", description="Kafka bootstrap servers")
    topic: str = Field("normalized.market.ticks.v1", description="Input topic")
    group_id: str = Field("data-quality-service", description="Consumer group ID")
    auto_offset_reset: str = Field("latest", description="Auto offset reset policy")
    enable_auto_commit: bool = Field(True, description="Enable auto commit")
    max_poll_records: int = Field(100, description="Max records per poll")
    session_timeout_ms: int = Field(30000, description="Session timeout")
    heartbeat_interval_ms: int = Field(10000, description="Heartbeat interval")
    consumer_timeout_ms: int = Field(1000, description="Consumer timeout")


class KafkaConsumerService:
    """
    Kafka consumer service for data quality monitoring.

    This service consumes market data from Kafka topics and analyzes it
    for quality issues and anomalies.
    """

    def __init__(self, config: ConsumerConfig, anomaly_detector: AnomalyDetector, kafka_producer):
        """
        Initialize the Kafka consumer service.

        Args:
            config: Consumer configuration
            anomaly_detector: Anomaly detector instance
            kafka_producer: Kafka producer for publishing anomalies
        """
        self.config = config
        self.anomaly_detector = anomaly_detector
        self.kafka_producer = kafka_producer
        self.logger = structlog.get_logger(__name__)

        # Consumer instance
        self.consumer: Optional[AIOKafkaConsumer] = None

        # Processing statistics
        self.stats = {
            "total_messages": 0,
            "successful_analyses": 0,
            "failed_analyses": 0,
            "anomalies_detected": 0,
            "consumer_errors": 0,
            "last_message_time": None,
            "processing_latency_ms": []
        }

    async def start(self) -> None:
        """Start the Kafka consumer."""
        try:
            self.logger.info("Starting data quality Kafka consumer",
                           bootstrap_servers=self.config.bootstrap_servers,
                           topic=self.config.topic,
                           group_id=self.config.group_id)

            # Create consumer
            self.consumer = AIOKafkaConsumer(
                self.config.topic,
                bootstrap_servers=self.config.bootstrap_servers,
                group_id=self.config.group_id,
                auto_offset_reset=self.config.auto_offset_reset,
                enable_auto_commit=self.config.enable_auto_commit,
                max_poll_records=self.config.max_poll_records,
                session_timeout_ms=self.config.session_timeout_ms,
                heartbeat_interval_ms=self.config.heartbeat_interval_ms,
                consumer_timeout_ms=self.config.consumer_timeout_ms,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None
            )

            # Start consumer
            await self.consumer.start()

            self.logger.info("Data quality Kafka consumer started successfully")

        except Exception as e:
            self.logger.error("Failed to start data quality Kafka consumer", error=str(e))
            raise

    async def stop(self) -> None:
        """Stop the Kafka consumer."""
        try:
            if self.consumer:
                await self.consumer.stop()
                self.logger.info("Data quality Kafka consumer stopped")
        except Exception as e:
            self.logger.error("Failed to stop data quality Kafka consumer", error=str(e))

    async def consume_messages(self) -> None:
        """Consume messages from Kafka."""
        if not self.consumer:
            raise RuntimeError("Consumer not started")

        try:
            self.logger.info("Starting data quality message consumption")

            async for message in self.consumer:
                await self._process_message(message)

        except Exception as e:
            self.logger.error("Error consuming data quality messages", error=str(e))
            self.stats["consumer_errors"] += 1
            raise

    async def _process_message(self, message) -> None:
        """
        Process a single Kafka message.

        Args:
            message: Kafka message
        """
        start_time = datetime.now(timezone.utc)

        try:
            self.stats["total_messages"] += 1

            # Extract message data
            message_data = message.value
            if not message_data:
                self.logger.warning("Received empty message")
                return

            # Extract payload if present
            payload = message_data.get("payload", message_data)

            self.logger.debug("Analyzing data quality",
                            topic=message.topic,
                            partition=message.partition,
                            offset=message.offset)

            # Detect anomalies
            anomalies = await self.anomaly_detector.detect_anomalies(payload)

            # Publish anomalies if detected
            if anomalies:
                for anomaly in anomalies:
                    await self.kafka_producer.publish_anomaly(anomaly)

                self.stats["anomalies_detected"] += len(anomalies)

                self.logger.info("Anomalies detected and published",
                               count=len(anomalies),
                               topic=message.topic)

            # Update statistics
            self.stats["successful_analyses"] += 1
            self.stats["last_message_time"] = datetime.now(timezone.utc).isoformat()

            # Calculate processing latency
            end_time = datetime.now(timezone.utc)
            latency_ms = (end_time - start_time).total_seconds() * 1000
            self.stats["processing_latency_ms"].append(latency_ms)

            # Keep only last 1000 latency measurements
            if len(self.stats["processing_latency_ms"]) > 1000:
                self.stats["processing_latency_ms"] = self.stats["processing_latency_ms"][-1000:]

        except Exception as e:
            self.stats["failed_analyses"] += 1
            self.logger.error("Failed to process message",
                            error=str(e),
                            topic=message.topic,
                            partition=message.partition,
                            offset=message.offset)

    async def is_ready(self) -> bool:
        """
        Check if consumer is ready.

        Returns:
            bool: True if ready, False otherwise
        """
        try:
            return self.consumer is not None and not self.consumer._closed
        except Exception:
            return False

    def get_stats(self) -> Dict[str, Any]:
        """
        Get consumer statistics.

        Returns:
            Dict[str, Any]: Consumer statistics
        """
        # Calculate average latency
        if self.stats["processing_latency_ms"]:
            avg_latency = sum(self.stats["processing_latency_ms"]) / len(self.stats["processing_latency_ms"])
        else:
            avg_latency = 0.0

        return {
            **self.stats,
            "average_latency_ms": avg_latency,
            "success_rate": (
                self.stats["successful_analyses"] / max(1, self.stats["total_messages"])
            ),
            "consumer_ready": self.consumer is not None and not self.consumer._closed if self.consumer else False
        }

    def reset_stats(self) -> None:
        """Reset consumer statistics."""
        self.stats = {
            "total_messages": 0,
            "successful_analyses": 0,
            "failed_analyses": 0,
            "anomalies_detected": 0,
            "consumer_errors": 0,
            "last_message_time": None,
            "processing_latency_ms": []
        }
