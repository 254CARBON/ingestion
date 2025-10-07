"""
Kafka consumer service for aggregation service.

This module provides Kafka consumer functionality for consuming enriched
market data and processing it through the aggregation pipeline.
"""

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Callable
from uuid import uuid4

import structlog
from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError
from pydantic import BaseModel, Field

from ..core.aggregator import AggregationService


class ConsumerConfig(BaseModel):
    """Configuration for Kafka consumer."""
    
    bootstrap_servers: str = Field("localhost:9092", description="Kafka bootstrap servers")
    topic: str = Field("enriched.market.ticks.v1", description="Input topic")
    group_id: str = Field("aggregation-service", description="Consumer group ID")
    auto_offset_reset: str = Field("latest", description="Auto offset reset policy")
    enable_auto_commit: bool = Field(True, description="Enable auto commit")
    max_poll_records: int = Field(100, description="Max records per poll")
    session_timeout_ms: int = Field(30000, description="Session timeout")
    heartbeat_interval_ms: int = Field(10000, description="Heartbeat interval")
    consumer_timeout_ms: int = Field(1000, description="Consumer timeout")
    
    class Config:
        """Pydantic configuration."""
        extra = "forbid"


class KafkaConsumerService:
    """
    Kafka consumer service for aggregation.
    
    This service consumes enriched market data from Kafka topics and
    processes it through the aggregation pipeline.
    """
    
    def __init__(self, config: ConsumerConfig, aggregation_service: AggregationService):
        """
        Initialize the Kafka consumer service.
        
        Args:
            config: Consumer configuration
            aggregation_service: Aggregation service instance
        """
        self.config = config
        self.aggregation_service = aggregation_service
        self.logger = structlog.get_logger(__name__)
        
        # Consumer instance
        self.consumer: Optional[AIOKafkaConsumer] = None
        
        # Processing statistics
        self.stats = {
            "total_messages": 0,
            "successful_aggregations": 0,
            "failed_aggregations": 0,
            "consumer_errors": 0,
            "last_message_time": None,
            "processing_latency_ms": []
        }
        
        # Processing callback
        self.processing_callback: Optional[Callable] = None
    
    async def start(self) -> None:
        """Start the Kafka consumer."""
        try:
            self.logger.info("Starting Kafka consumer", 
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
            
            self.logger.info("Kafka consumer started successfully")
            
        except Exception as e:
            self.logger.error("Failed to start Kafka consumer", error=str(e))
            raise
    
    async def stop(self) -> None:
        """Stop the Kafka consumer."""
        try:
            if self.consumer:
                await self.consumer.stop()
                self.logger.info("Kafka consumer stopped")
        except Exception as e:
            self.logger.error("Failed to stop Kafka consumer", error=str(e))
    
    async def consume_messages(self) -> None:
        """Consume messages from Kafka."""
        if not self.consumer:
            raise RuntimeError("Consumer not started")
        
        try:
            self.logger.info("Starting message consumption")
            
            async for message in self.consumer:
                await self._process_message(message)
                
        except Exception as e:
            self.logger.error("Error consuming messages", error=str(e))
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
            
            # Extract enriched data from message payload
            enriched_data = message_data.get("payload", message_data)
            
            # Add message metadata
            enriched_data["_kafka_metadata"] = {
                "topic": message.topic,
                "partition": message.partition,
                "offset": message.offset,
                "timestamp": message.timestamp,
                "key": message.key.decode('utf-8') if message.key else None
            }
            
            self.logger.info("Processing message", 
                           topic=message.topic,
                           partition=message.partition,
                           offset=message.offset)
            
            # Aggregate the data
            aggregation_result = await self.aggregation_service.aggregate(enriched_data)
            
            # Call processing callback if set
            if self.processing_callback:
                await self.processing_callback(aggregation_result)
            
            # Update statistics
            self.stats["successful_aggregations"] += 1
            self.stats["last_message_time"] = datetime.now(timezone.utc).isoformat()
            
            # Calculate processing latency
            end_time = datetime.now(timezone.utc)
            latency_ms = (end_time - start_time).total_seconds() * 1000
            self.stats["processing_latency_ms"].append(latency_ms)
            
            # Keep only last 1000 latency measurements
            if len(self.stats["processing_latency_ms"]) > 1000:
                self.stats["processing_latency_ms"] = self.stats["processing_latency_ms"][-1000:]
            
            self.logger.info("Message processed successfully", 
                           latency_ms=latency_ms,
                           ohlc_bars=len(aggregation_result.ohlc_bars),
                           rolling_metrics=len(aggregation_result.rolling_metrics),
                           curve_prestage=len(aggregation_result.curve_prestage))
            
        except Exception as e:
            self.stats["failed_aggregations"] += 1
            self.logger.error("Failed to process message", 
                            error=str(e),
                            topic=message.topic,
                            partition=message.partition,
                            offset=message.offset)
    
    def set_processing_callback(self, callback: Callable) -> None:
        """
        Set processing callback for aggregated data.
        
        Args:
            callback: Callback function to call with aggregated data
        """
        self.processing_callback = callback
    
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
                self.stats["successful_aggregations"] / max(1, self.stats["total_messages"])
            ),
            "consumer_ready": self.consumer is not None and not self.consumer._closed if self.consumer else False
        }
    
    def reset_stats(self) -> None:
        """Reset consumer statistics."""
        self.stats = {
            "total_messages": 0,
            "successful_aggregations": 0,
            "failed_aggregations": 0,
            "consumer_errors": 0,
            "last_message_time": None,
            "processing_latency_ms": []
        }
