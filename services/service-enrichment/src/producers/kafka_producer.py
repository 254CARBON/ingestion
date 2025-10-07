"""
Kafka producer service for enrichment service.

This module provides Kafka producer functionality for publishing enriched
market data to downstream topics.
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

from ..core.enricher import EnrichmentResult


class ProducerConfig(BaseModel):
    """Configuration for Kafka producer."""
    
    bootstrap_servers: str = Field("localhost:9092", description="Kafka bootstrap servers")
    output_topic: str = Field("enriched.market.ticks.v1", description="Output topic")
    compression_type: str = Field("gzip", description="Compression type")
    batch_size: int = Field(16384, description="Batch size")
    linger_ms: int = Field(10, description="Linger time in ms")
    retries: int = Field(3, description="Number of retries")
    acks: str = Field("all", description="Acknowledgment level")
    request_timeout_ms: int = Field(30000, description="Request timeout")
    
    class Config:
        """Pydantic configuration."""
        extra = "forbid"


class KafkaProducerService:
    """
    Kafka producer service for enrichment.
    
    This service publishes enriched market data to Kafka topics for
    downstream consumption by aggregation and other services.
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
            self.logger.info("Starting Kafka producer", 
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
            
            self.logger.info("Kafka producer started successfully")
            
        except Exception as e:
            self.logger.error("Failed to start Kafka producer", error=str(e))
            raise
    
    async def stop(self) -> None:
        """Stop the Kafka producer."""
        try:
            if self.producer:
                await self.producer.stop()
                self.logger.info("Kafka producer stopped")
        except Exception as e:
            self.logger.error("Failed to stop Kafka producer", error=str(e))
    
    async def publish_enriched_data(self, enrichment_result: EnrichmentResult) -> bool:
        """
        Publish enriched data to Kafka.
        
        Args:
            enrichment_result: Enrichment result to publish
            
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
                "trace_id": enrichment_result.enriched_data.get("trace_id"),
                "occurred_at": enrichment_result.timestamp.isoformat(),
                "tenant_id": enrichment_result.enriched_data.get("tenant_id", "default"),
                "schema_version": "1.0.0",
                "producer": "enrichment-service",
                "payload": enrichment_result.enriched_data,
                "metadata": enrichment_result.metadata,
                "taxonomy_tags": enrichment_result.taxonomy_tags,
                "semantic_tags": enrichment_result.semantic_tags,
                "geospatial_data": enrichment_result.geospatial_data,
                "enrichment_score": enrichment_result.enrichment_score
            }
            
            # Add Kafka-specific metadata
            message_payload["_kafka_metadata"] = {
                "topic": self.config.output_topic,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "producer_service": "enrichment-service"
            }
            
            # Publish message
            await self.producer.send_and_wait(
                self.config.output_topic,
                value=message_payload,
                key=message_payload["event_id"].encode('utf-8')
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
            
            self.logger.info("Enriched data published successfully", 
                           event_id=message_payload["event_id"],
                           topic=self.config.output_topic,
                           latency_ms=latency_ms,
                           enrichment_score=enrichment_result.enrichment_score)
            
            return True
            
        except Exception as e:
            self.stats["failed_publishes"] += 1
            self.logger.error("Failed to publish enriched data", 
                            error=str(e),
                            enrichment_score=enrichment_result.enrichment_score)
            return False
    
    async def publish_batch(self, enrichment_results: List[EnrichmentResult]) -> Dict[str, int]:
        """
        Publish a batch of enriched data.
        
        Args:
            enrichment_results: List of enrichment results to publish
            
        Returns:
            Dict[str, int]: Publishing statistics
        """
        if not self.producer:
            raise RuntimeError("Producer not started")
        
        batch_stats = {
            "total": len(enrichment_results),
            "successful": 0,
            "failed": 0
        }
        
        try:
            self.logger.info("Publishing batch of enriched data", batch_size=len(enrichment_results))
            
            # Publish each result
            for enrichment_result in enrichment_results:
                success = await self.publish_enriched_data(enrichment_result)
                if success:
                    batch_stats["successful"] += 1
                else:
                    batch_stats["failed"] += 1
            
            self.logger.info("Batch publishing completed", **batch_stats)
            return batch_stats
            
        except Exception as e:
            self.logger.error("Failed to publish batch", error=str(e))
            batch_stats["failed"] = len(enrichment_results)
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
