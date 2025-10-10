"""
Kafka producer service for aggregation service.

This module provides Kafka producer functionality for publishing aggregated
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

from ..core.aggregator import AggregationResult


class ProducerConfig(BaseModel):
    """Configuration for Kafka producer."""
    
    bootstrap_servers: str = Field("localhost:9092", description="Kafka bootstrap servers")
    output_topics: Dict[str, str] = Field(
        default_factory=lambda: {
            "ohlc": "aggregation.ohlc.bars.v1",
            "rolling": "aggregation.rolling.metrics.v1",
            "curve": "aggregation.curve.prestage.v1"
        },
        description="Output topics for different aggregation types"
    )
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
    Kafka producer service for aggregation.
    
    This service publishes aggregated market data to Kafka topics for
    downstream consumption by projection and other services.
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
            "publish_latency_ms": [],
            "ohlc_published": 0,
            "rolling_published": 0,
            "curve_published": 0
        }
    
    async def start(self) -> None:
        """Start the Kafka producer."""
        try:
            self.logger.info("Starting Kafka producer", 
                           bootstrap_servers=self.config.bootstrap_servers,
                           output_topics=self.config.output_topics)
            
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
    
    async def publish_aggregated_data(self, aggregation_result: AggregationResult) -> bool:
        """
        Publish aggregated data to Kafka.
        
        Args:
            aggregation_result: Aggregation result to publish
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.producer:
            raise RuntimeError("Producer not started")
        
        start_time = datetime.now(timezone.utc)
        success_count = 0
        total_count = 0
        
        try:
            # Publish OHLC bars
            if aggregation_result.ohlc_bars:
                success = await self._publish_ohlc_bars(aggregation_result.ohlc_bars)
                if success:
                    success_count += 1
                total_count += 1
            
            # Publish rolling metrics
            if aggregation_result.rolling_metrics:
                success = await self._publish_rolling_metrics(aggregation_result.rolling_metrics)
                if success:
                    success_count += 1
                total_count += 1
            
            # Publish curve pre-stage
            if aggregation_result.curve_prestage:
                success = await self._publish_curve_prestage(aggregation_result.curve_prestage)
                if success:
                    success_count += 1
                total_count += 1
            
            # Update statistics
            self.stats["total_messages"] += total_count
            self.stats["successful_publishes"] += success_count
            self.stats["last_publish_time"] = datetime.now(timezone.utc).isoformat()
            
            # Calculate publish latency
            end_time = datetime.now(timezone.utc)
            latency_ms = (end_time - start_time).total_seconds() * 1000
            self.stats["publish_latency_ms"].append(latency_ms)
            
            # Keep only last 1000 latency measurements
            if len(self.stats["publish_latency_ms"]) > 1000:
                self.stats["publish_latency_ms"] = self.stats["publish_latency_ms"][-1000:]
            
            success = success_count == total_count
            
            self.logger.info("Aggregated data published", 
                           ohlc_bars=len(aggregation_result.ohlc_bars),
                           rolling_metrics=len(aggregation_result.rolling_metrics),
                           curve_prestage=len(aggregation_result.curve_prestage),
                           latency_ms=latency_ms,
                           success=success)
            
            return success
            
        except Exception as e:
            self.stats["failed_publishes"] += total_count
            self.logger.error("Failed to publish aggregated data", 
                            error=str(e),
                            ohlc_bars=len(aggregation_result.ohlc_bars),
                            rolling_metrics=len(aggregation_result.rolling_metrics),
                            curve_prestage=len(aggregation_result.curve_prestage))
            return False
    
    async def _publish_ohlc_bars(self, ohlc_bars: List[Any]) -> bool:
        """Publish OHLC bars to Kafka."""
        try:
            topic = self.config.output_topics["ohlc"]
            
            for bar in ohlc_bars:
                payload = self._model_to_payload(bar)
                message_payload = {
                    "event_id": str(uuid4()),
                    "trace_id": getattr(bar, 'trace_id', None),
                    "occurred_at": bar.aggregation_timestamp.isoformat(),
                    "tenant_id": getattr(bar, 'tenant_id', "default"),
                    "schema_version": "1.0.0",
                    "producer": "aggregation-service",
                    "payload": payload,
                    "metadata": {
                        "aggregation_type": "ohlc",
                        "bar_type": bar.bar_type,
                        "bar_size": bar.bar_size,
                        "market": bar.market,
                        "delivery_location": bar.delivery_location
                    }
                }
                
                await self.producer.send_and_wait(
                    topic,
                    value=message_payload,
                    key=message_payload["event_id"].encode('utf-8')
                )
            
            self.stats["ohlc_published"] += len(ohlc_bars)
            return True
            
        except Exception as e:
            self.logger.error("Failed to publish OHLC bars", error=str(e))
            return False
    
    async def _publish_rolling_metrics(self, rolling_metrics: List[Any]) -> bool:
        """Publish rolling metrics to Kafka."""
        try:
            topic = self.config.output_topics["rolling"]
            
            for metric in rolling_metrics:
                payload = self._model_to_payload(metric)
                message_payload = {
                    "event_id": str(uuid4()),
                    "trace_id": getattr(metric, 'trace_id', None),
                    "occurred_at": metric.calculation_timestamp.isoformat(),
                    "tenant_id": getattr(metric, 'tenant_id', "default"),
                    "schema_version": "1.0.0",
                    "producer": "aggregation-service",
                    "payload": payload,
                    "metadata": {
                        "aggregation_type": "rolling_metrics",
                        "metric_type": metric.metric_type,
                        "window_size": metric.window_size,
                        "market": metric.market,
                        "delivery_location": metric.delivery_location
                    }
                }
                
                await self.producer.send_and_wait(
                    topic,
                    value=message_payload,
                    key=message_payload["event_id"].encode('utf-8')
                )
            
            self.stats["rolling_published"] += len(rolling_metrics)
            return True
            
        except Exception as e:
            self.logger.error("Failed to publish rolling metrics", error=str(e))
            return False
    
    async def _publish_curve_prestage(self, curve_prestage: List[Any]) -> bool:
        """Publish curve pre-stage to Kafka."""
        try:
            topic = self.config.output_topics["curve"]
            
            for curve in curve_prestage:
                payload = self._model_to_payload(curve)
                message_payload = {
                    "event_id": str(uuid4()),
                    "trace_id": getattr(curve, 'trace_id', None),
                    "occurred_at": curve.aggregation_timestamp.isoformat(),
                    "tenant_id": getattr(curve, 'tenant_id', "default"),
                    "schema_version": "1.0.0",
                    "producer": "aggregation-service",
                    "payload": payload,
                    "metadata": {
                        "aggregation_type": "curve_prestage",
                        "curve_type": curve.curve_type,
                        "market": curve.market,
                        "delivery_location": curve.delivery_location
                    }
                }
                
                await self.producer.send_and_wait(
                    topic,
                    value=message_payload,
                    key=message_payload["event_id"].encode('utf-8')
                )
            
            self.stats["curve_published"] += len(curve_prestage)
            return True
            
        except Exception as e:
            self.logger.error("Failed to publish curve pre-stage", error=str(e))
            return False
    
    async def publish_batch(self, aggregation_results: List[AggregationResult]) -> Dict[str, int]:
        """
        Publish a batch of aggregated data.
        
        Args:
            aggregation_results: List of aggregation results to publish
            
        Returns:
            Dict[str, int]: Publishing statistics
        """
        if not self.producer:
            raise RuntimeError("Producer not started")
        
        batch_stats = {
            "total": len(aggregation_results),
            "successful": 0,
            "failed": 0
        }
        
        try:
            self.logger.info("Publishing batch of aggregated data", batch_size=len(aggregation_results))
            
            # Publish each result
            for aggregation_result in aggregation_results:
                success = await self.publish_aggregated_data(aggregation_result)
                if success:
                    batch_stats["successful"] += 1
                else:
                    batch_stats["failed"] += 1
            
            self.logger.info("Batch publishing completed", **batch_stats)
            return batch_stats
            
        except Exception as e:
            self.logger.error("Failed to publish batch", error=str(e))
            batch_stats["failed"] = len(aggregation_results)
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
            "publish_latency_ms": [],
            "ohlc_published": 0,
            "rolling_published": 0,
            "curve_published": 0
        }

    @staticmethod
    def _model_to_payload(model: Any) -> Any:
        """
        Convert a Pydantic model to a JSON-serializable payload.
        
        Args:
            model: Model instance to convert
        
        Returns:
            Any: JSON-serializable representation
        """
        if isinstance(model, BaseModel):
            # Prefer model_dump for Pydantic v2 support
            if hasattr(model, "model_dump"):
                return model.model_dump(mode="json")
            return json.loads(model.json())
        return model
