"""
Base connector class providing the foundation for all data connectors.

This abstract base class defines the interface that all connectors must implement,
including extraction, transformation, and loading operations.
"""

import asyncio
import json
import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Union
from uuid import uuid4

import structlog
from pydantic import BaseModel, Field, ValidationError

from .exceptions import (
    ConnectorError,
    ExtractionError,
    TransformationError,
    ValidationError as ConnectorValidationError,
)
from .utils import retry_with_backoff, setup_logging


class ConnectorConfig(BaseModel):
    """Configuration model for connectors."""
    
    name: str = Field(..., description="Connector name")
    version: str = Field(..., description="Connector version")
    market: str = Field(..., description="Market identifier")
    mode: str = Field(..., description="Execution mode: batch|streaming|hybrid")
    schedule: Optional[str] = Field(None, description="Cron schedule for batch mode")
    enabled: bool = Field(True, description="Whether connector is enabled")
    output_topic: str = Field(..., description="Kafka topic for output")
    schema: str = Field(..., description="Path to Avro schema file")
    retries: int = Field(3, description="Number of retry attempts")
    backoff_seconds: int = Field(30, description="Backoff delay in seconds")
    tenant_strategy: str = Field("single", description="Tenant handling strategy")
    transforms: List[str] = Field(default_factory=list, description="Transform names")
    owner: str = Field("platform", description="Connector owner")
    kafka_bootstrap_servers: str = Field("localhost:9092", description="Kafka bootstrap servers")
    schema_registry_url: str = Field("http://localhost:8081", description="Schema Registry URL")
    
    class Config:
        """Pydantic configuration."""
        extra = "forbid"
        validate_assignment = True


class ExtractionResult(BaseModel):
    """Result of data extraction operation."""
    
    data: List[Dict[str, Any]] = Field(..., description="Extracted data records")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Extraction metadata")
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    record_count: int = Field(..., description="Number of records extracted")
    
    class Config:
        """Pydantic configuration."""
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class TransformationResult(BaseModel):
    """Result of data transformation operation."""
    
    data: List[Dict[str, Any]] = Field(..., description="Transformed data records")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Transformation metadata")
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    record_count: int = Field(..., description="Number of records transformed")
    validation_errors: List[str] = Field(default_factory=list, description="Validation errors")
    
    class Config:
        """Pydantic configuration."""
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class BaseConnector(ABC):
    """
    Abstract base class for all data connectors.
    
    This class provides the foundation for implementing connectors that extract,
    transform, and load data from various sources into the 254Carbon platform.
    """
    
    def __init__(self, config: ConnectorConfig):
        """
        Initialize the connector with configuration.
        
        Args:
            config: Connector configuration
        """
        self.config = config
        self.logger = setup_logging(self.__class__.__name__)
        self._validate_config()
    
    def _validate_config(self) -> None:
        """Validate connector configuration."""
        try:
            # Additional validation logic can be added here
            if not self.config.enabled:
                self.logger.warning(f"Connector {self.config.name} is disabled")
        except Exception as e:
            raise ConnectorValidationError(f"Invalid configuration: {e}") from e
    
    @abstractmethod
    async def extract(self, **kwargs) -> ExtractionResult:
        """
        Extract data from the source system.
        
        This method must be implemented by subclasses to extract data from
        the specific source system (API, database, file, etc.).
        
        Args:
            **kwargs: Additional extraction parameters
            
        Returns:
            ExtractionResult: Extracted data and metadata
            
        Raises:
            ExtractionError: If extraction fails
        """
        pass
    
    @abstractmethod
    async def transform(self, extraction_result: ExtractionResult) -> TransformationResult:
        """
        Transform extracted data into the target schema.
        
        This method must be implemented by subclasses to transform the
        extracted data into the format expected by the target system.
        
        Args:
            extraction_result: Result from the extract operation
            
        Returns:
            TransformationResult: Transformed data and metadata
            
        Raises:
            TransformationError: If transformation fails
        """
        pass
    
    async def load(self, transformation_result: TransformationResult) -> bool:
        """
        Load transformed data into the target system.
        
        This method publishes the transformed data to Kafka topics.
        
        Args:
            transformation_result: Result from the transform operation
            
        Returns:
            bool: True if loading succeeded, False otherwise
        """
        try:
            from .kafka_producer import KafkaProducerService
            
            self.logger.info(
                f"Loading {transformation_result.record_count} records to topic {self.config.output_topic}"
            )
            
            # Initialize Kafka producer
            producer = KafkaProducerService(
                bootstrap_servers=self.config.kafka_bootstrap_servers,
                schema_registry_url=self.config.schema_registry_url,
                output_topic=self.config.output_topic
            )
            
            await producer.start()
            
            # Publish each record
            success_count = 0
            for record in transformation_result.data:
                try:
                    await producer.publish_record(record)
                    success_count += 1
                except Exception as e:
                    self.logger.error(f"Failed to publish record: {e}")
                    continue
            
            await producer.stop()
            
            self.logger.info(f"Successfully loaded {success_count}/{transformation_result.record_count} records")
            return success_count == transformation_result.record_count
            
        except Exception as e:
            self.logger.error(f"Failed to load data: {e}")
            return False
    
    async def run(self, **kwargs) -> bool:
        """
        Execute the complete ETL pipeline.
        
        This method orchestrates the extract, transform, and load operations
        with proper error handling and retry logic.
        
        Args:
            **kwargs: Additional parameters for the pipeline
            
        Returns:
            bool: True if the pipeline succeeded, False otherwise
        """
        try:
            self.logger.info(f"Starting connector {self.config.name}")
            
            # Extract data
            extraction_result = await retry_with_backoff(
                self.extract,
                max_retries=self.config.retries,
                backoff_seconds=self.config.backoff_seconds,
                **kwargs
            )
            
            self.logger.info(f"Extracted {extraction_result.record_count} records")
            
            # Transform data
            transformation_result = await retry_with_backoff(
                self.transform,
                extraction_result,
                max_retries=self.config.retries,
                backoff_seconds=self.config.backoff_seconds,
            )
            
            self.logger.info(f"Transformed {transformation_result.record_count} records")
            
            # Load data
            success = await self.load(transformation_result)
            
            if success:
                self.logger.info(f"Connector {self.config.name} completed successfully")
            else:
                self.logger.error(f"Connector {self.config.name} failed during loading")
            
            return success
            
        except Exception as e:
            self.logger.error(f"Connector {self.config.name} failed: {e}")
            return False
    
    def get_health_status(self) -> Dict[str, Any]:
        """
        Get the health status of the connector.
        
        Returns:
            Dict[str, Any]: Health status information
        """
        return {
            "name": self.config.name,
            "version": self.config.version,
            "market": self.config.market,
            "mode": self.config.mode,
            "enabled": self.config.enabled,
            "status": "healthy" if self.config.enabled else "disabled",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
    
    def get_metrics(self) -> Dict[str, Any]:
        """
        Get connector metrics.
        
        Returns:
            Dict[str, Any]: Connector metrics
        """
        return {
            "name": self.config.name,
            "version": self.config.version,
            "market": self.config.market,
            "mode": self.config.mode,
            "enabled": self.config.enabled,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
