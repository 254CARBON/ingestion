"""
Base connector class providing the foundation for all data connectors.

This abstract base class defines the interface that all connectors must implement,
including extraction, transformation, and loading operations.
"""

import asyncio
import inspect
import json
import logging
import time
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
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


class LoadResult(BaseModel):
    """Result of data load / publish operation."""
    
    records_attempted: int = Field(..., description="Total records attempted to publish")
    records_published: int = Field(..., description="Number of records successfully published")
    records_failed: int = Field(..., description="Number of records that failed to publish")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Load metadata and timings")
    errors: List[str] = Field(default_factory=list, description="List of publish errors")
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    
    @property
    def success(self) -> bool:
        """Return True when all attempted records were published."""
        return self.records_failed == 0


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
        self._last_run_details: Dict[str, Any] = {}
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
    
    def _resolve_schema_path(self) -> Optional[Path]:
        """
        Resolve the schema path for the connector.
        
        Returns:
            Optional[Path]: Path to schema file if found
        """
        schema_path = Path(self.config.schema)
        if schema_path.is_file():
            return schema_path
        
        connector_dir = Path(inspect.getfile(self.__class__)).resolve().parent
        candidate = connector_dir / schema_path
        if candidate.is_file():
            return candidate
        
        return None
    
    async def load(self, transformation_result: TransformationResult) -> LoadResult:
        """
        Load transformed data into the target system.
        
        This method publishes the transformed data to Kafka topics.
        
        Args:
            transformation_result: Result from the transform operation
            
        Returns:
            LoadResult: Structured load result with metrics and errors
        """
        from .kafka_producer import KafkaProducerService
        
        start_time = time.perf_counter()
        records = transformation_result.data or []
        total_records = len(records)
        errors: List[str] = []
        schema_str: Optional[str] = None
        
        if total_records == 0:
            metadata = {
                "output_topic": self.config.output_topic,
                "duration_seconds": 0.0,
                "load_method": "kafka",
                "schema_applied": False,
            }
            return LoadResult(
                records_attempted=0,
                records_published=0,
                records_failed=0,
                metadata=metadata,
            )
        
        schema_path = self._resolve_schema_path()
        if schema_path:
            try:
                schema_str = schema_path.read_text(encoding="utf-8")
            except OSError as exc:
                error_msg = f"Failed to read schema at {schema_path}: {exc}"
                self.logger.error(error_msg)
                errors.append(error_msg)
        else:
            self.logger.warning(
                "Schema path could not be resolved; falling back to JSON publishing",
                connector=self.config.name,
                schema=self.config.schema,
            )
        
        producer = KafkaProducerService(
            bootstrap_servers=self.config.kafka_bootstrap_servers,
            schema_registry_url=self.config.schema_registry_url,
            output_topic=self.config.output_topic,
            use_event_envelope=False,
        )
        
        await producer.start()
        
        publish_summary = await producer.publish_batch(
            records,
            schema_str=schema_str,
        )
        
        await producer.stop()
        
        duration = time.perf_counter() - start_time
        
        errors.extend(publish_summary.get("errors", []))
        
        load_metadata = {
            "output_topic": self.config.output_topic,
            "duration_seconds": duration,
            "load_method": "kafka",
            "schema_applied": bool(schema_str),
            "producer_client_id": producer.client_id,
        }
        
        load_result = LoadResult(
            records_attempted=total_records,
            records_published=publish_summary.get("success_count", 0),
            records_failed=publish_summary.get("failure_count", total_records - publish_summary.get("success_count", 0)),
            metadata=load_metadata,
            errors=errors,
        )
        
        if load_result.records_failed:
            self.logger.error(
                "Failed to publish some records",
                connector=self.config.name,
                topic=self.config.output_topic,
                failed=load_result.records_failed,
                errors=errors[:5],
            )
        else:
            self.logger.info(
                "Successfully loaded all records",
                connector=self.config.name,
                topic=self.config.output_topic,
                count=load_result.records_published,
            )
        
        return load_result
    
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
            load_result = await self.load(transformation_result)
            
            self._last_run_details = {
                "extraction_result": extraction_result,
                "transformation_result": transformation_result,
                "load_result": load_result,
            }
            
            if load_result.success:
                self.logger.info(
                    "Connector completed successfully",
                    connector=self.config.name,
                    published=load_result.records_published,
                )
                return True
            
            self.logger.error(
                "Connector completed with load failures",
                connector=self.config.name,
                published=load_result.records_published,
                failed=load_result.records_failed,
            )
            return False
            
        except Exception as e:
            self.logger.error(f"Connector {self.config.name} failed: {e}")
            self._last_run_details = {}
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
    
    async def cleanup(self) -> None:
        """
        Cleanup connector resources.
        
        Subclasses can override to release open connections or sessions.
        """
        return None
