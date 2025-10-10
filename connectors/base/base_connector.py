"""
Base connector class providing the foundation for all data connectors.

This abstract base class defines the interface that all connectors must implement,
including extraction, transformation, and loading operations.
"""

import asyncio
import hashlib
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

    def _producer_base_name(self) -> str:
        """Return canonical producer name without version."""
        return f"{self.config.name}-connector"

    def _normalize_producer(self, producer: Optional[str]) -> str:
        """
        Ensure producer identifier follows `<name>@semver` convention.
        """
        base = (producer or self._producer_base_name()).strip()
        version = getattr(self.config, "version", "1.0.0")
        if "@" in base:
            return base
        return f"{base}@{version}"

    @staticmethod
    def _coerce_microseconds(value: Any, default: Optional[int] = None) -> int:
        """
        Convert various timestamp representations into microseconds.
        """
        if value is None:
            if default is not None:
                return default
            raise ValueError("Timestamp value is required")

        if isinstance(value, (int, float)):
            return int(value)

        if isinstance(value, str):
            cleaned = value.strip()
            if not cleaned:
                if default is not None:
                    return default
                raise ValueError("Blank timestamp string")

            # Normalise common timezone suffixes
            if cleaned.endswith("Z"):
                cleaned = cleaned[:-1] + "+00:00"
            if cleaned.endswith("-00:00"):
                cleaned = cleaned[:-6] + "+00:00"

            dt = datetime.fromisoformat(cleaned)
            return int(dt.timestamp() * 1_000_000)

        raise ValueError(f"Unsupported timestamp type: {type(value)}")

    @staticmethod
    def _coerce_optional_int(value: Any) -> Optional[int]:
        """Best-effort conversion of an optional value to int."""
        if value in (None, "", "null"):
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _is_ingestion_event(record: Dict[str, Any]) -> bool:
        """
        Check whether the record already matches the ingestion event envelope.
        """
        required = {
            "event_id",
            "trace_id",
            "schema_version",
            "tenant_id",
            "producer",
            "occurred_at",
            "ingested_at",
            "payload",
        }
        return required.issubset(record.keys())

    def _build_metadata_map(self, source: Dict[str, Any]) -> Dict[str, str]:
        """
        Convert remaining record fields into the metadata map expected by the schema.
        """
        metadata: Dict[str, str] = {}
        for key, value in source.items():
            if value is None:
                continue
            try:
                if isinstance(value, (dict, list)):
                    metadata[str(key)] = json.dumps(value, default=str)
                else:
                    metadata[str(key)] = str(value)
            except Exception:
                metadata[str(key)] = str(value)
        return metadata

    def _ensure_ingestion_event(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform arbitrary connector output into the standard ingestion.*.raw.v1 envelope.
        """
        if self._is_ingestion_event(record):
            return record

        working = dict(record)
        now_us = int(datetime.now(timezone.utc).timestamp() * 1_000_000)

        event_id = str(working.pop("event_id", uuid4()))
        trace_id = str(working.pop("trace_id", uuid4()))
        schema_version = str(working.pop("schema_version", "1.0.0"))
        tenant_id = str(working.pop("tenant_id", "default"))

        occurred_raw = working.pop("occurred_at", None)
        ingested_raw = working.pop("ingested_at", None)
        received_raw = working.pop("received_at", None)

        try:
            occurred_at = self._coerce_microseconds(occurred_raw, default=now_us)
        except ValueError:
            self.logger.debug("Falling back to current timestamp for occurred_at", record=record)
            occurred_at = now_us

        try:
            ingested_at = self._coerce_microseconds(ingested_raw, default=now_us)
        except ValueError:
            self.logger.debug("Falling back to current timestamp for ingested_at", record=record)
            ingested_at = now_us

        try:
            received_at = self._coerce_microseconds(received_raw, default=ingested_at)
        except ValueError:
            received_at = ingested_at

        producer = self._normalize_producer(working.pop("producer", None))
        source_system = working.pop("source", None) or self._producer_base_name()
        market = str(working.pop("market", self.config.market or "UNKNOWN"))

        symbol_value = working.pop("symbol", None)
        instrument_id = working.pop("instrument_id", None)
        trade_id = working.pop("trade_id", None)
        settlement_point = working.pop("settlement_point", None)
        delivery_location = working.pop("delivery_location", None)
        hub = working.pop("hub", None)
        market_id = working.pop("market_id", None)
        curve_type = working.pop("curve_type", None)

        symbol_candidates = [
            symbol_value,
            instrument_id,
            trade_id,
            settlement_point,
            delivery_location,
            hub,
            market_id,
            curve_type,
        ]
        symbol = next((str(val) for val in symbol_candidates if val not in (None, "")), market)

        sequence_val = working.pop("sequence", None)
        if sequence_val is None:
            sequence_val = working.pop("seq", None)
        if sequence_val is None:
            sequence_val = working.pop("offset", None)
        sequence = self._coerce_optional_int(sequence_val)

        retry_raw = working.pop("retry_count", None)
        if retry_raw is None:
            retry_raw = working.pop("retries", None)
        try:
            retry_count = int(retry_raw) if retry_raw not in (None, "") else 0
        except (TypeError, ValueError):
            retry_count = 0

        metadata_field = working.pop("metadata", None)
        encoding = working.pop("encoding", None) or "json"
        checksum_value = working.pop("checksum", None)

        raw_payload_value = working.pop("raw_payload", None)
        raw_data_value = working.pop("raw_data", None)
        if raw_payload_value is not None:
            if isinstance(raw_payload_value, str):
                raw_payload = raw_payload_value
            else:
                raw_payload = json.dumps(raw_payload_value, default=str)
        elif raw_data_value is not None:
            if isinstance(raw_data_value, str):
                raw_payload = raw_data_value
            else:
                raw_payload = json.dumps(raw_data_value, default=str)
        else:
            raw_payload = json.dumps(record, default=str)

        metadata_map = {}
        if isinstance(metadata_field, dict):
            metadata_map.update(self._build_metadata_map(metadata_field))
        elif metadata_field not in (None, "", {}):
            metadata_map["metadata"] = str(metadata_field)

        # Include any remaining fields as metadata for downstream enrichment
        leftover_metadata = self._build_metadata_map(working)
        metadata_map.update(leftover_metadata)

        if instrument_id is not None:
            metadata_map.setdefault("instrument_id", str(instrument_id))
        if trade_id is not None:
            metadata_map.setdefault("trade_id", str(trade_id))
        if settlement_point is not None:
            metadata_map.setdefault("settlement_point", str(settlement_point))
        if delivery_location is not None:
            metadata_map.setdefault("delivery_location", str(delivery_location))
        if hub is not None:
            metadata_map.setdefault("hub", str(hub))
        if market_id is not None:
            metadata_map.setdefault("market_id", str(market_id))
        if curve_type is not None:
            metadata_map.setdefault("curve_type", str(curve_type))

        if not metadata_map:
            metadata_map = {}

        checksum = checksum_value or hashlib.sha256(raw_payload.encode("utf-8")).hexdigest()

        event = {
            "event_id": event_id,
            "trace_id": trace_id,
            "schema_version": schema_version,
            "tenant_id": tenant_id,
            "producer": producer,
            "occurred_at": occurred_at,
            "ingested_at": ingested_at,
            "payload": {
                "source_system": str(source_system),
                "market": str(market),
                "symbol": str(symbol),
                "sequence": sequence,
                "received_at": received_at,
                "raw_payload": raw_payload,
                "encoding": encoding,
                "metadata": metadata_map,
                "retry_count": retry_count,
                "checksum": checksum,
            },
        }

        return event
    
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
        raw_records = transformation_result.data or []
        total_input_records = len(raw_records)
        errors: List[str] = []
        schema_str: Optional[str] = None
        
        if total_input_records == 0:
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

        formatted_records: List[Dict[str, Any]] = []
        conversion_errors: List[str] = []
        
        for idx, record in enumerate(raw_records):
            try:
                formatted_records.append(self._ensure_ingestion_event(record))
            except Exception as exc:
                error_msg = f"Record {idx}: {exc}"
                conversion_errors.append(error_msg)
                self.logger.error(
                    "Failed to format record for ingestion",
                    index=idx,
                    error=str(exc),
                    connector=self.config.name,
                )
        
        errors.extend(conversion_errors)
        conversion_failures = total_input_records - len(formatted_records)
        
        if not formatted_records:
            duration = time.perf_counter() - start_time
            load_metadata = {
                "output_topic": self.config.output_topic,
                "duration_seconds": duration,
                "load_method": "kafka",
                "schema_applied": False,
                "producer_client_id": None,
                "conversion_failures": conversion_failures,
            }
            return LoadResult(
                records_attempted=total_input_records,
                records_published=0,
                records_failed=total_input_records,
                metadata=load_metadata,
                errors=errors,
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
            formatted_records,
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
            "conversion_failures": conversion_failures,
        }
        
        load_result = LoadResult(
            records_attempted=total_input_records,
            records_published=publish_summary.get("success_count", 0),
            records_failed=conversion_failures + publish_summary.get("failure_count", 0),
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
